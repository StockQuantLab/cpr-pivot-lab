"""KiteTicker WebSocket adapter for live paper sessions."""

from __future__ import annotations

import logging
import threading
import warnings
from collections import defaultdict
from datetime import datetime
from typing import Any

from config.settings import get_settings
from engine.live_market_data import IST, FiveMinuteCandleBuilder, MarketSnapshot

logger = logging.getLogger(__name__)


def _coerce_tick_timestamp(tick: dict[str, Any], fallback_now: datetime) -> tuple[datetime, str]:
    """Pick the most authoritative timestamp available on a Kite tick.

    Kite emits exchange timestamps in quote/full mode. Some SDK payloads also
    carry `timestamp` or `last_trade_time`. Prefer those exchange-sourced values
    first and only fall back to local receive time as a last resort.
    """

    for key in ("exchange_timestamp", "timestamp", "last_trade_time"):
        ts = tick.get(key)
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            return ts, key
        if isinstance(ts, str) and ts:
            try:
                parsed = datetime.fromisoformat(ts)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=IST)
            return parsed, key
    return fallback_now, "receive-time"


class KiteTickerAdapter:
    """Thread-safe wrapper around KiteTicker with per-session builders."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        access_token: str | None = None,
        exchange: str = "NSE",
    ) -> None:
        settings = get_settings()
        self._api_key = api_key or settings.kite_api_key
        self._access_token = access_token or settings.kite_access_token
        if not self._api_key or not self._access_token:
            raise ValueError("Kite API key and access token are required for live trading")

        self._exchange = exchange
        self._lock = threading.RLock()
        self._ticker: Any | None = None
        self._connected = threading.Event()
        self._instruments_loaded = False
        self._token_to_symbol: dict[int, str] = {}
        self._symbol_to_token: dict[str, int] = {}
        self._subscribed_tokens: set[int] = set()
        self._last_ltp: dict[str, float] = {}
        self._tick_count = 0
        self._last_tick_ts: datetime | None = None
        self._reconnect_count = 0
        self._session_builders: dict[str, FiveMinuteCandleBuilder] = {}
        self._session_symbols: dict[str, set[str]] = {}
        # Per-symbol tick timestamps for coverage telemetry.
        self._symbol_last_tick_ts: dict[str, datetime] = {}
        self._last_close_ts: datetime | None = None
        self._close_count = 0
        self._last_recovery_attempt_ts: datetime | None = None
        self._last_recovery_success_ts: datetime | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_ts(self) -> datetime | None:
        return self._last_tick_ts

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    def get_last_ltp(self, symbol: str) -> float | None:
        with self._lock:
            return self._last_ltp.get(symbol)

    def register_session(
        self,
        session_id: str,
        symbols: list[str],
        builder: FiveMinuteCandleBuilder,
    ) -> None:
        wanted = {s.strip() for s in symbols if s and s.strip()}
        with self._lock:
            self._session_builders[session_id] = builder
            self._session_symbols[session_id] = wanted
            union_symbols = self._union_symbols_locked()
        self.connect(sorted(union_symbols))
        self.update_symbols(session_id, sorted(wanted))

    def unregister_session(self, session_id: str) -> None:
        should_close = False
        with self._lock:
            self._session_builders.pop(session_id, None)
            self._session_symbols.pop(session_id, None)
            should_close = not self._session_builders
        if should_close:
            self.close()
            return
        self._reconcile_subscriptions()

    def connect(self, symbols: list[str]) -> None:
        want = sorted({s.strip() for s in symbols if s and s.strip()})
        if not want:
            return
        self._resolve_tokens(want)

        with self._lock:
            if self._ticker is not None and self.is_connected:
                self._reconcile_subscriptions()
                return
            if self._ticker is not None and not self.is_connected:
                self.close()

            from kiteconnect import KiteTicker

            warnings.filterwarnings(
                "ignore",
                message=".*signal only works in main thread.*",
            )
            self._ticker = KiteTicker(self._api_key, self._access_token)
            self._ticker.on_ticks = self._on_ticks
            self._ticker.on_connect = self._on_connect
            self._ticker.on_close = self._on_close
            self._ticker.on_error = self._on_error
            self._ticker.on_reconnect = self._on_reconnect
            self._connected.clear()
            self._ticker.connect(threaded=True)

        # Guard against TCP connect hanging indefinitely after a network interface
        # change. The WebSocket thread may block on DNS/TCP connect with no timeout.
        # The 15-second wait on the callback event covers normal startup; an
        # additional 15-second limit ensures we never hang past 30s total.
        if not self._connected.wait(timeout=30):
            self.close()
            raise ConnectionError(
                "KiteTicker did not connect within 30 seconds (network may have changed)"
            )

    def close(self) -> None:
        ticker = None
        with self._lock:
            ticker = self._ticker
            self._ticker = None
            self._subscribed_tokens.clear()
            self._connected.clear()
        if ticker is not None:
            try:
                ticker.close()
            except Exception:
                logger.exception("KiteTicker close failed")

    def recover_connection(
        self,
        *,
        now: datetime | None = None,
        reconnect_after_sec: float = 30.0,
        cooldown_sec: float = 30.0,
    ) -> dict[str, Any]:
        """Recreate the underlying KiteTicker if a disconnect has stalled.

        The Kite client already tries to reconnect internally. This watchdog only
        intervenes when the socket has been down long enough that we should discard
        the current client object and recreate it from the session registry.
        """

        current_time = now or datetime.now(IST)
        reconnect_after_sec = max(0.0, float(reconnect_after_sec or 0.0))
        cooldown_sec = max(0.0, float(cooldown_sec or 0.0))
        with self._lock:
            union_symbols = sorted(self._union_symbols_locked())
            connected = self._connected.is_set()
            last_close = self._last_close_ts
            last_attempt = self._last_recovery_attempt_ts
            last_success = self._last_recovery_success_ts
            close_count = self._close_count
            reconnect_count = self._reconnect_count
        if not union_symbols:
            return {
                "action": "noop",
                "reason": "no_sessions",
                "connected": connected,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }
        if connected:
            return {
                "action": "noop",
                "reason": "connected",
                "connected": True,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }
        if last_close is None:
            return {
                "action": "noop",
                "reason": "no_close_seen",
                "connected": False,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }

        down_sec = max(0.0, (current_time - last_close).total_seconds())
        if down_sec < reconnect_after_sec:
            return {
                "action": "cooldown",
                "reason": "waiting_for_internal_reconnect",
                "connected": False,
                "down_sec": down_sec,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }
        if (
            last_attempt is not None
            and (current_time - last_attempt).total_seconds() < cooldown_sec
        ):
            return {
                "action": "cooldown",
                "reason": "recent_watchdog_attempt",
                "connected": False,
                "down_sec": down_sec,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }

        with self._lock:
            self._last_recovery_attempt_ts = current_time
        logger.warning(
            "KiteTicker watchdog recreating client down_sec=%.0f closes=%d reconnects=%d sessions=%d",
            down_sec,
            close_count,
            reconnect_count,
            len(union_symbols),
        )
        try:
            self.connect(union_symbols)
        except Exception as exc:
            logger.exception("KiteTicker watchdog reconnect failed")
            return {
                "action": "failed",
                "reason": "connect_failed",
                "error": str(exc),
                "connected": False,
                "down_sec": down_sec,
                "close_count": close_count,
                "reconnect_count": reconnect_count,
            }

        with self._lock:
            self._last_recovery_success_ts = current_time
        return {
            "action": "recovered",
            "reason": "recreated_client",
            "connected": self._connected.is_set(),
            "down_sec": down_sec,
            "close_count": close_count,
            "reconnect_count": self._reconnect_count,
            "last_success_ts": last_success.isoformat() if last_success else None,
        }

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        wanted = {s.strip() for s in symbols if s and s.strip()}
        with self._lock:
            if session_id not in self._session_builders:
                return
            self._session_symbols[session_id] = wanted
            union_symbols = self._union_symbols_locked()
        if union_symbols:
            self._resolve_tokens(sorted(union_symbols))
        self._reconcile_subscriptions()

    def synthesize_quiet_symbols(self, session_id: str, symbols: list[str], now: datetime) -> None:
        with self._lock:
            builder = self._session_builders.get(session_id)
            if builder is None:
                return
            last_ltp = dict(self._last_ltp)
        for sym in symbols:
            price = last_ltp.get(sym)
            if price is None:
                continue
            builder.ingest(
                MarketSnapshot(
                    symbol=sym,
                    ts=now,
                    last_price=float(price),
                    volume=None,
                    source="websocket_synthetic",
                )
            )

    def drain_closed(self, session_id: str) -> list[Any]:
        with self._lock:
            builder = self._session_builders.get(session_id)
        if builder is None:
            return []
        return builder.drain_closed()

    def _resolve_tokens(self, symbols: list[str]) -> None:
        wanted = {s.strip() for s in symbols if s and s.strip()}
        if not wanted:
            return
        with self._lock:
            missing = wanted - set(self._symbol_to_token)
            if not missing:
                return
            need_fetch = not self._instruments_loaded
        if not need_fetch:
            logger.warning(
                "Instrument tokens missing for %d symbols (not in cached map): %s",
                len(missing),
                sorted(missing)[:10],
            )
            return

        from kiteconnect import KiteConnect

        kite = KiteConnect(api_key=self._api_key)
        kite.set_access_token(self._access_token)
        instruments = kite.instruments(self._exchange)
        token_map: dict[str, int] = {}
        for inst in instruments:
            if str(inst.get("exchange") or "").upper() != self._exchange.upper():
                continue
            symbol = str(inst.get("tradingsymbol") or "")
            if not symbol:
                continue
            token = int(inst.get("instrument_token"))
            token_map[symbol] = token

        with self._lock:
            self._instruments_loaded = True
            for symbol, token in token_map.items():
                self._symbol_to_token[symbol] = token
                self._token_to_symbol[token] = symbol
            still_missing = wanted - set(self._symbol_to_token)
        if still_missing:
            logger.warning(
                "Instrument tokens not found for %d symbols: %s",
                len(still_missing),
                sorted(still_missing)[:10],
            )

    def _union_symbols_locked(self) -> set[str]:
        union: set[str] = set()
        for symbols in self._session_symbols.values():
            union.update(symbols)
        return union

    def _reconcile_subscriptions(self) -> None:
        with self._lock:
            ticker = self._ticker
            if ticker is None or not self.is_connected:
                return
            needed_symbols = self._union_symbols_locked()
            needed_tokens = {
                self._symbol_to_token[s] for s in needed_symbols if s in self._symbol_to_token
            }
            current_tokens = set(self._subscribed_tokens)
            to_sub = sorted(needed_tokens - current_tokens)
            to_unsub = sorted(current_tokens - needed_tokens)

        if to_unsub:
            try:
                ticker.unsubscribe(to_unsub)
            except Exception:
                logger.exception("KiteTicker unsubscribe failed for %s", to_unsub[:10])
        if to_sub:
            try:
                ticker.subscribe(to_sub)
                ticker.set_mode(ticker.MODE_QUOTE, to_sub)
            except Exception:
                logger.exception("KiteTicker subscribe failed for %s", to_sub[:10])
        with self._lock:
            self._subscribed_tokens = needed_tokens

    def _on_connect(self, ws: Any, _response: Any) -> None:
        self._connected.set()
        self._reconnect_count = 0
        self._reconcile_subscriptions()
        logger.info("KiteTicker connected")

    def _on_ticks(self, _ws: Any, ticks: list[dict[str, Any]]) -> None:
        if not ticks:
            return
        now = datetime.now(IST)
        with self._lock:
            session_builders = dict(self._session_builders)
            session_symbols = {k: set(v) for k, v in self._session_symbols.items()}
            token_to_symbol = dict(self._token_to_symbol)
        symbol_to_builders: dict[str, list[FiveMinuteCandleBuilder]] = defaultdict(list)
        for session_id, symbols in session_symbols.items():
            builder = session_builders.get(session_id)
            if builder is None:
                continue
            for symbol in symbols:
                symbol_to_builders[symbol].append(builder)

        snapshots_by_symbol: dict[str, list[MarketSnapshot]] = defaultdict(list)
        ltp_updates: dict[str, float] = {}
        tick_count = 0
        fallback_count = 0
        latest_ts: datetime | None = None
        for tick in ticks:
            token = tick.get("instrument_token")
            if token is None:
                continue
            tick_symbol = token_to_symbol.get(int(token))
            if not tick_symbol:
                continue
            symbol = tick_symbol
            last_price = tick.get("last_price")
            if last_price is None:
                continue
            ts, ts_source = _coerce_tick_timestamp(tick, now)
            if ts_source != "exchange_timestamp":
                fallback_count += 1
            volume = tick.get("volume_traded")
            snapshot = MarketSnapshot(
                symbol=symbol,
                ts=ts,
                last_price=float(last_price),
                volume=float(volume) if volume is not None else None,
                source="websocket",
            )
            snapshots_by_symbol[symbol].append(snapshot)
            ltp_updates[symbol] = float(last_price)
            tick_count += 1
            latest_ts = ts

        if tick_count == 0:
            return

        if fallback_count > 0:
            logger.debug(
                "exchange_timestamp missing on %d/%d ticks in batch — used timestamp fallback where available, else receive-time",
                fallback_count,
                tick_count,
            )

        with self._lock:
            self._last_ltp.update(ltp_updates)
            self._tick_count += tick_count
            if latest_ts is not None:
                self._last_tick_ts = latest_ts
            for sym, snaps in snapshots_by_symbol.items():
                if snaps:
                    self._symbol_last_tick_ts[sym] = snaps[-1].ts

        builder_batches: dict[FiveMinuteCandleBuilder, list[MarketSnapshot]] = defaultdict(list)
        for symbol, snapshots in snapshots_by_symbol.items():
            for builder in symbol_to_builders.get(symbol, []):
                builder_batches[builder].extend(snapshots)

        for builder, batch in builder_batches.items():
            if hasattr(builder, "ingest_many"):
                builder.ingest_many(batch)
            else:
                for snapshot in batch:
                    builder.ingest(snapshot)

    def _on_close(self, _ws: Any, code: Any, reason: Any) -> None:
        self._connected.clear()
        with self._lock:
            self._close_count += 1
            self._last_close_ts = datetime.now(IST)
        logger.warning(
            "KiteTicker closed code=%s reason=%s close_count=%d — waiting on "
            "internal reconnect or watchdog recreation",
            code,
            reason,
            self._close_count,
        )

    def _on_error(self, _ws: Any, code: Any, reason: Any) -> None:
        logger.error("KiteTicker error code=%s reason=%s", code, reason)

    def _on_reconnect(self, _ws: Any, attempts: int) -> None:
        self._reconnect_count = int(attempts or 0)
        logger.warning("KiteTicker reconnecting attempt=%d", self._reconnect_count)

    def health_stats(self) -> dict[str, Any]:
        """Return a telemetry snapshot for logging / alerting."""
        now = datetime.now(IST)
        with self._lock:
            last_tick = self._last_tick_ts
            last_close = self._last_close_ts
            tick_count = self._tick_count
            close_count = self._close_count
            reconnect_count = self._reconnect_count
            recovery_attempt_ts = self._last_recovery_attempt_ts
            recovery_success_ts = self._last_recovery_success_ts
            subscribed = len(self._subscribed_tokens)
            sessions = list(self._session_symbols.keys())
            per_symbol = dict(self._symbol_last_tick_ts)
        last_tick_age_sec: float | None = None
        if last_tick is not None:
            last_tick_age_sec = (now - last_tick).total_seconds()
        return {
            "connected": self._connected.is_set(),
            "tick_count": tick_count,
            "last_tick_ts": last_tick.isoformat() if last_tick else None,
            "last_tick_age_sec": last_tick_age_sec,
            "last_close_ts": last_close.isoformat() if last_close else None,
            "close_count": close_count,
            "reconnect_count": reconnect_count,
            "last_recovery_attempt_ts": (
                recovery_attempt_ts.isoformat() if recovery_attempt_ts else None
            ),
            "last_recovery_success_ts": (
                recovery_success_ts.isoformat() if recovery_success_ts else None
            ),
            "subscribed_tokens": subscribed,
            "sessions": sessions,
            "per_symbol_last_tick_count": len(per_symbol),
        }

    def symbol_coverage(self, symbols: list[str], within_sec: float = 300.0) -> dict[str, Any]:
        """Return tick-coverage stats for a specific symbol set.

        covered = symbols whose last tick is within ``within_sec`` seconds.
        """
        now = datetime.now(IST)
        with self._lock:
            per_symbol = dict(self._symbol_last_tick_ts)
        covered = 0
        stale: list[str] = []
        missing: list[str] = []
        for sym in symbols:
            ts = per_symbol.get(sym)
            if ts is None:
                missing.append(sym)
                continue
            if (now - ts).total_seconds() <= within_sec:
                covered += 1
            else:
                stale.append(sym)
        total = max(1, len(symbols))
        return {
            "total": len(symbols),
            "covered": covered,
            "stale": len(stale),
            "missing": len(missing),
            "coverage_pct": 100.0 * covered / total,
            "stale_sample": stale[:10],
            "missing_sample": missing[:10],
        }
