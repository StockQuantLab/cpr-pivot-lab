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

        if not self._connected.wait(timeout=15):
            raise ConnectionError("KiteTicker did not connect within 15 seconds")

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
            ts = tick.get("exchange_timestamp")
            if not isinstance(ts, datetime):
                ts = now
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

        with self._lock:
            self._last_ltp.update(ltp_updates)
            self._tick_count += tick_count
            if latest_ts is not None:
                self._last_tick_ts = latest_ts

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
        logger.warning("KiteTicker closed code=%s reason=%s", code, reason)

    def _on_error(self, _ws: Any, code: Any, reason: Any) -> None:
        logger.error("KiteTicker error code=%s reason=%s", code, reason)

    def _on_reconnect(self, _ws: Any, attempts: int) -> None:
        self._reconnect_count = int(attempts or 0)
        logger.warning("KiteTicker reconnecting attempt=%d", self._reconnect_count)
