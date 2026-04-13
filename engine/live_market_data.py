"""Broker-agnostic live market-data adapter and candle builder."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable
from zoneinfo import ZoneInfo

from config.settings import get_settings

IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    symbol: str
    ts: datetime
    last_price: float
    volume: float | None = None
    source: str = "quote"


@dataclass(frozen=True, slots=True)
class ClosedCandle:
    symbol: str
    bar_start: datetime
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_snapshot_ts: datetime
    last_snapshot_ts: datetime


@runtime_checkable
class MarketDataAdapter(Protocol):
    def poll(self, symbols: Sequence[str]) -> list[MarketSnapshot]:
        """Fetch the latest quote or tick snapshots for the requested symbols."""


@dataclass(slots=True)
class _CandleState:
    bar_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_snapshot_ts: datetime
    last_snapshot_ts: datetime


class FiveMinuteCandleBuilder:
    """Aggregate snapshots into deterministic 5-minute closed candles."""

    def __init__(self, interval_minutes: int = 5):
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be positive")
        self.interval_minutes = interval_minutes
        self._states: dict[str, _CandleState] = {}
        self._pending_closed: list[ClosedCandle] = []
        self._prev_cumulative_vol: dict[str, float] = {}
        self._lock = threading.RLock()

    def _bucket_start(self, ts: datetime) -> datetime:
        minute = (ts.hour * 60 + ts.minute) // self.interval_minutes * self.interval_minutes
        return ts.replace(
            hour=minute // 60,
            minute=minute % 60,
            second=0,
            microsecond=0,
        )

    def _close_state(self, symbol: str, state: _CandleState) -> ClosedCandle:
        return ClosedCandle(
            symbol=symbol,
            bar_start=state.bar_start,
            bar_end=state.bar_start + timedelta(minutes=self.interval_minutes),
            open=state.open,
            high=state.high,
            low=state.low,
            close=state.close,
            volume=state.volume,
            first_snapshot_ts=state.first_snapshot_ts,
            last_snapshot_ts=state.last_snapshot_ts,
        )

    def _ingest_locked(self, snapshot: MarketSnapshot) -> list[ClosedCandle]:
        bucket_start = self._bucket_start(snapshot.ts)
        bar_volume_delta = 0.0
        if snapshot.volume is not None:
            cumulative = float(snapshot.volume)
            prev = self._prev_cumulative_vol.get(snapshot.symbol)
            bar_volume_delta = (
                max(0.0, cumulative - prev) if prev is not None else max(0.0, cumulative)
            )
            self._prev_cumulative_vol[snapshot.symbol] = cumulative

        state = self._states.get(snapshot.symbol)
        if state is None:
            self._states[snapshot.symbol] = _CandleState(
                bar_start=bucket_start,
                open=snapshot.last_price,
                high=snapshot.last_price,
                low=snapshot.last_price,
                close=snapshot.last_price,
                volume=bar_volume_delta,
                first_snapshot_ts=snapshot.ts,
                last_snapshot_ts=snapshot.ts,
            )
            return []

        if bucket_start < state.bar_start:
            return []

        if bucket_start == state.bar_start:
            state.high = max(state.high, snapshot.last_price)
            state.low = min(state.low, snapshot.last_price)
            state.close = snapshot.last_price
            state.volume += bar_volume_delta
            state.last_snapshot_ts = snapshot.ts
            return []

        closed = [self._close_state(snapshot.symbol, state)]
        self._pending_closed.extend(closed)
        self._states[snapshot.symbol] = _CandleState(
            bar_start=bucket_start,
            open=snapshot.last_price,
            high=snapshot.last_price,
            low=snapshot.last_price,
            close=snapshot.last_price,
            volume=bar_volume_delta,
            first_snapshot_ts=snapshot.ts,
            last_snapshot_ts=snapshot.ts,
        )
        return closed

    def ingest(self, snapshot: MarketSnapshot) -> list[ClosedCandle]:
        with self._lock:
            return self._ingest_locked(snapshot)

    def ingest_many(self, snapshots: Sequence[MarketSnapshot]) -> list[ClosedCandle]:
        if not snapshots:
            return []
        closed: list[ClosedCandle] = []
        with self._lock:
            for snapshot in snapshots:
                closed.extend(self._ingest_locked(snapshot))
        return closed

    def flush(self, symbol: str | None = None) -> list[ClosedCandle]:
        with self._lock:
            if symbol is None:
                symbols = list(self._states)
            else:
                symbols = [symbol]
            closed: list[ClosedCandle] = []
            for sym in symbols:
                state = self._states.pop(sym, None)
                if state is not None:
                    closed.append(self._close_state(sym, state))
            self._pending_closed.extend(closed)
            return closed

    def drain_closed(self) -> list[ClosedCandle]:
        with self._lock:
            closed = list(self._pending_closed)
            self._pending_closed.clear()
            return closed


class KiteQuoteAdapter:
    """Poll-based Kite adapter that returns quote snapshots."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        access_token: str | None = None,
        exchange: str = "NSE",
        quote_mode: str = "ltp",
        quote_batch_size: int | None = None,
        tz: ZoneInfo = IST,
    ):
        settings = get_settings()
        api_key = api_key or settings.kite_api_key
        access_token = access_token or settings.kite_access_token
        if not api_key or not access_token:
            raise ValueError("Kite API key and access token are required for live paper trading")

        from kiteconnect import KiteConnect

        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)
        self.exchange = exchange
        self.quote_mode = quote_mode.lower().strip()
        self.quote_batch_size = max(
            1, int(quote_batch_size or settings.paper_live_quote_batch_size)
        )
        self.tz = tz

    def _iter_batches(self, symbols: Sequence[str]) -> list[list[str]]:
        requested = [s.strip() for s in symbols if s and s.strip()]
        if not requested:
            return []
        size = max(1, int(self.quote_batch_size))
        return [requested[idx : idx + size] for idx in range(0, len(requested), size)]

    def poll(self, symbols: Sequence[str]) -> list[MarketSnapshot]:
        requested = list(dict.fromkeys(s for s in (s.strip() for s in symbols) if s))
        if not requested:
            return []

        payload: dict[str, dict[str, object]] = {}
        for batch in self._iter_batches(requested):
            keys = [f"{self.exchange}:{symbol}" for symbol in batch]
            if self.quote_mode == "quote":
                batch_payload = self._kite.quote(keys)
            else:
                batch_payload = self._kite.ltp(keys)
            payload.update(batch_payload)

        now = datetime.now(self.tz)
        snapshots: list[MarketSnapshot] = []
        for symbol in requested:
            key = f"{self.exchange}:{symbol}"
            item = payload.get(key)
            if not item:
                continue
            last_price = item.get("last_price")
            if last_price is None:
                continue
            volume = item.get("volume_traded")
            if volume is None:
                volume = item.get("volume")
            symbol = key.split(":", 1)[1]
            snapshots.append(
                MarketSnapshot(
                    symbol=symbol,
                    ts=now,
                    last_price=float(last_price),
                    volume=float(volume) if volume is not None else None,
                    source=self.quote_mode,
                )
            )
        return snapshots
