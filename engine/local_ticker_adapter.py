"""DuckDB-backed mock ticker adapter for local feed testing of the live paper workflow.

Replaces KiteTickerAdapter for --feed-source local mode. Emits ClosedCandle objects
directly from intraday_day_pack data — no synthetic ticks, no builder dependency.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from typing import Any

from db.duckdb import _use_market_read_replica, get_dashboard_db, get_live_market_db
from engine.cpr_atr_strategy import DayPack
from engine.live_market_data import IST, ClosedCandle, FiveMinuteCandleBuilder

logger = logging.getLogger(__name__)


def _has_column(db: Any, table: str, column: str) -> bool:
    checker = getattr(db, "_table_has_column", None)
    if not callable(checker):
        return False
    try:
        return bool(checker(table, column))
    except Exception:
        return False


def _minute_to_time_str(minute_of_day: int | float | str) -> str:
    total = int(minute_of_day)
    return f"{total // 60:02d}:{total % 60:02d}"


def _combine_bar_ts(trade_date: str, time_str: str) -> datetime:
    trade_day = date.fromisoformat(str(trade_date)[:10])
    candle_time = dt_time.fromisoformat(time_str)
    return datetime.combine(trade_day, candle_time, tzinfo=IST)


def _load_day_packs(
    trade_date: str,
    symbols: list[str],
) -> dict[str, DayPack]:
    """Load intraday_day_pack for one date, return {symbol: DayPack}."""
    # Use the explicit live market accessor so local live/replay validation is
    # pinned to the market source rather than the dashboard replica contract.
    db = get_dashboard_db() if _use_market_read_replica() else get_live_market_db()
    pack_time_mode = (
        "minute_arr" if _has_column(db, "intraday_day_pack", "minute_arr") else "time_arr"
    )
    rvol_select = (
        "p.rvol_baseline_arr"
        if _has_column(db, "intraday_day_pack", "rvol_baseline_arr")
        else "NULL::DOUBLE[] AS rvol_baseline_arr"
    )
    pack_time_col = (
        "p.minute_arr AS pack_time_arr"
        if pack_time_mode == "minute_arr"
        else "p.time_arr AS pack_time_arr"
    )
    query = f"""
        SELECT
            p.symbol,
            p.trade_date::VARCHAR AS trade_date,
            {pack_time_col},
            p.open_arr,
            p.high_arr,
            p.low_arr,
            p.close_arr,
            p.volume_arr,
            {rvol_select}
        FROM intraday_day_pack p
        WHERE list_contains($symbols, p.symbol)
          AND p.trade_date = $trade_date::DATE
        ORDER BY p.symbol
    """
    params: dict[str, object] = {
        "symbols": list(symbols),
        "trade_date": trade_date,
    }
    pack_df = db.con.execute(query, params).pl()
    if pack_df.is_empty():
        return {}

    result: dict[str, DayPack] = {}
    for idx in range(pack_df.height):
        sym = pack_df["symbol"].to_list()[idx]
        raw_times = pack_df["pack_time_arr"].to_list()[idx]
        opens = pack_df["open_arr"].to_list()[idx]
        highs = pack_df["high_arr"].to_list()[idx]
        lows = pack_df["low_arr"].to_list()[idx]
        closes = pack_df["close_arr"].to_list()[idx]
        volumes = pack_df["volume_arr"].to_list()[idx]
        rvol_arrs = pack_df["rvol_baseline_arr"].to_list()[idx]

        if not raw_times:
            continue
        if pack_time_mode == "minute_arr":
            times = [_minute_to_time_str(t) for t in raw_times]
        else:
            times = [str(t) for t in raw_times]

        if not (len(times) == len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)):
            continue

        result[sym] = DayPack(
            time_str=times,
            opens=[float(x) for x in opens],
            highs=[float(x) for x in highs],
            lows=[float(x) for x in lows],
            closes=[float(x) for x in closes],
            volumes=[float(x) for x in volumes],
            rvol_baseline=[float(v) if v is not None else None for v in rvol_arrs]
            if rvol_arrs
            else None,
        )
    return result


class LocalTickerAdapter:
    """DuckDB-backed mock of KiteTickerAdapter for --feed-source local mode.

    Emits ClosedCandle objects directly from intraday_day_pack data.
    Uses per-session bar cursors so variants that register a few scheduler
    ticks apart still see the same historical session from the first bar.
    Marker attribute ``_local_feed = True`` lets paper_live.py detect local mode.
    """

    _local_feed = True  # marker for paper_live.py detection

    def __init__(
        self,
        *,
        trade_date: str,
        symbols: list[str],
        candle_interval_minutes: int = 5,
    ) -> None:
        self._trade_date = trade_date
        self._candle_interval = candle_interval_minutes
        self._lock = threading.RLock()

        # Load data
        self._symbol_packs = _load_day_packs(trade_date, symbols)
        if not self._symbol_packs:
            raise RuntimeError(
                f"No intraday_day_pack data found for date={trade_date} "
                f"symbols={len(symbols)}. Run pivot-build --table pack first."
            )

        # Compute sorted union of all bar times across all symbols
        all_times: set[str] = set()
        for pack in self._symbol_packs.values():
            all_times.update(pack.time_str)
        self._sorted_bar_times = sorted(all_times)
        self._total_bars = len(self._sorted_bar_times)

        # Per-session state
        self._session_symbols: dict[str, set[str]] = {}
        self._session_bar_idx: dict[str, int] = {}
        self._session_exhausted: dict[str, bool] = {}

        # Metrics
        self._tick_count = 0
        self._last_tick_ts: datetime | None = None
        self._last_ltp: dict[str, float] = {}

    # -- Properties (KiteTickerAdapter-compatible interface) --

    @property
    def is_connected(self) -> bool:
        return True

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_ts(self) -> datetime | None:
        return self._last_tick_ts

    @property
    def reconnect_count(self) -> int:
        return 0

    def get_last_ltp(self, symbol: str) -> float | None:
        return self._last_ltp.get(symbol)

    # -- Session management --

    def register_session(
        self,
        session_id: str,
        symbols: list[str],
        builder: FiveMinuteCandleBuilder,
    ) -> None:
        wanted = {s.strip() for s in symbols if s and s.strip()}
        with self._lock:
            self._session_symbols[session_id] = wanted
            self._session_bar_idx[session_id] = 0
            self._session_exhausted[session_id] = False
        n_available = len(wanted & set(self._symbol_packs.keys()))
        logger.info(
            "LocalTicker register session=%s symbols=%d available=%d trade_date=%s",
            session_id,
            len(wanted),
            n_available,
            self._trade_date,
        )

    def unregister_session(self, session_id: str) -> None:
        with self._lock:
            self._session_symbols.pop(session_id, None)
            self._session_bar_idx.pop(session_id, None)
            self._session_exhausted.pop(session_id, None)

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        wanted = {s.strip() for s in symbols if s and s.strip()}
        with self._lock:
            if session_id in self._session_symbols:
                self._session_symbols[session_id] = wanted

    def synthesize_quiet_symbols(
        self,
        session_id: str,
        symbols: list[str],
        now: datetime,
    ) -> None:
        """No-op: all symbols get data from the day pack."""

    # -- Core data pump --

    def drain_closed(self, session_id: str) -> list[ClosedCandle]:
        """Advance one session by one bar and return that session's candles."""
        delay_sec = float(os.getenv("PIVOT_LOCAL_FEED_BAR_DELAY_SEC", "0") or "0")
        if delay_sec > 0:
            time.sleep(delay_sec)
        with self._lock:
            sym_set = self._session_symbols.get(session_id)
            if not sym_set:
                return []

            if self._session_exhausted.get(session_id, False):
                return []

            bar_idx = self._session_bar_idx.get(session_id, 0)
            if bar_idx >= self._total_bars:
                self._session_exhausted[session_id] = True
                return []

            bar_time_str = self._sorted_bar_times[bar_idx]
            bar_end = _combine_bar_ts(self._trade_date, bar_time_str)
            bar_start = bar_end - timedelta(minutes=self._candle_interval)
            self._session_bar_idx[session_id] = bar_idx + 1

            result: list[ClosedCandle] = []
            for symbol in sorted(sym_set):
                pack = self._symbol_packs.get(symbol)
                if pack is None:
                    continue
                candle_idx = pack._idx_by_time.get(bar_time_str)
                if candle_idx is None:
                    last_ltp = self._last_ltp.get(symbol)
                    if last_ltp is None:
                        continue
                    candle = ClosedCandle(
                        symbol=symbol,
                        bar_start=bar_start,
                        bar_end=bar_end,
                        open=float(last_ltp),
                        high=float(last_ltp),
                        low=float(last_ltp),
                        close=float(last_ltp),
                        volume=0.0,
                        first_snapshot_ts=bar_start,
                        last_snapshot_ts=bar_end,
                    )
                else:
                    candle = ClosedCandle(
                        symbol=symbol,
                        bar_start=bar_start,
                        bar_end=bar_end,
                        open=float(pack.opens[candle_idx]),
                        high=float(pack.highs[candle_idx]),
                        low=float(pack.lows[candle_idx]),
                        close=float(pack.closes[candle_idx]),
                        volume=float(pack.volumes[candle_idx]),
                        first_snapshot_ts=bar_start,
                        last_snapshot_ts=bar_end,
                    )
                result.append(candle)
                self._last_ltp[symbol] = candle.close

            if result:
                self._tick_count += len(result)
                self._last_tick_ts = bar_end

            if self._session_bar_idx[session_id] >= self._total_bars:
                self._session_exhausted[session_id] = True

            return result

    def close(self) -> None:
        """No-op: no resources to release."""
