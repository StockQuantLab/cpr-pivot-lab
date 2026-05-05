"""Setup-row loading and refresh helpers for paper/live runtime."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import Any

from db.duckdb import get_live_market_db
from engine.cpr_atr_shared import regime_snapshot_close_col
from engine.cpr_atr_utils import (
    calculate_gap_pct,
    calculate_or_atr_ratio,
    normalize_cpr_bounds,
    resolve_cpr_direction,
)
from engine.paper_params import PaperRuntimeState
from engine.paper_summary import _float_or_none

logger = logging.getLogger(__name__)

_MARKET_DB_READ_LOCK = threading.RLock()


def _build_intraday_summary(
    candles: list[dict[str, Any]],
    *,
    or_minutes: int,
    bar_end_offset: timedelta | None = None,
) -> dict[str, float | bool | None]:
    if not candles:
        return {
            "open_915": None,
            "or_high_5": None,
            "or_low_5": None,
            "or_close_5": None,
            "or_proxy": False,
        }
    first = candles[0]
    first_bar_end = first["bar_end"]
    range_end = datetime.combine(
        first_bar_end.date(),
        dt_time(9, 15, tzinfo=getattr(first_bar_end, "tzinfo", None)),
    ) + timedelta(minutes=max(1, int(or_minutes or 5)))
    # bar_end_offset corrects for pack-convention candles where bar_end stores bar_start
    # time. Live Kite candles already carry the true close time so offset defaults to zero.
    _beo = bar_end_offset or timedelta(0)
    window = [candle for candle in candles if candle["bar_end"] + _beo <= range_end]
    if not window:
        # Late-start continuity mode: synthesize OR from first seen bar when early
        # in-memory bars are unavailable after OR completion.
        return {
            "open_915": _float_or_none(first.get("open")),
            "or_high_5": _float_or_none(first.get("high")),
            "or_low_5": _float_or_none(first.get("low")),
            "or_close_5": _float_or_none(first.get("close")),
            "or_proxy": True,
        }
    if candles[-1]["bar_end"] + _beo < range_end:
        return {
            "open_915": _float_or_none(first.get("open")),
            "or_high_5": None,
            "or_low_5": None,
            "or_close_5": None,
            "or_proxy": False,
        }
    last = window[-1]
    return {
        "open_915": _float_or_none(first.get("open")),
        "or_high_5": max(float(candle["high"]) for candle in window),
        "or_low_5": min(float(candle["low"]) for candle in window),
        "or_close_5": _float_or_none(last.get("close")),
        "or_proxy": False,
    }


def _or_proxy_and_source(live_intraday: dict[str, Any] | None) -> tuple[bool, str]:
    or_proxy = bool((live_intraday or {}).get("or_proxy"))
    setup_source = "market_day_state_or_proxy" if or_proxy else "market_day_state"
    return or_proxy, setup_source


def setup_row_uses_or_proxy(setup_row: dict[str, Any] | None) -> bool:
    if not setup_row:
        return False
    source = str(setup_row.get("setup_source") or "").lower()
    return "late_start" in source or "or_proxy" in source or bool(setup_row.get("or_proxy"))


def _load_live_setup_row(
    symbol: str,
    trade_date: str,
    live_candles: list[dict[str, Any]],
    *,
    or_minutes: int,
    bar_end_offset: timedelta | None = None,
) -> dict[str, Any] | None:
    db = get_live_market_db()
    with _MARKET_DB_READ_LOCK:
        setup_base = db.con.execute(
            """
            WITH prev_daily AS (
                SELECT date::VARCHAR AS prev_date, high, low, close
                FROM v_daily
                WHERE symbol = ? AND date < ?::DATE
                ORDER BY date DESC
                LIMIT 1
            ),
            prev_atr AS (
                SELECT trade_date::VARCHAR AS atr_prev_date, atr
                FROM atr_intraday
                WHERE symbol = ? AND trade_date < ?::DATE
                ORDER BY trade_date DESC
                LIMIT 1
            ),
            prev_threshold AS (
                SELECT trade_date::VARCHAR AS threshold_prev_date, cpr_threshold_pct
                FROM cpr_thresholds
                WHERE symbol = ? AND trade_date < ?::DATE
                ORDER BY trade_date DESC
                LIMIT 1
            )
            SELECT
                d.prev_date,
                d.high,
                d.low,
                d.close,
                a.atr_prev_date,
                a.atr,
                t.threshold_prev_date,
                t.cpr_threshold_pct
            FROM prev_daily d
            LEFT JOIN prev_atr a ON TRUE
            LEFT JOIN prev_threshold t ON TRUE
            """,
            [symbol, trade_date, symbol, trade_date, symbol, trade_date],
        ).fetchone()
    if not setup_base:
        return None

    prev_date = str(setup_base[0])
    prev_high = float(setup_base[1] or 0.0)
    prev_low = float(setup_base[2] or 0.0)
    prev_close = float(setup_base[3] or 0.0)
    pivot = (prev_high + prev_low + prev_close) / 3.0
    bc = (prev_high + prev_low) / 2.0
    tc = (pivot + bc) / 2.0
    cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
    cpr_width_pct = abs(tc - bc) / pivot * 100 if pivot else 0.0
    r1 = 2.0 * pivot - prev_low
    s1 = 2.0 * pivot - prev_high
    r2 = pivot + (prev_high - prev_low)
    s2 = pivot - (prev_high - prev_low)

    if setup_base[4] is None or setup_base[5] is None:
        return None
    atr_prev_date = str(setup_base[4])
    if atr_prev_date != prev_date:
        logger.warning(
            "Live setup row for %s on %s: daily prev_date=%s != atr prev_date=%s - skipping",
            symbol,
            trade_date,
            prev_date,
            atr_prev_date,
        )
        return None
    atr = float(setup_base[5] or 0.0)
    if atr <= 0:
        return None

    if setup_base[6] is None or setup_base[7] is None:
        return None
    threshold_prev_date = str(setup_base[6])
    if threshold_prev_date != prev_date:
        logger.warning(
            "Live setup row for %s on %s: daily prev_date=%s != threshold prev_date=%s - skipping",
            symbol,
            trade_date,
            prev_date,
            threshold_prev_date,
        )
        return None
    cpr_threshold = float(setup_base[7])

    intraday = _build_intraday_summary(
        live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
    )
    open_915 = intraday["open_915"]
    or_high_5 = intraday["or_high_5"]
    or_low_5 = intraday["or_low_5"]
    or_close_5 = intraday["or_close_5"]
    if open_915 is None or or_high_5 is None or or_low_5 is None:
        return None

    if open_915 < cpr_lower:
        open_side = "BELOW"
    elif open_915 > cpr_upper:
        open_side = "ABOVE"
    else:
        open_side = "INSIDE"
    direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
    setup_source = "live_fallback_late_start" if bool(intraday.get("or_proxy")) else "live_fallback"
    return {
        "trade_date": trade_date,
        "prev_day_close": prev_close,
        "tc": tc,
        "bc": bc,
        "pivot": pivot,
        "r1": r1,
        "s1": s1,
        "r2": r2,
        "s2": s2,
        "atr": atr,
        "cpr_width_pct": cpr_width_pct,
        "cpr_threshold": cpr_threshold,
        "or_high_5": or_high_5,
        "or_low_5": or_low_5,
        "open_915": open_915,
        "or_close_5": or_close_5,
        "open_side": open_side,
        "open_to_cpr_atr": abs(open_915 - (cpr_lower if open_side == "BELOW" else cpr_upper)) / atr
        if open_side in {"BELOW", "ABOVE"}
        else 0.0,
        "gap_abs_pct": abs(calculate_gap_pct(open_915, prev_close)),
        "or_atr_5": calculate_or_atr_ratio(or_high_5, or_low_5, atr),
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": int(cpr_width_pct < cpr_threshold),
        "or_proxy": bool(intraday.get("or_proxy")),
        "setup_source": setup_source,
    }


def load_setup_row(
    symbol: str,
    trade_date: str,
    live_candles: list[dict[str, Any]] | None = None,
    *,
    or_minutes: int = 5,
    allow_live_fallback: bool = True,
    bar_end_offset: timedelta | None = None,
    regime_index_symbol: str | None = None,
    regime_snapshot_minutes: int = 30,
) -> dict[str, Any] | None:
    db = get_live_market_db()
    regime_symbol = str(regime_index_symbol or "").strip().upper()
    regime_close_col = regime_snapshot_close_col(regime_snapshot_minutes)
    with _MARKET_DB_READ_LOCK:
        row = db.con.execute(
            f"""
            SELECT
                m.trade_date::VARCHAR,
                m.prev_close,
                m.tc,
                m.bc,
                m."pivot",
                m.r1,
                m.s1,
                m.r2,
                m.s2,
                m.atr,
                m.cpr_width_pct,
                m.cpr_threshold_pct,
                m.or_high_5,
                m.or_low_5,
                m.open_915,
                m.or_close_5,
                s.open_side,
                s.open_to_cpr_atr,
                s.gap_abs_pct,
                s.or_atr_5,
                s.direction_5,
                m.is_narrowing,
                m.cpr_shift,
                CASE
                    WHEN reg.open_915 > 0 AND reg.{regime_close_col} IS NOT NULL
                    THEN ((reg.{regime_close_col} - reg.open_915) / reg.open_915) * 100.0
                    ELSE NULL
                END AS regime_move_pct
            FROM market_day_state m
            LEFT JOIN strategy_day_state s
              ON s.symbol = m.symbol
             AND s.trade_date = m.trade_date
            LEFT JOIN market_day_state reg
              ON reg.symbol = ? AND reg.trade_date = m.trade_date
            WHERE m.symbol = ? AND m.trade_date = ?::DATE
            LIMIT 1
            """,
            [regime_symbol, symbol, trade_date],
        ).fetchone()
    if not row:
        if not allow_live_fallback:
            return None
        return _load_live_setup_row(
            symbol,
            trade_date,
            live_candles or [],
            or_minutes=or_minutes,
            bar_end_offset=bar_end_offset,
        )

    open_side = str(row[16] or "")
    tc = float(row[2] or 0.0)
    bc = float(row[3] or 0.0)
    atr = float(row[9] or 0.0)
    prev_close = _float_or_none(row[1])
    db_or_high = float(row[12] or 0.0)
    db_or_low = float(row[13] or 0.0)
    db_open_915 = float(row[14] or 0.0)
    or_close_5 = _float_or_none(row[15])
    direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
    if direction == "NONE" and or_close_5 is None:
        direction = str(row[20] or "NONE")
    live_intraday: dict[str, Any] | None = None
    if live_candles and (
        direction == "NONE"
        or db_or_high <= 0.0
        or db_or_low <= 0.0
        or db_open_915 <= 0.0
        or or_close_5 is None
    ):
        intraday = _build_intraday_summary(
            live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
        )
        live_intraday = intraday
        live_or_close_5 = intraday.get("or_close_5")
        if live_or_close_5 is not None:
            direction = resolve_cpr_direction(live_or_close_5, tc, bc, fallback="NONE")
            or_close_5 = live_or_close_5
    or_high_5 = db_or_high
    or_low_5 = db_or_low
    open_915 = db_open_915
    if live_intraday is not None:
        or_high_5 = db_or_high or float(live_intraday.get("or_high_5") or 0.0)
        or_low_5 = db_or_low or float(live_intraday.get("or_low_5") or 0.0)
        open_915 = db_open_915 or float(live_intraday.get("open_915") or 0.0)
        if open_915 > 0 and tc > 0 and bc > 0:
            cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
            if open_915 < cpr_lower:
                open_side = "BELOW"
            elif open_915 > cpr_upper:
                open_side = "ABOVE"
            else:
                open_side = "INSIDE"

    rvol_baseline: list[float | None] | None = None
    try:
        with _MARKET_DB_READ_LOCK:
            pack_row = db.con.execute(
                "SELECT rvol_baseline_arr FROM intraday_day_pack"
                " WHERE symbol = ? AND trade_date = ?::DATE LIMIT 1",
                [symbol, trade_date],
            ).fetchone()
        if pack_row and pack_row[0]:
            rvol_baseline = [float(v) if v is not None else None for v in pack_row[0]]
    except Exception:
        pass

    open_to_cpr_atr = _float_or_none(row[17])
    gap_abs_pct = _float_or_none(row[18])
    or_atr_5 = _float_or_none(row[19])
    if live_intraday is not None and open_915 > 0 and or_high_5 > 0 and or_low_5 > 0 and atr > 0:
        cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
        open_to_cpr_atr = (
            abs(open_915 - (cpr_lower if open_side == "BELOW" else cpr_upper)) / atr
            if open_side in {"BELOW", "ABOVE"}
            else 0.0
        )
        if prev_close is not None:
            gap_abs_pct = abs(calculate_gap_pct(open_915, prev_close))
        or_atr_5 = calculate_or_atr_ratio(or_high_5, or_low_5, atr)

    result = {
        "trade_date": str(row[0] or trade_date),
        "prev_day_close": prev_close,
        "tc": tc,
        "bc": bc,
        "pivot": float(row[4] or 0.0),
        "r1": float(row[5] or 0.0),
        "s1": float(row[6] or 0.0),
        "r2": float(row[7] or 0.0),
        "s2": float(row[8] or 0.0),
        "atr": atr,
        "cpr_width_pct": float(row[10] or 0.0),
        "cpr_threshold": float(row[11] or 0.0),
        "or_high_5": or_high_5,
        "or_low_5": or_low_5,
        "open_915": open_915,
        "or_close_5": or_close_5,
        "open_side": open_side,
        "open_to_cpr_atr": open_to_cpr_atr,
        "gap_abs_pct": gap_abs_pct,
        "or_atr_5": or_atr_5,
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": bool(row[21]),
        "cpr_shift": str(row[22] or "OVERLAP"),
        "regime_move_pct": float(row[23]) if row[23] is not None else None,
        "rvol_baseline": rvol_baseline,
    }
    result["or_proxy"], result["setup_source"] = _or_proxy_and_source(live_intraday)
    return result


def _live_setup_status(setup_row: dict[str, Any] | None) -> str:
    if setup_row is None:
        return "pending"
    if bool(setup_row.get("direction_pending")):
        return "pending"
    direction = str(setup_row.get("direction") or "").upper()
    if direction in {"LONG", "SHORT"}:
        return "candidate"
    return "pending"


def _bar_candle_payload(candle: Any) -> dict[str, Any]:
    return {
        "bar_end": candle.bar_end,
        "open": float(candle.open),
        "high": float(candle.high),
        "low": float(candle.low),
        "close": float(candle.close),
        "volume": float(candle.volume),
    }


def _hydrate_setup_row_from_market_row(
    *,
    trade_date: str,
    row: tuple[Any, ...],
    live_candles: list[dict[str, Any]] | None = None,
    or_minutes: int = 5,
    bar_end_offset: timedelta | None = None,
) -> dict[str, Any] | None:
    tc = float(row[3] or 0.0)
    bc = float(row[4] or 0.0)
    atr = float(row[10] or 0.0)
    if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
        return None
    or_close_5 = float(row[16]) if row[16] is not None else None
    direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
    if direction == "NONE" and or_close_5 is None:
        direction = str(row[21] or "NONE")
    live_intraday: dict[str, Any] | None = None
    if direction == "NONE" and live_candles:
        live_intraday = _build_intraday_summary(
            live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
        )
        live_or_close_5 = live_intraday.get("or_close_5")
        if live_or_close_5 is not None:
            direction = resolve_cpr_direction(live_or_close_5, tc, bc, fallback="NONE")
            or_close_5 = live_or_close_5
    _db_or_high = float(row[13] or 0.0)
    _db_or_low = float(row[14] or 0.0)
    _db_open_915 = float(row[15] or 0.0)
    if live_intraday is not None:
        or_high_5 = _db_or_high or float(live_intraday.get("or_high_5") or 0.0)
        or_low_5 = _db_or_low or float(live_intraday.get("or_low_5") or 0.0)
        open_915_val = _db_open_915 or float(live_intraday.get("open_915") or 0.0)
    else:
        or_high_5, or_low_5, open_915_val = _db_or_high, _db_or_low, _db_open_915
    or_proxy, setup_source = _or_proxy_and_source(live_intraday)
    rvol_baseline: list[float | None] | None = None
    if row[24]:
        rvol_baseline = [float(v) if v is not None else None for v in row[24]]
    return {
        "trade_date": str(row[1] or trade_date),
        "prev_day_close": float(row[2]) if row[2] is not None else None,
        "tc": tc,
        "bc": bc,
        "pivot": float(row[5] or 0.0),
        "r1": float(row[6] or 0.0),
        "s1": float(row[7] or 0.0),
        "r2": float(row[8] or 0.0),
        "s2": float(row[9] or 0.0),
        "atr": atr,
        "cpr_width_pct": float(row[11] or 0.0),
        "cpr_threshold": float(row[12] or 0.0),
        "or_high_5": or_high_5,
        "or_low_5": or_low_5,
        "open_915": open_915_val,
        "or_close_5": or_close_5,
        "open_side": str(row[17] or ""),
        "open_to_cpr_atr": float(row[18]) if row[18] is not None else None,
        "gap_abs_pct": float(row[19]) if row[19] is not None else None,
        "or_atr_5": float(row[20]) if row[20] is not None else None,
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": bool(row[22]),
        "cpr_shift": str(row[23] or "OVERLAP"),
        "rvol_baseline": rvol_baseline,
        "or_proxy": or_proxy,
        "setup_source": setup_source,
    }


def refresh_pending_setup_rows_for_bar(
    *,
    runtime_state: PaperRuntimeState,
    symbols: list[str],
    trade_date: str,
    bar_candles: list[Any] | None,
    or_minutes: int,
    allow_live_fallback: bool,
) -> dict[str, int]:
    """Batch-refresh unresolved setup rows once per bar cycle."""
    if not symbols:
        return {"resolved": 0, "pending": 0, "missing": 0, "updated": 0}

    bar_end = bar_candles[0].bar_end if bar_candles else None
    current_rows: dict[str, dict[str, Any]] = {}
    for candle in bar_candles or []:
        current_rows[str(candle.symbol)] = _bar_candle_payload(candle)

    pending_symbols: list[str] = []
    for symbol in dict.fromkeys(symbols):
        state = runtime_state.symbols.get(symbol)
        if state is None:
            state = runtime_state.for_symbol(symbol)
        if bar_end is not None and state.setup_refresh_bar_end == bar_end:
            continue
        if runtime_setup_status(runtime_state, symbol) == "pending":
            pending_symbols.append(symbol)

    if not pending_symbols:
        return {"resolved": 0, "pending": 0, "missing": 0, "updated": 0}

    db = get_live_market_db()
    placeholders = ", ".join(["?"] * len(pending_symbols))
    query = f"""
        SELECT
            m.symbol,
            m.trade_date::VARCHAR,
            m.prev_close,
            m.tc,
            m.bc,
            m."pivot",
            m.r1,
            m.s1,
            m.r2,
            m.s2,
            m.atr,
            m.cpr_width_pct,
            m.cpr_threshold_pct,
            m.or_high_5,
            m.or_low_5,
            m.open_915,
            m.or_close_5,
            s.open_side,
            s.open_to_cpr_atr,
            s.gap_abs_pct,
            s.or_atr_5,
            s.direction_5,
            m.is_narrowing,
            m.cpr_shift,
            p.rvol_baseline_arr
        FROM market_day_state m
        LEFT JOIN strategy_day_state s
          ON s.symbol = m.symbol
         AND s.trade_date = m.trade_date
        LEFT JOIN intraday_day_pack p
          ON p.symbol = m.symbol
         AND p.trade_date = m.trade_date
        WHERE m.trade_date = ?::DATE
          AND m.symbol IN ({placeholders})
    """
    with _MARKET_DB_READ_LOCK:
        rows = db.con.execute(query, [trade_date, *pending_symbols]).fetchall()
    batch_rows = {str(row[0]): row for row in rows}

    resolved = 0
    pending = 0
    missing = 0
    updated = 0
    for symbol in pending_symbols:
        state = runtime_state.for_symbol(symbol)
        state.setup_refresh_bar_end = bar_end
        row = batch_rows.get(symbol)
        live_candles = list(state.candles)
        if current_rows.get(symbol) is not None:
            live_candles = [*live_candles, current_rows[symbol]]
        if row is None:
            missing += 1
            if allow_live_fallback:
                fallback_row = _load_live_setup_row(
                    symbol,
                    trade_date,
                    live_candles,
                    or_minutes=or_minutes,
                    bar_end_offset=runtime_state.bar_end_offset,
                )
                if fallback_row is not None:
                    if not runtime_state.allow_or_proxy_setup and setup_row_uses_or_proxy(
                        fallback_row
                    ):
                        pending += 1
                        continue
                    state.setup_row = fallback_row
                    updated += 1
                    if bool(fallback_row.get("direction_pending")):
                        pending += 1
                    else:
                        resolved += 1
            continue
        setup_row = _hydrate_setup_row_from_market_row(
            trade_date=trade_date,
            row=row,
            live_candles=live_candles,
            or_minutes=or_minutes,
            bar_end_offset=runtime_state.bar_end_offset,
        )
        if setup_row is None:
            missing += 1
            continue
        if not runtime_state.allow_or_proxy_setup and setup_row_uses_or_proxy(setup_row):
            pending += 1
            continue
        state.setup_row = setup_row
        updated += 1
        if bool(setup_row.get("direction_pending")):
            pending += 1
        else:
            resolved += 1
    return {"resolved": resolved, "pending": pending, "missing": missing, "updated": updated}


def runtime_setup_status(runtime_state: PaperRuntimeState, symbol: str) -> str:
    state = runtime_state.symbols.get(symbol)
    if state is None or state.setup_row is None:
        return "pending"
    if bool(state.setup_row.get("direction_pending")):
        return "pending"
    direction = str(state.setup_row.get("direction") or "").upper()
    return "candidate" if direction in {"LONG", "SHORT"} else "pending"


__all__ = [
    "_build_intraday_summary",
    "_live_setup_status",
    "load_setup_row",
    "refresh_pending_setup_rows_for_bar",
    "runtime_setup_status",
    "setup_row_uses_or_proxy",
]
