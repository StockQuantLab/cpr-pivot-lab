from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from db.duckdb import get_dashboard_db
from db.paper_db import get_dashboard_paper_db
from engine.cpr_atr_utils import calculate_gap_pct, normalize_cpr_bounds, resolve_cpr_direction
from engine.live_market_data import IST

_PACK_SOURCE_INTRADAY = "intraday_day_pack"
_PACK_SOURCE_FEED_AUDIT = "paper_feed_audit"


def normalize_pack_source(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"", "intraday", "intraday_day_pack", "pack"}:
        return _PACK_SOURCE_INTRADAY
    if normalized in {"paper_feed_audit", "feed_audit", "audit"}:
        return _PACK_SOURCE_FEED_AUDIT
    raise ValueError(f"Unsupported pack source: {value!r}")


def is_feed_audit_pack_source(value: str | None) -> bool:
    return normalize_pack_source(value) == _PACK_SOURCE_FEED_AUDIT


def _minute_to_time_str(minute_of_day: int | float | str) -> str:
    total = int(minute_of_day)
    return f"{total // 60:02d}:{total % 60:02d}"


def _time_str_to_minute(time_str: str) -> int:
    hour, minute = str(time_str).split(":", 1)
    return int(hour) * 60 + int(minute)


def _load_rvol_lookup(
    *,
    symbol_dates: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, float | None]]:
    if not symbol_dates:
        return {}
    market_db = get_dashboard_db()
    if not getattr(market_db, "_table_has_column", lambda *_: False)(
        "intraday_day_pack", "rvol_baseline_arr"
    ):
        return {}
    has_minute_arr = getattr(market_db, "_table_has_column", lambda *_: False)(
        "intraday_day_pack", "minute_arr"
    )
    time_select = (
        "p.minute_arr AS pack_time_arr" if has_minute_arr else "p.time_arr AS pack_time_arr"
    )
    query = f"""
        SELECT
            p.symbol,
            p.trade_date::VARCHAR AS trade_date,
            {time_select},
            p.rvol_baseline_arr
        FROM intraday_day_pack p
        WHERE list_contains($symbols, p.symbol)
          AND p.trade_date >= $start_date::DATE
          AND p.trade_date <= $end_date::DATE
        ORDER BY p.symbol, p.trade_date
    """
    symbols = sorted({symbol for symbol, _ in symbol_dates})
    dates = [trade_date for _, trade_date in symbol_dates]
    rows = market_db.con.execute(
        query,
        {
            "symbols": symbols,
            "start_date": min(dates),
            "end_date": max(dates),
        },
    ).fetchall()
    requested = set(symbol_dates)
    lookup: dict[tuple[str, str], dict[str, float | None]] = {}
    for symbol, trade_date, raw_times, baselines in rows:
        key = (str(symbol), str(trade_date)[:10])
        if key not in requested or not raw_times or not baselines:
            continue
        if has_minute_arr:
            times = [_minute_to_time_str(t) for t in raw_times]
        else:
            times = [str(t) for t in raw_times]
        lookup[key] = {
            time_str: (float(value) if value is not None else None)
            for time_str, value in zip(times, baselines, strict=False)
        }
    return lookup


def load_feed_audit_day_pack_records(
    *,
    session_id: str,
    symbols: Sequence[str],
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    session = str(session_id or "").strip()
    if not session:
        raise ValueError("paper_feed_audit source requires pack_source_session_id")
    symbol_list = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not symbol_list:
        return []

    paper_db = get_dashboard_paper_db()
    where = ["session_id = $session_id", "list_contains($symbols, symbol)"]
    params: dict[str, Any] = {"session_id": session, "symbols": symbol_list}
    if start_date:
        where.append("trade_date >= $start_date")
        params["start_date"] = str(start_date)
    if end_date:
        where.append("trade_date <= $end_date")
        params["end_date"] = str(end_date)

    rows = paper_db.con.execute(
        f"""
        SELECT
            symbol,
            trade_date,
            bar_start,
            open,
            high,
            low,
            close,
            volume
        FROM paper_feed_audit
        WHERE {" AND ".join(where)}
        ORDER BY symbol, trade_date, bar_start
        """,
        params,
    ).fetchall()
    if not rows:
        return []

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for symbol, trade_date, bar_start, open_, high, low, close, volume in rows:
        key = (str(symbol), str(trade_date)[:10])
        record = grouped.setdefault(
            key,
            {
                "symbol": key[0],
                "trade_date": key[1],
                "time_str": [],
                "opens": [],
                "highs": [],
                "lows": [],
                "closes": [],
                "volumes": [],
            },
        )
        bar_start_ist = bar_start.astimezone(IST) if hasattr(bar_start, "astimezone") else bar_start
        record["time_str"].append(bar_start_ist.strftime("%H:%M"))
        record["opens"].append(float(open_))
        record["highs"].append(float(high))
        record["lows"].append(float(low))
        record["closes"].append(float(close))
        record["volumes"].append(float(volume or 0.0))

    rvol_lookup = _load_rvol_lookup(symbol_dates=list(grouped))
    records: list[dict[str, Any]] = []
    for key, record in grouped.items():
        baseline_by_time = rvol_lookup.get(key, {})
        record["rvol_baseline"] = [baseline_by_time.get(t) for t in record["time_str"]]
        records.append(record)
    return records


def apply_opening_range_from_day_pack(
    setup_row: Mapping[str, Any],
    *,
    time_str: Sequence[str],
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[float] | None = None,
    or_minutes: int,
    source_label: str,
) -> dict[str, Any]:
    if not time_str:
        return dict(setup_row)

    start_minute = 9 * 60 + 15
    end_minute = start_minute + max(1, int(or_minutes or 5))
    indices = [
        idx
        for idx, value in enumerate(time_str)
        if start_minute <= _time_str_to_minute(value) < end_minute
    ]
    if not indices:
        return dict(setup_row)

    first_idx = indices[0]
    last_idx = indices[-1]
    open_915 = float(opens[first_idx])
    or_high = max(float(highs[idx]) for idx in indices)
    or_low = min(float(lows[idx]) for idx in indices)
    or_close = float(closes[last_idx])
    atr = float(setup_row.get("atr") or 0.0)
    tc = float(setup_row.get("tc") or 0.0)
    bc = float(setup_row.get("bc") or 0.0)
    cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
    if open_915 < cpr_lower:
        open_side = "BELOW"
    elif open_915 > cpr_upper:
        open_side = "ABOVE"
    else:
        open_side = "INSIDE"
    prev_close = setup_row.get("prev_day_close")
    direction = resolve_cpr_direction(or_close, tc, bc, fallback="NONE")
    patched = dict(setup_row)
    patched["open_915"] = open_915
    patched["high_915"] = or_high
    patched["low_915"] = or_low
    patched["close_915"] = or_close
    patched["or_high_5"] = or_high
    patched["or_low_5"] = or_low
    patched["or_close_5"] = or_close
    patched["direction"] = direction
    patched["direction_pending"] = direction not in {"LONG", "SHORT"}
    patched["open_side"] = open_side
    patched["open_to_cpr_atr"] = (
        abs(open_915 - (cpr_lower if open_side == "BELOW" else cpr_upper)) / atr
        if atr > 0 and open_side in {"BELOW", "ABOVE"}
        else 0.0
    )
    patched["gap_abs_pct"] = (
        abs(calculate_gap_pct(open_915, prev_close)) if prev_close is not None else None
    )
    if volumes:
        patched["volume_915"] = float(sum(float(volumes[idx]) for idx in indices))
    patched["setup_source"] = source_label
    return patched
