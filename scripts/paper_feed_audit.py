"""Live feed audit helpers for paper trading.

The audit is intentionally compact:
- one row per session/symbol/closed bar
- captures the OHLCV values seen by the live candle engine
- compares against the EOD-built intraday_day_pack after the session completes
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from db.duckdb import get_dashboard_db
from db.paper_db import FeedAudit, PaperDB, get_dashboard_paper_db, get_paper_db
from engine.live_market_data import IST, ClosedCandle

_PRICE_TOLERANCE = 0.01
_VOLUME_TOLERANCE = 0.5


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


def _audit_row_from_candle(
    *,
    session_id: str,
    trade_date: str,
    feed_source: str,
    transport: str,
    candle: ClosedCandle,
) -> dict[str, Any]:
    bar_start = getattr(candle, "bar_start", None)
    if bar_start is None:
        bar_start = candle.bar_end - timedelta(minutes=5)
    first_snapshot_ts = getattr(candle, "first_snapshot_ts", None)
    last_snapshot_ts = getattr(candle, "last_snapshot_ts", None)
    return {
        "session_id": session_id,
        "trade_date": trade_date,
        "feed_source": feed_source,
        "transport": transport,
        "symbol": candle.symbol,
        "bar_start": bar_start,
        "bar_end": candle.bar_end,
        "open": float(candle.open),
        "high": float(candle.high),
        "low": float(candle.low),
        "close": float(candle.close),
        "volume": float(candle.volume),
        "first_snapshot_ts": first_snapshot_ts,
        "last_snapshot_ts": last_snapshot_ts,
    }


def record_closed_candles(
    *,
    session_id: str,
    trade_date: str,
    feed_source: str,
    transport: str,
    bar_candles: list[ClosedCandle],
    paper_db: PaperDB | None = None,
) -> int:
    if not bar_candles:
        return 0
    rows = [
        _audit_row_from_candle(
            session_id=session_id,
            trade_date=trade_date,
            feed_source=feed_source,
            transport=transport,
            candle=candle,
        )
        for candle in bar_candles
    ]
    db = paper_db or get_paper_db()
    return db.upsert_feed_audit_rows(rows)


def _load_intraday_pack_lookup(
    *,
    trade_date: str,
    symbols: list[str],
    market_db: Any | None = None,
) -> dict[str, dict[str, dict[str, float]]]:
    if not symbols:
        return {}
    db = market_db or get_dashboard_db()
    pack_time_col = (
        "p.minute_arr AS pack_time_arr"
        if _has_column(db, "intraday_day_pack", "minute_arr")
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
            p.volume_arr
        FROM intraday_day_pack p
        WHERE list_contains($symbols, p.symbol)
          AND p.trade_date = $trade_date::DATE
        ORDER BY p.symbol
    """
    pack_df = db.con.execute(
        query,
        {"symbols": list(symbols), "trade_date": trade_date},
    ).pl()
    if pack_df.is_empty():
        return {}

    lookup: dict[str, dict[str, dict[str, float]]] = {}
    for idx in range(pack_df.height):
        symbol = str(pack_df["symbol"].to_list()[idx])
        raw_times = pack_df["pack_time_arr"].to_list()[idx]
        if not raw_times:
            continue
        if _has_column(db, "intraday_day_pack", "minute_arr"):
            times = [_minute_to_time_str(t) for t in raw_times]
        else:
            times = [str(t) for t in raw_times]

        opens = pack_df["open_arr"].to_list()[idx]
        highs = pack_df["high_arr"].to_list()[idx]
        lows = pack_df["low_arr"].to_list()[idx]
        closes = pack_df["close_arr"].to_list()[idx]
        volumes = pack_df["volume_arr"].to_list()[idx]
        if not (len(times) == len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)):
            continue

        symbol_lookup: dict[str, dict[str, float]] = {}
        for i, time_str in enumerate(times):
            symbol_lookup[time_str] = {
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": float(volumes[i]),
            }
        lookup[symbol] = symbol_lookup
    return lookup


def _audit_time_key(*, feed_source: str, bar_start: Any, bar_end: Any) -> str:
    """Return the pack lookup key for an audited bar.

    Kite live rows are emitted at bar close, but the materialized day-pack contract
    stores the candle bucket start time. Replay/local rows already use the pack's
    stored bar timestamp as their `bar_end`, so they keep the existing key.
    """

    source = str(feed_source or "").strip().lower()
    if source == "kite":
        ts = bar_start
    else:
        ts = bar_end
    if hasattr(ts, "astimezone"):
        ts = ts.astimezone(IST)
    return ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)


def _session_ids_for_trade_date(
    *,
    trade_date: str,
    feed_source: str,
    paper_db: PaperDB,
) -> list[str]:
    where = ["trade_date = ?"]
    params: list[Any] = [trade_date]
    if feed_source != "all":
        where.append("feed_source = ?")
        params.append(feed_source)
    rows = paper_db.con.execute(
        f"""
        SELECT DISTINCT session_id
        FROM paper_feed_audit
        WHERE {" AND ".join(where)}
        ORDER BY session_id
        """,
        params,
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _compare_row(
    *,
    audit_row: FeedAudit,
    pack_row: dict[str, float] | None,
) -> dict[str, Any] | None:
    if pack_row is None:
        return {
            "symbol": audit_row.symbol,
            "bar_end": audit_row.bar_end,
            "status": "MISSING_PACK",
            "expected": None,
            "actual": {
                "open": audit_row.open,
                "high": audit_row.high,
                "low": audit_row.low,
                "close": audit_row.close,
                "volume": audit_row.volume,
            },
        }

    mismatches: dict[str, dict[str, float]] = {}
    actual = {
        "open": audit_row.open,
        "high": audit_row.high,
        "low": audit_row.low,
        "close": audit_row.close,
        "volume": audit_row.volume,
    }
    for field, tolerance in (
        ("open", _PRICE_TOLERANCE),
        ("high", _PRICE_TOLERANCE),
        ("low", _PRICE_TOLERANCE),
        ("close", _PRICE_TOLERANCE),
        ("volume", _VOLUME_TOLERANCE),
    ):
        diff = abs(actual[field] - float(pack_row[field]))
        if diff > tolerance:
            mismatches[field] = {
                "actual": actual[field],
                "expected": float(pack_row[field]),
                "delta": diff,
            }

    if not mismatches:
        return None

    return {
        "symbol": audit_row.symbol,
        "bar_end": audit_row.bar_end,
        "status": "VALUE_MISMATCH",
        "expected": pack_row,
        "actual": actual,
        "mismatches": mismatches,
    }


def compare_feed_audit(
    *,
    trade_date: str,
    feed_source: str = "kite",
    session_id: str | None = None,
    paper_db: PaperDB | None = None,
    market_db: Any | None = None,
) -> dict[str, Any]:
    db = paper_db or get_dashboard_paper_db()
    source_filter = feed_source.strip().lower()
    if source_filter not in {"kite", "local", "replay", "all"}:
        raise ValueError("feed_source must be one of kite, local, replay, or all")

    session_ids = (
        [session_id]
        if session_id
        else _session_ids_for_trade_date(
            trade_date=trade_date,
            feed_source=source_filter,
            paper_db=db,
        )
    )
    if session_id and source_filter != "all":
        session_ids = [sid for sid in session_ids if sid == session_id]

    summaries: list[dict[str, Any]] = []
    total_rows = 0
    matched_rows = 0
    missing_pack_rows = 0
    mismatched_rows = 0
    mismatch_samples: list[dict[str, Any]] = []
    price_exact_rows = 0
    volume_exact_rows = 0
    field_mismatch_counts = dict.fromkeys(("open", "high", "low", "close", "volume"), 0)

    for sid in session_ids:
        audit_rows = db.get_feed_audit_rows(
            trade_date=trade_date,
            session_id=sid,
            feed_source=None if source_filter == "all" else source_filter,
        )
        if not audit_rows:
            summaries.append(
                {
                    "session_id": sid,
                    "rows": 0,
                    "matched_rows": 0,
                    "mismatched_rows": 0,
                    "missing_pack_rows": 0,
                    "status": "NO_ROWS",
                }
            )
            continue

        symbols = sorted({row.symbol for row in audit_rows})
        pack_lookup = _load_intraday_pack_lookup(
            trade_date=trade_date,
            symbols=symbols,
            market_db=market_db,
        )

        session_total = 0
        session_matched = 0
        session_missing = 0
        session_mismatched = 0
        session_price_exact = 0
        session_volume_exact = 0
        session_field_mismatch_counts = dict.fromkeys(
            ("open", "high", "low", "close", "volume"), 0
        )
        session_samples: list[dict[str, Any]] = []

        for row in audit_rows:
            session_total += 1
            total_rows += 1
            bar_time = _audit_time_key(
                feed_source=str(getattr(row, "feed_source", source_filter) or source_filter),
                bar_start=getattr(row, "bar_start", None),
                bar_end=getattr(row, "bar_end", None),
            )
            pack_row = pack_lookup.get(row.symbol, {}).get(bar_time)
            comparison = _compare_row(audit_row=row, pack_row=pack_row)
            if comparison is None:
                session_matched += 1
                matched_rows += 1
                session_price_exact += 1
                session_volume_exact += 1
                price_exact_rows += 1
                volume_exact_rows += 1
                continue
            if comparison["status"] == "MISSING_PACK":
                session_missing += 1
                missing_pack_rows += 1
            else:
                session_mismatched += 1
                mismatched_rows += 1
                mismatches = comparison.get("mismatches") or {}
                price_fields = {"open", "high", "low", "close"}
                price_ok = all(field not in mismatches for field in price_fields)
                volume_ok = "volume" not in mismatches
                if price_ok:
                    session_price_exact += 1
                    price_exact_rows += 1
                if volume_ok:
                    session_volume_exact += 1
                    volume_exact_rows += 1
                for field in mismatches:
                    if field in session_field_mismatch_counts:
                        session_field_mismatch_counts[field] += 1
                        field_mismatch_counts[field] += 1
            if len(session_samples) < 5:
                session_samples.append(comparison)
            if len(mismatch_samples) < 10:
                mismatch_samples.append(comparison)

        summaries.append(
            {
                "session_id": sid,
                "rows": session_total,
                "matched_rows": session_matched,
                "mismatched_rows": session_mismatched,
                "missing_pack_rows": session_missing,
                "price_exact_rows": session_price_exact,
                "volume_exact_rows": session_volume_exact,
                "field_mismatch_counts": session_field_mismatch_counts,
                "status": "PASS" if session_mismatched == 0 and session_missing == 0 else "FAIL",
                "samples": session_samples,
            }
        )

    overall_ok = mismatched_rows == 0 and missing_pack_rows == 0 and total_rows > 0
    return {
        "trade_date": trade_date,
        "feed_source": source_filter,
        "session_id": session_id,
        "session_count": len(session_ids),
        "rows": total_rows,
        "matched_rows": matched_rows,
        "mismatched_rows": mismatched_rows,
        "missing_pack_rows": missing_pack_rows,
        "price_exact_rows": price_exact_rows,
        "volume_exact_rows": volume_exact_rows,
        "field_mismatch_counts": field_mismatch_counts,
        "ok": overall_ok,
        "sessions": summaries,
        "samples": mismatch_samples,
    }


__all__ = ["compare_feed_audit", "record_closed_candles"]
