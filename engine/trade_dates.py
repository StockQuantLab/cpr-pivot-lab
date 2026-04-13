"""Trade-date resolution from DuckDB tables.

Provides a shared utility for resolving trade dates from the market database,
used by paper trading scripts (daily-prepare, daily-replay, daily-live) and
other non-WF code paths.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from engine.constants import parse_iso_date

logger = logging.getLogger(__name__)


def iter_session_calendar_trade_dates(start_date: str, end_date: str) -> list[str]:
    """Resolve trade dates between start_date and end_date from DuckDB tables.

    Tries multiple tables in order (v_5min, v_daily, market_day_state, etc.)
    and falls back to weekday-only calendar enumeration if no DB is available.
    """
    from db.duckdb import get_dashboard_db, get_db

    start = date.fromisoformat(parse_iso_date(start_date))
    end = date.fromisoformat(parse_iso_date(end_date))
    if start > end:
        raise ValueError("start_date must be <= end_date")

    def _query_trade_dates(db) -> list[str]:
        for table, column in (
            ("v_5min", "date"),
            ("v_daily", "date"),
            ("market_day_state", "trade_date"),
            ("strategy_day_state", "trade_date"),
            ("intraday_day_pack", "trade_date"),
        ):
            try:
                rows = db.con.execute(
                    f"""
                    SELECT DISTINCT {column}::VARCHAR
                    FROM {table}
                    WHERE {column} BETWEEN ?::DATE AND ?::DATE
                    ORDER BY {column}
                    """,
                    [start.isoformat(), end.isoformat()],
                ).fetchall()
            except Exception as exc:
                logger.debug("Failed to resolve trade dates from %s: %s", table, exc)
                continue
            if rows:
                return [str(row[0]) for row in rows if row and row[0] is not None]
        return []

    for accessor in (get_db, get_dashboard_db):
        try:
            resolved_trade_dates = _query_trade_dates(accessor())
        except Exception as exc:
            logger.debug("Failed to open trade-date database via %s: %s", accessor.__name__, exc)
            continue
        if resolved_trade_dates:
            return resolved_trade_dates

    fallback_trade_dates: list[str] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            fallback_trade_dates.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return fallback_trade_dates
