"""Shared trading-signal generation helpers.

This module contains the reusable computation layer for alerting and
paper-trading orchestration. It intentionally stops before transport,
formatting, or email delivery.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from db.duckdb import get_db
from engine.constants import normalize_symbol


@dataclass
class AlertSignal:
    """A trading signal to alert about."""

    symbol: str
    trade_date: date
    condition: str
    details: str
    cpr_width: float | None = None
    pivot: float | None = None
    tc: float | None = None
    bc: float | None = None


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    for symbol in symbols:
        cleaned = normalize_symbol(str(symbol))
        if cleaned and cleaned not in normalized:
            normalized.append(cleaned)
    return normalized


def _query_signals(query: str, params: list[object] | None = None) -> list[AlertSignal]:
    df = get_db().con.execute(query, params or []).pl()
    if df.is_empty():
        return []
    return [
        AlertSignal(
            symbol=row["symbol"],
            trade_date=row["trade_date"],
            condition=row["condition"],
            details=row["details"],
            cpr_width=row.get("cpr_width"),
            pivot=row.get("pivot"),
            tc=row.get("tc"),
            bc=row.get("bc"),
        )
        for row in df.iter_rows(named=True)
    ]


def check_narrow_cpr(symbols: list[str], trade_date: date | None = None) -> list[AlertSignal]:
    """Check for narrow CPR days (bottom percentile)."""
    if trade_date is None:
        trade_date = date.today()

    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return []
    symbols_clause = ", ".join("?" for _ in normalized_symbols)
    query = f"""
    SELECT
        symbol,
        trade_date,
        'narrow-cpr' AS condition,
        'CPR width ' || printf('%.2f', cpr_width_pct) || '% < threshold ' ||
        printf('%.2f', cpr_threshold_pct) || '%' ||
        ' (' || CASE WHEN is_narrowing THEN 'Narrowing' ELSE 'stable' END || ')' AS details,
        cpr_width_pct AS cpr_width,
        pivot,
        tc,
        bc
    FROM cpr_daily
    WHERE symbol IN ({symbols_clause})
      AND trade_date = ?
      AND cpr_width_pct < cpr_threshold_pct
    ORDER BY cpr_width_pct ASC
    """
    return _query_signals(query, [*normalized_symbols, trade_date])


def check_virgin_cpr(symbols: list[str], trade_date: date | None = None) -> list[AlertSignal]:
    """Check for virgin CPR (untouched zones)."""
    if trade_date is None:
        trade_date = date.today()

    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return []
    symbols_clause = ", ".join("?" for _ in normalized_symbols)
    query = f"""
    SELECT
        v.symbol AS symbol,
        v.trade_date AS trade_date,
        'virgin-cpr' AS condition,
        'Virgin CPR zone — ' ||
        CASE WHEN o.o0915 > c.tc THEN 'BULLISH' ELSE 'BEARISH' END ||
        ' (OR: ' || printf('%.2f', o.h0940) || ' / ' || printf('%.2f', o.l0940) || ')' AS details,
        c.cpr_width_pct AS cpr_width,
        c.pivot AS pivot,
        c.tc AS tc,
        c.bc AS bc
    FROM virgin_cpr_flags v
    JOIN cpr_daily c ON v.symbol = c.symbol AND v.trade_date = c.trade_date
    LEFT JOIN or_daily o ON v.symbol = o.symbol AND v.trade_date = o.trade_date
    WHERE v.symbol IN ({symbols_clause})
      AND v.trade_date = ?
      AND v.is_virgin_cpr = true
    ORDER BY v.symbol
    """
    return _query_signals(query, [*normalized_symbols, trade_date])


def check_orb_fail(symbols: list[str], trade_date: date | None = None) -> list[AlertSignal]:
    """Check for ORB failure (86% rule — FBR entry signal)."""
    if trade_date is None:
        trade_date = date.today() - timedelta(days=1)

    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return []
    symbols_clause = ", ".join("?" for _ in normalized_symbols)
    query = f"""
    SELECT
        o.symbol AS symbol,
        o.trade_date AS trade_date,
        'orb-fail' AS condition,
        'ORB failed (' ||
        CASE
            WHEN o.c0940 > o.h0940 THEN 'BULLISH_OR'
            WHEN o.c0940 < o.l0940 THEN 'BEARISH_OR'
            ELSE 'NEUTRAL'
        END || ') → FBR ' ||
        CASE WHEN o.c0940 < o.l0940 THEN 'LONG' ELSE 'SHORT' END ||
        ' entry signal (OR: ' || printf('%.2f', o.o0915) ||
        ', Close: ' || printf('%.2f', o.c0940) || ')' AS details,
        c.cpr_width_pct AS cpr_width,
        c.pivot AS pivot,
        c.tc AS tc,
        c.bc AS bc
    FROM or_daily o
    JOIN cpr_daily c ON o.symbol = c.symbol AND o.trade_date = c.trade_date
    WHERE o.symbol IN ({symbols_clause})
      AND o.trade_date = ?
      AND (
          (o.c0940 > o.h0940 AND o.o0915 < o.l0940)
          OR (o.c0940 < o.l0940 AND o.o0915 > o.h0940)
      )
    ORDER BY o.symbol
    """
    return _query_signals(query, [*normalized_symbols, trade_date])


def check_gap_signals(
    symbols: list[str],
    trade_date: date | None = None,
    gap_threshold: float = 1.5,
    direction: str = "both",
) -> list[AlertSignal]:
    """Check for gap up/down signals."""
    if trade_date is None:
        trade_date = date.today()

    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return []
    symbols_clause = ", ".join("?" for _ in normalized_symbols)

    gap_filter = ""
    if direction == "up":
        gap_filter = "AND s.gap_pct > 0"
    elif direction == "down":
        gap_filter = "AND s.gap_pct < 0"

    query = f"""
    SELECT
        s.symbol AS symbol,
        s.trade_date AS trade_date,
        CASE WHEN s.gap_pct > 0 THEN 'gap-up' ELSE 'gap-down' END AS condition,
        CASE WHEN s.gap_pct > 0 THEN 'GAP UP' ELSE 'GAP DOWN' END || ' ' ||
        printf('%.2f', ABS(s.gap_pct)) || '% (Prev: ' || printf('%.2f', s.prev_close) ||
        ', Today: ' || printf('%.2f', o.o0915) || ')' AS details,
        c.cpr_width_pct AS cpr_width,
        c.pivot AS pivot,
        c.tc AS tc,
        c.bc AS bc
    FROM market_day_state s
    JOIN cpr_daily c ON s.symbol = c.symbol AND s.trade_date = c.trade_date
    LEFT JOIN or_daily o ON s.symbol = o.symbol AND s.trade_date = o.trade_date
    WHERE s.symbol IN ({symbols_clause})
      AND s.trade_date = ?
      AND ABS(s.gap_pct) >= ?
      {gap_filter}
    ORDER BY s.gap_pct DESC
    """
    return _query_signals(query, [*normalized_symbols, trade_date, gap_threshold])


__all__ = [
    "AlertSignal",
    "check_gap_signals",
    "check_narrow_cpr",
    "check_orb_fail",
    "check_virgin_cpr",
]
