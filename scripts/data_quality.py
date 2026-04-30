"""Data quality registry CLI for backtest symbol universe.

Fast mode (default): checks for missing 5-min parquet files only.
Full mode (--full):  comprehensive scan — OHLC validity, timestamp range,
                     duplicate candles, date gaps, extreme moves, zero volume days.

Usage:
    pivot-data-quality --refresh              # fast refresh (parquet presence only)
    pivot-data-quality --refresh --full       # comprehensive scan (takes 1-5 min)
    pivot-data-quality --date 2026-03-27      # trade-date readiness gate
    pivot-data-quality                        # print active issues
    pivot-data-quality --issue-code OHLC_VIOLATION
    pivot-data-quality --show-inactive        # include resolved issues
    pivot-data-quality --window-start 2025-01-01 --window-end 2026-03-27
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from db.duckdb import get_db
from engine.cli_setup import configure_windows_stdio
from engine.command_lock import acquire_command_lock
from scripts.paper_prepare import CANONICAL_FULL_UNIVERSE_NAME, resolve_trade_date

_IST = timezone(timedelta(hours=5, minutes=30))


def _is_pre_market(trade_date: str) -> bool:
    """True if trade_date is today and current IST time is before 09:15."""
    now_ist = datetime.now(_IST)
    today = now_ist.date().isoformat()
    return trade_date == today and (now_ist.hour, now_ist.minute) < (9, 15)


_SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
_SPARSE_MISSING_MAX_RATIO = 0.05

# Human-readable labels for each issue code
_ISSUE_LABELS: dict[str, str] = {
    "MISSING_5MIN_PARQUET": "Missing 5-min parquet",
    "OHLC_VIOLATION": "OHLC constraint violated",
    "NULL_PRICE": "Null OHLC prices",
    "ZERO_PRICE": "Zero open/close/high",
    "TIMESTAMP_INVALID": "Candle time outside NSE session",
    "DUPLICATE_CANDLE": "Duplicate (date, time) candles",
    "DATE_GAP": "Trading date gap >7 days",
    "EXTREME_CANDLE": "Candle range >50% of open",
    "ZERO_VOLUME_DAY": "Full trading day with zero volume",
}


def _print_issues(rows: list[dict[str, object]], limit: int) -> None:
    """Render issue rows in a compact table sorted by severity → code → symbol."""
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            _SEVERITY_ORDER.get(str(r.get("severity", "WARNING")), 9),
            str(r.get("issue_code", "")),
            str(r.get("symbol", "")),
        ),
    )
    print(f"{'SEV':<9} {'Symbol':<16} {'Issue':<26} Details")
    print("-" * 100)
    shown = sorted_rows if limit <= 0 else sorted_rows[:limit]
    for row in shown:
        sev = str(row.get("severity", "WARNING"))[:8]
        symbol = str(row.get("symbol", ""))
        issue = str(row.get("issue_code", ""))
        details = str(row.get("details", ""))
        print(f"{sev:<9} {symbol:<16} {issue:<26} {details}")


def _print_summary(summary: dict[str, int]) -> None:
    """Print scan summary grouped by issue code."""
    total_active = summary.pop("total_active_issues", 0)
    print("\nScan results:")
    print(f"  {'Issue code':<26} Affected symbols")
    print("  " + "-" * 45)
    for code, cnt in sorted(summary.items()):
        label = _ISSUE_LABELS.get(code, code)
        marker = " [!]" if cnt > 0 else ""
        print(f"  {code:<26} {cnt}{marker}  ({label})")
    print(f"\n  Total active issues in registry: {total_active}")


def _preview_symbols(symbols: list[str], limit: int = 10) -> str:
    """Return a short comma-separated preview of symbol lists."""
    if not symbols:
        return "None"
    preview = ", ".join(symbols[:limit])
    if len(symbols) > limit:
        preview += f" ... (+{len(symbols) - limit} more)"
    return preview


def _default_full_universe_name(trade_date: str) -> str:
    return f"full_{trade_date.replace('-', '_')}"


def _load_default_universe_symbols(db: Any, trade_date: str) -> tuple[str, list[str]]:
    """Load dated paper universe, falling back to the stable canonical universe."""
    if not hasattr(db, "get_universe_symbols"):
        return "none", []
    for universe_name in (_default_full_universe_name(trade_date), CANONICAL_FULL_UNIVERSE_NAME):
        try:
            symbols = db.get_universe_symbols(universe_name)
        except Exception:
            continue
        resolved = sorted({str(symbol).upper() for symbol in symbols if str(symbol or "").strip()})
        if resolved:
            return universe_name, resolved
    return "none", []


def _symbols_missing_for_exact_table(
    db: Any,
    table: str,
    symbols: list[str],
    trade_date: str,
) -> list[str]:
    """Return requested symbols with no exact trade_date row in a runtime table."""
    if not symbols:
        return []
    placeholders = ", ".join("?" for _ in symbols)
    try:
        rows = db.con.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {table}
            WHERE trade_date = ?::DATE
              AND symbol IN ({placeholders})
            """,
            [trade_date, *symbols],
        ).fetchall()
    except Exception:
        return list(symbols)
    present = {str(row[0]).upper() for row in rows if row and row[0]}
    return [symbol for symbol in symbols if symbol not in present]


def _coverage_status(
    *,
    requested_count: int,
    missing_count: int,
) -> str:
    """Classify sparse symbol/day gaps as warnings, not hard readiness failures."""
    if missing_count <= 0:
        return "ok"
    if requested_count <= 0:
        return "blocking"
    missing_ratio = missing_count / requested_count
    return "warning" if missing_ratio <= _SPARSE_MISSING_MAX_RATIO else "blocking"


def _coverage_blocking_tables(
    coverage: dict[str, list[str]],
    *,
    requested_count: int,
    tables: tuple[str, ...],
) -> dict[str, str]:
    return {
        table: _coverage_status(
            requested_count=requested_count,
            missing_count=len(coverage.get(table) or []),
        )
        for table in tables
    }


def _live_prereq_coverage(db: Any, symbols: list[str], trade_date: str) -> dict[str, list[str]]:
    """Return live prerequisites missing for current/future trade date setup."""
    coverage = {
        "v_daily": [],
        "v_5min": [],
        "atr_intraday": [],
        "cpr_thresholds": [],
        "atr_date_mismatch": [],
        "cpr_threshold_date_mismatch": [],
    }
    if not symbols:
        return coverage
    placeholders = ", ".join("?" for _ in symbols)
    params = [*symbols, trade_date]
    daily_rows = db.con.execute(
        f"SELECT symbol, MAX(date)::VARCHAR FROM v_daily WHERE symbol IN ({placeholders}) AND date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()
    five_min_rows = db.con.execute(
        f"SELECT symbol, MAX(date)::VARCHAR FROM v_5min WHERE symbol IN ({placeholders}) AND date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()
    atr_rows = db.con.execute(
        f"SELECT symbol, MAX(trade_date)::VARCHAR FROM atr_intraday WHERE symbol IN ({placeholders}) AND trade_date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()
    threshold_rows = db.con.execute(
        f"SELECT symbol, MAX(trade_date)::VARCHAR FROM cpr_thresholds WHERE symbol IN ({placeholders}) AND trade_date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()
    daily_map = {str(row[0]): str(row[1]) if row[1] is not None else None for row in daily_rows}
    five_min_map = {
        str(row[0]): str(row[1]) if row[1] is not None else None for row in five_min_rows
    }
    atr_map = {str(row[0]): str(row[1]) if row[1] is not None else None for row in atr_rows}
    threshold_map = {
        str(row[0]): str(row[1]) if row[1] is not None else None for row in threshold_rows
    }
    for symbol in symbols:
        prev_daily = daily_map.get(symbol)
        prev_5min = five_min_map.get(symbol)
        prev_atr = atr_map.get(symbol)
        prev_threshold = threshold_map.get(symbol)
        if prev_daily is None:
            coverage["v_daily"].append(symbol)
        if prev_5min is None:
            coverage["v_5min"].append(symbol)
        if prev_atr is None:
            coverage["atr_intraday"].append(symbol)
        if prev_threshold is None:
            coverage["cpr_thresholds"].append(symbol)
        if prev_daily is not None and prev_atr is not None and prev_daily != prev_atr:
            coverage["atr_date_mismatch"].append(symbol)
        if prev_daily is not None and prev_threshold is not None and prev_daily != prev_threshold:
            coverage["cpr_threshold_date_mismatch"].append(symbol)
    return coverage


def _print_window_report(start_date: str, end_date: str) -> None:
    """Print a lightweight DQ report for a bounded date window."""
    db = get_db()
    con = db.con

    print(f"\nActive DQ issues ({start_date} -> {end_date}):")
    rows = con.execute(
        """
        SELECT
            issue_code,
            severity,
            COUNT(*) AS symbols,
            MIN(details) AS sample_detail
        FROM data_quality_issues
        WHERE is_active = TRUE
        GROUP BY issue_code, severity
        ORDER BY
            CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
            issue_code
        """
    ).fetchall()
    if rows:
        for issue_code, severity, symbols, sample_detail in rows:
            sample = str(sample_detail or "")[:90]
            print(f"  {severity:<10} {issue_code:<20} {int(symbols):>5} symbols | {sample}")
    else:
        print("  None")

    checks = [
        (
            "OHLC_VIOLATION",
            """
            SELECT COUNT(*)
            FROM v_5min
            WHERE date BETWEEN ? AND ?
              AND (
                  high < low
                  OR close > high
                  OR close < low
                  OR open > high
                  OR open < low
              )
            """,
        ),
        (
            "ZERO_PRICE",
            """
            SELECT COUNT(*)
            FROM v_5min
            WHERE date BETWEEN ? AND ?
              AND (open = 0 OR close = 0 OR high = 0)
            """,
        ),
        (
            "TIMESTAMP_INVALID",
            """
            SELECT COUNT(*)
            FROM v_5min
            WHERE date BETWEEN ? AND ?
              AND (
                  HOUR(candle_time) < 9
                  OR HOUR(candle_time) > 15
                  OR (HOUR(candle_time) = 9 AND MINUTE(candle_time) < 15)
                  OR (HOUR(candle_time) = 15 AND MINUTE(candle_time) > 30)
              )
            """,
        ),
        (
            "EXTREME_CANDLE",
            """
            SELECT COUNT(*)
            FROM v_5min
            WHERE date BETWEEN ? AND ?
              AND open > 0
              AND (high - low) / open > 0.5
            """,
        ),
    ]

    print(f"\nWindow checks ({start_date} -> {end_date}):")
    for label, sql in checks:
        count = con.execute(sql, [start_date, end_date]).fetchone()[0]
        print(f"  {label:<20} {int(count):>10,}")


def build_trade_date_readiness_report(trade_date: str) -> dict[str, Any]:
    """Build a readiness report for a specific trade date."""
    db = get_db()
    pre_market = _is_pre_market(trade_date)
    candidate_symbols = sorted(db.get_symbols_with_parquet_data([trade_date]))
    symbol_source = "same-day_5min_parquet"
    setup_only_mode = False
    if not candidate_symbols:
        symbol_source, candidate_symbols = _load_default_universe_symbols(db, trade_date)
        setup_only_mode = bool(candidate_symbols)

    # Tables expected to be at trade_date after a correct EOD build (one day ahead of pack).
    # Being at exactly trade_date with pack behind = valid post-EOD state.
    # Being BEYOND trade_date = genuinely unexpected / accidental future build.
    _next_day_setup_tables = {
        "cpr_daily",
        "cpr_thresholds",
        "market_day_state",
        "strategy_day_state",
    }

    freshness_tables = [
        "or_daily",
        "cpr_daily",
        "cpr_thresholds",
        "market_day_state",
        "strategy_day_state",
        "intraday_day_pack",
    ]
    table_max_dates = db.get_table_max_trade_dates(freshness_tables)
    pack_date = table_max_dates.get("intraday_day_pack")
    freshness_rows: list[dict[str, str | None]] = []
    freshness_blocking = False
    for table in freshness_tables:
        max_date = table_max_dates.get(table)
        if max_date is None:
            status = "MISSING"
            freshness_blocking = True
        elif pack_date is None:
            status = "NO PACK DATE"
            freshness_blocking = True
        elif max_date == pack_date:
            status = "OK"
        elif max_date > pack_date:
            if setup_only_mode and table in _next_day_setup_tables:
                # Being at exactly trade_date is expected after EOD (next-day setup built).
                # Being beyond trade_date is genuinely unexpected.
                if max_date == trade_date:
                    status = f"OK next-day ({max_date})"
                else:
                    status = f"UNEXPECTED FUTURE STATE ({max_date} > {trade_date})"
                    freshness_blocking = True
            else:
                status = f"AHEAD of pack ({max_date} > {pack_date})"
        else:
            # State table is BEHIND pack — genuine staleness problem.
            status = f"OUT OF SYNC vs {pack_date}"
            freshness_blocking = True
        freshness_rows.append({"table": table, "max_trade_date": max_date, "status": status})

    freshness_comparisons: list[dict[str, str | None]] = []
    for left, right in [
        ("or_daily", "intraday_day_pack"),
        ("cpr_daily", "intraday_day_pack"),
        ("cpr_thresholds", "intraday_day_pack"),
        ("market_day_state", "intraday_day_pack"),
        ("strategy_day_state", "intraday_day_pack"),
    ]:
        left_date = table_max_dates.get(left)
        right_date = table_max_dates.get(right)
        if left_date is not None and right_date is not None and left_date == right_date:
            status = f"OK ({left_date})"
        elif left_date is not None and right_date is not None and left_date > right_date:
            if setup_only_mode and left in _next_day_setup_tables:
                if left_date == trade_date:
                    status = f"OK next-day ({left_date})"
                else:
                    status = f"UNEXPECTED FUTURE STATE ({left_date} > {trade_date})"
                    freshness_blocking = True
            else:
                status = f"AHEAD ({left_date} > {right_date})"
        else:
            status = f"OUT OF SYNC ({left_date or 'None'} vs {right_date or 'None'})"
            freshness_blocking = True
        freshness_comparisons.append({"left": left, "right": right, "status": status})

    setup_capable_symbols: list[str] = []
    setup_query_failed = False
    if candidate_symbols:
        placeholders = ", ".join("?" for _ in candidate_symbols)
        try:
            rows = db.con.execute(
                f"""
                SELECT DISTINCT symbol
                FROM intraday_day_pack
                WHERE trade_date = ?::DATE
                  AND symbol IN ({placeholders})
                  AND minute_arr[1] = 555
                ORDER BY symbol
                """,
                [trade_date, *candidate_symbols],
            ).fetchall()
            setup_capable_symbols = sorted({str(row[0]) for row in rows if row and row[0]})
        except Exception:
            setup_query_failed = True

    late_starting = [
        symbol for symbol in candidate_symbols if symbol not in set(setup_capable_symbols)
    ]

    if setup_only_mode:
        coverage = _live_prereq_coverage(db, candidate_symbols, trade_date)
    else:
        coverage = db.get_runtime_trade_date_coverage(candidate_symbols, trade_date)
    missing_counts = {table: len(symbols) for table, symbols in coverage.items()}

    blocking_tables = (
        (
            "v_daily",
            "v_5min",
            "atr_intraday",
            "cpr_thresholds",
            "atr_date_mismatch",
            "cpr_threshold_date_mismatch",
        )
        if setup_only_mode
        else ("cpr_daily", "market_day_state", "strategy_day_state", "intraday_day_pack")
    )
    coverage_status = _coverage_blocking_tables(
        coverage,
        requested_count=len(candidate_symbols),
        tables=blocking_tables,
    )
    coverage_blocking = any(status == "blocking" for status in coverage_status.values())

    # Direction coverage: how many strategy_day_state rows for this date have
    # direction_5 resolved to LONG/SHORT vs NONE. Pre-market this is expected
    # to be 100% NONE (resolved at 9:15 from live ticks). On historical dates
    # it should be mostly LONG/SHORT. A missing SHORT bucket on today's date
    # is the specific failure mode from the 2026-04-15 incident.
    direction_counts: dict[str, int] = {"LONG": 0, "SHORT": 0, "NONE": 0, "OTHER": 0}
    try:
        rows = db.con.execute(
            """
            SELECT COALESCE(UPPER(direction_5), 'NONE') AS d, COUNT(*)
            FROM strategy_day_state
            WHERE trade_date = ?::DATE
            GROUP BY 1
            """,
            [trade_date],
        ).fetchall()
        for direction_value, count in rows:
            key = str(direction_value or "NONE").upper()
            if key not in direction_counts:
                key = "OTHER"
            direction_counts[key] += int(count)
    except Exception:
        pass

    # For live/future dates: assert market_day_state and cpr_daily have rows for trade_date.
    # This catches the "EOD did not build next-day CPR rows" failure that causes zero trades.
    next_day_mds_count = 0
    next_day_cpr_count = 0
    next_day_rows_missing = False
    if setup_only_mode:
        try:
            next_day_mds_count = int(
                (
                    db.con.execute(
                        "SELECT COUNT(*) FROM market_day_state WHERE trade_date = ?::DATE",
                        [trade_date],
                    ).fetchone()
                    or [0]
                )[0]
            )
            next_day_cpr_count = int(
                (
                    db.con.execute(
                        "SELECT COUNT(*) FROM cpr_daily WHERE trade_date = ?::DATE",
                        [trade_date],
                    ).fetchone()
                    or [0]
                )[0]
            )
            next_day_rows_missing = next_day_mds_count == 0 or next_day_cpr_count == 0
        except Exception:
            pass

    if setup_only_mode:
        # Setup-only: current/future live day has no 5-min parquet yet. Gate on the
        # latest completed trading day's daily + ATR data plus a non-empty dated universe.
        # Also require that next-day CPR and market_day_state rows exist (built by EOD pipeline).
        ready = (
            bool(candidate_symbols)
            and not freshness_blocking
            and not coverage_blocking
            and not next_day_rows_missing
        )
    else:
        ready = (
            bool(candidate_symbols)
            and not freshness_blocking
            and not coverage_blocking
            and not setup_query_failed
        )

    return {
        "trade_date": trade_date,
        "requested_symbols": candidate_symbols,
        "symbol_source": symbol_source,
        "freshness_tables": freshness_tables,
        "table_max_trade_dates": table_max_dates,
        "freshness_rows": freshness_rows,
        "freshness_comparisons": freshness_comparisons,
        "setup_capable_symbols": setup_capable_symbols,
        "late_starting_symbols": late_starting,
        "setup_query_failed": setup_query_failed,
        "coverage": coverage,
        "missing_counts": missing_counts,
        "coverage_status": coverage_status,
        "coverage_blocking": coverage_blocking,
        "freshness_blocking": freshness_blocking,
        "pre_market": pre_market,
        "setup_only_mode": setup_only_mode,
        "next_day_mds_count": next_day_mds_count,
        "next_day_cpr_count": next_day_cpr_count,
        "next_day_rows_missing": next_day_rows_missing,
        "ready": ready,
    }


def print_trade_date_readiness_report(report: dict[str, Any]) -> None:
    """Render a trade-date readiness report."""
    trade_date = str(report.get("trade_date", ""))
    candidate_symbols = list(report.get("requested_symbols") or [])
    freshness_rows = list(report.get("freshness_rows") or [])
    freshness_comparisons = list(report.get("freshness_comparisons") or [])
    setup_capable_symbols = list(report.get("setup_capable_symbols") or [])
    late_starting_symbols = list(report.get("late_starting_symbols") or [])
    setup_query_failed = bool(report.get("setup_query_failed", False))
    coverage = report.get("coverage") or {}
    coverage_status = report.get("coverage_status") or {}
    ready = bool(report.get("ready", False))

    pre_market = bool(report.get("pre_market", False))
    setup_only_mode = bool(report.get("setup_only_mode", False))
    symbol_source = str(report.get("symbol_source") or "unknown")

    mode_label = " [SETUP-ONLY MODE]" if setup_only_mode else ""
    if pre_market:
        mode_label = " [PRE-MARKET MODE]"
    print(f"\nTrade-date readiness ({trade_date}){mode_label}:")
    if setup_only_mode:
        print(
            "  Setup-only: same-day 5-min parquet is not expected yet — checking previous completed-day data."
        )
        print(f"  Symbol source: {symbol_source} ({len(candidate_symbols):,} symbols)")
        if not candidate_symbols:
            print("  No dated universe found for the requested trade date.")
    else:
        print(f"  5-min symbols on date: {len(candidate_symbols):,}")
        if not candidate_symbols:
            print("  No 5-min symbols found for the requested trade date.")
            print("  Suggested fix: ingest the date range and rebuild runtime tables.")
    print("\nRuntime table freshness:")
    print(f"  {'Table':<20} {'max trade_date':<12} Status")
    print("  " + "-" * 50)
    for row in freshness_rows:
        print(
            f"  {row.get('table', '')!s:<20} "
            f"{(row.get('max_trade_date') or 'None')!s:<12} "
            f"{row.get('status', '')!s}"
        )

    print("\nFreshness comparison:")
    print(f"  {'Table pair':<43} Status")
    print("  " + "-" * 60)
    for row in freshness_comparisons:
        print(
            f"  {row.get('left', '')!s:<20} vs {row.get('right', '')!s:<20} "
            f"{row.get('status', '')!s}"
        )

    print("\n09:15 candle coverage:")
    if setup_query_failed:
        print("  Unable to inspect 09:15 candle coverage.")
    else:
        print(f"  setup-capable symbols: {len(setup_capable_symbols):,}")
        print(f"  no 09:15 candle (info only): {len(late_starting_symbols):,}")
        if late_starting_symbols:
            print(f"  sample: {_preview_symbols(late_starting_symbols)}")

    print("\nRuntime coverage:")
    print(f"  {'Table':<20} Missing symbols")
    print("  " + "-" * 40)
    coverage_tables = (
        (
            "v_daily",
            "v_5min",
            "atr_intraday",
            "cpr_thresholds",
            "atr_date_mismatch",
            "cpr_threshold_date_mismatch",
        )
        if setup_only_mode
        else ("market_day_state", "strategy_day_state", "intraday_day_pack")
    )
    for table in coverage_tables:
        missing = list((coverage or {}).get(table, []))
        status = str((coverage_status or {}).get(table) or "ok").upper()
        marker = f" [{status}]" if missing else ""
        print(f"  {table:<20} {len(missing):>14,}{marker}")
        if missing:
            print(f"    sample: {_preview_symbols(missing)}")

    if setup_only_mode:
        next_day_mds = int(report.get("next_day_mds_count") or 0)
        next_day_cpr = int(report.get("next_day_cpr_count") or 0)
        next_day_missing = bool(report.get("next_day_rows_missing", False))
        marker = " [MISSING - BLOCKING]" if next_day_missing else " [OK]"
        print(f"\nNext-day setup rows ({trade_date}):")
        print(f"  market_day_state : {next_day_mds:>6,}{marker}")
        print(f"  cpr_daily        : {next_day_cpr:>6,}{marker if next_day_cpr == 0 else ' [OK]'}")
        if next_day_missing:
            print(
                f"\n  [CRITICAL] EOD pipeline did not build next-day CPR/state rows.\n"
                f"  Fix (targeted table rebuild + replica sync):\n"
                f"    doppler run -- uv run pivot-build --table cpr --refresh-date {trade_date}\n"
                f"    doppler run -- uv run pivot-build --table thresholds --refresh-date {trade_date}\n"
                f"    doppler run -- uv run pivot-build --table state --refresh-date {trade_date}\n"
                f"    doppler run -- uv run pivot-build --table strategy --refresh-date {trade_date}\n"
                f"    doppler run -- uv run pivot-sync-replica --verify --trade-date {trade_date}"
            )

    print("\nReadiness:")
    print(f"  {'Ready':<10} {'YES' if ready else 'NO'}")
    if not ready:
        if setup_only_mode:
            print(
                "  Check: previous completed trading day must have v_daily, v_5min, "
                "atr_intraday, and cpr_thresholds coverage. Sparse symbol/day gaps are "
                "warnings; broad gaps block."
            )
            if bool(report.get("next_day_rows_missing")):
                print(
                    f"  Fix (next-day rows):\n"
                    f"    doppler run -- uv run pivot-build --table cpr --refresh-date {trade_date}\n"
                    f"    doppler run -- uv run pivot-build --table thresholds --refresh-date {trade_date}\n"
                    f"    doppler run -- uv run pivot-build --table state --refresh-date {trade_date}\n"
                    f"    doppler run -- uv run pivot-build --table strategy --refresh-date {trade_date}\n"
                    f"    doppler run -- uv run pivot-sync-replica --verify --trade-date {trade_date}"
                )
            else:
                print(
                    "  Fix (prev-day data): doppler run -- uv run pivot-refresh --eod-ingest "
                    "--date <prev_trading_date> --trade-date <trade_date>"
                )
        else:
            print(
                f"  Suggested fix: doppler run -- uv run pivot-build --refresh-since {trade_date}"
            )


def _print_trade_date_report(trade_date: str) -> bool:
    """Print a readiness report for a specific trade date."""
    report = build_trade_date_readiness_report(trade_date)
    print_trade_date_readiness_report(report)
    return bool(report["ready"])


def main() -> int:
    configure_windows_stdio(line_buffering=True, write_through=True)
    parser = argparse.ArgumentParser(description="Inspect and refresh backtest data quality issues")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-scan dataset and refresh issue registry (fast: parquet presence only)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="With --refresh: run comprehensive scan (OHLC, timestamps, gaps, extremes). Takes 1-5 min.",
    )
    parser.add_argument(
        "--show-inactive",
        action="store_true",
        help="Include inactive/resolved issue rows",
    )
    parser.add_argument(
        "--issue-code",
        default=None,
        help="Filter by issue code (e.g. OHLC_VIOLATION, TIMESTAMP_INVALID)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max rows to print (0 = all, default 100)",
    )
    parser.add_argument(
        "--window-start",
        default=None,
        help="Window start date (YYYY-MM-DD) for a lightweight bounded DQ report.",
    )
    parser.add_argument(
        "--window-end",
        default=None,
        help="Window end date (YYYY-MM-DD) for a lightweight bounded DQ report.",
    )
    parser.add_argument(
        "--date",
        "--trade-date",
        dest="trade_date",
        default=None,
        help="Trade date (YYYY-MM-DD) for the runtime readiness gate.",
    )
    args = parser.parse_args()

    if bool(args.window_start) ^ bool(args.window_end):
        parser.error("--window-start and --window-end must be provided together")

    if args.trade_date:
        return 0 if _print_trade_date_report(resolve_trade_date(args.trade_date)) else 1

    db = get_db()

    if args.refresh:
        with acquire_command_lock("runtime-writer", detail="runtime writer"):
            # Always run the fast parquet-presence check
            fast_summary = db.refresh_data_quality_issues()
            print(
                f"Fast refresh: missing_5min={fast_summary.get('missing_5min', 0)} "
                f"active={fast_summary.get('active_issues', 0)}",
                flush=True,
            )

            if args.full:
                print(
                    "\nRunning comprehensive DQ scan (this may take 1-5 minutes)...",
                    flush=True,
                )
                t0 = time.time()
                scan_summary = db.run_comprehensive_dq_scan()
                elapsed = time.time() - t0
                print(f"Scan completed in {elapsed:.1f}s", flush=True)
                _print_summary(scan_summary)
            else:
                # Fast-refresh only: publish replica so dashboard picks up changes
                db._publish_replica(force=True)

    if args.window_start and args.window_end:
        _print_window_report(args.window_start, args.window_end)

    rows = db.get_data_quality_issues(
        active_only=not args.show_inactive,
        issue_code=args.issue_code,
    )

    if not rows:
        scope = "active" if not args.show_inactive else "active+inactive"
        print(f"\nNo {scope} data quality issues found.")
        return 0

    print()
    _print_issues(rows, limit=args.limit)
    if args.limit > 0 and len(rows) > args.limit:
        print(f"\nTotal rows: {len(rows)} (showing first {args.limit})")
    else:
        print(f"\nTotal rows: {len(rows)}")

    return 0


if __name__ in {"__main__", "__mp_main__"}:
    raise SystemExit(main())
