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
from engine.command_lock import acquire_command_lock
from scripts.paper_prepare import resolve_trade_date

_IST = timezone(timedelta(hours=5, minutes=30))


def _is_pre_market(trade_date: str) -> bool:
    """True if trade_date is today and current IST time is before 09:15."""
    now_ist = datetime.now(_IST)
    today = now_ist.date().isoformat()
    return trade_date == today and (now_ist.hour, now_ist.minute) < (9, 15)


_SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}

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
    candidate_symbols = sorted(db.get_symbols_with_parquet_data([trade_date]))

    freshness_tables = [
        "or_daily",
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
            # State tables ahead of pack — expected pre-market on a live trading day.
            # Pack only contains historical data; state tables are built one day ahead.
            status = f"AHEAD of pack ({max_date} > {pack_date})"
        else:
            # State table is BEHIND pack — genuine staleness problem.
            status = f"OUT OF SYNC vs {pack_date}"
            freshness_blocking = True
        freshness_rows.append({"table": table, "max_trade_date": max_date, "status": status})

    freshness_comparisons: list[dict[str, str | None]] = []
    for left, right in [
        ("or_daily", "intraday_day_pack"),
        ("market_day_state", "intraday_day_pack"),
        ("strategy_day_state", "intraday_day_pack"),
    ]:
        left_date = table_max_dates.get(left)
        right_date = table_max_dates.get(right)
        if left_date is not None and right_date is not None and left_date == right_date:
            status = f"OK ({left_date})"
        elif left_date is not None and right_date is not None and left_date > right_date:
            # Left (state) is ahead of right (pack) — expected pre-market for live day.
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

    coverage = db.get_runtime_trade_date_coverage(candidate_symbols, trade_date)
    missing_counts = {table: len(symbols) for table, symbols in coverage.items()}

    coverage_blocking = any(
        missing_counts[table] > 0
        for table in (
            "market_day_state",
            "strategy_day_state",
            "intraday_day_pack",
        )
    )

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

    pre_market = _is_pre_market(trade_date)
    if pre_market:
        # Pre-market: no 5-min parquet exists yet for today. Gate only on state table coverage.
        ready = not freshness_blocking and not coverage_blocking
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
        "freshness_tables": freshness_tables,
        "table_max_trade_dates": table_max_dates,
        "freshness_rows": freshness_rows,
        "freshness_comparisons": freshness_comparisons,
        "setup_capable_symbols": setup_capable_symbols,
        "late_starting_symbols": late_starting,
        "setup_query_failed": setup_query_failed,
        "coverage": coverage,
        "missing_counts": missing_counts,
        "coverage_blocking": coverage_blocking,
        "freshness_blocking": freshness_blocking,
        "pre_market": pre_market,
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
    ready = bool(report.get("ready", False))

    pre_market = bool(report.get("pre_market", False))

    print(f"\nTrade-date readiness ({trade_date}){' [PRE-MARKET MODE]' if pre_market else ''}:")
    if pre_market:
        print(
            "  Pre-market: 5-min parquet for today does not exist yet — checking state tables only."
        )
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
    for table in ("market_day_state", "strategy_day_state", "intraday_day_pack"):
        missing = list((coverage or {}).get(table, []))
        marker = " [BLOCKING]" if missing else ""
        print(f"  {table:<20} {len(missing):>14,}{marker}")
        if missing:
            print(f"    sample: {_preview_symbols(missing)}")

    print("\nReadiness:")
    print(f"  {'Ready':<10} {'YES' if ready else 'NO'}")
    if not ready:
        if pre_market:
            print("  Check: market_day_state and strategy_day_state must cover today.")
            print(
                "  Fix:   doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols"
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
