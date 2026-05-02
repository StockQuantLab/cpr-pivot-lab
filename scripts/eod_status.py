"""Read-only EOD readiness report for the next paper trading day."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from db.duckdb import get_db
from engine.cli_setup import configure_windows_stdio
from scripts.paper_prepare import CANONICAL_FULL_UNIVERSE_NAME

configure_windows_stdio(line_buffering=True, write_through=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")
RUNTIME_TABLES = (
    "cpr_daily",
    "atr_intraday",
    "cpr_thresholds",
    "or_daily",
    "virgin_cpr_flags",
    "market_day_state",
    "strategy_day_state",
    "intraday_day_pack",
)


def _resolve_date(value: str | None, *, default_today: bool = False) -> str | None:
    if value is None:
        return dt.datetime.now(IST).date().isoformat() if default_today else None
    text = value.strip().lower()
    today = dt.datetime.now(IST).date()
    if text == "today":
        return today.isoformat()
    if text == "tomorrow":
        return (today + dt.timedelta(days=1)).isoformat()
    return dt.date.fromisoformat(value).isoformat()


def _dated_universe_name(trade_date: str) -> str:
    return f"full_{trade_date.replace('-', '_')}"


def _load_universe(db: Any, trade_date: str) -> tuple[str, list[str]]:
    for name in (_dated_universe_name(trade_date), CANONICAL_FULL_UNIVERSE_NAME):
        try:
            symbols = db.get_universe_symbols(name)
        except Exception:
            symbols = []
        normalized = sorted({str(s).upper() for s in symbols if str(s or "").strip()})
        if normalized:
            return name, normalized
    return "none", []


def _table_count(db: Any, table: str, trade_date: str) -> int | None:
    try:
        row = db.con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE trade_date = ?::DATE",
            [trade_date],
        ).fetchone()
    except Exception:
        return None
    return int(row[0] or 0) if row else 0


def _table_date_count(db: Any, table: str, column: str, date_value: str) -> int | None:
    try:
        row = db.con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} = ?::DATE",
            [date_value],
        ).fetchone()
    except Exception:
        return None
    return int(row[0] or 0) if row else 0


def _latest_parquet_dates(db: Any, ingest_date: str) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    queries = {
        "v_daily": "SELECT MAX(date)::VARCHAR FROM v_daily WHERE date <= ?::DATE",
        "v_5min": "SELECT MAX(date)::VARCHAR FROM v_5min WHERE date <= ?::DATE",
    }
    for name, sql in queries.items():
        try:
            row = db.con.execute(sql, [ingest_date]).fetchone()
        except Exception:
            row = None
        result[name] = str(row[0]) if row and row[0] is not None else None
    return result


def build_report(ingest_date: str, trade_date: str) -> dict[str, Any]:
    db = get_db()
    universe_name, symbols = _load_universe(db, trade_date)
    max_dates = db.get_table_max_trade_dates(list(RUNTIME_TABLES))
    table_counts = {table: _table_count(db, table, trade_date) for table in RUNTIME_TABLES}
    completed_date_counts = {
        "atr_intraday": _table_count(db, "atr_intraday", ingest_date),
        "intraday_day_pack": _table_count(db, "intraday_day_pack", ingest_date),
        "v_daily": _table_date_count(db, "v_daily", "date", ingest_date),
        "v_5min": _table_date_count(db, "v_5min", "date", ingest_date),
    }
    parquet_dates = _latest_parquet_dates(db, ingest_date)

    issues: list[str] = []
    warnings: list[str] = []
    if not symbols:
        issues.append("No dated or canonical full universe found.")
    for source, max_date in parquet_dates.items():
        if max_date != ingest_date:
            issues.append(f"{source} latest date is {max_date}; expected {ingest_date}.")

    completed_date_tables = ("v_daily", "v_5min", "atr_intraday", "intraday_day_pack")
    for table in completed_date_tables:
        count = completed_date_counts.get(table)
        if count is None:
            issues.append(f"{table} is missing or unreadable for {ingest_date}.")
        elif symbols and count < int(len(symbols) * 0.95):
            issues.append(
                f"{table} has {count} rows for {ingest_date}; expected near {len(symbols)}."
            )
        elif symbols and count < len(symbols):
            warnings.append(f"{table} has sparse completed-date coverage: {count}/{len(symbols)}.")

    setup_tables = (
        "cpr_daily",
        "cpr_thresholds",
        "or_daily",
        "virgin_cpr_flags",
        "market_day_state",
        "strategy_day_state",
    )
    for table in setup_tables:
        count = table_counts.get(table)
        if count is None:
            issues.append(f"{table} is missing or unreadable.")
        elif symbols and count < int(len(symbols) * 0.95):
            issues.append(
                f"{table} has {count} rows for {trade_date}; expected near {len(symbols)}."
            )
        elif symbols and count < len(symbols):
            warnings.append(f"{table} has sparse coverage: {count}/{len(symbols)} rows.")

    dated_universe_exists = universe_name == _dated_universe_name(trade_date)
    if not dated_universe_exists:
        warnings.append(
            f"Dated universe {_dated_universe_name(trade_date)} is missing; using {universe_name}."
        )

    ready = not issues
    recovery = []
    if issues:
        recovery.extend(
            [
                (
                    "doppler run -- uv run pivot-refresh --eod-ingest "
                    f"--date {ingest_date} --trade-date {trade_date}"
                ),
                f"doppler run -- uv run pivot-data-quality --date {trade_date}",
            ]
        )
    return {
        "ready": ready,
        "ingest_date": ingest_date,
        "trade_date": trade_date,
        "universe_name": universe_name,
        "symbol_count": len(symbols),
        "parquet_latest_dates": parquet_dates,
        "runtime_max_dates": max_dates,
        "completed_date_row_counts": completed_date_counts,
        "trade_date_row_counts": table_counts,
        "issues": issues,
        "warnings": warnings,
        "recovery_commands": recovery,
    }


def _print_report(report: dict[str, Any]) -> None:
    print(f"EOD readiness for {report['trade_date']} (ingest date {report['ingest_date']})")
    print(f"Ready: {'YES' if report['ready'] else 'NO'}")
    print(f"Universe: {report['universe_name']} ({report['symbol_count']} symbols)")
    print("\nLatest source dates:")
    for key, value in report["parquet_latest_dates"].items():
        print(f"  {key:<10} {value}")
    print("\nTrade-date row counts:")
    for key, value in report["trade_date_row_counts"].items():
        print(f"  {key:<22} {value}")
    print("\nCompleted-date row counts:")
    for key, value in report["completed_date_row_counts"].items():
        print(f"  {key:<22} {value}")
    if report["warnings"]:
        print("\nWarnings:")
        for item in report["warnings"]:
            print(f"  - {item}")
    if report["issues"]:
        print("\nBlocking issues:")
        for item in report["issues"]:
            print(f"  - {item}")
        print("\nRecovery:")
        for command in report["recovery_commands"]:
            print(f"  {command}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print read-only EOD readiness status.")
    parser.add_argument("--date", default="today", help="Completed ingestion date.")
    parser.add_argument("--trade-date", required=True, help="Next paper trading date.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when not ready.")
    args = parser.parse_args()
    ingest_date = _resolve_date(args.date, default_today=True)
    trade_date = _resolve_date(args.trade_date)
    if ingest_date is None or trade_date is None:
        raise SystemExit("Both --date and --trade-date must resolve to dates.")
    report = build_report(ingest_date, trade_date)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report)
    if args.strict and not report["ready"]:
        raise SystemExit(1)


if __name__ in {"__main__", "__mp_main__"}:
    main()
