"""Reset backtest, walk-forward, and paper-trading run history."""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import text

from db.backtest_db import get_backtest_db
from db.postgres import get_db_session
from engine.cli_setup import configure_windows_asyncio, configure_windows_stdio
from engine.command_lock import command_lock

BACKTEST_TABLES: tuple[str, ...] = (
    "backtest_results",
    "run_daily_pnl",
    "run_metrics",
    "run_metadata",
    "setup_funnel",
)

POSTGRES_TABLES: tuple[str, ...] = (
    "walk_forward_runs",
    "walk_forward_folds",
    "paper_trading_sessions",
    "paper_positions",
    "paper_orders",
    "paper_feed_state",
)


def _count_duckdb_table(db, table: str) -> int:
    try:
        row = db.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    except Exception:
        return 0
    return int(row[0] or 0) if row else 0


def _collect_duckdb_counts() -> dict[str, int]:
    backtest_db = get_backtest_db()
    for ensure in (
        backtest_db.ensure_backtest_table,
        backtest_db.ensure_run_daily_pnl_table,
        backtest_db.ensure_run_metrics_table,
        backtest_db.ensure_run_metadata_table,
        backtest_db.ensure_setup_funnel_table,
    ):
        try:
            ensure()
        except Exception:
            continue
    return {table: _count_duckdb_table(backtest_db, table) for table in BACKTEST_TABLES}


async def _collect_postgres_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    async with get_db_session() as session:
        for table in POSTGRES_TABLES:
            try:
                result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = int(result.scalar_one() or 0)
            except Exception:
                counts[table] = 0
    return counts


def _wipe_duckdb() -> dict[str, int]:
    counts = _collect_duckdb_counts()
    backtest_db = get_backtest_db()
    backtest_db.con.execute("BEGIN TRANSACTION")
    try:
        for table in BACKTEST_TABLES:
            try:
                backtest_db.con.execute(f"DELETE FROM {table}")
            except Exception:
                continue
        backtest_db.con.execute("COMMIT")
        sync = getattr(backtest_db, "_sync", None)
        if sync is not None:
            sync.mark_dirty()
            sync.force_sync(backtest_db.con)
    except Exception:
        backtest_db.con.execute("ROLLBACK")
        raise
    return counts


def _delete_duckdb_run(run_id: str) -> dict[str, int]:
    """Delete a single run by run_id from backtest.duckdb. Syncs replica."""
    deleted = {}
    backtest_db = get_backtest_db()
    backtest_db.con.execute("BEGIN TRANSACTION")
    try:
        for table in BACKTEST_TABLES:
            try:
                result = backtest_db.con.execute(f"DELETE FROM {table} WHERE run_id = ?", [run_id])
                deleted[table] = int(result.rowcount or 0)
            except Exception:
                deleted[table] = 0
        backtest_db.con.execute("COMMIT")
        sync = getattr(backtest_db, "_sync", None)
        if sync is not None:
            sync.mark_dirty()
            sync.force_sync(backtest_db.con)
    except Exception:
        backtest_db.con.execute("ROLLBACK")
        raise
    return deleted


async def _wipe_postgres() -> dict[str, int]:
    counts = await _collect_postgres_counts()
    async with get_db_session() as session:
        for table in ("walk_forward_runs", "paper_trading_sessions"):
            try:
                await session.execute(text(f"DELETE FROM {table}"))
            except Exception:
                continue
    return counts


def _print_counts(title: str, counts: dict[str, int]) -> None:
    print(title)
    for table, count in counts.items():
        print(f"  {table:<24} {count:>12,}")


@command_lock("runtime-writer", detail="runtime writer")
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset DuckDB run history and PostgreSQL paper/walk-forward state."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Only wipe DuckDB run history.",
    )
    group.add_argument(
        "--postgres-only",
        action="store_true",
        help="Only wipe PostgreSQL paper/walk-forward state.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the deletes. Default is dry-run only.",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Delete a specific run by run_id from backtest.duckdb only. "
        "Requires --apply. Cannot be combined with full wipe.",
    )
    args = parser.parse_args()

    if args.run_id and (args.duckdb_only or args.postgres_only or not args.apply):
        parser.error(
            "--run-id requires --apply and cannot be combined with --duckdb-only/--postgres-only"
        )

    configure_windows_stdio(line_buffering=True, write_through=True)
    configure_windows_asyncio()

    # --- Single run deletion ---
    if args.run_id:
        print(f"Targeting run_id: {args.run_id}")
        deleted = _delete_duckdb_run(args.run_id)
        total = sum(deleted.values())
        if total == 0:
            print(f"No rows found for run_id={args.run_id}")
        else:
            _print_counts("Deleted:", deleted)
        return

    # --- Full wipe mode ---
    do_duckdb = not args.postgres_only
    do_postgres = not args.duckdb_only

    duckdb_counts = _collect_duckdb_counts() if do_duckdb else {}
    postgres_counts = asyncio.run(_collect_postgres_counts()) if do_postgres else {}

    if do_duckdb:
        _print_counts("DuckDB run history:", duckdb_counts)
    if do_postgres:
        _print_counts("PostgreSQL run state:", postgres_counts)

    if not args.apply:
        print("Dry-run only. Re-run with --apply to execute deletes.")
        return

    wiped_duckdb = _wipe_duckdb() if do_duckdb else {}
    wiped_postgres = asyncio.run(_wipe_postgres()) if do_postgres else {}
    print("Reset complete.")
    if do_duckdb:
        _print_counts("DuckDB rows deleted:", wiped_duckdb)
    if do_postgres:
        _print_counts("PostgreSQL rows deleted:", wiped_postgres)


if __name__ in {"__main__", "__mp_main__"}:
    main()
