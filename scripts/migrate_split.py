"""Backfill historical backtest tables from market.duckdb into backtest.duckdb.

This script is intentionally non-destructive:
- it copies missing backtest rows into backtest.duckdb
- it does not delete anything from market.duckdb
- it preserves existing rows already present in backtest.duckdb

That lets us seed the historical baseline once, then keep the replica
in sync going forward without risking the original data.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from db.backtest_db import BacktestDB
from db.replica import ReplicaSync

DATA_DIR = Path(__file__).parent.parent / "data"
MARKET_DB = DATA_DIR / "market.duckdb"
BACKTEST_DB = DATA_DIR / "backtest.duckdb"
BACKTEST_REPLICA_DIR = DATA_DIR / "backtest_replica"

BACKTEST_TABLES = [
    "backtest_results",
    "run_metadata",
    "run_metrics",
    "run_daily_pnl",
    "setup_funnel",
]


def get_row_count(con, table: str) -> int:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def table_exists(con, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _table_columns(con, table: str) -> list[str]:
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """,
            [table],
        ).fetchall()
    except Exception:
        return []
    return [str(row[0]) for row in rows if row and row[0]]


def _missing_row_count(dest_con, source_alias: str, table: str) -> int:
    try:
        row = dest_con.execute(
            f"""
            SELECT COUNT(*)
            FROM {source_alias}.{table} AS src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {table} AS dst
                WHERE dst.run_id = src.run_id
            )
            """,
        ).fetchone()
    except Exception:
        return 0
    return int(row[0] or 0) if row else 0


def _copy_missing_rows(dest_con, source_con, source_alias: str, table: str) -> int:
    source_cols = _table_columns(source_con, table)
    dest_cols = _table_columns(dest_con, table)
    columns: list[str] = []
    seen: set[str] = set()
    for col in dest_cols:
        if col in source_cols and col not in seen:
            columns.append(col)
            seen.add(col)
    if not columns:
        return 0

    missing_rows = _missing_row_count(dest_con, source_alias, table)
    if missing_rows <= 0:
        return 0

    select_cols = ", ".join(f"src.{col}" for col in columns)
    insert_cols = ", ".join(columns)
    dest_con.execute(
        f"""
        INSERT INTO {table} ({insert_cols})
        SELECT {select_cols}
        FROM {source_alias}.{table} AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table} AS dst
            WHERE dst.run_id = src.run_id
        )
        """
    )
    return missing_rows


def _seed_replica_from_sources(*, source_paths: list[Path]) -> None:
    """Build a fresh backtest replica snapshot from readable source files.

    This is the fallback when the canonical backtest.duckdb file is locked by
    a live process. It keeps the dashboard working even before the source file
    can be backfilled.
    """
    seed_path = DATA_DIR / "_backtest_replica_seed.duckdb"
    seed_path.unlink(missing_ok=True)
    seed_sync = ReplicaSync(seed_path, BACKTEST_REPLICA_DIR, min_interval_sec=0.0)
    seed = BacktestDB(db_path=seed_path, replica_sync=seed_sync)
    try:
        seed.ensure_backtest_table()
        seed.ensure_run_metadata_table()
        seed.ensure_run_metrics_table()
        seed.ensure_run_daily_pnl_table()
        seed.ensure_setup_funnel_table()

        copied_total = 0
        for source_path in source_paths:
            if not source_path.exists():
                continue
            source_alias = f"{source_path.stem}_src"
            try:
                source_con = duckdb.connect(str(source_path), read_only=True)
            except Exception as exc:
                print(f"  Skipping {source_path.name}: {exc}")
                continue
            try:
                seed.con.execute(f"ATTACH '{source_path.as_posix()}' AS {source_alias} (READ_ONLY)")
                try:
                    for table in BACKTEST_TABLES:
                        if not table_exists(source_con, table):
                            continue
                        copied_total += _copy_missing_rows(
                            seed.con, source_con, source_alias, table
                        )
                finally:
                    try:
                        seed.con.execute(f"DETACH {source_alias}")
                    except Exception:
                        pass
            finally:
                source_con.close()

        replica_sync = seed._sync
        if replica_sync is not None:
            replica_sync.force_sync(seed.con)
        seed.close()
        print(f"\nPublished replica snapshot from {copied_total:,} backtest rows.")
        print(f"Replica snapshot: {BACKTEST_REPLICA_DIR}")
    finally:
        seed_path.unlink(missing_ok=True)


def run_migration(*, dry_run: bool = False) -> None:
    print("=== Backfill: market.duckdb -> backtest.duckdb ===\n")

    if not MARKET_DB.exists():
        print(f"ERROR: {MARKET_DB} not found. Run pivot-build first.")
        sys.exit(1)

    print(f"Source: {MARKET_DB} ({MARKET_DB.stat().st_size / 1e6:.1f} MB)")
    market_con = duckdb.connect(str(MARKET_DB), read_only=True)
    source_counts: dict[str, int] = {}
    for table in BACKTEST_TABLES:
        if table_exists(market_con, table):
            count = get_row_count(market_con, table)
            source_counts[table] = count
            print(f"  {table}: {count:,} rows")
        else:
            print(f"  {table}: NOT FOUND (skipping)")

    if not source_counts:
        print("\nNo backtest tables found in market.duckdb — nothing to backfill.")
        market_con.close()
        return

    replica_sync = ReplicaSync(BACKTEST_DB, BACKTEST_REPLICA_DIR, min_interval_sec=0.0)
    bt: BacktestDB | None = None
    try:
        bt = BacktestDB(db_path=BACKTEST_DB, replica_sync=replica_sync)
    except Exception as exc:
        print(f"\nCanonical backtest.duckdb is locked ({exc}).")
        print("Seeding the dashboard replica directly from readable sources instead.")
        _seed_replica_from_sources(source_paths=[BACKTEST_DB, MARKET_DB])
        market_con.close()
        return
    try:
        bt.ensure_backtest_table()
        bt.ensure_run_metadata_table()
        bt.ensure_run_metrics_table()
        bt.ensure_run_daily_pnl_table()
        bt.ensure_setup_funnel_table()

        source_alias = "market_src"
        bt.con.execute(f"ATTACH '{MARKET_DB.as_posix()}' AS {source_alias} (READ_ONLY)")
        try:
            if dry_run:
                print("\n[DRY RUN] Missing rows that would be copied:")
                for table in BACKTEST_TABLES:
                    if not table_exists(market_con, table):
                        continue
                    missing = _missing_row_count(bt.con, source_alias, table)
                    print(f"  {table}: {missing:,} rows")
                return

            bt.con.execute("BEGIN TRANSACTION")
            try:
                total_copied = 0
                for table in BACKTEST_TABLES:
                    if not table_exists(market_con, table):
                        continue
                    copied = _copy_missing_rows(bt.con, market_con, source_alias, table)
                    total_copied += copied
                    dest_count = get_row_count(bt.con, table)
                    print(
                        f"  {table}: copied {copied:,} missing rows; "
                        f"target now has {dest_count:,} rows"
                    )
                bt.con.execute("COMMIT")
            except Exception:
                bt.con.execute("ROLLBACK")
                raise

            try:
                bt.con.execute(f"DETACH {source_alias}")
            except Exception:
                pass
            bt_replica_sync: ReplicaSync | None = bt._sync
            if bt_replica_sync is not None:
                bt_replica_sync.force_sync(bt.con)
            bt.close()
            bt = None
            print(f"\nCopied {total_copied:,} missing rows into {BACKTEST_DB}")
            print("Source market.duckdb was left intact.")
            print(f"Replica snapshot: {BACKTEST_REPLICA_DIR}")
        finally:
            pass
    finally:
        market_con.close()
        if bt is not None:
            bt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill backtest tables into backtest.duckdb")
    parser.add_argument(
        "--split-backtest",
        action="store_true",
        help="Copy backtest tables from market.duckdb into backtest.duckdb",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without making changes",
    )
    args = parser.parse_args()

    if not args.split_backtest:
        parser.print_help()
        sys.exit(1)

    run_migration(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
