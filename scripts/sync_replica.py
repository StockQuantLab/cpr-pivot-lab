"""Sync market_replica/ with the latest market.duckdb snapshot.

Uses force_sync() which raises on error — never silently succeeds on failure.
Verification reads the actual replica file, not the source database.

Usage:
    doppler run -- uv run pivot-sync-replica
    doppler run -- uv run pivot-sync-replica --verify --trade-date 2026-04-30
    doppler run -- uv run pivot-sync-replica --verify --trade-date today
"""

from __future__ import annotations

import argparse
import datetime
import sys
from zoneinfo import ZoneInfo

import duckdb

from db.duckdb import DUCKDB_FILE, REPLICA_DIR, get_db
from db.replica import ReplicaSync
from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

IST = ZoneInfo("Asia/Kolkata")


def _get_trade_date(raw: str | None) -> str:
    if not raw or raw.strip().lower() == "today":
        return datetime.datetime.now(IST).date().isoformat()
    return raw.strip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync market_replica/ with the latest market.duckdb data."
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="After syncing, assert the replica has market_day_state rows for --trade-date.",
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="Trade date to verify in the replica (YYYY-MM-DD or 'today'). Required with --verify.",
    )
    args = parser.parse_args()

    db = get_db()

    # force_sync() always runs and raises on error — never silently swallows failures.
    sync = ReplicaSync(DUCKDB_FILE, REPLICA_DIR)
    sync.force_sync(source_conn=db.con)

    # Confirm the replica pointer file was written.
    if not sync.latest_pointer.exists():
        print("[ERROR] Replica sync appeared to succeed but pointer file is missing.", flush=True)
        sys.exit(1)

    version_tag = sync.latest_pointer.read_text().strip()
    replica_file = REPLICA_DIR / f"{DUCKDB_FILE.stem}_replica_{version_tag}.duckdb"
    if not replica_file.exists():
        print(
            f"[ERROR] Replica file {replica_file} not found after sync.",
            flush=True,
        )
        sys.exit(1)

    print(f"market_replica synced → {replica_file.name}", flush=True)

    if args.verify:
        trade_date = _get_trade_date(args.trade_date)

        # Open the replica file directly — NOT the source db — to verify what the
        # live session will actually read.
        replica_con = duckdb.connect(str(replica_file), read_only=True)
        try:
            mds_row = replica_con.execute(
                "SELECT COUNT(*) FROM market_day_state WHERE trade_date = ?::DATE",
                [trade_date],
            ).fetchone()
            mds_count = int(mds_row[0] if mds_row else 0)

            cpr_row = replica_con.execute(
                "SELECT COUNT(*) FROM cpr_daily WHERE trade_date = ?::DATE",
                [trade_date],
            ).fetchone()
            cpr_count = int(cpr_row[0] if cpr_row else 0)
        finally:
            replica_con.close()

        print(f"Replica market_day_state rows for {trade_date}: {mds_count}", flush=True)
        print(f"Replica cpr_daily rows for {trade_date}: {cpr_count}", flush=True)

        if mds_count == 0 or cpr_count == 0:
            print(
                f"\n[CRITICAL] Replica is missing next-day rows for {trade_date}.\n"
                "Fix (run in order):\n"
                f"  doppler run -- uv run pivot-build --table cpr --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table thresholds --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table state --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table strategy --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-sync-replica --verify --trade-date {trade_date}",
                flush=True,
            )
            sys.exit(1)

        print(
            f"Replica verified for {trade_date} — safe to start daily-live.",
            flush=True,
        )

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
