from __future__ import annotations

import argparse
from datetime import datetime

from db.duckdb import get_db
from engine.command_lock import command_lock

TABLES = ["backtest_results", "run_daily_pnl", "run_metrics", "run_metadata"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prune historical backtest runs from dashboard/runtime tables. "
            "Dry-run by default; pass --apply to execute deletes."
        )
    )
    parser.add_argument(
        "--keep-since",
        default="2026-03-09 00:00:00",
        help="Keep runs with run_metadata.created_at >= this timestamp (ISO format).",
    )
    parser.add_argument(
        "--gold-symbol-count",
        type=int,
        default=51,
        help="Symbol count used to identify gold_51 runs in run_metrics.",
    )
    parser.add_argument(
        "--no-keep-latest-gold",
        action="store_true",
        help="Disable retaining the latest gold_51 run.",
    )
    parser.add_argument(
        "--keep-all-gold",
        action="store_true",
        help="Retain all gold_51 runs (identified by --gold-symbol-count).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute delete statements (default is dry-run only).",
    )
    parser.add_argument(
        "--keep-run-id",
        action="append",
        default=[],
        help="Explicit run_id to retain (can be repeated).",
    )
    return parser.parse_args()


def _build_keep_set(
    keep_since: datetime,
    keep_gold_mode: str,
    gold_symbol_count: int,
) -> tuple[set[str], str | None, int]:
    con = get_db().con
    keep_ids = {
        row[0]
        for row in con.execute(
            """
            SELECT run_id
            FROM run_metadata
            WHERE created_at >= ?
            """,
            [keep_since],
        ).fetchall()
    }

    latest_gold_run_id: str | None = None
    kept_gold_count = 0
    if keep_gold_mode == "all":
        gold_rows = con.execute(
            """
            SELECT run_id
            FROM run_metrics
            WHERE symbol_count = ?
            """,
            [gold_symbol_count],
        ).fetchall()
        gold_ids = {row[0] for row in gold_rows if row and row[0]}
        keep_ids.update(gold_ids)
        kept_gold_count = len(gold_ids)
        if gold_ids:
            latest_gold_run_id = con.execute(
                """
                SELECT run_id
                FROM run_metrics
                WHERE symbol_count = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                [gold_symbol_count],
            ).fetchone()[0]
    elif keep_gold_mode == "latest":
        row = con.execute(
            """
            SELECT run_id
            FROM run_metrics
            WHERE symbol_count = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            [gold_symbol_count],
        ).fetchone()
        if row and row[0]:
            latest_gold_run_id = row[0]
            keep_ids.add(latest_gold_run_id)
            kept_gold_count = 1

    return keep_ids, latest_gold_run_id, kept_gold_count


def _count_rows(table: str) -> int:
    con = get_db().con
    row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0)


def _summarize_impact(prune_ids: set[str]) -> list[tuple[str, int, int, int]]:
    con = get_db().con
    summary: list[tuple[str, int, int, int]] = []

    if prune_ids:
        con.execute("CREATE OR REPLACE TEMP TABLE _prune_ids(run_id VARCHAR PRIMARY KEY)")
        con.executemany(
            "INSERT INTO _prune_ids(run_id) VALUES (?)",
            [(rid,) for rid in sorted(prune_ids)],
        )

    for table in TABLES:
        total = _count_rows(table)
        remove = 0
        if prune_ids:
            row = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id IN (SELECT run_id FROM _prune_ids)"
            ).fetchone()
            remove = int(row[0] or 0)
        summary.append((table, total, remove, total - remove))
    return summary


def _apply_prune(prune_ids: set[str]) -> None:
    if not prune_ids:
        print("Nothing to delete.")
        return

    con = get_db().con
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("CREATE OR REPLACE TEMP TABLE _prune_ids(run_id VARCHAR PRIMARY KEY)")
        con.executemany(
            "INSERT INTO _prune_ids(run_id) VALUES (?)",
            [(rid,) for rid in sorted(prune_ids)],
        )
        for table in TABLES:
            con.execute(f"DELETE FROM {table} WHERE run_id IN (SELECT run_id FROM _prune_ids)")
        con.execute("COMMIT")
    except Exception as _:
        con.execute("ROLLBACK")
        raise


@command_lock("runtime-writer", detail="runtime writer")
def main() -> None:
    args = _parse_args()
    keep_since = datetime.fromisoformat(args.keep_since)
    if args.keep_all_gold:
        keep_gold_mode = "all"
    elif args.no_keep_latest_gold:
        keep_gold_mode = "none"
    else:
        keep_gold_mode = "latest"

    con = get_db().con
    # Build prune universe from all runtime tables (captures orphan run_ids too).
    all_ids = {
        row[0]
        for row in con.execute(
            """
            SELECT DISTINCT run_id FROM (
                SELECT run_id FROM run_metadata
                UNION ALL
                SELECT run_id FROM run_metrics
                UNION ALL
                SELECT run_id FROM run_daily_pnl
                UNION ALL
                SELECT run_id FROM backtest_results
            )
            WHERE run_id IS NOT NULL AND run_id <> ''
            """
        ).fetchall()
    }
    keep_ids, latest_gold_run_id, kept_gold_count = _build_keep_set(
        keep_since=keep_since,
        keep_gold_mode=keep_gold_mode,
        gold_symbol_count=args.gold_symbol_count,
    )

    if args.keep_run_id:
        keep_ids.update(rid for rid in args.keep_run_id if rid)
    prune_ids = all_ids - keep_ids

    print(f"Total run_ids across runtime tables: {len(all_ids):,}")
    print(f"Keep run_ids: {len(keep_ids):,}")
    print(f"Prune run_ids: {len(prune_ids):,}")
    print(f"Gold keep mode: {keep_gold_mode} (kept {kept_gold_count})")
    if latest_gold_run_id:
        print(f"Latest gold run id: {latest_gold_run_id[:12]}")
    print(
        f"Rule: keep run_metadata.created_at >= {keep_since.isoformat(sep=' ')}"
        + (
            " + all gold runs"
            if keep_gold_mode == "all"
            else (" + latest gold run" if keep_gold_mode == "latest" else "")
        )
    )

    summary = _summarize_impact(prune_ids)
    for table, total, remove, keep in summary:
        print(f"{table}: total={total:,} remove={remove:,} keep={keep:,}")

    if prune_ids:
        sample = sorted(prune_ids)[:10]
        print("Sample prune run_ids:", [rid[:12] for rid in sample])

    if not args.apply:
        print("Dry-run only. Re-run with --apply to execute deletes.")
        return

    _apply_prune(prune_ids)
    print("Delete completed.")
    for table in TABLES:
        print(f"{table} now has {_count_rows(table):,} rows")


if __name__ in {"__main__", "__mp_main__"}:
    main()
