"""Data hygiene: detect and purge dead (delisted) symbols from parquet and DuckDB."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from engine.cli_setup import configure_windows_stdio
from engine.command_lock import command_lock
from engine.kite_ingestion import get_kite_paths, tradeable_symbols

logger = logging.getLogger(__name__)

MIN_TRADEABLE_SYMBOLS = 1000
MIN_HISTORY_DAYS = 252  # < 1 year of trading days → SHORT_HISTORY
MIN_AVG_TURNOVER = 5_000_000  # ₹50 lakh avg 9:15 candle turnover → ILLIQUID

TABLES_TO_PURGE = [
    "cpr_daily",
    "atr_intraday",
    "cpr_thresholds",
    "market_day_state",
    "strategy_day_state",
    "intraday_day_pack",
    "or_daily",
    "virgin_cpr_flags",
    "data_quality_issues",
]


def detect_dead_symbols() -> set[str]:
    """Return parquet symbols not in the current Kite instrument master.

    Raises SystemExit if the instrument master is missing or suspiciously small.
    """
    tradeable = tradeable_symbols()
    if tradeable is None:
        raise SystemExit(
            "Instrument master CSV not found. Run `pivot-kite-ingest --refresh-instruments` first."
        )
    if len(tradeable) < MIN_TRADEABLE_SYMBOLS:
        raise SystemExit(
            f"Safety check: instrument master has too few symbols ({len(tradeable)} < "
            f"{MIN_TRADEABLE_SYMBOLS}). The CSV may be corrupted."
        )

    paths = get_kite_paths()
    parquet_root = paths.parquet_root
    parquet_symbols: set[str] = set()
    for mode in ("5min", "daily"):
        mode_dir = parquet_root / mode
        if mode_dir.exists():
            parquet_symbols.update(d.name for d in mode_dir.iterdir() if d.is_dir())

    return parquet_symbols - tradeable


def _parquet_dir_size_bytes(root: Path, symbol: str) -> int:
    """Total bytes of parquet files for a symbol across 5min and daily."""
    total = 0
    for mode in ("5min", "daily"):
        sym_dir = root / mode / symbol
        if sym_dir.exists():
            total += sum(f.stat().st_size for f in sym_dir.rglob("*") if f.is_file())
    return total


def _write_audit_log(dead_symbols: set[str]) -> Path:
    """Write dead symbol list to .tmp_logs/ for audit trail."""
    log_dir = Path(".tmp_logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    path = log_dir / f"hygiene_{ts}.json"
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "action": "purge",
        "dead_symbol_count": len(dead_symbols),
        "dead_symbols": sorted(dead_symbols),
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def _duckdb_dead_row_counts(dead_list: list[str]) -> dict[str, int]:
    """Query DuckDB for per-table row counts of dead symbols."""
    from db.duckdb import get_db

    db = get_db()
    placeholders = ",".join(["?"] * len(dead_list))
    counts: dict[str, int] = {}
    for table in TABLES_TO_PURGE:
        try:
            result = db.con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE symbol IN ({placeholders})",
                dead_list,
            )
            counts[table] = result.fetchone()[0]
        except Exception:
            counts[table] = 0
    return counts


def dry_run(dead_symbols: set[str]) -> None:
    """Print a preview of what would be purged."""
    if not dead_symbols:
        print("No dead symbols found. All parquet symbols are in the instrument master.")
        return

    paths = get_kite_paths()
    parquet_root = paths.parquet_root
    dead_list = sorted(dead_symbols)

    # Parquet sizes
    total_bytes = 0
    print(f"\n{'Symbol':<20} {'Parquet Size':>15}")
    print("-" * 37)
    for sym in dead_list:
        sz = _parquet_dir_size_bytes(parquet_root, sym)
        total_bytes += sz
        print(f"{sym:<20} {sz / 1024 / 1024:>12.1f} MB")

    print("-" * 37)
    print(f"{'TOTAL':<20} {total_bytes / 1024 / 1024:>12.1f} MB")

    # DuckDB row counts
    row_counts = _duckdb_dead_row_counts(dead_list)
    total_rows = sum(row_counts.values())
    print("\nDuckDB rows to delete:")
    for table, count in sorted(row_counts.items()):
        if count > 0:
            print(f"  {table:<25} {count:>10,}")
    print(f"  {'TOTAL':<25} {total_rows:>10,}")

    print(f"\n{len(dead_symbols)} dead symbols would be purged.")
    print("Run with --purge --confirm to execute.")


def list_dead(dead_symbols: set[str]) -> None:
    """Print dead symbol names one per line."""
    for sym in sorted(dead_symbols):
        print(sym)


def purge(dead_symbols: set[str]) -> None:
    """Delete dead symbol data from DuckDB (transactional) then parquet directories."""
    from db.duckdb import get_db

    if not dead_symbols:
        print("No dead symbols to purge.")
        return

    audit_path = _write_audit_log(dead_symbols)
    print(f"Audit log written: {audit_path}")

    dead_list = sorted(dead_symbols)
    placeholders = ",".join(["?"] * len(dead_list))

    # --- Phase 1: DuckDB row deletes (single transaction) ---
    db = get_db()
    total_deleted = 0

    print("\nPurging DuckDB runtime tables...")
    db.con.execute("BEGIN TRANSACTION")
    try:
        for table in TABLES_TO_PURGE:
            try:
                result = db.con.execute(
                    f"DELETE FROM {table} WHERE symbol IN ({placeholders})",
                    dead_list,
                )
                count = result.fetchone()[0]
                total_deleted += count
                print(f"  {table}: {count:,} rows deleted")
            except Exception:
                logger.warning("Table %s not found or empty, skipping", table)
        db.con.execute("COMMIT")
    except Exception:
        db.con.execute("ROLLBACK")
        raise

    # Rebuild dataset_meta
    print("  Rebuilding dataset_meta...")
    db._build_dataset_meta()

    # --- Phase 2: Parquet directory deletion ---
    paths = get_kite_paths()
    parquet_root = paths.parquet_root
    dirs_deleted = 0
    bytes_freed = 0
    failed: list[str] = []

    print("\nDeleting parquet directories...")
    for sym in dead_list:
        for mode in ("5min", "daily"):
            sym_dir = parquet_root / mode / sym
            if sym_dir.exists():
                try:
                    sz = sum(f.stat().st_size for f in sym_dir.rglob("*") if f.is_file())
                    shutil.rmtree(sym_dir)
                    bytes_freed += sz
                    dirs_deleted += 1
                except PermissionError:
                    failed.append(f"{mode}/{sym}")
                    logger.warning(
                        "Cannot delete %s/%s — file locked (close dashboard/DuckDB consumers)",
                        mode,
                        sym,
                    )

    print("\nPurge complete:")
    print(f"  Symbols removed: {len(dead_list)}")
    print(f"  DuckDB rows deleted: {total_deleted:,}")
    print(f"  Parquet dirs deleted: {dirs_deleted}")
    print(f"  Disk freed: {bytes_freed / 1024 / 1024:.1f} MB")
    if failed:
        print(f"  WARNING: {len(failed)} dirs could not be deleted (file locked):")
        for f in failed:
            print(f"    {f}")


def detect_short_history() -> set[str]:
    """Return symbols in market_day_state with fewer than MIN_HISTORY_DAYS trading days."""
    from db.duckdb import get_db

    db = get_db()
    rows = db.con.execute(
        """
        SELECT symbol
        FROM market_day_state
        GROUP BY symbol
        HAVING COUNT(DISTINCT trade_date) < ?
        """,
        [MIN_HISTORY_DAYS],
    ).fetchall()
    return {r[0] for r in rows if r}


def detect_illiquid() -> set[str]:
    """Return symbols whose avg 9:15 turnover (volume_915 × prev_close) is below threshold.

    Requires at least 60 trading days of data to avoid flagging newly added symbols.
    """
    from db.duckdb import get_db

    db = get_db()
    try:
        rows = db.con.execute(
            """
            SELECT symbol
            FROM (
                SELECT symbol, AVG(volume_915 * prev_close) AS avg_turnover
                FROM market_day_state
                WHERE trade_date >= (CURRENT_DATE - INTERVAL '1 year')
                  AND volume_915 IS NOT NULL
                  AND prev_close > 0
                GROUP BY symbol
                HAVING COUNT(*) >= 60
            )
            WHERE avg_turnover < ?
            """,
            [MIN_AVG_TURNOVER],
        ).fetchall()
        return {r[0] for r in rows if r}
    except Exception as e:
        logger.warning("Illiquid detection query failed: %s", e)
        return set()


def check_stale() -> None:
    """Detect short-history and illiquid symbols and record them in data_quality_issues."""
    from db.duckdb import get_db

    db = get_db()
    db.ensure_data_quality_table()

    print("Detecting short-history symbols...")
    short = detect_short_history()
    if short:
        n = db.upsert_data_quality_issues(
            sorted(short),
            "SHORT_HISTORY",
            f"Fewer than {MIN_HISTORY_DAYS} trading days in market_day_state",
        )
    else:
        n = 0
    active_short = db.deactivate_data_quality_issue("SHORT_HISTORY", keep_symbols=sorted(short))
    if short:
        print(f"  SHORT_HISTORY: {active_short} active, {n} upserted")
    else:
        print("  SHORT_HISTORY: none found")

    print("Detecting illiquid symbols...")
    illiquid = detect_illiquid()
    if illiquid:
        n = db.upsert_data_quality_issues(
            sorted(illiquid),
            "ILLIQUID",
            f"Avg 9:15 turnover < ₹{MIN_AVG_TURNOVER:,} over last year",
        )
    else:
        n = 0
    active_illiquid = db.deactivate_data_quality_issue("ILLIQUID", keep_symbols=sorted(illiquid))
    if illiquid:
        print(f"  ILLIQUID: {active_illiquid} active, {n} upserted")
    else:
        print("  ILLIQUID: none found")

    total = active_short + active_illiquid
    print(f"\nTotal: {total} data quality issues flagged.")
    print("View in dashboard: /data_quality")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect and purge dead (delisted) symbols from parquet and DuckDB"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Preview what would be purged")
    group.add_argument("--purge", action="store_true", help="Execute purge (requires --confirm)")
    group.add_argument("--list-dead", action="store_true", help="Print dead symbol names")
    group.add_argument(
        "--check-stale",
        action="store_true",
        help="Flag short-history and illiquid symbols in data_quality_issues",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required with --purge to confirm deletion",
    )
    return parser


@command_lock("runtime-writer", detail="data hygiene purge")
def main() -> int:
    configure_windows_stdio(line_buffering=True, write_through=True)
    parser = build_parser()
    args = parser.parse_args()

    start = time.time()

    if args.check_stale:
        check_stale()
        elapsed = time.time() - start
        print(f"\nTotal time: {elapsed:.1f}s")
        return 0

    dead = detect_dead_symbols()

    if args.list_dead:
        list_dead(dead)
        return 0

    if args.dry_run:
        dry_run(dead)
        return 0

    if args.purge:
        if not args.confirm:
            parser.error("--purge requires --confirm to execute deletion.")
        purge(dead)
        elapsed = time.time() - start
        print(f"\nTotal time: {elapsed:.1f}s")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
