"""
Build runtime DuckDB tables from Parquet data.

Default (no --table) builds all runtime tables for all symbols:
  - cpr_daily
  - atr_intraday
  - cpr_thresholds
  - virgin_cpr_flags
  - or_daily
  - market_day_state
  - strategy_day_state
  - intraday_day_pack
  - dataset_meta

Usage:
    doppler run -- uv run pivot-build                                # build if not exists
    doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild
                                                                  # staged full-history rebuild only
    doppler run -- uv run pivot-build --table state                  # rebuild only market_day_state
    doppler run -- uv run pivot-build --refresh-since 2026-03-21     # incremental ALL tables
    doppler run -- uv run pivot-build --refresh-date 2026-03-21      # exact-day ALL tables
    doppler run -- uv run pivot-build --table cpr --refresh-since 2026-03-21
                                                                  # incremental cpr_daily only
    doppler run -- uv run pivot-build --table pack --refresh-since 2026-03-21
                                                                  # incremental intraday_day_pack refresh
    doppler run -- uv run pivot-build --table pack --force --allow-full-pack-rebuild
                                                                  # full intraday_day_pack rebuild
    doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild \
      --allow-full-history-rebuild
                                                                  # explicit full-history staged rebuild
    doppler run -- uv run pivot-build --table pack --universe-name gold_51 --force
    doppler run -- uv run pivot-build --table pack --symbols SBIN,TCS --force
    doppler run -- uv run pivot-build --table pack --since 2026-03-21
    doppler run -- uv run pivot-build --status                       # show table stats

Symbol-level rebuild (state + strategy + pack for specific symbols only):
    doppler run -- uv run pivot-build --symbols RELIANCE,TCS,INFY
    doppler run -- uv run pivot-build --symbols-file path/to/symbols.txt
    doppler run -- uv run pivot-build --missing          # auto-detect: v_daily NOT IN market_day_state
    doppler run -- uv run pivot-build --missing --table pack   # auto-detect: v_daily NOT IN intraday_day_pack
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager

from db.duckdb import MarketDB, get_db
from engine.cli_setup import configure_windows_stdio
from engine.command_lock import command_lock
from engine.constants import normalize_symbol
from engine.kite_ingestion import ensure_repo_process_preflight

configure_windows_stdio(line_buffering=True, write_through=True)

SYMBOL_SCOPED_TABLES = {"atr", "thresholds", "virgin", "or", "state", "strategy", "pack"}

# Tables rebuilt for symbol-level operations (--symbols / --symbols-file / --missing without --table).
# Maps to NSE project's feat_daily_core + feat_intraday_core + feat_2lynch_derived.
SYMBOL_REBUILD_TABLES = ["state", "strategy", "pack"]

FULL_REBUILD_ORDER = [
    "cpr",
    "atr",
    "thresholds",
    "or",
    "state",
    "strategy",
    "pack",
    "virgin",
    "meta",
]

# Maps --table CLI value to the runtime table used as the "already-built" reference
# when resolving --missing symbols. For symbol-level rebuilds without --table, the
# default is market_day_state.
_MISSING_REFERENCE_TABLE: dict[str, str] = {
    "pack": "intraday_day_pack",
    "strategy": "strategy_day_state",
    "state": "market_day_state",
}


@command_lock("runtime-writer", detail="runtime writer")
def main() -> None:
    parser = argparse.ArgumentParser(description="Build runtime DuckDB tables from Parquet data")
    parser.add_argument(
        "--force", action="store_true", help="Force rebuild even if tables already exist"
    )
    parser.add_argument(
        "--table",
        choices=["cpr", "atr", "thresholds", "virgin", "or", "state", "strategy", "pack", "meta"],
        help="Build only a specific table (default: all runtime tables)",
    )
    parser.add_argument(
        "--status", action="store_true", help="Show current table statistics and exit"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help="Symbol batch size for ATR and intraday_day_pack batched rebuilds (used by --table atr/pack and staged/full builds)",
    )
    parser.add_argument(
        "--pack-lookback",
        type=int,
        default=10,
        help="RVOL lookback days for intraday_day_pack build",
    )
    parser.add_argument(
        "--refresh-since",
        "--since",
        dest="since",
        default=None,
        help=(
            "Incremental build: only insert/replace rows for trade_date >= SINCE (YYYY-MM-DD). "
            "Deletes existing rows for that date range and re-inserts from source data. "
            "Works for ALL runtime tables (cpr, atr, thresholds, or, state, strategy, virgin, pack). "
            "Much faster than --force for monthly data updates. "
            "Preferred alias: --refresh-since. Example: --refresh-since 2025-04-01"
        ),
    )
    parser.add_argument(
        "--refresh-date",
        dest="refresh_date",
        default=None,
        help=(
            "Exact-day incremental build: only refresh rows for one trade date (YYYY-MM-DD). "
            "Uses the same runtime-table refresh path as --refresh-since, but limits the window to that date. "
            "Preferred for same-day DQ / replay parity. "
            "Can be combined with --symbols, --symbols-file, or --missing."
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume an interrupted pack build from where it left off. "
            "Skips symbols already present in intraday_day_pack. "
            "Applies to --table pack or --staged-full-rebuild --resume-from pack."
        ),
    )
    parser.add_argument(
        "--allow-full-pack-rebuild",
        action="store_true",
        help=(
            "Required for a full-universe `--table pack --force` rebuild without --refresh-since/--since. "
            "Use only when you intentionally want to drop and rebuild the entire intraday_day_pack table."
        ),
    )
    parser.add_argument(
        "--allow-full-history-rebuild",
        action="store_true",
        help=(
            "Required for `--force --staged-full-rebuild` when no --refresh-since/--since is supplied. "
            "Use only when you intentionally want to scan full local parquet history. "
            "Prefer --refresh-since for recent data windows."
        ),
    )
    sym_group = parser.add_mutually_exclusive_group()
    sym_group.add_argument(
        "--symbols",
        default=None,
        help=(
            "Comma-separated symbol list. Without a date window, rebuilds state/strategy/pack for "
            "these symbols only (e.g. RELIANCE,TCS,INFY). With --refresh-since or --refresh-date, "
            "refreshes all runtime tables for just those symbols."
        ),
    )
    sym_group.add_argument(
        "--symbols-file",
        dest="symbols_file",
        default=None,
        metavar="PATH",
        help="Path to a text file with one symbol per line (blank lines and '#' comments ignored).",
    )
    sym_group.add_argument(
        "--missing",
        action="store_true",
        help=(
            "Auto-detect symbols in v_daily (parquet) that are absent from market_day_state "
            "and rebuild state/strategy/pack for them. Safe to re-run: deletes old rows for "
            "the target symbols then re-inserts."
        ),
    )
    parser.add_argument(
        "--universe-name",
        default=None,
        help=(
            "Use symbols from a saved backtest universe for symbol-scoped table builds "
            "(e.g. gold_51). Requires --table."
        ),
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Acknowledge that a no-table --force rebuild scans all local parquet history, not just newly ingested dates.",
    )
    parser.add_argument(
        "--staged-full-rebuild",
        action="store_true",
        help="Run a resumable table-by-table full-history rebuild instead of one build_all call.",
    )
    parser.add_argument(
        "--resume-from",
        choices=FULL_REBUILD_ORDER,
        default=None,
        help="When used with --staged-full-rebuild, resume from this table in the staged rebuild order.",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=None,
        help="Override DUCKDB_THREADS for this command only.",
    )
    parser.add_argument(
        "--duckdb-max-memory",
        default=None,
        help="Override DUCKDB_MAX_MEMORY for this command only (example: 24GB).",
    )
    parser.add_argument(
        "--skip-status",
        action="store_true",
        help=(
            "Skip the final table row-count/status summary. Use for orchestrated EOD runs where "
            "a separate readiness gate verifies the result; avoids expensive COUNT(DISTINCT) scans."
        ),
    )
    parser.add_argument(
        "--defer-replica-sync",
        action="store_true",
        help=(
            "Defer market replica publication for this build command. Use only when an orchestrator "
            "runs an explicit sync/verify stage after the build."
        ),
    )
    args = parser.parse_args()

    if not args.status:
        ensure_repo_process_preflight("build")

    if args.universe_name and (args.symbols or args.symbols_file or args.missing):
        parser.error(
            "--universe-name cannot be combined with --symbols, --symbols-file, or --missing."
        )
    if args.universe_name and args.table is None:
        parser.error("--universe-name requires --table.")
    if (
        args.table
        and (args.symbols or args.symbols_file or args.missing or args.universe_name)
        and args.table not in SYMBOL_SCOPED_TABLES
    ):
        parser.error(
            f"--table {args.table!r} does not support symbol scoping. "
            f"Use one of: {', '.join(sorted(SYMBOL_SCOPED_TABLES))}."
        )
    resume_pack_mode = args.staged_full_rebuild and args.resume_from == "pack"
    if args.resume and not (args.table == "pack" or resume_pack_mode):
        parser.error("--resume only applies to --table pack or --resume-from pack.")
    if args.resume and args.force and not resume_pack_mode:
        parser.error(
            "--resume and --force are mutually exclusive. Use --resume to continue an interrupted build."
        )
    if args.resume_from and not args.staged_full_rebuild:
        parser.error("--resume-from requires --staged-full-rebuild.")
    if args.staged_full_rebuild and args.table:
        parser.error("Use either --table or --staged-full-rebuild, not both.")
    if args.staged_full_rebuild and (
        args.symbols or args.symbols_file or args.missing or args.universe_name
    ):
        parser.error("Staged full rebuild does not support symbol scoping.")
    if args.allow_full_pack_rebuild and args.table != "pack":
        parser.error("--allow-full-pack-rebuild only applies to --table pack.")
    if args.allow_full_pack_rebuild and args.since:
        parser.error("--allow-full-pack-rebuild cannot be combined with --refresh-since/--since.")
    if args.refresh_date and args.force:
        parser.error(
            "--refresh-date is a bounded incremental refresh and cannot be combined with --force."
        )
    if args.refresh_date and args.staged_full_rebuild:
        parser.error("--refresh-date cannot be combined with --staged-full-rebuild.")
    if (
        args.staged_full_rebuild
        and args.force
        and args.since is None
        and not args.allow_full_history_rebuild
    ):
        parser.error(
            "Full-history staged rebuilds are expensive and should be deliberate. "
            "For recent data windows use --refresh-since YYYY-MM-DD. "
            "If you intentionally want a full-history rebuild, add --allow-full-history-rebuild."
        )
    if (
        args.table == "pack"
        and args.force
        and args.since is None
        and not args.resume
        and args.symbols is None
        and args.symbols_file is None
        and not args.missing
        and args.universe_name is None
        and not args.allow_full_pack_rebuild
    ):
        parser.error(
            "Full-universe `--table pack --force` rebuilds are destructive and expensive. "
            "For recent parquet updates use `--since YYYY-MM-DD`. "
            "If you intentionally want a full pack rebuild, add --allow-full-pack-rebuild."
        )
    _symbol_level = bool(args.symbols or args.symbols_file or args.missing)
    if (args.force and args.table is None) and not args.full_history and not _symbol_level:
        parser.error(
            "Full-history runtime rebuild requires --full-history. "
            "Use table-scoped rebuilds for targeted work, or add --staged-full-rebuild for a resumable full rebuild."
        )
    if (args.force and args.table is None) and not args.staged_full_rebuild and not _symbol_level:
        parser.error(
            "Non-resumable full-history rebuilds are disabled. "
            "Use --staged-full-rebuild for any no-table --force rebuild."
        )

    _apply_duckdb_runtime_overrides(args)

    db = get_db()
    symbols = _resolve_symbols(parser, db, args.symbols, args.symbols_file, args.universe_name)

    # --missing: auto-detect symbols in parquet but absent from the target runtime table
    if args.missing:
        symbols = _detect_missing_symbols(db, table=args.table)
        if not symbols:
            _ref = _MISSING_REFERENCE_TABLE.get(args.table or "", "market_day_state")
            print(f"No missing symbols detected — all parquet symbols are present in {_ref}.")
            return
        print(f"Missing symbols detected: {len(symbols)}")

    # --- Tradeable-only gate for symbol-scoped tables ---
    if symbols is None and args.table in SYMBOL_SCOPED_TABLES:
        from engine.kite_ingestion import tradeable_symbols

        tradeable = tradeable_symbols()
        if tradeable is not None:
            all_parquet = db.get_available_symbols()
            dead = set(all_parquet) - tradeable
            if dead:
                symbols = sorted(tradeable & set(all_parquet))
                print(
                    f"Tradeable filter: skipping {len(dead)} dead symbols, "
                    f"building {len(symbols)} tradeable symbols"
                )

    if args.status:
        _show_status(db)
        return

    start = time.time()
    refresh_since = args.since or args.refresh_date
    refresh_until = args.refresh_date

    with _replica_sync_scope(db, defer=args.defer_replica_sync):
        if args.table:
            _build_single(
                db,
                args.table,
                force=args.force,
                batch_size=args.batch_size,
                pack_lookback=args.pack_lookback,
                since_date=refresh_since,
                until_date=refresh_until,
                symbols=symbols,
                resume=args.resume,
            )
        elif symbols is not None and not args.staged_full_rebuild and refresh_since is None:
            # Symbol-level rebuild: no --table given, but symbols were resolved via
            # --symbols / --symbols-file / --missing → rebuild core tables for those symbols.
            _run_symbol_rebuild(
                db,
                symbols,
                batch_size=args.batch_size,
                pack_lookback=args.pack_lookback,
                since_date=refresh_since,
            )
        elif symbols is not None and not args.staged_full_rebuild:
            db.build_all(
                force=args.force,
                symbols=symbols,
                atr_batch_size=args.batch_size,
                pack_batch_size=args.batch_size,
                pack_rvol_lookback_days=args.pack_lookback,
                since_date=refresh_since,
                until_date=refresh_until,
            )
        elif args.staged_full_rebuild:
            _run_staged_full_rebuild(
                db,
                force=args.force,
                batch_size=args.batch_size,
                pack_lookback=args.pack_lookback,
                since_date=refresh_since,
                resume_from=args.resume_from,
                resume_pack=args.resume,
            )
        else:
            print("=" * 60)
            print(" Building ALL runtime tables from full local parquet history")
            print("=" * 60)
            db.build_all(
                force=args.force,
                symbols=None,
                atr_batch_size=args.batch_size,
                pack_batch_size=args.batch_size,
                pack_rvol_lookback_days=args.pack_lookback,
                since_date=refresh_since,
                until_date=refresh_until,
            )

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s")
    if args.skip_status:
        print("Skipped final table status summary (--skip-status).")
    else:
        print()
        _show_status(db)


def _build_single(
    db: MarketDB,
    table: str,
    force: bool,
    batch_size: int,
    pack_lookback: int,
    since_date: str | None = None,
    until_date: str | None = None,
    symbols: list[str] | None = None,
    resume: bool = False,
) -> None:
    """Build a single table by name."""
    if symbols:
        print(
            f"Symbol scope: {len(symbols)} symbols "
            f"({', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''})"
        )
    if table == "cpr":
        db.build_cpr_table(
            force=force,
            symbols=symbols,
            since_date=since_date,
            until_date=until_date,
            # next_trading_date auto-detected inside build_cpr_table when since==until and no data
        )
    elif table == "atr":
        db.build_atr_table(
            force=force,
            symbols=symbols,
            batch_size=batch_size,
            since_date=since_date,
            until_date=until_date,
        )
    elif table == "thresholds":
        db.build_cpr_thresholds(
            force=force, symbols=symbols, since_date=since_date, until_date=until_date
        )
    elif table == "virgin":
        db.build_virgin_cpr_flags(
            force=force, symbols=symbols, since_date=since_date, until_date=until_date
        )
    elif table == "or":
        db.build_or_table(
            force=force, symbols=symbols, since_date=since_date, until_date=until_date
        )
    elif table == "state":
        db.build_market_day_state(
            force=force, symbols=symbols, since_date=since_date, until_date=until_date
        )
    elif table == "strategy":
        db.build_strategy_day_state(
            force=force, symbols=symbols, since_date=since_date, until_date=until_date
        )
    elif table == "pack":
        db.build_intraday_day_pack(
            force=force,
            symbols=symbols,
            rvol_lookback_days=pack_lookback,
            batch_size=batch_size,
            since_date=since_date,
            until_date=until_date,
            resume=resume,
        )
    elif table == "meta":
        db._build_dataset_meta()


@contextmanager
def _replica_sync_scope(db: MarketDB, *, defer: bool) -> Iterator[None]:
    """Optionally batch market replica publication for an orchestrated build."""
    if defer:
        db._begin_replica_batch()
    try:
        yield
    finally:
        if defer:
            db._end_replica_batch()


def _run_staged_full_rebuild(
    db: MarketDB,
    *,
    force: bool,
    batch_size: int,
    pack_lookback: int,
    since_date: str | None,
    resume_from: str | None,
    resume_pack: bool = False,
) -> None:
    if not force:
        raise SystemExit("--staged-full-rebuild requires --force.")

    start_index = FULL_REBUILD_ORDER.index(resume_from) if resume_from else 0
    selected = FULL_REBUILD_ORDER[start_index:]
    print("=" * 60)
    print(" Staged Full-History Runtime Rebuild")
    print("=" * 60)
    print(f"Tables: {', '.join(selected)}")
    if resume_from:
        print(f"Resuming from: {resume_from}")

    for idx, table in enumerate(selected, start=1):
        table_started = time.time()
        print(f"\n[{idx}/{len(selected)}] rebuilding {table}...", flush=True)
        force_table = True
        resume_table = False
        if table == "pack" and resume_pack and resume_from == "pack":
            force_table = False
            resume_table = True
        _build_single(
            db,
            table,
            force=force_table,
            batch_size=batch_size,
            pack_lookback=pack_lookback,
            since_date=since_date,
            symbols=None,
            resume=resume_table,
        )
        print(
            f"[{idx}/{len(selected)}] {table} completed in {time.time() - table_started:.1f}s",
            flush=True,
        )


def _apply_duckdb_runtime_overrides(args: argparse.Namespace) -> None:
    if args.duckdb_threads is not None:
        os.environ["DUCKDB_THREADS"] = str(args.duckdb_threads)
    if args.duckdb_max_memory:
        os.environ["DUCKDB_MAX_MEMORY"] = str(args.duckdb_max_memory)


def _resolve_symbols(
    parser: argparse.ArgumentParser,
    db: MarketDB,
    symbols_csv: str | None,
    symbols_file: str | None,
    universe_name: str | None,
) -> list[str] | None:
    """Resolve optional symbol scope from CLI args."""
    if symbols_csv:
        symbols = [normalize_symbol(s) for s in symbols_csv.split(",") if s.strip()]
        if not symbols:
            parser.error("--symbols provided but no valid symbols were parsed.")
        return symbols
    if symbols_file:
        from engine.kite_ingestion import parse_symbols_file

        try:
            symbols = parse_symbols_file(symbols_file)
        except Exception as exc:
            parser.error(str(exc))
        if not symbols:
            parser.error(f"--symbols-file {symbols_file!r} contained no valid symbols.")
        return symbols
    if universe_name:
        symbols = db.get_universe_symbols(universe_name)
        if not symbols:
            parser.error(
                f"Universe '{universe_name}' not found or empty. "
                "Run `uv run pivot-gold prepare --name <name> --start ... --end ...` first."
            )
        return symbols
    return None


def _detect_missing_symbols(db: MarketDB, table: str | None = None) -> list[str]:
    """Return symbols present in v_daily (parquet) but absent from the target runtime table.

    The reference table is selected based on `table`:
      - "pack"     → intraday_day_pack
      - "strategy" → strategy_day_state
      - "state" / None → market_day_state (default)

    Falls back to all parquet symbols if the reference table does not exist yet
    (fresh catalog — safe for first-time builds).
    """
    runtime_table = _MISSING_REFERENCE_TABLE.get(table or "", "market_day_state")
    try:
        rows = db.con.execute(
            f"SELECT DISTINCT symbol FROM v_daily "
            f"WHERE symbol NOT IN (SELECT DISTINCT symbol FROM {runtime_table}) "
            f"ORDER BY symbol"
        ).fetchall()
    except Exception:
        # runtime_table does not exist yet — treat all parquet symbols as missing
        try:
            rows = db.con.execute("SELECT DISTINCT symbol FROM v_daily ORDER BY symbol").fetchall()
        except Exception:
            return []
    return [r[0] for r in rows if r[0]]


def _run_symbol_rebuild(
    db: MarketDB,
    symbols: list[str],
    *,
    batch_size: int,
    pack_lookback: int,
    since_date: str | None,
) -> None:
    """Rebuild state, strategy, and pack for the given symbols only.

    Idempotent: each build function deletes old rows for the target symbols
    then re-inserts recomputed rows. Safe to re-run with the same symbol list.
    Maps to NSE project's feat_daily_core + feat_intraday_core + feat_2lynch_derived.
    """
    print(f"\nSymbol rebuild: {len(symbols)} symbols → {', '.join(SYMBOL_REBUILD_TABLES)}")
    for table in SYMBOL_REBUILD_TABLES:
        t0 = time.time()
        print(f"\n  [{table}]", flush=True)
        _build_single(
            db,
            table,
            force=True,
            batch_size=batch_size,
            pack_lookback=pack_lookback,
            since_date=since_date,
            symbols=symbols,
        )
        print(f"  [{table}] done in {time.time() - t0:.1f}s", flush=True)


def _show_status(db: MarketDB) -> None:
    """Print row counts and symbol coverage for tables."""
    con = db.con
    status = db.get_status()
    row_counts = status.get("tables", {}) if isinstance(status, dict) else {}
    tables = [
        "cpr_daily",
        "atr_intraday",
        "cpr_thresholds",
        "virgin_cpr_flags",
        "or_daily",
        "market_day_state",
        "strategy_day_state",
        "intraday_day_pack",
        "dataset_meta",
        "backtest_results",
        "run_daily_pnl",
        "run_metrics",
    ]
    print("=" * 60)
    print(f" {'Table':<22} {'Rows':>12}  {'Symbols':>8}")
    print("-" * 60)
    for t in tables:
        try:
            row_count = int(row_counts.get(t, 0))
            if t in {"dataset_meta", "run_daily_pnl", "run_metrics"}:
                sym_count = "-"
            else:
                sym_count = str(
                    con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {t}").fetchone()[0]
                )
            print(f" {t:<22} {row_count:>12,}  {sym_count:>8}")
        except Exception as e:
            print(f" {t:<22} {'NOT BUILT':>12}  {'-':>8}  ({e})")
    print("=" * 60)


if __name__ in {"__main__", "__mp_main__"}:
    main()
