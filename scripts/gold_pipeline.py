"""Bronze/Silver/Gold workflow utilities for fast, repeatable backtests."""

from __future__ import annotations

import argparse
import time

from db.duckdb import MarketDB, get_db
from engine.cli_setup import configure_windows_stdio
from engine.constants import PUBLIC_STRATEGIES, preview_list
from engine.cpr_atr_strategy import BacktestParams, CPRATRBacktest
from engine.progress import append_progress_event

configure_windows_stdio()


def _missing_symbol_coverage(db: MarketDB, table: str, symbols: list[str]) -> list[str]:
    if not symbols:
        return []
    if not db._table_exists(table):
        return sorted(set(symbols))

    placeholders = ",".join("?" for _ in symbols)
    rows = db.execute_sql(
        f"SELECT DISTINCT symbol FROM {table} WHERE symbol IN ({placeholders})",
        symbols,
    ).fetchall()
    covered = {r[0] for r in rows if r and r[0]}
    return sorted([s for s in symbols if s not in covered])


def _runtime_coverage(db: MarketDB, symbols: list[str]) -> tuple[list[str], list[str]]:
    state_missing = _missing_symbol_coverage(db, "market_day_state", symbols)
    pack_missing = _missing_symbol_coverage(db, "intraday_day_pack", symbols)
    return state_missing, pack_missing


def _print_runtime_coverage(
    symbols: list[str],
    state_missing: list[str],
    pack_missing: list[str],
) -> None:
    total = len(symbols)
    state_cov = total - len(state_missing)
    pack_cov = total - len(pack_missing)
    print(
        "Runtime coverage: "
        f"market_day_state={state_cov}/{total} "
        f"intraday_day_pack={pack_cov}/{total}"
    )
    if state_missing:
        print(
            "Missing market_day_state symbols "
            f"({len(state_missing)}): {preview_list(state_missing, limit=20)}"
        )
    if pack_missing:
        print(
            "Missing intraday_day_pack symbols "
            f"({len(pack_missing)}): {preview_list(pack_missing, limit=20)}"
        )


def _runtime_precondition_message(name: str) -> str:
    return (
        f"Runtime coverage is incomplete for universe '{name}'. "
        "Materialize runtime tables explicitly before benchmark/backtest: "
        "`uv run pivot-build --table pack --refresh-since <YYYY-MM-DD>` for recent catch-up "
        "or `uv run pivot-build --force --full-history --staged-full-rebuild` for a full rebuild, "
        "then refresh quality with "
        "`uv run pivot-data-quality --refresh --limit 50`."
    )


def cmd_prepare(args: argparse.Namespace) -> None:
    db = get_db()

    dq = db.refresh_data_quality_issues()
    publish_replica = getattr(db, "_publish_replica", None)
    if callable(publish_replica):
        publish_replica(force=False)
    print(
        "Data quality refreshed: "
        f"missing_5min={dq.get('missing_5min', 0)} active_issues={dq.get('active_issues', 0)}"
    )

    symbols = db.get_liquid_symbols(
        args.start,
        args.end,
        limit=args.universe_size,
        min_price=args.min_price,
    )
    if not symbols:
        raise RuntimeError(
            "No symbols resolved for gold universe. Check parquet/data_quality state."
        )

    source = f"{args.source}:top_{args.universe_size}"
    saved = db.upsert_universe(
        args.name,
        symbols,
        start_date=args.start,
        end_date=args.end,
        source=source,
        notes=args.notes or "",
    )
    print(f"Saved universe '{args.name}' with {saved} symbols: {preview_list(symbols)}")

    state_missing, pack_missing = _runtime_coverage(db, symbols)
    _print_runtime_coverage(symbols, state_missing, pack_missing)
    if state_missing or pack_missing:
        print(
            "Universe persisted, but runtime coverage is incomplete. No auto-rebuild was triggered."
        )
        print(_runtime_precondition_message(args.name))
    else:
        print("Universe is runtime-ready for strict benchmark runs.")


def cmd_status(args: argparse.Namespace) -> None:
    db = get_db()
    rows = db.list_universes()
    if args.name:
        rows = [r for r in rows if r["name"] == args.name]

    if not rows:
        print("No saved universes found.")
        return

    print(f"{'Name':<20} {'Symbols':>7} {'Start':<12} {'End':<12} {'Source':<24}")
    print("-" * 80)
    for row in rows:
        print(
            f"{row['name']:<20} {row['symbol_count']:>7} "
            f"{(row['start_date'] or ''):<12} {(row['end_date'] or ''):<12} "
            f"{(row['source'] or ''):<24}"
        )

    if args.show_symbols:
        for row in rows:
            symbols = db.get_universe_symbols(str(row["name"]))
            print(f"\n[{row['name']}] {len(symbols)} symbols")
            print(preview_list(symbols, limit=30))


def cmd_benchmark(args: argparse.Namespace) -> None:
    db = get_db()
    symbols = db.get_universe_symbols(args.name)
    if not symbols:
        raise RuntimeError(
            f"Universe '{args.name}' not found or empty. Run `uv run pivot-gold prepare ...` first."
        )

    state_missing, pack_missing = _runtime_coverage(db, symbols)
    _print_runtime_coverage(symbols, state_missing, pack_missing)
    if state_missing or pack_missing:
        raise RuntimeError(_runtime_precondition_message(args.name))

    params = BacktestParams(
        strategy=args.strategy,
        skip_rvol_check=args.skip_rvol,
    )
    bt = CPRATRBacktest(params=params, db=db)

    print(
        f"Benchmark start: universe={args.name} symbols={len(symbols)} "
        f"window={args.start}..{args.end} strategy={args.strategy}"
    )

    if args.progress_file:
        append_progress_event(
            args.progress_file,
            {
                "event": "gold_benchmark_start",
                "name": args.name,
                "symbol_count": len(symbols),
                "strategy": args.strategy,
            },
        )

    t0 = time.time()
    progress_hook = (
        (lambda row: append_progress_event(args.progress_file, row)) if args.progress_file else None
    )
    result = bt.run(
        symbols=symbols,
        start=args.start,
        end=args.end,
        verbose=not args.quiet,
        progress_hook=progress_hook,
    )
    elapsed = time.time() - t0

    if args.progress_file:
        append_progress_event(
            args.progress_file,
            {
                "event": "gold_benchmark_complete",
                "name": args.name,
                "run_id": result.run_id,
                "elapsed_s": round(elapsed, 4),
                "symbol_count": len(symbols),
            },
        )

    if args.save:
        n = result.save_to_db(db)
        print(f"Saved {n} trades to backtest_results")
        if args.progress_file:
            append_progress_event(
                args.progress_file,
                {
                    "event": "gold_benchmark_save",
                    "run_id": result.run_id,
                    "saved_trades": n,
                },
            )

    if not args.quiet:
        print(result.summary())

    target_seconds = args.target_minutes * 60.0
    gate = "PASS" if elapsed <= target_seconds else "FAIL"
    print(
        "Benchmark result: "
        f"elapsed={elapsed:.2f}s target={args.target_minutes:.2f}m "
        f"trades={result.df.height} gate={gate}"
    )

    if gate == "FAIL" and args.fail_on_breach:
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze/Silver/Gold backtest workflow")
    sub = parser.add_subparsers(dest="command", required=True)

    p_prepare = sub.add_parser(
        "prepare",
        help="Create/update a gold universe and validate runtime coverage",
    )
    p_prepare.add_argument("--name", default="gold_51", help="Universe name (default: gold_51)")
    p_prepare.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p_prepare.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    p_prepare.add_argument("--universe-size", type=int, default=51, help="Top N liquid symbols")
    p_prepare.add_argument("--min-price", type=float, default=0.0, help="Min close price filter")
    p_prepare.add_argument("--source", default="liquidity_rank", help="Universe source label")
    p_prepare.add_argument("--notes", default="", help="Optional notes")
    p_prepare.set_defaults(func=cmd_prepare)

    p_status = sub.add_parser("status", help="Show saved universes")
    p_status.add_argument("--name", default=None, help="Filter by universe name")
    p_status.add_argument("--show-symbols", action="store_true", help="Print symbol previews")
    p_status.set_defaults(func=cmd_status)

    p_bench = sub.add_parser("benchmark", help="Run backtest benchmark on saved universe")
    p_bench.add_argument("--name", required=True, help="Saved universe name")
    p_bench.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p_bench.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    p_bench.add_argument(
        "--strategy",
        choices=PUBLIC_STRATEGIES,
        default=PUBLIC_STRATEGIES[0],
        help="Strategy to benchmark (default: CPR_LEVELS)",
    )
    p_bench.add_argument(
        "--skip-rvol", action="store_true", help="Disable RVOL gate for speed tests"
    )
    p_bench.add_argument("--save", action="store_true", help="Persist trade rows")
    p_bench.add_argument(
        "--target-minutes", type=float, default=10.0, help="Benchmark gate threshold"
    )
    p_bench.add_argument(
        "--fail-on-breach", action="store_true", help="Exit non-zero if gate fails"
    )
    p_bench.add_argument(
        "--progress-file",
        default=None,
        help="Optional NDJSON path for benchmark progress heartbeats",
    )
    p_bench.add_argument("--quiet", action="store_true", help="Suppress detailed progress/summary")
    p_bench.set_defaults(func=cmd_benchmark)

    args = parser.parse_args()
    args.func(args)


if __name__ in {"__main__", "__mp_main__"}:
    main()
