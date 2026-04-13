from __future__ import annotations

import argparse
from pathlib import Path

from engine.cli_setup import configure_windows_stdio
from engine.kite_ingestion import (
    KiteIngestionError,
    KiteIngestionRequest,
    UniverseMode,
    compact_daily_overlays,
    parse_symbols_csv,
    parse_symbols_file,
    refresh_instrument_master,
    refresh_runtime_tables,
    resolve_date_window,
    resolve_missing_ingest_symbols,
    resolve_target_symbols,
    run_ingestion,
    summarize_result,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh Kite instruments and ingest daily or 5-minute historical candles into local parquet"
    )
    parser.add_argument(
        "--refresh-instruments",
        action="store_true",
        help="Refresh the cached Kite instrument master CSV and exit",
    )
    parser.add_argument(
        "--compact-daily",
        action="store_true",
        help=(
            "Merge daily kite.parquet overlays into all.parquet for the resolved symbols "
            "and delete the overlay files."
        ),
    )
    parser.add_argument(
        "--exchange", default="NSE", help="Exchange code for instrument refresh/load"
    )

    sym_group = parser.add_mutually_exclusive_group()
    sym_group.add_argument(
        "--symbols",
        default=None,
        help=(
            "Comma-separated symbol list. Bypasses universe resolution and ingests exactly "
            "the requested symbols (e.g. RELIANCE,TCS,INFY)."
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
            "Auto-detect tradeable symbols with no local parquet data and ingest them. "
            "Computes: tradeable_set - parquet_set. "
            "Use with --from/--to to specify the date window to backfill."
        ),
    )

    parser.add_argument("--today", action="store_true", help="Use today in Asia/Kolkata")
    parser.add_argument("--date", default=None, help="Single trading date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="start_date", default=None, help="Range start (YYYY-MM-DD)")
    parser.add_argument("--to", dest="end_date", default=None, help="Range end (YYYY-MM-DD)")
    parser.add_argument(
        "--5min",
        dest="five_min",
        action="store_true",
        help="Ingest 5-minute candles instead of daily candles",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume from the last checkpoint file"
    )
    parser.add_argument(
        "--checkpoint-file",
        default=None,
        help="Optional explicit checkpoint JSON path",
    )
    parser.add_argument(
        "--save-raw",
        action="store_true",
        help="Save normalized raw Kite response snapshots to data/raw/kite/",
    )
    parser.add_argument(
        "--update-features",
        action="store_true",
        help="Run a full local runtime-table rebuild after ingestion completes",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=60,
        help="5-minute ingestion chunk size in calendar days (default: 60)",
    )
    parser.add_argument(
        "--daily-chunk-days",
        type=int,
        default=2000,
        help="Daily ingestion chunk size in calendar days (default: 2000 ≈ 1370 trading days, safely under Kite's 2000-candle limit)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print progress every N processed symbols (default: 25, 0 disables periodic progress)",
    )
    parser.add_argument(
        "--universe",
        choices=["local-first", "current-master"],
        default="local-first",
        help=(
            "Symbol universe source. "
            "'local-first' (default): symbols with existing parquet, filtered to tradeable. "
            "'current-master': all tradeable symbols in the current Kite instrument master, "
            "regardless of local parquet state — use this to backfill missing symbols."
        ),
    )
    parser.add_argument(
        "--no-filter-tradeable",
        action="store_true",
        help="Include all parquet symbols even if they are not in the current instrument master",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip symbols already ingested for the target date (check local parquet max date)",
    )
    return parser


def main() -> int:
    configure_windows_stdio(line_buffering=True, write_through=True)
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.refresh_instruments:
            out_path = refresh_instrument_master(exchange=args.exchange)
            print(f"Instrument master refreshed: {out_path}")
            return 0

        if args.compact_daily:
            if args.missing:
                raise KiteIngestionError("--compact-daily does not support --missing.")
            explicit_symbols: list[str] = []
            if args.symbols:
                explicit_symbols = parse_symbols_csv(args.symbols)
            elif args.symbols_file:
                explicit_symbols = parse_symbols_file(args.symbols_file)
            requested_symbols = resolve_target_symbols(
                explicit_symbols=explicit_symbols or None,
                exchange=args.exchange,
                tradeable_only=not args.no_filter_tradeable,
                universe=args.universe,
            )
            if not requested_symbols:
                raise KiteIngestionError("No symbols resolved for daily overlay compaction.")
            result = compact_daily_overlays(requested_symbols)
            print(
                "Daily overlay compaction finished: "
                f"compacted={len(result.compacted_symbols)} "
                f"skipped={len(result.skipped_symbols)} "
                f"rows_written={result.rows_written}"
            )
            return 0

        start_date, end_date = resolve_date_window(
            today=args.today,
            one_date=args.date,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        # Resolve symbol list — explicit symbols bypass universe resolution entirely
        explicit_symbols: list[str] = []
        if args.symbols:
            explicit_symbols = parse_symbols_csv(args.symbols)
        elif args.symbols_file:
            explicit_symbols = parse_symbols_file(args.symbols_file)
        elif args.missing:
            mode = "5min" if args.five_min else "daily"
            explicit_symbols = resolve_missing_ingest_symbols(exchange=args.exchange, mode=mode)
            if not explicit_symbols:
                print(
                    "No missing symbols detected — all tradeable symbols already have local parquet."
                )
                return 0
            print(f"Missing symbols detected: {len(explicit_symbols)}")

        universe: UniverseMode = args.universe
        requested_symbols = resolve_target_symbols(
            explicit_symbols=explicit_symbols or None,
            exchange=args.exchange,
            tradeable_only=not args.no_filter_tradeable,
            universe=universe,
        )
        if not requested_symbols:
            raise KiteIngestionError("No symbols resolved for ingestion.")

        request = KiteIngestionRequest(
            mode="5min" if args.five_min else "daily",
            start_date=start_date,
            end_date=end_date,
            exchange=args.exchange.upper(),
            symbols=requested_symbols,
            save_raw=args.save_raw,
            resume=args.resume,
            skip_existing=args.skip_existing,
            checkpoint_file=None
            if not args.checkpoint_file
            else Path(args.checkpoint_file).expanduser(),
            five_min_chunk_days=args.chunk_days,
            daily_chunk_days=args.daily_chunk_days,
            universe=universe,
        )
        print(
            f"Starting Kite {request.mode} ingestion for {len(requested_symbols)} symbols "
            f"from {start_date.isoformat()} to {end_date.isoformat()} "
            f"(exchange={request.exchange}, resume={request.resume}, save_raw={request.save_raw})"
        )

        def _print_progress(event: dict[str, object]) -> None:
            status = str(event.get("status"))
            processed = int(event.get("processed_count", 0) or 0)
            total = int(event.get("total_processable", 0) or 0)
            completed = int(event.get("completed_count", 0) or 0)
            rows_written = int(event.get("rows_written", 0) or 0)
            missing_count = int(event.get("missing_instruments_count", 0) or 0)
            errors_count = int(event.get("errors_count", 0) or 0)
            elapsed = float(event.get("elapsed_sec", 0.0) or 0.0)
            symbol = event.get("symbol")
            symbol_txt = f" symbol={symbol}" if symbol else ""

            if status == "start":
                print(
                    f"[KITE] processable_symbols={total} missing_instruments={missing_count} "
                    f"checkpoint={event.get('checkpoint_path')}"
                )
                return

            if status == "error":
                print(
                    f"[KITE] {processed}/{total}{symbol_txt} ERROR errors={errors_count} "
                    f"elapsed={elapsed:.1f}s message={event.get('error')}"
                )
                return

            if status == "checkpoint_flushed":
                print(
                    f"[KITE] persisted {request.mode} checkpoint "
                    f"after {processed} processed symbols: {event.get('checkpoint_path')} "
                    f"(completed={completed} rows={rows_written} errors={errors_count} elapsed={elapsed:.1f}s)"
                )
                return

            if status == "finished":
                print(
                    f"[KITE] finished completed={completed}/{total} rows={rows_written} "
                    f"errors={errors_count} missing_instruments={missing_count} elapsed={elapsed:.1f}s"
                )
                return

            if status == "completed":
                progress_every = int(args.progress_every)
                if progress_every <= 0:
                    return
                if processed % progress_every != 0 and processed != total:
                    return
                pct = (processed / total * 100.0) if total else 100.0
                print(
                    f"[KITE] {processed}/{total} ({pct:.1f}%) completed={completed} rows={rows_written} "
                    f"errors={errors_count} missing_instruments={missing_count} elapsed={elapsed:.1f}s"
                )

        result = run_ingestion(request, progress_hook=_print_progress)
        print()
        print(summarize_result(result))

        if args.update_features and not result.errors and not result.missing_instruments:
            print("\nRebuilding local DuckDB runtime tables from parquet...")
            refresh_runtime_tables(force=True)

        if result.errors or result.missing_instruments:
            return 1
        return 0
    except KiteIngestionError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    raise SystemExit(main())
