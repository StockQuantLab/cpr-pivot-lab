"""
CPR-ATR Backtest CLI entry point.

Before first run, build the static DuckDB tables once:
    doppler run -- uv run pivot-build

Usage:
    doppler run -- uv run pivot-backtest --symbol SBIN --start 2020-01-01 --end 2024-12-31
    doppler run -- uv run pivot-backtest --symbols RELIANCE,TCS,SBIN --start 2020-01-01 --end 2024-12-31
    doppler run -- uv run pivot-backtest --all --start 2020-01-01 --end 2024-12-31


"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
import uuid
from typing import Any

import polars as pl

from db.backtest_db import get_backtest_db
from db.duckdb import get_db
from engine.cli_setup import configure_windows_stdio
from engine.command_lock import command_lock
from engine.constants import CPR_SHIFTS, DIRECTIONS, PUBLIC_STRATEGIES, normalize_symbol
from engine.constants import parse_iso_date as parse_iso_date_str
from engine.cpr_atr_strategy import (
    BacktestParams,
    BacktestResult,
    CPRATRBacktest,
)
from engine.progress import append_progress_event
from engine.strategy_presets import (
    build_strategy_config_from_overrides,
    build_strategy_config_from_preset,
    list_strategy_preset_names,
)

logger = logging.getLogger(__name__)


def _parse_iso_date(value: str, arg_name: str) -> dt.date:
    """Parse YYYY-MM-DD input and raise argparse-style error message on failure."""
    try:
        return dt.date.fromisoformat(parse_iso_date_str(value))
    except ValueError as err:
        raise ValueError(f"{arg_name} must be YYYY-MM-DD (got {value!r})") from err


def _month_end(day: dt.date) -> dt.date:
    """Return last day of month for the provided date."""
    if day.month == 12:
        return dt.date(day.year, 12, 31)
    return dt.date(day.year, day.month + 1, 1) - dt.timedelta(days=1)


def _build_chunks(
    start_date: dt.date, end_date: dt.date, chunk_by: str
) -> list[tuple[str, str, str]]:
    """Return [(chunk_start, chunk_end, label)] for none/year/month execution."""
    if chunk_by == "none":
        return [(start_date.isoformat(), end_date.isoformat(), start_date.isoformat())]

    chunks: list[tuple[str, str, str]] = []
    cursor = start_date
    while cursor <= end_date:
        if chunk_by == "year":
            raw_end = dt.date(cursor.year, 12, 31)
            label = f"{cursor.year}"
        elif chunk_by == "month":
            raw_end = _month_end(cursor)
            label = cursor.strftime("%Y-%m")
        else:
            raise ValueError(f"Unsupported chunk_by={chunk_by!r}")
        chunk_end = min(raw_end, end_date)
        chunks.append((cursor.isoformat(), chunk_end.isoformat(), label))
        cursor = chunk_end + dt.timedelta(days=1)
    return chunks


def _progress_line(percent: float, event: str, message: str) -> None:
    """Print human-readable progress lines with local wall-clock timestamp."""
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"{ts}  [PROGRESS] {percent:5.1f}% [{event}] {message}")


class _BacktestHeartbeat:
    """Throttle progress output for long single-window runs."""

    def __init__(self, total_symbols: int):
        self.total_symbols = max(1, int(total_symbols))
        self.started_at = time.time()
        self.completed_symbols = 0
        self.last_emit_at = self.started_at
        self.last_emit_done = 0

    def handle(self, row: dict[str, object]) -> None:
        event = str(row.get("event", ""))
        if event == "run_start":
            total_symbols = row.get("total_symbols")
            if isinstance(total_symbols, int | float) and int(total_symbols) > 0:
                self.total_symbols = int(total_symbols)
            self.started_at = time.time()
            self.completed_symbols = 0
            self.last_emit_at = self.started_at
            self.last_emit_done = 0
            return

        if event != "symbol_done":
            return

        self.completed_symbols += 1
        done = self.completed_symbols
        total = self.total_symbols
        now = time.time()
        elapsed = max(now - self.started_at, 0.001)
        percent = (done / total) * 100.0

        min_symbol_step = max(1, total // 20)
        if done < total:
            if (done - self.last_emit_done) < min_symbol_step and (now - self.last_emit_at) < 15.0:
                return

        rate = done / elapsed if elapsed > 0 else 0.0
        eta_s = ((total - done) / rate) if rate > 0 else 0.0
        symbol = str(row.get("symbol", ""))
        extra = (
            f"{done}/{total} symbols complete | elapsed={elapsed:.0f}s | "
            f"ETA={eta_s / 60.0:.1f}min | last={symbol}"
        )
        _progress_line(percent, "symbol_done", extra)
        self.last_emit_done = done
        self.last_emit_at = now


def _chunk_return_pct(result: BacktestResult, portfolio_value: float) -> float:
    """Compute chunk return % on shared-portfolio basis."""
    if portfolio_value <= 0:
        return 0.0
    df = result.df
    if df.is_empty():
        return 0.0
    total_pnl = float(df["profit_loss"].sum())
    return (total_pnl / float(portfolio_value)) * 100.0


def _combine_chunk_results(
    *,
    chunk_results: list[BacktestResult],
    params: BacktestParams,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> BacktestResult:
    """Merge chunk result frames for a single end-of-run summary."""
    frames: list[pl.DataFrame] = []
    for res in chunk_results:
        df = res.df
        if not df.is_empty():
            frames.append(df)
    merged = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    run_id = uuid.uuid4().hex[:12]
    if not merged.is_empty():
        if "run_id" in merged.columns:
            merged = merged.with_columns(pl.lit(run_id).alias("run_id"))
        else:
            merged = merged.with_columns(pl.lit(run_id).alias("run_id"))
    return BacktestResult(
        run_id=run_id,
        params=params,
        _loaded_df=merged,
        run_context={
            "start_date": start_date,
            "end_date": end_date,
            "symbols": symbols,
            "run_id": run_id,
        },
    )


def _print_compact_summary(result: BacktestResult, *, elapsed_s: float, symbol_count: int) -> None:
    """Print concise run metrics for high-signal CLI output."""
    df = result.df
    trades = int(df.height)
    if trades <= 0:
        print(
            "Compact summary: "
            f"run_id={result.run_id} elapsed={elapsed_s:.1f}s symbols={symbol_count} trades=0 pnl=₹0.00"
        )
        return

    wins = int((df["profit_loss"] > 0).sum())
    losses = int((df["profit_loss"] < 0).sum())
    win_rate = (wins / trades) * 100.0
    total_pnl = float(df["profit_loss"].sum())
    gross_profit = float(df.filter(pl.col("profit_loss") > 0)["profit_loss"].sum())
    gross_loss = abs(float(df.filter(pl.col("profit_loss") < 0)["profit_loss"].sum()))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    print(
        "Compact summary: "
        f"run_id={result.run_id} "
        f"elapsed={elapsed_s:.1f}s "
        f"symbols={symbol_count} "
        f"trades={trades} "
        f"wins={wins} "
        f"losses={losses} "
        f"wr={win_rate:.2f}% "
        f"pf={profit_factor:.3f} "
        f"pnl=₹{total_pnl:,.2f}"
    )


def build_parser() -> argparse.ArgumentParser:
    """Build and return the backtest argument parser.

    Separated from main() so that sweep schema validation can import it
    without triggering side effects (DB connection, Windows setup, etc.).
    """
    parser = argparse.ArgumentParser(description="CPR-ATR Strategy Backtest")
    parser.add_argument("--symbol", help="Stock symbol (e.g. SBIN). Overrides --all.")
    parser.add_argument(
        "--symbols", help="Comma-separated list of symbols (e.g. RELIANCE,TCS,SBIN)"
    )
    parser.add_argument(
        "--universe-name",
        help="Saved universe name from backtest_universe table (e.g. gold_51)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run liquid symbol universe for the selected date range",
    )
    parser.add_argument(
        "--universe-size",
        type=int,
        default=51,
        help="With --all: top N symbols by avg daily traded value (default 51). Use 0 for every available symbol.",
    )
    parser.add_argument(
        "--yes-full-run",
        action="store_true",
        help="Confirm large-symbol runs (required when symbol count > 100)",
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--preset",
        choices=list_strategy_preset_names(),
        default=None,
        help=(
            "Named strategy preset that fills the full canonical config bundle "
            "(for example CPR_LEVELS_RISK_LONG or FBR_RISK_LONG)."
        ),
    )

    # Strategy param overrides
    parser.add_argument(
        "--cpr-percentile",
        type=float,
        default=33.0,
        help="CPR width percentile threshold (default 33.0 = bottom-third only)",
    )
    parser.add_argument(
        "--atr-periods",
        type=int,
        default=12,
        help="ATR lookback periods (default 12 = last 1 hour of 5-min candles)",
    )
    parser.add_argument(
        "--buffer-pct",
        type=float,
        default=0.0005,
        help="Breakout buffer %% above TC / below BC (default 0.0005)",
    )
    parser.add_argument(
        "--rvol",
        type=float,
        default=1.0,
        help="Min relative volume on entry candle (default 1.0 = average-or-better)",
    )
    parser.add_argument(
        "--rr-ratio",
        type=float,
        default=2.0,
        help="Risk-reward ratio for FBR target (default 2.0)",
    )
    parser.add_argument(
        "--breakeven-r",
        type=float,
        default=1.0,
        help="Move SL to entry at this R-multiple (default 1.0). Use 0.3 for early breakeven.",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100_000,
        help=(
            "Per-trade capital base for position sizing "
            "(paper runtime only; no-op in portfolio-overlay mode, default 100000)"
        ),
    )
    parser.add_argument(
        "--cpr-max-width-pct",
        type=float,
        default=2.0,
        help="Hard cap: skip days with CPR width > this %% (default 2.0)",
    )
    parser.add_argument(
        "--min-sl-atr-ratio",
        type=float,
        default=0.5,
        help="Min SL distance as ATR multiple (default 0.5; must be <= --max-sl-atr-ratio)",
    )
    parser.add_argument(
        "--risk-pct",
        type=float,
        default=0.01,
        help="Risk %% per trade for position sizing (paper runtime only; no-op in portfolio-overlay backtests)",
    )
    parser.add_argument(
        "--portfolio-value",
        type=float,
        default=1_000_000.0,
        help="Initial portfolio value for shared-cash execution and run metrics (default 1000000)",
    )
    parser.add_argument(
        "--compound-equity",
        action="store_true",
        default=False,
        help="Carry forward equity across trading days (default: reset to --portfolio-value daily)",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=10,
        help="Max concurrent positions sharing the same portfolio cash pool (default 10)",
    )
    parser.add_argument(
        "--max-position-pct",
        type=float,
        default=0.10,
        help="Max capital allocated to a single position as portfolio fraction (default 0.10)",
    )
    parser.add_argument(
        "--risk-based-sizing",
        "--legacy-sizing",
        dest="risk_based_sizing",
        action="store_true",
        help=(
            "Use per-trade risk-based sizing before the shared portfolio overlay "
            "(compat alias: --legacy-sizing)"
        ),
    )
    parser.add_argument(
        "--max-sl-atr-ratio",
        type=float,
        default=2.0,
        help="Max SL distance as ATR multiple (default 2.0)",
    )
    parser.add_argument(
        "--atr-sl-buffer",
        type=float,
        default=0.0,
        help="ATR multiplier for noise buffer below/above OR extreme (default 0.0)",
    )
    parser.add_argument(
        "--trail-atr-multiplier",
        type=float,
        default=None,
        help="LONG trailing-stop ATR multiplier once TRAIL begins (default: preset/default config)",
    )
    parser.add_argument(
        "--short-trail-atr-multiplier",
        type=float,
        default=None,
        help=(
            "SHORT trailing-stop ATR multiplier once TRAIL begins (default: preset/default config)"
        ),
    )
    parser.add_argument(
        "--time-exit",
        default="15:15",
        help="Time to close all positions HH:MM (default 15:15). Use 12:00 for mid-day kill.",
    )
    parser.add_argument(
        "--entry-window-end",
        default="10:15",
        help="Stop scanning for new entries after this time HH:MM (default 10:15)",
    )
    parser.add_argument("--save", action="store_true", help="Save results to DuckDB")
    parser.add_argument(
        "--progress-file",
        default=None,
        help="Optional NDJSON path for structured run progress heartbeats",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress verbose symbol-level output and print compact final summary only",
    )
    parser.add_argument(
        "--chunk-by",
        choices=["none", "year", "month"],
        default="none",
        help=(
            "Split execution window into smaller chunks and run/save each chunk independently "
            "(none|year|month). Each chunk gets a unique run_id."
        ),
    )
    parser.add_argument(
        "--skip-rvol", action="store_true", help="Skip RVOL check for faster testing"
    )
    parser.add_argument(
        "--runtime-batch-size",
        type=int,
        default=512,
        help="Number of symbols fetched/simulated per runtime chunk (default 512)",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=12,
        help="DuckDB thread count for this backtest run (default 12)",
    )
    parser.add_argument(
        "--duckdb-max-memory",
        default="36GB",
        help="DuckDB memory cap for this backtest run (default 36GB)",
    )
    parser.add_argument(
        "--direction",
        "--trade-direction-filter",
        dest="direction",
        choices=DIRECTIONS,
        default=DIRECTIONS[2],
        help=(
            "Restrict to LONG or SHORT trades only (default BOTH). "
            "For CPR_LEVELS: filters setup direction directly. "
            "For FBR: LONG = trade LONG reversals (failed SHORT breakdowns); "
            "SHORT = trade SHORT reversals (failed LONG breakouts). "
            "The internal direction_filter is automatically inverted for FBR."
        ),
    )
    parser.add_argument(
        "--or-atr-min",
        type=float,
        default=0.3,
        help="Min OR/ATR ratio — skip tiny ORs with no momentum (default 0.3)",
    )
    parser.add_argument(
        "--or-atr-max",
        type=float,
        default=2.5,
        help="Max OR/ATR ratio — skip exhausted ORs with no follow-through (default 2.5)",
    )
    parser.add_argument(
        "--max-gap-pct",
        type=float,
        default=1.5,
        help="Max |gap from prev close| %% — skip large opening gaps (default 1.5%%)",
    )
    parser.add_argument(
        "--long-max-gap-pct",
        type=float,
        default=None,
        help=(
            "Optional tighter long-side gap cap %% while leaving shorts on --max-gap-pct "
            "(e.g. 1.0 trims weak gap-up longs without touching shorts)"
        ),
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=0.0,
        help="Skip symbols with prev_close below this Rs. threshold (default 0 = no filter, 50 = skip penny stocks)",
    )
    parser.add_argument(
        "--regime-index-symbol",
        default="",
        help=(
            "Optional broad index symbol for the market-regime gate "
            "(for example NIFTY 500). Leave empty to disable."
        ),
    )
    parser.add_argument(
        "--regime-min-move-pct",
        type=float,
        default=0.0,
        help=(
            "Skip LONG when the regime index is down at least this %% and skip SHORT when "
            "it is up at least this %% (default 0.0 = off)."
        ),
    )
    parser.add_argument(
        "--or-minutes",
        type=int,
        default=5,
        choices=[5, 10, 15, 30],
        help="Opening Range duration in minutes (default: 5). Wider OR reduces false breakouts.",
    )

    # Transaction costs
    parser.add_argument(
        "--commission-model",
        choices=["zerodha", "zero"],
        default="zerodha",
        help="Brokerage cost model (default: zerodha = realistic intraday costs; zero = no costs)",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=0.0,
        help="Slippage in basis points per side (default 0.0; try 1-3 for realistic estimates)",
    )

    # Strategy selection
    parser.add_argument(
        "--strategy",
        choices=PUBLIC_STRATEGIES,
        default=PUBLIC_STRATEGIES[0],
        help="Strategy to run: CPR_LEVELS (floor pivot targets) or FBR (Failed Breakout Reversal).",
    )

    # FBR-specific params
    parser.add_argument(
        "--failure-window",
        type=int,
        default=8,
        help="FBR: candles after breakout to detect failure (default 8)",
    )
    parser.add_argument(
        "--reversal-buffer-pct",
        type=float,
        default=0.001,
        help="FBR: buffer %% for reversal entry/SL (default 0.001 = 0.1%%)",
    )
    parser.add_argument(
        "--fbr-min-or-atr",
        type=float,
        default=0.5,
        help="FBR: minimum OR/ATR ratio — breakout from tiny OR is noise (default 0.5)",
    )
    parser.add_argument(
        "--fbr-failure-depth",
        type=float,
        default=0.3,
        help="FBR: min fraction of OR range the failure close must penetrate inside OR (default 0.3)",
    )
    parser.add_argument(
        "--fbr-entry-window",
        default="10:30",
        help="FBR: latest time to look for a breakout (default 10:30 — after this momentum is spent)",
    )

    # CPR_LEVELS params
    parser.add_argument(
        "--cpr-shift",
        choices=CPR_SHIFTS,
        default=CPR_SHIFTS[0],
        help="CPR_LEVELS: filter by CPR value shift vs previous day (default ALL)",
    )
    parser.add_argument(
        "--min-effective-rr",
        type=float,
        default=2.0,
        help="CPR_LEVELS: min effective R:R — R1/S1 must be at least this many R away (default 2.0)",
    )
    parser.add_argument(
        "--narrowing-filter",
        action="store_true",
        help="CPR_LEVELS/FBR: only trade days where CPR is narrowing vs yesterday (squeeze days)",
    )

    # CPR_LEVELS entry quality controls
    parser.add_argument(
        "--cpr-entry-start",
        default="",
        help=(
            "CPR_LEVELS: earliest candle time for entry scan (default: auto from --or-minutes, "
            "min 09:20). Set explicitly e.g. '09:25' to override."
        ),
    )
    parser.add_argument(
        "--cpr-confirm-entry",
        action="store_true",
        help=(
            "CPR_LEVELS: require 2-step confirmation — signal candle closes beyond TC/BC, "
            "then next candle must hold the level AND break signal extreme before entry."
        ),
    )
    parser.add_argument(
        "--cpr-hold-confirm",
        action="store_true",
        help=(
            "CPR_LEVELS: softer confirmation — signal candle closes beyond TC/BC, "
            "then next candle must NOT close back inside CPR before entry."
        ),
    )
    parser.add_argument(
        "--cpr-min-close-atr",
        type=float,
        default=0.5,
        help=(
            "CPR_LEVELS: require signal close to clear TC/BC by at least this ATR multiple "
            "(0.0 disables; default 0.5 after long-side validation)."
        ),
    )
    parser.add_argument(
        "--cpr-scale-out-pct",
        type=float,
        default=0.0,
        help=(
            "CPR_LEVELS: scale out this fraction of position at R1/S1 and leave the runner "
            "for R2/S2 (0.0 disables)."
        ),
    )
    parser.add_argument(
        "--short-open-to-cpr-atr-min",
        type=float,
        default=0.0,
        help=(
            "CPR_LEVELS short-only: require the 09:15 open to be at least this ATR distance "
            "from the CPR band (0.0 disables; try 0.5)."
        ),
    )

    return parser


def main() -> None:
    configure_windows_stdio(line_buffering=True, write_through=True)

    parser = build_parser()
    args = parser.parse_args()
    # parse_args() exits on --help before reaching the lock.
    _run_with_lock(parser, args)


@command_lock("runtime-writer", detail="runtime writer")
def _run_with_lock(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.cpr_confirm_entry and args.cpr_hold_confirm:
        parser.error("Use either --cpr-confirm-entry or --cpr-hold-confirm, not both.")
    if args.cpr_min_close_atr < 0.0:
        parser.error("--cpr-min-close-atr must be >= 0.0")
    if args.cpr_scale_out_pct < 0.0 or args.cpr_scale_out_pct >= 1.0:
        parser.error("--cpr-scale-out-pct must be in the range [0.0, 1.0)")
    if args.short_open_to_cpr_atr_min < 0.0:
        parser.error("--short-open-to-cpr-atr-min must be >= 0.0")
    if args.runtime_batch_size < 1:
        parser.error("--runtime-batch-size must be >= 1")
    if args.duckdb_threads < 1:
        parser.error("--duckdb-threads must be >= 1")
    if not str(args.duckdb_max_memory).strip():
        parser.error("--duckdb-max-memory must be a non-empty value")
    if args.max_positions < 1:
        parser.error("--max-positions must be >= 1")
    if args.portfolio_value <= 0:
        parser.error("--portfolio-value must be > 0")
    if args.max_position_pct <= 0:
        parser.error("--max-position-pct must be > 0")
    try:
        start_date = _parse_iso_date(args.start, "--start")
        end_date = _parse_iso_date(args.end, "--end")
    except ValueError as e:
        parser.error(str(e))
    if start_date > end_date:
        parser.error("--start must be <= --end")
    if args.min_sl_atr_ratio <= 0:
        parser.error("--min-sl-atr-ratio must be > 0")
    if args.min_sl_atr_ratio > args.max_sl_atr_ratio:
        parser.error(
            f"--min-sl-atr-ratio ({args.min_sl_atr_ratio}) "
            f"must be <= --max-sl-atr-ratio ({args.max_sl_atr_ratio})"
        )
    if args.risk_pct <= 0:
        parser.error("--risk-pct must be > 0")
    if args.cpr_max_width_pct < 0:
        parser.error("--cpr-max-width-pct must be >= 0")

    # Tune DuckDB only for this backtest process. Both market and backtest DB
    # connections read these env vars during initialization.
    os.environ["DUCKDB_THREADS"] = str(args.duckdb_threads)
    os.environ["DUCKDB_MAX_MEMORY"] = str(args.duckdb_max_memory)

    if args.progress_file:
        append_progress_event(
            args.progress_file,
            {
                "event": "cli_invoked",
                "start": args.start,
                "end": args.end,
                "strategy": args.strategy,
                "direction": args.direction,
                "chunk_by": args.chunk_by,
                "all": bool(args.all),
                "symbol": args.symbol or "",
                "symbols": args.symbols or "",
                "universe_name": args.universe_name or "",
                "universe_size": args.universe_size,
            },
        )

    db = get_db()
    backtest_db = get_backtest_db()

    # Determine symbols to run
    if args.symbol:
        symbols = [normalize_symbol(args.symbol)]
    elif args.symbols:
        symbols = [normalize_symbol(s) for s in args.symbols.split(",")]
    elif args.universe_name:
        symbols = db.get_universe_symbols(args.universe_name)
        if not symbols:
            print(
                f"ERROR: Universe '{args.universe_name}' not found or empty. "
                "Run `uv run pivot-gold prepare --name <name> --start ... --end ...` first."
            )
            sys.exit(1)
        print(
            f"Running universe '{args.universe_name}' ({len(symbols)} symbols): "
            f"{', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}"
        )
    elif args.all:
        dq_summary = db.refresh_data_quality_issues()
        publish_replica = getattr(db, "_publish_replica", None)
        if callable(publish_replica):
            publish_replica(force=False)
        if dq_summary.get("missing_5min", 0):
            print(
                "Data quality filter: "
                f"excluded {dq_summary.get('missing_5min', 0)} symbols missing 5-min parquet"
            )

        if args.universe_size > 0:
            symbols = db.get_liquid_symbols(
                args.start,
                args.end,
                limit=args.universe_size,
                min_price=args.min_price,
            )
            if symbols:
                print(
                    f"Running top {len(symbols)} liquid symbols: "
                    f"{', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}"
                )
            else:
                symbols = db.get_available_symbols()
        else:
            symbols = db.get_available_symbols()

        # --- Tradeable-only gate ---
        from engine.kite_ingestion import tradeable_symbols

        tradeable = tradeable_symbols()
        if tradeable is not None:
            before = len(symbols)
            symbols = [s for s in symbols if s in tradeable]
            skipped = before - len(symbols)
            if skipped:
                print(f"Tradeable filter: skipped {skipped} dead symbols")

        if not symbols:
            print("ERROR: No symbols found in Parquet data. Run `pivot-convert` first.")
            sys.exit(1)

        if args.universe_size <= 0:
            print(
                f"Running all {len(symbols)} available symbols: "
                f"{', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}"
            )
    else:
        parser.error(
            "Specify --symbol SBIN, --symbols RELIANCE,TCS,SBIN, --universe-name gold_51, or --all"
        )
    if len(symbols) > 100 and not args.yes_full_run:
        parser.error(
            f"Symbol count ({len(symbols)}) exceeds 100. "
            "Add --yes-full-run to confirm large-symbol runs."
        )
    if len(symbols) > 100 and args.quiet and not args.progress_file:
        parser.error(
            "Large quiet runs must use --progress-file so the job has a visible heartbeat."
        )
    # For FBR, --direction means the *trade* direction (LONG = buy, SHORT = sell).
    # Internally direction_filter stores the *breakout* direction being scanned, which is
    # the opposite: LONG trade = failed SHORT breakdown; SHORT trade = failed LONG breakout.
    if args.strategy.upper() == "FBR" and args.direction != "BOTH":
        _fbr_setup_filter = "BREAKDOWN" if args.direction == "LONG" else "BREAKOUT"
        _direction_filter = "SHORT" if args.direction == "LONG" else "LONG"
    else:
        _fbr_setup_filter = "BOTH"
        _direction_filter = args.direction
    # Build params
    strategy_overrides = {
        "cpr_percentile": args.cpr_percentile,
        "cpr_max_width_pct": args.cpr_max_width_pct,
        "atr_periods": args.atr_periods,
        "buffer_pct": args.buffer_pct,
        "rvol_threshold": args.rvol,
        "rr_ratio": args.rr_ratio,
        "breakeven_r": args.breakeven_r,
        "capital": args.capital,
        "risk_pct": args.risk_pct,
        "portfolio_value": args.portfolio_value,
        "max_positions": args.max_positions,
        "max_position_pct": args.max_position_pct,
        "risk_based_sizing": args.risk_based_sizing,
        "compound_equity": args.compound_equity,
        "max_sl_atr_ratio": args.max_sl_atr_ratio,
        "min_sl_atr_ratio": args.min_sl_atr_ratio,
        "skip_rvol_check": args.skip_rvol,
        "runtime_batch_size": args.runtime_batch_size,
        "atr_sl_buffer": args.atr_sl_buffer,
        "time_exit": args.time_exit,
        "entry_window_end": args.entry_window_end,
        "direction_filter": _direction_filter,
        "fbr_setup_filter": _fbr_setup_filter,
        "short_open_to_cpr_atr_min": args.short_open_to_cpr_atr_min,
        "or_atr_min": args.or_atr_min,
        "or_atr_max": args.or_atr_max,
        "max_gap_pct": args.max_gap_pct,
        "long_max_gap_pct": args.long_max_gap_pct,
        "min_price": args.min_price,
        "regime_index_symbol": args.regime_index_symbol,
        "regime_min_move_pct": args.regime_min_move_pct,
        "or_minutes": args.or_minutes,
        "strategy": args.strategy,
        "commission_model": args.commission_model,
        "slippage_bps": args.slippage_bps,
        "cpr_levels_config": {
            "cpr_shift_filter": args.cpr_shift,
            "min_effective_rr": args.min_effective_rr,
            "use_narrowing_filter": args.narrowing_filter,
            "cpr_entry_start": args.cpr_entry_start,
            "cpr_confirm_entry": args.cpr_confirm_entry,
            "cpr_hold_confirm": args.cpr_hold_confirm,
            "cpr_min_close_atr": args.cpr_min_close_atr,
            "scale_out_pct": args.cpr_scale_out_pct,
        },
        "fbr_config": {
            "failure_window": args.failure_window,
            "reversal_buffer_pct": args.reversal_buffer_pct,
            "fbr_min_or_atr": args.fbr_min_or_atr,
            "fbr_failure_depth": args.fbr_failure_depth,
            "fbr_entry_window_end": args.fbr_entry_window,
            "use_narrowing_filter": args.narrowing_filter,
        },
    }
    if args.trail_atr_multiplier is not None:
        strategy_overrides["trail_atr_multiplier"] = args.trail_atr_multiplier
    if args.short_trail_atr_multiplier is not None:
        strategy_overrides["short_trail_atr_multiplier"] = args.short_trail_atr_multiplier
    if args.preset:
        # When using a preset, pass only infrastructure fields plus strategy flags that were
        # explicitly set.  Passing the full strategy_overrides dict would silently override
        # preset values with argparse defaults (e.g. narrowing_filter=False, min_price=0.0).
        preset_cli_overrides: dict[str, Any] = {
            "portfolio_value": args.portfolio_value,
            "capital": args.capital,
            "compound_equity": args.compound_equity,
            "max_positions": args.max_positions,
            "max_position_pct": args.max_position_pct,
            "runtime_batch_size": args.runtime_batch_size,
            "commission_model": args.commission_model,
            "slippage_bps": args.slippage_bps,
            "time_exit": args.time_exit,
            "entry_window_end": args.entry_window_end,
        }
        # Strategy semantics: only include if explicitly provided (non-default value).
        if args.narrowing_filter:
            preset_cli_overrides["narrowing_filter"] = True
        if args.risk_based_sizing:
            preset_cli_overrides["risk_based_sizing"] = True
        if args.skip_rvol:
            preset_cli_overrides["skip_rvol_check"] = True
        if args.min_price > 0.0:
            preset_cli_overrides["min_price"] = args.min_price
        if args.regime_index_symbol:
            preset_cli_overrides["regime_index_symbol"] = args.regime_index_symbol
        if args.regime_min_move_pct > 0.0:
            preset_cli_overrides["regime_min_move_pct"] = args.regime_min_move_pct
        if args.direction != "BOTH":
            preset_cli_overrides["direction_filter"] = args.direction
        if args.trail_atr_multiplier is not None:
            preset_cli_overrides["trail_atr_multiplier"] = args.trail_atr_multiplier
        if args.short_trail_atr_multiplier is not None:
            preset_cli_overrides["short_trail_atr_multiplier"] = args.short_trail_atr_multiplier
        params = build_strategy_config_from_preset(args.preset, preset_cli_overrides)
    else:
        params = build_strategy_config_from_overrides(args.strategy, strategy_overrides)

    # Run (single window or chunked windows)
    chunks = _build_chunks(start_date, end_date, args.chunk_by)
    bt = CPRATRBacktest(params=params, db=db)

    heartbeat = (
        _BacktestHeartbeat(total_symbols=len(symbols))
        if len(chunks) == 1 and not args.quiet
        else None
    )

    def _progress_sink(row: dict[str, object]) -> None:
        if args.progress_file:
            append_progress_event(args.progress_file, row)
        if heartbeat is not None:
            heartbeat.handle(row)

    progress_hook = _progress_sink

    if args.progress_file:
        append_progress_event(
            args.progress_file,
            {
                "event": "cli_run_start",
                "symbol_count": len(symbols),
                "strategy": args.strategy,
                "chunk_by": args.chunk_by,
                "chunk_count": len(chunks),
            },
        )

    run_started = time.time()
    chunk_results: list[BacktestResult] = []
    completed_chunks = 0

    if len(chunks) > 1:
        _progress_line(
            0.0,
            "chunk_plan",
            (
                f"Running {len(chunks)} {args.chunk_by} chunks for {len(symbols)} symbols "
                f"({args.start} -> {args.end})"
            ),
        )
        if args.progress_file:
            append_progress_event(
                args.progress_file,
                {
                    "event": "chunk_plan",
                    "chunk_by": args.chunk_by,
                    "chunk_count": len(chunks),
                    "start": args.start,
                    "end": args.end,
                },
            )

    for idx, (chunk_start, chunk_end, label) in enumerate(chunks, start=1):
        percent_done = ((idx - 1) / len(chunks)) * 100.0
        chunk_run_id = bt._make_run_id(symbols, chunk_start, chunk_end)

        _progress_line(
            percent_done,
            f"running_{args.chunk_by}",
            f"{args.chunk_by.title()} {label} started ({idx}/{len(chunks)})",
        )
        if args.progress_file:
            append_progress_event(
                args.progress_file,
                {
                    "event": "chunk_start",
                    "chunk_by": args.chunk_by,
                    "chunk_label": label,
                    "chunk_index": idx,
                    "chunk_count": len(chunks),
                    "chunk_start": chunk_start,
                    "chunk_end": chunk_end,
                    "chunk_run_id": chunk_run_id,
                },
            )

        chunk_started = time.time()
        chunk_result = bt.run(
            symbols=symbols,
            start=chunk_start,
            end=chunk_end,
            run_id=chunk_run_id,
            verbose=not args.quiet,
            progress_hook=progress_hook,
        )
        chunk_elapsed = time.time() - chunk_started
        chunk_results.append(chunk_result)
        completed_chunks += 1

        if args.save and len(chunks) > 1:
            n = chunk_result.save_to_db(backtest_db)
            print(
                f"Saved {n} trades for {args.chunk_by} {label} "
                "to backtest.duckdb -> backtest_results table"
            )
            if args.progress_file:
                append_progress_event(
                    args.progress_file,
                    {
                        "event": "chunk_save_complete",
                        "chunk_by": args.chunk_by,
                        "chunk_label": label,
                        "chunk_index": idx,
                        "chunk_count": len(chunks),
                        "chunk_run_id": chunk_result.run_id,
                        "saved_trades": n,
                    },
                )

        chunk_trades = len(chunk_result.df)
        chunk_ret = _chunk_return_pct(chunk_result, portfolio_value=params.portfolio_value)
        _progress_line(
            (idx / len(chunks)) * 100.0,
            f"{args.chunk_by}_complete",
            (
                f"{args.chunk_by.title()} {label} complete: trades={chunk_trades}, "
                f"return={chunk_ret:+.2f}% ({idx}/{len(chunks)}) in {chunk_elapsed:.1f}s"
            ),
        )
        if args.progress_file:
            append_progress_event(
                args.progress_file,
                {
                    "event": "chunk_complete",
                    "chunk_by": args.chunk_by,
                    "chunk_label": label,
                    "chunk_index": idx,
                    "chunk_count": len(chunks),
                    "chunk_start": chunk_start,
                    "chunk_end": chunk_end,
                    "chunk_run_id": chunk_result.run_id,
                    "elapsed_s": round(chunk_elapsed, 4),
                    "trade_count": chunk_trades,
                    "chunk_return_pct": round(chunk_ret, 4),
                },
            )

    result = _combine_chunk_results(
        chunk_results=chunk_results,
        params=params,
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
    )
    run_elapsed = time.time() - run_started

    if args.progress_file:
        append_progress_event(
            args.progress_file,
            {
                "event": "cli_run_complete",
                "run_id": result.run_id,
                "elapsed_s": round(run_elapsed, 4),
                "symbol_count": len(symbols),
                "chunk_by": args.chunk_by,
                "chunk_count": len(chunks),
                "chunks_completed": completed_chunks,
            },
        )

    if len(chunks) > 1:
        _progress_line(
            100.0,
            "run_complete",
            (f"Chunked run complete in {run_elapsed:.1f}s (completed={completed_chunks})"),
        )

    if args.quiet:
        _print_compact_summary(result, elapsed_s=run_elapsed, symbol_count=len(symbols))
    else:
        print(result.summary())

    if args.save:
        n = result.save_to_db(backtest_db)
        print(
            f"\nSaved {n} trades to backtest.duckdb -> backtest_results table (run_id={result.run_id})"
        )
        if args.progress_file:
            append_progress_event(
                args.progress_file,
                {
                    "event": "cli_save_complete",
                    "run_id": result.run_id,
                    "saved_trades": n,
                },
            )


if __name__ in {"__main__", "__mp_main__"}:
    main()
