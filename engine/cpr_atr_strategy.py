"""
CPR-ATR Strategy v2 — Vectorized Backtest Engine

Architecture:
    1. DuckDB pre-filters trading days (CPR width, direction bias) — bulk SQL
    2. ONE bulk candle fetch per symbol (all setup days in a single query)
    3. Polars partition_by splits candles by date into an O(1)-lookup dict
    4. Python loop only over valid-setup days (~5-20% of all days)
    5. TrailingStop per trade (sequential by nature, unavoidable)

This hybrid approach gives ~50-100x speedup over the old row-by-row PostgreSQL version.
The batch candle fetch (step 2-3) eliminates the previous N-queries-per-symbol bottleneck.

Usage:
    from engine.cpr_atr_strategy import CPRATRBacktest, StrategyConfig

    params = StrategyConfig()
    bt = CPRATRBacktest(params)
    results = bt.run(symbols=["SBIN", "TCS"], start="2020-01-01", end="2024-12-31")
    print(results.summary())
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from bisect import bisect_left, bisect_right
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import date
from types import SimpleNamespace
from typing import Any

import numpy as np
import polars as pl

from db.backtest_db import BacktestDB, get_backtest_db
from db.duckdb import MarketDB
from db.duckdb import get_db as get_market_db
from engine.bar_orchestrator import (
    SessionPositionTracker,
    minimum_trade_notional_for,
    select_entries_for_bar,
    should_process_symbol,
    slot_capital_for,
)
from engine.constants import preview_list
from engine.cost_model import CostModel, cost_model_from_name
from engine.cpr_atr_shared import (
    TradeLifecycleOutcome,
    get_cpr_entry_scan_start,
    normalize_stop_loss,
    regime_snapshot_close_col,
    scan_cpr_levels_entry,
    simulate_trade_lifecycle,
)
from engine.cpr_atr_shared import (
    _last_reject_reason as _cpr_reject_reason,
)
from engine.cpr_atr_shared import (
    find_first_close_idx as shared_find_first_close_idx,
)
from engine.cpr_atr_utils import (
    calculate_gap_pct,
    calculate_or_atr_ratio,
    calculate_position_size,
    normalize_cpr_bounds,
)
from engine.progress import BacktestProgress

logger = logging.getLogger(__name__)

STRATEGY_VERSION = "cpr-atr-v3"


def get_db() -> BacktestDB:
    """Return the dedicated backtest DuckDB handle.

    Kept as a module-level shim so tests can monkeypatch the persistence seam
    without reaching into the new backtest-specific helper directly.
    """

    return get_backtest_db()


def _int_from_mapping(
    values: Mapping[str, object] | None,
    key: str,
    default: int,
) -> int:
    if values is None:
        return default
    value = values.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CPRLevelsParams:
    """Strategy-specific parameters for CPR_LEVELS."""

    cpr_shift_filter: str
    min_effective_rr: float
    use_narrowing_filter: bool
    cpr_entry_start: str
    cpr_confirm_entry: bool
    cpr_hold_confirm: bool
    cpr_min_close_atr: float
    scale_out_pct: float = 0.0


@dataclass(frozen=True)
class FBRParams:
    """Strategy-specific parameters for failed-breakout reversals."""

    failure_window: int
    reversal_buffer_pct: float
    fbr_min_or_atr: float
    fbr_failure_depth: float
    fbr_entry_window_end: str
    use_narrowing_filter: bool


@dataclass(frozen=True)
class VirginCPRParams:
    """Strategy-specific parameters for VIRGIN_CPR breakout entries."""

    vcpr_confirm_candles: int
    vcpr_body_pct: float
    vcpr_sl_mode: str
    candle_exit: int
    vcpr_scan_start: str
    vcpr_scan_end: str
    vcpr_min_open_dist_atr: float


@dataclass
class StrategyConfig:
    """CPR-ATR strategy parameters. Change here to test different configs."""

    # CPR filter
    cpr_percentile: float = 33.0  # Dynamic threshold: P33 of historical widths (bottom third only)
    cpr_max_width_pct: float = 2.0  # Hard cap — skip days wider than this

    # ATR
    atr_periods: int = 12  # Last 12 five-min candles of prev day = 1 hour

    # Entry
    buffer_pct: float = 0.0005  # 0.05% breakout buffer above/below OR
    rvol_threshold: float = (
        1.0  # Minimum relative volume on entry candle (average-or-better required)
    )
    entry_window_end: str = "10:15"  # Stop looking for entry after this time
    short_open_to_cpr_atr_min: float = 0.0  # Short-only min distance from open to CPR band

    # Risk management
    min_sl_atr_ratio: float = 0.5
    max_sl_atr_ratio: float = (
        2.0  # Max SL as ATR multiple (was 3.0 — wide SL defeats tight-CPR thesis)
    )
    rr_ratio: float = 2.0  # 1:2 risk-reward
    breakeven_r: float = 1.0  # Move SL to entry at this R-multiple (0.3 = early breakeven)
    atr_sl_buffer: float = 0.0  # ATR multiplier added as noise buffer beyond OR extreme
    trail_atr_multiplier: float = 1.0  # LONG trailing SL ATR multiplier once TRAIL begins
    short_trail_atr_multiplier: float = 1.0  # SHORT trailing SL ATR multiplier

    # Position sizing
    capital: float = 100_000  # Risk-based sizing base for candidate trades
    risk_pct: float = 0.01  # Risk sizing; portfolio overlay is the default execution path
    portfolio_value: float = 1_000_000.0  # Shared portfolio base for execution + reporting
    max_positions: int = 10  # Max concurrent intraday positions
    max_position_pct: float = 0.10  # Max capital allocated to one position
    risk_based_sizing: bool = False  # Use per-trade risk-based sizing before portfolio overlay
    compound_equity: bool = False  # Carry forward equity across days (True = old behavior)

    # Time exit
    time_exit: str = "15:15"  # Close all positions by this time

    # Volume profile lookback
    rvol_lookback_days: int = 10
    skip_rvol_check: bool = False  # Set True for faster testing
    runtime_batch_size: int = 512  # Day-pack fetch batch size; setup always fetched in one shot

    # Trade direction filter: "BOTH" | "LONG" | "SHORT"
    # For CPR_LEVELS/VIRGIN_CPR: filters setup direction directly (LONG = buy, SHORT = sell).
    # For FBR: the CLI maps --direction to the *trade* direction; run_backtest.py inverts this
    # internally so direction_filter here stores the *breakout* direction being scanned
    # (LONG breakout setup → SHORT reversal trade; SHORT breakdown setup → LONG reversal trade).
    direction_filter: str = "BOTH"

    # FBR-only: human-readable label for the breakout setup type targeted.
    # "BREAKOUT"  = scanning for failed LONG breakouts  → produces SHORT reversal trades.
    # "BREAKDOWN" = scanning for failed SHORT breakdowns → produces LONG reversal trades.
    # "BOTH" = both setup types. Ignored for CPR_LEVELS/VIRGIN_CPR.
    fbr_setup_filter: str = "BOTH"

    # Opening Range duration: how many minutes to observe before entry is allowed
    or_minutes: int = 5  # 5 | 10 | 15 | 30 — wider OR reduces false breakouts

    # OR/ATR ratio filter — skip days where OR is tiny (noise) or huge (exhausted move)
    or_atr_min: float = 0.3  # Min OR/ATR ratio — skip tiny ORs with no momentum (was 0.0)
    or_atr_max: float = (
        2.5  # Max OR/ATR ratio — skip exhausted ORs with no follow-through (was 99.0)
    )

    # Gap filter — skip days with a large opening gap (tends to fill, fighting the breakout)
    max_gap_pct: float = 1.5  # Max |gap from prev close| % (was 99.0 — large gaps tend to fill)
    long_max_gap_pct: float | None = (
        None  # Optional tighter long-side cap while leaving shorts unchanged
    )

    # Price filter — skip symbols with prev_close below this threshold (eliminates penny stocks)
    min_price: float = 0.0  # 0 = no filter; 50 = skip stocks trading below Rs.50

    # Optional market-regime gate:
    # When enabled, use a broad index snapshot to skip LONG trades on weak days and
    # SHORT trades on strong days. Leave disabled by default until a regime hypothesis is proven.
    regime_index_symbol: str = ""
    regime_min_move_pct: float = 0.0
    regime_snapshot_minutes: int = 30

    # Strategy selection: "CPR_LEVELS" | "FBR" | "VIRGIN_CPR"
    strategy: str = "CPR_LEVELS"

    # Strategy-specific grouped configs
    cpr_levels_config: CPRLevelsParams = field(
        default_factory=lambda: CPRLevelsParams(
            cpr_shift_filter="ALL",
            min_effective_rr=2.0,
            use_narrowing_filter=False,
            cpr_entry_start="",
            cpr_confirm_entry=False,
            cpr_hold_confirm=False,
            cpr_min_close_atr=0.0,
            scale_out_pct=0.0,
        )
    )
    fbr_config: FBRParams = field(
        default_factory=lambda: FBRParams(
            failure_window=8,
            reversal_buffer_pct=0.001,
            fbr_min_or_atr=0.5,
            fbr_failure_depth=0.3,
            fbr_entry_window_end="10:30",
            use_narrowing_filter=False,
        )
    )
    virgin_cpr_config: VirginCPRParams = field(
        default_factory=lambda: VirginCPRParams(
            vcpr_confirm_candles=1,
            vcpr_body_pct=0.0,
            vcpr_sl_mode="ZONE",
            candle_exit=0,
            vcpr_scan_start="09:20",
            vcpr_scan_end="12:30",
            vcpr_min_open_dist_atr=0.3,
        )
    )

    # Transaction costs — default to realistic Zerodha model
    commission_model: str = "zerodha"  # "zerodha" | "zero"
    slippage_bps: float = 0.0  # Slippage in basis points per side

    @property
    def cpr_levels(self) -> CPRLevelsParams:
        """Expose CPR_LEVELS-specific config as a structured object."""
        return self.cpr_levels_config

    @property
    def fbr(self) -> FBRParams:
        """Expose FBR-specific config as a structured object."""
        return self.fbr_config

    @property
    def virgin_cpr(self) -> VirginCPRParams:
        """Expose VIRGIN_CPR-specific config as a structured object."""
        return self.virgin_cpr_config

    def get_cost_model(self) -> CostModel:
        """Create a CostModel from the commission_model and slippage_bps params."""
        return cost_model_from_name(self.commission_model, slippage_bps=self.slippage_bps)

    def max_gap_for_direction(self, direction: str) -> float:
        """Return the applicable gap cap for the requested direction."""
        if direction == "LONG" and self.long_max_gap_pct is not None:
            return float(self.long_max_gap_pct)
        return float(self.max_gap_pct)

    def apply_strategy_configs(
        self,
        *,
        cpr_levels: CPRLevelsParams | None = None,
        fbr: FBRParams | None = None,
        virgin_cpr: VirginCPRParams | None = None,
    ) -> StrategyConfig:
        """
        Return a copy with updated grouped strategy config objects.
        """
        updates: dict[str, object] = {}
        if cpr_levels is not None:
            updates["cpr_levels_config"] = cpr_levels
        if fbr is not None:
            updates["fbr_config"] = fbr
        if virgin_cpr is not None:
            updates["virgin_cpr_config"] = virgin_cpr
        if not updates:
            return self
        return replace(self, **updates)


BacktestParams = StrategyConfig


# ---------------------------------------------------------------------------
# Trade result
# ---------------------------------------------------------------------------


@dataclass
class TradeResult:
    run_id: str
    symbol: str
    trade_date: str
    direction: str
    entry_time: str | None = None
    exit_time: str | None = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    sl_price: float = 0.0
    target_price: float = 0.0
    profit_loss: float = 0.0
    profit_loss_pct: float = 0.0
    exit_reason: str = ""  # "SL" | "TARGET" | "TIME" | "NO_ENTRY"
    sl_phase: str = ""  # Phase when trade ended
    atr: float = 0.0
    cpr_width_pct: float = 0.0
    cpr_threshold: float = 0.0
    rvol: float = 0.0
    position_size: int = 0
    position_value: float = 0.0
    strategy_version: str = STRATEGY_VERSION
    mfe_r: float = 0.0  # Max favorable excursion in R-multiples
    mae_r: float = 0.0  # Max adverse excursion in R-multiples (negative by convention)
    or_atr_ratio: float = 0.0  # OR range / ATR (diagnostic: 0.3–1.5 is the sweet spot)
    gap_pct: float = 0.0  # Gap from previous close % (large gaps tend to fill)
    # Cost fields (§0.1)
    gross_pnl: float = 0.0  # PnL before transaction costs
    total_costs: float = 0.0  # Brokerage + STT + exchange + slippage
    # Exit diagnostics (§0.3)
    reached_1r: bool = False  # MFE reached >= 1R during trade
    reached_2r: bool = False  # MFE reached >= 2R during trade
    max_r: float = 0.0  # Maximum favorable excursion in R-multiples


@dataclass
class FunnelCounts:
    """Per-run setup selection funnel counts."""

    run_id: str = ""
    strategy: str = ""
    universe_count: int = 0  # Total symbol-days in the universe
    after_cpr_width: int = 0  # Passed CPR width filter
    after_direction: int = 0  # Had valid direction (LONG/SHORT, not NONE)
    after_dir_filter: int = 0  # Passed direction filter (BOTH/LONG/SHORT)
    after_min_price: int = 0  # Passed min-price filter
    after_gap: int = 0  # Passed gap filter
    after_or_atr: int = 0  # Passed OR/ATR filter
    after_narrowing: int = 0  # Passed narrowing filter (if enabled)
    after_shift: int = 0  # Passed CPR shift filter (if enabled)
    entry_triggered: int = 0  # Entry rules met -> trade taken


@dataclass
class DayPack:
    """Compact per-day intraday payload used by runtime simulation."""

    time_str: list[str]
    opens: list[float]
    highs: list[float]
    lows: list[float]
    closes: list[float]
    volumes: list[float]
    rvol_baseline: list[float | None] | None = None
    _idx_by_time: dict[str, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._idx_by_time = {t: i for i, t in enumerate(self.time_str)}

    def to_frame(self) -> pl.DataFrame:
        """Materialize a DataFrame view on demand for current strategy logic."""
        return pl.DataFrame(
            {
                "time_str": self.time_str,
                "open": self.opens,
                "high": self.highs,
                "low": self.lows,
                "close": self.closes,
                "volume": self.volumes,
            }
        )

    def baseline_for_time(self, time_str: str) -> float:
        """Return RVOL baseline volume for the given candle time, or 0 when unavailable."""
        idx = self._idx_by_time.get(time_str, -1)
        if idx < 0:
            return 0.0
        return self.baseline_for_index(idx)

    def baseline_for_index(self, idx: int) -> float:
        """Return RVOL baseline volume for a candle index, or 0 when unavailable."""
        if not self.rvol_baseline:
            return 0.0
        if idx < 0 or idx >= len(self.rvol_baseline):
            return 0.0
        val = self.rvol_baseline[idx]
        if val is None:
            return 0.0
        avg = float(val)
        return avg if avg > 0 else 0.0

    def index_of(self, time_str: str) -> int:
        """Return exact candle index for a time string, or -1 when absent."""
        return self._idx_by_time.get(time_str, -1)

    def range_indices(self, start_time: str, end_time: str) -> tuple[int, int]:
        """Return inclusive index range for [start_time, end_time], or (-1, -1) if empty."""
        if not self.time_str:
            return -1, -1
        lo = bisect_left(self.time_str, start_time)
        hi = bisect_right(self.time_str, end_time) - 1
        if lo >= len(self.time_str) or hi < lo:
            return -1, -1
        return lo, hi


# ---------------------------------------------------------------------------
# Main backtest class
# ---------------------------------------------------------------------------


class CPRATRBacktest:
    """
    Runs CPR-ATR strategy v2 over a date range for one or more symbols.

    Flow:
        run()
          -> _get_all_setups_batch() from market_day_state
          -> _prefetch_day_pack_batch() from intraday_day_pack
          -> _simulate_day_*() per setup with TrailingStop
    """

    def __init__(self, params: BacktestParams | None = None, db: MarketDB | None = None):
        self.params = params or BacktestParams()
        self.db = db or get_market_db()
        self._uses_compact_pack_schema: bool | None = None
        self._cost_model = self.params.get_cost_model()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        symbols: list[str] | str,
        start: str,
        end: str,
        run_id: str | None = None,
        verbose: bool = True,
        use_cache: bool = True,
        progress_hook: Callable[[dict[str, object]], None] | None = None,
    ) -> BacktestResult:
        """
        Run backtest for given symbols over date range.

        Batch architecture (fast path):
            Data for non-cached symbols is fetched in bounded runtime batches
            (`runtime_batch_size`) to control memory and avoid all-universe
            materialization spikes. Simulation (TrailingStop) stays per-symbol
            since it is inherently sequential.

        Caching:
            Backtests are append-only. Each execution generates a unique run_id.
            No cache reuse — every run computes fresh results.

        Args:
            symbols:    e.g. ["SBIN", "TCS"] or "SBIN"
            start:      "2020-01-01"
            end:        "2024-12-31"
            run_id:     Override the unique run_id (advanced use)
            verbose:    print progress
            use_cache:  Ignored (kept for API compatibility). All runs compute fresh.
            progress_hook:
                        Optional callback that receives structured progress events.

        Returns: BacktestResult with trades and summary stats
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        run_id = run_id or self._make_run_id(symbols, start, end)
        started_at = time.time()
        all_trades: list[TradeResult] = []
        cpr_prefetched_setups_by_sym: dict[str, pl.DataFrame] = {}
        cpr_prefetched_candles_by_sym: dict[str, dict[str, DayPack]] = {}

        def _emit_progress(event: str, **payload: object) -> None:
            if progress_hook is None:
                return
            row: dict[str, object] = {
                "event": event,
                "run_id": run_id,
                "start": start,
                "end": end,
            }
            row.update(payload)
            try:
                progress_hook(row)
            except Exception as e:
                # Progress sinks are best-effort and must never affect backtest execution.
                logger.debug("Progress hook failed for event=%s: %s", event, e)

        # --- Phase 0: all symbols need simulation (no cache) ---
        symbols_to_run = list(symbols)
        param_signature = self._make_param_signature(symbols, start, end)

        run_context: dict[str, object] = {
            "start_date": start,
            "end_date": end,
            "symbols": symbols,
            "run_id": run_id,
            "param_signature": param_signature,
        }

        _emit_progress(
            "run_start",
            total_symbols=len(symbols),
            to_run_count=len(symbols_to_run),
            strategy=self.params.strategy,
        )

        # Runtime contract is precomputed state only (no raw-candle fallback path).
        if symbols_to_run:
            if verbose:
                print(f"[stage] runtime validate start | symbols={len(symbols_to_run)}")
            _emit_progress("runtime_validate_start", symbol_count=len(symbols_to_run))

            t_runtime = time.time()
            missing_coverage = self.db.get_missing_runtime_symbol_coverage(symbols_to_run)
            missing_state = missing_coverage["market_day_state"]
            missing_strategy = missing_coverage["strategy_day_state"]
            missing_pack = missing_coverage["intraday_day_pack"]
            runtime_elapsed = time.time() - t_runtime

            has_missing_coverage = bool(missing_state or missing_strategy or missing_pack)
            if verbose:
                print(
                    f"[stage] runtime validate done  | elapsed={runtime_elapsed:.2f}s"
                    f" | missing_state={len(missing_state)}"
                    f" | missing_strategy={len(missing_strategy)}"
                    f" | missing_pack={len(missing_pack)}"
                )
            _emit_progress(
                "runtime_validate_done",
                elapsed_s=round(runtime_elapsed, 4),
                missing_state=len(missing_state),
                missing_strategy=len(missing_strategy),
                missing_pack=len(missing_pack),
                coverage_ok=not has_missing_coverage,
            )

            if has_missing_coverage:
                _emit_progress(
                    "runtime_validate_failed",
                    missing_state=missing_state,
                    missing_strategy=missing_strategy,
                    missing_pack=missing_pack,
                )
                raise RuntimeError(
                    self._format_runtime_coverage_error(
                        requested_symbols=symbols_to_run,
                        missing_state=missing_state,
                        missing_strategy=missing_strategy,
                        missing_pack=missing_pack,
                    )
                )

        # --- Phase 0.5: setup selection funnel diagnostics ---
        funnel: FunnelCounts | None = None
        if symbols_to_run:
            try:
                t_funnel = time.time()
                funnel = self._count_setup_funnel(symbols_to_run, start, end)
                funnel.run_id = run_id
                funnel_elapsed = time.time() - t_funnel
                if verbose:
                    print(
                        f"[stage] funnel count done  | elapsed={funnel_elapsed:.2f}s"
                        f" | universe={funnel.universe_count}"
                        f" | after_filters={funnel.after_shift}"
                    )
                _emit_progress(
                    "funnel_done",
                    elapsed_s=round(funnel_elapsed, 4),
                    universe=funnel.universe_count,
                    after_filters=funnel.after_shift,
                )
            except Exception as e:
                logger.warning("Setup funnel counting failed: %s", e)

        # --- Phase 1: single setup query for ALL symbols ---
        runtime_batch_size = max(1, int(self.params.runtime_batch_size))
        setups_by_sym: dict[str, pl.DataFrame] = {}
        symbols_with_setups: list[str] = []
        pack_batches: list[list[str]] = []

        if symbols_to_run:
            if verbose:
                print(f"[stage] setup fetch start | {len(symbols_to_run)} symbols")
            _emit_progress("setup_fetch_start", symbol_count=len(symbols_to_run))

            t_setup = time.time()
            all_setups_df = self._get_all_setups_batch(symbols_to_run, start, end)
            setup_elapsed = time.time() - t_setup

            if not all_setups_df.is_empty():
                for part in all_setups_df.partition_by("symbol"):
                    setups_by_sym[part["symbol"][0]] = part

            symbols_with_setups = [s for s in symbols_to_run if s in setups_by_sym]
            pack_batches = self._iter_symbol_batches(symbols_with_setups, runtime_batch_size)

            if verbose:
                print(
                    f"[stage] setup fetch done  | elapsed={setup_elapsed:.2f}s"
                    f" | setup_days={len(all_setups_df)}"
                    f" | symbols_with_setups={len(symbols_with_setups)}/{len(symbols_to_run)}"
                    f" | pack_batches={len(pack_batches)}"
                )
            _emit_progress(
                "setup_fetch_done",
                elapsed_s=round(setup_elapsed, 4),
                setup_days=len(all_setups_df),
                symbols_with_setups=len(symbols_with_setups),
                symbols_no_setups=len(symbols_to_run) - len(symbols_with_setups),
                pack_batches=len(pack_batches),
            )
            _emit_progress(
                "batch_plan",
                runtime_batch_size=runtime_batch_size,
                batch_count=len(pack_batches),
                to_run_count=len(symbols_with_setups),
            )

        # --- Phase 2: day-pack fetch + simulation, batched over symbols_with_setups ---
        progress = BacktestProgress(total_symbols=len(symbols), verbose=verbose)
        try:
            # Advance progress bar immediately for symbols with no setup days
            for symbol in symbols_to_run:
                if symbol not in setups_by_sym:
                    if self.params.strategy != "CPR_LEVELS":
                        progress.update_symbol(symbol, 0, 0.0)
                        _emit_progress(
                            "symbol_done", symbol=symbol, setups=0, trades=0, elapsed_s=0.0
                        )

            for batch_idx, batch_symbols in enumerate(pack_batches, start=1):
                if verbose:
                    progress.log_stage(
                        f"pack {batch_idx}/{len(pack_batches)}"
                        f" | fetching {len(batch_symbols)} symbols"
                    )
                _emit_progress(
                    "day_pack_fetch_start",
                    batch_idx=batch_idx,
                    batch_count=len(pack_batches),
                    symbol_count=len(batch_symbols),
                )

                batch_setups_df = pl.concat([setups_by_sym[sym] for sym in batch_symbols])
                t_pack = time.time()
                candles_by_sym = self._prefetch_day_pack_batch(batch_setups_df)
                pack_elapsed = time.time() - t_pack

                if verbose:
                    progress.log_stage(
                        f"pack {batch_idx}/{len(pack_batches)} done",
                        elapsed_s=f"{pack_elapsed:.2f}s",
                        symbols=len(candles_by_sym),
                    )
                _emit_progress(
                    "day_pack_fetch_done",
                    batch_idx=batch_idx,
                    batch_count=len(pack_batches),
                    elapsed_s=round(pack_elapsed, 4),
                    packed_symbols=len(candles_by_sym),
                )

                if self.params.strategy == "CPR_LEVELS":
                    # CPR_LEVELS is simulated once across the full prefetched universe
                    # so cash/slot ordering is global rather than batch-local.
                    for symbol in batch_symbols:
                        if symbol in setups_by_sym:
                            cpr_prefetched_setups_by_sym[symbol] = setups_by_sym[symbol]
                        if symbol in candles_by_sym:
                            cpr_prefetched_candles_by_sym[symbol] = candles_by_sym[symbol]
                    continue

                batch_trades_by_symbol: dict[str, list[TradeResult]] = {}
                for symbol in batch_symbols:
                    symbol_start = time.time()

                    if verbose and progress.overall_bar is None:
                        print(f"[{symbol}] Running {self.params.strategy} {start} to {end}")

                    sym_setups = setups_by_sym.get(symbol, pl.DataFrame())
                    sym_candles = candles_by_sym.get(symbol, {})

                    if self.params.strategy == "CPR_LEVELS":
                        trades = batch_trades_by_symbol.get(symbol, [])
                    else:
                        trades = self._simulate_from_preloaded(
                            symbol, run_id, sym_setups, sym_candles
                        )

                    all_trades.extend(trades)

                    symbol_elapsed = time.time() - symbol_start
                    wins = sum(1 for t in trades if t.profit_loss > 0)
                    total_pnl = float(sum(t.profit_loss for t in trades))

                    if verbose and trades and progress.overall_bar is None:
                        print(
                            f"  {len(trades)} trades | {wins}/{len(trades)} wins "
                            f"({wins / len(trades) * 100:.1f}%) | P/L: {total_pnl:+.2f}"
                        )

                    progress.update_symbol(symbol, len(trades), symbol_elapsed)
                    _emit_progress(
                        "symbol_done",
                        symbol=symbol,
                        setups=len(sym_setups),
                        trades=len(trades),
                        wins=wins,
                        pnl=round(total_pnl, 4),
                        elapsed_s=round(symbol_elapsed, 4),
                    )
            if self.params.strategy == "CPR_LEVELS":
                batch_trades_by_symbol = self._simulate_cpr_levels_batch(
                    run_id=run_id,
                    batch_symbols=list(cpr_prefetched_setups_by_sym.keys()),
                    setups_by_sym=cpr_prefetched_setups_by_sym,
                    candles_by_sym=cpr_prefetched_candles_by_sym,
                )
                for symbol in symbols_to_run:
                    symbol_start = time.time()
                    trades = batch_trades_by_symbol.get(symbol, [])
                    all_trades.extend(trades)
                    symbol_elapsed = time.time() - symbol_start
                    wins = sum(1 for t in trades if t.profit_loss > 0)
                    total_pnl = float(sum(t.profit_loss for t in trades))
                    progress.update_symbol(symbol, len(trades), symbol_elapsed)
                    _emit_progress(
                        "symbol_done",
                        symbol=symbol,
                        setups=len(setups_by_sym.get(symbol, pl.DataFrame())),
                        trades=len(trades),
                        wins=wins,
                        pnl=round(total_pnl, 4),
                        elapsed_s=round(symbol_elapsed, 4),
                    )
            else:
                pass
        finally:
            progress.close()
            if verbose:
                progress.print_summary()

        total_elapsed = time.time() - started_at
        candidate_trade_count = len(all_trades)

        # Finalize funnel: set entry_triggered to actual trade count
        if funnel is not None:
            funnel.entry_triggered = len(all_trades)

        execution_stats: dict[str, object] | None = None
        if symbols_to_run:
            if self.params.strategy == "CPR_LEVELS" and not self.params.compound_equity:
                # Batch path already applied portfolio constraints in-bar via tracker.
                # SessionPositionTracker enforces both max_positions and max_position_pct:
                #   slot_capital = min(portfolio_value / max_positions, portfolio_value * max_position_pct)
                # This matches the overlay's sizing logic, making the post-hoc pass redundant
                # for CPR_LEVELS daily-reset (the overlay's remaining value is compound equity).
                execution_stats = {
                    "candidate_trade_count": len(all_trades),
                    "executed_trade_count": len(all_trades),
                    "not_executed_portfolio": 0,
                    "skipped_no_cash": 0,
                    "skipped_no_slots": 0,
                    "initial_portfolio_value": round(float(self.params.portfolio_value or 0.0), 2),
                    "ending_portfolio_value": round(
                        float(self.params.portfolio_value or 0.0)
                        + sum(float(t.profit_loss) for t in all_trades),
                        2,
                    ),
                }
            else:
                all_trades, execution_stats = self._apply_portfolio_constraints(all_trades)
            if execution_stats:
                run_context["execution_stats"] = execution_stats

        # Store funnel to DuckDB
        if funnel is not None and funnel.universe_count > 0:
            try:
                self.db.store_setup_funnel(asdict(funnel))
            except Exception as e:
                logger.warning("Failed to store setup funnel for run_id=%s: %s", run_id, e)

        # Caller is responsible for saving (save_to_db) if persistence is needed.
        _emit_progress(
            "run_complete",
            total_elapsed_s=round(total_elapsed, 4),
            simulated_trades=len(all_trades),
            candidate_trades=candidate_trade_count,
            not_executed_portfolio=_int_from_mapping(
                execution_stats,
                "not_executed_portfolio",
                0,
            ),
        )

        return BacktestResult(
            run_id=run_id,
            trades=all_trades,
            params=self.params,
            run_context=run_context,
            funnel=funnel,
        )

    def _apply_portfolio_constraints(
        self, trades: list[TradeResult]
    ) -> tuple[list[TradeResult], dict[str, object]]:
        """Apply equal-slot portfolio execution with shared cash.

        This mirrors the live backtest path in nse-momentum-lab more closely than
        the older per-symbol capital model: one shared portfolio, fixed slot
        capital, max concurrent positions, and skipped trades when cash/slots are
        exhausted. CPR is intraday-only, so positions are always flat by end of day.
        """
        portfolio_value = float(self.params.portfolio_value or 0.0)
        max_positions = max(1, int(self.params.max_positions or 1))
        max_position_pct = float(self.params.max_position_pct or 0.0)
        if portfolio_value <= 0 or not trades:
            total_pnl = round(sum(float(t.profit_loss) for t in trades), 2)
            return trades, {
                "candidate_trade_count": len(trades),
                "executed_trade_count": len(trades),
                "not_executed_portfolio": 0,
                "skipped_no_cash": 0,
                "skipped_no_slots": 0,
                "initial_portfolio_value": round(portfolio_value, 2),
                "ending_portfolio_value": round(portfolio_value + total_pnl, 2),
            }

        def _sort_key(trade: TradeResult) -> tuple[str, str, str]:
            return (
                trade.trade_date,
                trade.entry_time or "99:99",
                trade.symbol,
            )

        def _per_share_pnl(trade: TradeResult) -> float:
            if trade.direction == "LONG":
                return float(trade.exit_price - trade.entry_price)
            return float(trade.entry_price - trade.exit_price)

        sorted_trades = sorted(trades, key=_sort_key)
        trades_by_day: dict[str, list[TradeResult]] = {}
        for trade in sorted_trades:
            trades_by_day.setdefault(trade.trade_date, []).append(trade)

        executed: list[TradeResult] = []
        equity = portfolio_value
        skipped_no_cash = 0
        skipped_no_slots = 0

        for trade_date in sorted(trades_by_day.keys()):
            if not self.params.compound_equity:
                equity = portfolio_value
            day_start_equity = equity
            cash_available = equity
            slot_capital = slot_capital_for(
                max_positions=max_positions,
                portfolio_value=day_start_equity,
                max_position_pct=max_position_pct,
                capital_base=day_start_equity,
            )
            min_notional = max(1_000.0, slot_capital * 0.05)
            open_positions: list[dict[str, float | str]] = []

            for trade in sorted(trades_by_day[trade_date], key=_sort_key):
                entry_time = trade.entry_time or "99:99"
                released_cash = 0.0
                remaining: list[dict[str, float | str]] = []
                for pos in sorted(open_positions, key=lambda row: str(row["exit_time"])):
                    if str(pos["exit_time"]) <= entry_time:
                        released_cash += float(pos["exit_value"])
                    else:
                        remaining.append(pos)
                open_positions = remaining
                cash_available += released_cash

                if len(open_positions) >= max_positions:
                    skipped_no_slots += 1
                    continue
                if trade.entry_price <= 0:
                    skipped_no_cash += 1
                    continue

                if self.params.risk_based_sizing:
                    # Keep the historical risk-based quantity, but cap the
                    # allocation at the shared slot capital. Do not partially
                    # fill from leftover cash; if we cannot afford the full
                    # capped allocation, skip the trade.
                    desired_notional = min(
                        max(1, int(trade.position_size or 0)) * float(trade.entry_price),
                        slot_capital,
                    )
                else:
                    desired_notional = slot_capital
                if (
                    desired_notional <= 0
                    or desired_notional < min_notional
                    or cash_available < desired_notional
                ):
                    skipped_no_cash += 1
                    continue
                qty = int(desired_notional / float(trade.entry_price))
                if qty < 1:
                    skipped_no_cash += 1
                    continue

                position_value = round(qty * float(trade.entry_price), 2)
                cash_available -= position_value

                gross_pnl_trade = round(_per_share_pnl(trade) * qty, 2)
                trade_cost = self._cost_model.round_trip_cost(
                    entry_price=float(trade.entry_price),
                    exit_price=float(trade.exit_price),
                    qty=qty,
                    direction=trade.direction,
                )
                net_pnl_trade = round(gross_pnl_trade - trade_cost, 2)
                net_pct_trade = round(
                    (net_pnl_trade / position_value * 100) if position_value > 0 else 0.0,
                    4,
                )
                exit_value = round(position_value + gross_pnl_trade, 2)
                open_positions.append(
                    {
                        "exit_time": trade.exit_time or self.params.time_exit,
                        "exit_value": exit_value,
                    }
                )
                executed.append(
                    replace(
                        trade,
                        position_size=qty,
                        position_value=position_value,
                        gross_pnl=gross_pnl_trade,
                        total_costs=round(trade_cost, 2),
                        profit_loss=net_pnl_trade,
                        profit_loss_pct=net_pct_trade,
                    )
                )

            released_cash = 0.0
            for pos in open_positions:
                released_cash += float(pos["exit_value"])
            open_positions = []
            cash_available += released_cash
            equity = round(cash_available, 2)

        return executed, {
            "candidate_trade_count": len(sorted_trades),
            "executed_trade_count": len(executed),
            "not_executed_portfolio": max(len(sorted_trades) - len(executed), 0),
            "skipped_no_cash": skipped_no_cash,
            "skipped_no_slots": skipped_no_slots,
            "initial_portfolio_value": round(portfolio_value, 2),
            "ending_portfolio_value": round(equity, 2),
        }

    def _make_run_id(self, symbols: list[str], start: str, end: str) -> str:
        """Generate a unique execution ID (UUID-based, 12 hex chars).

        Every backtest execution gets a fresh run_id — results are append-only.
        Use _make_param_signature() for deterministic grouping/comparison.
        """
        return uuid.uuid4().hex[:12]

    def _make_param_signature(self, symbols: list[str], start: str, end: str) -> str:
        """Deterministic hash of params + scope (SHA-256, 12 hex chars).

        Same params + symbols + date range always produce the same signature.
        Used for grouping and comparison, NOT as the storage identity.
        """
        p = self.params
        cpr_cfg = p.cpr_levels
        fbr_cfg = p.fbr
        vcpr_cfg = p.virgin_cpr
        key = {
            "symbols": sorted(symbols),
            "start": start,
            "end": end,
            "version": STRATEGY_VERSION,
            "cpr_percentile": p.cpr_percentile,
            "cpr_max_width_pct": p.cpr_max_width_pct,
            "atr_periods": p.atr_periods,
            "buffer_pct": p.buffer_pct,
            "rvol_threshold": p.rvol_threshold,
            "min_sl_atr_ratio": p.min_sl_atr_ratio,
            "max_sl_atr_ratio": p.max_sl_atr_ratio,
            "rr_ratio": p.rr_ratio,
            "breakeven_r": p.breakeven_r,
            "capital": p.capital,
            "risk_pct": p.risk_pct,
            "portfolio_value": p.portfolio_value,
            "max_positions": p.max_positions,
            "max_position_pct": p.max_position_pct,
            "risk_based_sizing": p.risk_based_sizing,
            "entry_window_end": p.entry_window_end,
            "time_exit": p.time_exit,
            "short_open_to_cpr_atr_min": p.short_open_to_cpr_atr_min,
            "skip_rvol": p.skip_rvol_check,
            "runtime_batch_size": p.runtime_batch_size,
            "atr_sl_buffer": p.atr_sl_buffer,
            "direction_filter": p.direction_filter,
            "fbr_setup_filter": p.fbr_setup_filter,
            "or_atr_min": p.or_atr_min,
            "or_atr_max": p.or_atr_max,
            "max_gap_pct": p.max_gap_pct,
            "long_max_gap_pct": p.long_max_gap_pct,
            "min_price": p.min_price,
            "or_minutes": p.or_minutes,
            "strategy": p.strategy,
            "failure_window": fbr_cfg.failure_window,
            "reversal_buffer_pct": fbr_cfg.reversal_buffer_pct,
            "fbr_min_or_atr": fbr_cfg.fbr_min_or_atr,
            "fbr_failure_depth": fbr_cfg.fbr_failure_depth,
            "fbr_entry_window_end": fbr_cfg.fbr_entry_window_end,
            "cpr_shift_filter": cpr_cfg.cpr_shift_filter,
            "min_effective_rr": cpr_cfg.min_effective_rr,
            "cpr_use_narrowing_filter": cpr_cfg.use_narrowing_filter,
            "fbr_use_narrowing_filter": fbr_cfg.use_narrowing_filter,
            "vcpr_confirm_candles": vcpr_cfg.vcpr_confirm_candles,
            "vcpr_body_pct": vcpr_cfg.vcpr_body_pct,
            "vcpr_sl_mode": vcpr_cfg.vcpr_sl_mode,
            "candle_exit": vcpr_cfg.candle_exit,
            "vcpr_scan_end": vcpr_cfg.vcpr_scan_end,
            "vcpr_min_open_dist_atr": vcpr_cfg.vcpr_min_open_dist_atr,
            "vcpr_scan_start": vcpr_cfg.vcpr_scan_start,
            "cpr_entry_start": cpr_cfg.cpr_entry_start,
            "cpr_confirm_entry": cpr_cfg.cpr_confirm_entry,
            "cpr_hold_confirm": cpr_cfg.cpr_hold_confirm,
            "cpr_min_close_atr": cpr_cfg.cpr_min_close_atr,
            "scale_out_pct": cpr_cfg.scale_out_pct,
            "commission_model": p.commission_model,
            "slippage_bps": p.slippage_bps,
            "compound_equity": p.compound_equity,
        }
        digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:12]
        return digest

    # ------------------------------------------------------------------
    # Batch data fetch — ONE DuckDB query for ALL symbols
    # ------------------------------------------------------------------

    @staticmethod
    def _iter_symbol_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
        """Split symbols into fixed-size batches, preserving order."""
        size = max(1, int(batch_size))
        if not symbols:
            return []
        return [symbols[i : i + size] for i in range(0, len(symbols), size)]

    def _format_runtime_coverage_error(
        self,
        *,
        requested_symbols: list[str],
        missing_state: list[str],
        missing_strategy: list[str],
        missing_pack: list[str],
    ) -> str:
        """Build a strict and actionable runtime coverage validation error message."""
        lines = [
            (
                "Runtime table coverage is incomplete for "
                f"{len(requested_symbols)} requested symbol(s)."
            )
        ]
        if missing_state:
            lines.append(
                "- market_day_state missing "
                f"{len(missing_state)} symbol(s): "
                f"{preview_list(missing_state)}"
            )
        if missing_pack:
            lines.append(
                "- intraday_day_pack missing "
                f"{len(missing_pack)} symbol(s): "
                f"{preview_list(missing_pack)}"
            )
        if missing_strategy:
            lines.append(
                "- strategy_day_state missing "
                f"{len(missing_strategy)} symbol(s): "
                f"{preview_list(missing_strategy)}"
            )
        lines.append(
            "Run full runtime materialization before backtesting: "
            "`uv run pivot-build --force --batch-size 128`"
        )
        return "\n".join(lines)

    def _or_columns_for_minutes(self, or_minutes: int) -> tuple[str, str, str]:
        """Return precomputed OR high/low/close columns for the selected OR window."""
        mapping = {
            5: ("or_high_5", "or_low_5", "or_close_5"),
            10: ("or_high_10", "or_low_10", "or_close_10"),
            15: ("or_high_15", "or_low_15", "or_close_15"),
            30: ("or_high_30", "or_low_30", "or_close_30"),
        }
        cols = mapping.get(or_minutes)
        if cols is None:
            raise ValueError(f"Unsupported or_minutes={or_minutes}. Supported: 5, 10, 15, 30")
        return cols

    def _strategy_columns_for_minutes(self, or_minutes: int) -> tuple[str, str]:
        """Return strategy_day_state direction/or_atr columns for selected OR window."""
        mapping = {
            5: ("direction_5", "or_atr_5"),
            10: ("direction_10", "or_atr_10"),
            15: ("direction_15", "or_atr_15"),
            30: ("direction_30", "or_atr_30"),
        }
        cols = mapping.get(or_minutes)
        if cols is None:
            raise ValueError(f"Unsupported or_minutes={or_minutes}. Supported: 5, 10, 15, 30")
        return cols

    def _count_setup_funnel(self, symbols: list[str], start: str, end: str) -> FunnelCounts:
        """Run staged COUNT queries to measure how many symbol-days pass each filter.

        Uses conditional aggregation in a single SQL query for CPR_LEVELS and FBR.
        VIRGIN_CPR is not supported yet (returns a minimal FunnelCounts).
        """
        p = self.params
        strategy = p.strategy
        funnel = FunnelCounts(strategy=strategy)

        if not symbols:
            return funnel

        if strategy == "VIRGIN_CPR":
            # VIRGIN_CPR has a different filter pipeline; skip for now.
            return funnel

        sym_param = list(symbols)
        direction_col, or_atr_col = self._strategy_columns_for_minutes(p.or_minutes)
        cpr_cfg = p.cpr_levels
        fbr_cfg = p.fbr

        use_narrowing = (
            cpr_cfg.use_narrowing_filter
            if strategy == "CPR_LEVELS"
            else fbr_cfg.use_narrowing_filter
        )
        or_atr_min = (
            max(p.or_atr_min, fbr_cfg.fbr_min_or_atr) if strategy == "FBR" else p.or_atr_min
        )
        regime_close_col = regime_snapshot_close_col(p.regime_snapshot_minutes)

        # Build the CPR shift clause — only applies to CPR_LEVELS
        shift_clause = "1"  # always true for FBR
        if strategy == "CPR_LEVELS":
            shift_clause = "($cpr_shift_filter = 'ALL' OR m.cpr_shift = $cpr_shift_filter)"

        regime_move_expr = (
            f"CASE WHEN reg.open_915 > 0 AND reg.{regime_close_col} IS NOT NULL "
            f"THEN ((reg.{regime_close_col} - reg.open_915) / reg.open_915) * 100.0 END"
        )
        regime_gate_sql = f"""
                     AND (
                         $regime_index_symbol = ''
                         OR $regime_min_move_pct <= 0
                         OR (
                             {regime_move_expr} IS NOT NULL
                             AND NOT (
                                 (s.{direction_col} = 'SHORT' AND {regime_move_expr} >= $regime_min_move_pct)
                                 OR (s.{direction_col} = 'LONG' AND {regime_move_expr} <= -$regime_min_move_pct)
                             )
                         )
                     )
        """

        query = f"""
            SELECT
                COUNT(*) AS universe_count,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                    THEN 1 ELSE 0
                END) AS after_cpr_width,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                    THEN 1 ELSE 0
                END) AS after_direction,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                    THEN 1 ELSE 0
                END) AS after_dir_filter,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                     AND ($min_price <= 0 OR m.prev_close >= $min_price)
{regime_gate_sql}
                    THEN 1 ELSE 0
                END) AS after_min_price,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                     AND ($min_price <= 0 OR m.prev_close >= $min_price)
                     AND s.gap_abs_pct <= $max_gap
{regime_gate_sql}
                    THEN 1 ELSE 0
                END) AS after_gap,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                     AND ($min_price <= 0 OR m.prev_close >= $min_price)
                     AND s.gap_abs_pct <= $max_gap
                     AND s.{or_atr_col} >= $or_atr_min
                     AND s.{or_atr_col} <= $or_atr_max
{regime_gate_sql}
                    THEN 1 ELSE 0
                END) AS after_or_atr,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                     AND ($min_price <= 0 OR m.prev_close >= $min_price)
                     AND s.gap_abs_pct <= $max_gap
                     AND s.{or_atr_col} >= $or_atr_min
                     AND s.{or_atr_col} <= $or_atr_max
                     AND (NOT $use_narrowing OR CAST(m.is_narrowing AS INTEGER) = 1)
{regime_gate_sql}
                    THEN 1 ELSE 0
                END) AS after_narrowing,
                SUM(CASE
                    WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                     AND s.{direction_col} IN ('LONG', 'SHORT')
                     AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                     AND ($min_price <= 0 OR m.prev_close >= $min_price)
                     AND s.gap_abs_pct <= $max_gap
                     AND s.{or_atr_col} >= $or_atr_min
                     AND s.{or_atr_col} <= $or_atr_max
                     AND (NOT $use_narrowing OR CAST(m.is_narrowing AS INTEGER) = 1)
                     AND ({shift_clause})
{regime_gate_sql}
                    THEN 1 ELSE 0
                END) AS after_shift
            FROM market_day_state m
            JOIN strategy_day_state s
              ON s.symbol = m.symbol AND s.trade_date = m.trade_date
            LEFT JOIN market_day_state reg
              ON reg.symbol = $regime_index_symbol
             AND reg.trade_date = m.trade_date
            WHERE list_contains($symbols, m.symbol)
              AND m.trade_date >= $start::DATE
              AND m.trade_date <= $end::DATE
        """

        params_dict: dict[str, object] = {
            "start": start,
            "end": end,
            "max_width": p.cpr_max_width_pct,
            "direction_filter": p.direction_filter,
            "min_price": p.min_price,
            "max_gap": p.max_gap_pct,
            "or_atr_min": or_atr_min,
            "or_atr_max": p.or_atr_max,
            "use_narrowing": use_narrowing,
            "symbols": sym_param,
            "regime_index_symbol": str(p.regime_index_symbol or "").upper(),
            "regime_min_move_pct": float(p.regime_min_move_pct or 0.0),
        }
        if strategy == "CPR_LEVELS":
            params_dict["cpr_shift_filter"] = cpr_cfg.cpr_shift_filter

        try:
            row = self.db.con.execute(query, params_dict).fetchone()
        except Exception as e:
            logger.warning("Setup funnel query failed: %s", e)
            return funnel

        if row:
            funnel.universe_count = int(row[0] or 0)
            funnel.after_cpr_width = int(row[1] or 0)
            funnel.after_direction = int(row[2] or 0)
            funnel.after_dir_filter = int(row[3] or 0)
            funnel.after_min_price = int(row[4] or 0)
            funnel.after_gap = int(row[5] or 0)
            funnel.after_or_atr = int(row[6] or 0)
            funnel.after_narrowing = int(row[7] or 0)
            funnel.after_shift = int(row[8] or 0)

        return funnel

    def _get_all_setups_batch(self, symbols: list[str], start: str, end: str) -> pl.DataFrame:
        """Load all setup rows from market_day_state (no raw-candle fallback path)."""
        if not symbols:
            return pl.DataFrame()

        p = self.params
        cpr_cfg = p.cpr_levels
        fbr_cfg = p.fbr
        vcpr_cfg = p.virgin_cpr
        strategy = p.strategy
        sym_param = list(symbols)

        if strategy == "VIRGIN_CPR":
            query = """
                SELECT
                    m.symbol,
                    m.trade_date::VARCHAR AS trade_date,
                    m.tc,
                    m.bc,
                    m."pivot",
                    m.cpr_width_pct,
                    m.atr,
                    m.prev_close AS prev_day_close,
                    m.prev_is_virgin
                FROM market_day_state m
                JOIN strategy_day_state s
                  ON s.symbol = m.symbol
                 AND s.trade_date = m.trade_date
                WHERE list_contains($symbols, m.symbol)
                  AND m.trade_date >= $start::DATE
                  AND m.trade_date <= $end::DATE
                  AND m.prev_is_virgin = TRUE
                  AND ($min_price <= 0 OR m.prev_close >= $min_price)
                  AND s.gap_abs_pct <= $max_gap
                  AND s.open_side IN ('ABOVE', 'BELOW')
                  AND (
                      $direction_filter = 'BOTH'
                      OR ($direction_filter = 'LONG' AND s.open_side = 'BELOW')
                      OR ($direction_filter = 'SHORT' AND s.open_side = 'ABOVE')
                  )
                  AND ($vcpr_open_dist <= 0 OR s.open_to_cpr_atr >= $vcpr_open_dist)
                ORDER BY m.symbol, m.trade_date
            """
            try:
                return self.db.con.execute(
                    query,
                    {
                        "start": start,
                        "end": end,
                        "min_price": p.min_price,
                        "max_gap": p.max_gap_pct,
                        "direction_filter": p.direction_filter,
                        "vcpr_open_dist": vcpr_cfg.vcpr_min_open_dist_atr,
                        "symbols": sym_param,
                    },
                ).pl()
            except Exception as e:
                logger.error("Batch VIRGIN_CPR setup query failed: %s", e, exc_info=True)
                return pl.DataFrame()

        high_col, low_col, close_col = self._or_columns_for_minutes(p.or_minutes)
        direction_col, or_atr_col = self._strategy_columns_for_minutes(p.or_minutes)
        regime_close_col = regime_snapshot_close_col(p.regime_snapshot_minutes)

        strategy_filter_sql = ""
        if strategy == "CPR_LEVELS":
            strategy_filter_sql = (
                "\n              AND ($cpr_shift_filter = 'ALL' OR cpr_shift = $cpr_shift_filter)"
            )

        query = f"""
            WITH base AS (
                SELECT
                    m.symbol,
                    m.trade_date,
                    m.prev_close AS prev_day_close,
                    m.tc,
                    m.bc,
                    m."pivot",
                    m.cpr_width_pct,
                    m.cpr_threshold_pct AS cpr_threshold,
                    m.r1,
                    m.s1,
                    m.r2,
                    m.s2,
                    m.r3,
                    m.s3,
                    m.cpr_shift,
                    m.is_narrowing,
                    m.atr,
                    m.open_915,
                    s.open_to_cpr_atr,
                    m.volume_915,
                    m.{high_col} AS high_915,
                    m.{low_col} AS low_915,
                    m.{close_col} AS close_915,
                    s.{direction_col} AS direction_915,
                    s.{or_atr_col} AS or_atr_915,
                    s.gap_abs_pct,
                    CASE
                        WHEN reg.open_915 > 0 AND reg.{regime_close_col} IS NOT NULL
                        THEN ((reg.{regime_close_col} - reg.open_915) / reg.open_915) * 100.0
                        ELSE NULL
                    END AS regime_move_pct
                FROM market_day_state m
                JOIN strategy_day_state s
                  ON s.symbol = m.symbol
                 AND s.trade_date = m.trade_date
                LEFT JOIN market_day_state reg
                  ON reg.symbol = $regime_index_symbol
                 AND reg.trade_date = m.trade_date
                WHERE list_contains($symbols, m.symbol)
                  AND m.trade_date >= $start::DATE
                  AND m.trade_date <= $end::DATE
            ),
            with_direction AS (
                SELECT
                    *,
                    direction_915 AS direction
                FROM base
                WHERE cpr_width_pct < LEAST(cpr_threshold, $max_width)
            )
            SELECT
                symbol,
                trade_date::VARCHAR AS trade_date,
                high_915,
                low_915,
                close_915,
                open_915,
                volume_915,
                tc,
                bc,
                "pivot",
                cpr_width_pct,
                r1,
                s1,
                r2,
                s2,
                r3,
                s3,
                cpr_shift,
                is_narrowing,
                cpr_threshold,
                atr,
                prev_day_close,
                open_to_cpr_atr,
                direction,
                regime_move_pct
            FROM with_direction
            WHERE direction IN ('LONG', 'SHORT')
              AND ($direction_filter = 'BOTH' OR direction = $direction_filter)
              AND ($min_price <= 0 OR prev_day_close >= $min_price)
              AND gap_abs_pct <= $max_gap
              AND or_atr_915 >= $or_atr_min
              AND or_atr_915 <= $or_atr_max
              AND (NOT $use_narrowing OR CAST(is_narrowing AS INTEGER) = 1){strategy_filter_sql}
              AND (
                  $regime_index_symbol = ''
                  OR $regime_min_move_pct <= 0
                  OR (
                      regime_move_pct IS NOT NULL
                      AND NOT (
                          (direction = 'SHORT' AND regime_move_pct >= $regime_min_move_pct)
                          OR (direction = 'LONG' AND regime_move_pct <= -$regime_min_move_pct)
                      )
                  )
              )
            ORDER BY symbol, trade_date
        """

        params_dict: dict[str, object] = {
            "start": start,
            "end": end,
            "max_width": p.cpr_max_width_pct,
            "direction_filter": p.direction_filter,
            "min_price": p.min_price,
            "max_gap": p.max_gap_pct,
            "or_atr_min": max(p.or_atr_min, fbr_cfg.fbr_min_or_atr)
            if strategy == "FBR"
            else p.or_atr_min,
            "or_atr_max": p.or_atr_max,
            "use_narrowing": cpr_cfg.use_narrowing_filter
            if strategy == "CPR_LEVELS"
            else fbr_cfg.use_narrowing_filter,
            "symbols": sym_param,
            "regime_index_symbol": str(p.regime_index_symbol or "").upper(),
            "regime_min_move_pct": float(p.regime_min_move_pct or 0.0),
        }
        if strategy == "CPR_LEVELS":
            params_dict["cpr_shift_filter"] = cpr_cfg.cpr_shift_filter

        try:
            return self.db.con.execute(query, params_dict).pl()
        except Exception as e:
            print(f"  ERROR in batch setup query ({strategy}): {e}")
            return pl.DataFrame()

    @staticmethod
    def _minute_to_time_str(minute_of_day: int | float | str) -> str:
        """Convert minute-of-day integer (e.g. 555) to HH:MM (09:15)."""
        total = int(minute_of_day)
        return f"{total // 60:02d}:{total % 60:02d}"

    def _resolve_pack_time_mode(self) -> str:
        """Return `minute_arr` for compact day-pack schema, else legacy `time_arr`."""
        if self._uses_compact_pack_schema is None:
            has_column = getattr(self.db, "_table_has_column", None)
            use_compact = False
            if callable(has_column):
                try:
                    use_compact = bool(has_column("intraday_day_pack", "minute_arr"))
                except Exception as e:
                    logger.debug("Failed to probe compact day-pack schema: %s", e)
                    use_compact = False
            self._uses_compact_pack_schema = use_compact
        return "minute_arr" if self._uses_compact_pack_schema else "time_arr"

    def _prefetch_day_pack_batch(self, setups: pl.DataFrame) -> dict[str, dict[str, DayPack]]:
        """Fetch intraday day-pack rows for setup dates and convert to runtime DayPack dicts."""
        if setups.is_empty():
            return {}

        include_rvol = not self.params.skip_rvol_check
        pack_time_mode = self._resolve_pack_time_mode()
        pack_time_select = (
            "p.minute_arr AS pack_time_arr"
            if pack_time_mode == "minute_arr"
            else "p.time_arr AS pack_time_arr"
        )
        setup_days = setups.select(["symbol", "trade_date"]).unique(maintain_order=True)
        # Date bounds let DuckDB zone-map prune intraday_day_pack (10yr table, scan only the
        # relevant window instead of all 3.3M rows).
        min_date = setup_days["trade_date"].min()
        max_date = setup_days["trade_date"].max()
        self.db.con.register("_setup_days", setup_days.to_arrow())
        try:
            rvol_select = (
                "p.rvol_baseline_arr" if include_rvol else "NULL::DOUBLE[] AS rvol_baseline_arr"
            )
            query = """
                SELECT
                    p.symbol,
                    p.trade_date::VARCHAR AS trade_date,
                    {pack_time_select},
                    p.open_arr,
                    p.high_arr,
                    p.low_arr,
                    p.close_arr,
                    p.volume_arr,
                    {rvol_select}
                FROM intraday_day_pack p
                JOIN _setup_days s
                  ON s.symbol = p.symbol
                 AND s.trade_date::DATE = p.trade_date
                WHERE p.trade_date >= $min_date::DATE
                  AND p.trade_date <= $max_date::DATE
                ORDER BY p.symbol, p.trade_date
            """
            pack_df = self.db.con.execute(
                query.format(rvol_select=rvol_select, pack_time_select=pack_time_select),
                {"min_date": str(min_date), "max_date": str(max_date)},
            ).pl()
        except Exception as e:
            print(f"  ERROR in day-pack fetch: {e}")
            return {}
        finally:
            self.db.con.unregister("_setup_days")

        if pack_df.is_empty():
            return {}

        candles_by_sym: dict[str, dict[str, DayPack]] = {}

        # Column-level extraction (avoid per-row dict allocation via iter_rows)
        symbols_col = pack_df["symbol"].to_list()
        dates_col = pack_df["trade_date"].to_list()
        pack_time_arrs = pack_df["pack_time_arr"].to_list()
        open_arrs = pack_df["open_arr"].to_list()
        high_arrs = pack_df["high_arr"].to_list()
        low_arrs = pack_df["low_arr"].to_list()
        close_arrs = pack_df["close_arr"].to_list()
        volume_arrs = pack_df["volume_arr"].to_list()
        rvol_arrs = pack_df["rvol_baseline_arr"].to_list() if include_rvol else None

        for idx in range(pack_df.height):
            sym = symbols_col[idx]
            trade_date = str(dates_col[idx])[:10]

            raw_times = pack_time_arrs[idx]
            if not raw_times:
                continue
            if pack_time_mode == "minute_arr":
                try:
                    times = [self._minute_to_time_str(t) for t in raw_times]
                except (TypeError, ValueError):
                    continue
            else:
                times = [str(t) for t in raw_times]
            opens = list(open_arrs[idx] or [])
            highs = list(high_arrs[idx] or [])
            lows = list(low_arrs[idx] or [])
            closes = list(close_arrs[idx] or [])
            volumes = list(volume_arrs[idx] or [])

            if not (
                len(times) == len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)
            ):
                continue

            baselines: list[float | None] | None = None
            if include_rvol and rvol_arrs is not None:
                raw_baselines = list(rvol_arrs[idx] or [])
                baselines = [float(v) if v is not None else None for v in raw_baselines]

            day_pack = DayPack(
                time_str=times,
                opens=[float(x) for x in opens],
                highs=[float(x) for x in highs],
                lows=[float(x) for x in lows],
                closes=[float(x) for x in closes],
                volumes=[float(x) for x in volumes],
                rvol_baseline=baselines,
            )
            candles_by_sym.setdefault(sym, {})[trade_date] = day_pack

        for sym in setups["symbol"].unique().to_list():
            candles_by_sym.setdefault(sym, {})
        return candles_by_sym

    def _simulate_from_preloaded(
        self,
        symbol: str,
        run_id: str,
        setups: pl.DataFrame,
        candles_by_date: dict[str, DayPack],
    ) -> list[TradeResult]:
        """
        Inner simulation loop using pre-loaded data — no DB queries.

        This is the hot path after batch fetch.  Each day's simulation is the
        same as before; only the data-loading overhead has been eliminated.
        """
        strategy = self.params.strategy
        if strategy == "VIRGIN_CPR":
            simulate = self._simulate_day_virgin_cpr
        elif strategy == "CPR_LEVELS":
            simulate = self._simulate_day_cpr_levels
        elif strategy == "FBR":
            simulate = self._simulate_day_fbr
        else:
            raise ValueError(
                f"Unknown strategy '{strategy}'. Choose CPR_LEVELS, FBR, or VIRGIN_CPR."
            )

        trades: list[TradeResult] = []
        columns = setups.columns
        for values in setups.iter_rows(named=False):
            row = {columns[i]: values[i] for i in range(len(columns))}
            trade_date = str(row["trade_date"])[:10]
            day_pack = candles_by_date.get(trade_date)
            if day_pack is None or not day_pack.time_str:
                continue
            trade = simulate(symbol, run_id, row, day_pack)
            if trade:
                trades.append(trade)
        return trades

    def _simulate_cpr_levels_batch(
        self,
        *,
        run_id: str,
        batch_symbols: list[str],
        setups_by_sym: dict[str, pl.DataFrame],
        candles_by_sym: dict[str, dict[str, DayPack]],
    ) -> dict[str, list[TradeResult]]:
        """Run CPR_LEVELS with session-style bar ordering across all symbols.

        The historical paper/live paths process bars in session order so a symbol
        can be skipped on an early bar and still enter later when cash is freed.
        This helper mirrors that behavior for backtest parity.
        """

        trades_by_symbol: dict[str, list[TradeResult]] = {symbol: [] for symbol in batch_symbols}
        if not batch_symbols:
            return trades_by_symbol

        max_positions = max(1, int(self.params.max_positions or 1))
        portfolio_value = float(self.params.portfolio_value or 0.0)
        entry_window_end = str(self.params.entry_window_end)

        grouped_by_date: dict[str, list[tuple[str, dict[str, Any], DayPack]]] = {}
        for symbol in batch_symbols:
            setups = setups_by_sym.get(symbol)
            if setups is None or setups.is_empty():
                continue
            symbol_candles = candles_by_sym.get(symbol, {})
            for values in setups.iter_rows(named=False):
                columns = setups.columns
                setup_row = {columns[i]: values[i] for i in range(len(columns))}
                trade_date = str(setup_row.get("trade_date") or "")[:10]
                day_pack = symbol_candles.get(trade_date)
                if day_pack is None or not day_pack.time_str:
                    continue
                grouped_by_date.setdefault(trade_date, []).append((symbol, setup_row, day_pack))

        for trade_date in sorted(grouped_by_date):
            day_rows = grouped_by_date[trade_date]
            if not day_rows:
                continue

            rows_by_symbol: dict[str, tuple[dict[str, Any], DayPack]] = {
                symbol: (setup_row, day_pack) for symbol, setup_row, day_pack in day_rows
            }
            all_times = sorted(
                {time_str for _, _, day_pack in day_rows for time_str in day_pack.time_str}
            )
            tracker = SessionPositionTracker(
                max_positions=max_positions,
                portfolio_value=portfolio_value,
                max_position_pct=float(self.params.max_position_pct or 0.0),
            )
            pending_closings: list[dict[str, Any]] = []

            for bar_time in all_times:
                if pending_closings:
                    remaining: list[dict[str, Any]] = []
                    for close in pending_closings:
                        if str(close["exit_time"]) <= bar_time:
                            tracker.record_close(str(close["symbol"]), float(close["exit_value"]))
                        else:
                            remaining.append(close)
                    pending_closings = remaining
                capital_base = tracker.current_equity() if self.params.compound_equity else None

                candidates: list[dict[str, Any]] = []
                evaluated = 0
                skipped_reasons: dict[str, int] = {}
                for symbol, setup_row, day_pack in day_rows:
                    if tracker.has_open_position(symbol) or tracker.has_traded_today(symbol):
                        continue
                    setup_status = (
                        "candidate"
                        if str(setup_row.get("direction") or "").upper() in {"LONG", "SHORT"}
                        else "rejected"
                    )
                    if not should_process_symbol(
                        bar_time=bar_time,
                        entry_window_end=entry_window_end,
                        tracker=tracker,
                        symbol=symbol,
                        setup_status=setup_status,
                    ):
                        continue
                    current_idx = day_pack.index_of(bar_time)
                    if current_idx < 0:
                        continue
                    evaluated += 1
                    candidate = scan_cpr_levels_entry(
                        day_pack=day_pack,
                        setup_row=setup_row,
                        params=self.params,
                        scan_start_idx=current_idx,
                        scan_end_idx=current_idx,
                        capital_base=capital_base,
                    )
                    if candidate is None:
                        reason_val = getattr(_cpr_reject_reason, "value", "UNKNOWN") or "UNKNOWN"
                        skipped_reasons[reason_val] = skipped_reasons.get(reason_val, 0) + 1
                        continue
                    candidate = dict(candidate)
                    candidate["symbol"] = symbol
                    candidate["setup_row"] = setup_row
                    candidates.append(candidate)

                if evaluated > 0 and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "CPR batch %s bar=%s evaluated=%d candidates=%d occupancy=%d/%d reject=%s",
                        trade_date,
                        bar_time,
                        evaluated,
                        len(candidates),
                        tracker.open_count,
                        max_positions,
                        skipped_reasons or "none",
                    )

                selected_entries = select_entries_for_bar(candidates, tracker)
                for selected in selected_entries:
                    symbol = str(selected.get("symbol") or "")
                    setup_row, day_pack = rows_by_symbol[symbol]
                    if self.params.compound_equity and self.params.risk_based_sizing:
                        # Keep the raw risk-sized quantity for the overlay pass.
                        # Compound risk needs the later portfolio pass to apply the
                        # growing equity/slot cap; pre-clamping here would freeze
                        # the quantity at the daily-reset slot size.
                        actual_qty = max(1, int(selected["position_size"]))
                    else:
                        actual_qty = tracker.compute_position_qty(
                            entry_price=float(selected["entry_price"]),
                            risk_based_sizing=self.params.risk_based_sizing,
                            candidate_size=int(selected["position_size"]),
                            capital_base=capital_base,
                        )
                    if actual_qty < 1:
                        continue
                    trade = self._simulate_trade(
                        day_pack=day_pack,
                        start_idx=int(selected["entry_idx"]) + 1,
                        entry_price=float(selected["entry_price"]),
                        sl_price=float(selected["sl_price"]),
                        target_price=float(selected["target_price"]),
                        runner_target_price=(
                            float(selected["runner_target_price"])
                            if selected.get("runner_target_price")
                            else None
                        ),
                        direction=str(selected["direction"]),
                        sl_distance=float(selected["sl_distance"]),
                        atr=float(setup_row["atr"]),
                        position_size=actual_qty,
                        run_id=run_id,
                        symbol=symbol,
                        trade_date=trade_date,
                        entry_time=str(selected["entry_time"]),
                        cpr_width_pct=float(selected["cpr_width_pct"]),
                        cpr_threshold=float(selected["cpr_threshold"]),
                        rvol=float(selected["rvol"]),
                        or_atr_ratio=float(selected["or_atr_ratio"]),
                        gap_pct=float(selected["gap_pct"]),
                        rr_ratio=float(selected["rr_ratio"]),
                    )
                    trades_by_symbol[symbol].append(trade)
                    position_value = round(float(trade.position_value), 2)
                    tracker.record_open(
                        SimpleNamespace(
                            position_id=f"{run_id}:{trade_date}:{symbol}:{trade.entry_time}",
                            symbol=symbol,
                            direction=str(trade.direction),
                            entry_price=float(trade.entry_price),
                            stop_loss=float(trade.sl_price),
                            target_price=float(trade.target_price),
                            trail_state={"entry_time": trade.entry_time},
                            quantity=float(trade.position_size),
                            current_qty=float(trade.position_size),
                        ),
                        position_value=position_value,
                    )
                    pending_closings.append(
                        {
                            "symbol": symbol,
                            "exit_time": trade.exit_time or self.params.time_exit,
                            "exit_value": round(position_value + float(trade.gross_pnl), 2),
                        }
                    )

            for close in pending_closings:
                tracker.record_close(str(close["symbol"]), float(close["exit_value"]))

        return trades_by_symbol

    def _normalize_sl(
        self, entry_price: float, sl_price: float, direction: str, atr: float
    ) -> tuple[float, float] | None:
        """Apply ATR guardrails to SL distance and normalize stop side by direction."""
        p = self.params
        return normalize_stop_loss(
            entry_price=entry_price,
            sl_price=sl_price,
            direction=direction,
            atr=atr,
            min_sl_atr_ratio=p.min_sl_atr_ratio,
            max_sl_atr_ratio=p.max_sl_atr_ratio,
        )

    @staticmethod
    def _find_first_close_idx(
        day_pack: DayPack,
        start_idx: int,
        end_idx: int,
        *,
        direction: str,
        trigger: float,
    ) -> int:
        """Find first candle index whose close crosses trigger in direction; -1 if absent."""
        return shared_find_first_close_idx(
            day_pack.closes,
            start_idx,
            end_idx,
            direction=direction,
            trigger=trigger,
        )

    # ------------------------------------------------------------------
    # Strategy simulation methods
    # ------------------------------------------------------------------

    def _simulate_day_cpr_levels(
        self,
        symbol: str,
        run_id: str,
        setup_row: dict,
        day_pack: DayPack,
    ) -> TradeResult | None:
        """
        CPR_LEVELS strategy: normalized CPR-band touch entry with CPR-zone SL and R1/S1 target.

        Entry logic (replaces ORB breakout):
            - LONG:  enter when a 5-min candle closes above the upper CPR boundary + buffer
            - SHORT: enter when a 5-min candle closes below the lower CPR boundary - buffer
            - Entry scan starts at 09:15 (the signal candle itself can trigger entry)

        Risk/reward:
            - SL at the opposite CPR boundary ± ATR buffer
            - Target at R1 (LONG) or S1 (SHORT) — institutional floor pivot
            - On narrow CPR days (CPR width ~0.3–0.5%), effective RR ≈ 3–6:1
            - Dynamic effective RR is passed to TrailingStop for phase transitions

        OR window data (high_915, low_915) is still used for or_atr_ratio diagnostic
        and gap_pct filter — unchanged.
        """
        p = self.params
        cpr_cfg = p.cpr_levels
        trade_date = setup_row["trade_date"]
        direction = setup_row["direction"]
        atr = float(setup_row["atr"])

        # Entry scan starts after the direction-defining OR candle completes.
        # or_minutes=5 → direction set by 09:15 close → scan from 09:20.
        # Explicit override via cpr_entry_start takes precedence.
        entry_start = get_cpr_entry_scan_start(p.or_minutes, cpr_cfg.cpr_entry_start)
        _cpr_width_pct = float(setup_row["cpr_width_pct"])
        _cpr_threshold = float(setup_row["cpr_threshold"])

        or_high = float(setup_row["high_915"])
        or_low = float(setup_row["low_915"])
        open_915 = float(setup_row["open_915"])

        # OR/ATR ratio filter (diagnostic — kept for consistency with other strategies)
        _or_atr_ratio = calculate_or_atr_ratio(or_high, or_low, atr)
        if _or_atr_ratio < p.or_atr_min or _or_atr_ratio > p.or_atr_max:
            return None

        # Gap filter
        prev_close = setup_row.get("prev_day_close")
        _gap_pct = calculate_gap_pct(open_915, prev_close)
        if abs(_gap_pct) > p.max_gap_for_direction(direction):
            return None
        if direction == "SHORT" and p.short_open_to_cpr_atr_min > 0:
            if float(setup_row.get("open_to_cpr_atr") or 0.0) < p.short_open_to_cpr_atr_min:
                return None

        # CPR band touch entry — replaces ORB breakout trigger
        tc = float(setup_row["tc"])
        bc = float(setup_row["bc"])
        cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
        atr_buffer = p.atr_sl_buffer * atr

        if direction == "LONG":
            trigger = cpr_upper * (1.0 + p.buffer_pct)
            sl_price = cpr_lower - atr_buffer
            target_price = float(setup_row["r1"])
            runner_target_price = (
                float(setup_row["r2"])
                if cpr_cfg.scale_out_pct > 0 and float(setup_row["r2"]) > target_price
                else 0.0
            )
        else:
            trigger = cpr_lower * (1.0 - p.buffer_pct)
            sl_price = cpr_upper + atr_buffer
            target_price = float(setup_row["s1"])
            runner_target_price = (
                float(setup_row["s2"])
                if cpr_cfg.scale_out_pct > 0 and float(setup_row["s2"]) < target_price
                else 0.0
            )

        # Validate: target must be beyond entry (R1 > entry for LONG, S1 < entry for SHORT)
        if direction == "LONG" and target_price <= trigger:
            return None  # R1 already behind the entry point
        if direction == "SHORT" and target_price >= trigger:
            return None  # S1 already behind the entry point

        # Scan the CPR window candle-by-candle so backtest matches replay/live.
        scan_start_idx, scan_end_idx = day_pack.range_indices(entry_start, p.entry_window_end)
        if scan_start_idx < 0:
            return None

        capital_base = float(p.portfolio_value or 0.0) if p.compound_equity else None
        entry_candidate = scan_cpr_levels_entry(
            day_pack=day_pack,
            setup_row=setup_row,
            params=p,
            scan_start_idx=scan_start_idx,
            scan_end_idx=scan_end_idx,
            capital_base=capital_base,
        )
        if entry_candidate is None:
            return None

        entry_idx = int(entry_candidate["entry_idx"])
        entry_time = str(entry_candidate["entry_time"])
        entry_price = float(entry_candidate["entry_price"])
        sl_price = float(entry_candidate["sl_price"])
        target_price = float(entry_candidate["target_price"])
        runner_target_price = entry_candidate.get("runner_target_price") or 0.0
        if runner_target_price:
            runner_target_price = float(runner_target_price)
        rvol = float(entry_candidate["rvol"])
        effective_rr = float(entry_candidate["rr_ratio"])
        sl_distance = float(entry_candidate["sl_distance"])
        position_size = int(entry_candidate["position_size"])

        # Shared simulation via _simulate_trade().
        return self._simulate_trade(
            day_pack=day_pack,
            start_idx=entry_idx + 1,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            runner_target_price=runner_target_price or None,
            direction=direction,
            sl_distance=sl_distance,
            atr=atr,
            position_size=position_size,
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            cpr_width_pct=float(entry_candidate["cpr_width_pct"]),
            cpr_threshold=float(entry_candidate["cpr_threshold"]),
            rvol=rvol,
            or_atr_ratio=float(entry_candidate["or_atr_ratio"]),
            gap_pct=float(entry_candidate["gap_pct"]),
            rr_ratio=effective_rr,
        )

    # ------------------------------------------------------------------
    # FBR: Failed Breakout Reversal
    # ------------------------------------------------------------------

    def _simulate_day_fbr(
        self,
        symbol: str,
        run_id: str,
        setup_row: dict,
        day_pack: DayPack,
    ) -> TradeResult | None:
        """
        FBR strategy: detect a failed ORB breakout, then trade the reversal.

        Steps:
            1. Same OR observation + breakout detection as ORB
            2. After breakout candle, scan failure_window candles for close back inside OR
            3. If failure → enter OPPOSITE direction at failure close
            4. SL = beyond the failed breakout extreme + ATR buffer
            5. Target = opposite OR side, then trail with ATR
        """
        p = self.params
        fbr_cfg = p.fbr
        trade_date = setup_row["trade_date"]
        breakout_direction = setup_row["direction"]  # Original breakout direction
        atr = float(setup_row["atr"])

        or_candle_count = max(1, p.or_minutes // 5)
        entry_start_minutes = (9 * 60 + 15) + or_candle_count * 5
        entry_start = f"{entry_start_minutes // 60:02d}:{entry_start_minutes % 60:02d}"
        cpr_width_pct = float(setup_row["cpr_width_pct"])
        cpr_threshold = float(setup_row["cpr_threshold"])

        or_high = float(setup_row["high_915"])
        or_low = float(setup_row["low_915"])
        open_915 = float(setup_row["open_915"])

        # OR/ATR ratio filter — FBR needs a meaningful breakout, not noise
        or_atr_ratio = calculate_or_atr_ratio(or_high, or_low, atr)
        fbr_or_min = max(p.or_atr_min, fbr_cfg.fbr_min_or_atr)  # FBR floor is stricter
        if or_atr_ratio < fbr_or_min or or_atr_ratio > p.or_atr_max:
            return None

        # Gap filter
        prev_close = setup_row.get("prev_day_close")
        gap_pct = calculate_gap_pct(open_915, prev_close)
        if abs(gap_pct) > p.max_gap_for_direction(breakout_direction):
            return None

        # --- Phase 1: Find the original breakout ---
        buffer = p.buffer_pct
        if breakout_direction == "LONG":
            trigger = or_high * (1.0 + buffer)
        else:
            trigger = or_low * (1.0 - buffer)

        # FBR uses its own entry window (typically tighter than CPR_LEVELS)
        fbr_window_end = fbr_cfg.fbr_entry_window_end
        scan_start_idx, scan_end_idx = day_pack.range_indices(entry_start, fbr_window_end)
        if scan_start_idx < 0:
            return None  # No breakout → no FBR setup

        breakout_idx = self._find_first_close_idx(
            day_pack,
            scan_start_idx,
            scan_end_idx,
            direction=breakout_direction,
            trigger=trigger,
        )
        if breakout_idx < 0:
            return None  # No breakout → no FBR setup

        # --- Phase 2: Detect failure after breakout ---
        or_range = or_high - or_low
        depth_pts = fbr_cfg.fbr_failure_depth * or_range
        failure_end_idx = min(scan_end_idx, breakout_idx + fbr_cfg.failure_window)

        failure_idx = -1
        entry_price = 0.0
        entry_volume = 0.0
        reversal_direction = ""

        highs = day_pack.highs
        lows = day_pack.lows
        closes = day_pack.closes
        volumes = day_pack.volumes
        times = day_pack.time_str

        if breakout_direction == "LONG":
            extreme = float("-inf")
            for i in range(breakout_idx, failure_end_idx + 1):
                high = float(highs[i])
                close = float(closes[i])
                if high > extreme:
                    extreme = high
                if i > breakout_idx and close < (or_high - depth_pts):
                    failure_idx = i
                    reversal_direction = "SHORT"
                    entry_price = close
                    entry_volume = float(volumes[i])
                    break
        else:
            extreme = float("inf")
            for i in range(breakout_idx, failure_end_idx + 1):
                low = float(lows[i])
                close = float(closes[i])
                if low < extreme:
                    extreme = low
                if i > breakout_idx and close > (or_low + depth_pts):
                    failure_idx = i
                    reversal_direction = "LONG"
                    entry_price = close
                    entry_volume = float(volumes[i])
                    break

        if failure_idx < 0:
            return None  # Breakout held → no reversal

        # --- Phase 3: Enter reversal ---
        entry_time = times[failure_idx]

        # SL = beyond the failed breakout extreme + buffer
        atr_buffer = p.atr_sl_buffer * atr
        if reversal_direction == "SHORT":
            sl_price = extreme + atr_buffer + (fbr_cfg.reversal_buffer_pct * extreme)
        else:
            sl_price = extreme - atr_buffer - (fbr_cfg.reversal_buffer_pct * extreme)

        normalized_sl = self._normalize_sl(
            entry_price=entry_price,
            sl_price=sl_price,
            direction=reversal_direction,
            atr=atr,
        )
        if normalized_sl is None:
            return None
        sl_price, sl_distance = normalized_sl

        # Target: opposite OR side first, then trail
        if reversal_direction == "LONG":
            target_price = entry_price + (p.rr_ratio * sl_distance)
        else:
            target_price = entry_price - (p.rr_ratio * sl_distance)

        capital_base = float(p.portfolio_value or 0.0) if p.compound_equity else None
        risk_capital = float(capital_base) if capital_base is not None else float(p.capital or 0.0)
        position_size = calculate_position_size(risk_capital, p.risk_pct, sl_distance)
        min_notional = minimum_trade_notional_for(
            max_positions=max(1, int(p.max_positions or 1)),
            portfolio_value=float(p.portfolio_value or 0.0),
            max_position_pct=float(p.max_position_pct or 0.0),
            capital_base=capital_base,
        )
        if float(position_size) * float(entry_price) < min_notional:
            return None

        # RVOL check on the failure candle
        avg_vol = day_pack.baseline_for_index(failure_idx)
        rvol = (entry_volume / avg_vol) if avg_vol > 0 else 0.0

        if not p.skip_rvol_check and avg_vol > 0 and rvol < p.rvol_threshold:
            return None

        # Shared simulation via _simulate_trade()
        return self._simulate_trade(
            day_pack=day_pack,
            start_idx=failure_idx + 1,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            direction=reversal_direction,
            sl_distance=sl_distance,
            atr=atr,
            position_size=position_size,
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            cpr_width_pct=cpr_width_pct,
            cpr_threshold=cpr_threshold,
            rvol=rvol,
            or_atr_ratio=or_atr_ratio,
            gap_pct=gap_pct,
        )

    # ------------------------------------------------------------------
    # Virgin CPR Bounce
    # ------------------------------------------------------------------

    def _simulate_day_virgin_cpr(
        self,
        symbol: str,
        run_id: str,
        setup_row: dict,
        day_pack: DayPack,
    ) -> TradeResult | None:
        """
        Virgin CPR Bounce: trade first touch of an untouched CPR zone.

        Steps:
            1. Previous day's CPR was never touched (virgin) — now carried to today
            2. Bias: CPR above open → SHORT at TC touch; CPR below open → LONG at BC touch
            3. Entry = first candle whose range intersects the virgin CPR zone
            4. SL = opposite side of CPR zone + ATR buffer
            5. Trail with ATR (no fixed RR target — let the bounce develop)
        """
        p = self.params
        vcpr_cfg = p.virgin_cpr
        trade_date = setup_row["trade_date"]
        atr = float(setup_row["atr"])
        tc = float(setup_row["tc"])
        bc = float(setup_row["bc"])
        cpr_width_pct = float(setup_row["cpr_width_pct"])

        cpr_top = max(tc, bc)
        cpr_bottom = min(tc, bc)

        # Determine bias from open relative to CPR.
        open_idx = day_pack.index_of("09:15")
        if open_idx < 0:
            return None
        open_price = float(day_pack.opens[open_idx])

        # Virgin CPR = untested zone = magnet, not wall.
        # Data shows 73% break-through from above, 48% from below.
        # Trade WITH the break-through direction, not against it.
        # Open below CPR, price approaches up → expect break-through → LONG
        # Open above CPR, price approaches down → expect break-through → SHORT
        if open_price < cpr_bottom:
            direction = "LONG"  # Price below CPR, approaching up — ride the break-through
        elif open_price > cpr_top:
            direction = "SHORT"  # Price above CPR, approaching down — ride the break-through
        else:
            return None  # Open inside CPR — no clean setup

        # Minimum open-to-CPR distance filter.
        # Open too close to CPR means price is essentially at the zone already.
        if vcpr_cfg.vcpr_min_open_dist_atr > 0 and atr > 0:
            nearest_edge = cpr_bottom if direction == "LONG" else cpr_top
            if abs(open_price - nearest_edge) < vcpr_cfg.vcpr_min_open_dist_atr * atr:
                return None

        # Direction filter
        if p.direction_filter != "BOTH" and direction != p.direction_filter:
            return None

        prev_close = setup_row.get("prev_day_close")
        gap_pct = calculate_gap_pct(open_price, prev_close)
        if abs(gap_pct) > p.max_gap_for_direction(direction):
            return None

        # Scan for break-through with multi-candle confirmation.
        # 1. First candle must intersect CPR AND close beyond it with strong body
        # 2. Then N-1 more candles must also close beyond CPR (consecutive)
        # Entry = close of the last confirmation candle.
        scan_start_idx, scan_end_idx = day_pack.range_indices(
            vcpr_cfg.vcpr_scan_start, vcpr_cfg.vcpr_scan_end
        )
        if scan_start_idx < 0:
            return None

        confirm_needed = vcpr_cfg.vcpr_confirm_candles  # Total candles needed (including first)
        confirm_count = 0  # Consecutive candles closing beyond CPR
        first_break_found = False
        entry_price = None
        entry_time = None
        entry_volume = 0.0

        highs = day_pack.highs
        lows = day_pack.lows
        closes = day_pack.closes
        opens = day_pack.opens
        times = day_pack.time_str
        vols = day_pack.volumes

        found_entry = False
        entry_idx = -1
        for i in range(scan_start_idx, scan_end_idx + 1):
            high = float(highs[i])
            low = float(lows[i])
            close = float(closes[i])
            candle_open = float(opens[i])

            if not first_break_found:
                # Looking for initial break-through candle
                if not (high >= cpr_bottom and low <= cpr_top):
                    continue  # Must intersect CPR zone

                closes_beyond = (direction == "LONG" and close > cpr_top) or (
                    direction == "SHORT" and close < cpr_bottom
                )
                if not closes_beyond:
                    continue

                # Body ratio filter on the first break-through candle
                candle_range = high - low
                if candle_range > 0:
                    body = abs(close - candle_open)
                    body_ratio = body / candle_range
                    if body_ratio < vcpr_cfg.vcpr_body_pct:
                        continue  # Weak candle (doji/indecision) — keep scanning

                first_break_found = True
                confirm_count = 1
                if confirm_count >= confirm_needed:
                    entry_price = float(close)
                    entry_idx = i
                    entry_time = times[i]
                    entry_volume = float(vols[i])
                    found_entry = True
                    break
            else:
                # Waiting for subsequent confirmation candles
                closes_beyond = (direction == "LONG" and close > cpr_top) or (
                    direction == "SHORT" and close < cpr_bottom
                )
                if closes_beyond:
                    confirm_count += 1
                    if confirm_count >= confirm_needed:
                        entry_price = float(close)
                        entry_idx = i
                        entry_time = times[i]
                        entry_volume = float(vols[i])
                        found_entry = True
                        break
                else:
                    # Confirmation broken — reset and keep scanning
                    first_break_found = False
                    confirm_count = 0

        if not found_entry:
            return None  # No confirmed break-through

        assert entry_price is not None and entry_time is not None  # guaranteed by found_entry
        assert entry_idx >= 0

        # SL placement depends on mode
        if vcpr_cfg.vcpr_sl_mode == "EDGE":
            # EDGE: tight SL at the broken CPR level + small buffer
            # If price falls back through the level it just broke, thesis is dead
            edge_buffer = max(p.atr_sl_buffer, 0.2) * atr
            if direction == "LONG":
                sl_price = cpr_top - edge_buffer  # Just below the broken level
            else:
                sl_price = cpr_bottom + edge_buffer  # Just above the broken level
        else:
            # ZONE (default): SL at far side of CPR zone + ATR buffer
            zone_buffer = max(p.atr_sl_buffer, 0.5) * atr
            if direction == "LONG":
                sl_price = cpr_bottom - zone_buffer
            else:
                sl_price = cpr_top + zone_buffer

        normalized_sl = self._normalize_sl(
            entry_price=entry_price,
            sl_price=sl_price,
            direction=direction,
            atr=atr,
        )
        if normalized_sl is None:
            return None
        sl_price, sl_distance = normalized_sl

        if direction == "LONG":
            target_price = entry_price + (p.rr_ratio * sl_distance)
        else:
            target_price = entry_price - (p.rr_ratio * sl_distance)

        # RVOL check
        avg_vol = day_pack.baseline_for_index(entry_idx)
        rvol = (entry_volume / avg_vol) if avg_vol > 0 else 0.0
        if not p.skip_rvol_check and avg_vol > 0 and rvol < p.rvol_threshold:
            return None

        capital_base = float(p.portfolio_value or 0.0) if p.compound_equity else None
        risk_capital = float(capital_base) if capital_base is not None else float(p.capital or 0.0)
        position_size = calculate_position_size(risk_capital, p.risk_pct, sl_distance)
        min_notional = minimum_trade_notional_for(
            max_positions=max(1, int(p.max_positions or 1)),
            portfolio_value=float(p.portfolio_value or 0.0),
            max_position_pct=float(p.max_position_pct or 0.0),
            capital_base=capital_base,
        )
        if float(position_size) * float(entry_price) < min_notional:
            return None

        # OR/ATR ratio (for TradeResult consistency)
        or_atr_ratio = calculate_or_atr_ratio(tc, bc, atr)

        # Shared simulation via _simulate_trade()
        return self._simulate_trade(
            day_pack=day_pack,
            start_idx=entry_idx + 1,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            direction=direction,
            sl_distance=sl_distance,
            atr=atr,
            position_size=position_size,
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            cpr_width_pct=cpr_width_pct,
            cpr_threshold=0.0,  # VCPR doesn't use CPR threshold
            rvol=rvol,
            or_atr_ratio=or_atr_ratio,
            gap_pct=gap_pct,
            candle_exit=vcpr_cfg.candle_exit,  # VCPR-specific: candle-based exit
        )

    def _simulate_trade(
        self,
        day_pack: DayPack,
        start_idx: int,
        entry_price: float,
        sl_price: float,
        target_price: float,
        direction: str,
        sl_distance: float,
        atr: float,
        position_size: int,
        run_id: str,
        symbol: str,
        trade_date: str,
        entry_time: str,
        cpr_width_pct: float,
        cpr_threshold: float,
        rvol: float,
        or_atr_ratio: float,
        gap_pct: float,
        candle_exit: int = 0,
        rr_ratio: float | None = None,
        runner_target_price: float | None = None,
        trail_atr_multiplier: float = 1.0,
    ) -> TradeResult:
        """
        Shared trade simulation loop for all three strategies.

        This method extracts the common simulation logic that was duplicated
        across CPR_LEVELS, FBR, and VIRGIN_CPR strategies. The simulation
        tracks MFE/MAE, manages trailing stop phases, and handles all exit
        conditions (SL hit, target hit, time exit, candle exit).

        Args:
            day_pack: Day-level candle arrays.
            start_idx: Start index in day arrays (first candle after entry).
            entry_price: Actual entry fill price
            sl_price: Initial stop loss price
            target_price: Target price (or 0.0 if no fixed target)
            direction: "LONG" or "SHORT"
            sl_distance: Stop loss distance in price points
            atr: Average True Range
            position_size: Position size (shares/contracts)
            run_id: Backtest run identifier
            symbol: Trading symbol
            trade_date: Trade date string
            entry_time: Entry time string
            cpr_width_pct: CPR width percentage
            cpr_threshold: CPR threshold used for filtering
            rvol: Relative volume at entry
            or_atr_ratio: Opening Range / ATR ratio
            gap_pct: Gap percentage from previous close
            candle_exit: Exit after N candles (0 = disabled, used by VIRGIN_CPR)

        Returns:
            TradeResult with all trade metrics populated
        """
        p = self.params
        actual_rr_ratio = rr_ratio if rr_ratio is not None else p.rr_ratio
        effective_trail_atr_multiplier = trail_atr_multiplier
        if effective_trail_atr_multiplier == 1.0:
            effective_trail_atr_multiplier = (
                p.short_trail_atr_multiplier
                if direction.upper() == "SHORT"
                else p.trail_atr_multiplier
            )
        outcome: TradeLifecycleOutcome = simulate_trade_lifecycle(
            day_pack=day_pack,
            start_idx=start_idx,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            runner_target_price=runner_target_price,
            scale_out_pct=self.params.cpr_levels.scale_out_pct,
            direction=direction,
            sl_distance=sl_distance,
            atr=atr,
            position_size=position_size,
            entry_time=entry_time,
            time_exit=p.time_exit,
            trail_atr_multiplier=effective_trail_atr_multiplier,
            rr_ratio=actual_rr_ratio,
            breakeven_r=p.breakeven_r,
            candle_exit=candle_exit,
        )

        # Apply transaction cost model
        gross_pnl = outcome.profit_loss
        if outcome.exit_fills:
            cost = sum(
                self._cost_model.round_trip_cost(
                    entry_price=entry_price,
                    exit_price=fill_price,
                    qty=int(fill_qty),
                    direction=direction,
                )
                for fill_qty, fill_price in outcome.exit_fills
            )
        else:
            cost = self._cost_model.round_trip_cost(
                entry_price=entry_price,
                exit_price=outcome.exit_price,
                qty=position_size,
                direction=direction,
            )
        net_pnl = round(gross_pnl - cost, 2)
        net_pct = round(
            (net_pnl / (position_size * entry_price) * 100)
            if position_size * entry_price > 0
            else 0.0,
            4,
        )

        return TradeResult(
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            direction=direction,
            entry_time=entry_time,
            exit_time=outcome.exit_time,
            entry_price=entry_price,
            exit_price=outcome.exit_price,
            sl_price=sl_price,
            target_price=target_price,
            profit_loss=net_pnl,
            profit_loss_pct=net_pct,
            exit_reason=outcome.exit_reason,
            sl_phase=outcome.sl_phase,
            atr=atr,
            cpr_width_pct=cpr_width_pct,
            cpr_threshold=cpr_threshold,
            rvol=round(rvol, 2),
            position_size=position_size,
            position_value=round(position_size * entry_price, 2),
            strategy_version=STRATEGY_VERSION,
            mfe_r=outcome.mfe_r,
            mae_r=outcome.mae_r,
            or_atr_ratio=round(or_atr_ratio, 4),
            gap_pct=gap_pct,
            gross_pnl=round(gross_pnl, 2),
            total_costs=cost,
            reached_1r=outcome.reached_1r,
            reached_2r=outcome.reached_2r,
            max_r=outcome.max_r,
        )

    def _to_dataframe(self, trades: list[TradeResult]) -> pl.DataFrame:
        """Convert trade list to Polars DataFrame."""
        if not trades:
            return pl.DataFrame()
        return pl.from_dicts([t.__dict__ for t in trades])


# ---------------------------------------------------------------------------
# BacktestResult: container + analytics
# ---------------------------------------------------------------------------


class BacktestResult:
    """Holds trades and computes performance metrics."""

    def __init__(
        self,
        run_id: str,
        trades: list[TradeResult] | None = None,
        params: BacktestParams | None = None,
        _loaded_df: pl.DataFrame | None = None,
        cache_info: dict[str, object] | None = None,
        run_context: dict[str, object] | None = None,
        funnel: FunnelCounts | None = None,
    ):
        self.run_id = run_id
        self.trades = trades or []
        self.params = params
        self.cache_info = cache_info or {}
        self.run_context = run_context or {}
        self.funnel = funnel
        # _loaded_df: pre-loaded from DuckDB cache — merged with in-memory trades in .df
        self._loaded_df = _loaded_df
        self._df: pl.DataFrame | None = None

    @property
    def df(self) -> pl.DataFrame:
        if self._df is not None:
            return self._df

        frames = []
        if self.trades:
            frames.append(pl.from_dicts([t.__dict__ for t in self.trades]))
        if self._loaded_df is not None and not self._loaded_df.is_empty():
            # Select only TradeResult columns so schemas match
            common_cols = [
                "run_id",
                "symbol",
                "trade_date",
                "direction",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "sl_price",
                "target_price",
                "profit_loss",
                "profit_loss_pct",
                "exit_reason",
                "sl_phase",
                "atr",
                "cpr_width_pct",
                "cpr_threshold",
                "rvol",
                "position_size",
                "position_value",
                "strategy_version",
                "mfe_r",
                "mae_r",
                "or_atr_ratio",
                "gap_pct",
                "gross_pnl",
                "total_costs",
                "reached_1r",
                "reached_2r",
                "max_r",
            ]
            available = [c for c in common_cols if c in self._loaded_df.columns]
            frames.append(self._loaded_df.select(available))

        if not frames:
            self._df = pl.DataFrame()
        elif len(frames) == 1:
            self._df = frames[0]
        else:
            self._df = pl.concat(frames, how="diagonal_relaxed")

        return self._df

    def validate(self) -> None:
        """Validate summary fields before persisting or publishing results."""
        df = self.df
        if df.is_empty() or "profit_loss" not in df.columns:
            return

        total_trades = int(df.height)
        total_pnl = float(df["profit_loss"].sum())
        if "run_id" in df.columns:
            run_ids = {str(value) for value in df["run_id"].drop_nulls().unique().to_list()}
            if run_ids and run_ids != {self.run_id}:
                raise ValueError(
                    f"Mixed run_id values in backtest result for run_id={self.run_id}: "
                    f"{sorted(run_ids)}"
                )
        if total_trades == 0 and total_pnl != 0:
            raise ValueError(f"PnL without trades: {total_pnl}")
        if abs(total_pnl) > 1e12:
            logger.warning("Suspiciously large PnL for run_id=%s: %s", self.run_id, total_pnl)

    def _time_of_day_breakdown(self) -> list[tuple[str, int, float, float]]:
        """Return hourly entry-time buckets with trade count, PnL, and win rate."""
        df = self.df
        if df.is_empty() or "entry_time" not in df.columns or "profit_loss" not in df.columns:
            return []

        bucketed = df.with_columns(
            pl.col("entry_time").cast(pl.Utf8).str.slice(0, 2).alias("_entry_hour")
        ).filter(pl.col("_entry_hour").str.contains(r"^\d{2}$"))
        if bucketed.is_empty():
            return []

        grouped = (
            bucketed.group_by("_entry_hour")
            .agg(
                pl.len().alias("trades"),
                pl.sum("profit_loss").alias("pnl"),
                (pl.col("profit_loss") > 0).cast(pl.Float64).mean().mul(100.0).alias("win_rate"),
            )
            .sort("_entry_hour")
        )
        return [
            (
                str(row[0]),
                int(row[1] or 0),
                float(row[2] or 0.0),
                float(row[3] or 0.0),
            )
            for row in grouped.iter_rows()
        ]

    def summary(self) -> str:
        """Print-friendly performance summary."""
        df = self.df
        if df.is_empty():
            return "No trades found."

        df = self.df
        total = len(df)
        wins = int((df["profit_loss"] > 0).sum())
        losses = total - wins
        win_rate = wins / total * 100 if total else 0
        total_pnl = float(df["profit_loss"].sum())
        avg_pnl = float(df["profit_loss"].mean())
        best = float(df["profit_loss"].max())
        worst = float(df["profit_loss"].min())

        # Cost breakdown
        has_costs = "gross_pnl" in df.columns and "total_costs" in df.columns
        if has_costs:
            gross_pnl_total = float(df["gross_pnl"].sum())
            total_costs_sum = float(df["total_costs"].sum())
        else:
            gross_pnl_total = total_pnl
            total_costs_sum = 0.0

        # Exit breakdown — granular SL types for phase analysis
        sl_initial = int((df["exit_reason"] == "INITIAL_SL").sum())
        sl_breakeven = int((df["exit_reason"] == "BREAKEVEN_SL").sum())
        sl_trailing = int((df["exit_reason"] == "TRAILING_SL").sum())
        sl_exits = sl_initial + sl_breakeven + sl_trailing
        tgt_exits = int((df["exit_reason"] == "TARGET").sum())
        time_exits = int((df["exit_reason"] == "TIME").sum())

        # MFE/MAE stats — only if columns are present (older cached results may lack them)
        has_excursion = "mfe_r" in df.columns and "mae_r" in df.columns
        if has_excursion:
            avg_mfe = float(df["mfe_r"].mean())
            avg_mae = float(df["mae_r"].mean())
            pct_mfe_1r = float((df["mfe_r"] >= 1.0).sum()) / total * 100 if total else 0.0
            target_r = self.params.rr_ratio if self.params else 2.0
            pct_mfe_target = float((df["mfe_r"] >= target_r).sum()) / total * 100 if total else 0.0

        symbols = df["symbol"].unique().to_list()

        lines = [
            f"{'=' * 55}",
            f" CPR-ATR Backtest -- Run {self.run_id}",
            f"{'=' * 55}",
            f" Symbols:       {', '.join(sorted(symbols))}",
            f" Date range:    {str(df['trade_date'].min())} to {str(df['trade_date'].max())}",
            f"{'-' * 55}",
            f" Total trades:  {total}",
            f" Wins / Losses: {wins} / {losses}  ({win_rate:.1f}% win rate)",
            f" Total P/L:     Rs.{total_pnl:+,.2f} (net)",
            f" Avg P/L/trade: Rs.{avg_pnl:+,.2f}",
            f" Best trade:    Rs.{best:+,.2f}",
            f" Worst trade:   Rs.{worst:+,.2f}",
            f"{'-' * 55}",
            f" SL exits:      {sl_exits} (Initial:{sl_initial} BE:{sl_breakeven} Trail:{sl_trailing})",
            f" Target exits:  {tgt_exits}",
            f" Time exits:    {time_exits}",
        ]
        if has_costs and total_costs_sum > 0:
            lines += [
                f"{'-' * 55}",
                f" Gross P/L:     Rs.{gross_pnl_total:+,.2f}",
                f" Total Costs:   Rs.{total_costs_sum:,.2f}",
                f" Net P/L:       Rs.{total_pnl:+,.2f}",
                f" Cost/Trade:    Rs.{total_costs_sum / total:,.2f}",
            ]
        candidate_trades = _int_from_mapping(self.cache_info, "candidate_trade_count", total)
        skipped_portfolio = _int_from_mapping(self.cache_info, "not_executed_portfolio", 0)
        if candidate_trades > total or skipped_portfolio > 0:
            lines += [
                f"{'-' * 55}",
                f" Candidate trades: {candidate_trades}",
                f" Skipped (cash/slots): {skipped_portfolio}",
            ]
        if has_excursion:
            lines += [
                f"{'-' * 55}",
                f" MFE (avg):    {avg_mfe:+.2f}R  (how close trades get to target)",
                f" MAE (avg):    {avg_mae:+.2f}R  (how far trades go against us)",
                f" MFE > 1.0R:   {pct_mfe_1r:.1f}%  (trades that reached 1:1 R)",
                f" MFE > target: {pct_mfe_target:.1f}%  (trades that hit {target_r:.1f}R target)",
            ]

        # Exit diagnostics (§0.3)
        has_diagnostics = "reached_1r" in df.columns and "max_r" in df.columns
        if has_diagnostics and total > 0:
            reached_1r_count = int(df["reached_1r"].sum())
            reached_2r_count = int(df["reached_2r"].sum()) if "reached_2r" in df.columns else 0
            zero_pnl_count = int(((df["profit_loss"] >= -0.01) & (df["profit_loss"] <= 0.01)).sum())
            be_exits = sl_breakeven
            # Trades that reached >=1R but did NOT hit target
            reached_1r_no_target = int((df["reached_1r"] & (df["exit_reason"] != "TARGET")).sum())
            avg_max_r = float(df["max_r"].mean())
            lines += [
                f"{'-' * 55}",
                " EXIT DIAGNOSTICS",
                f" Zero-PnL exits:    {zero_pnl_count} ({zero_pnl_count / total * 100:.1f}%)",
                f" Breakeven SL:      {be_exits} ({be_exits / total * 100:.1f}%)",
                f" Reached >=1R:      {reached_1r_count} ({reached_1r_count / total * 100:.1f}%)",
                f" Reached >=2R:      {reached_2r_count} ({reached_2r_count / total * 100:.1f}%)",
                f" >=1R but no target: {reached_1r_no_target} ({reached_1r_no_target / total * 100:.1f}%)",
                f" Avg Max R:         {avg_max_r:.2f}R",
            ]

        tod_rows = self._time_of_day_breakdown()
        if tod_rows:
            lines += [
                f"{'-' * 55}",
                " TIME OF DAY PNL",
                " Hour  Trades  WinRate   P/L",
            ]
            for hour, trades, pnl, win_rate_pct in tod_rows:
                lines.append(f" {hour}:00  {trades:>6,}  {win_rate_pct:>7.1f}%  Rs.{pnl:+,.2f}")

        # Setup selection funnel
        if self.funnel and self.funnel.universe_count > 0:
            f = self.funnel
            lines += [
                f"{'-' * 55}",
                " SETUP FUNNEL",
                f" Universe:        {f.universe_count:>8,} symbol-days",
            ]

            def _funnel_line(label: str, count: int, prev: int) -> str:
                pct = count / prev * 100 if prev > 0 else 0.0
                return f" {label:<17} {count:>8,} ({pct:5.1f}% pass)"

            lines.append(_funnel_line("CPR width:", f.after_cpr_width, f.universe_count))
            lines.append(_funnel_line("Direction:", f.after_direction, f.after_cpr_width))
            setup_label = (
                f"Setup ({self.params.fbr_setup_filter}):"
                if self.params and self.params.strategy == "FBR"
                else "Dir filter:"
            )
            lines.append(_funnel_line(setup_label, f.after_dir_filter, f.after_direction))
            lines.append(_funnel_line("Min price:", f.after_min_price, f.after_dir_filter))
            lines.append(_funnel_line("Gap filter:", f.after_gap, f.after_min_price))
            lines.append(_funnel_line("OR/ATR:", f.after_or_atr, f.after_gap))
            lines.append(_funnel_line("Narrowing:", f.after_narrowing, f.after_or_atr))
            if f.after_shift != f.after_narrowing:
                lines.append(_funnel_line("CPR shift:", f.after_shift, f.after_narrowing))
            # Final stage: entry_triggered vs last filter output
            last_filter = f.after_shift if f.after_shift else f.after_narrowing
            lines.append(_funnel_line("Entry triggered:", f.entry_triggered, last_filter))

        # Advanced risk metrics
        adv = self._advanced_metrics()
        if adv:
            pf = adv["profit_factor"]
            pf_str = f"{pf:.2f}" if pf < 1e9 else "∞"
            calmar = adv["calmar"]
            calmar_str = f"{calmar:.2f}" if calmar < 1e9 else "∞"
            lines += [
                f"{'-' * 55}",
                f" Profit Factor:  {pf_str}",
                f" Max Drawdown:   -{adv['max_dd_pct']:.1f}%  (Rs.{adv['max_dd_abs']:,.0f} abs)",
                f" Annual Return:  {adv['annual_return_pct']:+.1f}%",
                f" Calmar Ratio:   {calmar_str}",
                f" Sharpe Ratio:   {adv['sharpe']:.2f}",
            ]
        lines.append(f"{'=' * 55}")

        # Per-symbol breakdown
        if len(symbols) > 1:
            lines.append(" Per-symbol:")
            for sym in sorted(symbols):
                sym_df = df.filter(pl.col("symbol") == sym)
                sym_pnl = float(sym_df["profit_loss"].sum())
                sym_trades = len(sym_df)
                sym_wins = int((sym_df["profit_loss"] > 0).sum())
                lines.append(
                    f"   {sym:<12} {sym_trades:3d} trades  "
                    f"{sym_wins}/{sym_trades} wins  ₹{sym_pnl:+,.2f}"
                )
            lines.append(f"{'=' * 55}")

        return "\n".join(lines)

    def _advanced_metrics(self) -> dict:
        """
        Compute advanced risk metrics from trade-level data.

        Returns a dict with keys: profit_factor, max_dd_abs, max_dd_pct,
        annual_return_pct, calmar, sharpe. Returns {} on any failure so
        older cached results (missing columns) degrade gracefully.
        """
        df = self.df
        if df.is_empty() or "profit_loss" not in df.columns:
            return {}
        try:
            capital_per_symbol = (self.params.capital if self.params else 100_000.0) or 100_000.0
            symbol_count = max(1, int(df["symbol"].n_unique())) if "symbol" in df.columns else 1
            allocated_capital = float(capital_per_symbol) * float(symbol_count)
            initial_equity = (
                float(self.params.portfolio_value)
                if self.params and float(self.params.portfolio_value or 0.0) > 0
                else allocated_capital
            )

            # Profit Factor
            gross_profit = float(df.filter(pl.col("profit_loss") > 0)["profit_loss"].sum() or 0.0)
            gross_loss = abs(
                float(df.filter(pl.col("profit_loss") < 0)["profit_loss"].sum() or 0.0)
            )
            profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else 1e10

            # Max Drawdown via end-of-day portfolio equity curve
            daily = df.group_by("trade_date").agg(pl.col("profit_loss").sum()).sort("trade_date")
            daily_pnl = daily["profit_loss"].to_numpy()
            equity_curve = initial_equity + np.cumsum(daily_pnl)
            running_peak = np.maximum.accumulate(np.maximum(equity_curve, initial_equity))
            dd = equity_curve - running_peak
            max_dd_abs = float(dd.min()) if len(dd) else 0.0
            max_dd_pct = round(abs(max_dd_abs) / max(initial_equity, 1.0) * 100, 2)

            # Annualized return: use requested run window when available, not first/last trade date.
            ctx_start = str(self.run_context.get("start_date", "") or "")
            ctx_end = str(self.run_context.get("end_date", "") or "")
            if ctx_start and ctx_end:
                min_d = date.fromisoformat(ctx_start[:10])
                max_d = date.fromisoformat(ctx_end[:10])
            else:
                min_d = date.fromisoformat(str(df["trade_date"].min())[:10])
                max_d = date.fromisoformat(str(df["trade_date"].max())[:10])
            days = (max_d - min_d).days + 1
            total_pnl = float(df["profit_loss"].sum())
            ending_equity = initial_equity + total_pnl
            if ending_equity <= 0:
                annual_return_pct = -100.0
            else:
                annual_return_pct = round(
                    ((ending_equity / max(initial_equity, 1.0)) ** (365.25 / max(days, 1)) - 1.0)
                    * 100.0,
                    2,
                )

            # Calmar Ratio = annual_return / max_dd_%
            calmar = round(annual_return_pct / max_dd_pct, 3) if max_dd_pct > 0 else 1e10

            # Sharpe Ratio — daily P/L normalized by portfolio equity base.
            daily_arr = daily["profit_loss"].to_numpy() / max(initial_equity, 1.0)
            std_d = float(np.std(daily_arr))
            sharpe = (
                round(float(np.mean(daily_arr)) / std_d * np.sqrt(252), 3) if std_d > 0 else 0.0
            )

            return {
                "profit_factor": profit_factor,
                "max_dd_abs": max_dd_abs,
                "max_dd_pct": max_dd_pct,
                "annual_return_pct": annual_return_pct,
                "calmar": calmar,
                "sharpe": sharpe,
            }
        except Exception as e:
            logger.debug("Advanced metrics computation failed for run_id=%s: %s", self.run_id, e)
            return {}

    def save_to_db(
        self,
        db: BacktestDB | None = None,
        *,
        execution_mode: str = "BACKTEST",
    ) -> int:
        """Persist results to DuckDB backtest_results + run_metadata tables."""
        target_db = db or get_db()
        strategy = self.params.strategy if self.params else "UNKNOWN"
        run_context = self.run_context
        params_dict = asdict(self.params) if self.params else None
        param_signature = run_context.get("param_signature")
        self.validate()
        row_count = 0
        replica_sync = getattr(target_db, "_sync", None)
        begin_batch = getattr(target_db, "_begin_replica_batch", None)
        end_batch = getattr(target_db, "_end_replica_batch", None)
        if callable(begin_batch):
            begin_batch()
        target_db.con.execute("BEGIN TRANSACTION")
        try:
            mode_label = (
                "compound" if self.params and self.params.compound_equity else "daily-reset"
            )
            sizing_label = "risk" if self.params and self.params.risk_based_sizing else "standard"
            target_db.store_run_metadata(
                run_id=self.run_id,
                strategy=strategy,
                label=(
                    f"{strategy}"
                    f" {mode_label}-{sizing_label}"
                    f" | {run_context.get('start_date', '')}"
                    f" to {run_context.get('end_date', '')}"
                ),
                symbols=run_context.get("symbols"),
                start_date=run_context.get("start_date"),
                end_date=run_context.get("end_date"),
                params=params_dict,
                param_signature=param_signature,
                execution_mode=execution_mode,
            )
            row_count = target_db.store_backtest_results(
                self.df,
                execution_mode=execution_mode,
                transactional=False,
            )
            target_db.con.execute("COMMIT")
        except Exception:
            try:
                target_db.con.execute("ROLLBACK")
            except Exception as rollback_err:
                logger.debug("Rollback failed after save_to_db failure: %s", rollback_err)
            raise
        finally:
            if callable(end_batch):
                end_batch()
        if replica_sync is not None:
            try:
                replica_sync.force_sync(target_db.con)
            except Exception as exc:
                logger.warning("Replica sync failed after save_to_db: %s", exc)
        try:
            from web.state import invalidate_run_cache

            invalidate_run_cache(self.run_id)
        except Exception as exc:
            logger.debug(
                "Skipping dashboard cache invalidation for run_id=%s: %s", self.run_id, exc
            )
        return row_count
