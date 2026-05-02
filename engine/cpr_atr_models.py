"""Shared CPR-ATR strategy data models."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field, replace

import polars as pl

from engine.cost_model import CostModel, cost_model_from_name
from engine.execution_defaults import (
    DEFAULT_MAX_POSITION_PCT,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_PORTFOLIO_VALUE,
    DEFAULT_POSITION_CAPITAL,
    DEFAULT_RISK_PCT,
)

STRATEGY_VERSION = "cpr-atr-v3"


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
    time_stop_bars: int = 0
    momentum_confirm: bool = False


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
    """CPR-ATR strategy parameters."""

    cpr_percentile: float = 33.0
    cpr_max_width_pct: float = 2.0
    atr_periods: int = 12
    buffer_pct: float = 0.0005
    rvol_threshold: float = 1.0
    entry_window_end: str = "10:15"
    short_open_to_cpr_atr_min: float = 0.0
    min_sl_atr_ratio: float = 0.5
    max_sl_atr_ratio: float = 2.0
    rr_ratio: float = 2.0
    breakeven_r: float = 1.0
    atr_sl_buffer: float = 0.0
    trail_atr_multiplier: float = 1.0
    short_trail_atr_multiplier: float = 1.0
    capital: float = DEFAULT_POSITION_CAPITAL
    risk_pct: float = DEFAULT_RISK_PCT
    portfolio_value: float = DEFAULT_PORTFOLIO_VALUE
    max_positions: int = DEFAULT_MAX_POSITIONS
    max_position_pct: float = DEFAULT_MAX_POSITION_PCT
    risk_based_sizing: bool = False
    compound_equity: bool = False
    time_exit: str = "15:15"
    rvol_lookback_days: int = 10
    skip_rvol_check: bool = False
    runtime_batch_size: int = 512
    direction_filter: str = "BOTH"
    fbr_setup_filter: str = "BOTH"
    or_minutes: int = 5
    or_atr_min: float = 0.3
    or_atr_max: float = 2.5
    max_gap_pct: float = 1.5
    long_max_gap_pct: float | None = None
    min_price: float = 0.0
    regime_index_symbol: str = ""
    regime_min_move_pct: float = 0.0
    regime_snapshot_minutes: int = 30
    pack_source: str = "intraday_day_pack"
    pack_source_session_id: str = ""
    strategy: str = "CPR_LEVELS"
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
    commission_model: str = "zerodha"
    slippage_bps: float = 0.0

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
        """Return a copy with updated grouped strategy config objects."""
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
    exit_reason: str = ""
    sl_phase: str = ""
    atr: float = 0.0
    cpr_width_pct: float = 0.0
    cpr_threshold: float = 0.0
    rvol: float = 0.0
    position_size: int = 0
    position_value: float = 0.0
    strategy_version: str = STRATEGY_VERSION
    mfe_r: float = 0.0
    mae_r: float = 0.0
    or_atr_ratio: float = 0.0
    gap_pct: float = 0.0
    gross_pnl: float = 0.0
    total_costs: float = 0.0
    reached_1r: bool = False
    reached_2r: bool = False
    max_r: float = 0.0


@dataclass
class FunnelCounts:
    """Per-run setup selection funnel counts."""

    run_id: str = ""
    strategy: str = ""
    universe_count: int = 0
    after_cpr_width: int = 0
    after_direction: int = 0
    after_dir_filter: int = 0
    after_min_price: int = 0
    after_gap: int = 0
    after_or_atr: int = 0
    after_narrowing: int = 0
    after_shift: int = 0
    entry_triggered: int = 0


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
