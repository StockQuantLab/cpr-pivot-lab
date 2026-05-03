"""Shared trade lifecycle simulation for CPR-ATR backtests."""

from __future__ import annotations

from typing import Any

from engine.cpr_atr_models import STRATEGY_VERSION, BacktestParams, DayPack, TradeResult
from engine.cpr_atr_shared import TradeLifecycleOutcome, simulate_trade_lifecycle


def simulate_strategy_trade(
    *,
    params: BacktestParams,
    cost_model: Any,
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
    p = params
    actual_rr_ratio = rr_ratio if rr_ratio is not None else p.rr_ratio
    effective_trail_atr_multiplier = trail_atr_multiplier
    if effective_trail_atr_multiplier == 1.0:
        effective_trail_atr_multiplier = (
            p.short_trail_atr_multiplier if direction.upper() == "SHORT" else p.trail_atr_multiplier
        )
    outcome: TradeLifecycleOutcome = simulate_trade_lifecycle(
        day_pack=day_pack,
        start_idx=start_idx,
        entry_price=entry_price,
        sl_price=sl_price,
        target_price=target_price,
        runner_target_price=runner_target_price,
        scale_out_pct=params.cpr_levels.scale_out_pct,
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
        time_stop_bars=params.cpr_levels.time_stop_bars,
        momentum_confirm=params.cpr_levels.momentum_confirm,
    )

    # Apply transaction cost model
    gross_pnl = outcome.profit_loss
    if outcome.exit_fills:
        cost = sum(
            cost_model.round_trip_cost(
                entry_price=entry_price,
                exit_price=fill_price,
                qty=int(fill_qty),
                direction=direction,
            )
            for fill_qty, fill_price in outcome.exit_fills
        )
    else:
        cost = cost_model.round_trip_cost(
            entry_price=entry_price,
            exit_price=outcome.exit_price,
            qty=position_size,
            direction=direction,
        )
    net_pnl = round(gross_pnl - cost, 2)
    net_pct = round(
        (net_pnl / (position_size * entry_price) * 100) if position_size * entry_price > 0 else 0.0,
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
