"""Shared CPR-ATR decision helpers.

These helpers are pure or near-pure and are intended to be reused by both the
current historical backtest engine and the future paper-trading loop.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from engine.bar_orchestrator import SessionPositionTracker
from engine.constants import SL_PHASE_TO_EXIT_REASON
from engine.cpr_atr_utils import (
    TrailingStop,
    calculate_gap_pct,
    calculate_or_atr_ratio,
    calculate_position_size,
    normalize_cpr_bounds,
    validate_and_adjust_sl_distance,
)

logger = logging.getLogger(__name__)

# Thread-local reject reason from find_cpr_levels_entry.
# Set on every call; callers can read _last_reject_reason.value after a None return
# to understand why the candidate was rejected.
_last_reject_reason = threading.local()


@dataclass(frozen=True, slots=True)
class TradeLifecycleOutcome:
    exit_time: str | None
    exit_price: float
    exit_reason: str
    sl_phase: str
    profit_loss: float
    profit_loss_pct: float
    mfe_r: float
    mae_r: float
    max_favorable: float
    max_adverse: float
    candle_count: int
    # Exit diagnostics (§0.3)
    reached_1r: bool = False
    reached_2r: bool = False
    max_r: float = 0.0
    exit_fills: tuple[tuple[float, float], ...] = ()


def get_cpr_entry_scan_start(or_minutes: int, cpr_entry_start: str | None) -> str:
    """Return the CPR entry scan start time string."""
    if cpr_entry_start:
        return cpr_entry_start
    or_candle_count = max(1, or_minutes // 5)
    entry_start_min = (9 * 60 + 15) + or_candle_count * 5
    return f"{entry_start_min // 60:02d}:{entry_start_min % 60:02d}"


def normalize_stop_loss(
    *,
    entry_price: float,
    sl_price: float,
    direction: str,
    atr: float,
    min_sl_atr_ratio: float,
    max_sl_atr_ratio: float,
) -> tuple[float, float] | None:
    """Normalize SL distance and re-anchor the stop to the adjusted distance."""
    if direction == "LONG":
        sl_distance = entry_price - sl_price
    else:
        sl_distance = sl_price - entry_price

    sl_distance, valid = validate_and_adjust_sl_distance(
        sl_distance=sl_distance,
        atr=atr,
        min_sl_atr_ratio=min_sl_atr_ratio,
        max_sl_atr_ratio=max_sl_atr_ratio,
    )
    if not valid:
        return None

    if direction == "LONG":
        sl_price = entry_price - sl_distance
    else:
        sl_price = entry_price + sl_distance
    return sl_price, sl_distance


def find_first_close_idx(
    closes: Sequence[float],
    start_idx: int,
    end_idx: int,
    *,
    direction: str,
    trigger: float,
) -> int:
    """Find the first candle whose close crosses the trigger in the given direction."""
    if direction == "LONG":
        for i in range(start_idx, end_idx + 1):
            if closes[i] >= trigger:
                return i
    else:
        for i in range(start_idx, end_idx + 1):
            if closes[i] <= trigger:
                return i
    return -1


def find_cpr_levels_entry(
    *,
    day_pack: Any,
    setup_row: Mapping[str, Any],
    params: Any,
    current_idx: int,
    capital_base: float | None = None,
) -> dict[str, Any] | None:
    """Evaluate a CPR_LEVELS entry on the current candle.

    This is intentionally candle-by-candle so live/replay and backtest can
    share the same entry logic. The caller decides how to search for the next
    candidate candle.
    """

    cpr_cfg = params.cpr_levels
    direction = str(setup_row.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        _last_reject_reason.value = "INVALID_DIRECTION"
        return None
    if params.direction_filter != "BOTH" and direction != params.direction_filter:
        _last_reject_reason.value = "DIRECTION_FILTER"
        return None

    cpr_width_pct = float(setup_row.get("cpr_width_pct") or 0.0)
    cpr_threshold = float(setup_row.get("cpr_threshold") or 0.0)
    effective_max = (
        min(cpr_threshold, params.cpr_max_width_pct)
        if cpr_threshold > 0
        else params.cpr_max_width_pct
    )
    if cpr_width_pct >= effective_max:
        _last_reject_reason.value = "CPR_WIDTH"
        return None

    if cpr_cfg.use_narrowing_filter and not setup_row.get("is_narrowing", False):
        _last_reject_reason.value = "NARROWING"
        return None

    atr = float(setup_row["atr"])
    or_high_raw = setup_row.get("high_915")
    if or_high_raw is None:
        or_high_raw = setup_row.get("or_high_5")
    or_low_raw = setup_row.get("low_915")
    if or_low_raw is None:
        or_low_raw = setup_row.get("or_low_5")
    open_915_raw = setup_row.get("open_915")
    or_high = float(or_high_raw)
    or_low = float(or_low_raw)
    open_915 = float(open_915_raw)
    prev_close = setup_row.get("prev_day_close")
    if params.min_price > 0 and (prev_close is None or float(prev_close) < params.min_price):
        _last_reject_reason.value = "MIN_PRICE"
        return None

    or_atr_ratio = calculate_or_atr_ratio(or_high, or_low, atr)
    if or_atr_ratio < params.or_atr_min or or_atr_ratio > params.or_atr_max:
        _last_reject_reason.value = "OR_ATR_RATIO"
        return None

    gap_pct = calculate_gap_pct(open_915, prev_close)
    if abs(gap_pct) > params.max_gap_for_direction(direction):
        _last_reject_reason.value = "GAP_SIZE"
        return None

    if direction == "SHORT" and params.short_open_to_cpr_atr_min > 0:
        if float(setup_row.get("open_to_cpr_atr") or 0.0) < params.short_open_to_cpr_atr_min:
            _last_reject_reason.value = "SHORT_OPEN_TO_CPR_ATR"
            return None

    entry_start = get_cpr_entry_scan_start(params.or_minutes, cpr_cfg.cpr_entry_start)
    scan_start_idx, scan_end_idx = day_pack.range_indices(entry_start, params.entry_window_end)
    if scan_start_idx < 0 or current_idx < scan_start_idx or current_idx > scan_end_idx:
        _last_reject_reason.value = "SCAN_WINDOW"
        return None

    current_close = float(day_pack.closes[current_idx])
    if not params.skip_rvol_check:
        rvol_val = day_pack.baseline_for_index(current_idx)
        if rvol_val is not None and rvol_val > 0:
            if float(day_pack.volumes[current_idx]) / rvol_val < params.rvol_threshold:
                _last_reject_reason.value = "RVOL"
                return None

    tc = float(setup_row["tc"])
    bc = float(setup_row["bc"])
    cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
    atr_buffer = params.atr_sl_buffer * atr

    if direction == "LONG":
        trigger = cpr_upper * (1.0 + params.buffer_pct)
        if current_close < trigger:
            _last_reject_reason.value = "TRIGGER_NOT_HIT"
            return None
        if (
            cpr_cfg.cpr_min_close_atr > 0
            and current_close < cpr_upper + cpr_cfg.cpr_min_close_atr * atr
        ):
            _last_reject_reason.value = "CPR_MIN_CLOSE_ATR"
            return None
        sl_price = cpr_lower - atr_buffer
        first_target_price = float(setup_row["r1"])
        runner_target_price = (
            float(setup_row["r2"])
            if cpr_cfg.scale_out_pct > 0 and float(setup_row["r2"]) > first_target_price
            else None
        )
    else:
        trigger = cpr_lower * (1.0 - params.buffer_pct)
        if current_close > trigger:
            _last_reject_reason.value = "TRIGGER_NOT_HIT"
            return None
        if (
            cpr_cfg.cpr_min_close_atr > 0
            and current_close > cpr_lower - cpr_cfg.cpr_min_close_atr * atr
        ):
            _last_reject_reason.value = "CPR_MIN_CLOSE_ATR"
            return None
        sl_price = cpr_upper + atr_buffer
        first_target_price = float(setup_row["s1"])
        runner_target_price = (
            float(setup_row["s2"])
            if cpr_cfg.scale_out_pct > 0 and float(setup_row["s2"]) < first_target_price
            else None
        )

    use_scale_out = runner_target_price is not None
    target_price = runner_target_price if use_scale_out else first_target_price

    candle_open = float(day_pack.opens[current_idx])
    fill_price = max(trigger, candle_open) if direction == "LONG" else min(trigger, candle_open)

    normalized_sl = normalize_stop_loss(
        entry_price=fill_price,
        sl_price=sl_price,
        direction=direction,
        atr=atr,
        min_sl_atr_ratio=params.min_sl_atr_ratio,
        max_sl_atr_ratio=params.max_sl_atr_ratio,
    )
    if normalized_sl is None:
        _last_reject_reason.value = "SL_NORMALIZE_FAILED"
        return None
    normalized_sl_price, sl_distance = normalized_sl

    if direction == "LONG" and target_price <= fill_price:
        _last_reject_reason.value = "TARGET_BEHIND_ENTRY"
        return None
    if direction == "SHORT" and target_price >= fill_price:
        _last_reject_reason.value = "TARGET_BEHIND_ENTRY"
        return None

    entry_volume = float(day_pack.volumes[current_idx])
    avg_vol = day_pack.baseline_for_index(current_idx)
    rvol = (entry_volume / avg_vol) if avg_vol and avg_vol > 0 else 0.0

    effective_rr = (
        abs(target_price - fill_price) / sl_distance if sl_distance > 0 else params.rr_ratio
    )
    if effective_rr < cpr_cfg.min_effective_rr:
        _last_reject_reason.value = "MIN_EFFECTIVE_RR"
        return None

    slot_tracker = SessionPositionTracker(
        max_positions=max(1, int(getattr(params, "max_positions", 1) or 1)),
        portfolio_value=float(getattr(params, "portfolio_value", 0.0) or 0.0),
        max_position_pct=float(getattr(params, "max_position_pct", 0.0) or 0.0),
    )
    min_notional = slot_tracker.minimum_trade_notional(capital_base=capital_base)
    risk_capital = (
        float(capital_base)
        if capital_base is not None and bool(getattr(params, "risk_based_sizing", False))
        else float(getattr(params, "capital", 0.0) or 0.0)
    )
    position_size = calculate_position_size(risk_capital, params.risk_pct, sl_distance)
    if position_size <= 0 or float(position_size) * float(fill_price) < min_notional:
        _last_reject_reason.value = "MIN_TRADE_NOTIONAL"
        return None

    # Success — clear reject reason.
    _last_reject_reason.value = None

    return {
        "direction": direction,
        "entry_idx": current_idx,
        "entry_time": day_pack.time_str[current_idx],
        "event_time": getattr(day_pack, "bar_end", None),
        "entry_price": fill_price,
        "sl_price": normalized_sl_price,
        "target_price": target_price,
        "runner_target_price": runner_target_price,
        "first_target_price": first_target_price,
        "scale_out_pct": float(cpr_cfg.scale_out_pct if use_scale_out else 0.0),
        "sl_distance": sl_distance,
        "position_size": position_size,
        "rr_ratio": effective_rr,
        "rvol": rvol,
        "or_atr_ratio": or_atr_ratio,
        "gap_pct": gap_pct,
        "cpr_width_pct": cpr_width_pct,
        "cpr_threshold": cpr_threshold,
    }


def scan_cpr_levels_entry(
    *,
    day_pack: Any,
    setup_row: Mapping[str, Any],
    params: Any,
    scan_start_idx: int,
    scan_end_idx: int,
    capital_base: float | None = None,
) -> dict[str, Any] | None:
    """Scan a CPR entry window and return the first qualifying candidate.

    This keeps the candle-by-candle CPR evaluation in one place so backtest,
    paper replay, and live paper can all reuse the same search semantics.
    """
    if scan_start_idx < 0 or scan_end_idx < scan_start_idx:
        return None

    last_idx = min(int(scan_end_idx), len(day_pack.time_str) - 1)
    for current_idx in range(int(scan_start_idx), last_idx + 1):
        candidate = find_cpr_levels_entry(
            day_pack=day_pack,
            setup_row=setup_row,
            params=params,
            current_idx=current_idx,
            capital_base=capital_base,
        )
        if candidate is not None:
            return candidate
    return None


def split_scale_out_quantity(position_size: int, scale_out_pct: float) -> tuple[int, int] | None:
    """Return (scale_out_qty, runner_qty) for a partial-exit trade."""
    if position_size <= 1 or scale_out_pct <= 0:
        return None

    scale_out_qty = round(position_size * scale_out_pct)
    scale_out_qty = max(1, min(scale_out_qty, position_size - 1))
    runner_qty = position_size - scale_out_qty
    if runner_qty <= 0:
        return None
    return scale_out_qty, runner_qty


def simulate_trade_lifecycle(
    *,
    day_pack: Any,
    start_idx: int,
    entry_price: float,
    sl_price: float,
    target_price: float,
    direction: str,
    sl_distance: float,
    atr: float,
    position_size: int,
    entry_time: str,
    time_exit: str,
    rr_ratio: float,
    breakeven_r: float,
    runner_target_price: float | None = None,
    scale_out_pct: float = 0.0,
    candle_exit: int = 0,
) -> TradeLifecycleOutcome:
    """Run the sequential trade lifecycle that follows entry."""
    ts = TrailingStop(
        entry_price=entry_price,
        direction=direction,
        sl_price=sl_price,
        atr=atr,
        rr_ratio=rr_ratio,
        breakeven_r=breakeven_r,
    )

    exit_time = None
    exit_price = None
    exit_reason = "TIME"
    final_phase = ts.phase
    max_favorable = 0.0
    max_adverse = 0.0
    candle_count = 0
    exit_fills: list[tuple[float, float]] = []
    remaining_qty = float(position_size)
    scaled_out = False
    scale_split = (
        split_scale_out_quantity(position_size, scale_out_pct)
        if runner_target_price is not None and scale_out_pct > 0
        else None
    )
    scale_out_qty = float(scale_split[0]) if scale_split else 0.0
    runner_qty = float(scale_split[1]) if scale_split else 0.0

    def _record_fill(qty: float, price: float) -> None:
        nonlocal remaining_qty, exit_price
        if qty <= 0:
            return
        exit_fills.append((float(qty), float(price)))
        total_qty = sum(fill_qty for fill_qty, _ in exit_fills)
        total_value = sum(fill_qty * fill_price for fill_qty, fill_price in exit_fills)
        remaining = max(float(position_size) - total_qty, 0.0)
        remaining_qty = remaining
        exit_price = round(total_value / total_qty, 4) if total_qty > 0 else float(price)

    times = day_pack.time_str
    lows = day_pack.lows
    highs = day_pack.highs
    closes = day_pack.closes
    n = len(times)
    i0 = max(0, int(start_idx))

    for i in range(i0, n):
        time_str = times[i]
        if time_str <= entry_time:
            continue
        low = float(lows[i])
        high = float(highs[i])
        close = float(closes[i])
        candle_count += 1

        if direction == "LONG":
            favorable = high - entry_price
            adverse = entry_price - low
        else:
            favorable = entry_price - low
            adverse = high - entry_price
        max_favorable = max(max_favorable, favorable)
        max_adverse = max(max_adverse, adverse)

        ts.update(close)
        final_phase = ts.phase

        if ts.is_hit(low, high):
            exit_time = time_str
            exit_price = ts.current_sl
            if ts.phase == "PROTECT":
                exit_reason = SL_PHASE_TO_EXIT_REASON["PROTECT"]
            elif ts.phase == "BREAKEVEN":
                exit_reason = SL_PHASE_TO_EXIT_REASON["BREAKEVEN"]
            elif ts.phase == "TRAIL":
                exit_reason = SL_PHASE_TO_EXIT_REASON["TRAIL"]
            else:
                exit_reason = "SL"
            _record_fill(remaining_qty, exit_price)
            break

        if runner_target_price is not None and scale_out_qty > 0 and runner_qty > 0:
            first_target_hit = (direction == "LONG" and high >= target_price) or (
                direction == "SHORT" and low <= target_price
            )
            runner_target_hit = (direction == "LONG" and high >= runner_target_price) or (
                direction == "SHORT" and low <= runner_target_price
            )
            if first_target_hit:
                exit_time = time_str
                if runner_target_hit and not scaled_out:
                    _record_fill(scale_out_qty, target_price)
                    _record_fill(runner_qty, runner_target_price)
                    exit_reason = "TARGET"
                    break
                if not scaled_out:
                    _record_fill(scale_out_qty, target_price)
                    scaled_out = True
                    if direction == "LONG":
                        ts.phase = "BREAKEVEN"
                        ts.current_sl = max(ts.current_sl, entry_price)
                    else:
                        ts.phase = "BREAKEVEN"
                        ts.current_sl = min(ts.current_sl, entry_price)
                    final_phase = ts.phase
                    continue
            if scaled_out and runner_target_hit:
                exit_time = time_str
                _record_fill(remaining_qty, runner_target_price)
                exit_reason = "TARGET"
                break
        else:
            if direction == "LONG" and high >= target_price:
                exit_time = time_str
                exit_price = target_price
                exit_reason = "TARGET"
                _record_fill(remaining_qty, target_price)
                break
            if direction == "SHORT" and low <= target_price:
                exit_time = time_str
                exit_price = target_price
                exit_reason = "TARGET"
                _record_fill(remaining_qty, target_price)
                break

        if candle_exit > 0 and candle_count >= candle_exit:
            exit_time = time_str
            exit_price = close
            exit_reason = "CANDLE_EXIT"
            _record_fill(remaining_qty, close)
            break

        if time_str >= time_exit:
            exit_time = time_str
            exit_price = close
            exit_reason = "TIME"
            _record_fill(remaining_qty, close)
            break

    if exit_price is None:
        if i0 < n:
            exit_price = float(closes[-1])
        else:
            exit_price = entry_price
        exit_time = time_exit
        exit_reason = "TIME"

    if direction == "LONG":
        pl_pts = exit_price - entry_price
    else:
        pl_pts = entry_price - exit_price

    pl_total = pl_pts * position_size
    pl_pct = (pl_pts / entry_price * 100) if entry_price > 0 else 0.0
    mfe_r = round(max_favorable / sl_distance, 4) if sl_distance > 0 else 0.0
    mae_r = round(-max_adverse / sl_distance, 4) if sl_distance > 0 else 0.0
    max_r = round(max_favorable / sl_distance, 4) if sl_distance > 0 else 0.0

    return TradeLifecycleOutcome(
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=exit_reason,
        sl_phase=final_phase,
        profit_loss=round(pl_total, 2),
        profit_loss_pct=round(pl_pct, 4),
        mfe_r=mfe_r,
        mae_r=mae_r,
        max_favorable=max_favorable,
        max_adverse=max_adverse,
        candle_count=candle_count,
        reached_1r=mfe_r >= 1.0,
        reached_2r=mfe_r >= 2.0,
        max_r=max_r,
        exit_fills=tuple(exit_fills),
    )
