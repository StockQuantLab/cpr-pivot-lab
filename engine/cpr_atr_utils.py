"""
CPR-ATR Strategy v2 — Core Utilities

Pure calculations with no database dependency.
All DB interaction is handled by db/duckdb.py.

Classes:
    TrailingStop: 4-phase SL/target/trailing manager

Functions:
    calculate_cpr:          CPR levels from prev day OHLC
    calculate_rvol:         Relative volume ratio
    check_entry_setup:      Validate entry conditions, return setup dict or None
"""

from __future__ import annotations

import polars as pl

# ---------------------------------------------------------------------------
# TrailingStop: 4-phase SL management
# ---------------------------------------------------------------------------


class TrailingStop:
    """
    Manages stop-loss through 4 phases for CPR-ATR strategy.

    Phases:
        PROTECT   → SL at OR extreme (initial risk)
        BREAKEVEN → SL moves to entry (risk-free after breakeven_r × R)
        TRAIL     → SL trails at highest/lowest - 1×ATR (after rr_ratio target)
        DONE      → Position closed

    Usage:
        ts = TrailingStop(entry_price=520.0, direction="LONG", sl_price=515.0, atr=3.5)
        for candle in candles:
            ts.update(candle["close"], candle_high=candle["high"], candle_low=candle["low"])
            if ts.is_hit(candle["low"], candle["high"]):
                exit_price = ts.current_sl
                break

    Note on SL deferral: TRAIL phase activation (whether via candle close or intraday
    high/low) always defers SL tightening to the *next* bar. This avoids same-bar
    ambiguity — OHLC order within a 5-min candle is unknown, so applying a tighter
    stop on the activation bar itself would be an optimistic modeling assumption.
    """

    def __init__(
        self,
        entry_price: float,
        direction: str,  # "LONG" or "SHORT"
        sl_price: float,
        atr: float,
        trail_atr_multiplier: float = 1.0,
        rr_ratio: float = 2.0,
        breakeven_r: float = 1.0,
    ):
        self.entry_price = entry_price
        self.direction = direction.upper()
        self.atr = atr
        self.trail_atr_multiplier = trail_atr_multiplier
        self.initial_sl = sl_price
        self.current_sl = sl_price

        if self.direction == "LONG":
            self.sl_distance = entry_price - sl_price
            self.target_price = entry_price + (rr_ratio * self.sl_distance)
            self.breakeven_level = entry_price + (breakeven_r * self.sl_distance)
        else:
            self.sl_distance = sl_price - entry_price
            self.target_price = entry_price - (rr_ratio * self.sl_distance)
            self.breakeven_level = entry_price - (breakeven_r * self.sl_distance)

        self.phase = "PROTECT"
        self.highest_since_entry = entry_price
        self.lowest_since_entry = entry_price

    def update(
        self,
        current_price: float,
        candle_high: float | None = None,
        candle_low: float | None = None,
    ) -> dict:
        """
        Update state based on current candle close price.

        Args:
            current_price: Candle close price (used for all phase tracking).
            candle_high: Candle high — when provided, also used to trigger the
                BREAKEVEN→TRAIL transition for LONG positions so that intraday
                peaks activate trailing even if the close does not reach 2R.
                SL tightening is deferred to the next bar when triggered only
                by the intraday high (same-bar ambiguity avoidance).
            candle_low: Candle low — mirror of candle_high for SHORT positions.

        Returns: {"sl": float, "phase": str}
        """
        if self.direction == "LONG":
            # highest_since_entry tracks closes only; SL is computed from this anchor.
            # Note: using closes (not highs) is intentional and conservative — it keeps
            # the trailing reference tighter, which benefits LONG continuation trades but
            # can cause SHORT trailing exits to clip below the original fixed target when
            # the post-2R SHORT move is weak. The trade-off is accepted to avoid over-fit.
            self.highest_since_entry = max(self.highest_since_entry, current_price)

            # BREAKEVEN→TRAIL trigger considers intraday high so that a bar whose
            # high reaches 2R (but close does not) still activates the trail phase.
            trail_trigger = (
                max(current_price, candle_high) if candle_high is not None else current_price
            )

            if self.phase == "PROTECT" and current_price >= self.breakeven_level:
                self.current_sl = self.entry_price
                self.phase = "BREAKEVEN"

            # Separate (not elif) so PROTECT→BREAKEVEN and BREAKEVEN→TRAIL can both
            # fire on the same bar when a single candle crosses both thresholds.
            if self.phase == "BREAKEVEN" and trail_trigger >= self.target_price:
                # SL tightening is always deferred to the next bar — whether TRAIL was
                # triggered by the candle close or the intraday high. The OHLC order
                # inside a 5-min bar is unknown, so tightening on the activation bar
                # itself would be optimistic: a candle that closes through 2R can also
                # have traded back through the new stop within the same bar.
                # The TRAIL branch on the next update() call tightens correctly.
                self.phase = "TRAIL"

            elif self.phase == "TRAIL":
                new_sl = self.highest_since_entry - (self.atr * self.trail_atr_multiplier)
                if new_sl > self.current_sl:
                    self.current_sl = new_sl

        else:  # SHORT
            self.lowest_since_entry = min(self.lowest_since_entry, current_price)

            trail_trigger = (
                min(current_price, candle_low) if candle_low is not None else current_price
            )

            if self.phase == "PROTECT" and current_price <= self.breakeven_level:
                self.current_sl = self.entry_price
                self.phase = "BREAKEVEN"

            if self.phase == "BREAKEVEN" and trail_trigger <= self.target_price:
                # Same deferred-SL logic as LONG: tightening happens on the next bar.
                self.phase = "TRAIL"

            elif self.phase == "TRAIL":
                new_sl = self.lowest_since_entry + (self.atr * self.trail_atr_multiplier)
                if new_sl < self.current_sl:
                    self.current_sl = new_sl

        return {"sl": self.current_sl, "phase": self.phase}

    def is_hit(self, candle_low: float, candle_high: float) -> bool:
        """True if this candle's range touches/crosses the stop loss."""
        if self.direction == "LONG":
            return candle_low <= self.current_sl
        return candle_high >= self.current_sl


# ---------------------------------------------------------------------------
# Pure calculation functions (no I/O)
# ---------------------------------------------------------------------------


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two floats; return default when denominator is near zero."""
    if abs(float(denominator)) < 1e-12:
        return float(default)
    return float(numerator) / float(denominator)


def calculate_cpr(prev_high: float, prev_low: float, prev_close: float) -> dict:
    """
    Calculate CPR levels + floor pivot levels from previous day's OHLC.

    Returns:
        pivot, tc, bc, cpr_width, cpr_width_pct,
        r1, s1, r2, s2, r3, s3
    """
    pivot = (prev_high + prev_low + prev_close) / 3.0
    bc = (prev_high + prev_low) / 2.0
    tc = (pivot + bc) / 2.0
    cpr_width = abs(tc - bc)
    cpr_width_pct = safe_divide(cpr_width, pivot, default=0.0) * 100.0

    # Floor pivot levels
    r1 = 2.0 * pivot - prev_low
    s1 = 2.0 * pivot - prev_high
    r2 = pivot + (prev_high - prev_low)
    s2 = pivot - (prev_high - prev_low)
    r3 = prev_high + 2.0 * (pivot - prev_low)
    s3 = prev_low - 2.0 * (prev_high - prev_low)

    return {
        "pivot": pivot,
        "tc": tc,
        "bc": bc,
        "cpr_width": cpr_width,
        "cpr_width_pct": cpr_width_pct,
        "r1": r1,
        "s1": s1,
        "r2": r2,
        "s2": s2,
        "r3": r3,
        "s3": s3,
    }


def calculate_rvol(candle_volume: float, avg_volume: float) -> float:
    """
    Relative Volume = candle_volume / avg_volume at same time slot.

    Returns 0.0 if avg_volume is zero (no baseline data).
    """
    if avg_volume <= 0:
        return 0.0
    return candle_volume / avg_volume


# ---------------------------------------------------------------------------
# Helper calculation functions (eliminate code duplication)
# ---------------------------------------------------------------------------


def calculate_gap_pct(open_price: float, prev_close: float | None) -> float:
    """
    Calculate gap percentage from previous close.

    Gap = ((open - prev_close) / prev_close) * 100

    Args:
        open_price: Current day's opening price
        prev_close: Previous day's close price (None returns 0.0)

    Returns:
        Gap percentage rounded to 4 decimals, or 0.0 if prev_close is invalid
    """
    if prev_close is None or prev_close <= 0:
        return 0.0
    return round(safe_divide(open_price - prev_close, prev_close, default=0.0) * 100.0, 4)


def calculate_or_atr_ratio(or_high: float, or_low: float, atr: float) -> float:
    """
    Calculate Opening Range to ATR ratio.

    OR/ATR = (or_high - or_low) / atr

    Args:
        or_high: Opening Range high price
        or_low: Opening Range low price
        atr: Average True Range value

    Returns:
        OR/ATR ratio rounded to 4 decimals, or 0.0 if atr is invalid
    """
    if atr <= 0:
        return 0.0
    or_range = or_high - or_low
    return round(or_range / atr, 4)


def normalize_cpr_bounds(tc: float, bc: float) -> tuple[float, float]:
    """Return the CPR band as (lower, upper) regardless of TC/BC naming order."""
    return (min(tc, bc), max(tc, bc))


def resolve_cpr_direction(
    close_price: float | None,
    tc: float,
    bc: float,
    *,
    fallback: str = "NONE",
) -> str:
    """Resolve CPR direction from a close price against the normalized CPR band."""
    if close_price is None:
        return fallback
    lower, upper = normalize_cpr_bounds(tc, bc)
    if close_price > upper:
        return "LONG"
    if close_price < lower:
        return "SHORT"
    return fallback


def calculate_position_size(capital_base: float, risk_pct: float, sl_distance: float) -> int:
    """
    Calculate position size based on risk parameters.

    Position = max(1, int((capital_base * risk_pct) / sl_distance))

    Args:
        capital_base: Current risk capital base for sizing
        risk_pct: Risk percentage per trade (e.g., 0.01 for 1%)
        sl_distance: Stop loss distance in price points

    Returns:
        Position size (minimum 1)
    """
    if sl_distance <= 0:
        return 1
    return max(1, int((capital_base * risk_pct) / sl_distance))


def validate_and_adjust_sl_distance(
    sl_distance: float,
    atr: float,
    min_sl_atr_ratio: float = 0.5,
    max_sl_atr_ratio: float = 2.0,
) -> tuple[float, bool]:
    """
    Validate and adjust SL distance against ATR guardrails.

    Args:
        sl_distance: Initial stop loss distance
        atr: Average True Range
        min_sl_atr_ratio: Minimum SL as multiple of ATR (default 0.5)
        max_sl_atr_ratio: Maximum SL as multiple of ATR (default 2.0)

    Returns:
        Tuple of (adjusted_sl_distance, is_valid)
        - is_valid is False if SL exceeds max_sl_atr_ratio
        - SL is floored at min_sl_atr_ratio * atr if too tight
    """
    if atr <= 0:
        return sl_distance, False

    # Floor SL at minimum ATR ratio
    if sl_distance < min_sl_atr_ratio * atr:
        sl_distance = min_sl_atr_ratio * atr

    # Reject if SL exceeds maximum ATR ratio
    if sl_distance > max_sl_atr_ratio * atr:
        return sl_distance, False

    return sl_distance, True


def check_entry_setup(
    candle_915: dict,
    cpr: dict,
    atr: float,
    cpr_threshold_pct: float,
    buffer_pct: float = 0.0005,
    min_sl_atr_ratio: float = 0.5,
    max_sl_atr_ratio: float = 2.0,
    rr_ratio: float = 2.0,
    capital: float = 100_000,
    risk_pct: float = 0.01,
    atr_sl_buffer: float = 0.0,
) -> dict | None:
    """
    Validate entry conditions after the 9:15 observation candle.

    Returns setup dict if entry is valid, None otherwise.

    Observation window (9:15 candle only):
        OR_High = 9:15 high, OR_Low = 9:15 low
        Direction = 9:15 close vs normalized CPR band:
            close > upper → LONG, close < lower → SHORT
    Entry window begins at 9:20.

    Filters:
        1. CPR width < threshold (narrow CPR = strong trend day)
        2. 9:15 close relative to CPR (directional bias)
        3. SL distance within ATR limits (not too tight, not too wide)

    Args:
        atr_sl_buffer: ATR multiplier added as noise buffer beyond the OR extreme.
            LONG:  sl_price = or_low  - (atr_sl_buffer * atr)
            SHORT: sl_price = or_high + (atr_sl_buffer * atr)
            0.5 is recommended to prevent premature SL hits from random wicks.

    Returns dict with:
        direction, entry_trigger, sl_price, target_price, sl_distance,
        or_high, or_low, rr_ratio, position_size
    """
    # Filter 1: CPR width check
    if cpr["cpr_width_pct"] >= cpr_threshold_pct:
        return None  # Wide CPR = consolidation day, skip

    or_high = candle_915["high"]
    or_low = candle_915["low"]
    close_915 = candle_915["close"]
    tc = cpr["tc"]
    bc = cpr["bc"]

    direction = resolve_cpr_direction(close_915, tc, bc, fallback="")
    if direction not in {"LONG", "SHORT"}:
        return None  # Price inside CPR = no bias, skip

    atr_buffer = atr_sl_buffer * atr
    if direction == "LONG":
        entry_trigger = or_high * (1.0 + buffer_pct)
        sl_price = or_low - atr_buffer  # Noise buffer below OR low
        sl_distance = entry_trigger - sl_price
    else:
        entry_trigger = or_low * (1.0 - buffer_pct)
        sl_price = or_high + atr_buffer  # Noise buffer above OR high
        sl_distance = sl_price - entry_trigger

    # Filter 3: SL distance vs ATR limits
    sl_distance, is_valid = validate_and_adjust_sl_distance(
        sl_distance=sl_distance,
        atr=atr,
        min_sl_atr_ratio=min_sl_atr_ratio,
        max_sl_atr_ratio=max_sl_atr_ratio,
    )
    if not is_valid:
        return None  # SL too wide, skip (avoids low-probability setups)

    target_price = (
        entry_trigger + (rr_ratio * sl_distance)
        if direction == "LONG"
        else entry_trigger - (rr_ratio * sl_distance)
    )

    position_size = calculate_position_size(capital, risk_pct, sl_distance)

    return {
        "direction": direction,
        "entry_trigger": round(entry_trigger, 2),
        "sl_price": round(sl_price, 2),
        "target_price": round(target_price, 2),
        "sl_distance": round(sl_distance, 2),
        "rr_ratio": rr_ratio,
        "position_size": position_size,
        "or_high": or_high,
        "or_low": or_low,
    }


# ---------------------------------------------------------------------------
# Failed Breakout Reversal (FBR) — detect breakout failure
# ---------------------------------------------------------------------------


def check_failed_breakout(
    candles_after_breakout: pl.DataFrame,
    breakout_direction: str,
    or_high: float,
    or_low: float,
    failure_window: int = 6,
    failure_depth: float = 0.0,
) -> dict | None:
    """
    Detect a failed breakout: price breaks out of OR, then closes back inside within N candles.

    Args:
        candles_after_breakout: Candles starting from the breakout candle (inclusive).
        breakout_direction: "LONG" (broke above OR) or "SHORT" (broke below OR).
        or_high: Opening Range high.
        or_low: Opening Range low.
        failure_window: Max candles to wait for the close back inside OR.
        failure_depth: Minimum fraction of OR range the failure close must penetrate inside OR.
            0.0 = any close back inside (current behaviour).
            0.3 = must close at least 30% of OR range from the breakout edge — filters marginal failures.

    Returns:
        dict with failure_idx, reversal_entry, reversal_direction, extreme_price
        or None if no failure detected within the window.
    """
    if candles_after_breakout.is_empty() or candles_after_breakout.height < 2:
        return None

    or_range = or_high - or_low
    depth_pts = failure_depth * or_range  # Minimum penetration distance in price points

    limit = min(failure_window + 1, candles_after_breakout.height)
    extreme = 0.0

    # Pre-extract numpy arrays (avoid per-iteration dict allocation via .row(named=True))
    _closes = candles_after_breakout["close"].to_numpy()
    _highs = candles_after_breakout["high"].to_numpy()
    _lows = candles_after_breakout["low"].to_numpy()
    _times = candles_after_breakout["time_str"].to_list()
    _vols = candles_after_breakout["volume"].to_numpy()

    for i in range(limit):
        close = float(_closes[i])
        high = float(_highs[i])
        low = float(_lows[i])

        if breakout_direction == "LONG":
            extreme = max(extreme, high)
            if i > 0 and close < (or_high - depth_pts):
                return {
                    "failure_idx": i,
                    "reversal_entry": close,
                    "reversal_direction": "SHORT",
                    "extreme_price": extreme,
                    "failure_time": _times[i],
                    "failure_volume": float(_vols[i]),
                }
        else:  # SHORT breakout
            extreme = min(extreme, low) if extreme > 0 else low
            if i > 0 and close > (or_low + depth_pts):
                return {
                    "failure_idx": i,
                    "reversal_entry": close,
                    "reversal_direction": "LONG",
                    "extreme_price": extreme,
                    "failure_time": _times[i],
                    "failure_volume": float(_vols[i]),
                }

    return None
