"""Tests for engine/cpr_atr_utils.py — CPR calculations + TrailingStop."""

import pytest

from engine.cpr_atr_utils import (
    TrailingStop,
    calculate_cpr,
    calculate_gap_pct,
    calculate_rvol,
    check_entry_setup,
    safe_divide,
    validate_and_adjust_sl_distance,
)


class TestCalculateCPR:
    """Test CPR + floor pivot level calculations."""

    def test_basic_cpr_levels(self):
        """Verify Pivot, TC, BC with known values."""
        result = calculate_cpr(prev_high=100.0, prev_low=90.0, prev_close=95.0)
        assert result["pivot"] == pytest.approx(95.0, abs=0.01)
        assert result["bc"] == pytest.approx(95.0, abs=0.01)
        # TC = 2*pivot - bc = 2*95 - 95 = 95
        assert result["tc"] == pytest.approx(95.0, abs=0.01)

    def test_floor_pivot_r1_s1(self):
        """R1 = 2*Pivot - Low, S1 = 2*Pivot - High."""
        result = calculate_cpr(prev_high=100.0, prev_low=90.0, prev_close=95.0)
        # R1 = 2*95 - 90 = 100
        assert result["r1"] == pytest.approx(100.0, abs=0.01)
        # S1 = 2*95 - 100 = 90
        assert result["s1"] == pytest.approx(90.0, abs=0.01)

    def test_floor_pivot_r2_s2(self):
        """R2 = Pivot + (H-L), S2 = Pivot - (H-L)."""
        result = calculate_cpr(prev_high=100.0, prev_low=90.0, prev_close=95.0)
        # R2 = 95 + (100-90) = 105
        assert result["r2"] == pytest.approx(105.0, abs=0.01)
        # S2 = 95 - (100-90) = 85
        assert result["s2"] == pytest.approx(85.0, abs=0.01)

    def test_floor_pivot_r3_s3(self):
        """R3 = H + 2*(Pivot-Low), S3 = L - 2*(H-L)."""
        result = calculate_cpr(prev_high=100.0, prev_low=90.0, prev_close=95.0)
        # R3 = 100 + 2*(95-90) = 110
        assert result["r3"] == pytest.approx(110.0, abs=0.01)
        # S3 = 90 - 2*(100-90) = 70
        assert result["s3"] == pytest.approx(70.0, abs=0.01)

    def test_cpr_width_pct(self):
        """CPR width percentage is non-negative."""
        result = calculate_cpr(prev_high=500.0, prev_low=480.0, prev_close=490.0)
        assert result["cpr_width_pct"] >= 0.0
        assert result["cpr_width"] >= 0.0

    def test_all_keys_present(self):
        """Verify all expected keys are in the return dict."""
        result = calculate_cpr(100.0, 90.0, 95.0)
        expected_keys = {
            "pivot",
            "tc",
            "bc",
            "cpr_width",
            "cpr_width_pct",
            "r1",
            "s1",
            "r2",
            "s2",
            "r3",
            "s3",
        }
        assert set(result.keys()) == expected_keys

    def test_pivot_ordering(self):
        """S3 < S2 < S1 < Pivot < R1 < R2 < R3 for normal data."""
        result = calculate_cpr(prev_high=110.0, prev_low=90.0, prev_close=100.0)
        assert result["s3"] < result["s2"] < result["s1"]
        assert result["s1"] < result["pivot"]
        assert result["pivot"] < result["r1"] < result["r2"] < result["r3"]

    def test_zero_pivot_no_division_error(self):
        """Edge case: all zeros should not raise."""
        result = calculate_cpr(0.0, 0.0, 0.0)
        assert result["cpr_width_pct"] == 0.0


class TestCalculateRVOL:
    """Test relative volume calculation."""

    def test_normal(self):
        assert calculate_rvol(100.0, 50.0) == pytest.approx(2.0)

    def test_zero_avg(self):
        assert calculate_rvol(100.0, 0.0) == 0.0

    def test_negative_avg(self):
        assert calculate_rvol(100.0, -10.0) == 0.0


class TestSafeDivide:
    """Test safe division helper used by calculation functions."""

    def test_returns_default_for_zero_denominator(self):
        assert safe_divide(10.0, 0.0, default=7.5) == pytest.approx(7.5)

    def test_divides_when_denominator_nonzero(self):
        assert safe_divide(9.0, 3.0) == pytest.approx(3.0)


class TestGapPct:
    """Test gap percentage helper guardrails."""

    def test_gap_pct_handles_none_prev_close(self):
        assert calculate_gap_pct(open_price=100.0, prev_close=None) == 0.0

    def test_gap_pct_uses_safe_division(self):
        assert calculate_gap_pct(open_price=105.0, prev_close=100.0) == pytest.approx(5.0)


class TestCheckEntrySetup:
    """Test entry validation logic."""

    def test_long_setup(self):
        candle = {"open": 510, "high": 515, "low": 507, "close": 514}
        cpr = {"tc": 512, "bc": 510, "pivot": 511, "cpr_width_pct": 0.3}
        result = check_entry_setup(candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        assert result is not None
        assert result["direction"] == "LONG"

    def test_short_setup(self):
        candle = {"open": 515, "high": 516, "low": 508, "close": 509}
        cpr = {"tc": 512, "bc": 510, "pivot": 511, "cpr_width_pct": 0.3}
        result = check_entry_setup(candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        assert result is not None
        assert result["direction"] == "SHORT"

    def test_skip_wide_cpr(self):
        candle = {"open": 510, "high": 520, "low": 505, "close": 518}
        cpr = {"tc": 515, "bc": 510, "pivot": 512.5, "cpr_width_pct": 2.0}
        result = check_entry_setup(candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        assert result is None

    def test_skip_inside_cpr(self):
        candle = {"open": 510, "high": 515, "low": 508, "close": 512}
        cpr = {"tc": 515, "bc": 510, "pivot": 512.5, "cpr_width_pct": 0.5}
        result = check_entry_setup(candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        assert result is None

    def test_flipped_cpr_band_uses_normalized_bounds(self):
        long_candle = {"open": 512, "high": 514, "low": 508, "close": 513}
        short_candle = {"open": 512, "high": 513, "low": 507, "close": 509}
        inside_candle = {"open": 512, "high": 513, "low": 508, "close": 511}
        cpr = {"tc": 510, "bc": 512, "pivot": 511, "cpr_width_pct": 0.5}

        long_result = check_entry_setup(long_candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        short_result = check_entry_setup(short_candle, cpr, atr=5.0, cpr_threshold_pct=1.0)
        inside_result = check_entry_setup(inside_candle, cpr, atr=5.0, cpr_threshold_pct=1.0)

        assert long_result is not None
        assert long_result["direction"] == "LONG"
        assert short_result is not None
        assert short_result["direction"] == "SHORT"
        assert inside_result is None


class TestTrailingStop:
    """Test the 4-phase trailing stop engine."""

    def test_long_protect_phase(self):
        ts = TrailingStop(entry_price=100.0, direction="LONG", sl_price=95.0, atr=3.0)
        assert ts.phase == "PROTECT"
        assert ts.current_sl == 95.0

    def test_long_breakeven_transition(self):
        ts = TrailingStop(
            entry_price=100.0,
            direction="LONG",
            sl_price=95.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        # Move price to 1R (100 + 5 = 105)
        ts.update(105.0)
        assert ts.phase == "BREAKEVEN"
        assert ts.current_sl == 100.0  # Moved to entry

    def test_long_trail_transition(self):
        ts = TrailingStop(
            entry_price=100.0,
            direction="LONG",
            sl_price=95.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(105.0)  # → BREAKEVEN
        ts.update(110.0)  # → TRAIL activates and tightens immediately after bar close
        assert ts.phase == "TRAIL"
        assert ts.current_sl == pytest.approx(107.0, abs=0.01)
        # Next bar: TRAIL branch keeps the stop at the same level unless a new high prints
        ts.update(110.0)
        assert ts.current_sl == pytest.approx(107.0, abs=0.01)

    def test_short_protect_phase(self):
        ts = TrailingStop(entry_price=100.0, direction="SHORT", sl_price=105.0, atr=3.0)
        assert ts.phase == "PROTECT"
        assert ts.current_sl == 105.0

    def test_short_breakeven_transition(self):
        ts = TrailingStop(
            entry_price=100.0,
            direction="SHORT",
            sl_price=105.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(95.0)  # Move to 1R
        assert ts.phase == "BREAKEVEN"
        assert ts.current_sl == 100.0

    def test_is_hit_long(self):
        ts = TrailingStop(entry_price=100.0, direction="LONG", sl_price=95.0, atr=3.0)
        assert ts.is_hit(candle_low=94.0, candle_high=102.0) is True
        assert ts.is_hit(candle_low=96.0, candle_high=102.0) is False

    def test_is_hit_short(self):
        ts = TrailingStop(entry_price=100.0, direction="SHORT", sl_price=105.0, atr=3.0)
        assert ts.is_hit(candle_low=98.0, candle_high=106.0) is True
        assert ts.is_hit(candle_low=98.0, candle_high=104.0) is False

    def test_trail_sl_only_moves_favorable_direction(self):
        ts = TrailingStop(
            entry_price=100.0,
            direction="LONG",
            sl_price=95.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(105.0)  # → BREAKEVEN
        ts.update(110.0)  # → TRAIL activates, SL deferred at 100
        ts.update(110.0)  # TRAIL tightens: highest=110, SL=107
        assert ts.current_sl == pytest.approx(107.0, abs=0.01)
        ts.update(108.0)  # Price drops — SL must NOT move down
        assert ts.current_sl == pytest.approx(107.0, abs=0.01)
        ts.update(112.0)  # New high — SL moves up to 109
        assert ts.current_sl == pytest.approx(109.0, abs=0.01)

    def test_long_intraday_high_triggers_trail_after_breakeven(self):
        """Bar whose high reaches 2R but close doesn't should still arm TRAIL."""
        ts = TrailingStop(
            entry_price=100.0,
            direction="LONG",
            sl_price=95.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(105.0)  # Close=1R → BREAKEVEN
        assert ts.phase == "BREAKEVEN"
        # Close=107 (< 2R=110), candle high=112 (>2R) → TRAIL activates and tightens to 109
        ts.update(107.0, candle_high=112.0)
        assert ts.phase == "TRAIL"
        assert ts.current_sl == pytest.approx(109.0, abs=0.01)

    def test_long_same_bar_multi_transition_protect_to_trail(self):
        """Single candle crossing both 1R close and 2R high fires both transitions."""
        ts = TrailingStop(
            entry_price=100.0,
            direction="LONG",
            sl_price=95.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        # close=105 (≥ 1R breakeven), high=112 (≥ 2R trail target=110)
        # Both PROTECT→BREAKEVEN and BREAKEVEN→TRAIL should fire in one call.
        ts.update(105.0, candle_high=112.0)
        assert ts.phase == "TRAIL"
        assert ts.current_sl == pytest.approx(109.0, abs=0.01)

    def test_short_intraday_low_triggers_trail_after_breakeven(self):
        """SHORT: bar whose low reaches 2R but close doesn't should activate TRAIL."""
        ts = TrailingStop(
            entry_price=100.0,
            direction="SHORT",
            sl_price=105.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(95.0)  # Close=1R → BREAKEVEN
        assert ts.phase == "BREAKEVEN"
        # Close=93 (>target=90), candle low=88 (<90) → TRAIL activates, SL deferred
        ts.update(93.0, candle_low=88.0)
        assert ts.phase == "TRAIL"
        assert ts.current_sl == pytest.approx(100.0, abs=0.01)  # Deferred
        # Next bar: lowest_since_entry honors the prior candle low=88, new_sl=88+3=91.
        ts.update(93.0)
        assert ts.current_sl == pytest.approx(91.0, abs=0.01)

    def test_short_same_bar_multi_transition_protect_to_trail(self):
        """SHORT: single candle crossing both 1R close and 2R low fires both transitions."""
        ts = TrailingStop(
            entry_price=100.0,
            direction="SHORT",
            sl_price=105.0,
            atr=3.0,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        # close=95 (≤ 1R breakeven), low=88 (≤ 2R target=90)
        ts.update(95.0, candle_low=88.0)
        assert ts.phase == "TRAIL"
        assert ts.current_sl == pytest.approx(100.0, abs=0.01)  # Deferred (close>target=90)
        # Next bar: lowest_since_entry honors the prior candle low=88, new_sl=88+3=91.
        ts.update(94.0)
        assert ts.current_sl == pytest.approx(91.0, abs=0.01)

    def test_short_trail_multiplier_can_loosen_stop(self):
        """SHORT trail multiplier should widen the post-activation stop distance."""
        ts = TrailingStop(
            entry_price=100.0,
            direction="SHORT",
            sl_price=105.0,
            atr=3.0,
            trail_atr_multiplier=1.5,
            rr_ratio=2.0,
            breakeven_r=1.0,
        )
        ts.update(95.0)  # → BREAKEVEN
        ts.update(93.0, candle_low=88.0)  # → TRAIL, SL deferred
        ts.update(93.0)  # trailing branch now uses 1.5x ATR
        assert ts.current_sl == pytest.approx(92.5, abs=0.01)


class TestValidateAndAdjustSLDistance:
    """Test ATR guardrail helper used by strategy simulations."""

    def test_floors_tight_sl_distance(self):
        adjusted, ok = validate_and_adjust_sl_distance(
            sl_distance=0.1,
            atr=10.0,
            min_sl_atr_ratio=0.5,
            max_sl_atr_ratio=2.0,
        )
        assert ok is True
        assert adjusted == pytest.approx(5.0)

    def test_rejects_wide_sl_distance(self):
        adjusted, ok = validate_and_adjust_sl_distance(
            sl_distance=25.0,
            atr=10.0,
            min_sl_atr_ratio=0.5,
            max_sl_atr_ratio=2.0,
        )
        assert ok is False
        assert adjusted == pytest.approx(25.0)

    def test_rejects_non_positive_atr(self):
        adjusted, ok = validate_and_adjust_sl_distance(
            sl_distance=1.0,
            atr=0.0,
            min_sl_atr_ratio=0.5,
            max_sl_atr_ratio=2.0,
        )
        assert ok is False
        assert adjusted == pytest.approx(1.0)
