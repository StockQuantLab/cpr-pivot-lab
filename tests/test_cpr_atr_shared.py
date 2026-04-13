"""Tests for shared CPR-ATR extraction helpers."""

from __future__ import annotations

import pytest

from engine.cpr_atr_shared import (
    find_first_close_idx,
    get_cpr_entry_scan_start,
    normalize_stop_loss,
    simulate_trade_lifecycle,
)
from engine.cpr_atr_strategy import DayPack


def test_get_cpr_entry_scan_start_uses_explicit_override() -> None:
    assert get_cpr_entry_scan_start(5, "09:25") == "09:25"


def test_get_cpr_entry_scan_start_defaults_from_or_minutes() -> None:
    assert get_cpr_entry_scan_start(5, "") == "09:20"
    assert get_cpr_entry_scan_start(15, None) == "09:30"


def test_normalize_stop_loss_preserves_direction_and_guardrails() -> None:
    long_result = normalize_stop_loss(
        entry_price=100.0,
        sl_price=95.0,
        direction="LONG",
        atr=10.0,
        min_sl_atr_ratio=0.5,
        max_sl_atr_ratio=2.0,
    )
    assert long_result == (95.0, 5.0)

    short_result = normalize_stop_loss(
        entry_price=100.0,
        sl_price=105.0,
        direction="SHORT",
        atr=10.0,
        min_sl_atr_ratio=0.5,
        max_sl_atr_ratio=2.0,
    )
    assert short_result == (105.0, 5.0)


def test_normalize_stop_loss_clamps_too_wide_distance() -> None:
    assert normalize_stop_loss(
        entry_price=100.0,
        sl_price=125.0,
        direction="LONG",
        atr=10.0,
        min_sl_atr_ratio=0.5,
        max_sl_atr_ratio=2.0,
    ) == (95.0, 5.0)


def test_find_first_close_idx_respects_direction() -> None:
    closes = [99.0, 100.0, 101.0, 98.0]
    assert find_first_close_idx(closes, 0, 3, direction="LONG", trigger=100.5) == 2
    assert find_first_close_idx(closes, 0, 3, direction="SHORT", trigger=99.5) == 0


def test_simulate_trade_lifecycle_long_hits_target() -> None:
    pack = DayPack(
        time_str=["09:20", "09:25", "09:30"],
        opens=[100.0, 101.0, 102.0],
        highs=[101.0, 111.0, 103.0],
        lows=[99.0, 101.0, 101.0],
        closes=[100.0, 108.0, 102.0],
        volumes=[1000.0, 1100.0, 1200.0],
    )

    outcome = simulate_trade_lifecycle(
        day_pack=pack,
        start_idx=1,
        entry_price=100.0,
        sl_price=95.0,
        target_price=110.0,
        direction="LONG",
        sl_distance=5.0,
        atr=10.0,
        position_size=100,
        entry_time="09:20",
        time_exit="15:15",
        rr_ratio=2.0,
        breakeven_r=1.0,
    )

    assert outcome.exit_reason == "TARGET"
    assert outcome.exit_price == pytest.approx(110.0)
    assert outcome.exit_time == "09:25"
    assert outcome.profit_loss == pytest.approx(1000.0)
    assert outcome.mfe_r == pytest.approx(2.2, abs=0.0001)
    assert outcome.mae_r == pytest.approx(0.0, abs=0.0001)


def test_simulate_trade_lifecycle_scales_out_and_runners() -> None:
    pack = DayPack(
        time_str=["09:20", "09:25"],
        opens=[100.0, 111.0],
        highs=[111.0, 121.0],
        lows=[100.5, 110.5],
        closes=[110.0, 119.0],
        volumes=[1000.0, 1100.0],
    )

    outcome = simulate_trade_lifecycle(
        day_pack=pack,
        start_idx=0,
        entry_price=100.0,
        sl_price=95.0,
        target_price=110.0,
        runner_target_price=120.0,
        scale_out_pct=0.8,
        direction="LONG",
        sl_distance=5.0,
        atr=10.0,
        position_size=100,
        entry_time="09:15",
        time_exit="15:15",
        rr_ratio=2.0,
        breakeven_r=1.0,
    )

    assert outcome.exit_reason == "TARGET"
    assert outcome.exit_time == "09:25"
    assert outcome.exit_price == pytest.approx(112.0)
    assert outcome.profit_loss == pytest.approx(1200.0)
    assert outcome.exit_fills == ((80.0, 110.0), (20.0, 120.0))
