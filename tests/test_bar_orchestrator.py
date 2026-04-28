from __future__ import annotations

from types import SimpleNamespace

import pytest

from engine.bar_orchestrator import (
    SessionPositionTracker,
    candidate_quality_score,
    entry_quality_score,
    minimum_trade_notional_for,
    select_entries_for_bar,
    should_process_symbol,
)


def test_session_position_tracker_records_open_and_close_cash_flow() -> None:
    tracker = SessionPositionTracker(max_positions=2, portfolio_value=100_000.0)
    position = SimpleNamespace(
        position_id="p1",
        symbol="SBIN",
        direction="LONG",
        entry_price=500.0,
        stop_loss=490.0,
        target_price=530.0,
        trail_state={"entry_time": "09:20"},
        quantity=100.0,
        current_qty=100.0,
    )

    tracker.record_open(position, position_value=50_000.0)
    assert tracker.has_open_position("SBIN")
    assert tracker.cash_available == pytest.approx(50_000.0)

    tracker.record_close("SBIN", exit_value=52_000.0)
    assert not tracker.has_open_position("SBIN")
    assert tracker.has_traded_today("SBIN")
    assert tracker.cash_available == pytest.approx(102_000.0)


def test_should_process_symbol_respects_window_status_and_open_positions() -> None:
    tracker = SessionPositionTracker(max_positions=2, portfolio_value=100_000.0)

    assert should_process_symbol(
        bar_time="09:20",
        entry_window_end="10:15",
        tracker=tracker,
        symbol="SBIN",
        setup_status="pending",
    )
    assert not should_process_symbol(
        bar_time="09:20",
        entry_window_end="10:15",
        tracker=tracker,
        symbol="SBIN",
        setup_status="rejected",
    )
    assert not should_process_symbol(
        bar_time="10:20",
        entry_window_end="10:15",
        tracker=tracker,
        symbol="SBIN",
        setup_status="candidate",
    )

    position = SimpleNamespace(
        position_id="p1",
        symbol="SBIN",
        direction="LONG",
        entry_price=100.0,
        stop_loss=95.0,
        target_price=110.0,
        trail_state={"entry_time": "09:20"},
        quantity=10.0,
        current_qty=10.0,
    )
    tracker.record_open(position, position_value=1_000.0)
    assert should_process_symbol(
        bar_time="10:30",
        entry_window_end="10:15",
        tracker=tracker,
        symbol="SBIN",
        setup_status="rejected",
    )


def test_select_entries_for_bar_prioritizes_quality_then_symbol_tiebreak() -> None:
    tracker = SessionPositionTracker(max_positions=2, portfolio_value=100_000.0)
    tracker.record_open(
        SimpleNamespace(
            position_id="p1",
            symbol="AAA",
            direction="LONG",
            entry_price=100.0,
            stop_loss=95.0,
            target_price=110.0,
            trail_state={"entry_time": "09:20"},
            quantity=10.0,
            current_qty=10.0,
        ),
        position_value=1_000.0,
    )
    candidates = [
        {"symbol": "INFY", "rr_ratio": 3.0, "or_atr_ratio": 1.0},
        {"symbol": "SBIN", "rr_ratio": 1.5, "or_atr_ratio": 0.25},
        {"symbol": "ABB", "rr_ratio": 3.0, "or_atr_ratio": 1.0},
    ]

    selected = select_entries_for_bar(candidates, tracker)
    assert [item["symbol"] for item in selected] == ["ABB"]


def test_select_entries_for_bar_prioritizes_higher_quality_over_alphabetical_order() -> None:
    tracker = SessionPositionTracker(max_positions=1, portfolio_value=100_000.0)
    candidates = [
        {"symbol": "ZZZ", "rr_ratio": 3.0, "or_atr_ratio": 1.0},
        {"symbol": "AAA", "rr_ratio": 2.0, "or_atr_ratio": 0.2},
    ]

    selected = select_entries_for_bar(candidates, tracker)
    assert [item["symbol"] for item in selected] == ["AAA"]


def test_candidate_quality_score_uses_shared_scalar_score() -> None:
    candidate = {
        "symbol": "SBIN",
        "candidate": {"rr_ratio": 3.0, "or_atr_ratio": 0.5},
    }

    assert candidate_quality_score(candidate) == pytest.approx(
        entry_quality_score(effective_rr=3.0, or_atr_ratio=0.5)
    )


def test_compute_position_qty_risk_sizing_respects_slot_cap() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )

    qty = tracker.compute_position_qty(
        entry_price=61.23,
        risk_based_sizing=True,
        candidate_size=5_882,
    )

    assert qty == 1_633


def test_compute_position_qty_compound_risk_scales_with_capital_base() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )

    qty = tracker.compute_position_qty(
        entry_price=100.0,
        risk_based_sizing=True,
        candidate_size=6_000,
        capital_base=2_000_000.0,
    )

    assert qty == 2_000


def test_compute_position_qty_rejects_dust_notional() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )

    qty = tracker.compute_position_qty(
        entry_price=61.23,
        risk_based_sizing=True,
        candidate_size=1,
    )

    assert qty == 0


def test_update_budget_reduces_future_entry_capacity_without_resizing_open_position() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )
    position = type(
        "Position",
        (),
        {
            "position_id": "pos-1",
            "symbol": "SBIN",
            "direction": "SHORT",
            "entry_price": 100.0,
            "stop_loss": 105.0,
            "target_price": 90.0,
            "quantity": 1000.0,
            "current_qty": 1000.0,
            "trail_state": {},
        },
    )()
    tracker.record_open(position, 100_000.0)

    tracker.update_budget(portfolio_value=500_000.0)

    assert tracker.initial_capital == pytest.approx(500_000.0)
    assert tracker.current_open_notional() == pytest.approx(100_000.0)
    assert tracker.cash_available == pytest.approx(400_000.0)
    assert tracker.slot_capital == pytest.approx(50_000.0)


def test_minimum_trade_notional_for_matches_tracker_rule() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )

    assert minimum_trade_notional_for(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    ) == pytest.approx(tracker.minimum_trade_notional())

    assert minimum_trade_notional_for(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
        capital_base=2_000_000.0,
    ) == pytest.approx(tracker.minimum_trade_notional(capital_base=2_000_000.0))


def test_current_equity_includes_open_positions_cost_basis() -> None:
    tracker = SessionPositionTracker(
        max_positions=10,
        portfolio_value=1_000_000.0,
        max_position_pct=0.10,
    )
    position = SimpleNamespace(
        position_id="p1",
        symbol="SBIN",
        direction="LONG",
        entry_price=250.0,
        stop_loss=245.0,
        target_price=265.0,
        trail_state={"entry_time": "09:20"},
        quantity=400.0,
        current_qty=400.0,
    )

    tracker.record_open(position, position_value=100_000.0)

    assert tracker.cash_available == pytest.approx(900_000.0)
    assert tracker.current_equity() == pytest.approx(1_000_000.0)
