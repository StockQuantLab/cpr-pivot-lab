from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from engine.bar_orchestrator import SessionPositionTracker
from engine.paper_runtime import PaperRuntimeState, SymbolRuntimeState
from engine.paper_session_driver import process_closed_bar_group


@pytest.mark.asyncio
async def test_process_closed_bar_group_skips_duplicate_candle() -> None:
    bar_end = datetime(2026, 4, 9, 9, 20, tzinfo=UTC)
    runtime_state = PaperRuntimeState()
    runtime_state.symbols["SBIN"] = SymbolRuntimeState(
        trade_date="2026-04-09",
        candles=[{"bar_end": bar_end}],
        setup_row={"direction": "SHORT"},
    )
    tracker = SessionPositionTracker(max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.10)
    params = SimpleNamespace(entry_window_end="10:15")
    candle = SimpleNamespace(
        symbol="SBIN",
        bar_end=bar_end,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1000.0,
    )

    called = False

    async def _evaluate_candle(**_: object) -> dict[str, object]:
        nonlocal called
        called = True
        return {"action": "ENTRY_CANDIDATE"}

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    result = await process_closed_bar_group(
        session_id="s1",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=[candle],
        runtime_state=runtime_state,
        tracker=tracker,
        params=params,
        active_symbols=["SBIN"],
        strategy="CPR_LEVELS",
        direction_filter="BOTH",
        stage_b_applied=False,
        symbol_last_prices={},
        last_price=None,
        evaluate_candle_fn=_evaluate_candle,
        execute_entry_fn=lambda **_: {"action": "SKIP"},
        enforce_risk_controls=_enforce_risk_controls,
        build_feed_state=lambda **_: SimpleNamespace(),
    )

    assert called is False
    assert result["active_symbols"] == []
    assert result["triggered"] is False
