from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from engine.bar_orchestrator import SessionPositionTracker
from engine.live_market_data import IST
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
    tracker = SessionPositionTracker(
        max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.10
    )
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


@pytest.mark.asyncio
async def test_process_closed_bar_group_updates_tracker_on_partial_exit() -> None:
    bar_end = datetime(2026, 4, 9, 9, 20, tzinfo=UTC)
    runtime_state = PaperRuntimeState()
    tracker = SessionPositionTracker(
        max_positions=10, portfolio_value=10_000.0, max_position_pct=1.0
    )
    position = SimpleNamespace(
        position_id="pos-1",
        symbol="SBIN",
        direction="LONG",
        entry_price=100.0,
        stop_loss=95.0,
        target_price=110.0,
        quantity=100.0,
        current_qty=100.0,
        trail_state={},
    )
    tracker.record_open(position, position_value=10_000.0)
    candle = SimpleNamespace(
        symbol="SBIN",
        bar_end=bar_end,
        open=110.0,
        high=111.0,
        low=109.0,
        close=110.0,
        volume=1000.0,
    )

    async def _evaluate_candle(**_: object) -> dict[str, object]:
        return {
            "action": "ADVANCE",
            "advance_result": {
                "action": "PARTIAL",
                "exit_value": 6_600.0,
                "remaining_qty": 40.0,
            },
        }

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    result = await process_closed_bar_group(
        session_id="s1",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=[candle],
        runtime_state=runtime_state,
        tracker=tracker,
        params=SimpleNamespace(entry_window_end="10:15"),
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

    assert position.current_qty == pytest.approx(40.0)
    assert tracker.current_open_notional() == pytest.approx(4_000.0)
    assert tracker.cash_available == pytest.approx(6_600.0)
    assert result["triggered"] is False


@pytest.mark.asyncio
async def test_process_closed_bar_group_keeps_none_symbols_pending_before_entry_window_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bar_end = datetime(2026, 4, 15, 9, 25, tzinfo=IST)
    runtime_state = PaperRuntimeState()
    runtime_state.symbols["SBIN"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "NONE", "direction_pending": True},
    )
    runtime_state.symbols["RELIANCE"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "NONE", "direction_pending": True},
    )
    tracker = SessionPositionTracker(
        max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.10
    )
    params = SimpleNamespace(entry_window_end="10:15")
    bar_candles = [
        SimpleNamespace(
            symbol="RELIANCE",
            bar_end=bar_end,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
        ),
        SimpleNamespace(
            symbol="SBIN",
            bar_end=bar_end,
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=1500.0,
        ),
    ]

    symbols_updates: list[list[str]] = []

    async def _evaluate_candle(**kwargs: object) -> dict[str, object]:
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": kwargs["runtime_state"].symbols[str(kwargs["candle"].symbol)].setup_row,
        }

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    monkeypatch.setattr(
        "engine.paper_session_driver.refresh_pending_setup_rows_for_bar",
        lambda **kwargs: None,
    )

    result = await process_closed_bar_group(
        session_id="s1",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=bar_candles,
        runtime_state=runtime_state,
        tracker=tracker,
        params=params,
        active_symbols=["SBIN", "RELIANCE"],
        strategy="CPR_LEVELS",
        direction_filter="SHORT",
        stage_b_applied=False,
        symbol_last_prices={},
        last_price=None,
        evaluate_candle_fn=_evaluate_candle,
        execute_entry_fn=lambda **_: {"action": "SKIP"},
        enforce_risk_controls=_enforce_risk_controls,
        build_feed_state=lambda **_: SimpleNamespace(),
        update_symbols_cb=lambda symbols: symbols_updates.append(list(symbols)),
    )

    assert symbols_updates == [["SBIN", "RELIANCE"]]
    assert result["stage_b_applied"] is True
    assert result["active_symbols"] == ["SBIN", "RELIANCE"]
    assert result["should_complete"] is False
    assert result["stop_reason"] is None


@pytest.mark.asyncio
async def test_process_closed_bar_group_applies_stage_b_immediately_for_resolved_rows() -> None:
    bar_end = datetime(2026, 4, 15, 9, 25, tzinfo=IST)
    runtime_state = PaperRuntimeState()
    runtime_state.symbols["SBIN"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "SHORT", "direction_pending": False},
    )
    runtime_state.symbols["RELIANCE"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "NONE", "direction_pending": True},
    )
    tracker = SessionPositionTracker(
        max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.10
    )
    params = SimpleNamespace(entry_window_end="10:15")
    bar_candles = [
        SimpleNamespace(
            symbol="RELIANCE",
            bar_end=bar_end,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
        ),
        SimpleNamespace(
            symbol="SBIN",
            bar_end=bar_end,
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=1500.0,
        ),
    ]

    symbols_updates: list[list[str]] = []

    async def _evaluate_candle(**kwargs: object) -> dict[str, object]:
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": kwargs["runtime_state"].symbols[str(kwargs["candle"].symbol)].setup_row,
        }

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    result = await process_closed_bar_group(
        session_id="s1",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=bar_candles,
        runtime_state=runtime_state,
        tracker=tracker,
        params=params,
        active_symbols=["SBIN", "RELIANCE"],
        strategy="CPR_LEVELS",
        direction_filter="LONG",
        stage_b_applied=False,
        symbol_last_prices={},
        last_price=None,
        evaluate_candle_fn=_evaluate_candle,
        execute_entry_fn=lambda **_: {"action": "SKIP"},
        enforce_risk_controls=_enforce_risk_controls,
        build_feed_state=lambda **_: SimpleNamespace(),
        update_symbols_cb=lambda symbols: symbols_updates.append(list(symbols)),
    )

    assert result["stage_b_applied"] is True
    assert result["active_symbols"] == ["RELIANCE"]
    assert symbols_updates == [["RELIANCE"]]


@pytest.mark.asyncio
async def test_process_closed_bar_group_reapplies_stage_b_when_symbols_change() -> None:
    bar_end = datetime(2026, 4, 15, 9, 30, tzinfo=IST)
    runtime_state = PaperRuntimeState()
    runtime_state.symbols["SBIN"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "LONG", "direction_pending": False},
    )
    runtime_state.symbols["TCS"] = SymbolRuntimeState(
        trade_date="2026-04-15",
        candles=[],
        setup_row={"direction": "SHORT", "direction_pending": False},
    )
    tracker = SessionPositionTracker(
        max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.10
    )
    candles = [
        SimpleNamespace(
            symbol="SBIN",
            bar_end=bar_end,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
        ),
        SimpleNamespace(
            symbol="TCS",
            bar_end=bar_end,
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=1000.0,
        ),
    ]
    updates: list[list[str]] = []

    async def _evaluate_candle(**_: object) -> dict[str, object]:
        return {"action": "SKIP", "setup_status": "candidate", "advance_result": None}

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    result = await process_closed_bar_group(
        session_id="s1",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=candles,
        runtime_state=runtime_state,
        tracker=tracker,
        params=SimpleNamespace(entry_window_end="10:15"),
        active_symbols=["SBIN", "TCS"],
        strategy="CPR_LEVELS",
        direction_filter="LONG",
        stage_b_applied=True,
        symbol_last_prices={},
        last_price=None,
        evaluate_candle_fn=_evaluate_candle,
        execute_entry_fn=lambda **_: {"action": "SKIP"},
        enforce_risk_controls=_enforce_risk_controls,
        build_feed_state=lambda **_: SimpleNamespace(),
        update_symbols_cb=lambda symbols: updates.append(list(symbols)),
    )

    assert result["stage_b_applied"] is True
    assert result["active_symbols"] == ["SBIN"]
    assert updates == [["SBIN"]]


@pytest.mark.asyncio
async def test_process_closed_bar_group_writes_signal_decision_audit() -> None:
    bar_end = datetime(2026, 5, 5, 9, 20, tzinfo=IST)
    runtime_state = PaperRuntimeState()
    runtime_state.symbols["AAA"] = SymbolRuntimeState(
        trade_date="2026-05-05",
        candles=[],
        setup_row={"direction": "LONG", "direction_pending": False, "atr": 2.0},
    )
    runtime_state.symbols["BBB"] = SymbolRuntimeState(
        trade_date="2026-05-05",
        candles=[],
        setup_row={"direction": "LONG", "direction_pending": False, "atr": 2.0},
    )
    tracker = SessionPositionTracker(
        max_positions=1, portfolio_value=100_000.0, max_position_pct=1.0
    )
    candles = [
        SimpleNamespace(
            symbol="AAA",
            bar_end=bar_end,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1000.0,
        ),
        SimpleNamespace(
            symbol="BBB",
            bar_end=bar_end,
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.0,
            volume=1000.0,
        ),
    ]
    audit_rows: list[dict[str, object]] = []
    executed: list[str] = []

    async def _evaluate_candle(**kwargs: object) -> dict[str, object]:
        symbol = str(kwargs["candle"].symbol)
        rr = 4.0 if symbol == "BBB" else 2.0
        setup_row = kwargs["runtime_state"].symbols[symbol].setup_row
        return {
            "symbol": symbol,
            "action": "ENTRY_CANDIDATE",
            "reason": None,
            "setup_status": "candidate",
            "candidate": {
                "symbol": symbol,
                "direction": "LONG",
                "entry_price": 200.0 if symbol == "BBB" else 100.0,
                "sl_price": 190.0 if symbol == "BBB" else 95.0,
                "target_price": 240.0 if symbol == "BBB" else 110.0,
                "rr_ratio": rr,
                "or_atr_ratio": 0.5,
                "position_size": 10,
            },
            "advance_result": None,
            "setup_row": setup_row,
        }

    async def _execute_entry(**kwargs: object) -> dict[str, object]:
        symbol = str(kwargs["candidate"]["symbol"])
        executed.append(symbol)
        return {"action": "OPEN", "symbol": symbol}

    async def _enforce_risk_controls(**_: object) -> dict[str, object]:
        return {"triggered": False}

    result = await process_closed_bar_group(
        session_id="audit-session",
        session=SimpleNamespace(strategy="CPR_LEVELS"),
        bar_candles=candles,
        runtime_state=runtime_state,
        tracker=tracker,
        params=SimpleNamespace(entry_window_end="10:15"),
        active_symbols=["AAA", "BBB"],
        strategy="CPR_LEVELS",
        direction_filter="LONG",
        stage_b_applied=False,
        symbol_last_prices={},
        last_price=None,
        evaluate_candle_fn=_evaluate_candle,
        execute_entry_fn=_execute_entry,
        enforce_risk_controls=_enforce_risk_controls,
        build_feed_state=lambda **_: SimpleNamespace(),
        signal_audit_writer=lambda rows: audit_rows.extend(rows),
    )

    assert executed == ["BBB"]
    assert result["triggered"] is False
    ranked = [row for row in audit_rows if row["stage"] == "ENTRY_RANKED"]
    assert [(row["symbol"], row["action"], row["candidate_rank"]) for row in ranked] == [
        ("BBB", "SELECTED", 1),
        ("AAA", "NOT_SELECTED", 2),
    ]
    executed_rows = [row for row in audit_rows if row["stage"] == "ENTRY_EXECUTED"]
    assert len(executed_rows) == 1
    assert executed_rows[0]["symbol"] == "BBB"
    assert executed_rows[0]["selected_rank"] == 1
