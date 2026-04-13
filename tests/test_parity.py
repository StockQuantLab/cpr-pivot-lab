"""Cross-mode parity tests for backtest vs paper replay execution semantics."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import engine.paper_runtime as paper_runtime
from engine.bar_orchestrator import SessionPositionTracker
from engine.cpr_atr_strategy import CPRATRBacktest, DayPack
from engine.paper_runtime import PaperRuntimeState, build_backtest_params, process_closed_candle


def _make_setup_row() -> dict[str, float | str | bool]:
    return {
        "trade_date": "2026-04-02",
        "direction": "LONG",
        "atr": 1.0,
        "cpr_width_pct": 0.2,
        "cpr_threshold": 1.0,
        "high_915": 101.0,
        "low_915": 99.0,
        "open_915": 100.0,
        "tc": 100.0,
        "bc": 99.0,
        "r1": 106.0,
        "s1": 98.0,
        "r2": 108.0,
        "s2": 97.0,
        "prev_day_close": 100.0,
        "open_to_cpr_atr": 1.0,
        "is_narrowing": True,
    }


def _make_day_pack() -> DayPack:
    return DayPack(
        time_str=["09:20", "09:25"],
        opens=[100.0, 100.6],
        highs=[100.8, 106.2],
        lows=[99.8, 100.5],
        closes=[100.6, 106.0],
        volumes=[1_000.0, 1_200.0],
        rvol_baseline=[1_000.0, 1_000.0],
    )


def _make_candle(
    *,
    symbol: str,
    bar_end: datetime,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        bar_end=bar_end,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


@pytest.mark.asyncio
async def test_cpr_levels_backtest_and_replay_match_single_trade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_row = _make_setup_row()
    day_pack = _make_day_pack()
    session = SimpleNamespace(
        session_id="CPR_LEVELS_LONG-2026-04-02",
        strategy="CPR_LEVELS",
        strategy_params={
            "direction_filter": "LONG",
            "skip_rvol_check": True,
            "min_price": 50.0,
            "cpr_min_close_atr": 0.0,
            "narrowing_filter": False,
            "risk_based_sizing": True,
            "capital": 100_000.0,
            "portfolio_value": 1_000_000.0,
        },
        max_positions=10,
    )
    params = build_backtest_params(session)

    backtest = CPRATRBacktest(params=params, db=object())
    backtest_trade = backtest._simulate_day_cpr_levels("SBIN", "run-parity", setup_row, day_pack)
    assert backtest_trade is not None

    positions: list[SimpleNamespace] = []
    updates: list[dict[str, object]] = []
    next_position_id = 1

    async def fake_get_session_positions(
        session_id: str, symbol: str | None = None, statuses=None
    ) -> list[SimpleNamespace]:
        _ = session_id
        status_set = {str(status).upper() for status in statuses or []}
        return [
            position
            for position in positions
            if (not status_set or str(position.status).upper() in status_set)
            and (symbol is None or str(position.symbol).upper() == str(symbol).upper())
        ]

    async def fake_open_position(**kwargs):
        nonlocal next_position_id
        position = SimpleNamespace(
            position_id=next_position_id,
            session_id=kwargs["session_id"],
            symbol=kwargs["symbol"],
            direction=kwargs["direction"],
            quantity=float(kwargs["quantity"]),
            current_qty=float(kwargs["quantity"]),
            entry_price=float(kwargs["entry_price"]),
            stop_loss=float(kwargs["stop_loss"]),
            target_price=float(kwargs["target_price"]),
            trail_state=dict(kwargs["trail_state"]),
            status="OPEN",
            last_price=float(kwargs["entry_price"]),
            close_price=None,
            realized_pnl=None,
            opened_by=str(kwargs.get("opened_by") or "CPR_LEVELS"),
        )
        next_position_id += 1
        positions.append(position)
        return position

    async def fake_append_order_event(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_update_position(position_id: int, **kwargs):
        for position in positions:
            if int(position.position_id) != int(position_id):
                continue
            updates.append({"position_id": position_id, **kwargs})
            for key, value in kwargs.items():
                setattr(position, key, value)
            return position
        return None

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = kwargs
        if str(symbol).upper() == "SBIN" and trade_date == "2026-04-02":
            return dict(setup_row)
        return None

    monkeypatch.setattr(paper_runtime, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(paper_runtime, "open_position", fake_open_position)
    monkeypatch.setattr(paper_runtime, "append_order_event", fake_append_order_event)
    monkeypatch.setattr(paper_runtime, "update_position", fake_update_position)
    monkeypatch.setattr(paper_runtime, "load_setup_row", fake_load_setup_row)
    monkeypatch.setattr(paper_runtime, "_dispatch_alert", lambda *args, **kwargs: None)

    runtime_state = PaperRuntimeState(allow_live_setup_fallback=False)
    tracker = SessionPositionTracker(
        max_positions=max(1, int(getattr(session, "max_positions", 1) or 1)),
        portfolio_value=float(session.strategy_params.get("portfolio_value", 1_000_000.0)),
    )
    entry_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2026, 4, 2, 9, 20, tzinfo=UTC),
        open_price=100.0,
        high=100.8,
        low=99.8,
        close=100.6,
        volume=1_000.0,
    )
    exit_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2026, 4, 2, 9, 25, tzinfo=UTC),
        open_price=100.6,
        high=106.2,
        low=100.5,
        close=106.0,
        volume=1_200.0,
    )

    opened = await process_closed_candle(
        session=session,
        candle=entry_candle,
        runtime_state=runtime_state,
        now=entry_candle.bar_end,
        position_tracker=tracker,
    )
    closed = await process_closed_candle(
        session=session,
        candle=exit_candle,
        runtime_state=runtime_state,
        now=exit_candle.bar_end,
        position_tracker=tracker,
    )

    assert opened["opened"] == 1
    assert closed["closed"] == 1
    assert len(positions) == 1

    replay_position = positions[0]
    close_update = next(update for update in updates if update.get("status") == "CLOSED")
    closed_at = close_update.get("closed_at")
    assert isinstance(closed_at, datetime)

    assert str(replay_position.trail_state.get("entry_time")) == backtest_trade.entry_time
    assert closed_at.strftime("%H:%M") == backtest_trade.exit_time
    assert str(replay_position.direction).upper() == str(backtest_trade.direction).upper()
    assert float(replay_position.entry_price) == pytest.approx(float(backtest_trade.entry_price))
    assert float(close_update["close_price"]) == pytest.approx(float(backtest_trade.exit_price))
    assert abs(round(float(replay_position.quantity)) - int(backtest_trade.position_size)) <= 1
    assert str(close_update["closed_by"]) == str(backtest_trade.exit_reason)
    assert float(close_update["realized_pnl"]) == pytest.approx(
        float(backtest_trade.profit_loss), abs=15.0
    )
