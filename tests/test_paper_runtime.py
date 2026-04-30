"""Tests for the shared paper-trading runtime helpers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import engine.paper_runtime as paper_runtime
from engine.bar_orchestrator import SessionPositionTracker
from engine.cpr_atr_strategy import BacktestParams, CPRLevelsParams, DayPack
from engine.paper_runtime import (
    PaperRuntimeState,
    _format_close_alert,
    build_backtest_params,
    build_backtest_params_from_overrides,
    enforce_session_risk_controls,
    process_closed_candle,
)


def _make_session(
    strategy: str,
    *,
    strategy_params: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        session_id="paper-1",
        strategy=strategy,
        strategy_params=dict(strategy_params or {}),
        max_positions=1,
    )


def _make_tracker(
    session: SimpleNamespace, portfolio_value: float = 1_000_000.0
) -> SessionPositionTracker:
    return SessionPositionTracker(
        max_positions=max(1, int(getattr(session, "max_positions", 1) or 1)),
        portfolio_value=float(portfolio_value),
    )


def test_build_backtest_params_accepts_legacy_strategy_override_aliases() -> None:
    session = SimpleNamespace(
        session_id="paper-1",
        strategy="FBR",
        strategy_params={
            "direction_filter": "SHORT",
            "failure_window": 10,
            "narrowing_filter": True,
            "cpr_min_close_atr": 0.35,
        },
    )

    params = build_backtest_params(session)

    assert params.direction_filter == "SHORT"
    assert params.fbr.failure_window == 10
    assert params.fbr.use_narrowing_filter is True
    assert params.cpr_levels.use_narrowing_filter is True
    assert params.cpr_levels.cpr_min_close_atr == 0.35


def test_build_params_from_overrides_ignores_version() -> None:
    """Legacy paper session payloads with 'version' key should not crash."""
    import dataclasses

    params = build_backtest_params_from_overrides(
        "CPR_LEVELS", {"version": "cpr-atr-v2", "cpr_percentile": 25.0}
    )
    assert params.cpr_percentile == 25.0
    field_names = {f.name for f in dataclasses.fields(params)}
    assert "version" not in field_names


def test_build_params_from_overrides_accepts_risk_based_sizing() -> None:
    params = build_backtest_params_from_overrides("CPR_LEVELS", {"risk_based_sizing": True})

    assert params.risk_based_sizing is True


def test_build_params_from_overrides_accepts_legacy_sizing_alias() -> None:
    params = build_backtest_params_from_overrides("CPR_LEVELS", {"legacy_sizing": True})

    assert params.risk_based_sizing is True


def test_format_close_alert_uses_signed_pnl_display() -> None:
    subject, body = _format_close_alert(
        symbol="ASTERDM",
        direction="SHORT",
        entry_price=673.4,
        close_price=677.62,
        reason="INITIAL_SL",
        realized_pnl=-708.76,
        strategy="CPR_LEVELS",
        session_id="CPR_LEVELS_SHORT-2026-04-01",
        event_time=datetime(2024, 4, 1, 9, 50, tzinfo=UTC),
    )

    assert subject == "❌ [LOSS] ASTERDM SHORT INITIAL_SL"
    assert "P&L: <code>-₹709</code> (-0.63%)" in body
    assert "09:50 01-Apr" in body


def test_alert_sink_can_capture_dispatch_without_sending(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[object, str, str]] = []

    def sink(alert_type, subject: str, body: str):
        captured.append((alert_type, subject, body))

    paper_runtime.set_alerts_suppressed(False)
    paper_runtime.set_alert_sink(sink)
    try:
        paper_runtime.dispatch_session_error_alert(
            session_id="CPR_LEVELS_SHORT-2026-04-09",
            reason="test_signal",
            details="sandbox",
        )
    finally:
        paper_runtime.set_alert_sink(None)
        paper_runtime.set_alerts_suppressed(False)

    assert len(captured) == 1
    assert "SESSION_ERROR" in captured[0][1]
    assert "test_signal" in captured[0][1]


def test_maybe_open_cpr_levels_rejects_below_minimum_effective_rr() -> None:
    params = BacktestParams(skip_rvol_check=True)
    setup_row = {
        "trade_date": "2024-01-01",
        "prev_day_close": 100.0,
        "tc": 98.0,
        "bc": 97.0,
        "pivot": 97.5,
        "r1": 100.9,
        "s1": 96.0,
        "r2": 103.0,
        "s2": 94.0,
        "atr": 3.0,
        "cpr_width_pct": 0.2,
        "cpr_threshold": 1.0,
        "or_high_5": 100.5,
        "or_low_5": 99.5,
        "open_915": 100.0,
        "or_close_5": 100.5,
        "open_side": "ABOVE",
        "open_to_cpr_atr": 0.5,
        "gap_abs_pct": 0.0,
        "or_atr_5": 0.4,
        "direction": "LONG",
    }
    day_pack = DayPack(
        time_str=["09:20"],
        opens=[99.8],
        highs=[101.0],
        lows=[99.0],
        closes=[100.0],
        volumes=[1_000.0],
    )
    candle = {
        "time_str": "09:20",
        "bar_end": datetime(2024, 1, 1, 9, 20, tzinfo=UTC),
        "open": 99.8,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 1_000.0,
    }

    candidate = paper_runtime._maybe_open_cpr_levels(
        candle=candle,
        day_pack=day_pack,
        setup_row=setup_row,
        params=params,
    )

    assert candidate is None


def _make_cpr_setup_row() -> dict[str, float | str | None]:
    return {
        "trade_date": "2024-01-01",
        "prev_day_close": 99.8,
        "tc": 100.0,
        "bc": 98.0,
        "pivot": 99.0,
        "r1": 105.0,
        "s1": 95.0,
        "r2": 109.0,
        "s2": 91.0,
        "atr": 3.0,
        "cpr_width_pct": 0.2,
        "cpr_threshold": 1.0,
        "or_high_5": 101.0,
        "or_low_5": 99.0,
        "open_915": 100.0,
        "or_close_5": 100.5,
        "open_side": "BELOW",
        "open_to_cpr_atr": 0.5,
        "gap_abs_pct": 0.5,
        "or_atr_5": 0.8,
        "direction": "LONG",
    }


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


def _matching_positions(
    positions: list[SimpleNamespace],
    *,
    symbol: str | None,
    statuses: object,
) -> list[SimpleNamespace]:
    if statuses == ["OPEN"]:
        return [
            p for p in positions if p.status == "OPEN" and (symbol is None or p.symbol == symbol)
        ]
    return list(positions)


def _apply_position_update(
    positions: list[SimpleNamespace], position_id: int, kwargs: dict[str, object]
) -> SimpleNamespace | None:
    for position in positions:
        if position.position_id == position_id:
            for key, value in kwargs.items():
                setattr(position, key, value)
            return position
    return None


def _install_runtime_fakes(
    monkeypatch: pytest.MonkeyPatch,
    setup_row: dict[str, float | str | None],
    positions: list[SimpleNamespace],
    events: list[dict[str, object]],
    updates: list[dict[str, object]],
) -> None:
    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        await asyncio.sleep(0)
        return _matching_positions(positions, symbol=symbol, statuses=statuses)

    def fake_load_setup_row(
        symbol: str,
        trade_date: str,
        live_candles=None,
        *,
        or_minutes: int = 5,
        allow_live_fallback: bool = True,
        bar_end_offset=None,
        **kwargs,
    ):
        _ = allow_live_fallback, bar_end_offset, kwargs
        return setup_row if symbol == "SBIN" and trade_date == "2024-01-01" else None

    async def fake_open_position(**kwargs):
        await asyncio.sleep(0)
        position = SimpleNamespace(
            position_id=1,
            session_id=kwargs["session_id"],
            symbol=kwargs["symbol"],
            direction=kwargs["direction"],
            quantity=kwargs["quantity"],
            entry_price=kwargs["entry_price"],
            stop_loss=kwargs["stop_loss"],
            target_price=kwargs["target_price"],
            trail_state=kwargs["trail_state"],
            status="OPEN",
            current_qty=kwargs["quantity"],
            last_price=kwargs["entry_price"],
            realized_pnl=None,
        )
        positions.append(position)
        return position

    async def fake_append_order_event(**kwargs):
        await asyncio.sleep(0)
        events.append(kwargs)
        return SimpleNamespace(**kwargs)

    async def fake_update_position(position_id: int, **kwargs):
        await asyncio.sleep(0)
        updates.append({"position_id": position_id, **kwargs})
        return _apply_position_update(positions, position_id, kwargs)

    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.load_setup_row", fake_load_setup_row)
    monkeypatch.setattr("engine.paper_runtime.open_position", fake_open_position)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)
    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)


def test_load_setup_row_falls_back_to_live_intraday_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if "FROM market_day_state" in query:
                return SimpleNamespace(fetchone=lambda: None)
            if "FROM v_daily" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 110.0, 100.0, 105.0))
            if "FROM atr_intraday" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 4.0))
            if "FROM cpr_thresholds" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 1.5))
            raise AssertionError(f"Unexpected query: {query}")

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_runtime, "get_dashboard_db", lambda: _FakeDB())

    row = paper_runtime.load_setup_row(
        "SBIN",
        "2024-01-08",
        live_candles=[
            {
                "time_str": "09:20",
                "bar_end": datetime(2024, 1, 8, 9, 20, tzinfo=UTC),
                "open": 96.0,
                "high": 108.0,
                "low": 95.0,
                "close": 107.0,
                "volume": 1000.0,
            }
        ],
    )

    assert row is not None
    assert row["trade_date"] == "2024-01-08"
    assert row["prev_day_close"] == pytest.approx(105.0)
    assert row["open_915"] == pytest.approx(96.0)
    assert row["or_high_5"] == pytest.approx(108.0)
    assert row["or_low_5"] == pytest.approx(95.0)
    assert row["or_close_5"] == pytest.approx(107.0)
    assert row["atr"] == pytest.approx(4.0)
    assert row["cpr_threshold"] == pytest.approx(1.5)
    assert row["direction"] == "LONG"
    assert row["direction_pending"] is False


def test_load_setup_row_waits_for_full_opening_range_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if "FROM market_day_state" in query:
                return SimpleNamespace(fetchone=lambda: None)
            if "FROM v_daily" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 110.0, 100.0, 105.0))
            if "FROM atr_intraday" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 4.0))
            if "FROM cpr_thresholds" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-01-05", 1.5))
            raise AssertionError(f"Unexpected query: {query}")

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_runtime, "get_dashboard_db", lambda: _FakeDB())

    incomplete = paper_runtime.load_setup_row(
        "SBIN",
        "2024-01-08",
        live_candles=[
            {
                "time_str": "09:20",
                "bar_end": datetime(2024, 1, 8, 9, 20),
                "open": 96.0,
                "high": 101.0,
                "low": 95.0,
                "close": 100.0,
                "volume": 1000.0,
            },
            {
                "time_str": "09:25",
                "bar_end": datetime(2024, 1, 8, 9, 25),
                "open": 100.0,
                "high": 104.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 900.0,
            },
        ],
        or_minutes=15,
    )

    assert incomplete is None

    complete = paper_runtime.load_setup_row(
        "SBIN",
        "2024-01-08",
        live_candles=[
            {
                "time_str": "09:20",
                "bar_end": datetime(2024, 1, 8, 9, 20),
                "open": 96.0,
                "high": 101.0,
                "low": 95.0,
                "close": 100.0,
                "volume": 1000.0,
            },
            {
                "time_str": "09:25",
                "bar_end": datetime(2024, 1, 8, 9, 25),
                "open": 100.0,
                "high": 104.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 900.0,
            },
            {
                "time_str": "09:30",
                "bar_end": datetime(2024, 1, 8, 9, 30),
                "open": 103.0,
                "high": 108.0,
                "low": 98.0,
                "close": 107.0,
                "volume": 800.0,
            },
        ],
        or_minutes=15,
    )

    assert complete is not None
    assert complete["open_915"] == pytest.approx(96.0)
    assert complete["or_high_5"] == pytest.approx(108.0)
    assert complete["or_low_5"] == pytest.approx(95.0)
    assert complete["or_close_5"] == pytest.approx(107.0)


@pytest.mark.asyncio
async def test_process_closed_candle_loads_setup_row_from_runtime_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS")
    state = PaperRuntimeState()
    tracker = _make_tracker(session)
    called: dict[str, object] = {}

    def fake_load_setup_row(symbol: str, trade_date: str, live_candles=None, **kwargs):
        called["symbol"] = symbol
        called["trade_date"] = trade_date
        called["kwargs"] = kwargs
        return {
            **_make_cpr_setup_row(),
            "direction": "NONE",
        }

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        return []

    monkeypatch.setattr("engine.paper_runtime.load_setup_row", fake_load_setup_row)
    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime._maybe_open_cpr_levels", lambda **kwargs: None)

    candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20, tzinfo=UTC),
        open_price=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1_000.0,
    )

    result = await process_closed_candle(
        session=session,
        candle=candle,
        runtime_state=state,
        now=candle.bar_end,
        position_tracker=tracker,
    )

    assert called["symbol"] == "SBIN"
    assert called["trade_date"] == "2024-01-01"
    assert called["kwargs"]["or_minutes"] == 5
    assert called["kwargs"]["allow_live_fallback"] is True
    assert called["kwargs"]["regime_snapshot_minutes"] == 30
    assert result["reason"] == "setup_pending"
    assert result["setup_status"] == "pending"


@pytest.mark.asyncio
async def test_process_closed_candle_refreshes_pending_setup_row_from_runtime_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS")
    state = PaperRuntimeState()
    state.for_symbol("SBIN").setup_row = {
        **_make_cpr_setup_row(),
        "direction": "NONE",
        "direction_pending": True,
    }
    tracker = _make_tracker(session)
    called: list[dict[str, object]] = []

    def fake_load_setup_row(symbol: str, trade_date: str, live_candles=None, **kwargs):
        called.append({"symbol": symbol, "trade_date": trade_date, "kwargs": kwargs})
        return {
            **_make_cpr_setup_row(),
            "direction": "LONG",
        }

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        return []

    monkeypatch.setattr("engine.paper_runtime.load_setup_row", fake_load_setup_row)
    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime._maybe_open_cpr_levels", lambda **kwargs: None)

    candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20, tzinfo=UTC),
        open_price=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1_000.0,
    )

    result = await process_closed_candle(
        session=session,
        candle=candle,
        runtime_state=state,
        now=candle.bar_end,
        position_tracker=tracker,
    )

    assert called and called[0]["symbol"] == "SBIN"
    assert called[0]["kwargs"]["regime_snapshot_minutes"] == 30
    assert result["setup_status"] == "candidate"
    assert result["reason"] == "setup_ready"


@pytest.mark.asyncio
async def test_pending_setup_rows_refresh_once_per_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS")
    state = PaperRuntimeState()
    state.for_symbol("SBIN").setup_row = {
        **_make_cpr_setup_row(),
        "direction": "NONE",
        "direction_pending": True,
    }
    tracker = _make_tracker(session)

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return list(self._rows)

    class FakeCon:
        def execute(self, query, params):
            del query, params
            row = (
                "SBIN",
                "2024-01-01",
                99.0,
                100.0,
                95.0,
                97.0,
                105.0,
                92.0,
                107.0,
                90.0,
                5.0,
                2.0,
                101.0,
                99.0,
                100.0,
                100.0,
                101.0,
                "BELOW",
                0.25,
                0.1,
                0.5,
                "LONG",
                False,
                "OVERLAP",
                [1.0, 2.0, 3.0],
            )
            return FakeResult([row])

    monkeypatch.setattr(
        "engine.paper_runtime.get_dashboard_db",
        lambda: SimpleNamespace(con=FakeCon()),
    )
    monkeypatch.setattr(
        "engine.paper_runtime.load_setup_row",
        lambda *args, **kwargs: pytest.fail("load_setup_row should not run after batch refresh"),
    )
    monkeypatch.setattr("engine.paper_runtime._maybe_open_cpr_levels", lambda **kwargs: None)

    candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20, tzinfo=UTC),
        open_price=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1_000.0,
    )

    refresh = paper_runtime.refresh_pending_setup_rows_for_bar(
        runtime_state=state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        bar_candles=[candle],
        or_minutes=5,
        allow_live_fallback=True,
    )
    assert refresh["updated"] == 1
    assert state.for_symbol("SBIN").setup_refresh_bar_end == candle.bar_end
    assert state.for_symbol("SBIN").setup_row["direction"] == "LONG"
    assert state.for_symbol("SBIN").setup_row["direction_pending"] is False

    result = await process_closed_candle(
        session=session,
        candle=candle,
        runtime_state=state,
        now=candle.bar_end,
        position_tracker=tracker,
    )

    assert result["setup_status"] == "candidate"
    assert result["reason"] == "setup_ready"


def test_realized_pnl_for_close_applies_transaction_costs() -> None:
    position = SimpleNamespace(entry_price=100.0, quantity=10.0, direction="LONG")
    zero_cost = BacktestParams(commission_model="zero")
    zerodha_cost = BacktestParams()

    gross = paper_runtime._realized_pnl_for_close(position, 105.0, params=zero_cost)
    net = paper_runtime._realized_pnl_for_close(position, 105.0, params=zerodha_cost)

    expected_cost = zerodha_cost.get_cost_model().round_trip_cost(
        entry_price=100.0,
        exit_price=105.0,
        qty=10.0,
        direction="LONG",
    )
    assert gross == pytest.approx(50.0)
    assert net == pytest.approx(50.0 - expected_cost)


@pytest.mark.asyncio
async def test_process_closed_candle_opens_and_closes_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS")
    state = PaperRuntimeState()
    tracker = _make_tracker(session)
    events: list[dict[str, object]] = []
    updated: list[dict[str, object]] = []
    positions: list[SimpleNamespace] = []
    _install_runtime_fakes(monkeypatch, _make_cpr_setup_row(), positions, events, updated)

    entry_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20),
        open_price=100.0,
        high=102.0,
        low=99.5,
        close=100.2,
        volume=1_000.0,
    )
    entry_result = await process_closed_candle(
        session=session,
        candle=entry_candle,
        runtime_state=state,
        now=entry_candle.bar_end,
        position_tracker=tracker,
    )
    assert entry_result["opened"] == 1
    assert len(positions) == 1
    assert len(events) == 1
    assert positions[0].status == "OPEN"

    exit_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 25),
        open_price=101.5,
        high=106.0,
        low=101.0,
        close=105.5,
        volume=900.0,
    )
    exit_result = await process_closed_candle(
        session=session,
        candle=exit_candle,
        runtime_state=state,
        now=exit_candle.bar_end,
        position_tracker=tracker,
    )
    assert exit_result["closed"] == 1
    assert positions[0].status == "CLOSED"
    assert any(event["side"] == "SELL" for event in events)
    assert any(update.get("status") == "CLOSED" for update in updated)


@pytest.mark.asyncio
async def test_process_closed_candle_marks_pending_setups_for_pruning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS")
    state = PaperRuntimeState()
    tracker = _make_tracker(session)

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        return []

    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(
        "engine.paper_runtime.load_setup_row",
        lambda symbol,
        trade_date,
        live_candles=None,
        *,
        or_minutes=5,
        allow_live_fallback=True,
        bar_end_offset=None,
        **kwargs: {
            **_make_cpr_setup_row(),
            "direction": "NONE",
        },
    )

    candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20),
        open_price=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1_000.0,
    )

    result = await process_closed_candle(
        session=session,
        candle=candle,
        runtime_state=state,
        now=candle.bar_end,
        position_tracker=tracker,
    )

    assert result["setup_status"] == "pending"
    assert result["reason"] == "setup_pending"


@pytest.mark.asyncio
async def test_process_closed_candle_uses_session_breakeven_r_for_new_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS", strategy_params={"breakeven_r": 0.4})
    state = PaperRuntimeState()
    tracker = _make_tracker(session)
    events: list[dict[str, object]] = []
    updated: list[dict[str, object]] = []
    positions: list[SimpleNamespace] = []
    _install_runtime_fakes(monkeypatch, _make_cpr_setup_row(), positions, events, updated)

    entry_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20),
        open_price=100.0,
        high=102.0,
        low=99.5,
        close=100.2,
        volume=1_000.0,
    )
    await process_closed_candle(
        session=session,
        candle=entry_candle,
        runtime_state=state,
        now=entry_candle.bar_end,
        position_tracker=tracker,
    )

    assert positions[0].trail_state["breakeven_r"] == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_advance_open_position_preserves_initial_sl_for_trail_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    position = SimpleNamespace(
        position_id=1,
        session_id="paper-1",
        symbol="HECPROJECT",
        direction="SHORT",
        quantity=985.0,
        current_qty=985.0,
        entry_price=99.29,
        stop_loss=100.305,
        target_price=96.48,
        trail_state={
            "entry_price": 99.29,
            "direction": "SHORT",
            "initial_sl": 100.305,
            "current_sl": 100.305,
            "atr": 1.2,
            "rr_ratio": 2.7684729064039275,
            "breakeven_r": 1.0,
            "phase": "PROTECT",
            "highest_since_entry": 99.29,
            "lowest_since_entry": 99.29,
            "entry_time": "09:35",
            "first_target_price": 96.48,
            "scale_out_pct": 0.0,
            "scaled_out": False,
            "initial_qty": 985.0,
            "candle_count": 0,
        },
        realized_pnl=0.0,
        opened_by="CPR_LEVELS",
        status="OPEN",
    )

    async def fake_update_position(position_id: int, **kwargs):
        updates.append({"position_id": position_id, **kwargs})
        for key, value in kwargs.items():
            setattr(position, key, value)
        return position

    async def fake_append_order_event(**kwargs):
        events.append(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)

    params = BacktestParams()
    first = await paper_runtime._advance_open_position(
        position=position,
        candle={
            "bar_end": datetime(2024, 1, 1, 9, 40),
            "open": 97.0,
            "high": 98.0,
            "low": 97.0,
            "close": 98.0,
            "volume": 1000.0,
        },
        params=params,
    )
    second = await paper_runtime._advance_open_position(
        position=position,
        candle={
            "bar_end": datetime(2024, 1, 1, 9, 45),
            "open": 98.0,
            "high": 98.0,
            "low": 97.1,
            "close": 97.1,
            "volume": 900.0,
        },
        params=params,
    )

    assert first["action"] == "HOLD"
    assert second["action"] == "HOLD"
    assert events == []
    assert position.trail_state["phase"] == "BREAKEVEN"
    assert position.trail_state["current_sl"] == pytest.approx(99.29)
    assert position.stop_loss == pytest.approx(99.29)
    assert len(updates) == 2


@pytest.mark.asyncio
async def test_advance_open_position_honors_momentum_confirm_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    position = SimpleNamespace(
        position_id=1,
        session_id="paper-1",
        symbol="SBIN",
        direction="LONG",
        quantity=100.0,
        current_qty=100.0,
        entry_price=100.0,
        stop_loss=95.0,
        target_price=115.0,
        trail_state={
            "entry_price": 100.0,
            "direction": "LONG",
            "initial_sl": 95.0,
            "current_sl": 95.0,
            "atr": 2.0,
            "rr_ratio": 3.0,
            "breakeven_r": 1.0,
            "phase": "PROTECT",
            "highest_since_entry": 100.0,
            "lowest_since_entry": 100.0,
            "entry_time": "09:30",
            "first_target_price": 115.0,
            "scale_out_pct": 0.0,
            "scaled_out": False,
            "initial_qty": 100.0,
            "candle_count": 0,
        },
        realized_pnl=0.0,
        opened_by="CPR_LEVELS",
        status="OPEN",
    )

    async def fake_update_position(position_id: int, **kwargs):
        updates.append({"position_id": position_id, **kwargs})
        for key, value in kwargs.items():
            setattr(position, key, value)
        return position

    async def fake_append_order_event(**kwargs):
        events.append(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)

    params = BacktestParams(
        cpr_levels_config=CPRLevelsParams(
            cpr_shift_filter="ALL",
            min_effective_rr=2.0,
            use_narrowing_filter=True,
            cpr_entry_start="",
            cpr_confirm_entry=False,
            cpr_hold_confirm=False,
            cpr_min_close_atr=0.5,
            momentum_confirm=True,
        )
    )
    first = await paper_runtime._advance_open_position(
        position=position,
        candle={
            "bar_end": datetime(2024, 1, 1, 9, 35),
            "open": 100.0,
            "high": 100.5,
            "low": 98.5,
            "close": 99.0,
            "volume": 1000.0,
        },
        params=params,
    )
    second = await paper_runtime._advance_open_position(
        position=position,
        candle={
            "bar_end": datetime(2024, 1, 1, 9, 40),
            "open": 98.0,
            "high": 99.0,
            "low": 97.5,
            "close": 98.5,
            "volume": 900.0,
        },
        params=params,
    )

    assert first["action"] == "HOLD"
    assert position.trail_state["momentum_exit_pending"] is True
    assert second["action"] == "CLOSE"
    assert second["reason"] == "MOMENTUM_FAIL"
    assert second["close_price"] == pytest.approx(98.0)
    assert position.status == "CLOSED"
    assert position.exit_reason == "MOMENTUM_FAIL"
    assert events[-1]["fill_price"] == pytest.approx(98.0)


@pytest.mark.asyncio
async def test_process_closed_candle_scales_out_then_runners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("CPR_LEVELS", strategy_params={"cpr_scale_out_pct": 0.8})
    state = PaperRuntimeState()
    tracker = _make_tracker(session)
    events: list[dict[str, object]] = []
    updates: list[dict[str, object]] = []
    positions: list[SimpleNamespace] = []
    setup_row = {
        **_make_cpr_setup_row(),
        "r2": 115.0,
        "s2": 90.0,
    }
    _install_runtime_fakes(monkeypatch, setup_row, positions, events, updates)

    entry_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20),
        open_price=100.0,
        high=103.0,
        low=99.5,
        close=102.5,
        volume=1_000.0,
    )
    partial_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 25),
        open_price=102.5,
        high=106.5,
        low=101.8,
        close=104.5,
        volume=900.0,
    )
    runner_candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 30),
        open_price=104.5,
        high=116.0,
        low=103.0,
        close=114.5,
        volume=800.0,
    )

    await process_closed_candle(
        session=session,
        candle=entry_candle,
        runtime_state=state,
        now=entry_candle.bar_end,
        position_tracker=tracker,
    )
    first = await process_closed_candle(
        session=session,
        candle=partial_candle,
        runtime_state=state,
        now=partial_candle.bar_end,
        position_tracker=tracker,
    )
    second = await process_closed_candle(
        session=session,
        candle=runner_candle,
        runtime_state=state,
        now=runner_candle.bar_end,
        position_tracker=tracker,
    )

    assert first["opened"] == 0
    assert first["result"]["action"] == "PARTIAL"
    assert second["closed"] == 1
    assert positions[0].status == "CLOSED"
    assert positions[0].target_price == pytest.approx(115.0)
    assert positions[0].current_qty == pytest.approx(0.0)
    initial_qty = float(positions[0].quantity)
    assert any(
        update.get("current_qty") == pytest.approx(initial_qty * 0.2, rel=0.05)
        for update in updates
    )
    assert any(event["fill_qty"] == pytest.approx(initial_qty * 0.8, rel=0.05) for event in events)
    assert any(event["fill_qty"] == pytest.approx(initial_qty * 0.2, rel=0.05) for event in events)


@pytest.mark.asyncio
async def test_process_closed_candle_rejects_non_cpr_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session("FBR")
    state = PaperRuntimeState()
    tracker = _make_tracker(session)
    events: list[dict[str, object]] = []
    updates: list[dict[str, object]] = []
    positions: list[SimpleNamespace] = []
    _install_runtime_fakes(monkeypatch, _make_cpr_setup_row(), positions, events, updates)

    candle = _make_candle(
        symbol="SBIN",
        bar_end=datetime(2024, 1, 1, 9, 20),
        open_price=100.0,
        high=102.0,
        low=99.5,
        close=100.2,
        volume=1_000.0,
    )
    result = await process_closed_candle(
        session=session,
        candle=candle,
        runtime_state=state,
        now=candle.bar_end,
        position_tracker=tracker,
    )

    assert result["opened"] == 0
    assert result["closed"] == 0
    assert result["reason"] == "unsupported_strategy:FBR"


@pytest.mark.asyncio
async def test_enforce_session_risk_controls_flattens_on_daily_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="paper-risk-1",
        flatten_time=None,
        max_daily_loss_pct=0.01,
        strategy_params={"portfolio_value": 100_000},
    )
    positions = [
        SimpleNamespace(
            symbol="SBIN",
            status="OPEN",
            current_qty=100.0,
            quantity=100.0,
            entry_price=100.0,
            direction="LONG",
            realized_pnl=0.0,
            last_price=None,
        )
    ]
    session_updates: list[dict[str, object]] = []
    flatten_calls: list[dict[str, object]] = []

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        await asyncio.sleep(0)
        return positions

    async def fake_update_session_state(session_id: str, **kwargs):
        await asyncio.sleep(0)
        session_updates.append(kwargs)
        return session

    async def fake_flatten_session_positions(
        session_id: str, *, notes: str | None = None, feed_state=None
    ):
        await asyncio.sleep(0)
        flatten_calls.append({"notes": notes, "feed_state": feed_state})
        return {"session_id": session_id, "closed_positions": 1}

    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr(
        "engine.paper_runtime.flatten_session_positions", fake_flatten_session_positions
    )

    result = await enforce_session_risk_controls(
        session=session,
        as_of=datetime(2024, 1, 1, 10, 0),
        feed_state=SimpleNamespace(
            raw_state={"symbol_last_prices": {"SBIN": 80.0}}, last_price=80.0
        ),
    )

    assert result["triggered"] is True
    assert result["daily_pnl_used"] == pytest.approx(-2000.0)
    assert result["reasons"] == ["daily_loss_limit:1000.00"]
    assert session_updates[0]["daily_pnl_used"] == pytest.approx(-2000.0)
    assert "daily_loss_limit:1000.00" in str(flatten_calls[0]["notes"])


@pytest.mark.asyncio
async def test_enforce_session_risk_controls_flattens_at_session_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="paper-risk-2",
        flatten_time=datetime.strptime("15:15:00", "%H:%M:%S").time(),
        max_daily_loss_pct=0.0,
        strategy_params={"portfolio_value": 100_000},
    )
    session_updates: list[dict[str, object]] = []
    flatten_calls: list[dict[str, object]] = []

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        await asyncio.sleep(0)
        return []

    async def fake_update_session_state(session_id: str, **kwargs):
        await asyncio.sleep(0)
        session_updates.append(kwargs)
        return session

    async def fake_flatten_session_positions(
        session_id: str, *, notes: str | None = None, feed_state=None
    ):
        await asyncio.sleep(0)
        flatten_calls.append({"notes": notes})
        return {"session_id": session_id, "closed_positions": 0}

    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr(
        "engine.paper_runtime.flatten_session_positions", fake_flatten_session_positions
    )

    result = await enforce_session_risk_controls(
        session=session,
        as_of=datetime(2024, 1, 1, 15, 15),
        feed_state=SimpleNamespace(raw_state={"symbol_last_prices": {}}, last_price=None),
    )

    assert result["triggered"] is True
    assert result["reasons"] == ["flatten_time:15:15:00"]
    assert session_updates[0]["daily_pnl_used"] == pytest.approx(0.0)
    assert "flatten_time:15:15:00" in str(flatten_calls[0]["notes"])


@pytest.mark.asyncio
async def test_enforce_session_risk_controls_flattens_on_max_drawdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Max drawdown check triggers flatten when session PnL falls below the drawdown limit."""
    session = SimpleNamespace(
        session_id="paper-risk-dd",
        flatten_time=None,
        max_daily_loss_pct=0.0,  # daily loss check disabled
        max_drawdown_pct=0.05,  # 5% drawdown limit → ₹5,000 on ₹100k
        strategy_params={"portfolio_value": 100_000},
    )
    positions = [
        SimpleNamespace(
            symbol="SBIN",
            status="OPEN",
            current_qty=100.0,
            quantity=100.0,
            entry_price=100.0,
            direction="LONG",
            realized_pnl=0.0,
            last_price=None,
        )
    ]
    session_updates: list[dict[str, object]] = []
    flatten_calls: list[dict[str, object]] = []

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        await asyncio.sleep(0)
        return positions

    async def fake_update_session_state(session_id: str, **kwargs):
        await asyncio.sleep(0)
        session_updates.append(kwargs)
        return session

    async def fake_flatten_session_positions(
        session_id: str, *, notes: str | None = None, feed_state=None
    ):
        await asyncio.sleep(0)
        flatten_calls.append({"notes": notes, "feed_state": feed_state})
        return {"session_id": session_id, "closed_positions": 1}

    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr(
        "engine.paper_runtime.flatten_session_positions", fake_flatten_session_positions
    )

    # last_price=45 → unrealized PnL = 100*(45-100) = -5500 → exceeds 5k drawdown limit
    result = await enforce_session_risk_controls(
        session=session,
        as_of=datetime(2024, 1, 1, 10, 0),
        feed_state=SimpleNamespace(
            raw_state={"symbol_last_prices": {"SBIN": 45.0}}, last_price=45.0
        ),
    )

    assert result["triggered"] is True
    assert result["daily_pnl_used"] == pytest.approx(-5500.0)
    assert result["reasons"] == ["max_drawdown:5000.00"]
    assert session_updates[0]["daily_pnl_used"] == pytest.approx(-5500.0)
    assert "max_drawdown:5000.00" in str(flatten_calls[0]["notes"])


@pytest.mark.asyncio
async def test_flatten_session_positions_uses_ist_closed_at_and_dispatches_per_trade_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    flatten_session_positions must:
    1. Pass a timezone-aware IST closed_at to update_position (not UTC naive).
       Bug: omitting closed_at caused datetime.utcnow() fallback → 15:10 IST stored as 09:40 UTC.
    2. Dispatch a TRADE_CLOSED alert for each force-closed position.
       Bug: only FLATTEN_EOD was dispatched; per-position alerts were missing.
    3. Still dispatch exactly one FLATTEN_EOD at the end.
    """
    from engine.alert_dispatcher import AlertType
    from engine.paper_runtime import (
        flatten_session_positions,
        set_alert_sink,
        set_alerts_suppressed,
    )

    session = SimpleNamespace(
        session_id="test-flatten-1",
        strategy="CPR_LEVELS",
        strategy_params={},
        max_positions=10,
        max_daily_loss_pct=0.0,
        max_position_pct=0.1,
        portfolio_value=1_000_000.0,
        trade_date="2026-04-13",
    )

    open_position = SimpleNamespace(
        position_id="pos-1",
        session_id="test-flatten-1",
        symbol="SUKHJITS",
        direction="SHORT",
        status="OPEN",
        entry_price=175.90,
        current_qty=568.0,
        quantity=568.0,
        stop_loss=180.0,
        target_price=165.0,
        last_price=176.34,
        trail_state={},
        realized_pnl=None,
        opened_by="CPR_LEVELS",
    )

    update_position_calls: list[dict] = []
    session_state_calls: list[dict] = []

    async def fake_get_session(session_id: str):
        return session

    async def fake_get_session_positions(session_id: str, *, symbol=None, statuses=None):
        if statuses == ["OPEN"]:
            return [open_position]
        if statuses == ["CLOSED"]:
            return [
                SimpleNamespace(
                    realized_pnl=update_position_calls[0]["realized_pnl"]
                    if update_position_calls
                    else None
                )
            ]
        return []

    async def fake_append_order_event(**kwargs):
        pass

    async def fake_update_position(position_id: str, **kwargs):
        update_position_calls.append({"position_id": position_id, **kwargs})

    async def fake_update_session_state(session_id: str, **kwargs):
        session_state_calls.append({"session_id": session_id, **kwargs})
        pass

    class _FakeCon:
        def execute(self, sql: str, params: list):
            # FLATTEN_EOD dedup check — report "not yet sent"
            return SimpleNamespace(fetchone=lambda: (0,))

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr("engine.paper_runtime.get_session", fake_get_session)
    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)
    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr("engine.paper_runtime._db", lambda: _FakeDB())

    dispatched: list[tuple] = []
    set_alerts_suppressed(False)
    set_alert_sink(lambda t, s, b: dispatched.append((t, s, b)))
    try:
        feed_state = SimpleNamespace(raw_state={"symbol_last_prices": {"SUKHJITS": 176.34}})
        result = await flatten_session_positions(
            "test-flatten-1", notes="eod", feed_state=feed_state
        )
    finally:
        set_alert_sink(None)
        set_alerts_suppressed(False)

    # closed_at must be IST-aware — not UTC naive (the pre-fix bug)
    assert len(update_position_calls) == 1
    closed_at = update_position_calls[0].get("closed_at")
    assert closed_at is not None, "closed_at must be passed to update_position when status=CLOSED"
    assert closed_at.tzinfo is not None, (
        "closed_at must be timezone-aware (was UTC-naive before fix)"
    )
    assert str(closed_at.tzinfo) == "Asia/Kolkata", f"expected IST tz, got {closed_at.tzinfo}"

    # per-position TRADE_CLOSED must fire for every force-flattened symbol
    trade_closed = [d for d in dispatched if d[0] == AlertType.TRADE_CLOSED]
    assert len(trade_closed) == 1, f"expected 1 TRADE_CLOSED alert, got {len(trade_closed)}"
    assert "SUKHJITS" in trade_closed[0][1], "TRADE_CLOSED subject must contain the symbol"
    assert "AUTO_FLATTEN" in trade_closed[0][1], "TRADE_CLOSED subject must say AUTO_FLATTEN"

    # exactly one FLATTEN_EOD summary at the end
    eod = [d for d in dispatched if d[0] == AlertType.FLATTEN_EOD]
    assert len(eod) == 1, f"expected 1 FLATTEN_EOD alert, got {len(eod)}"

    assert session_state_calls[-1]["total_pnl"] == pytest.approx(
        update_position_calls[0]["realized_pnl"]
    )

    assert result["closed_positions"] == 1


@pytest.mark.asyncio
async def test_flatten_session_positions_can_disable_summary_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admin/command paths can close positions early and defer final summary to terminal finalization."""
    from engine.alert_dispatcher import AlertType
    from engine.paper_runtime import (
        flatten_session_positions,
        set_alert_sink,
        set_alerts_suppressed,
    )

    session = SimpleNamespace(
        session_id="test-flatten-2",
        strategy="CPR_LEVELS",
        strategy_params={},
        max_positions=10,
        max_daily_loss_pct=0.0,
        max_position_pct=0.1,
        portfolio_value=1_000_000.0,
        trade_date="2026-04-13",
    )

    open_position = SimpleNamespace(
        position_id="pos-1",
        session_id="test-flatten-2",
        symbol="SUKHJITS",
        direction="SHORT",
        status="OPEN",
        entry_price=175.90,
        current_qty=568.0,
        quantity=568.0,
        stop_loss=180.0,
        target_price=165.0,
        last_price=176.34,
        trail_state={},
        realized_pnl=None,
        opened_by="CPR_LEVELS",
    )

    update_position_calls: list[dict] = []
    session_state_calls: list[dict] = []

    async def fake_get_session(session_id: str):
        return session

    async def fake_get_session_positions(session_id: str, *, symbol=None, statuses=None):
        if statuses == ["OPEN"]:
            return [open_position]
        if statuses == ["CLOSED"]:
            return [
                SimpleNamespace(
                    realized_pnl=update_position_calls[0]["realized_pnl"]
                    if update_position_calls
                    else None
                )
            ]
        return []

    async def fake_append_order_event(**kwargs):
        pass

    async def fake_update_position(position_id: str, **kwargs):
        update_position_calls.append({"position_id": position_id, **kwargs})

    async def fake_update_session_state(session_id: str, **kwargs):
        session_state_calls.append({"session_id": session_id, **kwargs})
        return session

    class _FakeCon:
        def execute(self, sql: str, params: list):
            # Flatten EOD dedup check — report "not yet sent"
            return SimpleNamespace(fetchone=lambda: (0,))

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr("engine.paper_runtime.get_session", fake_get_session)
    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)
    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr("engine.paper_runtime._db", lambda: _FakeDB())

    dispatched: list[tuple] = []
    set_alerts_suppressed(False)
    set_alert_sink(lambda t, s, b: dispatched.append((t, s, b)))
    try:
        feed_state = SimpleNamespace(raw_state={"symbol_last_prices": {"SUKHJITS": 176.34}})
        result = await flatten_session_positions(
            "test-flatten-2",
            notes="admin-close",
            feed_state=feed_state,
            emit_summary=False,
        )
    finally:
        set_alert_sink(None)
        set_alerts_suppressed(False)

    assert len(update_position_calls) == 1

    trade_closed = [d for d in dispatched if d[0] == AlertType.TRADE_CLOSED]
    assert len(trade_closed) == 1
    assert "SUKHJITS" in trade_closed[0][1]

    eod = [d for d in dispatched if d[0] == AlertType.FLATTEN_EOD]
    assert len(eod) == 0

    assert session_state_calls[-1]["total_pnl"] == pytest.approx(
        update_position_calls[0]["realized_pnl"]
    )

    assert result["closed_positions"] == 1


@pytest.mark.asyncio
async def test_flatten_session_positions_eod_dedup_in_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling flatten_session_positions twice for the same session dispatches FLATTEN_EOD once.

    The in-memory _flatten_eod_sent set is checked synchronously before dispatch, so even
    two concurrent (near-simultaneous) callers cannot both pass the guard.
    """
    import engine.paper_runtime as _pm
    from engine.paper_runtime import (
        flatten_session_positions,
        set_alert_sink,
        set_alerts_suppressed,
    )

    session_id = "CPR_LEVELS_SHORT-2026-04-13-DEDUP-TEST"
    # Clear in-memory dedup state from any previous test runs.
    _pm._flatten_eod_sent.discard(session_id)

    session = SimpleNamespace(
        session_id=session_id,
        strategy="CPR_LEVELS",
        strategy_params={},
        max_positions=10,
        max_daily_loss_pct=0.0,
        max_position_pct=0.1,
        portfolio_value=1_000_000.0,
        trade_date="2026-04-13",
    )
    open_position = SimpleNamespace(
        position_id="pos-1",
        session_id=session_id,
        symbol="SUKHJITS",
        direction="SHORT",
        status="OPEN",
        entry_price=175.90,
        current_qty=568.0,
        quantity=568.0,
        stop_loss=180.0,
        target_price=165.0,
        last_price=176.34,
        trail_state={},
        realized_pnl=None,
        opened_by="CPR_LEVELS",
    )

    async def fake_get_session(session_id_arg: str):
        return session

    async def fake_get_session_positions(session_id_arg: str, *, symbol=None, statuses=None):
        if statuses == ["OPEN"]:
            return [open_position]
        return []

    async def fake_append_order_event(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_update_position(position_id: str, **kwargs):
        return None

    async def fake_update_session_state(session_id_arg: str, **kwargs):
        return session

    class _FakeDB:
        pass

    monkeypatch.setattr("engine.paper_runtime.get_session", fake_get_session)
    monkeypatch.setattr("engine.paper_runtime.get_session_positions", fake_get_session_positions)
    monkeypatch.setattr("engine.paper_runtime.append_order_event", fake_append_order_event)
    monkeypatch.setattr("engine.paper_runtime.update_position", fake_update_position)
    monkeypatch.setattr("engine.paper_runtime.update_session_state", fake_update_session_state)
    monkeypatch.setattr("engine.paper_runtime._db", lambda: _FakeDB())

    dispatched: list[tuple] = []
    set_alerts_suppressed(False)
    set_alert_sink(lambda t, s, b: dispatched.append((t, s, b)))
    try:
        # First call — should dispatch FLATTEN_EOD.
        result1 = await flatten_session_positions(
            session_id,
            notes="eod",
            feed_state=SimpleNamespace(raw_state={"symbol_last_prices": {"SUKHJITS": 176.34}}),
        )
        # Second call (simulates async race / retry path) — must be suppressed by in-memory guard.
        await flatten_session_positions(
            session_id,
            notes="eod duplicate",
            feed_state=SimpleNamespace(raw_state={"symbol_last_prices": {"SUKHJITS": 176.34}}),
        )
    finally:
        set_alert_sink(None)
        set_alerts_suppressed(False)
        _pm._flatten_eod_sent.discard(session_id)

    # Exactly one FLATTEN_EOD regardless of how many times flatten was called.
    eod_alerts = [d for d in dispatched if d[0].value == "FLATTEN_EOD"]
    assert len(eod_alerts) == 1, f"expected 1 FLATTEN_EOD, got {len(eod_alerts)}"
    assert session_id in eod_alerts[0][2]

    assert result1["closed_positions"] == 1


def test_dispatch_session_started_alert_dedups_per_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import engine.paper_runtime as _pm

    dispatched: list[tuple] = []
    _pm.set_alerts_suppressed(False)
    _pm.set_alert_sink(lambda t, s, b: dispatched.append((t, s, b)))
    try:
        _pm.dispatch_session_started_alert(
            session_id="paper-live-1",
            strategy="CPR_LEVELS",
            direction="LONG",
            symbol_count=12,
            trade_date="2026-04-21",
        )
        _pm.dispatch_session_started_alert(
            session_id="paper-live-1",
            strategy="CPR_LEVELS",
            direction="LONG",
            symbol_count=12,
            trade_date="2026-04-21",
        )
    finally:
        _pm.set_alert_sink(None)
        _pm.set_alerts_suppressed(False)
        _pm._session_started_sent.discard("paper-live-1")

    started_alerts = [d for d in dispatched if d[0].value == "SESSION_STARTED"]
    assert len(started_alerts) == 1
    assert started_alerts[0][1].startswith("🟢 Session Started — paper live 1")
    assert "Strategy: <code>CPR_LEVELS</code>" in started_alerts[0][2]
    assert "Direction: <b>LONG</b>" in started_alerts[0][2]
