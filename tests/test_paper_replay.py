"""Tests for historical paper replay parity and archive flow."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

import scripts.paper_replay as paper_replay
from db.postgres import FeedState
from engine.cpr_atr_strategy import DayPack


@pytest.mark.asyncio
async def test_replay_session_streams_candles_and_archives_completed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="paper-1",
        strategy="CPR_LEVELS",
        strategy_params={},
        status="ACTIVE",
        symbols=["SBIN"],
        stale_feed_timeout_sec=60,
    )
    updated_statuses: list[str | None] = []
    processed_candles: list[datetime] = []
    archived: list[str] = []
    risk_checks: list[dict[str, object]] = []

    day_pack = paper_replay.ReplayDayPack(
        symbol="SBIN",
        trade_date="2024-01-02",
        day_pack=DayPack(
            time_str=["09:15", "09:20"],
            opens=[100.0, 101.0],
            highs=[101.0, 102.0],
            lows=[99.5, 100.5],
            closes=[100.5, 101.5],
            volumes=[1_000.0, 900.0],
        ),
    )

    async def fake_get_session(session_id: str):
        return session

    async def fake_update_session_state(session_id: str, **kwargs):
        updated_statuses.append(kwargs.get("status"))
        if kwargs.get("status"):
            session.status = kwargs["status"]
        return session

    async def fake_upsert_feed_state(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_evaluate_candle(**kwargs):
        processed_candles.append(kwargs["candle"].bar_end)
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_enforce_session_risk_controls(**kwargs):
        risk_checks.append(kwargs)
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    async def fake_get_feed_state(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=101.5,
            stale_reason=None,
            raw_state={},
            updated_at=None,
        )

    async def fake_archive_completed_session(session_id: str, paper_db=None):
        archived.append(session_id)
        return {"session_id": session_id, "archived": True}

    async def fake_get_session_positions(session_id: str, statuses=None):
        return []

    fake_pdb = SimpleNamespace(
        update_session=lambda session_id, **kwargs: session,
        upsert_feed_state=lambda **kwargs: None,
        get_feed_state=lambda session_id: None,
        _sync=None,
        con=None,
    )

    monkeypatch.setattr(paper_replay, "_pdb", lambda: fake_pdb)
    monkeypatch.setattr(paper_replay, "get_session", fake_get_session)
    monkeypatch.setattr(paper_replay, "update_session_state", fake_update_session_state)
    monkeypatch.setattr(paper_replay, "upsert_feed_state", fake_upsert_feed_state)
    monkeypatch.setattr(paper_replay, "evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(
        paper_replay, "enforce_session_risk_controls", fake_enforce_session_risk_controls
    )
    monkeypatch.setattr(paper_replay, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(paper_replay, "get_feed_state", fake_get_feed_state)
    monkeypatch.setattr(paper_replay, "archive_completed_session", fake_archive_completed_session)
    monkeypatch.setattr(paper_replay, "load_replay_day_packs", lambda **kwargs: [day_pack])

    result = await paper_replay.replay_session(
        session_id="paper-1",
        symbols=["SBIN"],
        start_date="2024-01-02",
        end_date="2024-01-02",
        leave_active=False,
    )

    assert processed_candles == [
        datetime(2024, 1, 2, 9, 15, tzinfo=paper_replay.IST),
        datetime(2024, 1, 2, 9, 20, tzinfo=paper_replay.IST),
    ]
    assert updated_statuses[-1] == "COMPLETED"
    assert archived == ["paper-1"]
    assert len(risk_checks) == 2
    assert result["days_replayed"] == 1
    assert result["bars_replayed"] == 2
    assert result["completed"] is True
    assert result["archive"]["archived"] is True


@pytest.mark.asyncio
async def test_replay_session_stops_later_dates_when_risk_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="paper-2",
        strategy="CPR_LEVELS",
        strategy_params={},
        status="ACTIVE",
        symbols=["SBIN"],
        stale_feed_timeout_sec=60,
    )
    processed_candles: list[datetime] = []
    archived: list[str] = []
    risk_calls: list[dict[str, object]] = []

    day_1 = paper_replay.ReplayDayPack(
        symbol="SBIN",
        trade_date="2024-01-02",
        day_pack=DayPack(
            time_str=["09:15"],
            opens=[100.0],
            highs=[101.0],
            lows=[99.5],
            closes=[100.5],
            volumes=[1_000.0],
        ),
    )
    day_2 = paper_replay.ReplayDayPack(
        symbol="SBIN",
        trade_date="2024-01-03",
        day_pack=DayPack(
            time_str=["09:15"],
            opens=[101.0],
            highs=[102.0],
            lows=[100.5],
            closes=[101.5],
            volumes=[900.0],
        ),
    )

    async def fake_get_session(session_id: str):
        return session

    async def fake_update_session_state(session_id: str, **kwargs):
        if kwargs.get("status"):
            session.status = kwargs["status"]
        return session

    async def fake_upsert_feed_state(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_evaluate_candle(**kwargs):
        processed_candles.append(kwargs["candle"].bar_end)
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_enforce_session_risk_controls(**kwargs):
        risk_calls.append(kwargs)
        return {
            "triggered": len(risk_calls) == 1,
            "daily_pnl_used": 0.0,
            "reasons": ["flatten_time:15:15:00"] if len(risk_calls) == 1 else [],
        }

    async def fake_get_feed_state(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=101.5,
            stale_reason=None,
            raw_state={},
            updated_at=None,
        )

    async def fake_archive_completed_session(session_id: str, paper_db=None):
        archived.append(session_id)
        return {"session_id": session_id, "archived": True}

    async def fake_get_session_positions(session_id: str, statuses=None):
        return []

    fake_pdb = SimpleNamespace(
        update_session=lambda session_id, **kwargs: session,
        upsert_feed_state=lambda **kwargs: None,
        get_feed_state=lambda session_id: None,
        _sync=None,
        con=None,
    )

    monkeypatch.setattr(paper_replay, "_pdb", lambda: fake_pdb)
    monkeypatch.setattr(paper_replay, "get_session", fake_get_session)
    monkeypatch.setattr(paper_replay, "update_session_state", fake_update_session_state)
    monkeypatch.setattr(paper_replay, "upsert_feed_state", fake_upsert_feed_state)
    monkeypatch.setattr(paper_replay, "evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(
        paper_replay, "enforce_session_risk_controls", fake_enforce_session_risk_controls
    )
    monkeypatch.setattr(paper_replay, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(paper_replay, "get_feed_state", fake_get_feed_state)
    monkeypatch.setattr(paper_replay, "archive_completed_session", fake_archive_completed_session)
    monkeypatch.setattr(paper_replay, "load_replay_day_packs", lambda **kwargs: [day_1, day_2])

    result = await paper_replay.replay_session(
        session_id="paper-2",
        symbols=["SBIN"],
        start_date="2024-01-02",
        end_date="2024-01-03",
        leave_active=False,
    )

    assert [dt.date() for dt in processed_candles] == [datetime(2024, 1, 2).date()]
    assert len(risk_calls) == 1
    assert archived == []
    assert result["completed"] is False


@pytest.mark.asyncio
async def test_process_replay_bar_major_logs_progress_without_symbol_name(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = SimpleNamespace(session_id="paper-3", strategy="CPR_LEVELS", strategy_params={})
    day_pack = paper_replay.ReplayDayPack(
        symbol="SBIN",
        trade_date="2024-01-02",
        day_pack=DayPack(
            time_str=["09:15", "09:20"],
            opens=[100.0, 101.0],
            highs=[101.0, 102.0],
            lows=[99.5, 100.5],
            closes=[100.5, 101.5],
            volumes=[1_000.0, 900.0],
        ),
    )

    async def fake_evaluate_candle(**kwargs):
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_upsert_feed_state(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_update_session_state(session_id: str, **kwargs):
        return None

    async def fake_get_session_positions(session_id: str, statuses=None):
        return []

    async def fake_enforce_session_risk_controls(**kwargs):
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    monkeypatch.setattr(paper_replay, "evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(paper_replay, "upsert_feed_state", fake_upsert_feed_state)
    monkeypatch.setattr(paper_replay, "update_session_state", fake_update_session_state)
    monkeypatch.setattr(paper_replay, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(
        paper_replay,
        "enforce_session_risk_controls",
        fake_enforce_session_risk_controls,
    )

    caplog.set_level("INFO", logger="scripts.paper_replay")
    await paper_replay._process_replay_bar_major(
        session_id="paper-3",
        session=session,
        date_items=[day_pack],
        stale_timeout=60,
        runtime_state=paper_replay.PaperRuntimeState(),
        symbol_last_prices={},
        tracker=paper_replay.SessionPositionTracker(max_positions=10, portfolio_value=100_000.0),
        params=SimpleNamespace(entry_window_end="10:15"),
        log_candle_progress=True,
    )

    messages = [
        record.getMessage() for record in caplog.records if "Replay candle" in record.getMessage()
    ]
    assert messages
    assert all("symbol=" not in message for message in messages)
