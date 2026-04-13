"""Tests for the daily paper-trading orchestration workflow."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.paper_trading as paper_trading


def test_default_session_id_includes_direction_for_daily_sessions() -> None:
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "CPR_LEVELS",
            {"direction_filter": "LONG"},
            "live",
        )
        == "paper-cpr_levels-long-2024-01-02-live"
    )
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "FBR",
            {"direction_filter": "SHORT"},
            "live",
        )
        == "paper-fbr-short-2024-01-02-live"
    )


@pytest.mark.asyncio
async def test_run_daily_workflow_replay_uses_shared_preparation_and_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls["prepare"] = {
            "trade_date": trade_date,
            "symbols": symbols,
            "mode": mode,
        }
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": True,
            "coverage": {"paper_trading_sessions": [], "paper_positions": []},
        }

    async def fake_ensure_daily_session(
        *,
        session_id: str | None,
        trade_date: str,
        strategy: str,
        symbols: list[str],
        strategy_params: dict,
        notes: str | None,
        mode: str = "replay",
    ):
        calls["ensure"] = {
            "session_id": session_id,
            "trade_date": trade_date,
            "strategy": strategy,
            "symbols": symbols,
            "strategy_params": strategy_params,
            "notes": notes,
        }
        return SimpleNamespace(session_id="paper-2024-01-02")

    async def fake_replay_session(
        *,
        session_id: str,
        symbols: list[str] | None,
        start_date: str | None,
        end_date: str | None,
        leave_active: bool,
        notes: str | None,
        preloaded_days=None,
    ):
        calls["replay"] = {
            "session_id": session_id,
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
            "leave_active": leave_active,
            "notes": notes,
        }
        return {"status": "REPLAYED", "bars": 12}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "replay_session", fake_replay_session)

    payload = await paper_trading._run_daily_workflow(
        mode="replay",
        trade_date="2024-01-02",
        symbols=["SBIN", "RELIANCE"],
        strategy="FBR",
        strategy_params={"failure_window": 10},
        session_id="paper-2024-01-02",
        notes="daily replay",
        replay_kwargs={"leave_active": True},
    )

    assert calls["prepare"] == {
        "trade_date": "2024-01-02",
        "symbols": ["SBIN", "RELIANCE"],
        "mode": "replay",
    }
    assert calls["ensure"] == {
        "session_id": "paper-2024-01-02",
        "trade_date": "2024-01-02",
        "strategy": "FBR",
        "symbols": ["SBIN", "RELIANCE"],
        "strategy_params": {"failure_window": 10},
        "notes": "daily replay",
    }
    assert calls["replay"] == {
        "session_id": "paper-2024-01-02",
        "symbols": ["SBIN", "RELIANCE"],
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
        "leave_active": True,
        "notes": "daily replay",
    }
    assert payload["session_id"] == "paper-2024-01-02"
    assert payload["preparation"]["coverage_ready"] is True
    assert payload["status"] == "REPLAYED"


@pytest.mark.asyncio
async def test_run_daily_workflow_live_uses_live_session_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls["prepare"] = {
            "trade_date": trade_date,
            "symbols": symbols,
            "mode": mode,
        }
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": True,
            "coverage": {"paper_trading_sessions": [], "paper_positions": []},
        }

    async def fake_ensure_daily_session(
        *,
        session_id: str | None,
        trade_date: str,
        strategy: str,
        symbols: list[str],
        strategy_params: dict,
        notes: str | None,
        mode: str = "replay",
    ):
        calls["ensure"] = {
            "session_id": session_id,
            "trade_date": trade_date,
            "strategy": strategy,
            "symbols": symbols,
            "strategy_params": strategy_params,
            "notes": notes,
        }
        return SimpleNamespace(session_id="paper-live-1")

    async def fake_run_live_session(
        *,
        session_id: str,
        symbols: list[str] | None,
        poll_interval_sec: float | None,
        candle_interval_minutes: int | None,
        max_cycles: int | None,
        complete_on_exit: bool,
        allow_late_start_fallback: bool,
        notes: str | None,
        ticker_adapter: object | None = None,
    ):
        calls["live"] = {
            "session_id": session_id,
            "symbols": symbols,
            "poll_interval_sec": poll_interval_sec,
            "candle_interval_minutes": candle_interval_minutes,
            "max_cycles": max_cycles,
            "complete_on_exit": complete_on_exit,
            "allow_late_start_fallback": allow_late_start_fallback,
            "notes": notes,
            "ticker_adapter": ticker_adapter,
        }
        return {"status": "LIVE", "cycles": 2}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "run_live_session", fake_run_live_session)

    payload = await paper_trading._run_daily_workflow(
        mode="live",
        trade_date="2024-01-03",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={"rr_ratio": 1.8},
        session_id=None,
        notes=None,
        live_kwargs={
            "poll_interval_sec": 2.5,
            "candle_interval_minutes": 5,
            "max_cycles": 7,
            "complete_on_exit": True,
        },
    )

    assert calls["prepare"] == {
        "trade_date": "2024-01-03",
        "symbols": ["SBIN"],
        "mode": "live",
    }
    assert calls["ensure"] == {
        "session_id": None,
        "trade_date": "2024-01-03",
        "strategy": "CPR_LEVELS",
        "symbols": ["SBIN"],
        "strategy_params": {"rr_ratio": 1.8},
        "notes": None,
    }
    assert calls["live"] == {
        "session_id": "paper-live-1",
        "symbols": ["SBIN"],
        "poll_interval_sec": 2.5,
        "candle_interval_minutes": 5,
        "max_cycles": 7,
        "complete_on_exit": True,
        "allow_late_start_fallback": False,
        "notes": None,
        "ticker_adapter": None,
    }
    assert payload["session_id"] == "paper-live-1"
    assert payload["status"] == "LIVE"


@pytest.mark.asyncio
async def test_run_daily_workflow_skips_when_coverage_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls.append("prepare")
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": False,
            "coverage": {
                "market_day_state": [],
                "strategy_day_state": ["SBIN"],
                "intraday_day_pack": ["SBIN"],
            },
        }

    async def fake_ensure_daily_session(**kwargs):
        calls.append("ensure")
        return SimpleNamespace(session_id="unused")

    async def fake_replay_session(**kwargs):
        calls.append("replay")
        return {"status": "REPLAYED"}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "replay_session", fake_replay_session)

    with pytest.raises(SystemExit) as exc:
        await paper_trading._run_daily_workflow(
            mode="replay",
            trade_date="2024-01-04",
            symbols=["SBIN"],
            strategy="FBR",
            strategy_params={},
            session_id=None,
            notes=None,
        )

    assert calls == ["prepare"]
    assert "Runtime coverage incomplete" in str(exc.value)
    assert "pivot-build --table strategy --force" in str(exc.value)
    assert "pivot-build --table pack --force --batch-size 64" in str(exc.value)


@pytest.mark.asyncio
async def test_run_daily_workflow_live_raises_when_coverage_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls.append("prepare")
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": False,
            "coverage": {
                "market_day_state": [],
                "strategy_day_state": ["SBIN"],
                "intraday_day_pack": [],
            },
        }

    async def fake_ensure_daily_session(**kwargs):
        calls.append("ensure")
        return SimpleNamespace(session_id="unused")

    async def fake_run_live_session(**kwargs):
        calls.append("live")
        return {"status": "LIVE"}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "run_live_session", fake_run_live_session)

    with pytest.raises(SystemExit) as exc:
        await paper_trading._run_daily_workflow(
            mode="live",
            trade_date="2024-01-04",
            symbols=["SBIN"],
            strategy="FBR",
            strategy_params={},
            session_id=None,
            notes=None,
        )

    assert calls == ["prepare"]
    assert "Runtime coverage incomplete" in str(exc.value)
