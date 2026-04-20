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
        == "paper-cpr_levels-long-2024-01-02-live-kite"
    )
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "FBR",
            {"direction_filter": "SHORT"},
            "live",
            "local",
        )
        == "paper-fbr-short-2024-01-02-live-local"
    )
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "CPR_LEVELS",
            {"direction_filter": "SHORT"},
            "replay",
        )
        == "paper-cpr_levels-short-2024-01-02-replay-historical"
    )


def test_variant_exit_summary_and_retry_policy_detects_spurious_early_completion() -> None:
    summary = paper_trading._variant_exit_summary(
        {
            "final_status": "COMPLETED",
            "last_bar_ts": "2026-04-15T09:25:00+05:30",
            "terminal_reason": "spurious_completion",
            "closed_bars": 17,
            "cycles": 9,
        }
    )

    should_retry, reason = paper_trading._should_retry_variant_exit(
        summary,
        current_hhmm="09:26",
        entry_window_closed_hhmm="10:30",
        eod_cutoff_hhmm="14:30",
    )

    assert summary["status"] == "COMPLETED"
    assert summary["last_bar_hhmm"] == "09:25"
    assert summary["closed_bars"] == 17
    assert should_retry is True
    assert reason == "completed early at 09:25"


def test_variant_exit_summary_and_retry_policy_respects_intentional_completion() -> None:
    summary = paper_trading._variant_exit_summary(
        {
            "final_status": "COMPLETED",
            "last_bar_ts": "2026-04-15T09:25:00+05:30",
            "terminal_reason": "complete_on_exit",
        }
    )

    should_retry, reason = paper_trading._should_retry_variant_exit(
        summary,
        current_hhmm="09:26",
        entry_window_closed_hhmm="10:30",
        eod_cutoff_hhmm="14:30",
    )

    assert should_retry is False
    assert reason == "complete_on_exit"


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
        auto_flatten_on_abnormal_exit: bool,
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
            "auto_flatten_on_abnormal_exit": auto_flatten_on_abnormal_exit,
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
        "auto_flatten_on_abnormal_exit": True,
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


@pytest.mark.asyncio
async def test_ensure_daily_session_reuses_existing_live_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    existing = SimpleNamespace(session_id="paper-live-1", status="FAILED")
    calls: list[str] = []

    async def fake_get_session(session_id: str):
        calls.append(session_id)
        return existing

    async def fake_create_paper_session(**kwargs):
        raise AssertionError(f"create_paper_session should not be called: {kwargs}")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    session = await pt._ensure_daily_session(
        session_id="paper-live-1",
        trade_date="2024-01-03",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"rr_ratio": 1.8},
        notes="retry",
        mode="live",
    )

    assert session is existing
    assert calls == ["paper-live-1"]


async def test_ensure_daily_session_creates_fallback_for_replay_collisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    existing = SimpleNamespace(session_id="paper-replay-1", status="COMPLETED")
    calls: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        assert session_id == "paper-replay-1"
        return existing

    async def fake_create_paper_session(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(session_id=kwargs["session_id"])

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)
    monkeypatch.setattr(pt, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))

    session = await pt._ensure_daily_session(
        session_id="paper-replay-1",
        trade_date="2024-01-03",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"rr_ratio": 1.8},
        notes="retry",
        mode="replay",
    )

    assert session.session_id == "paper-replay-1-abcdef"
    assert calls and calls[0]["session_id"] == "paper-replay-1-abcdef"
    assert calls[0]["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_reuses_same_session_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-live-1",
        status="FAILED",
        strategy="CPR_LEVELS",
    )
    open_positions = [
        SimpleNamespace(symbol="SBIN"),
        SimpleNamespace(symbol="RELIANCE"),
    ]
    calls: dict[str, object] = {}
    alerts: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        calls["get_session"] = session_id
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        calls["get_positions"] = {
            "session_id": session_id,
            "statuses": list(statuses or []),
            "symbol": symbol,
        }
        return open_positions

    async def fake_run_live_session(**kwargs):
        calls["run"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(pt, "run_live_session", fake_run_live_session)
    monkeypatch.setattr(
        pt,
        "dispatch_session_state_alert",
        lambda **kwargs: alerts.append(dict(kwargs)),
    )

    await pt._cmd_daily_live_resume(
        SimpleNamespace(
            session_id="paper-live-1",
            poll_interval_sec=1.5,
            candle_interval_minutes=5,
            max_cycles=9,
            complete_on_exit=False,
            no_alerts=False,
        )
    )

    assert calls["get_session"] == "paper-live-1"
    assert calls["get_positions"] == {
        "session_id": "paper-live-1",
        "statuses": ["OPEN"],
        "symbol": None,
    }
    assert calls["run"]["session_id"] == "paper-live-1"
    assert calls["run"]["symbols"] == ["SBIN", "RELIANCE"]
    assert calls["run"]["poll_interval_sec"] == 1.5
    assert calls["run"]["candle_interval_minutes"] == 5
    assert calls["run"]["max_cycles"] == 9
    assert calls["run"]["auto_flatten_on_abnormal_exit"] is False
    assert alerts and alerts[0]["state"] == "RESUMED"
    assert alerts[0]["session_id"] == "paper-live-1"


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_infers_session_id_from_preset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-cpr_levels-short-2024-01-03-live-kite",
        status="FAILED",
        strategy="CPR_LEVELS",
    )
    open_positions = [SimpleNamespace(symbol="SBIN")]
    calls: dict[str, object] = {}

    async def fake_get_session(session_id: str):
        calls["get_session"] = session_id
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        calls["get_positions"] = {
            "session_id": session_id,
            "statuses": list(statuses or []),
            "symbol": symbol,
        }
        return open_positions

    async def fake_run_live_session(**kwargs):
        calls["run"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(pt, "run_live_session", fake_run_live_session)
    monkeypatch.setattr(pt, "dispatch_session_state_alert", lambda **kwargs: None)

    await pt._cmd_daily_live_resume(
        SimpleNamespace(
            session_id=None,
            trade_date="2024-01-03",
            strategy="CPR_LEVELS",
            preset="CPR_LEVELS_RISK_SHORT",
            strategy_params=None,
            poll_interval_sec=1.5,
            candle_interval_minutes=5,
            max_cycles=9,
            complete_on_exit=False,
            no_alerts=True,
        )
    )

    assert calls["get_session"] == "paper-cpr_levels-short-2024-01-03-live-kite"
    assert calls["get_positions"] == {
        "session_id": "paper-cpr_levels-short-2024-01-03-live-kite",
        "statuses": ["OPEN"],
        "symbol": None,
    }
    assert calls["run"]["session_id"] == "paper-cpr_levels-short-2024-01-03-live-kite"
    assert calls["run"]["symbols"] == ["SBIN"]
    assert calls["run"]["poll_interval_sec"] == 1.5
    assert calls["run"]["candle_interval_minutes"] == 5
    assert calls["run"]["max_cycles"] == 9


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name, expected_state",
    [
        ("_cmd_pause", "PAUSED"),
        ("_cmd_resume", "RESUMED"),
    ],
)
async def test_pause_and_resume_commands_emit_state_alerts(
    monkeypatch: pytest.MonkeyPatch,
    handler_name: str,
    expected_state: str,
) -> None:
    import scripts.paper_trading as pt
    from db.paper_db import PaperSession

    alerts: list[dict[str, object]] = []

    def fake_update_session(session_id: str, **kwargs):
        return PaperSession(
            session_id=session_id,
            strategy="CPR_LEVELS",
            status=str(kwargs.get("status") or "ACTIVE"),
        )

    monkeypatch.setattr(pt, "_pdb", lambda: SimpleNamespace(update_session=fake_update_session))
    monkeypatch.setattr(
        pt,
        "dispatch_session_state_alert",
        lambda **kwargs: alerts.append(dict(kwargs)),
    )

    handler = getattr(pt, handler_name)
    await handler(SimpleNamespace(session_id="paper-live-1", notes="manual op"))

    assert alerts and alerts[0]["state"] == expected_state
    assert alerts[0]["session_id"] == "paper-live-1"


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_rejects_session_without_open_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-live-1",
        status="FAILED",
        strategy="CPR_LEVELS",
    )

    async def fake_get_session(session_id: str):
        assert session_id == "paper-live-1"
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        assert session_id == "paper-live-1"
        assert list(statuses or []) == ["OPEN"]
        assert symbol is None
        return []

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)

    with pytest.raises(SystemExit, match="No OPEN positions"):
        await pt._cmd_daily_live_resume(
            SimpleNamespace(
                session_id="paper-live-1",
                poll_interval_sec=1.0,
                candle_interval_minutes=5,
                max_cycles=None,
                complete_on_exit=False,
                no_alerts=False,
            )
        )
