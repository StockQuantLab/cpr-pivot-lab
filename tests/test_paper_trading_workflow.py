"""Tests for the daily paper-trading orchestration workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

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
        wf_run_id: str | None = None,
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
        wf_run_id: str | None = None,
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


def test_summarize_walk_forward_sessions_uses_fetchall_without_pandas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            captured["query"] = query
            captured["params"] = params
            return SimpleNamespace(fetchall=lambda: [("paper-1", 1000.0), ("paper-2", -500.0)])

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr("db.duckdb.get_db", lambda: _FakeDB())

    summary = paper_trading._summarize_walk_forward_sessions(["paper-1", "paper-2"], 1_000_000.0)

    assert captured["params"] == ["paper-1", "paper-2", "PAPER"]
    assert summary == {
        "replayed_days": 2,
        "sessions_with_trades": 2,
        "total_pnl": 500.0,
        "total_return_pct": 0.05,
        "avg_daily_return_pct": 0.025,
        "profitable_days": 1,
        "profitable_days_ratio": 0.5,
        "worst_daily_return_pct": -0.05,
        "best_daily_return_pct": 0.1,
    }


@pytest.mark.asyncio
async def test_run_walk_forward_replay_uses_daily_workflow_per_trade_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    progress_events: list[dict[str, object]] = []

    async def fake_run_daily_workflow(
        *,
        mode: str,
        trade_date: str,
        symbols: list[str],
        strategy: str,
        strategy_params: dict,
        session_id: str | None,
        notes: str | None,
        replay_kwargs: dict[str, object] | None = None,
        live_kwargs: dict[str, object] | None = None,
        wf_run_id: str | None = None,
    ):
        calls.append(trade_date)
        if trade_date == "2024-01-02":
            return {
                "trade_date": trade_date,
                "preparation": {
                    "trade_date": trade_date,
                    "coverage_ready": False,
                    "missing_counts": {"paper_positions": 1},
                },
            }
        return {
            "trade_date": trade_date,
            "session_id": f"paper-{trade_date}",
            "preparation": {
                "trade_date": trade_date,
                "coverage_ready": True,
                "missing_counts": {"paper_positions": 0},
            },
            "replay": {
                "session_id": f"paper-{trade_date}",
                "bars": 12,
            },
            "status": "REPLAYED",
        }

    async def fake_create_walk_forward_run(**kwargs):
        return SimpleNamespace(wf_run_id=kwargs.get("wf_run_id"))

    async def fake_update_walk_forward_run(wf_run_id: str, **kwargs):
        return SimpleNamespace(wf_run_id=wf_run_id, **kwargs)

    async def fake_get_session(session_id: str):
        return None  # no existing session → proceed normally

    def fake_summarize(session_ids, portfolio_value):
        return {"replayed_days": len(session_ids), "sessions_with_trades": 0}

    monkeypatch.setattr(paper_trading, "_run_daily_workflow", fake_run_daily_workflow)
    monkeypatch.setattr(paper_trading, "create_walk_forward_run", fake_create_walk_forward_run)
    monkeypatch.setattr(paper_trading, "update_walk_forward_run", fake_update_walk_forward_run)
    monkeypatch.setattr(paper_trading, "get_session", fake_get_session)
    monkeypatch.setattr(
        paper_trading, "_get_archived_paper_result_run_ids", lambda session_ids: set()
    )
    monkeypatch.setattr(paper_trading, "_summarize_walk_forward_sessions", fake_summarize)
    monkeypatch.setattr(
        paper_trading, "iter_trade_dates", lambda s, e: ["2024-01-01", "2024-01-02", "2024-01-03"]
    )

    async def fake_preflight(**kw):
        return {"ready": True}

    monkeypatch.setattr(
        paper_trading,
        "_validate_walk_forward_runtime_preflight",
        fake_preflight,
    )

    @dataclass
    class _FakeFold:
        fold_id: int = 0
        wf_run_id: str = ""
        fold_index: int = 0
        trade_date: str = ""
        status: str = ""
        reference_run_id: str | None = None
        paper_session_id: str | None = None
        total_trades: int = 0
        total_pnl: float | None = None
        total_return_pct: float | None = None
        summary_json: dict[str, Any] = field(default_factory=dict)
        parity_actual_run_id: str | None = None
        parity_status: str | None = None
        created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
        updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    upserted_folds: list[_FakeFold] = []

    async def fake_list_folds(wf_run_id):
        return list(upserted_folds)

    async def fake_upsert_fold(**kw):
        fold = _FakeFold(**{k: v for k, v in kw.items() if k in _FakeFold.__dataclass_fields__})
        upserted_folds.append(fold)
        return fold

    monkeypatch.setattr(paper_trading, "list_walk_forward_folds", fake_list_folds)
    monkeypatch.setattr(paper_trading, "upsert_walk_forward_fold", fake_upsert_fold)

    payload = await paper_trading.run_walk_forward_replay(
        start_date="2024-01-01",
        end_date="2024-01-03",
        symbols=["SBIN"],
        strategy="FBR",
        strategy_params={"failure_window": 10},
        progress_hook=progress_events.append,
    )

    assert calls == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert payload["trade_dates"] == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert payload["replayed_days"] == 2
    assert payload["days_requested"] == 3
    assert payload["results"][1]["status"] == "SKIPPED"
    assert payload["results"][0]["status"] == "REPLAYED"
    assert payload["results"][2]["replay"]["session_id"] == "paper-2024-01-03"
    assert "decision" in payload
    assert "wf_run_id" in payload
    assert [event["status"] for event in progress_events] == ["REPLAYED", "SKIPPED", "REPLAYED"]


@pytest.mark.asyncio
async def test_run_walk_forward_replay_marks_completed_sessions_without_archive_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_calls: list[str] = []

    async def fake_run_daily_workflow(**kwargs):
        workflow_calls.append(kwargs["trade_date"])
        return {"status": "REPLAYED"}

    async def fake_create_walk_forward_run(**kwargs):
        return SimpleNamespace(wf_run_id=kwargs.get("wf_run_id"))

    async def fake_update_walk_forward_run(wf_run_id: str, **kwargs):
        return SimpleNamespace(wf_run_id=wf_run_id, **kwargs)

    async def fake_get_session(session_id: str):
        return SimpleNamespace(session_id=session_id, status="COMPLETED")

    monkeypatch.setattr(paper_trading, "_run_daily_workflow", fake_run_daily_workflow)
    monkeypatch.setattr(paper_trading, "create_walk_forward_run", fake_create_walk_forward_run)
    monkeypatch.setattr(paper_trading, "update_walk_forward_run", fake_update_walk_forward_run)
    monkeypatch.setattr(paper_trading, "get_session", fake_get_session)
    monkeypatch.setattr(
        paper_trading, "_get_archived_paper_result_run_ids", lambda session_ids: set()
    )
    monkeypatch.setattr(
        paper_trading,
        "_summarize_walk_forward_sessions",
        lambda session_ids, portfolio_value: {
            "replayed_days": len(session_ids),
            "sessions_with_trades": 0,
        },
    )
    monkeypatch.setattr(paper_trading, "iter_trade_dates", lambda s, e: ["2024-01-01"])

    async def fake_preflight(**kw):
        return {"ready": True}

    monkeypatch.setattr(
        paper_trading,
        "_validate_walk_forward_runtime_preflight",
        fake_preflight,
    )

    async def fake_list_folds(wf_run_id):
        return []

    monkeypatch.setattr(paper_trading, "list_walk_forward_folds", fake_list_folds)

    payload = await paper_trading.run_walk_forward_replay(
        start_date="2024-01-01",
        end_date="2024-01-01",
        symbols=["SBIN"],
        strategy="FBR",
        strategy_params={},
    )

    assert workflow_calls == []
    assert payload["replayed_days"] == 0
    assert payload["results"] == [
        {
            "trade_date": "2024-01-01",
            "status": "MISSING_RESULTS",
            "session_id": "paper-fbr-2024-01-01",
            "reason": "completed session has no archived PAPER results; rerun with --force",
        }
    ]


@pytest.mark.asyncio
async def test_run_walk_forward_validation_writes_fold_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, object]] = []
    folds: list[dict[str, object]] = []
    updated: list[dict[str, object]] = []
    validator_calls: list[dict[str, object]] = []

    async def fake_create_walk_forward_run(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(wf_run_id=kwargs["wf_run_id"], gate_key=kwargs["gate_key"])

    async def fake_reset_walk_forward_folds(wf_run_id: str) -> None:
        folds.clear()

    async def fake_upsert_walk_forward_fold(**kwargs):
        folds.append(kwargs)
        return SimpleNamespace(**kwargs)

    async def fake_update_walk_forward_run(wf_run_id: str, **kwargs):
        updated.append({"wf_run_id": wf_run_id, **kwargs})
        return SimpleNamespace(wf_run_id=wf_run_id, **kwargs)

    def fake_run_fast_walk_forward_validation(**kwargs):
        validator_calls.append(kwargs)
        return {
            "trade_dates": ["2024-01-02", "2024-01-03"],
            "folds": [
                {
                    "fold_index": 1,
                    "trade_date": "2024-01-02",
                    "status": "COMPLETED",
                    "reference_run_id": "run-a",
                    "total_trades": 2,
                    "total_pnl": 10.0,
                    "total_return_pct": 0.1,
                },
                {
                    "fold_index": 2,
                    "trade_date": "2024-01-03",
                    "status": "COMPLETED",
                    "reference_run_id": "run-b",
                    "total_trades": 1,
                    "total_pnl": -5.0,
                    "total_return_pct": -0.05,
                },
            ],
            "portfolio_value": 1_000_000.0,
            "gate_key": "abc123",
            "normalized_strategy_params": kwargs["strategy_params"],
        }

    monkeypatch.setattr(paper_trading, "create_walk_forward_run", fake_create_walk_forward_run)
    monkeypatch.setattr(paper_trading, "reset_walk_forward_folds", fake_reset_walk_forward_folds)
    monkeypatch.setattr(paper_trading, "upsert_walk_forward_fold", fake_upsert_walk_forward_fold)
    monkeypatch.setattr(paper_trading, "update_walk_forward_run", fake_update_walk_forward_run)
    monkeypatch.setattr(
        paper_trading, "run_fast_walk_forward_validation", fake_run_fast_walk_forward_validation
    )
    monkeypatch.setattr(
        paper_trading,
        "validate_walk_forward_runtime_coverage",
        lambda **kwargs: {
            "coverage_ready": True,
            "trade_dates": ["2024-01-02", "2024-01-03"],
            "missing_by_date": [],
        },
    )

    async def fake_preflight(**kw):
        return {"ready": True}

    monkeypatch.setattr(
        paper_trading,
        "_validate_walk_forward_runtime_preflight",
        fake_preflight,
    )
    monkeypatch.setattr(
        paper_trading, "iter_trade_dates", lambda s, e: ["2024-01-02", "2024-01-03"]
    )

    payload = await paper_trading.run_walk_forward_validation(
        start_date="2024-01-02",
        end_date="2024-01-03",
        symbols=["SBIN", "RELIANCE"],
        strategy="CPR_LEVELS",
        strategy_params={"rr_ratio": 1.8},
        notes="gate",
        force=False,
        all_symbols=True,
    )

    expected_gate_key = paper_trading.make_gate_key(
        "CPR_LEVELS", paper_trading.normalize_strategy_params({"rr_ratio": 1.8})
    )
    assert created[0]["validation_engine"] == "fast_validator"
    assert created[0]["gate_key"] == expected_gate_key
    assert created[0]["scope_key"] == "ALL:2"
    assert payload["wf_run_id"] == created[0]["wf_run_id"]
    assert folds[0]["reference_run_id"] == "run-a"
    assert updated[0]["decision"] == "INCONCLUSIVE"  # 2 days < min 5 days
    assert payload["validation_engine"] == "fast_validator"
    assert payload["gate_key"] == expected_gate_key
    assert payload["summary"]["replayed_days"] == 2
    assert validator_calls[0]["wf_run_id"] == created[0]["wf_run_id"]


@pytest.mark.asyncio
async def test_run_walk_forward_validation_fails_fast_on_stale_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, object]] = []

    def fake_validate_walk_forward_runtime_coverage(**kwargs):
        return {
            "start_date": kwargs["start_date"],
            "end_date": kwargs["end_date"],
            "requested_symbols": kwargs["symbols"],
            "trade_dates": ["2024-03-11", "2024-03-12"],
            "coverage_ready": False,
            "missing_by_date": [
                {
                    "trade_date": "2024-03-11",
                    "missing_counts": {
                        "market_day_state": 1,
                        "strategy_day_state": 1,
                        "intraday_day_pack": 0,
                    },
                }
            ],
            "table_max_trade_dates": {
                "cpr_daily": "2024-03-10",
                "atr_intraday": "2024-03-10",
                "cpr_thresholds": "2024-03-10",
                "or_daily": "2024-03-10",
                "virgin_cpr_flags": "2024-03-10",
                "market_day_state": "2024-03-10",
                "strategy_day_state": "2024-03-10",
                "intraday_day_pack": "2024-03-12",
            },
        }

    async def fake_create_walk_forward_run(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(wf_run_id=kwargs["wf_run_id"], gate_key=kwargs["gate_key"])

    monkeypatch.setattr(
        paper_trading,
        "validate_walk_forward_runtime_coverage",
        fake_validate_walk_forward_runtime_coverage,
    )
    monkeypatch.setattr(paper_trading, "create_walk_forward_run", fake_create_walk_forward_run)

    async def fake_get_schema():
        return {"walk_forward_runs": True, "walk_forward_folds": True}

    monkeypatch.setattr(paper_trading, "get_walk_forward_run_schema", fake_get_schema)

    with pytest.raises(SystemExit) as exc:
        await paper_trading.run_walk_forward_validation(
            start_date="2024-03-11",
            end_date="2024-03-12",
            symbols=["SBIN"],
            strategy="CPR_LEVELS",
            strategy_params={"rr_ratio": 1.8},
            notes=None,
            force=False,
            all_symbols=True,
        )

    assert "Walk-forward runtime coverage is incomplete" in str(exc.value)
    assert created == []
