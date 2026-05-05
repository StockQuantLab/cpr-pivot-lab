"""Tests for live paper admin command helpers."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import agent.tools.backtest_tools as backtest_tools
from engine.paper_runtime import write_admin_command


def test_write_admin_command_writes_queue_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    path = write_admin_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "close_positions",
        symbols=["sbin", "reliance"],
        reason="manual",
        requester="tester",
    )

    cmd_path = Path(path)
    assert cmd_path.exists()
    payload = json.loads(cmd_path.read_text())
    assert payload == {
        "action": "close_positions",
        "reason": "manual",
        "requester": "tester",
        "symbols": ["SBIN", "RELIANCE"],
    }
    assert cmd_path.parent.name == "cmd_CPR_LEVELS_LONG-2026-04-24-live-kite"


def test_write_admin_command_writes_risk_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    path = write_admin_command(
        "CPR_LEVELS_SHORT-2026-04-24-live-kite",
        "set_risk_budget",
        portfolio_value=500_000,
        max_positions=5,
        max_position_pct=0.10,
        reason="reduce_short_risk",
        requester="tester",
    )

    payload = json.loads(Path(path).read_text())
    assert payload["action"] == "set_risk_budget"
    assert payload["portfolio_value"] == 500_000
    assert payload["max_positions"] == 5
    assert payload["max_position_pct"] == 0.10


def test_write_admin_command_rejects_path_traversal_session_id(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="session_id contains unsupported characters"):
        write_admin_command("..\\..\\Windows\\Temp\\x", "close_all")


def test_write_admin_command_rejects_invalid_action(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="action must be one of"):
        write_admin_command("CPR_LEVELS_LONG-2026-04-24-live-kite", "../close_all")


def test_write_admin_command_rejects_invalid_symbol(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="Invalid symbol name"):
        write_admin_command(
            "CPR_LEVELS_LONG-2026-04-24-live-kite",
            "close_positions",
            symbols=["SBIN;DROP TABLE"],
        )


def test_write_admin_command_rejects_oversized_risk_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="portfolio_value"):
        write_admin_command(
            "CPR_LEVELS_LONG-2026-04-24-live-kite",
            "set_risk_budget",
            portfolio_value=100_000_000,
        )


def test_write_admin_command_strips_control_chars_from_text(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    path = write_admin_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "pause_entries",
        reason="risk\x00off",
        requester="operator\x1f",
    )

    payload = json.loads(Path(path).read_text())
    assert payload["reason"] == "riskoff"
    assert payload["requester"] == "operator"


def test_cancel_pending_admin_commands_deletes_other_files(tmp_path) -> None:
    from scripts.paper_live import _cancel_pending_admin_commands

    cmd_dir = tmp_path / "cmd_session"
    cmd_dir.mkdir()
    current = cmd_dir / "002_cancel_pending_intents.json"
    old_one = cmd_dir / "001_pause_entries.json"
    old_two = cmd_dir / "003_resume_entries.json"
    for path in (current, old_one, old_two):
        path.write_text("{}")

    cancelled = _cancel_pending_admin_commands(cmd_dir, current)

    assert cancelled == 2
    assert current.exists()
    assert not old_one.exists()
    assert not old_two.exists()


def test_paper_send_command_validates_and_forwards(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PIVOT_AGENT_ALLOW_MUTATIONS", "1")

    result = backtest_tools.paper_send_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "close_positions",
        symbols=["sbin"],
        reason="operator",
    )

    assert result["action"] == "close_positions"
    assert result["symbols"] == ["sbin"]
    assert result["session_id"] == "CPR_LEVELS_LONG-2026-04-24-live-kite"
    assert Path(result["command_file"]).exists()


def test_paper_send_command_rejects_invalid_action(monkeypatch) -> None:
    monkeypatch.setenv("PIVOT_AGENT_ALLOW_MUTATIONS", "1")
    result = backtest_tools.paper_send_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "close_one",
        symbols=["SBIN"],
    )

    assert (
        result["error"]
        == "Unknown action 'close_one'. Use one of: cancel_pending_intents, close_all, close_positions, pause_entries, resume_entries, set_risk_budget."
    )


def test_paper_send_command_requires_mutation_env(monkeypatch) -> None:
    monkeypatch.delenv("PIVOT_AGENT_ALLOW_MUTATIONS", raising=False)

    result = backtest_tools.paper_send_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "close_all",
    )

    assert "PIVOT_AGENT_ALLOW_MUTATIONS=1" in result["error"]


def test_paper_send_command_accepts_set_risk_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PIVOT_AGENT_ALLOW_MUTATIONS", "1")

    result = backtest_tools.paper_send_command(
        "CPR_LEVELS_SHORT-2026-04-24-live-kite",
        "set_risk_budget",
        portfolio_value=500_000,
        max_positions=5,
        reason="reduce_short_risk",
    )

    assert result["action"] == "set_risk_budget"
    assert result["portfolio_value"] == 500_000
    assert result["max_positions"] == 5
    assert Path(result["command_file"]).exists()


def test_paper_send_command_accepts_entry_pause_resume_and_cancel(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PIVOT_AGENT_ALLOW_MUTATIONS", "1")

    for action in ("pause_entries", "resume_entries", "cancel_pending_intents"):
        result = backtest_tools.paper_send_command(
            "CPR_LEVELS_SHORT-2026-04-24-live-kite",
            action,
            reason="operator_control",
        )

        assert result["action"] == action
        assert Path(result["command_file"]).exists()
        payload = json.loads(Path(result["command_file"]).read_text())
        assert payload["action"] == action


@pytest.mark.asyncio
async def test_cmd_send_command_queues_close_positions(tmp_path, monkeypatch, capsys) -> None:
    import scripts.paper_trading as pt

    monkeypatch.chdir(tmp_path)

    await pt._cmd_send_command(
        SimpleNamespace(
            session_id="CPR_LEVELS_LONG-2026-04-24-live-kite",
            action="close_positions",
            symbols="sbin,reliance",
            reason="operator",
            requester="test",
        )
    )

    out = json.loads(capsys.readouterr().out)
    cmd_path = Path(out["command_file"])
    assert cmd_path.exists()
    assert json.loads(cmd_path.read_text())["symbols"] == ["SBIN", "RELIANCE"]


@pytest.mark.asyncio
async def test_cmd_send_command_queues_pause_entries(tmp_path, monkeypatch, capsys) -> None:
    import scripts.paper_trading as pt

    monkeypatch.chdir(tmp_path)

    await pt._cmd_send_command(
        SimpleNamespace(
            session_id="CPR_LEVELS_LONG-2026-04-24-live-kite",
            action="pause_entries",
            symbols=None,
            reason="operator",
            requester="test",
            portfolio_value=None,
            max_positions=None,
            max_position_pct=None,
        )
    )

    out = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(out["command_file"]).read_text())
    assert payload["action"] == "pause_entries"


@pytest.mark.asyncio
async def test_cmd_flatten_both_queues_long_and_short(tmp_path, monkeypatch, capsys) -> None:
    import scripts.paper_trading as pt

    monkeypatch.chdir(tmp_path)

    class FakeCon:
        def execute(self, sql: str, params: list[str]):
            assert params == ["2026-04-24"]
            return SimpleNamespace(
                fetchall=lambda: [
                    ("CPR_LEVELS_LONG-2026-04-24-live-kite", "LONG", "ACTIVE"),
                    ("CPR_LEVELS_SHORT-2026-04-24-live-kite", "SHORT", "FAILED"),
                ]
            )

    monkeypatch.setattr(pt, "_pdb", lambda: SimpleNamespace(con=FakeCon()))

    await pt._cmd_flatten_both(
        SimpleNamespace(
            trade_date="2026-04-24",
            reason="risk_off",
            requester="test",
        )
    )

    out = json.loads(capsys.readouterr().out)
    assert len(out["commands"]) == 2
    for command in out["commands"]:
        payload = json.loads(Path(command["command_file"]).read_text())
        assert payload["action"] == "close_all"
        assert payload["reason"] == "risk_off"


class _FakeMultiTracker:
    def __init__(self) -> None:
        self._open: dict[str, object] = {}
        self.open_count = 0
        self.initial_capital = 200_000.0
        self.max_positions = 5
        self.max_position_pct = 0.2
        self.cash_available = 200_000.0

    def update_budget(
        self,
        *,
        portfolio_value: float | None = None,
        max_positions: int | None = None,
        max_position_pct: float | None = None,
    ) -> None:
        if portfolio_value is not None:
            self.initial_capital = portfolio_value
        if max_positions is not None:
            self.max_positions = max_positions
        if max_position_pct is not None:
            self.max_position_pct = max_position_pct

    def current_open_notional(self) -> float:
        return 0.0


class _FakeMultiPaperDb:
    def __init__(self) -> None:
        self.notes: list[str] = []
        self.syncs = 0

    def update_session(self, session_id: str, **kwargs):
        assert session_id == "sess-multi"
        self.notes.append(str(kwargs.get("notes") or ""))
        return None

    def force_sync(self) -> None:
        self.syncs += 1


class _FakeTicker:
    def __init__(self) -> None:
        self.updates: list[tuple[str, list[str]]] = []

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        self.updates.append((session_id, list(symbols)))


def _multi_ctx() -> SimpleNamespace:
    return SimpleNamespace(
        session_id="sess-multi",
        active_symbols=["SBIN", "RELIANCE"],
        entry_resume_symbols=["SBIN", "RELIANCE"],
        entry_universe_symbols=["SBIN", "RELIANCE"],
        entries_disabled=False,
        tracker=_FakeMultiTracker(),
        symbol_last_prices={"SBIN": 100.0},
        real_order_router=None,
        final_status="ACTIVE",
        terminal_reason=None,
    )


@pytest.mark.asyncio
async def test_live_multi_operator_controls_pause_resume_and_set_budget(
    tmp_path,
    monkeypatch,
) -> None:
    import scripts.paper_live as paper_live

    monkeypatch.chdir(tmp_path)
    fake_db = _FakeMultiPaperDb()
    monkeypatch.setattr(paper_live, "get_paper_db", lambda: fake_db)
    ctx = _multi_ctx()
    ticker = _FakeTicker()
    cmd_dir = tmp_path / ".tmp_logs" / "cmd_sess-multi"
    cmd_dir.mkdir(parents=True)

    (cmd_dir / "001_pause.json").write_text(
        json.dumps({"action": "pause_entries", "reason": "risk_off", "requester": "test"})
    )
    stopped = await paper_live._apply_live_multi_operator_controls(
        ctx=ctx,
        ticker_adapter=ticker,
        use_websocket=True,
        now=paper_live.datetime.now(paper_live.IST),
    )
    assert stopped is False
    assert ctx.entries_disabled is True
    assert not (cmd_dir / "001_pause.json").exists()
    assert fake_db.notes[-1] == "ENTRIES_PAUSED reason=risk_off requester=test"

    (cmd_dir / "002_budget.json").write_text(
        json.dumps(
            {
                "action": "set_risk_budget",
                "reason": "pilot_reduce",
                "requester": "test",
                "portfolio_value": 100_000,
                "max_positions": 1,
                "max_position_pct": 0.1,
            }
        )
    )
    stopped = await paper_live._apply_live_multi_operator_controls(
        ctx=ctx,
        ticker_adapter=ticker,
        use_websocket=True,
        now=paper_live.datetime.now(paper_live.IST),
    )
    assert stopped is False
    assert ctx.tracker.initial_capital == 100_000
    assert ctx.tracker.max_positions == 1
    assert ctx.tracker.max_position_pct == 0.1

    (cmd_dir / "003_resume.json").write_text(
        json.dumps({"action": "resume_entries", "reason": "risk_on", "requester": "test"})
    )
    stopped = await paper_live._apply_live_multi_operator_controls(
        ctx=ctx,
        ticker_adapter=ticker,
        use_websocket=True,
        now=paper_live.datetime.now(paper_live.IST),
    )
    assert stopped is False
    assert ctx.entries_disabled is False
    assert ticker.updates[-1] == ("sess-multi", ["SBIN", "RELIANCE"])
    assert fake_db.notes[-1] == "ENTRIES_RESUMED reason=risk_on requester=test"


@pytest.mark.asyncio
async def test_live_multi_operator_controls_cancel_pending(tmp_path, monkeypatch) -> None:
    import scripts.paper_live as paper_live

    monkeypatch.chdir(tmp_path)
    fake_db = _FakeMultiPaperDb()
    monkeypatch.setattr(paper_live, "get_paper_db", lambda: fake_db)
    ctx = _multi_ctx()
    cmd_dir = tmp_path / ".tmp_logs" / "cmd_sess-multi"
    cmd_dir.mkdir(parents=True)
    old_cmd = cmd_dir / "001_pause.json"
    cancel_cmd = cmd_dir / "002_cancel.json"
    old_cmd.write_text(json.dumps({"action": "pause_entries"}))
    cancel_cmd.write_text(json.dumps({"action": "cancel_pending_intents", "requester": "test"}))

    stopped = await paper_live._apply_live_multi_operator_controls(
        ctx=ctx,
        ticker_adapter=_FakeTicker(),
        use_websocket=True,
        now=paper_live.datetime.now(paper_live.IST),
    )

    assert stopped is False
    assert not old_cmd.exists()
    assert not cancel_cmd.exists()
    assert fake_db.notes[-1].startswith("PENDING_INTENTS_CANCELLED count=1")


@pytest.mark.asyncio
async def test_live_multi_operator_controls_logs_unknown_action(
    tmp_path, monkeypatch, caplog
) -> None:
    import scripts.paper_live as paper_live

    monkeypatch.chdir(tmp_path)
    ctx = _multi_ctx()
    cmd_dir = tmp_path / ".tmp_logs" / "cmd_sess-multi"
    cmd_dir.mkdir(parents=True)
    cmd_file = cmd_dir / "001_typo.json"
    cmd_file.write_text(json.dumps({"action": "restart_positions", "requester": "test"}))

    with caplog.at_level("WARNING"):
        stopped = await paper_live._apply_live_multi_operator_controls(
            ctx=ctx,
            ticker_adapter=_FakeTicker(),
            use_websocket=True,
            now=paper_live.datetime.now(paper_live.IST),
        )

    assert stopped is False
    assert not cmd_file.exists()
    assert "Unknown multi admin command action" in caplog.text


@pytest.mark.asyncio
async def test_live_multi_operator_controls_flatten_signal(tmp_path, monkeypatch) -> None:
    import scripts.paper_live as paper_live

    monkeypatch.chdir(tmp_path)
    signal_dir = tmp_path / ".tmp_logs"
    signal_dir.mkdir()
    signal_file = signal_dir / "flatten_sess-multi.signal"
    signal_file.write_text("1")
    ctx = _multi_ctx()
    flatten_calls: list[dict[str, object]] = []

    async def fake_flatten_session_positions(*args, **kwargs):
        flatten_calls.append({"args": args, "kwargs": kwargs})

    monkeypatch.setattr(paper_live, "flatten_session_positions", fake_flatten_session_positions)
    monkeypatch.setattr(paper_live, "_reconcile_live_session", lambda **kwargs: False)

    stopped = await paper_live._apply_live_multi_operator_controls(
        ctx=ctx,
        ticker_adapter=_FakeTicker(),
        use_websocket=True,
        now=paper_live.datetime.now(paper_live.IST),
    )

    assert stopped is True
    assert ctx.final_status == "COMPLETED"
    assert ctx.terminal_reason == "manual_flatten_signal"
    assert not signal_file.exists()
    assert flatten_calls[0]["args"] == ("sess-multi",)
