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


def test_paper_send_command_rejects_invalid_action() -> None:
    result = backtest_tools.paper_send_command(
        "CPR_LEVELS_LONG-2026-04-24-live-kite",
        "close_one",
        symbols=["SBIN"],
    )

    assert (
        result["error"]
        == "Unknown action 'close_one'. Use one of: cancel_pending_intents, close_all, close_positions, pause_entries, resume_entries, set_risk_budget."
    )


def test_paper_send_command_accepts_set_risk_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

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
                    ("CPR_LEVELS_SHORT-2026-04-24-live-kite", "SHORT", "ACTIVE"),
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
