"""Tests for live paper admin command helpers."""

from __future__ import annotations

import json
from pathlib import Path

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

    assert result["error"] == "Unknown action 'close_one'. Use 'close_positions' or 'close_all'."
