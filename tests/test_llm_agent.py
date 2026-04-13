"""Tests for paper-session agent tool wiring."""

from __future__ import annotations

import importlib
from types import SimpleNamespace


def test_create_agent_includes_paper_session_tools(monkeypatch) -> None:
    captured: dict[str, object] = {}

    import engine.cli_setup as cli_setup

    monkeypatch.setattr(cli_setup, "configure_windows_stdio", lambda **kwargs: None)

    llm_agent = importlib.import_module("agent.llm_agent")

    class FakeAgent:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(llm_agent, "Agent", FakeAgent)
    monkeypatch.setattr(llm_agent, "create_ollama_model", lambda: "model")
    monkeypatch.setattr(
        llm_agent,
        "PgAgentStorage",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )

    agent = llm_agent.create_agent(session_id="paper-session")

    assert isinstance(agent, FakeAgent)
    tool_names = [getattr(tool, "__wrapped__", tool).__name__ for tool in captured["tools"]]
    assert "list_paper_sessions" in tool_names
    assert "get_paper_session_summary" in tool_names
    assert "get_paper_positions" in tool_names
    assert "get_paper_ledger" in tool_names
    assert "run_backtest" in tool_names
