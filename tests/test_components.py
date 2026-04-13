"""Tests for reusable dashboard UI components."""

from __future__ import annotations

from types import SimpleNamespace

from web.components import THEME_CLEAN, extract_row_payload, get_current_theme


def test_extract_row_payload_prefers_row_in_list_args() -> None:
    event = SimpleNamespace(
        args=[
            {"type": "click", "clientX": 120, "clientY": 200},
            {"symbol": "SBIN", "date": "2026-03-09", "entry_time": "09:20", "exit_time": "09:55"},
        ]
    )
    row = extract_row_payload(event)
    assert row["symbol"] == "SBIN"
    assert row["date"] == "2026-03-09"


def test_extract_row_payload_uses_named_row_key() -> None:
    event = SimpleNamespace(args={"row": {"run_id": "abc123", "symbol": "RELIANCE"}})
    row = extract_row_payload(event)
    assert row["run_id"] == "abc123"
    assert row["symbol"] == "RELIANCE"


def test_extract_row_payload_rejects_pointer_only_payload() -> None:
    event = SimpleNamespace(args={"type": "click", "clientX": 10, "clientY": 20, "shiftKey": False})
    row = extract_row_payload(event)
    assert row == {}


def test_dashboard_defaults_to_clean_theme() -> None:
    assert get_current_theme() == THEME_CLEAN
