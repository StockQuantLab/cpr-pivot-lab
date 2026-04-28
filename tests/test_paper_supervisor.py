"""Tests for the live paper process supervisor."""

from __future__ import annotations

from scripts.paper_supervisor import _normalize_live_args


def test_supervisor_defaults_to_canonical_daily_live_args() -> None:
    assert _normalize_live_args([]) == [
        "--multi",
        "--strategy",
        "CPR_LEVELS",
        "--trade-date",
        "today",
    ]


def test_supervisor_accepts_args_after_separator() -> None:
    assert _normalize_live_args(["--", "--multi", "--strategy", "CPR_LEVELS"]) == [
        "--multi",
        "--strategy",
        "CPR_LEVELS",
    ]


def test_supervisor_accepts_optional_daily_live_prefix() -> None:
    assert _normalize_live_args(["daily-live", "--trade-date", "2026-04-29"]) == [
        "--trade-date",
        "2026-04-29",
    ]
