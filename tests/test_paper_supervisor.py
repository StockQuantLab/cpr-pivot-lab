"""Tests for the live paper process supervisor."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.paper_supervisor import (
    _extract_trade_date,
    _has_active_session_for_trade_date,
    _normalize_live_args,
    _warn_if_clock_drift,
    _watch_relaunch_allowed,
)


class _FakeCursor:
    def __init__(self, row: tuple[str, ...] | None):
        self._row = row

    def fetchone(self) -> tuple[str, ...] | None:
        return self._row


class _FakeConn:
    def __init__(self, row: tuple[str, ...] | None, expected_trade_date: str):
        self._row = row
        self._expected_trade_date = expected_trade_date
        self.seen: list[str] = []
        self.params: list[object] = []

    def execute(self, query: str, params: list[object]) -> _FakeCursor:
        self.seen.append(query)
        self.params = list(params)
        assert params[0] == self._expected_trade_date
        return _FakeCursor(self._row)


class _FakePaperDb:
    def __init__(self, row: tuple[str, ...] | None = None, trade_date: str = "2026-04-29"):
        self.con = _FakeConn(row, trade_date)


def test_supervisor_extracts_trade_date() -> None:
    assert _extract_trade_date(["--trade-date", "2026-04-29"]) == "2026-04-29"
    assert _extract_trade_date(["--trade-date=2026-04-29"]) == "2026-04-29"
    assert _extract_trade_date([]) == "today"
    assert _extract_trade_date(["--strategy", "CPR_LEVELS"]) == "today"


def test_supervisor_has_active_session_for_trade_date() -> None:
    paper_db = _FakePaperDb(row=("1",))
    assert _has_active_session_for_trade_date(
        trade_date="2026-04-29",
        db=paper_db,
    )
    assert paper_db.con.params == ["2026-04-29", "ACTIVE", "PAUSED", "STOPPING"]


def test_supervisor_has_no_active_session_for_trade_date() -> None:
    paper_db = _FakePaperDb(row=None, trade_date="2026-04-29")
    assert not _has_active_session_for_trade_date(trade_date="2026-04-29", db=paper_db)


@pytest.mark.parametrize("raw", [[], ["--multi", "--strategy", "CPR_LEVELS"]])
def test_supervisor_defaults_to_canonical_daily_live_args(raw: list[str]) -> None:
    defaulted = _normalize_live_args(raw)
    if raw:
        assert defaulted == raw
    else:
        assert defaulted == [
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


def test_supervisor_watch_relaunch_cutoff() -> None:
    assert _watch_relaunch_allowed(datetime(2026, 5, 2, 14, 59, 59))
    assert not _watch_relaunch_allowed(datetime(2026, 5, 2, 15, 0, 0))


def test_supervisor_watch_relaunch_cutoff_uses_ist_for_aware_time() -> None:
    # 09:29:59 UTC is 14:59:59 IST, still allowed.
    assert _watch_relaunch_allowed(datetime(2026, 5, 2, 9, 29, 59, tzinfo=UTC))
    # 09:30:00 UTC is 15:00:00 IST, cutoff reached.
    assert not _watch_relaunch_allowed(datetime(2026, 5, 2, 9, 30, 0, tzinfo=UTC))


def test_supervisor_clock_drift_warning(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "scripts.paper_supervisor._measure_clock_drift_sec",
        lambda **kwargs: 45.0,
    )

    drift = _warn_if_clock_drift(warn_threshold_sec=30.0)

    assert drift == 45.0
    assert "WARNING: local clock drift" in capsys.readouterr().out
