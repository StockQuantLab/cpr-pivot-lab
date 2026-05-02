from __future__ import annotations

import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from engine.paper_runtime import PaperRuntimeState
from scripts.paper_live import (
    _GLOBAL_FLATTEN_SIGNAL,
    _cleanup_feed_audit_if_needed,
    _entry_disabled_symbols,
    _is_admin_command_stale,
    _is_zero_trade_restart_session,
    _live_mark_feed_state,
    _prefetch_setup_rows,
    _resolve_poll_interval,
    _should_use_global_flatten_signal,
)


def test_should_use_global_flatten_signal_follows_file_exists(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    signal_file = _GLOBAL_FLATTEN_SIGNAL
    assert not _should_use_global_flatten_signal()

    (tmp_path / ".tmp_logs").mkdir()
    signal_file.touch()
    assert _should_use_global_flatten_signal()
    signal_file.unlink()
    assert not _should_use_global_flatten_signal()


def test_is_admin_command_stale_uses_file_mtime(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    cmd_file = tmp_path / "old_cmd.json"
    cmd_file.write_text("{}")
    now = datetime(2024, 1, 1, 10, 0, 0)
    old_time = now.timestamp() - 400
    os.utime(cmd_file, (old_time, old_time))

    assert _is_admin_command_stale(cmd_file, now, max_age_sec=300)
    assert not _is_admin_command_stale(cmd_file, now, max_age_sec=500)


def test_is_zero_trade_restart_session_checks_reason_and_db_closed_positions() -> None:
    class FakePaperDB:
        def __init__(self, closed_positions: int):
            self.closed_positions = closed_positions

        def get_session_positions(self, session_id: str, statuses: list[str] | None = None):
            _ = (session_id, statuses)
            if statuses == ["CLOSED"]:
                return [object()] * self.closed_positions
            return []

    assert _is_zero_trade_restart_session(
        "session-2026-04-29",
        terminal_reason="no_trades_entry_window_closed",
        paper_db=FakePaperDB(closed_positions=0),
    )
    assert not _is_zero_trade_restart_session(
        "session-2026-04-29",
        terminal_reason="feed_stale",
        paper_db=FakePaperDB(closed_positions=0),
    )
    assert not _is_zero_trade_restart_session(
        "session-2026-04-29",
        terminal_reason="NO_TRADES_ENTRY_WINDOW_CLOSED",
        paper_db=FakePaperDB(closed_positions=2),
    )


def test_resolve_poll_interval_pulls_tighter_near_candle_close() -> None:
    settings = SimpleNamespace(paper_live_poll_interval_sec=5.0)
    now = datetime(2024, 1, 1, 9, 19, 57)

    assert _resolve_poll_interval(settings, None, 5, now=now) == 0.5


def test_resolve_poll_interval_keeps_base_far_from_close() -> None:
    settings = SimpleNamespace(paper_live_poll_interval_sec=5.0)
    now = datetime(2024, 1, 1, 9, 16, 0)

    assert _resolve_poll_interval(settings, None, 5, now=now) == 5.0


def test_live_mark_feed_state_prefers_latest_ticker_ltp() -> None:
    class FakeTicker:
        def get_last_ltp(self, symbol: str) -> float | None:
            return {"SBIN": 102.5}.get(symbol)

    feed_state = _live_mark_feed_state(
        session_id="paper-live-1",
        symbol_last_prices={"SBIN": 100.0, "RELIANCE": 2500.0},
        ticker_adapter=FakeTicker(),
        symbols=["SBIN", "RELIANCE"],
    )

    assert feed_state.raw_state["symbol_last_prices"]["SBIN"] == 102.5
    assert feed_state.raw_state["symbol_last_prices"]["RELIANCE"] == 2500.0
    assert feed_state.raw_state["mark_source"] == "live_ltp"


def test_entry_disabled_symbols_keeps_only_open_positions() -> None:
    tracker = SimpleNamespace(_open={"SBIN": object(), "RELIANCE": object()})

    assert _entry_disabled_symbols(
        tracker=tracker,
        active_symbols=["SBIN", "RELIANCE", "TCS"],
    ) == ["RELIANCE", "SBIN"]


def test_entry_disabled_symbols_returns_empty_when_no_open_positions() -> None:
    tracker = SimpleNamespace(_open={})

    assert _entry_disabled_symbols(tracker=tracker, active_symbols=["SBIN"]) == []


def test_cleanup_feed_audit_retention_skips_when_interval_not_reached(monkeypatch) -> None:
    calls: list[int] = []

    class FakeDB:
        def cleanup_feed_audit_older_than(self, days: int) -> int:
            calls.append(days)
            return 3

        def cleanup_alert_log_older_than(self, days: int) -> int:
            calls.append(days)
            return 2

    monkeypatch.setattr("scripts.paper_live.get_paper_db", lambda: FakeDB())

    settings = SimpleNamespace(feed_audit_retention_days=7)
    now = datetime(2024, 1, 1, 10, 0, 0)
    last_cleanup = now - timedelta(seconds=20)

    updated, deleted = _cleanup_feed_audit_if_needed(
        now=now,
        last_cleanup=last_cleanup,
        settings=settings,
    )

    assert updated == last_cleanup
    assert deleted == 0
    assert calls == []


def test_cleanup_feed_audit_retention_runs_on_interval(monkeypatch) -> None:
    calls: list[int] = []

    class FakeDB:
        def cleanup_feed_audit_older_than(self, days: int) -> int:
            calls.append(days)
            return 3

        def cleanup_alert_log_older_than(self, days: int) -> int:
            calls.append(days)
            return 2

    monkeypatch.setattr("scripts.paper_live.get_paper_db", lambda: FakeDB())

    settings = SimpleNamespace(feed_audit_retention_days=7)
    now = datetime(2024, 1, 1, 10, 0, 0)
    last_cleanup = now - timedelta(minutes=40)

    updated, deleted = _cleanup_feed_audit_if_needed(
        now=now,
        last_cleanup=last_cleanup,
        settings=settings,
    )

    assert updated == now
    assert deleted == 5
    assert calls == [7, 7]


def test_prefetch_setup_rows_skips_invalid_critical_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=True)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return {
            "trade_date": "2024-01-01",
            "direction": "LONG",
            "tc": 0.0,
            "bc": 99.5,
            "atr": 1.2,
        }

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None


def test_prefetch_setup_rows_skips_invalid_critical_fields_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=False)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return {
            "trade_date": "2024-01-01",
            "direction": "SHORT",
            "tc": 101.0,
            "bc": 100.0,
            "atr": 0.0,
        }

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None


def test_prefetch_setup_rows_skips_missing_rows_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=False)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return None

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None
