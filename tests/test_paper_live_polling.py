from __future__ import annotations

import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from engine.paper_runtime import PaperRuntimeState
from scripts.paper_live import (
    _GLOBAL_FLATTEN_SIGNAL,
    _catch_up_true_or_from_kite,
    _cleanup_feed_audit_if_needed,
    _entry_disabled_symbols,
    _is_admin_command_stale,
    _is_zero_trade_restart_session,
    _kite_history_to_live_candles,
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


def test_kite_history_to_live_candles_anchors_or_bar_end() -> None:
    candles = _kite_history_to_live_candles(
        "SBIN",
        [
            {
                "date": datetime(2024, 1, 1, 9, 15),
                "open": 100.0,
                "high": 103.0,
                "low": 99.5,
                "close": 102.0,
                "volume": 1000,
            },
            {
                "date": datetime(2024, 1, 1, 9, 20),
                "open": 102.0,
                "high": 104.0,
                "low": 101.0,
                "close": 103.0,
                "volume": 900,
            },
        ],
        trade_date="2024-01-01",
        or_minutes=5,
    )

    assert len(candles) == 1
    assert candles[0]["bar_end"].hour == 9
    assert candles[0]["bar_end"].minute == 20
    assert candles[0]["time_str"] == "09:20"
    assert candles[0]["close"] == pytest.approx(102.0)


def test_catch_up_true_or_from_kite_merges_historical_or_candle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_or_proxy_setup=False)

    monkeypatch.setattr(
        "engine.kite_ingestion.resolve_instrument_tokens",
        lambda symbols, exchange="NSE": ({"SBIN": 123}, []),
    )
    monkeypatch.setattr("engine.kite_ingestion.get_kite_client", lambda: object())
    monkeypatch.setattr(
        "engine.kite_ingestion._historical_data_with_retry",
        lambda *args, **kwargs: [
            {
                "date": datetime(2024, 1, 1, 9, 15),
                "open": 100.0,
                "high": 103.0,
                "low": 99.5,
                "close": 102.0,
                "volume": 1000,
            }
        ],
    )

    result = _catch_up_true_or_from_kite(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        or_minutes=5,
        session_id="sess-real",
    )

    assert result == {"requested": 1, "fetched": 1, "missing": 0, "errors": 0}
    state = runtime_state.for_symbol("SBIN")
    assert len(state.candles) == 1
    assert state.candles[0]["bar_end"].strftime("%H:%M") == "09:20"


def test_prefetch_batch_path_fills_missing_or_from_caught_up_candle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_live as paper_live

    runtime_state = PaperRuntimeState(allow_live_setup_fallback=True, allow_or_proxy_setup=False)
    runtime_state.for_symbol("SBIN").candles = [
        {
            "bar_end": datetime(2024, 1, 1, 9, 20),
            "open": 96.0,
            "high": 108.0,
            "low": 95.0,
            "close": 107.0,
            "volume": 1000.0,
        }
    ]

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            _ = params
            assert "FROM market_day_state" in query
            return SimpleNamespace(
                fetchall=lambda: [
                    (
                        "SBIN",
                        "2024-01-01",
                        105.0,
                        101.0,
                        99.0,
                        100.0,
                        103.0,
                        97.0,
                        106.0,
                        94.0,
                        4.0,
                        1.9,
                        2.5,
                        0.0,
                        0.0,
                        0.0,
                        None,
                        "",
                        None,
                        None,
                        None,
                        "NONE",
                        True,
                        "OVERLAP",
                        None,
                        None,
                    )
                ]
            )

    class _FakeDb:
        con = _FakeCon()

    monkeypatch.setattr(paper_live, "get_live_market_db", lambda: _FakeDb())

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    setup_row = runtime_state.for_symbol("SBIN").setup_row
    assert setup_row["direction"] == "LONG"
    assert setup_row["open_915"] == pytest.approx(96.0)
    assert setup_row["or_high_5"] == pytest.approx(108.0)
    assert setup_row["or_low_5"] == pytest.approx(95.0)
    assert setup_row["or_close_5"] == pytest.approx(107.0)
    assert setup_row["open_side"] == "BELOW"
    assert setup_row["or_proxy"] is False


def test_prefetch_batch_path_falls_back_to_live_setup_when_market_row_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_live as paper_live

    runtime_state = PaperRuntimeState(allow_live_setup_fallback=True)
    runtime_state.for_symbol("SBIN").candles = [
        {
            "bar_end": datetime(2024, 1, 1, 9, 20),
            "open": 100.0,
            "high": 103.0,
            "low": 99.5,
            "close": 102.0,
            "volume": 1000.0,
        }
    ]

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            _ = params
            assert "FROM market_day_state" in query
            return SimpleNamespace(fetchall=lambda: [])

    class _FakeDb:
        con = _FakeCon()

    def fake_load_setup_row(symbol: str, trade_date: str, live_candles=None, **kwargs):
        assert symbol == "SBIN"
        assert trade_date == "2024-01-01"
        assert live_candles
        _ = kwargs
        return {
            "trade_date": trade_date,
            "direction": "LONG",
            "direction_pending": False,
            "tc": 101.0,
            "bc": 100.0,
            "atr": 2.0,
            "setup_source": "live_fallback",
        }

    monkeypatch.setattr(paper_live, "get_live_market_db", lambda: _FakeDb())
    monkeypatch.setattr(paper_live, "load_setup_row", fake_load_setup_row)
    monkeypatch.setattr(paper_live, "_ORIGINAL_LOAD_SETUP_ROW", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.for_symbol("SBIN").setup_row["direction"] == "LONG"
