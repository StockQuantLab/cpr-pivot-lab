"""Tests for live paper dashboard state helpers."""

from __future__ import annotations

from types import SimpleNamespace

from engine.paper_runtime import summarize_paper_positions


def test_summarize_paper_positions_combines_realized_and_unrealized_pnl() -> None:
    session = SimpleNamespace(
        session_id="paper-1",
        status="ACTIVE",
        latest_candle_ts="2024-01-01T09:20:00",
        stale_feed_at="2024-01-01T09:25:00",
    )
    positions = [
        SimpleNamespace(
            status="OPEN",
            direction="LONG",
            current_qty=2,
            quantity=2,
            entry_price=100.0,
            last_price=105.0,
            realized_pnl=None,
        ),
        SimpleNamespace(
            status="CLOSED",
            direction="SHORT",
            current_qty=1,
            quantity=1,
            entry_price=110.0,
            last_price=106.0,
            realized_pnl=4.0,
        ),
    ]
    feed_state = SimpleNamespace(status="OK", stale_reason=None, last_price=105.0)

    summary = summarize_paper_positions(session, positions, feed_state)

    assert summary["session_id"] == "paper-1"
    assert summary["open_positions"] == 1
    assert summary["closed_positions"] == 1
    assert summary["realized_pnl"] == 4.0
    assert summary["unrealized_pnl"] == 10.0
    assert summary["net_pnl"] == 14.0
    assert summary["feed_status"] == "OK"


def test_summarize_paper_positions_uses_symbol_specific_marks() -> None:
    session = SimpleNamespace(
        session_id="paper-2",
        status="ACTIVE",
        latest_candle_ts="2024-01-01T09:20:00",
        stale_feed_at="2024-01-01T09:25:00",
    )
    positions = [
        SimpleNamespace(
            status="OPEN",
            symbol="SBIN",
            direction="LONG",
            current_qty=1,
            quantity=1,
            entry_price=100.0,
            last_price=None,
            realized_pnl=None,
        ),
        SimpleNamespace(
            status="OPEN",
            symbol="RELIANCE",
            direction="SHORT",
            current_qty=1,
            quantity=1,
            entry_price=200.0,
            last_price=None,
            realized_pnl=None,
        ),
    ]
    feed_state = SimpleNamespace(
        status="OK",
        stale_reason=None,
        last_price=999.0,
        raw_state={"symbol_last_prices": {"SBIN": 105.0, "RELIANCE": 195.0}},
    )

    summary = summarize_paper_positions(session, positions, feed_state)

    assert summary["unrealized_pnl"] == 10.0
    assert summary["net_pnl"] == 10.0


def test_summarize_paper_positions_uses_symbol_specific_marks_from_raw_json_string() -> None:
    session = SimpleNamespace(
        session_id="paper-3",
        status="ACTIVE",
        latest_candle_ts="2024-01-01T09:20:00",
        stale_feed_at="2024-01-01T09:25:00",
    )
    positions = [
        SimpleNamespace(
            status="OPEN",
            symbol="SBIN",
            direction="LONG",
            current_qty=1,
            quantity=1,
            entry_price=100.0,
            last_price=None,
            realized_pnl=None,
        ),
        SimpleNamespace(
            status="OPEN",
            symbol="RELIANCE",
            direction="LONG",
            current_qty=1,
            quantity=1,
            entry_price=200.0,
            last_price=None,
            realized_pnl=None,
        ),
    ]
    feed_state = SimpleNamespace(
        status="OK",
        stale_reason=None,
        last_price=999.0,
        raw_state='{"symbol_last_prices": {"SBIN": 105.0, "RELIANCE": 195.0}}',
    )

    summary = summarize_paper_positions(session, positions, feed_state)

    assert summary["unrealized_pnl"] == 0.0
