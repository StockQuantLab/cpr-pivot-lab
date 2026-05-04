from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from db import paper_db as paper_db_module
from db.paper_db import PaperDB


def test_cleanup_stale_sessions_only_cancels_stopping_rows(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        active = db.create_session(session_id="paper-active", status="ACTIVE")
        stopping = db.create_session(session_id="paper-stopping", status="STOPPING")

        db.con.execute(
            "UPDATE paper_sessions SET updated_at = CURRENT_TIMESTAMP - INTERVAL '16 minutes' "
            "WHERE session_id = ?",
            [stopping.session_id],
        )

        changed = db.cleanup_stale_sessions()

        assert changed == 1
        assert db.get_session(active.session_id).status == "ACTIVE"
        assert db.get_session(stopping.session_id).status == "CANCELLED"
        assert "auto-cancelled" in (db.get_session(stopping.session_id).notes or "")
    finally:
        db.close()


def test_deferred_sync_marker_written_and_cleared(tmp_path: Path, monkeypatch) -> None:
    marker = tmp_path / "replica" / "deferred_sync_pending.flag"
    monkeypatch.setattr(paper_db_module, "REPLICA_DIR", marker.parent)
    monkeypatch.setattr(paper_db_module, "DEFERRED_SYNC_MARKER", marker)

    class FakeSync:
        def __init__(self) -> None:
            self.force_sync_calls = 0

        def mark_dirty(self) -> None:
            return None

        def maybe_sync(self, _con) -> None:
            return None

        def force_sync(self, _con) -> None:
            self.force_sync_calls += 1

    sync = FakeSync()
    db = PaperDB(db_path=tmp_path / "paper.duckdb", replica_sync=sync)
    try:
        db.create_session(session_id="paper-sync", status="ACTIVE")
        db.defer_sync()
        assert marker.exists()

        db.open_position(
            session_id="paper-sync",
            symbol="SBIN",
            direction="LONG",
            qty=1,
            entry_price=100.0,
        )
        db.flush_deferred_sync()

        assert sync.force_sync_calls == 1
        assert not marker.exists()
    finally:
        db.close()


def test_deferred_sync_recovery_forces_snapshot_and_clears_marker(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "replica" / "deferred_sync_pending.flag"
    marker.parent.mkdir(parents=True)
    marker.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(paper_db_module, "REPLICA_DIR", marker.parent)
    monkeypatch.setattr(paper_db_module, "DEFERRED_SYNC_MARKER", marker)

    calls: list[str] = []

    class FakeSync:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

    class FakePaperDB:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def force_sync(self) -> None:
            calls.append("force_sync")
            marker.unlink(missing_ok=True)

        def close(self) -> None:
            calls.append("close")

    monkeypatch.setattr(paper_db_module, "ReplicaSync", FakeSync)
    monkeypatch.setattr(paper_db_module, "PaperDB", FakePaperDB)

    paper_db_module._recover_deferred_sync_if_needed()

    assert calls == ["force_sync", "close"]
    assert not marker.exists()


def test_deferred_sync_recovery_can_skip_live_db_open_for_dashboard(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "replica" / "deferred_sync_pending.flag"
    marker.parent.mkdir(parents=True)
    marker.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(paper_db_module, "REPLICA_DIR", marker.parent)
    monkeypatch.setattr(paper_db_module, "DEFERRED_SYNC_MARKER", marker)

    def fail_paper_db_open(*args, **kwargs):
        del args, kwargs
        raise AssertionError("dashboard recovery must not open live paper.duckdb")

    monkeypatch.setattr(paper_db_module, "PaperDB", fail_paper_db_open)

    paper_db_module._recover_deferred_sync_if_needed(allow_live_db_open=False)

    assert marker.exists()


def test_recent_orders_tolerates_legacy_schema_without_broker_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_paper.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE paper_orders (
                order_id VARCHAR,
                session_id VARCHAR,
                position_id VARCHAR,
                signal_id INT,
                symbol VARCHAR,
                side VARCHAR,
                order_type VARCHAR,
                requested_qty INT,
                request_price DOUBLE,
                fill_price DOUBLE,
                fill_qty INT,
                status VARCHAR,
                requested_at TIMESTAMPTZ,
                filled_at TIMESTAMPTZ,
                exchange_order_id VARCHAR,
                idempotency_key VARCHAR,
                broker_mode VARCHAR,
                broker_payload VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            )
        """)
        con.execute(
            """
            INSERT INTO paper_orders (
                order_id, session_id, symbol, side, order_type, requested_qty,
                status, exchange_order_id, broker_mode, broker_payload, created_at, updated_at
            ) VALUES ('ord-1', 'sess-1', 'ITC', 'BUY', 'LIMIT', 1, 'PENDING',
                'kite-1', 'LIVE', '{"tradingsymbol":"ITC"}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )
    finally:
        con.close()

    db = PaperDB(db_path=db_path, read_only=True)
    try:
        orders = db.get_recent_orders(limit=10, broker_only=True)
        assert len(orders) == 1
        assert orders[0].symbol == "ITC"
        assert orders[0].broker_latency_ms is None
        assert orders[0].broker_response is None
    finally:
        db.close()


def test_update_order_from_broker_snapshot_persists_final_status(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        db.append_order_event(
            session_id="sess-1",
            symbol="ITC",
            side="BUY",
            order_type="LIMIT",
            requested_qty=1,
            request_price=313.05,
            status="PENDING",
            exchange_order_id="kite-1",
            broker_mode="LIVE",
            broker_payload='{"tradingsymbol":"ITC"}',
        )

        changed = db.update_order_from_broker_snapshot(
            "kite-1",
            {
                "order_id": "kite-1",
                "exchange_order_id": "exchange-1",
                "status": "REJECTED",
                "status_message_raw": "17177 : Invalid PAN Number",
                "order_timestamp": "2026-05-04 14:11:35",
                "exchange_timestamp": "2026-05-04 14:11:35",
                "filled_quantity": 0,
                "average_price": 0,
            },
        )

        order = db.get_session_orders("sess-1")[0]
        assert changed == 1
        assert order.status == "REJECTED"
        assert order.broker_status_message == "17177 : Invalid PAN Number"
        assert order.broker_exchange_order_id == "exchange-1"
        assert "Invalid PAN Number" in str(order.broker_response)
    finally:
        db.close()


def test_cleanup_stale_sessions_cancels_old_planning_rows(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        planning = db.create_session(
            session_id="paper-planning-old",
            status="PLANNING",
        )
        db.con.execute(
            "UPDATE paper_sessions SET updated_at = CURRENT_TIMESTAMP - INTERVAL '2 days' "
            "WHERE session_id = ?",
            [planning.session_id],
        )

        changed = db.cleanup_stale_sessions()

        assert changed == 1
        assert db.get_session(planning.session_id).status == "CANCELLED"
        assert "auto-cancelled: stale session" in (db.get_session(planning.session_id).notes or "")
    finally:
        db.close()


def test_delete_all_rows_clears_every_paper_table(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session = db.create_session(session_id="paper-clean", status="ACTIVE")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            qty=10,
            entry_price=100.0,
        )
        db.append_order_event(session_id=session.session_id, symbol="SBIN", side="BUY")
        db.upsert_feed_state(session_id=session.session_id, status="LIVE")
        db.con.execute(
            """
            INSERT INTO alert_log (alert_type, alert_level, subject, body, channel, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ["TEST", "INFO", "subject", "body", "LOG", "sent"],
        )

        counts = db.delete_all_rows()

        assert counts["paper_sessions"] == 1
        assert counts["paper_positions"] == 1
        assert counts["paper_orders"] == 1
        assert counts["paper_feed_state"] == 1
        assert counts["alert_log"] == 1
        assert db.get_session(session.session_id) is None
        assert db.get_session_positions(session.session_id) == []
        assert db.get_session_orders(session.session_id) == []
        assert db.get_feed_state(session.session_id) is None
        assert db.get_status() == {
            "paper_sessions": 0,
            "paper_positions": 0,
            "paper_orders": 0,
            "paper_feed_state": 0,
            "paper_feed_audit": 0,
            "alert_log": 0,
        }
        assert position.session_id == session.session_id
    finally:
        db.close()


def test_open_position_allows_multiple_closed_rows_for_same_symbol(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session = db.create_session(session_id="paper-reentry", status="ACTIVE")
        first = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            qty=10,
            entry_price=100.0,
        )
        db.update_position(first.position_id, status="CLOSED", exit_price=101.0, pnl=10.0)
        second = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            qty=5,
            entry_price=102.0,
        )
        db.update_position(second.position_id, status="CLOSED", exit_price=103.0, pnl=5.0)

        closed = db.get_session_positions(session.session_id, statuses=["CLOSED"])

        assert [position.position_id for position in closed] == [
            first.position_id,
            second.position_id,
        ]
    finally:
        db.close()


def test_update_position_rejects_reopening_closed_position(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session = db.create_session(session_id="paper-transition", status="ACTIVE")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            qty=10,
            entry_price=100.0,
        )
        db.update_position(position.position_id, status="CLOSED", exit_price=101.0, pnl=10.0)

        with pytest.raises(RuntimeError, match="Invalid paper position status transition"):
            db.update_position(position.position_id, status="OPEN")
    finally:
        db.close()


def test_delete_sessions_by_trade_date_clears_feed_audit_rows(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session = db.create_session(
            session_id="paper-audit",
            status="ACTIVE",
            trade_date="2026-04-13",
        )
        db.upsert_feed_audit_rows(
            [
                {
                    "session_id": session.session_id,
                    "trade_date": "2026-04-13",
                    "feed_source": "kite",
                    "transport": "websocket",
                    "symbol": "MANOMAY",
                    "bar_start": "2026-04-13 09:20:00",
                    "bar_end": "2026-04-13 09:25:00",
                    "open": 221.0,
                    "high": 222.0,
                    "low": 220.5,
                    "close": 221.5,
                    "volume": 12345.0,
                    "first_snapshot_ts": "2026-04-13 09:20:01",
                    "last_snapshot_ts": "2026-04-13 09:24:59",
                }
            ]
        )

        counts = db.delete_sessions_by_trade_date("2026-04-13")

        assert counts["paper_sessions"] == 1
        assert counts["paper_feed_audit"] == 1
        assert db.get_feed_audit_rows(trade_date="2026-04-13") == []
    finally:
        db.close()


def test_cleanup_feed_audit_older_than_removes_only_expired_rows(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        now = datetime.now(UTC)
        old_ts = now - timedelta(days=10)
        recent_ts = now - timedelta(days=2)

        db.con.execute(
            """
            INSERT INTO paper_feed_audit (
                session_id, trade_date, feed_source, transport, symbol,
                bar_start, bar_end, open, high, low, close, volume,
                first_snapshot_ts, last_snapshot_ts, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sess-old",
                "2026-04-01",
                "kite",
                "websocket",
                "OLDSYM",
                old_ts,
                old_ts + timedelta(minutes=5),
                100.0,
                101.0,
                99.5,
                100.5,
                1234.0,
                old_ts,
                old_ts + timedelta(minutes=5),
                old_ts,
                old_ts,
            ],
        )
        db.con.execute(
            """
            INSERT INTO paper_feed_audit (
                session_id, trade_date, feed_source, transport, symbol,
                bar_start, bar_end, open, high, low, close, volume,
                first_snapshot_ts, last_snapshot_ts, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sess-new",
                "2026-04-17",
                "kite",
                "websocket",
                "NEWSYM",
                recent_ts,
                recent_ts + timedelta(minutes=5),
                200.0,
                201.0,
                199.5,
                200.5,
                5678.0,
                recent_ts,
                recent_ts + timedelta(minutes=5),
                recent_ts,
                recent_ts,
            ],
        )

        deleted = db.cleanup_feed_audit_older_than(7)

        assert deleted == 1
        rows = db.get_feed_audit_rows()
        assert len(rows) == 1
        assert rows[0].session_id == "sess-new"
    finally:
        db.close()


def test_cleanup_feed_audit_uses_bar_date_not_insert_date(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        now = datetime.now(UTC)
        old_bar_ts = now - timedelta(days=10)
        db.con.execute(
            """
            INSERT INTO paper_feed_audit (
                session_id, trade_date, feed_source, transport, symbol,
                bar_start, bar_end, open, high, low, close, volume,
                first_snapshot_ts, last_snapshot_ts, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sess-late",
                old_bar_ts.date().isoformat(),
                "local",
                "replay",
                "LATESYM",
                old_bar_ts,
                old_bar_ts + timedelta(minutes=5),
                100.0,
                101.0,
                99.5,
                100.5,
                1234.0,
                old_bar_ts,
                old_bar_ts + timedelta(minutes=5),
                now,
                now,
            ],
        )

        deleted = db.cleanup_feed_audit_older_than(7)

        assert deleted == 1
        assert db.get_feed_audit_rows() == []
    finally:
        db.close()
