from __future__ import annotations

from pathlib import Path

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
            "alert_log": 0,
        }
        assert position.session_id == session.session_id
    finally:
        db.close()
