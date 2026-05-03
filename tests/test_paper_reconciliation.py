from __future__ import annotations

from types import SimpleNamespace

import pytest

from db.paper_db import PaperDB
from engine.paper_reconciliation import reconcile_paper_session


def _make_db(tmp_path) -> PaperDB:
    return PaperDB(db_path=tmp_path / "paper.duckdb")


def test_reconcile_clean_open_position(tmp_path) -> None:
    db = _make_db(tmp_path)
    try:
        session = db.create_session(session_id="paper-live-1", status="ACTIVE")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            quantity=10,
            entry_price=100.0,
        )
        db.append_order_event(
            session_id=session.session_id,
            position_id=position.position_id,
            symbol="SBIN",
            side="BUY",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            notes="paper entry",
        )

        payload = reconcile_paper_session(db, session.session_id)

        assert payload["ok"] is True
        assert payload["summary"]["critical"] == 0
        assert payload["findings"] == []
    finally:
        db.close()


def test_reconcile_closed_position_missing_exit_order(tmp_path) -> None:
    db = _make_db(tmp_path)
    try:
        session = db.create_session(session_id="paper-live-1", status="ACTIVE")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            quantity=10,
            entry_price=100.0,
        )
        db.append_order_event(
            session_id=session.session_id,
            position_id=position.position_id,
            symbol="SBIN",
            side="BUY",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            notes="paper entry",
        )
        db.update_position(
            position.position_id,
            status="CLOSED",
            close_price=101.0,
            realized_pnl=10.0,
            exit_reason="MANUAL_CLOSE",
        )

        payload = reconcile_paper_session(db, session.session_id)

        assert payload["ok"] is False
        assert any(f["code"] == "CLOSED_POSITION_MISSING_EXIT_ORDER" for f in payload["findings"])
    finally:
        db.close()


def test_reconcile_closed_position_underfilled_exit_order(tmp_path) -> None:
    db = _make_db(tmp_path)
    try:
        session = db.create_session(session_id="paper-live-1", status="ACTIVE")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="LONG",
            quantity=10,
            entry_price=100.0,
        )
        db.append_order_event(
            session_id=session.session_id,
            position_id=position.position_id,
            symbol="SBIN",
            side="BUY",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            notes="paper entry",
        )
        db.append_order_event(
            session_id=session.session_id,
            position_id=position.position_id,
            symbol="SBIN",
            side="SELL",
            requested_qty=10,
            fill_qty=4,
            status="FILLED",
            notes="paper exit underfilled",
        )
        db.update_position(
            position.position_id,
            status="CLOSED",
            close_price=101.0,
            realized_pnl=10.0,
            exit_reason="MANUAL_CLOSE",
        )

        payload = reconcile_paper_session(db, session.session_id)

        assert payload["ok"] is False
        assert any(f["code"] == "EXIT_UNDERFILLED" for f in payload["findings"])
    finally:
        db.close()


def test_reconcile_terminal_session_with_open_position(tmp_path) -> None:
    db = _make_db(tmp_path)
    try:
        session = db.create_session(session_id="paper-live-1", status="COMPLETED")
        position = db.open_position(
            session_id=session.session_id,
            symbol="SBIN",
            direction="SHORT",
            quantity=10,
            entry_price=100.0,
        )
        db.append_order_event(
            session_id=session.session_id,
            position_id=position.position_id,
            symbol="SBIN",
            side="SELL",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            notes="paper entry",
        )

        payload = reconcile_paper_session(db, session.session_id)

        assert payload["ok"] is False
        assert any(f["code"] == "TERMINAL_SESSION_HAS_OPEN_POSITIONS" for f in payload["findings"])
    finally:
        db.close()


@pytest.mark.asyncio
async def test_cmd_reconcile_strict_exits_nonzero(monkeypatch, capsys) -> None:
    import scripts.paper_trading as pt

    monkeypatch.setattr(
        pt,
        "reconcile_paper_session",
        lambda db, session_id: {
            "ok": False,
            "session_id": session_id,
            "findings": [{"severity": "CRITICAL", "code": "BROKEN"}],
        },
    )
    monkeypatch.setattr(pt, "_pdb", lambda: object())

    with pytest.raises(SystemExit):
        await pt._cmd_reconcile(SimpleNamespace(session_id="paper-live-1", strict=True))

    assert '"ok": false' in capsys.readouterr().out
