from __future__ import annotations

from db.paper_db import PaperDB
from engine.broker_reconciliation import (
    BrokerOrderSnapshot,
    BrokerPositionSnapshot,
    PilotGuardrails,
    reconcile_local_to_broker,
)


def _create_session_with_open_position(db: PaperDB) -> tuple[str, str]:
    session = db.create_session(
        session_id="broker-recon-1",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        status="ACTIVE",
        trade_date="2026-04-28",
        direction="LONG",
    )
    position = db.open_position(
        session_id=session.session_id,
        symbol="SBIN",
        direction="LONG",
        entry_price=100.0,
        qty=5,
        signal_id=1,
    )
    db.append_order_event(
        session_id=session.session_id,
        position_id=position.position_id,
        signal_id=1,
        symbol="SBIN",
        side="BUY",
        requested_qty=5,
        fill_qty=5,
        status="FILLED",
        exchange_order_id="kite-1",
        notes="paper entry",
    )
    return session.session_id, position.position_id


def test_broker_reconcile_ok_when_order_and_position_match(tmp_path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session_id, _ = _create_session_with_open_position(db)

        payload = reconcile_local_to_broker(
            db=db,
            session_id=session_id,
            broker_orders=[
                BrokerOrderSnapshot(
                    order_id="kite-1",
                    symbol="SBIN",
                    side="BUY",
                    quantity=5,
                    filled_quantity=5,
                    status="COMPLETE",
                )
            ],
            broker_positions=[BrokerPositionSnapshot(symbol="SBIN", quantity=5)],
        )

        assert payload["ok"] is True
        assert payload["summary"]["critical"] == 0
    finally:
        db.close()


def test_broker_reconcile_flags_untracked_broker_position(tmp_path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session_id, _ = _create_session_with_open_position(db)

        payload = reconcile_local_to_broker(
            db=db,
            session_id=session_id,
            broker_orders=[
                BrokerOrderSnapshot(
                    order_id="kite-1",
                    symbol="SBIN",
                    side="BUY",
                    quantity=5,
                    filled_quantity=5,
                    status="COMPLETE",
                )
            ],
            broker_positions=[
                BrokerPositionSnapshot(symbol="SBIN", quantity=5),
                BrokerPositionSnapshot(symbol="RELIANCE", quantity=1),
            ],
        )

        assert payload["ok"] is False
        assert any(f["code"] == "UNTRACKED_BROKER_POSITION" for f in payload["findings"])
    finally:
        db.close()


def test_broker_reconcile_flags_missing_broker_order(tmp_path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session_id, _ = _create_session_with_open_position(db)

        payload = reconcile_local_to_broker(
            db=db,
            session_id=session_id,
            broker_orders=[],
            broker_positions=[BrokerPositionSnapshot(symbol="SBIN", quantity=5)],
        )

        assert payload["ok"] is False
        assert any(f["code"] == "BROKER_ORDER_MISSING" for f in payload["findings"])
    finally:
        db.close()


def test_pilot_guardrails_require_tiny_scope_and_acknowledgement() -> None:
    payload = PilotGuardrails().validate(
        symbols=["SBIN", "RELIANCE", "TCS"],
        order_quantity=2,
        estimated_notional=50_000,
        acknowledgement=None,
    )

    assert payload["ok"] is False
    codes = {f["code"] for f in payload["findings"]}
    assert "PILOT_TOO_MANY_SYMBOLS" in codes
    assert "PILOT_QUANTITY_TOO_HIGH" in codes
    assert "PILOT_NOTIONAL_TOO_HIGH" in codes
    assert "PILOT_ACK_MISSING" in codes
    assert payload["real_orders_enabled"] is False


def test_pilot_guardrails_allow_explicit_minimal_scope_but_do_not_enable_orders() -> None:
    payload = PilotGuardrails().validate(
        symbols=["SBIN"],
        order_quantity=1,
        estimated_notional=5000,
        acknowledgement="I_ACCEPT_REAL_ORDER_RISK",
    )

    assert payload["ok"] is True
    assert payload["real_orders_enabled"] is False
