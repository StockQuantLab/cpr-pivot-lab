"""Paper execution reconciliation checks.

These checks model the invariants a real broker reconciliation layer will need:
local positions must be explainable by order events, exits must not overfill, and
terminal sessions must not retain open positions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from db.paper_db import PaperDB, PaperOrder, PaperPosition


@dataclass(slots=True)
class ReconciliationFinding:
    severity: str
    code: str
    message: str
    session_id: str
    symbol: str | None = None
    position_id: str | None = None
    order_id: str | None = None


def _entry_side(position: PaperPosition) -> str:
    return "BUY" if str(position.direction).upper() == "LONG" else "SELL"


def _exit_side(position: PaperPosition) -> str:
    return "SELL" if str(position.direction).upper() == "LONG" else "BUY"


def _filled_orders(orders: list[PaperOrder]) -> list[PaperOrder]:
    return [o for o in orders if str(o.status).upper() == "FILLED"]


def reconcile_paper_session(db: PaperDB, session_id: str) -> dict[str, Any]:
    session = db.get_session(session_id)
    if session is None:
        finding = ReconciliationFinding(
            severity="CRITICAL",
            code="SESSION_MISSING",
            message=f"Session not found: {session_id}",
            session_id=session_id,
        )
        return {
            "ok": False,
            "session_id": session_id,
            "findings": [asdict(finding)],
            "summary": {"critical": 1, "warning": 0, "info": 0},
        }

    positions = db.get_session_positions(session_id)
    orders = db.get_session_orders(session_id)
    orders_by_position: dict[str, list[PaperOrder]] = {}
    findings: list[ReconciliationFinding] = []
    for order in orders:
        if order.position_id:
            orders_by_position.setdefault(str(order.position_id), []).append(order)

    terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED"}
    open_positions = [p for p in positions if str(p.status).upper() == "OPEN"]
    if str(session.status).upper() in terminal_statuses and open_positions:
        findings.append(
            ReconciliationFinding(
                severity="CRITICAL",
                code="TERMINAL_SESSION_HAS_OPEN_POSITIONS",
                message=(
                    f"Session {session_id} is {session.status} but has "
                    f"{len(open_positions)} open position(s)."
                ),
                session_id=session_id,
            )
        )

    known_position_ids = {p.position_id for p in positions}
    for order in orders:
        if order.position_id and str(order.position_id) not in known_position_ids:
            findings.append(
                ReconciliationFinding(
                    severity="WARNING",
                    code="ORDER_POSITION_MISSING",
                    message=f"Order {order.order_id} references missing position {order.position_id}.",
                    session_id=session_id,
                    symbol=order.symbol,
                    position_id=str(order.position_id),
                    order_id=order.order_id,
                )
            )

    for position in positions:
        filled = _filled_orders(orders_by_position.get(position.position_id, []))
        entry_fills = [
            o
            for o in filled
            if str(o.side).upper() == _entry_side(position)
            and not str(o.notes or "").lower().startswith("paper exit")
            and "flatten" not in str(o.notes or "").lower()
        ]
        exit_fills = [o for o in filled if str(o.side).upper() == _exit_side(position)]

        if not entry_fills:
            findings.append(
                ReconciliationFinding(
                    severity="CRITICAL",
                    code="POSITION_MISSING_ENTRY_ORDER",
                    message=f"Position {position.position_id} has no filled entry order.",
                    session_id=session_id,
                    symbol=position.symbol,
                    position_id=position.position_id,
                )
            )

        position_status = str(position.status).upper()
        if position_status in {"CLOSED", "FLATTENED"} and not exit_fills:
            findings.append(
                ReconciliationFinding(
                    severity="CRITICAL",
                    code="CLOSED_POSITION_MISSING_EXIT_ORDER",
                    message=f"Closed position {position.position_id} has no filled exit order.",
                    session_id=session_id,
                    symbol=position.symbol,
                    position_id=position.position_id,
                )
            )

        entry_qty = sum(float(o.fill_qty or 0.0) for o in entry_fills)
        exit_qty = sum(float(o.fill_qty or 0.0) for o in exit_fills)
        expected_qty = float(position.quantity or position.qty or 0.0)
        if entry_fills and entry_qty + 1e-9 < expected_qty:
            findings.append(
                ReconciliationFinding(
                    severity="WARNING",
                    code="ENTRY_UNDERFILLED",
                    message=(
                        f"Position {position.position_id} qty={expected_qty:g} but "
                        f"entry fills total {entry_qty:g}."
                    ),
                    session_id=session_id,
                    symbol=position.symbol,
                    position_id=position.position_id,
                )
            )
        if exit_qty > expected_qty + 1e-9:
            findings.append(
                ReconciliationFinding(
                    severity="CRITICAL",
                    code="EXIT_OVERFILLED",
                    message=(
                        f"Position {position.position_id} qty={expected_qty:g} but "
                        f"exit fills total {exit_qty:g}."
                    ),
                    session_id=session_id,
                    symbol=position.symbol,
                    position_id=position.position_id,
                )
            )
        if position_status == "OPEN" and exit_qty > 0:
            findings.append(
                ReconciliationFinding(
                    severity="CRITICAL",
                    code="OPEN_POSITION_HAS_EXIT_FILL",
                    message=f"Open position {position.position_id} already has exit fills.",
                    session_id=session_id,
                    symbol=position.symbol,
                    position_id=position.position_id,
                )
            )

    summary = {
        "critical": sum(1 for f in findings if f.severity == "CRITICAL"),
        "warning": sum(1 for f in findings if f.severity == "WARNING"),
        "info": sum(1 for f in findings if f.severity == "INFO"),
    }
    return {
        "ok": summary["critical"] == 0,
        "session_id": session_id,
        "session_status": session.status,
        "positions": len(positions),
        "orders": len(orders),
        "open_positions": len(open_positions),
        "summary": summary,
        "findings": [asdict(f) for f in findings],
    }
