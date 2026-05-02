"""Broker reconciliation and real-pilot guardrails.

This module is intentionally side-effect free. It compares local paper order/position
state against broker snapshots supplied by a caller. It does not call Kite and does
not place or cancel orders.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from db.paper_db import PaperDB, PaperOrder, PaperPosition


@dataclass(frozen=True, slots=True)
class BrokerOrderSnapshot:
    order_id: str
    symbol: str
    side: str
    quantity: float
    filled_quantity: float = 0.0
    status: str = "UNKNOWN"
    tag: str | None = None
    broker_payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> BrokerOrderSnapshot:
        return cls(
            order_id=str(
                data.get("order_id")
                or data.get("exchange_order_id")
                or data.get("order_id_external")
                or ""
            ),
            symbol=str(data.get("tradingsymbol") or data.get("symbol") or "").upper(),
            side=str(data.get("transaction_type") or data.get("side") or "").upper(),
            quantity=float(data.get("quantity") or data.get("requested_qty") or 0.0),
            filled_quantity=float(
                data.get("filled_quantity") or data.get("filled_qty") or data.get("fill_qty") or 0.0
            ),
            status=str(data.get("status") or "UNKNOWN").upper(),
            tag=data.get("tag"),
            broker_payload=dict(data),
        )


@dataclass(frozen=True, slots=True)
class BrokerPositionSnapshot:
    symbol: str
    quantity: float
    product: str = "MIS"
    exchange: str = "NSE"
    average_price: float | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> BrokerPositionSnapshot:
        return cls(
            symbol=str(data.get("tradingsymbol") or data.get("symbol") or "").upper(),
            quantity=float(data.get("quantity") or data.get("net_quantity") or 0.0),
            product=str(data.get("product") or "MIS").upper(),
            exchange=str(data.get("exchange") or "NSE").upper(),
            average_price=(
                float(data["average_price"]) if data.get("average_price") is not None else None
            ),
        )


@dataclass(frozen=True, slots=True)
class BrokerReconciliationFinding:
    severity: str
    code: str
    message: str
    session_id: str
    symbol: str | None = None
    local_order_id: str | None = None
    broker_order_id: str | None = None
    position_id: str | None = None


@dataclass(frozen=True, slots=True)
class PilotGuardrails:
    max_symbols: int = 2
    max_order_quantity: int = 1
    max_notional: float = 10_000.0
    allowed_products: tuple[str, ...] = ("MIS",)
    allowed_order_types: tuple[str, ...] = ("LIMIT",)

    def validate(
        self,
        *,
        symbols: list[str],
        order_quantity: int,
        estimated_notional: float,
        product: str = "MIS",
        order_type: str = "LIMIT",
        acknowledgement: str | None = None,
    ) -> dict[str, Any]:
        findings: list[BrokerReconciliationFinding] = []
        clean_symbols = sorted({s.strip().upper() for s in symbols if s.strip()})
        session_id = "pilot_guard"
        if len(clean_symbols) == 0:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_SYMBOLS_EMPTY",
                    message="Pilot requires at least one symbol.",
                    session_id=session_id,
                )
            )
        if len(clean_symbols) > self.max_symbols:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_TOO_MANY_SYMBOLS",
                    message=f"Pilot allows at most {self.max_symbols} symbols.",
                    session_id=session_id,
                )
            )
        if int(order_quantity) > self.max_order_quantity:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_QUANTITY_TOO_HIGH",
                    message=f"Pilot order quantity cap is {self.max_order_quantity}.",
                    session_id=session_id,
                )
            )
        if float(estimated_notional) > self.max_notional:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_NOTIONAL_TOO_HIGH",
                    message=f"Pilot notional cap is {self.max_notional:g}.",
                    session_id=session_id,
                )
            )
        if product.upper() not in self.allowed_products:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_PRODUCT_NOT_ALLOWED",
                    message=f"Pilot product must be one of {self.allowed_products}.",
                    session_id=session_id,
                )
            )
        if order_type.upper() not in self.allowed_order_types:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_ORDER_TYPE_NOT_ALLOWED",
                    message=f"Pilot order type must be one of {self.allowed_order_types}.",
                    session_id=session_id,
                )
            )
        if acknowledgement != "I_ACCEPT_REAL_ORDER_RISK":
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="PILOT_ACK_MISSING",
                    message="Pilot requires acknowledgement I_ACCEPT_REAL_ORDER_RISK.",
                    session_id=session_id,
                )
            )

        return _payload(
            session_id=session_id,
            findings=findings,
            extra={
                "symbols": clean_symbols,
                "max_symbols": self.max_symbols,
                "max_order_quantity": self.max_order_quantity,
                "max_notional": self.max_notional,
                "real_orders_enabled": False,
            },
        )


def reconcile_local_to_broker(
    *,
    db: PaperDB,
    session_id: str,
    broker_orders: list[BrokerOrderSnapshot],
    broker_positions: list[BrokerPositionSnapshot],
) -> dict[str, Any]:
    session = db.get_session(session_id)
    local_orders = db.get_session_orders(session_id)
    local_positions = db.get_session_positions(session_id)
    findings: list[BrokerReconciliationFinding] = []

    if session is None:
        findings.append(
            BrokerReconciliationFinding(
                severity="CRITICAL",
                code="SESSION_MISSING",
                message=f"Local session not found: {session_id}",
                session_id=session_id,
            )
        )
        return _payload(session_id=session_id, findings=findings)

    broker_by_id = {o.order_id: o for o in broker_orders if o.order_id}
    broker_positions_by_symbol = {
        p.symbol: p for p in broker_positions if p.symbol and abs(p.quantity) > 1e-9
    }

    for local in local_orders:
        _check_local_order(
            session_id=session_id,
            local=local,
            broker_by_id=broker_by_id,
            findings=findings,
        )

    expected_open_by_symbol = _expected_open_quantities(local_positions)
    for symbol, expected_qty in expected_open_by_symbol.items():
        broker_position = broker_positions_by_symbol.get(symbol)
        if broker_position is None:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="BROKER_POSITION_MISSING",
                    message=f"Local open quantity {expected_qty:g} for {symbol}, but broker has none.",
                    session_id=session_id,
                    symbol=symbol,
                )
            )
            continue
        if abs(abs(broker_position.quantity) - abs(expected_qty)) > 1e-9:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="BROKER_POSITION_QTY_MISMATCH",
                    message=(
                        f"Local open quantity {expected_qty:g} for {symbol}, "
                        f"broker quantity {broker_position.quantity:g}."
                    ),
                    session_id=session_id,
                    symbol=symbol,
                )
            )

    for symbol, broker_position in broker_positions_by_symbol.items():
        if symbol not in expected_open_by_symbol:
            findings.append(
                BrokerReconciliationFinding(
                    severity="CRITICAL",
                    code="UNTRACKED_BROKER_POSITION",
                    message=(
                        f"Broker has position {symbol} quantity {broker_position.quantity:g} "
                        "with no local open position."
                    ),
                    session_id=session_id,
                    symbol=symbol,
                )
            )

    return _payload(
        session_id=session_id,
        findings=findings,
        extra={
            "local_orders": len(local_orders),
            "local_positions": len(local_positions),
            "broker_orders": len(broker_orders),
            "broker_positions": len(broker_positions),
        },
    )


def _check_local_order(
    *,
    session_id: str,
    local: PaperOrder,
    broker_by_id: dict[str, BrokerOrderSnapshot],
    findings: list[BrokerReconciliationFinding],
) -> None:
    broker_id = str(local.exchange_order_id or "")
    if not broker_id:
        if str(local.broker_mode or "").upper() == "REAL_DRY_RUN":
            return
        findings.append(
            BrokerReconciliationFinding(
                severity="WARNING",
                code="LOCAL_ORDER_WITHOUT_BROKER_ID",
                message=f"Local order {local.order_id} has no exchange_order_id.",
                session_id=session_id,
                symbol=local.symbol,
                local_order_id=local.order_id,
            )
        )
        return

    broker = broker_by_id.get(broker_id)
    if broker is None:
        severity = "INFO" if broker_id.startswith("dryrun-") else "CRITICAL"
        findings.append(
            BrokerReconciliationFinding(
                severity=severity,
                code="BROKER_ORDER_MISSING",
                message=f"Local order {local.order_id} broker id {broker_id} not in broker snapshot.",
                session_id=session_id,
                symbol=local.symbol,
                local_order_id=local.order_id,
                broker_order_id=broker_id,
            )
        )
        return

    if broker.symbol != str(local.symbol).upper():
        findings.append(
            BrokerReconciliationFinding(
                severity="CRITICAL",
                code="BROKER_ORDER_SYMBOL_MISMATCH",
                message=f"Local order {local.order_id} symbol {local.symbol}, broker {broker.symbol}.",
                session_id=session_id,
                symbol=local.symbol,
                local_order_id=local.order_id,
                broker_order_id=broker.order_id,
            )
        )
    if broker.side != str(local.side).upper():
        findings.append(
            BrokerReconciliationFinding(
                severity="CRITICAL",
                code="BROKER_ORDER_SIDE_MISMATCH",
                message=f"Local order {local.order_id} side {local.side}, broker {broker.side}.",
                session_id=session_id,
                symbol=local.symbol,
                local_order_id=local.order_id,
                broker_order_id=broker.order_id,
            )
        )

    local_qty = float(local.fill_qty or local.requested_qty or 0.0)
    broker_qty = float(broker.filled_quantity or broker.quantity or 0.0)
    if str(local.status).upper() == "FILLED" and abs(local_qty - broker_qty) > 1e-9:
        findings.append(
            BrokerReconciliationFinding(
                severity="CRITICAL",
                code="BROKER_ORDER_QTY_MISMATCH",
                message=f"Local order {local.order_id} qty {local_qty:g}, broker {broker_qty:g}.",
                session_id=session_id,
                symbol=local.symbol,
                local_order_id=local.order_id,
                broker_order_id=broker.order_id,
            )
        )


def _expected_open_quantities(positions: list[PaperPosition]) -> dict[str, float]:
    quantities: dict[str, float] = {}
    for position in positions:
        if str(position.status).upper() != "OPEN":
            continue
        qty = float(position.current_qty or position.quantity or position.qty or 0.0)
        signed_qty = qty if str(position.direction).upper() == "LONG" else -qty
        quantities[str(position.symbol).upper()] = (
            quantities.get(str(position.symbol).upper(), 0.0) + signed_qty
        )
    return quantities


def _payload(
    *,
    session_id: str,
    findings: list[BrokerReconciliationFinding],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "critical": sum(1 for f in findings if f.severity == "CRITICAL"),
        "warning": sum(1 for f in findings if f.severity == "WARNING"),
        "info": sum(1 for f in findings if f.severity == "INFO"),
    }
    payload: dict[str, Any] = {
        "ok": summary["critical"] == 0,
        "session_id": session_id,
        "summary": summary,
        "findings": [asdict(f) for f in findings],
    }
    if extra:
        payload.update(extra)
    return payload
