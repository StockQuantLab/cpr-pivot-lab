"""Broker adapter contracts for dry-run and future real-order execution."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from engine.broker_reconciliation import BrokerOrderSnapshot, BrokerPositionSnapshot
from engine.execution_safety import (
    OrderRateGovernor,
    build_order_idempotency_key,
    get_default_order_governor,
)


class RealOrderPlacementDisabledError(RuntimeError):
    """Raised when a code path attempts real broker placement before it is enabled."""


@dataclass(frozen=True, slots=True)
class BrokerOrderIntent:
    session_id: str
    symbol: str
    side: str
    quantity: int
    role: str = "manual"
    position_id: str | None = None
    signal_id: int | None = None
    order_type: str = "MARKET"
    price: float | None = None
    product: str = "MIS"
    exchange: str = "NSE"
    variety: str = "regular"
    validity: str = "DAY"
    tag: str | None = None
    event_time: str | None = None

    def normalized(self) -> BrokerOrderIntent:
        symbol = self.symbol.strip().upper()
        side = self.side.strip().upper()
        order_type = self.order_type.strip().upper()
        if not symbol:
            raise ValueError("symbol is required")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if int(self.quantity) <= 0:
            raise ValueError("quantity must be positive")
        if order_type not in {"MARKET", "LIMIT", "SL", "SL-M"}:
            raise ValueError("unsupported order_type")
        if order_type in {"LIMIT", "SL"} and self.price is None:
            raise ValueError(f"{order_type} orders require price")
        return BrokerOrderIntent(
            **{
                **asdict(self),
                "symbol": symbol,
                "side": side,
                "quantity": int(self.quantity),
                "order_type": order_type,
                "product": self.product.strip().upper(),
                "exchange": self.exchange.strip().upper(),
                "variety": self.variety.strip().lower(),
                "validity": self.validity.strip().upper(),
            }
        )

    def idempotency_key(self) -> str:
        intent = self.normalized()
        return build_order_idempotency_key(
            session_id=intent.session_id,
            role=intent.role,
            symbol=intent.symbol,
            side=intent.side,
            position_id=intent.position_id,
            signal_id=intent.signal_id,
            event_time=intent.event_time,
        )

    def zerodha_payload(self) -> dict[str, Any]:
        intent = self.normalized()
        tag = intent.tag or _default_zerodha_tag(intent.session_id, intent.role)
        payload: dict[str, Any] = {
            "variety": intent.variety,
            "exchange": intent.exchange,
            "tradingsymbol": intent.symbol,
            "transaction_type": intent.side,
            "quantity": intent.quantity,
            "product": intent.product,
            "order_type": intent.order_type,
            "validity": intent.validity,
            "tag": tag,
        }
        if intent.price is not None:
            payload["price"] = float(intent.price)
        return payload


@dataclass(frozen=True, slots=True)
class BrokerExecutionResult:
    broker: str
    mode: str
    status: str
    payload: dict[str, Any]
    idempotency_key: str
    exchange_order_id: str | None = None


class BrokerAdapter(Protocol):
    mode: str

    async def place_order(self, intent: BrokerOrderIntent) -> BrokerExecutionResult:
        """Place or simulate one order intent."""

    async def fetch_order_snapshots(self) -> list[BrokerOrderSnapshot]:
        """Read broker order snapshots."""

    async def fetch_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        """Read broker position snapshots."""


class PaperBrokerAdapter:
    mode = "PAPER"

    async def place_order(self, intent: BrokerOrderIntent) -> BrokerExecutionResult:
        payload = intent.zerodha_payload()
        return BrokerExecutionResult(
            broker="paper",
            mode=self.mode,
            status="FILLED",
            payload=payload,
            idempotency_key=intent.idempotency_key(),
            exchange_order_id=None,
        )

    async def fetch_order_snapshots(self) -> list[BrokerOrderSnapshot]:
        return []

    async def fetch_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        return []


class ZerodhaBrokerAdapter:
    """Zerodha payload builder.

    `REAL_DRY_RUN` intentionally records payloads but never calls Kite `place_order`.
    Real placement is blocked until a future feature explicitly adds the required
    environment and CLI guards.
    """

    def __init__(
        self,
        *,
        mode: str = "REAL_DRY_RUN",
        governor: OrderRateGovernor | None = None,
        allow_real_orders: bool = False,
        kite_client: Any | None = None,
    ) -> None:
        self.mode = mode.upper()
        self._governor = governor or get_default_order_governor()
        self._allow_real_orders = bool(allow_real_orders)
        self._kite_client = kite_client

    async def place_order(self, intent: BrokerOrderIntent) -> BrokerExecutionResult:
        await self._governor.acquire()
        payload = intent.zerodha_payload()
        idempotency_key = intent.idempotency_key()
        if self.mode == "REAL_DRY_RUN":
            return BrokerExecutionResult(
                broker="zerodha",
                mode=self.mode,
                status="DRY_RUN",
                payload=payload,
                idempotency_key=idempotency_key,
                exchange_order_id=f"dryrun-{_short_key(idempotency_key)}",
            )

        if not self._allow_real_orders:
            raise RealOrderPlacementDisabledError(
                "Real Zerodha order placement is disabled. Use REAL_DRY_RUN."
            )
        raise RealOrderPlacementDisabledError(
            "Real Zerodha order placement is not implemented in this safety phase."
        )

    async def fetch_order_snapshots(self) -> list[BrokerOrderSnapshot]:
        if self._kite_client is None:
            return []
        orders = self._kite_client.orders()
        return [BrokerOrderSnapshot.from_mapping(dict(order)) for order in orders or []]

    async def fetch_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        if self._kite_client is None:
            return []
        positions = self._kite_client.positions()
        day_positions = []
        if isinstance(positions, dict):
            day_positions = list(positions.get("day") or positions.get("net") or [])
        elif isinstance(positions, list):
            day_positions = positions
        return [
            BrokerPositionSnapshot.from_mapping(dict(position))
            for position in day_positions
            if abs(float(position.get("quantity") or position.get("net_quantity") or 0.0)) > 1e-9
        ]


async def record_real_dry_run_order(
    *,
    paper_db: Any,
    intent: BrokerOrderIntent,
    adapter: ZerodhaBrokerAdapter | None = None,
) -> dict[str, Any]:
    adapter = adapter or ZerodhaBrokerAdapter(mode="REAL_DRY_RUN")
    result = await adapter.place_order(intent)
    payload_json = json.dumps(result.payload, sort_keys=True, separators=(",", ":"))
    normalized = intent.normalized()
    order_id = paper_db.append_order_event(
        session_id=normalized.session_id,
        position_id=normalized.position_id,
        signal_id=normalized.signal_id,
        symbol=normalized.symbol,
        side=normalized.side,
        order_type=normalized.order_type,
        requested_qty=normalized.quantity,
        request_price=normalized.price,
        fill_price=None,
        fill_qty=0,
        status="PENDING",
        requested_at=datetime.now(UTC),
        exchange_order_id=result.exchange_order_id,
        idempotency_key=result.idempotency_key,
        notes="REAL_DRY_RUN",
        broker_mode=result.mode,
        broker_payload=payload_json,
    )
    return {
        "order_id": order_id,
        "broker": result.broker,
        "mode": result.mode,
        "status": result.status,
        "idempotency_key": result.idempotency_key,
        "exchange_order_id": result.exchange_order_id,
        "payload": result.payload,
    }


def _default_zerodha_tag(session_id: str, role: str) -> str:
    raw = f"cpr-{role}-{session_id}".replace("_", "-")
    safe = "".join(ch for ch in raw.lower() if ch.isalnum() or ch == "-")
    return safe[:20] or "cpr-dry-run"


def _short_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())[:18] or "order"
