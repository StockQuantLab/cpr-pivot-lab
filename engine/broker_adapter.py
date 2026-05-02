"""Broker adapter contracts for dry-run and future real-order execution."""

from __future__ import annotations

import json
import math
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


class OrderSafetyError(ValueError):
    """Raised when a broker order intent violates real-money safety guards."""


DEFAULT_EXIT_MAX_SLIPPAGE_PCT = 2.0
DEFAULT_MAX_QUOTE_AGE_SEC = 5.0
DEFAULT_MARKET_PROTECTION_PCT = 2.0
DEFAULT_TICK_SIZE = 0.01
_PROTECTED_EXIT_ROLES = (
    "exit",
    "close",
    "flatten",
    "manual_flatten",
    "emergency",
    "kill",
    "stop",
)


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
    trigger_price: float | None = None
    reference_price: float | None = None
    reference_price_age_sec: float | None = None
    max_slippage_pct: float = DEFAULT_EXIT_MAX_SLIPPAGE_PCT
    market_protection: float | None = DEFAULT_MARKET_PROTECTION_PCT
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
        if order_type in {"SL", "SL-M"} and self.trigger_price is None:
            raise ValueError(f"{order_type} orders require trigger_price")
        _require_positive_finite(self.price, "price", allow_none=True)
        _require_positive_finite(self.trigger_price, "trigger_price", allow_none=True)
        _require_positive_finite(self.reference_price, "reference_price", allow_none=True)
        _require_non_negative_finite(
            self.reference_price_age_sec,
            "reference_price_age_sec",
            allow_none=True,
        )
        _require_positive_finite(self.max_slippage_pct, "max_slippage_pct", allow_none=False)
        if self.market_protection is not None:
            _require_non_negative_finite(
                self.market_protection,
                "market_protection",
                allow_none=False,
            )
            if self.market_protection > 100:
                raise ValueError("market_protection must be <= 100")
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
        if intent.trigger_price is not None:
            payload["trigger_price"] = float(intent.trigger_price)
        if intent.order_type in {"MARKET", "SL-M"} and intent.market_protection is not None:
            payload["market_protection"] = float(intent.market_protection)
        return payload

    def validate_for_broker(self) -> BrokerOrderIntent:
        intent = self.normalized()
        if intent.order_type in {"MARKET", "SL-M"} and (
            intent.market_protection is None or float(intent.market_protection) <= 0
        ):
            raise OrderSafetyError("MARKET and SL-M orders require market_protection > 0")

        if _is_protected_exit_role(intent.role):
            _validate_protected_exit(intent)
        return intent


def build_protected_flatten_intent(
    *,
    session_id: str,
    symbol: str,
    side: str,
    quantity: int,
    latest_price: float,
    quote_age_sec: float,
    role: str = "manual_flatten",
    position_id: str | None = None,
    signal_id: int | None = None,
    product: str = "MIS",
    exchange: str = "NSE",
    max_slippage_pct: float = DEFAULT_EXIT_MAX_SLIPPAGE_PCT,
    tick_size: float = DEFAULT_TICK_SIZE,
    event_time: str | None = None,
) -> BrokerOrderIntent:
    """Build a bounded marketable LIMIT order for emergency/manual exits."""
    _require_positive_finite(latest_price, "latest_price", allow_none=False)
    _require_positive_finite(tick_size, "tick_size", allow_none=False)
    side_upper = side.strip().upper()
    if side_upper == "SELL":
        raw_price = latest_price * (1.0 - max_slippage_pct / 100.0)
        price = math.floor(raw_price / tick_size) * tick_size
    elif side_upper == "BUY":
        raw_price = latest_price * (1.0 + max_slippage_pct / 100.0)
        price = math.ceil(raw_price / tick_size) * tick_size
    else:
        raise ValueError("side must be BUY or SELL")

    return BrokerOrderIntent(
        session_id=session_id,
        symbol=symbol,
        side=side_upper,
        quantity=quantity,
        role=role,
        position_id=position_id,
        signal_id=signal_id,
        order_type="LIMIT",
        price=round(price, 4),
        reference_price=latest_price,
        reference_price_age_sec=quote_age_sec,
        max_slippage_pct=max_slippage_pct,
        product=product,
        exchange=exchange,
        event_time=event_time,
    )


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
        safe_intent = intent.validate_for_broker()
        payload = safe_intent.zerodha_payload()
        idempotency_key = safe_intent.idempotency_key()
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


def _is_protected_exit_role(role: str) -> bool:
    normalized = role.strip().lower().replace("-", "_")
    return any(token in normalized for token in _PROTECTED_EXIT_ROLES)


def _validate_protected_exit(intent: BrokerOrderIntent) -> None:
    if intent.order_type != "LIMIT":
        raise OrderSafetyError("exit/flatten roles must use protected LIMIT orders")
    if intent.reference_price is None:
        raise OrderSafetyError("exit/flatten roles require a fresh reference_price")
    if intent.reference_price_age_sec is None:
        raise OrderSafetyError("exit/flatten roles require reference_price_age_sec")
    if intent.reference_price_age_sec > DEFAULT_MAX_QUOTE_AGE_SEC:
        raise OrderSafetyError(
            f"reference price is stale: {intent.reference_price_age_sec:.2f}s > "
            f"{DEFAULT_MAX_QUOTE_AGE_SEC:.2f}s"
        )
    if intent.price is None:
        raise OrderSafetyError("protected exit LIMIT order requires price")

    ref = float(intent.reference_price)
    price = float(intent.price)
    max_slip = float(intent.max_slippage_pct) / 100.0
    if intent.side == "SELL":
        min_price = ref * (1.0 - max_slip)
        if price < min_price:
            raise OrderSafetyError(
                f"SELL exit limit price {price:.4f} is below protected floor {min_price:.4f}"
            )
    else:
        max_price = ref * (1.0 + max_slip)
        if price > max_price:
            raise OrderSafetyError(
                f"BUY exit limit price {price:.4f} is above protected cap {max_price:.4f}"
            )


def _require_positive_finite(value: float | None, field: str, *, allow_none: bool) -> None:
    if value is None:
        if allow_none:
            return
        raise ValueError(f"{field} is required")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0:
        raise ValueError(f"{field} must be a positive finite number")


def _require_non_negative_finite(value: float | None, field: str, *, allow_none: bool) -> None:
    if value is None:
        if allow_none:
            return
        raise ValueError(f"{field} is required")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"{field} must be a non-negative finite number")
