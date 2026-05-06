"""Broker adapter contracts for dry-run and future real-order execution."""

from __future__ import annotations

import json
import math
import os
import time
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
REAL_ORDER_ENABLE_ENV = "CPR_ZERODHA_REAL_ORDERS_ENABLED"
REAL_ORDER_ACK_ENV = "CPR_ZERODHA_REAL_ORDER_ACK"
REAL_ORDER_ACK_VALUE = "I_UNDERSTAND_REAL_MONEY_ORDERS"
REAL_ORDER_MAX_QTY_ENV = "CPR_ZERODHA_REAL_MAX_QTY"
REAL_ORDER_MAX_NOTIONAL_ENV = "CPR_ZERODHA_REAL_MAX_NOTIONAL"
REAL_ORDER_ALLOWED_PRODUCTS_ENV = "CPR_ZERODHA_REAL_ALLOWED_PRODUCTS"
REAL_ORDER_ALLOWED_ORDER_TYPES_ENV = "CPR_ZERODHA_REAL_ALLOWED_ORDER_TYPES"
REAL_ORDER_DEFAULT_ALLOWED_PRODUCTS = frozenset({"MIS"})
REAL_ORDER_DEFAULT_ALLOWED_ORDER_TYPES = frozenset({"LIMIT", "SL", "SL-M"})
_PROTECTED_EXIT_ROLES = (
    "exit",
    "close",
    "flatten",
    "manual_flatten",
    "emergency",
    "kill",
    "stop",
)
_EMERGENCY_FLATTEN_ROLE_TOKENS = ("flatten", "manual_flatten", "emergency", "kill")
_STOP_LOSS_ROLE_TOKENS = ("stop", "stop_loss", "protective_sl", "sl_hit", "slm")


def _role_matches_any(role: str, tokens: tuple[str, ...]) -> bool:
    normalized = role.strip().lower().replace("-", "_")
    return any(token in normalized for token in tokens)


@dataclass(frozen=True, slots=True)
class RealOrderGuardConfig:
    enabled: bool = False
    acknowledgement: str = ""
    max_quantity: int = 1
    max_notional: float = 10_000.0
    allowed_products: frozenset[str] = REAL_ORDER_DEFAULT_ALLOWED_PRODUCTS
    allowed_order_types: frozenset[str] = REAL_ORDER_DEFAULT_ALLOWED_ORDER_TYPES

    @classmethod
    def from_env(cls) -> RealOrderGuardConfig:
        return cls(
            enabled=_env_flag(REAL_ORDER_ENABLE_ENV),
            acknowledgement=str(os.getenv(REAL_ORDER_ACK_ENV, "")).strip(),
            max_quantity=_env_int(REAL_ORDER_MAX_QTY_ENV, default=1),
            max_notional=_env_float(REAL_ORDER_MAX_NOTIONAL_ENV, default=10_000.0),
            allowed_products=_env_set(
                REAL_ORDER_ALLOWED_PRODUCTS_ENV,
                default=REAL_ORDER_DEFAULT_ALLOWED_PRODUCTS,
            ),
            allowed_order_types=_env_set(
                REAL_ORDER_ALLOWED_ORDER_TYPES_ENV,
                default=REAL_ORDER_DEFAULT_ALLOWED_ORDER_TYPES,
            ),
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
        price = _ceil_to_tick(raw_price, tick_size)
    elif side_upper == "BUY":
        raw_price = latest_price * (1.0 + max_slippage_pct / 100.0)
        price = _floor_to_tick(raw_price, tick_size)
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
    latency_ms: float | None = None


class BrokerAdapter(Protocol):
    mode: str

    async def place_order(self, intent: BrokerOrderIntent) -> BrokerExecutionResult:
        """Place or simulate one order intent."""

    async def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        """Cancel one broker order."""

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

    async def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        return str(order_id)

    async def fetch_order_snapshots(self) -> list[BrokerOrderSnapshot]:
        return []

    async def fetch_position_snapshots(self) -> list[BrokerPositionSnapshot]:
        return []


class ZerodhaBrokerAdapter:
    """Zerodha payload builder.

    `REAL_DRY_RUN` intentionally records payloads but never calls Kite `place_order`.
    Real placement requires explicit code and environment gates.
    """

    def __init__(
        self,
        *,
        mode: str = "REAL_DRY_RUN",
        governor: OrderRateGovernor | None = None,
        allow_real_orders: bool = False,
        kite_client: Any | None = None,
        guard_config: RealOrderGuardConfig | None = None,
    ) -> None:
        self.mode = mode.upper()
        self._governor = governor or get_default_order_governor()
        self._allow_real_orders = bool(allow_real_orders)
        self._kite_client = kite_client
        self._guard_config = guard_config

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
                latency_ms=0.0,
            )

        if not self._allow_real_orders:
            raise RealOrderPlacementDisabledError(
                "Real Zerodha order placement is disabled. Use REAL_DRY_RUN."
            )
        guard = self._guard_config or RealOrderGuardConfig.from_env()
        _validate_real_order_gate(
            intent=safe_intent,
            payload=payload,
            guard=guard,
            kite_client=self._kite_client,
        )
        started = time.perf_counter()
        order_id = _call_kite_place_order(self._kite_client, payload)
        latency_ms = (time.perf_counter() - started) * 1000.0
        return BrokerExecutionResult(
            broker="zerodha",
            mode=self.mode,
            status="PLACED",
            payload=payload,
            idempotency_key=idempotency_key,
            exchange_order_id=str(order_id),
            latency_ms=round(latency_ms, 3),
        )

    async def fetch_order_snapshots(self) -> list[BrokerOrderSnapshot]:
        if self._kite_client is None:
            return []
        orders = self._kite_client.orders()
        return [BrokerOrderSnapshot.from_mapping(dict(order)) for order in orders or []]

    async def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        if self.mode == "REAL_DRY_RUN":
            return str(order_id)
        if not self._allow_real_orders:
            raise RealOrderPlacementDisabledError(
                "Real Zerodha order cancellation is disabled. Use REAL_DRY_RUN."
            )
        if self._kite_client is None or not callable(
            getattr(self._kite_client, "cancel_order", None)
        ):
            raise RealOrderPlacementDisabledError("A Kite client with cancel_order is required.")
        return str(
            self._kite_client.cancel_order(
                variety=str(variety or "regular"),
                order_id=str(order_id),
            )
        )

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
        broker_latency_ms=result.latency_ms,
    )
    return {
        "order_id": order_id,
        "broker": result.broker,
        "mode": result.mode,
        "status": result.status,
        "idempotency_key": result.idempotency_key,
        "exchange_order_id": result.exchange_order_id,
        "broker_latency_ms": result.latency_ms,
        "payload": result.payload,
    }


async def record_real_order(
    *,
    paper_db: Any,
    intent: BrokerOrderIntent,
    adapter: ZerodhaBrokerAdapter,
) -> dict[str, Any]:
    safe_intent = intent.validate_for_broker()
    normalized = safe_intent.normalized()
    idempotency_key = normalized.idempotency_key()
    existing = (
        paper_db.get_order_by_idempotency_key(idempotency_key)
        if hasattr(paper_db, "get_order_by_idempotency_key")
        else None
    )
    if existing is not None and existing.exchange_order_id:
        payload = {}
        if existing.broker_payload:
            try:
                payload = json.loads(str(existing.broker_payload))
            except json.JSONDecodeError:
                payload = {}
        return {
            "order_id": existing.order_id,
            "broker": "zerodha",
            "mode": existing.broker_mode or adapter.mode,
            "status": existing.status,
            "idempotency_key": idempotency_key,
            "exchange_order_id": existing.exchange_order_id,
            "broker_latency_ms": existing.broker_latency_ms,
            "payload": payload,
            "recovered_existing": True,
        }

    payload_json = json.dumps(safe_intent.zerodha_payload(), sort_keys=True, separators=(",", ":"))
    if existing is not None:
        payload = {}
        if existing.broker_payload:
            try:
                payload = json.loads(str(existing.broker_payload))
            except json.JSONDecodeError:
                payload = {}
        existing_status = str(existing.status or "").upper()
        if (
            adapter.mode != "REAL_DRY_RUN"
            and existing.exchange_order_id is None
            and existing_status in {"REJECTED", "CANCELLED"}
        ):
            if hasattr(paper_db, "prepare_order_broker_retry"):
                paper_db.prepare_order_broker_retry(
                    existing.order_id,
                    broker_mode=adapter.mode,
                    broker_payload=payload_json,
                )
        else:
            recovered_snapshot = await _recover_pending_submission(
                paper_db=paper_db,
                order_id=existing.order_id,
                adapter=adapter,
                payload=payload or safe_intent.zerodha_payload(),
            )
            if recovered_snapshot is not None:
                return {
                    "order_id": existing.order_id,
                    "broker": "zerodha",
                    "mode": existing.broker_mode or adapter.mode,
                    "status": recovered_snapshot.status,
                    "idempotency_key": idempotency_key,
                    "exchange_order_id": recovered_snapshot.order_id,
                    "broker_latency_ms": existing.broker_latency_ms,
                    "payload": recovered_snapshot.broker_payload,
                    "recovered_existing": True,
                }
            if adapter.mode != "REAL_DRY_RUN":
                raise OrderSafetyError(
                    "real-order intent already exists without broker order id; "
                    "manual broker reconciliation required before retry"
                )

    order_id = (
        existing.order_id
        if existing is not None
        else paper_db.append_order_event(
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
            exchange_order_id=None,
            idempotency_key=idempotency_key,
            notes="PENDING_DISPATCH",
            broker_mode=adapter.mode,
            broker_payload=payload_json,
            broker_latency_ms=None,
        )
    )
    try:
        result = await adapter.place_order(safe_intent)
    except Exception as exc:
        if hasattr(paper_db, "update_order_broker_rejection"):
            paper_db.update_order_broker_rejection(
                order_id,
                broker_mode=adapter.mode,
                broker_payload=payload_json,
                broker_status_message=str(exc),
            )
        raise
    payload_json = json.dumps(result.payload, sort_keys=True, separators=(",", ":"))
    if hasattr(paper_db, "update_order_broker_submission"):
        paper_db.update_order_broker_submission(
            order_id,
            exchange_order_id=result.exchange_order_id,
            broker_mode=result.mode,
            broker_payload=payload_json,
            broker_latency_ms=result.latency_ms,
            notes=result.mode,
        )
    return {
        "order_id": order_id,
        "broker": result.broker,
        "mode": result.mode,
        "status": result.status,
        "idempotency_key": result.idempotency_key,
        "exchange_order_id": result.exchange_order_id,
        "broker_latency_ms": result.latency_ms,
        "payload": result.payload,
    }


async def _recover_pending_submission(
    *,
    paper_db: Any,
    order_id: str,
    adapter: ZerodhaBrokerAdapter,
    payload: dict[str, Any],
) -> BrokerOrderSnapshot | None:
    """Recover a broker order for a pre-dispatch row whose exchange id was not saved."""
    try:
        snapshots = await adapter.fetch_order_snapshots()
    except Exception:
        return None
    matches = [snapshot for snapshot in snapshots if _snapshot_matches_payload(snapshot, payload)]
    if len(matches) != 1:
        return None
    for snapshot in matches:
        broker_payload = json.dumps(
            snapshot.broker_payload,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        )
        if hasattr(paper_db, "update_order_broker_submission"):
            paper_db.update_order_broker_submission(
                order_id,
                exchange_order_id=snapshot.order_id,
                broker_mode=adapter.mode,
                broker_payload=broker_payload,
                broker_latency_ms=None,
                notes="RECOVERED_PENDING_DISPATCH",
            )
        if hasattr(paper_db, "update_order_from_broker_snapshot"):
            paper_db.update_order_from_broker_snapshot(snapshot.order_id, snapshot.broker_payload)
        return snapshot
    return None


def _snapshot_matches_payload(snapshot: BrokerOrderSnapshot, payload: dict[str, Any]) -> bool:
    tag = str(payload.get("tag") or "").strip()
    if tag and str(snapshot.tag or "").strip() != tag:
        return False
    if str(snapshot.symbol or "").upper() != str(payload.get("tradingsymbol") or "").upper():
        return False
    if str(snapshot.side or "").upper() != str(payload.get("transaction_type") or "").upper():
        return False
    if int(float(snapshot.quantity or 0)) != int(float(payload.get("quantity") or 0)):
        return False
    raw = snapshot.broker_payload or {}
    for snap_key, payload_key in (
        ("product", "product"),
        ("exchange", "exchange"),
        ("order_type", "order_type"),
    ):
        if str(raw.get(snap_key) or "").upper() != str(payload.get(payload_key) or "").upper():
            return False
    return True


def _default_zerodha_tag(session_id: str, role: str) -> str:
    raw = f"cpr-{role}-{session_id}".replace("_", "-")
    safe = "".join(ch for ch in raw.lower() if ch.isalnum() or ch == "-")
    return safe[:20] or "cpr-dry-run"


def _short_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())[:18] or "order"


def _ceil_to_tick(value: float, tick_size: float) -> float:
    return math.ceil((float(value) - 1e-12) / float(tick_size)) * float(tick_size)


def _floor_to_tick(value: float, tick_size: float) -> float:
    return math.floor((float(value) + 1e-12) / float(tick_size)) * float(tick_size)


def _is_protected_exit_role(role: str) -> bool:
    return _role_matches_any(role, _PROTECTED_EXIT_ROLES)


def _is_emergency_flatten_role(role: str) -> bool:
    return _role_matches_any(role, _EMERGENCY_FLATTEN_ROLE_TOKENS)


def _is_stop_loss_role(role: str) -> bool:
    return _role_matches_any(role, _STOP_LOSS_ROLE_TOKENS)


def _validate_protected_exit(intent: BrokerOrderIntent) -> None:
    if intent.order_type in {"SL", "SL-M"} and _is_stop_loss_role(intent.role):
        if intent.trigger_price is None:
            raise OrderSafetyError("stop-loss orders require trigger_price")
        if intent.order_type == "SL" and intent.price is None:
            raise OrderSafetyError("SL stop-loss orders require price")
        if intent.reference_price is None:
            raise OrderSafetyError("stop-loss orders require a fresh reference_price")
        if intent.reference_price_age_sec is None:
            raise OrderSafetyError("stop-loss orders require reference_price_age_sec")
        if intent.reference_price_age_sec > DEFAULT_MAX_QUOTE_AGE_SEC:
            raise OrderSafetyError(
                f"reference price is stale: {intent.reference_price_age_sec:.2f}s > "
                f"{DEFAULT_MAX_QUOTE_AGE_SEC:.2f}s"
            )
        return
    if intent.order_type != "LIMIT":
        raise OrderSafetyError("exit/flatten roles must use protected LIMIT orders")
    if intent.reference_price is None:
        raise OrderSafetyError("exit/flatten roles require a fresh reference_price")
    if intent.reference_price_age_sec is None:
        raise OrderSafetyError("exit/flatten roles require reference_price_age_sec")
    if (
        intent.reference_price_age_sec > DEFAULT_MAX_QUOTE_AGE_SEC
        and not _is_emergency_flatten_role(intent.role)
    ):
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


def _validate_real_order_gate(
    *,
    intent: BrokerOrderIntent,
    payload: dict[str, Any],
    guard: RealOrderGuardConfig,
    kite_client: Any | None,
) -> None:
    if not guard.enabled:
        raise RealOrderPlacementDisabledError(
            f"Real Zerodha order placement is disabled by {REAL_ORDER_ENABLE_ENV}."
        )
    if guard.acknowledgement != REAL_ORDER_ACK_VALUE:
        raise RealOrderPlacementDisabledError(
            f"Set {REAL_ORDER_ACK_ENV}={REAL_ORDER_ACK_VALUE} to acknowledge real-money orders."
        )
    if kite_client is None or not callable(getattr(kite_client, "place_order", None)):
        raise RealOrderPlacementDisabledError("A Kite client with place_order is required.")
    if intent.product not in guard.allowed_products:
        raise OrderSafetyError(
            f"product {intent.product} is not allowed for real orders "
            f"(allowed={sorted(guard.allowed_products)})"
        )
    if intent.order_type not in guard.allowed_order_types:
        raise OrderSafetyError(
            f"order_type {intent.order_type} is not allowed for real orders "
            f"(allowed={sorted(guard.allowed_order_types)})"
        )
    if intent.quantity > guard.max_quantity:
        raise OrderSafetyError(
            f"quantity {intent.quantity} exceeds real-order max {guard.max_quantity}"
        )
    if intent.reference_price is None:
        raise OrderSafetyError("real orders require a fresh reference_price")
    if intent.reference_price_age_sec is None:
        raise OrderSafetyError("real orders require reference_price_age_sec")
    if (
        intent.reference_price_age_sec > DEFAULT_MAX_QUOTE_AGE_SEC
        and not _is_emergency_flatten_role(intent.role)
    ):
        raise OrderSafetyError(
            f"reference price is stale: {intent.reference_price_age_sec:.2f}s > "
            f"{DEFAULT_MAX_QUOTE_AGE_SEC:.2f}s"
        )

    estimated_notional = _estimated_order_price(intent) * intent.quantity
    if estimated_notional > guard.max_notional:
        raise OrderSafetyError(
            f"estimated notional {estimated_notional:.2f} exceeds real-order max "
            f"{guard.max_notional:.2f}"
        )
    if payload.get("tradingsymbol") != intent.symbol:
        raise OrderSafetyError("payload symbol mismatch")
    if payload.get("transaction_type") != intent.side:
        raise OrderSafetyError("payload side mismatch")


def _estimated_order_price(intent: BrokerOrderIntent) -> float:
    prices = [
        float(value)
        for value in (intent.price, intent.trigger_price, intent.reference_price)
        if value is not None
    ]
    if not prices:
        raise OrderSafetyError("real orders require a price, trigger_price, or reference_price")
    return max(prices)


def _call_kite_place_order(kite_client: Any, payload: dict[str, Any]) -> str:
    if "market_protection" not in payload or not hasattr(kite_client, "_post"):
        return kite_client.place_order(**payload)
    params = dict(payload)
    variety = str(params.pop("variety"))
    params = {key: value for key, value in params.items() if value is not None}
    response = kite_client._post(
        "order.place",
        url_args={"variety": variety},
        params=params,
    )
    return str(response["order_id"])


def _env_flag(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise OrderSafetyError(f"{name} must be an integer") from exc
    if value <= 0:
        raise OrderSafetyError(f"{name} must be positive")
    return value


def _env_float(name: str, *, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise OrderSafetyError(f"{name} must be numeric") from exc
    if not math.isfinite(value) or value <= 0:
        raise OrderSafetyError(f"{name} must be a positive finite number")
    return value


def _env_set(name: str, *, default: frozenset[str]) -> frozenset[str]:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    values = frozenset(part.strip().upper() for part in raw.split(",") if part.strip())
    return values or default


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
