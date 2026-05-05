"""Guarded real-order routing for live CPR paper sessions."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from db.paper_db import get_paper_db
from engine.broker_adapter import (
    DEFAULT_EXIT_MAX_SLIPPAGE_PCT,
    DEFAULT_MARKET_PROTECTION_PCT,
    DEFAULT_TICK_SIZE,
    BrokerOrderIntent,
    OrderSafetyError,
    ZerodhaBrokerAdapter,
    build_protected_flatten_intent,
    record_real_order,
)


@dataclass(frozen=True, slots=True)
class RealOrderRuntimeConfig:
    enabled: bool = False
    sizing_mode: str = "FIXED_QTY"
    fixed_quantity: int = 1
    max_positions: int = 1
    cash_budget: float = 10_000.0
    require_account_cash_check: bool = True
    entry_order_type: str = "LIMIT"
    entry_max_slippage_pct: float = 0.5
    exit_max_slippage_pct: float = DEFAULT_EXIT_MAX_SLIPPAGE_PCT
    product: str = "MIS"
    exchange: str = "NSE"
    adapter_mode: str = "LIVE"
    shadow: bool = False

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> RealOrderRuntimeConfig:
        data = dict(raw or {})
        fixed_quantity = data.get("fixed_quantity", 1)
        max_positions = data.get("max_positions", 1)
        cash_budget = data.get("cash_budget", 10_000.0)
        entry_slippage = data.get("entry_max_slippage_pct", 0.5)
        exit_slippage = data.get("exit_max_slippage_pct", DEFAULT_EXIT_MAX_SLIPPAGE_PCT)
        return cls(
            enabled=bool(data.get("enabled", False)),
            sizing_mode=str(data.get("sizing_mode") or "FIXED_QTY").upper(),
            fixed_quantity=int(fixed_quantity if fixed_quantity is not None else 1),
            max_positions=int(max_positions if max_positions is not None else 1),
            cash_budget=float(cash_budget if cash_budget is not None else 10_000.0),
            require_account_cash_check=bool(data.get("require_account_cash_check", True)),
            entry_order_type=str(data.get("entry_order_type") or "LIMIT").upper(),
            entry_max_slippage_pct=float(entry_slippage if entry_slippage is not None else 0.5),
            exit_max_slippage_pct=float(
                exit_slippage if exit_slippage is not None else DEFAULT_EXIT_MAX_SLIPPAGE_PCT
            ),
            product=str(data.get("product") or "MIS").upper(),
            exchange=str(data.get("exchange") or "NSE").upper(),
            adapter_mode=str(data.get("adapter_mode") or "LIVE").upper(),
            shadow=bool(data.get("shadow", False)),
        ).validate()

    def validate(self) -> RealOrderRuntimeConfig:
        if self.sizing_mode not in {"FIXED_QTY", "CASH_BUDGET"}:
            raise OrderSafetyError("real-order sizing mode must be FIXED_QTY or CASH_BUDGET")
        if self.fixed_quantity <= 0:
            raise OrderSafetyError("real-order fixed quantity must be positive")
        if self.max_positions <= 0:
            raise OrderSafetyError("real-order max positions must be positive")
        if not math.isfinite(self.cash_budget) or self.cash_budget <= 0:
            raise OrderSafetyError("real-order cash budget must be positive")
        if self.entry_order_type not in {"MARKET", "LIMIT"}:
            raise OrderSafetyError("automated real entries support only MARKET or LIMIT")
        if not math.isfinite(self.entry_max_slippage_pct) or self.entry_max_slippage_pct <= 0:
            raise OrderSafetyError("entry max slippage must be positive")
        if not math.isfinite(self.exit_max_slippage_pct) or self.exit_max_slippage_pct <= 0:
            raise OrderSafetyError("exit max slippage must be positive")
        if self.product != "MIS":
            raise OrderSafetyError("automated CPR real-order pilot supports MIS only")
        if self.exchange != "NSE":
            raise OrderSafetyError("automated CPR real-order pilot supports NSE only")
        if self.adapter_mode not in {"LIVE", "REAL_DRY_RUN"}:
            raise OrderSafetyError("real-order adapter mode must be LIVE or REAL_DRY_RUN")
        return self


class RealOrderRouter:
    """Places guarded real broker orders for live-session paper events.

    The router keeps broker sizing independent of paper sizing. This lets paper
    strategy math continue unchanged while the real-money pilot starts small and
    only scales after the Doppler caps and CLI sizing mode are raised.
    """

    def __init__(
        self,
        config: RealOrderRuntimeConfig,
        *,
        adapter: ZerodhaBrokerAdapter | None = None,
        account_available_cash: float | None = None,
    ) -> None:
        self.config = config
        self.adapter = adapter or _default_live_adapter()
        self._open_real_positions = 0
        self._used_cash_notional = 0.0
        self._open_notional_by_symbol: dict[str, float] = {}
        if config.enabled and config.require_account_cash_check and config.adapter_mode == "LIVE":
            if account_available_cash is None:
                raise OrderSafetyError(
                    "real-order cash check required but available_cash could not be fetched"
                )
            if config.cash_budget > float(account_available_cash):
                raise OrderSafetyError(
                    f"real-order cash budget {config.cash_budget:.2f} exceeds available cash "
                    f"{float(account_available_cash):.2f}"
                )

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    async def place_entry(
        self,
        *,
        session_id: str,
        symbol: str,
        direction: str,
        reference_price: float,
        event_time: datetime | str | None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        started = time.perf_counter()
        event_lag_ms = _event_lag_ms(event_time)
        if not self.config.shadow and self._open_real_positions >= self.config.max_positions:
            raise OrderSafetyError(
                f"real-order max open positions reached ({self.config.max_positions})"
            )
        side = "BUY" if direction.upper() == "LONG" else "SELL"
        intent = self._entry_intent(
            session_id=session_id,
            symbol=symbol,
            side=side,
            reference_price=reference_price,
            event_time=_event_time_value(event_time),
        )
        intent_latency_ms = (time.perf_counter() - started) * 1000.0
        entry_notional = _intent_notional(intent)
        if (
            not self.config.shadow
            and self._used_cash_notional + entry_notional > self.config.cash_budget
        ):
            raise OrderSafetyError(
                f"real-order cash budget exceeded: requested cumulative notional "
                f"{self._used_cash_notional + entry_notional:.2f} > "
                f"{self.config.cash_budget:.2f}"
            )
        result = await record_real_order(
            paper_db=get_paper_db(),
            intent=intent,
            adapter=self.adapter,
        )
        total_latency_ms = (time.perf_counter() - started) * 1000.0
        self._open_real_positions += 1
        self._used_cash_notional += entry_notional
        self._open_notional_by_symbol[symbol.upper()] = (
            self._open_notional_by_symbol.get(symbol.upper(), 0.0) + entry_notional
        )
        _log_order_latency(
            session_id=session_id,
            symbol=symbol,
            role="entry",
            mode=str(result.get("mode") or self.adapter.mode),
            event_lag_ms=event_lag_ms,
            quote_age_sec=0.0,
            intent_latency_ms=intent_latency_ms,
            broker_latency_ms=result.get("broker_latency_ms"),
            total_latency_ms=total_latency_ms,
        )
        return {
            "real_order_qty": intent.quantity,
            "real_entry_order_id": result.get("exchange_order_id"),
            "real_entry_order_type": intent.order_type,
            "real_entry_side": side,
            "real_entry_payload": result.get("payload"),
            "real_remaining_qty": intent.quantity,
            "real_sizing_mode": self.config.sizing_mode,
            "real_entry_mode": result.get("mode") or self.adapter.mode,
            "real_entry_intent_latency_ms": round(intent_latency_ms, 3),
            "real_entry_broker_latency_ms": result.get("broker_latency_ms"),
            "real_entry_total_latency_ms": round(total_latency_ms, 3),
            "real_entry_event_lag_ms": event_lag_ms,
        }

    async def place_exit(
        self,
        *,
        session_id: str,
        symbol: str,
        direction: str,
        position_id: str | None,
        quantity: int,
        reference_price: float,
        role: str,
        event_time: datetime | str | None,
        quote_age_sec: float = 0.0,
    ) -> dict[str, Any]:
        if not self.enabled or quantity <= 0:
            return {}
        started = time.perf_counter()
        event_lag_ms = _event_lag_ms(event_time)
        side = "SELL" if direction.upper() == "LONG" else "BUY"
        intent = build_protected_flatten_intent(
            session_id=session_id,
            symbol=symbol,
            side=side,
            quantity=int(quantity),
            latest_price=float(reference_price),
            quote_age_sec=float(quote_age_sec),
            role=role,
            position_id=position_id,
            product=self.config.product,
            exchange=self.config.exchange,
            max_slippage_pct=self.config.exit_max_slippage_pct,
            event_time=_event_time_value(event_time),
        )
        intent_latency_ms = (time.perf_counter() - started) * 1000.0
        result = await record_real_order(
            paper_db=get_paper_db(),
            intent=intent,
            adapter=self.adapter,
        )
        total_latency_ms = (time.perf_counter() - started) * 1000.0
        self._open_real_positions = max(0, self._open_real_positions - 1)
        released = self._open_notional_by_symbol.pop(symbol.upper(), 0.0)
        self._used_cash_notional = max(0.0, self._used_cash_notional - released)
        _log_order_latency(
            session_id=session_id,
            symbol=symbol,
            role=role,
            mode=str(result.get("mode") or self.adapter.mode),
            event_lag_ms=event_lag_ms,
            quote_age_sec=float(quote_age_sec),
            intent_latency_ms=intent_latency_ms,
            broker_latency_ms=result.get("broker_latency_ms"),
            total_latency_ms=total_latency_ms,
        )
        return {
            "real_exit_order_id": result.get("exchange_order_id"),
            "real_exit_order_type": intent.order_type,
            "real_exit_side": side,
            "real_exit_payload": result.get("payload"),
            "real_exit_mode": result.get("mode") or self.adapter.mode,
            "real_exit_intent_latency_ms": round(intent_latency_ms, 3),
            "real_exit_broker_latency_ms": result.get("broker_latency_ms"),
            "real_exit_total_latency_ms": round(total_latency_ms, 3),
            "real_exit_event_lag_ms": event_lag_ms,
            "real_exit_quote_age_sec": round(float(quote_age_sec), 3),
        }

    def exit_quantity_for_position(self, position: Any) -> int:
        if not self.enabled:
            return 0
        trail_state = dict(getattr(position, "trail_state", None) or {})
        remaining = trail_state.get("real_remaining_qty")
        if remaining is None:
            remaining = trail_state.get("real_order_qty")
        if remaining is None:
            remaining = self.config.fixed_quantity
        return max(0, int(remaining))

    def _entry_intent(
        self,
        *,
        session_id: str,
        symbol: str,
        side: str,
        reference_price: float,
        event_time: str | None,
    ) -> BrokerOrderIntent:
        order_type = self.config.entry_order_type
        price = None
        if order_type == "LIMIT":
            price = _marketable_limit_price(
                side=side,
                reference_price=reference_price,
                max_slippage_pct=self.config.entry_max_slippage_pct,
            )
        quantity = self._entry_quantity(reference_price=reference_price, price=price)
        return BrokerOrderIntent(
            session_id=session_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            role="entry",
            order_type=order_type,
            price=price,
            reference_price=float(reference_price),
            reference_price_age_sec=0.0,
            max_slippage_pct=self.config.entry_max_slippage_pct,
            market_protection=DEFAULT_MARKET_PROTECTION_PCT if order_type == "MARKET" else None,
            product=self.config.product,
            exchange=self.config.exchange,
            event_time=event_time,
        )

    def _entry_quantity(self, *, reference_price: float, price: float | None) -> int:
        if self.config.sizing_mode == "FIXED_QTY":
            return self.config.fixed_quantity
        sizing_price = float(price if price is not None else reference_price)
        if not math.isfinite(sizing_price) or sizing_price <= 0:
            raise OrderSafetyError("cash-budget sizing requires a positive entry price")
        remaining_budget = max(0.0, self.config.cash_budget - self._used_cash_notional)
        quantity = math.floor(remaining_budget / sizing_price)
        if quantity <= 0:
            raise OrderSafetyError(
                f"real-order cash budget {self.config.cash_budget:.2f} cannot buy one share "
                f"at entry price {sizing_price:.2f}"
            )
        return int(quantity)


def build_real_order_router(
    config: RealOrderRuntimeConfig | dict[str, Any] | None,
) -> RealOrderRouter | None:
    runtime_config = (
        config
        if isinstance(config, RealOrderRuntimeConfig)
        else RealOrderRuntimeConfig.from_mapping(config)
    )
    if not runtime_config.enabled:
        return None
    if runtime_config.adapter_mode == "REAL_DRY_RUN":
        return RealOrderRouter(
            runtime_config,
            adapter=ZerodhaBrokerAdapter(mode="REAL_DRY_RUN"),
            account_available_cash=None,
        )
    if runtime_config.require_account_cash_check:
        kite_client = _get_kite_client()
        return RealOrderRouter(
            runtime_config,
            adapter=ZerodhaBrokerAdapter(
                mode="LIVE",
                allow_real_orders=True,
                kite_client=kite_client,
            ),
            account_available_cash=_fetch_available_cash(kite_client),
        )
    return RealOrderRouter(runtime_config)


def _default_live_adapter() -> ZerodhaBrokerAdapter:
    return ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=_get_kite_client(),
    )


def _get_kite_client() -> Any:
    from engine.kite_ingestion import get_kite_client

    return get_kite_client()


def _marketable_limit_price(
    *,
    side: str,
    reference_price: float,
    max_slippage_pct: float,
    tick_size: float = DEFAULT_TICK_SIZE,
) -> float:
    if reference_price <= 0 or not math.isfinite(reference_price):
        raise OrderSafetyError("reference price must be positive for real entry")
    side_upper = side.upper()
    if side_upper == "BUY":
        raw = reference_price * (1.0 + max_slippage_pct / 100.0)
        return round(math.ceil(raw / tick_size) * tick_size, 4)
    if side_upper == "SELL":
        raw = reference_price * (1.0 - max_slippage_pct / 100.0)
        return round(math.floor(raw / tick_size) * tick_size, 4)
    raise OrderSafetyError("entry side must be BUY or SELL")


def _intent_notional(intent: BrokerOrderIntent) -> float:
    normalized = intent.normalized()
    price = normalized.price or normalized.reference_price
    if price is None:
        raise OrderSafetyError("real-order cash budget requires price or reference_price")
    if not math.isfinite(float(price)) or float(price) <= 0:
        raise OrderSafetyError("real-order cash budget price must be positive")
    return float(price) * int(normalized.quantity)


def _event_lag_ms(event_time: datetime | str | None) -> float | None:
    event = _coerce_event_datetime(event_time)
    if event is None:
        return None
    now = datetime.now(event.tzinfo)
    lag = now - event
    # Historical replay/local-feed drills use old candle timestamps. Report
    # event lag only for real-time bars so the latency number stays meaningful.
    if lag < timedelta(seconds=-5) or lag > timedelta(hours=6):
        return None
    return round(lag.total_seconds() * 1000.0, 3)


def _coerce_event_datetime(event_time: datetime | str | None) -> datetime | None:
    if isinstance(event_time, datetime):
        return event_time if event_time.tzinfo is not None else event_time.astimezone()
    if isinstance(event_time, str) and event_time.strip():
        try:
            parsed = datetime.fromisoformat(event_time.strip())
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.astimezone()
    return None


def _log_order_latency(
    *,
    session_id: str,
    symbol: str,
    role: str,
    mode: str,
    event_lag_ms: float | None,
    quote_age_sec: float | None,
    intent_latency_ms: float,
    broker_latency_ms: Any,
    total_latency_ms: float,
) -> None:
    import logging

    logging.getLogger(__name__).info(
        "ORDER_LATENCY session_id=%s symbol=%s role=%s mode=%s "
        "event_lag_ms=%s quote_age_sec=%s intent_ms=%.3f broker_ms=%s total_ms=%.3f",
        session_id,
        symbol.upper(),
        role,
        mode,
        "n/a" if event_lag_ms is None else f"{event_lag_ms:.3f}",
        "n/a" if quote_age_sec is None else f"{float(quote_age_sec):.3f}",
        intent_latency_ms,
        "n/a" if broker_latency_ms is None else f"{float(broker_latency_ms):.3f}",
        total_latency_ms,
    )


def _fetch_available_cash(kite_client: Any) -> float | None:
    margins = getattr(kite_client, "margins", None)
    if not callable(margins):
        return None
    try:
        payload = margins("equity")
    except TypeError:
        payload = margins()
    return _extract_available_cash(payload)


def _extract_available_cash(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    candidates = [
        ("equity", "available", "cash"),
        ("available", "cash"),
        ("cash",),
        ("live_balance",),
        ("opening_balance",),
    ]
    for path in candidates:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if current is None:
            continue
        try:
            value = float(current)
        except TypeError, ValueError:
            continue
        if math.isfinite(value):
            return value
    return None


def _event_time_value(value: datetime | str | None) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None:
        return None
    return str(value)
