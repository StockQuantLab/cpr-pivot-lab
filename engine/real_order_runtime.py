"""Guarded real-order routing for live CPR paper sessions."""

from __future__ import annotations

import asyncio
import logging
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

logger = logging.getLogger(__name__)


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
    fill_wait_timeout_sec: float = 8.0
    fill_poll_sec: float = 0.5
    protective_sl_market_protection_pct: float = DEFAULT_MARKET_PROTECTION_PCT

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
            fill_wait_timeout_sec=float(data.get("fill_wait_timeout_sec", 8.0) or 8.0),
            fill_poll_sec=float(data.get("fill_poll_sec", 0.5) or 0.5),
            protective_sl_market_protection_pct=float(
                data.get("protective_sl_market_protection_pct", DEFAULT_MARKET_PROTECTION_PCT)
                or DEFAULT_MARKET_PROTECTION_PCT
            ),
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
        if not math.isfinite(self.fill_wait_timeout_sec) or self.fill_wait_timeout_sec < 0:
            raise OrderSafetyError("fill wait timeout must be non-negative")
        if not math.isfinite(self.fill_poll_sec) or self.fill_poll_sec <= 0:
            raise OrderSafetyError("fill poll interval must be positive")
        if (
            not math.isfinite(self.protective_sl_market_protection_pct)
            or self.protective_sl_market_protection_pct <= 0
        ):
            raise OrderSafetyError("protective SL market protection must be positive")
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
        stop_loss: float | None,
        event_time: datetime | str | None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        if stop_loss is None or not math.isfinite(float(stop_loss)) or float(stop_loss) <= 0:
            raise OrderSafetyError("real entry requires a positive strategy stop_loss")
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
        fill = await self._await_order_fill(
            exchange_order_id=str(result.get("exchange_order_id") or ""),
            intent=intent,
            fallback_price=reference_price,
        )
        filled_qty = int(fill["filled_qty"])
        fill_price = float(fill["fill_price"])
        if filled_qty <= 0:
            raise OrderSafetyError(
                "broker returned zero fill quantity; no real exposure registered"
            )
        protective_stop_loss = _stop_loss_from_actual_fill(
            direction=direction,
            model_entry_price=reference_price,
            model_stop_loss=float(stop_loss),
            actual_fill_price=fill_price,
        )
        filled_notional = filled_qty * fill_price
        self._register_real_exposure(symbol=symbol, notional=filled_notional)
        try:
            protective_result = await self._place_protective_stop(
                session_id=session_id,
                symbol=symbol,
                direction=direction,
                quantity=filled_qty,
                stop_loss=protective_stop_loss,
                event_time=event_time,
            )
            await self._await_order_acceptance(
                exchange_order_id=str(protective_result.get("exchange_order_id") or ""),
                accepted_statuses={"TRIGGER PENDING", "OPEN", "PENDING"},
                expected_quantity=filled_qty,
                fallback_status="TRIGGER PENDING",
            )
        except Exception as exc:
            logger.critical(
                "Protective SL placement failed after real entry fill session=%s symbol=%s "
                "entry_order_id=%s qty=%s fill_price=%.2f; attempting immediate flatten",
                session_id,
                symbol,
                result.get("exchange_order_id"),
                filled_qty,
                fill_price,
                exc_info=True,
            )
            rollback_error: Exception | None = None
            try:
                await self._flatten_unprotected_entry(
                    session_id=session_id,
                    symbol=symbol,
                    direction=direction,
                    quantity=filled_qty,
                    reference_price=fill_price,
                    event_time=event_time,
                )
                self._release_real_exposure(symbol=symbol)
                logger.critical(
                    "Unprotected real entry flattened after protective SL failure "
                    "session=%s symbol=%s qty=%s",
                    session_id,
                    symbol,
                    filled_qty,
                )
            except Exception as flatten_exc:  # pragma: no cover - exercised by live broker failure
                rollback_error = flatten_exc
                logger.critical(
                    "Unprotected real entry could not be flattened session=%s symbol=%s qty=%s; "
                    "manual broker intervention required",
                    session_id,
                    symbol,
                    filled_qty,
                    exc_info=True,
                )
            if rollback_error is not None:
                raise OrderSafetyError(
                    "protective SL placement failed after entry fill and rollback flatten failed; "
                    "manual broker intervention required"
                ) from exc
            raise OrderSafetyError(
                "protective SL placement failed after entry fill; entry was flattened"
            ) from exc
        total_latency_ms = (time.perf_counter() - started) * 1000.0
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
            "real_filled_qty": filled_qty,
            "real_entry_fill_price": fill_price,
            "real_entry_order_id": result.get("exchange_order_id"),
            "real_entry_order_type": intent.order_type,
            "real_entry_side": side,
            "real_entry_payload": result.get("payload"),
            "real_remaining_qty": filled_qty,
            "real_protective_sl_order_id": protective_result.get("exchange_order_id"),
            "real_model_stop_loss": float(stop_loss),
            "real_protective_sl_trigger_price": protective_stop_loss,
            "real_protection_status": "PLACED",
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
        protective_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not self.enabled or quantity <= 0:
            return {}
        started = time.perf_counter()
        event_lag_ms = _event_lag_ms(event_time)
        side = "SELL" if direction.upper() == "LONG" else "BUY"
        if protective_order_id:
            if "SL_HIT" in role.upper():
                try:
                    fill = await self._await_order_fill(
                        exchange_order_id=protective_order_id,
                        intent=None,
                        fallback_price=reference_price,
                        expected_quantity=int(quantity),
                    )
                except Exception:
                    logger.critical(
                        "Protective SL fill confirmation failed session=%s symbol=%s "
                        "order_id=%s qty=%s; keeping exposure reserved",
                        session_id,
                        symbol,
                        protective_order_id,
                        quantity,
                        exc_info=True,
                    )
                    raise
                self._release_real_exposure(symbol=symbol)
                total_latency_ms = (time.perf_counter() - started) * 1000.0
                _log_order_latency(
                    session_id=session_id,
                    symbol=symbol,
                    role=role,
                    mode=self.adapter.mode,
                    event_lag_ms=event_lag_ms,
                    quote_age_sec=float(quote_age_sec),
                    intent_latency_ms=0.0,
                    broker_latency_ms=None,
                    total_latency_ms=total_latency_ms,
                )
                return {
                    "real_exit_order_id": protective_order_id,
                    "real_exit_order_type": "SL-M",
                    "real_exit_side": side,
                    "real_exit_mode": self.adapter.mode,
                    "real_exit_via": "protective_sl",
                    "real_exit_filled_qty": int(fill["filled_qty"]),
                    "real_exit_fill_price": float(fill["fill_price"]),
                    "real_exit_total_latency_ms": round(total_latency_ms, 3),
                    "real_exit_event_lag_ms": event_lag_ms,
                    "real_exit_quote_age_sec": round(float(quote_age_sec), 3),
                }
            await self.cancel_protective_order(protective_order_id)
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
        try:
            fill = await self._await_order_fill(
                exchange_order_id=str(result.get("exchange_order_id") or ""),
                intent=intent,
                fallback_price=reference_price,
            )
        except Exception:
            logger.critical(
                "Real exit fill confirmation failed session=%s symbol=%s order_id=%s "
                "role=%s qty=%s; keeping exposure reserved",
                session_id,
                symbol,
                result.get("exchange_order_id"),
                role,
                quantity,
                exc_info=True,
            )
            raise
        total_latency_ms = (time.perf_counter() - started) * 1000.0
        self._release_real_exposure(symbol=symbol)
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
            "real_exit_filled_qty": int(fill["filled_qty"]),
            "real_exit_fill_price": float(fill["fill_price"]),
            "real_exit_mode": result.get("mode") or self.adapter.mode,
            "real_exit_intent_latency_ms": round(intent_latency_ms, 3),
            "real_exit_broker_latency_ms": result.get("broker_latency_ms"),
            "real_exit_total_latency_ms": round(total_latency_ms, 3),
            "real_exit_event_lag_ms": event_lag_ms,
            "real_exit_quote_age_sec": round(float(quote_age_sec), 3),
        }

    async def cancel_protective_order(self, order_id: str | None) -> str | None:
        oid = str(order_id or "").strip()
        if not oid:
            return None
        cancelled_id = await self.adapter.cancel_order(order_id=oid, variety="regular")
        get_paper_db().update_order_from_broker_snapshot(
            oid,
            {
                "status": "CANCELLED",
                "filled_quantity": 0,
                "average_price": 0,
            },
        )
        return cancelled_id

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

    async def _place_protective_stop(
        self,
        *,
        session_id: str,
        symbol: str,
        direction: str,
        quantity: int,
        stop_loss: float,
        event_time: datetime | str | None,
    ) -> dict[str, Any]:
        side = "SELL" if direction.upper() == "LONG" else "BUY"
        intent = BrokerOrderIntent(
            session_id=session_id,
            symbol=symbol,
            side=side,
            quantity=int(quantity),
            role="protective_sl",
            order_type="SL-M",
            trigger_price=float(stop_loss),
            reference_price=float(stop_loss),
            reference_price_age_sec=0.0,
            market_protection=self.config.protective_sl_market_protection_pct,
            product=self.config.product,
            exchange=self.config.exchange,
            event_time=_event_time_value(event_time),
        )
        return await record_real_order(
            paper_db=get_paper_db(),
            intent=intent,
            adapter=self.adapter,
        )

    async def _flatten_unprotected_entry(
        self,
        *,
        session_id: str,
        symbol: str,
        direction: str,
        quantity: int,
        reference_price: float,
        event_time: datetime | str | None,
    ) -> dict[str, Any]:
        side = "SELL" if direction.upper() == "LONG" else "BUY"
        intent = build_protected_flatten_intent(
            session_id=session_id,
            symbol=symbol,
            side=side,
            quantity=int(quantity),
            latest_price=float(reference_price),
            quote_age_sec=0.0,
            role="emergency_flatten:entry_protection_failed",
            product=self.config.product,
            exchange=self.config.exchange,
            max_slippage_pct=self.config.exit_max_slippage_pct,
            event_time=_event_time_value(event_time),
        )
        result = await record_real_order(
            paper_db=get_paper_db(),
            intent=intent,
            adapter=self.adapter,
        )
        fill = await self._await_order_fill(
            exchange_order_id=str(result.get("exchange_order_id") or ""),
            intent=intent,
            fallback_price=reference_price,
        )
        return {**result, **fill}

    async def _await_order_fill(
        self,
        *,
        exchange_order_id: str,
        intent: BrokerOrderIntent | None,
        fallback_price: float,
        expected_quantity: int | None = None,
    ) -> dict[str, Any]:
        expected_qty = int(expected_quantity or (intent.quantity if intent is not None else 0))
        if self.config.shadow or self.adapter.mode == "REAL_DRY_RUN":
            if exchange_order_id:
                get_paper_db().update_order_from_broker_snapshot(
                    exchange_order_id,
                    {
                        "status": "COMPLETE",
                        "filled_quantity": expected_qty,
                        "average_price": float(
                            (
                                intent.price
                                if intent is not None and intent.price is not None
                                else None
                            )
                            or fallback_price
                        ),
                        "exchange_timestamp": datetime.now().isoformat(),
                    },
                )
            return {
                "filled_qty": expected_qty,
                "fill_price": float(
                    (intent.price if intent is not None and intent.price is not None else None)
                    or fallback_price
                ),
                "status": "COMPLETE",
            }

        if not exchange_order_id:
            raise OrderSafetyError("broker order id missing; cannot confirm fill")
        deadline = time.monotonic() + float(self.config.fill_wait_timeout_sec)
        last_snapshot: Any = None
        while True:
            snapshots = await self.adapter.fetch_order_snapshots()
            for snapshot in snapshots:
                if str(snapshot.order_id) != str(exchange_order_id):
                    continue
                last_snapshot = snapshot
                if hasattr(get_paper_db(), "update_order_from_broker_snapshot"):
                    get_paper_db().update_order_from_broker_snapshot(
                        exchange_order_id, snapshot.broker_payload
                    )
                status = str(snapshot.status or "").upper()
                filled_qty = int(float(snapshot.filled_quantity or 0))
                if status == "COMPLETE" and filled_qty >= expected_qty:
                    return {
                        "filled_qty": filled_qty,
                        "fill_price": float(snapshot.average_price or fallback_price),
                        "status": status,
                    }
                if status in {"REJECTED", "CANCELLED"}:
                    raise OrderSafetyError(
                        f"broker order {exchange_order_id} ended {status} before fill"
                    )
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(float(self.config.fill_poll_sec))

        if last_snapshot is not None and float(last_snapshot.filled_quantity or 0) > 0:
            filled_qty = int(float(last_snapshot.filled_quantity or 0))
            raise OrderSafetyError(
                f"broker order {exchange_order_id} partially filled {filled_qty}/{expected_qty}; "
                "manual reconciliation required"
            )
        raise OrderSafetyError(f"broker order {exchange_order_id} did not fill before timeout")

    async def _await_order_acceptance(
        self,
        *,
        exchange_order_id: str,
        accepted_statuses: set[str],
        expected_quantity: int,
        fallback_status: str,
    ) -> dict[str, Any]:
        accepted = {str(status or "").upper() for status in accepted_statuses}
        if self.config.shadow or self.adapter.mode == "REAL_DRY_RUN":
            if exchange_order_id:
                get_paper_db().update_order_from_broker_snapshot(
                    exchange_order_id,
                    {
                        "status": fallback_status,
                        "filled_quantity": 0,
                        "average_price": 0.0,
                        "exchange_timestamp": datetime.now().isoformat(),
                    },
                )
            return {"status": fallback_status, "filled_qty": 0}

        if not exchange_order_id:
            raise OrderSafetyError("broker order id missing; cannot confirm protective order")
        deadline = time.monotonic() + float(self.config.fill_wait_timeout_sec)
        last_status = ""
        while True:
            snapshots = await self.adapter.fetch_order_snapshots()
            for snapshot in snapshots:
                if str(snapshot.order_id) != str(exchange_order_id):
                    continue
                if hasattr(get_paper_db(), "update_order_from_broker_snapshot"):
                    get_paper_db().update_order_from_broker_snapshot(
                        exchange_order_id, snapshot.broker_payload
                    )
                status = str(snapshot.status or "").upper()
                last_status = status
                filled_qty = int(float(snapshot.filled_quantity or 0))
                if status in accepted:
                    return {"status": status, "filled_qty": filled_qty}
                if status in {"REJECTED", "CANCELLED"}:
                    raise OrderSafetyError(
                        f"broker protective order {exchange_order_id} ended {status}"
                    )
                if status == "COMPLETE":
                    raise OrderSafetyError(
                        f"broker protective order {exchange_order_id} filled immediately "
                        f"{filled_qty}/{expected_quantity}; manual reconciliation required"
                    )
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(float(self.config.fill_poll_sec))

        detail = f" last_status={last_status}" if last_status else ""
        raise OrderSafetyError(
            f"broker protective order {exchange_order_id} was not accepted before timeout{detail}"
        )

    def _release_real_exposure(self, *, symbol: str) -> None:
        self._open_real_positions = max(0, self._open_real_positions - 1)
        released = self._open_notional_by_symbol.pop(symbol.upper(), 0.0)
        self._used_cash_notional = max(0.0, self._used_cash_notional - released)

    def _register_real_exposure(self, *, symbol: str, notional: float) -> None:
        self._open_real_positions += 1
        self._used_cash_notional += float(notional)
        self._open_notional_by_symbol[symbol.upper()] = self._open_notional_by_symbol.get(
            symbol.upper(), 0.0
        ) + float(notional)

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


def _stop_loss_from_actual_fill(
    *,
    direction: str,
    model_entry_price: float,
    model_stop_loss: float,
    actual_fill_price: float,
) -> float:
    stop_distance = abs(float(model_entry_price) - float(model_stop_loss))
    if not math.isfinite(stop_distance) or stop_distance <= 0:
        raise OrderSafetyError("real entry stop distance must be positive")
    if direction.upper() == "LONG":
        adjusted = float(actual_fill_price) - stop_distance
    elif direction.upper() == "SHORT":
        adjusted = float(actual_fill_price) + stop_distance
    else:
        raise OrderSafetyError("entry direction must be LONG or SHORT")
    if not math.isfinite(adjusted) or adjusted <= 0:
        raise OrderSafetyError("adjusted protective stop loss is not positive")
    return round(adjusted, 4)


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
