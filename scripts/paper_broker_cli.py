"""Broker and order maintenance commands for the paper trading CLI."""

from __future__ import annotations

import argparse
import json
import math
import shlex
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from db.paper_db import get_paper_db
from engine.broker_adapter import (
    BrokerOrderIntent,
    ZerodhaBrokerAdapter,
    build_protected_flatten_intent,
    record_real_dry_run_order,
    record_real_order,
)
from engine.broker_reconciliation import (
    BrokerOrderSnapshot,
    BrokerPositionSnapshot,
    PilotGuardrails,
    reconcile_local_to_broker,
)

_ZERO_FILL_TERMINAL_ORDER_STATUSES = {"REJECTED", "CANCELLED"}
_MANUAL_PILOT_MARKERS = (
    "ZERODHA_LIVE_REAL_ORDERS_MANUAL_PILOT",
    "MANUAL REAL-ORDER PILOT",
    "MANUAL REAL ORDER PILOT",
    "MANUAL ITC REAL-ORDER PILOT",
)


def _pdb():
    return get_paper_db()


def _is_manual_real_order_pilot_session(session: Any) -> bool:
    text = " ".join(
        str(value or "")
        for value in (
            getattr(session, "session_id", ""),
            getattr(session, "name", ""),
            getattr(session, "notes", ""),
        )
    ).upper()
    return any(marker in text for marker in _MANUAL_PILOT_MARKERS)


def _maybe_complete_zero_fill_manual_pilot(db: Any, session_id: str) -> dict[str, Any] | None:
    session = db.get_session(session_id)
    if session is None or str(session.status).upper() not in {"ACTIVE", "PLANNING", "PAUSED"}:
        return None
    if not _is_manual_real_order_pilot_session(session):
        return None
    if db.get_open_positions(session_id):
        return None

    broker_orders = [
        order for order in db.get_session_orders(session_id) if str(order.broker_mode or "").strip()
    ]
    if not broker_orders:
        return None
    if any(
        str(order.status or "").upper() not in _ZERO_FILL_TERMINAL_ORDER_STATUSES
        or int(order.fill_qty or 0) != 0
        for order in broker_orders
    ):
        return None

    updated = db.update_session(
        session_id,
        status="COMPLETED",
        notes="auto_completed_manual_pilot_zero_fill_terminal_orders",
    )
    return {
        "session_id": session_id,
        "status": getattr(updated, "status", "COMPLETED") if updated else "COMPLETED",
        "reason": "zero_fill_terminal_broker_orders",
        "orders": len(broker_orders),
    }


def _load_json_list_arg(value: str | None, *, label: str) -> list[dict[str, Any]]:
    if not value:
        return []
    text = str(value).strip()
    path = Path(text) if text and not text.startswith("[") else None
    try:
        raw = path.read_text(encoding="utf-8") if path is not None and path.exists() else text
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} must be JSON array or path to JSON file: {exc}") from exc
    if not isinstance(parsed, list):
        raise SystemExit(f"{label} must be a JSON array")
    if not all(isinstance(item, dict) for item in parsed):
        raise SystemExit(f"{label} must contain JSON objects")
    return parsed


async def _cmd_broker_reconcile(args: argparse.Namespace) -> None:
    order_rows = _load_json_list_arg(args.broker_orders_json, label="--broker-orders-json")
    position_rows = _load_json_list_arg(args.broker_positions_json, label="--broker-positions-json")
    payload = reconcile_local_to_broker(
        db=_pdb(),
        session_id=args.session_id,
        broker_orders=[BrokerOrderSnapshot.from_mapping(row) for row in order_rows],
        broker_positions=[BrokerPositionSnapshot.from_mapping(row) for row in position_rows],
    )
    print(json.dumps(payload, default=str, indent=2))
    if not payload.get("ok", False) and bool(getattr(args, "strict", False)):
        raise SystemExit(1)


async def _cmd_broker_sync_orders(args: argparse.Namespace) -> None:
    db = _pdb()
    kite = _get_kite_client()
    orders = [dict(row) for row in kite.orders() or []]
    kite_by_id = {str(row.get("order_id") or ""): row for row in orders}
    local_orders = db.get_recent_orders(limit=int(args.limit), broker_only=True)
    updated: list[dict[str, Any]] = []
    missing: list[str] = []
    touched_sessions: set[str] = set()
    for order in local_orders:
        if args.session_id and str(order.session_id) != str(args.session_id):
            continue
        broker_order_id = str(order.exchange_order_id or "")
        if not broker_order_id or broker_order_id.startswith("dryrun-"):
            continue
        snapshot = kite_by_id.get(broker_order_id)
        if not snapshot:
            missing.append(broker_order_id)
            continue
        changed = db.update_order_from_broker_snapshot(broker_order_id, snapshot)
        if changed:
            touched_sessions.add(str(order.session_id))
            updated.append(
                {
                    "local_order_id": order.order_id,
                    "kite_order_id": broker_order_id,
                    "symbol": order.symbol,
                    "status": snapshot.get("status"),
                    "filled_quantity": snapshot.get("filled_quantity"),
                    "average_price": snapshot.get("average_price"),
                    "status_message": snapshot.get("status_message_raw")
                    or snapshot.get("status_message"),
                }
            )
    completed_sessions = [
        result
        for session_id in sorted(touched_sessions)
        if (result := _maybe_complete_zero_fill_manual_pilot(db, session_id)) is not None
    ]
    print(
        json.dumps(
            {
                "kite_orders": len(orders),
                "local_checked": len(local_orders),
                "updated": updated,
                "missing_kite_order_ids": missing,
                "completed_manual_pilot_sessions": completed_sessions,
            },
            default=str,
            indent=2,
        )
    )


async def _cmd_pilot_check(args: argparse.Namespace) -> None:
    symbols = [s.strip().upper() for s in str(args.symbols or "").split(",") if s.strip()]
    payload = PilotGuardrails().validate(
        symbols=symbols,
        order_quantity=int(args.order_quantity),
        estimated_notional=float(args.estimated_notional),
        product=args.product,
        order_type=args.order_type,
        acknowledgement=args.acknowledgement,
    )
    print(json.dumps(payload, default=str, indent=2))
    if not payload.get("ok", False) and bool(getattr(args, "strict", False)):
        raise SystemExit(1)


async def _cmd_real_pilot_plan(args: argparse.Namespace) -> None:
    """Build a no-placement one-symbol real-order pilot plan from fresh Kite LTP."""

    symbol = str(args.symbol or "").strip().upper()
    exchange = str(args.exchange or "NSE").strip().upper()
    quantity = int(args.quantity)
    product = str(args.product or "MIS").strip().upper()
    max_slippage_pct = float(args.max_slippage_pct)
    tick_size = float(args.tick_size)
    session_id = str(args.session_id or "").strip() or (
        f"manual-pilot-{datetime.now(UTC).date().isoformat()}-{symbol.lower()}"
    )
    kite = _get_kite_client()
    ltp = _fetch_ltp(kite, exchange=exchange, symbol=symbol)
    fetched_at = datetime.now(UTC).isoformat()
    buy_price = _ceil_to_tick(
        ltp * (1.0 + (float(args.buy_limit_offset_pct) / 100.0)),
        tick_size,
    )
    exit_intent = build_protected_flatten_intent(
        session_id=session_id,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        latest_price=ltp,
        quote_age_sec=float(args.reference_price_age_sec),
        role="manual_flatten",
        product=product,
        exchange=exchange,
        max_slippage_pct=max_slippage_pct,
        tick_size=tick_size,
        event_time=fetched_at,
    )
    stop_trigger = _floor_to_tick(ltp * (1.0 - (float(args.stop_loss_pct) / 100.0)), tick_size)
    stop_limit = _floor_to_tick(
        stop_trigger * (1.0 - (float(args.stop_limit_buffer_pct) / 100.0)),
        tick_size,
    )
    guardrails = PilotGuardrails(max_notional=float(args.max_notional)).validate(
        symbols=[symbol],
        order_quantity=quantity,
        estimated_notional=ltp * quantity,
        product=product,
        order_type="LIMIT",
        acknowledgement=args.acknowledgement,
    )
    buy_argv = _real_order_argv(
        session_id=session_id,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        role="manual",
        order_type="LIMIT",
        price=buy_price,
        reference_price=ltp,
        reference_price_age_sec=float(args.reference_price_age_sec),
        product=product,
        exchange=exchange,
    )
    market_buy_argv = _real_order_argv(
        session_id=session_id,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        role="manual_market_fallback",
        order_type="MARKET",
        reference_price=ltp,
        reference_price_age_sec=float(args.reference_price_age_sec),
        market_protection=float(args.market_protection),
        product=product,
        exchange=exchange,
    )
    sell_argv = _real_order_argv(
        session_id=session_id,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        role="manual_flatten",
        order_type="LIMIT",
        price=float(exit_intent.price or 0.0),
        reference_price=ltp,
        reference_price_age_sec=float(args.reference_price_age_sec),
        max_slippage_pct=max_slippage_pct,
        product=product,
        exchange=exchange,
    )
    sl_argv = _real_order_argv(
        session_id=session_id,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        role="manual_stop_loss",
        order_type="SL",
        price=stop_limit,
        trigger_price=stop_trigger,
        reference_price=ltp,
        reference_price_age_sec=float(args.reference_price_age_sec),
        product=product,
        exchange=exchange,
    )
    payload: dict[str, Any] = {
        "places_real_orders": False,
        "session_id": session_id,
        "symbol": symbol,
        "exchange": exchange,
        "quantity": quantity,
        "product": product,
        "ltp": ltp,
        "ltp_fetched_at_utc": fetched_at,
        "estimated_notional": round(ltp * quantity, 2),
        "guardrails": guardrails,
        "buy_limit": {
            "price": buy_price,
            "argv": buy_argv,
            "command": shlex.join(buy_argv),
        },
        "market_buy_fallback": {
            "market_protection": float(args.market_protection),
            "requires_allowed_order_type": "MARKET",
            "warning": (
                "Use only if LIMIT does not fill and Doppler allows MARKET in "
                "CPR_ZERODHA_REAL_ALLOWED_ORDER_TYPES."
            ),
            "argv": market_buy_argv,
            "command": shlex.join(market_buy_argv),
        },
        "protected_sell_limit": {
            "price": exit_intent.price,
            "max_slippage_pct": max_slippage_pct,
            "argv": sell_argv,
            "command": shlex.join(sell_argv),
        },
        "stop_loss_limit": {
            "trigger_price": stop_trigger,
            "price": stop_limit,
            "warning": (
                "If you place this actual SL order and later manually sell, cancel the pending "
                "SL order first to avoid unintended exposure."
            ),
            "argv": sl_argv,
            "command": shlex.join(sl_argv),
        },
        "post_order_monitoring": {
            "sync_command": shlex.join(
                [
                    "doppler",
                    "run",
                    "--",
                    "uv",
                    "run",
                    "pivot-paper-trading",
                    "broker-sync-orders",
                    "--session-id",
                    session_id,
                ]
            ),
            "note": (
                "Run broker-sync-orders after each actual order. Use Kite Console for broker-side "
                "cancels until a guarded cancel CLI exists."
            ),
        },
    }
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_order(args: argparse.Namespace) -> None:
    order_id = _pdb().append_order_event(
        session_id=args.session_id,
        symbol=args.symbol,
        side=args.side,
        requested_qty=int(args.quantity),
        order_type=args.order_type,
        request_price=args.request_price,
        fill_qty=int(args.fill_qty) if args.fill_qty is not None else None,
        fill_price=args.fill_price,
        status=args.status,
        notes=args.notes,
    )
    print(json.dumps({"order_id": order_id}, default=str, indent=2))


async def _cmd_real_dry_run_order(args: argparse.Namespace) -> None:
    intent = _build_broker_order_intent(args)
    payload = await record_real_dry_run_order(
        paper_db=_pdb(),
        intent=intent,
        adapter=ZerodhaBrokerAdapter(mode="REAL_DRY_RUN"),
    )
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_real_order(args: argparse.Namespace) -> None:
    if not bool(getattr(args, "confirm_real_order", False)):
        raise SystemExit("--confirm-real-order is required for real Zerodha placement")
    intent = _build_broker_order_intent(args)
    payload = await record_real_order(
        paper_db=_pdb(),
        intent=intent,
        adapter=ZerodhaBrokerAdapter(
            mode="LIVE",
            allow_real_orders=True,
            kite_client=_get_kite_client(),
        ),
    )
    print(json.dumps(payload, default=str, indent=2))


def _build_broker_order_intent(args: argparse.Namespace) -> BrokerOrderIntent:
    return BrokerOrderIntent(
        session_id=args.session_id,
        symbol=args.symbol,
        side=args.side,
        quantity=int(args.quantity),
        role=args.role,
        position_id=args.position_id,
        signal_id=args.signal_id,
        order_type=args.order_type,
        price=args.price,
        trigger_price=args.trigger_price,
        reference_price=args.reference_price,
        reference_price_age_sec=args.reference_price_age_sec,
        max_slippage_pct=args.max_slippage_pct,
        market_protection=args.market_protection,
        product=args.product,
        exchange=args.exchange,
        variety=args.variety,
        validity=args.validity,
        tag=args.tag,
        event_time=args.event_time,
    )


def _fetch_ltp(kite: Any, *, exchange: str, symbol: str) -> float:
    key = f"{exchange}:{symbol}"
    data = kite.ltp([key])
    row = dict((data or {}).get(key) or {})
    price = row.get("last_price")
    if price is None:
        raise SystemExit(f"Kite LTP did not return last_price for {key}")
    return float(price)


def _ceil_to_tick(value: float, tick_size: float) -> float:
    return round(math.ceil((float(value) - 1e-12) / float(tick_size)) * float(tick_size), 4)


def _floor_to_tick(value: float, tick_size: float) -> float:
    return round(math.floor((float(value) + 1e-12) / float(tick_size)) * float(tick_size), 4)


def _real_order_argv(
    *,
    session_id: str,
    symbol: str,
    side: str,
    quantity: int,
    role: str,
    order_type: str,
    reference_price: float,
    reference_price_age_sec: float,
    product: str,
    exchange: str,
    price: float | None = None,
    trigger_price: float | None = None,
    max_slippage_pct: float | None = None,
    market_protection: float | None = None,
) -> list[str]:
    argv = [
        "doppler",
        "run",
        "--",
        "uv",
        "run",
        "pivot-paper-trading",
        "real-order",
        "--session-id",
        session_id,
        "--symbol",
        symbol,
        "--side",
        side,
        "--quantity",
        str(quantity),
        "--role",
        role,
        "--order-type",
        order_type,
        "--reference-price",
        f"{float(reference_price):.4f}",
        "--reference-price-age-sec",
        f"{float(reference_price_age_sec):.2f}",
        "--product",
        product,
        "--exchange",
        exchange,
    ]
    if price is not None:
        argv.extend(["--price", f"{float(price):.4f}"])
    if trigger_price is not None:
        argv.extend(["--trigger-price", f"{float(trigger_price):.4f}"])
    if max_slippage_pct is not None:
        argv.extend(["--max-slippage-pct", f"{float(max_slippage_pct):.2f}"])
    if market_protection is not None:
        argv.extend(["--market-protection", f"{float(market_protection):.2f}"])
    argv.append("--confirm-real-order")
    return argv


def _get_kite_client():
    from engine.kite_ingestion import get_kite_client

    return get_kite_client()


async def _cmd_close_position(args: argparse.Namespace) -> None:
    _pdb().close_position(
        position_id=str(args.position_id),
        exit_price=args.close_price,
        exit_reason="manual_close",
        pnl=args.realized_pnl or 0.0,
        closed_by=args.closed_by,
    )
    print(json.dumps({"position_id": args.position_id, "status": "CLOSED"}, default=str))
