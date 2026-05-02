"""Broker and order maintenance commands for the paper trading CLI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from db.paper_db import get_paper_db
from engine.broker_adapter import (
    BrokerOrderIntent,
    ZerodhaBrokerAdapter,
    record_real_dry_run_order,
    record_real_order,
)
from engine.broker_reconciliation import (
    BrokerOrderSnapshot,
    BrokerPositionSnapshot,
    PilotGuardrails,
    reconcile_local_to_broker,
)


def _pdb():
    return get_paper_db()


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
