from __future__ import annotations

import json

import pytest

from db.paper_db import PaperDB
from engine.broker_adapter import BrokerExecutionResult, OrderSafetyError
from engine.real_order_runtime import (
    RealOrderRouter,
    RealOrderRuntimeConfig,
    build_real_order_router,
)


class _FakeLiveAdapter:
    mode = "LIVE"

    def __init__(self) -> None:
        self.intents = []

    async def place_order(self, intent):
        self.intents.append(intent)
        return BrokerExecutionResult(
            broker="zerodha",
            mode="LIVE",
            status="PLACED",
            payload=intent.zerodha_payload(),
            idempotency_key=intent.idempotency_key(),
            exchange_order_id=f"kite-{len(self.intents)}",
        )


@pytest.mark.asyncio
async def test_real_order_router_places_fixed_quantity_market_entry(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FakeLiveAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=1_000.0,
                entry_order_type="MARKET",
            ),
            adapter=adapter,
            account_available_cash=1_000.0,
        )

        meta = await router.place_entry(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            reference_price=750.0,
            event_time="2026-05-04T09:20:00+05:30",
        )

        assert meta["real_order_qty"] == 1
        assert meta["real_entry_order_id"] == "kite-1"
        assert adapter.intents[0].order_type == "MARKET"
        assert adapter.intents[0].quantity == 1
        orders = db.get_session_orders("paper-live-1")
        assert len(orders) == 1
        assert orders[0].broker_mode == "LIVE"
        payload = json.loads(str(orders[0].broker_payload))
        assert payload["tradingsymbol"] == "SBIN"
        assert payload["order_type"] == "MARKET"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_uses_protected_limit_for_exit(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FakeLiveAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=1_000.0,
            ),
            adapter=adapter,
            account_available_cash=1_000.0,
        )

        await router.place_exit(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            position_id="pos-1",
            quantity=1,
            reference_price=750.0,
            quote_age_sec=1.0,
            role="exit:SL_HIT",
            event_time="2026-05-04T09:25:00+05:30",
        )

        intent = adapter.intents[0]
        assert intent.order_type == "LIMIT"
        assert intent.side == "SELL"
        assert intent.price == pytest.approx(735.0)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_blocks_more_than_configured_positions(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FakeLiveAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=2_000.0,
            ),
            adapter=adapter,
            account_available_cash=2_000.0,
        )
        await router.place_entry(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            reference_price=750.0,
            event_time=None,
        )

        with pytest.raises(OrderSafetyError, match="max open positions"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="RELIANCE",
                direction="LONG",
                reference_price=1400.0,
                event_time=None,
            )
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_blocks_cash_budget_overuse(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FakeLiveAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=2,
                max_positions=2,
                cash_budget=1_000.0,
            ),
            adapter=adapter,
            account_available_cash=1_000.0,
        )

        with pytest.raises(OrderSafetyError, match="cash budget exceeded"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="LONG",
                reference_price=750.0,
                event_time=None,
            )
    finally:
        db.close()


def test_real_order_router_rejects_budget_above_account_cash() -> None:
    with pytest.raises(OrderSafetyError, match="exceeds available cash"):
        RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=200_000.0,
            ),
            adapter=_FakeLiveAdapter(),
            account_available_cash=100_000.0,
        )


@pytest.mark.asyncio
async def test_real_dry_run_shadow_router_records_multiple_entries_without_kite(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    monkeypatch.setattr(
        "engine.real_order_runtime._get_kite_client",
        lambda: (_ for _ in ()).throw(AssertionError("Kite client must not be loaded")),
    )
    try:
        router = build_real_order_router(
            {
                "enabled": True,
                "fixed_quantity": 1,
                "max_positions": 1,
                "cash_budget": 100.0,
                "require_account_cash_check": False,
                "entry_order_type": "LIMIT",
                "adapter_mode": "REAL_DRY_RUN",
                "shadow": True,
            }
        )
        assert router is not None

        await router.place_entry(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            reference_price=750.0,
            event_time="2026-05-04T09:20:00+05:30",
        )
        await router.place_entry(
            session_id="paper-live-1",
            symbol="RELIANCE",
            direction="LONG",
            reference_price=1400.0,
            event_time="2026-05-04T09:20:00+05:30",
        )

        orders = db.get_session_orders("paper-live-1")
        assert len(orders) == 2
        assert {order.broker_mode for order in orders} == {"REAL_DRY_RUN"}
        assert all(order.broker_latency_ms == 0.0 for order in orders)
    finally:
        db.close()
