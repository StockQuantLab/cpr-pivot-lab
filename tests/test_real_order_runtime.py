from __future__ import annotations

import json

import pytest

from db.paper_db import PaperDB
from engine.broker_adapter import BrokerExecutionResult, OrderSafetyError
from engine.broker_reconciliation import BrokerOrderSnapshot
from engine.real_order_runtime import (
    RealOrderRouter,
    RealOrderRuntimeConfig,
    build_real_order_router,
)


class _FakeLiveAdapter:
    mode = "LIVE"

    def __init__(self) -> None:
        self.intents = []
        self.cancelled = []

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

    async def fetch_order_snapshots(self):
        snapshots = []
        for idx, intent in enumerate(self.intents, start=1):
            normalized = intent.normalized()
            if normalized.order_type == "SL-M" and normalized.role == "protective_sl":
                status = "TRIGGER PENDING"
                filled_quantity = 0
                average_price = None
            else:
                status = "COMPLETE"
                filled_quantity = normalized.quantity
                average_price = normalized.price or normalized.reference_price
            snapshots.append(
                BrokerOrderSnapshot(
                    order_id=f"kite-{idx}",
                    symbol=normalized.symbol,
                    side=normalized.side,
                    quantity=normalized.quantity,
                    filled_quantity=filled_quantity,
                    average_price=average_price,
                    status=status,
                    broker_payload={
                        "order_id": f"kite-{idx}",
                        "tradingsymbol": normalized.symbol,
                        "transaction_type": normalized.side,
                        "quantity": normalized.quantity,
                        "filled_quantity": filled_quantity,
                        "average_price": average_price,
                        "status": status,
                    },
                )
            )
        return snapshots

    async def cancel_order(self, *, order_id: str, variety: str = "regular"):
        self.cancelled.append((order_id, variety))
        return order_id


class _FailingProtectiveAdapter(_FakeLiveAdapter):
    async def place_order(self, intent):
        self.intents.append(intent)
        if intent.normalized().role == "protective_sl":
            raise OrderSafetyError("protective SL rejected")
        return BrokerExecutionResult(
            broker="zerodha",
            mode="LIVE",
            status="PLACED",
            payload=intent.zerodha_payload(),
            idempotency_key=intent.idempotency_key(),
            exchange_order_id=f"kite-{len(self.intents)}",
        )


class _RejectedProtectiveSnapshotAdapter(_FakeLiveAdapter):
    async def fetch_order_snapshots(self):
        snapshots = []
        for idx, intent in enumerate(self.intents, start=1):
            normalized = intent.normalized()
            if normalized.role == "protective_sl":
                status = "REJECTED"
                filled_quantity = 0
                average_price = None
                status_message = "RMS rejected protective SL"
            else:
                status = "COMPLETE"
                filled_quantity = normalized.quantity
                average_price = normalized.price or normalized.reference_price
                status_message = None
            snapshots.append(
                BrokerOrderSnapshot(
                    order_id=f"kite-{idx}",
                    symbol=normalized.symbol,
                    side=normalized.side,
                    quantity=normalized.quantity,
                    filled_quantity=filled_quantity,
                    average_price=average_price,
                    status=status,
                    broker_payload={
                        "order_id": f"kite-{idx}",
                        "tradingsymbol": normalized.symbol,
                        "transaction_type": normalized.side,
                        "quantity": normalized.quantity,
                        "filled_quantity": filled_quantity,
                        "average_price": average_price,
                        "status": status,
                        "status_message": status_message,
                    },
                )
            )
        return snapshots


class _ExitNeverFillsAdapter(_FakeLiveAdapter):
    async def fetch_order_snapshots(self):
        snapshots = []
        for idx, intent in enumerate(self.intents, start=1):
            normalized = intent.normalized()
            if idx <= 2:
                status = "TRIGGER PENDING" if normalized.role == "protective_sl" else "COMPLETE"
                filled_quantity = 0 if normalized.role == "protective_sl" else normalized.quantity
                average_price = normalized.price or normalized.reference_price
            else:
                status = "OPEN"
                filled_quantity = 0
                average_price = None
            snapshots.append(
                BrokerOrderSnapshot(
                    order_id=f"kite-{idx}",
                    symbol=normalized.symbol,
                    side=normalized.side,
                    quantity=normalized.quantity,
                    filled_quantity=filled_quantity,
                    average_price=average_price,
                    status=status,
                    broker_payload={
                        "order_id": f"kite-{idx}",
                        "tradingsymbol": normalized.symbol,
                        "transaction_type": normalized.side,
                        "quantity": normalized.quantity,
                        "filled_quantity": filled_quantity,
                        "average_price": average_price,
                        "status": status,
                    },
                )
            )
        return snapshots


def test_real_order_runtime_config_rejects_explicit_zero_cash_budget() -> None:
    with pytest.raises(OrderSafetyError, match="cash budget must be positive"):
        RealOrderRuntimeConfig.from_mapping(
            {
                "enabled": True,
                "fixed_quantity": 1,
                "max_positions": 1,
                "cash_budget": 0,
                "require_account_cash_check": False,
            }
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
            stop_loss=735.0,
            event_time="2026-05-04T09:20:00+05:30",
        )

        assert meta["real_order_qty"] == 1
        assert meta["real_entry_order_id"] == "kite-1"
        assert [intent.order_type for intent in adapter.intents] == ["MARKET", "SL-M"]
        assert adapter.intents[0].quantity == 1
        assert adapter.intents[1].trigger_price == pytest.approx(735.0)
        orders = db.get_session_orders("paper-live-1")
        assert len(orders) == 2
        assert orders[0].broker_mode == "LIVE"
        assert orders[0].status == "FILLED"
        assert orders[1].status == "PENDING"
        payload = json.loads(str(orders[0].broker_payload))
        assert payload["tradingsymbol"] == "SBIN"
        assert payload["order_type"] == "MARKET"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_can_size_entry_from_cash_budget(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FakeLiveAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                sizing_mode="CASH_BUDGET",
                fixed_quantity=1,
                max_positions=1,
                cash_budget=10_000.0,
                entry_order_type="LIMIT",
                entry_max_slippage_pct=0.5,
            ),
            adapter=adapter,
            account_available_cash=20_000.0,
        )

        meta = await router.place_entry(
            session_id="paper-live-1",
            symbol="ITC",
            direction="LONG",
            reference_price=311.5,
            stop_loss=305.0,
            event_time="2026-05-06T09:20:00+05:30",
        )

        # LIMIT sizing uses the protected marketable limit price, not raw LTP.
        assert meta["real_order_qty"] == 31
        assert meta["real_sizing_mode"] == "CASH_BUDGET"
        assert adapter.intents[0].quantity == 31
        assert adapter.intents[0].price == pytest.approx(313.06)
        assert adapter.intents[1].trigger_price == pytest.approx(306.56)
        assert meta["real_model_stop_loss"] == pytest.approx(305.0)
        assert meta["real_protective_sl_trigger_price"] == pytest.approx(306.56)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_rejects_zero_fill_before_registering_exposure(
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

        async def _zero_fill(**_: object) -> dict[str, object]:
            return {"filled_qty": 0, "fill_price": 750.0, "status": "COMPLETE"}

        monkeypatch.setattr(router, "_await_order_fill", _zero_fill)

        with pytest.raises(OrderSafetyError, match="zero fill quantity"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="LONG",
                reference_price=750.0,
                stop_loss=735.0,
                event_time="2026-05-04T09:20:00+05:30",
            )

        assert router._open_real_positions == 0
        assert router._used_cash_notional == pytest.approx(0.0)
        assert [intent.normalized().role for intent in adapter.intents] == ["entry"]
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
async def test_real_order_router_cancels_protective_sl_before_target_exit(
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

        await router.place_entry(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            reference_price=750.0,
            stop_loss=735.0,
            event_time="2026-05-04T09:20:00+05:30",
        )
        await router.place_exit(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            position_id="pos-1",
            quantity=1,
            reference_price=760.0,
            quote_age_sec=1.0,
            role="exit:TARGET",
            event_time="2026-05-04T09:25:00+05:30",
            protective_order_id="kite-2",
        )

        assert adapter.cancelled == [("kite-2", "regular")]
        orders = db.get_session_orders("paper-live-1")
        by_exchange_id = {order.exchange_order_id: order for order in orders}
        assert by_exchange_id["kite-2"].status == "CANCELLED"
        assert by_exchange_id["kite-3"].status == "FILLED"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_flattens_entry_when_protective_sl_fails(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _FailingProtectiveAdapter()
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

        with pytest.raises(OrderSafetyError, match="entry was flattened"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="LONG",
                reference_price=750.0,
                stop_loss=735.0,
                event_time="2026-05-04T09:20:00+05:30",
            )

        assert [intent.normalized().role for intent in adapter.intents] == [
            "entry",
            "protective_sl",
            "emergency_flatten:entry_protection_failed",
        ]
        assert router._open_real_positions == 0
        assert router._used_cash_notional == pytest.approx(0.0)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_flattens_entry_when_protective_sl_is_rejected_after_submit(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _RejectedProtectiveSnapshotAdapter()
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

        with pytest.raises(OrderSafetyError, match="entry was flattened"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="LONG",
                reference_price=750.0,
                stop_loss=735.0,
                event_time="2026-05-04T09:20:00+05:30",
            )

        assert [intent.normalized().role for intent in adapter.intents] == [
            "entry",
            "protective_sl",
            "emergency_flatten:entry_protection_failed",
        ]
        assert router._open_real_positions == 0
        assert router._used_cash_notional == pytest.approx(0.0)
        by_exchange_id = {
            str(order.exchange_order_id): order for order in db.get_session_orders("paper-live-1")
        }
        assert by_exchange_id["kite-2"].status == "REJECTED"
        assert by_exchange_id["kite-3"].status == "FILLED"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_keeps_exposure_reserved_when_exit_does_not_fill(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = _ExitNeverFillsAdapter()
    monkeypatch.setattr("engine.real_order_runtime.get_paper_db", lambda: db)
    try:
        router = RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=1_000.0,
                fill_wait_timeout_sec=0.0,
            ),
            adapter=adapter,
            account_available_cash=1_000.0,
        )
        await router.place_entry(
            session_id="paper-live-1",
            symbol="SBIN",
            direction="LONG",
            reference_price=750.0,
            stop_loss=735.0,
            event_time="2026-05-04T09:20:00+05:30",
        )

        with pytest.raises(OrderSafetyError, match="did not fill"):
            await router.place_exit(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="LONG",
                position_id="pos-1",
                quantity=1,
                reference_price=760.0,
                quote_age_sec=1.0,
                role="exit:TARGET",
                event_time="2026-05-04T09:25:00+05:30",
            )

        assert router._open_real_positions == 1
        assert router._used_cash_notional == pytest.approx(753.75)
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
            stop_loss=735.0,
            event_time=None,
        )

        with pytest.raises(OrderSafetyError, match="max open positions"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="RELIANCE",
                direction="LONG",
                reference_price=1400.0,
                stop_loss=1380.0,
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
                stop_loss=735.0,
                event_time=None,
            )
    finally:
        db.close()


@pytest.mark.asyncio
async def test_real_order_router_blocks_duplicate_symbol_exposure(
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
                max_positions=2,
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
            stop_loss=735.0,
            event_time=None,
        )

        with pytest.raises(OrderSafetyError, match="symbol already has open exposure"):
            await router.place_entry(
                session_id="paper-live-1",
                symbol="SBIN",
                direction="SHORT",
                reference_price=750.0,
                stop_loss=765.0,
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


def test_real_order_router_rejects_missing_required_account_cash() -> None:
    with pytest.raises(OrderSafetyError, match="available_cash could not be fetched"):
        RealOrderRouter(
            RealOrderRuntimeConfig(
                enabled=True,
                fixed_quantity=1,
                max_positions=1,
                cash_budget=10_000.0,
                require_account_cash_check=True,
            ),
            adapter=_FakeLiveAdapter(),
            account_available_cash=None,
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
            stop_loss=735.0,
            event_time="2026-05-04T09:20:00+05:30",
        )
        await router.place_entry(
            session_id="paper-live-1",
            symbol="RELIANCE",
            direction="LONG",
            reference_price=1400.0,
            stop_loss=1380.0,
            event_time="2026-05-04T09:20:00+05:30",
        )

        orders = db.get_session_orders("paper-live-1")
        assert len(orders) == 4
        assert {order.broker_mode for order in orders} == {"REAL_DRY_RUN"}
        assert all(order.broker_latency_ms == 0.0 for order in orders)
    finally:
        db.close()
