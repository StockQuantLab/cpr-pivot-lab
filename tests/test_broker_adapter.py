from __future__ import annotations

import json

import pytest

from db.paper_db import PaperDB
from engine.broker_adapter import (
    BrokerOrderIntent,
    RealOrderPlacementDisabledError,
    ZerodhaBrokerAdapter,
    record_real_dry_run_order,
)
from engine.execution_safety import OrderRateGovernor


class _NoSleepGovernor(OrderRateGovernor):
    def __init__(self) -> None:
        super().__init__(rate_per_second=1000.0, burst_capacity=1000.0)
        self.acquired = 0

    async def acquire(self, tokens: float = 1.0) -> float:
        self.acquired += 1
        return 0.0


def test_zerodha_payload_contains_required_fields() -> None:
    intent = BrokerOrderIntent(
        session_id="CPR_LEVELS_LONG-2026-04-28-live-kite",
        symbol="sbin",
        side="buy",
        quantity=10,
        role="entry",
    )

    payload = intent.zerodha_payload()

    assert payload == {
        "variety": "regular",
        "exchange": "NSE",
        "tradingsymbol": "SBIN",
        "transaction_type": "BUY",
        "quantity": 10,
        "product": "MIS",
        "order_type": "MARKET",
        "validity": "DAY",
        "tag": "cpr-entry-cpr-levels",
    }


@pytest.mark.asyncio
async def test_real_dry_run_does_not_call_kite_place_order() -> None:
    class ExplodingKiteClient:
        def place_order(self, **kwargs):  # pragma: no cover - should never be reached
            raise AssertionError(f"place_order must not be called: {kwargs}")

    governor = _NoSleepGovernor()
    adapter = ZerodhaBrokerAdapter(
        mode="REAL_DRY_RUN",
        governor=governor,
        kite_client=ExplodingKiteClient(),
    )

    result = await adapter.place_order(
        BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="RELIANCE",
            side="SELL",
            quantity=2,
            role="manual_flatten",
        )
    )

    assert result.mode == "REAL_DRY_RUN"
    assert result.status == "DRY_RUN"
    assert result.exchange_order_id
    assert result.payload["tradingsymbol"] == "RELIANCE"
    assert governor.acquired == 1


@pytest.mark.asyncio
async def test_real_order_mode_is_blocked_even_with_client() -> None:
    adapter = ZerodhaBrokerAdapter(mode="LIVE", kite_client=object())

    with pytest.raises(RealOrderPlacementDisabledError):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="SBIN",
                side="BUY",
                quantity=1,
            )
        )


@pytest.mark.asyncio
async def test_zerodha_adapter_fetches_read_only_snapshots_without_placing_orders() -> None:
    class FakeKiteClient:
        def __init__(self) -> None:
            self.place_order_called = False

        def place_order(self, **kwargs):  # pragma: no cover - should never be reached
            self.place_order_called = True
            raise AssertionError(f"place_order must not be called: {kwargs}")

        def orders(self):
            return [
                {
                    "order_id": "kite-1",
                    "tradingsymbol": "SBIN",
                    "transaction_type": "BUY",
                    "quantity": 1,
                    "filled_quantity": 1,
                    "status": "COMPLETE",
                }
            ]

        def positions(self):
            return {
                "day": [
                    {
                        "tradingsymbol": "SBIN",
                        "quantity": 1,
                        "product": "MIS",
                        "exchange": "NSE",
                    }
                ]
            }

    kite = FakeKiteClient()
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", kite_client=kite)

    orders = await adapter.fetch_order_snapshots()
    positions = await adapter.fetch_position_snapshots()

    assert orders[0].order_id == "kite-1"
    assert orders[0].symbol == "SBIN"
    assert positions[0].symbol == "SBIN"
    assert positions[0].quantity == 1
    assert kite.place_order_called is False


@pytest.mark.asyncio
async def test_record_real_dry_run_order_writes_payload_and_dedupes(tmp_path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            quantity=3,
            role="entry",
            event_time="2026-04-28T09:20:00+05:30",
        )
        adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

        first = await record_real_dry_run_order(paper_db=db, intent=intent, adapter=adapter)
        second = await record_real_dry_run_order(paper_db=db, intent=intent, adapter=adapter)

        orders = db.get_session_orders("paper-live-1")
        assert second["order_id"] == first["order_id"]
        assert len(orders) == 1
        assert orders[0].status == "PENDING"
        assert orders[0].broker_mode == "REAL_DRY_RUN"
        assert orders[0].notes == "REAL_DRY_RUN"
        assert orders[0].exchange_order_id == first["exchange_order_id"]
        payload = json.loads(str(orders[0].broker_payload))
        assert payload["tradingsymbol"] == "TCS"
        assert payload["quantity"] == 3
    finally:
        db.close()
