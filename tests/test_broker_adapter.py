from __future__ import annotations

import json
import math

import pytest

from db.paper_db import PaperDB
from engine.broker_adapter import (
    BrokerOrderIntent,
    OrderSafetyError,
    PaperBrokerAdapter,
    RealOrderPlacementDisabledError,
    ZerodhaBrokerAdapter,
    build_protected_flatten_intent,
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
        "market_protection": 2.0,
    }


@pytest.mark.parametrize(
    ("intent", "message"),
    [
        (
            BrokerOrderIntent(session_id="s", symbol="", side="BUY", quantity=1),
            "symbol is required",
        ),
        (
            BrokerOrderIntent(session_id="s", symbol="SBIN", side="HOLD", quantity=1),
            "side must be BUY or SELL",
        ),
        (
            BrokerOrderIntent(session_id="s", symbol="SBIN", side="BUY", quantity=0),
            "quantity must be positive",
        ),
        (
            BrokerOrderIntent(
                session_id="s", symbol="SBIN", side="BUY", quantity=1, order_type="BOGUS"
            ),
            "unsupported order_type",
        ),
        (
            BrokerOrderIntent(
                session_id="s", symbol="SBIN", side="BUY", quantity=1, order_type="LIMIT"
            ),
            "LIMIT orders require price",
        ),
        (
            BrokerOrderIntent(
                session_id="s",
                symbol="SBIN",
                side="BUY",
                quantity=1,
                order_type="SL-M",
            ),
            "SL-M orders require trigger_price",
        ),
    ],
)
def test_broker_order_intent_rejects_malformed_orders(
    intent: BrokerOrderIntent,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        intent.normalized()


def test_broker_order_intent_rejects_non_finite_prices() -> None:
    intent = BrokerOrderIntent(
        session_id="s",
        symbol="SBIN",
        side="SELL",
        quantity=1,
        order_type="LIMIT",
        price=math.nan,
    )

    with pytest.raises(ValueError, match="price must be a positive finite number"):
        intent.normalized()


@pytest.mark.parametrize("market_protection", [None, 0.0])
def test_market_orders_require_positive_market_protection(
    market_protection: float | None,
) -> None:
    intent = BrokerOrderIntent(
        session_id="s",
        symbol="SBIN",
        side="BUY",
        quantity=1,
        order_type="MARKET",
        market_protection=market_protection,
    )

    with pytest.raises(OrderSafetyError, match="market_protection"):
        intent.validate_for_broker()


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
            order_type="LIMIT",
            price=98.0,
            reference_price=100.0,
            reference_price_age_sec=1.0,
        )
    )

    assert result.mode == "REAL_DRY_RUN"
    assert result.status == "DRY_RUN"
    assert result.exchange_order_id
    assert result.payload["tradingsymbol"] == "RELIANCE"
    assert governor.acquired == 1


@pytest.mark.asyncio
async def test_zerodha_rejects_zero_price_limit_orders() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(ValueError, match="price must be a positive finite number"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="SELL",
                quantity=2,
                role="manual_flatten",
                order_type="LIMIT",
                price=0.0,
                reference_price=100.0,
                reference_price_age_sec=1.0,
            )
        )


@pytest.mark.asyncio
async def test_zerodha_rejects_raw_market_flatten_orders() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(OrderSafetyError, match="protected LIMIT"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="SELL",
                quantity=2,
                role="manual_flatten",
                order_type="MARKET",
            )
        )


@pytest.mark.asyncio
async def test_zerodha_rejects_stale_flatten_reference_price() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(OrderSafetyError, match="stale"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="SELL",
                quantity=2,
                role="manual_flatten",
                order_type="LIMIT",
                price=98.0,
                reference_price=100.0,
                reference_price_age_sec=30.0,
            )
        )


@pytest.mark.asyncio
async def test_zerodha_rejects_sell_flatten_below_slippage_floor() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(OrderSafetyError, match="protected floor"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="SELL",
                quantity=2,
                role="manual_flatten",
                order_type="LIMIT",
                price=90.0,
                reference_price=100.0,
                reference_price_age_sec=1.0,
                max_slippage_pct=2.0,
            )
        )


@pytest.mark.asyncio
async def test_zerodha_rejects_buy_flatten_above_slippage_cap() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(OrderSafetyError, match="protected cap"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="BUY",
                quantity=2,
                role="emergency_flatten",
                order_type="LIMIT",
                price=110.0,
                reference_price=100.0,
                reference_price_age_sec=1.0,
                max_slippage_pct=2.0,
            )
        )


@pytest.mark.asyncio
async def test_protected_flatten_intent_builds_bounded_limit_order() -> None:
    intent = build_protected_flatten_intent(
        session_id="paper-live-1",
        symbol="reliance",
        side="SELL",
        quantity=2,
        latest_price=100.0,
        quote_age_sec=1.0,
        max_slippage_pct=2.0,
    )
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    result = await adapter.place_order(intent)

    assert result.payload["order_type"] == "LIMIT"
    assert result.payload["transaction_type"] == "SELL"
    assert result.payload["price"] == 98.0


def test_protected_buy_flatten_intent_builds_limit_cap() -> None:
    intent = build_protected_flatten_intent(
        session_id="paper-live-1",
        symbol="reliance",
        side="BUY",
        quantity=2,
        latest_price=100.0,
        quote_age_sec=1.0,
        max_slippage_pct=2.0,
        tick_size=0.05,
    )

    payload = intent.zerodha_payload()

    assert payload["transaction_type"] == "BUY"
    assert payload["order_type"] == "LIMIT"
    assert payload["price"] == 102.0


def test_protected_flatten_intent_rejects_invalid_side() -> None:
    with pytest.raises(ValueError, match="side must be BUY or SELL"):
        build_protected_flatten_intent(
            session_id="paper-live-1",
            symbol="RELIANCE",
            side="HOLD",
            quantity=2,
            latest_price=100.0,
            quote_age_sec=1.0,
        )


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
async def test_real_order_mode_still_blocked_even_when_allow_flag_true() -> None:
    adapter = ZerodhaBrokerAdapter(mode="LIVE", allow_real_orders=True, kite_client=object())

    with pytest.raises(RealOrderPlacementDisabledError, match="not implemented"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="SBIN",
                side="BUY",
                quantity=1,
            )
        )


@pytest.mark.asyncio
async def test_paper_adapter_records_payload_without_broker_ids() -> None:
    result = await PaperBrokerAdapter().place_order(
        BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="sbin",
            side="buy",
            quantity=1,
        )
    )

    assert result.broker == "paper"
    assert result.mode == "PAPER"
    assert result.status == "FILLED"
    assert result.payload["tradingsymbol"] == "SBIN"
    assert result.exchange_order_id is None


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
async def test_zerodha_snapshot_fetchers_return_empty_without_client() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN")

    assert await adapter.fetch_order_snapshots() == []
    assert await adapter.fetch_position_snapshots() == []


@pytest.mark.asyncio
async def test_zerodha_position_snapshots_accept_list_payload() -> None:
    class FakeKiteClient:
        def positions(self):
            return [
                {"tradingsymbol": "SBIN", "quantity": 0},
                {"tradingsymbol": "TCS", "quantity": -2},
            ]

        def orders(self):  # pragma: no cover - not used by this test
            return []

    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", kite_client=FakeKiteClient())

    positions = await adapter.fetch_position_snapshots()

    assert len(positions) == 1
    assert positions[0].symbol == "TCS"
    assert positions[0].quantity == -2


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
