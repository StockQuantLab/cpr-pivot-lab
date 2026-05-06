from __future__ import annotations

import json
import math

import pytest

from db.paper_db import PaperDB
from engine.broker_adapter import (
    BrokerOrderIntent,
    OrderSafetyError,
    PaperBrokerAdapter,
    RealOrderGuardConfig,
    RealOrderPlacementDisabledError,
    ZerodhaBrokerAdapter,
    build_protected_flatten_intent,
    record_real_dry_run_order,
    record_real_order,
)
from engine.broker_reconciliation import BrokerOrderSnapshot
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


def test_manual_stop_loss_role_allows_slm_order() -> None:
    intent = BrokerOrderIntent(
        session_id="s",
        symbol="SBIN",
        side="SELL",
        quantity=1,
        role="manual_stop_loss",
        order_type="SL-M",
        trigger_price=100.0,
        reference_price=105.0,
        reference_price_age_sec=1.0,
        market_protection=5.0,
    )

    payload = intent.validate_for_broker().zerodha_payload()

    assert payload["order_type"] == "SL-M"
    assert payload["trigger_price"] == 100.0
    assert payload["market_protection"] == 5.0


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
async def test_zerodha_rejects_stale_normal_exit_reference_price() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    with pytest.raises(OrderSafetyError, match="stale"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="RELIANCE",
                side="SELL",
                quantity=2,
                role="exit:target",
                order_type="LIMIT",
                price=98.0,
                reference_price=100.0,
                reference_price_age_sec=30.0,
            )
        )


@pytest.mark.asyncio
async def test_zerodha_allows_stale_emergency_flatten_reference_price() -> None:
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    result = await adapter.place_order(
        BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="RELIANCE",
            side="SELL",
            quantity=2,
            role="manual_flatten:feed_stale",
            order_type="LIMIT",
            price=98.0,
            reference_price=100.0,
            reference_price_age_sec=300.0,
        )
    )

    assert result.status == "DRY_RUN"


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


@pytest.mark.asyncio
async def test_protected_sell_flatten_rounds_inside_slippage_floor() -> None:
    intent = build_protected_flatten_intent(
        session_id="paper-live-1",
        symbol="reliance",
        side="SELL",
        quantity=2,
        latest_price=504.4166,
        quote_age_sec=1.0,
        max_slippage_pct=2.0,
        tick_size=0.05,
    )
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    result = await adapter.place_order(intent)

    assert result.payload["price"] == 494.35


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


@pytest.mark.asyncio
async def test_protected_buy_flatten_rounds_inside_slippage_cap() -> None:
    intent = build_protected_flatten_intent(
        session_id="paper-live-1",
        symbol="reliance",
        side="BUY",
        quantity=2,
        latest_price=1759.55,
        quote_age_sec=1.0,
        max_slippage_pct=2.0,
        tick_size=0.05,
    )
    adapter = ZerodhaBrokerAdapter(mode="REAL_DRY_RUN", governor=_NoSleepGovernor())

    result = await adapter.place_order(intent)

    assert result.payload["price"] == 1794.7


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
async def test_real_order_mode_is_blocked_without_code_gate() -> None:
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
async def test_real_order_mode_is_blocked_without_env_gate() -> None:
    class FakeKiteClient:
        def place_order(self, **kwargs):  # pragma: no cover - should never be reached
            raise AssertionError(f"place_order must not be called: {kwargs}")

    adapter = ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=FakeKiteClient(),
        guard_config=RealOrderGuardConfig(enabled=False),
    )

    with pytest.raises(RealOrderPlacementDisabledError, match="disabled"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="SBIN",
                side="BUY",
                quantity=1,
                order_type="LIMIT",
                price=700.0,
                reference_price=700.0,
                reference_price_age_sec=1.0,
            )
        )


@pytest.mark.asyncio
async def test_real_order_mode_requires_acknowledgement() -> None:
    class FakeKiteClient:
        def place_order(self, **kwargs):  # pragma: no cover - should never be reached
            raise AssertionError(f"place_order must not be called: {kwargs}")

    adapter = ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=FakeKiteClient(),
        guard_config=RealOrderGuardConfig(enabled=True, acknowledgement=""),
    )

    with pytest.raises(RealOrderPlacementDisabledError, match="acknowledge"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="SBIN",
                side="BUY",
                quantity=1,
                order_type="LIMIT",
                price=700.0,
                reference_price=700.0,
                reference_price_age_sec=1.0,
            )
        )


@pytest.mark.asyncio
async def test_real_order_mode_places_kite_order_when_all_gates_pass() -> None:
    class FakeKiteClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def place_order(self, **kwargs):
            self.calls.append(kwargs)
            return "kite-real-1"

    kite = FakeKiteClient()
    adapter = ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=kite,
        governor=_NoSleepGovernor(),
        guard_config=RealOrderGuardConfig(
            enabled=True,
            acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
            max_quantity=1,
            max_notional=1_000.0,
            allowed_products=frozenset({"MIS"}),
            allowed_order_types=frozenset({"LIMIT"}),
        ),
    )

    result = await adapter.place_order(
        BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="SBIN",
            side="BUY",
            quantity=1,
            order_type="LIMIT",
            price=700.0,
            reference_price=700.0,
            reference_price_age_sec=1.0,
            market_protection=2.0,
        )
    )

    assert result.status == "PLACED"
    assert result.exchange_order_id == "kite-real-1"
    assert kite.calls == [
        {
            "variety": "regular",
            "exchange": "NSE",
            "tradingsymbol": "SBIN",
            "transaction_type": "BUY",
            "quantity": 1,
            "product": "MIS",
            "order_type": "LIMIT",
            "validity": "DAY",
            "tag": "cpr-manual-paper-liv",
            "price": 700.0,
        }
    ]


@pytest.mark.asyncio
async def test_real_order_mode_rejects_disallowed_market_order() -> None:
    class FakeKiteClient:
        def place_order(self, **kwargs):  # pragma: no cover - should never be reached
            raise AssertionError(f"place_order must not be called: {kwargs}")

    adapter = ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=FakeKiteClient(),
        guard_config=RealOrderGuardConfig(
            enabled=True,
            acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
            allowed_order_types=frozenset({"LIMIT"}),
        ),
    )

    with pytest.raises(OrderSafetyError, match="order_type MARKET is not allowed"):
        await adapter.place_order(
            BrokerOrderIntent(
                session_id="paper-live-1",
                symbol="SBIN",
                side="BUY",
                quantity=1,
                order_type="MARKET",
                reference_price=700.0,
                reference_price_age_sec=1.0,
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


@pytest.mark.asyncio
async def test_record_real_order_writes_submitted_broker_order(tmp_path) -> None:
    class FakeKiteClient:
        def place_order(self, **kwargs):
            return "kite-real-2"

    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            quantity=1,
            role="manual",
            order_type="LIMIT",
            price=3500.0,
            reference_price=3500.0,
            reference_price_age_sec=1.0,
            event_time="2026-04-28T09:20:00+05:30",
        )
        adapter = ZerodhaBrokerAdapter(
            mode="LIVE",
            allow_real_orders=True,
            kite_client=FakeKiteClient(),
            governor=_NoSleepGovernor(),
            guard_config=RealOrderGuardConfig(
                enabled=True,
                acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
                max_quantity=1,
                max_notional=4_000.0,
                allowed_order_types=frozenset({"LIMIT"}),
            ),
        )

        payload = await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        orders = db.get_session_orders("paper-live-1")
        assert payload["exchange_order_id"] == "kite-real-2"
        assert len(orders) == 1
        assert orders[0].status == "PENDING"
        assert orders[0].broker_mode == "LIVE"
        assert orders[0].notes == "LIVE"
        assert orders[0].exchange_order_id == "kite-real-2"
        assert orders[0].broker_latency_ms is not None
        assert orders[0].broker_latency_ms >= 0
        broker_payload = json.loads(str(orders[0].broker_payload))
        assert broker_payload["tradingsymbol"] == "TCS"
        assert broker_payload["quantity"] == 1
    finally:
        db.close()


@pytest.mark.asyncio
async def test_live_slm_order_uses_kite_post_for_market_protection() -> None:
    class FakeKiteClient:
        def __init__(self) -> None:
            self.place_calls = 0
            self.post_calls: list[dict] = []

        def place_order(self, **kwargs):
            self.place_calls += 1
            raise AssertionError("SL-M with market_protection must use Kite _post")

        def _post(self, route, *, url_args, params):
            self.post_calls.append(
                {"route": route, "url_args": dict(url_args), "params": dict(params)}
            )
            return {"order_id": "kite-slm-1"}

    kite = FakeKiteClient()
    intent = BrokerOrderIntent(
        session_id="paper-live-1",
        symbol="TCS",
        side="SELL",
        quantity=1,
        role="protective_sl",
        order_type="SL-M",
        trigger_price=3450.0,
        reference_price=3500.0,
        reference_price_age_sec=1.0,
        market_protection=5.0,
        event_time="2026-04-28T09:20:00+05:30",
    )
    adapter = ZerodhaBrokerAdapter(
        mode="LIVE",
        allow_real_orders=True,
        kite_client=kite,
        governor=_NoSleepGovernor(),
        guard_config=RealOrderGuardConfig(
            enabled=True,
            acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
            max_quantity=1,
            max_notional=4_000.0,
            allowed_order_types=frozenset({"SL-M"}),
        ),
    )

    result = await adapter.place_order(intent)

    assert result.exchange_order_id == "kite-slm-1"
    assert kite.place_calls == 0
    assert len(kite.post_calls) == 1
    call = kite.post_calls[0]
    assert call["route"] == "order.place"
    assert call["url_args"] == {"variety": "regular"}
    assert call["params"]["order_type"] == "SL-M"
    assert call["params"]["market_protection"] == 5.0


@pytest.mark.asyncio
async def test_record_real_order_blocks_retry_for_pending_live_intent_without_broker_id(
    tmp_path,
) -> None:
    class FakeKiteClient:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, **kwargs):
            self.calls += 1
            raise AssertionError("retry must not submit duplicate live order")

        def orders(self):
            return []

    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    kite = FakeKiteClient()
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            quantity=1,
            role="manual",
            order_type="LIMIT",
            price=3500.0,
            reference_price=3500.0,
            reference_price_age_sec=1.0,
            event_time="2026-04-28T09:20:00+05:30",
        )
        adapter = ZerodhaBrokerAdapter(
            mode="LIVE",
            allow_real_orders=True,
            kite_client=kite,
            governor=_NoSleepGovernor(),
            guard_config=RealOrderGuardConfig(
                enabled=True,
                acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
                max_quantity=1,
                max_notional=4_000.0,
                allowed_order_types=frozenset({"LIMIT"}),
            ),
        )
        db.append_order_event(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            order_type="LIMIT",
            requested_qty=1,
            request_price=3500.0,
            fill_qty=0,
            fill_price=None,
            status="PENDING",
            exchange_order_id=None,
            idempotency_key=intent.idempotency_key(),
            notes="PENDING_DISPATCH",
            broker_mode="LIVE",
            broker_payload=json.dumps(intent.zerodha_payload()),
        )

        with pytest.raises(OrderSafetyError, match="manual broker reconciliation"):
            await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        assert kite.calls == 0
    finally:
        db.close()


@pytest.mark.asyncio
async def test_record_real_order_rejected_pre_dispatch_intent_can_retry(tmp_path) -> None:
    class FakeKiteClient:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("Kite rejected SL-M: missing market protection")
            return "kite-retry-1"

        def orders(self):
            return []

    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    kite = FakeKiteClient()
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="SELL",
            quantity=1,
            role="protective_sl",
            order_type="SL-M",
            trigger_price=3450.0,
            reference_price=3500.0,
            reference_price_age_sec=1.0,
            market_protection=5.0,
            event_time="2026-04-28T09:20:00+05:30",
        )
        adapter = ZerodhaBrokerAdapter(
            mode="LIVE",
            allow_real_orders=True,
            kite_client=kite,
            governor=_NoSleepGovernor(),
            guard_config=RealOrderGuardConfig(
                enabled=True,
                acknowledgement="I_UNDERSTAND_REAL_MONEY_ORDERS",
                max_quantity=1,
                max_notional=4_000.0,
                allowed_order_types=frozenset({"SL-M"}),
            ),
        )

        with pytest.raises(RuntimeError, match="missing market protection"):
            await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        order = db.get_session_orders("paper-live-1")[0]
        assert order.status == "REJECTED"
        assert order.exchange_order_id is None
        assert order.notes == "BROKER_REJECTED"

        payload = await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        orders = db.get_session_orders("paper-live-1")
        assert kite.calls == 2
        assert len(orders) == 1
        assert payload["exchange_order_id"] == "kite-retry-1"
        assert orders[0].status == "PENDING"
        assert orders[0].exchange_order_id == "kite-retry-1"
        assert orders[0].notes == "LIVE"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_record_real_order_recovers_pending_live_intent_from_orderbook(
    tmp_path,
) -> None:
    class FakeAdapter(ZerodhaBrokerAdapter):
        def __init__(self) -> None:
            super().__init__(mode="LIVE", allow_real_orders=True, kite_client=None)
            self.place_calls = 0

        async def place_order(self, intent):
            self.place_calls += 1
            raise AssertionError("recovered pending intent must not submit duplicate order")

        async def fetch_order_snapshots(self):
            return [
                BrokerOrderSnapshot(
                    order_id="kite-recovered-1",
                    symbol="TCS",
                    side="BUY",
                    quantity=1,
                    filled_quantity=1,
                    average_price=3500.0,
                    status="COMPLETE",
                    tag="cpr-manual-paper-liv",
                    broker_payload={
                        "order_id": "kite-recovered-1",
                        "tradingsymbol": "TCS",
                        "transaction_type": "BUY",
                        "quantity": 1,
                        "filled_quantity": 1,
                        "average_price": 3500.0,
                        "status": "COMPLETE",
                        "tag": "cpr-manual-paper-liv",
                        "product": "MIS",
                        "exchange": "NSE",
                        "order_type": "LIMIT",
                    },
                )
            ]

    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = FakeAdapter()
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            quantity=1,
            role="manual",
            order_type="LIMIT",
            price=3500.0,
            reference_price=3500.0,
            reference_price_age_sec=1.0,
            event_time="2026-04-28T09:20:00+05:30",
        )
        db.append_order_event(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            order_type="LIMIT",
            requested_qty=1,
            request_price=3500.0,
            fill_qty=0,
            fill_price=None,
            status="PENDING",
            exchange_order_id=None,
            idempotency_key=intent.idempotency_key(),
            notes="PENDING_DISPATCH",
            broker_mode="LIVE",
            broker_payload=json.dumps(intent.zerodha_payload()),
        )

        payload = await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        assert payload["recovered_existing"] is True
        assert payload["exchange_order_id"] == "kite-recovered-1"
        assert adapter.place_calls == 0
        order = db.get_session_orders("paper-live-1")[0]
        assert order.exchange_order_id == "kite-recovered-1"
        assert order.status == "FILLED"
        assert order.notes == "RECOVERED_PENDING_DISPATCH"
    finally:
        db.close()


@pytest.mark.parametrize("snapshot_count", [0, 2])
@pytest.mark.asyncio
async def test_record_real_order_blocks_pending_live_intent_when_recovery_not_unique(
    tmp_path,
    snapshot_count: int,
) -> None:
    class FakeAdapter(ZerodhaBrokerAdapter):
        def __init__(self) -> None:
            super().__init__(mode="LIVE", allow_real_orders=True, kite_client=None)
            self.place_calls = 0

        async def place_order(self, intent):
            self.place_calls += 1
            raise AssertionError("ambiguous pending intent must not submit duplicate order")

        async def fetch_order_snapshots(self):
            return [
                BrokerOrderSnapshot(
                    order_id=f"kite-recovered-{idx}",
                    symbol="TCS",
                    side="BUY",
                    quantity=1,
                    filled_quantity=1,
                    average_price=3500.0,
                    status="COMPLETE",
                    tag="cpr-manual-paper-liv",
                    broker_payload={
                        "order_id": f"kite-recovered-{idx}",
                        "tradingsymbol": "TCS",
                        "transaction_type": "BUY",
                        "quantity": 1,
                        "filled_quantity": 1,
                        "average_price": 3500.0,
                        "status": "COMPLETE",
                        "tag": "cpr-manual-paper-liv",
                        "product": "MIS",
                        "exchange": "NSE",
                        "order_type": "LIMIT",
                    },
                )
                for idx in range(snapshot_count)
            ]

    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    adapter = FakeAdapter()
    try:
        intent = BrokerOrderIntent(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            quantity=1,
            role="manual",
            order_type="LIMIT",
            price=3500.0,
            reference_price=3500.0,
            reference_price_age_sec=1.0,
            event_time="2026-04-28T09:20:00+05:30",
        )
        db.append_order_event(
            session_id="paper-live-1",
            symbol="TCS",
            side="BUY",
            order_type="LIMIT",
            requested_qty=1,
            request_price=3500.0,
            fill_qty=0,
            fill_price=None,
            status="PENDING",
            exchange_order_id=None,
            idempotency_key=intent.idempotency_key(),
            notes="PENDING_DISPATCH",
            broker_mode="LIVE",
            broker_payload=json.dumps(intent.zerodha_payload()),
        )

        with pytest.raises(OrderSafetyError, match="manual broker reconciliation"):
            await record_real_order(paper_db=db, intent=intent, adapter=adapter)

        assert adapter.place_calls == 0
        order = db.get_session_orders("paper-live-1")[0]
        assert order.exchange_order_id is None
        assert order.status == "PENDING"
        assert order.notes == "PENDING_DISPATCH"
    finally:
        db.close()
