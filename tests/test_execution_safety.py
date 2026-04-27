from __future__ import annotations

import pytest

from db.paper_db import PaperDB
from engine.execution_safety import OrderRateGovernor, build_order_idempotency_key


@pytest.mark.asyncio
async def test_order_rate_governor_waits_after_burst() -> None:
    now = 0.0
    sleeps: list[float] = []

    def clock() -> float:
        return now

    async def sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    governor = OrderRateGovernor(
        rate_per_second=2.0,
        burst_capacity=2.0,
        clock=clock,
        sleep=sleep,
    )

    assert await governor.acquire() == pytest.approx(0.0)
    assert await governor.acquire() == pytest.approx(0.0)
    assert await governor.acquire() == pytest.approx(0.5)
    assert sleeps == [pytest.approx(0.5)]


def test_build_order_idempotency_key_is_deterministic() -> None:
    kwargs = {
        "session_id": "CPR_LEVELS_LONG-2026-04-28-live-kite",
        "role": "entry",
        "symbol": "sbin",
        "side": "buy",
        "position_id": "pos-1",
        "event_time": "2026-04-28T09:20:00+05:30",
    }

    assert build_order_idempotency_key(**kwargs) == build_order_idempotency_key(**kwargs)
    assert "SBIN" in build_order_idempotency_key(**kwargs)
    assert "BUY" in build_order_idempotency_key(**kwargs)


def test_paper_order_idempotency_returns_existing_order(tmp_path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        session_id = "paper-live-1"
        key = "paper-live-1|entry|SBIN|BUY|pos-1||09:20"
        first = db.append_order_event(
            session_id=session_id,
            symbol="SBIN",
            side="BUY",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            idempotency_key=key,
            notes="paper entry",
        )
        second = db.append_order_event(
            session_id=session_id,
            symbol="SBIN",
            side="BUY",
            requested_qty=10,
            fill_qty=10,
            status="FILLED",
            idempotency_key=key,
            notes="paper entry retry",
        )

        orders = db.get_session_orders(session_id)
        assert second == first
        assert len(orders) == 1
        assert orders[0].idempotency_key == key
        assert orders[0].notes == "paper entry"
    finally:
        db.close()
