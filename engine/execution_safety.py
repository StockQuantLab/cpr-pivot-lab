"""Order execution safety primitives shared by paper and future broker modes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], float]


@dataclass
class OrderRateGovernor:
    """Async token-bucket governor for order dispatch.

    The default cap intentionally stays below the 10 orders/sec regulatory threshold
    so bursts have operational headroom.
    """

    rate_per_second: float = 8.0
    burst_capacity: float = 8.0
    clock: ClockFn = time.monotonic
    sleep: SleepFn = asyncio.sleep
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        if self.rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if self.burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")
        self._tokens = float(self.burst_capacity)
        self._last_refill = self.clock()

    async def acquire(self, tokens: float = 1.0) -> float:
        """Acquire dispatch capacity and return seconds waited."""
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        waited = 0.0
        async with self._lock:
            while True:
                now = self.clock()
                elapsed = max(0.0, now - self._last_refill)
                self._tokens = min(
                    self.burst_capacity,
                    self._tokens + elapsed * self.rate_per_second,
                )
                self._last_refill = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited

                deficit = tokens - self._tokens
                wait_for = deficit / self.rate_per_second
                waited += wait_for
                await self.sleep(wait_for)


_DEFAULT_ORDER_GOVERNOR = OrderRateGovernor()


def get_default_order_governor() -> OrderRateGovernor:
    return _DEFAULT_ORDER_GOVERNOR


def build_order_idempotency_key(
    *,
    session_id: str,
    role: str,
    symbol: str,
    side: str,
    position_id: str | None = None,
    signal_id: int | None = None,
    event_time: str | None = None,
) -> str:
    """Build a deterministic key for retry-safe paper/broker order writes."""
    parts = [
        session_id,
        role,
        symbol.upper(),
        side.upper(),
        str(position_id or ""),
        str(signal_id or ""),
        str(event_time or ""),
    ]
    return "|".join(parts)
