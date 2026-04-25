"""Shared per-bar orchestration helpers for replay/live paper sessions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any


def slot_capital_for(
    *,
    max_positions: int,
    portfolio_value: float,
    max_position_pct: float = 0.0,
    capital_base: float | None = None,
) -> float:
    """Return the per-slot capital cap for a session."""
    base = float(portfolio_value if capital_base is None else capital_base or 0.0)
    if base <= 0:
        return 0.0
    max_positions = max(1, int(max_positions or 1))
    pct_cap = base * float(max_position_pct or 0.0)
    slot_by_count = base / float(max_positions)
    # When max_position_pct > 0, it caps the per-slot allocation.
    # When max_position_pct == 0, only max_positions controls sizing.
    return min(slot_by_count, pct_cap) if pct_cap > 0 else slot_by_count


def minimum_trade_notional_for(
    *,
    max_positions: int,
    portfolio_value: float,
    max_position_pct: float = 0.0,
    capital_base: float | None = None,
) -> float:
    """Return the smallest non-dust allocation allowed for a new position."""
    slot_capital = slot_capital_for(
        max_positions=max_positions,
        portfolio_value=portfolio_value,
        max_position_pct=max_position_pct,
        capital_base=capital_base,
    )
    return max(1_000.0, slot_capital * 0.05)


@dataclass(slots=True)
class TrackedPosition:
    """Lightweight in-memory position state for one symbol."""

    position_id: str
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    target_price: float
    entry_time: str
    quantity: float
    current_qty: float
    status: str = "OPEN"
    raw_position: Any | None = None


class SessionPositionTracker:
    """In-memory position book for one paper session variant."""

    def __init__(
        self,
        max_positions: int,
        portfolio_value: float,
        max_position_pct: float = 0.0,
    ):
        self.max_positions = max(1, int(max_positions or 1))
        self.initial_capital = float(portfolio_value or 0.0)
        self.cash_available = self.initial_capital
        self.max_position_pct = float(max_position_pct or 0.0)
        self.slot_capital = slot_capital_for(
            max_positions=self.max_positions,
            portfolio_value=self.initial_capital,
            max_position_pct=self.max_position_pct,
            capital_base=self.initial_capital,
        )
        self._open: dict[str, TrackedPosition] = {}
        self._closed_today: set[str] = set()

    def minimum_trade_notional(self, capital_base: float | None = None) -> float:
        """Return the smallest non-dust allocation we allow for a new position."""
        return minimum_trade_notional_for(
            max_positions=self.max_positions,
            portfolio_value=self.initial_capital,
            max_position_pct=self.max_position_pct,
            capital_base=capital_base,
        )

    @property
    def open_count(self) -> int:
        return len(self._open)

    def has_open_position(self, symbol: str) -> bool:
        return str(symbol) in self._open

    def get_open_position(self, symbol: str) -> Any | None:
        tracked = self._open.get(str(symbol))
        return tracked.raw_position if tracked is not None else None

    def has_traded_today(self, symbol: str) -> bool:
        return str(symbol) in self._closed_today

    def can_open_new(self) -> bool:
        return self.open_count < self.max_positions

    def slots_available(self) -> int:
        return max(0, self.max_positions - self.open_count)

    def open_symbols(self) -> set[str]:
        return set(self._open)

    def current_equity(self) -> float:
        """Return the current equity base for compound sizing.

        Cash is already adjusted for open/closed positions; add back the
        remaining cost basis of open positions so compound sizing uses the
        full current session equity instead of only free cash.
        """
        open_cost_basis = sum(
            max(0.0, float(tracked.current_qty or 0.0) * float(tracked.entry_price or 0.0))
            for tracked in self._open.values()
        )
        return max(0.0, float(self.cash_available or 0.0) + open_cost_basis)

    def credit_cash(self, amount: float) -> None:
        self.cash_available += max(0.0, float(amount or 0.0))

    def mark_traded(self, symbol: str) -> None:
        self._closed_today.add(str(symbol))

    def record_open(self, position: Any, position_value: float) -> None:
        symbol = str(getattr(position, "symbol", ""))
        tracked = TrackedPosition(
            position_id=str(getattr(position, "position_id", "")),
            symbol=symbol,
            direction=str(getattr(position, "direction", "")),
            entry_price=float(getattr(position, "entry_price", 0.0) or 0.0),
            stop_loss=float(getattr(position, "stop_loss", 0.0) or 0.0),
            target_price=float(getattr(position, "target_price", 0.0) or 0.0),
            entry_time=str((getattr(position, "trail_state", {}) or {}).get("entry_time") or ""),
            quantity=float(getattr(position, "quantity", 0.0) or 0.0),
            current_qty=float(
                getattr(position, "current_qty", None) or getattr(position, "quantity", 0.0) or 0.0
            ),
            raw_position=position,
        )
        self._open[symbol] = tracked
        self.cash_available -= max(0.0, float(position_value or 0.0))

    def update_trail_state(self, symbol: str, trail_state: dict[str, Any]) -> None:
        """Refresh the cached trail_state so subsequent candles see accumulated state."""
        tracked = self._open.get(str(symbol))
        if tracked is not None and tracked.raw_position is not None:
            tracked.raw_position.trail_state = dict(trail_state)

    def record_close(self, symbol: str, exit_value: float) -> None:
        normalized = str(symbol)
        self._open.pop(normalized, None)
        self._closed_today.add(normalized)
        self.cash_available += max(0.0, float(exit_value or 0.0))

    def seed_open_positions(self, positions: list[Any]) -> None:
        for position in positions:
            qty = float(
                getattr(position, "current_qty", None) or getattr(position, "quantity", 0.0) or 0.0
            )
            entry_price = float(getattr(position, "entry_price", 0.0) or 0.0)
            self.record_open(position, position_value=qty * entry_price)

    def compute_position_qty(
        self,
        *,
        entry_price: float,
        risk_based_sizing: bool,
        candidate_size: int = 0,
        capital_base: float | None = None,
    ) -> int:
        if entry_price <= 0:
            return 0
        slot_capital = slot_capital_for(
            max_positions=self.max_positions,
            portfolio_value=self.initial_capital,
            max_position_pct=self.max_position_pct,
            capital_base=capital_base,
        )
        if risk_based_sizing:
            candidate_notional = max(0.0, float(candidate_size or 0) * float(entry_price))
            desired_notional = min(candidate_notional, slot_capital)
        else:
            desired_notional = slot_capital
        # All-or-nothing sizing: do not partially fill with residual cash.
        min_notional = self.minimum_trade_notional(capital_base=capital_base)
        if (
            desired_notional <= 0
            or desired_notional < min_notional
            or self.cash_available < desired_notional
        ):
            return 0
        investable = desired_notional
        if investable <= 0:
            return 0
        return max(0, int(investable / float(entry_price)))


def should_process_symbol(
    *,
    bar_time: str,
    entry_window_end: str,
    tracker: SessionPositionTracker,
    symbol: str,
    setup_status: str,
) -> bool:
    normalized_symbol = str(symbol)
    status = str(setup_status or "pending").lower()
    if tracker.has_open_position(normalized_symbol):
        return True
    if bar_time > str(entry_window_end):
        return False
    if status == "rejected":
        return False
    if tracker.has_traded_today(normalized_symbol):
        return False
    return True


def _candidate_quality_score(c: dict[str, Any]) -> float:
    # Backtest candidates have rr_ratio/or_atr_ratio directly on the dict.
    # Live/replay candidates nest the raw entry under "candidate".
    inner = c.get("candidate") or c
    effective_rr = float(inner.get("rr_ratio") or 1.0)
    or_atr = float(inner.get("or_atr_ratio") or 1.0)
    # Higher effective_rr = better reward/risk. Lower or_atr = tighter opening range.
    # Dividing by (1 + or_atr) penalises exhausted-open symbols without a hard cutoff.
    return effective_rr / (1.0 + or_atr)


def select_entries_for_bar(
    candidates: list[dict[str, Any]],
    tracker: SessionPositionTracker,
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    slots = tracker.slots_available()
    if slots <= 0:
        return []
    # Quality-sort: best effective_rr / (1 + or_atr) wins the slot.
    # Break ties by symbol so the result stays deterministic even when live and
    # backtest feed candidates in a different iteration order.
    ordered = sorted(
        candidates,
        key=lambda c: (-_candidate_quality_score(c), str(c.get("symbol", ""))),
    )
    return ordered[:slots]


async def check_bar_risk_controls(
    *,
    session: Any,
    session_id: str,
    as_of: datetime,
    symbol_last_prices: dict[str, float],
    last_price: float | None,
    enforce_risk_controls: Callable[..., Any],
    build_feed_state: Callable[..., Any],
) -> bool:
    risk_result = await enforce_risk_controls(
        session=session,
        as_of=as_of,
        feed_state=build_feed_state(
            session_id=session_id,
            symbol_last_prices=symbol_last_prices,
            last_price=last_price,
        ),
    )
    return bool((risk_result or {}).get("triggered"))
