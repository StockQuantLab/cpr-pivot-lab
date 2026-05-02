"""Paper session summary and mark-price helpers."""

from __future__ import annotations

import json
from typing import Any

from db.paper_db import FeedState, PaperPosition, PaperSession


def _exit_value_for_position(position: PaperPosition, qty: float, close_price: float) -> float:
    """Compute exit value matching backtest's portfolio constraint model."""
    entry = float(position.entry_price or 0.0)
    direction = str(getattr(position, "direction", "")).upper()
    if direction == "SHORT":
        return round(float(qty) * (2.0 * entry - float(close_price)), 2)
    return round(float(qty) * float(close_price), 2)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except TypeError, ValueError:
        return None


def _symbol_price_from_raw_state(raw_state: Any, symbol: str) -> float | None:
    if isinstance(raw_state, str):
        try:
            raw_state = json.loads(raw_state)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw_state, dict):
        return None
    symbol_prices = raw_state.get("symbol_last_prices")
    if isinstance(symbol_prices, dict) and symbol in symbol_prices:
        return _float_or_none(symbol_prices[symbol])
    legacy_prices = raw_state.get("prices")
    if isinstance(legacy_prices, dict) and symbol in legacy_prices:
        return _float_or_none(legacy_prices[symbol])
    if raw_state.get("symbol") == symbol:
        return _float_or_none(raw_state.get("last_price"))
    return None


def mark_price_for_position(position: PaperPosition, feed_state: FeedState | None) -> float | None:
    direct = _float_or_none(getattr(position, "last_price", None))
    if direct is not None:
        return direct
    if feed_state is None:
        return None
    raw_state = getattr(feed_state, "raw_state", None)
    symbol = str(getattr(position, "symbol", "") or "")
    symbol_price = _symbol_price_from_raw_state(raw_state, symbol)
    if symbol_price is not None:
        return symbol_price
    return _float_or_none(getattr(feed_state, "last_price", None))


def summarize_paper_positions(
    session: PaperSession, positions: list[PaperPosition], feed_state: FeedState | None
) -> dict[str, object]:
    open_positions = [p for p in positions if str(getattr(p, "status", "")).upper() == "OPEN"]
    closed_positions = [p for p in positions if str(getattr(p, "status", "")).upper() == "CLOSED"]

    def _mtm(position: PaperPosition) -> float:
        qty = float(
            getattr(position, "current_qty", None) or getattr(position, "quantity", 0.0) or 0.0
        )
        entry = float(getattr(position, "entry_price", 0.0) or 0.0)
        mark = mark_price_for_position(position, feed_state)
        if mark is None:
            mark = entry
        direction = str(getattr(position, "direction", "")).upper()
        return (entry - mark) * qty if direction == "SHORT" else (mark - entry) * qty

    realized = sum(
        float(getattr(position, "realized_pnl", 0.0) or 0.0) for position in closed_positions
    )
    unrealized = sum(_mtm(position) for position in open_positions)
    return {
        "session_id": getattr(session, "session_id", ""),
        "name": getattr(session, "name", None),
        "strategy": getattr(session, "strategy", ""),
        "status": getattr(session, "status", ""),
        "feed_status": getattr(feed_state, "status", None),
        "feed_reason": getattr(feed_state, "stale_reason", None),
        "open_positions": len(open_positions),
        "closed_positions": len(closed_positions),
        "orders": 0,
        "realized_pnl": round(realized, 2),
        "unrealized_pnl": round(unrealized, 2),
        "net_pnl": round(realized + unrealized, 2),
        "gross_exposure": round(
            sum(
                float(getattr(position, "entry_price", 0.0) or 0.0)
                * float(
                    getattr(position, "current_qty", None)
                    or getattr(position, "quantity", 0.0)
                    or 0.0
                )
                for position in open_positions
            ),
            2,
        ),
        "last_price": getattr(feed_state, "last_price", None) if feed_state else None,
        "latest_candle_ts": getattr(session, "latest_candle_ts", None),
        "stale_feed_at": getattr(session, "stale_feed_at", None),
    }


def build_summary_feed_state(
    *,
    session_id: str,
    symbol_last_prices: dict[str, float],
    last_price: float | None,
) -> FeedState:
    """Create a lightweight feed-state object for risk control checks."""
    return FeedState(
        session_id=session_id,
        status="OK",
        last_event_ts=None,
        last_bar_ts=None,
        last_price=last_price,
        stale_reason=None,
        raw_state={"symbol_last_prices": dict(symbol_last_prices)},
    )


__all__ = [
    "_exit_value_for_position",
    "_float_or_none",
    "build_summary_feed_state",
    "mark_price_for_position",
    "summarize_paper_positions",
]
