"""Paper DB access helpers used by the runtime."""

from __future__ import annotations

import logging
import threading
from typing import Any

from db.paper_db import FeedState, PaperPosition, PaperSession, get_paper_db
from engine.execution_safety import get_default_order_governor

logger = logging.getLogger(__name__)

PAPER_DB_IO_LOCK = threading.RLock()


def _db():
    return get_paper_db()


async def get_session(session_id: str) -> PaperSession | None:
    with PAPER_DB_IO_LOCK:
        return _db().get_session(session_id)


async def get_session_positions(
    session_id: str, symbol: str | None = None, statuses: list[str] | None = None
) -> list[PaperPosition]:
    with PAPER_DB_IO_LOCK:
        return _db().get_session_positions(session_id, symbol=symbol, statuses=statuses)


async def get_feed_state(session_id: str) -> FeedState | None:
    with PAPER_DB_IO_LOCK:
        return _db().get_feed_state(session_id)


async def update_session_state(session_id: str, **kwargs: Any) -> PaperSession | None:
    with PAPER_DB_IO_LOCK:
        return _db().update_session(session_id, **kwargs)


async def accumulate_session_pnl(session_id: str, pnl_delta: float) -> None:
    """Add a position's realized PnL to the session's running total_pnl."""
    with PAPER_DB_IO_LOCK:
        session = _db().get_session(session_id)
        if session is None:
            return
        new_total = round(float(session.total_pnl or 0.0) + pnl_delta, 2)
        _db().update_session(session_id, total_pnl=new_total)


async def open_position(**kwargs: Any) -> PaperPosition:
    with PAPER_DB_IO_LOCK:
        return _db().open_position(**kwargs)


async def append_order_event(**kwargs: Any) -> Any:
    throttle = bool(kwargs.pop("throttle", True))
    if throttle:
        waited = await get_default_order_governor().acquire()
        if waited > 0:
            logger.info(
                "Paper order governor delayed order %.3fs session_id=%s symbol=%s side=%s",
                waited,
                kwargs.get("session_id"),
                kwargs.get("symbol"),
                kwargs.get("side"),
            )
    with PAPER_DB_IO_LOCK:
        return _db().append_order_event(**kwargs)


async def update_position(position_id: str, **kwargs: Any) -> PaperPosition | None:
    with PAPER_DB_IO_LOCK:
        return _db().update_position(position_id, **kwargs)


def force_paper_db_sync(paper_db: Any | None = None) -> None:
    """Force a replica sync while serializing access to the shared writer DB."""
    with PAPER_DB_IO_LOCK:
        pdb = paper_db or _db()
        sync = getattr(pdb, "_sync", None)
        if sync is not None:
            sync.force_sync(source_conn=getattr(pdb, "con", None))


__all__ = [
    "PAPER_DB_IO_LOCK",
    "_db",
    "accumulate_session_pnl",
    "append_order_event",
    "force_paper_db_sync",
    "get_feed_state",
    "get_session",
    "get_session_positions",
    "open_position",
    "update_position",
    "update_session_state",
]
