"""DuckDB archive helpers for completed paper sessions."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import polars as pl

from db.backtest_db import get_backtest_db
from db.paper_db import PaperDB, PaperPosition, PaperSession, get_paper_db

logger = logging.getLogger(__name__)


def _position_to_trade_row(session: PaperSession, position: PaperPosition) -> dict[str, Any]:
    opened_at = position.entry_time or datetime.utcnow()
    closed_at = position.exit_time or opened_at
    quantity = float(position.qty or 0.0)
    entry_price = float(position.entry_price or 0.0)
    exit_price = float(position.exit_price or entry_price)
    direction = position.direction.upper()
    pnl = position.pnl
    if not pnl:
        pnl = (
            (exit_price - entry_price) * quantity
            if direction == "LONG"
            else (entry_price - exit_price) * quantity
        )
    position_value = entry_price * quantity
    pnl_pct = (pnl / position_value * 100.0) if position_value else 0.0
    trail_state = position.trail_state or {}
    raw_exit_reason = str(
        trail_state.get("exit_reason")
        or trail_state.get("close_reason")
        or getattr(position, "exit_reason", None)
        or "TIME"
    )
    # Normalize legacy/paper-only exit reason values to backtest_results CHECK constraint values.
    exit_reason_map = {
        "SL": "INITIAL_SL",
        "MANUAL_FLATTEN": "TIME",
        "MANUAL_CLOSE": "TIME",
        "MANUAL": "TIME",
        "OPERATOR_CLOSE": "TIME",
        "CLOSE_POSITIONS": "TIME",
        "CLOSE_ALL": "TIME",
        "FLATTEN": "TIME",
    }
    exit_reason = exit_reason_map.get(raw_exit_reason.strip().upper(), raw_exit_reason)
    sl_phase = str(trail_state.get("sl_phase") or "PROTECT")

    return {
        "run_id": session.session_id,
        "session_id": session.session_id,
        "source_session_id": session.session_id,
        "execution_mode": "PAPER",
        "symbol": position.symbol,
        "trade_date": opened_at.date().isoformat(),
        "direction": direction,
        "entry_time": opened_at.strftime("%H:%M"),
        "exit_time": closed_at.strftime("%H:%M"),
        "entry_timestamp": opened_at,
        "exit_timestamp": closed_at,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "sl_price": trail_state.get("initial_sl") or position.stop_loss,
        "target_price": position.target_price,
        "profit_loss": float(pnl),
        "profit_loss_pct": float(pnl_pct),
        "exit_reason": exit_reason,
        "sl_phase": sl_phase,
        "atr": trail_state.get("atr"),
        "cpr_width_pct": trail_state.get("cpr_width_pct"),
        "position_size": round(quantity),
        "position_value": float(position_value),
        "mfe_r": trail_state.get("mfe_r"),
        "mae_r": trail_state.get("mae_r"),
        "or_atr_ratio": trail_state.get("or_atr_ratio"),
        "gap_pct": trail_state.get("gap_pct"),
    }


def archive_completed_session(
    session_id: str,
    *,
    paper_db: PaperDB | None = None,
) -> dict[str, Any]:
    """Archive closed paper-session positions to DuckDB analytics tables.

    By default we read from the live writer connection, but replay/live callers
    may pass a read-only replica snapshot after forcing sync. That avoids
    contention with a still-running concurrent session in the same process.
    """
    paper_db = paper_db or get_paper_db()
    session = paper_db.get_session(session_id)
    if session is None:
        return {"session_id": session_id, "archived": False, "error": "session not found"}

    logger.info(
        "Archive start session_id=%s strategy=%s status=%s",
        session_id,
        session.strategy,
        session.status,
    )
    closed_positions = paper_db.get_session_positions(session_id, statuses=["CLOSED"])
    rows = [_position_to_trade_row(session, position) for position in closed_positions]

    if rows:
        trade_dates = [datetime.fromisoformat(str(row["trade_date"])) for row in rows]
        start_date = min(d.date().isoformat() for d in trade_dates)
        end_date = max(d.date().isoformat() for d in trade_dates)
    else:
        # Zero-trade session — still archive metadata so the session is visible in the dashboard.
        trade_date = (session.trade_date or "").strip()
        if not trade_date:
            return {
                "session_id": session_id,
                "archived": False,
                "rows": 0,
                "reason": "no closed positions and no trade_date on session",
            }
        start_date = trade_date
        end_date = trade_date
    backtest_db = get_backtest_db()
    params = dict(session.strategy_params or {})
    paper_session_mode = str(getattr(session, "mode", "") or "LIVE").upper()
    paper_feed_source = str(params.get("feed_source") or "").strip().lower()
    if not paper_feed_source:
        paper_feed_source = "historical" if paper_session_mode == "REPLAY" else "kite"
    params.update(
        {
            "paper_session_id": session.session_id,
            "paper_session_mode": paper_session_mode,
            "paper_feed_source": paper_feed_source,
            "portfolio_value": getattr(session, "portfolio_value", None),
            "max_daily_loss_pct": session.max_daily_loss_pct,
            "max_positions": session.max_positions,
            "max_position_pct": session.max_position_pct,
        }
    )
    backtest_db.store_run_metadata(
        run_id=session.session_id,
        strategy=session.strategy,
        label=session.strategy,
        symbols=session.symbols,
        start_date=start_date,
        end_date=end_date,
        params=params,
        execution_mode="PAPER",
        session_id=session.session_id,
        source_session_id=session.session_id,
    )
    if rows:
        backtest_db.store_backtest_results(pl.DataFrame(rows))

    # Ensure the archive reaches the backtest replica promptly. The writer's
    # replica sync has a 30 s debounce — sequential archive calls (e.g. LONG
    # then SHORT within seconds) can have the second one silently skipped.
    if backtest_db._sync:
        backtest_db._sync.force_sync(source_conn=backtest_db.con)

    logger.info(
        "Archive done session_id=%s rows=%d symbols=%d",
        session.session_id,
        len(rows),
        len({row["symbol"] for row in rows}),
    )
    return {
        "session_id": session.session_id,
        "archived": True,
        "rows": len(rows),
        "symbols": sorted({row["symbol"] for row in rows}),
        "trade_count": len(rows),
        "execution_mode": "PAPER",
    }


__all__ = ["archive_completed_session"]
