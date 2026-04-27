"""
Agent tools for backtest queries and data management.

All market data operations use DuckDB (never PostgreSQL).
Each function returns a dict — wrapped with _json_tool() in llm_agent.py.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict

from db.duckdb import get_db
from db.postgres import (
    get_active_sessions,
    get_feed_state,
    get_session,
    get_session_orders,
    get_session_positions,
)
from engine.constants import normalize_symbol
from engine.cpr_atr_strategy import (
    BacktestParams,
    CPRATRBacktest,
    CPRLevelsParams,
    FBRParams,
)
from engine.paper_runtime import summarize_paper_positions, write_admin_command

logger = logging.getLogger(__name__)


def _internal_error(message: str, **extra: object) -> dict:
    payload: dict[str, object] = {"error": message}
    payload.update(extra)
    return payload


def _build_backtest_params(
    *,
    strategy: str,
    cpr_percentile: float,
    cpr_max_width_pct: float,
    rr_ratio: float,
    capital: float,
    risk_pct: float,
    portfolio_value: float,
    max_positions: int,
    max_position_pct: float,
    risk_based_sizing: bool,
    time_exit: str,
    entry_window_end: str,
    direction_filter: str,
    min_price: float,
    long_max_gap_pct: float | None,
    cpr_min_close_atr: float,
    min_sl_atr_ratio: float,
    narrowing_filter: bool,
    failure_window: int,
    atr_periods: int | None = None,
    buffer_pct: float | None = None,
    rvol_threshold: float | None = None,
) -> BacktestParams:
    """Build BacktestParams with one shared mapping to avoid drift across tools."""
    kwargs: dict[str, float | int | str | bool | None] = {
        "strategy": strategy.upper(),
        "cpr_percentile": cpr_percentile,
        "cpr_max_width_pct": cpr_max_width_pct,
        "rr_ratio": rr_ratio,
        "capital": capital,
        "risk_pct": risk_pct,
        "portfolio_value": portfolio_value,
        "max_positions": max_positions,
        "max_position_pct": max_position_pct,
        "risk_based_sizing": risk_based_sizing,
        "time_exit": time_exit,
        "entry_window_end": entry_window_end,
        # For FBR: --direction LONG means trade LONG (failed SHORT breakdowns).
        # Internally direction_filter stores the breakout direction, which is inverted.
        "direction_filter": (
            ("SHORT" if direction_filter.upper() == "LONG" else "LONG")
            if strategy.upper() == "FBR" and direction_filter.upper() != "BOTH"
            else direction_filter.upper()
        ),
        "fbr_setup_filter": (
            "BREAKDOWN"
            if direction_filter.upper() == "LONG"
            else "BREAKOUT"
            if direction_filter.upper() == "SHORT"
            else "BOTH"
        )
        if strategy.upper() == "FBR"
        else "BOTH",
        "min_price": min_price,
        "long_max_gap_pct": long_max_gap_pct,
    }
    if atr_periods is not None:
        kwargs["atr_periods"] = atr_periods
    if buffer_pct is not None:
        kwargs["buffer_pct"] = buffer_pct
    if rvol_threshold is not None:
        kwargs["rvol_threshold"] = rvol_threshold
    params = BacktestParams(**kwargs)
    return params.apply_strategy_configs(
        cpr_levels=CPRLevelsParams(
            cpr_shift_filter=params.cpr_levels.cpr_shift_filter,
            min_effective_rr=params.cpr_levels.min_effective_rr,
            use_narrowing_filter=narrowing_filter,
            cpr_entry_start=params.cpr_levels.cpr_entry_start,
            cpr_confirm_entry=params.cpr_levels.cpr_confirm_entry,
            cpr_hold_confirm=params.cpr_levels.cpr_hold_confirm,
            cpr_min_close_atr=cpr_min_close_atr,
        ),
        fbr=FBRParams(
            failure_window=failure_window,
            reversal_buffer_pct=params.fbr.reversal_buffer_pct,
            fbr_min_or_atr=params.fbr.fbr_min_or_atr,
            fbr_failure_depth=params.fbr.fbr_failure_depth,
            fbr_entry_window_end=params.fbr.fbr_entry_window_end,
            use_narrowing_filter=narrowing_filter,
        ),
    )


def run_backtest(
    symbol: str,
    start_date: str,
    end_date: str,
    strategy: str = "CPR_LEVELS",
    cpr_percentile: float = 33.0,
    cpr_max_width_pct: float = 2.0,
    atr_periods: int = 12,
    buffer_pct: float = 0.0005,
    rvol_threshold: float = 1.0,
    rr_ratio: float = 2.0,
    capital: float = 100_000,
    risk_pct: float = 0.01,
    portfolio_value: float = 1_000_000,
    max_positions: int = 10,
    max_position_pct: float = 0.10,
    risk_based_sizing: bool = False,
    time_exit: str = "15:15",
    entry_window_end: str = "10:15",
    direction_filter: str = "BOTH",
    min_price: float = 0.0,
    long_max_gap_pct: float | None = None,
    cpr_min_close_atr: float = 0.0,
    min_sl_atr_ratio: float = 0.5,
    narrowing_filter: bool = False,
    failure_window: int = 8,
    force_rerun: bool = False,
) -> dict:
    """
    Run CPR-ATR backtest for a single NSE stock.

    Args:
        symbol:           NSE symbol e.g. 'RELIANCE', 'SBIN', 'TCS'
        start_date:       Start date YYYY-MM-DD
        end_date:         End date YYYY-MM-DD
        strategy:         CPR_LEVELS | FBR | VIRGIN_CPR
        cpr_percentile:   CPR width filter percentile (33 = bottom-third, lower = stricter)
        atr_periods:      ATR lookback 5-min candles (12 = last 1hr of prev day)
        buffer_pct:       Breakout buffer above/below OR (0.0005 = 0.05%)
        rvol_threshold:   Minimum relative volume on entry candle
        rr_ratio:         Risk-reward ratio (2.0 = 1:2)
        capital:          Legacy per-trade sizing base (used when portfolio mode is disabled)
        portfolio_value:  Shared-cash portfolio base in INR
        max_positions:    Max concurrent positions sharing the portfolio cash pool
        max_position_pct: Max fraction of portfolio allocated to one position
        time_exit:        Forceful exit time HH:MM (default 15:15; use 12:00 for mid-day kill)
        entry_window_end: Stop scanning for new entries after this time HH:MM
        direction_filter: 'BOTH' | 'LONG' | 'SHORT' — restrict trade direction.
            CPR_LEVELS/VIRGIN_CPR: filters setup direction directly.
            FBR: LONG = trade LONG reversals (failed SHORT breakdowns);
                 SHORT = trade SHORT reversals (failed LONG breakouts).
        min_price:        Skip symbols whose prior close is below this threshold
        long_max_gap_pct: Optional tighter gap cap for LONG trades only
        cpr_min_close_atr: CPR_LEVELS: min ATR clearance beyond TC/BC on the signal close
        narrowing_filter: CPR_LEVELS/FBR: only trade narrowing CPR days when True
        failure_window:   FBR: candles to detect reversal after failed breakout
        force_rerun: True recomputes even if results already exist for this run_id

    Returns:
        dict with total_trades, wins, losses, win_rate_pct, total_pnl, avg_pnl, summary
    """
    try:
        normalized_symbol = normalize_symbol(symbol)
        params = _build_backtest_params(
            strategy=strategy,
            cpr_percentile=cpr_percentile,
            cpr_max_width_pct=cpr_max_width_pct,
            rr_ratio=rr_ratio,
            capital=capital,
            risk_pct=risk_pct,
            portfolio_value=portfolio_value,
            max_positions=max_positions,
            max_position_pct=max_position_pct,
            risk_based_sizing=risk_based_sizing,
            time_exit=time_exit,
            entry_window_end=entry_window_end,
            direction_filter=direction_filter,
            min_price=min_price,
            long_max_gap_pct=long_max_gap_pct,
            cpr_min_close_atr=cpr_min_close_atr,
            min_sl_atr_ratio=min_sl_atr_ratio,
            narrowing_filter=narrowing_filter,
            failure_window=failure_window,
            atr_periods=atr_periods,
            buffer_pct=buffer_pct,
            rvol_threshold=rvol_threshold,
        )
        db = get_db()
        result = CPRATRBacktest(params=params, db=db).run(
            symbols=[normalized_symbol],
            start=start_date,
            end=end_date,
            verbose=False,
        )
        trades = result.df.to_dicts() if result.df.height > 0 else []
        wins = sum(1 for t in trades if t["profit_loss"] > 0)
        return {
            "symbol": normalized_symbol,
            "start_date": start_date,
            "end_date": end_date,
            "total_trades": len(trades),
            "wins": wins,
            "losses": len(trades) - wins,
            "win_rate_pct": round(wins / len(trades) * 100, 1) if trades else 0,
            "total_pnl": round(sum(t["profit_loss"] for t in trades), 2),
            "avg_pnl": round(sum(t["profit_loss"] for t in trades) / len(trades), 2)
            if trades
            else 0,
            "summary": result.summary(),
            "run_id": result.run_id,
        }
    except Exception:
        logger.exception(
            "run_backtest failed: symbol=%s start=%s end=%s", symbol, start_date, end_date
        )
        return _internal_error("Internal error while running backtest.", symbol=symbol)


def run_multi_stock_backtest(
    symbols: list[str],
    start_date: str,
    end_date: str,
    strategy: str = "CPR_LEVELS",
    cpr_percentile: float = 33.0,
    cpr_max_width_pct: float = 2.0,
    rr_ratio: float = 2.0,
    capital: float = 100_000,
    risk_pct: float = 0.01,
    portfolio_value: float = 1_000_000,
    max_positions: int = 10,
    max_position_pct: float = 0.10,
    risk_based_sizing: bool = False,
    time_exit: str = "15:15",
    entry_window_end: str = "10:15",
    direction_filter: str = "BOTH",
    min_price: float = 0.0,
    long_max_gap_pct: float | None = None,
    cpr_min_close_atr: float = 0.0,
    min_sl_atr_ratio: float = 0.5,
    narrowing_filter: bool = False,
    failure_window: int = 8,
    force_rerun: bool = False,
) -> dict:
    """
    Run CPR-ATR backtest across multiple NSE stocks.

    Args:
        symbols:          List e.g. ['RELIANCE', 'TCS', 'SBIN']
        start_date:       Start date YYYY-MM-DD
        end_date:         End date YYYY-MM-DD
        strategy:         CPR_LEVELS | FBR | VIRGIN_CPR
        cpr_percentile:   CPR filter percentile
        rr_ratio:         Risk-reward ratio (2.0 = 1:2)
        capital:          Legacy per-trade sizing base when portfolio mode is disabled
        portfolio_value:  Shared-cash portfolio base in INR
        max_positions:    Max concurrent positions across the symbol basket
        max_position_pct: Max fraction of portfolio allocated to one position
        time_exit:        Forceful exit time HH:MM (default 15:15; use 12:00 for mid-day kill)
        entry_window_end: Stop scanning for new entries after this time HH:MM
        direction_filter: 'BOTH' | 'LONG' | 'SHORT' — restrict trade direction.
            CPR_LEVELS/VIRGIN_CPR: filters setup direction directly.
            FBR: LONG = trade LONG reversals (failed SHORT breakdowns);
                 SHORT = trade SHORT reversals (failed LONG breakouts).
        min_price:        Skip symbols whose prior close is below this threshold
        long_max_gap_pct: Optional tighter gap cap for LONG trades only
        cpr_min_close_atr: CPR_LEVELS: min ATR clearance beyond TC/BC on the signal close
        narrowing_filter: CPR_LEVELS/FBR: only trade narrowing CPR days when True
        failure_window:   FBR: candles to detect reversal after failed breakout
        force_rerun: True recomputes even if results already exist for this run_id

    Returns:
        Combined summary and per-symbol performance table
    """
    try:
        normalized_symbols = [normalize_symbol(s) for s in symbols]
        params = _build_backtest_params(
            strategy=strategy,
            cpr_percentile=cpr_percentile,
            cpr_max_width_pct=cpr_max_width_pct,
            rr_ratio=rr_ratio,
            capital=capital,
            risk_pct=risk_pct,
            portfolio_value=portfolio_value,
            max_positions=max_positions,
            max_position_pct=max_position_pct,
            risk_based_sizing=risk_based_sizing,
            time_exit=time_exit,
            entry_window_end=entry_window_end,
            direction_filter=direction_filter,
            min_price=min_price,
            long_max_gap_pct=long_max_gap_pct,
            cpr_min_close_atr=cpr_min_close_atr,
            min_sl_atr_ratio=min_sl_atr_ratio,
            narrowing_filter=narrowing_filter,
            failure_window=failure_window,
        )
        db = get_db()
        result = CPRATRBacktest(params=params, db=db).run(
            symbols=normalized_symbols,
            start=start_date,
            end=end_date,
            verbose=False,
        )
        return {
            "symbols": normalized_symbols,
            "total_trades": len(result.df),
            "summary": result.summary(),
            "run_id": result.run_id,
        }
    except Exception:
        logger.exception(
            "run_multi_stock_backtest failed: symbols=%s start=%s end=%s",
            symbols,
            start_date,
            end_date,
        )
        return _internal_error("Internal error while running multi-stock backtest.")


def get_backtest_summary(symbol: str | None = None) -> dict:
    """
    Get aggregated performance from stored backtest results.

    Args:
        symbol: Filter by symbol (None = all symbols)

    Returns:
        Table with total_trades, win_rate_pct, total_pnl, avg_pnl, exit breakdown
    """
    try:
        db = get_db()
        df = db.get_backtest_summary(symbol=normalize_symbol(symbol) if symbol else None)
        return {"results": df.to_dicts(), "count": len(df)}
    except Exception:
        logger.exception("get_backtest_summary failed: symbol=%s", symbol)
        return _internal_error("Internal error while fetching backtest summary.")


def get_available_symbols() -> dict:
    """
    List all NSE symbols available in the Parquet dataset with date ranges.

    Returns:
        symbols list, count, date_ranges per symbol
    """
    try:
        db = get_db()
        symbols = db.get_available_symbols()
        all_ranges = db.get_all_date_ranges()
        ranges = {s: all_ranges.get(s, {}) for s in symbols}
        return {"symbols": symbols, "count": len(symbols), "date_ranges": ranges}
    except Exception:
        logger.exception("get_available_symbols failed")
        return _internal_error("Internal error while fetching available symbols.")


def get_cpr_for_date(symbol: str, trade_date: str) -> dict:
    """
    Get CPR levels, ATR, and dynamic threshold for a specific trading day.

    Args:
        symbol:     NSE symbol e.g. 'RELIANCE'
        trade_date: Date YYYY-MM-DD

    Returns:
        pivot, tc, bc, cpr_width_pct, atr, cpr_threshold_pct, is_narrow (bool)
    """
    try:
        db = get_db()
        normalized_symbol = normalize_symbol(symbol)
        cpr = db.get_cpr(normalized_symbol, trade_date)
        if not cpr:
            return {
                "error": f"No CPR data for {symbol} on {trade_date}. Check that DuckDB tables are built."
            }
        atr = db.get_atr(normalized_symbol, trade_date)
        threshold = db.get_cpr_threshold(normalized_symbol, trade_date)
        return {
            **cpr,
            "atr": atr,
            "cpr_threshold_pct": threshold,
            "is_narrow_cpr": (cpr["cpr_width_pct"] < threshold) if threshold else None,
        }
    except Exception:
        logger.exception("get_cpr_for_date failed: symbol=%s trade_date=%s", symbol, trade_date)
        return _internal_error("Internal error while fetching CPR for date.")


def get_data_status() -> dict:
    """
    Show current data and table status.
    Useful to check what's loaded before running backtests.

    Returns:
        parquet status, table row counts, symbol count, date range, candle count
    """
    try:
        db = get_db()
        return db.get_status()
    except Exception:
        logger.exception("get_data_status failed")
        return _internal_error("Internal error while fetching data status.")


def rebuild_indicators(
    force: bool = False,
    cpr_percentile: float = 33.0,
    atr_periods: int = 12,
) -> dict:
    """
    Rebuild DuckDB runtime tables (CPR/ATR/thresholds/OR/state-pack).

    Run after importing new Parquet data with pivot-convert.

    Args:
        force: Rebuild even if tables already exist (default False)

        cpr_percentile: CPR width threshold percentile (33 = bottom-third, lower = stricter)
        atr_periods: ATR lookback in 5-min candles (12 = last 1 hr)

    Returns:
        status message
    """
    try:
        db = get_db()
        db.build_all(force=force, cpr_percentile=cpr_percentile, atr_periods=atr_periods)
        return {
            "status": "ok",
            "message": "Runtime state-pack tables rebuilt successfully",
            "force": force,
            "cpr_percentile": cpr_percentile,
            "atr_periods": atr_periods,
        }
    except Exception:
        logger.exception(
            "rebuild_indicators failed: force=%s cpr_percentile=%s atr_periods=%s",
            force,
            cpr_percentile,
            atr_periods,
        )
        return _internal_error("Internal error while rebuilding indicator tables.")


async def _load_paper_snapshot(session_id: str) -> dict:
    session, positions, orders, feed_state = await asyncio.gather(
        get_session(session_id),
        get_session_positions(session_id),
        get_session_orders(session_id),
        get_feed_state(session_id),
    )
    if session is None:
        return {"session_id": session_id, "missing": True}
    summary = summarize_paper_positions(session, positions, feed_state)
    summary["orders"] = len(orders)
    return {
        "session": asdict(session),
        "positions": [asdict(position) for position in positions],
        "orders": [asdict(order) for order in orders],
        "feed_state": asdict(feed_state) if feed_state else None,
        "summary": summary,
    }


async def _load_paper_sessions() -> dict:
    sessions = await get_active_sessions()
    live = await asyncio.gather(*(_load_paper_snapshot(session.session_id) for session in sessions))
    archived = get_db().get_runs_with_metrics(execution_mode="PAPER")
    return {
        "active_sessions": [
            {
                "session": item.get("session"),
                "summary": item.get("summary"),
                "positions": item.get("positions") or [],
                "orders": item.get("orders") or [],
                "feed_state": item.get("feed_state"),
            }
            for item in live
            if item and item.get("session") is not None
        ],
        "archived_sessions": archived,
    }


def list_paper_sessions() -> dict:
    """Inspect active PostgreSQL paper sessions and archived DuckDB paper runs."""
    try:
        return asyncio.run(_load_paper_sessions())
    except Exception:
        logger.exception("list_paper_sessions failed")
        return _internal_error("Internal error while fetching paper sessions.")


def get_paper_session_summary(session_id: str) -> dict:
    """Inspect one live paper session from PostgreSQL."""
    try:
        return asyncio.run(_load_paper_snapshot(session_id))
    except Exception:
        logger.exception("get_paper_session_summary failed: session_id=%s", session_id)
        return _internal_error(
            "Internal error while fetching paper session summary.", session_id=session_id
        )


def get_paper_positions(session_id: str, include_closed: bool = True) -> dict:
    """Inspect live paper positions from PostgreSQL."""
    try:

        async def _load() -> dict:
            positions = await get_session_positions(session_id)
            if not include_closed:
                positions = [
                    p for p in positions if str(getattr(p, "status", "")).upper() == "OPEN"
                ]
            return {
                "session_id": session_id,
                "positions": [asdict(p) for p in positions],
                "count": len(positions),
            }

        return asyncio.run(_load())
    except Exception:
        logger.exception("get_paper_positions failed: session_id=%s", session_id)
        return _internal_error(
            "Internal error while fetching paper positions.", session_id=session_id
        )


def get_paper_ledger(session_id: str) -> dict:
    """Inspect an archived paper ledger from DuckDB."""
    try:
        db = get_db()
        run_meta = db.get_runs_with_metrics(execution_mode="PAPER")
        ledger = db.get_backtest_trades(session_id, execution_mode="PAPER")
        matching_meta = next((row for row in run_meta if row.get("run_id") == session_id), {})
        return {
            "session_id": session_id,
            "run_meta": matching_meta,
            "trades": ledger.to_dicts(),
            "trade_count": len(ledger),
        }
    except Exception:
        logger.exception("get_paper_ledger failed: session_id=%s", session_id)
        return _internal_error("get_paper_ledger failed", session_id=session_id)


def paper_send_command(
    session_id: str,
    action: str,
    symbols: list[str] | None = None,
    portfolio_value: float | None = None,
    max_positions: int | None = None,
    max_position_pct: float | None = None,
    reason: str = "agent_command",
) -> dict:
    """Send a control command to a live paper session.

    The command is written to a queue file that the live process picks up on its
    next poll cycle (~1s). The session keeps running after a 'close_positions' command;
    only 'close_all' terminates it like the sentinel file does. Entry pause/resume
    commands affect future entries only; open positions continue to be monitored.

    Args:
        session_id: Full session ID (e.g. 'CPR_LEVELS_LONG-2026-04-24-live-kite').
        action: 'close_positions' to close specific symbols, 'close_all' to end the session,
            'set_risk_budget' to reduce/adjust future-entry sizing, 'pause_entries',
            'resume_entries', or 'cancel_pending_intents'.
        symbols: Required for 'close_positions'. List of symbols to close (e.g. ['SBIN', 'RELIANCE']).
        portfolio_value: Optional new session budget for future entries.
        max_positions: Optional new concurrent-position cap for future entries.
        max_position_pct: Optional new per-position percentage cap for future entries.
        reason: Free-text reason logged in alerts and DB.

    Returns:
        Dict with 'command_file' path and 'action' confirmation.
    """
    try:
        allowed_actions = {
            "close_positions",
            "close_all",
            "set_risk_budget",
            "pause_entries",
            "resume_entries",
            "cancel_pending_intents",
        }
        if action not in allowed_actions:
            return _internal_error(
                f"Unknown action '{action}'. Use one of: {', '.join(sorted(allowed_actions))}."
            )
        if action == "close_positions" and not symbols:
            return _internal_error("'close_positions' requires a non-empty symbols list.")
        if action == "set_risk_budget" and all(
            value is None for value in (portfolio_value, max_positions, max_position_pct)
        ):
            return _internal_error(
                "'set_risk_budget' requires portfolio_value, max_positions, or max_position_pct."
            )
        cmd_file = write_admin_command(
            session_id,
            action,
            symbols=symbols,
            portfolio_value=portfolio_value,
            max_positions=max_positions,
            max_position_pct=max_position_pct,
            reason=reason,
            requester="agent",
        )
        return {
            "command_file": cmd_file,
            "action": action,
            "symbols": symbols,
            "portfolio_value": portfolio_value,
            "max_positions": max_positions,
            "max_position_pct": max_position_pct,
            "session_id": session_id,
        }
    except Exception:
        logger.exception("paper_send_command failed: session_id=%s action=%s", session_id, action)
        return _internal_error("paper_send_command failed")
