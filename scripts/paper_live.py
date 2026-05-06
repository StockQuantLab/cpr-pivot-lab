"""Live paper-session runner with transport-agnostic candle processing."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from config.settings import get_settings
from db.duckdb import get_live_market_db
from db.paper_db import get_paper_db
from engine import paper_session_driver as paper_session_driver
from engine.bar_orchestrator import SessionPositionTracker
from engine.command_lock import acquire_command_lock
from engine.cpr_atr_shared import regime_snapshot_close_col
from engine.kite_ticker_adapter import KiteTickerAdapter
from engine.live_market_data import (
    IST,
    ClosedCandle,
    FiveMinuteCandleBuilder,
    KiteQuoteAdapter,
    MarketDataAdapter,
)
from engine.paper_reconciliation import reconcile_paper_session
from engine.paper_runtime import (
    PaperRuntimeState,
    SymbolRuntimeState,
    _start_alert_dispatcher,
    build_backtest_params,
    build_summary_feed_state,
    dispatch_feed_recovered_alert,
    dispatch_feed_stale_alert,
    dispatch_session_completed_alert,
    dispatch_session_error_alert,
    dispatch_session_started_alert,
    enforce_session_risk_controls,
    evaluate_candle,
    execute_entry,
    flatten_positions_subset,
    flatten_session_positions,
    force_paper_db_sync,
    get_session_positions,
    load_setup_row,
    maybe_shutdown_alert_dispatcher,
    register_session_start,
)
from engine.paper_setup_loader import _or_proxy_and_source, setup_row_uses_or_proxy
from engine.real_order_runtime import build_real_order_router
from scripts import paper_live_helpers as _live_helpers
from scripts.paper_archive import archive_completed_session
from scripts.paper_feed_audit import record_closed_candles, record_signal_decisions
from scripts.paper_prepare import pre_filter_symbols_for_strategy

logger = logging.getLogger(__name__)
_GLOBAL_FLATTEN_SIGNAL = _live_helpers.GLOBAL_FLATTEN_SIGNAL
_feed_snapshot_payload = _live_helpers.feed_snapshot_payload
_closed_candle_payload = _live_helpers.closed_candle_payload
_live_mark_feed_state = _live_helpers.live_mark_feed_state
_entry_disabled_symbols = _live_helpers.entry_disabled_symbols
_cancel_pending_admin_commands = _live_helpers.cancel_pending_admin_commands
_is_admin_command_stale = _live_helpers.is_admin_command_stale
_is_zero_trade_restart_session = _live_helpers.is_zero_trade_restart_session
_should_use_global_flatten_signal = _live_helpers.should_use_global_flatten_signal
_resolve_poll_interval = _live_helpers.resolve_poll_interval
_resolve_candle_interval = _live_helpers.resolve_candle_interval
_resolve_active_symbols = _live_helpers.resolve_active_symbols
_floor_bucket_start = _live_helpers.floor_bucket_start
_normalize_setup_row_direction = _live_helpers.normalize_setup_row_direction
_log_parity_trace = _live_helpers.log_parity_trace
_log_bar_heartbeats = _live_helpers.log_bar_heartbeats
_log_ticker_health = _live_helpers.log_ticker_health
_ticker_last_tick_age_sec = _live_helpers.ticker_last_tick_age_sec
_log_direction_readiness = _live_helpers.log_direction_readiness
_BOOL_TRUE = {"1", "true", "yes", "on"}
_PARITY_TRACE_ENABLED = str(os.getenv("PIVOT_LIVE_PARITY_TRACE", "0")).strip().lower() in _BOOL_TRUE
_SETUP_PARITY_CHECK_ENABLED = (
    str(os.getenv("PIVOT_LIVE_SETUP_PARITY_CHECK", "0")).strip().lower() in _BOOL_TRUE
)
_ORIGINAL_LOAD_SETUP_ROW = load_setup_row
_WEBSOCKET_RECONNECT_ALERT_ATTEMPTS = 3
_WEBSOCKET_RECOVERY_AFTER_SEC = 20.0
_WEBSOCKET_RECOVERY_COOLDOWN_SEC = 30.0


def _session_lock_name(session_id: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(session_id))
    return f"paper-session-{safe[:120]}"


_FEED_AUDIT_CLEANUP_INTERVAL_SEC = 30 * 60
try:
    _FEED_STALE_ALERT_COOLDOWN_SEC = float(os.getenv("PIVOT_FEED_STALE_ALERT_COOLDOWN_SEC", "900"))
except ValueError:
    _FEED_STALE_ALERT_COOLDOWN_SEC = 900.0


def _or_range_end(trade_date: str, or_minutes: int) -> datetime:
    trading_day = datetime.fromisoformat(str(trade_date)).date()
    return datetime.combine(trading_day, dt_time(9, 15), tzinfo=IST) + timedelta(
        minutes=max(1, int(or_minutes or 5))
    )


def _coerce_kite_candle_start(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)


def _kite_history_to_live_candles(
    symbol: str,
    candles: list[dict[str, Any]],
    *,
    trade_date: str,
    or_minutes: int,
) -> list[dict[str, Any]]:
    range_start = datetime.combine(
        datetime.fromisoformat(str(trade_date)).date(), dt_time(9, 15), tzinfo=IST
    )
    range_end = range_start + timedelta(minutes=max(1, int(or_minutes or 5)))
    live_candles: list[dict[str, Any]] = []
    for candle in candles:
        bar_start = _coerce_kite_candle_start(candle.get("date"))
        if bar_start is None or not (range_start <= bar_start < range_end):
            continue
        bar_end = bar_start + timedelta(minutes=5)
        live_candles.append(
            {
                "symbol": symbol,
                "time_str": bar_end.strftime("%H:%M"),
                "bar_end": bar_end,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle.get("volume") or 0.0),
            }
        )
    return sorted(live_candles, key=lambda row: row["bar_end"])


def _merge_state_candles(state: SymbolRuntimeState, candles: list[dict[str, Any]]) -> None:
    if not candles:
        return
    merged: dict[datetime, dict[str, Any]] = {}
    for candle in [*candles, *state.candles]:
        bar_end = candle.get("bar_end")
        if isinstance(bar_end, datetime):
            merged[bar_end] = dict(candle)
    state.candles = [merged[key] for key in sorted(merged)]


def _symbols_needing_true_or(runtime_state: PaperRuntimeState, symbols: list[str]) -> list[str]:
    needed: list[str] = []
    for symbol in dict.fromkeys(symbols):
        state = runtime_state.symbols.get(symbol)
        setup_row = state.setup_row if state is not None else None
        if (
            setup_row is None
            or bool(setup_row.get("direction_pending"))
            or float(setup_row.get("open_915") or 0.0) <= 0.0
            or float(setup_row.get("or_high_5") or 0.0) <= 0.0
            or float(setup_row.get("or_low_5") or 0.0) <= 0.0
            or setup_row.get("or_close_5") is None
            or setup_row_uses_or_proxy(setup_row)
        ):
            needed.append(symbol)
    return needed


def _is_local_feed_adapter(ticker_adapter: Any) -> bool:
    return bool(getattr(ticker_adapter, "_local_feed", False))


def _allow_or_proxy_setup_for_adapter(ticker_adapter: Any) -> bool:
    # Kite live must not synthesize OR from the first post-start bar. Local-feed
    # diagnostics may still use the proxy fallback for continuity tests.
    return _is_local_feed_adapter(ticker_adapter)


def _clear_unresolved_setup_rows(runtime_state: PaperRuntimeState, symbols: list[str]) -> None:
    for symbol in symbols:
        state = runtime_state.symbols.get(symbol)
        if state is None or state.setup_row is None:
            continue
        if (
            bool(state.setup_row.get("direction_pending"))
            or float(state.setup_row.get("open_915") or 0.0) <= 0.0
            or float(state.setup_row.get("or_high_5") or 0.0) <= 0.0
            or float(state.setup_row.get("or_low_5") or 0.0) <= 0.0
            or state.setup_row.get("or_close_5") is None
            or setup_row_uses_or_proxy(state.setup_row)
        ):
            state.setup_row = None


def _catch_up_true_or_from_kite(
    *,
    runtime_state: PaperRuntimeState,
    symbols: list[str],
    trade_date: str,
    or_minutes: int,
    session_id: str,
    shared_or_cache: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, int]:
    if not symbols:
        return {"requested": 0, "fetched": 0, "missing": 0, "errors": 0}
    if datetime.now(IST) < _or_range_end(trade_date, or_minutes):
        logger.info(
            "[%s] OR historical catch-up skipped: OR window still open for %s",
            session_id,
            trade_date,
        )
        return {"requested": len(symbols), "fetched": 0, "missing": 0, "errors": 0}

    from engine.kite_ingestion import (
        _historical_data_with_retry,
        get_kite_client,
        resolve_instrument_tokens,
    )

    requested = list(dict.fromkeys(symbols))
    token_map, missing_instruments = resolve_instrument_tokens(requested, exchange="NSE")
    kite = get_kite_client()
    trading_day = datetime.fromisoformat(str(trade_date)).date()
    range_start = datetime.combine(trading_day, dt_time(9, 15))
    from_ts = range_start.strftime("%Y-%m-%d %H:%M:%S")
    to_ts = (range_start + timedelta(minutes=max(1, int(or_minutes or 5)))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    fetched = 0
    cached = 0
    errors = 0
    for symbol in requested:
        if shared_or_cache is not None and symbol in shared_or_cache:
            live_candles = list(shared_or_cache.get(symbol) or [])
            if live_candles:
                _merge_state_candles(runtime_state.for_symbol(symbol), live_candles)
                cached += 1
            continue
        token = token_map.get(symbol)
        if token is None:
            if shared_or_cache is not None:
                shared_or_cache[symbol] = []
            continue
        try:
            raw_candles = _historical_data_with_retry(
                kite,
                token,
                "5minute",
                from_ts,
                to_ts,
                attempts=2,
            )
        except Exception as exc:
            errors += 1
            logger.warning(
                "[%s] OR historical catch-up failed for %s on %s: %s",
                session_id,
                symbol,
                trade_date,
                exc,
            )
            if shared_or_cache is not None:
                shared_or_cache[symbol] = []
            continue
        live_candles = _kite_history_to_live_candles(
            symbol, raw_candles, trade_date=trade_date, or_minutes=or_minutes
        )
        if shared_or_cache is not None:
            shared_or_cache[symbol] = list(live_candles)
        if not live_candles:
            continue
        _merge_state_candles(runtime_state.for_symbol(symbol), live_candles)
        fetched += 1

    missing = len(requested) - fetched - cached
    logger.warning(
        "[%s] OR historical catch-up complete trade_date=%s requested=%d fetched=%d "
        "cached=%d missing=%d missing_instruments=%d errors=%d",
        session_id,
        trade_date,
        len(requested),
        fetched,
        cached,
        missing,
        len(missing_instruments),
        errors,
    )
    return {
        "requested": len(requested),
        "fetched": fetched + cached,
        "missing": missing,
        "errors": errors,
    }


def _catch_up_kite_true_or_if_needed(
    *,
    runtime_state: PaperRuntimeState,
    active_symbols: list[str],
    trade_date: str,
    candle_interval: int,
    session_id: str,
    ticker_adapter: Any,
    strategy_params: dict[str, Any],
    shared_or_cache: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, int] | None:
    if _is_local_feed_adapter(ticker_adapter):
        return None

    catchup_symbols = _symbols_needing_true_or(runtime_state, active_symbols)
    if not catchup_symbols:
        return {"requested": 0, "fetched": 0, "missing": 0, "errors": 0}

    try:
        result = _catch_up_true_or_from_kite(
            runtime_state=runtime_state,
            symbols=catchup_symbols,
            trade_date=trade_date,
            or_minutes=candle_interval,
            session_id=session_id,
            shared_or_cache=shared_or_cache,
        )
    except Exception as exc:
        logger.warning(
            "[%s] OR historical catch-up unavailable for %d symbols on %s: %s",
            session_id,
            len(catchup_symbols),
            trade_date,
            exc,
        )
        result = {
            "requested": len(catchup_symbols),
            "fetched": 0,
            "missing": len(catchup_symbols),
            "errors": len(catchup_symbols),
        }
    _clear_unresolved_setup_rows(runtime_state, catchup_symbols)
    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=catchup_symbols,
        trade_date=trade_date,
        candle_interval_minutes=candle_interval,
        regime_index_symbol=str(strategy_params.get("regime_index_symbol") or ""),
        regime_snapshot_minutes=int(strategy_params.get("regime_snapshot_minutes") or 30),
    )
    return result


@dataclass(slots=True)
class LiveSessionDeps:
    session_loader: Callable[[str], Any] | None = None
    session_updater: Callable[..., Any] | None = None
    feed_writer: Callable[..., Any] | None = None
    feed_reader: Callable[[str], Any] | None = None
    sleep_fn: Callable[[float], Awaitable[None]] | None = None
    now_fn: Callable[[], datetime] | None = None
    alerts_enabled: bool | None = None


@dataclass(slots=True)
class LiveMultiSessionSpec:
    session_id: str
    symbols: list[str]
    notes: str | None = None
    real_order_config: dict[str, Any] | None = None


@dataclass(slots=True)
class _LiveMultiContext:
    session_id: str
    session: Any
    strategy: str
    params: Any
    direction_filter: str
    active_symbols: list[str]
    runtime_state: PaperRuntimeState
    tracker: SessionPositionTracker
    real_order_router: Any
    symbol_last_prices: dict[str, float]
    builder: FiveMinuteCandleBuilder
    notes: str | None
    stage_b_applied: bool = False
    entries_disabled: bool = False
    entry_resume_symbols: list[str] | None = None
    entry_universe_symbols: list[str] | None = None
    closed_bars: int = 0
    quote_events: int = 0
    final_status: str = "ACTIVE"
    terminal_reason: str | None = None
    last_bar_ts: datetime | None = None
    last_snapshot_ts: datetime | None = None
    last_price: float | None = None
    feed_ready_marked: bool = False


def _cleanup_feed_audit_if_needed(
    *,
    now: datetime,
    last_cleanup: datetime | None,
    settings: Any,
) -> tuple[datetime | None, int]:
    """Purge old feed-audit rows if retention is configured and interval elapsed."""
    retention_days = int(getattr(settings, "feed_audit_retention_days", 0) or 0)
    if retention_days <= 0:
        return last_cleanup, 0
    if (
        last_cleanup is not None
        and (now - last_cleanup).total_seconds() < _FEED_AUDIT_CLEANUP_INTERVAL_SEC
    ):
        return last_cleanup, 0
    try:
        paper_db = get_paper_db()
        deleted = paper_db.cleanup_feed_audit_older_than(retention_days)
        deleted_alerts = paper_db.cleanup_alert_log_older_than(retention_days)
        total_deleted = deleted + deleted_alerts
        if total_deleted:
            logger.info(
                "run_live_session purged %d paper_feed_audit rows and %d alert_log rows older than %d day(s)",
                deleted,
                deleted_alerts,
                retention_days,
            )
        return now, total_deleted
    except Exception:
        logger.debug("Feed audit retention cleanup skipped due to error", exc_info=True)
        return last_cleanup, 0


def _reconcile_live_session(
    *,
    session_id: str,
    reason: str,
    alerts_enabled: bool,
) -> bool:
    """Return True when critical findings require entry-disable mode."""
    paper_db = get_paper_db()
    if not hasattr(paper_db, "get_session"):
        logger.debug(
            "[%s] Skipping reconciliation for injected paper DB after %s", session_id, reason
        )
        return False
    payload = reconcile_paper_session(paper_db, session_id)
    critical = int((payload.get("summary") or {}).get("critical") or 0)
    if critical <= 0:
        return False

    logger.error(
        "[%s] Reconciliation critical findings after %s: %s",
        session_id,
        reason,
        payload.get("findings"),
    )
    if alerts_enabled:
        dispatch_session_error_alert(
            session_id=session_id,
            reason="reconciliation_critical",
            details=f"after={reason} critical={critical}",
        )
    try:
        get_paper_db().update_session(
            session_id,
            notes=f"ENTRY_DISABLED_RECONCILIATION after={reason} critical={critical}",
        )
    except Exception:
        logger.debug("[%s] Failed to stamp reconciliation note", session_id, exc_info=True)
    return True


def _session_now(deps: LiveSessionDeps | None = None) -> datetime:
    if deps and deps.now_fn is not None:
        return deps.now_fn()
    return datetime.now(IST)


async def _sleep(deps: LiveSessionDeps | None, seconds: float) -> None:
    if deps and deps.sleep_fn is not None:
        await deps.sleep_fn(seconds)
        return
    await asyncio.sleep(seconds)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _load_session(session_id: str, deps: LiveSessionDeps | None = None) -> Any:
    if deps and deps.session_loader is not None:
        return await _maybe_await(deps.session_loader(session_id))
    return get_paper_db().get_session(session_id)


async def _update_session(session_id: str, deps: LiveSessionDeps | None = None, **kwargs) -> Any:
    if deps and deps.session_updater is not None:
        return await _maybe_await(deps.session_updater(session_id, **kwargs))
    return get_paper_db().update_session(session_id, **kwargs)


async def _write_feed_state(deps: LiveSessionDeps | None = None, **kwargs) -> Any:
    if deps and deps.feed_writer is not None:
        return await _maybe_await(deps.feed_writer(**kwargs))
    return get_paper_db().upsert_feed_state(**kwargs)


async def _mark_multi_feed_ok_from_ticks(
    ctx: _LiveMultiContext,
    ticker_adapter: Any,
) -> None:
    """Publish a lightweight feed heartbeat before the first closed candle arrives."""
    if ctx.feed_ready_marked:
        return
    last_tick_ts = getattr(ticker_adapter, "last_tick_ts", None)
    if last_tick_ts is None:
        return
    await _write_feed_state(
        None,
        session_id=ctx.session_id,
        status="OK",
        last_event_ts=last_tick_ts,
        last_bar_ts=ctx.last_bar_ts,
        last_price=ctx.last_price,
        stale_reason=None,
        raw_state={
            "connected": bool(getattr(ticker_adapter, "is_connected", False)),
            "active_symbols": len(ctx.active_symbols),
            "closed_bars": ctx.closed_bars,
            "quote_events": ctx.quote_events,
            "pre_bar_heartbeat": True,
            "transport": "websocket",
        },
    )
    ctx.feed_ready_marked = True


def _admin_command_sort_key(path: Path) -> tuple[int, str]:
    try:
        action = str((json.loads(path.read_text()) or {}).get("action") or "")
    except Exception:
        action = ""
    return (0 if action == "cancel_pending_intents" else 1, path.name)


def _resolve_trade_date(session: Any) -> str:
    raw = str(getattr(session, "trade_date", "") or "").strip()
    if raw:
        return raw[:10]
    return datetime.now(IST).date().isoformat()


def _log_setup_row_parity(symbol: str, trade_date: str, setup_row: dict[str, Any] | None) -> None:
    if not _SETUP_PARITY_CHECK_ENABLED:
        return
    if not setup_row:
        return
    from engine.paper_runtime import _MARKET_DB_READ_LOCK

    db = get_live_market_db()
    with _MARKET_DB_READ_LOCK:
        row = db.con.execute(
            """
            SELECT tc, bc, atr
            FROM market_day_state
            WHERE symbol = ? AND trade_date = ?::DATE
            LIMIT 1
            """,
            [symbol, trade_date],
        ).fetchone()
    if not row:
        return
    expected_tc, expected_bc, expected_atr = float(row[0]), float(row[1]), float(row[2])
    got_tc = float(setup_row.get("tc") or 0.0)
    got_bc = float(setup_row.get("bc") or 0.0)
    got_atr = float(setup_row.get("atr") or 0.0)
    if (
        abs(expected_tc - got_tc) > 1e-6
        or abs(expected_bc - got_bc) > 1e-6
        or abs(expected_atr - got_atr) > 1e-6
    ):
        logger.warning(
            "SETUP_PARITY_MISMATCH symbol=%s trade_date=%s market_day_state(tc=%.6f bc=%.6f atr=%.6f)"
            " runtime(tc=%.6f bc=%.6f atr=%.6f)",
            symbol,
            trade_date,
            expected_tc,
            expected_bc,
            expected_atr,
            got_tc,
            got_bc,
            got_atr,
        )


def _prefetch_setup_rows(
    *,
    runtime_state: PaperRuntimeState,
    symbols: list[str],
    trade_date: str,
    candle_interval_minutes: int,
    regime_index_symbol: str = "",
    regime_snapshot_minutes: int = 30,
) -> None:
    missing_symbols: list[str] = []
    invalid_symbols: list[tuple[str, float, float, float]] = []
    unique_symbols = list(dict.fromkeys(symbols))
    regime_close_col = regime_snapshot_close_col(regime_snapshot_minutes)

    def _hydrate_setup_row(
        *,
        symbol: str,
        row: tuple[Any, ...],
        live_candles: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        from engine.cpr_atr_utils import resolve_cpr_direction

        tc = float(row[3] or 0.0)
        bc = float(row[4] or 0.0)
        atr = float(row[10] or 0.0)
        if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
            invalid_symbols.append((symbol, tc, bc, atr))
            return None
        prev_close = float(row[2]) if row[2] is not None else None
        or_close_5 = float(row[16]) if row[16] is not None else None
        direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
        if direction == "NONE" and or_close_5 is None:
            direction = str(row[21] or "NONE")
        live_intraday: dict[str, Any] | None = None
        db_or_high = float(row[13] or 0.0)
        db_or_low = float(row[14] or 0.0)
        db_open_915 = float(row[15] or 0.0)
        if live_candles and (
            direction == "NONE"
            or db_or_high <= 0.0
            or db_or_low <= 0.0
            or db_open_915 <= 0.0
            or or_close_5 is None
        ):
            from engine.paper_setup_loader import _build_intraday_summary

            intraday = _build_intraday_summary(
                live_candles,
                or_minutes=candle_interval_minutes,
                bar_end_offset=runtime_state.bar_end_offset,
            )
            live_intraday = intraday
            live_or_close_5 = intraday.get("or_close_5")
            if live_or_close_5 is not None:
                direction = resolve_cpr_direction(live_or_close_5, tc, bc, fallback="NONE")
                or_close_5 = live_or_close_5
        or_high_5 = db_or_high
        or_low_5 = db_or_low
        open_915_val = db_open_915
        open_side = str(row[17] or "")
        if live_intraday is not None:
            from engine.cpr_atr_utils import (
                calculate_gap_pct,
                calculate_or_atr_ratio,
                normalize_cpr_bounds,
            )

            or_high_5 = db_or_high or float(live_intraday.get("or_high_5") or 0.0)
            or_low_5 = db_or_low or float(live_intraday.get("or_low_5") or 0.0)
            open_915_val = db_open_915 or float(live_intraday.get("open_915") or 0.0)
            if open_915_val > 0:
                cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
                if open_915_val < cpr_lower:
                    open_side = "BELOW"
                elif open_915_val > cpr_upper:
                    open_side = "ABOVE"
                else:
                    open_side = "INSIDE"
                open_to_cpr_atr = (
                    abs(open_915_val - (cpr_lower if open_side == "BELOW" else cpr_upper)) / atr
                    if open_side in {"BELOW", "ABOVE"}
                    else 0.0
                )
                gap_abs_pct = (
                    abs(calculate_gap_pct(open_915_val, prev_close))
                    if prev_close is not None
                    else None
                )
                or_atr_5 = (
                    calculate_or_atr_ratio(or_high_5, or_low_5, atr)
                    if or_high_5 > 0 and or_low_5 > 0
                    else None
                )
            else:
                open_to_cpr_atr = float(row[18]) if row[18] is not None else None
                gap_abs_pct = float(row[19]) if row[19] is not None else None
                or_atr_5 = float(row[20]) if row[20] is not None else None
        else:
            open_to_cpr_atr = float(row[18]) if row[18] is not None else None
            gap_abs_pct = float(row[19]) if row[19] is not None else None
            or_atr_5 = float(row[20]) if row[20] is not None else None
        or_proxy, setup_source = _or_proxy_and_source(live_intraday)
        rvol_baseline: list[float | None] | None = None
        if row[25]:
            rvol_baseline = [float(v) if v is not None else None for v in row[25]]
        setup_row = {
            "trade_date": str(row[1] or trade_date),
            "prev_day_close": prev_close,
            "tc": tc,
            "bc": bc,
            "pivot": float(row[5] or 0.0),
            "r1": float(row[6] or 0.0),
            "s1": float(row[7] or 0.0),
            "r2": float(row[8] or 0.0),
            "s2": float(row[9] or 0.0),
            "atr": atr,
            "cpr_width_pct": float(row[11] or 0.0),
            "cpr_threshold": float(row[12] or 0.0),
            "or_high_5": or_high_5,
            "or_low_5": or_low_5,
            "open_915": open_915_val,
            "or_close_5": or_close_5,
            "open_side": open_side,
            "open_to_cpr_atr": open_to_cpr_atr,
            "gap_abs_pct": gap_abs_pct,
            "or_atr_5": or_atr_5,
            "direction": direction,
            "is_narrowing": bool(row[22]),
            "cpr_shift": str(row[23] or "OVERLAP"),
            "regime_move_pct": float(row[24]) if row[24] is not None else None,
            "rvol_baseline": rvol_baseline,
            "or_proxy": or_proxy,
            "setup_source": setup_source,
        }
        return setup_row

    batch_rows: dict[str, tuple[Any, ...]] | None = None
    use_batch_prefetch = load_setup_row is _ORIGINAL_LOAD_SETUP_ROW and bool(unique_symbols)
    if use_batch_prefetch:
        try:
            from engine.paper_runtime import _MARKET_DB_READ_LOCK

            db = get_live_market_db()
            placeholders = ", ".join(["?"] * len(unique_symbols))
            query = f"""
                SELECT
                    m.symbol,
                    m.trade_date::VARCHAR,
                    m.prev_close,
                    m.tc,
                    m.bc,
                    m."pivot",
                    m.r1,
                    m.s1,
                    m.r2,
                    m.s2,
                    m.atr,
                    m.cpr_width_pct,
                    m.cpr_threshold_pct,
                    m.or_high_5,
                    m.or_low_5,
                    m.open_915,
                    m.or_close_5,
                    s.open_side,
                    s.open_to_cpr_atr,
                    s.gap_abs_pct,
                    s.or_atr_5,
                    s.direction_5,
                    m.is_narrowing,
                    m.cpr_shift,
                    CASE
                        WHEN reg.open_915 > 0 AND reg.{regime_close_col} IS NOT NULL
                        THEN ((reg.{regime_close_col} - reg.open_915) / reg.open_915) * 100.0
                        ELSE NULL
                    END AS regime_move_pct,
                    p.rvol_baseline_arr
                FROM market_day_state m
                LEFT JOIN strategy_day_state s
                  ON s.symbol = m.symbol
                 AND s.trade_date = m.trade_date
                LEFT JOIN intraday_day_pack p
                  ON p.symbol = m.symbol
                 AND p.trade_date = m.trade_date
                LEFT JOIN market_day_state reg
                  ON reg.symbol = ?
                 AND reg.trade_date = m.trade_date
                WHERE m.trade_date = ?::DATE
                  AND m.symbol IN ({placeholders})
            """
            with _MARKET_DB_READ_LOCK:
                rows = db.con.execute(
                    query,
                    [regime_index_symbol, trade_date, *unique_symbols],
                ).fetchall()
            batch_rows = {str(row[0]): row for row in rows}
        except Exception:
            logger.exception("Batch setup prefetch failed; falling back to serial loading")

    if batch_rows is not None:
        for symbol in unique_symbols:
            state = runtime_state.symbols.setdefault(symbol, SymbolRuntimeState())
            if state.setup_row is not None:
                continue
            row = batch_rows.get(symbol)
            if row is None:
                if not runtime_state.allow_live_setup_fallback:
                    missing_symbols.append(symbol)
                    continue
                setup_row = load_setup_row(
                    symbol,
                    trade_date,
                    live_candles=state.candles,
                    or_minutes=candle_interval_minutes,
                    allow_live_fallback=True,
                    bar_end_offset=runtime_state.bar_end_offset,
                    regime_index_symbol=regime_index_symbol,
                    regime_snapshot_minutes=regime_snapshot_minutes,
                )
                if setup_row is None:
                    missing_symbols.append(symbol)
                    continue
                if not runtime_state.allow_or_proxy_setup and setup_row_uses_or_proxy(setup_row):
                    missing_symbols.append(symbol)
                    continue
                state.setup_row = _normalize_setup_row_direction(setup_row)
                _log_setup_row_parity(symbol, trade_date, state.setup_row)
                continue
            setup_row = _hydrate_setup_row(symbol=symbol, row=row, live_candles=state.candles)
            if setup_row is None:
                continue
            if not runtime_state.allow_or_proxy_setup and setup_row_uses_or_proxy(setup_row):
                missing_symbols.append(symbol)
                continue
            if row[24] is not None:
                setup_row["regime_move_pct"] = float(row[24])
            state.setup_row = _normalize_setup_row_direction(setup_row)
            _log_setup_row_parity(symbol, trade_date, state.setup_row)
    else:
        for symbol in unique_symbols:
            state = runtime_state.symbols.setdefault(symbol, SymbolRuntimeState())
            if state.setup_row is not None:
                continue
            setup_row = load_setup_row(
                symbol,
                trade_date,
                live_candles=state.candles,
                or_minutes=candle_interval_minutes,
                allow_live_fallback=runtime_state.allow_live_setup_fallback,
                bar_end_offset=runtime_state.bar_end_offset,
                regime_index_symbol=regime_index_symbol,
                regime_snapshot_minutes=regime_snapshot_minutes,
            )
            if setup_row is None:
                missing_symbols.append(symbol)
                continue
            tc = float(setup_row.get("tc") or 0.0)
            bc = float(setup_row.get("bc") or 0.0)
            atr = float(setup_row.get("atr") or 0.0)
            # Guard against caching incomplete fast-path setup rows.
            # If these fields are unusable, keep the symbol unresolved so strict mode
            # fails fast (or permissive mode can attempt late fallback reads).
            if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
                invalid_symbols.append((symbol, tc, bc, atr))
                continue
            setup_row.setdefault("setup_source", "market_day_state")
            if not runtime_state.allow_or_proxy_setup and setup_row_uses_or_proxy(setup_row):
                missing_symbols.append(symbol)
                continue
            state.setup_row = _normalize_setup_row_direction(setup_row)
            _log_setup_row_parity(symbol, trade_date, state.setup_row)
    if invalid_symbols:
        sample = ", ".join(
            f"{symbol}(tc={tc:.4f},bc={bc:.4f},atr={atr:.4f})"
            for symbol, tc, bc, atr in sorted(invalid_symbols)[:10]
        )
        logger.warning(
            "Setup prefetch skipped %d invalid rows on %s (critical fields <= 0): %s",
            len(invalid_symbols),
            trade_date,
            sample,
        )
    if missing_symbols:
        logger.warning(
            "Setup prefetch missing rows for %d symbols on %s; those symbols will be skipped",
            len(set(missing_symbols)),
            trade_date,
        )
    runtime_state.skipped_setup_rows += len(missing_symbols)
    runtime_state.invalid_setup_rows += len(invalid_symbols)


async def _prepare_live_multi_context(
    *,
    spec: LiveMultiSessionSpec,
    ticker_adapter: Any,
    candle_interval: int,
    allow_live_setup_fallback: bool,
    shared_or_cache: dict[str, list[dict[str, Any]]] | None = None,
) -> _LiveMultiContext:
    session = await _load_session(spec.session_id)
    if session is None:
        raise RuntimeError(f"session {spec.session_id!r} not found")
    session_status = str(getattr(session, "status", "") or "").upper()
    if session_status in {"COMPLETED", "CANCELLED", "STOPPING"}:
        raise RuntimeError(f"session {spec.session_id!r} already terminal: {session_status}")

    trade_date = _resolve_trade_date(session)
    strategy = str(getattr(session, "strategy", "") or "CPR_LEVELS")
    strategy_params = dict(getattr(session, "strategy_params", {}) or {})
    active_symbols = pre_filter_symbols_for_strategy(
        trade_date,
        _resolve_active_symbols(session, spec.symbols),
        strategy,
        strategy_params,
        require_trade_date_rows=not bool(getattr(ticker_adapter, "_local_feed", False)),
    )
    if not active_symbols:
        raise RuntimeError(f"session {spec.session_id!r} has no symbols after Stage A pre-filter")
    if session_status not in {"ACTIVE", "PAUSED"}:
        session = await _update_session(spec.session_id, None, status="ACTIVE", notes=spec.notes)
        force_paper_db_sync(get_paper_db())

    params = build_backtest_params(session)
    real_order_router = build_real_order_router(spec.real_order_config)
    runtime_state = PaperRuntimeState(
        allow_live_setup_fallback=allow_live_setup_fallback,
        allow_or_proxy_setup=_allow_or_proxy_setup_for_adapter(ticker_adapter),
        bar_end_offset=timedelta(minutes=5) if _is_local_feed_adapter(ticker_adapter) else None,
    )
    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=active_symbols,
        trade_date=trade_date,
        candle_interval_minutes=candle_interval,
        regime_index_symbol=str(strategy_params.get("regime_index_symbol") or ""),
        regime_snapshot_minutes=int(strategy_params.get("regime_snapshot_minutes") or 30),
    )
    _catch_up_kite_true_or_if_needed(
        runtime_state=runtime_state,
        active_symbols=active_symbols,
        trade_date=trade_date,
        candle_interval=candle_interval,
        session_id=spec.session_id,
        ticker_adapter=ticker_adapter,
        strategy_params=strategy_params,
        shared_or_cache=shared_or_cache,
    )
    direction_readiness = _log_direction_readiness(
        session_id=spec.session_id,
        runtime_state=runtime_state,
        active_symbols=active_symbols,
    )
    if active_symbols and int(direction_readiness["with_setup"]) == 0:
        reason = "startup_missing_trade_date_setup_rows"
        await _update_session(spec.session_id, None, status="FAILED", notes=reason)
        force_paper_db_sync(get_paper_db())
        raise RuntimeError(f"session {spec.session_id!r} startup blocked: {reason}")

    tracker = SessionPositionTracker(
        max_positions=int(getattr(session, "max_positions", 1) or 1),
        portfolio_value=float(getattr(params, "portfolio_value", 0.0) or 0.0),
        max_position_pct=float(getattr(params, "max_position_pct", 0.0) or 0.0),
    )
    tracker.seed_open_positions(await get_session_positions(spec.session_id, statuses=["OPEN"]))
    for closed_pos in await get_session_positions(
        spec.session_id, statuses=["CLOSED", "FLATTENED"]
    ):
        tracker.mark_traded(closed_pos.symbol)
        runtime_state.for_symbol(closed_pos.symbol).position_closed_today = True

    if real_order_router is not None:
        logger.warning(
            "[%s] broker order routing enabled for multi-live mode=%s fixed_qty=%d",
            spec.session_id,
            real_order_router.adapter.mode,
            real_order_router.config.fixed_quantity,
        )

    builder = FiveMinuteCandleBuilder(interval_minutes=candle_interval)
    ticker_adapter.register_session(spec.session_id, active_symbols, builder)
    await _write_feed_state(
        None,
        session_id=spec.session_id,
        status="CONNECTING",
        last_event_ts=None,
        last_bar_ts=None,
        last_price=None,
        stale_reason=None,
        raw_state={"mode": "multi_startup", "symbols": len(active_symbols)},
    )
    if session_status in {"PLANNING", ""}:
        dispatch_session_started_alert(
            session_id=spec.session_id,
            strategy=strategy,
            direction=str(getattr(params, "direction_filter", "BOTH") or "BOTH").upper(),
            symbol_count=len(active_symbols),
            trade_date=trade_date,
        )
    return _LiveMultiContext(
        session_id=spec.session_id,
        session=session,
        strategy=strategy,
        params=params,
        direction_filter=str(getattr(params, "direction_filter", "BOTH") or "BOTH").upper(),
        active_symbols=active_symbols,
        runtime_state=runtime_state,
        tracker=tracker,
        real_order_router=real_order_router,
        symbol_last_prices={},
        builder=builder,
        notes=spec.notes,
        entry_resume_symbols=list(active_symbols),
        entry_universe_symbols=list(active_symbols),
    )


async def _process_live_multi_bar(
    *,
    ctx: _LiveMultiContext,
    bar_end: datetime,
    bar_candles: list[ClosedCandle],
    ticker_adapter: Any,
    feed_source: str,
    transport: str,
) -> None:
    bar_candles = sorted(bar_candles, key=lambda c: c.symbol)
    for candle in bar_candles:
        ctx.closed_bars += 1
        ctx.last_bar_ts = candle.bar_end
        ctx.last_price = candle.close
        ctx.symbol_last_prices[candle.symbol] = candle.close
        state = ctx.runtime_state.symbols.get(candle.symbol)
        setup_row = state.setup_row if state is not None else None
        _log_parity_trace(session_id=ctx.session_id, candle=candle, setup_row=setup_row)

    logger.info(
        "LIVE_MULTI_BAR_PROCESS_START session=%s bar_end=%s symbols=%d",
        ctx.session_id,
        bar_end.isoformat(),
        len(bar_candles),
    )
    started = datetime.now(IST)
    get_paper_db().defer_sync()
    try:
        driver_result = await paper_session_driver.process_closed_bar_group(
            session_id=ctx.session_id,
            session=ctx.session,
            bar_candles=bar_candles,
            runtime_state=ctx.runtime_state,
            tracker=ctx.tracker,
            params=ctx.params,
            active_symbols=(
                _entry_disabled_symbols(tracker=ctx.tracker, active_symbols=ctx.active_symbols)
                if ctx.entries_disabled
                else ctx.active_symbols
            ),
            strategy=ctx.strategy,
            direction_filter=ctx.direction_filter,
            stage_b_applied=ctx.stage_b_applied,
            symbol_last_prices=ctx.symbol_last_prices,
            last_price=ctx.last_price,
            feed_source=feed_source,
            transport=transport,
            feed_audit_writer=record_closed_candles,
            signal_audit_writer=record_signal_decisions,
            evaluate_candle_fn=evaluate_candle,
            execute_entry_fn=execute_entry,
            enforce_risk_controls=enforce_session_risk_controls,
            build_feed_state=build_summary_feed_state,
            real_order_router=ctx.real_order_router,
            update_symbols_cb=lambda symbols: ticker_adapter.update_symbols(
                ctx.session_id, symbols
            ),
        )
    finally:
        get_paper_db().flush_deferred_sync()
    elapsed_ms = (datetime.now(IST) - started).total_seconds() * 1000.0
    logger.info(
        "LIVE_MULTI_BAR_PROCESS_DONE session=%s bar_end=%s elapsed_ms=%.3f active_symbols=%d",
        ctx.session_id,
        bar_end.isoformat(),
        elapsed_ms,
        len(driver_result["active_symbols"]),
    )
    ctx.active_symbols = list(driver_result["active_symbols"])
    if not ctx.entries_disabled:
        ctx.entry_resume_symbols = list(ctx.active_symbols)
    ctx.last_price = driver_result["last_price"]
    ctx.stage_b_applied = bool(driver_result["stage_b_applied"])
    if driver_result["should_complete"]:
        ctx.final_status = "NO_TRADES_ENTRY_WINDOW_CLOSED"
        ctx.terminal_reason = driver_result.get("stop_reason") or "entry_window_closed"
    elif not ctx.active_symbols and not ctx.entries_disabled:
        ctx.final_status = "NO_ACTIVE_SYMBOLS"
        ctx.terminal_reason = "no_active_symbols"
    elif driver_result["triggered"]:
        ctx.final_status = "STOPPING"
        ctx.terminal_reason = "risk_control_triggered"

    connected = bool(ticker_adapter is not None and getattr(ticker_adapter, "is_connected", True))
    await _write_feed_state(
        None,
        session_id=ctx.session_id,
        status="OK" if connected else "STALE",
        last_event_ts=getattr(ticker_adapter, "last_tick_ts", None),
        last_bar_ts=ctx.last_bar_ts,
        last_price=ctx.last_price,
        stale_reason=None if connected else "ticker_not_connected",
        raw_state={
            "connected": connected,
            "active_symbols": len(ctx.active_symbols),
            "closed_bars": ctx.closed_bars,
        },
    )


async def _apply_live_multi_operator_controls(
    *,
    ctx: _LiveMultiContext,
    ticker_adapter: Any,
    use_websocket: bool,
    now: datetime,
) -> bool:
    """Apply per-session operator controls for the multi-live dispatcher."""
    stop_requested = False

    signal_file = Path(".tmp_logs") / f"flatten_{ctx.session_id}.signal"
    if signal_file.exists():
        logger.info(
            "[%s] Multi flatten signal detected — closing all positions and completing session",
            ctx.session_id,
        )
        try:
            signal_file.unlink()
        except OSError:
            pass
        live_feed_state = _live_mark_feed_state(
            session_id=ctx.session_id,
            symbol_last_prices=ctx.symbol_last_prices,
            ticker_adapter=ticker_adapter if use_websocket else None,
            symbols=list(ctx.tracker._open.keys()) or ctx.active_symbols,
        )
        await flatten_session_positions(
            ctx.session_id,
            notes="manual_flatten_signal",
            feed_state=live_feed_state,
            real_order_router=ctx.real_order_router,
        )
        if _reconcile_live_session(
            session_id=ctx.session_id,
            reason="manual_flatten_signal",
            alerts_enabled=True,
        ):
            ctx.final_status = "FAILED"
            ctx.terminal_reason = "reconciliation_failed_after_manual_flatten"
        else:
            ctx.final_status = "COMPLETED"
            ctx.terminal_reason = "manual_flatten_signal"
        return True

    cmd_dir = Path(".tmp_logs") / f"cmd_{ctx.session_id}"
    if not cmd_dir.exists():
        return False

    command_files = sorted(cmd_dir.glob("*.json"), key=_admin_command_sort_key)
    for cmd_file in command_files:
        if not cmd_file.exists():
            continue
        if _is_admin_command_stale(cmd_file, now):
            logger.warning("[%s] Stale admin command dropped: %s", ctx.session_id, cmd_file.name)
            try:
                cmd_file.unlink()
            except OSError:
                logger.debug(
                    "[%s] Failed to delete stale admin command %s",
                    ctx.session_id,
                    cmd_file,
                    exc_info=True,
                )
            continue

        command_processed = False
        try:
            cmd = json.loads(cmd_file.read_text())
            action = cmd.get("action", "")
            reason = cmd.get("reason", "admin_command")
            requester = cmd.get("requester", "unknown")
            logger.info(
                "[%s] Multi admin command: action=%s symbols=%s requester=%s",
                ctx.session_id,
                action,
                cmd.get("symbols"),
                requester,
            )

            if action == "close_all":
                live_feed_state = _live_mark_feed_state(
                    session_id=ctx.session_id,
                    symbol_last_prices=ctx.symbol_last_prices,
                    ticker_adapter=ticker_adapter if use_websocket else None,
                    symbols=list(ctx.tracker._open.keys()) or ctx.active_symbols,
                )
                await flatten_session_positions(
                    ctx.session_id,
                    notes=f"admin_{reason}_{requester}",
                    feed_state=live_feed_state,
                    real_order_router=ctx.real_order_router,
                )
                if _reconcile_live_session(
                    session_id=ctx.session_id,
                    reason=f"admin:{action}",
                    alerts_enabled=True,
                ):
                    ctx.entries_disabled = True
                    ctx.final_status = "FAILED"
                    ctx.terminal_reason = "reconciliation_failed_after_admin_close_all"
                else:
                    ctx.final_status = "COMPLETED"
                    ctx.terminal_reason = f"admin_{reason}"
                stop_requested = True

            elif action == "close_positions":
                symbols = [str(symbol).upper() for symbol in (cmd.get("symbols") or [])]
                if symbols:
                    live_feed_state = _live_mark_feed_state(
                        session_id=ctx.session_id,
                        symbol_last_prices=ctx.symbol_last_prices,
                        ticker_adapter=ticker_adapter if use_websocket else None,
                        symbols=symbols,
                    )
                    close_result = await flatten_positions_subset(
                        ctx.session_id,
                        symbols,
                        notes=f"admin_{reason}_{requester}",
                        feed_state=live_feed_state,
                        real_order_router=ctx.real_order_router,
                    )
                    for position in close_result.get("positions", []):
                        symbol = str(position.get("symbol", ""))
                        if symbol and ctx.tracker.has_open_position(symbol):
                            position_obj = ctx.tracker.get_open_position(symbol)
                            close_price = float(position.get("close_price", 0))
                            quantity = float(
                                getattr(position_obj, "current_qty", None)
                                or getattr(position_obj, "quantity", 0)
                                or 0
                            )
                            direction = str(getattr(position_obj, "direction", "LONG")).upper()
                            entry_price = float(getattr(position_obj, "entry_price", 0) or 0)
                            exit_value = (
                                quantity * close_price
                                if direction == "LONG"
                                else quantity * (2 * entry_price - close_price)
                            )
                            ctx.tracker.record_close(symbol, exit_value)
                    get_paper_db().force_sync()
                    if _reconcile_live_session(
                        session_id=ctx.session_id,
                        reason=f"admin:{action}",
                        alerts_enabled=True,
                    ):
                        ctx.entries_disabled = True
                        monitor_symbols = _entry_disabled_symbols(
                            tracker=ctx.tracker,
                            active_symbols=ctx.active_symbols,
                        )
                        if monitor_symbols:
                            ctx.active_symbols = monitor_symbols
                            if use_websocket and ticker_adapter is not None:
                                ticker_adapter.update_symbols(ctx.session_id, ctx.active_symbols)

            elif action == "set_risk_budget":
                portfolio_value = cmd.get("portfolio_value")
                max_positions = cmd.get("max_positions")
                max_position_pct = cmd.get("max_position_pct")
                ctx.tracker.update_budget(
                    portfolio_value=(
                        float(portfolio_value) if portfolio_value is not None else None
                    ),
                    max_positions=(int(max_positions) if max_positions is not None else None),
                    max_position_pct=(
                        float(max_position_pct) if max_position_pct is not None else None
                    ),
                )
                ctx.entries_disabled = False
                open_notional = ctx.tracker.current_open_notional()
                if ctx.tracker.initial_capital > 0 and open_notional >= ctx.tracker.initial_capital:
                    ctx.entries_disabled = True
                    monitor_symbols = _entry_disabled_symbols(
                        tracker=ctx.tracker,
                        active_symbols=ctx.active_symbols,
                    )
                    if monitor_symbols:
                        ctx.active_symbols = monitor_symbols
                        if use_websocket and ticker_adapter is not None:
                            ticker_adapter.update_symbols(ctx.session_id, ctx.active_symbols)
                logger.warning(
                    "[%s] Multi risk budget updated by %s: portfolio_value=%.2f "
                    "max_positions=%d max_position_pct=%.4f open_notional=%.2f "
                    "cash_available=%.2f entries_disabled=%s",
                    ctx.session_id,
                    requester,
                    ctx.tracker.initial_capital,
                    ctx.tracker.max_positions,
                    ctx.tracker.max_position_pct,
                    open_notional,
                    ctx.tracker.cash_available,
                    ctx.entries_disabled,
                )
                try:
                    get_paper_db().update_session(
                        ctx.session_id,
                        notes=(
                            f"RISK_BUDGET_UPDATED portfolio_value="
                            f"{ctx.tracker.initial_capital:.2f} max_positions="
                            f"{ctx.tracker.max_positions} max_position_pct="
                            f"{ctx.tracker.max_position_pct:.4f} reason={reason}"
                        ),
                    )
                    get_paper_db().force_sync()
                except Exception:
                    logger.debug(
                        "[%s] Failed to stamp multi risk budget update",
                        ctx.session_id,
                        exc_info=True,
                    )

            elif action == "pause_entries":
                ctx.entries_disabled = True
                monitor_symbols = _entry_disabled_symbols(
                    tracker=ctx.tracker,
                    active_symbols=ctx.active_symbols,
                )
                if monitor_symbols:
                    ctx.active_symbols = monitor_symbols
                    if use_websocket and ticker_adapter is not None:
                        ticker_adapter.update_symbols(ctx.session_id, ctx.active_symbols)
                logger.warning(
                    "[%s] Multi entries paused by %s reason=%s open_positions=%d",
                    ctx.session_id,
                    requester,
                    reason,
                    ctx.tracker.open_count,
                )
                try:
                    get_paper_db().update_session(
                        ctx.session_id,
                        notes=f"ENTRIES_PAUSED reason={reason} requester={requester}",
                    )
                    get_paper_db().force_sync()
                except Exception:
                    logger.debug(
                        "[%s] Failed to stamp multi pause note",
                        ctx.session_id,
                        exc_info=True,
                    )

            elif action == "resume_entries":
                ctx.entries_disabled = False
                ctx.active_symbols = list(ctx.entry_resume_symbols or ctx.active_symbols)
                if use_websocket and ticker_adapter is not None:
                    ticker_adapter.update_symbols(ctx.session_id, ctx.active_symbols)
                logger.warning(
                    "[%s] Multi entries resumed by %s reason=%s symbols=%d original_universe=%d",
                    ctx.session_id,
                    requester,
                    reason,
                    len(ctx.active_symbols),
                    len(ctx.entry_universe_symbols or []),
                )
                try:
                    get_paper_db().update_session(
                        ctx.session_id,
                        notes=f"ENTRIES_RESUMED reason={reason} requester={requester}",
                    )
                    get_paper_db().force_sync()
                except Exception:
                    logger.debug(
                        "[%s] Failed to stamp multi resume note",
                        ctx.session_id,
                        exc_info=True,
                    )

            elif action == "cancel_pending_intents":
                cancelled = _cancel_pending_admin_commands(cmd_dir, cmd_file)
                logger.warning(
                    "[%s] Multi pending admin intents cancelled by %s reason=%s count=%d",
                    ctx.session_id,
                    requester,
                    reason,
                    cancelled,
                )
                try:
                    get_paper_db().update_session(
                        ctx.session_id,
                        notes=(
                            f"PENDING_INTENTS_CANCELLED count={cancelled} "
                            f"reason={reason} requester={requester}"
                        ),
                    )
                    get_paper_db().force_sync()
                except Exception:
                    logger.debug(
                        "[%s] Failed to stamp multi cancel-pending note",
                        ctx.session_id,
                        exc_info=True,
                    )
                stop_requested = False

            else:
                logger.warning(
                    "[%s] Unknown multi admin command action %r dropped: %s",
                    ctx.session_id,
                    action,
                    cmd_file.name,
                )

            command_processed = True
        except Exception:
            logger.exception("[%s] Multi admin command failed: %s", ctx.session_id, cmd_file.name)
        finally:
            if command_processed:
                try:
                    cmd_file.unlink()
                except OSError:
                    pass
        if command_processed and action == "cancel_pending_intents":
            break
        if stop_requested:
            break

    return stop_requested


async def _finalize_live_session(
    *,
    session_id: str,
    complete_on_exit: bool,
    last_bar_ts: datetime | None,
    stale_timeout: int,
    notes: str | None,
    deps: LiveSessionDeps | None = None,
) -> None:
    if complete_on_exit:
        await _update_session(
            session_id,
            deps,
            status="COMPLETED",
            latest_candle_ts=last_bar_ts,
            clear_stale_feed_at=True,
            notes=notes,
        )
        return
    await _update_session(
        session_id,
        deps,
        latest_candle_ts=last_bar_ts,
        stale_feed_at=(
            last_bar_ts + timedelta(seconds=stale_timeout)
            if last_bar_ts is not None and stale_timeout > 0
            else None
        ),
        notes=notes,
    )


async def _finalize_live_multi_context(
    *,
    ctx: _LiveMultiContext,
    ticker_adapter: Any,
    complete_on_exit: bool,
) -> dict[str, Any]:
    if ctx.final_status in {"COMPLETED", "NO_ACTIVE_SYMBOLS", "NO_TRADES_ENTRY_WINDOW_CLOSED"}:
        try:
            await flatten_session_positions(
                ctx.session_id,
                notes=ctx.notes or "multi session flatten",
                feed_state=_live_mark_feed_state(
                    session_id=ctx.session_id,
                    symbol_last_prices=ctx.symbol_last_prices,
                    ticker_adapter=ticker_adapter,
                    symbols=list(ctx.tracker._open.keys()) or ctx.active_symbols,
                ),
                real_order_router=ctx.real_order_router,
            )
        except Exception:
            logger.warning("[%s] Multi final flatten failed", ctx.session_id, exc_info=True)
            ctx.final_status = "FAILED"
            ctx.terminal_reason = "multi_final_flatten_failed"
    stop_is_terminal = complete_on_exit or ctx.final_status in {
        "NO_ACTIVE_SYMBOLS",
        "NO_TRADES_ENTRY_WINDOW_CLOSED",
        "COMPLETED",
    }
    try:
        await paper_session_driver.complete_session(
            session_id=ctx.session_id,
            complete_on_exit=stop_is_terminal,
            last_bar_ts=ctx.last_bar_ts,
            stale_timeout=0,
            notes=ctx.notes,
            update_session_state=lambda sid, **kwargs: _update_session(sid, None, **kwargs),
        )
    except Exception:
        logger.exception("[%s] Multi final session completion failed", ctx.session_id)
        ctx.final_status = "FAILED"
        ctx.terminal_reason = "session_finalize_failed"
    if ctx.final_status == "FAILED":
        try:
            await _update_session(
                ctx.session_id,
                None,
                status="FAILED",
                latest_candle_ts=ctx.last_bar_ts,
                clear_stale_feed_at=True,
                notes=ctx.terminal_reason or ctx.notes or "multi_session_failed",
            )
            get_paper_db().force_sync()
        except Exception:
            logger.exception("[%s] Multi failed-status stamp failed", ctx.session_id)
    final_session = await _load_session(ctx.session_id)
    if final_session is not None and getattr(final_session, "status", None):
        loaded_status = str(final_session.status)
        if loaded_status.upper() != "ACTIVE":
            ctx.final_status = loaded_status
            ctx.terminal_reason = ctx.terminal_reason or f"db_status:{loaded_status.lower()}"
    archive_payload = None
    if final_session and final_session.status in ("COMPLETED", "FAILED"):
        try:
            archive_payload = archive_completed_session(ctx.session_id, paper_db=get_paper_db())
        except Exception:
            logger.exception("[%s] Multi paper session archive failed", ctx.session_id)
            archive_payload = {
                "session_id": ctx.session_id,
                "archived": False,
                "error": "archive_failed",
            }
    if stop_is_terminal and ctx.final_status in {
        "COMPLETED",
        "NO_TRADES_ENTRY_WINDOW_CLOSED",
        "NO_ACTIVE_SYMBOLS",
    }:
        dispatch_session_completed_alert(session_id=ctx.session_id)
    feed_state = get_paper_db().get_feed_state(ctx.session_id)
    return {
        "session_id": ctx.session_id,
        "strategy": ctx.strategy,
        "symbols": ctx.active_symbols,
        "quote_events": ctx.quote_events,
        "closed_bars": ctx.closed_bars,
        "last_snapshot_ts": ctx.last_snapshot_ts.isoformat() if ctx.last_snapshot_ts else None,
        "last_bar_ts": ctx.last_bar_ts.isoformat() if ctx.last_bar_ts else None,
        "terminal_reason": ctx.terminal_reason,
        "broker_execution": "ZERODHA_LIVE" if ctx.real_order_router is not None else "PAPER_LIVE",
        "real_orders_enabled": ctx.real_order_router is not None,
        "final_status": ctx.final_status
        if ctx.final_status != "ACTIVE"
        else getattr(final_session, "status", "ACTIVE"),
        "feed_state": asdict(feed_state) if feed_state else None,
        "archive": archive_payload,
    }


async def run_live_multi_sessions(
    *,
    specs: list[LiveMultiSessionSpec],
    ticker_adapter: Any,
    poll_interval_sec: float | None = None,
    candle_interval_minutes: int | None = None,
    max_cycles: int | None = None,
    complete_on_exit: bool = False,
    allow_live_setup_fallback: bool = True,
) -> list[dict[str, Any]]:
    """Run multiple live paper sessions with one bar-major dispatcher."""
    if not specs:
        return []
    settings = get_settings()
    candle_interval = _resolve_candle_interval(settings, candle_interval_minutes)
    poll_interval = _resolve_poll_interval(settings, poll_interval_sec, candle_interval)
    local_feed = bool(getattr(ticker_adapter, "_local_feed", False))
    transport = "local" if local_feed else "websocket"
    feed_source = "local" if local_feed else "kite"
    contexts: list[_LiveMultiContext] = []
    lock_stack = ExitStack()
    shared_or_cache: dict[str, list[dict[str, Any]]] | None = {} if not local_feed else None
    for spec in specs:
        lock_stack.enter_context(
            acquire_command_lock(
                _session_lock_name(spec.session_id),
                detail=f"paper live session {spec.session_id}",
            )
        )
    register_session_start()
    _start_alert_dispatcher()
    try:
        for spec in specs:
            contexts.append(
                await _prepare_live_multi_context(
                    spec=spec,
                    ticker_adapter=ticker_adapter,
                    candle_interval=candle_interval,
                    allow_live_setup_fallback=allow_live_setup_fallback,
                    shared_or_cache=shared_or_cache,
                )
            )
        last_ticker_tick_count = ticker_adapter.tick_count if ticker_adapter is not None else 0
        last_bucket_start = _floor_bucket_start(datetime.now(IST), candle_interval)
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            now = datetime.now(IST)
            active_contexts = [ctx for ctx in contexts if ctx.final_status == "ACTIVE"]
            if not active_contexts:
                break
            if _should_use_global_flatten_signal():
                logger.info("LIVE_MULTI_GLOBAL_FLATTEN sessions=%d", len(active_contexts))
                for ctx in active_contexts:
                    live_feed_state = _live_mark_feed_state(
                        session_id=ctx.session_id,
                        symbol_last_prices=ctx.symbol_last_prices,
                        ticker_adapter=ticker_adapter if not local_feed else None,
                        symbols=list(ctx.tracker._open.keys()) or ctx.active_symbols,
                    )
                    await flatten_session_positions(
                        ctx.session_id,
                        notes="global_flatten_signal",
                        feed_state=live_feed_state,
                        real_order_router=ctx.real_order_router,
                    )
                    if _reconcile_live_session(
                        session_id=ctx.session_id,
                        reason="global_flatten_signal",
                        alerts_enabled=True,
                    ):
                        ctx.final_status = "FAILED"
                        ctx.terminal_reason = "reconciliation_failed_after_global_flatten"
                    else:
                        ctx.final_status = "COMPLETED"
                        ctx.terminal_reason = "global_flatten_signal"
                try:
                    _GLOBAL_FLATTEN_SIGNAL.unlink(missing_ok=True)
                except OSError:
                    logger.debug(
                        "Failed to delete global flatten signal %s",
                        _GLOBAL_FLATTEN_SIGNAL,
                        exc_info=True,
                    )
                continue
            for ctx in active_contexts:
                await _apply_live_multi_operator_controls(
                    ctx=ctx,
                    ticker_adapter=ticker_adapter,
                    use_websocket=not local_feed,
                    now=now,
                )
            active_contexts = [ctx for ctx in contexts if ctx.final_status == "ACTIVE"]
            if not active_contexts:
                break
            current_ticks = ticker_adapter.tick_count
            tick_delta = current_ticks - last_ticker_tick_count
            if tick_delta > 0:
                for ctx in active_contexts:
                    ctx.quote_events += tick_delta
                    ctx.last_snapshot_ts = ticker_adapter.last_tick_ts
                    if not local_feed:
                        await _mark_multi_feed_ok_from_ticks(ctx, ticker_adapter)
            last_ticker_tick_count = current_ticks
            session_candles: dict[str, list[ClosedCandle]] = {}
            if local_feed:
                for ctx in active_contexts:
                    session_candles[ctx.session_id] = ticker_adapter.drain_closed(ctx.session_id)
            else:
                current_bucket_start = _floor_bucket_start(now, candle_interval)
                if current_bucket_start <= last_bucket_start:
                    await asyncio.sleep(max(0.1, poll_interval))
                    continue
                for ctx in active_contexts:
                    ticker_adapter.synthesize_quiet_symbols(ctx.session_id, ctx.active_symbols, now)
                    session_candles[ctx.session_id] = ticker_adapter.drain_closed(ctx.session_id)
                last_bucket_start = current_bucket_start
            bars_by_end: dict[datetime, list[tuple[_LiveMultiContext, list[ClosedCandle]]]] = {}
            for ctx in active_contexts:
                candles = sorted(
                    session_candles.get(ctx.session_id, []),
                    key=lambda candle: (candle.bar_end, candle.symbol),
                )
                _log_bar_heartbeats(
                    session_id=ctx.session_id,
                    active_symbols=ctx.active_symbols,
                    cycle_closed=candles,
                )
                if candles:
                    _log_ticker_health(
                        session_id=ctx.session_id,
                        ticker_adapter=ticker_adapter,
                        active_symbols=ctx.active_symbols,
                    )
                grouped: dict[datetime, list[ClosedCandle]] = {}
                for candle in candles:
                    grouped.setdefault(candle.bar_end, []).append(candle)
                for bar_end, bar_candles in grouped.items():
                    bars_by_end.setdefault(bar_end, []).append((ctx, bar_candles))
            if not bars_by_end:
                if local_feed and all(
                    getattr(ticker_adapter, "_session_exhausted", {}).get(ctx.session_id, False)
                    for ctx in active_contexts
                ):
                    for ctx in active_contexts:
                        ctx.final_status = "COMPLETED"
                        ctx.terminal_reason = "local_feed_exhausted"
                    break
                await asyncio.sleep(max(0.1, poll_interval))
                continue
            for bar_end in sorted(bars_by_end):
                logger.info(
                    "LIVE_MULTI_BAR_DISPATCH bar_end=%s sessions=%d",
                    bar_end.isoformat(),
                    len(bars_by_end[bar_end]),
                )
                for ctx, bar_candles in sorted(
                    bars_by_end[bar_end], key=lambda item: item[0].session_id
                ):
                    if ctx.final_status != "ACTIVE":
                        continue
                    try:
                        await _process_live_multi_bar(
                            ctx=ctx,
                            bar_end=bar_end,
                            bar_candles=bar_candles,
                            ticker_adapter=ticker_adapter,
                            feed_source=feed_source,
                            transport=transport,
                        )
                    except Exception:
                        logger.exception(
                            "[%s] Multi bar group processing failed bar_end=%s",
                            ctx.session_id,
                            bar_end.isoformat(),
                        )
                        ctx.final_status = "FAILED"
                        ctx.terminal_reason = "bar_processing_error"
                        dispatch_session_error_alert(
                            session_id=ctx.session_id,
                            reason="bar_processing_error",
                            details=f"bar_end={bar_end.isoformat()} symbols={len(bar_candles)}",
                        )
            await asyncio.sleep(0)
    finally:
        try:
            results = []
            for ctx in contexts:
                try:
                    ticker_adapter.unregister_session(ctx.session_id)
                except Exception:
                    logger.debug("[%s] Multi unregister failed", ctx.session_id, exc_info=True)
                results.append(
                    await _finalize_live_multi_context(
                        ctx=ctx,
                        ticker_adapter=ticker_adapter,
                        complete_on_exit=complete_on_exit,
                    )
                )
            await maybe_shutdown_alert_dispatcher()
        finally:
            lock_stack.close()
    return results


async def run_live_session(
    *,
    session_id: str,
    symbols: list[str] | None = None,
    adapter: MarketDataAdapter | None = None,
    ticker_adapter: Any = None,
    poll_interval_sec: float | None = None,
    candle_interval_minutes: int | None = None,
    max_cycles: int | None = None,
    complete_on_exit: bool = False,
    auto_flatten_on_abnormal_exit: bool = True,
    allow_late_start_fallback: bool = False,
    allow_live_setup_fallback: bool = True,
    real_order_config: dict[str, Any] | None = None,
    notes: str | None = None,
    deps: LiveSessionDeps | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    candle_interval = _resolve_candle_interval(settings, candle_interval_minutes)

    # Install a custom async exception handler so fatal errors (OOM, segfault in C
    # extension) get logged before the process dies. This does NOT prevent crashes
    # but ensures the root cause appears in the log instead of silent death.
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(
        lambda _loop, context: logger.critical(
            "ASYNC_FATAL_EXCEPTION session=%s message=%s exception=%s",
            session_id,
            context.get("message", "unknown"),
            context.get("exception", "none"),
        )
    )

    session = await _load_session(session_id, deps)
    if session is None:
        return {"session_id": session_id, "error": "session not found"}
    session_status = str(getattr(session, "status", "") or "").upper()
    if session_status in {"COMPLETED", "CANCELLED", "STOPPING"}:
        logger.info(
            "[%s] Session already terminal at startup status=%s — no live loop started",
            session_id,
            session_status,
        )
        return {
            "session_id": session_id,
            "final_status": session_status,
            "terminal_reason": f"startup_db_status:{session_status.lower()}",
            "cycles": 0,
        }

    trade_date = _resolve_trade_date(session)
    strategy = str(getattr(session, "strategy", "") or "CPR_LEVELS")
    strategy_params = dict(getattr(session, "strategy_params", {}) or {})
    initial_symbols = _resolve_active_symbols(session, symbols)
    if not initial_symbols:
        return {"session_id": session_id, "error": "no symbols available for live replay"}

    try:
        if deps is None:
            active_symbols = pre_filter_symbols_for_strategy(
                trade_date,
                initial_symbols,
                strategy,
                strategy_params,
                require_trade_date_rows=True,
            )
        else:
            active_symbols = list(initial_symbols)
    except RuntimeError as exc:
        return {"session_id": session_id, "error": str(exc)}
    if not active_symbols:
        return {
            "session_id": session_id,
            "error": f"no symbols remain after Stage A pre-filter for {trade_date}",
        }
    entry_universe_symbols = list(active_symbols)

    register_session_start()
    _start_alert_dispatcher()  # start consumer eagerly so first alerts are not delayed
    _was_already_active = session_status not in {"PLANNING", ""}
    if session_status not in {"ACTIVE", "PAUSED"}:
        session = await _update_session(session_id, deps, status="ACTIVE", notes=notes)
        force_paper_db_sync(get_paper_db())

    params_session = session
    if not hasattr(session, "strategy_params"):
        params_session = SimpleNamespace(
            strategy=strategy,
            strategy_params=strategy_params,
        )
    params = build_backtest_params(params_session)
    direction_filter = str(getattr(params, "direction_filter", "BOTH") or "BOTH").upper()
    real_order_router = build_real_order_router(real_order_config)
    if real_order_router is not None:
        logger.warning(
            "[%s] ZERODHA_LIVE real-order routing enabled: fixed_qty=%d entry_order_type=%s",
            session_id,
            real_order_router.config.fixed_quantity,
            real_order_router.config.entry_order_type,
        )
        scale_out_pct = float(
            getattr(getattr(params, "cpr_levels", None), "scale_out_pct", 0.0) or 0.0
        )
        if scale_out_pct > 0:
            reason = "real_order_partial_scale_out_unsupported"
            logger.error(
                "[%s] Startup blocked: real-order routing does not support CPR partial "
                "scale-out exits scale_out_pct=%.4f",
                session_id,
                scale_out_pct,
            )
            await _update_session(session_id, deps, status="FAILED", notes=reason)
            force_paper_db_sync(get_paper_db())
            return {
                "session_id": session_id,
                "final_status": "FAILED",
                "terminal_reason": reason,
                "cycles": 0,
            }

    runtime_state = PaperRuntimeState(
        allow_live_setup_fallback=allow_live_setup_fallback,
        allow_or_proxy_setup=_allow_or_proxy_setup_for_adapter(ticker_adapter),
        bar_end_offset=timedelta(minutes=5) if _is_local_feed_adapter(ticker_adapter) else None,
    )
    if deps is None:
        _prefetch_setup_rows(
            runtime_state=runtime_state,
            symbols=active_symbols,
            trade_date=trade_date,
            candle_interval_minutes=candle_interval,
            regime_index_symbol=str(strategy_params.get("regime_index_symbol") or ""),
            regime_snapshot_minutes=int(strategy_params.get("regime_snapshot_minutes") or 30),
        )
        _catch_up_kite_true_or_if_needed(
            runtime_state=runtime_state,
            active_symbols=active_symbols,
            trade_date=trade_date,
            candle_interval=candle_interval,
            session_id=session_id,
            ticker_adapter=ticker_adapter,
            strategy_params=strategy_params,
            shared_or_cache=None,
        )
    direction_readiness = _log_direction_readiness(
        session_id=session_id,
        runtime_state=runtime_state,
        active_symbols=active_symbols,
    )
    logger.info(
        "LIVE_STARTUP_READY session=%s resolved=%d pending=%d missing=%d with_setup=%d "
        "coverage=%.0f%% symbols=%d",
        session_id,
        int(direction_readiness["resolved"]),
        int(direction_readiness["pending"]),
        int(direction_readiness["missing"]),
        int(direction_readiness["with_setup"]),
        float(direction_readiness["coverage_pct"]),
        len(active_symbols),
    )
    if deps is None and active_symbols and int(direction_readiness["with_setup"]) == 0:
        reason = "startup_missing_trade_date_setup_rows"
        logger.error(
            "[STARTUP BLOCKED] %s: no market_day_state setup rows loaded for %d requested "
            "active symbol(s) on %s. Refusing live fallback from an empty setup universe.",
            session_id,
            len(active_symbols),
            trade_date,
        )
        await _update_session(session_id, deps, status="FAILED", notes=reason)
        force_paper_db_sync(get_paper_db())
        return {
            "session_id": session_id,
            "final_status": "FAILED",
            "terminal_reason": reason,
            "cycles": 0,
        }
    tracker = SessionPositionTracker(
        max_positions=int(getattr(session, "max_positions", 1) or 1),
        portfolio_value=float(getattr(params, "portfolio_value", 0.0) or 0.0),
        max_position_pct=float(getattr(params, "max_position_pct", 0.0) or 0.0),
    )
    if deps is None:
        tracker.seed_open_positions(await get_session_positions(session_id, statuses=["OPEN"]))
        # R2: on session resume, seed closed/flattened symbols so they cannot re-enter today.
        # Two guards exist: tracker._closed_today (bar_orchestrator) and
        # state.position_closed_today (paper_runtime). Both start empty; both need seeding.
        for _closed_pos in await get_session_positions(
            session_id, statuses=["CLOSED", "FLATTENED"]
        ):
            tracker.mark_traded(_closed_pos.symbol)
            runtime_state.for_symbol(_closed_pos.symbol).position_closed_today = True

    use_websocket = ticker_adapter is not None or adapter is None
    local_ticker_created = False
    market_adapter = adapter
    if use_websocket:
        if ticker_adapter is None:
            ticker_adapter = KiteTickerAdapter()
            local_ticker_created = True
        builder = FiveMinuteCandleBuilder(interval_minutes=candle_interval)
        ticker_adapter.register_session(session_id, active_symbols, builder)
    else:
        market_adapter = market_adapter or KiteQuoteAdapter()
        builder = FiveMinuteCandleBuilder(interval_minutes=candle_interval)

    await _write_feed_state(
        deps,
        session_id=session_id,
        status="CONNECTING",
        last_event_ts=None,
        last_bar_ts=None,
        last_price=None,
        stale_reason=None,
        raw_state={"mode": "startup", "symbols": len(active_symbols)},
    )

    if not _was_already_active:
        dispatch_session_started_alert(
            session_id=session_id,
            strategy=strategy,
            direction=direction_filter,
            symbol_count=len(active_symbols),
            trade_date=trade_date,
        )
    else:
        logger.info(
            "[%s] Session already started status=%s — skipping duplicate SESSION_STARTED alert",
            session_id,
            session_status,
        )

    stale_timeout = max(0, int(getattr(session, "stale_feed_timeout_sec", 0) or 0))
    poll_interval = _resolve_poll_interval(settings, poll_interval_sec, candle_interval)
    supervision_sleep = 1.0 if use_websocket else max(0.1, poll_interval)
    symbol_last_prices: dict[str, float] = {}

    quote_events = 0
    closed_bars = 0
    cycles = 0
    final_status = "ACTIVE"
    terminal_reason: str | None = None
    last_snapshot_ts: datetime | None = None
    last_bar_ts: datetime | None = None
    last_bucket_start = _floor_bucket_start(_session_now(deps), candle_interval)
    no_snapshot_streak = 0
    stage_b_applied = False
    last_ticker_tick_count = ticker_adapter.tick_count if ticker_adapter is not None else 0
    reconnect_alerted = False
    stale_alerted = False
    last_disconnect_alert_ts: datetime | None = None
    last_stale_alert_ts: datetime | None = None
    entries_disabled = False
    entry_resume_symbols = list(active_symbols)
    alerts_enabled = True if deps is None else bool(deps.alerts_enabled)
    last_feed_audit_cleanup: datetime | None = None
    audit_feed_source = "kite"
    audit_transport = "websocket" if use_websocket else "rest"

    session_lock_ctx = acquire_command_lock(
        _session_lock_name(session_id),
        detail=f"paper live session {session_id}",
    )
    session_lock_ctx.__enter__()
    try:
        print(
            f"[live] {session_id} started - strategy={strategy} symbols={len(active_symbols)}"
            f" transport={'websocket' if use_websocket else 'rest'}",
            flush=True,
        )
        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            now = _session_now(deps)
            last_feed_audit_cleanup, _ = _cleanup_feed_audit_if_needed(
                now=now,
                last_cleanup=last_feed_audit_cleanup,
                settings=settings,
            )
            current_session = await _load_session(session_id, deps)
            if current_session is None:
                final_status = "MISSING"
                terminal_reason = "session_missing"
                break
            if _should_use_global_flatten_signal():
                logger.info(
                    "[%s] Global flatten signal detected — closing all positions and completing session",
                    session_id,
                )
                live_feed_state = _live_mark_feed_state(
                    session_id=session_id,
                    symbol_last_prices=symbol_last_prices,
                    ticker_adapter=ticker_adapter if use_websocket else None,
                    symbols=list(tracker._open.keys()) or active_symbols,
                )
                await flatten_session_positions(
                    session_id,
                    notes="global_flatten_signal",
                    feed_state=live_feed_state,
                    real_order_router=real_order_router,
                )
                if _reconcile_live_session(
                    session_id=session_id,
                    reason="global_flatten_signal",
                    alerts_enabled=alerts_enabled,
                ):
                    final_status = "FAILED"
                    terminal_reason = "reconciliation_failed_after_global_flatten"
                    complete_on_exit = False
                else:
                    final_status = "COMPLETED"
                    terminal_reason = "global_flatten_signal"
                    complete_on_exit = True
                try:
                    _GLOBAL_FLATTEN_SIGNAL.unlink(missing_ok=True)
                except OSError:
                    logger.debug(
                        "[%s] Failed to delete global flatten signal %s",
                        session_id,
                        _GLOBAL_FLATTEN_SIGNAL,
                        exc_info=True,
                    )
                stop_requested = True
                break
            if current_session.status == "PAUSED":
                await _write_feed_state(
                    deps,
                    session_id=session_id,
                    status="PAUSED",
                    last_event_ts=last_snapshot_ts,
                    last_bar_ts=last_bar_ts,
                    last_price=None,
                    stale_reason=None,
                    raw_state={
                        "mode": "paused",
                        "symbols": active_symbols,
                        "direction_readiness": direction_readiness,
                        "setup_prefetch": {
                            "skipped": runtime_state.skipped_setup_rows,
                            "invalid": runtime_state.invalid_setup_rows,
                        },
                    },
                )
                await _sleep(deps, supervision_sleep)
                continue
            if current_session.status in {"STOPPING", "COMPLETED", "CANCELLED", "FAILED"}:
                final_status = str(current_session.status)
                terminal_reason = f"db_status:{str(current_session.status).lower()}"
                break

            cycle_closed: list[ClosedCandle] = []
            latest_raw_state: dict[str, Any] | None = None
            last_price: float | None = None
            local_feed = bool(
                use_websocket
                and ticker_adapter is not None
                and getattr(ticker_adapter, "_local_feed", False)
            )
            local_feed_exhausted = False
            audit_feed_source = "local" if local_feed else "kite"
            audit_transport = "local" if local_feed else ("websocket" if use_websocket else "rest")

            if use_websocket and ticker_adapter is not None:
                current_ticks = ticker_adapter.tick_count
                tick_delta = current_ticks - last_ticker_tick_count
                if tick_delta > 0:
                    quote_events += tick_delta
                    no_snapshot_streak = 0
                elif tick_delta == 0:
                    no_snapshot_streak += 1
                else:
                    # A reconnect may reset the adapter tick counter.
                    no_snapshot_streak = 0
                last_ticker_tick_count = current_ticks
                if ticker_adapter.last_tick_ts is not None:
                    last_snapshot_ts = ticker_adapter.last_tick_ts
                current_bucket_start = _floor_bucket_start(now, candle_interval)
                if local_feed:
                    # Local feed: drain every cycle (adapter drives bar progression)
                    cycle_closed = ticker_adapter.drain_closed(session_id)
                    local_feed_exhausted = bool(getattr(ticker_adapter, "_exhausted", False))
                elif current_bucket_start > last_bucket_start:
                    ticker_adapter.synthesize_quiet_symbols(session_id, active_symbols, now)
                    cycle_closed = ticker_adapter.drain_closed(session_id)
                    last_bucket_start = current_bucket_start
                if (
                    ticker_adapter.reconnect_count >= _WEBSOCKET_RECONNECT_ALERT_ATTEMPTS
                    and not reconnect_alerted
                ):
                    reconnect_alerted = True
                    logger.error(
                        "WebSocket reconnect stalled session=%s reconnect_attempts=%d",
                        session_id,
                        ticker_adapter.reconnect_count,
                    )
                    if alerts_enabled:
                        dispatch_session_error_alert(
                            session_id=session_id,
                            reason="websocket_reconnect_stalled",
                            details=(
                                f"reconnect_attempts={ticker_adapter.reconnect_count}"
                                f" threshold={_WEBSOCKET_RECONNECT_ALERT_ATTEMPTS}"
                                f" symbols={len(active_symbols)}"
                            ),
                        )

                # If the socket stays down long enough, recreate the client rather
                # than waiting forever on Kite's internal reconnect loop.
                if not ticker_adapter.is_connected and hasattr(
                    ticker_adapter, "recover_connection"
                ):
                    recovery: dict[str, Any] = {}
                    try:
                        recovery = await asyncio.to_thread(
                            ticker_adapter.recover_connection,
                            now=now,
                            reconnect_after_sec=_WEBSOCKET_RECOVERY_AFTER_SEC,
                            cooldown_sec=_WEBSOCKET_RECOVERY_COOLDOWN_SEC,
                        )
                    except Exception:
                        logger.exception(
                            "WebSocket recovery watchdog failed session=%s", session_id
                        )
                        recovery = {"action": "failed", "reason": "watchdog_exception"}

                    recovery_action = str(recovery.get("action") or "noop")
                    if recovery_action == "recovered":
                        logger.warning(
                            "WebSocket recovered via watchdog session=%s down_sec=%.0f reconnects=%d",
                            session_id,
                            float(recovery.get("down_sec") or 0.0),
                            int(recovery.get("reconnect_count") or 0),
                        )
                        if alerts_enabled:
                            down_sec = float(recovery.get("down_sec") or 0.0)
                            dispatch_feed_recovered_alert(
                                session_id=session_id,
                                stale_minutes=max(1, int(down_sec / 60)) if down_sec > 0 else None,
                                open_count=len(tracker._open),
                            )
                    elif recovery_action == "failed":
                        _failed_cooldown_ok = (
                            last_disconnect_alert_ts is None
                            or (now - last_disconnect_alert_ts).total_seconds()
                            >= _FEED_STALE_ALERT_COOLDOWN_SEC
                        )
                        if alerts_enabled and _failed_cooldown_ok:
                            last_disconnect_alert_ts = now
                            logger.error(
                                "WebSocket watchdog recovery failed session=%s down_sec=%s reason=%s",
                                session_id,
                                recovery.get("down_sec"),
                                recovery.get("reason"),
                            )
                            dispatch_session_error_alert(
                                session_id=session_id,
                                reason="websocket_reconnect_failed",
                                details=(
                                    f"down_sec={float(recovery.get('down_sec') or 0.0):.0f} "
                                    f"trigger_after={_WEBSOCKET_RECOVERY_AFTER_SEC:.0f}s "
                                    f"reconnects={int(recovery.get('reconnect_count') or 0)} "
                                    f"error={recovery.get('error') or recovery.get('reason')}"
                                ),
                            )
            else:
                assert market_adapter is not None
                try:
                    snapshots = await asyncio.to_thread(market_adapter.poll, active_symbols)
                except Exception:
                    logger.exception("Market data poll failed - treating as empty cycle")
                    snapshots = []
                if snapshots:
                    no_snapshot_streak = 0
                    quote_events += len(snapshots)
                    for snapshot in snapshots:
                        last_snapshot_ts = snapshot.ts
                        symbol_last_prices[snapshot.symbol] = float(snapshot.last_price)
                        latest_raw_state = {
                            **_feed_snapshot_payload(snapshot),
                            "symbol_last_prices": dict(symbol_last_prices),
                        }
                        builder.ingest(snapshot)
                    cycle_closed = builder.drain_closed()
                else:
                    no_snapshot_streak += 1

            cycle_closed.sort(key=lambda c: (c.bar_end, c.symbol))
            _log_bar_heartbeats(
                session_id=session_id,
                active_symbols=active_symbols,
                cycle_closed=cycle_closed,
            )
            if cycle_closed:
                _log_ticker_health(
                    session_id=session_id,
                    ticker_adapter=ticker_adapter,
                    active_symbols=active_symbols,
                )
            stop_requested = False
            if cycle_closed:
                bars_by_end: dict[datetime, list[ClosedCandle]] = {}
                for candle in cycle_closed:
                    bars_by_end.setdefault(candle.bar_end, []).append(candle)

                for bar_end in sorted(bars_by_end):
                    bar_candles = sorted(bars_by_end[bar_end], key=lambda c: c.symbol)
                    for candle in bar_candles:
                        closed_bars += 1
                        last_bar_ts = candle.bar_end
                        symbol_last_prices[candle.symbol] = candle.close
                        state = runtime_state.symbols.get(candle.symbol)
                        setup_row = state.setup_row if state is not None else None
                        _log_parity_trace(session_id=session_id, candle=candle, setup_row=setup_row)
                        latest_raw_state = {
                            **_closed_candle_payload(candle),
                            "symbol_last_prices": dict(symbol_last_prices),
                            "setup_prefetch": {
                                "skipped": runtime_state.skipped_setup_rows,
                                "invalid": runtime_state.invalid_setup_rows,
                            },
                        }

                    try:
                        bar_active_symbols = (
                            _entry_disabled_symbols(
                                tracker=tracker,
                                active_symbols=active_symbols,
                            )
                            if entries_disabled
                            else active_symbols
                        )
                        # Defer replica sync for the entire bar group — sync once
                        # after all position opens/closes instead of per-write.
                        get_paper_db().defer_sync()
                        try:
                            driver_result = await paper_session_driver.process_closed_bar_group(
                                session_id=session_id,
                                session=current_session,
                                bar_candles=bar_candles,
                                runtime_state=runtime_state,
                                tracker=tracker,
                                params=params,
                                active_symbols=bar_active_symbols,
                                strategy=strategy,
                                direction_filter=direction_filter,
                                stage_b_applied=stage_b_applied,
                                symbol_last_prices=symbol_last_prices,
                                last_price=last_price,
                                feed_source=audit_feed_source,
                                transport=audit_transport,
                                feed_audit_writer=record_closed_candles,
                                signal_audit_writer=record_signal_decisions,
                                evaluate_candle_fn=evaluate_candle,
                                execute_entry_fn=execute_entry,
                                enforce_risk_controls=enforce_session_risk_controls,
                                build_feed_state=build_summary_feed_state,
                                real_order_router=real_order_router,
                                update_symbols_cb=(
                                    (
                                        lambda symbols: ticker_adapter.update_symbols(
                                            session_id, symbols
                                        )
                                    )
                                    if use_websocket and ticker_adapter is not None
                                    else None
                                ),
                            )
                        finally:
                            get_paper_db().flush_deferred_sync()
                    except Exception:
                        logger.exception(
                            "[%s] Bar group processing failed bar_end=%s — halting session",
                            session_id,
                            bar_end,
                        )
                        if alerts_enabled:
                            dispatch_session_error_alert(
                                session_id=session_id,
                                reason="bar_processing_error",
                                details=f"bar_end={bar_end.isoformat()} symbols={len(bar_candles)}",
                            )
                        final_status = "FAILED"
                        terminal_reason = "bar_processing_error"
                        stop_requested = True
                        break
                    active_symbols = list(driver_result["active_symbols"])
                    if not entries_disabled:
                        entry_resume_symbols = list(active_symbols)
                    if entries_disabled:
                        monitor_symbols = _entry_disabled_symbols(
                            tracker=tracker,
                            active_symbols=active_symbols,
                        )
                        if monitor_symbols:
                            active_symbols = monitor_symbols
                            if use_websocket and ticker_adapter is not None:
                                ticker_adapter.update_symbols(session_id, active_symbols)
                    last_price = driver_result["last_price"]
                    stage_b_applied = bool(driver_result["stage_b_applied"])
                    if deps is None:
                        if driver_result["should_complete"]:
                            final_status = "NO_TRADES_ENTRY_WINDOW_CLOSED"
                            terminal_reason = (
                                driver_result.get("stop_reason") or "entry_window_closed"
                            )
                            logger.info(
                                "[%s] Entry window closed with no open positions bar_end=%s",
                                session_id,
                                bar_end.isoformat(),
                            )
                            stop_requested = True
                        elif not active_symbols and not entries_disabled:
                            final_status = "NO_ACTIVE_SYMBOLS"
                            terminal_reason = "no_active_symbols"
                            logger.info(
                                "[%s] No active symbols remain after bar_end=%s",
                                session_id,
                                bar_end.isoformat(),
                            )
                            stop_requested = True

                    if driver_result["triggered"]:
                        final_status = "STOPPING"
                        terminal_reason = "risk_control_triggered"
                        stop_requested = True

                    should_reconcile = bool(driver_result["triggered"]) or (
                        bar_end.minute % 15 == 0
                    )
                    if should_reconcile and _reconcile_live_session(
                        session_id=session_id,
                        reason=f"bar:{bar_end.isoformat()}",
                        alerts_enabled=alerts_enabled,
                    ):
                        entries_disabled = True
                        monitor_symbols = _entry_disabled_symbols(
                            tracker=tracker,
                            active_symbols=active_symbols,
                        )
                        if monitor_symbols:
                            active_symbols = monitor_symbols
                            if use_websocket and ticker_adapter is not None:
                                ticker_adapter.update_symbols(session_id, active_symbols)

                    if stop_requested:
                        break
                if stop_requested:
                    break

                if local_feed and local_feed_exhausted:
                    final_status = "COMPLETED"
                    terminal_reason = "local_feed_exhausted"
                    complete_on_exit = True
                    break

            # Sentinel-file flatten: checked every poll cycle regardless of bar activity
            # so it fires even when active_symbols is a small quiet set with no ticks.
            _signal_file = Path(".tmp_logs") / f"flatten_{session_id}.signal"
            if _signal_file.exists():
                logger.info(
                    "[%s] Flatten signal detected — closing all positions and completing session",
                    session_id,
                )
                try:
                    _signal_file.unlink()
                except OSError:
                    pass
                live_feed_state = _live_mark_feed_state(
                    session_id=session_id,
                    symbol_last_prices=symbol_last_prices,
                    ticker_adapter=ticker_adapter if use_websocket else None,
                    symbols=list(tracker._open.keys()) or active_symbols,
                )
                await flatten_session_positions(
                    session_id,
                    notes="manual_flatten_signal",
                    feed_state=live_feed_state,
                    real_order_router=real_order_router,
                )
                if _reconcile_live_session(
                    session_id=session_id,
                    reason="manual_flatten_signal",
                    alerts_enabled=alerts_enabled,
                ):
                    final_status = "FAILED"
                    terminal_reason = "reconciliation_failed_after_manual_flatten"
                    complete_on_exit = False
                else:
                    final_status = "COMPLETED"
                    terminal_reason = "manual_flatten_signal"
                    complete_on_exit = True
                stop_requested = True
                break

            # Admin command queue: dashboard / agent / operator drop JSON files here.
            _cmd_dir = Path(".tmp_logs") / f"cmd_{session_id}"
            if _cmd_dir.exists():
                for _cmd_file in sorted(_cmd_dir.glob("*.json"), key=_admin_command_sort_key):
                    if not _cmd_file.exists():
                        continue
                    if _is_admin_command_stale(_cmd_file, now):
                        logger.warning(
                            "[%s] Stale admin command dropped: %s",
                            session_id,
                            _cmd_file.name,
                        )
                        try:
                            _cmd_file.unlink()
                        except OSError:
                            logger.debug(
                                "[%s] Failed to delete stale admin command %s",
                                session_id,
                                _cmd_file,
                                exc_info=True,
                            )
                        continue
                    _command_processed = False
                    try:
                        _cmd = json.loads(_cmd_file.read_text())
                        _action = _cmd.get("action", "")
                        _reason = _cmd.get("reason", "admin_command")
                        _requester = _cmd.get("requester", "unknown")
                        logger.info(
                            "[%s] Admin command: action=%s symbols=%s requester=%s",
                            session_id,
                            _action,
                            _cmd.get("symbols"),
                            _requester,
                        )
                        if _action == "close_all":
                            live_feed_state = _live_mark_feed_state(
                                session_id=session_id,
                                symbol_last_prices=symbol_last_prices,
                                ticker_adapter=ticker_adapter if use_websocket else None,
                                symbols=list(tracker._open.keys()) or active_symbols,
                            )
                            await flatten_session_positions(
                                session_id,
                                notes=f"admin_{_reason}_{_requester}",
                                feed_state=live_feed_state,
                                real_order_router=real_order_router,
                            )
                            if _reconcile_live_session(
                                session_id=session_id,
                                reason=f"admin:{_action}",
                                alerts_enabled=alerts_enabled,
                            ):
                                entries_disabled = True
                                final_status = "FAILED"
                                terminal_reason = "reconciliation_failed_after_admin_close_all"
                                complete_on_exit = False
                            else:
                                final_status = "COMPLETED"
                                terminal_reason = f"admin_{_reason}"
                                complete_on_exit = True
                            stop_requested = True
                        elif _action == "close_positions":
                            _syms = [str(s).upper() for s in (_cmd.get("symbols") or [])]
                            if _syms:
                                live_feed_state = _live_mark_feed_state(
                                    session_id=session_id,
                                    symbol_last_prices=symbol_last_prices,
                                    ticker_adapter=ticker_adapter if use_websocket else None,
                                    symbols=_syms,
                                )
                                _close_result = await flatten_positions_subset(
                                    session_id,
                                    _syms,
                                    notes=f"admin_{_reason}_{_requester}",
                                    feed_state=live_feed_state,
                                    real_order_router=real_order_router,
                                )
                                for _pos in _close_result.get("positions", []):
                                    _sym = str(_pos.get("symbol", ""))
                                    if _sym and tracker.has_open_position(_sym):
                                        _pos_obj = tracker.get_open_position(_sym)
                                        _cp = float(_pos.get("close_price", 0))
                                        _qty = float(
                                            getattr(_pos_obj, "current_qty", None)
                                            or getattr(_pos_obj, "quantity", 0)
                                            or 0
                                        )
                                        _dir = str(getattr(_pos_obj, "direction", "LONG")).upper()
                                        _ep = float(getattr(_pos_obj, "entry_price", 0) or 0)
                                        _exit_v = (
                                            _qty * _cp if _dir == "LONG" else _qty * (2 * _ep - _cp)
                                        )
                                        tracker.record_close(_sym, _exit_v)
                                get_paper_db().force_sync()
                                if _reconcile_live_session(
                                    session_id=session_id,
                                    reason=f"admin:{_action}",
                                    alerts_enabled=alerts_enabled,
                                ):
                                    entries_disabled = True
                                    monitor_symbols = _entry_disabled_symbols(
                                        tracker=tracker,
                                        active_symbols=active_symbols,
                                    )
                                    if monitor_symbols:
                                        active_symbols = monitor_symbols
                                        if use_websocket and ticker_adapter is not None:
                                            ticker_adapter.update_symbols(
                                                session_id, active_symbols
                                            )
                        elif _action == "set_risk_budget":
                            portfolio_value = _cmd.get("portfolio_value")
                            max_positions = _cmd.get("max_positions")
                            max_position_pct = _cmd.get("max_position_pct")
                            tracker.update_budget(
                                portfolio_value=(
                                    float(portfolio_value) if portfolio_value is not None else None
                                ),
                                max_positions=(
                                    int(max_positions) if max_positions is not None else None
                                ),
                                max_position_pct=(
                                    float(max_position_pct)
                                    if max_position_pct is not None
                                    else None
                                ),
                            )
                            entries_disabled = False
                            open_notional = tracker.current_open_notional()
                            if (
                                tracker.initial_capital > 0
                                and open_notional >= tracker.initial_capital
                            ):
                                entries_disabled = True
                                monitor_symbols = _entry_disabled_symbols(
                                    tracker=tracker,
                                    active_symbols=active_symbols,
                                )
                                if monitor_symbols:
                                    active_symbols = monitor_symbols
                                    if use_websocket and ticker_adapter is not None:
                                        ticker_adapter.update_symbols(session_id, active_symbols)
                            logger.warning(
                                "[%s] Risk budget updated by %s: portfolio_value=%.2f "
                                "max_positions=%d max_position_pct=%.4f open_notional=%.2f "
                                "cash_available=%.2f entries_disabled=%s",
                                session_id,
                                _requester,
                                tracker.initial_capital,
                                tracker.max_positions,
                                tracker.max_position_pct,
                                open_notional,
                                tracker.cash_available,
                                entries_disabled,
                            )
                            try:
                                get_paper_db().update_session(
                                    session_id,
                                    notes=(
                                        f"RISK_BUDGET_UPDATED portfolio_value="
                                        f"{tracker.initial_capital:.2f} max_positions="
                                        f"{tracker.max_positions} max_position_pct="
                                        f"{tracker.max_position_pct:.4f} reason={_reason}"
                                    ),
                                )
                                get_paper_db().force_sync()
                            except Exception:
                                logger.debug(
                                    "[%s] Failed to stamp risk budget update",
                                    session_id,
                                    exc_info=True,
                                )
                        elif _action == "pause_entries":
                            entries_disabled = True
                            monitor_symbols = _entry_disabled_symbols(
                                tracker=tracker,
                                active_symbols=active_symbols,
                            )
                            if monitor_symbols:
                                active_symbols = monitor_symbols
                                if use_websocket and ticker_adapter is not None:
                                    ticker_adapter.update_symbols(session_id, active_symbols)
                            logger.warning(
                                "[%s] Entries paused by %s reason=%s open_positions=%d",
                                session_id,
                                _requester,
                                _reason,
                                tracker.open_count,
                            )
                            try:
                                get_paper_db().update_session(
                                    session_id,
                                    notes=f"ENTRIES_PAUSED reason={_reason} requester={_requester}",
                                )
                                get_paper_db().force_sync()
                            except Exception:
                                logger.debug(
                                    "[%s] Failed to stamp pause note", session_id, exc_info=True
                                )
                        elif _action == "resume_entries":
                            entries_disabled = False
                            active_symbols = list(entry_resume_symbols)
                            if use_websocket and ticker_adapter is not None:
                                ticker_adapter.update_symbols(session_id, active_symbols)
                            logger.warning(
                                "[%s] Entries resumed by %s reason=%s symbols=%d original_universe=%d",
                                session_id,
                                _requester,
                                _reason,
                                len(active_symbols),
                                len(entry_universe_symbols),
                            )
                            try:
                                get_paper_db().update_session(
                                    session_id,
                                    notes=f"ENTRIES_RESUMED reason={_reason} requester={_requester}",
                                )
                                get_paper_db().force_sync()
                            except Exception:
                                logger.debug(
                                    "[%s] Failed to stamp resume note", session_id, exc_info=True
                                )
                        elif _action == "cancel_pending_intents":
                            cancelled = _cancel_pending_admin_commands(_cmd_dir, _cmd_file)
                            logger.warning(
                                "[%s] Pending admin intents cancelled by %s reason=%s count=%d",
                                session_id,
                                _requester,
                                _reason,
                                cancelled,
                            )
                            try:
                                get_paper_db().update_session(
                                    session_id,
                                    notes=(
                                        f"PENDING_INTENTS_CANCELLED count={cancelled} "
                                        f"reason={_reason} requester={_requester}"
                                    ),
                                )
                                get_paper_db().force_sync()
                            except Exception:
                                logger.debug(
                                    "[%s] Failed to stamp cancel-pending note",
                                    session_id,
                                    exc_info=True,
                                )
                        else:
                            logger.warning(
                                "[%s] Unknown admin command action %r dropped: %s",
                                session_id,
                                _action,
                                _cmd_file.name,
                            )
                        _command_processed = True
                    except Exception:
                        logger.exception(
                            "[%s] Admin command failed: %s", session_id, _cmd_file.name
                        )
                    finally:
                        if _command_processed:
                            try:
                                _cmd_file.unlink()
                            except OSError:
                                pass
                    if _command_processed and _action == "cancel_pending_intents":
                        break
                if stop_requested:
                    break

            stale = False
            freshness_ts = last_snapshot_ts
            if use_websocket and ticker_adapter is not None:
                ws_last_tick_ts = getattr(ticker_adapter, "last_tick_ts", None)
                if ws_last_tick_ts is not None:
                    freshness_ts = ws_last_tick_ts
            if not local_feed and freshness_ts is not None:
                elapsed = (now - freshness_ts).total_seconds()
                # Zombie check: runs regardless of stale_timeout config.
                # stale_feed_timeout_sec may be NULL/0 in the DB (common for live sessions),
                # which previously caused the entire stale block to be skipped. A WebSocket
                # that is "connected" but silent for >5 min is a zombie — detect it always.
                if use_websocket and ticker_adapter is not None and ticker_adapter.is_connected:
                    tick_age = _ticker_last_tick_age_sec(ticker_adapter) or 0
                    if tick_age > 300:
                        # Zombie: socket alive but no ticks for 5+ min — treat as disconnected.
                        # 600s matches the stale_exit_sec threshold defined below.
                        stale = elapsed > 600
                    elif stale_timeout > 0:
                        # Normal connected path: lenient threshold to tolerate quiet symbols.
                        stale = elapsed > max(stale_timeout * 4, 120)
                elif stale_timeout > 0 and elapsed > stale_timeout:
                    stale = True
            if stale:
                no_snapshot_streak += 1
                await _write_feed_state(
                    deps,
                    session_id=session_id,
                    status="STALE",
                    last_event_ts=freshness_ts,
                    last_bar_ts=last_bar_ts,
                    last_price=last_price,
                    stale_reason="No market-data snapshots within timeout",
                    raw_state={
                        "mode": "stale",
                        "symbols": active_symbols,
                        "setup_prefetch": {
                            "skipped": runtime_state.skipped_setup_rows,
                            "invalid": runtime_state.invalid_setup_rows,
                        },
                        "last_snapshot_ts": freshness_ts.isoformat() if freshness_ts else None,
                        "stale_timeout_sec": stale_timeout,
                    },
                )
                await _update_session(session_id, deps, stale_feed_at=now)
            else:
                _was_stale = no_snapshot_streak > 0
                if use_websocket and ticker_adapter is not None and ticker_adapter.is_connected:
                    if stale_alerted and _was_stale:
                        stale_dur_min = (
                            int((now - last_stale_alert_ts).total_seconds() / 60)
                            if last_stale_alert_ts
                            else None
                        )
                        logger.warning(
                            "[%s] market data feed recovered after %d stale cycles",
                            session_id,
                            no_snapshot_streak,
                        )
                        if alerts_enabled:
                            dispatch_feed_recovered_alert(
                                session_id=session_id,
                                stale_minutes=stale_dur_min,
                                open_count=len(tracker._open),
                            )
                        stale_alerted = False  # allow re-alert if it goes stale again
                    no_snapshot_streak = 0
                if latest_raw_state is not None:
                    await _write_feed_state(
                        deps,
                        session_id=session_id,
                        status="OK",
                        last_event_ts=freshness_ts,
                        last_bar_ts=last_bar_ts,
                        last_price=last_price,
                        stale_reason=None,
                        raw_state=latest_raw_state,
                    )
                elif (
                    _was_stale
                    and use_websocket
                    and ticker_adapter is not None
                    and ticker_adapter.is_connected
                ):
                    # Reconnected after a brief drop but next bar hasn't closed yet.
                    # Write OK immediately so the dashboard clears STALE without waiting
                    # up to 5 minutes for the next candle snapshot.
                    await _write_feed_state(
                        deps,
                        session_id=session_id,
                        status="OK",
                        last_event_ts=freshness_ts,
                        last_bar_ts=last_bar_ts,
                        last_price=last_price,
                        stale_reason=None,
                        raw_state={"mode": "reconnected", "connected": True},
                    )

            # Alert once at streak=3, but stay alive to allow KiteConnect protocol-level
            # reconnect to recover.  Only exit after 600s (10 min) of no data.
            stale_exit_sec = 600
            if not local_feed and no_snapshot_streak >= 3:
                _stale_cooldown_ok = (
                    last_stale_alert_ts is None
                    or (now - last_stale_alert_ts).total_seconds() > _FEED_STALE_ALERT_COOLDOWN_SEC
                )
                if alerts_enabled and not stale_alerted and _stale_cooldown_ok:
                    stale_alerted = True
                    last_stale_alert_ts = now
                    open_pos_data = [
                        {
                            "symbol": sym,
                            "direction": pos.direction,
                            "entry_price": pos.entry_price,
                            "stop_loss": pos.stop_loss,
                            "target_price": pos.target_price,
                            "qty": int(pos.current_qty),
                        }
                        for sym, pos in tracker._open.items()
                    ]
                    dispatch_feed_stale_alert(
                        session_id=session_id,
                        last_tick_ts=freshness_ts,
                        open_positions=open_pos_data,
                    )
                stale_duration_sec = (now - freshness_ts).total_seconds() if freshness_ts else 0
                if stale_duration_sec > stale_exit_sec:
                    logger.warning(
                        "[%s] market data stale for %.0fs (limit %ds) — terminating",
                        session_id,
                        stale_duration_sec,
                        stale_exit_sec,
                    )
                    final_status = "STALE"
                    terminal_reason = "feed_stale"
                    break

            await _sleep(deps, supervision_sleep)
    finally:
        # Fix 1: guard the flush so a candle-processing error cannot skip the mandatory
        # cleanup steps (adapter teardown, position flatten, alert dispatcher shutdown).
        try:
            flush_session = session
            try:
                latest_session = await _load_session(session_id, deps)
                if latest_session is not None:
                    flush_session = latest_session
            except Exception:
                logger.debug("[%s] Failed to refresh session before final flush", session_id)
            flush_params = build_backtest_params(flush_session)
            flush_candles: list[ClosedCandle] = []
            if use_websocket and ticker_adapter is not None:
                ticker_adapter.synthesize_quiet_symbols(
                    session_id, active_symbols, _session_now(deps)
                )
                flush_candles.extend(ticker_adapter.drain_closed(session_id))
            flush_candles.extend(builder.flush())
            if flush_candles:
                flush_bars_by_end: dict[datetime, list[ClosedCandle]] = {}
                for candle in flush_candles:
                    flush_bars_by_end.setdefault(candle.bar_end, []).append(candle)
                for bar_end in sorted(flush_bars_by_end):
                    bar_candles = sorted(flush_bars_by_end[bar_end], key=lambda c: c.symbol)
                    for candle in bar_candles:
                        closed_bars += 1
                        last_bar_ts = candle.bar_end
                        symbol_last_prices[candle.symbol] = candle.close
                        state = runtime_state.symbols.get(candle.symbol)
                        setup_row = state.setup_row if state is not None else None
                        _log_parity_trace(session_id=session_id, candle=candle, setup_row=setup_row)
                    driver_result = await paper_session_driver.process_closed_bar_group(
                        session_id=session_id,
                        session=flush_session,
                        bar_candles=bar_candles,
                        runtime_state=runtime_state,
                        tracker=tracker,
                        params=flush_params,
                        active_symbols=active_symbols,
                        strategy=strategy,
                        direction_filter=direction_filter,
                        stage_b_applied=stage_b_applied,
                        symbol_last_prices=symbol_last_prices,
                        last_price=last_price,
                        feed_source=audit_feed_source,
                        transport=audit_transport,
                        feed_audit_writer=record_closed_candles,
                        signal_audit_writer=record_signal_decisions,
                        evaluate_candle_fn=evaluate_candle,
                        execute_entry_fn=execute_entry,
                        enforce_risk_controls=enforce_session_risk_controls,
                        build_feed_state=build_summary_feed_state,
                        real_order_router=real_order_router,
                    )
                    active_symbols = list(driver_result["active_symbols"])
                    last_price = driver_result["last_price"]
                    stage_b_applied = bool(driver_result["stage_b_applied"])
        except Exception:
            logger.exception("[%s] Final flush failed — some candles may be dropped", session_id)
            final_status = "FAILED"
            terminal_reason = "final_flush_failed"
            if alerts_enabled:
                dispatch_session_error_alert(
                    session_id=session_id,
                    reason="session_finalize_failed",
                    details=(
                        "final_flush failed while draining remaining candles; "
                        "session will fail closed and may need reconciliation."
                    ),
                )

        # Adapter teardown — always runs even if flush raised.
        if use_websocket and ticker_adapter is not None:
            try:
                ticker_adapter.unregister_session(session_id)
                if local_ticker_created:
                    ticker_adapter.close()
            except Exception:
                logger.exception("[%s] Adapter teardown failed", session_id)

        # Position flatten — always runs.
        if final_status in {"COMPLETED", "NO_ACTIVE_SYMBOLS", "NO_TRADES_ENTRY_WINDOW_CLOSED"}:
            try:
                await flatten_session_positions(
                    session_id,
                    notes=notes or "session flatten",
                    feed_state=_live_mark_feed_state(
                        session_id=session_id,
                        symbol_last_prices=symbol_last_prices,
                        ticker_adapter=ticker_adapter if use_websocket else None,
                        symbols=list(tracker._open.keys()) or active_symbols,
                    ),
                    real_order_router=real_order_router,
                )
            except Exception:
                logger.warning("Final EOD flatten/summary failed (best-effort)", exc_info=True)
        elif final_status in {"STALE", "FAILED"} and tracker.open_count > 0:
            if auto_flatten_on_abnormal_exit:
                # Auto-flatten on abnormal exit so positions don't linger overnight.
                try:
                    logger.warning(
                        "[%s] %s exit with %d open position(s) — auto-flattening",
                        session_id,
                        final_status,
                        tracker.open_count,
                    )
                    await flatten_session_positions(
                        session_id,
                        notes=f"{final_status}_AUTO_FLATTEN",
                        feed_state=_live_mark_feed_state(
                            session_id=session_id,
                            symbol_last_prices=symbol_last_prices,
                            ticker_adapter=ticker_adapter if use_websocket else None,
                            symbols=list(tracker._open.keys()) or active_symbols,
                        ),
                        real_order_router=real_order_router,
                    )
                except Exception:
                    # Fix 2: alert operator when auto-flatten itself fails — orphaned positions.
                    logger.exception(
                        "[%s] %s auto-flatten failed — positions may be orphaned",
                        session_id,
                        final_status,
                    )
                    if alerts_enabled:
                        dispatch_session_error_alert(
                            session_id=session_id,
                            reason="auto_flatten_failed",
                            details=(
                                f"{final_status} exit; {tracker.open_count} position(s) may be orphaned."
                                " Check paper.duckdb and close manually in Kite."
                            ),
                        )
            else:
                logger.warning(
                    "[%s] %s exit with %d open position(s) — preserving for resume",
                    session_id,
                    final_status,
                    tracker.open_count,
                )

        try:
            await maybe_shutdown_alert_dispatcher()
        finally:
            session_lock_ctx.__exit__(None, None, None)

    # Fix 3: stamp STALE/FAILED into the DB now so the session is never left looking
    # "ACTIVE" after the loop exits.  complete_session() below only writes COMPLETED
    # when complete_on_exit=True; for other statuses it only updates timestamps.
    if final_status in {"STALE", "FAILED"}:
        try:
            # STALE is not a valid DB status (CHECK constraint). Map it to FAILED
            # with a note so the resume path can detect it. The internal final_status
            # variable stays "STALE" for flow-control below (auto-flatten, etc.).
            db_status = "FAILED" if final_status == "STALE" else final_status
            db_notes = f"stale_exit: {notes}" if final_status == "STALE" else notes
            await _update_session(session_id, deps, status=db_status, notes=db_notes)
        except Exception:
            logger.debug(
                "[%s] Failed to stamp terminal status %s", session_id, final_status, exc_info=True
            )

    if complete_on_exit and final_status == "COMPLETED" and terminal_reason is None:
        terminal_reason = "complete_on_exit"
    elif final_status == "STOPPING" and terminal_reason is None:
        terminal_reason = "manual_stop"

    stop_is_terminal = complete_on_exit or final_status in {
        "NO_ACTIVE_SYMBOLS",
        "NO_TRADES_ENTRY_WINDOW_CLOSED",
        "COMPLETED",
    }
    if not stop_is_terminal and final_status == "STOPPING":
        try:
            open_positions = await get_session_positions(session_id, statuses=["OPEN"])
            stop_is_terminal = len(open_positions) == 0
        except Exception:
            logger.debug(
                "[%s] Failed to inspect open positions before finalization",
                session_id,
                exc_info=True,
            )

    try:
        await paper_session_driver.complete_session(
            session_id=session_id,
            complete_on_exit=stop_is_terminal,
            last_bar_ts=last_bar_ts,
            stale_timeout=stale_timeout,
            notes=notes,
            update_session_state=lambda sid, **kwargs: _update_session(sid, deps, **kwargs),
        )
    except Exception:
        logger.exception("[%s] Final session completion failed", session_id)
        final_status = "FAILED"
        terminal_reason = "session_finalize_failed"
        if alerts_enabled:
            dispatch_session_error_alert(
                session_id=session_id,
                reason="session_finalize_failed",
                details=(
                    f"final_status={final_status} last_bar_ts="
                    f"{last_bar_ts.isoformat() if last_bar_ts else 'none'}"
                    f" complete_on_exit={complete_on_exit}"
                ),
            )
        try:
            await _update_session(session_id, deps, status=final_status)
        except Exception:
            logger.debug(
                "[%s] Failed to stamp fallback terminal status %s",
                session_id,
                final_status,
                exc_info=True,
            )
    if stop_is_terminal:
        force_paper_db_sync(get_paper_db())
        if alerts_enabled and final_status in {
            "COMPLETED",
            "NO_TRADES_ENTRY_WINDOW_CLOSED",
            "NO_ACTIVE_SYMBOLS",
        }:
            dispatch_session_completed_alert(session_id=session_id)

    final_session = await _load_session(session_id, deps)
    if final_session is not None and getattr(final_session, "status", None):
        loaded_status = str(final_session.status)
        if loaded_status.upper() != "ACTIVE":
            final_status = loaded_status
            terminal_reason = terminal_reason or f"db_status:{loaded_status.lower()}"
    archive_payload = None
    if final_session and final_session.status in ("COMPLETED", "FAILED"):
        # Archive on both COMPLETED and FAILED exits so stale/crash sessions with
        # closed positions are visible in the dashboard without manual intervention.
        # store_backtest_results has a PAPER dedup guard so re-archiving is safe.
        # Skip archiving zero-trade restart sessions (entry window already closed) —
        # they have no closed trades and create spurious entries in the dashboard
        # dropdown.
        _is_zero_trade_restart = _is_zero_trade_restart_session(
            session_id=session_id,
            terminal_reason=terminal_reason,
            paper_db=get_paper_db(),
        )
        if _is_zero_trade_restart:
            logger.info(
                "[%s] Skipping archive: zero-trade restart session (entry window closed)",
                session_id,
            )
            archive_payload = None
        else:
            try:
                archive_payload = archive_completed_session(session_id, paper_db=get_paper_db())
            except Exception:
                logger.exception("[%s] Paper session archive failed", session_id)
                archive_payload = {
                    "session_id": session_id,
                    "archived": False,
                    "error": "archive_failed",
                }

    feed_state = get_paper_db().get_feed_state(session_id)
    logger.info(
        "[%s] Live exit summary status=%s reason=%s last_bar_ts=%s open_count=%d cycles=%d",
        session_id,
        final_status,
        terminal_reason or "none",
        last_bar_ts.isoformat() if last_bar_ts else "none",
        tracker.open_count,
        cycles,
    )
    return {
        "session_id": session_id,
        "strategy": strategy,
        "symbols": active_symbols,
        "poll_interval_sec": supervision_sleep,
        "candle_interval_minutes": candle_interval,
        "cycles": cycles,
        "quote_events": quote_events,
        "closed_bars": closed_bars,
        "last_snapshot_ts": last_snapshot_ts.isoformat() if last_snapshot_ts else None,
        "last_bar_ts": last_bar_ts.isoformat() if last_bar_ts else None,
        "terminal_reason": terminal_reason,
        "broker_execution": "ZERODHA_LIVE" if real_order_router is not None else "PAPER_LIVE",
        "real_orders_enabled": real_order_router is not None,
        "real_order_fixed_qty": (
            real_order_router.config.fixed_quantity if real_order_router is not None else None
        ),
        "real_order_max_positions": (
            real_order_router.config.max_positions if real_order_router is not None else None
        ),
        "real_order_cash_budget": (
            real_order_router.config.cash_budget if real_order_router is not None else None
        ),
        "final_status": final_status
        if final_status != "ACTIVE"
        else getattr(final_session, "status", "ACTIVE"),
        "feed_state": asdict(feed_state) if feed_state else None,
        "archive": archive_payload,
    }
