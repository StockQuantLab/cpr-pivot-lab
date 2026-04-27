"""Live paper-session runner with transport-agnostic candle processing."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from config.settings import get_settings
from db.duckdb import get_dashboard_db
from db.paper_db import get_dashboard_paper_db, get_paper_db
from engine import paper_session_driver as paper_session_driver
from engine.bar_orchestrator import (
    SessionPositionTracker,
    select_entries_for_bar,
    should_process_symbol,
)
from engine.cpr_atr_shared import regime_snapshot_close_col
from engine.kite_ticker_adapter import KiteTickerAdapter
from engine.live_market_data import (
    IST,
    ClosedCandle,
    FiveMinuteCandleBuilder,
    KiteQuoteAdapter,
    MarketDataAdapter,
    MarketSnapshot,
)
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
    refresh_pending_setup_rows_for_bar,
    register_session_start,
    runtime_setup_status,
)
from scripts.paper_archive import archive_completed_session
from scripts.paper_feed_audit import record_closed_candles
from scripts.paper_prepare import pre_filter_symbols_for_strategy

logger = logging.getLogger(__name__)
_BOOL_TRUE = {"1", "true", "yes", "on"}
_PARITY_TRACE_ENABLED = str(os.getenv("PIVOT_LIVE_PARITY_TRACE", "0")).strip().lower() in _BOOL_TRUE
_SETUP_PARITY_CHECK_ENABLED = (
    str(os.getenv("PIVOT_LIVE_SETUP_PARITY_CHECK", "0")).strip().lower() in _BOOL_TRUE
)
_ORIGINAL_LOAD_SETUP_ROW = load_setup_row
_WEBSOCKET_RECONNECT_ALERT_ATTEMPTS = 3
_WEBSOCKET_RECOVERY_AFTER_SEC = 20.0
_WEBSOCKET_RECOVERY_COOLDOWN_SEC = 30.0


@dataclass(slots=True)
class LiveSessionDeps:
    session_loader: Callable[[str], Any] | None = None
    session_updater: Callable[..., Any] | None = None
    feed_writer: Callable[..., Any] | None = None
    feed_reader: Callable[[str], Any] | None = None
    sleep_fn: Callable[[float], Awaitable[None]] | None = None
    now_fn: Callable[[], datetime] | None = None
    alerts_enabled: bool | None = None


def _feed_snapshot_payload(snapshot: MarketSnapshot) -> dict[str, Any]:
    return {
        "mode": "live_quote",
        "symbol": snapshot.symbol,
        "ts": snapshot.ts.isoformat(),
        "last_price": snapshot.last_price,
        "volume": snapshot.volume,
        "source": snapshot.source,
    }


def _closed_candle_payload(candle: ClosedCandle) -> dict[str, Any]:
    return {
        "mode": "closed_bar",
        "symbol": candle.symbol,
        "bar_start": candle.bar_start.isoformat(),
        "bar_end": candle.bar_end.isoformat(),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "first_snapshot_ts": candle.first_snapshot_ts.isoformat(),
        "last_snapshot_ts": candle.last_snapshot_ts.isoformat(),
    }


def _seconds_until_next_candle_close(now: datetime, candle_interval_minutes: int) -> float:
    interval_seconds = max(1, int(candle_interval_minutes)) * 60
    seconds_since_midnight = (
        now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1_000_000.0
    )
    remaining = interval_seconds - (seconds_since_midnight % interval_seconds)
    if remaining <= 0:
        return float(interval_seconds)
    return float(remaining)


def _resolve_poll_interval(
    settings: Any,
    poll_interval_sec: float | None,
    candle_interval_minutes: int,
    *,
    now: datetime | None = None,
) -> float:
    base_interval = (
        settings.paper_live_poll_interval_sec if poll_interval_sec is None else poll_interval_sec
    )
    if base_interval <= 0:
        base_interval = settings.paper_live_poll_interval_sec
    if candle_interval_minutes <= 0:
        return base_interval

    current_time = now or datetime.now(IST)
    seconds_to_close = _seconds_until_next_candle_close(current_time, candle_interval_minutes)
    if seconds_to_close <= 5.0:
        return min(base_interval, 0.5)
    if seconds_to_close <= 20.0:
        return min(base_interval, 1.0)
    if seconds_to_close <= 60.0:
        return min(base_interval, 2.0)
    return base_interval


def _resolve_candle_interval(settings: Any, candle_interval_minutes: int | None) -> int:
    if candle_interval_minutes is None:
        return settings.paper_candle_interval_minutes
    return candle_interval_minutes


def _resolve_active_symbols(session: Any, symbols: list[str] | None) -> list[str]:
    return [s.strip() for s in symbols or session.symbols if s and s.strip()]


def _floor_bucket_start(ts: datetime, interval_minutes: int) -> datetime:
    total_minutes = ts.hour * 60 + ts.minute
    bucket_minutes = (total_minutes // interval_minutes) * interval_minutes
    return ts.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0,
        microsecond=0,
    )


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

    db = get_dashboard_db()
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
        from engine.paper_runtime import resolve_cpr_direction

        tc = float(row[3] or 0.0)
        bc = float(row[4] or 0.0)
        atr = float(row[10] or 0.0)
        if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
            invalid_symbols.append((symbol, tc, bc, atr))
            return None
        or_close_5 = float(row[16]) if row[16] is not None else None
        direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
        if direction == "NONE" and or_close_5 is None:
            direction = str(row[21] or "NONE")
        if direction == "NONE" and live_candles:
            from engine.paper_runtime import _build_intraday_summary

            intraday = _build_intraday_summary(
                live_candles,
                or_minutes=candle_interval_minutes,
                bar_end_offset=runtime_state.bar_end_offset,
            )
            live_or_close_5 = intraday.get("or_close_5")
            if live_or_close_5 is not None:
                direction = resolve_cpr_direction(live_or_close_5, tc, bc, fallback="NONE")
                or_close_5 = live_or_close_5
        rvol_baseline: list[float | None] | None = None
        if row[25]:
            rvol_baseline = [float(v) if v is not None else None for v in row[25]]
        setup_row = {
            "trade_date": str(row[1] or trade_date),
            "prev_day_close": float(row[2]) if row[2] is not None else None,
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
            "or_high_5": float(row[13] or 0.0),
            "or_low_5": float(row[14] or 0.0),
            "open_915": float(row[15] or 0.0),
            "or_close_5": or_close_5,
            "open_side": str(row[17] or ""),
            "open_to_cpr_atr": float(row[18]) if row[18] is not None else None,
            "gap_abs_pct": float(row[19]) if row[19] is not None else None,
            "or_atr_5": float(row[20]) if row[20] is not None else None,
            "direction": direction,
            "is_narrowing": bool(row[22]),
            "cpr_shift": str(row[23] or "OVERLAP"),
            "regime_move_pct": float(row[24]) if row[24] is not None else None,
            "rvol_baseline": rvol_baseline,
            "setup_source": "market_day_state",
        }
        return setup_row

    batch_rows: dict[str, tuple[Any, ...]] | None = None
    use_batch_prefetch = load_setup_row is _ORIGINAL_LOAD_SETUP_ROW and bool(unique_symbols)
    if use_batch_prefetch:
        try:
            from engine.paper_runtime import _MARKET_DB_READ_LOCK

            db = get_dashboard_db()
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
                missing_symbols.append(symbol)
                continue
            setup_row = _hydrate_setup_row(symbol=symbol, row=row, live_candles=state.candles)
            if setup_row is None:
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


def _normalize_setup_row_direction(setup_row: dict[str, Any]) -> dict[str, Any]:
    direction = str(setup_row.get("direction") or "").upper()
    setup_row["direction"] = direction
    setup_row["direction_pending"] = direction not in {"LONG", "SHORT"}
    return setup_row


def _log_parity_trace(
    *,
    session_id: str,
    candle: ClosedCandle,
    setup_row: dict[str, Any] | None,
) -> None:
    if not _PARITY_TRACE_ENABLED:
        return
    logger.info(
        "PARITY_TRACE session=%s symbol=%s bar_end=%s setup_source=%s tc=%.6f bc=%.6f atr=%.6f"
        " ohlcv=(%.2f,%.2f,%.2f,%.2f,%.2f)",
        session_id,
        candle.symbol,
        candle.bar_end.isoformat(),
        str((setup_row or {}).get("setup_source") or "unknown"),
        float((setup_row or {}).get("tc") or 0.0),
        float((setup_row or {}).get("bc") or 0.0),
        float((setup_row or {}).get("atr") or 0.0),
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
    )


def _log_bar_heartbeats(
    *,
    session_id: str,
    active_symbols: list[str],
    cycle_closed: list[ClosedCandle],
) -> None:
    if not cycle_closed:
        return
    counts_by_bar: dict[str, int] = {}
    for candle in cycle_closed:
        key = candle.bar_end.isoformat()
        counts_by_bar[key] = counts_by_bar.get(key, 0) + 1
    for bar_end_iso, closed_count in sorted(counts_by_bar.items()):
        logger.info(
            "LIVE_BAR session=%s bar_end=%s closed=%d active=%d",
            session_id,
            bar_end_iso,
            closed_count,
            len(active_symbols),
        )


def _log_ticker_health(
    *,
    session_id: str,
    ticker_adapter: Any,
    active_symbols: list[str],
) -> dict[str, Any] | None:
    """Emit a one-line ticker health summary. No-op for non-Kite adapters."""
    if ticker_adapter is None or not hasattr(ticker_adapter, "health_stats"):
        return None
    try:
        stats = ticker_adapter.health_stats()
        coverage = ticker_adapter.symbol_coverage(active_symbols, within_sec=300.0)
    except Exception:
        logger.debug("ticker health_stats failed", exc_info=True)
        return None
    logger.info(
        "TICKER_HEALTH session=%s connected=%s ticks=%d last_tick_age=%s "
        "closes=%d reconnects=%d subs=%d coverage=%.0f%% (%d/%d) stale=%d missing=%d",
        session_id,
        stats["connected"],
        stats["tick_count"],
        f"{stats['last_tick_age_sec']:.0f}s" if stats["last_tick_age_sec"] is not None else "none",
        stats["close_count"],
        stats["reconnect_count"],
        stats["subscribed_tokens"],
        coverage["coverage_pct"],
        coverage["covered"],
        coverage["total"],
        coverage["stale"],
        coverage["missing"],
    )
    return {"stats": stats, "coverage": coverage}


def _ticker_last_tick_age_sec(ticker_adapter: Any) -> float | None:
    """Return last-tick age from either the live or replay adapter API."""
    if ticker_adapter is None:
        return None

    stats_fn = getattr(ticker_adapter, "health_stats", None)
    if callable(stats_fn):
        try:
            stats = stats_fn() or {}
        except Exception:
            logger.debug("ticker health_stats failed for stale probe", exc_info=True)
            return None
        return float(stats.get("last_tick_age_sec") or 0)

    stats_fn = getattr(ticker_adapter, "get_stats", None)
    if callable(stats_fn):
        try:
            stats = stats_fn() or {}
        except Exception:
            logger.debug("ticker get_stats failed for stale probe", exc_info=True)
            return None
        return float(stats.get("last_tick_age_sec") or 0)

    return None


def _log_direction_readiness(
    *,
    session_id: str,
    runtime_state: PaperRuntimeState,
    active_symbols: list[str],
) -> dict[str, int | float]:
    resolved = 0
    pending = 0
    missing = 0
    with_setup = 0
    for symbol in active_symbols:
        state = runtime_state.symbols.get(symbol)
        setup_row = state.setup_row if state is not None else None
        if setup_row is None:
            missing += 1
            continue
        with_setup += 1
        if bool(setup_row.get("direction_pending")):
            pending += 1
            continue
        direction = str(setup_row.get("direction") or "").upper()
        if direction in {"LONG", "SHORT"}:
            resolved += 1
        else:
            pending += 1
    coverage = (resolved / with_setup) if with_setup else 0.0
    logger.info(
        "LIVE_DIRECTION_PREFLIGHT session=%s resolved=%d pending=%d with_setup=%d missing=%d "
        "coverage=%.0f%%",
        session_id,
        resolved,
        pending,
        with_setup,
        missing,
        coverage * 100,
    )
    return {
        "resolved": resolved,
        "pending": pending,
        "missing": missing,
        "with_setup": with_setup,
        "coverage_pct": coverage * 100,
    }


async def _process_closed_bar_group(
    *,
    session_id: str,
    session: Any,
    bar_candles: list[ClosedCandle],
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    params: Any,
    active_symbols: list[str],
) -> tuple[list[str], float | None]:
    if not bar_candles:
        return active_symbols, None
    bar_candles_sorted = sorted(bar_candles, key=lambda c: c.symbol)
    bar_time = bar_candles_sorted[0].bar_end.astimezone(IST).strftime("%H:%M")
    entry_window_end = str(params.entry_window_end)

    refresh_pending_setup_rows_for_bar(
        runtime_state=runtime_state,
        symbols=active_symbols,
        trade_date=bar_candles_sorted[0].bar_end.date().isoformat(),
        bar_candles=bar_candles_sorted,
        or_minutes=int(getattr(params, "or_minutes", 5) or 5),
        allow_live_fallback=bool(getattr(runtime_state, "allow_live_setup_fallback", True)),
    )

    # Step 1: exits/position advances first.
    for candle in bar_candles_sorted:
        if not tracker.has_open_position(candle.symbol):
            continue
        evaluation = await evaluate_candle(
            session=session,
            candle=candle,
            runtime_state=runtime_state,
            now=candle.bar_end,
            position_tracker=tracker,
            allow_entry_evaluation=False,
        )
        advance = dict(evaluation.get("advance_result") or {})
        if advance.get("action") == "CLOSE":
            tracker.record_close(candle.symbol, float(advance.get("exit_value") or 0.0))
            logger.info(
                "[%s] CLOSE %s reason=%s",
                session_id,
                candle.symbol,
                str(advance.get("reason") or "exit"),
            )
        elif advance.get("action") == "PARTIAL":
            tracker.credit_cash(float(advance.get("exit_value") or 0.0))

    # Step 2: evaluate entry candidates for this bar.
    entry_candidates: list[dict[str, Any]] = []
    for candle in bar_candles_sorted:
        if tracker.has_open_position(candle.symbol):
            continue
        setup_status = runtime_setup_status(runtime_state, candle.symbol)
        if not should_process_symbol(
            bar_time=bar_time,
            entry_window_end=entry_window_end,
            tracker=tracker,
            symbol=candle.symbol,
            setup_status=setup_status,
        ):
            continue
        evaluation = await evaluate_candle(
            session=session,
            candle=candle,
            runtime_state=runtime_state,
            now=candle.bar_end,
            position_tracker=tracker,
            allow_entry_evaluation=True,
        )
        if evaluation.get("action") == "ENTRY_CANDIDATE":
            entry_candidates.append(evaluation)

    # Step 3: select + execute entries.
    selected_entries = select_entries_for_bar(entry_candidates, tracker)
    for selected in selected_entries:
        execute_result = await execute_entry(
            session=session,
            candidate=dict(selected.get("candidate") or {}),
            setup_row=dict(selected.get("setup_row") or {}),
            params=params,
            now=bar_candles_sorted[0].bar_end,
            position_tracker=tracker,
        )
        if execute_result.get("action") == "OPEN":
            candidate = dict(selected.get("candidate") or {})
            logger.info(
                "[%s] OPEN %s @ %.2f",
                session_id,
                str(candidate.get("symbol") or ""),
                float(candidate.get("entry_price") or 0.0),
            )

    # Step 4: prune symbol universe with shared logic.
    reduced_symbols = [
        symbol
        for symbol in active_symbols
        if should_process_symbol(
            bar_time=bar_time,
            entry_window_end=entry_window_end,
            tracker=tracker,
            symbol=symbol,
            setup_status=runtime_setup_status(runtime_state, symbol),
        )
    ]
    return reduced_symbols, float(bar_candles_sorted[-1].close)


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

    register_session_start()
    _start_alert_dispatcher()  # start consumer eagerly so first alerts are not delayed
    _was_already_active = getattr(session, "status", "") == "ACTIVE"
    if session.status != "ACTIVE":
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

    runtime_state = PaperRuntimeState(
        allow_live_setup_fallback=allow_late_start_fallback,
        bar_end_offset=timedelta(minutes=5)
        if getattr(ticker_adapter, "_local_feed", False)
        else None,
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
            "[%s] Session already ACTIVE — skipping duplicate SESSION_STARTED alert",
            session_id,
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
    alerts_enabled = True if deps is None else bool(deps.alerts_enabled)
    _stale_alert_cooldown_sec = 300  # 5 min between repeated FEED_STALE alerts
    audit_feed_source = "kite"
    audit_transport = "websocket" if use_websocket else "rest"

    try:
        print(
            f"[live] {session_id} started - strategy={strategy} symbols={len(active_symbols)}"
            f" transport={'websocket' if use_websocket else 'rest'}",
            flush=True,
        )
        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            now = _session_now(deps)
            current_session = await _load_session(session_id, deps)
            if current_session is None:
                final_status = "MISSING"
                terminal_reason = "session_missing"
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
                            >= _stale_alert_cooldown_sec
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
                        latest_raw_state = {
                            **_feed_snapshot_payload(snapshot),
                            "symbol_last_prices": {
                                **symbol_last_prices,
                                snapshot.symbol: snapshot.last_price,
                            },
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
                                active_symbols=active_symbols,
                                strategy=strategy,
                                direction_filter=direction_filter,
                                stage_b_applied=stage_b_applied,
                                symbol_last_prices=symbol_last_prices,
                                last_price=last_price,
                                feed_source=audit_feed_source,
                                transport=audit_transport,
                                feed_audit_writer=record_closed_candles,
                                evaluate_candle_fn=evaluate_candle,
                                execute_entry_fn=execute_entry,
                                enforce_risk_controls=enforce_session_risk_controls,
                                build_feed_state=build_summary_feed_state,
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
                        elif not active_symbols:
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
                final_status = "COMPLETED"
                terminal_reason = "manual_flatten_signal"
                complete_on_exit = True
                stop_requested = True
                break

            # Admin command queue: dashboard / agent / operator drop JSON files here.
            _cmd_dir = Path(".tmp_logs") / f"cmd_{session_id}"
            if _cmd_dir.exists():
                for _cmd_file in sorted(_cmd_dir.glob("*.json")):
                    try:
                        import json as _json

                        _cmd = _json.loads(_cmd_file.read_text())
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
                            final_status = "COMPLETED"
                            terminal_reason = f"admin_{_reason}"
                            complete_on_exit = True
                            stop_requested = True
                        elif _action == "close_positions":
                            _syms = [str(s).upper() for s in (_cmd.get("symbols") or [])]
                            if _syms:
                                _close_result = await flatten_positions_subset(
                                    session_id,
                                    _syms,
                                    notes=f"admin_{_reason}_{_requester}",
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
                    except Exception:
                        logger.exception(
                            "[%s] Admin command failed: %s", session_id, _cmd_file.name
                        )
                    finally:
                        try:
                            _cmd_file.unlink()
                        except OSError:
                            pass
                if stop_requested:
                    break

            stale = False
            if not local_feed and last_snapshot_ts is not None:
                elapsed = (now - last_snapshot_ts).total_seconds()
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
                    last_event_ts=last_snapshot_ts,
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
                        "last_snapshot_ts": last_snapshot_ts.isoformat()
                        if last_snapshot_ts
                        else None,
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
                        last_event_ts=last_snapshot_ts,
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
                        last_event_ts=last_snapshot_ts,
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
                    or (now - last_stale_alert_ts).total_seconds() > _stale_alert_cooldown_sec
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
                        last_tick_ts=last_snapshot_ts,
                        open_positions=open_pos_data,
                    )
                stale_duration_sec = (
                    (now - last_snapshot_ts).total_seconds() if last_snapshot_ts else 0
                )
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
                        session=session,
                        bar_candles=bar_candles,
                        runtime_state=runtime_state,
                        tracker=tracker,
                        params=params,
                        active_symbols=active_symbols,
                        strategy=strategy,
                        direction_filter=direction_filter,
                        stage_b_applied=stage_b_applied,
                        symbol_last_prices=symbol_last_prices,
                        last_price=last_price,
                        feed_source=audit_feed_source,
                        transport=audit_transport,
                        feed_audit_writer=record_closed_candles,
                        evaluate_candle_fn=evaluate_candle,
                        execute_entry_fn=execute_entry,
                        enforce_risk_controls=enforce_session_risk_controls,
                        build_feed_state=build_summary_feed_state,
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
            ticker_adapter.unregister_session(session_id)
            if local_ticker_created:
                ticker_adapter.close()

        # Position flatten — always runs.
        if final_status in {"COMPLETED", "NO_ACTIVE_SYMBOLS", "NO_TRADES_ENTRY_WINDOW_CLOSED"}:
            try:
                await flatten_session_positions(session_id, notes=notes or "session flatten")
            except Exception:
                logger.debug("Final EOD flatten/summary failed (best-effort)", exc_info=True)
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
                        session_id, notes=f"{final_status}_AUTO_FLATTEN"
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

        await maybe_shutdown_alert_dispatcher()

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
        if alerts_enabled and final_status == "COMPLETED":
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
        # they have no trades and create spurious entries in the dashboard dropdown.
        _is_zero_trade_restart = (
            terminal_reason in ("no_trades_entry_window_closed", "NO_TRADES_ENTRY_WINDOW_CLOSED")
            and len(tracker._closed_today) == 0
            and "-" in session_id
            and len(session_id.split("-")[-1]) == 6  # suffix pattern: -abc123
        )
        if _is_zero_trade_restart:
            logger.info(
                "[%s] Skipping archive: zero-trade restart session (entry window closed)",
                session_id,
            )
            archive_payload = None
        else:
            archive_payload = archive_completed_session(
                session_id, paper_db=get_dashboard_paper_db()
            )

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
        "final_status": final_status
        if final_status != "ACTIVE"
        else getattr(final_session, "status", "ACTIVE"),
        "feed_state": asdict(feed_state) if feed_state else None,
        "archive": archive_payload,
    }
