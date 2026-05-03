"""Shared paper-session driver logic for replay and live paths."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from engine.bar_orchestrator import (
    SessionPositionTracker,
    check_bar_risk_controls,
    select_entries_for_bar,
    should_process_symbol,
)
from engine.live_market_data import IST, ClosedCandle
from engine.paper_runtime import (
    PaperRuntimeState,
    build_summary_feed_state,
    enforce_session_risk_controls,
    evaluate_candle,
    execute_entry,
    refresh_pending_setup_rows_for_bar,
    runtime_setup_status,
)
from engine.real_order_runtime import RealOrderRouter

logger = logging.getLogger(__name__)


def _direction_readiness(
    runtime_state: PaperRuntimeState, active_symbols: list[str]
) -> tuple[int, int, int, float]:
    """Return (resolved, pending, with_setup, coverage_pct_of_setup_loaded)."""
    resolved = 0
    pending = 0
    with_setup = 0
    for symbol in active_symbols:
        state = runtime_state.symbols.get(symbol)
        if state is None or state.setup_row is None:
            continue
        with_setup += 1
        if bool(state.setup_row.get("direction_pending")):
            pending += 1
            continue
        direction = str(state.setup_row.get("direction") or "").upper()
        if direction in {"LONG", "SHORT"}:
            resolved += 1
        else:
            pending += 1
    coverage = (resolved / with_setup) if with_setup else 0.0
    return resolved, pending, with_setup, coverage


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def apply_stage_b_direction_filter(
    *,
    active_symbols: list[str],
    runtime_state: PaperRuntimeState,
    direction_filter: str,
) -> list[str]:
    """Drop symbols whose pre-computed direction (from strategy_day_state) doesn't match.

    Uses setup_row["direction"] — derived from the true 9:15 close by pivot-build — rather
    than re-deriving from a live candle close. This avoids a one-bar timing skew between
    Kite (bar_end = bar-close time) and replay/local (bar_end = bar-start time) modes.
    """
    normalized_dir = str(direction_filter or "BOTH").upper()
    if normalized_dir not in {"LONG", "SHORT"}:
        return active_symbols

    filtered: list[str] = []
    dropped = 0
    for symbol in active_symbols:
        state = runtime_state.symbols.get(symbol)
        setup_row = state.setup_row if state is not None else None
        if not setup_row:
            # No setup row yet — keep; will be pruned by should_process_symbol later.
            filtered.append(symbol)
            continue
        direction = str(setup_row.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            # Rejected/unknown direction — already pruned by should_process_symbol.
            filtered.append(symbol)
            continue
        if direction == normalized_dir:
            filtered.append(symbol)
        else:
            dropped += 1
    if dropped > 0:
        logger.info(
            "Stage B direction filter: %d -> %d (dropped %d) direction=%s",
            len(active_symbols),
            len(filtered),
            dropped,
            normalized_dir,
        )
    return filtered


async def process_closed_bar_group(
    *,
    session_id: str,
    session: Any,
    bar_candles: list[ClosedCandle],
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    params: Any,
    active_symbols: list[str],
    strategy: str,
    direction_filter: str,
    stage_b_applied: bool,
    symbol_last_prices: dict[str, float],
    last_price: float | None,
    feed_source: str = "unknown",
    transport: str = "unknown",
    feed_audit_writer: Callable[..., Any] | None = None,
    evaluate_candle_fn: Callable[..., Any] = evaluate_candle,
    execute_entry_fn: Callable[..., Any] = execute_entry,
    enforce_risk_controls: Callable[..., Any] = enforce_session_risk_controls,
    build_feed_state: Callable[..., Any] = build_summary_feed_state,
    update_symbols_cb: Callable[[list[str]], Any] | None = None,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    if not bar_candles:
        return {
            "active_symbols": active_symbols,
            "last_price": last_price,
            "stage_b_applied": stage_b_applied,
            "triggered": False,
            "should_complete": False,
            "stop_reason": None,
        }

    bar_candles_sorted = sorted(bar_candles, key=lambda c: c.symbol)
    bar_time = bar_candles_sorted[0].bar_end.astimezone(IST).strftime("%H:%M")
    entry_window_end = str(params.entry_window_end)
    normalized_strategy = str(strategy or "").upper()

    if feed_audit_writer is not None:
        await _maybe_await(
            feed_audit_writer(
                session_id=session_id,
                trade_date=bar_candles_sorted[0].bar_end.date().isoformat(),
                feed_source=feed_source,
                transport=transport,
                bar_candles=bar_candles_sorted,
            )
        )

    # Refresh unresolved setup rows once per bar before any symbol-level evaluation.
    refresh_pending_setup_rows_for_bar(
        runtime_state=runtime_state,
        symbols=active_symbols,
        trade_date=bar_candles_sorted[0].bar_end.date().isoformat(),
        bar_candles=bar_candles_sorted,
        or_minutes=int(getattr(params, "or_minutes", 5) or 5),
        allow_live_fallback=bool(getattr(runtime_state, "allow_live_setup_fallback", True)),
    )

    # Step 1: exits/position advances first.
    # Yield to the event loop every 64 symbols so alert consumer can send Telegram messages
    # between symbol batches — otherwise 600+ synchronous evaluations starve the consumer.
    for _i, candle in enumerate(bar_candles_sorted):
        state = runtime_state.symbols.get(candle.symbol)
        if state is not None and state.candles:
            last_seen = state.candles[-1].get("bar_end")
            if isinstance(last_seen, datetime) and last_seen >= candle.bar_end:
                logger.debug(
                    "[%s] skipping duplicate/out-of-order candle %s at %s (last=%s)",
                    session_id,
                    candle.symbol,
                    candle.bar_end.isoformat(),
                    last_seen.isoformat(),
                )
                continue
        if not tracker.has_open_position(candle.symbol):
            continue
        evaluation = await evaluate_candle_fn(
            session=session,
            candle=candle,
            runtime_state=runtime_state,
            now=candle.bar_end,
            position_tracker=tracker,
            allow_entry_evaluation=False,
            real_order_router=real_order_router,
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
            tracker.record_partial(
                candle.symbol,
                float(advance.get("exit_value") or 0.0),
                float(advance.get("remaining_qty") or 0.0),
            )
        if _i % 64 == 63:
            await asyncio.sleep(0)

    # Yield after exit loop so any CLOSE alerts fire before we scan entries.
    await asyncio.sleep(0)

    # Step 2: evaluate entry candidates for this bar.
    entry_candidates: list[dict[str, Any]] = []
    for _i, candle in enumerate(bar_candles_sorted):
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
        evaluation = await evaluate_candle_fn(
            session=session,
            candle=candle,
            runtime_state=runtime_state,
            now=candle.bar_end,
            position_tracker=tracker,
            allow_entry_evaluation=True,
            real_order_router=real_order_router,
        )
        if evaluation.get("action") == "ENTRY_CANDIDATE":
            entry_candidates.append(evaluation)
        if _i % 64 == 63:
            await asyncio.sleep(0)

    # Step 3: select + execute entries.
    selected_entries = select_entries_for_bar(entry_candidates, tracker)
    for selected in selected_entries:
        execute_result = await execute_entry_fn(
            session=session,
            candidate=dict(selected.get("candidate") or {}),
            setup_row=dict(selected.get("setup_row") or {}),
            params=params,
            now=bar_candles_sorted[0].bar_end,
            position_tracker=tracker,
            real_order_router=real_order_router,
        )
        if execute_result.get("action") == "OPEN":
            candidate = dict(selected.get("candidate") or {})
            logger.info(
                "[%s] OPEN %s @ %.2f",
                session_id,
                str(candidate.get("symbol") or ""),
                float(candidate.get("entry_price") or 0.0),
            )

    # Step 4: apply CPR LONG/SHORT direction filter as soon as any setup rows
    # are loaded. Unresolved directions remain pending and are kept alive by
    # should_process_symbol(); resolved opposite-direction rows are dropped
    # immediately so we do not trade the wrong side while waiting on slow ticks.
    if not stage_b_applied and normalized_strategy == "CPR_LEVELS":
        resolved, pending, with_setup, coverage = _direction_readiness(
            runtime_state, active_symbols
        )
        if with_setup > 0:
            log_fn = logger.warning if pending > 0 and coverage < 0.80 else logger.info
            log_fn(
                "[%s] Stage B applied: resolved=%d pending=%d with_setup=%d coverage=%.0f%% "
                "bar_time=%s direction=%s",
                session_id,
                resolved,
                pending,
                with_setup,
                coverage * 100,
                bar_time,
                direction_filter,
            )
            active_symbols = apply_stage_b_direction_filter(
                active_symbols=active_symbols,
                runtime_state=runtime_state,
                direction_filter=direction_filter,
            )
            stage_b_applied = True
            if update_symbols_cb is not None:
                update_symbols_cb(active_symbols)

    # Step 5: prune symbol universe with shared logic.
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
    if update_symbols_cb is not None and reduced_symbols != active_symbols:
        update_symbols_cb(reduced_symbols)

    triggered = False
    if bar_candles_sorted:
        last_bar_ts = bar_candles_sorted[-1].bar_end
        current_bar_close = float(bar_candles_sorted[-1].close)
        if await check_bar_risk_controls(
            session=session,
            session_id=session_id,
            as_of=last_bar_ts,
            symbol_last_prices=symbol_last_prices,
            last_price=current_bar_close,
            enforce_risk_controls=enforce_risk_controls,
            build_feed_state=build_feed_state,
            real_order_router=real_order_router,
        ):
            triggered = True

    should_complete = bar_time >= entry_window_end and not tracker.open_symbols()
    if should_complete and update_symbols_cb is not None:
        update_symbols_cb([])

    return {
        "active_symbols": reduced_symbols,
        "last_price": float(bar_candles_sorted[-1].close),
        "stage_b_applied": stage_b_applied,
        "triggered": triggered,
        "should_complete": should_complete,
        "stop_reason": "NO_TRADES_ENTRY_WINDOW_CLOSED" if should_complete else None,
    }


async def finalize_session_state(
    *,
    session_id: str,
    complete_on_exit: bool,
    last_bar_ts: datetime | None,
    stale_timeout: int,
    notes: str | None,
    update_session_state: Callable[..., Any],
) -> Any:
    if complete_on_exit:
        return await _maybe_await(
            update_session_state(
                session_id,
                status="COMPLETED",
                latest_candle_ts=last_bar_ts,
                clear_stale_feed_at=True,
                notes=notes,
            )
        )
    return await _maybe_await(
        update_session_state(
            session_id,
            latest_candle_ts=last_bar_ts,
            stale_feed_at=(
                last_bar_ts + timedelta(seconds=stale_timeout)
                if last_bar_ts is not None and stale_timeout > 0
                else None
            ),
            notes=notes,
        )
    )


async def complete_session(
    *,
    session_id: str,
    complete_on_exit: bool,
    last_bar_ts: datetime | None,
    stale_timeout: int,
    notes: str | None,
    update_session_state: Callable[..., Any],
) -> Any:
    """Finalize a paper session using the shared terminal-state contract."""

    return await finalize_session_state(
        session_id=session_id,
        complete_on_exit=complete_on_exit,
        last_bar_ts=last_bar_ts,
        stale_timeout=stale_timeout,
        notes=notes,
        update_session_state=update_session_state,
    )
