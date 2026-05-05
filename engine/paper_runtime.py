"""Shared helpers for paper-trading runtime state and summaries."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

from db.paper_db import FeedState, PaperPosition, PaperSession
from engine.alert_dispatcher import AlertDispatcher, AlertType, get_alert_config
from engine.bar_orchestrator import SessionPositionTracker
from engine.cpr_atr_shared import (
    CompletedCandleDecision,
    resolve_completed_candle_trade_step,
    scan_cpr_levels_entry,
    split_scale_out_quantity,
)
from engine.cpr_atr_strategy import DayPack
from engine.execution_safety import build_order_idempotency_key
from engine.paper_admin import write_admin_command
from engine.paper_alerts import (
    _format_close_alert,
    _format_event_time,
    _format_open_alert,
    _format_risk_alert,
    _parse_session_label,
)
from engine.paper_params import (
    BacktestParams,
    PaperRuntimeState,
    SymbolRuntimeState,
    apply_paper_strategy_defaults,
    build_backtest_params,
    build_backtest_params_from_overrides,
)
from engine.paper_risk import _risk_limit_reasons
from engine.paper_setup_loader import (
    _MARKET_DB_READ_LOCK,
    _build_intraday_summary,
    _live_setup_status,
    load_setup_row,
    refresh_pending_setup_rows_for_bar,
    runtime_setup_status,
    setup_row_uses_or_proxy,
)
from engine.paper_store import (
    _db,
    append_order_event,
    get_feed_state,
    get_session,
    get_session_positions,
    open_position,
    update_session_state,
)
from engine.paper_store import (
    accumulate_session_pnl as _store_accumulate_session_pnl,
)
from engine.paper_store import (
    force_paper_db_sync as _store_force_paper_db_sync,
)
from engine.paper_store import (
    update_position as _store_update_position,
)
from engine.paper_summary import (
    _exit_value_for_position,
    _float_or_none,
    build_summary_feed_state,
    mark_price_for_position,
    summarize_paper_positions,
)
from engine.paper_trailing import (
    _get_trailing_stop,
    _updated_trail_state,
    clear_trailing_stop_cache,
)
from engine.real_order_runtime import RealOrderRouter

logger = logging.getLogger(__name__)
build_strategy_config_from_overrides = build_backtest_params_from_overrides
build_strategy_config = build_backtest_params

_IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Alert dispatcher (lazy singleton — best-effort, never blocks trading)
# ---------------------------------------------------------------------------

_alert_dispatcher: AlertDispatcher | None = None
_background_tasks: set[asyncio.Task] = set()
_active_session_count = 0
_active_session_lock = threading.Lock()
_suppress_alerts = False
_alert_sink: Callable[[AlertType, str, str], Any] | None = None
# In-memory dedup guard: session IDs that have already dispatched FLATTEN_EOD.
# Checked and set synchronously before dispatch so no async race with alert_log writes.
_flatten_eod_sent: set[str] = set()
# In-memory dedup guard for session-start notifications. A retry/restart inside the same
# process should not spam duplicate "session started" alerts for the same session_id.
_session_started_sent: set[str] = set()


def _has_flatten_eod_in_alert_log(session_id: str, *, trade_date: str | None = None) -> bool:
    """Return True when a persisted FLATTEN_EOD row already exists for this session.

    This makes EOD alert dedupe resilient across process restarts. Keep the session-id
    prefix match to avoid false positives from different sessions with similar suffixes.
    """
    try:
        con = _db().con
    except Exception:
        return False
    try:
        session_tag = f"Session: <code>{session_id}</code>"
        if trade_date:
            row = con.execute(
                """
                SELECT 1
                FROM alert_log
                WHERE alert_type='FLATTEN_EOD'
                  AND body LIKE ?
                  AND (subject LIKE ? OR body LIKE ?)
                LIMIT 1
                """,
                [f"%{session_tag}%", f"%{trade_date}%", f"%{trade_date}%"],
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT 1
                FROM alert_log
                WHERE alert_type='FLATTEN_EOD'
                  AND body LIKE ?
                LIMIT 1
                """,
                [f"%{session_tag}%"],
            ).fetchone()
        if not row:
            return False
        first = row[0]
        if isinstance(first, int | float):
            return bool(first)
        return True
    except Exception:
        logger.debug("Failed to check FLATTEN_EOD alert dedupe from alert_log", exc_info=True)
        return False


def set_alerts_suppressed(suppress: bool) -> None:
    """Control whether alerts are dispatched (True = suppress, False = enable)."""
    global _suppress_alerts
    _suppress_alerts = suppress


def set_alert_sink(sink: Callable[[AlertType, str, str], Any] | None) -> None:
    """Override alert dispatch for tests or alternate sinks."""
    global _alert_sink
    _alert_sink = sink


def reset_alert_dedupe(session_id: str | None = None) -> None:
    """Clear in-memory alert dedupe state for an explicit in-process restart."""
    if session_id is None:
        _flatten_eod_sent.clear()
        _session_started_sent.clear()
        return
    normalized = str(session_id)
    _flatten_eod_sent.discard(normalized)
    _session_started_sent.discard(normalized)


def register_session_start() -> None:
    global _active_session_count
    with _active_session_lock:
        _active_session_count += 1


def _decrement_active_session_count() -> None:
    global _active_session_count
    with _active_session_lock:
        _active_session_count -= 1


async def maybe_shutdown_alert_dispatcher() -> None:
    global _active_session_count
    _decrement_active_session_count()
    with _active_session_lock:
        if _active_session_count <= 0:
            _active_session_count = 0
    should_shutdown = _active_session_count <= 0
    if should_shutdown:
        await shutdown_alert_dispatcher()


def _get_alert_dispatcher() -> AlertDispatcher:
    global _alert_dispatcher
    if _alert_dispatcher is None:
        config = get_alert_config()
        _alert_dispatcher = AlertDispatcher(_db(), config)
    return _alert_dispatcher


def _start_alert_dispatcher() -> None:
    """Start the alert consumer task. Safe to call multiple times."""
    dispatcher = _get_alert_dispatcher()
    try:
        loop = asyncio.get_running_loop()
        if not dispatcher._running:
            loop.create_task(dispatcher.start())  # noqa: RUF006
    except RuntimeError:
        pass  # No event loop — dispatcher will be started lazily on first dispatch


async def shutdown_alert_dispatcher() -> None:
    """Drain queued alerts and stop the consumer. Call on session exit."""
    dispatcher = _get_alert_dispatcher()
    if dispatcher._running:
        await dispatcher.shutdown()
    # Wait for background fire-and-forget tasks to complete (don't cancel them).
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)
        _background_tasks.clear()


async def _accumulate_session_pnl(session_id: str, pnl_delta: float) -> None:
    await _store_accumulate_session_pnl(session_id, pnl_delta)


def force_paper_db_sync(paper_db: Any | None = None) -> None:
    _store_force_paper_db_sync(paper_db)


async def update_position(position_id: str, **kwargs: Any) -> PaperPosition | None:
    position = await _store_update_position(position_id, **kwargs)
    status = str(kwargs.get("status") or "").upper()
    if status in {"CLOSED", "FLATTENED"}:
        _clear_trailing_stop_cache(position_id)
    return position


def _clear_trailing_stop_cache(position_id: str) -> None:
    clear_trailing_stop_cache(position_id)


def _dispatch_alert(alert_type: AlertType, subject: str, body: str) -> None:
    """Fire-and-forget alert dispatch. Best-effort, never blocks trading."""
    if _suppress_alerts:
        return
    try:
        if _alert_sink is not None:
            result = _alert_sink(alert_type, subject, body)
            if inspect.isawaitable(result):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(result)  # noqa: RUF006
                except RuntimeError:
                    asyncio.run(result)
            return
        dispatcher = _get_alert_dispatcher()
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(dispatcher.dispatch(alert_type, subject, body))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
        except RuntimeError:
            # No event loop running -- log to PaperDB directly
            _db().log_alert(
                str(alert_type.value),
                subject,
                body,
                channel="LOG",
                status="failed",
                error_msg="no_event_loop",
            )
    except Exception as exc:
        logger.warning("Alert dispatch failed (best-effort, non-blocking): %s", exc)


def dispatch_session_error_alert(
    *,
    session_id: str,
    reason: str,
    details: str | None = None,
) -> None:
    """Emit a non-blocking SESSION_ERROR alert for live/replay operators."""
    session_tag = str(session_id or "")[:16]
    normalized_reason = str(reason or "session_error").strip() or "session_error"
    subject = f"SESSION_ERROR {session_tag} {normalized_reason}"
    body = f"Session: <code>{session_tag}</code>\nReason: {normalized_reason}"
    if details:
        body += f"\nDetails: {details}"
    _dispatch_alert(AlertType.SESSION_ERROR, subject, body)


def dispatch_session_state_alert(
    *,
    session_id: str,
    state: str,
    details: str | None = None,
) -> None:
    """Emit a non-blocking session lifecycle alert for manual pause/resume actions."""
    normalized_state = str(state or "").strip().upper()
    if normalized_state not in {"PAUSED", "RESUMED"}:
        raise ValueError(f"Unsupported session state alert: {normalized_state!r}")
    short_label, date_label = _parse_session_label(session_id)
    alert_type = (
        AlertType.SESSION_PAUSED if normalized_state == "PAUSED" else AlertType.SESSION_RESUMED
    )
    icon = "⏸️" if normalized_state == "PAUSED" else "🔄"
    subject = f"{icon} Session {normalized_state.title()} — {short_label} · {date_label}"
    now_str = datetime.now(_IST).strftime("%H:%M IST")
    body_lines = [
        f"{icon} <b>Session {normalized_state.lower()}</b> · {short_label}",
        f"Time: <code>{now_str}</code>",
        f"ID: <code>{session_id}</code>",
    ]
    if details:
        body_lines.append(f"Details: {details}")
    _dispatch_alert(alert_type, subject, "\n".join(body_lines))


def dispatch_feed_stale_alert(
    *,
    session_id: str,
    last_tick_ts: datetime | None = None,
    open_positions: list[dict[str, object]] | None = None,
    details: str | None = None,
) -> None:
    """Emit a FEED_STALE alert.

    Pass `last_tick_ts` and `open_positions` for rich formatting. `open_positions`
    is a list of dicts with keys: symbol, direction, entry_price, stop_loss,
    target_price, qty.
    """
    short_label, date_label = _parse_session_label(session_id)
    subject = f"⚠️ Feed Stale — {short_label} · {date_label}"

    body_lines = [f"📡 <b>Feed stale</b> · {short_label}"]

    if last_tick_ts is not None:
        try:
            tick_ist = last_tick_ts.astimezone(_IST)
            elapsed_min = max(0, int((datetime.now(_IST) - tick_ist).total_seconds() / 60))
            body_lines.append(
                f"Last data: <code>{tick_ist.strftime('%H:%M IST')}</code>  ({elapsed_min} min ago)"
            )
        except Exception:
            pass

    if details:
        body_lines.append(details)

    if open_positions:
        body_lines.append("\n⚡ <b>Open positions</b> — place manual SL orders now:")
        for pos in open_positions:
            sym = str(pos.get("symbol", ""))
            dirn = str(pos.get("direction", ""))
            entry = float(pos.get("entry_price") or 0)
            sl = float(pos.get("stop_loss") or 0)
            tgt = float(pos.get("target_price") or 0)
            qty = int(cast(int | float | str, pos.get("qty") or 0))
            sl_pct = abs(entry - sl) / entry * 100 if entry else 0
            icon = "🟢" if dirn == "LONG" else "🔴"
            body_lines.append(
                f"  {icon} <code>{sym}</code>  {dirn}"
                f"  Entry <code>₹{entry:,.2f}</code>"
                f"  SL <code>₹{sl:,.2f}</code> ({sl_pct:.2f}%)"
                f"  Tgt <code>₹{tgt:,.2f}</code>"
                f"  Qty {qty:,}"
            )
    elif open_positions is not None:
        body_lines.append("No open positions.")

    _dispatch_alert(AlertType.FEED_STALE, subject, "\n".join(body_lines))


def dispatch_feed_recovered_alert(
    *,
    session_id: str,
    stale_minutes: int | None = None,
    open_count: int | None = None,
    details: str | None = None,
) -> None:
    """Emit a FEED_RECOVERED alert when market data resumes after a stale period."""
    short_label, date_label = _parse_session_label(session_id)
    subject = f"✅ Feed Recovered — {short_label} · {date_label}"

    body_lines = [f"✅ <b>Feed recovered</b> · {short_label}"]

    parts: list[str] = []
    if stale_minutes is not None and stale_minutes > 0:
        parts.append(f"Stale for ~{stale_minutes} min")
    if open_count is not None:
        pos_word = "position" if open_count == 1 else "positions"
        parts.append(f"monitoring {open_count} open {pos_word}")
    if parts:
        body_lines.append(". ".join(p.capitalize() for p in parts) + ".")
    elif details:
        body_lines.append(details)

    _dispatch_alert(AlertType.FEED_RECOVERED, subject, "\n".join(body_lines))


def dispatch_session_started_alert(
    *,
    session_id: str,
    strategy: str,
    direction: str,
    symbol_count: int,
    trade_date: str,
) -> None:
    """Emit a SESSION_STARTED alert when a live session becomes ACTIVE."""
    if session_id in _session_started_sent:
        return
    short_label, date_label = _parse_session_label(session_id)
    icon = "🟢" if direction.upper() == "LONG" else "🔴"
    subject = f"{icon} Session Started — {short_label} · {date_label}"
    now_str = datetime.now(_IST).strftime("%H:%M IST")
    body = (
        f"{icon} <b>Session started</b> · {short_label}\n"
        f"Strategy: <code>{strategy}</code>  Direction: <b>{direction}</b>\n"
        f"Symbols: {symbol_count:,}  Date: {trade_date}\n"
        f"Started: <code>{now_str}</code>\n"
        f"ID: <code>{session_id}</code>"
    )
    _session_started_sent.add(session_id)
    _dispatch_alert(AlertType.SESSION_STARTED, subject, body)


def dispatch_session_completed_alert(*, session_id: str) -> None:
    """Emit a SESSION_COMPLETED alert when a session ends normally."""
    short_label, date_label = _parse_session_label(session_id)
    now_str = datetime.now(_IST).strftime("%H:%M IST")
    subject = f"✅ Session Completed — {short_label} · {date_label}"
    body = (
        f"✅ <b>Session completed</b> · {short_label}\n"
        f"Completed: <code>{now_str}</code>\n"
        f"ID: <code>{session_id}</code>"
    )
    _dispatch_alert(AlertType.SESSION_COMPLETED, subject, body)


def _hhmm(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%H:%M")


def _reset_symbol_state_for_trade_date(
    state: SymbolRuntimeState,
    *,
    trade_date: str,
) -> None:
    if state.trade_date == trade_date:
        return
    keep_setup_row = (
        state.setup_row is not None and str(state.setup_row.get("trade_date") or "") == trade_date
    )
    state.trade_date = trade_date
    state.candles = []
    state.time_str = []
    state.opens = []
    state.highs = []
    state.lows = []
    state.closes = []
    state.volumes = []
    if not keep_setup_row:
        state.setup_row = None
        state.setup_refresh_bar_end = None
    state.position_closed_today = False
    state.entry_window_closed_without_trade = False


def _append_candle_to_symbol_state(state: SymbolRuntimeState, candle: Any) -> str:
    time_str = _hhmm(candle.bar_end)
    open_price = float(candle.open)
    high_price = float(candle.high)
    low_price = float(candle.low)
    close_price = float(candle.close)
    volume = float(candle.volume)

    if state.candles:
        last = state.candles[-1]
        last_time = str(last.get("time_str") or "")
        last_bar_end = last.get("bar_end")
        if last_time == str(time_str or "") and last_bar_end == candle.bar_end:
            last.update(
                {
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                }
            )
            state.opens[-1] = open_price
            state.highs[-1] = high_price
            state.lows[-1] = low_price
            state.closes[-1] = close_price
            state.volumes[-1] = volume
            return str(time_str or "")

    state.candles.append(
        {
            "time_str": time_str,
            "bar_end": candle.bar_end,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        }
    )
    if time_str is not None:
        state.time_str.append(time_str)
    state.opens.append(open_price)
    state.highs.append(high_price)
    state.lows.append(low_price)
    state.closes.append(close_price)
    state.volumes.append(volume)
    return str(time_str or "")


def _build_day_pack(state: SymbolRuntimeState) -> DayPack:
    rvol_baseline = state.setup_row.get("rvol_baseline") if state.setup_row else None
    return DayPack(
        time_str=state.time_str,
        opens=state.opens,
        highs=state.highs,
        lows=state.lows,
        closes=state.closes,
        volumes=state.volumes,
        rvol_baseline=rvol_baseline,
    )


def _build_symbol_price_map(feed_state: FeedState | None) -> dict[str, float]:
    raw_state = getattr(feed_state, "raw_state", None)
    if isinstance(raw_state, str):
        try:
            raw_state = json.loads(raw_state)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw_state, dict):
        return {}
    symbol_prices = raw_state.get("symbol_last_prices")
    if not isinstance(symbol_prices, dict):
        return {}
    return {
        str(symbol): price
        for symbol, value in symbol_prices.items()
        if (price := _float_or_none(value)) is not None
    }


def _feed_quote_age_sec(feed_state: FeedState | None, now: datetime) -> float:
    last_event_ts = getattr(feed_state, "last_event_ts", None)
    if not isinstance(last_event_ts, datetime):
        return 0.0
    if last_event_ts.tzinfo is None and now.tzinfo is not None:
        last_event_ts = last_event_ts.replace(tzinfo=now.tzinfo)
    try:
        return max(0.0, (now - last_event_ts.astimezone(now.tzinfo)).total_seconds())
    except Exception:
        return 0.0


def _close_price_for_position(position: PaperPosition, price_map: dict[str, float]) -> float:
    return (
        price_map.get(position.symbol)
        or _float_or_none(getattr(position, "last_price", None))
        or float(position.entry_price)
    )


def _realized_pnl_for_close(
    position: PaperPosition,
    close_price: float,
    *,
    params: BacktestParams | None = None,
    qty: float | None = None,
) -> float:
    entry_price = float(position.entry_price)
    quantity = float(qty if qty is not None else position.quantity)
    direction = str(position.direction).upper()
    if direction == "LONG":
        gross_pnl = (close_price - entry_price) * quantity
    else:
        gross_pnl = (entry_price - close_price) * quantity
    cost = (
        (params or BacktestParams())
        .get_cost_model()
        .round_trip_cost(
            entry_price=entry_price,
            exit_price=close_price,
            qty=quantity,
            direction=direction,
        )
    )
    return round(gross_pnl - cost, 2)


def _remaining_position_qty(position: PaperPosition) -> float:
    current_qty = getattr(position, "current_qty", None)
    if current_qty is not None:
        return max(0.0, float(current_qty or 0.0))
    return max(0.0, float(getattr(position, "quantity", 0.0) or 0.0))


async def flatten_session_positions(
    session_id: str,
    *,
    notes: str | None = None,
    feed_state: FeedState | None = None,
    emit_summary: bool = True,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    session = await get_session(session_id)
    if session is None:
        return {"session_id": session_id, "missing": True}

    positions = await get_session_positions(session_id, statuses=["OPEN"])
    if feed_state is None:
        feed_state = await get_feed_state(session_id)
    price_map = _build_symbol_price_map(feed_state)
    params = build_backtest_params(session)

    closed: list[dict[str, Any]] = []
    total_realized = 0.0
    now_ist = datetime.now(tz=_IST)
    for position in positions:
        close_qty = _remaining_position_qty(position)
        if close_qty <= 0:
            continue
        close_price = _close_price_for_position(position, price_map)
        realized = _realized_pnl_for_close(position, close_price, params=params, qty=close_qty)
        total_realized += realized
        side = "SELL" if str(position.direction).upper() == "LONG" else "BUY"
        real_exit_meta: dict[str, Any] = {}
        if real_order_router is not None and real_order_router.enabled:
            real_qty = real_order_router.exit_quantity_for_position(position)
            real_exit_meta = await real_order_router.place_exit(
                session_id=session_id,
                symbol=position.symbol,
                direction=str(position.direction),
                position_id=position.position_id,
                quantity=real_qty,
                reference_price=close_price,
                quote_age_sec=_feed_quote_age_sec(feed_state, now_ist),
                role=f"manual_flatten:{notes or 'paper flatten'}",
                event_time=now_ist,
            )
        await append_order_event(
            session_id=session_id,
            symbol=position.symbol,
            side=side,
            requested_qty=close_qty,
            position_id=position.position_id,
            order_type="MARKET",
            request_price=close_price,
            fill_qty=close_qty,
            fill_price=close_price,
            status="FILLED",
            idempotency_key=build_order_idempotency_key(
                session_id=session_id,
                role=f"session_flatten:{notes or 'paper flatten'}",
                symbol=position.symbol,
                side=side,
                position_id=str(position.position_id),
            ),
            notes=notes or "paper flatten",
        )
        await update_position(
            position.position_id,
            status="CLOSED",
            current_qty=0.0,
            last_price=close_price,
            close_price=close_price,
            realized_pnl=realized,
            exit_reason="MANUAL_FLATTEN",
            closed_by="MANUAL_FLATTEN",
            closed_at=now_ist,
            trail_state={**(position.trail_state or {}), "close_reason": "MANUAL_FLATTEN"},
        )
        if real_exit_meta:
            await update_position(
                position.position_id,
                trail_state={
                    **(position.trail_state or {}),
                    **real_exit_meta,
                    "real_remaining_qty": 0,
                    "close_reason": "MANUAL_FLATTEN",
                },
            )
        # Dispatch individual TRADE_CLOSED alert so the user sees the position close
        # in Telegram immediately — not just in the EOD summary.
        try:
            subject, body = _format_close_alert(
                symbol=position.symbol,
                direction=str(position.direction),
                entry_price=float(position.entry_price or 0),
                close_price=close_price,
                reason="AUTO_FLATTEN",
                realized_pnl=realized,
                strategy=str(getattr(position, "opened_by", "")),
                session_id=str(position.session_id),
                event_time=now_ist,
            )
            _dispatch_alert(AlertType.TRADE_CLOSED, subject, body)
        except Exception:
            logger.warning(
                "Alert dispatch for flatten trade %s failed (best-effort)",
                position.symbol,
                exc_info=True,
            )
        closed.append(
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "close_price": close_price,
            }
        )

    await update_session_state(session_id, status="STOPPING", notes=notes)
    # Always send EOD summary — even if no positions needed force-closing
    # (they may have exited earlier via SL/target).
    # Guard: only fire once per session_id (multiple processes / resume runs can
    # each call flatten_session_positions — only the first should send the alert).
    #
    # NOTE: all_closed is fetched AFTER update_position commits above, so the
    # just-flattened positions are already included. Do NOT add len(closed) again
    # (double-count) and always sum from all_closed so the EOD P&L reflects the
    # full session, not just the force-closed subset.
    all_closed = await get_session_positions(session_id, statuses=["CLOSED"])
    total_trades = len(all_closed)
    total_realized = sum(float(p.realized_pnl or 0) for p in all_closed)
    # Stamp total_pnl on the session row so the dashboard shows the correct P&L
    # for all exit paths (normal EOD, flatten, flatten-all, auto-flatten on stale).
    await update_session_state(session_id, total_pnl=round(total_realized, 2))
    try:
        if emit_summary:
            # Skip if nothing to report — zero-trade restart sessions after a FAILED original
            # should not send a second EOD with 0 trades, 0 PnL.
            if total_trades == 0 and not closed:
                logger.debug("FLATTEN_EOD skipped for session %s — no trades to report", session_id)
            elif session_id in _flatten_eod_sent:
                # Synchronous in-memory dedup — set before dispatch so concurrent callers
                # can't race past the check before alert_log is written.
                logger.debug(
                    "FLATTEN_EOD already sent for session %s — skipping duplicate", session_id
                )
            else:
                # Persisted alert log dedupe protects against duplicate EOD summaries
                # across a restart, while _flatten_eod_sent protects this process.
                if _has_flatten_eod_in_alert_log(
                    session_id, trade_date=getattr(session, "trade_date", None)
                ):
                    _flatten_eod_sent.add(session_id)
                    logger.debug(
                        "FLATTEN_EOD already persisted for session %s — skipping duplicate",
                        session_id,
                    )
                else:
                    _flatten_eod_sent.add(session_id)
                    subject, body = _format_risk_alert(
                        reason=notes or "session flatten",
                        net_pnl=total_realized,
                        session_id=session_id,
                        positions_closed=len(closed),
                        total_trades=total_trades,
                        trade_date=getattr(session, "trade_date", None),
                    )
                    _dispatch_alert(AlertType.FLATTEN_EOD, subject, body)
    except Exception:
        logger.warning("Alert dispatch for flatten failed (best-effort)", exc_info=True)
    return {"session_id": session_id, "closed_positions": len(closed), "positions": closed}


async def flatten_positions_subset(
    session_id: str,
    symbols: list[str],
    *,
    notes: str | None = None,
    feed_state: FeedState | None = None,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    """Close specific open positions without stopping the session.

    Unlike flatten_session_positions, this does not set status=STOPPING and does
    not dispatch FLATTEN_EOD — the session keeps running with remaining positions.
    Caller is responsible for syncing the in-memory tracker after this returns.
    """
    if not symbols:
        return {"session_id": session_id, "closed_positions": 0, "positions": []}

    symbol_set = {str(s).upper() for s in symbols}
    session = await get_session(session_id)
    if session is None:
        return {"session_id": session_id, "missing": True}

    all_open = await get_session_positions(session_id, statuses=["OPEN"])
    positions = [p for p in all_open if str(p.symbol).upper() in symbol_set]
    if not positions:
        return {"session_id": session_id, "closed_positions": 0, "positions": []}

    if feed_state is None:
        feed_state = await get_feed_state(session_id)
    price_map = _build_symbol_price_map(feed_state)
    params = build_backtest_params(session)

    closed: list[dict[str, Any]] = []
    now_ist = datetime.now(tz=_IST)
    for position in positions:
        close_qty = _remaining_position_qty(position)
        if close_qty <= 0:
            continue
        close_price = _close_price_for_position(position, price_map)
        realized = _realized_pnl_for_close(position, close_price, params=params, qty=close_qty)
        side = "SELL" if str(position.direction).upper() == "LONG" else "BUY"
        real_exit_meta: dict[str, Any] = {}
        if real_order_router is not None and real_order_router.enabled:
            real_qty = real_order_router.exit_quantity_for_position(position)
            real_exit_meta = await real_order_router.place_exit(
                session_id=session_id,
                symbol=position.symbol,
                direction=str(position.direction),
                position_id=position.position_id,
                quantity=real_qty,
                reference_price=close_price,
                quote_age_sec=_feed_quote_age_sec(feed_state, now_ist),
                role=f"close:{notes or 'partial flatten'}",
                event_time=now_ist,
            )
        await append_order_event(
            session_id=session_id,
            symbol=position.symbol,
            side=side,
            requested_qty=close_qty,
            position_id=position.position_id,
            order_type="MARKET",
            request_price=close_price,
            fill_qty=close_qty,
            fill_price=close_price,
            status="FILLED",
            idempotency_key=build_order_idempotency_key(
                session_id=session_id,
                role=f"position_flatten:{notes or 'partial flatten'}",
                symbol=position.symbol,
                side=side,
                position_id=str(position.position_id),
            ),
            notes=notes or "partial flatten",
        )
        await update_position(
            position.position_id,
            status="CLOSED",
            current_qty=0.0,
            last_price=close_price,
            close_price=close_price,
            realized_pnl=realized,
            exit_reason="MANUAL_CLOSE",
            closed_by="MANUAL_CLOSE",
            closed_at=now_ist,
            trail_state={**(position.trail_state or {}), "close_reason": "MANUAL_CLOSE"},
        )
        if real_exit_meta:
            await update_position(
                position.position_id,
                trail_state={
                    **(position.trail_state or {}),
                    **real_exit_meta,
                    "real_remaining_qty": 0,
                    "close_reason": "MANUAL_CLOSE",
                },
            )
        try:
            subject, body = _format_close_alert(
                symbol=position.symbol,
                direction=str(position.direction),
                entry_price=float(position.entry_price or 0),
                close_price=close_price,
                reason="MANUAL_CLOSE",
                realized_pnl=realized,
                strategy=str(getattr(position, "opened_by", "")),
                session_id=str(position.session_id),
                event_time=now_ist,
            )
            _dispatch_alert(AlertType.TRADE_CLOSED, subject, body)
        except Exception:
            logger.warning(
                "Alert dispatch for partial flatten %s failed", position.symbol, exc_info=True
            )
        closed.append(
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "close_price": close_price,
            }
        )

    return {"session_id": session_id, "closed_positions": len(closed), "positions": closed}


async def enforce_session_risk_controls(
    *,
    session: Any,
    as_of: datetime,
    feed_state: FeedState | None = None,
    notes_prefix: str = "paper risk",
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    positions = await get_session_positions(session.session_id)
    summary = summarize_paper_positions(session, positions, feed_state)
    realized_pnl = float(summary.get("realized_pnl", 0.0) or 0.0)
    risk_pnl = float(summary.get("net_pnl", realized_pnl) or 0.0)
    await update_session_state(session.session_id, daily_pnl_used=risk_pnl)

    reasons = _risk_limit_reasons(session, as_of, risk_pnl)
    if not reasons:
        return {"triggered": False, "daily_pnl_used": risk_pnl, "reasons": []}

    flatten_kwargs: dict[str, Any] = {
        "notes": f"{notes_prefix}: {', '.join(reasons)}",
        "feed_state": feed_state,
    }
    if real_order_router is not None:
        flatten_kwargs["real_order_router"] = real_order_router
    flatten_result = await flatten_session_positions(session.session_id, **flatten_kwargs)
    # flatten_session_positions already dispatches the EOD summary alert.
    # No need to send a second DAILY_LOSS_LIMIT alert here.
    return {
        "triggered": True,
        "daily_pnl_used": risk_pnl,
        "reasons": reasons,
        "flatten": flatten_result,
    }


async def _open_position_from_candidate(
    *,
    session: PaperSession,
    symbol: str,
    candidate: dict[str, Any],
    setup_row: dict[str, Any],
    params: BacktestParams,
    now: datetime,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    real_entry_meta: dict[str, Any] = {}
    if real_order_router is not None and real_order_router.enabled:
        real_entry_meta = await real_order_router.place_entry(
            session_id=session.session_id,
            symbol=symbol,
            direction=str(candidate["direction"]),
            reference_price=float(candidate["entry_price"]),
            event_time=candidate.get("event_time") or now,
        )
    position = await open_position(
        session_id=session.session_id,
        symbol=symbol,
        direction=candidate["direction"],
        quantity=float(candidate["position_size"]),
        entry_price=float(candidate["entry_price"]),
        stop_loss=float(candidate["sl_price"]),
        target_price=float(candidate.get("runner_target_price") or candidate["target_price"]),
        trail_state={
            "entry_price": float(candidate["entry_price"]),
            "direction": candidate["direction"],
            "initial_sl": float(candidate["sl_price"]),
            "current_sl": float(candidate["sl_price"]),
            "atr": float(setup_row["atr"]),
            "trail_atr_multiplier": float(
                params.short_trail_atr_multiplier
                if str(candidate["direction"]).upper() == "SHORT"
                else params.trail_atr_multiplier
            ),
            "rr_ratio": float(candidate["rr_ratio"]),
            "breakeven_r": float(params.breakeven_r),
            "phase": "PROTECT",
            "highest_since_entry": float(candidate["entry_price"]),
            "lowest_since_entry": float(candidate["entry_price"]),
            "entry_time": candidate["entry_time"],
            "first_target_price": float(
                candidate.get("first_target_price") or candidate["target_price"]
            ),
            "runner_target_price": (
                float(candidate["runner_target_price"])
                if candidate.get("runner_target_price") is not None
                else None
            ),
            "scale_out_pct": float(candidate.get("scale_out_pct") or 0.0),
            "scaled_out": False,
            "initial_qty": float(candidate["position_size"]),
            "candle_count": 0,
            **real_entry_meta,
        },
        opened_by=str(getattr(session, "strategy", "") or "paper_runtime"),
        opened_at=now,
    )
    await append_order_event(
        session_id=session.session_id,
        symbol=symbol,
        side="BUY" if candidate["direction"] == "LONG" else "SELL",
        requested_qty=float(candidate["position_size"]),
        position_id=position.position_id,
        order_type="MARKET",
        request_price=float(candidate["entry_price"]),
        fill_qty=float(candidate["position_size"]),
        fill_price=float(candidate["entry_price"]),
        status="FILLED",
        idempotency_key=build_order_idempotency_key(
            session_id=session.session_id,
            role="entry",
            symbol=symbol,
            side="BUY" if candidate["direction"] == "LONG" else "SELL",
            position_id=str(position.position_id),
            event_time=str(candidate.get("event_time") or now),
        ),
        notes="paper entry",
    )
    logger.info(
        "Paper trade open session_id=%s symbol=%s direction=%s time=%s entry=%.2f sl=%.2f target=%.2f rr=%.2f qty=%s",
        session.session_id,
        symbol,
        candidate["direction"],
        _format_event_time(candidate.get("event_time")),
        float(candidate["entry_price"]),
        float(candidate["sl_price"]),
        float(candidate["target_price"]),
        float(candidate["rr_ratio"]),
        float(candidate["position_size"]),
    )
    try:
        subject, body = _format_open_alert(
            symbol=symbol,
            direction=candidate["direction"],
            entry_price=candidate["entry_price"],
            sl_price=candidate["sl_price"],
            target_price=candidate["target_price"],
            sl_distance=candidate["sl_distance"],
            position_size=candidate["position_size"],
            rr_ratio=candidate["rr_ratio"],
            strategy=str(getattr(session, "strategy", "")),
            session_id=session.session_id,
            event_time=candidate.get("event_time"),
        )
        _dispatch_alert(AlertType.TRADE_OPENED, subject, body)
    except Exception:
        logger.warning("Alert dispatch for trade open failed (best-effort)", exc_info=True)
    return {
        "action": "OPEN",
        "position_id": position.position_id,
        "symbol": symbol,
        "entry_price": candidate["entry_price"],
        "executed_qty": float(candidate["position_size"]),
        "position": position,
    }


async def _advance_open_position(
    *,
    position: PaperPosition,
    candle: dict[str, Any],
    params: BacktestParams,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    trail_state = dict(position.trail_state or {})
    current_qty = float(position.current_qty or position.quantity or 0.0)
    realized_so_far = float(position.realized_pnl or 0.0)
    scale_out_pct = float(trail_state.get("scale_out_pct") or 0.0)
    scaled_out = bool(trail_state.get("scaled_out") or False)
    first_target_price = float(
        trail_state.get("first_target_price") or position.target_price or position.entry_price
    )
    final_target_price = float(
        position.target_price or trail_state.get("target_price") or first_target_price
    )
    scale_split = (
        split_scale_out_quantity(
            round(float(trail_state.get("initial_qty") or position.quantity or 0.0)), scale_out_pct
        )
        if scale_out_pct > 0 and not scaled_out
        else None
    )
    ts = _get_trailing_stop(position, params, trail_state=trail_state)
    candle_count = int(trail_state.get("candle_count") or 0) + 1

    mark_price = float(candle["close"])
    direction = position.direction.upper()
    momentum_confirm = bool(getattr(params.cpr_levels, "momentum_confirm", False))
    if bool(trail_state.get("momentum_exit_pending") or False):
        decision = CompletedCandleDecision(
            action="CLOSE",
            exit_reason="MOMENTUM_FAIL",
            fills=((current_qty, float(candle["open"])),),
        )
    else:
        if momentum_confirm and candle_count == 1:
            if (direction == "LONG" and mark_price < float(position.entry_price)) or (
                direction == "SHORT" and mark_price > float(position.entry_price)
            ):
                trail_state["momentum_exit_pending"] = True
        decision = resolve_completed_candle_trade_step(
            ts=ts,
            direction=direction,
            low=float(candle["low"]),
            high=float(candle["high"]),
            close=mark_price,
            time_str=_hhmm(candle["bar_end"]) or "",
            time_exit=params.time_exit,
            target_price=first_target_price if scale_out_pct > 0 else final_target_price,
            current_qty=current_qty,
            candle_count=candle_count,
            candle_exit=0,
            runner_target_price=final_target_price if scale_out_pct > 0 else None,
            scale_out_pct=scale_out_pct,
            scale_split=scale_split,
            scaled_out=scaled_out,
        )

    pre_update_trail_state = _updated_trail_state(ts, trail_state, candle)
    pre_update_trail_state["candle_count"] = candle_count

    async def _record_exit_fill(qty: float, price: float, *, notes: str) -> float:
        realized = _realized_pnl_for_close(position, price, params=params, qty=qty)
        await append_order_event(
            session_id=position.session_id,
            symbol=position.symbol,
            side="SELL" if direction == "LONG" else "BUY",
            requested_qty=qty,
            position_id=position.position_id,
            order_type="MARKET",
            request_price=price,
            fill_qty=qty,
            fill_price=price,
            status="FILLED",
            idempotency_key=build_order_idempotency_key(
                session_id=position.session_id,
                role=notes,
                symbol=position.symbol,
                side="SELL" if direction == "LONG" else "BUY",
                position_id=str(position.position_id),
                event_time=str(candle.get("bar_end") or ""),
            ),
            notes=notes,
        )
        return realized

    if decision.action == "HOLD":
        next_trail_state = _updated_trail_state(ts, trail_state, candle)
        next_trail_state["candle_count"] = candle_count
        await update_position(
            position.position_id,
            stop_loss=float(ts.current_sl),
            trail_state=next_trail_state,
            current_qty=current_qty,
            last_price=float(candle["close"]),
        )
        return {
            "action": "HOLD",
            "position_id": position.position_id,
            "mark": float(candle["close"]),
            "next_trail_state": next_trail_state,
        }

    if decision.action == "PARTIAL" and real_order_router is not None and real_order_router.enabled:
        next_trail_state = _updated_trail_state(ts, trail_state, candle)
        next_trail_state["candle_count"] = candle_count
        logger.warning(
            "Real-order partial scale-out not supported for %s in session %s; deferring to full close",
            position.symbol,
            position.session_id,
        )
        await update_position(
            position.position_id,
            stop_loss=float(ts.current_sl),
            trail_state=next_trail_state,
            current_qty=current_qty,
            last_price=float(candle["close"]),
        )
        return {
            "action": "HOLD",
            "position_id": position.position_id,
            "mark": float(candle["close"]),
            "next_trail_state": next_trail_state,
        }

    real_exit_meta: dict[str, Any] = {}
    if decision.action == "CLOSE" and real_order_router is not None and real_order_router.enabled:
        real_qty = real_order_router.exit_quantity_for_position(position)
        exit_price_for_order = float(decision.fills[-1][1] if decision.fills else mark_price)
        real_exit_meta = await real_order_router.place_exit(
            session_id=position.session_id,
            symbol=position.symbol,
            direction=position.direction,
            position_id=position.position_id,
            quantity=real_qty,
            reference_price=exit_price_for_order,
            role=f"exit:{decision.exit_reason or 'TIME'}",
            event_time=candle.get("bar_end"),
        )

    fill_total_qty = 0.0
    fill_total_value = 0.0
    realized = realized_so_far
    for qty, price in decision.fills:
        realized += await _record_exit_fill(
            qty, price, notes=f"paper exit:{decision.exit_reason or 'TIME'}"
        )
        fill_total_qty += float(qty)
        fill_total_value += float(qty) * float(price)

    close_price = round(fill_total_value / fill_total_qty, 4) if fill_total_qty > 0 else mark_price
    exit_reason = decision.exit_reason or "TIME"
    exit_payload = {
        "exit_reason": exit_reason,
        "scaled_out": decision.action == "PARTIAL",
    }
    if decision.action == "PARTIAL":
        remaining_qty = max(current_qty - float(decision.fills[0][0]), 0.0)
        next_trail_state = _updated_trail_state(ts, trail_state, candle)
        next_trail_state["candle_count"] = candle_count
        next_trail_state = {
            **next_trail_state,
            "current_sl": float(ts.current_sl),
            "phase": ts.phase,
            "scaled_out": True,
        }
        await update_position(
            position.position_id,
            stop_loss=float(ts.current_sl),
            target_price=final_target_price,
            trail_state=next_trail_state,
            current_qty=remaining_qty,
            last_price=float(candle["close"]),
            realized_pnl=realized,
        )
        position.current_qty = remaining_qty
        position.realized_pnl = realized
        await _accumulate_session_pnl(position.session_id, realized - realized_so_far)
        logger.info(
            "Paper trade partial session_id=%s symbol=%s direction=%s first_exit=%.2f runner_exit=%.2f pnl=%.2f bars=%d",
            position.session_id,
            position.symbol,
            position.direction,
            decision.fills[0][1],
            final_target_price,
            realized,
            candle_count,
        )
        return {
            "action": "PARTIAL",
            "position_id": position.position_id,
            "mark": float(candle["close"]),
            "symbol": position.symbol,
            "partial_qty": float(decision.fills[0][0]),
            "partial_exit_price": float(decision.fills[0][1]),
            "exit_value": _exit_value_for_position(
                position,
                float(decision.fills[0][0]),
                float(decision.fills[0][1]),
            ),
            "remaining_qty": remaining_qty,
            "realized_pnl": realized,
            "next_trail_state": next_trail_state,
        }

    await update_position(
        position.position_id,
        status="CLOSED",
        stop_loss=float(ts.current_sl),
        trail_state={
            **pre_update_trail_state,
            **exit_payload,
            **real_exit_meta,
            "real_remaining_qty": 0,
        }
        if real_exit_meta
        else {**pre_update_trail_state, **exit_payload},
        current_qty=0.0,
        last_price=close_price,
        close_price=close_price,
        realized_pnl=realized,
        exit_reason=exit_reason,
        closed_by=exit_reason,
        closed_at=candle.get("bar_end") if isinstance(candle, dict) else None,
    )
    # Update session total_pnl so the dashboard shows live PnL during trading.
    await _accumulate_session_pnl(position.session_id, realized - realized_so_far)
    logger.info(
        "Paper trade close session_id=%s symbol=%s direction=%s time=%s reason=%s exit=%.2f pnl=%.2f bars=%d",
        position.session_id,
        position.symbol,
        position.direction,
        _format_event_time(candle.get("bar_end") if isinstance(candle, dict) else None),
        exit_reason,
        close_price,
        realized,
        candle_count,
    )
    try:
        subject, body = _format_close_alert(
            symbol=position.symbol,
            direction=position.direction,
            entry_price=float(position.entry_price),
            close_price=close_price,
            reason=exit_reason,
            realized_pnl=realized,
            duration_bars=candle_count,
            strategy=str(getattr(position, "opened_by", "")),
            session_id=position.session_id,
            event_time=candle.get("bar_end") if isinstance(candle, dict) else None,
        )
        _dispatch_alert(
            AlertType.SL_HIT if "SL" in exit_reason else AlertType.TRADE_CLOSED,
            subject,
            body,
        )
    except Exception:
        logger.warning("Alert dispatch for resolved exit failed (best-effort)", exc_info=True)
    return {
        "action": "CLOSE",
        "position_id": position.position_id,
        "symbol": position.symbol,
        "reason": exit_reason,
        "close_price": close_price,
        "closed_qty": current_qty,
        "exit_value": _exit_value_for_position(position, current_qty, close_price),
    }


async def evaluate_candle(
    *,
    session: Any,
    candle: Any,
    runtime_state: PaperRuntimeState,
    now: datetime,
    position_tracker: SessionPositionTracker,
    allow_entry_evaluation: bool = True,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    state = runtime_state.for_symbol(candle.symbol)
    params = runtime_state.get_session_params(session)
    trade_date = candle.bar_end.date().isoformat()
    _reset_symbol_state_for_trade_date(state, trade_date=trade_date)
    candle_time = _append_candle_to_symbol_state(state, candle)

    setup_status = _live_setup_status(state.setup_row)
    needs_setup_refresh = state.setup_row is None or setup_status == "pending"
    if needs_setup_refresh and state.setup_refresh_bar_end != candle.bar_end:
        # Keep setup-row reads synchronous in the event loop. This avoids
        # thread contention/crashes on shared DuckDB connections.
        setup_row = load_setup_row(
            candle.symbol,
            trade_date,
            live_candles=state.candles,
            or_minutes=params.or_minutes,
            allow_live_fallback=runtime_state.allow_live_setup_fallback,
            bar_end_offset=runtime_state.bar_end_offset,
            regime_index_symbol=getattr(params, "regime_index_symbol", ""),
            regime_snapshot_minutes=int(getattr(params, "regime_snapshot_minutes", 30) or 30),
        )
        if setup_row is not None:
            if runtime_state.allow_or_proxy_setup or not setup_row_uses_or_proxy(setup_row):
                state.setup_row = setup_row
        state.setup_refresh_bar_end = candle.bar_end
    setup_status = _live_setup_status(state.setup_row)
    if state.setup_row is None:
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "no_setup",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    tracked_position = position_tracker.get_open_position(candle.symbol)
    if tracked_position is not None:
        result = await _advance_open_position(
            position=tracked_position,
            candle=state.candles[-1],
            params=params,
            real_order_router=real_order_router,
        )
        if result["action"] == "CLOSE":
            state.position_closed_today = True
        elif result["action"] in ("HOLD", "PARTIAL"):
            # Keep in-memory trail_state in sync with the DB update inside
            # _advance_open_position so subsequent candles see accumulated state.
            nts = result.get("next_trail_state")
            if nts:
                position_tracker.update_trail_state(candle.symbol, nts)
        return {
            "symbol": candle.symbol,
            "action": "ADVANCE",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": result,
            "setup_row": state.setup_row,
        }

    if state.position_closed_today:
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "already_traded_today",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    entry_end = str(params.entry_window_end)
    if candle_time and candle_time > entry_end:
        state.entry_window_closed_without_trade = True
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "entry_window_closed",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }
    if state.entry_window_closed_without_trade:
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "entry_window_closed",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    if setup_status == "pending":
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "setup_pending",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    if not allow_entry_evaluation:
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "entry_evaluation_deferred",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    strategy = str(session.strategy or "").upper()
    if strategy != "CPR_LEVELS":
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": f"unsupported_strategy:{strategy or 'UNKNOWN'}",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    candidate = _maybe_open_cpr_levels(
        candle=state.candles[-1],
        day_pack=_build_day_pack(state),
        setup_row=state.setup_row,
        params=params,
        capital_base=(
            float(
                position_tracker.current_equity()
                if hasattr(position_tracker, "current_equity")
                else getattr(position_tracker, "initial_capital", position_tracker.cash_available)
            )
            if params.compound_equity
            else None
        ),
    )
    if candidate is None:
        return {
            "symbol": candle.symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": setup_status,
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    candidate_with_symbol = {**candidate, "symbol": candle.symbol}
    return {
        "symbol": candle.symbol,
        "action": "ENTRY_CANDIDATE",
        "setup_status": setup_status,
        "candidate": candidate_with_symbol,
        "advance_result": None,
        "setup_row": state.setup_row,
    }


async def execute_entry(
    *,
    session: Any,
    candidate: dict[str, Any],
    setup_row: dict[str, Any],
    params: BacktestParams,
    now: datetime,
    position_tracker: SessionPositionTracker,
    real_order_router: RealOrderRouter | None = None,
) -> dict[str, Any]:
    symbol = str(candidate.get("symbol") or "")
    if not symbol:
        return {"action": "SKIP", "reason": "missing_symbol"}

    candidate_to_open = dict(candidate)
    entry_price = float(candidate_to_open.get("entry_price") or 0.0)
    executable_qty = position_tracker.compute_position_qty(
        entry_price=entry_price,
        risk_based_sizing=bool(params.risk_based_sizing),
        candidate_size=int(candidate_to_open.get("position_size") or 0),
        capital_base=(
            float(
                position_tracker.current_equity()
                if hasattr(position_tracker, "current_equity")
                else getattr(position_tracker, "initial_capital", position_tracker.cash_available)
            )
            if params.compound_equity
            else None
        ),
    )
    if executable_qty < 1:
        return {"action": "SKIP", "reason": "no_cash", "symbol": symbol}
    candidate_to_open["position_size"] = executable_qty

    result = await _open_position_from_candidate(
        session=session,
        symbol=symbol,
        candidate=candidate_to_open,
        setup_row=setup_row,
        params=params,
        now=now,
        real_order_router=real_order_router,
    )
    if result.get("action") == "OPEN":
        position = result.get("position")
        if position is not None:
            qty = float(candidate_to_open.get("position_size") or 0.0)
            position_tracker.record_open(position, qty * entry_price)
    return result


async def process_closed_candle(
    *,
    session: Any,
    candle: Any,
    runtime_state: PaperRuntimeState,
    now: datetime,
    position_tracker: SessionPositionTracker,
) -> dict[str, Any]:
    evaluation = await evaluate_candle(
        session=session,
        candle=candle,
        runtime_state=runtime_state,
        now=now,
        position_tracker=position_tracker,
        allow_entry_evaluation=True,
    )
    setup_status = str(evaluation.get("setup_status") or "pending")
    action = str(evaluation.get("action") or "SKIP")

    if action == "ADVANCE":
        advance = evaluation.get("advance_result") or {}
        if advance.get("action") == "CLOSE":
            position_tracker.record_close(
                str(candle.symbol),
                float(advance.get("exit_value") or 0.0),
            )
        elif advance.get("action") == "PARTIAL":
            position_tracker.record_partial(
                str(candle.symbol),
                float(advance.get("exit_value") or 0.0),
                float(advance.get("remaining_qty") or 0.0),
                float(advance["realized_pnl"]) if advance.get("realized_pnl") is not None else None,
            )
        return {
            "symbol": candle.symbol,
            "opened": 0,
            "closed": 1 if advance.get("action") == "CLOSE" else 0,
            "setup_status": setup_status,
            "result": advance,
        }

    if action == "ENTRY_CANDIDATE":
        params = runtime_state.get_session_params(session)
        if not position_tracker.can_open_new():
            return {
                "symbol": candle.symbol,
                "opened": 0,
                "closed": 0,
                "reason": "max_positions",
                "setup_status": setup_status,
            }

        execute_result = await execute_entry(
            session=session,
            candidate=dict(evaluation.get("candidate") or {}),
            setup_row=dict(evaluation.get("setup_row") or {}),
            params=params,
            now=now,
            position_tracker=position_tracker,
        )
        opened = 1 if execute_result.get("action") == "OPEN" else 0
        payload = {
            "symbol": candle.symbol,
            "opened": opened,
            "closed": 0,
            "setup_status": setup_status,
            "result": execute_result,
        }
        if opened == 0 and execute_result.get("reason"):
            payload["reason"] = str(execute_result.get("reason"))
        return payload

    return {
        "symbol": candle.symbol,
        "opened": 0,
        "closed": 0,
        "reason": str(evaluation.get("reason") or "skip"),
        "setup_status": setup_status,
    }


def _maybe_open_cpr_levels(
    *,
    candle: dict[str, Any],
    day_pack: DayPack,
    setup_row: dict[str, Any],
    params: BacktestParams,
    capital_base: float | None = None,
) -> dict[str, Any] | None:
    del candle
    current_idx = len(day_pack.time_str) - 1
    if current_idx < 0:
        return None
    # Evaluate only the current bar. Historical bars are not re-scanned after
    # rejection, matching backtest's "signal exists only on its triggering bar".
    return scan_cpr_levels_entry(
        day_pack=day_pack,
        setup_row=setup_row,
        params=params,
        scan_start_idx=current_idx,
        scan_end_idx=current_idx,
        capital_base=capital_base,
    )


__all__ = [
    "_MARKET_DB_READ_LOCK",
    "PaperRuntimeState",
    "SymbolRuntimeState",
    "_build_intraday_summary",
    "apply_paper_strategy_defaults",
    "build_backtest_params",
    "build_backtest_params_from_overrides",
    "build_summary_feed_state",
    "dispatch_session_completed_alert",
    "dispatch_session_error_alert",
    "dispatch_session_started_alert",
    "dispatch_session_state_alert",
    "enforce_session_risk_controls",
    "evaluate_candle",
    "execute_entry",
    "flatten_positions_subset",
    "flatten_session_positions",
    "load_setup_row",
    "mark_price_for_position",
    "maybe_shutdown_alert_dispatcher",
    "process_closed_candle",
    "refresh_pending_setup_rows_for_bar",
    "register_session_start",
    "reset_alert_dedupe",
    "runtime_setup_status",
    "set_alert_sink",
    "set_alerts_suppressed",
    "summarize_paper_positions",
    "write_admin_command",
]
