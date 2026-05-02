"""Shared helpers for paper-trading runtime state and summaries."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
from collections.abc import Callable
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import Any, cast
from zoneinfo import ZoneInfo

from db.duckdb import get_dashboard_db
from db.paper_db import FeedState, PaperPosition, PaperSession
from engine.alert_dispatcher import AlertDispatcher, AlertType, get_alert_config
from engine.bar_orchestrator import SessionPositionTracker
from engine.cpr_atr_shared import (
    CompletedCandleDecision,
    normalize_stop_loss,
    regime_snapshot_close_col,
    resolve_completed_candle_trade_step,
    scan_cpr_levels_entry,
    split_scale_out_quantity,
)
from engine.cpr_atr_strategy import DayPack
from engine.cpr_atr_utils import (
    calculate_gap_pct,
    calculate_or_atr_ratio,
    calculate_position_size,
    normalize_cpr_bounds,
    resolve_cpr_direction,
)
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
        pattern = f"%{session_id}%"
        if trade_date:
            row = con.execute(
                """
                SELECT 1
                FROM alert_log
                WHERE alert_type='FLATTEN_EOD'
                  AND (subject LIKE ? OR body LIKE ?)
                  AND (subject LIKE ? OR body LIKE ?)
                LIMIT 1
                """,
                [pattern, pattern, f"%{trade_date}%", f"%{trade_date}%"],
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT 1
                FROM alert_log
                WHERE alert_type='FLATTEN_EOD'
                  AND (subject LIKE ? OR body LIKE ?)
                LIMIT 1
                """,
                [pattern, pattern],
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


_MARKET_DB_READ_LOCK = (
    threading.RLock()
)  # serializes market.duckdb reads across threads (reentrant)


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
            _db().log_alert(str(alert_type.value), subject, body, status="skipped_no_loop")
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
    _dispatch_alert(AlertType.SESSION_STARTED, subject, body)
    _session_started_sent.add(session_id)


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


def _build_intraday_summary(
    candles: list[dict[str, Any]],
    *,
    or_minutes: int,
    bar_end_offset: timedelta | None = None,
) -> dict[str, float | bool | None]:
    if not candles:
        return {
            "open_915": None,
            "or_high_5": None,
            "or_low_5": None,
            "or_close_5": None,
            "or_proxy": False,
        }
    first = candles[0]
    first_bar_end = first["bar_end"]
    range_end = datetime.combine(
        first_bar_end.date(),
        dt_time(9, 15, tzinfo=getattr(first_bar_end, "tzinfo", None)),
    ) + timedelta(minutes=max(1, int(or_minutes or 5)))
    # bar_end_offset corrects for pack-convention candles where bar_end stores bar_start
    # time (e.g. "09:15" stored as bar_end for the 09:15-09:20 candle).  Live Kite candles
    # already carry the true close time so offset defaults to zero.
    _beo = bar_end_offset or timedelta(0)
    window = [candle for candle in candles if candle["bar_end"] + _beo <= range_end]
    if not window:
        # Late-start continuity mode: when the process starts after OR completion
        # and early bars are unavailable in-memory, synthesize OR from first seen bar.
        return {
            "open_915": _float_or_none(first.get("open")),
            "or_high_5": _float_or_none(first.get("high")),
            "or_low_5": _float_or_none(first.get("low")),
            "or_close_5": _float_or_none(first.get("close")),
            "or_proxy": True,
        }
    if candles[-1]["bar_end"] + _beo < range_end:
        return {
            "open_915": _float_or_none(first.get("open")),
            "or_high_5": None,
            "or_low_5": None,
            "or_close_5": None,
            "or_proxy": False,
        }
    last = window[-1]
    return {
        "open_915": _float_or_none(first.get("open")),
        "or_high_5": max(float(candle["high"]) for candle in window),
        "or_low_5": min(float(candle["low"]) for candle in window),
        "or_close_5": _float_or_none(last.get("close")),
        "or_proxy": False,
    }


def _load_live_setup_row(
    symbol: str,
    trade_date: str,
    live_candles: list[dict[str, Any]],
    *,
    or_minutes: int,
    bar_end_offset: timedelta | None = None,
) -> dict[str, Any] | None:
    # Removed early-return guard `if not live_candles: return None`.
    # CPR levels (tc/bc/pivot/r1/s1) and ATR are derived from v_daily + atr_intraday and
    # do not require candles. With empty candles, direction stays "NONE" (pending) and
    # or_close_5/open_915 are None; they resolve from the first live tick. This allows
    # the per-symbol fallback path to produce a partial setup row at session startup
    # instead of returning None and permanently skipping the symbol.
    # Note: this fallback is defense-in-depth. The primary fix is the EOD pipeline building
    # market_day_state rows so the batch prefetch succeeds (no per-symbol fallback needed).
    db = get_dashboard_db()
    with _MARKET_DB_READ_LOCK:
        setup_base = db.con.execute(
            """
            WITH prev_daily AS (
                SELECT date::VARCHAR AS prev_date, high, low, close
                FROM v_daily
                WHERE symbol = ? AND date < ?::DATE
                ORDER BY date DESC
                LIMIT 1
            ),
            prev_atr AS (
                SELECT trade_date::VARCHAR AS atr_prev_date, atr
                FROM atr_intraday
                WHERE symbol = ? AND trade_date < ?::DATE
                ORDER BY trade_date DESC
                LIMIT 1
            ),
            prev_threshold AS (
                SELECT trade_date::VARCHAR AS threshold_prev_date, cpr_threshold_pct
                FROM cpr_thresholds
                WHERE symbol = ? AND trade_date < ?::DATE
                ORDER BY trade_date DESC
                LIMIT 1
            )
            SELECT
                d.prev_date,
                d.high,
                d.low,
                d.close,
                a.atr_prev_date,
                a.atr,
                t.threshold_prev_date,
                t.cpr_threshold_pct
            FROM prev_daily d
            LEFT JOIN prev_atr a ON TRUE
            LEFT JOIN prev_threshold t ON TRUE
            """,
            [symbol, trade_date, symbol, trade_date, symbol, trade_date],
        ).fetchone()
    if not setup_base:
        return None

    prev_date = str(setup_base[0])
    prev_high = float(setup_base[1] or 0.0)
    prev_low = float(setup_base[2] or 0.0)
    prev_close = float(setup_base[3] or 0.0)
    pivot = (prev_high + prev_low + prev_close) / 3.0
    bc = (prev_high + prev_low) / 2.0
    tc = (pivot + bc) / 2.0
    cpr_lower, cpr_upper = normalize_cpr_bounds(tc, bc)
    cpr_width_pct = abs(tc - bc) / pivot * 100 if pivot else 0.0
    r1 = 2.0 * pivot - prev_low
    s1 = 2.0 * pivot - prev_high
    r2 = pivot + (prev_high - prev_low)
    s2 = pivot - (prev_high - prev_low)

    if setup_base[4] is None or setup_base[5] is None:
        return None
    atr_prev_date = str(setup_base[4])
    if atr_prev_date != prev_date:
        logger.warning(
            "Live setup row for %s on %s: daily prev_date=%s != atr prev_date=%s — skipping",
            symbol,
            trade_date,
            prev_date,
            atr_prev_date,
        )
        return None
    atr = float(setup_base[5] or 0.0)
    if atr <= 0:
        return None

    if setup_base[6] is None or setup_base[7] is None:
        return None
    threshold_prev_date = str(setup_base[6])
    if threshold_prev_date != prev_date:
        logger.warning(
            "Live setup row for %s on %s: daily prev_date=%s != threshold prev_date=%s — skipping",
            symbol,
            trade_date,
            prev_date,
            threshold_prev_date,
        )
        return None
    cpr_threshold = float(setup_base[7])

    intraday = _build_intraday_summary(
        live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
    )
    open_915 = intraday["open_915"]
    or_high_5 = intraday["or_high_5"]
    or_low_5 = intraday["or_low_5"]
    or_close_5 = intraday["or_close_5"]
    if open_915 is None or or_high_5 is None or or_low_5 is None:
        return None

    if open_915 < cpr_lower:
        open_side = "BELOW"
    elif open_915 > cpr_upper:
        open_side = "ABOVE"
    else:
        open_side = "INSIDE"
    direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
    setup_source = "live_fallback_late_start" if bool(intraday.get("or_proxy")) else "live_fallback"
    return {
        "trade_date": trade_date,
        "prev_day_close": prev_close,
        "tc": tc,
        "bc": bc,
        "pivot": pivot,
        "r1": r1,
        "s1": s1,
        "r2": r2,
        "s2": s2,
        "atr": atr,
        "cpr_width_pct": cpr_width_pct,
        "cpr_threshold": cpr_threshold,
        "or_high_5": or_high_5,
        "or_low_5": or_low_5,
        "open_915": open_915,
        "or_close_5": or_close_5,
        "open_side": open_side,
        "open_to_cpr_atr": abs(open_915 - (cpr_lower if open_side == "BELOW" else cpr_upper)) / atr
        if open_side in {"BELOW", "ABOVE"}
        else 0.0,
        "gap_abs_pct": abs(calculate_gap_pct(open_915, prev_close)),
        "or_atr_5": calculate_or_atr_ratio(or_high_5, or_low_5, atr),
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": int(cpr_width_pct < cpr_threshold),
        "setup_source": setup_source,
    }


def load_setup_row(
    symbol: str,
    trade_date: str,
    live_candles: list[dict[str, Any]] | None = None,
    *,
    or_minutes: int = 5,
    allow_live_fallback: bool = True,
    bar_end_offset: timedelta | None = None,
    regime_index_symbol: str | None = None,
    regime_snapshot_minutes: int = 30,
) -> dict[str, Any] | None:
    db = get_dashboard_db()
    regime_symbol = str(regime_index_symbol or "").strip().upper()
    regime_close_col = regime_snapshot_close_col(regime_snapshot_minutes)
    with _MARKET_DB_READ_LOCK:
        row = db.con.execute(
            f"""
            SELECT
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
                END AS regime_move_pct
            FROM market_day_state m
            LEFT JOIN strategy_day_state s
              ON s.symbol = m.symbol
             AND s.trade_date = m.trade_date
            LEFT JOIN market_day_state reg
              ON reg.symbol = ? AND reg.trade_date = m.trade_date
            WHERE m.symbol = ? AND m.trade_date = ?::DATE
            LIMIT 1
            """,
            [regime_symbol, symbol, trade_date],
        ).fetchone()
    if not row:
        if not allow_live_fallback:
            return None
        return _load_live_setup_row(
            symbol,
            trade_date,
            live_candles or [],
            or_minutes=or_minutes,
            bar_end_offset=bar_end_offset,
        )

    open_side = str(row[16] or "")
    or_close_5 = _float_or_none(row[15])
    direction = resolve_cpr_direction(
        or_close_5, float(row[2] or 0.0), float(row[3] or 0.0), fallback="NONE"
    )
    if direction == "NONE" and or_close_5 is None:
        direction = str(row[20] or "NONE")
    # market_day_state row exists (pre-market ASOF ATR build) but 9:15 data not yet
    # available (or_close_5 NULL, strategy_day_state not yet built).  If live candles
    # have arrived, derive direction from them instead of falling back to _load_live_setup_row.
    if direction == "NONE" and live_candles:
        intraday = _build_intraday_summary(
            live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
        )
        live_or_close_5 = intraday.get("or_close_5")
        if live_or_close_5 is not None:
            direction = resolve_cpr_direction(
                live_or_close_5, float(row[2] or 0.0), float(row[3] or 0.0), fallback="NONE"
            )
            or_close_5 = live_or_close_5  # use live close for is_narrowing/entry checks

    # Load rvol_baseline_arr from intraday_day_pack so RVOL filtering matches backtest.
    rvol_baseline: list[float | None] | None = None
    try:
        with _MARKET_DB_READ_LOCK:
            pack_row = db.con.execute(
                "SELECT rvol_baseline_arr FROM intraday_day_pack"
                " WHERE symbol = ? AND trade_date = ?::DATE LIMIT 1",
                [symbol, trade_date],
            ).fetchone()
        if pack_row and pack_row[0]:
            rvol_baseline = [float(v) if v is not None else None for v in pack_row[0]]
    except Exception:
        pass

    return {
        "trade_date": str(row[0] or trade_date),
        "prev_day_close": _float_or_none(row[1]),
        "tc": float(row[2] or 0.0),
        "bc": float(row[3] or 0.0),
        "pivot": float(row[4] or 0.0),
        "r1": float(row[5] or 0.0),
        "s1": float(row[6] or 0.0),
        "r2": float(row[7] or 0.0),
        "s2": float(row[8] or 0.0),
        "atr": float(row[9] or 0.0),
        "cpr_width_pct": float(row[10] or 0.0),
        "cpr_threshold": float(row[11] or 0.0),
        "or_high_5": float(row[12] or 0.0),
        "or_low_5": float(row[13] or 0.0),
        "open_915": float(row[14] or 0.0),
        "or_close_5": or_close_5,
        "open_side": open_side,
        "open_to_cpr_atr": _float_or_none(row[17]),
        "gap_abs_pct": _float_or_none(row[18]),
        "or_atr_5": _float_or_none(row[19]),
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": bool(row[21]),
        "cpr_shift": str(row[22] or "OVERLAP"),
        "regime_move_pct": float(row[23]) if row[23] is not None else None,
        "rvol_baseline": rvol_baseline,
        "setup_source": "market_day_state",
    }


def _live_setup_status(setup_row: dict[str, Any] | None) -> str:
    if setup_row is None:
        return "pending"
    if bool(setup_row.get("direction_pending")):
        return "pending"
    direction = str(setup_row.get("direction") or "").upper()
    if direction in {"LONG", "SHORT"}:
        return "candidate"
    return "pending"


def _bar_candle_payload(candle: Any) -> dict[str, Any]:
    return {
        "bar_end": candle.bar_end,
        "open": float(candle.open),
        "high": float(candle.high),
        "low": float(candle.low),
        "close": float(candle.close),
        "volume": float(candle.volume),
    }


def _hydrate_setup_row_from_market_row(
    *,
    trade_date: str,
    row: tuple[Any, ...],
    live_candles: list[dict[str, Any]] | None = None,
    or_minutes: int = 5,
    bar_end_offset: timedelta | None = None,
) -> dict[str, Any] | None:
    tc = float(row[3] or 0.0)
    bc = float(row[4] or 0.0)
    atr = float(row[10] or 0.0)
    if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
        return None
    or_close_5 = float(row[16]) if row[16] is not None else None
    direction = resolve_cpr_direction(or_close_5, tc, bc, fallback="NONE")
    if direction == "NONE" and or_close_5 is None:
        direction = str(row[21] or "NONE")
    live_intraday: dict[str, Any] | None = None
    if direction == "NONE" and live_candles:
        live_intraday = _build_intraday_summary(
            live_candles, or_minutes=or_minutes, bar_end_offset=bar_end_offset
        )
        live_or_close_5 = live_intraday.get("or_close_5")
        if live_or_close_5 is not None:
            direction = resolve_cpr_direction(live_or_close_5, tc, bc, fallback="NONE")
            or_close_5 = live_or_close_5
    # OR OHLCV fields: DB is NULL pre-market (market_day_state built before 9:15 candle).
    # Fall back to live candle summary so or_atr_ratio / gap filters use real values.
    _db_or_high = float(row[13] or 0.0)
    _db_or_low = float(row[14] or 0.0)
    _db_open_915 = float(row[15] or 0.0)
    if live_intraday is not None:
        or_high_5 = _db_or_high or float(live_intraday.get("or_high_5") or 0.0)
        or_low_5 = _db_or_low or float(live_intraday.get("or_low_5") or 0.0)
        open_915_val = _db_open_915 or float(live_intraday.get("open_915") or 0.0)
    else:
        or_high_5, or_low_5, open_915_val = _db_or_high, _db_or_low, _db_open_915
    rvol_baseline: list[float | None] | None = None
    if row[24]:
        rvol_baseline = [float(v) if v is not None else None for v in row[24]]
    return {
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
        "or_high_5": or_high_5,
        "or_low_5": or_low_5,
        "open_915": open_915_val,
        "or_close_5": or_close_5,
        "open_side": str(row[17] or ""),
        "open_to_cpr_atr": float(row[18]) if row[18] is not None else None,
        "gap_abs_pct": float(row[19]) if row[19] is not None else None,
        "or_atr_5": float(row[20]) if row[20] is not None else None,
        "direction": direction,
        "direction_pending": direction not in {"LONG", "SHORT"},
        "is_narrowing": bool(row[22]),
        "cpr_shift": str(row[23] or "OVERLAP"),
        "rvol_baseline": rvol_baseline,
        "setup_source": "market_day_state",
    }


def refresh_pending_setup_rows_for_bar(
    *,
    runtime_state: PaperRuntimeState,
    symbols: list[str],
    trade_date: str,
    bar_candles: list[Any] | None,
    or_minutes: int,
    allow_live_fallback: bool,
) -> dict[str, int]:
    """Batch-refresh unresolved setup rows once per bar cycle."""
    if not symbols:
        return {"resolved": 0, "pending": 0, "missing": 0, "updated": 0}

    bar_end = bar_candles[0].bar_end if bar_candles else None
    current_rows: dict[str, dict[str, Any]] = {}
    for candle in bar_candles or []:
        current_rows[str(candle.symbol)] = _bar_candle_payload(candle)

    pending_symbols: list[str] = []
    for symbol in dict.fromkeys(symbols):
        state = runtime_state.symbols.get(symbol)
        if state is None:
            state = runtime_state.for_symbol(symbol)
        if bar_end is not None and state.setup_refresh_bar_end == bar_end:
            continue
        if runtime_setup_status(runtime_state, symbol) == "pending":
            pending_symbols.append(symbol)

    if not pending_symbols:
        return {"resolved": 0, "pending": 0, "missing": 0, "updated": 0}

    db = get_dashboard_db()
    placeholders = ", ".join(["?"] * len(pending_symbols))
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
            p.rvol_baseline_arr
        FROM market_day_state m
        LEFT JOIN strategy_day_state s
          ON s.symbol = m.symbol
         AND s.trade_date = m.trade_date
        LEFT JOIN intraday_day_pack p
          ON p.symbol = m.symbol
         AND p.trade_date = m.trade_date
        WHERE m.trade_date = ?::DATE
          AND m.symbol IN ({placeholders})
    """
    with _MARKET_DB_READ_LOCK:
        rows = db.con.execute(query, [trade_date, *pending_symbols]).fetchall()
    batch_rows = {str(row[0]): row for row in rows}

    resolved = 0
    pending = 0
    missing = 0
    updated = 0
    for symbol in pending_symbols:
        state = runtime_state.for_symbol(symbol)
        state.setup_refresh_bar_end = bar_end
        row = batch_rows.get(symbol)
        live_candles = list(state.candles)
        if current_rows.get(symbol) is not None:
            live_candles = [*live_candles, current_rows[symbol]]
        if row is None:
            missing += 1
            if allow_live_fallback:
                fallback_row = _load_live_setup_row(
                    symbol,
                    trade_date,
                    live_candles,
                    or_minutes=or_minutes,
                    bar_end_offset=runtime_state.bar_end_offset,
                )
                if fallback_row is not None:
                    state.setup_row = fallback_row
                    updated += 1
                    if bool(fallback_row.get("direction_pending")):
                        pending += 1
                    else:
                        resolved += 1
            continue
        setup_row = _hydrate_setup_row_from_market_row(
            trade_date=trade_date,
            row=row,
            live_candles=live_candles,
            or_minutes=or_minutes,
            bar_end_offset=runtime_state.bar_end_offset,
        )
        if setup_row is None:
            missing += 1
            continue
        state.setup_row = setup_row
        updated += 1
        if bool(setup_row.get("direction_pending")):
            pending += 1
        else:
            resolved += 1
    return {"resolved": resolved, "pending": pending, "missing": missing, "updated": updated}


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


def runtime_setup_status(runtime_state: PaperRuntimeState, symbol: str) -> str:
    state = runtime_state.symbols.get(symbol)
    if state is None or state.setup_row is None:
        return "pending"
    if bool(state.setup_row.get("direction_pending")):
        return "pending"
    direction = str(state.setup_row.get("direction") or "").upper()
    return "candidate" if direction in {"LONG", "SHORT"} else "pending"


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


async def flatten_session_positions(
    session_id: str,
    *,
    notes: str | None = None,
    feed_state: FeedState | None = None,
    emit_summary: bool = True,
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
        close_price = _close_price_for_position(position, price_map)
        realized = _realized_pnl_for_close(position, close_price, params=params)
        total_realized += realized
        side = "SELL" if str(position.direction).upper() == "LONG" else "BUY"
        await append_order_event(
            session_id=session_id,
            symbol=position.symbol,
            side=side,
            requested_qty=float(position.quantity),
            position_id=position.position_id,
            order_type="MARKET",
            request_price=close_price,
            fill_qty=float(position.quantity),
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
        close_price = _close_price_for_position(position, price_map)
        realized = _realized_pnl_for_close(position, close_price, params=params)
        side = "SELL" if str(position.direction).upper() == "LONG" else "BUY"
        await append_order_event(
            session_id=session_id,
            symbol=position.symbol,
            side=side,
            requested_qty=float(position.quantity),
            position_id=position.position_id,
            order_type="MARKET",
            request_price=close_price,
            fill_qty=float(position.quantity),
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
) -> dict[str, Any]:
    positions = await get_session_positions(session.session_id)
    summary = summarize_paper_positions(session, positions, feed_state)
    realized_pnl = float(summary.get("realized_pnl", 0.0) or 0.0)
    await update_session_state(session.session_id, daily_pnl_used=realized_pnl)

    reasons = _risk_limit_reasons(session, as_of, realized_pnl)
    if not reasons:
        return {"triggered": False, "daily_pnl_used": realized_pnl, "reasons": []}

    flatten_result = await flatten_session_positions(
        session.session_id,
        notes=f"{notes_prefix}: {', '.join(reasons)}",
        feed_state=feed_state,
    )
    # flatten_session_positions already dispatches the EOD summary alert.
    # No need to send a second DAILY_LOSS_LIMIT alert here.
    return {
        "triggered": True,
        "daily_pnl_used": realized_pnl,
        "reasons": reasons,
        "flatten": flatten_result,
    }


def _entry_candidate(
    *,
    direction: str,
    candle: dict[str, Any],
    sl_price: float,
    target_price: float,
    first_target_price: float | None = None,
    scale_out_pct: float = 0.0,
    atr: float,
    params: BacktestParams,
    entry_price: float | None = None,
    capital_base: float | None = None,
) -> dict[str, Any] | None:
    # Use caller-supplied fill price (e.g. stop-order simulation) or fall back to close.
    fill_price = entry_price if entry_price is not None else float(candle["close"])
    target_for_rr = float(first_target_price or target_price)
    normalized = normalize_stop_loss(
        entry_price=fill_price,
        sl_price=sl_price,
        direction=direction,
        atr=atr,
        min_sl_atr_ratio=params.min_sl_atr_ratio,
        max_sl_atr_ratio=params.max_sl_atr_ratio,
    )
    if normalized is None:
        return None
    sl_price, sl_distance = normalized
    if direction == "LONG" and target_for_rr <= fill_price:
        return None
    if direction == "SHORT" and target_for_rr >= fill_price:
        return None

    capital_for_sizing = (
        float(capital_base) if capital_base is not None else float(params.portfolio_value or 0.0)
    )
    risk_capital = (
        float(capital_base)
        if capital_base is not None and bool(params.risk_based_sizing)
        else float(params.capital or 0.0)
    )
    position_size = calculate_position_size(risk_capital, params.risk_pct, sl_distance)
    if not params.risk_based_sizing:
        notional_cap = max(
            1,
            int((capital_for_sizing * params.max_position_pct) / max(1.0, fill_price)),
        )
        position_size = max(1, min(position_size, notional_cap))
    rr_ratio = abs(target_for_rr - fill_price) / sl_distance if sl_distance > 0 else params.rr_ratio
    return {
        "direction": direction,
        "entry_price": fill_price,
        "entry_time": candle["time_str"],
        "event_time": candle.get("bar_end"),
        "sl_price": float(sl_price),
        "target_price": float(target_price),
        "first_target_price": float(first_target_price or target_price),
        "scale_out_pct": float(scale_out_pct),
        "sl_distance": float(sl_distance),
        "position_size": int(position_size),
        "rr_ratio": float(rr_ratio),
    }


async def _open_position_from_candidate(
    *,
    session: PaperSession,
    symbol: str,
    candidate: dict[str, Any],
    setup_row: dict[str, Any],
    params: BacktestParams,
    now: datetime,
) -> dict[str, Any]:
    position = await open_position(
        session_id=session.session_id,
        symbol=symbol,
        direction=candidate["direction"],
        quantity=float(candidate["position_size"]),
        entry_price=float(candidate["entry_price"]),
        stop_loss=float(candidate["sl_price"]),
        target_price=float(candidate["target_price"]),
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
            "scale_out_pct": float(candidate.get("scale_out_pct") or 0.0),
            "scaled_out": False,
            "initial_qty": float(candidate["position_size"]),
            "candle_count": 0,
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
            "exit_value": float(decision.fills[0][0]) * float(decision.fills[0][1]),
            "next_trail_state": next_trail_state,
        }

    await update_position(
        position.position_id,
        status="CLOSED",
        stop_loss=float(ts.current_sl),
        trail_state={**pre_update_trail_state, **exit_payload},
        current_qty=0.0,
        last_price=close_price,
        close_price=close_price,
        realized_pnl=realized,
        exit_reason=exit_reason,
        closed_by=exit_reason,
        closed_at=candle.get("bar_end") if isinstance(candle, dict) else None,
    )
    # Update session total_pnl so the dashboard shows live PnL during trading.
    await _accumulate_session_pnl(position.session_id, realized)
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
            position_tracker.credit_cash(float(advance.get("exit_value") or 0.0))
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
    "PaperRuntimeState",
    "SymbolRuntimeState",
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
