"""Small helper functions for live paper-session orchestration."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from engine.live_market_data import IST, ClosedCandle, MarketSnapshot

logger = logging.getLogger(__name__)

GLOBAL_FLATTEN_SIGNAL = Path(".tmp_logs") / "flatten_all.signal"
ADMIN_COMMAND_MAX_AGE_SEC = 300.0


def feed_snapshot_payload(snapshot: MarketSnapshot) -> dict[str, Any]:
    return {
        "mode": "live_quote",
        "symbol": snapshot.symbol,
        "ts": snapshot.ts.isoformat(),
        "last_price": snapshot.last_price,
        "volume": snapshot.volume,
        "source": snapshot.source,
    }


def closed_candle_payload(candle: ClosedCandle) -> dict[str, Any]:
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


def live_mark_feed_state(
    *,
    session_id: str,
    symbol_last_prices: dict[str, float],
    ticker_adapter: Any = None,
    symbols: list[str] | set[str] | tuple[str, ...] | None = None,
) -> Any:
    """Build a feed-state view using latest live marks for immediate flatten fills."""
    prices = dict(symbol_last_prices)
    if ticker_adapter is not None and hasattr(ticker_adapter, "get_last_ltp"):
        for symbol in symbols or prices.keys():
            normalized = str(symbol).upper()
            try:
                ltp = ticker_adapter.get_last_ltp(normalized)
            except Exception:
                logger.debug("Failed to read latest LTP for %s", normalized, exc_info=True)
                continue
            if ltp is not None:
                prices[normalized] = float(ltp)
    return SimpleNamespace(
        session_id=session_id,
        status="LIVE_MARK",
        last_event_ts=datetime.now(IST),
        last_bar_ts=None,
        last_price=None,
        stale_reason=None,
        raw_state={"symbol_last_prices": prices, "mark_source": "live_ltp"},
    )


def open_symbols_from_tracker(tracker: Any) -> list[str]:
    return sorted(str(symbol).upper() for symbol in getattr(tracker, "_open", {}).keys())


def entry_disabled_symbols(
    *,
    tracker: Any,
    active_symbols: list[str],
) -> list[str]:
    del active_symbols
    return open_symbols_from_tracker(tracker)


def cancel_pending_admin_commands(cmd_dir: Path, current_file: Path) -> int:
    cancelled = 0
    if not cmd_dir.exists():
        return cancelled
    for pending_file in sorted(cmd_dir.glob("*.json")):
        if pending_file == current_file:
            continue
        try:
            pending_file.unlink()
            cancelled += 1
        except OSError:
            logger.debug("Failed to delete pending admin command %s", pending_file, exc_info=True)
    return cancelled


def is_admin_command_stale(
    cmd_file: Path,
    now: datetime,
    *,
    max_age_sec: float = ADMIN_COMMAND_MAX_AGE_SEC,
) -> bool:
    """Return True when a command file is older than the configured expiry window."""
    try:
        age_seconds = now.timestamp() - cmd_file.stat().st_mtime
    except OSError:
        return False
    return age_seconds > max_age_sec


def has_closed_positions(
    session_id: str,
    *,
    paper_db: Any,
) -> bool:
    try:
        positions = paper_db.get_session_positions(session_id, statuses=["CLOSED"])
        return len(positions) > 0
    except Exception:
        logger.debug("Failed to load closed positions for session %s", session_id, exc_info=True)
        return False


def is_zero_trade_restart_session(
    session_id: str,
    *,
    terminal_reason: str | None,
    paper_db: Any,
) -> bool:
    if terminal_reason is None:
        return False
    if terminal_reason not in {"no_trades_entry_window_closed", "NO_TRADES_ENTRY_WINDOW_CLOSED"}:
        return False
    return not has_closed_positions(session_id=session_id, paper_db=paper_db)


def should_use_global_flatten_signal() -> bool:
    return GLOBAL_FLATTEN_SIGNAL.exists()


def seconds_until_next_candle_close(now: datetime, candle_interval_minutes: int) -> float:
    interval_seconds = max(1, int(candle_interval_minutes)) * 60
    seconds_since_midnight = (
        now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1_000_000.0
    )
    remaining = interval_seconds - (seconds_since_midnight % interval_seconds)
    if remaining <= 0:
        return float(interval_seconds)
    return float(remaining)


def resolve_poll_interval(
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
    seconds_to_close = seconds_until_next_candle_close(current_time, candle_interval_minutes)
    if seconds_to_close <= 5.0:
        return min(base_interval, 0.5)
    if seconds_to_close <= 20.0:
        return min(base_interval, 1.0)
    if seconds_to_close <= 60.0:
        return min(base_interval, 2.0)
    return base_interval


def resolve_candle_interval(settings: Any, candle_interval_minutes: int | None) -> int:
    if candle_interval_minutes is None:
        return settings.paper_candle_interval_minutes
    return candle_interval_minutes


def resolve_active_symbols(session: Any, symbols: list[str] | None) -> list[str]:
    return [s.strip() for s in symbols or session.symbols if s and s.strip()]


def floor_bucket_start(ts: datetime, interval_minutes: int) -> datetime:
    total_minutes = ts.hour * 60 + ts.minute
    bucket_minutes = (total_minutes // interval_minutes) * interval_minutes
    return ts.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0,
        microsecond=0,
    )
