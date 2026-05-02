"""Alert message formatting helpers for paper trading."""

from __future__ import annotations

import html
import re
from datetime import datetime

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _html_text(value: object) -> str:
    return html.escape(_CONTROL_CHARS.sub("", str(value)), quote=True)


def _format_event_time(event_time: datetime | None) -> str:
    """Format trade event time as 'HH:MM DD-Mon' for alerts."""
    if event_time is None:
        return ""
    return event_time.strftime("%H:%M %d-%b")


def _format_open_alert(
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    sl_price: float,
    target_price: float,
    sl_distance: float,
    position_size: int,
    rr_ratio: float,
    strategy: str,
    session_id: str,
    event_time: datetime | None = None,
) -> tuple[str, str]:
    """Format TRADE_OPENED alert subject and body (HTML for Telegram)."""
    icon = "🟢" if direction == "LONG" else "🔴"
    clean_symbol = _CONTROL_CHARS.sub("", str(symbol))
    clean_direction = _CONTROL_CHARS.sub("", str(direction))
    safe_strategy = _html_text(strategy)
    safe_session_id = _html_text(str(session_id)[:16])
    chart_symbol = re.sub(r"[^A-Za-z0-9_.-]", "", str(symbol).upper())
    subject = f"{icon} {clean_direction} OPENED: {clean_symbol}"
    time_str = _format_event_time(event_time)
    risk_rupees = sl_distance * position_size
    chart_link = (
        f"📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:{chart_symbol}'>Chart</a>"
    )
    body = (
        f"📥 Entry: <code>₹{entry_price:.2f}</code> | 🛡️ SL: <code>₹{sl_price:.2f}</code>\n"
        f"🎯 Target: <code>₹{target_price:.2f}</code> | 📏 Qty: <code>{position_size}</code>\n"
        f"💰 Risk: ₹{risk_rupees:,.0f} ({rr_ratio:.1f}R)"
        + (f" | 🕒 {time_str}" if time_str else "")
        + f"\n{chart_link}"
        + f"\n<i>{safe_strategy} · {safe_session_id}</i>"
    )
    return subject, body


def _format_close_alert(
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    close_price: float,
    reason: str,
    realized_pnl: float,
    duration_bars: int | None = None,
    strategy: str = "",
    session_id: str = "",
    event_time: datetime | None = None,
) -> tuple[str, str]:
    """Format TRADE_CLOSED alert subject and body (HTML for Telegram)."""
    safe_reason = _html_text(reason)
    safe_strategy = _html_text(strategy)
    safe_session_id = _html_text(str(session_id)[:16])
    pnl_pct = (
        ((close_price - entry_price) / entry_price * 100)
        if direction == "LONG"
        else ((entry_price - close_price) / entry_price * 100)
    )
    is_win = realized_pnl >= 0
    result_tag = "WIN" if is_win else "LOSS"
    icon = "✅" if is_win else "❌"
    trend_icon = "📈" if is_win else "📉"
    subject = (
        f"{icon} [{result_tag}] "
        f"{_CONTROL_CHARS.sub('', str(symbol))} "
        f"{_CONTROL_CHARS.sub('', str(direction))} "
        f"{_CONTROL_CHARS.sub('', str(reason))}"
    )
    time_str = _format_event_time(event_time)
    pnl_display = f"{'+' if is_win else '-'}₹{abs(realized_pnl):,.0f}"
    chart_symbol = re.sub(r"[^A-Za-z0-9_.-]", "", str(symbol).upper())
    chart_link = (
        f"📊 <a href='https://www.tradingview.com/chart/?symbol=NSE:{chart_symbol}'>Chart</a>"
    )
    body = (
        f"💰 P&L: <code>{pnl_display}</code> ({pnl_pct:+.2f}%)\n"
        f"🏁 Reason: {safe_reason}\n"
        f"{trend_icon} Exit: <code>{entry_price:.2f}</code> → <code>{close_price:.2f}</code>"
        + (f"\n🕒 {time_str}" if time_str else "")
        + (f"\n{chart_link}" if chart_link else "")
        + (f"\n<i>{safe_strategy} · {safe_session_id}</i>" if strategy else "")
    )
    return subject, body


def _format_risk_alert(
    *,
    reason: str,
    net_pnl: float,
    session_id: str,
    positions_closed: int = 0,
    total_trades: int | None = None,
    trade_date: str | None = None,
) -> tuple[str, str]:
    """Format risk limit alert (HTML for Telegram daily-summary style)."""
    safe_session_id = _html_text(session_id)
    pnl_emoji = "📈" if net_pnl >= 0 else "📉"
    date_str = trade_date if trade_date else ""
    if date_str and len(date_str) == 10:
        date_str = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    subject = f"📊 EOD Summary — {date_str}" if date_str else "📊 EOD Summary"
    body = (
        f"Session: <code>{safe_session_id}</code>\n"
        f"Net P&L: <code>{net_pnl:+,.2f}</code> {pnl_emoji}\n"
        f"Trades closed: {total_trades if total_trades is not None else positions_closed}"
    )
    return subject, body


def _parse_session_label(session_id: str) -> tuple[str, str]:
    """Extract (short_label, date_label) from session_id for alert subjects."""
    parts = str(session_id or "").split("-")
    label_parts: list[str] = []
    date_label = ""
    i = 0
    while i < len(parts):
        if len(parts[i]) == 4 and parts[i].isdigit() and i + 2 < len(parts):
            try:
                from datetime import date as _date

                d = _date(int(parts[i]), int(parts[i + 1]), int(parts[i + 2]))
                date_label = d.strftime("%d %b").lstrip("0")
                break
            except ValueError, IndexError:
                pass
        label_parts.append(parts[i])
        i += 1
    raw = "_".join(label_parts)
    short = (
        raw.replace("CPR_LEVELS_LONG", "CPR LONG")
        .replace("CPR_LEVELS_SHORT", "CPR SHORT")
        .replace("FBR_LONG", "FBR LONG")
        .replace("FBR_SHORT", "FBR SHORT")
        .replace("_", " ")
        .strip()
    )
    return short or raw, date_label


__all__ = [
    "_CONTROL_CHARS",
    "_format_close_alert",
    "_format_event_time",
    "_format_open_alert",
    "_format_risk_alert",
    "_html_text",
    "_parse_session_label",
]
