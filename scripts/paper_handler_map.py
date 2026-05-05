"""Command-handler mapping helpers for the paper trading CLI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

PAPER_TRADING_HANDLER_NAMES: tuple[str, ...] = (
    "_cmd_start",
    "_cmd_status",
    "_cmd_universes",
    "_cmd_pause",
    "_cmd_resume",
    "_cmd_stop",
    "_cmd_resend_eod",
    "_cmd_flatten",
    "_cmd_flatten_all",
    "_cmd_send_command",
    "_cmd_flatten_both",
    "_cmd_reconcile",
    "_cmd_broker_reconcile",
    "_cmd_broker_sync_orders",
    "_cmd_pilot_check",
    "_cmd_real_pilot_plan",
    "_cmd_order",
    "_cmd_real_dry_run_order",
    "_cmd_real_order",
    "_cmd_close_position",
    "_cmd_cleanup",
    "_cmd_feed_audit",
    "_cmd_signal_audit",
    "_cmd_replay",
    "_cmd_live",
    "_cmd_daily_prepare",
    "_cmd_daily_replay",
    "_cmd_daily_sim",
    "_cmd_daily_live",
)


def build_paper_trading_handler_map(
    namespace: Mapping[str, Callable[..., Any]],
) -> dict[str, Callable[..., Any]]:
    """Build parser handler map from the paper_trading module namespace."""
    return {name: namespace[name] for name in PAPER_TRADING_HANDLER_NAMES}
