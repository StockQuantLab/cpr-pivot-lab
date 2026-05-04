"""Argument parser construction for the paper-trading CLI."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from typing import Any

from engine.execution_defaults import DEFAULT_MAX_POSITION_PCT, DEFAULT_MAX_POSITIONS
from scripts.paper_strategy_params import (
    PAPER_ALLOWED_PRESETS,
    PAPER_ALLOWED_STRATEGIES,
    _paper_default_strategy,
    _StandardSizingAction,
)

Handler = Callable[[argparse.Namespace], Any]


def build_paper_trading_parser(
    *,
    settings: Any,
    handlers: Mapping[str, Handler],
    market_ready_hhmm: str,
) -> argparse.ArgumentParser:
    _cmd_start = handlers["_cmd_start"]
    _cmd_status = handlers["_cmd_status"]
    _cmd_universes = handlers["_cmd_universes"]
    _cmd_pause = handlers["_cmd_pause"]
    _cmd_resume = handlers["_cmd_resume"]
    _cmd_stop = handlers["_cmd_stop"]
    _cmd_resend_eod = handlers["_cmd_resend_eod"]
    _cmd_flatten = handlers["_cmd_flatten"]
    _cmd_flatten_all = handlers["_cmd_flatten_all"]
    _cmd_send_command = handlers["_cmd_send_command"]
    _cmd_flatten_both = handlers["_cmd_flatten_both"]
    _cmd_reconcile = handlers["_cmd_reconcile"]
    _cmd_broker_reconcile = handlers["_cmd_broker_reconcile"]
    _cmd_broker_sync_orders = handlers["_cmd_broker_sync_orders"]
    _cmd_pilot_check = handlers["_cmd_pilot_check"]
    _cmd_order = handlers["_cmd_order"]
    _cmd_real_dry_run_order = handlers["_cmd_real_dry_run_order"]
    _cmd_real_order = handlers["_cmd_real_order"]
    _cmd_close_position = handlers["_cmd_close_position"]
    _cmd_cleanup = handlers["_cmd_cleanup"]
    _cmd_feed_audit = handlers["_cmd_feed_audit"]
    _cmd_signal_audit = handlers["_cmd_signal_audit"]
    _cmd_replay = handlers["_cmd_replay"]
    _cmd_live = handlers["_cmd_live"]
    _cmd_daily_prepare = handlers["_cmd_daily_prepare"]
    _cmd_daily_replay = handlers["_cmd_daily_replay"]
    _cmd_daily_sim = handlers["_cmd_daily_sim"]
    _cmd_daily_live = handlers["_cmd_daily_live"]

    parser = argparse.ArgumentParser(description="Paper trading session control")
    sub = parser.add_subparsers(dest="command", required=True)

    def _add_symbol_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--symbols", default=None, help="Optional comma-separated symbol override")
        sp.add_argument(
            "--universe-name",
            default=None,
            help="Saved universe name from backtest_universe (for example full_2026_04_24).",
        )
        sp.add_argument(
            "--all-symbols",
            action="store_true",
            help=(
                "Override the dated saved-universe default and use the current "
                "dynamic full universe from DuckDB metadata."
            ),
        )

    def _add_strategy_args(sp: argparse.ArgumentParser) -> None:
        sizing = sp.add_mutually_exclusive_group()
        sizing.add_argument(
            "--risk-based-sizing",
            "--legacy-sizing",
            dest="risk_based_sizing",
            action="store_true",
            default=True,
            help="Use per-trade risk-based sizing before the shared portfolio overlay (default).",
        )
        sizing.add_argument(
            "--standard-sizing",
            dest="standard_sizing",
            action=_StandardSizingAction,
            nargs=0,
            default=False,
            help=(
                "Legacy opt-out alias that disables risk-based sizing. "
                "Use --risk-based-sizing in baseline commands."
            ),
        )
        sp.add_argument(
            "--strategy",
            choices=list(PAPER_ALLOWED_STRATEGIES),
            default=_paper_default_strategy(settings),
            help="Strategy name (CPR-only for paper workflows).",
        )
        sp.add_argument(
            "--preset",
            choices=list(PAPER_ALLOWED_PRESETS),
            default=None,
            help=(
                "Named strategy preset that fills the full canonical config bundle "
                "(for example CPR_LEVELS_RISK_LONG)."
            ),
        )
        sp.add_argument("--strategy-params", default=None, help="JSON strategy parameter overrides")
        sp.add_argument(
            "--direction",
            choices=["BOTH", "LONG", "SHORT"],
            default=None,
            help="Trade direction filter.",
        )
        rvol = sp.add_mutually_exclusive_group()
        rvol.add_argument(
            "--skip-rvol",
            action="store_true",
            help="Skip RVOL filtering for this run/session.",
        )
        rvol.add_argument(
            "--no-skip-rvol",
            action="store_true",
            help="Force RVOL filtering on, even when the selected preset disables it.",
        )
        sp.add_argument(
            "--min-price",
            type=float,
            default=None,
            help="Minimum previous close required for symbol eligibility.",
        )
        sp.add_argument(
            "--regime-index-symbol",
            default=None,
            help=(
                "Optional broad-index symbol for the market-regime gate "
                "(for example NIFTY 500). Leave empty to disable."
            ),
        )
        sp.add_argument(
            "--regime-min-move-pct",
            type=float,
            default=None,
            help=(
                "Skip LONG when the regime index is down at least this %% and skip SHORT when "
                "it is up at least this %% (default off)."
            ),
        )
        sp.add_argument(
            "--regime-snapshot-minutes",
            type=int,
            choices=[5, 10, 15, 30],
            default=30,
            help=(
                "Regime snapshot window in minutes from the open (default 30 = 09:45 close). "
                "Use 5 for 09:20 or 10 for 09:25."
            ),
        )
        sp.add_argument(
            "--pack-source",
            choices=["intraday_day_pack", "paper_feed_audit"],
            default="intraday_day_pack",
            help=(
                "Intraday candle source for replay/backtest-style sessions. "
                "Default uses intraday_day_pack; paper_feed_audit replays one archived session exactly."
            ),
        )
        sp.add_argument(
            "--pack-source-session-id",
            default=None,
            help=(
                "Required with --pack-source paper_feed_audit: archived paper session_id whose "
                "captured bars should drive replay."
            ),
        )
        sp.add_argument(
            "--cpr-min-close-atr",
            type=float,
            default=None,
            help="Minimum ATR clearance for CPR close confirmation.",
        )
        sp.add_argument(
            "--cpr-no-progress-exit-candles",
            type=int,
            default=None,
            help="CPR_LEVELS: exit after N candles if the trade has not shown enough favorable excursion.",
        )
        sp.add_argument(
            "--cpr-no-progress-min-r",
            type=float,
            default=None,
            help="CPR_LEVELS: minimum favorable excursion in R before no-progress exit can trigger.",
        )
        sp.add_argument(
            "--cpr-scale-out-pct",
            type=float,
            default=None,
            help="CPR_LEVELS: fraction of position to exit at R1/S1 before the runner.",
        )
        sp.add_argument(
            "--narrowing-filter",
            action="store_true",
            help="Enable the canonical CPR narrowing filter.",
        )
        sp.add_argument(
            "--or-minutes",
            type=int,
            choices=[5, 10, 15, 30],
            default=None,
            help="Opening-range duration in minutes.",
        )
        sp.add_argument(
            "--entry-window-end",
            default=None,
            help="Stop scanning for new entries after this time HH:MM.",
        )
        sp.add_argument(
            "--time-exit",
            default=None,
            help="Force-close any open position by this time HH:MM.",
        )
        sp.add_argument(
            "--cpr-entry-start",
            default=None,
            help="Explicit CPR entry scan start time HH:MM.",
        )

    def _add_live_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--poll-interval-sec",
            type=float,
            default=settings.paper_live_poll_interval_sec,
            help="Seconds between live data polls (default: 1.0)",
        )
        sp.add_argument(
            "--candle-interval-minutes",
            type=int,
            default=settings.paper_candle_interval_minutes,
            help="Minutes per candle (default: 5)",
        )
        sp.add_argument(
            "--max-cycles",
            type=int,
            default=None,
            help="Max poll cycles before exit (default: unlimited)",
        )
        sp.add_argument(
            "--complete-on-exit",
            action="store_true",
            help="Mark the session completed when the loop exits",
        )
        sp.add_argument(
            "--no-alerts",
            action="store_true",
            help="Suppress Telegram/email alerts during live execution",
        )
        sp.add_argument(
            "--allow-late-start-fallback",
            action="store_true",
            help=(
                "Allow live setup-row fallback from same-day candles when market_day_state "
                "rows are missing."
            ),
        )
        sp.add_argument(
            "--wait-for-open",
            action="store_true",
            help=(
                f"Allow daily-live to sleep until {market_ready_hhmm} IST when launched early. "
                "Default is fail-fast before market-ready time."
            ),
        )
        sp.add_argument(
            "--real-orders",
            action="store_true",
            help=(
                "Route daily-live paper entries/exits to real Zerodha orders. "
                "Requires Doppler real-order gates; default is paper-only."
            ),
        )
        sp.add_argument(
            "--simulate-real-orders",
            action="store_true",
            help=(
                "Route daily-live paper entries/exits through the Zerodha REAL_DRY_RUN "
                "adapter and record broker-intent latency without calling Kite place_order."
            ),
        )
        sp.add_argument(
            "--real-order-fixed-qty",
            type=int,
            default=1,
            help="Fixed real quantity per automated order (default: 1 share).",
        )
        sp.add_argument(
            "--real-order-max-positions",
            type=int,
            default=1,
            help="Max open real-routed positions for this pilot session (default: 1).",
        )
        sp.add_argument(
            "--real-order-cash-budget",
            type=float,
            default=10_000.0,
            help=(
                "Cash-only notional budget for real-routed open positions "
                "(default: 10000). Set to available start-of-day cash you are willing to use."
            ),
        )
        sp.add_argument(
            "--real-order-skip-account-cash-check",
            action="store_true",
            help=(
                "Skip Kite account cash read before startup. Not recommended; "
                "the local cash budget still applies."
            ),
        )
        sp.add_argument(
            "--real-entry-order-type",
            choices=["LIMIT", "MARKET"],
            default="LIMIT",
            help="Real entry order type. LIMIT is default; MARKET requires Doppler allow-list.",
        )
        sp.add_argument(
            "--real-entry-max-slippage-pct",
            type=float,
            default=0.5,
            help="Marketable LIMIT entry protection percent (default: 0.5).",
        )
        sp.add_argument(
            "--real-exit-max-slippage-pct",
            type=float,
            default=2.0,
            help="Protected LIMIT exit/flatten protection percent (default: 2.0).",
        )

    start = sub.add_parser("start", help="Create a new paper session")
    start.add_argument(
        "--session-id", default=None, help="Custom session ID (auto-generated if omitted)"
    )
    start.add_argument("--name", default=None, help="Human-readable session name")
    start.add_argument(
        "--strategy",
        choices=list(PAPER_ALLOWED_STRATEGIES),
        default=_paper_default_strategy(settings),
        help="Strategy name (CPR-only for paper workflows).",
    )
    start.add_argument(
        "--symbols", default=settings.paper_default_symbols or "", help="Comma-separated symbols"
    )
    start.add_argument("--strategy-params", default=None, help="JSON strategy parameter overrides")
    start.add_argument("--created-by", default=None, help="Creator identifier")
    start.add_argument(
        "--flatten-time",
        default=settings.paper_flatten_time,
        help="EOD flatten time HH:MM:SS",
    )
    start.add_argument(
        "--stale-feed-timeout-sec",
        type=int,
        default=settings.paper_stale_feed_timeout_sec,
        help="Seconds before feed marked STALE (default: 120)",
    )
    start.add_argument(
        "--max-daily-loss-pct",
        type=float,
        default=settings.paper_max_daily_loss_pct,
        help="Max daily loss %% before flatten (default: 0.03)",
    )
    start.add_argument(
        "--max-positions",
        type=int,
        default=settings.paper_max_positions,
        help=f"Max concurrent positions (default: {DEFAULT_MAX_POSITIONS})",
    )
    start.add_argument(
        "--max-position-pct",
        type=float,
        default=settings.paper_max_position_pct,
        help=f"Max allocation per position (default: {DEFAULT_MAX_POSITION_PCT:.2f})",
    )
    start.add_argument("--notes", default=None, help="Optional free-text annotation")
    start.add_argument(
        "--activate",
        action="store_true",
        help="Create the session directly in ACTIVE state",
    )
    start.set_defaults(handler=_cmd_start)

    status = sub.add_parser("status", help="Show session or active-session status")
    status.add_argument("--session-id", default=None, help="Session to inspect (default: active)")
    status.add_argument(
        "--summary",
        action="store_true",
        help="Print only a compact operational summary.",
    )
    status.set_defaults(handler=_cmd_status)

    universes = sub.add_parser("universes", help="List saved universe snapshots")
    universes.add_argument("--name", default=None, help="Optional exact universe name filter")
    universes.add_argument(
        "--prune-before",
        default=None,
        help="Delete saved snapshots whose end_date is older than this YYYY-MM-DD cutoff.",
    )
    universes.add_argument(
        "--apply",
        action="store_true",
        help="Execute the prune. Without this flag, universes are only listed.",
    )
    universes.set_defaults(handler=_cmd_universes)

    pause = sub.add_parser("pause", help="Pause a session")
    pause.add_argument("--session-id", required=True, help="Session to pause")
    pause.add_argument("--notes", default=None, help="Optional free-text annotation")
    pause.set_defaults(handler=_cmd_pause)

    resume = sub.add_parser(
        "resume",
        help="Mark a paused session ACTIVE in DB (DB-only; does not restart the live loop)",
    )
    resume.add_argument("--session-id", required=True, help="Session to resume")
    resume.add_argument("--notes", default=None, help="Optional free-text annotation")
    resume.set_defaults(handler=_cmd_resume)

    stop = sub.add_parser("stop", help="Stop a session")
    stop.add_argument("--session-id", required=True, help="Session to stop")
    stop.add_argument("--complete", action="store_true", help="Mark the session completed")
    stop.add_argument("--notes", default=None, help="Optional free-text annotation")
    stop.set_defaults(handler=_cmd_stop)

    resend_eod = sub.add_parser(
        "resend-eod",
        help="Re-send the EOD summary alert for a completed session (recovery after missed alert)",
    )
    resend_eod.add_argument("--session-id", required=True, help="Session to summarise")
    resend_eod.add_argument("--notes", default=None, help="Optional note appended to the alert")
    resend_eod.set_defaults(handler=_cmd_resend_eod)

    flatten = sub.add_parser("flatten", help="Request flattening of open positions")
    flatten.add_argument("--session-id", required=True, help="Session to flatten")
    flatten.add_argument("--notes", default=None, help="Optional free-text annotation")
    flatten.set_defaults(handler=_cmd_flatten)

    flatten_all = sub.add_parser(
        "flatten-all", help="Flatten all active sessions for a trade date (emergency exit)"
    )
    flatten_all.add_argument("--trade-date", default="today", help="Trade date (default: today)")
    flatten_all.add_argument("--notes", default=None, help="Optional free-text annotation")
    flatten_all.set_defaults(handler=_cmd_flatten_all)

    send_command = sub.add_parser(
        "send-command",
        help="Queue a live-loop admin command without taking the DB writer lock",
    )
    send_command.add_argument("--session-id", required=True, help="Target live session")
    send_command.add_argument(
        "--action",
        required=True,
        choices=[
            "close_positions",
            "close_all",
            "set_risk_budget",
            "pause_entries",
            "resume_entries",
            "cancel_pending_intents",
        ],
        help="Admin action to enqueue",
    )
    send_command.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols for close_positions",
    )
    send_command.add_argument("--reason", default="operator", help="Reason stored in alerts/logs")
    send_command.add_argument("--requester", default="cli", help="Requester label")
    send_command.add_argument(
        "--portfolio-value",
        type=float,
        default=None,
        help="New session budget for future entries (set_risk_budget).",
    )
    send_command.add_argument(
        "--max-positions",
        type=int,
        default=None,
        help="New concurrent-position cap for future entries (set_risk_budget).",
    )
    send_command.add_argument(
        "--max-position-pct",
        type=float,
        default=None,
        help="New per-position percentage cap for future entries (set_risk_budget).",
    )
    send_command.set_defaults(handler=_cmd_send_command)

    flatten_both = sub.add_parser(
        "flatten-both",
        help="Queue close_all for all ACTIVE/PAUSED LONG and SHORT sessions for a trade date",
    )
    flatten_both.add_argument("--trade-date", default="today", help="Trade date (default: today)")
    flatten_both.add_argument("--reason", default="operator_flatten_both", help="Reason label")
    flatten_both.add_argument("--requester", default="cli", help="Requester label")
    flatten_both.set_defaults(handler=_cmd_flatten_both)

    reconcile = sub.add_parser(
        "reconcile",
        help="Check paper session order/position/session invariants",
    )
    reconcile.add_argument("--session-id", required=True, help="Session to reconcile")
    reconcile.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when critical reconciliation findings exist",
    )
    reconcile.set_defaults(handler=_cmd_reconcile)

    broker_reconcile = sub.add_parser(
        "broker-reconcile",
        help="Compare local paper state against supplied broker order/position snapshots",
    )
    broker_reconcile.add_argument("--session-id", required=True, help="Paper session to reconcile")
    broker_reconcile.add_argument(
        "--broker-orders-json",
        default="[]",
        help="Broker orders JSON array or path to JSON file",
    )
    broker_reconcile.add_argument(
        "--broker-positions-json",
        default="[]",
        help="Broker positions JSON array or path to JSON file",
    )
    broker_reconcile.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when critical broker reconciliation findings exist",
    )
    broker_reconcile.set_defaults(handler=_cmd_broker_reconcile)

    broker_sync = sub.add_parser(
        "broker-sync-orders",
        help="Fetch Kite orderbook and persist final broker status onto local paper_orders rows",
    )
    broker_sync.add_argument(
        "--session-id",
        default=None,
        help="Optional paper session filter. Defaults to recent real-order rows across sessions.",
    )
    broker_sync.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Recent local broker-order rows to inspect (default: 100)",
    )
    broker_sync.set_defaults(handler=_cmd_broker_sync_orders)

    pilot_check = sub.add_parser(
        "pilot-check",
        help="Validate real small-size pilot guardrails without enabling real orders",
    )
    pilot_check.add_argument("--symbols", required=True, help="Comma-separated pilot symbols")
    pilot_check.add_argument("--order-quantity", required=True, type=int, help="Per-order quantity")
    pilot_check.add_argument(
        "--estimated-notional",
        required=True,
        type=float,
        help="Estimated total pilot notional",
    )
    pilot_check.add_argument("--product", default="MIS", help="Pilot product")
    pilot_check.add_argument("--order-type", default="LIMIT", help="Pilot order type")
    pilot_check.add_argument(
        "--acknowledgement",
        default=None,
        help="Must be I_ACCEPT_REAL_ORDER_RISK for pilot readiness",
    )
    pilot_check.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when guardrail findings exist",
    )
    pilot_check.set_defaults(handler=_cmd_pilot_check)

    order = sub.add_parser("order", help="Append a paper order event")
    order.add_argument("--session-id", required=True, help="Session for the order")
    order.add_argument("--symbol", required=True, help="Symbol for the order")
    order.add_argument("--side", required=True, choices=["BUY", "SELL"], help="Order side")
    order.add_argument("--quantity", required=True, type=float, help="Order quantity")
    order.add_argument("--order-type", default="MARKET", help="Order type (default: MARKET)")
    order.add_argument(
        "--request-price", type=float, default=None, help="Limit price if applicable"
    )
    order.add_argument("--fill-qty", type=float, default=None, help="Filled quantity")
    order.add_argument("--fill-price", type=float, default=None, help="Fill price")
    order.add_argument(
        "--status",
        default="NEW",
        choices=["NEW", "SUBMITTED", "PARTIAL", "FILLED", "REJECTED", "CANCELLED"],
        help="Order status (default: NEW)",
    )
    order.add_argument("--notes", default=None, help="Optional free-text annotation")
    order.set_defaults(handler=_cmd_order)

    dry_run_order = sub.add_parser(
        "real-dry-run-order",
        help="Build and record a Zerodha order payload without placing a real order",
    )
    dry_run_order.add_argument("--session-id", required=True, help="Session for the order intent")
    dry_run_order.add_argument("--symbol", required=True, help="NSE tradingsymbol")
    dry_run_order.add_argument("--side", required=True, choices=["BUY", "SELL"], help="Order side")
    dry_run_order.add_argument("--quantity", required=True, type=int, help="Order quantity")
    dry_run_order.add_argument("--role", default="manual", help="Intent role for idempotency")
    dry_run_order.add_argument("--position-id", default=None, help="Optional linked position id")
    dry_run_order.add_argument("--signal-id", type=int, default=None, help="Optional signal id")
    dry_run_order.add_argument(
        "--order-type",
        default="MARKET",
        choices=["MARKET", "LIMIT", "SL", "SL-M"],
        help="Zerodha order type",
    )
    dry_run_order.add_argument("--price", type=float, default=None, help="Required for LIMIT/SL")
    dry_run_order.add_argument(
        "--trigger-price", type=float, default=None, help="Required for SL/SL-M"
    )
    dry_run_order.add_argument(
        "--reference-price",
        type=float,
        default=None,
        help="Fresh LTP/mark used to validate protected exit or flatten orders",
    )
    dry_run_order.add_argument(
        "--reference-price-age-sec",
        type=float,
        default=None,
        help="Age in seconds of --reference-price",
    )
    dry_run_order.add_argument(
        "--max-slippage-pct",
        type=float,
        default=2.0,
        help="Max exit/flatten slippage from reference price (default: 2.0)",
    )
    dry_run_order.add_argument(
        "--market-protection",
        type=float,
        default=2.0,
        help="Zerodha market_protection for MARKET/SL-M orders (default: 2.0)",
    )
    dry_run_order.add_argument("--product", default="MIS", help="Zerodha product, default MIS")
    dry_run_order.add_argument("--exchange", default="NSE", help="Zerodha exchange, default NSE")
    dry_run_order.add_argument("--variety", default="regular", help="Zerodha variety")
    dry_run_order.add_argument("--validity", default="DAY", help="Order validity")
    dry_run_order.add_argument("--tag", default=None, help="Optional Zerodha order tag")
    dry_run_order.add_argument(
        "--event-time",
        default=None,
        help="Optional event timestamp included in the idempotency key",
    )
    dry_run_order.set_defaults(handler=_cmd_real_dry_run_order)

    real_order = sub.add_parser(
        "real-order",
        help="Place a real Zerodha order after Doppler and CLI confirmation gates",
    )
    real_order.add_argument("--session-id", required=True, help="Session for the order intent")
    real_order.add_argument("--symbol", required=True, help="NSE tradingsymbol")
    real_order.add_argument("--side", required=True, choices=["BUY", "SELL"], help="Order side")
    real_order.add_argument("--quantity", required=True, type=int, help="Order quantity")
    real_order.add_argument("--role", default="manual", help="Intent role for idempotency")
    real_order.add_argument("--position-id", default=None, help="Optional linked position id")
    real_order.add_argument("--signal-id", type=int, default=None, help="Optional signal id")
    real_order.add_argument(
        "--order-type",
        default="LIMIT",
        choices=["MARKET", "LIMIT", "SL", "SL-M"],
        help="Zerodha order type; real-order env default allows LIMIT/SL/SL-M",
    )
    real_order.add_argument("--price", type=float, default=None, help="Required for LIMIT/SL")
    real_order.add_argument(
        "--trigger-price", type=float, default=None, help="Required for SL/SL-M"
    )
    real_order.add_argument(
        "--reference-price",
        type=float,
        required=True,
        help="Fresh LTP/mark used for real-order guardrails",
    )
    real_order.add_argument(
        "--reference-price-age-sec",
        type=float,
        required=True,
        help="Age in seconds of --reference-price; must be fresh",
    )
    real_order.add_argument(
        "--max-slippage-pct",
        type=float,
        default=2.0,
        help="Max exit/flatten slippage from reference price (default: 2.0)",
    )
    real_order.add_argument(
        "--market-protection",
        type=float,
        default=2.0,
        help="Zerodha market_protection for MARKET/SL-M payloads (default: 2.0)",
    )
    real_order.add_argument("--product", default="MIS", help="Zerodha product, default MIS")
    real_order.add_argument("--exchange", default="NSE", help="Zerodha exchange, default NSE")
    real_order.add_argument("--variety", default="regular", help="Zerodha variety")
    real_order.add_argument("--validity", default="DAY", help="Order validity")
    real_order.add_argument("--tag", default=None, help="Optional Zerodha order tag")
    real_order.add_argument(
        "--event-time",
        default=None,
        help="Optional event timestamp included in the idempotency key",
    )
    real_order.add_argument(
        "--confirm-real-order",
        action="store_true",
        help="Required extra CLI confirmation for real-money order placement",
    )
    real_order.set_defaults(handler=_cmd_real_order)

    close = sub.add_parser("close-position", help="Close a paper position row")
    close.add_argument("--position-id", required=True, type=int, help="Position to close")
    close.add_argument("--close-price", required=True, type=float, help="Closing price")
    close.add_argument("--realized-pnl", type=float, default=None, help="Override realized PnL")
    close.add_argument("--closed-by", default=None, help="Who/what closed the position")
    close.set_defaults(handler=_cmd_close_position)

    cleanup = sub.add_parser(
        "cleanup",
        help="Delete paper-session rows and archived PAPER analytics rows for a specific date",
    )
    cleanup.add_argument(
        "--trade-date",
        type=str,
        required=True,
        help="Delete sessions for this date (YYYY-MM-DD).",
    )
    cleanup.add_argument(
        "--apply",
        action="store_true",
        help="Execute deletes. Default is dry-run only.",
    )
    cleanup.set_defaults(handler=_cmd_cleanup)

    feed_audit = sub.add_parser(
        "feed-audit",
        help="Compare stored paper feed audit rows against intraday_day_pack for a trade date",
    )
    feed_audit.add_argument("--trade-date", required=True, help="Audit trade date YYYY-MM-DD")
    feed_audit.add_argument(
        "--session-id",
        default=None,
        help="Optional paper session to compare. Defaults to all sessions for the date.",
    )
    feed_audit.add_argument(
        "--feed-source",
        choices=["kite", "local", "replay", "all"],
        default="kite",
        help="Filter audit rows by feed source (default: kite/live).",
    )
    feed_audit.set_defaults(handler=_cmd_feed_audit)

    signal_audit = sub.add_parser(
        "signal-audit",
        help="Summarize or compare stored strategy decision audit rows",
    )
    signal_audit.add_argument("--session-id", required=True, help="Primary paper session ID")
    signal_audit.add_argument(
        "--compare-session-id",
        default=None,
        help="Optional second session ID to compare executed OPEN decisions against.",
    )
    signal_audit.add_argument(
        "--trade-date",
        default=None,
        help="Optional trade date filter YYYY-MM-DD.",
    )
    signal_audit.set_defaults(handler=_cmd_signal_audit)

    replay = sub.add_parser("replay", help="Replay historical candles into paper feed state")
    replay.add_argument("--session-id", required=True, help="Session to replay into")
    _add_symbol_args(replay)
    replay.add_argument(
        "--start-date", "--start", dest="start_date", default=None, help="Replay start date"
    )
    replay.add_argument(
        "--end-date", "--end", dest="end_date", default=None, help="Replay end date"
    )
    replay.add_argument(
        "--leave-active",
        action="store_true",
        help="Do not mark the session completed",
    )
    replay.add_argument("--notes", default=None, help="Optional free-text annotation")
    replay.set_defaults(handler=_cmd_replay)

    live = sub.add_parser("live", help="Run the live market-data adapter loop")
    live.add_argument("--session-id", required=True, help="Session to run live")
    _add_symbol_args(live)
    _add_live_args(live)
    live.add_argument("--notes", default=None, help="Optional free-text annotation")
    live.set_defaults(handler=_cmd_live)

    daily_prepare = sub.add_parser("daily-prepare", help="Prepare daily paper runtime tables")
    daily_prepare.add_argument(
        "--trade-date", default=None, help="Trading date YYYY-MM-DD to prepare"
    )
    daily_prepare.add_argument(
        "--snapshot-universe-name",
        default=None,
        help=(
            "Optional saved-universe name to persist the resolved symbol list into "
            "backtest_universe for later reuse."
        ),
    )
    daily_prepare.add_argument(
        "--refresh-universe-snapshot",
        action="store_true",
        help=(
            "Explicitly overwrite an existing dated full_YYYY_MM_DD snapshot when it differs "
            "from canonical_full. Normal reruns refuse mismatched overwrites."
        ),
    )
    _add_symbol_args(daily_prepare)
    daily_prepare.set_defaults(handler=_cmd_daily_prepare)

    daily_replay = sub.add_parser(
        "daily-replay",
        help="Prepare runtime tables and replay one trading date",
    )
    daily_replay.add_argument("--trade-date", default=None, help="Trading date YYYY-MM-DD")
    _add_symbol_args(daily_replay)
    _add_strategy_args(daily_replay)
    daily_replay.add_argument("--session-id", default=None, help="Custom session ID")
    daily_replay.add_argument(
        "--leave-active",
        action="store_true",
        help="Do not mark the session completed",
    )
    daily_replay.add_argument(
        "--no-alerts",
        action="store_true",
        help="Suppress Telegram/email alerts during replay",
    )
    daily_replay.add_argument(
        "--simulate-real-orders",
        action="store_true",
        help=(
            "Replay order intents through the Zerodha REAL_DRY_RUN adapter and record "
            "broker-intent latency without calling Kite place_order."
        ),
    )
    daily_replay.add_argument(
        "--real-order-fixed-qty",
        type=int,
        default=1,
        help="Fixed simulated real quantity per order (default: 1 share).",
    )
    daily_replay.add_argument(
        "--real-entry-order-type",
        choices=["LIMIT", "MARKET"],
        default="LIMIT",
        help="Simulated real entry order type. LIMIT is default.",
    )
    daily_replay.add_argument(
        "--real-entry-max-slippage-pct",
        type=float,
        default=0.5,
        help="Marketable LIMIT entry protection percent for simulated real orders.",
    )
    daily_replay.add_argument(
        "--real-exit-max-slippage-pct",
        type=float,
        default=2.0,
        help="Protected LIMIT exit/flatten protection percent for simulated real orders.",
    )
    daily_replay.add_argument(
        "--multi",
        action="store_true",
        help="Replay canonical CPR variants concurrently (LONG + SHORT).",
    )
    daily_replay.add_argument("--notes", default=None, help="Optional free-text annotation")
    daily_replay.set_defaults(handler=_cmd_daily_replay)

    daily_sim = sub.add_parser(
        "daily-sim",
        help=(
            "Fast daily simulation: runs backtest engine for one date, "
            "stores results as PAPER. Runs canonical CPR variants by default."
        ),
    )
    daily_sim.add_argument("--trade-date", default=None, help="Trading date YYYY-MM-DD")
    _add_symbol_args(daily_sim)
    daily_sim.add_argument(
        "--strategy",
        default=None,
        choices=list(PAPER_ALLOWED_STRATEGIES),
        help="Single strategy to run (omit to run canonical CPR matrix variants).",
    )
    daily_sim.add_argument(
        "--strategy-params", default=None, help="JSON strategy parameter overrides"
    )
    daily_sim.add_argument(
        "--force",
        action="store_true",
        help="Recompute even if cached run already exists",
    )
    daily_sim.set_defaults(handler=_cmd_daily_sim)

    daily_live = sub.add_parser(
        "daily-live",
        help="Prepare runtime tables and start live paper session",
    )
    daily_live.add_argument("--trade-date", default=None, help="Trading date YYYY-MM-DD")
    _add_symbol_args(daily_live)
    _add_strategy_args(daily_live)
    daily_live.add_argument("--session-id", default=None, help="Custom session ID")
    daily_live.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume a stale/stopped session. When --session-id is omitted, the "
            "deterministic preset-based session_id is inferred from "
            "--strategy/--preset and --trade-date. Loads open positions from DB "
            "and monitors them to EOD. No new entries. Skips pre-filter and "
            "session creation."
        ),
    )
    _add_live_args(daily_live)
    daily_live.add_argument(
        "--skip-coverage",
        action="store_true",
        help="Skip runtime coverage validation and start live immediately.",
    )
    daily_live.add_argument("--notes", default=None, help="Optional free-text annotation")
    daily_live.add_argument(
        "--multi",
        action="store_true",
        help="Launch canonical CPR variants (LONG + SHORT) concurrently.",
    )
    daily_live.add_argument(
        "--feed-source",
        choices=["kite", "local"],
        default="kite",
        help="Market data source: 'kite' (live WebSocket) or 'local' (DuckDB replay).",
    )
    daily_live.set_defaults(handler=_cmd_daily_live)

    return parser


__all__ = ["build_paper_trading_parser"]
