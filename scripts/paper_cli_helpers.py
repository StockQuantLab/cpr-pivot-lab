"""CLI helper functions for paper-trading commands."""

from __future__ import annotations

import argparse
from typing import Any

from engine.paper_params import build_backtest_params_from_overrides
from scripts.paper_prepare import (
    CANONICAL_FULL_UNIVERSE_NAME,
    load_universe_symbols,
    resolve_prepare_symbols,
)


def parse_symbols_arg(value: str | None) -> list[str] | None:
    if not value:
        return None
    symbols = [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]
    return symbols or None


def default_full_universe_name(trade_date: str) -> str:
    """Return the canonical dated snapshot universe name for a date."""
    return f"full_{trade_date.replace('-', '_')}"


def load_saved_universe_for_guard(universe_name: str) -> list[str]:
    """Best-effort saved-universe load for defaulting and safety guards."""
    try:
        return load_universe_symbols(universe_name, read_only=True)
    except Exception:
        return []


def universe_diff_summary(left: list[str], right: list[str]) -> str:
    left_set = set(left)
    right_set = set(right)
    only_left = sorted(left_set - right_set)
    only_right = sorted(right_set - left_set)
    parts = [f"counts {len(left_set)} != {len(right_set)}"]
    if only_left:
        parts.append(f"only_left={only_left[:5]}{'...' if len(only_left) > 5 else ''}")
    if only_right:
        parts.append(f"only_right={only_right[:5]}{'...' if len(only_right) > 5 else ''}")
    return "; ".join(parts)


def apply_default_saved_universe(args: argparse.Namespace, trade_date: str) -> None:
    """Default paper runs to dated snapshot, then canonical fallback."""
    if parse_symbols_arg(getattr(args, "symbols", None)):
        return
    if bool(getattr(args, "all_symbols", False)):
        return
    if str(getattr(args, "universe_name", "") or "").strip():
        return
    dated_name = default_full_universe_name(trade_date)
    dated_symbols = load_saved_universe_for_guard(dated_name)
    canonical_symbols = load_saved_universe_for_guard(CANONICAL_FULL_UNIVERSE_NAME)
    if dated_symbols and canonical_symbols and set(dated_symbols) != set(canonical_symbols):
        raise SystemExit(
            f"Refusing default universe for {trade_date}: {dated_name} differs from "
            f"{CANONICAL_FULL_UNIVERSE_NAME} "
            f"({universe_diff_summary(dated_symbols, canonical_symbols)}). "
            "Repair explicitly with daily-prepare --refresh-universe-snapshot, or pass "
            "--universe-name intentionally."
        )
    if dated_symbols:
        args.universe_name = dated_name
        return
    if canonical_symbols:
        args.universe_name = CANONICAL_FULL_UNIVERSE_NAME
        return
    args.universe_name = dated_name


def resolve_cli_symbols(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    *,
    read_only: bool = True,
) -> list[str]:
    symbols = parse_symbols_arg(getattr(args, "symbols", None))
    use_all_symbols = bool(getattr(args, "all_symbols", False))
    universe_name = str(getattr(args, "universe_name", "") or "").strip() or None
    if use_all_symbols and (symbols or universe_name):
        parser.error("Use either --symbols, --universe-name, or --all-symbols, not more than one.")
    if symbols and universe_name:
        parser.error("Use either --symbols or --universe-name, not both.")
    if universe_name:
        resolved = resolve_prepare_symbols(
            None,
            None,
            universe_name=universe_name,
            read_only=read_only,
        )
        if not resolved:
            parser.error(f"Universe '{universe_name}' not found or empty.")
        return resolved
    return resolve_prepare_symbols(symbols, None, all_symbols=use_all_symbols, read_only=read_only)


def session_direction_suffix(
    _strategy: str,
    strategy_params: dict[str, Any] | None,
) -> str:
    direction = str((strategy_params or {}).get("direction_filter", "BOTH") or "BOTH").upper()
    if direction == "BOTH":
        return ""
    return direction.lower()


def workflow_session_suffix(mode: str, feed_source: str | None = None) -> str:
    mode_token = str(mode or "").strip().lower()
    feed_token = str(feed_source or "").strip().lower()
    if not feed_token:
        if mode_token == "replay":
            feed_token = "historical"
        elif mode_token == "live":
            feed_token = "kite"

    tokens: list[str] = []
    if mode_token:
        tokens.append(mode_token)
    if feed_token and feed_token != mode_token:
        tokens.append(feed_token)
    return f"-{'-'.join(tokens)}" if tokens else ""


def default_session_id(
    prefix: str,
    trade_date: str,
    strategy: str,
    strategy_params: dict[str, Any] | None = None,
    mode: str = "",
    feed_source: str | None = None,
) -> str:
    strategy_feed_source = str((strategy_params or {}).get("feed_source") or "").strip().lower()
    workflow_suffix = workflow_session_suffix(mode, feed_source or strategy_feed_source)
    direction = session_direction_suffix(strategy, strategy_params)
    direction_tag = f"-{direction}" if direction else ""
    return f"{prefix}-{strategy.lower()}{direction_tag}-{trade_date}{workflow_suffix}"


def real_order_notes(notes: str | None) -> str:
    marker = "ZERODHA_LIVE_REAL_ORDERS"
    if notes and marker in notes:
        return notes
    return f"{marker}: {notes}" if notes else marker


def simulated_real_order_notes(notes: str | None) -> str:
    marker = "ZERODHA_REAL_DRY_RUN_ORDERS"
    if notes and marker in notes:
        return notes
    return f"{marker}: {notes}" if notes else marker


def build_real_order_config(
    args: argparse.Namespace,
    *,
    strategy: str,
    strategy_params: dict[str, Any],
    feed_source: str,
) -> dict[str, Any] | None:
    real_orders = bool(getattr(args, "real_orders", False))
    simulate_real_orders = bool(getattr(args, "simulate_real_orders", False))
    if real_orders and simulate_real_orders:
        raise SystemExit("Use either --real-orders or --simulate-real-orders, not both.")
    if not real_orders and not simulate_real_orders:
        return None
    if real_orders and str(feed_source or "").lower() != "kite":
        raise SystemExit("--real-orders is supported only with --feed-source kite.")
    if (
        real_orders
        and bool(getattr(args, "multi", False))
        and not bool(getattr(args, "allow_multi_real_orders", False))
    ):
        raise SystemExit(
            "--multi --real-orders is intentionally blocked for the pilot. "
            "Pass --allow-multi-real-orders only for an explicitly approved small-capital pilot."
        )
    if real_orders and bool(getattr(args, "resume", False)):
        raise SystemExit(
            "--resume --real-orders is intentionally blocked for the pilot. "
            "Reconcile live Kite state manually before starting another real-routed session."
        )
    resolved = build_backtest_params_from_overrides(strategy, strategy_params)
    scale_out_pct = float(getattr(getattr(resolved, "cpr_levels", None), "scale_out_pct", 0.0) or 0)
    if scale_out_pct > 0:
        raise SystemExit("--real-orders does not support CPR partial scale-out yet.")
    fixed_quantity = getattr(args, "real_order_fixed_qty", 1)
    max_positions = getattr(args, "real_order_max_positions", 1)
    cash_budget = getattr(args, "real_order_cash_budget", 10_000.0)
    entry_slippage = getattr(args, "real_entry_max_slippage_pct", 0.5)
    exit_slippage = getattr(args, "real_exit_max_slippage_pct", 2.0)
    return {
        "enabled": True,
        "sizing_mode": str(getattr(args, "real_order_sizing_mode", "fixed-qty") or "fixed-qty")
        .replace("-", "_")
        .upper(),
        "fixed_quantity": int(fixed_quantity if fixed_quantity is not None else 1),
        "max_positions": int(max_positions if max_positions is not None else 1),
        "cash_budget": float(cash_budget if cash_budget is not None else 10_000.0),
        "require_account_cash_check": (
            False
            if simulate_real_orders
            else not bool(getattr(args, "real_order_skip_account_cash_check", False))
        ),
        "entry_order_type": str(getattr(args, "real_entry_order_type", "LIMIT") or "LIMIT").upper(),
        "entry_max_slippage_pct": float(entry_slippage if entry_slippage is not None else 0.5),
        "exit_max_slippage_pct": float(exit_slippage if exit_slippage is not None else 2.0),
        "product": "MIS",
        "exchange": "NSE",
        "adapter_mode": "REAL_DRY_RUN" if simulate_real_orders else "LIVE",
        "shadow": simulate_real_orders,
    }
