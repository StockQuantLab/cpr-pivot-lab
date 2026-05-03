"""Paper trading session control CLI."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import uuid4

import polars as pl

from config.settings import get_settings
from db.backtest_db import get_backtest_db
from db.duckdb import get_db
from db.paper_db import PaperSession, get_paper_db
from engine.cli_setup import configure_windows_asyncio, configure_windows_stdio, run_asyncio
from engine.command_lock import acquire_command_lock
from engine.cpr_atr_strategy import BacktestResult, CPRATRBacktest
from engine.kite_ticker_adapter import KiteTickerAdapter
from engine.live_market_data import IST
from engine.paper_reconciliation import reconcile_paper_session
from engine.paper_runtime import (
    _start_alert_dispatcher,
    build_backtest_params_from_overrides,
    dispatch_session_state_alert,
    flatten_session_positions,
    maybe_shutdown_alert_dispatcher,
    register_session_start,
    set_alerts_suppressed,
    write_admin_command,
)
from scripts import data_quality as _data_quality
from scripts.paper_archive import archive_completed_session
from scripts.paper_broker_cli import (
    _cmd_broker_reconcile,
    _cmd_close_position,
    _cmd_order,
    _cmd_pilot_check,
    _cmd_real_dry_run_order,
    _cmd_real_order,
    _load_json_list_arg,
)
from scripts.paper_cli_helpers import (
    apply_default_saved_universe as _apply_default_saved_universe,
)
from scripts.paper_cli_helpers import (
    build_real_order_config as _build_real_order_config,
)
from scripts.paper_cli_helpers import (
    default_session_id as _default_session_id,
)
from scripts.paper_cli_helpers import (
    load_saved_universe_for_guard as _load_saved_universe_for_guard,
)
from scripts.paper_cli_helpers import (
    parse_symbols_arg as _parse_symbols_arg,
)
from scripts.paper_cli_helpers import (
    real_order_notes as _real_order_notes,
)
from scripts.paper_cli_helpers import (
    resolve_cli_symbols as _resolve_cli_symbols,
)
from scripts.paper_cli_helpers import (
    universe_diff_summary as _universe_diff_summary,
)
from scripts.paper_cli_helpers import (
    workflow_session_suffix as _workflow_session_suffix,
)
from scripts.paper_coverage import (
    _build_runtime_coverage_fix_lines,
    _count_duckdb_rows_for_run_ids,
    _handle_coverage_gaps,
)
from scripts.paper_feed_audit import compare_feed_audit
from scripts.paper_handler_map import build_paper_trading_handler_map
from scripts.paper_live import run_live_session
from scripts.paper_prepare import (
    CANONICAL_FULL_UNIVERSE_NAME,
    ensure_canonical_universe,
    pre_filter_symbols_for_strategy,
    prepare_runtime_for_daily_paper,
    resolve_trade_date,
    snapshot_candidate_universe,
)
from scripts.paper_replay import ReplayDayPack, load_replay_day_packs, replay_session
from scripts.paper_strategy_params import (
    PAPER_ALLOWED_PRESETS,
    PAPER_ALLOWED_STRATEGIES,
    PAPER_STANDARD_MATRIX,
    _assert_cpr_only_strategy,
    _paper_default_strategy,
    _parse_json,
    _prepare_paper_multi_strategy_params,
    _resolve_paper_strategy_params,
    _resolved_strategy_config_dict,
    _session_execution_kwargs,
    _strategy_config_diffs,
    _with_resolved_strategy_metadata,
    normalize_strategy_params,
)
from scripts.paper_trading_parser import build_paper_trading_parser

logger = logging.getLogger(__name__)

__all__ = [
    "PAPER_ALLOWED_PRESETS",
    "PAPER_ALLOWED_STRATEGIES",
    "PAPER_STANDARD_MATRIX",
    "_build_runtime_coverage_fix_lines",
    "_count_duckdb_rows_for_run_ids",
    "_load_json_list_arg",
    "_parse_json",
]

_BROKER_HANDLER_EXPORTS = (
    _cmd_broker_reconcile,
    _cmd_close_position,
    _cmd_order,
    _cmd_pilot_check,
    _cmd_real_dry_run_order,
    _cmd_real_order,
)


def _pdb():
    return get_paper_db()


def _cleanup_feed_audit_retention(*, command_name: str) -> int:
    """Keep live/replay audit tables bounded to a rolling retention window."""

    retention_days = int(get_settings().feed_audit_retention_days or 0)
    if retention_days <= 0:
        return 0
    paper_db = _pdb()
    deleted = paper_db.cleanup_feed_audit_older_than(retention_days)
    deleted_alerts = paper_db.cleanup_alert_log_older_than(retention_days)
    if deleted > 0 or deleted_alerts > 0:
        logger.info(
            "%s purged %d paper_feed_audit row(s) and %d alert_log row(s) older than %d day(s)",
            command_name,
            deleted,
            deleted_alerts,
            retention_days,
        )
    return deleted + deleted_alerts


async def get_session(session_id: str):
    return _pdb().get_session(session_id)


async def get_active_sessions():
    return _pdb().get_active_sessions()


async def get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
    return _pdb().get_session_positions(session_id, symbol=symbol, statuses=statuses)


async def get_session_orders(
    session_id: str,
    symbol: str | None = None,
):
    return _pdb().get_session_orders(session_id, symbol=symbol)


async def get_feed_state(session_id: str):
    return _pdb().get_feed_state(session_id)


async def create_paper_session(**kwargs):
    strategy = str(kwargs.get("strategy") or "CPR_LEVELS")
    strategy_params = kwargs.get("strategy_params") or {}
    kwargs["strategy_params"] = _with_resolved_strategy_metadata(
        strategy,
        strategy_params,
        canonical_preset=strategy_params.get("_canonical_preset")
        if isinstance(strategy_params, dict)
        else None,
    )
    for key, value in _session_execution_kwargs(strategy, kwargs["strategy_params"]).items():
        kwargs.setdefault(key, value)
    return _pdb().create_session(**kwargs)


async def update_session_state(session_id: str, **kwargs):
    return _pdb().update_session(session_id, **kwargs)


def _warn_non_tradeable(symbols: list[str]) -> None:
    """Log a warning if any requested symbols are not in the instrument master."""
    from engine.kite_ingestion import tradeable_symbols

    tradeable = tradeable_symbols()
    if tradeable is None:
        return
    dead = set(symbols) - tradeable
    if dead:
        logger.warning(
            "Non-tradeable symbols requested: %s — these may have been delisted",
            ", ".join(sorted(dead)),
        )


async def _ensure_daily_session(
    *,
    session_id: str | None,
    trade_date: str,
    strategy: str,
    symbols: list[str],
    strategy_params: dict,
    notes: str | None,
    mode: str = "replay",
) -> PaperSession:
    requested_params = _with_resolved_strategy_metadata(
        strategy,
        strategy_params,
        canonical_preset=strategy_params.get("_canonical_preset")
        if isinstance(strategy_params, dict)
        else None,
    )
    execution_kwargs = _session_execution_kwargs(strategy, requested_params)
    requested_session_id = session_id or _default_session_id(
        "paper",
        trade_date,
        strategy,
        requested_params,
        mode,
    )
    session = await get_session(requested_session_id)

    direction = str((requested_params or {}).get("direction_filter", "BOTH") or "BOTH").upper()
    direction_label = f" {direction}" if direction != "BOTH" else ""
    session_name = f"{strategy}{direction_label} {trade_date}"

    if session is None:
        return await create_paper_session(
            session_id=requested_session_id,
            name=session_name,
            strategy=strategy,
            symbols=symbols,
            status="ACTIVE",
            strategy_params=requested_params,
            trade_date=trade_date,
            mode=mode,
            notes=notes,
            **execution_kwargs,
        )

    if mode == "live":
        session_execution_diffs: list[str] = []
        for key, requested_value in execution_kwargs.items():
            existing_value = getattr(session, key, None)
            if existing_value is not None and float(existing_value) != float(requested_value):
                session_execution_diffs.append(f"{key}: {existing_value!r} != {requested_value!r}")
        if session_execution_diffs:
            raise SystemExit(
                f"Existing live session {requested_session_id} execution sizing differs from "
                f"requested params: {'; '.join(session_execution_diffs)}. "
                "Stop/recreate the session; refusing drift."
            )
        existing_params = getattr(session, "strategy_params", {}) or {}
        existing_fingerprint = (
            existing_params.get("_strategy_config_fingerprint")
            if isinstance(existing_params, dict)
            else None
        )
        requested_fingerprint = requested_params.get("_strategy_config_fingerprint")
        if existing_fingerprint and existing_fingerprint != requested_fingerprint:
            raise SystemExit(
                f"Existing live session {requested_session_id} was created with different "
                "strategy params. Stop/recreate the session; refusing silent live/backtest drift."
            )
        if not existing_fingerprint:
            existing_config = _resolved_strategy_config_dict(
                str(getattr(session, "strategy", strategy) or strategy),
                existing_params if isinstance(existing_params, dict) else {},
            )
            requested_config = requested_params["_resolved_strategy_config"]
            diffs = _strategy_config_diffs(existing_config, requested_config)
            if diffs:
                raise SystemExit(
                    f"Existing live session {requested_session_id} params differ from requested "
                    f"params: {'; '.join(diffs[:8])}. Stop/recreate the session; refusing drift."
                )
        logger.info(
            "paper session_id %s already exists (status=%s); reusing existing live session",
            requested_session_id,
            getattr(session, "status", "UNKNOWN"),
        )
        return session

    fallback_session_id = f"{requested_session_id}-{uuid4().hex[:6]}"
    logger.warning(
        "paper session_id %s already exists (status=%s); creating fresh replay session %s",
        requested_session_id,
        getattr(session, "status", "UNKNOWN"),
        fallback_session_id,
    )
    return await create_paper_session(
        session_id=fallback_session_id,
        name=session_name,
        strategy=strategy,
        symbols=symbols,
        status="ACTIVE",
        strategy_params=requested_params,
        trade_date=trade_date,
        mode=mode,
        notes=notes,
        **execution_kwargs,
    )


def _enforce_kite_live_setup_gate(trade_date: str, symbols: list[str], *, feed_source: str) -> None:
    """Fail before live pre-filter/session creation if previous-day prerequisites are missing."""
    if str(feed_source or "").strip().lower() != "kite":
        return
    preparation = prepare_runtime_for_daily_paper(
        trade_date=trade_date,
        symbols=symbols,
        mode="live",
    )
    if not preparation.get("coverage_ready", False):
        _handle_coverage_gaps(preparation, trade_date=trade_date, mode="live")


async def _run_daily_workflow(
    *,
    mode: str,
    trade_date: str,
    symbols: list[str],
    strategy: str,
    strategy_params: dict,
    session_id: str | None,
    notes: str | None,
    replay_kwargs: dict[str, Any] | None = None,
    live_kwargs: dict[str, Any] | None = None,
    skip_preparation: bool = False,
) -> dict[str, Any]:
    _warn_non_tradeable(symbols)
    if skip_preparation:
        preparation = {
            "trade_date": trade_date,
            "requested_symbols": list(symbols),
            "coverage_ready": True,
            "mode": mode,
            "skipped": True,
        }
    else:
        preparation = prepare_runtime_for_daily_paper(
            trade_date=trade_date,
            symbols=symbols,
            mode=mode,
        )
        if not preparation.get("coverage_ready", False):
            _handle_coverage_gaps(preparation, trade_date=trade_date, mode=mode)

    session = await _ensure_daily_session(
        session_id=session_id,
        trade_date=trade_date,
        strategy=strategy,
        symbols=symbols,
        strategy_params=strategy_params,
        notes=notes,
        mode=mode,
    )

    if mode == "replay":
        payload = await replay_session(
            session_id=session.session_id,
            symbols=symbols,
            start_date=trade_date,
            end_date=trade_date,
            leave_active=bool((replay_kwargs or {}).get("leave_active")),
            notes=notes,
            preloaded_days=(replay_kwargs or {}).get("preloaded_days"),
        )
    elif mode == "live":
        payload = await run_live_session(
            session_id=session.session_id,
            symbols=symbols,
            poll_interval_sec=(live_kwargs or {}).get("poll_interval_sec"),
            candle_interval_minutes=(live_kwargs or {}).get("candle_interval_minutes"),
            max_cycles=(live_kwargs or {}).get("max_cycles"),
            complete_on_exit=bool((live_kwargs or {}).get("complete_on_exit")),
            auto_flatten_on_abnormal_exit=bool(
                (live_kwargs or {}).get("auto_flatten_on_abnormal_exit", True)
            ),
            allow_late_start_fallback=bool(
                (live_kwargs or {}).get("allow_late_start_fallback", False)
            ),
            real_order_config=(live_kwargs or {}).get("real_order_config"),
            notes=notes,
            ticker_adapter=(live_kwargs or {}).get("ticker_adapter"),
        )
    else:
        raise ValueError(f"Unsupported daily workflow mode: {mode}")

    payload["preparation"] = preparation
    payload["session_id"] = session.session_id
    return payload


def _last_available_trade_date(db) -> str:
    """Return the most recent source trade date available for runtime refreshes.

    Prefer `v_5min` because live paper depends on intraday bars. Fall back to
    `v_daily` so the command still works if only daily history is present.
    """
    for view in ("v_5min", "v_daily"):
        try:
            row = db.con.execute(f"SELECT MAX(date)::VARCHAR FROM {view}").fetchone()
        except Exception:
            continue
        if row and row[0]:
            return str(row[0])
    raise RuntimeError("No source trade dates found in v_5min or v_daily")


async def _cmd_daily_prepare(args: argparse.Namespace) -> None:
    trade_date = resolve_trade_date(args.trade_date)
    canonical_symbols: list[str] = []
    canonical_created = False
    if (
        bool(getattr(args, "all_symbols", False))
        and not _parse_symbols_arg(getattr(args, "symbols", None))
        and not str(getattr(args, "universe_name", "") or "").strip()
    ):
        with acquire_command_lock("runtime-writer", detail="runtime writer"):
            canonical_symbols, canonical_created = ensure_canonical_universe(trade_date=trade_date)
        if canonical_symbols:
            print(
                f"Using canonical universe '{CANONICAL_FULL_UNIVERSE_NAME}' "
                f"with {len(canonical_symbols)} symbols"
                + (" (created)." if canonical_created else "."),
                flush=True,
            )
    symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
    snapshot_universe_name = str(getattr(args, "snapshot_universe_name", "") or "").strip()
    if not snapshot_universe_name and bool(getattr(args, "all_symbols", False)):
        snapshot_universe_name = f"full_{trade_date.replace('-', '_')}"
    if snapshot_universe_name:
        existing_symbols = _load_saved_universe_for_guard(snapshot_universe_name)
        refresh_snapshot = bool(getattr(args, "refresh_universe_snapshot", False))
        if existing_symbols and set(existing_symbols) != set(symbols) and not refresh_snapshot:
            raise SystemExit(
                f"Refusing to overwrite existing universe '{snapshot_universe_name}' during "
                f"daily-prepare: it differs from resolved canonical symbols "
                f"({_universe_diff_summary(existing_symbols, symbols)}). "
                "Use --refresh-universe-snapshot only after confirming the canonical universe."
            )
        if existing_symbols and set(existing_symbols) == set(symbols):
            saved_count = len(existing_symbols)
            print(
                f"Universe '{snapshot_universe_name}' already matches canonical "
                f"({saved_count} symbols); not rewriting.",
                flush=True,
            )
        else:
            with acquire_command_lock("runtime-writer", detail="runtime writer"):
                saved_count = snapshot_candidate_universe(
                    snapshot_universe_name,
                    symbols,
                    trade_date=trade_date,
                    source="paper-daily-prepare",
                    notes=f"snapshot from daily-prepare trade_date={trade_date}",
                )
            print(
                f"Saved universe '{snapshot_universe_name}' with {saved_count} symbols.",
                flush=True,
            )
    # Detect whether this is a future/live date (no 5-min data yet) or a historical replay date.
    db = get_db()
    has_intraday = bool(
        db.con.execute(
            "SELECT 1 FROM intraday_day_pack WHERE trade_date = ?::DATE LIMIT 1",
            [trade_date],
        ).fetchone()
    )
    mode = "replay" if has_intraday else "live"
    payload = prepare_runtime_for_daily_paper(
        trade_date=trade_date,
        symbols=symbols,
        mode=mode,
    )
    if snapshot_universe_name:
        payload["snapshot_universe_name"] = snapshot_universe_name
        payload["snapshot_universe_count"] = saved_count
    if bool(getattr(args, "all_symbols", False)):
        payload["canonical_universe_name"] = CANONICAL_FULL_UNIVERSE_NAME
        payload["canonical_universe_count"] = len(canonical_symbols) or len(symbols)
        payload["canonical_universe_created"] = canonical_created
    if not has_intraday:
        mds_count = int(
            (
                db.con.execute(
                    "SELECT COUNT(*) FROM market_day_state WHERE trade_date = ?::DATE",
                    [trade_date],
                ).fetchone()
                or [0]
            )[0]
        )
        cpr_count = int(
            (
                db.con.execute(
                    "SELECT COUNT(*) FROM cpr_daily WHERE trade_date = ?::DATE",
                    [trade_date],
                ).fetchone()
                or [0]
            )[0]
        )
        if mds_count == 0 or cpr_count == 0:
            raise SystemExit(
                f"\n[CRITICAL] daily-prepare: next-day setup rows missing for {trade_date}.\n"
                f"  market_day_state = {mds_count} rows\n"
                f"  cpr_daily        = {cpr_count} rows\n\n"
                "The EOD pivot-build did not create forward-looking CPR rows.\n"
                "Fix (run in order):\n"
                f"  doppler run -- uv run pivot-build --table cpr --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table thresholds --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table state --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-build --table strategy --refresh-date {trade_date}\n"
                f"  doppler run -- uv run pivot-sync-replica --verify --trade-date {trade_date}\n"
                f"  doppler run -- uv run pivot-paper-trading daily-prepare --trade-date {trade_date} --all-symbols"
            )
        print(
            f"\nNote: {trade_date} has no intraday data yet (live session).\n"
            "Checking previous completed-day live prerequisites only.\n"
            f"  market_day_state rows for {trade_date}: {mds_count} ✓\n"
            f"  cpr_daily rows for {trade_date}: {cpr_count} ✓\n"
        )
    readiness = _data_quality.build_trade_date_readiness_report(trade_date)  # type: ignore[attr-defined]
    _data_quality.print_trade_date_readiness_report(readiness)  # type: ignore[attr-defined]
    payload["dq_readiness"] = readiness
    payload["dq_date"] = trade_date
    print(json.dumps(payload, default=str, indent=2))
    if not readiness.get("ready", False):
        raise SystemExit(1)


async def _cmd_daily_replay(args: argparse.Namespace) -> None:
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        if (
            str(getattr(args, "pack_source", "intraday_day_pack")).strip().lower()
            == "paper_feed_audit"
            and not str(getattr(args, "pack_source_session_id", "") or "").strip()
        ):
            raise SystemExit(
                "--pack-source paper_feed_audit requires --pack-source-session-id for daily-replay."
            )
        if getattr(args, "multi", False):
            await _cmd_daily_replay_multi(args)
            return

        trade_date = resolve_trade_date(args.trade_date)
        _apply_default_saved_universe(args, trade_date)
        symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
        strategy_params = _resolve_paper_strategy_params(args.strategy, args.strategy_params, args)

        suppress_alerts = bool(getattr(args, "no_alerts", False))
        if suppress_alerts:
            set_alerts_suppressed(True)
        try:
            filtered = pre_filter_symbols_for_strategy(
                trade_date, symbols, args.strategy, strategy_params
            )
            if not filtered:
                raise SystemExit(
                    "No symbols remain after replay pre-filtering. "
                    "Check min_price, CPR width, and narrowing filters."
                )
            n_removed = len(symbols) - len(filtered)
            if n_removed > 0:
                print(
                    f"Pre-filtered: {len(symbols)} → {len(filtered)} symbols"
                    f"  ({n_removed} removed: CPR width, narrowing, min_price)",
                    flush=True,
                )

            payload = await _run_daily_workflow(
                mode="replay",
                trade_date=trade_date,
                symbols=filtered,
                strategy=args.strategy,
                strategy_params=strategy_params,
                session_id=args.session_id,
                notes=args.notes,
                replay_kwargs={"leave_active": args.leave_active},
            )
            print(json.dumps(payload, default=str, indent=2))
        finally:
            _cleanup_feed_audit_retention(command_name="daily-replay")
            if suppress_alerts:
                set_alerts_suppressed(False)


async def _cmd_daily_live_resume(args: argparse.Namespace) -> None:
    """Resume a stale/stopped live session by session_id.

    Loads open positions directly from DB — no pre-filter, no fresh session.
    run_live_session() seeds open + closed positions and updates status to ACTIVE.
    """
    if bool(getattr(args, "real_orders", False)):
        raise SystemExit(
            "--resume --real-orders is intentionally blocked for the pilot. "
            "Reconcile live Kite state manually before starting another real-routed session."
        )
    trade_date = resolve_trade_date(getattr(args, "trade_date", None))
    strategy = _assert_cpr_only_strategy(
        getattr(args, "strategy", None) or _paper_default_strategy(get_settings()),
        source="strategy",
    )
    strategy_params = _resolve_paper_strategy_params(
        strategy, getattr(args, "strategy_params", None), args
    )
    strategy_params = {**strategy_params, "feed_source": getattr(args, "feed_source", "kite")}
    session_id = args.session_id or _default_session_id(
        "paper",
        trade_date,
        strategy,
        strategy_params,
        "live",
        getattr(args, "feed_source", None),
    )

    session = await get_session(session_id)
    if session is None:
        raise SystemExit(f"Session {session_id!r} not found in DB")

    resumable = {"FAILED", "STALE", "STOPPING", "CANCELLED"}
    if session.status not in resumable:
        raise SystemExit(
            f"Session {session_id!r} has status {session.status!r}. "
            f"Only {resumable} sessions can be resumed."
        )

    open_positions = await get_session_positions(session_id, statuses=["OPEN"])
    symbols = [p.symbol for p in open_positions]
    if not symbols:
        raise SystemExit(
            f"No OPEN positions in session {session_id!r} — nothing to resume. "
            f"(Use daily-live without --resume to start a fresh session.)"
        )

    print(
        f"Resuming {session_id!r}: {len(symbols)} open position(s): {symbols}",
        flush=True,
    )

    suppress_alerts = bool(getattr(args, "no_alerts", False))
    if suppress_alerts:
        set_alerts_suppressed(True)

    try:
        dispatch_session_state_alert(
            session_id=session_id,
            state="RESUMED",
            details=f"open_positions={len(symbols)}",
        )
        payload = await run_live_session(
            session_id=session_id,
            symbols=symbols,
            poll_interval_sec=getattr(args, "poll_interval_sec", None),
            candle_interval_minutes=getattr(args, "candle_interval_minutes", None),
            max_cycles=getattr(args, "max_cycles", None),
            complete_on_exit=getattr(args, "complete_on_exit", False),
            auto_flatten_on_abnormal_exit=False,
        )
    finally:
        if suppress_alerts:
            set_alerts_suppressed(False)

    print(json.dumps(payload, default=str, indent=2))


async def _cmd_daily_live(args: argparse.Namespace) -> None:
    if getattr(args, "multi", False):
        await _cmd_daily_live_multi(args)
        return

    if getattr(args, "resume", False):
        await _cmd_daily_live_resume(args)
        return

    trade_date = resolve_trade_date(args.trade_date)
    _apply_default_saved_universe(args, trade_date)
    symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
    strategy_params = _resolve_paper_strategy_params(args.strategy, args.strategy_params, args)

    direction = (strategy_params.get("direction_filter") or "BOTH").upper()
    if direction == "BOTH":
        raise SystemExit(
            "daily-live requires an explicit direction (LONG or SHORT).\n"
            "To run both sessions together use --multi:\n"
            "  doppler run -- uv run pivot-paper-trading daily-live \\\n"
            "    --multi --strategy CPR_LEVELS --trade-date today\n"
            "Or specify a preset with a single direction:\n"
            "  --preset CPR_LEVELS_RISK_LONG  or  --preset CPR_LEVELS_RISK_SHORT"
        )

    feed_source = getattr(args, "feed_source", "kite")
    suppress_alerts = bool(getattr(args, "no_alerts", False))
    real_order_config = _build_real_order_config(
        args,
        strategy=args.strategy,
        strategy_params=strategy_params,
        feed_source=feed_source,
    )
    session_notes = _real_order_notes(args.notes) if real_order_config else args.notes

    if feed_source == "kite":
        _reject_early_kite_live_start(
            trade_date,
            wait_for_open=bool(getattr(args, "wait_for_open", False)),
        )
        if bool(getattr(args, "wait_for_open", False)):
            await _wait_until_market_ready(trade_date)

    _enforce_kite_live_setup_gate(trade_date, symbols, feed_source=feed_source)

    filtered = pre_filter_symbols_for_strategy(
        trade_date,
        symbols,
        args.strategy,
        strategy_params,
        require_trade_date_rows=feed_source == "kite",
    )
    if not filtered:
        raise SystemExit(
            "No symbols remain after live pre-filtering. "
            "Check min_price, CPR width, and narrowing filters."
        )
    n_removed = len(symbols) - len(filtered)
    if n_removed > 0:
        print(
            f"Pre-filtered: {len(symbols)} → {len(filtered)} symbols"
            f"  ({n_removed} removed: CPR width, narrowing, min_price)",
            flush=True,
        )

    # Build live_kwargs for live execution.
    live_kwargs: dict[str, Any] = {
        "poll_interval_sec": args.poll_interval_sec,
        "candle_interval_minutes": args.candle_interval_minutes,
        "max_cycles": args.max_cycles,
        "complete_on_exit": args.complete_on_exit,
        "allow_late_start_fallback": bool(getattr(args, "allow_late_start_fallback", False)),
        "real_order_config": real_order_config,
    }

    if feed_source == "local":
        from engine.local_ticker_adapter import LocalTickerAdapter

        local_ticker = LocalTickerAdapter(
            trade_date=trade_date,
            symbols=filtered,
            candle_interval_minutes=args.candle_interval_minutes or 5,
        )
        live_kwargs["ticker_adapter"] = local_ticker
        strategy_params = {**strategy_params, "feed_source": "local"}

    if suppress_alerts:
        set_alerts_suppressed(True)

    try:
        payload = await _run_daily_workflow(
            mode="live",
            trade_date=trade_date,
            symbols=filtered,
            strategy=args.strategy,
            strategy_params=strategy_params,
            session_id=args.session_id,
            notes=session_notes,
            skip_preparation=bool(args.skip_coverage),
            live_kwargs=live_kwargs,
        )
    finally:
        _cleanup_feed_audit_retention(command_name="daily-live")
        if suppress_alerts:
            set_alerts_suppressed(False)

    print(json.dumps(payload, default=str, indent=2))


def _namespace_to_argv(args: argparse.Namespace, *, exclude: set[str] | None = None) -> list[str]:
    """Convert a parsed argparse Namespace back into CLI argv.

    Skips None values, booleans that are False, and any keys in *exclude*.
    """
    exclude = exclude or set()
    argv: list[str] = []
    for key, value in vars(args).items():
        if key in exclude or value is None or value is False:
            continue
        flag = f"--{key.replace('_', '-')}"
        if value is True:
            argv.append(flag)
        else:
            argv.extend([flag, str(value)])
    return argv


def _variant_exit_summary(result: Any) -> dict[str, Any]:
    """Normalize a variant result for retry decisions and logging."""
    if isinstance(result, Exception):
        return {
            "status": "ERROR",
            "terminal_reason": type(result).__name__,
            "last_bar_hhmm": None,
            "cycles": None,
            "closed_bars": None,
        }
    if not isinstance(result, dict):
        return {
            "status": "unknown",
            "terminal_reason": type(result).__name__,
            "last_bar_hhmm": None,
            "cycles": None,
            "closed_bars": None,
        }

    last_bar_ts = result.get("last_bar_ts")
    last_bar_hhmm: str | None = None
    if isinstance(last_bar_ts, str) and last_bar_ts:
        try:
            last_bar_hhmm = datetime.fromisoformat(last_bar_ts).astimezone(IST).strftime("%H:%M")
        except ValueError:
            last_bar_hhmm = None

    return {
        "status": str(result.get("final_status") or "unknown"),
        "terminal_reason": str(result.get("terminal_reason") or result.get("stop_reason") or ""),
        "last_bar_hhmm": last_bar_hhmm,
        "cycles": result.get("cycles"),
        "closed_bars": result.get("closed_bars"),
    }


def _should_retry_variant_exit(
    summary: dict[str, Any],
    *,
    current_hhmm: str,
    entry_window_closed_hhmm: str,
    eod_cutoff_hhmm: str,
) -> tuple[bool, str]:
    """Decide whether a variant exit should be restarted."""
    status = str(summary.get("status") or "unknown").upper()
    terminal_reason = str(summary.get("terminal_reason") or "").lower()
    last_bar_hhmm = str(summary.get("last_bar_hhmm") or "")

    if current_hhmm >= eod_cutoff_hhmm:
        return False, "past EOD cutoff"
    if status == "SKIPPED":
        return False, "no symbols remain after pre-filtering"
    if status == "COMPLETED":
        if terminal_reason in {"complete_on_exit", "entry_window_closed", "local_feed_exhausted"}:
            return False, terminal_reason or "completed"
        compare_hhmm = last_bar_hhmm or current_hhmm
        if compare_hhmm < entry_window_closed_hhmm:
            return True, f"completed early at {compare_hhmm}"
        return False, f"completed at {compare_hhmm}"
    if status == "NO_TRADES_ENTRY_WINDOW_CLOSED":
        return True, terminal_reason or "entry window closed early"
    if status in {"FAILED", "STALE", "MISSING", "NO_ACTIVE_SYMBOLS", "ERROR", "ACTIVE"}:
        # Feed stale after entry window: positions were auto-flattened. A fresh session
        # has no open positions and can't take new trades — restart is pointless.
        if terminal_reason == "feed_stale" and current_hhmm >= entry_window_closed_hhmm:
            return False, "feed_stale after entry window — no new trades or open positions"
        return True, terminal_reason or status.lower()
    return True, terminal_reason or status.lower()


async def _run_multi_variants(
    args: argparse.Namespace,
    *,
    mode: str,
    read_only: bool,
    banner_label: str,
    execute_variant: Any,
    retry_on_early_exit: bool = False,
) -> None:
    """Run multiple paper variants with shared orchestration."""
    strategy_override = _assert_cpr_only_strategy(
        getattr(args, "strategy", None) or _paper_default_strategy(get_settings()),
        source="strategy",
    )

    if args.strategy_params:
        print(
            "WARNING: --multi with --strategy-params; strategy defaults "
            "will be applied per-variant. Use --multi alone for full control."
        )

    strategy_upper = strategy_override.upper()
    variants = [
        (label, strat, params)
        for label, strat, params in PAPER_STANDARD_MATRIX
        if strat == strategy_upper
    ]
    if not variants:
        print(f"ERROR: No variants match strategy '{strategy_override}'")
        raise SystemExit(1)

    trade_date = resolve_trade_date(args.trade_date)
    all_symbols = _resolve_cli_symbols(build_parser(), args, read_only=read_only)

    preparation = prepare_runtime_for_daily_paper(
        trade_date=trade_date,
        symbols=all_symbols,
        mode=mode,
    )
    if not preparation.get("coverage_ready", False):
        _handle_coverage_gaps(preparation, trade_date=trade_date, mode=mode)

    # Pre-compute normalized params + filtered symbols per variant.
    variant_setup: list[tuple[str, str, dict[str, Any], list[str]]] = []
    require_trade_date_rows = (
        mode == "live" and str(getattr(args, "feed_source", "kite")).strip().lower() == "kite"
    )
    for label, strategy, base_params in variants:
        normalized_params = _prepare_paper_multi_strategy_params(label, strategy, base_params)
        filtered = pre_filter_symbols_for_strategy(
            trade_date,
            all_symbols,
            strategy,
            normalized_params,
            require_trade_date_rows=require_trade_date_rows,
        )
        variant_setup.append((label, strategy, normalized_params, filtered))

    print(
        f"\n{'=' * 60}\n"
        f"  {banner_label} {len(variants)} variant(s) for {trade_date}\n"
        f"  Full universe: {len(all_symbols)} symbols\n"
        f"{'=' * 60}",
        flush=True,
    )

    register_session_start()

    retry_max = 5
    retry_wait_base_sec = 10
    # Don't retry COMPLETED after entry window + buffer (10:15 + 15 min).
    entry_window_closed_hhmm = "10:30"
    # Hard stop — no retries after EOD.
    eod_cutoff_hhmm = "14:30"

    async def _execute_with_retry(
        label: str, strategy: str, normalized_params: dict[str, Any], filtered: list[str]
    ) -> Any:
        """Run a variant; auto-restart if it exits prematurely before EOD.

        Premature exits that trigger a retry:
          - Any exception
          - final_status FAILED / STALE / MISSING / NO_ACTIVE_SYMBOLS
          - final_status COMPLETED before 10:30 (entry window not yet closed)
          - final_status NO_TRADES_ENTRY_WINDOW_CLOSED before 10:30

        live retries reuse the original session_id so the same session can be
        resumed; replay still creates a fallback session ID (base-{uuid[:6]})
        when the original session_id already exists.
        """
        if not retry_on_early_exit:
            try:
                return await execute_variant(label, strategy, normalized_params, filtered)
            except Exception as exc:
                logger.exception("[%s] Variant raised exception", label)
                return exc

        result: Any = None
        for attempt in range(retry_max + 1):
            try:
                result = await execute_variant(label, strategy, normalized_params, filtered)
            except Exception as exc:
                result = exc
                logger.exception("[%s] Variant raised exception (attempt %d)", label, attempt)

            now_hhmm = datetime.now(IST).strftime("%H:%M")
            summary = _variant_exit_summary(result)
            final_status = summary["status"]

            # ── decide whether to retry ───────────────────────────────────
            if attempt >= retry_max:
                break
            should_retry, retry_reason = _should_retry_variant_exit(
                summary,
                current_hhmm=now_hhmm,
                entry_window_closed_hhmm=entry_window_closed_hhmm,
                eod_cutoff_hhmm=eod_cutoff_hhmm,
            )
            if not should_retry:
                break
            # ─────────────────────────────────────────────────────────────

            wait_sec = retry_wait_base_sec * (attempt + 1)
            logger.warning(
                "[%s] Variant exited early: status=%s reason=%s last_bar=%s at=%s "
                "restart=%d/%d wait_sec=%d",
                label,
                final_status,
                summary["terminal_reason"] or retry_reason,
                summary["last_bar_hhmm"] or "n/a",
                now_hhmm,
                attempt + 1,
                retry_max,
                wait_sec,
            )
            print(
                f"\n  [{label}] Variant exited early: status={final_status}"
                f" reason={summary['terminal_reason'] or retry_reason}"
                f" last_bar={summary['last_bar_hhmm'] or 'n/a'} at {now_hhmm}"
                f" — restart {attempt + 1}/{retry_max} in {wait_sec}s",
                flush=True,
            )
            await asyncio.sleep(wait_sec)

        if isinstance(result, Exception):
            logger.error(
                "[%s] Variant finished with exception after retries",
                label,
                exc_info=(type(result), result, result.__traceback__),
            )
        else:
            final_summary = _variant_exit_summary(result)
            if str(final_summary.get("status") or "").upper() in {"FAILED", "STALE", "MISSING"}:
                logger.error(
                    "[%s] Variant finished unhealthy after retries: %s", label, final_summary
                )
        return result

    results = list(
        await asyncio.gather(
            *[
                _execute_with_retry(label, strategy, normalized_params, filtered)
                for label, strategy, normalized_params, filtered in variant_setup
            ],
            return_exceptions=True,
        )
    )

    await maybe_shutdown_alert_dispatcher()

    # Report results
    print(f"\n{'=' * 60}")
    for (label, _strategy, _params, _filtered), result in zip(variant_setup, results, strict=True):
        summary = _variant_exit_summary(result)
        if isinstance(result, Exception):
            print(f"  {label:20s} ERROR: {result}")
        else:
            reason = summary["terminal_reason"]
            suffix = f" reason={reason}" if reason else ""
            print(f"  {label:20s} status={summary['status']}{suffix}")
    print(f"{'=' * 60}\n")

    payloads = [
        {"label": label, "result": str(result) if isinstance(result, Exception) else result}
        for (label, _s, _p, _f), result in zip(variant_setup, results, strict=True)
    ]
    print(json.dumps({"trade_date": trade_date, "variants": payloads}, default=str, indent=2))


# Market opens at 09:15 IST. Waiting until 09:16 before connecting the
# WebSocket avoids the pre-market→regular cycle event at 09:15 (KiteTicker
# disconnects + reconnects when the segment flips), and gives the 9:15 candle
# roughly a minute of ticks before prefetch so direction resolution from the
# live candle has data to work with. Before 09:16 we sleep; after, we start
# immediately. See docs/PARITY_INCIDENT_LOG.md 2026-04-15 entry.
MARKET_READY_HHMM = "09:16"


async def _wait_until_market_ready(trade_date: str) -> None:
    now = datetime.now(IST)
    if now.date().isoformat() != trade_date:
        return
    ready_hh, ready_mm = (int(x) for x in MARKET_READY_HHMM.split(":"))
    ready_at = now.replace(hour=ready_hh, minute=ready_mm, second=0, microsecond=0)
    if now >= ready_at:
        return
    wait_sec = (ready_at - now).total_seconds()
    print(
        f"  Waiting {wait_sec:.0f}s until {MARKET_READY_HHMM} IST "
        f"(market open + 1min) before subscribing WebSocket...",
        flush=True,
    )
    await asyncio.sleep(wait_sec)


def _reject_early_kite_live_start(trade_date: str, *, wait_for_open: bool) -> None:
    """Fail fast for live Kite starts before the market-ready time."""
    if wait_for_open:
        return
    now = datetime.now(IST)
    if now.date().isoformat() != trade_date:
        return
    ready_hh, ready_mm = (int(x) for x in MARKET_READY_HHMM.split(":"))
    ready_at = now.replace(hour=ready_hh, minute=ready_mm, second=0, microsecond=0)
    if now < ready_at:
        raise SystemExit(
            f"daily-live --feed-source kite should be launched at/after {MARKET_READY_HHMM} IST. "
            f"Current time is {now.strftime('%H:%M:%S')} IST. "
            "Relaunch after market-ready time, or pass --wait-for-open to intentionally sleep."
        )


async def _cmd_daily_live_multi(args: argparse.Namespace) -> None:
    """Run multiple paper variants concurrently in a single process.

    DuckDB allows only one writer at a time. Running variants in-process
    with asyncio.gather avoids multi-process file-lock conflicts on
    paper.duckdb while allowing all variants to poll simultaneously.
    """
    if bool(getattr(args, "real_orders", False)):
        raise SystemExit(
            "--multi --real-orders is intentionally blocked for the pilot. "
            "Run one LONG or one SHORT real-routed session first."
        )
    trade_date = resolve_trade_date(args.trade_date)
    _apply_default_saved_universe(args, trade_date)
    all_symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)

    feed_source = getattr(args, "feed_source", "kite")
    suppress_alerts = bool(getattr(args, "no_alerts", False))
    if feed_source == "kite":
        _reject_early_kite_live_start(
            trade_date,
            wait_for_open=bool(getattr(args, "wait_for_open", False)),
        )
        _enforce_kite_live_setup_gate(trade_date, all_symbols, feed_source=feed_source)

    local_union_symbols: list[str] | None = None
    strategy_upper = _assert_cpr_only_strategy(
        getattr(args, "strategy", None) or _paper_default_strategy(get_settings()),
        source="strategy",
    ).upper()
    raw_variants = [
        (label, strat, params)
        for label, strat, params in PAPER_STANDARD_MATRIX
        if strat == strategy_upper
    ]
    variant_setup: list[tuple[str, str, dict[str, Any], list[str]]] = []
    for label, strategy, base_params in raw_variants:
        normalized_params = _prepare_paper_multi_strategy_params(label, strategy, base_params)
        filtered = pre_filter_symbols_for_strategy(
            trade_date,
            all_symbols,
            strategy,
            normalized_params,
            require_trade_date_rows=feed_source == "kite",
        )
        variant_setup.append((label, strategy, normalized_params, filtered))

    if feed_source == "local":
        local_union_symbols = sorted({s for _, _, _, syms in variant_setup for s in syms})

    if feed_source == "kite":
        # Pre-create sessions with PLANNING status so the dashboard shows them
        # immediately at startup, before the 09:16 market-open wait completes.
        for label, strategy, normalized_params, filtered in variant_setup:
            if not filtered:
                continue
            session_id = f"{label}-{trade_date}{_workflow_session_suffix('live', feed_source)}"
            existing = await get_session(session_id)
            if existing is None:
                direction = str(normalized_params.get("direction_filter", "BOTH") or "BOTH").upper()
                direction_label = f" {direction}" if direction != "BOTH" else ""
                await create_paper_session(
                    session_id=session_id,
                    name=f"{strategy}{direction_label} {trade_date}",
                    strategy=strategy,
                    symbols=filtered,
                    status="PLANNING",
                    strategy_params={**normalized_params, "feed_source": feed_source},
                    trade_date=trade_date,
                    mode="live",
                    notes="Waiting for 09:16 market open",
                    **_session_execution_kwargs(strategy, normalized_params),
                )
                print(f"  [pre-create] {session_id} (PLANNING)", flush=True)
        # Force a single replica sync after all sessions are written so the
        # dashboard sees every PLANNING session immediately, bypassing the
        # 5-second debounce that otherwise only captures the first session.
        _pdb().force_sync()
        if bool(getattr(args, "wait_for_open", False)):
            await _wait_until_market_ready(trade_date)

    if feed_source == "local":
        from engine.local_ticker_adapter import LocalTickerAdapter

        shared_ticker: Any = LocalTickerAdapter(
            trade_date=trade_date,
            symbols=local_union_symbols or all_symbols,
            candle_interval_minutes=getattr(args, "candle_interval_minutes", None) or 5,
        )
    else:
        shared_ticker = KiteTickerAdapter()

    if suppress_alerts:
        set_alerts_suppressed(True)

    live_kwargs = {
        "poll_interval_sec": getattr(args, "poll_interval_sec", None),
        "candle_interval_minutes": getattr(args, "candle_interval_minutes", None),
        "max_cycles": getattr(args, "max_cycles", None),
        "complete_on_exit": getattr(args, "complete_on_exit", False),
        "allow_late_start_fallback": bool(getattr(args, "allow_late_start_fallback", False)),
    }

    async def _execute_variant(
        label: str, strategy: str, normalized_params: dict[str, Any], filtered: list[str]
    ) -> dict[str, Any]:
        session_id = f"{label}-{trade_date}{_workflow_session_suffix('live', feed_source)}"

        if not filtered:
            return {
                "label": label,
                "session_id": session_id,
                "final_status": "SKIPPED",
                "reason": "no symbols remain after pre-filtering",
            }

        direction = (normalized_params.get("direction_filter") or "BOTH").upper()
        skip_rvol = normalized_params.get("skip_rvol_check", False)
        normalized_params = {**normalized_params, "feed_source": feed_source}
        rvol_tag = "rvol=OFF" if skip_rvol else "rvol=ON"
        print(
            f"  {label}: {len(all_symbols)} -> {len(filtered)} candidates"
            f"  dir={direction} {rvol_tag} session={session_id}",
            flush=True,
        )

        session = await _ensure_daily_session(
            session_id=session_id,
            trade_date=trade_date,
            strategy=strategy,
            symbols=filtered,
            strategy_params=normalized_params,
            notes=args.notes,
            mode="live",
        )

        return await run_live_session(
            session_id=session.session_id,
            symbols=filtered,
            ticker_adapter=shared_ticker,
            poll_interval_sec=live_kwargs.get("poll_interval_sec"),
            candle_interval_minutes=live_kwargs.get("candle_interval_minutes"),
            max_cycles=live_kwargs.get("max_cycles"),
            complete_on_exit=bool(live_kwargs.get("complete_on_exit")),
            auto_flatten_on_abnormal_exit=True,
            allow_late_start_fallback=bool(live_kwargs.get("allow_late_start_fallback", False)),
            notes=args.notes,
        )

    try:
        await _run_multi_variants(
            args,
            mode="live",
            read_only=True,
            banner_label="Launching",
            execute_variant=_execute_variant,
            retry_on_early_exit=not bool(getattr(args, "complete_on_exit", False)),
        )
    finally:
        _cleanup_feed_audit_retention(command_name="daily-live --multi")
        shared_ticker.close()
        if suppress_alerts:
            set_alerts_suppressed(False)


async def _cmd_daily_replay_multi(args: argparse.Namespace) -> None:
    """Replay multiple paper variants concurrently in a single process."""
    if str(getattr(args, "pack_source", "intraday_day_pack")).strip().lower() == "paper_feed_audit":
        raise SystemExit(
            "--multi does not support --pack-source paper_feed_audit yet. "
            "Replay one archived session at a time."
        )
    trade_date = resolve_trade_date(args.trade_date)
    _apply_default_saved_universe(args, trade_date)
    all_symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)

    # Preload day packs once for the union of all filtered symbols.
    # We need the symbols before entering _run_multi_variants, so we resolve
    # the variant setup here and pass the packs via closure.
    strategy_upper = _assert_cpr_only_strategy(
        getattr(args, "strategy", None) or _paper_default_strategy(get_settings()),
        source="strategy",
    ).upper()
    raw_variants = [
        (label, strat, params)
        for label, strat, params in PAPER_STANDARD_MATRIX
        if strat == strategy_upper
    ]
    if not raw_variants:
        print(f"ERROR: No variants match strategy '{args.strategy}'")
        raise SystemExit(1)

    variant_setup: list[tuple[str, str, dict[str, Any], list[str]]] = []
    for label, strategy, base_params in raw_variants:
        normalized_params = _prepare_paper_multi_strategy_params(label, strategy, base_params)
        normalized_params = {**normalized_params, "feed_source": "historical"}
        filtered = pre_filter_symbols_for_strategy(
            trade_date, all_symbols, strategy, normalized_params
        )
        variant_setup.append((label, strategy, normalized_params, filtered))

    union_symbols = sorted({s for _, _, _, syms in variant_setup for s in syms})
    union_packs: list[ReplayDayPack] = load_replay_day_packs(
        symbols=union_symbols,
        start_date=trade_date,
        end_date=trade_date,
    )

    suppress_alerts = bool(getattr(args, "no_alerts", False))
    if suppress_alerts:
        set_alerts_suppressed(True)
    try:

        async def _execute_variant(
            label: str, strategy: str, normalized_params: dict[str, Any], filtered: list[str]
        ) -> dict[str, Any]:
            session_id = f"{label}-{trade_date}{_workflow_session_suffix('replay', 'historical')}"

            if not filtered:
                return {
                    "label": label,
                    "session_id": session_id,
                    "final_status": "SKIPPED",
                    "reason": "no symbols remain after pre-filtering",
                }

            direction = (normalized_params.get("direction_filter") or "BOTH").upper()
            skip_rvol = normalized_params.get("skip_rvol_check", False)
            rvol_tag = "rvol=OFF" if skip_rvol else "rvol=ON"
            logger.info(
                "Paper replay variant start label=%s strategy=%s session_id=%s candidates=%d dir=%s %s",
                label,
                strategy,
                session_id,
                len(filtered),
                direction,
                rvol_tag,
            )
            print(
                f"  {label}: {len(all_symbols)} -> {len(filtered)} candidates"
                f"  dir={direction} {rvol_tag} session={session_id}",
                flush=True,
            )
            payload = await _run_daily_workflow(
                mode="replay",
                trade_date=trade_date,
                symbols=filtered,
                strategy=strategy,
                strategy_params=normalized_params,
                session_id=session_id,
                notes=args.notes,
                replay_kwargs={
                    "leave_active": args.leave_active,
                    "preloaded_days": union_packs,
                },
                skip_preparation=True,
            )
            logger.info(
                "Paper replay variant done label=%s session_id=%s final_status=%s",
                label,
                session_id,
                payload.get("final_status"),
            )
            return payload

        await _run_multi_variants(
            args,
            mode="replay",
            read_only=True,
            banner_label=f"Replaying — loaded {len(union_packs)} pack rows for {len(union_symbols)} candidates",
            execute_variant=_execute_variant,
        )
    finally:
        _cleanup_feed_audit_retention(command_name="daily-replay --multi")
        if suppress_alerts:
            set_alerts_suppressed(False)


def _paper_run_id(backtest_run_id: str) -> str:
    """Derive a PAPER-namespaced run_id that never collides with BACKTEST run_ids."""
    return hashlib.sha256(f"PAPER:{backtest_run_id}".encode()).hexdigest()[:12]


def _run_sim_variant(
    *,
    trade_date: str,
    symbols: list[str],
    strategy: str,
    strategy_params: dict[str, Any] | None,
    force: bool = False,
) -> dict[str, Any]:
    """Run one daily-sim variant using the fast backtest engine, store as PAPER.

    Each execution gets a unique run_id. We derive a PAPER-specific run_id via
    _paper_run_id() *after* computation, then rewrite the run_id in the result
    before saving to ensure PAPER and BACKTEST runs never collide.
    """
    params = build_backtest_params_from_overrides(
        strategy, normalize_strategy_params(strategy_params)
    )
    db = get_db()
    backtest = CPRATRBacktest(params=params, db=db)
    t0 = time.time()
    result = backtest.run(
        symbols=symbols,
        start=trade_date,
        end=trade_date,
        verbose=False,
    )
    elapsed = round(time.time() - t0, 2)

    # Derive a PAPER-specific run_id and rewrite the result before saving.
    paper_id = _paper_run_id(result.run_id)
    paper_df = (
        result.df.with_columns(pl.lit(paper_id).alias("run_id"))
        if not result.df.is_empty()
        else result.df
    )
    paper_result = BacktestResult(
        run_id=paper_id,
        params=result.params,
        _loaded_df=paper_df,
        run_context={**result.run_context, "run_id": paper_id},
    )
    paper_result.save_to_db(db, execution_mode="PAPER")

    df = paper_result.df
    trades = int(df.height)
    total_pnl = round(float(df["profit_loss"].sum()) if trades else 0.0, 2)
    wins = int((df["profit_loss"] > 0).sum()) if trades else 0
    return {
        "run_id": paper_id,
        "strategy": strategy,
        "strategy_params": normalize_strategy_params(strategy_params),
        "trade_date": trade_date,
        "symbol_count": len(symbols),
        "trades": trades,
        "wins": wins,
        "win_rate": round(wins / trades * 100, 1) if trades else 0.0,
        "total_pnl": total_pnl,
        "elapsed_sec": elapsed,
    }


async def _cmd_daily_sim(args: argparse.Namespace) -> None:
    """Fast daily simulation: runs backtest engine for one date, stores as PAPER."""
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        trade_date = resolve_trade_date(args.trade_date)
        _apply_default_saved_universe(args, trade_date)
        symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
        strategy_name = _assert_cpr_only_strategy(args.strategy or "CPR_LEVELS", source="strategy")
        strategy_params = _resolve_paper_strategy_params(strategy_name, args.strategy_params, args)
        force = getattr(args, "force", False)

        if args.strategy:
            # Single variant
            variants = [(strategy_name, strategy_params)]
        else:
            # Run both canonical CPR variants (LONG + SHORT)
            variants = [
                (strategy, _prepare_paper_multi_strategy_params(label, strategy, params))
                for label, strategy, params in PAPER_STANDARD_MATRIX
            ]

        results = []
        for strategy, params in variants:
            variant_result = _run_sim_variant(
                trade_date=trade_date,
                symbols=symbols,
                strategy=strategy,
                strategy_params=params,
                force=force,
            )
            results.append(variant_result)
            direction = (params or {}).get("direction_filter", "BOTH")
            skip_rvol = (params or {}).get("skip_rvol_check", False)
            rvol_tag = "rvol=OFF" if skip_rvol else "rvol=ON"
            print(
                f"  {strategy} {direction} ({rvol_tag}): "
                f"trades={variant_result['trades']} pnl=₹{variant_result['total_pnl']:,.0f} "
                f"run_id={variant_result['run_id']} ({variant_result['elapsed_sec']}s)"
            )

        total_trades = sum(r["trades"] for r in results)
        total_pnl = round(sum(r["total_pnl"] for r in results), 2)
        print(
            f"\n  Total: {total_trades} trades  ₹{total_pnl:,.0f} across {len(results)} variant(s)"
        )
        print(json.dumps({"trade_date": trade_date, "variants": results}, default=str, indent=2))


async def _cmd_start(args: argparse.Namespace) -> None:
    strategy = _assert_cpr_only_strategy(args.strategy, source="strategy")
    strategy_params = _resolve_paper_strategy_params(strategy, args.strategy_params, args)
    strategy_params = _with_resolved_strategy_metadata(strategy, strategy_params)
    session = _pdb().create_session(
        session_id=args.session_id,
        name=args.name,
        strategy=strategy,
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        status="ACTIVE" if args.activate else "PLANNING",
        strategy_params=strategy_params,
        created_by=args.created_by,
        flatten_time=args.flatten_time,
        stale_feed_timeout_sec=args.stale_feed_timeout_sec,
        max_daily_loss_pct=args.max_daily_loss_pct,
        max_positions=args.max_positions,
        max_position_pct=args.max_position_pct,
        notes=args.notes,
    )
    print(json.dumps(asdict(session), default=str, indent=2))


async def _cmd_status(args: argparse.Namespace) -> None:
    if args.session_id:
        session = _pdb().get_session(args.session_id)
        if session is None:
            print(f"Session {args.session_id!r} not found.")
            return
        positions = _pdb().get_open_positions(args.session_id)
        orders = _pdb().get_session_orders(args.session_id)
        feed_state = _pdb().get_feed_state(args.session_id)
        if getattr(args, "summary", False):
            payload = {
                "session_id": session.session_id,
                "strategy": session.strategy,
                "status": session.status,
                "mode": session.mode,
                "symbol_count": len(session.symbols or []),
                "open_positions": len(positions),
                "orders": len(orders),
                "latest_candle_ts": session.latest_candle_ts,
                "stale_feed_at": session.stale_feed_at,
                "feed_status": feed_state.status if feed_state else None,
            }
        else:
            payload = {
                "session": asdict(session),
                "open_positions": [asdict(p) for p in positions],
                "orders": [asdict(o) for o in orders[:25]],
                "feed_state": asdict(feed_state) if feed_state else None,
            }
    else:
        sessions = _pdb().get_active_sessions()
        if getattr(args, "summary", False):
            payload = {
                "active_sessions": [
                    {
                        "session_id": session.session_id,
                        "strategy": session.strategy,
                        "status": session.status,
                        "mode": session.mode,
                        "symbol_count": len(session.symbols or []),
                        "latest_candle_ts": session.latest_candle_ts,
                        "stale_feed_at": session.stale_feed_at,
                    }
                    for session in sessions
                ]
            }
        else:
            payload = {"active_sessions": [asdict(s) for s in sessions]}
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_universes(args: argparse.Namespace) -> None:
    """List saved universe snapshots."""
    db = get_db()
    rows = db.list_universes()
    name_filter = str(getattr(args, "name", "") or "").strip()
    if name_filter:
        rows = [row for row in rows if str(row.get("name", "")).strip() == name_filter]
    prune_before = str(getattr(args, "prune_before", "") or "").strip()
    apply = bool(getattr(args, "apply", False))
    if prune_before:
        prunable = [
            row
            for row in rows
            if str(row.get("end_date") or "").strip() and str(row["end_date"]) < prune_before
        ]
        payload = {
            "prune_before": prune_before,
            "apply": apply,
            "count": len(prunable),
            "universes": prunable,
        }
        if apply and prunable:
            deleted = db.delete_universes([str(row.get("name", "")).strip() for row in prunable])
            payload["deleted"] = deleted
        print(json.dumps(payload, default=str, indent=2))
        return
    payload = {"universes": rows, "count": len(rows)}
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_pause(args: argparse.Namespace) -> None:
    session = _pdb().update_session(args.session_id, status="PAUSED", notes=args.notes)
    if session is not None:
        dispatch_session_state_alert(
            session_id=args.session_id,
            state="PAUSED",
            details=args.notes,
        )
    print(json.dumps(asdict(session), default=str, indent=2) if session else "{}")


async def _cmd_resume(args: argparse.Namespace) -> None:
    session = _pdb().update_session(args.session_id, status="ACTIVE", notes=args.notes)
    if session is not None:
        dispatch_session_state_alert(
            session_id=args.session_id,
            state="RESUMED",
            details=args.notes,
        )
    print(json.dumps(asdict(session), default=str, indent=2) if session else "{}")


async def _cmd_stop(args: argparse.Namespace) -> None:
    session = _pdb().update_session(
        args.session_id,
        status="COMPLETED" if args.complete else "STOPPING",
        notes=args.notes,
    )
    payload: dict[str, object] = {"session": asdict(session) if session else None}
    if session and args.complete:
        archive_result = archive_completed_session(args.session_id)
        payload["archive"] = (
            await archive_result if asyncio.iscoroutine(archive_result) else archive_result
        )
    print(json.dumps(payload, default=str, indent=2))


async def _cmd_resend_eod(args: argparse.Namespace) -> None:
    """Re-send the FLATTEN_EOD alert for a completed session.

    Useful when the original alert was lost (network outage, stale exit, process crash).
    Queries all CLOSED positions, builds the EOD summary, sends via Telegram, and logs
    to alert_log. Safe to run multiple times — each call writes a new alert_log entry
    (it does not check the dedup guard, which is in-memory and per-process only).
    """
    from engine.alert_dispatcher import get_alert_config
    from engine.notifiers.telegram import TelegramNotifier
    from engine.paper_runtime import _format_risk_alert, get_session_positions

    sid = args.session_id
    db = _pdb()
    session = db.get_session(sid)
    if session is None:
        raise SystemExit(f"Session not found: {sid!r}")

    all_closed = await get_session_positions(sid, statuses=["CLOSED"])
    total_trades = len(all_closed)
    if total_trades == 0:
        raise SystemExit(f"No closed positions for {sid!r} — nothing to report")
    open_positions = await get_session_positions(sid, statuses=["OPEN"])
    if open_positions:
        raise SystemExit(
            f"Session {sid!r} still has {len(open_positions)} OPEN position(s). "
            "Run `flatten --session-id <id>` first, then retry `resend-eod`."
        )

    total_pnl = sum(float(p.realized_pnl or p.pnl or 0) for p in all_closed)
    subject, body = _format_risk_alert(
        reason=args.notes or "resend_eod",
        net_pnl=total_pnl,
        session_id=sid,
        positions_closed=0,
        total_trades=total_trades,
        trade_date=getattr(session, "trade_date", None),
    )

    config = get_alert_config()
    tg = TelegramNotifier(config.telegram_bot_token, config.telegram_chat_ids)
    if not tg.enabled:
        raise SystemExit("Telegram not configured — check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS")

    await tg.send(subject, body)
    db.log_alert("FLATTEN_EOD", subject, body, channel="TELEGRAM", status="sent", error_msg=None)
    print(
        json.dumps(
            {
                "session_id": sid,
                "trades": total_trades,
                "net_pnl": round(total_pnl, 2),
                "alert": "sent",
            },
            indent=2,
        )
    )


async def _cmd_flatten(args: argparse.Namespace) -> None:
    # Start the alert consumer so TRADE_CLOSED and FLATTEN_EOD alerts actually deliver.
    register_session_start()
    _start_alert_dispatcher()
    try:
        payload = await flatten_session_positions(args.session_id, notes=args.notes)
        # Mark session COMPLETED so it leaves STOPPING/FAILED limbo and appears
        # correctly in the dashboard and archive queries.
        await update_session_state(args.session_id, status="COMPLETED", notes=args.notes)
        # Re-archive into backtest.duckdb so the dashboard shows the correct final trade
        # count and P/L. store_backtest_results has a PAPER dedup guard (DELETE + INSERT)
        # so this safely replaces any stale archive written at the earlier stale/failed exit.
        try:
            archive_result = archive_completed_session(args.session_id)
            payload["archive"] = (
                await archive_result if asyncio.iscoroutine(archive_result) else archive_result
            )
        except Exception as exc:
            payload["archive_error"] = str(exc)
        session = _pdb().get_session(args.session_id)
        payload["session"] = asdict(session) if session else None
        _pdb().force_sync()
        print(json.dumps(payload, default=str, indent=2))
    finally:
        # Drain the alert dispatcher before exiting, even if flatten/archive fails.
        await maybe_shutdown_alert_dispatcher()


async def _cmd_flatten_all(args: argparse.Namespace) -> None:
    """Flatten all ACTIVE/STOPPING/FAILED sessions for a trade date in one command."""
    trade_date = resolve_trade_date(args.trade_date)
    db = _pdb()
    rows = db.con.execute(
        "SELECT session_id, status FROM paper_sessions "
        "WHERE trade_date = ? AND status IN ('ACTIVE','PAUSED','STOPPING','FAILED','CANCELLED') "
        "ORDER BY created_at",
        [trade_date],
    ).fetchall()
    if not rows:
        print(f"No active/stopping/failed sessions to flatten for {trade_date}.")
        return
    # Start alert consumer once for all sessions in this batch.
    register_session_start()
    _start_alert_dispatcher()
    try:
        print(f"Flattening {len(rows)} session(s) for {trade_date}...")
        results = []
        for session_id, status in rows:
            print(f"  {session_id} (status={status})...")
            payload = await flatten_session_positions(session_id, notes=args.notes)
            await update_session_state(session_id, status="COMPLETED", notes=args.notes)
            n = payload.get("closed_positions", 0)
            # Archive into backtest.duckdb so session appears in dashboard archived view.
            try:
                archive_result = archive_completed_session(session_id)
                archived = bool(archive_result and archive_result.get("trade_count", 0) > 0)
            except Exception as exc:
                archived = False
                print(f"    WARNING: archive failed — {exc}")
            results.append({"session_id": session_id, "closed": n, "archived": archived})
            print(f"    -> closed {n} position(s), status -> COMPLETED, archived={archived}")
        _pdb().force_sync()
        print(json.dumps({"trade_date": trade_date, "sessions": results}, indent=2))
    finally:
        await maybe_shutdown_alert_dispatcher()


async def _cmd_send_command(args: argparse.Namespace) -> None:
    symbols = (
        [s.strip().upper() for s in str(args.symbols or "").split(",") if s.strip()]
        if args.symbols
        else None
    )
    if args.action == "close_positions" and not symbols:
        raise SystemExit("--symbols is required for close_positions")
    portfolio_value = getattr(args, "portfolio_value", None)
    max_positions = getattr(args, "max_positions", None)
    max_position_pct = getattr(args, "max_position_pct", None)
    if args.action == "set_risk_budget" and all(
        value is None for value in (portfolio_value, max_positions, max_position_pct)
    ):
        raise SystemExit(
            "--portfolio-value, --max-positions, or --max-position-pct is required for set_risk_budget"
        )
    command_file = write_admin_command(
        args.session_id,
        args.action,
        symbols=symbols,
        portfolio_value=portfolio_value,
        max_positions=max_positions,
        max_position_pct=max_position_pct,
        reason=args.reason,
        requester=args.requester,
    )
    print(
        json.dumps(
            {
                "session_id": args.session_id,
                "action": args.action,
                "symbols": symbols,
                "portfolio_value": portfolio_value,
                "max_positions": max_positions,
                "max_position_pct": max_position_pct,
                "command_file": command_file,
            },
            indent=2,
        )
    )


async def _cmd_flatten_both(args: argparse.Namespace) -> None:
    """Request close_all for both LONG and SHORT live sessions for a trade date."""
    trade_date = resolve_trade_date(args.trade_date)
    rows = (
        _pdb()
        .con.execute(
            """
        SELECT session_id, direction, status
        FROM paper_sessions
        WHERE trade_date = ?
          AND status IN ('ACTIVE','PAUSED')
          AND UPPER(direction) IN ('LONG','SHORT')
        ORDER BY direction, created_at
        """,
            [trade_date],
        )
        .fetchall()
    )
    if not rows:
        print(f"No ACTIVE/PAUSED LONG/SHORT sessions found for {trade_date}.")
        return

    results = []
    for session_id, direction, status in rows:
        command_file = write_admin_command(
            str(session_id),
            "close_all",
            reason=args.reason,
            requester=args.requester,
        )
        results.append(
            {
                "session_id": str(session_id),
                "direction": str(direction),
                "status": str(status),
                "command_file": command_file,
            }
        )
    print(json.dumps({"trade_date": trade_date, "commands": results}, indent=2))


async def _cmd_reconcile(args: argparse.Namespace) -> None:
    payload = reconcile_paper_session(_pdb(), args.session_id)
    print(json.dumps(payload, default=str, indent=2))
    if not payload.get("ok", False) and bool(getattr(args, "strict", False)):
        raise SystemExit(1)


async def _cmd_cleanup(args: argparse.Namespace) -> None:
    """Delete paper-session rows and archived PAPER analytics rows for a specific date."""
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        paper_db = _pdb()
        backtest_db = get_backtest_db()
        trade_date = args.trade_date

        # Collect matching active sessions
        session_ids_rows = paper_db.con.execute(
            "SELECT session_id FROM paper_sessions WHERE trade_date = ?",
            [trade_date],
        ).fetchall()
        matched_session_ids = [str(r[0]) for r in session_ids_rows if r and r[0]]

        # Always search archived PAPER runs in backtest.duckdb by date
        # (sessions may already be archived — no paper_sessions rows left)
        paper_run_ids: list[str] = []
        try:
            rows = backtest_db.con.execute(
                """
                SELECT DISTINCT run_id
                FROM (
                    SELECT run_id
                    FROM run_metadata
                    WHERE UPPER(COALESCE(execution_mode, 'BACKTEST')) = 'PAPER'
                      AND start_date = ?
                    UNION ALL
                    SELECT run_id
                    FROM backtest_results
                    WHERE UPPER(COALESCE(execution_mode, 'BACKTEST')) = 'PAPER'
                      AND trade_date = ?
                )
                WHERE run_id IS NOT NULL AND run_id <> ''
                ORDER BY run_id
                """,
                [trade_date, trade_date],
            ).fetchall()
            paper_run_ids = [str(row[0]) for row in rows if row and row[0] is not None]
        except Exception as exc:
            logger.warning("Failed to collect PAPER analytics run_ids: %s", exc)

        payload: dict[str, Any] = {
            "apply": bool(args.apply),
            "trade_date": trade_date,
            "matched_sessions": len(matched_session_ids),
            "paper_run_count": len(paper_run_ids),
            "paper_run_ids": paper_run_ids,
        }

        if args.apply:
            payload["paper_deleted"] = paper_db.delete_sessions_by_trade_date(trade_date)
            payload["analytics_deleted"] = backtest_db.delete_runs(paper_run_ids)

        print(json.dumps(payload, default=str, indent=2))


async def _cmd_feed_audit(args: argparse.Namespace) -> None:
    payload = compare_feed_audit(
        trade_date=args.trade_date,
        feed_source=args.feed_source,
        session_id=args.session_id,
    )
    print(json.dumps(payload, default=str, indent=2))
    if not payload.get("ok", False):
        raise SystemExit(1)


async def _cmd_replay(args: argparse.Namespace) -> None:
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
        )
        payload = await replay_session(
            session_id=args.session_id,
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            leave_active=args.leave_active,
            notes=args.notes,
        )
        print(json.dumps(payload, default=str, indent=2))


async def _cmd_live(args: argparse.Namespace) -> None:
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    payload = await run_live_session(
        session_id=args.session_id,
        symbols=symbols,
        poll_interval_sec=args.poll_interval_sec,
        candle_interval_minutes=args.candle_interval_minutes,
        max_cycles=args.max_cycles,
        complete_on_exit=args.complete_on_exit,
        allow_late_start_fallback=bool(getattr(args, "allow_late_start_fallback", False)),
        notes=args.notes,
    )
    print(json.dumps(payload, default=str, indent=2))


def build_parser() -> argparse.ArgumentParser:
    return build_paper_trading_parser(
        settings=get_settings(),
        handlers=build_paper_trading_handler_map(globals()),
        market_ready_hhmm=MARKET_READY_HHMM,
    )


def main() -> None:
    configure_windows_stdio(line_buffering=True, write_through=True)
    configure_windows_asyncio()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    # Startup: cancel any stale sessions left from a previous crash.
    file_only_commands = {"send-command"}
    if getattr(args, "command", "") not in file_only_commands:
        stale = _pdb().cleanup_stale_sessions()
        if stale:
            print(f"Cleaned up {stale} stale session(s) from previous run(s)", flush=True)
    run_asyncio(args.handler(args))


if __name__ in {"__main__", "__mp_main__"}:
    main()
