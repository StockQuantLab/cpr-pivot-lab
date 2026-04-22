"""Paper trading session control CLI."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import uuid4

import polars as pl

from config.settings import get_settings
from db.backtest_db import get_backtest_db
from db.duckdb import get_dashboard_db, get_db
from db.paper_db import PaperSession, get_paper_db
from engine.cli_setup import configure_windows_asyncio, configure_windows_stdio, run_asyncio
from engine.command_lock import acquire_command_lock
from engine.cpr_atr_strategy import BacktestResult, CPRATRBacktest
from engine.kite_ticker_adapter import KiteTickerAdapter
from engine.live_market_data import IST
from engine.paper_runtime import (
    _start_alert_dispatcher,
    apply_paper_strategy_defaults,
    build_backtest_params_from_overrides,
    dispatch_session_state_alert,
    flatten_session_positions,
    maybe_shutdown_alert_dispatcher,
    register_session_start,
    set_alerts_suppressed,
)
from engine.strategy_presets import ALL_STRATEGY_PRESETS, list_strategy_preset_names
from scripts import data_quality as _data_quality
from scripts.paper_archive import archive_completed_session
from scripts.paper_feed_audit import compare_feed_audit
from scripts.paper_live import run_live_session
from scripts.paper_prepare import (
    pre_filter_symbols_for_strategy,
    prepare_runtime_for_daily_paper,
    resolve_prepare_symbols,
    resolve_trade_date,
)
from scripts.paper_replay import ReplayDayPack, load_replay_day_packs, replay_session

logger = logging.getLogger(__name__)


def normalize_strategy_params(strategy_params: dict[str, Any] | None) -> dict[str, Any]:
    """Canonicalize strategy params via JSON round-trip for stable dict comparison."""
    parsed = strategy_params or {}
    return json.loads(json.dumps(parsed, sort_keys=True, separators=(",", ":")))


def _pdb():
    return get_paper_db()


def _cleanup_feed_audit_retention(*, command_name: str) -> int:
    """Keep the live/replay feed audit bounded to a rolling retention window."""

    retention_days = int(get_settings().feed_audit_retention_days or 0)
    if retention_days <= 0:
        return 0
    deleted = _pdb().cleanup_feed_audit_older_than(retention_days)
    if deleted > 0:
        logger.info(
            "%s purged %d paper_feed_audit row(s) older than %d day(s)",
            command_name,
            deleted,
            retention_days,
        )
    return deleted


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
    return _pdb().create_session(**kwargs)


async def update_session_state(session_id: str, **kwargs):
    return _pdb().update_session(session_id, **kwargs)


_SIMPLE_OBJECT_PAIR_RE = re.compile(r"\s*([^:,{}]+)\s*:\s*([^,{}]+)\s*")


def _coerce_simple_object_value(raw: str) -> Any:
    value = raw.strip()
    if not value:
        return ""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return float(value)
    return value


def _parse_simple_object(value: str) -> dict[str, Any]:
    text = value.strip()
    if not (text.startswith("{") and text.endswith("}")):
        raise ValueError("not a simple object")
    body = text[1:-1].strip()
    if not body:
        return {}
    parsed: dict[str, Any] = {}
    for item in body.split(","):
        match = _SIMPLE_OBJECT_PAIR_RE.fullmatch(item)
        if match is None:
            raise ValueError(f"invalid object entry: {item!r}")
        key = match.group(1).strip().strip("'\"")
        parsed[key] = _coerce_simple_object_value(match.group(2))
    return parsed


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


def _variant_params(base: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    """Return a shallow copy of a canonical variant recipe with overrides applied."""
    params = dict(base)
    params.update(overrides)
    return params


CPR_CANONICAL_PARAMS: dict[str, Any] = dict(
    ALL_STRATEGY_PRESETS["CPR_LEVELS_RISK_LONG"]["overrides"]
)

PAPER_ALLOWED_STRATEGIES: tuple[str, ...] = ("CPR_LEVELS",)
PAPER_ALLOWED_PRESETS: tuple[str, ...] = tuple(list_strategy_preset_names("CPR_LEVELS"))


def _normalize_strategy_name(value: str | None) -> str:
    return str(value or "").strip().upper()


def _paper_default_strategy(settings: Any) -> str:
    configured = _normalize_strategy_name(getattr(settings, "paper_default_strategy", None))
    if configured in PAPER_ALLOWED_STRATEGIES:
        return configured
    return "CPR_LEVELS"


def _assert_cpr_only_strategy(strategy: str | None, *, source: str) -> str:
    resolved = _normalize_strategy_name(strategy) or "CPR_LEVELS"
    if resolved not in PAPER_ALLOWED_STRATEGIES:
        allowed = ", ".join(PAPER_ALLOWED_STRATEGIES)
        raise ValueError(
            f"{source} '{resolved}' is not supported for paper workflows. Use {allowed}."
        )
    return resolved


PAPER_STANDARD_MATRIX: tuple[tuple[str, str, dict[str, Any]], ...] = (
    # CPR LONG: match the canonical backtest recipe.
    (
        "CPR_LEVELS_LONG",
        "CPR_LEVELS",
        _variant_params(CPR_CANONICAL_PARAMS, direction_filter="LONG"),
    ),
    # CPR SHORT: same recipe, but RVOL is skipped for the bearish setup.
    (
        "CPR_LEVELS_SHORT",
        "CPR_LEVELS",
        _variant_params(CPR_CANONICAL_PARAMS, direction_filter="SHORT", skip_rvol_check=True),
    ),
)


def _parse_json(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = _parse_simple_object(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("--strategy-params must decode to a JSON object")
    return parsed


class _StandardSizingAction(argparse.Action):
    """Mark the run as standard sizing and clear risk-based sizing in the namespace."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object | None,
        option_string: str | None = None,
    ) -> None:
        del parser, values, option_string
        namespace.standard_sizing = True
        namespace.risk_based_sizing = False


def _collect_strategy_cli_overrides(
    args: argparse.Namespace, *, has_preset: bool = False
) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    direction = getattr(args, "direction", None)
    if direction and str(direction).upper() != "BOTH":
        overrides["direction_filter"] = str(direction).upper()
    if getattr(args, "skip_rvol", False):
        overrides["skip_rvol_check"] = True
    if getattr(args, "standard_sizing", False):
        overrides["risk_based_sizing"] = False
    elif not has_preset and getattr(args, "risk_based_sizing", False):
        # In non-preset mode apply the CLI default (True).  In preset mode, skip this so the
        # preset's own risk_based_sizing value is not silently overridden by the default.
        overrides["risk_based_sizing"] = True
    min_price = getattr(args, "min_price", None)
    if min_price is not None:
        overrides["min_price"] = float(min_price)
    regime_index_symbol = getattr(args, "regime_index_symbol", None)
    if regime_index_symbol:
        overrides["regime_index_symbol"] = str(regime_index_symbol).strip().upper()
    regime_min_move_pct = getattr(args, "regime_min_move_pct", None)
    if regime_min_move_pct is not None:
        overrides["regime_min_move_pct"] = float(regime_min_move_pct)
    regime_snapshot_minutes = getattr(args, "regime_snapshot_minutes", None)
    if regime_snapshot_minutes is not None:
        overrides["regime_snapshot_minutes"] = int(regime_snapshot_minutes)
    cpr_min_close_atr = getattr(args, "cpr_min_close_atr", None)
    if cpr_min_close_atr is not None:
        overrides["cpr_min_close_atr"] = float(cpr_min_close_atr)
    scale_out_pct = getattr(args, "cpr_scale_out_pct", None)
    if scale_out_pct is not None:
        overrides.setdefault("cpr_levels_config", {})
        overrides["cpr_levels_config"]["scale_out_pct"] = float(scale_out_pct)
    if getattr(args, "narrowing_filter", False):
        overrides["narrowing_filter"] = True
    or_minutes = getattr(args, "or_minutes", None)
    if or_minutes is not None:
        overrides["or_minutes"] = int(or_minutes)
    entry_window_end = getattr(args, "entry_window_end", None)
    if entry_window_end:
        overrides["entry_window_end"] = str(entry_window_end)
    time_exit = getattr(args, "time_exit", None)
    if time_exit:
        overrides["time_exit"] = str(time_exit)
    cpr_entry_start = getattr(args, "cpr_entry_start", None)
    if cpr_entry_start:
        overrides.setdefault("cpr_levels_config", {})
        overrides["cpr_levels_config"]["cpr_entry_start"] = str(cpr_entry_start)
    return overrides


def _resolve_paper_strategy_params(
    strategy: str,
    raw_value: str | None,
    args: argparse.Namespace | None = None,
) -> dict[str, Any]:
    preset_name = str(getattr(args, "preset", None) or "").upper() if args is not None else ""
    resolved_strategy = _assert_cpr_only_strategy(strategy, source="strategy")
    if preset_name:
        preset = ALL_STRATEGY_PRESETS.get(preset_name)
        if preset is None:
            raise ValueError(f"Unknown strategy preset: {preset_name}")
        resolved_strategy = _assert_cpr_only_strategy(
            str(preset["strategy"]), source=f"preset {preset_name}"
        )
        params = dict(preset["overrides"])
    else:
        params = _parse_json(raw_value)
    if args is not None:
        params.update(_collect_strategy_cli_overrides(args, has_preset=bool(preset_name)))
    return apply_paper_strategy_defaults(resolved_strategy, normalize_strategy_params(params))


def _parse_symbols_arg(value: str | None) -> list[str] | None:
    if not value:
        return None
    symbols = [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]
    return symbols or None


def _resolve_cli_symbols(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    *,
    read_only: bool = True,
) -> list[str]:
    symbols = _parse_symbols_arg(getattr(args, "symbols", None))
    use_all_symbols = bool(getattr(args, "all_symbols", False))
    if use_all_symbols and symbols:
        parser.error("Use either --symbols or --all-symbols, not both.")
    return resolve_prepare_symbols(symbols, None, all_symbols=use_all_symbols, read_only=read_only)


def _count_duckdb_rows_for_run_ids(run_ids: list[str]) -> dict[str, int]:
    ids = sorted({str(run_id).strip() for run_id in run_ids if str(run_id).strip()})
    if not ids:
        return {
            "backtest_results": 0,
            "run_daily_pnl": 0,
            "run_metrics": 0,
            "run_metadata": 0,
        }
    placeholders = ", ".join("?" for _ in ids)
    from db.backtest_db import get_backtest_db
    from db.duckdb import get_db

    counts: dict[str, int] = dict.fromkeys(
        ("backtest_results", "run_daily_pnl", "run_metrics", "run_metadata"),
        0,
    )
    for db in (get_db(), get_backtest_db()):
        for table in counts:
            row = db.con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id IN ({placeholders})",
                ids,
            ).fetchone()
            counts[table] += int(row[0] or 0) if row else 0
    return counts


def _build_runtime_coverage_fix_lines(missing_counts: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if int(missing_counts.get("market_day_state") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table state --force")
    if int(missing_counts.get("strategy_day_state") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table strategy --force")
    if int(missing_counts.get("intraday_day_pack") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table pack --force")
    return lines or ["doppler run -- uv run pivot-data-quality --date <trade-date>"]


def _session_direction_suffix(strategy: str, strategy_params: dict[str, Any] | None) -> str:
    direction = str((strategy_params or {}).get("direction_filter", "BOTH") or "BOTH").upper()
    if direction == "BOTH":
        return ""
    return direction.lower()


def _workflow_session_suffix(mode: str, feed_source: str | None = None) -> str:
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


def _default_session_id(
    prefix: str,
    trade_date: str,
    strategy: str,
    strategy_params: dict[str, Any] | None = None,
    mode: str = "",
    feed_source: str | None = None,
) -> str:
    strategy_feed_source = str((strategy_params or {}).get("feed_source") or "").strip().lower()
    workflow_suffix = _workflow_session_suffix(mode, feed_source or strategy_feed_source)
    direction = _session_direction_suffix(strategy, strategy_params)
    direction_tag = f"-{direction}" if direction else ""
    return f"{prefix}-{strategy.lower()}{direction_tag}-{trade_date}{workflow_suffix}"


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
    requested_session_id = session_id or _default_session_id(
        "paper",
        trade_date,
        strategy,
        strategy_params,
        mode,
    )
    session = await get_session(requested_session_id)

    direction = str((strategy_params or {}).get("direction_filter", "BOTH") or "BOTH").upper()
    direction_label = f" {direction}" if direction != "BOTH" else ""
    session_name = f"{strategy}{direction_label} {trade_date}"

    if session is None:
        return await create_paper_session(
            session_id=requested_session_id,
            name=session_name,
            strategy=strategy,
            symbols=symbols,
            status="ACTIVE",
            strategy_params=strategy_params,
            trade_date=trade_date,
            mode=mode,
            notes=notes,
        )

    if mode == "live":
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
        strategy_params=strategy_params,
        trade_date=trade_date,
        mode=mode,
        notes=notes,
    )


def _handle_coverage_gaps(preparation: dict[str, Any], *, trade_date: str, mode: str) -> None:
    """Handle runtime coverage gaps for paper sessions.

    Strategy:
    - `intraday_day_pack` gaps are warnings only — missing symbols simply didn't
      trade that day and will produce no positions. Never a blocker.
    - `market_day_state` / `strategy_day_state` gaps are hard errors — CPR/ATR
      setup data is absent so the strategy cannot run for those symbols.
    """
    inner_coverage = preparation.get("coverage") or {}
    raw_coverage = inner_coverage.get("coverage") or inner_coverage
    missing_symbols: dict[str, list[str]] = {}
    missing_counts: dict[str, int] = inner_coverage.get("missing_counts") or {}
    if not missing_counts:
        for table, values in raw_coverage.items():
            if isinstance(values, list):
                missing_counts[str(table)] = len(values)
                missing_symbols[str(table)] = list(values)
    else:
        for table, values in raw_coverage.items():
            if isinstance(values, list):
                missing_symbols[str(table)] = list(values)

    pack_missing = int(missing_counts.get("intraday_day_pack") or 0)
    mds_missing = int(missing_counts.get("market_day_state") or 0)
    sds_missing = int(missing_counts.get("strategy_day_state") or 0)

    # Warn about pack gaps (symbols not trading that day) — never block.
    if pack_missing:
        syms = missing_symbols.get("intraday_day_pack") or []
        sym_str = f" — {', '.join(sorted(syms))}" if syms and len(syms) <= 20 else ""
        print(
            f"[coverage] WARNING: {pack_missing} symbol(s) missing from intraday_day_pack"
            f" for {trade_date}{sym_str}. They will be skipped (no trades).",
            flush=True,
        )

    # Block only on state-table gaps.
    if mds_missing == 0 and sds_missing == 0:
        return

    detail_lines: list[str] = []
    blocking: dict[str, int] = {}
    for table in ("market_day_state", "strategy_day_state"):
        count = int(missing_counts.get(table) or 0)
        if count:
            blocking[table] = count
            syms = missing_symbols.get(table) or []
            if syms and len(syms) <= 20:
                detail_lines.append(f"  {table}: {count} missing — {', '.join(sorted(syms))}")
            else:
                detail_lines.append(f"  {table}: {count} missing")

    fix_source = dict(blocking)
    if pack_missing:
        fix_source["intraday_day_pack"] = pack_missing
    fix_lines = _build_runtime_coverage_fix_lines(fix_source)
    pre_market_hint = (
        "\n\nFor daily-live, run this pre-market:\n"
        f"  doppler run -- uv run pivot-refresh --since <prev_trading_date>\n"
        f"  doppler run -- uv run pivot-paper-trading daily-prepare --trade-date {trade_date} --all-symbols"
        if mode == "live"
        else ""
    )
    raise SystemExit(
        f"Runtime coverage incomplete for {trade_date} (mode={mode}).\n"
        + "\n".join(detail_lines)
        + "\n\nFix:\n  "
        + "\n  ".join(fix_lines)
        + pre_market_hint
    )


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
    symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
    # Detect whether this is a future/live date (no 5-min data yet) or a historical replay date.
    db = get_dashboard_db()
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
    # For readiness reporting use the last available data date when the requested date
    # is in the future (live mode: today's candles don't exist yet).
    db = get_dashboard_db()
    dq_date = trade_date if has_intraday else _last_available_trade_date(db)
    if dq_date != trade_date:
        print(
            f"\nNote: {trade_date} has no intraday data yet (live session).\n"
            f"Reporting runtime readiness for prior trading date: {dq_date}\n"
            f"All runtime tables must be current through {dq_date} before market open.\n"
        )
    readiness = _data_quality.build_trade_date_readiness_report(dq_date)  # type: ignore[attr-defined]
    _data_quality.print_trade_date_readiness_report(readiness)  # type: ignore[attr-defined]
    payload["dq_readiness"] = readiness
    payload["dq_date"] = dq_date
    print(json.dumps(payload, default=str, indent=2))
    if not readiness.get("ready", False):
        raise SystemExit(1)


async def _cmd_daily_replay(args: argparse.Namespace) -> None:
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        if getattr(args, "multi", False):
            await _cmd_daily_replay_multi(args)
            return

        trade_date = resolve_trade_date(args.trade_date)
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
    symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)
    strategy_params = _resolve_paper_strategy_params(args.strategy, args.strategy_params, args)

    feed_source = getattr(args, "feed_source", "kite")
    suppress_alerts = bool(getattr(args, "no_alerts", False))

    if feed_source == "kite":
        await _wait_until_market_ready(trade_date)

    filtered = pre_filter_symbols_for_strategy(trade_date, symbols, args.strategy, strategy_params)
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
            notes=args.notes,
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
    for label, strategy, base_params in variants:
        normalized_params = apply_paper_strategy_defaults(
            strategy, normalize_strategy_params(base_params)
        )
        filtered = pre_filter_symbols_for_strategy(
            trade_date, all_symbols, strategy, normalized_params
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
            print(
                f"\n  [{label}] Variant exited early: status={final_status}"
                f" reason={summary['terminal_reason'] or retry_reason}"
                f" last_bar={summary['last_bar_hhmm'] or 'n/a'} at {now_hhmm}"
                f" — restart {attempt + 1}/{retry_max} in {wait_sec}s",
                flush=True,
            )
            await asyncio.sleep(wait_sec)

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


async def _cmd_daily_live_multi(args: argparse.Namespace) -> None:
    """Run multiple paper variants concurrently in a single process.

    DuckDB allows only one writer at a time. Running variants in-process
    with asyncio.gather avoids multi-process file-lock conflicts on
    paper.duckdb while allowing all variants to poll simultaneously.
    """
    trade_date = resolve_trade_date(args.trade_date)
    all_symbols = _resolve_cli_symbols(build_parser(), args, read_only=True)

    feed_source = getattr(args, "feed_source", "kite")
    suppress_alerts = bool(getattr(args, "no_alerts", False))

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
        normalized_params = apply_paper_strategy_defaults(
            strategy, normalize_strategy_params(base_params)
        )
        filtered = pre_filter_symbols_for_strategy(
            trade_date, all_symbols, strategy, normalized_params
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
                )
                print(f"  [pre-create] {session_id} (PLANNING)", flush=True)
        # Force a single replica sync after all sessions are written so the
        # dashboard sees every PLANNING session immediately, bypassing the
        # 5-second debounce that otherwise only captures the first session.
        _pdb().force_sync()
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

    preserve_open_positions_on_restart = not bool(getattr(args, "complete_on_exit", False))
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
            auto_flatten_on_abnormal_exit=not preserve_open_positions_on_restart,
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
    trade_date = resolve_trade_date(args.trade_date)
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
        normalized_params = apply_paper_strategy_defaults(
            strategy, normalize_strategy_params(base_params)
        )
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
                (strategy, apply_paper_strategy_defaults(strategy, dict(params)))
                for _, strategy, params in PAPER_STANDARD_MATRIX
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
    session = _pdb().create_session(
        session_id=args.session_id,
        name=args.name,
        strategy=strategy,
        symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        status="ACTIVE" if args.activate else "PLANNING",
        strategy_params=_resolve_paper_strategy_params(strategy, args.strategy_params, args),
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


async def _cmd_flatten(args: argparse.Namespace) -> None:
    # Start the alert consumer so TRADE_CLOSED and FLATTEN_EOD alerts actually deliver.
    register_session_start()
    _start_alert_dispatcher()
    try:
        payload = await flatten_session_positions(args.session_id, notes=args.notes)
        # Mark session COMPLETED so it leaves STOPPING/FAILED limbo and appears
        # correctly in the dashboard and archive queries.
        await update_session_state(args.session_id, status="COMPLETED", notes=args.notes)
        session = _pdb().get_session(args.session_id)
        payload["session"] = asdict(session) if session else None
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
        print(json.dumps({"trade_date": trade_date, "sessions": results}, indent=2))
    finally:
        await maybe_shutdown_alert_dispatcher()


async def _cmd_order(args: argparse.Namespace) -> None:
    order_id = _pdb().append_order_event(
        session_id=args.session_id,
        symbol=args.symbol,
        side=args.side,
        requested_qty=int(args.quantity),
        order_type=args.order_type,
        request_price=args.request_price,
        fill_qty=int(args.fill_qty) if args.fill_qty is not None else None,
        fill_price=args.fill_price,
        status=args.status,
        notes=args.notes,
    )
    print(json.dumps({"order_id": order_id}, default=str, indent=2))


async def _cmd_close_position(args: argparse.Namespace) -> None:
    _pdb().close_position(
        position_id=str(args.position_id),
        exit_price=args.close_price,
        exit_reason="manual_close",
        pnl=args.realized_pnl or 0.0,
        closed_by=args.closed_by,
    )
    print(json.dumps({"position_id": args.position_id, "status": "CLOSED"}, default=str))


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
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Paper trading session control")
    sub = parser.add_subparsers(dest="command", required=True)

    # -- Shared arg group helpers (reduce copy-paste across subcommands) --

    def _add_symbol_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--symbols", default=None, help="Optional comma-separated symbol override")
        sp.add_argument(
            "--all-symbols",
            action="store_true",
            help="Use the full local validated symbol universe from DuckDB metadata.",
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
        sp.add_argument(
            "--skip-rvol",
            action="store_true",
            help="Skip RVOL filtering for this run/session.",
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

    # -- Subcommands --

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
        help="Max concurrent positions (default: 10)",
    )
    start.add_argument(
        "--max-position-pct",
        type=float,
        default=settings.paper_max_position_pct,
        help="Max allocation per position (default: 0.10)",
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
    stale = _pdb().cleanup_stale_sessions()
    if stale:
        print(f"Cleaned up {stale} stale session(s) from previous run(s)", flush=True)
    run_asyncio(args.handler(args))


if __name__ in {"__main__", "__mp_main__"}:
    main()
