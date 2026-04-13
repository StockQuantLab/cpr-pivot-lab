from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from db.duckdb import get_dashboard_db, get_db
from engine.constants import parse_iso_date
from engine.cpr_atr_strategy import CPRATRBacktest
from engine.paper_runtime import build_backtest_params_from_overrides

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GateConfig:
    """Configurable walk-forward gate thresholds.

    Thresholds below which INCONCLUSIVE is returned (insufficient data):
        min_replayed_days: minimum trading days with completed folds
        min_total_trades: minimum trades across all folds

    Thresholds for PASS/FAIL decision (applied only when data is sufficient):
        min_profitable_ratio: fraction of days that must be profitable
        min_avg_return_pct: minimum average daily return %
        max_worst_daily_loss_pct: worst single-day return % floor (negative)
        min_profit_factor: minimum profit factor (sum wins / abs sum losses)
    """

    min_replayed_days: int = 5
    min_total_trades: int = 10
    min_profitable_ratio: float = 0.50
    min_avg_return_pct: float = 0.0
    max_worst_daily_loss_pct: float = -10.0
    min_profit_factor: float = 0.0  # 0 = disabled


def evaluate_walk_forward_gate(
    summary: dict[str, Any],
    gate: GateConfig | None = None,
) -> dict[str, Any]:
    """Return PASS / FAIL / INCONCLUSIVE decision with reasons.

    INCONCLUSIVE is returned when there is insufficient data to make a
    meaningful pass/fail judgment (too few days or trades).
    """
    gate = gate or GateConfig()
    reasons: list[str] = []

    replayed_days = int(summary.get("replayed_days") or 0)
    total_trades = int(summary.get("total_trades") or 0)

    # --- Insufficient data -> INCONCLUSIVE ---
    if replayed_days == 0:
        return {"status": "INCONCLUSIVE", "reasons": ["no_replayed_days"]}
    if replayed_days < gate.min_replayed_days:
        reasons.append(f"insufficient_days:{replayed_days}<{gate.min_replayed_days}")
    if total_trades < gate.min_total_trades:
        reasons.append(f"insufficient_trades:{total_trades}<{gate.min_total_trades}")
    if reasons:
        return {"status": "INCONCLUSIVE", "reasons": reasons}

    # --- Sufficient data -> evaluate PASS / FAIL ---
    avg_return = summary.get("avg_daily_return_pct")
    if avg_return is not None and avg_return <= gate.min_avg_return_pct:
        reasons.append("non_positive_average_return")

    profitable_ratio = summary.get("profitable_days_ratio")
    if profitable_ratio is not None and profitable_ratio < gate.min_profitable_ratio:
        reasons.append("insufficient_profitable_days")

    worst_day = summary.get("worst_daily_return_pct")
    if worst_day is not None and worst_day <= gate.max_worst_daily_loss_pct:
        reasons.append("excessive_daily_drawdown")

    if gate.min_profit_factor > 0:
        pf = summary.get("profit_factor")
        if pf is not None and pf < gate.min_profit_factor:
            reasons.append(f"low_profit_factor:{pf:.2f}<{gate.min_profit_factor:.2f}")

    return {"status": "PASS" if not reasons else "FAIL", "reasons": reasons}


def _json_canonical(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize_strategy_params(strategy_params: dict[str, Any] | None) -> dict[str, Any]:
    parsed = strategy_params or {}
    return json.loads(_json_canonical(parsed))


def make_gate_key(strategy: str, strategy_params: dict[str, Any] | None) -> str:
    payload = {
        "strategy": str(strategy or "").upper(),
        "strategy_params": normalize_strategy_params(strategy_params),
    }
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()[:16]


def make_scope_key(symbols: list[str], *, all_symbols: bool = False) -> str:
    normalized = sorted({str(symbol).upper() for symbol in symbols})
    if all_symbols:
        return f"ALL:{len(normalized)}"
    payload = {"symbols": normalized}
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()[:16]


def iter_session_calendar_trade_dates(start_date: str, end_date: str) -> list[str]:
    from datetime import date, timedelta

    # Validate inputs using shared utility
    start = date.fromisoformat(parse_iso_date(start_date))
    end = date.fromisoformat(parse_iso_date(end_date))
    if start > end:
        raise ValueError("start_date must be <= end_date")

    def _query_trade_dates(db) -> list[str]:
        for table, column in (
            ("v_5min", "date"),
            ("v_daily", "date"),
            ("market_day_state", "trade_date"),
            ("strategy_day_state", "trade_date"),
            ("intraday_day_pack", "trade_date"),
        ):
            try:
                rows = db.con.execute(
                    f"""
                    SELECT DISTINCT {column}::VARCHAR
                    FROM {table}
                    WHERE {column} BETWEEN ?::DATE AND ?::DATE
                    ORDER BY {column}
                    """,
                    [start.isoformat(), end.isoformat()],
                ).fetchall()
            except Exception as exc:
                logger.debug("Failed to resolve trade dates from %s: %s", table, exc)
                continue
            if rows:
                return [str(row[0]) for row in rows if row and row[0] is not None]
        return []

    for accessor in (get_db, get_dashboard_db):
        try:
            resolved_trade_dates = _query_trade_dates(accessor())
        except Exception as exc:
            logger.debug("Failed to open trade-date database via %s: %s", accessor.__name__, exc)
            continue
        if resolved_trade_dates:
            return resolved_trade_dates

    fallback_trade_dates: list[str] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            fallback_trade_dates.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return fallback_trade_dates


def validate_walk_forward_runtime_preflight(
    *,
    coverage: dict[str, Any],
    schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate runtime coverage and PostgreSQL schema before a long walk-forward run."""
    schema_payload = schema or {}
    columns = schema_payload.get("columns") or {}
    constraints = schema_payload.get("constraints") or {}
    decision_column = columns.get("decision") or {}
    decision_type = str(decision_column.get("data_type") or "").lower()
    decision_length = decision_column.get("character_maximum_length")
    decision_clause = str(constraints.get("walk_forward_runs_decision_check") or "")

    schema_reasons: list[str] = []
    if decision_type == "text":
        pass
    elif decision_type in {"character varying", "varchar"}:
        if decision_length is None or int(decision_length) < 12:
            schema_reasons.append("walk_forward_runs.decision varchar too narrow for INCONCLUSIVE")
    else:
        schema_reasons.append(
            f"walk_forward_runs.decision has unsupported type '{decision_type or 'missing'}'"
        )
    if "INCONCLUSIVE" not in decision_clause:
        schema_reasons.append("walk_forward_runs_decision_check is missing INCONCLUSIVE")

    return {
        "coverage": coverage,
        "schema": schema_payload,
        "coverage_ready": bool(coverage.get("coverage_ready", False)),
        "schema_ready": not schema_reasons,
        "schema_reasons": schema_reasons,
        "ready": bool(coverage.get("coverage_ready", False)) and not schema_reasons,
    }


def summarize_calendar_folds(
    folds: list[dict[str, Any]],
    portfolio_value: float,
    *,
    carry_forward_equity: bool = False,
    ending_equity: float | None = None,
) -> dict[str, Any]:
    completed = [fold for fold in folds if fold.get("status") == "COMPLETED"]
    returns = [float(fold.get("total_return_pct") or 0.0) for fold in completed]
    profitable = sum(1 for value in returns if value > 0)
    total_trades = sum(int(fold.get("total_trades") or 0) for fold in completed)
    total_pnl = round(sum(float(fold.get("total_pnl") or 0.0) for fold in completed), 2)
    avg_return = round(sum(returns) / len(returns), 4) if returns else None

    pnls = [float(fold.get("total_pnl") or 0.0) for fold in completed]
    gross_wins = sum(p for p in pnls if p > 0)
    gross_losses = abs(sum(p for p in pnls if p < 0))
    profit_factor = round(gross_wins / gross_losses, 4) if gross_losses > 0 else None

    summary: dict[str, Any] = {
        "validation_engine": "fast_validator",
        "portfolio_value": float(portfolio_value),
        "replayed_days": len(completed),
        "days_requested": len(folds),
        "total_trades": total_trades,
        "total_pnl": total_pnl,
        "avg_daily_return_pct": avg_return,
        "profitable_days": profitable,
        "profitable_days_ratio": round(profitable / len(completed), 4) if completed else None,
        "worst_daily_return_pct": round(min(returns), 4) if returns else None,
        "best_daily_return_pct": round(max(returns), 4) if returns else None,
        "profit_factor": profit_factor,
    }

    if carry_forward_equity:
        end_eq = ending_equity if ending_equity is not None else portfolio_value + total_pnl
        cumulative_return_pct = (
            round((end_eq - portfolio_value) / portfolio_value * 100.0, 4)
            if portfolio_value > 0
            else 0.0
        )
        summary["carry_forward_equity"] = True
        summary["ending_equity"] = round(end_eq, 2)
        summary["cumulative_return_pct"] = cumulative_return_pct

    return summary


def run_fast_walk_forward_validation(
    *,
    start_date: str,
    end_date: str,
    symbols: list[str],
    strategy: str,
    strategy_params: dict[str, Any] | None,
    wf_run_id: str | None = None,
    force: bool = False,
    carry_forward_equity: bool = False,
    progress_hook: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    normalized_params = normalize_strategy_params(strategy_params)
    params = build_backtest_params_from_overrides(strategy, normalized_params)
    trade_dates = iter_session_calendar_trade_dates(start_date, end_date)
    db = get_db()
    folds: list[dict[str, Any]] = []
    started = time.perf_counter()
    initial_portfolio_value = float(params.portfolio_value or 1_000_000.0)
    current_equity = initial_portfolio_value

    for index, trade_date in enumerate(trade_dates, start=1):
        fold_started = time.perf_counter()

        if carry_forward_equity:
            fold_params = replace(params, portfolio_value=current_equity)
        else:
            fold_params = params
        fold_portfolio = float(fold_params.portfolio_value or initial_portfolio_value)

        backtest = CPRATRBacktest(params=fold_params, db=db)
        result = backtest.run(
            symbols=symbols,
            start=trade_date,
            end=trade_date,
            verbose=False,
        )
        persisted_to_db = True
        persist_error: str | None = None
        try:
            result.save_to_db(db, wf_run_id=wf_run_id)
        except Exception as exc:
            persisted_to_db = False
            persist_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Skipping DuckDB persistence for walk-forward run_id=%s trade_date=%s: %s",
                result.run_id,
                trade_date,
                exc,
            )
        df = result.df
        total_trades = int(df.height)
        total_pnl = round(float(df["profit_loss"].sum()) if total_trades else 0.0, 2)
        total_return_pct = (
            round((total_pnl / fold_portfolio) * 100.0, 4) if fold_portfolio > 0 else 0.0
        )

        if carry_forward_equity:
            current_equity = round(current_equity + total_pnl, 2)

        fold = {
            "fold_index": index,
            "trade_date": trade_date,
            "status": "COMPLETED",
            "reference_run_id": result.run_id,
            "total_trades": total_trades,
            "total_pnl": total_pnl,
            "total_return_pct": total_return_pct,
            "elapsed_sec": round(time.perf_counter() - fold_started, 2),
            "fold_portfolio_value": fold_portfolio,
            "cumulative_equity": current_equity,
            "persisted_to_db": persisted_to_db,
        }
        if persist_error is not None:
            fold["persist_error"] = persist_error
        folds.append(fold)
        if progress_hook is not None:
            progress_hook(
                {
                    **fold,
                    "total": len(trade_dates),
                    "elapsed_total_sec": round(time.perf_counter() - started, 2),
                }
            )

    return {
        "trade_dates": trade_dates,
        "folds": folds,
        "gate_key": make_gate_key(strategy, normalized_params),
        "normalized_strategy_params": normalized_params,
        "portfolio_value": initial_portfolio_value,
        "carry_forward_equity": carry_forward_equity,
        "ending_equity": current_equity,
    }


__all__ = [
    "GateConfig",
    "evaluate_walk_forward_gate",
    "iter_session_calendar_trade_dates",
    "make_gate_key",
    "make_scope_key",
    "normalize_strategy_params",
    "run_fast_walk_forward_validation",
    "summarize_calendar_folds",
    "validate_walk_forward_runtime_preflight",
]
