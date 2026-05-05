"""Helper functions for CPR-ATR backtest orchestration."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from engine.constants import preview_list
from engine.cpr_atr_models import STRATEGY_VERSION, BacktestParams, FunnelCounts
from engine.cpr_atr_shared import regime_snapshot_close_col
from engine.day_pack_sources import is_feed_audit_pack_source

logger = logging.getLogger(__name__)


def make_param_signature(
    params: BacktestParams,
    symbols: list[str],
    start: str,
    end: str,
) -> str:
    """Return a deterministic hash of params + scope."""
    cpr_cfg = params.cpr_levels
    fbr_cfg = params.fbr
    vcpr_cfg = params.virgin_cpr
    key = {
        "symbols": sorted(symbols),
        "start": start,
        "end": end,
        "version": STRATEGY_VERSION,
        "cpr_percentile": params.cpr_percentile,
        "cpr_max_width_pct": params.cpr_max_width_pct,
        "atr_periods": params.atr_periods,
        "buffer_pct": params.buffer_pct,
        "rvol_threshold": params.rvol_threshold,
        "min_sl_atr_ratio": params.min_sl_atr_ratio,
        "max_sl_atr_ratio": params.max_sl_atr_ratio,
        "rr_ratio": params.rr_ratio,
        "breakeven_r": params.breakeven_r,
        "capital": params.capital,
        "risk_pct": params.risk_pct,
        "portfolio_value": params.portfolio_value,
        "max_positions": params.max_positions,
        "max_position_pct": params.max_position_pct,
        "risk_based_sizing": params.risk_based_sizing,
        "entry_window_end": params.entry_window_end,
        "time_exit": params.time_exit,
        "short_open_to_cpr_atr_min": params.short_open_to_cpr_atr_min,
        "skip_rvol": params.skip_rvol_check,
        "runtime_batch_size": params.runtime_batch_size,
        "atr_sl_buffer": params.atr_sl_buffer,
        "direction_filter": params.direction_filter,
        "fbr_setup_filter": params.fbr_setup_filter,
        "or_atr_min": params.or_atr_min,
        "or_atr_max": params.or_atr_max,
        "max_gap_pct": params.max_gap_pct,
        "long_max_gap_pct": params.long_max_gap_pct,
        "min_price": params.min_price,
        "or_minutes": params.or_minutes,
        "strategy": params.strategy,
        "failure_window": fbr_cfg.failure_window,
        "reversal_buffer_pct": fbr_cfg.reversal_buffer_pct,
        "fbr_min_or_atr": fbr_cfg.fbr_min_or_atr,
        "fbr_failure_depth": fbr_cfg.fbr_failure_depth,
        "fbr_entry_window_end": fbr_cfg.fbr_entry_window_end,
        "cpr_shift_filter": cpr_cfg.cpr_shift_filter,
        "min_effective_rr": cpr_cfg.min_effective_rr,
        "cpr_use_narrowing_filter": cpr_cfg.use_narrowing_filter,
        "fbr_use_narrowing_filter": fbr_cfg.use_narrowing_filter,
        "vcpr_confirm_candles": vcpr_cfg.vcpr_confirm_candles,
        "vcpr_body_pct": vcpr_cfg.vcpr_body_pct,
        "vcpr_sl_mode": vcpr_cfg.vcpr_sl_mode,
        "candle_exit": vcpr_cfg.candle_exit,
        "vcpr_scan_end": vcpr_cfg.vcpr_scan_end,
        "vcpr_min_open_dist_atr": vcpr_cfg.vcpr_min_open_dist_atr,
        "vcpr_scan_start": vcpr_cfg.vcpr_scan_start,
        "cpr_entry_start": cpr_cfg.cpr_entry_start,
        "cpr_confirm_entry": cpr_cfg.cpr_confirm_entry,
        "cpr_hold_confirm": cpr_cfg.cpr_hold_confirm,
        "cpr_min_close_atr": cpr_cfg.cpr_min_close_atr,
        "scale_out_pct": cpr_cfg.scale_out_pct,
        "target_level": cpr_cfg.target_level,
        "rr_gate_target_level": cpr_cfg.rr_gate_target_level,
        "commission_model": params.commission_model,
        "slippage_bps": params.slippage_bps,
        "compound_equity": params.compound_equity,
    }
    return hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:12]


def iter_symbol_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    """Split symbols into fixed-size batches, preserving order."""
    size = max(1, int(batch_size))
    if not symbols:
        return []
    return [symbols[i : i + size] for i in range(0, len(symbols), size)]


def format_runtime_coverage_error(
    *,
    requested_symbols: list[str],
    missing_state: list[str],
    missing_strategy: list[str],
    missing_pack: list[str],
) -> str:
    """Build a strict and actionable runtime coverage validation error message."""
    lines = [
        (f"Runtime table coverage is incomplete for {len(requested_symbols)} requested symbol(s).")
    ]
    if missing_state:
        lines.append(
            "- market_day_state missing "
            f"{len(missing_state)} symbol(s): "
            f"{preview_list(missing_state)}"
        )
    if missing_pack:
        lines.append(
            "- intraday_day_pack missing "
            f"{len(missing_pack)} symbol(s): "
            f"{preview_list(missing_pack)}"
        )
    if missing_strategy:
        lines.append(
            "- strategy_day_state missing "
            f"{len(missing_strategy)} symbol(s): "
            f"{preview_list(missing_strategy)}"
        )
    lines.append(
        "Run full runtime materialization before backtesting: "
        "`uv run pivot-build --force --batch-size 128`"
    )
    return "\n".join(lines)


def strategy_columns_for_minutes(or_minutes: int) -> tuple[str, str]:
    """Return strategy_day_state direction/or_atr columns for selected OR window."""
    mapping = {
        5: ("direction_5", "or_atr_5"),
        10: ("direction_10", "or_atr_10"),
        15: ("direction_15", "or_atr_15"),
        30: ("direction_30", "or_atr_30"),
    }
    cols = mapping.get(or_minutes)
    if cols is None:
        raise ValueError(f"Unsupported or_minutes={or_minutes}. Supported: 5, 10, 15, 30")
    return cols


def count_setup_funnel(
    *,
    db: Any,
    params: BacktestParams,
    symbols: list[str],
    start: str,
    end: str,
) -> FunnelCounts:
    """Run staged COUNT queries to measure how many symbol-days pass each filter."""
    strategy = params.strategy
    funnel = FunnelCounts(strategy=strategy)

    if not symbols:
        return funnel

    if is_feed_audit_pack_source(getattr(params, "pack_source", None)):
        return funnel

    if strategy == "VIRGIN_CPR":
        return funnel

    sym_param = list(symbols)
    direction_col, or_atr_col = strategy_columns_for_minutes(params.or_minutes)
    cpr_cfg = params.cpr_levels
    fbr_cfg = params.fbr

    use_narrowing = (
        cpr_cfg.use_narrowing_filter if strategy == "CPR_LEVELS" else fbr_cfg.use_narrowing_filter
    )
    or_atr_min = (
        max(params.or_atr_min, fbr_cfg.fbr_min_or_atr) if strategy == "FBR" else params.or_atr_min
    )
    regime_close_col = regime_snapshot_close_col(params.regime_snapshot_minutes)

    shift_clause = "1"
    if strategy == "CPR_LEVELS":
        shift_clause = "($cpr_shift_filter = 'ALL' OR m.cpr_shift = $cpr_shift_filter)"

    regime_move_expr = (
        f"CASE WHEN reg.open_915 > 0 AND reg.{regime_close_col} IS NOT NULL "
        f"THEN ((reg.{regime_close_col} - reg.open_915) / reg.open_915) * 100.0 END"
    )
    regime_gate_sql = f"""
                 AND (
                     $regime_index_symbol = ''
                     OR $regime_min_move_pct <= 0
                     OR (
                         {regime_move_expr} IS NOT NULL
                         AND NOT (
                             (s.{direction_col} = 'SHORT' AND {regime_move_expr} >= $regime_min_move_pct)
                             OR (s.{direction_col} = 'LONG' AND {regime_move_expr} <= -$regime_min_move_pct)
                         )
                     )
                 )
    """

    query = f"""
        SELECT
            COUNT(*) AS universe_count,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                THEN 1 ELSE 0
            END) AS after_cpr_width,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                THEN 1 ELSE 0
            END) AS after_direction,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                THEN 1 ELSE 0
            END) AS after_dir_filter,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                 AND ($min_price <= 0 OR m.prev_close >= $min_price)
{regime_gate_sql}
                THEN 1 ELSE 0
            END) AS after_min_price,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                 AND ($min_price <= 0 OR m.prev_close >= $min_price)
                 AND s.gap_abs_pct <= $max_gap
{regime_gate_sql}
                THEN 1 ELSE 0
            END) AS after_gap,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                 AND ($min_price <= 0 OR m.prev_close >= $min_price)
                 AND s.gap_abs_pct <= $max_gap
                 AND s.{or_atr_col} >= $or_atr_min
                 AND s.{or_atr_col} <= $or_atr_max
{regime_gate_sql}
                THEN 1 ELSE 0
            END) AS after_or_atr,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                 AND ($min_price <= 0 OR m.prev_close >= $min_price)
                 AND s.gap_abs_pct <= $max_gap
                 AND s.{or_atr_col} >= $or_atr_min
                 AND s.{or_atr_col} <= $or_atr_max
                 AND (NOT $use_narrowing OR CAST(m.is_narrowing AS INTEGER) = 1)
{regime_gate_sql}
                THEN 1 ELSE 0
            END) AS after_narrowing,
            SUM(CASE
                WHEN m.cpr_width_pct < LEAST(m.cpr_threshold_pct, $max_width)
                 AND s.{direction_col} IN ('LONG', 'SHORT')
                 AND ($direction_filter = 'BOTH' OR s.{direction_col} = $direction_filter)
                 AND ($min_price <= 0 OR m.prev_close >= $min_price)
                 AND s.gap_abs_pct <= $max_gap
                 AND s.{or_atr_col} >= $or_atr_min
                 AND s.{or_atr_col} <= $or_atr_max
                 AND (NOT $use_narrowing OR CAST(m.is_narrowing AS INTEGER) = 1)
                 AND ({shift_clause})
{regime_gate_sql}
                THEN 1 ELSE 0
            END) AS after_shift
        FROM market_day_state m
        JOIN strategy_day_state s
          ON s.symbol = m.symbol AND s.trade_date = m.trade_date
        LEFT JOIN market_day_state reg
          ON reg.symbol = $regime_index_symbol
         AND reg.trade_date = m.trade_date
        WHERE list_contains($symbols, m.symbol)
          AND m.trade_date >= $start::DATE
          AND m.trade_date <= $end::DATE
    """

    params_dict: dict[str, object] = {
        "start": start,
        "end": end,
        "max_width": params.cpr_max_width_pct,
        "direction_filter": params.direction_filter,
        "min_price": params.min_price,
        "max_gap": params.max_gap_pct,
        "or_atr_min": or_atr_min,
        "or_atr_max": params.or_atr_max,
        "use_narrowing": use_narrowing,
        "symbols": sym_param,
        "regime_index_symbol": str(params.regime_index_symbol or "").upper(),
        "regime_min_move_pct": float(params.regime_min_move_pct or 0.0),
    }
    if strategy == "CPR_LEVELS":
        params_dict["cpr_shift_filter"] = cpr_cfg.cpr_shift_filter

    try:
        row = db.con.execute(query, params_dict).fetchone()
    except Exception as e:
        logger.warning("Setup funnel query failed: %s", e)
        return funnel

    if row:
        funnel.universe_count = int(row[0] or 0)
        funnel.after_cpr_width = int(row[1] or 0)
        funnel.after_direction = int(row[2] or 0)
        funnel.after_dir_filter = int(row[3] or 0)
        funnel.after_min_price = int(row[4] or 0)
        funnel.after_gap = int(row[5] or 0)
        funnel.after_or_atr = int(row[6] or 0)
        funnel.after_narrowing = int(row[7] or 0)
        funnel.after_shift = int(row[8] or 0)

    return funnel


__all__ = [
    "count_setup_funnel",
    "format_runtime_coverage_error",
    "iter_symbol_batches",
    "make_param_signature",
    "strategy_columns_for_minutes",
]
