"""Canonical strategy presets shared by backtest, paper replay, and live.

These presets are the named, user-facing form of the same StrategyConfig object
used by the engine. They exist so operators can select a baseline by name
instead of re-specifying the full flag bundle on every command.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from engine.cpr_atr_strategy import StrategyConfig
from engine.execution_defaults import DEFAULT_EXECUTION_SIZING

CPR_LEVELS_PRESETS: dict[str, dict[str, Any]] = {
    "CPR_LEVELS_RISK_LONG": {
        "strategy": "CPR_LEVELS",
        "overrides": {
            **DEFAULT_EXECUTION_SIZING,
            "direction_filter": "LONG",
            "min_price": 50.0,
            "cpr_min_close_atr": 0.5,
            "narrowing_filter": True,
            "risk_based_sizing": True,
            "skip_rvol_check": False,
            "momentum_confirm": True,
        },
    },
    "CPR_LEVELS_RISK_SHORT": {
        "strategy": "CPR_LEVELS",
        "overrides": {
            **DEFAULT_EXECUTION_SIZING,
            "direction_filter": "SHORT",
            "min_price": 50.0,
            "cpr_min_close_atr": 0.5,
            "narrowing_filter": True,
            "risk_based_sizing": True,
            "skip_rvol_check": True,
            "short_trail_atr_multiplier": 1.25,
            "momentum_confirm": True,
        },
    },
    "CPR_LEVELS_STANDARD_LONG": {
        "strategy": "CPR_LEVELS",
        "overrides": {
            **DEFAULT_EXECUTION_SIZING,
            "direction_filter": "LONG",
            "min_price": 50.0,
            "cpr_min_close_atr": 0.5,
            "narrowing_filter": True,
            "risk_based_sizing": False,
            "skip_rvol_check": False,
            "momentum_confirm": True,
        },
    },
    "CPR_LEVELS_STANDARD_SHORT": {
        "strategy": "CPR_LEVELS",
        "overrides": {
            **DEFAULT_EXECUTION_SIZING,
            "direction_filter": "SHORT",
            "min_price": 50.0,
            "cpr_min_close_atr": 0.5,
            "narrowing_filter": True,
            "risk_based_sizing": False,
            "skip_rvol_check": True,
            "short_trail_atr_multiplier": 1.25,
            "momentum_confirm": True,
        },
    },
}

FBR_PRESETS: dict[str, dict[str, Any]] = {
    "FBR_RISK_LONG": {
        "strategy": "FBR",
        "overrides": {
            # FBR LONG means a failed SHORT breakdown that reverses LONG internally.
            "direction_filter": "SHORT",
            "fbr_setup_filter": "BREAKDOWN",
            "failure_window": 10,
            "min_price": 50.0,
            "risk_based_sizing": True,
            "skip_rvol_check": True,
        },
    },
    "FBR_RISK_SHORT": {
        "strategy": "FBR",
        "overrides": {
            # FBR SHORT means a failed LONG breakout that reverses SHORT internally.
            "direction_filter": "LONG",
            "fbr_setup_filter": "BREAKOUT",
            "failure_window": 10,
            "min_price": 50.0,
            "risk_based_sizing": True,
            "skip_rvol_check": True,
        },
    },
}

ALL_STRATEGY_PRESETS: dict[str, dict[str, Any]] = {
    **CPR_LEVELS_PRESETS,
    **FBR_PRESETS,
}


def build_strategy_config_from_overrides(
    strategy: str,
    overrides: Mapping[str, Any] | None = None,
) -> StrategyConfig:
    """Build a canonical StrategyConfig from flat or nested overrides."""
    params = StrategyConfig(strategy=str(strategy or "CPR_LEVELS").upper())
    overrides = dict(overrides or {})

    simple_fields = {
        "cpr_percentile",
        "cpr_max_width_pct",
        "atr_periods",
        "buffer_pct",
        "rvol_threshold",
        "entry_window_end",
        "min_sl_atr_ratio",
        "max_sl_atr_ratio",
        "rr_ratio",
        "breakeven_r",
        "atr_sl_buffer",
        "trail_atr_multiplier",
        "short_trail_atr_multiplier",
        "capital",
        "risk_pct",
        "portfolio_value",
        "max_positions",
        "max_position_pct",
        "time_exit",
        "rvol_lookback_days",
        "skip_rvol_check",
        "runtime_batch_size",
        "direction_filter",
        "fbr_setup_filter",
        "short_open_to_cpr_atr_min",
        "risk_based_sizing",
        "compound_equity",
        "legacy_sizing",
        "or_minutes",
        "or_atr_min",
        "or_atr_max",
        "max_gap_pct",
        "long_max_gap_pct",
        "min_price",
        "regime_index_symbol",
        "regime_min_move_pct",
        "regime_snapshot_minutes",
        "pack_source",
        "pack_source_session_id",
        "strategy",
    }
    overrides.pop("version", None)
    if "legacy_sizing" in overrides and "risk_based_sizing" not in overrides:
        overrides["risk_based_sizing"] = overrides.pop("legacy_sizing")
    else:
        overrides.pop("legacy_sizing", None)
    scalar_overrides = {key: overrides[key] for key in simple_fields if key in overrides}
    if scalar_overrides:
        params = replace(params, **scalar_overrides)

    cpr_overrides = dict(overrides.get("cpr_levels_config") or {})
    fbr_overrides = dict(overrides.get("fbr_config") or {})

    for _cpr_field in (
        "cpr_min_close_atr",
        "target_level",
        "rr_gate_target_level",
        "time_stop_bars",
        "momentum_confirm",
    ):
        if _cpr_field in overrides and _cpr_field not in cpr_overrides:
            cpr_overrides[_cpr_field] = overrides[_cpr_field]
    if "cpr_target_level" in overrides and "target_level" not in cpr_overrides:
        cpr_overrides["target_level"] = overrides["cpr_target_level"]
    if "cpr_rr_gate_target" in overrides and "rr_gate_target_level" not in cpr_overrides:
        cpr_overrides["rr_gate_target_level"] = overrides["cpr_rr_gate_target"]
    if "cpr_scale_out_pct" in overrides and "scale_out_pct" not in cpr_overrides:
        cpr_overrides["scale_out_pct"] = overrides["cpr_scale_out_pct"]
    if "scale_out_pct" in overrides and "scale_out_pct" not in cpr_overrides:
        cpr_overrides["scale_out_pct"] = overrides["scale_out_pct"]
    if "narrowing_filter" in overrides:
        cpr_overrides.setdefault("use_narrowing_filter", overrides["narrowing_filter"])
        fbr_overrides.setdefault("use_narrowing_filter", overrides["narrowing_filter"])
    if "failure_window" in overrides and "failure_window" not in fbr_overrides:
        fbr_overrides["failure_window"] = overrides["failure_window"]

    if cpr_overrides:
        params = replace(params, cpr_levels_config=replace(params.cpr_levels, **cpr_overrides))
    if fbr_overrides:
        params = replace(params, fbr_config=replace(params.fbr, **fbr_overrides))
    if isinstance(overrides.get("virgin_cpr_config"), dict):
        params = replace(
            params,
            virgin_cpr_config=replace(params.virgin_cpr, **overrides["virgin_cpr_config"]),
        )
    return params


def build_strategy_config_from_preset(
    preset_name: str,
    overrides: Mapping[str, Any] | None = None,
) -> StrategyConfig:
    """Resolve a named preset into a StrategyConfig."""
    key = str(preset_name or "").upper()
    preset = ALL_STRATEGY_PRESETS.get(key)
    if preset is None:
        raise KeyError(f"Unknown strategy preset: {preset_name!r}")
    merged = dict(preset["overrides"])
    if overrides:
        merged.update(overrides)
    merged.pop("strategy", None)
    return build_strategy_config_from_overrides(str(preset["strategy"]), merged)


def list_strategy_preset_names(strategy: str | None = None) -> list[str]:
    """Return preset names, optionally filtered by strategy."""
    if not strategy:
        return sorted(ALL_STRATEGY_PRESETS)
    strategy_upper = str(strategy).upper()
    return sorted(
        name
        for name, preset in ALL_STRATEGY_PRESETS.items()
        if preset["strategy"] == strategy_upper
    )
