"""Strategy parameter resolution helpers for paper trading commands."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from engine.execution_defaults import (
    DEFAULT_MAX_POSITION_PCT,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_PORTFOLIO_VALUE,
)
from engine.paper_runtime import apply_paper_strategy_defaults, build_backtest_params_from_overrides
from engine.strategy_presets import (
    ALL_STRATEGY_PRESETS,
    build_strategy_config_from_preset,
    list_strategy_preset_names,
)


def normalize_strategy_params(strategy_params: dict[str, Any] | None) -> dict[str, Any]:
    """Canonicalize strategy params via JSON round-trip for stable dict comparison."""
    parsed = strategy_params or {}
    return json.loads(json.dumps(parsed, sort_keys=True, separators=(",", ":")))


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


def _variant_params(base: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    """Return a shallow copy of a canonical variant recipe with overrides applied."""
    params = dict(base)
    params.update(overrides)
    return params


CPR_CANONICAL_PARAMS: dict[str, Any] = dict(
    ALL_STRATEGY_PRESETS["CPR_LEVELS_RISK_LONG"]["overrides"]
)
CPR_CANONICAL_SHORT_PARAMS: dict[str, Any] = dict(
    ALL_STRATEGY_PRESETS["CPR_LEVELS_RISK_SHORT"]["overrides"]
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
    # CPR SHORT: exact canonical SHORT preset, including SHORT-only trailing config.
    (
        "CPR_LEVELS_SHORT",
        "CPR_LEVELS",
        _variant_params(CPR_CANONICAL_SHORT_PARAMS, direction_filter="SHORT"),
    ),
)


_STRATEGY_METADATA_KEYS = {
    "_canonical_preset",
    "_strategy_config_fingerprint",
    "_resolved_strategy_config",
}
_NON_STRATEGY_PARAM_KEYS = {"feed_source"}


def _strip_strategy_metadata(params: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        key: value
        for key, value in dict(params or {}).items()
        if key not in _STRATEGY_METADATA_KEYS
    }


def _resolved_strategy_config_dict(
    strategy: str,
    strategy_params: Mapping[str, Any] | None,
) -> dict[str, Any]:
    config = build_backtest_params_from_overrides(
        strategy, _strip_strategy_metadata(strategy_params)
    )
    return json.loads(json.dumps(asdict(config), sort_keys=True, default=str))


def _strategy_config_fingerprint(resolved_config: Mapping[str, Any]) -> str:
    payload = json.dumps(resolved_config, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _with_resolved_strategy_metadata(
    strategy: str,
    strategy_params: Mapping[str, Any] | None,
    *,
    canonical_preset: str | None = None,
) -> dict[str, Any]:
    params = _strip_strategy_metadata(strategy_params)
    preset_name = str(canonical_preset or "").strip().upper()
    if preset_name:
        preset = ALL_STRATEGY_PRESETS.get(preset_name)
        if preset is None:
            raise ValueError(f"Unknown strategy preset: {preset_name}")
        resolved_preset_strategy = _assert_cpr_only_strategy(
            str(preset["strategy"]), source=f"preset {preset_name}"
        )
        passthrough = {
            key: value for key, value in params.items() if key in _NON_STRATEGY_PARAM_KEYS
        }
        supplied_config_overrides = {
            key: value for key, value in params.items() if key not in _NON_STRATEGY_PARAM_KEYS
        }
        if supplied_config_overrides:
            preset_config = asdict(build_strategy_config_from_preset(preset_name))
            supplied_config = asdict(
                build_backtest_params_from_overrides(
                    resolved_preset_strategy,
                    {**dict(preset["overrides"]), **supplied_config_overrides},
                )
            )
            if supplied_config != preset_config:
                diffs = _strategy_config_diffs(supplied_config, preset_config)
                detail = "; ".join(diffs[:8])
                raise SystemExit(
                    f"Canonical preset {preset_name} received non-canonical overrides"
                    f"{': ' + detail if detail else ''}. "
                    "Do not mark ad hoc params as canonical."
                )
        strategy = resolved_preset_strategy
        params = {**dict(preset["overrides"]), **passthrough}
        params["_canonical_preset"] = preset_name
    resolved = _resolved_strategy_config_dict(strategy, params)
    params["_resolved_strategy_config"] = resolved
    params["_strategy_config_fingerprint"] = _strategy_config_fingerprint(resolved)
    return normalize_strategy_params(params)


def _paper_multi_preset_for_label(label: str) -> str:
    normalized = str(label or "").strip().upper()
    if normalized == "CPR_LEVELS_LONG":
        return "CPR_LEVELS_RISK_LONG"
    if normalized == "CPR_LEVELS_SHORT":
        return "CPR_LEVELS_RISK_SHORT"
    raise ValueError(f"No canonical paper preset mapping for {label!r}")


def _strategy_config_diffs(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    prefix: str = "",
) -> list[str]:
    diffs: list[str] = []
    keys = sorted(set(left) | set(right))
    for key in keys:
        left_value = left.get(key)
        right_value = right.get(key)
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(left_value, dict) and isinstance(right_value, dict):
            diffs.extend(_strategy_config_diffs(left_value, right_value, prefix=path))
        elif left_value != right_value:
            diffs.append(f"{path}: {left_value!r} != {right_value!r}")
    return diffs


def _assert_paper_multi_params_match_preset(
    label: str,
    strategy: str,
    strategy_params: Mapping[str, Any],
) -> str:
    preset_name = _paper_multi_preset_for_label(label)
    preset = ALL_STRATEGY_PRESETS[preset_name]
    preset_config = asdict(build_strategy_config_from_preset(preset_name))
    params_config = asdict(
        build_backtest_params_from_overrides(strategy, _strip_strategy_metadata(strategy_params))
    )
    if str(preset["strategy"]).upper() != str(strategy).upper() or params_config != preset_config:
        diffs = _strategy_config_diffs(params_config, preset_config)
        detail = "; ".join(diffs[:8])
        suffix = f": {detail}" if detail else ""
        raise SystemExit(
            f"{label} paper params do not match preset {preset_name}{suffix}. "
            "Fix the shared preset, not live-only params."
        )
    return preset_name


def _prepare_paper_multi_strategy_params(
    label: str,
    strategy: str,
    base_params: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = apply_paper_strategy_defaults(
        strategy, normalize_strategy_params(dict(base_params))
    )
    preset_name = _assert_paper_multi_params_match_preset(label, strategy, normalized)
    return _with_resolved_strategy_metadata(strategy, normalized, canonical_preset=preset_name)


def _session_execution_kwargs(
    strategy: str,
    strategy_params: Mapping[str, Any] | None,
) -> dict[str, Any]:
    resolved = _resolved_strategy_config_dict(strategy, strategy_params)
    return {
        "portfolio_value": float(resolved.get("portfolio_value") or DEFAULT_PORTFOLIO_VALUE),
        "max_positions": int(resolved.get("max_positions") or DEFAULT_MAX_POSITIONS),
        "max_position_pct": float(resolved.get("max_position_pct") or DEFAULT_MAX_POSITION_PCT),
    }


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
    elif getattr(args, "no_skip_rvol", False):
        overrides["skip_rvol_check"] = False
    if getattr(args, "standard_sizing", False):
        overrides["risk_based_sizing"] = False
    elif not has_preset and getattr(args, "risk_based_sizing", False):
        # In non-preset mode apply the CLI default (True). In preset mode, skip this so the
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
    pack_source = getattr(args, "pack_source", None)
    if pack_source:
        overrides["pack_source"] = str(pack_source)
    pack_source_session_id = getattr(args, "pack_source_session_id", None)
    if pack_source_session_id:
        overrides["pack_source_session_id"] = str(pack_source_session_id)
    cpr_min_close_atr = getattr(args, "cpr_min_close_atr", None)
    if cpr_min_close_atr is not None:
        overrides["cpr_min_close_atr"] = float(cpr_min_close_atr)
    scale_out_pct = getattr(args, "cpr_scale_out_pct", None)
    if scale_out_pct is not None:
        overrides.setdefault("cpr_levels_config", {})
        overrides["cpr_levels_config"]["scale_out_pct"] = float(scale_out_pct)
    target_level = getattr(args, "cpr_target_level", None)
    if target_level:
        overrides.setdefault("cpr_levels_config", {})
        overrides["cpr_levels_config"]["target_level"] = str(target_level).upper()
    rr_gate_target = getattr(args, "cpr_rr_gate_target", None)
    if rr_gate_target:
        overrides.setdefault("cpr_levels_config", {})
        overrides["cpr_levels_config"]["rr_gate_target_level"] = str(rr_gate_target).upper()
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
    normalized = apply_paper_strategy_defaults(resolved_strategy, normalize_strategy_params(params))
    if preset_name:
        preset_config = asdict(build_strategy_config_from_preset(preset_name))
        normalized_config = asdict(
            build_backtest_params_from_overrides(resolved_strategy, normalized)
        )
        if normalized_config == preset_config:
            normalized["_canonical_preset"] = preset_name
    return normalized
