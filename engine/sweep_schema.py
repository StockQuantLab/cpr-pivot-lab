"""YAML sweep configuration schema and loader for pivot-sweep.

Validates sweep configs against the backtest parser's argparse definition
(action dest names) and computes cartesian product of sweep axes.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from engine.constants import PUBLIC_STRATEGIES

# Control flags managed by the sweep runner itself — not allowed in YAML sweep params.
_CONTROL_FLAGS = frozenset(
    {
        "save",
        "quiet",
        "yes_full_run",
        "progress_file",
        "chunk_by",
    }
)


def _get_valid_param_names() -> frozenset[str]:
    """Get valid param dest names from the backtest parser.

    Imports build_parser lazily to avoid DB connections at import time.
    """
    from engine.run_backtest import build_parser

    names: set[str] = set()
    for action in build_parser()._actions:
        if action.dest and action.dest != "help":
            names.add(action.dest)
    return frozenset(names)


def _validate_param_names(params: dict[str, Any], context: str = "params") -> None:
    """Validate that all param names are valid backtest param dest names."""
    valid = _get_valid_param_names()
    for key in params:
        if key not in valid:
            raise ValueError(
                f"{key!r} in {context} is not a valid backtest param. "
                f"Run `pivot-backtest --help` for valid options."
            )


def _validate_control_flags(params: dict[str, Any]) -> None:
    """Ensure no control flags leaked into sweep params."""
    for key in params:
        if key in _CONTROL_FLAGS:
            raise ValueError(
                f"{key!r} is a control flag managed by the sweep runner. Remove it from {params}."
            )


@dataclass(frozen=True)
class SweepAxis:
    """Single parameter axis to sweep."""

    param: str
    values: list[Any]

    def __post_init__(self) -> None:
        valid = _get_valid_param_names()
        if self.param not in valid:
            raise ValueError(
                f"{self.param!r} is not a valid backtest param. "
                f"Run `pivot-backtest --help` for valid options."
            )
        if not self.values:
            raise ValueError(f"Sweep axis {self.param!r} has empty values")

    def combinations(self) -> list[dict[str, Any]]:
        return [{self.param: v} for v in self.values]


_VALID_COMPARE_METRICS = frozenset(
    {
        "calmar",
        "win_rate",
        "total_pnl",
        "profit_factor",
        "annual_return_pct",
        "max_dd_pct",
        "trade_count",
    }
)


@dataclass(frozen=True)
class SweepCompare:
    """Comparison and ranking configuration."""

    metric: str = "calmar"
    sort: str = "desc"  # "asc" or "desc"
    top_n: int = 5

    def __post_init__(self) -> None:
        if self.metric not in _VALID_COMPARE_METRICS:
            raise ValueError(
                f"Unknown compare metric: {self.metric!r}. Valid: {sorted(_VALID_COMPARE_METRICS)}"
            )
        if self.sort not in ("asc", "desc"):
            raise ValueError(f"Sort must be 'asc' or 'desc', got {self.sort!r}")


@dataclass
class SweepConfig:
    """Full sweep configuration."""

    name: str
    strategy: str
    base_params: dict[str, Any] = field(default_factory=dict)
    sweep: list[SweepAxis] = field(default_factory=list)
    compare: SweepCompare = field(default_factory=SweepCompare)
    compare_against: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SweepConfig:
        # Validate strategy
        strategy = raw.get("strategy", "")
        if strategy not in PUBLIC_STRATEGIES:
            raise ValueError(f"Unknown strategy {strategy!r}. Must be one of {PUBLIC_STRATEGIES}")

        # Validate base_params
        base_params = raw.get("base_params", {})
        _validate_param_names(base_params, context="base_params")
        _validate_control_flags(base_params)

        # Validate sweep axes (check for duplicates)
        sweep_axes: list[SweepAxis] = []
        seen_params: set[str] = set()
        for a in raw.get("sweep", []):
            axis = SweepAxis(param=a["param"], values=a["values"])
            if axis.param in seen_params:
                raise ValueError(f"Duplicate sweep axis: {axis.param!r}")
            seen_params.add(axis.param)
            sweep_axes.append(axis)

        compare_raw = raw.get("compare", {})
        compare = SweepCompare(**compare_raw) if compare_raw else SweepCompare()

        # Baseline comparison: map of label → run_id
        compare_against = raw.get("compare_against", {})
        if not isinstance(compare_against, dict):
            raise ValueError("compare_against must be a mapping of label → run_id")

        return cls(
            name=raw["name"],
            strategy=strategy,
            base_params=base_params,
            sweep=sweep_axes,
            compare=compare,
            compare_against=compare_against,
            tags=raw.get("tags", []),
        )

    @classmethod
    def _cartesian(cls, axes: list[SweepAxis]) -> list[dict[str, Any]]:
        """Compute cartesian product of all axis combinations."""
        if not axes:
            return [{}]
        keys = [a.param for a in axes]
        value_lists = [a.values for a in axes]
        return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*value_lists)]

    def combinations(self) -> list[dict[str, Any]]:
        return self._cartesian(self.sweep)

    def build_params_for(self, combo: dict[str, Any]) -> dict[str, Any]:
        """Merge base_params with a single sweep combination."""
        merged = {**self.base_params, "strategy": self.strategy, **combo}
        return merged


def load_sweep_config(path: Path) -> SweepConfig:
    """Load sweep configuration from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML root must be a mapping, got {type(raw).__name__}")
    return SweepConfig.from_dict(raw)
