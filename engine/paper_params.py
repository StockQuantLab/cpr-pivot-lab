"""Paper runtime state and strategy parameter helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from db.paper_db import PaperSession
from engine.cpr_atr_strategy import StrategyConfig
from engine.strategy_presets import (
    build_strategy_config_from_overrides as shared_build_strategy_config_from_overrides,
)

BacktestParams = StrategyConfig


@dataclass(slots=True)
class SymbolRuntimeState:
    trade_date: str | None = None
    candles: list[dict[str, Any]] = field(default_factory=list)
    time_str: list[str] = field(default_factory=list)
    opens: list[float] = field(default_factory=list)
    highs: list[float] = field(default_factory=list)
    lows: list[float] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    volumes: list[float] = field(default_factory=list)
    setup_row: dict[str, Any] | None = None
    setup_refresh_bar_end: datetime | None = None
    position_closed_today: bool = False
    entry_window_closed_without_trade: bool = False


@dataclass(slots=True)
class PaperRuntimeState:
    symbols: dict[str, SymbolRuntimeState] = field(default_factory=dict)
    session_params_key: str | None = None
    session_params: BacktestParams | None = None
    allow_live_setup_fallback: bool = True
    bar_end_offset: timedelta | None = None
    skipped_setup_rows: int = 0
    invalid_setup_rows: int = 0

    def for_symbol(self, symbol: str) -> SymbolRuntimeState:
        state = self.symbols.get(symbol)
        if state is None:
            state = SymbolRuntimeState()
            self.symbols[symbol] = state
        return state

    def get_session_params(self, session: PaperSession) -> BacktestParams:
        raw_key = json.dumps(
            {
                "strategy": getattr(session, "strategy", None),
                "strategy_params": getattr(session, "strategy_params", {}) or {},
            },
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        if self.session_params is None or self.session_params_key != raw_key:
            self.session_params = build_backtest_params(session)
            self.session_params_key = raw_key
        return self.session_params


def build_backtest_params_from_overrides(
    strategy: str,
    overrides: Mapping[str, Any] | None = None,
) -> BacktestParams:
    return shared_build_strategy_config_from_overrides(strategy, overrides)


def apply_paper_strategy_defaults(
    strategy: str,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize explicit paper overrides without injecting paper-only defaults."""
    _ = str(strategy or "CPR_LEVELS").upper()
    resolved = dict(overrides or {})
    resolved["direction_filter"] = str(resolved.get("direction_filter", "BOTH") or "BOTH").upper()
    return resolved


def build_backtest_params(session: PaperSession) -> BacktestParams:
    strategy = getattr(session, "strategy", None)
    strategy_params = session.strategy_params or {}
    if not strategy and isinstance(strategy_params, Mapping):
        strategy = strategy_params.get("strategy")
    return build_backtest_params_from_overrides(str(strategy or "CPR_LEVELS"), strategy_params)


build_strategy_config_from_overrides = build_backtest_params_from_overrides
build_strategy_config = build_backtest_params


__all__ = [
    "BacktestParams",
    "PaperRuntimeState",
    "SymbolRuntimeState",
    "apply_paper_strategy_defaults",
    "build_backtest_params",
    "build_backtest_params_from_overrides",
    "build_strategy_config",
    "build_strategy_config_from_overrides",
]
