"""Trailing-stop cache and state helpers for paper runtime."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from db.paper_db import PaperPosition
from engine.cpr_atr_utils import TrailingStop
from engine.paper_params import BacktestParams

_TRAILING_STOP_CACHE: dict[str, TrailingStop] = {}
_TRAILING_STOP_CACHE_KEYS: dict[str, tuple[Any, ...]] = {}


def _hhmm(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%H:%M")


def clear_trailing_stop_cache(position_id: str) -> None:
    key = str(position_id)
    _TRAILING_STOP_CACHE.pop(key, None)
    _TRAILING_STOP_CACHE_KEYS.pop(key, None)


def _updated_trail_state(
    ts: TrailingStop, trail_state: dict[str, Any], candle: dict[str, Any]
) -> dict[str, Any]:
    return {
        **trail_state,
        "current_sl": float(ts.current_sl),
        "phase": ts.phase,
        "highest_since_entry": float(ts.highest_since_entry),
        "lowest_since_entry": float(ts.lowest_since_entry),
        "last_candle_ts": _hhmm(candle["bar_end"]),
    }


def _trailing_stop_cache_key(
    *,
    position: PaperPosition,
    trail_state: dict[str, Any],
    params: BacktestParams,
) -> tuple[Any, ...]:
    entry_price = float(trail_state.get("entry_price") or position.entry_price)
    direction = str(trail_state.get("direction") or position.direction).upper()
    initial_sl = trail_state.get("initial_sl")
    if initial_sl is None:
        initial_sl = position.stop_loss if position.stop_loss is not None else entry_price
    return (
        position.position_id,
        direction,
        float(entry_price),
        float(initial_sl),
        float(trail_state.get("atr") or 0.0),
        float(trail_state.get("trail_atr_multiplier") or 1.0),
        float(trail_state.get("rr_ratio") or params.rr_ratio),
        float(trail_state.get("breakeven_r") or params.breakeven_r),
    )


def _build_trailing_stop(
    *,
    position: PaperPosition,
    trail_state: dict[str, Any],
    params: BacktestParams,
) -> TrailingStop:
    entry_price = float(trail_state.get("entry_price") or position.entry_price)
    direction = str(trail_state.get("direction") or position.direction).upper()
    initial_sl = trail_state.get("initial_sl")
    if initial_sl is None:
        initial_sl = position.stop_loss if position.stop_loss is not None else entry_price
    return TrailingStop(
        entry_price=entry_price,
        direction=direction,
        sl_price=float(initial_sl),
        atr=float(trail_state.get("atr") or 0.0),
        trail_atr_multiplier=float(trail_state.get("trail_atr_multiplier") or 1.0),
        rr_ratio=float(trail_state.get("rr_ratio") or params.rr_ratio),
        breakeven_r=float(trail_state.get("breakeven_r") or params.breakeven_r),
    )


def _get_trailing_stop(
    position: PaperPosition,
    params: BacktestParams,
    trail_state: dict[str, Any],
) -> TrailingStop:
    key = str(position.position_id)
    cache_key = _trailing_stop_cache_key(
        position=position,
        trail_state=trail_state,
        params=params,
    )
    cached = _TRAILING_STOP_CACHE.get(key)
    if cached is None or _TRAILING_STOP_CACHE_KEYS.get(key) != cache_key:
        cached = _build_trailing_stop(
            position=position,
            trail_state=trail_state,
            params=params,
        )
        _TRAILING_STOP_CACHE[key] = cached
        _TRAILING_STOP_CACHE_KEYS[key] = cache_key

    cached.current_sl = float(trail_state.get("current_sl") or cached.current_sl)
    cached.phase = str(trail_state.get("phase") or cached.phase)
    cached.highest_since_entry = float(
        trail_state.get("highest_since_entry") or cached.highest_since_entry
    )
    cached.lowest_since_entry = float(
        trail_state.get("lowest_since_entry") or cached.lowest_since_entry
    )
    return cached


__all__ = [
    "_build_trailing_stop",
    "_get_trailing_stop",
    "_trailing_stop_cache_key",
    "_updated_trail_state",
    "clear_trailing_stop_cache",
]
