from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from engine.paper_runtime import PaperRuntimeState
from scripts.paper_live import _prefetch_setup_rows, _resolve_poll_interval


def test_resolve_poll_interval_pulls_tighter_near_candle_close() -> None:
    settings = SimpleNamespace(paper_live_poll_interval_sec=5.0)
    now = datetime(2024, 1, 1, 9, 19, 57)

    assert _resolve_poll_interval(settings, None, 5, now=now) == 0.5


def test_resolve_poll_interval_keeps_base_far_from_close() -> None:
    settings = SimpleNamespace(paper_live_poll_interval_sec=5.0)
    now = datetime(2024, 1, 1, 9, 16, 0)

    assert _resolve_poll_interval(settings, None, 5, now=now) == 5.0


def test_prefetch_setup_rows_skips_invalid_critical_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=True)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return {
            "trade_date": "2024-01-01",
            "direction": "LONG",
            "tc": 0.0,
            "bc": 99.5,
            "atr": 1.2,
        }

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None


def test_prefetch_setup_rows_skips_invalid_critical_fields_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=False)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return {
            "trade_date": "2024-01-01",
            "direction": "SHORT",
            "tc": 101.0,
            "bc": 100.0,
            "atr": 0.0,
        }

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None


def test_prefetch_setup_rows_skips_missing_rows_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_state = PaperRuntimeState(allow_live_setup_fallback=False)

    def fake_load_setup_row(symbol: str, trade_date: str, **kwargs):
        _ = (symbol, trade_date, kwargs)
        return None

    monkeypatch.setattr("scripts.paper_live.load_setup_row", fake_load_setup_row)

    _prefetch_setup_rows(
        runtime_state=runtime_state,
        symbols=["SBIN"],
        trade_date="2024-01-01",
        candle_interval_minutes=5,
    )

    assert runtime_state.symbols["SBIN"].setup_row is None
