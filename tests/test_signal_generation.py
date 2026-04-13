"""Tests for shared signal-generation helpers."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

import engine.signal_generation as signal_generation


class _FakeFrame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def is_empty(self) -> bool:
        return not self._rows

    def iter_rows(self, named: bool = False):
        if named:
            return iter(self._rows)
        return iter(tuple(row.values()) for row in self._rows)


class _FakeQueryResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def pl(self) -> _FakeFrame:
        return _FakeFrame(self._rows)


class _FakeCon:
    def __init__(self, rows_by_marker: dict[str, list[dict[str, object]]]) -> None:
        self.rows_by_marker = rows_by_marker
        self.calls: list[dict[str, object]] = []

    def execute(self, query: str, params=None) -> _FakeQueryResult:
        self.calls.append({"query": query, "params": list(params or [])})
        for marker, rows in self.rows_by_marker.items():
            if marker in query:
                return _FakeQueryResult(rows)
        return _FakeQueryResult([])


def _patch_db(
    monkeypatch: pytest.MonkeyPatch, rows_by_marker: dict[str, list[dict[str, object]]]
) -> _FakeCon:
    fake_con = _FakeCon(rows_by_marker)
    monkeypatch.setattr(signal_generation, "get_db", lambda: SimpleNamespace(con=fake_con))
    return fake_con


def test_check_narrow_cpr_returns_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_con = _patch_db(
        monkeypatch,
        {
            "narrow-cpr": [
                {
                    "symbol": "SBIN",
                    "trade_date": date(2024, 1, 2),
                    "condition": "narrow-cpr",
                    "details": "CPR width 1.20% < threshold 2.00% (Narrowing)",
                    "cpr_width": 1.2,
                    "pivot": 99.0,
                    "tc": 100.0,
                    "bc": 98.0,
                }
            ]
        },
    )

    signals = signal_generation.check_narrow_cpr(["SBIN"], date(2024, 1, 2))

    assert len(signals) == 1
    assert signals[0].symbol == "SBIN"
    assert signals[0].condition == "narrow-cpr"
    assert signals[0].cpr_width == pytest.approx(1.2)
    assert "narrow-cpr" in fake_con.calls[0]["query"]
    assert fake_con.calls[0]["params"] == ["SBIN", date(2024, 1, 2)]


def test_check_virgin_cpr_returns_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_con = _patch_db(
        monkeypatch,
        {
            "virgin-cpr": [
                {
                    "symbol": "TCS",
                    "trade_date": date(2024, 1, 3),
                    "condition": "virgin-cpr",
                    "details": "Virgin CPR zone — BULLISH (OR: 101.00 / 99.00)",
                    "cpr_width": 0.8,
                    "pivot": 100.0,
                    "tc": 101.0,
                    "bc": 99.0,
                }
            ]
        },
    )

    signals = signal_generation.check_virgin_cpr(["TCS"], date(2024, 1, 3))

    assert len(signals) == 1
    assert signals[0].symbol == "TCS"
    assert signals[0].condition == "virgin-cpr"
    assert signals[0].tc == pytest.approx(101.0)
    assert "virgin-cpr" in fake_con.calls[0]["query"]
    assert fake_con.calls[0]["params"] == ["TCS", date(2024, 1, 3)]


def test_check_orb_fail_returns_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_con = _patch_db(
        monkeypatch,
        {
            "orb-fail": [
                {
                    "symbol": "RELIANCE",
                    "trade_date": date(2024, 1, 4),
                    "condition": "orb-fail",
                    "details": "ORB failed (BEARISH_OR) → FBR LONG entry signal (OR: 100.00, Close: 96.00)",
                    "cpr_width": 1.4,
                    "pivot": 98.0,
                    "tc": 99.0,
                    "bc": 97.0,
                }
            ]
        },
    )

    signals = signal_generation.check_orb_fail(["RELIANCE"], date(2024, 1, 4))

    assert len(signals) == 1
    assert signals[0].symbol == "RELIANCE"
    assert signals[0].condition == "orb-fail"
    assert signals[0].bc == pytest.approx(97.0)
    assert "orb-fail" in fake_con.calls[0]["query"]
    assert fake_con.calls[0]["params"] == ["RELIANCE", date(2024, 1, 4)]


def test_check_gap_signals_filters_direction(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_con = _patch_db(
        monkeypatch,
        {
            "gap_pct": [
                {
                    "symbol": "SBIN",
                    "trade_date": date(2024, 1, 5),
                    "condition": "gap-up",
                    "details": "GAP UP 2.10% (Prev: 100.00, Today: 102.10)",
                    "cpr_width": 1.1,
                    "pivot": 101.0,
                    "tc": 102.0,
                    "bc": 100.0,
                }
            ]
        },
    )

    signals = signal_generation.check_gap_signals(["SBIN"], date(2024, 1, 5), 1.5, "up")

    assert len(signals) == 1
    assert signals[0].symbol == "SBIN"
    assert signals[0].condition == "gap-up"
    assert "gap_pct" in fake_con.calls[0]["query"]
    assert "AND s.gap_pct > 0" in fake_con.calls[0]["query"]
    assert fake_con.calls[0]["params"] == ["SBIN", date(2024, 1, 5), 1.5]
