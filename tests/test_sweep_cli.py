"""Tests for the YAML sweep CLI helpers."""

from __future__ import annotations

from types import SimpleNamespace

import db.duckdb
import engine.sweep_cli as sweep_cli


def test_build_baseline_delta_matches_semantic_labels(monkeypatch):
    rows = [
        {
            "run_id": "new-long",
            "trade_count": 12,
            "win_rate": 55.0,
            "total_pnl": 1200.0,
            "calmar": 1.25,
            "annual_return_pct": 18.0,
            "max_dd_pct": -4.0,
        },
        {
            "run_id": "baseline-long",
            "trade_count": 10,
            "win_rate": 50.0,
            "total_pnl": 1000.0,
            "calmar": 1.0,
            "annual_return_pct": 15.0,
            "max_dd_pct": -5.0,
        },
    ]

    class _FakeResult:
        def pl(self):
            return self

        def to_dicts(self):
            return rows

    class _FakeCon:
        def execute(self, sql, params=None):
            assert "run_metrics" in sql
            return _FakeResult()

    class _FakeDB:
        def __init__(self):
            self.con = _FakeCon()

        def close(self):
            pass

    monkeypatch.setattr(db.duckdb, "get_db", lambda: _FakeDB())

    results = [SimpleNamespace(label="STD_LONG", run_id="new-long", exit_code=0)]
    compare_against = {"STD_LONG": "baseline-long"}

    delta = sweep_cli._build_baseline_delta(results, compare_against)

    assert "STD_LONG" in delta
    assert "P/L ₹1,200" in delta
    assert "Δ+200" in delta
    assert "WR 55.0%" in delta


def test_build_label_special_cases_cpr_baselines():
    from engine.sweep_runner import _build_label

    assert (
        _build_label({"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": False}) == "STD_LONG"
    )
    assert (
        _build_label({"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": True})
        == "STD_LONG_CMP"
    )
    assert (
        _build_label({"preset": "CPR_LEVELS_RISK_SHORT", "compound_equity": True})
        == "RISK_SHORT_CMP"
    )
