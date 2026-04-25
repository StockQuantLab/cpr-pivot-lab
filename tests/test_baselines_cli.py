"""Tests for the dedicated CPR baselines CLI."""

from __future__ import annotations

import db.backtest_db
import db.duckdb
import engine.baselines_cli as baselines_cli


def test_build_backtest_args_includes_progress_file(tmp_path):
    progress_file = tmp_path / "baseline.jsonl"
    args = baselines_cli._build_backtest_args(
        "2025-01-01",
        "2025-01-31",
        {"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": True},
        progress_file=progress_file,
    )

    assert "--save" in args
    assert "--yes-full-run" in args
    assert "--quiet" in args
    assert "--progress-file" in args
    assert str(progress_file) in args


def test_build_backtest_args_prefers_saved_universe_snapshot(monkeypatch):
    class _Row:
        @staticmethod
        def fetchone():
            return (1,)

    class _FakeCon:
        def execute(self, sql, params=None):
            if "backtest_universe" in sql:
                return _Row()
            raise AssertionError(f"unexpected sql: {sql}")

    class _FakeDB:
        def __init__(self):
            self.con = _FakeCon()

        def close(self):
            pass

    monkeypatch.setattr(db.duckdb, "get_db", lambda: _FakeDB())

    args = baselines_cli._build_backtest_args(
        "2025-01-01",
        "2026-04-24",
        {"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": False},
    )

    assert "--universe-name" in args
    assert "full_2026_04_24" in args
    assert "--all" not in args


def test_find_previous_baselines_matches_legacy_param_signature(monkeypatch):
    captured: dict[str, object] = {}
    closed = {"value": False}

    rows = [
        {
            "run_id": "run-long",
            "compound": "false",
            "risk_sizing": "false",
            "direction": "LONG",
        },
        {
            "run_id": "run-short-cmp",
            "compound": "true",
            "risk_sizing": "true",
            "direction": "SHORT",
        },
    ]

    class _FakeResult:
        def pl(self):
            return self

        def to_dicts(self):
            return rows

    class _FakeCon:
        def execute(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return _FakeResult()

    class _FakeDB:
        def __init__(self):
            self.con = _FakeCon()

    fake_db = _FakeDB()
    monkeypatch.setattr(db.backtest_db, "get_backtest_db", lambda: fake_db)
    monkeypatch.setattr(
        db.backtest_db, "close_backtest_db", lambda: closed.__setitem__("value", True)
    )

    previous = baselines_cli._find_previous_baselines("2026-04-23")

    assert previous == {
        "STD_LONG": "run-long",
        "RISK_SHORT_CMP": "run-short-cmp",
    }
    assert "$.compound_equity" in str(captured["sql"])
    assert "$.risk_based_sizing" in str(captured["sql"])
    assert "$.min_price" in str(captured["sql"])
    assert captured["params"] == {"end": "2026-04-23"}
    assert closed["value"] is True


def test_build_baseline_table_formats_comparison(monkeypatch):
    rows = [
        {
            "run_id": "new-long",
            "label": "CPR_LEVELS daily-reset-standard | 2025-01-01 to 2026-04-23",
            "trade_count": 3216,
            "win_rate": 34.0,
            "total_pnl": 1045563.07,
            "calmar": 202.16,
            "annual_return_pct": 0.0,
            "max_dd_pct": 0.0,
        },
        {
            "run_id": "prev-long",
            "label": "CPR_LEVELS daily-reset-standard | 2025-01-01 to 2026-04-22",
            "trade_count": 3221,
            "win_rate": 33.9,
            "total_pnl": 1033435.83,
            "calmar": 201.77,
            "annual_return_pct": 0.0,
            "max_dd_pct": 0.0,
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

    monkeypatch.setattr(db.backtest_db, "get_backtest_db", lambda: _FakeDB())

    results = [
        baselines_cli.BaselineResult(
            label="STD_LONG",
            run_id="new-long",
            exit_code=0,
            elapsed_sec=10.0,
            params_dict={},
        )
    ]
    previous = {"STD_LONG": "prev-long"}

    table = baselines_cli._build_baseline_table(results, previous)

    assert "Baseline Comparison" in table
    assert "Variant" in table
    assert "STD_LONG" in table
    assert "new-long" in table
    assert "prev-long" in table
    assert "Rs 1,045,563" in table
    assert "Rs +12,127" in table
