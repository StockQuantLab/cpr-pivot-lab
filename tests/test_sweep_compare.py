"""Tests for sweep comparison logic."""

from __future__ import annotations

from unittest.mock import MagicMock

from engine.sweep_compare import SweepSummary, format_comparison_table, rank_sweeps


def _sample_summary(**overrides) -> SweepSummary:
    defaults = {
        "run_id": "run123",
        "label": "test",
        "trade_count": 100,
        "win_rate": 55.0,
        "total_pnl": 15000.0,
        "profit_factor": 1.4,
        "max_dd_pct": 8.5,
        "annual_return_pct": 18.0,
        "calmar": 2.1,
    }
    defaults.update(overrides)
    return SweepSummary(**defaults)


def test_rank_sweeps_by_calmar():
    summaries = [
        _sample_summary(run_id="a", label="A", calmar=0.0),
        _sample_summary(run_id="b", label="B", calmar=2.0),
        _sample_summary(run_id="c", label="C", calmar=0.1),
    ]
    ranked = rank_sweeps(summaries, metric="calmar", sort="desc", top_n=2)
    assert ranked[0].run_id == "b"
    assert ranked[1].run_id == "c"
    assert len(ranked) == 2


def test_rank_sweeps_by_win_rate():
    summaries = [
        _sample_summary(run_id="a", label="A", win_rate=50.0, calmar=1.0),
        _sample_summary(run_id="b", label="B", win_rate=70.0, calmar=1.5),
    ]
    ranked = rank_sweeps(summaries, metric="win_rate", sort="desc", top_n=5)
    assert ranked[0].run_id == "b"
    assert len(ranked) == 2


def test_rank_sweeps_empty():
    ranked = rank_sweeps([], metric="calmar", sort="desc", top_n=5)
    assert ranked == []


def test_format_comparison_table():
    summaries = [
        _sample_summary(
            run_id="a",
            label="4%-thresh",
            trade_count=100,
            calmar=2.0,
            win_rate=55.0,
            total_pnl=18000.0,
            profit_factor=1.4,
            max_dd_pct=8.5,
            annual_return_pct=18.0,
        ),
        _sample_summary(
            run_id="b",
            label="2%-thresh",
            trade_count=200,
            calmar=1.22,
            win_rate=52.0,
            total_pnl=22000.0,
            profit_factor=1.3,
            max_dd_pct=10.0,
            annual_return_pct=15.0,
        ),
    ]
    table = format_comparison_table(summaries)
    assert "4%-thresh" in table
    assert "2%-thresh" in table
    assert "Calmar" in table


def test_format_comparison_table_empty():
    table = format_comparison_table([])
    assert "No results" in table


def test_rank_sweeps_by_total_pnl():
    summaries = [
        _sample_summary(run_id="a", label="A", total_pnl=0.0, calmar=0.0),
        _sample_summary(run_id="b", label="B", total_pnl=50000.0, calmar=1.0),
    ]
    ranked = rank_sweeps(summaries, metric="total_pnl", sort="desc", top_n=5)
    assert ranked[0].run_id == "b"


def test_rank_sweeps_asc():
    summaries = [
        _sample_summary(run_id="a", label="A", calmar=3.0),
        _sample_summary(run_id="b", label="B", calmar=1.0),
    ]
    ranked = rank_sweeps(summaries, metric="calmar", sort="asc", top_n=5)
    assert ranked[0].run_id == "b"


def test_fetch_summaries_empty():
    mock_db = MagicMock()
    mock_db.con.execute.return_value.pl.return_value.to_dicts.return_value = []
    from engine.sweep_compare import fetch_summaries

    result = fetch_summaries(mock_db, ["nonexistent"])
    assert result == []


def test_fetch_summaries_with_data():
    mock_db = MagicMock()
    mock_db.con.execute.return_value.pl.return_value.to_dicts.return_value = [
        {
            "run_id": "abc123",
            "trade_count": 100,
            "win_rate": 55.0,
            "total_pnl": 15000.0,
            "profit_factor": 1.4,
            "max_dd_pct": 8.5,
            "annual_return_pct": 18.0,
            "calmar": 2.1,
        }
    ]
    from engine.sweep_compare import fetch_summaries

    result = fetch_summaries(mock_db, ["abc123"])
    assert len(result) == 1
    assert result[0].run_id == "abc123"
    assert result[0].calmar == 2.1
