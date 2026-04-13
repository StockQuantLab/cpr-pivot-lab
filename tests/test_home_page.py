"""Unit tests for dashboard home page run selection helpers."""

from web.pages.home import _select_best_calmar_runs


def test_select_best_calmar_runs_prefers_min_drawdown_threshold() -> None:
    runs = [
        {
            "run_id": "a",
            "trade_count": 100,
            "run_span_days": 400,
            "max_dd_pct": 0.01,
            "calmar": 99.0,
        },
        {
            "run_id": "b",
            "trade_count": 100,
            "run_span_days": 400,
            "max_dd_pct": 0.20,
            "calmar": 4.0,
        },
    ]

    selected = _select_best_calmar_runs(runs)
    assert [r["run_id"] for r in selected] == ["b"]


def test_select_best_calmar_runs_falls_back_when_all_drawdowns_tiny() -> None:
    runs = [
        {
            "run_id": "a",
            "trade_count": 100,
            "run_span_days": 500,
            "max_dd_pct": 0.02,
            "calmar": 40.0,
        },
        {
            "run_id": "b",
            "trade_count": 100,
            "run_span_days": 500,
            "max_dd_pct": 0.03,
            "calmar": 35.0,
        },
    ]

    selected = _select_best_calmar_runs(runs)
    assert len(selected) == 2
