"""Regression tests for agent tool defaults and symbol metadata fetch."""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import polars as pl

from agent.tools import backtest_tools
from engine.paper_runtime import summarize_paper_positions


class _DummyDB:
    def __init__(self) -> None:
        self.get_date_range_calls = 0

    @staticmethod
    def get_available_symbols() -> list[str]:
        return ["RELIANCE", "SBIN"]

    @staticmethod
    def get_all_date_ranges() -> dict[str, dict[str, str]]:
        return {
            "RELIANCE": {"start": "2015-01-01", "end": "2025-03-31"},
            "SBIN": {"start": "2016-01-01", "end": "2025-03-31"},
        }

    def get_date_range(self, symbol: str):  # pragma: no cover - should never be called
        self.get_date_range_calls += 1
        raise AssertionError(f"N+1 get_date_range call should not happen for {symbol}")


def test_agent_tool_defaults_match_backtest_params() -> None:
    run_backtest_sig = inspect.signature(backtest_tools.run_backtest)
    run_multi_sig = inspect.signature(backtest_tools.run_multi_stock_backtest)
    rebuild_sig = inspect.signature(backtest_tools.rebuild_indicators)

    assert run_backtest_sig.parameters["strategy"].default == "CPR_LEVELS"
    assert run_backtest_sig.parameters["cpr_percentile"].default == 33.0
    assert run_backtest_sig.parameters["cpr_max_width_pct"].default == 2.0
    assert run_backtest_sig.parameters["rvol_threshold"].default == 1.0
    assert run_backtest_sig.parameters["min_sl_atr_ratio"].default == 0.5
    assert run_backtest_sig.parameters["risk_pct"].default == 0.01
    assert run_backtest_sig.parameters["portfolio_value"].default == 1_000_000
    assert run_backtest_sig.parameters["max_positions"].default == 10
    assert run_backtest_sig.parameters["entry_window_end"].default == "10:15"
    assert run_backtest_sig.parameters["long_max_gap_pct"].default is None
    assert run_multi_sig.parameters["cpr_percentile"].default == 33.0
    assert run_multi_sig.parameters["cpr_max_width_pct"].default == 2.0
    assert run_multi_sig.parameters["min_sl_atr_ratio"].default == 0.5
    assert run_multi_sig.parameters["risk_pct"].default == 0.01
    assert run_multi_sig.parameters["portfolio_value"].default == 1_000_000
    assert run_multi_sig.parameters["entry_window_end"].default == "10:15"
    assert run_multi_sig.parameters["long_max_gap_pct"].default is None
    assert rebuild_sig.parameters["cpr_percentile"].default == 33.0


def test_get_available_symbols_uses_bulk_date_ranges(monkeypatch) -> None:
    db = _DummyDB()
    monkeypatch.setattr(backtest_tools, "get_db", lambda: db)

    result = backtest_tools.get_available_symbols()

    assert result["count"] == 2
    assert result["symbols"] == ["RELIANCE", "SBIN"]
    assert result["date_ranges"]["RELIANCE"]["start"] == "2015-01-01"
    assert result["date_ranges"]["SBIN"]["end"] == "2025-03-31"
    assert db.get_date_range_calls == 0


def test_build_backtest_params_applies_portfolio_and_quality_controls() -> None:
    params = backtest_tools._build_backtest_params(
        strategy="CPR_LEVELS",
        cpr_percentile=33.0,
        cpr_max_width_pct=2.0,
        rr_ratio=2.0,
        capital=100_000,
        risk_pct=0.01,
        portfolio_value=1_000_000,
        max_positions=10,
        max_position_pct=0.10,
        time_exit="15:15",
        entry_window_end="09:35",
        direction_filter="BOTH",
        min_price=50.0,
        long_max_gap_pct=1.0,
        cpr_min_close_atr=0.35,
        min_sl_atr_ratio=0.5,
        narrowing_filter=True,
        failure_window=10,
        atr_periods=12,
        buffer_pct=0.0005,
        rvol_threshold=1.0,
        risk_based_sizing=False,
    )

    assert params.strategy == "CPR_LEVELS"
    assert params.portfolio_value == 1_000_000
    assert params.cpr_max_width_pct == 2.0
    assert params.risk_pct == 0.01
    assert params.max_positions == 10
    assert params.max_position_pct == 0.10
    assert params.entry_window_end == "09:35"
    assert params.min_sl_atr_ratio == 0.5
    assert params.min_price == 50.0
    assert params.long_max_gap_pct == 1.0
    assert params.cpr_levels.cpr_min_close_atr == 0.35
    assert params.cpr_levels.use_narrowing_filter is True
    assert params.fbr.failure_window == 10


def test_paper_position_summary_calculates_live_pnl() -> None:
    session = SimpleNamespace(
        session_id="paper-1",
        name="Live Session",
        strategy="CPR_LEVELS",
        status="ACTIVE",
    )
    positions = [
        SimpleNamespace(
            status="OPEN",
            direction="LONG",
            current_qty=2,
            quantity=2,
            entry_price=100.0,
            last_price=105.0,
            realized_pnl=None,
        ),
        SimpleNamespace(
            status="CLOSED",
            direction="SHORT",
            current_qty=1,
            quantity=1,
            entry_price=110.0,
            last_price=106.0,
            realized_pnl=4.0,
        ),
    ]
    feed_state = SimpleNamespace(status="OK", stale_reason=None, last_price=105.0)

    summary = summarize_paper_positions(session, positions, feed_state)

    assert summary["open_positions"] == 1
    assert summary["closed_positions"] == 1
    assert summary["realized_pnl"] == 4.0
    assert summary["unrealized_pnl"] == 10.0
    assert summary["net_pnl"] == 14.0


def test_paper_position_summary_uses_symbol_specific_marks_from_json_state() -> None:
    session = SimpleNamespace(
        session_id="paper-2",
        name="Live Session",
        strategy="CPR_LEVELS",
        status="ACTIVE",
    )
    positions = [
        SimpleNamespace(
            status="OPEN",
            symbol="SBIN",
            direction="LONG",
            current_qty=1,
            quantity=1,
            entry_price=100.0,
            last_price=None,
            realized_pnl=None,
        ),
        SimpleNamespace(
            status="OPEN",
            symbol="RELIANCE",
            direction="SHORT",
            current_qty=1,
            quantity=1,
            entry_price=200.0,
            last_price=None,
            realized_pnl=None,
        ),
    ]
    feed_state = SimpleNamespace(
        status="OK",
        stale_reason=None,
        last_price=999.0,
        raw_state='{"symbol_last_prices": {"SBIN": 105.0, "RELIANCE": 195.0}}',
    )

    summary = summarize_paper_positions(session, positions, feed_state)

    assert summary["unrealized_pnl"] == 10.0


def test_get_paper_ledger_reads_archive_mode(monkeypatch) -> None:
    class DummyDB:
        def get_runs_with_metrics(self, execution_mode: str = "BACKTEST") -> list[dict]:
            assert execution_mode == "PAPER"
            return [
                {
                    "run_id": "paper-1",
                    "strategy": "CPR_LEVELS",
                    "label": "Paper Session",
                    "execution_mode": "PAPER",
                }
            ]

        def get_backtest_trades(
            self,
            run_id: str,
            symbols: list[str] | None = None,
            execution_mode: str | None = "BACKTEST",
        ) -> pl.DataFrame:
            assert run_id == "paper-1"
            assert execution_mode == "PAPER"
            return pl.DataFrame(
                [
                    {
                        "run_id": "paper-1",
                        "symbol": "SBIN",
                        "trade_date": "2024-01-01",
                        "direction": "LONG",
                        "entry_time": "09:20",
                        "exit_time": "09:35",
                        "entry_price": 100.0,
                        "exit_price": 104.0,
                        "sl_price": 95.0,
                        "target_price": 110.0,
                        "profit_loss": 400.0,
                        "profit_loss_pct": 4.0,
                        "exit_reason": "TIME",
                        "sl_phase": "PROTECT",
                        "atr": None,
                        "cpr_width_pct": None,
                        "position_size": 1,
                        "position_value": 100.0,
                        "mfe_r": None,
                        "mae_r": None,
                        "or_atr_ratio": None,
                        "gap_pct": None,
                        "execution_mode": "PAPER",
                        "session_id": "paper-1",
                    }
                ]
            )

    monkeypatch.setattr(backtest_tools, "get_db", lambda: DummyDB())

    result = backtest_tools.get_paper_ledger("paper-1")

    assert result["session_id"] == "paper-1"
    assert result["trade_count"] == 1
    assert result["run_meta"]["execution_mode"] == "PAPER"
    assert result["trades"][0]["symbol"] == "SBIN"


def test_list_paper_sessions_uses_live_and_archive_sources(monkeypatch) -> None:
    async def fake_load_paper_sessions() -> dict:
        return {
            "active_sessions": [
                {
                    "session": {"session_id": "paper-1"},
                    "summary": {"session_id": "paper-1"},
                    "positions": [],
                    "orders": [],
                    "feed_state": None,
                }
            ],
            "archived_sessions": [{"run_id": "paper-1", "execution_mode": "PAPER"}],
        }

    monkeypatch.setattr(backtest_tools, "_load_paper_sessions", fake_load_paper_sessions)

    result = backtest_tools.list_paper_sessions()

    assert len(result["active_sessions"]) == 1
    assert result["active_sessions"][0]["session"]["session_id"] == "paper-1"
    assert result["archived_sessions"][0]["execution_mode"] == "PAPER"
