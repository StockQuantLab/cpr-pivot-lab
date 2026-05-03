from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import polars as pl
import pytest

import scripts.paper_archive as paper_archive
from db.backtest_db import BacktestDB
from db.duckdb import MarketDB


def test_archive_completed_session_stamps_paper_metadata_and_coexists_with_backtests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db = BacktestDB(db_path=tmp_path / "paper-archive.duckdb")
    try:
        db.store_backtest_results(
            pl.DataFrame(
                [
                    {
                        "run_id": "bt-1",
                        "execution_mode": "BACKTEST",
                        "symbol": "SBIN",
                        "trade_date": "2024-03-10",
                        "direction": "LONG",
                        "entry_time": "09:20",
                        "exit_time": "09:45",
                        "entry_timestamp": datetime(2024, 3, 10, 9, 20),
                        "exit_timestamp": datetime(2024, 3, 10, 9, 45),
                        "entry_price": 100.0,
                        "exit_price": 104.0,
                        "sl_price": 98.0,
                        "target_price": 108.0,
                        "profit_loss": 400.0,
                        "profit_loss_pct": 4.0,
                        "exit_reason": "TARGET",
                        "sl_phase": "PROTECT",
                        "atr": 2.0,
                        "cpr_width_pct": 0.3,
                        "position_size": 10,
                        "position_value": 1000.0,
                        "mfe_r": 1.4,
                        "mae_r": -0.2,
                        "or_atr_ratio": 0.8,
                        "gap_pct": 0.1,
                    }
                ]
            )
        )

        session = SimpleNamespace(
            session_id="paper-1",
            strategy="CPR_LEVELS",
            status="COMPLETED",
            symbols=["SBIN"],
            strategy_params={"direction_filter": "BOTH"},
            max_daily_loss_pct=0.03,
            max_positions=10,
            max_position_pct=0.10,
        )
        position = SimpleNamespace(
            position_id="pos-1",
            session_id="paper-1",
            symbol="SBIN",
            direction="LONG",
            status="CLOSED",
            qty=10,
            entry_price=100.0,
            stop_loss=98.0,
            target_price=108.0,
            trail_state={
                "exit_reason": "TRAILING_SL",
                "sl_phase": "PROTECT",
                "atr": 2.0,
                "cpr_width_pct": 0.3,
                "mfe_r": 1.4,
                "mae_r": -0.2,
                "or_atr_ratio": 0.8,
                "gap_pct": 0.1,
            },
            entry_time=datetime(2024, 3, 10, 9, 20),
            exit_time=datetime(2024, 3, 10, 9, 45),
            exit_price=104.0,
            pnl=40.0,
        )
        session_updates: list[dict[str, object]] = []

        fake_paper_db = SimpleNamespace(
            get_session=lambda sid: session if sid == "paper-1" else None,
            get_session_positions=lambda sid, statuses=None: [position] if sid == "paper-1" else [],
            update_session=lambda sid, **kwargs: session_updates.append(
                {"session_id": sid, **kwargs}
            ),
        )

        monkeypatch.setattr(paper_archive, "get_backtest_db", lambda: db)
        monkeypatch.setattr(paper_archive, "get_paper_db", lambda: fake_paper_db)

        payload = paper_archive.archive_completed_session("paper-1")

        assert payload["archived"] is True
        assert payload["execution_mode"] == "PAPER"
        assert payload["rows"] == 1
        assert payload["total_pnl"] == 40.0
        assert session_updates == [{"session_id": "paper-1", "total_pnl": 40.0}]

        meta = db.con.execute(
            """
            SELECT execution_mode, session_id, source_session_id
            FROM run_metadata
            WHERE run_id = 'paper-1'
            """
        ).fetchone()
        assert meta == ("PAPER", "paper-1", "paper-1")

        paper_rows = db.get_backtest_trades("paper-1", execution_mode="PAPER")
        assert paper_rows.height == 1
        paper_row = paper_rows.to_dicts()[0]
        assert paper_row["execution_mode"] == "PAPER"
        assert paper_row["source_session_id"] == "paper-1"
        assert paper_row["entry_timestamp"] == datetime(2024, 3, 10, 9, 20)
        assert paper_row["exit_timestamp"] == datetime(2024, 3, 10, 9, 45)

        backtest_rows = db.get_backtest_trades("bt-1", execution_mode="BACKTEST")
        assert backtest_rows.height == 1
        assert db.get_runs_with_metrics(execution_mode="PAPER")[0]["execution_mode"] == "PAPER"
    finally:
        db.close()


def test_archive_completed_session_normalizes_manual_close_exit_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db = BacktestDB(db_path=tmp_path / "manual-close-archive.duckdb")
    try:
        session = SimpleNamespace(
            session_id="paper-manual",
            strategy="CPR_LEVELS",
            status="COMPLETED",
            symbols=["ZAGGLE"],
            strategy_params={"direction_filter": "SHORT"},
            max_daily_loss_pct=0.03,
            max_positions=10,
            max_position_pct=0.10,
            mode="LIVE",
            trade_date="2026-04-28",
            portfolio_value=1_000_000,
        )
        position = SimpleNamespace(
            position_id="pos-manual",
            session_id="paper-manual",
            symbol="ZAGGLE",
            direction="SHORT",
            status="CLOSED",
            qty=100,
            entry_price=100.0,
            stop_loss=102.0,
            target_price=96.0,
            trail_state={"close_reason": "MANUAL_CLOSE"},
            entry_time=datetime(2026, 4, 28, 10, 30),
            exit_time=datetime(2026, 4, 28, 10, 55),
            exit_price=99.0,
            exit_reason="MANUAL_CLOSE",
            pnl=100.0,
        )
        fake_paper_db = SimpleNamespace(
            get_session=lambda sid: session if sid == "paper-manual" else None,
            get_session_positions=lambda sid, statuses=None: (
                [position] if sid == "paper-manual" else []
            ),
        )

        monkeypatch.setattr(paper_archive, "get_backtest_db", lambda: db)
        monkeypatch.setattr(paper_archive, "get_paper_db", lambda: fake_paper_db)

        payload = paper_archive.archive_completed_session("paper-manual")

        assert payload["archived"] is True
        row = db.con.execute(
            "SELECT exit_reason FROM backtest_results WHERE run_id = 'paper-manual'"
        ).fetchone()
        assert row == ("TIME",)
    finally:
        db.close()


def test_get_runs_with_metrics_handles_legacy_run_metadata_without_execution_mode(
    tmp_path,
) -> None:
    db_path = tmp_path / "legacy-run-metrics.duckdb"

    writable = MarketDB(db_path=db_path)
    try:
        writable.con.execute(
            """
            CREATE TABLE run_metrics (
                run_id VARCHAR PRIMARY KEY,
                strategy VARCHAR,
                strategy_code VARCHAR,
                label VARCHAR,
                start_date DATE,
                end_date DATE,
                trade_count BIGINT,
                symbol_count BIGINT,
                allocated_capital DOUBLE,
                total_pnl DOUBLE,
                total_return_pct DOUBLE,
                win_rate DOUBLE,
                profit_factor DOUBLE,
                max_dd_abs DOUBLE,
                max_dd_pct DOUBLE,
                annual_return_pct DOUBLE,
                calmar DOUBLE,
                updated_at TIMESTAMP DEFAULT now()
            )
            """
        )
        writable.con.execute(
            """
            CREATE TABLE run_metadata (
                run_id VARCHAR PRIMARY KEY,
                strategy VARCHAR,
                label VARCHAR,
                symbols_json VARCHAR,
                start_date DATE,
                end_date DATE,
                params_json VARCHAR,
                session_id VARCHAR,
                source_session_id VARCHAR,
                created_at TIMESTAMP DEFAULT now()
            )
            """
        )
        writable.con.execute(
            """
            CREATE TABLE backtest_results (
                run_id VARCHAR,
                execution_mode VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                profit_loss DOUBLE
            )
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metrics (
                run_id, strategy, strategy_code, label, start_date, end_date,
                trade_count, symbol_count, allocated_capital, total_pnl,
                total_return_pct, win_rate, profit_factor, max_dd_abs,
                max_dd_pct, annual_return_pct, calmar
            ) VALUES ('legacy-1', 'CPR_LEVELS', 'CPR_LEVELS', 'Legacy Run',
                      '2024-03-01', '2024-03-01', 1, 1, 100000.0, 100.0,
                      0.1, 100.0, 99.9, 1.0, 1.0, 1.0, 1.0)
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metadata (
                run_id, strategy, label, symbols_json, start_date, end_date,
                params_json, session_id, source_session_id
            ) VALUES (
                'legacy-1', 'CPR_LEVELS', 'Legacy Run', '["SBIN"]',
                '2024-03-01', '2024-03-01',
                '{"direction_filter":"BOTH"}', 'paper-legacy', 'paper-legacy'
            )
            """
        )
        writable.con.execute(
            """
            INSERT INTO backtest_results (
                run_id, execution_mode, symbol, trade_date, profit_loss
            ) VALUES ('legacy-1', 'PAPER', 'SBIN', '2024-03-01', 100.0)
            """
        )
    finally:
        writable.close()

    readonly = MarketDB(db_path=db_path, read_only=True)
    try:
        rows = readonly.get_runs_with_metrics(execution_mode="PAPER")
        legacy_trades = readonly.get_backtest_trades("legacy-1", execution_mode=None)
        legacy_summary = readonly.get_backtest_summary(execution_mode=None)
    finally:
        readonly.close()

    assert len(rows) == 1
    assert rows[0]["run_id"] == "legacy-1"
    assert rows[0]["execution_mode"] == "PAPER"
    assert rows[0]["direction_filter"] == "BOTH"
    assert legacy_trades.height == 1
    assert legacy_summary.height == 1


def test_get_runs_with_metrics_filters_stale_runs_and_uses_live_trade_counts(
    tmp_path,
) -> None:
    db_path = tmp_path / "stale-run-metrics.duckdb"

    writable = MarketDB(db_path=db_path)
    try:
        writable.con.execute(
            """
            CREATE TABLE run_metrics (
                run_id VARCHAR PRIMARY KEY,
                strategy VARCHAR,
                strategy_code VARCHAR,
                label VARCHAR,
                start_date DATE,
                end_date DATE,
                trade_count BIGINT,
                symbol_count BIGINT,
                allocated_capital DOUBLE,
                total_pnl DOUBLE,
                total_return_pct DOUBLE,
                win_rate DOUBLE,
                profit_factor DOUBLE,
                max_dd_abs DOUBLE,
                max_dd_pct DOUBLE,
                annual_return_pct DOUBLE,
                calmar DOUBLE,
                updated_at TIMESTAMP DEFAULT now()
            )
            """
        )
        writable.con.execute(
            """
            CREATE TABLE run_metadata (
                run_id VARCHAR PRIMARY KEY,
                strategy VARCHAR,
                label VARCHAR,
                symbols_json VARCHAR,
                start_date DATE,
                end_date DATE,
                params_json VARCHAR,
                session_id VARCHAR,
                source_session_id VARCHAR,
                created_at TIMESTAMP DEFAULT now()
            )
            """
        )
        writable.con.execute(
            """
            CREATE TABLE backtest_results (
                run_id VARCHAR,
                execution_mode VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                profit_loss DOUBLE
            )
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metrics (
                run_id, strategy, strategy_code, label, start_date, end_date,
                trade_count, symbol_count, allocated_capital, total_pnl,
                total_return_pct, win_rate, profit_factor, max_dd_abs,
                max_dd_pct, annual_return_pct, calmar
            ) VALUES ('stale-1', 'CPR_LEVELS', 'CPR_LEVELS', 'Stale Run',
                      '2024-03-01', '2024-03-01', 999, 5, 100000.0, 9999.0,
                      9.9, 99.9, 99.9, 1.0, 1.0, 1.0, 1.0)
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metadata (
                run_id, strategy, label, symbols_json, start_date, end_date,
                params_json, session_id, source_session_id
            ) VALUES (
                'stale-1', 'CPR_LEVELS', 'Stale Run', '["SBIN"]',
                '2024-03-01', '2024-03-01',
                '{"direction_filter":"BOTH"}', 'paper-stale', 'paper-stale'
            )
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metrics (
                run_id, strategy, strategy_code, label, start_date, end_date,
                trade_count, symbol_count, allocated_capital, total_pnl,
                total_return_pct, win_rate, profit_factor, max_dd_abs,
                max_dd_pct, annual_return_pct, calmar
            ) VALUES ('live-1', 'FBR', 'FBR', 'Live Run',
                      '2024-03-02', '2024-03-02', 50, 2, 200000.0, 2000.0,
                      1.0, 50.0, 2.0, 10.0, 5.0, 4.0, 2.0)
            """
        )
        writable.con.execute(
            """
            INSERT INTO run_metadata (
                run_id, strategy, label, symbols_json, start_date, end_date,
                params_json, session_id, source_session_id
            ) VALUES (
                'live-1', 'FBR', 'Live Run', '["RELIANCE"]',
                '2024-03-02', '2024-03-02',
                '{"direction_filter":"SHORT"}', 'paper-live', 'paper-live'
            )
            """
        )
        writable.con.execute(
            """
            INSERT INTO backtest_results (
                run_id, execution_mode, symbol, trade_date, profit_loss
            ) VALUES ('live-1', 'BACKTEST', 'RELIANCE', '2024-03-02', 125.5)
            """
        )
    finally:
        writable.close()

    readonly = MarketDB(db_path=db_path, read_only=True)
    try:
        rows = readonly.get_runs_with_metrics()
    finally:
        readonly.close()

    assert [row["run_id"] for row in rows] == ["live-1"]
    assert rows[0]["trade_count"] == 1
    assert rows[0]["total_pnl"] == 125.5
