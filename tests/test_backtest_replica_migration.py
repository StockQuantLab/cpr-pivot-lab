from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl

import scripts.migrate_split as migrate_split
import web.state as web_state
from db.backtest_db import BacktestDB
from db.duckdb import MarketDB
from db.replica import ReplicaSync
from db.replica_consumer import ReplicaConsumer
from engine.cpr_atr_strategy import BacktestParams, BacktestResult


def _trade_frame(run_id: str, trade_date: str = "2024-03-10") -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "run_id": run_id,
                "session_id": f"session-{run_id}",
                "source_session_id": f"source-{run_id}",
                "execution_mode": "BACKTEST",
                "symbol": "SBIN",
                "trade_date": trade_date,
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


def _trade_frame_unordered(run_id: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "run_id": run_id,
                "session_id": f"session-{run_id}",
                "source_session_id": f"source-{run_id}",
                "execution_mode": "BACKTEST",
                "symbol": "ZZZZ",
                "trade_date": "2026-04-28",
                "direction": "SHORT",
                "entry_time": "10:00",
                "exit_time": "10:30",
                "entry_timestamp": datetime(2026, 4, 28, 10, 0),
                "exit_timestamp": datetime(2026, 4, 28, 10, 30),
                "entry_price": 100.0,
                "exit_price": 101.0,
                "sl_price": 102.0,
                "target_price": 99.0,
                "profit_loss": 100.0,
                "profit_loss_pct": 1.0,
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
            },
            {
                "run_id": run_id,
                "session_id": f"session-{run_id}",
                "source_session_id": f"source-{run_id}",
                "execution_mode": "BACKTEST",
                "symbol": "AAAA",
                "trade_date": "2026-04-28",
                "direction": "SHORT",
                "entry_time": "09:30",
                "exit_time": "09:50",
                "entry_timestamp": datetime(2026, 4, 28, 9, 30),
                "exit_timestamp": datetime(2026, 4, 28, 9, 50),
                "entry_price": 100.0,
                "exit_price": 98.5,
                "sl_price": 102.0,
                "target_price": 99.0,
                "profit_loss": -50.0,
                "profit_loss_pct": -0.5,
                "exit_reason": "BREAKEVEN_SL",
                "sl_phase": "PROTECT",
                "atr": 2.0,
                "cpr_width_pct": 0.3,
                "position_size": 10,
                "position_value": 1000.0,
                "mfe_r": 1.4,
                "mae_r": -0.2,
                "or_atr_ratio": 0.8,
                "gap_pct": 0.1,
            },
        ]
    )


def test_backtest_trades_are_returned_in_chronological_order_in_backtest_db(tmp_path) -> None:
    db = BacktestDB(db_path=tmp_path / "backtest-order.duckdb")
    try:
        db.ensure_backtest_table()
        db.store_backtest_results(_trade_frame_unordered("order-run-backtest"))
        rows = db.get_backtest_trades("order-run-backtest")
    finally:
        db.close()

    assert rows.height == 2
    assert rows["symbol"].to_list() == ["AAAA", "ZZZZ"]
    assert rows["profit_loss"].to_list() == [-50.0, 100.0]


def test_backtest_trades_are_returned_in_chronological_order_in_market_db(tmp_path) -> None:
    db = MarketDB(db_path=tmp_path / "market-order.duckdb")
    try:
        db.ensure_backtest_table()
        db.store_backtest_results(_trade_frame_unordered("order-run-market"))
        rows = db.get_backtest_trades("order-run-market")
    finally:
        db.close()

    assert rows.height == 2
    assert rows["symbol"].to_list() == ["AAAA", "ZZZZ"]
    assert rows["profit_loss"].to_list() == [-50.0, 100.0]


def _seed_run(db, run_id: str) -> None:
    db.store_run_metadata(
        run_id=run_id,
        strategy="CPR_LEVELS",
        label=run_id,
        symbols=["SBIN"],
        start_date="2024-03-10",
        end_date="2024-03-10",
        params={"portfolio_value": 100000.0},
        execution_mode="BACKTEST",
    )
    db.store_backtest_results(_trade_frame(run_id))


def test_backfill_preserves_source_and_copies_missing_backtest_rows(
    monkeypatch,
    tmp_path,
) -> None:
    source_db_path = tmp_path / "market.duckdb"
    target_db_path = tmp_path / "backtest.duckdb"
    replica_dir = tmp_path / "backtest_replica"

    source = MarketDB(db_path=source_db_path)
    try:
        source.ensure_backtest_table()
        source.ensure_run_metadata_table()
        source.ensure_run_daily_pnl_table()
        source.ensure_run_metrics_table()
        source.ensure_setup_funnel_table()
        _seed_run(source, "baseline-1")
    finally:
        source.close()

    target = BacktestDB(db_path=target_db_path)
    try:
        target.ensure_backtest_table()
        target.ensure_run_metadata_table()
        target.ensure_run_daily_pnl_table()
        target.ensure_run_metrics_table()
        target.ensure_setup_funnel_table()
        _seed_run(target, "r4")
    finally:
        target.close()

    monkeypatch.setattr(migrate_split, "MARKET_DB", source_db_path)
    monkeypatch.setattr(migrate_split, "BACKTEST_DB", target_db_path)
    monkeypatch.setattr(migrate_split, "BACKTEST_REPLICA_DIR", replica_dir)

    migrate_split.run_migration(dry_run=False)

    migrated = BacktestDB(db_path=target_db_path, read_only=True)
    preserved = MarketDB(db_path=source_db_path, read_only=True)
    try:
        migrated_ids = {
            row["run_id"] for row in migrated.get_runs_with_metrics(execution_mode="BACKTEST")
        }
        preserved_ids = {
            row["run_id"] for row in preserved.get_runs_with_metrics(execution_mode="BACKTEST")
        }
    finally:
        migrated.close()
        preserved.close()

    assert migrated_ids == {"baseline-1", "r4"}
    assert preserved_ids == {"baseline-1"}
    assert target_db_path.exists()
    assert source_db_path.exists()


def test_market_delete_runs_forces_replica_publish(tmp_path, monkeypatch) -> None:
    db = MarketDB(db_path=tmp_path / "market-delete.duckdb")
    try:
        db.ensure_backtest_table()
        db.ensure_run_metadata_table()
        db.ensure_run_daily_pnl_table()
        db.ensure_run_metrics_table()
        db.ensure_setup_funnel_table()
        _seed_run(db, "cleanup-1")
        sync = SimpleNamespace(mark_dirty=0, force_sync=0)

        def mark_dirty() -> None:
            sync.mark_dirty += 1

        def force_sync(source_conn=None) -> None:
            sync.force_sync += 1

        db._sync = SimpleNamespace(mark_dirty=mark_dirty, force_sync=force_sync)  # type: ignore[assignment]
        monkeypatch.setattr(
            web_state,
            "_runs_cache",
            [{"run_id": "cleanup-1", "strategy": "CPR_LEVELS"}],
            raising=False,
        )
        monkeypatch.setattr(web_state, "_runs_cache_time", 42.0, raising=False)

        counts = db.delete_runs(["cleanup-1"])
    finally:
        db.close()

    assert counts["backtest_results"] == 1
    assert counts["run_metadata"] == 1
    assert sync.mark_dirty == 1
    assert sync.force_sync == 1
    assert web_state._runs_cache is None
    assert web_state._runs_cache_time == 0


def test_backtest_delete_runs_forces_replica_publish(tmp_path, monkeypatch) -> None:
    db = BacktestDB(db_path=tmp_path / "backtest-delete.duckdb")
    try:
        db.ensure_backtest_table()
        db.ensure_run_metadata_table()
        db.ensure_run_daily_pnl_table()
        db.ensure_run_metrics_table()
        db.ensure_setup_funnel_table()
        _seed_run(db, "cleanup-2")
        sync = SimpleNamespace(mark_dirty=0, force_sync=0)

        def mark_dirty() -> None:
            sync.mark_dirty += 1

        def force_sync(source_conn=None) -> None:
            sync.force_sync += 1

        db._sync = SimpleNamespace(mark_dirty=mark_dirty, force_sync=force_sync)  # type: ignore[assignment]
        monkeypatch.setattr(
            web_state,
            "_runs_cache",
            [{"run_id": "cleanup-2", "strategy": "CPR_LEVELS"}],
            raising=False,
        )
        monkeypatch.setattr(web_state, "_runs_cache_time", 42.0, raising=False)

        counts = db.delete_runs(["cleanup-2"])
    finally:
        db.close()

    assert counts["backtest_results"] == 1
    assert counts["run_metadata"] == 1
    assert sync.mark_dirty == 1
    assert sync.force_sync == 1
    assert web_state._runs_cache is None
    assert web_state._runs_cache_time == 0


def test_replica_force_sync_publishes_snapshot(tmp_path) -> None:
    db_path = tmp_path / "backtest-sync.duckdb"
    replica_dir = tmp_path / "backtest-sync-replica"
    sync = ReplicaSync(db_path, replica_dir, min_interval_sec=0.0)
    db = BacktestDB(db_path=db_path, replica_sync=sync)
    try:
        db.ensure_backtest_table()
        db.con.execute(
            """
            INSERT INTO backtest_results (
                run_id, symbol, trade_date, direction, entry_time, exit_time,
                entry_price, exit_price, sl_price, target_price, profit_loss,
                profit_loss_pct, exit_reason, sl_phase, atr, cpr_width_pct,
                position_size, position_value, execution_mode
            ) VALUES (?, ?, ?::DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "sync-run",
                "SBIN",
                "2024-03-10",
                "LONG",
                "09:20",
                "09:45",
                100.0,
                104.0,
                98.0,
                108.0,
                400.0,
                4.0,
                "TARGET",
                "PROTECT",
                2.0,
                0.3,
                10,
                1000.0,
                "BACKTEST",
            ],
        )

        sync.force_sync(db.con)
    finally:
        db.close()

    consumer = ReplicaConsumer(replica_dir, db_path.stem)
    replica_path = consumer.get_replica_path()
    assert replica_path is not None and replica_path.exists()

    replica_db = BacktestDB(db_path=replica_path, read_only=True)
    try:
        rows = replica_db.get_backtest_trades("sync-run")
    finally:
        replica_db.close()

    assert rows.height == 1


def test_paper_daily_summary_excludes_diagnostic_paper_runs(tmp_path) -> None:
    db = BacktestDB(db_path=tmp_path / "paper-daily-summary.duckdb")
    try:
        db.ensure_backtest_table()
        db.store_backtest_results(
            pl.concat(
                [
                    _trade_frame("CPR_LEVELS_LONG-2026-05-04-live-kite", "2026-05-04").with_columns(
                        pl.lit("PAPER").alias("execution_mode"),
                        pl.lit("LONG").alias("direction"),
                        pl.lit(24753.32).alias("profit_loss"),
                    ),
                    _trade_frame(
                        "compare-kite-audit-long-2026-05-04-v2", "2026-05-04"
                    ).with_columns(
                        pl.lit("PAPER").alias("execution_mode"),
                        pl.lit("LONG").alias("direction"),
                        pl.lit(21218.73).alias("profit_loss"),
                    ),
                    _trade_frame(
                        "CPR_LEVELS_SHORT-2026-05-04-live-kite", "2026-05-04"
                    ).with_columns(
                        pl.lit("PAPER").alias("execution_mode"),
                        pl.lit("SHORT").alias("direction"),
                        pl.lit(-3737.70).alias("profit_loss"),
                    ),
                    _trade_frame(
                        "compare-kite-audit-short-2026-05-04-v2", "2026-05-04"
                    ).with_columns(
                        pl.lit("PAPER").alias("execution_mode"),
                        pl.lit("SHORT").alias("direction"),
                        pl.lit(-746.85).alias("profit_loss"),
                    ),
                ],
                how="vertical",
            )
        )

        rows = db.get_paper_daily_summary()
    finally:
        db.close()

    assert rows == [
        (
            datetime(2026, 5, 4).date(),
            1,
            1,
            24753.32,
            1,
            0,
            -3737.7,
            2,
            1,
            21015.62,
        )
    ]


def test_replica_replace_retries_transient_permission_error(tmp_path, monkeypatch) -> None:
    sync = ReplicaSync(tmp_path / "retry.duckdb", tmp_path / "retry-replica")
    source = tmp_path / "pointer.tmp"
    target = tmp_path / "pointer"
    source.write_text("v1")
    attempts = {"count": 0}
    original_replace = Path.replace

    def flaky_replace(self: Path, target_path: Path) -> Path:
        if self == source and attempts["count"] < 2:
            attempts["count"] += 1
            raise PermissionError("locked")
        return original_replace(self, target_path)

    monkeypatch.setattr("db.replica.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(Path, "replace", flaky_replace)

    sync._replace_with_retry(source, target)

    assert attempts["count"] == 2
    assert target.read_text() == "v1"


def test_market_publish_replica_uses_live_connection(tmp_path) -> None:
    db_path = tmp_path / "market-live.duckdb"
    replica_dir = tmp_path / "market-live-replica"
    sync = ReplicaSync(db_path, replica_dir, min_interval_sec=0.0)
    db = MarketDB(db_path=db_path, replica_sync=sync)
    try:
        db.con.execute("CREATE TABLE IF NOT EXISTS publish_probe(id INTEGER)")
        db.con.execute("DELETE FROM publish_probe")
        db.con.execute("INSERT INTO publish_probe VALUES (1), (2)")

        db._publish_replica(force=True)
    finally:
        db.close()

    consumer = ReplicaConsumer(replica_dir, db_path.stem)
    replica_path = consumer.get_replica_path()
    assert replica_path is not None and replica_path.exists()

    replica_db = duckdb.connect(str(replica_path), read_only=True)
    try:
        rows = replica_db.execute("SELECT COUNT(*) FROM publish_probe").fetchone()
    finally:
        replica_db.close()

    assert int(rows[0] or 0) == 2


def test_save_to_db_batches_replica_sync_until_commit(tmp_path, monkeypatch) -> None:
    db = BacktestDB(db_path=tmp_path / "save-sync.duckdb")
    sync = SimpleNamespace(mark_dirty=0, maybe_sync=0, force_sync=0)

    def mark_dirty() -> None:
        sync.mark_dirty += 1

    def maybe_sync(source_conn=None) -> None:
        sync.maybe_sync += 1

    def force_sync(source_conn=None) -> None:
        sync.force_sync += 1

    db._sync = SimpleNamespace(mark_dirty=mark_dirty, maybe_sync=maybe_sync, force_sync=force_sync)  # type: ignore[assignment]

    result = BacktestResult(
        run_id="save-sync-run",
        params=BacktestParams(),
        _loaded_df=_trade_frame("save-sync-run"),
        run_context={
            "param_signature": "sig-1",
            "start_date": "2024-03-10",
            "end_date": "2024-03-10",
            "symbols": ["SBIN"],
        },
    )
    monkeypatch.setattr(BacktestResult, "validate", lambda self: None)

    try:
        count = result.save_to_db(db=db)
    finally:
        db.close()

    assert count == 1
    assert sync.mark_dirty == 0
    assert sync.maybe_sync == 0
    assert sync.force_sync == 1
