"""
Backtest results storage in data/backtest.duckdb.

Separated from market.duckdb so the dashboard can read market data
(read-only) while the engine writes backtest results to a different file.
Stores: backtest_results, run_metadata, run_metrics, run_daily_pnl, setup_funnel.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
from pathlib import Path

import duckdb
import polars as pl

from db.replica import ReplicaSync
from db.replica_consumer import ReplicaConsumer

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BACKTEST_DUCKDB_FILE = DATA_DIR / "backtest.duckdb"
REPLICA_DIR = DATA_DIR / "backtest_replica"


class BacktestDB:
    """Backtest result storage in backtest.duckdb.

    Handles all write/read operations for backtest results, run metadata,
    run metrics, daily PnL, and setup funnel data.
    """

    def __init__(
        self,
        db_path: Path = BACKTEST_DUCKDB_FILE,
        replica_sync: ReplicaSync | None = None,
        read_only: bool = False,
    ):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._sync = replica_sync
        self.read_only = read_only
        self.con = duckdb.connect(str(db_path), read_only=read_only)
        if not read_only:
            self._configure_performance()
        # Table readiness flags (avoid re-running DDL)
        self._backtest_table_ready = False
        self._run_metadata_ready = False
        self._run_metrics_ready = False
        self._run_daily_pnl_ready = False
        self._setup_funnel_ready = False
        self._replica_batch_depth = 0

    def _configure_performance(self) -> None:
        """Apply performance tuning for write-heavy backtest operations."""
        default_threads = max(4, min(16, (os.cpu_count() or 4)))
        thread_count = int(os.getenv("DUCKDB_THREADS", str(default_threads)))
        max_memory = os.getenv("DUCKDB_MAX_MEMORY", "24GB")
        for stmt in [
            f"PRAGMA threads={thread_count}",
            f"SET threads={thread_count}",
        ]:
            try:
                self.con.execute(stmt)
                break
            except Exception:
                pass
        for stmt in [
            f"PRAGMA max_memory='{max_memory}'",
            f"SET max_memory='{max_memory}'",
        ]:
            try:
                self.con.execute(stmt)
                break
            except Exception:
                pass
        for stmt in [
            "PRAGMA enable_progress_bar=false",
            "SET enable_progress_bar=false",
        ]:
            try:
                self.con.execute(stmt)
                break
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------

    def _table_exists(self, table: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()
        return row is not None

    def _table_has_column(self, table: str, column: str) -> bool:
        row = self.con.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table, column],
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def ensure_backtest_table(self) -> None:
        if self._backtest_table_ready:
            return
        if self.read_only:
            self._backtest_table_ready = True
            return
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                run_id           VARCHAR,
                session_id       VARCHAR,
                source_session_id VARCHAR,
                execution_mode   VARCHAR DEFAULT 'BACKTEST',
                symbol           VARCHAR,
                trade_date       DATE,
                direction        VARCHAR CHECK (direction IN ('LONG', 'SHORT')),
                entry_time       VARCHAR,
                exit_time        VARCHAR,
                entry_timestamp  TIMESTAMP,
                exit_timestamp   TIMESTAMP,
                entry_price      DOUBLE,
                exit_price       DOUBLE,
                sl_price         DOUBLE,
                target_price     DOUBLE,
                profit_loss      DOUBLE,
                profit_loss_pct  DOUBLE,
                exit_reason      VARCHAR CHECK (exit_reason IN (
                    'TARGET', 'INITIAL_SL', 'BREAKEVEN_SL',
                    'TRAILING_SL', 'TIME', 'REVERSAL', 'CANDLE_EXIT',
                    'TIME_STOP', 'MOMENTUM_FAIL'
                )),
                sl_phase         VARCHAR CHECK (sl_phase IN ('PROTECT', 'BREAKEVEN', 'TRAIL')),
                atr              DOUBLE,
                cpr_width_pct    DOUBLE,
                position_size    INTEGER,
                position_value   DOUBLE,
                mfe_r            FLOAT,
                mae_r            FLOAT,
                or_atr_ratio     FLOAT,
                gap_pct          FLOAT,
                gross_pnl        DOUBLE,
                total_costs      DOUBLE,
                reached_1r       BOOLEAN,
                reached_2r       BOOLEAN,
                max_r            FLOAT
            )
        """)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol ON backtest_results(run_id, symbol)"
        )
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol_date ON backtest_results(run_id, symbol, trade_date)"
        )
        # Add any columns that may have been added in later schema versions
        for col, col_type in (
            ("session_id", "VARCHAR"),
            ("source_session_id", "VARCHAR"),
            ("execution_mode", "VARCHAR"),
            ("position_size", "INTEGER"),
            ("position_value", "DOUBLE"),
            ("entry_timestamp", "TIMESTAMP"),
            ("exit_timestamp", "TIMESTAMP"),
            ("mfe_r", "FLOAT"),
            ("mae_r", "FLOAT"),
            ("or_atr_ratio", "FLOAT"),
            ("gap_pct", "FLOAT"),
            ("gross_pnl", "DOUBLE"),
            ("total_costs", "DOUBLE"),
            ("reached_1r", "BOOLEAN"),
            ("reached_2r", "BOOLEAN"),
            ("max_r", "FLOAT"),
        ):
            try:
                self.con.execute(
                    f"ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS {col} {col_type}"
                )
            except Exception as e:
                logger.debug("Failed to add backtest_results column %s: %s", col, e)
        self._backtest_table_ready = True

    def ensure_run_metadata_table(self) -> None:
        if self._run_metadata_ready:
            return
        if self.read_only:
            self._run_metadata_ready = True
            return
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS run_metadata (
                run_id    VARCHAR PRIMARY KEY,
                strategy  VARCHAR,
                label     VARCHAR,
                symbols_json VARCHAR,
                start_date DATE,
                end_date DATE,
                params_json VARCHAR,
                param_signature VARCHAR,
                execution_mode VARCHAR,
                session_id VARCHAR,
                source_session_id VARCHAR,
                created_at TIMESTAMP DEFAULT now()
            )
        """)
        for col in (
            "symbols_json",
            "start_date",
            "end_date",
            "params_json",
            "param_signature",
            "execution_mode",
            "session_id",
            "source_session_id",
        ):
            try:
                self.con.execute(f"ALTER TABLE run_metadata ADD COLUMN IF NOT EXISTS {col} VARCHAR")
            except Exception as e:
                logger.debug("Failed to add run_metadata column %s: %s", col, e)
        self._run_metadata_ready = True

    def ensure_run_metrics_table(self) -> None:
        if self._run_metrics_ready:
            return
        if self.read_only:
            self._run_metrics_ready = True
            return
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS run_metrics (
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
        """)
        for col, col_type in (
            ("strategy_code", "VARCHAR"),
            ("label", "VARCHAR"),
            ("allocated_capital", "DOUBLE"),
            ("total_return_pct", "DOUBLE"),
        ):
            try:
                self.con.execute(
                    f"ALTER TABLE run_metrics ADD COLUMN IF NOT EXISTS {col} {col_type}"
                )
            except Exception as e:
                logger.debug("Failed to add run_metrics column %s: %s", col, e)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_metrics_trade_count ON run_metrics(trade_count)"
        )
        self._run_metrics_ready = True

    def ensure_run_daily_pnl_table(self) -> None:
        if self._run_daily_pnl_ready:
            return
        if self.read_only:
            self._run_daily_pnl_ready = True
            return
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS run_daily_pnl (
                run_id VARCHAR,
                trade_date DATE,
                day_pnl DOUBLE,
                cum_pnl DOUBLE,
                updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (run_id, trade_date)
            )
        """)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_daily_pnl_run_date ON run_daily_pnl(run_id, trade_date)"
        )
        self._run_daily_pnl_ready = True

    def ensure_setup_funnel_table(self) -> None:
        if self._setup_funnel_ready:
            return
        if self.read_only:
            self._setup_funnel_ready = True
            return
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS setup_funnel (
                run_id           VARCHAR PRIMARY KEY,
                strategy         VARCHAR,
                universe_count   INTEGER,
                after_cpr_width  INTEGER,
                after_direction  INTEGER,
                after_dir_filter INTEGER,
                after_min_price  INTEGER,
                after_gap        INTEGER,
                after_or_atr     INTEGER,
                after_narrowing  INTEGER,
                after_shift      INTEGER,
                entry_triggered  INTEGER
            )
        """)
        self._setup_funnel_ready = True

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def _after_write(self) -> None:
        """Mark replica dirty after writes."""
        if self._sync:
            if self._replica_batch_depth > 0:
                return
            self._sync.mark_dirty()
            self._sync.maybe_sync(self.con)

    def _begin_replica_batch(self) -> None:
        """Suppress replica publication until the current batch completes."""
        self._replica_batch_depth += 1

    def _end_replica_batch(self) -> None:
        """Re-enable replica publication after a batched write finishes."""
        self._replica_batch_depth = max(0, self._replica_batch_depth - 1)

    def store_run_metadata(
        self,
        run_id: str,
        strategy: str,
        label: str = "",
        symbols: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        params: dict | None = None,
        param_signature: str | None = None,
        execution_mode: str = "BACKTEST",
        session_id: str | None = None,
        source_session_id: str | None = None,
    ) -> None:
        """Insert a run_id -> strategy mapping into run_metadata."""
        self.ensure_run_metadata_table()
        symbols_json = json.dumps(sorted(set(symbols))) if symbols else None
        params_json = json.dumps(params, sort_keys=True) if isinstance(params, dict) else None
        try:
            self.con.execute(
                """
                INSERT OR REPLACE INTO run_metadata (
                    run_id, strategy, label, symbols_json, start_date, end_date, params_json,
                    param_signature, execution_mode, session_id, source_session_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    strategy,
                    label or strategy,
                    symbols_json,
                    start_date,
                    end_date,
                    params_json,
                    param_signature,
                    execution_mode,
                    session_id,
                    source_session_id or session_id,
                ],
            )
            self._after_write()
        except Exception as e:
            logger.exception("Failed to insert run_metadata for run_id=%s: %s", run_id, e)
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass

    def store_backtest_results(
        self,
        results_df: pl.DataFrame,
        execution_mode: str | None = None,
        transactional: bool = True,
    ) -> int:
        """Store trade-level results. Calls refresh_run_metrics after insert."""
        if results_df.is_empty():
            return 0
        self.ensure_backtest_table()
        self.ensure_run_daily_pnl_table()
        self.ensure_run_metrics_table()
        columns = [
            "run_id",
            "session_id",
            "source_session_id",
            "execution_mode",
            "symbol",
            "trade_date",
            "direction",
            "entry_time",
            "exit_time",
            "entry_timestamp",
            "exit_timestamp",
            "entry_price",
            "exit_price",
            "sl_price",
            "target_price",
            "profit_loss",
            "profit_loss_pct",
            "exit_reason",
            "sl_phase",
            "atr",
            "cpr_width_pct",
            "position_size",
            "position_value",
            "mfe_r",
            "mae_r",
            "or_atr_ratio",
            "gap_pct",
        ]
        working_df = results_df
        if execution_mode is not None:
            working_df = working_df.with_columns(
                pl.lit(str(execution_mode).upper()).alias("execution_mode")
            )
        elif "execution_mode" not in working_df.columns:
            working_df = working_df.with_columns(pl.lit("BACKTEST").alias("execution_mode"))
        if "session_id" not in working_df.columns:
            working_df = working_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("session_id"))

        available = [c for c in columns if c in working_df.columns]
        self.con.register("_tmp_br", working_df.select(available).to_arrow())
        try:
            insert_sql = (
                f"INSERT INTO backtest_results ({', '.join(available)}) SELECT * FROM _tmp_br"
            )
            if transactional:
                self.con.execute("BEGIN TRANSACTION")
            # For PAPER runs, run_id == session_id — re-archiving must not duplicate rows.
            # Delete existing rows for this run_id before inserting so a second call is
            # idempotent (backtest run_ids are UUIDs so this branch is never hit for them).
            run_id_for_dedup: str | None = None
            if "run_id" in working_df.columns:
                run_id_for_dedup = str(working_df["run_id"][0])
            if "execution_mode" in working_df.columns and run_id_for_dedup:
                mode_val = str(working_df["execution_mode"][0]).upper()
                if mode_val == "PAPER":
                    self.con.execute(
                        "DELETE FROM backtest_results WHERE run_id = ?", [run_id_for_dedup]
                    )
            self.con.execute(insert_sql)

            run_id_val: str | None = None
            if "run_id" in working_df.columns:
                run_id_val = str(working_df["run_id"][0])
            if run_id_val:
                self.refresh_run_metrics([run_id_val])
            if transactional:
                self.con.execute("COMMIT")
            self._after_write()
        except Exception as e:
            if transactional:
                try:
                    self.con.execute("ROLLBACK")
                except Exception as rb_err:
                    logger.debug("Rollback failed: %s", rb_err)
            logger.exception("Failed to store backtest_results: %s", e)
            raise
        finally:
            self.con.unregister("_tmp_br")
        return results_df.height

    def store_setup_funnel(self, funnel: dict) -> None:
        """Upsert a setup funnel row (DELETE + INSERT) for a run_id."""
        self.ensure_setup_funnel_table()
        run_id = funnel.get("run_id", "")
        if not run_id:
            return
        self.con.execute("DELETE FROM setup_funnel WHERE run_id = ?", [run_id])
        self.con.execute(
            """
            INSERT INTO setup_funnel (
                run_id, strategy, universe_count,
                after_cpr_width, after_direction, after_dir_filter,
                after_min_price, after_gap, after_or_atr,
                after_narrowing, after_shift, entry_triggered
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                funnel.get("strategy", ""),
                funnel.get("universe_count", 0),
                funnel.get("after_cpr_width", 0),
                funnel.get("after_direction", 0),
                funnel.get("after_dir_filter", 0),
                funnel.get("after_min_price", 0),
                funnel.get("after_gap", 0),
                funnel.get("after_or_atr", 0),
                funnel.get("after_narrowing", 0),
                funnel.get("after_shift", 0),
                funnel.get("entry_triggered", 0),
            ],
        )
        self._after_write()

    def delete_runs(self, run_ids: list[str]) -> dict[str, int]:
        """Delete run_ids from all backtest tables in one transaction."""
        self.ensure_backtest_table()
        self.ensure_run_daily_pnl_table()
        self.ensure_run_metrics_table()
        self.ensure_run_metadata_table()
        self.ensure_setup_funnel_table()

        ids = sorted({str(run_id).strip() for run_id in run_ids if str(run_id).strip()})
        if not ids:
            return {
                "backtest_results": 0,
                "run_daily_pnl": 0,
                "run_metrics": 0,
                "run_metadata": 0,
                "setup_funnel": 0,
            }

        placeholders = ", ".join("?" for _ in ids)
        counts: dict[str, int] = {}
        self.con.execute("BEGIN TRANSACTION")
        try:
            for table in (
                "backtest_results",
                "run_daily_pnl",
                "run_metrics",
                "run_metadata",
                "setup_funnel",
            ):
                row = self.con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE run_id IN ({placeholders})",
                    ids,
                ).fetchone()
                counts[table] = int(row[0] or 0) if row else 0
                self.con.execute(f"DELETE FROM {table} WHERE run_id IN ({placeholders})", ids)
            self.con.execute("COMMIT")
            if self._sync is not None:
                self._sync.mark_dirty()
                self._sync.force_sync(self.con)
            try:
                from web.state import invalidate_run_cache

                invalidate_run_cache(None)
            except Exception as exc:
                logger.debug("Skipping dashboard run-cache invalidation after delete_runs: %s", exc)
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return counts

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_backtest_trades(
        self,
        run_id: str,
        symbols: list[str] | None = None,
        execution_mode: str | None = "BACKTEST",
    ) -> pl.DataFrame:
        """Load trade-level results for a given run_id."""
        try:
            self.ensure_backtest_table()
        except Exception as e:
            logger.debug("Failed to ensure backtest_results for run_id=%s: %s", run_id, e)
            return pl.DataFrame()

        where = "WHERE run_id = ?"
        params: list = [run_id]
        has_execution_mode = self._table_has_column("backtest_results", "execution_mode")
        if execution_mode and has_execution_mode:
            where += " AND COALESCE(execution_mode, 'BACKTEST') = ?"
            params.append(execution_mode)
        if symbols:
            placeholders = ", ".join("?" * len(symbols))
            where += f" AND symbol IN ({placeholders})"
            params.extend(symbols)

        order_by = ["trade_date"]
        if self._table_has_column("backtest_results", "entry_time"):
            order_by.append("entry_time")
        if self._table_has_column("backtest_results", "exit_time"):
            order_by.append("exit_time")
        if self._table_has_column("backtest_results", "symbol"):
            order_by.append("symbol")

        return self.con.execute(
            f"""
            SELECT * FROM backtest_results
            {where}
            ORDER BY {", ".join(order_by)}
            """,
            params,
        ).pl()

    def get_backtest_summary(
        self,
        symbol: str | None = None,
        execution_mode: str | None = "BACKTEST",
    ) -> pl.DataFrame:
        """Aggregated performance from stored backtest_results."""
        try:
            self.ensure_backtest_table()
        except Exception as e:
            logger.debug("Failed to ensure backtest_results before summary: %s", e)
            return pl.DataFrame()

        clauses: list[str] = []
        params: list[object] = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if execution_mode and self._table_has_column("backtest_results", "execution_mode"):
            clauses.append("COALESCE(execution_mode, 'BACKTEST') = ?")
            params.append(execution_mode)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        has_exit_reason = self._table_has_column("backtest_results", "exit_reason")
        exit_reason_sql = (
            "SUM(CASE WHEN exit_reason = 'TARGET'     THEN 1 ELSE 0 END)    AS target_exits, "
            "SUM(CASE WHEN exit_reason LIKE '%SL'     THEN 1 ELSE 0 END)    AS sl_exits, "
            "SUM(CASE WHEN exit_reason = 'INITIAL_SL'   THEN 1 ELSE 0 END) AS initial_sl, "
            "SUM(CASE WHEN exit_reason = 'BREAKEVEN_SL' THEN 1 ELSE 0 END) AS breakeven_sl, "
            "SUM(CASE WHEN exit_reason = 'TRAILING_SL'  THEN 1 ELSE 0 END) AS trailing_sl, "
            "SUM(CASE WHEN exit_reason = 'TIME'       THEN 1 ELSE 0 END)    AS time_exits"
            if has_exit_reason
            else (
                "0 AS target_exits, 0 AS sl_exits, 0 AS initial_sl, "
                "0 AS breakeven_sl, 0 AS trailing_sl, 0 AS time_exits"
            )
        )
        return self.con.execute(
            f"""
            SELECT
                symbol,
                COUNT(*)                                                        AS total_trades,
                SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END)               AS wins,
                SUM(CASE WHEN profit_loss <= 0 THEN 1 ELSE 0 END)              AS losses,
                ROUND(AVG(CASE WHEN profit_loss > 0 THEN 1.0 ELSE 0.0 END)*100, 1) AS win_rate_pct,
                ROUND(SUM(profit_loss), 2)                                      AS total_pnl,
                ROUND(AVG(profit_loss), 2)                                      AS avg_pnl,
                ROUND(MAX(profit_loss), 2)                                      AS best_trade,
                ROUND(MIN(profit_loss), 2)                                      AS worst_trade,
                {exit_reason_sql}
            FROM backtest_results
            {where}
            GROUP BY symbol
            ORDER BY total_pnl DESC
            """,
            params,
        ).pl()

    def get_paper_daily_summary(self) -> list[tuple[object, ...]]:
        """Daily aggregate of all non-TMP paper sessions.

        Returns rows with: trade_date, long_trades, long_wins, long_pnl,
        short_trades, short_wins, short_pnl, total_trades, total_wins, total_pnl.
        """
        try:
            self.ensure_backtest_table()
        except Exception as e:
            logger.debug("Failed to ensure backtest_results for paper daily summary: %s", e)
            return []

        has_em = self._table_has_column("backtest_results", "execution_mode")
        if not has_em:
            return []

        return self.con.execute(
            """
            SELECT
                trade_date,
                SUM(CASE WHEN direction = 'LONG'  THEN 1 ELSE 0 END) AS long_trades,
                SUM(CASE WHEN direction = 'LONG' AND profit_loss > 0 THEN 1 ELSE 0 END) AS long_wins,
                ROUND(SUM(CASE WHEN direction = 'LONG'  THEN profit_loss ELSE 0 END), 2) AS long_pnl,
                SUM(CASE WHEN direction = 'SHORT' THEN 1 ELSE 0 END) AS short_trades,
                SUM(CASE WHEN direction = 'SHORT' AND profit_loss > 0 THEN 1 ELSE 0 END) AS short_wins,
                ROUND(SUM(CASE WHEN direction = 'SHORT' THEN profit_loss ELSE 0 END), 2) AS short_pnl,
                COUNT(*) AS total_trades,
                SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) AS total_wins,
                ROUND(SUM(profit_loss), 2) AS total_pnl
            FROM backtest_results
            WHERE COALESCE(execution_mode, 'BACKTEST') = 'PAPER'
              AND run_id NOT LIKE 'TMP_%'
            GROUP BY trade_date
            ORDER BY trade_date DESC
            """
        ).fetchall()

    def get_compare_breakdown(self, run_a: str, run_b: str) -> dict:
        """Trade-level breakdown for two runs used by the compare page.

        Returns a dict with keys: exit_reasons, win_loss, r_multiple, direction.
        Each contains per-run aggregates keyed by run_id.
        """
        try:
            self.ensure_backtest_table()
        except Exception as e:
            logger.debug("ensure_backtest_results for compare: %s", e)
            return {}

        has_exit_reason = self._table_has_column("backtest_results", "exit_reason")
        has_r_cols = self._table_has_column("backtest_results", "mfe_r") and self._table_has_column(
            "backtest_results", "reached_1r"
        )

        # ── Exit reason breakdown ─────────────────────────────────────
        exit_rows = []
        if has_exit_reason:
            exit_rows = self.con.execute(
                """
                SELECT run_id,
                       exit_reason,
                       COUNT(*) AS cnt,
                       ROUND(AVG(profit_loss), 2) AS avg_pnl
                FROM backtest_results
                WHERE run_id IN (?, ?) AND exit_reason IS NOT NULL
                GROUP BY run_id, exit_reason
                ORDER BY exit_reason, run_id
                """,
                [run_a, run_b],
            ).fetchall()

        exit_reasons: dict[str, dict[str, dict]] = {}
        for row in exit_rows:
            rid, reason, cnt, avg = row
            exit_reasons.setdefault(reason, {})
            exit_reasons[reason][rid] = {"count": int(cnt or 0), "avg_pnl": float(avg or 0)}

        # ── Win/Loss + R-multiple aggregate ───────────────────────────
        r_cols = ""
        r_cols_keys: list[str] = []
        if has_r_cols:
            r_cols = (
                ", AVG(mfe_r) FILTER (WHERE mfe_r IS NOT NULL) AS avg_mfe_r"
                ", AVG(mae_r) FILTER (WHERE mae_r IS NOT NULL) AS avg_mae_r"
                ", AVG(CASE WHEN reached_1r THEN 1.0 ELSE 0.0 END) * 100.0 AS pct_reached_1r"
                ", AVG(CASE WHEN reached_2r THEN 1.0 ELSE 0.0 END) * 100.0 AS pct_reached_2r"
            )
            r_cols_keys = ["avg_mfe_r", "avg_mae_r", "pct_reached_1r", "pct_reached_2r"]

        agg_rows = self.con.execute(
            f"""
            SELECT run_id
                , COUNT(*) AS total
                , AVG(profit_loss) FILTER (WHERE profit_loss > 0) AS avg_win
                , AVG(profit_loss) FILTER (WHERE profit_loss <= 0) AS avg_loss
                , MAX(profit_loss) AS best_trade
                , MIN(profit_loss) AS worst_trade
                , SUM(profit_loss) FILTER (WHERE profit_loss > 0) AS gross_profit
                , ABS(SUM(profit_loss) FILTER (WHERE profit_loss < 0)) AS gross_loss
                {r_cols}
            FROM backtest_results
            WHERE run_id IN (?, ?)
            GROUP BY run_id
            """,
            [run_a, run_b],
        ).fetchall()
        cols = [d[0] for d in self.con.description]

        win_loss: dict[str, dict] = {}
        r_multiple: dict[str, dict] = {}
        for row in agg_rows:
            rec = dict(zip(cols, row, strict=True))
            rid = str(rec.pop("run_id"))
            wl_keys = [
                "total",
                "avg_win",
                "avg_loss",
                "best_trade",
                "worst_trade",
                "gross_profit",
                "gross_loss",
            ]
            win_loss[rid] = {k: float(rec.get(k) or 0) for k in wl_keys}
            if r_cols_keys:
                r_multiple[rid] = {k: float(rec.get(k) or 0) for k in r_cols_keys}

        # ── Direction breakdown ────────────────────────────────────────
        dir_rows = self.con.execute(
            """
            SELECT run_id, direction,
                   COUNT(*) AS cnt,
                   AVG(profit_loss) FILTER (WHERE profit_loss > 0) * 100.0 AS win_pct,
                   SUM(profit_loss) AS total_pnl
            FROM backtest_results
            WHERE run_id IN (?, ?)
            GROUP BY run_id, direction
            """,
            [run_a, run_b],
        ).fetchall()

        direction: dict[str, dict[str, dict]] = {}
        for row in dir_rows:
            rid, d, cnt, wp, tp = row
            direction.setdefault(rid, {})[d] = {
                "count": int(cnt or 0),
                "win_pct": float(wp or 0),
                "total_pnl": float(tp or 0),
            }

        return {
            "exit_reasons": exit_reasons,
            "win_loss": win_loss,
            "r_multiple": r_multiple,
            "direction": direction,
        }

    def get_runs_with_metrics(
        self,
        limit: int | None = None,
        execution_mode: str | None = "BACKTEST",
    ) -> list[dict]:
        """Get run-level performance metrics from materialized run_metrics."""
        try:
            self.ensure_backtest_table()
            self.ensure_run_metadata_table()
            self.ensure_run_metrics_table()
        except Exception as e:
            logger.exception("Failed to ensure run metric tables: %s", e)
            return []

        # Refresh run_metrics if empty on the writable engine connection only.
        try:
            row = self.con.execute("SELECT COUNT(*) FROM run_metrics").fetchone()
            if int(row[0] or 0) == 0 and not self.read_only:
                self.refresh_run_metrics()
        except Exception as e:
            logger.debug("run_metrics pre-refresh failed: %s", e)

        try:
            has_run_metadata = self._table_exists("run_metadata")
            has_run_execution_mode = self._table_has_column("backtest_results", "execution_mode")

            br_metrics_sql = (
                "SELECT "
                "    run_id, "
                "    COUNT(*) AS actual_trade_count, "
                "    COUNT(DISTINCT symbol) AS actual_symbol_count, "
                "    MIN(trade_date) AS actual_start_date, "
                "    MAX(trade_date) AS actual_end_date, "
                "    SUM(profit_loss) AS actual_total_pnl, "
                "    AVG(CASE WHEN profit_loss > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS actual_win_rate, "
                "    SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) AS actual_gross_profit, "
                "    ABS(SUM(CASE WHEN profit_loss < 0 THEN profit_loss ELSE 0 END)) AS actual_gross_loss"
            )
            if has_run_execution_mode:
                br_metrics_sql += (
                    ", MAX(COALESCE(execution_mode, 'BACKTEST')) AS execution_mode "
                    "FROM backtest_results GROUP BY run_id"
                )
            else:
                br_metrics_sql += " FROM backtest_results GROUP BY run_id"

            if has_run_metadata:
                direction_sql = (
                    "UPPER(COALESCE(NULLIF(json_extract_string(rm.params_json, "
                    "'$.direction_filter'), ''), 'BOTH')) AS direction_filter"
                )
                fbr_setup_sql = (
                    "UPPER(COALESCE(NULLIF(json_extract_string(rm.params_json, "
                    "'$.fbr_setup_filter'), ''), 'BOTH')) AS fbr_setup_filter"
                )
                rvol_sql = (
                    "COALESCE(TRY_CAST(json_extract(rm.params_json, '$.rvol_threshold') "
                    "AS DOUBLE), 1.0) AS rvol_threshold"
                )
                cpr_min_close_atr_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_levels.cpr_min_close_atr') AS DOUBLE), "
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_levels_config.cpr_min_close_atr') AS DOUBLE), "
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_min_close_atr') AS DOUBLE), "
                    "0.0) AS cpr_min_close_atr"
                )
                failure_window_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.fbr_config.failure_window') AS INTEGER), "
                    "TRY_CAST(json_extract(rm.params_json, '$.fbr.failure_window') AS INTEGER), "
                    "TRY_CAST(json_extract(rm.params_json, '$.failure_window') AS INTEGER), "
                    "0) AS failure_window"
                )
                skip_rvol_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.skip_rvol_check') AS BOOLEAN), "
                    "TRY_CAST(json_extract(rm.params_json, '$.skip_rvol') AS BOOLEAN), "
                    "FALSE) AS skip_rvol_check"
                )
                risk_based_sizing_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.risk_based_sizing') AS BOOLEAN), "
                    "TRY_CAST(json_extract(rm.params_json, '$.legacy_sizing') AS BOOLEAN), "
                    "FALSE) AS risk_based_sizing"
                )
                compound_equity_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.compound_equity') AS BOOLEAN), "
                    "FALSE) AS compound_equity"
                )
                params_json_sql = "rm.params_json"
                symbols_json_sql = "rm.symbols_json"
                updated_at_sql = "COALESCE(r.updated_at, rm.created_at)::VARCHAR AS updated_at"
                if has_run_execution_mode:
                    execution_sql = (
                        "UPPER(COALESCE(brm.execution_mode, 'BACKTEST')) AS execution_mode"
                    )
                    from_sql = (
                        "FROM run_metrics r "
                        "LEFT JOIN ("
                        f"    {br_metrics_sql}"
                        ") brm ON brm.run_id = r.run_id "
                        "LEFT JOIN run_metadata rm ON rm.run_id = r.run_id"
                    )
                else:
                    execution_sql = "'BACKTEST' AS execution_mode"
                    from_sql = (
                        "FROM run_metrics r "
                        "LEFT JOIN ("
                        f"    {br_metrics_sql}"
                        ") brm ON brm.run_id = r.run_id "
                        "LEFT JOIN run_metadata rm ON rm.run_id = r.run_id"
                    )
                if execution_mode:
                    if has_run_execution_mode:
                        from_sql += " WHERE UPPER(COALESCE(brm.execution_mode, 'BACKTEST')) = ?"
                    else:
                        from_sql += " WHERE 'BACKTEST' = ?"
            else:
                direction_sql = "'BOTH' AS direction_filter"
                fbr_setup_sql = "'BOTH' AS fbr_setup_filter"
                rvol_sql = (
                    "COALESCE("
                    "TRY_CAST(NULLIF(REGEXP_EXTRACT(LOWER(COALESCE(r.label, '')), "
                    "'rvol([0-9]+(?:\\.[0-9]+)?)', 1), '') AS DOUBLE), "
                    "1.0) AS rvol_threshold"
                )
                cpr_min_close_atr_sql = "0.0 AS cpr_min_close_atr"
                failure_window_sql = "0 AS failure_window"
                skip_rvol_sql = (
                    "(POSITION('rvoloff' IN LOWER(COALESCE(r.label, ''))) > 0) AS skip_rvol_check"
                )
                risk_based_sizing_sql = "FALSE AS risk_based_sizing"
                compound_equity_sql = "FALSE AS compound_equity"
                params_json_sql = "NULL::VARCHAR AS params_json"
                symbols_json_sql = "NULL::VARCHAR AS symbols_json"
                updated_at_sql = "r.updated_at::VARCHAR AS updated_at"
                if has_run_execution_mode:
                    execution_sql = (
                        "UPPER(COALESCE(brm.execution_mode, 'BACKTEST')) AS execution_mode"
                    )
                    from_sql = (
                        "FROM run_metrics r "
                        "LEFT JOIN ("
                        f"    {br_metrics_sql}"
                        ") brm ON brm.run_id = r.run_id"
                    )
                    if execution_mode:
                        from_sql += " WHERE UPPER(COALESCE(brm.execution_mode, 'BACKTEST')) = ?"
                else:
                    execution_sql = "'BACKTEST' AS execution_mode"
                    from_sql = (
                        "FROM run_metrics r "
                        "LEFT JOIN ("
                        f"    {br_metrics_sql}"
                        ") brm ON brm.run_id = r.run_id"
                    )
                    if execution_mode:
                        from_sql += " WHERE 'BACKTEST' = ?"

            if "WHERE" in from_sql:
                from_sql += " AND COALESCE(brm.actual_trade_count, 0) > 0"
            else:
                from_sql += " WHERE COALESCE(brm.actual_trade_count, 0) > 0"

            query = """
                SELECT
                    r.run_id,
                    COALESCE(r.strategy_code, r.strategy) AS strategy_code,
                    COALESCE(r.label, r.strategy, r.run_id) AS label,
                    COALESCE(brm.actual_start_date, r.start_date)::VARCHAR,
                    COALESCE(brm.actual_end_date, r.end_date)::VARCHAR,
                    COALESCE(brm.actual_trade_count, r.trade_count),
                    COALESCE(brm.actual_symbol_count, r.symbol_count),
                    r.allocated_capital,
                    COALESCE(ROUND(brm.actual_total_pnl, 2), r.total_pnl),
                    CASE
                        WHEN GREATEST(COALESCE(r.allocated_capital, 0.0), 1.0) <= 0 THEN 0.0
                        ELSE ROUND(
                            COALESCE(brm.actual_total_pnl, r.total_pnl)
                            / GREATEST(COALESCE(r.allocated_capital, 0.0), 1.0) * 100.0,
                            2
                        )
                    END AS total_return_pct,
                    COALESCE(ROUND(brm.actual_win_rate, 1), r.win_rate),
                    CASE
                        WHEN COALESCE(brm.actual_gross_loss, 0.0) > 0
                        THEN ROUND(COALESCE(brm.actual_gross_profit, 0.0) / brm.actual_gross_loss, 2)
                        ELSE r.profit_factor
                    END AS profit_factor,
                    r.max_dd_abs,
                    r.max_dd_pct,
                    r.annual_return_pct,
                    r.calmar,
                    __EXECUTION_SQL__,
                    __DIRECTION_SQL__,
                    __RVOL_SQL__,
                    __CPR_MIN_CLOSE_ATR_SQL__,
                    __FAILURE_WINDOW_SQL__,
                    __SKIP_RVOL_SQL__,
                    __RISK_BASED_SIZING_SQL__,
                    __COMPOUND_EQUITY_SQL__,
                    __PARAMS_JSON_SQL__,
                    __SYMBOLS_JSON_SQL__,
                    __UPDATED_AT_SQL__,
                    CASE
                        WHEN r.start_date IS NOT NULL AND r.end_date IS NOT NULL
                        THEN DATE_DIFF('day', r.start_date, r.end_date) + 1
                        ELSE 0
                    END AS run_span_days,
                    __FBR_SETUP_SQL__
                __FROM_SQL__
                ORDER BY r.updated_at DESC NULLS LAST
            """
            query = (
                query.replace("__EXECUTION_SQL__", execution_sql)
                .replace("__DIRECTION_SQL__", direction_sql)
                .replace("__RVOL_SQL__", rvol_sql)
                .replace("__CPR_MIN_CLOSE_ATR_SQL__", cpr_min_close_atr_sql)
                .replace("__FAILURE_WINDOW_SQL__", failure_window_sql)
                .replace("__SKIP_RVOL_SQL__", skip_rvol_sql)
                .replace("__RISK_BASED_SIZING_SQL__", risk_based_sizing_sql)
                .replace("__COMPOUND_EQUITY_SQL__", compound_equity_sql)
                .replace("__PARAMS_JSON_SQL__", params_json_sql)
                .replace("__SYMBOLS_JSON_SQL__", symbols_json_sql)
                .replace("__UPDATED_AT_SQL__", updated_at_sql)
                .replace("__FBR_SETUP_SQL__", fbr_setup_sql)
                .replace("__FROM_SQL__", from_sql)
            )
            params: list[object] = []
            if execution_mode:
                params.append(execution_mode.upper())
            if limit is not None and int(limit) > 0:
                query += " LIMIT ?"
                params.append(int(limit))
            rows = self.con.execute(query, params).fetchall()
        except Exception as e:
            logger.exception("Failed to read run metrics: %s", e)
            return []

        result = [
            {
                "run_id": r[0],
                "strategy": (str(r[1]) if r[1] is not None else "") or r[0],
                "strategy_code": (str(r[1]) if r[1] is not None else "") or r[0],
                "label": (str(r[2]) if r[2] is not None else "") or (str(r[1]) if r[1] else r[0]),
                "start_date": (str(r[3]) if r[3] is not None else "")[:10],
                "end_date": (str(r[4]) if r[4] is not None else "")[:10],
                "trade_count": int(r[5] or 0),
                "symbol_count": int(r[6] or 0),
                "allocated_capital": float(r[7] or 0.0),
                "total_pnl": float(r[8] or 0.0),
                "total_return_pct": float(r[9] or 0.0),
                "win_rate": float(r[10] or 0.0),
                "profit_factor": float(r[11] or 0.0),
                "max_dd_abs": round(float(r[12] or 0.0), 0),
                "max_dd_pct": float(r[13] or 0.0),
                "annual_return_pct": float(r[14] or 0.0),
                "calmar": float(r[15] or 0.0),
                "execution_mode": str(r[16] or "BACKTEST").upper(),
                "direction_filter": str(r[17] or "BOTH").upper(),
                "rvol_threshold": float(r[18] or 1.0),
                "cpr_min_close_atr": float(r[19] or 0.0),
                "failure_window": int(r[20] or 0),
                "skip_rvol_check": bool(r[21] or False),
                "risk_based_sizing": bool(r[22] or False),
                "compound_equity": bool(r[23] or False),
                "params_json": str(r[24] or ""),
                "symbols_json": str(r[25] or ""),
                "updated_at": str(r[26] or ""),
                "run_span_days": int(r[27] or 0),
                "fbr_setup_filter": str(r[28] or "BOTH").upper(),
            }
            for r in rows
        ]

        # For PAPER queries: also include 0-trade sessions that exist in run_metadata
        # but are absent from run_metrics (which only covers sessions with trades).
        if execution_mode and execution_mode.upper() == "PAPER":
            try:

                def _bool_from_param(raw: object) -> bool:
                    if isinstance(raw, bool):
                        return raw
                    if raw is None:
                        return False
                    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

                def _float_from_param(raw: object, default: float) -> float:
                    try:
                        if raw is None:
                            return default
                        return float(raw)
                    except (TypeError, ValueError):
                        return default

                def _int_from_param(raw: object, default: int) -> int:
                    try:
                        if raw is None:
                            return default
                        if isinstance(raw, bool):
                            return int(raw)
                        if isinstance(raw, int):
                            return raw
                        if isinstance(raw, float):
                            return int(raw)
                        return int(str(raw).strip())
                    except (TypeError, ValueError):
                        return default

                returned_ids = {r["run_id"] for r in result}
                id_params: list[object] = []
                id_filter = ""
                if returned_ids:
                    placeholders = ", ".join("?" for _ in returned_ids)
                    id_filter = f"AND run_id NOT IN ({placeholders})"
                    id_params = list(returned_ids)
                zero_rows = self.con.execute(
                    f"SELECT run_id, strategy, label, start_date, end_date, params_json "
                    f"FROM run_metadata "
                    f"WHERE execution_mode = 'PAPER' {id_filter} "
                    f"ORDER BY created_at DESC",
                    id_params,
                ).fetchall()
                for zt in zero_rows:
                    params_str = str(zt[5] or "")
                    try:
                        p = json.loads(params_str) if params_str else {}
                    except Exception:
                        p = {}
                    skip_rvol_check = _bool_from_param(
                        p.get("skip_rvol_check", p.get("skip_rvol", False))
                    )
                    run_id = str(zt[0] or "")
                    strategy = str(zt[1] or "") or run_id
                    result.append(
                        {
                            "run_id": run_id,
                            "strategy": strategy,
                            "strategy_code": strategy,
                            "label": str(zt[2] or "") or strategy,
                            "start_date": str(zt[3] or "")[:10],
                            "end_date": str(zt[4] or "")[:10],
                            "trade_count": 0,
                            "symbol_count": 0,
                            "allocated_capital": 0.0,
                            "total_pnl": 0.0,
                            "total_return_pct": 0.0,
                            "win_rate": 0.0,
                            "profit_factor": 0.0,
                            "max_dd_abs": 0.0,
                            "max_dd_pct": 0.0,
                            "annual_return_pct": 0.0,
                            "calmar": 0.0,
                            "execution_mode": "PAPER",
                            "direction_filter": str(p.get("direction_filter") or "BOTH").upper(),
                            "rvol_threshold": _float_from_param(p.get("rvol_threshold"), 1.0),
                            "cpr_min_close_atr": _float_from_param(p.get("cpr_min_close_atr"), 0.0),
                            "failure_window": _int_from_param(p.get("failure_window"), 0),
                            "skip_rvol_check": skip_rvol_check,
                            "risk_based_sizing": bool(p.get("risk_based_sizing") or False),
                            "compound_equity": bool(p.get("compound_equity") or False),
                            "params_json": params_str,
                            "run_span_days": 0,
                            "fbr_setup_filter": str(p.get("fbr_setup_filter") or "BOTH").upper(),
                        }
                    )
            except Exception as e:
                logger.debug("Failed to fetch 0-trade PAPER sessions: %s", e)

        return result

    # ------------------------------------------------------------------
    # Refresh / materialize
    # ------------------------------------------------------------------

    def refresh_run_daily_pnl(self, run_ids: list[str] | None = None) -> int:
        """Recompute run-level daily PnL series."""
        self.ensure_backtest_table()
        self.ensure_run_daily_pnl_table()

        params: list[object] = []
        where_sql = ""
        if run_ids:
            ids = sorted({str(x).strip() for x in run_ids if str(x).strip()})
            if not ids:
                return 0
            placeholders = ", ".join("?" for _ in ids)
            where_sql = f"WHERE run_id IN ({placeholders})"
            params = list(ids)
            self.con.execute(f"DELETE FROM run_daily_pnl WHERE run_id IN ({placeholders})", params)
        else:
            self.con.execute("DELETE FROM run_daily_pnl")

        self.con.execute(
            f"""
            INSERT INTO run_daily_pnl (run_id, trade_date, day_pnl, cum_pnl, updated_at)
            WITH daily AS (
                SELECT run_id, trade_date, SUM(profit_loss) AS day_pnl
                FROM backtest_results
                {where_sql}
                GROUP BY run_id, trade_date
            ),
            with_cum AS (
                SELECT run_id, trade_date, day_pnl,
                    SUM(day_pnl) OVER (
                        PARTITION BY run_id ORDER BY trade_date
                        ROWS UNBOUNDED PRECEDING
                    ) AS cum_pnl
                FROM daily
            )
            SELECT run_id, trade_date, ROUND(day_pnl, 2), ROUND(cum_pnl, 2), now()
            FROM with_cum
            """,
            params,
        )

        if run_ids:
            placeholders = ", ".join("?" for _ in params)
            row = self.con.execute(
                f"SELECT COUNT(*) FROM run_daily_pnl WHERE run_id IN ({placeholders})",
                params,
            ).fetchone()
        else:
            row = self.con.execute("SELECT COUNT(*) FROM run_daily_pnl").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def refresh_run_metrics(self, run_ids: list[str] | None = None) -> int:
        """Recompute materialized run_metrics."""
        self.ensure_backtest_table()
        self.ensure_run_metadata_table()
        self.ensure_run_daily_pnl_table()
        self.ensure_run_metrics_table()
        self.refresh_run_daily_pnl(run_ids)

        params: list[object] = []
        where_sql = ""
        if run_ids:
            ids = sorted({str(x).strip() for x in run_ids if str(x).strip()})
            if not ids:
                return 0
            placeholders = ", ".join("?" for _ in ids)
            where_sql = f"WHERE run_id IN ({placeholders})"
            params = list(ids)
            self.con.execute(f"DELETE FROM run_metrics WHERE run_id IN ({placeholders})", params)
        else:
            self.con.execute("DELETE FROM run_metrics")

        self.con.execute(
            f"""
            INSERT INTO run_metrics (
                run_id, strategy, strategy_code, label,
                start_date, end_date, trade_count, symbol_count,
                allocated_capital, total_pnl, total_return_pct, win_rate,
                profit_factor, max_dd_abs, max_dd_pct,
                annual_return_pct, calmar, updated_at
            )
            WITH run_base AS (
                SELECT
                    br.run_id,
                    MIN(br.trade_date) AS start_date,
                    MAX(br.trade_date) AS end_date,
                    COUNT(*) AS trade_count,
                    COUNT(DISTINCT br.symbol) AS symbol_count,
                    SUM(br.profit_loss) AS total_pnl,
                    AVG(CASE WHEN br.profit_loss > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate,
                    SUM(CASE WHEN br.profit_loss > 0 THEN br.profit_loss ELSE 0 END) AS gross_profit,
                    ABS(SUM(CASE WHEN br.profit_loss < 0 THEN br.profit_loss ELSE 0 END)) AS gross_loss
                FROM backtest_results br
                {where_sql}
                GROUP BY br.run_id
            ),
            base_with_meta AS (
                SELECT
                    rb.run_id,
                    COALESCE(rm.strategy, 'UNKNOWN') AS strategy_code,
                    COALESCE(rm.label, rm.strategy, rb.run_id) AS label,
                    COALESCE(TRY_CAST(rm.start_date AS DATE), rb.start_date) AS run_start_date,
                    COALESCE(TRY_CAST(rm.end_date AS DATE), rb.end_date) AS run_end_date,
                    rb.trade_count, rb.symbol_count,
                    COALESCE(
                        TRY_CAST(json_extract(rm.params_json, '$.portfolio_value') AS DOUBLE),
                        rb.symbol_count * COALESCE(
                            TRY_CAST(json_extract(rm.params_json, '$.capital') AS DOUBLE),
                            100000.0
                        )
                    ) AS allocated_capital,
                    ROUND(rb.total_pnl, 2) AS total_pnl,
                    ROUND(rb.win_rate, 1) AS win_rate,
                    CASE WHEN rb.gross_loss > 0
                         THEN ROUND(rb.gross_profit / rb.gross_loss, 2)
                         ELSE 99.9 END AS profit_factor,
                    COALESCE(
                        TRY_CAST(json_extract(rm.params_json, '$.portfolio_value') AS DOUBLE),
                        rb.symbol_count * COALESCE(
                            TRY_CAST(json_extract(rm.params_json, '$.capital') AS DOUBLE),
                            100000.0
                        )
                    ) AS initial_equity
                FROM run_base rb
                LEFT JOIN run_metadata rm ON rb.run_id = rm.run_id
            ),
            dd_series AS (
                SELECT
                    rdp.run_id, rdp.trade_date, rdp.cum_pnl, bwm.allocated_capital,
                    bwm.initial_equity + rdp.cum_pnl AS equity_abs,
                    (bwm.initial_equity + rdp.cum_pnl) - GREATEST(
                        MAX(bwm.initial_equity + rdp.cum_pnl) OVER (
                            PARTITION BY rdp.run_id ORDER BY rdp.trade_date
                            ROWS UNBOUNDED PRECEDING
                        ), bwm.initial_equity
                    ) AS drawdown_abs,
                    GREATEST(
                        MAX(bwm.initial_equity + rdp.cum_pnl) OVER (
                            PARTITION BY rdp.run_id ORDER BY rdp.trade_date
                            ROWS UNBOUNDED PRECEDING
                        ), bwm.initial_equity
                    ) AS running_peak_abs
                FROM run_daily_pnl rdp
                JOIN base_with_meta bwm ON rdp.run_id = bwm.run_id
            ),
            dd_curve AS (
                SELECT run_id, drawdown_abs,
                    ((equity_abs / GREATEST(running_peak_abs, 1.0)) - 1.0) * 100.0 AS drawdown_pct
                FROM dd_series
            ),
            dd_agg AS (
                SELECT run_id,
                    COALESCE(MIN(drawdown_abs), 0.0) AS max_dd_abs,
                    COALESCE(ABS(MIN(drawdown_pct)), 0.0) AS max_dd_pct_raw
                FROM dd_curve GROUP BY run_id
            ),
            metric_base AS (
                SELECT bwm.run_id, bwm.strategy_code, bwm.label,
                    bwm.run_start_date, bwm.run_end_date,
                    bwm.trade_count, bwm.symbol_count, bwm.allocated_capital,
                    bwm.total_pnl,
                    CASE WHEN GREATEST(bwm.initial_equity, 1.0) <= 0 THEN 0.0
                         ELSE (bwm.total_pnl / GREATEST(bwm.initial_equity, 1.0)) * 100.0
                    END AS total_return_pct_raw,
                    bwm.win_rate, bwm.profit_factor,
                    COALESCE(da.max_dd_abs, 0.0) AS max_dd_abs,
                    COALESCE(da.max_dd_pct_raw, 0.0) AS max_dd_pct_raw,
                    CASE
                        WHEN (bwm.initial_equity + bwm.total_pnl) <= 0 THEN -100.0
                        ELSE (POWER(
                            GREATEST(
                                (bwm.initial_equity + bwm.total_pnl) / GREATEST(bwm.initial_equity, 1.0),
                                1e-12
                            ),
                            1.0 / GREATEST(
                                (DATE_DIFF('day', bwm.run_start_date, bwm.run_end_date) + 1) / 365.25,
                                1.0 / 365.25
                            )
                        ) - 1.0) * 100.0
                    END AS annual_return_pct_raw
                FROM base_with_meta bwm
                LEFT JOIN dd_agg da ON bwm.run_id = da.run_id
            )
            SELECT run_id, strategy_code AS strategy, strategy_code, label,
                run_start_date AS start_date, run_end_date AS end_date,
                trade_count, symbol_count,
                ROUND(allocated_capital, 2) AS allocated_capital,
                total_pnl,
                ROUND(total_return_pct_raw, 2) AS total_return_pct,
                win_rate, profit_factor, max_dd_abs,
                ROUND(max_dd_pct_raw, 4) AS max_dd_pct,
                ROUND(annual_return_pct_raw, 2) AS annual_return_pct,
                CASE WHEN max_dd_pct_raw > 0
                     THEN ROUND(annual_return_pct_raw / max_dd_pct_raw, 2)
                     WHEN annual_return_pct_raw > 0 THEN 99.9
                     ELSE 0.0 END AS calmar,
                now() AS updated_at
            FROM metric_base
            """,
            params,
        )

        if run_ids:
            placeholders = ", ".join("?" for _ in params)
            row = self.con.execute(
                f"SELECT COUNT(*) FROM run_metrics WHERE run_id IN ({placeholders})",
                params,
            ).fetchone()
        else:
            row = self.con.execute("SELECT COUNT(*) FROM run_metrics").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, int]:
        """Return row counts for backtest-related tables."""
        table_names = [
            "backtest_results",
            "run_metadata",
            "run_metrics",
            "run_daily_pnl",
            "setup_funnel",
        ]
        tables = dict.fromkeys(table_names, 0)
        existing = [t for t in table_names if self._table_exists(t)]
        if existing:
            union_sql = "\nUNION ALL\n".join(
                f"SELECT '{t}' AS tn, COUNT(*) AS rc FROM {t}" for t in existing
            )
            try:
                rows = self.con.execute(union_sql).fetchall()
                for row in rows:
                    tables[str(row[0])] = int(row[1] or 0)
            except Exception as e:
                logger.debug("Failed to collect backtest table counts: %s", e)
        return tables

    # ------------------------------------------------------------------
    # Raw SQL
    # ------------------------------------------------------------------

    def execute_sql(
        self, query: str, params: list | dict | None = None
    ) -> duckdb.DuckDBPyConnection:
        if params:
            return self.con.execute(query, params)
        return self.con.execute(query)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception as e:
            logger.debug("BacktestDB close ignored: %s", e)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Module-level singleton (thread-safe)
# ---------------------------------------------------------------------------
_backtest_db: BacktestDB | None = None
_dashboard_backtest_db: BacktestDB | None = None
_dashboard_backtest_consumer: ReplicaConsumer | None = None
_backtest_db_lock = threading.Lock()
_dashboard_backtest_lock = threading.Lock()
_backtest_db_atexit = False
_dashboard_backtest_atexit = False


def get_backtest_db() -> BacktestDB:
    """Return the global BacktestDB instance (creates on first call)."""
    global _backtest_db, _backtest_db_atexit
    if _backtest_db is None:
        with _backtest_db_lock:
            if _backtest_db is None:
                replica_dir = REPLICA_DIR
                replica_dir.mkdir(parents=True, exist_ok=True)
                sync = ReplicaSync(BACKTEST_DUCKDB_FILE, replica_dir, min_interval_sec=30.0)
                _backtest_db = BacktestDB(replica_sync=sync)
                if not _backtest_db_atexit:
                    atexit.register(close_backtest_db)
                    _backtest_db_atexit = True
    return _backtest_db


def get_dashboard_backtest_db() -> BacktestDB:
    """Return a read-only BacktestDB instance backed by the latest replica.

    Raises RuntimeError if no replica exists — the dashboard must never
    open the live backtest.duckdb directly.
    """
    global _dashboard_backtest_db, _dashboard_backtest_consumer, _dashboard_backtest_atexit
    REPLICA_DIR.mkdir(parents=True, exist_ok=True)
    if _dashboard_backtest_consumer is None:
        _dashboard_backtest_consumer = ReplicaConsumer(REPLICA_DIR, BACKTEST_DUCKDB_FILE.stem)
    replica_path = _dashboard_backtest_consumer.get_replica_path()
    if _dashboard_backtest_db is None:
        with _dashboard_backtest_lock:
            if _dashboard_backtest_db is None:
                if replica_path is None:
                    raise RuntimeError(
                        f"No backtest replica found in {REPLICA_DIR}. "
                        "Run a backtest with --save to create one."
                    )
                _dashboard_backtest_db = BacktestDB(
                    db_path=replica_path,
                    read_only=True,
                )
                if not _dashboard_backtest_atexit:
                    atexit.register(close_dashboard_backtest_db)
                    _dashboard_backtest_atexit = True
    elif replica_path is not None and _dashboard_backtest_db.db_path != replica_path:
        with _dashboard_backtest_lock:
            if (
                _dashboard_backtest_db is not None
                and _dashboard_backtest_db.db_path != replica_path
            ):
                _dashboard_backtest_db.close()
                _dashboard_backtest_db = BacktestDB(db_path=replica_path, read_only=True)
    return _dashboard_backtest_db


def close_backtest_db() -> None:
    global _backtest_db
    if _backtest_db is not None:
        _backtest_db.close()
        _backtest_db = None


def close_dashboard_backtest_db() -> None:
    global _dashboard_backtest_db
    if _dashboard_backtest_db is not None:
        _dashboard_backtest_db.close()
        _dashboard_backtest_db = None
