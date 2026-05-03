"""Backtest-result persistence mixin for DuckDB-backed stores."""

from __future__ import annotations

import json
import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


class DuckDBBacktestResultsMixin:
    """Backtest result table creation, storage, metrics, and lookup helpers."""

    con: Any
    read_only: bool
    _sync: Any

    def _table_exists(self, table: str) -> bool:
        raise NotImplementedError

    def _table_has_column(self, table: str, column: str) -> bool:
        raise NotImplementedError

    def _begin_replica_batch(self) -> None:
        raise NotImplementedError

    def _end_replica_batch(self) -> None:
        raise NotImplementedError

    def _publish_replica(self, *, force: bool = False) -> None:
        raise NotImplementedError

    # Backtest results storage
    # ------------------------------------------------------------------

    _backtest_table_ready: bool = False
    _run_metadata_ready: bool = False
    _run_metrics_ready: bool = False
    _run_daily_pnl_ready: bool = False
    _setup_funnel_ready: bool = False

    def ensure_run_metadata_table(self) -> None:
        """Create run_metadata table if it doesn't exist.

        Stores human-readable labels for each run_id (strategy name, params).
        Separate from backtest_results to keep trade-level table compact.
        """
        if self._run_metadata_ready:
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
                logger.debug(f"Failed to add run_metadata column {col}: {e}")
        self._run_metadata_ready = True

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
        """Insert a run_id → strategy mapping into run_metadata.

        Append-only: run_id must be unique. A duplicate run_id indicates a bug.
        param_signature is stored for grouping runs with identical parameters.
        """
        self.ensure_run_metadata_table()
        symbols_json = json.dumps(sorted(set(symbols))) if symbols else None
        params_json = json.dumps(params, sort_keys=True) if isinstance(params, dict) else None
        try:
            self.con.execute(
                """
                INSERT INTO run_metadata (
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
        except Exception as e:
            logger.exception("Failed to insert run_metadata for run_id=%s: %s", run_id, e)

    def delete_runs(self, run_ids: list[str]) -> dict[str, int]:
        """Delete run_ids from DuckDB runtime tables in one transaction."""
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

    def ensure_backtest_table(self) -> None:
        """Create backtest_results table if it doesn't exist.

        Stores the execution-level fields needed for trade audit and dashboard drilldown.
        Added CHECK constraints for
        better compression and query optimization.

        Also adds mfe_r / mae_r columns to existing tables that predate this feature
        (ALTER TABLE ... ADD COLUMN IF NOT EXISTS is a no-op when the column is present).
        """
        if self._backtest_table_ready:
            return
        if self.read_only:
            # Table already exists — DDL not permitted on a read-only connection
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
                    'TRAILING_SL', 'TIME', 'REVERSAL', 'CANDLE_EXIT'
                )),
                sl_phase         VARCHAR CHECK (sl_phase IN ('PROTECT', 'BREAKEVEN', 'TRAIL')),
                atr              DOUBLE,
                cpr_width_pct    DOUBLE,
                position_size    INTEGER,
                position_value   DOUBLE,
                -- Removed: created_at (unnecessary overhead)
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
        # Only create indexes that are actually used
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol ON backtest_results(run_id, symbol)"
        )
        # Index for trade inspector lookups (run_id, symbol, trade_date)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol_date ON backtest_results(run_id, symbol, trade_date)"
        )
        # Migrate existing tables that predate newer audit columns.
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
                # DuckDB older than 0.8 may not support IF NOT EXISTS here — safe to ignore
                logger.debug(f"Failed to add column {col}: {e}")
        self._backtest_table_ready = True

    def ensure_run_metrics_table(self) -> None:
        """Create materialized run-level metrics table for dashboard reads."""
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
                logger.debug(f"Failed to add run_metrics column {col}: {e}")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_metrics_trade_count ON run_metrics(trade_count)"
        )
        self._run_metrics_ready = True

    def ensure_run_daily_pnl_table(self) -> None:
        """Create run-level daily PnL materialization table if it doesn't exist."""
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
        """Create setup_funnel table to store per-run filter pipeline diagnostics."""
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
        self._publish_replica(force=True)

    def refresh_run_daily_pnl(self, run_ids: list[str] | None = None) -> int:
        """Recompute run-level daily PnL series for all runs or a run subset."""
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
            INSERT INTO run_daily_pnl (
                run_id,
                trade_date,
                day_pnl,
                cum_pnl,
                updated_at
            )
            WITH daily AS (
                SELECT
                    run_id,
                    trade_date,
                    SUM(profit_loss) AS day_pnl
                FROM backtest_results
                {where_sql}
                GROUP BY run_id, trade_date
            ),
            with_cum AS (
                SELECT
                    run_id,
                    trade_date,
                    day_pnl,
                    SUM(day_pnl) OVER (
                        PARTITION BY run_id
                        ORDER BY trade_date
                        ROWS UNBOUNDED PRECEDING
                    ) AS cum_pnl
                FROM daily
            )
            SELECT
                run_id,
                trade_date,
                ROUND(day_pnl, 2) AS day_pnl,
                ROUND(cum_pnl, 2) AS cum_pnl,
                now() AS updated_at
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
        result = int(row[0]) if row and row[0] is not None else 0
        self._publish_replica(force=True)
        return result

    def refresh_run_metrics(self, run_ids: list[str] | None = None) -> int:
        """Recompute materialized run_metrics for all runs or a run subset."""
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
                run_id,
                strategy,
                strategy_code,
                label,
                start_date,
                end_date,
                trade_count,
                symbol_count,
                allocated_capital,
                total_pnl,
                total_return_pct,
                win_rate,
                profit_factor,
                max_dd_abs,
                max_dd_pct,
                annual_return_pct,
                calmar,
                updated_at
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
                    rb.trade_count,
                    rb.symbol_count,
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
                    rdp.run_id,
                    rdp.trade_date,
                    rdp.cum_pnl,
                    bwm.allocated_capital,
                    bwm.initial_equity + rdp.cum_pnl AS equity_abs,
                    (bwm.initial_equity + rdp.cum_pnl) - GREATEST(
                        MAX(bwm.initial_equity + rdp.cum_pnl) OVER (
                            PARTITION BY rdp.run_id
                            ORDER BY rdp.trade_date
                            ROWS UNBOUNDED PRECEDING
                        ),
                        bwm.initial_equity
                    ) AS drawdown_abs,
                    GREATEST(
                        MAX(bwm.initial_equity + rdp.cum_pnl) OVER (
                            PARTITION BY rdp.run_id
                            ORDER BY rdp.trade_date
                            ROWS UNBOUNDED PRECEDING
                        ),
                        bwm.initial_equity
                    ) AS running_peak_abs
                FROM run_daily_pnl rdp
                JOIN base_with_meta bwm ON rdp.run_id = bwm.run_id
            ),
            dd_curve AS (
                SELECT
                    run_id,
                    drawdown_abs,
                    ((equity_abs / GREATEST(running_peak_abs, 1.0)) - 1.0) * 100.0 AS drawdown_pct
                FROM dd_series
            ),
            dd_agg AS (
                SELECT
                    run_id,
                    COALESCE(MIN(drawdown_abs), 0.0) AS max_dd_abs,
                    COALESCE(ABS(MIN(drawdown_pct)), 0.0) AS max_dd_pct_raw
                FROM dd_curve
                GROUP BY run_id
            ),
            metric_base AS (
                SELECT
                    bwm.run_id,
                    bwm.strategy_code,
                    bwm.label,
                    bwm.run_start_date,
                    bwm.run_end_date,
                    bwm.trade_count,
                    bwm.symbol_count,
                    bwm.allocated_capital,
                    bwm.total_pnl,
                    CASE
                        WHEN GREATEST(bwm.initial_equity, 1.0) <= 0 THEN 0.0
                        ELSE (bwm.total_pnl / GREATEST(bwm.initial_equity, 1.0)) * 100.0
                    END AS total_return_pct_raw,
                    bwm.win_rate,
                    bwm.profit_factor,
                    COALESCE(da.max_dd_abs, 0.0) AS max_dd_abs,
                    COALESCE(da.max_dd_pct_raw, 0.0) AS max_dd_pct_raw,
                    CASE
                        WHEN (bwm.initial_equity + bwm.total_pnl) <= 0
                        THEN -100.0
                        ELSE (
                            POWER(
                                GREATEST(
                                    (bwm.initial_equity + bwm.total_pnl)
                                    / GREATEST(bwm.initial_equity, 1.0),
                                    1e-12
                                ),
                                1.0 / GREATEST(
                                    (
                                        DATE_DIFF('day', bwm.run_start_date, bwm.run_end_date) + 1
                                    ) / 365.25,
                                    1.0 / 365.25
                                )
                            ) - 1.0
                        ) * 100.0
                    END AS annual_return_pct_raw
                FROM base_with_meta bwm
                LEFT JOIN dd_agg da ON bwm.run_id = da.run_id
            )
            SELECT
                run_id,
                strategy_code AS strategy,
                strategy_code,
                label,
                run_start_date AS start_date,
                run_end_date AS end_date,
                trade_count,
                symbol_count,
                ROUND(allocated_capital, 2) AS allocated_capital,
                total_pnl,
                ROUND(total_return_pct_raw, 2) AS total_return_pct,
                win_rate,
                profit_factor,
                max_dd_abs,
                ROUND(max_dd_pct_raw, 4) AS max_dd_pct,
                ROUND(annual_return_pct_raw, 2) AS annual_return_pct,
                CASE WHEN max_dd_pct_raw > 0
                     THEN ROUND(annual_return_pct_raw / max_dd_pct_raw, 2)
                     WHEN annual_return_pct_raw > 0
                     THEN 99.9
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
        result = int(row[0]) if row and row[0] is not None else 0
        self._publish_replica(force=True)
        return result

    def _migrate_backtest_results_table(self) -> None:
        """Recreate backtest_results table with refreshed CHECK constraint and copy rows."""
        temp_table = "_backtest_results_upgrade"
        self.con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        self.con.execute(f"""
            CREATE TABLE {temp_table} (
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
                profit_loss_pct   DOUBLE,
                exit_reason      VARCHAR CHECK (exit_reason IN (
                    'TARGET', 'INITIAL_SL', 'BREAKEVEN_SL',
                    'TRAILING_SL', 'TIME', 'REVERSAL', 'CANDLE_EXIT'
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
        source_columns = [
            "run_id",
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
            "gross_pnl",
            "total_costs",
            "reached_1r",
            "reached_2r",
            "max_r",
        ]
        select_exprs = [
            "run_id",
            "session_id"
            if self._table_has_column("backtest_results", "session_id")
            else "NULL::VARCHAR AS session_id",
            "source_session_id"
            if self._table_has_column("backtest_results", "source_session_id")
            else "NULL::VARCHAR AS source_session_id",
            (
                "COALESCE(execution_mode, 'BACKTEST') AS execution_mode"
                if self._table_has_column("backtest_results", "execution_mode")
                else "'BACKTEST'::VARCHAR AS execution_mode"
            ),
            "symbol",
            "trade_date",
            "direction",
            "entry_time",
            "exit_time",
            (
                "entry_timestamp"
                if self._table_has_column("backtest_results", "entry_timestamp")
                else "NULL::TIMESTAMP AS entry_timestamp"
            ),
            (
                "exit_timestamp"
                if self._table_has_column("backtest_results", "exit_timestamp")
                else "NULL::TIMESTAMP AS exit_timestamp"
            ),
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
            (
                "position_size"
                if self._table_has_column("backtest_results", "position_size")
                else "NULL::INTEGER AS position_size"
            ),
            (
                "position_value"
                if self._table_has_column("backtest_results", "position_value")
                else "NULL::DOUBLE AS position_value"
            ),
            "mfe_r",
            "mae_r",
            "or_atr_ratio",
            "gap_pct",
            (
                "gross_pnl"
                if self._table_has_column("backtest_results", "gross_pnl")
                else "NULL::DOUBLE AS gross_pnl"
            ),
            (
                "total_costs"
                if self._table_has_column("backtest_results", "total_costs")
                else "NULL::DOUBLE AS total_costs"
            ),
            (
                "reached_1r"
                if self._table_has_column("backtest_results", "reached_1r")
                else "NULL::BOOLEAN AS reached_1r"
            ),
            (
                "reached_2r"
                if self._table_has_column("backtest_results", "reached_2r")
                else "NULL::BOOLEAN AS reached_2r"
            ),
            "max_r"
            if self._table_has_column("backtest_results", "max_r")
            else "NULL::FLOAT AS max_r",
        ]
        self.con.execute(f"""
            INSERT INTO {temp_table} (
                {", ".join(source_columns)}
            )
            SELECT
                {", ".join(select_exprs)}
            FROM backtest_results
        """)
        self.con.execute("DROP TABLE backtest_results")
        self.con.execute(f"ALTER TABLE {temp_table} RENAME TO backtest_results")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol ON backtest_results(run_id, symbol)"
        )
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_br_run_symbol_date ON backtest_results(run_id, symbol, trade_date)"
        )

    def store_backtest_results(
        self,
        results_df: pl.DataFrame,
        execution_mode: str | None = None,
        transactional: bool = True,
    ) -> int:
        """
        Store trade-level results in DuckDB. Creates table if missing.

        Append-only: inserts new rows. run_id is unique per execution,
        so there should never be existing rows for the same run_id.

        Stores execution-level audit fields used by portfolio-aware reporting.
        """
        if results_df.is_empty():
            return 0
        self.ensure_backtest_table()
        self.ensure_run_daily_pnl_table()
        self.ensure_run_metrics_table()
        self._begin_replica_batch()
        success = False
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

        # Only select columns that exist in the DataFrame (handles old results)
        available = [c for c in columns if c in working_df.columns]
        self.con.register("_tmp_br", working_df.select(available).to_arrow())
        try:
            insert_sql = (
                f"INSERT INTO backtest_results ({', '.join(available)}) SELECT * FROM _tmp_br"
            )
            try:
                if transactional:
                    self.con.execute("BEGIN TRANSACTION")
                self.con.execute(insert_sql)
            except Exception as e:
                err_msg = str(e)
                exit_reasons = (
                    {str(v).upper() for v in working_df["exit_reason"].to_list()}
                    if "exit_reason" in available
                    else set()
                )
                has_new_exit_reason = bool(exit_reasons & {"CANDLE_EXIT"})
                low_msg = err_msg.lower()
                if (
                    "check constraint" in low_msg
                    and "exit_reason" in low_msg
                    and has_new_exit_reason
                ):
                    try:
                        self.con.execute("ROLLBACK")
                    except Exception as rollback_err:
                        logger.debug(
                            "Rollback failed after exit_reason constraint error (pre-migration): %s",
                            rollback_err,
                        )
                    logger.warning(
                        "Backtest results CHECK constraint is out of date; migrating table to allow new exit reasons"
                    )
                    self._migrate_backtest_results_table()
                    if transactional:
                        self.con.execute("BEGIN TRANSACTION")
                    self.con.execute(insert_sql)
                else:
                    if transactional:
                        try:
                            self.con.execute("ROLLBACK")
                        except Exception as rollback_err:
                            logger.debug(
                                "Rollback failed after backtest_results insert error: %s",
                                rollback_err,
                            )
                    raise
            run_id_val: str | None = None
            if "run_id" in working_df.columns:
                run_id_val = str(working_df["run_id"][0])
            if run_id_val:
                self.refresh_run_metrics([run_id_val])
            if transactional:
                self.con.execute("COMMIT")
            success = True
        except Exception as e:
            if transactional:
                try:
                    self.con.execute("ROLLBACK")
                except Exception as rollback_err:
                    logger.debug(
                        "Rollback failed after store_backtest_results outer exception: %s",
                        rollback_err,
                    )
            logger.exception("Failed to store backtest_results payload: %s", e)
            raise
        finally:
            self.con.unregister("_tmp_br")
            self._end_replica_batch()
            if success:
                self._publish_replica(force=True)
        return results_df.height

    def get_backtest_trades(
        self,
        run_id: str,
        symbols: list[str] | None = None,
        execution_mode: str | None = "BACKTEST",
    ) -> pl.DataFrame:
        """Load trade-level results from DB for a given run_id (used for cache loading)."""
        try:
            self.ensure_backtest_table()
        except Exception as e:
            logger.debug(
                "Failed to ensure backtest_results before loading run_id=%s: %s", run_id, e
            )
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
            logger.debug("Failed to ensure backtest_results before summary query: %s", e)
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
                "0 AS target_exits, "
                "0 AS sl_exits, "
                "0 AS initial_sl, "
                "0 AS breakeven_sl, "
                "0 AS trailing_sl, "
                "0 AS time_exits"
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

    def get_runs_with_metrics(
        self,
        limit: int | None = None,
        execution_mode: str | None = "BACKTEST",
    ) -> list[dict]:
        """Get run-level performance metrics from materialized run_metrics.

        Args:
            limit:
                Optional max rows (most recent first). None/<=0 returns full history.
        """
        if not self.read_only:
            # Write connection: ensure tables exist and refresh if empty
            try:
                self.ensure_backtest_table()
                self.ensure_run_metadata_table()
                self.ensure_run_metrics_table()
            except Exception as e:
                logger.exception("Failed to ensure run metric tables before read: %s", e)
                return []
            try:
                row = self.con.execute("SELECT COUNT(*) FROM run_metrics").fetchone()
                if int(row[0] or 0) == 0:
                    self.refresh_run_metrics()
            except Exception as e:
                logger.debug("run_metrics pre-refresh probe failed; proceeding with select: %s", e)

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
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.rvol_threshold') AS DOUBLE), "
                    "1.0"
                    ") AS rvol_threshold"
                )
                cpr_min_close_atr_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_levels.cpr_min_close_atr') AS DOUBLE), "
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_levels_config.cpr_min_close_atr') AS DOUBLE), "
                    "TRY_CAST(json_extract(rm.params_json, '$.cpr_min_close_atr') AS DOUBLE), "
                    "0.0"
                    ") AS cpr_min_close_atr"
                )
                failure_window_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.fbr_config.failure_window') AS INTEGER), "
                    "TRY_CAST(json_extract(rm.params_json, '$.fbr.failure_window') AS INTEGER), "
                    "TRY_CAST(json_extract(rm.params_json, '$.failure_window') AS INTEGER), "
                    "0"
                    ") AS failure_window"
                )
                skip_rvol_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.skip_rvol_check') AS BOOLEAN), "
                    "TRY_CAST(json_extract(rm.params_json, '$.skip_rvol') AS BOOLEAN), "
                    "FALSE"
                    ") AS skip_rvol_check"
                )
                risk_based_sizing_sql = (
                    "COALESCE("
                    "TRY_CAST(json_extract(rm.params_json, '$.risk_based_sizing') AS BOOLEAN), "
                    "TRY_CAST(json_extract(rm.params_json, '$.legacy_sizing') AS BOOLEAN), "
                    "FALSE"
                    ") AS risk_based_sizing"
                )
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
                rvol_sql = "1.0 AS rvol_threshold"
                cpr_min_close_atr_sql = "0.0 AS cpr_min_close_atr"
                failure_window_sql = "0 AS failure_window"
                skip_rvol_sql = "FALSE AS skip_rvol_check"
                updated_at_sql = "COALESCE(r.updated_at, rm.created_at)::VARCHAR AS updated_at"
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
                    rm.params_json,
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

        return [
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
                "params_json": str(r[23] or ""),
                "updated_at": str(r[24] or ""),
                "run_span_days": int(r[25] or 0),
                "fbr_setup_filter": str(r[26] or "BOTH").upper(),
            }
            for r in rows
        ]
