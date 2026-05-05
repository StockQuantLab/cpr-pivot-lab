"""
DuckDB connection and market data layer.

DuckDB handles ALL market data analytics:
  - 5-min OHLCV candles  → queried directly from Parquet (zero import)
  - Daily OHLCV          → queried directly from Parquet
  - CPR levels           → pre-computed materialized table (built once)
  - ATR values           → pre-computed materialized table (built once)
  - Backtest results     → stored in backtest.duckdb (db/backtest_db.py)

PostgreSQL handles ONLY: agent_sessions, signals.
alert_log lives in paper.duckdb (db/paper_db.py).

Data layout expected:
    data/parquet/5min/RELIANCE/2015.parquet
    data/parquet/5min/RELIANCE/2016.parquet  ...
    data/parquet/daily/RELIANCE/all.parquet
    data/parquet/daily/RELIANCE/kite.parquet  (incremental overlay)

Run `uv run pivot-convert` first to generate Parquet files from raw CSVs.

Usage:
    from db.duckdb import get_db
    db = get_db()
    df = db.query_5min("RELIANCE", "2023-01-01", "2023-12-31")
    cpr = db.get_cpr("RELIANCE", "2023-06-15")
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
from pathlib import Path

import duckdb
import polars as pl

from db.duckdb_backtest_results import DuckDBBacktestResultsMixin
from db.duckdb_data_quality import MarketDataQualityMixin
from db.duckdb_indicator_builders import DuckDBIndicatorBuilderMixin
from db.duckdb_lock import (
    acquire_write_lock as _acquire_write_lock,
)
from db.duckdb_lock import (
    release_write_lock as _release_write_lock,
)
from db.duckdb_runtime_builders import DuckDBRuntimeBuilderMixin
from db.duckdb_validation import (
    validate_symbols as _validate_symbols,
)
from db.duckdb_validation import (
    validate_universe_name as _validate_universe_name,
)
from db.replica import ReplicaSync
from db.replica_consumer import ReplicaConsumer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DUCKDB_FILE = DATA_DIR / "market.duckdb"
REPLICA_DIR = DATA_DIR / "market_replica"


class MarketDB(
    DuckDBRuntimeBuilderMixin,
    DuckDBIndicatorBuilderMixin,
    DuckDBBacktestResultsMixin,
    MarketDataQualityMixin,
):
    """
    Central DuckDB access point for all market data.

    Single file-based connection (market.duckdb).
    Registers Parquet views on init; materializes CPR/ATR tables on demand.
    """

    def __init__(
        self,
        db_path: Path = DUCKDB_FILE,
        read_only: bool = False,
        replica_sync: ReplicaSync | None = None,
    ):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.read_only = read_only
        self._sync = replica_sync
        self._replica_batch_depth = 0
        self._lock_path = db_path.parent / (db_path.name + ".writelock")
        if not read_only:
            _acquire_write_lock(self._lock_path)
        self.con = duckdb.connect(str(db_path), read_only=read_only)
        self._parquet_dir = PARQUET_DIR
        self._has_5min = False
        self._has_daily = False
        self._metadata_cache_ttl_sec = max(0.0, float(os.getenv("CPR_DB_META_CACHE_TTL_SEC", "60")))
        self._available_symbols_cache: list[str] | None = None
        self._available_symbols_cache_time: float = 0.0
        self._all_date_ranges_cache: dict[str, dict[str, str]] | None = None
        self._all_date_ranges_cache_time: float = 0.0
        self._table_exists_cache: set[str] = set()
        self._table_has_column_cache: set[tuple[str, str]] = set()
        # Performance tuning (only for write connections)
        if not read_only:
            self._configure_performance()
        self._setup()

    def _configure_performance(self) -> None:
        """
        Apply DuckDB performance settings for optimal backtest performance.

        Note: Some PRAGMA settings may not be available in all DuckDB versions.
        Silently ignores unsupported settings.
        """
        default_threads = max(4, int((os.cpu_count() or 4) * 0.8))
        thread_count = int(os.getenv("DUCKDB_THREADS", str(default_threads)))
        max_memory = os.getenv("DUCKDB_MAX_MEMORY", "24GB")
        max_temp_directory_size = os.getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "120GB")

        def _try_setting(name: str, statements: list[str]) -> None:
            last_error: Exception | None = None
            for stmt in statements:
                try:
                    self.con.execute(stmt)
                    return
                except Exception as e:
                    last_error = e
            if last_error is not None:
                logger.debug("DuckDB setting %s not supported: %s", name, last_error)

        _try_setting("threads", [f"PRAGMA threads={thread_count}", f"SET threads={thread_count}"])
        _try_setting(
            "max_memory", [f"PRAGMA max_memory='{max_memory}'", f"SET max_memory='{max_memory}'"]
        )
        # Python-level batch/stage logging is used instead of DuckDB ETA output.
        _try_setting(
            "enable_progress_bar",
            ["PRAGMA enable_progress_bar=false", "SET enable_progress_bar=false"],
        )
        # Keep temp spill bounded for large full-history builds.
        _try_setting(
            "max_temp_directory_size",
            [
                f"SET max_temp_directory_size='{max_temp_directory_size}'",
                f"PRAGMA max_temp_directory_size='{max_temp_directory_size}'",
            ],
        )
        # Avoid carrying insertion-order guarantees we do not rely on.
        _try_setting(
            "preserve_insertion_order",
            ["SET preserve_insertion_order=false", "PRAGMA preserve_insertion_order=false"],
        )

    def _setup(self) -> None:
        """
        Register Parquet glob views.
        Fast — only reads schema metadata, not data.
        Prints a warning if Parquet files don't exist yet.
        In read-only mode uses TEMP VIEW (session-only, no catalog write needed).
        """
        five_min_glob = str(self._parquet_dir / "5min" / "*" / "*.parquet").replace("\\", "/")
        daily_glob = str(self._parquet_dir / "daily" / "*" / "*.parquet").replace("\\", "/")

        # Short-circuit existence check — avoids walking 15K+ files on Windows (was 4-8s)
        five_min_files = next(self._parquet_dir.glob("5min/**/*.parquet"), None) is not None
        daily_files = next(self._parquet_dir.glob("daily/**/*.parquet"), None) is not None

        # Use TEMP VIEW in read-only mode (stored in session memory, not catalog)
        view_prefix = "CREATE OR REPLACE TEMP VIEW" if self.read_only else "CREATE OR REPLACE VIEW"

        if five_min_files:
            self.con.execute(f"""
                {view_prefix} v_5min AS
                SELECT * FROM read_parquet('{five_min_glob}', hive_partitioning=false)
            """)
            self._has_5min = True
        else:
            print(
                "WARNING: No 5-min Parquet files found.\n"
                "  Run: uv run pivot-convert\n"
                "  Then: uv run pivot-build --force"
            )

        if daily_files:
            self.con.execute(f"""
                {view_prefix} v_daily AS
                SELECT
                    open,
                    high,
                    low,
                    close,
                    volume,
                    date,
                    symbol
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, date
                            ORDER BY
                                CASE
                                    WHEN lower(filename) LIKE '%/kite.parquet'
                                      OR lower(filename) LIKE '%\\kite.parquet'
                                    THEN 1
                                    ELSE 0
                                END DESC,
                                filename DESC
                        ) AS _rn
                    FROM read_parquet(
                        '{daily_glob}',
                        hive_partitioning=false,
                        filename=true
                    )
                )
                WHERE _rn = 1
            """)
            self._has_daily = True
        else:
            print("WARNING: No daily Parquet files found. CPR/ATR tables cannot be built.")
        self._invalidate_metadata_caches()

    def _require_data(self, view: str = "v_5min") -> None:
        """Raise a clear error if Parquet data hasn't been converted yet."""
        if view == "v_5min" and not self._has_5min:
            raise RuntimeError("5-min Parquet data not found. Run: uv run pivot-convert")
        if view == "v_daily" and not self._has_daily:
            raise RuntimeError("Daily Parquet data not found. Run: uv run pivot-convert")

    def _invalidate_metadata_caches(self) -> None:
        """Invalidate short-lived metadata caches used by dashboard queries."""
        self._available_symbols_cache = None
        self._available_symbols_cache_time = 0.0
        self._all_date_ranges_cache = None
        self._all_date_ranges_cache_time = 0.0
        self._table_exists_cache.clear()
        self._table_has_column_cache.clear()

    def _begin_replica_batch(self) -> None:
        """Suppress replica publication until the current batch completes."""
        self._replica_batch_depth += 1

    def _end_replica_batch(self) -> None:
        """Re-enable replica publication after a batched write finishes."""
        self._replica_batch_depth = max(0, self._replica_batch_depth - 1)

    def _publish_replica(self, *, force: bool = False) -> None:
        """Publish the latest market DB state to the dashboard replica."""
        if self.read_only or self._sync is None:
            return
        self._sync.mark_dirty()
        if self._replica_batch_depth > 0:
            return
        if force:
            self._sync.force_sync(self.con)
        else:
            self._sync.maybe_sync(self.con)

    # ------------------------------------------------------------------
    def _build_dataset_meta(self) -> None:
        """Pre-compute static dataset metadata so get_status() never scans Parquet."""
        self.con.execute("DROP TABLE IF EXISTS dataset_meta")
        if self._has_daily:
            self.con.execute("""
                CREATE TABLE dataset_meta AS
                SELECT
                    COUNT(DISTINCT symbol) AS symbol_count,
                    MIN(date)::VARCHAR AS min_date,
                    MAX(date)::VARCHAR AS max_date
                FROM v_daily
            """)
        self._publish_replica(force=True)

    def ensure_universe_table(self) -> None:
        """Create saved universe table used by bronze/silver/gold workflow."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS backtest_universe (
                universe_name VARCHAR PRIMARY KEY,
                symbols_json VARCHAR NOT NULL,
                symbol_count INTEGER NOT NULL,
                start_date DATE,
                end_date DATE,
                source VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            )
        """)

    def upsert_universe(
        self,
        name: str,
        symbols: list[str],
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        source: str = "manual",
        notes: str = "",
    ) -> int:
        """Insert or update a named backtest universe."""
        universe_name = _validate_universe_name((name or "").strip())
        target = sorted(set(_validate_symbols(symbols)))
        if not target:
            return 0

        self.ensure_universe_table()
        symbols_json = json.dumps(target)
        symbol_count = len(target)
        self.con.execute(
            """
            INSERT INTO backtest_universe (
                universe_name,
                symbols_json,
                symbol_count,
                start_date,
                end_date,
                source,
                notes,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())
            ON CONFLICT (universe_name)
            DO UPDATE SET
                symbols_json = excluded.symbols_json,
                symbol_count = excluded.symbol_count,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                source = excluded.source,
                notes = excluded.notes,
                updated_at = now()
            """,
            [
                universe_name,
                symbols_json,
                symbol_count,
                start_date,
                end_date,
                source,
                notes,
            ],
        )
        self._publish_replica(force=True)
        return symbol_count

    def get_universe_symbols(self, name: str) -> list[str]:
        """Load symbol list for a saved universe name."""
        universe_name = _validate_universe_name((name or "").strip())
        if not self.read_only:
            self.ensure_universe_table()
        row = self.con.execute(
            "SELECT symbols_json FROM backtest_universe WHERE universe_name = ?",
            [universe_name],
        ).fetchone()
        if not row or not row[0]:
            return []
        try:
            raw = json.loads(row[0])
        except Exception as e:
            logger.warning("Invalid symbols_json for universe '%s': %s", universe_name, e)
            return []
        if not isinstance(raw, list):
            return []
        symbols = [str(s).upper() for s in raw]
        return sorted(set(_validate_symbols(symbols)))

    def list_universes(self) -> list[dict[str, object]]:
        """List saved universe metadata rows."""
        if not self.read_only:
            self.ensure_universe_table()
        rows = self.con.execute(
            """
            SELECT
                universe_name,
                symbol_count,
                start_date::VARCHAR,
                end_date::VARCHAR,
                source,
                notes,
                created_at::VARCHAR,
                updated_at::VARCHAR
            FROM backtest_universe
            ORDER BY updated_at DESC, universe_name
            """
        ).fetchall()
        return [
            {
                "name": r[0],
                "symbol_count": int(r[1]) if r[1] is not None else 0,
                "start_date": r[2],
                "end_date": r[3],
                "source": r[4],
                "notes": r[5],
                "created_at": r[6],
                "updated_at": r[7],
            }
            for r in rows
        ]

    def delete_universes(self, names: list[str]) -> int:
        """Delete named universe snapshots from backtest_universe."""
        self.ensure_universe_table()
        target = sorted(
            {
                _validate_universe_name((name or "").strip())
                for name in names
                if str(name or "").strip()
            }
        )
        if not target:
            return 0
        placeholders = ", ".join("?" for _ in target)
        count_row = self.con.execute(
            f"SELECT COUNT(*) FROM backtest_universe WHERE universe_name IN ({placeholders})",
            target,
        ).fetchone()
        deleted = int(count_row[0]) if count_row and count_row[0] is not None else 0
        self.con.execute(
            f"DELETE FROM backtest_universe WHERE universe_name IN ({placeholders})",
            target,
        )
        self._publish_replica(force=True)
        return deleted

    # ------------------------------------------------------------------
    # Query API — ALL use parameterized queries (no SQL injection risk)
    # ------------------------------------------------------------------

    def query_5min(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Fetch 5-min candles for a symbol over a date range."""
        self._require_data("v_5min")
        cols = ", ".join(columns) if columns else "*"
        return self.con.execute(
            f"SELECT {cols} FROM v_5min WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY candle_time",
            [symbol, start_date, end_date],
        ).pl()

    def get_trading_days(self, symbol: str, start_date: str, end_date: str) -> list[str]:
        """Return sorted list of trading dates (ISO strings) for a symbol."""
        self._require_data("v_5min")
        rows = self.con.execute(
            "SELECT DISTINCT date::VARCHAR FROM v_5min WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
            [symbol, start_date, end_date],
        ).fetchall()
        return [r[0] for r in rows]

    def get_cpr(self, symbol: str, trade_date: str) -> dict | None:
        """Get CPR levels + floor pivots for a specific trading day."""
        row = self.con.execute(
            'SELECT "pivot", tc, bc, cpr_width_pct, prev_high, prev_low, prev_close, '
            "r1, s1, r2, s2, r3, s3, cpr_shift, is_narrowing "
            "FROM cpr_daily WHERE symbol = ? AND trade_date = ?",
            [symbol, trade_date],
        ).fetchone()
        if not row:
            return None
        return {
            "pivot": row[0],
            "tc": row[1],
            "bc": row[2],
            "cpr_width_pct": row[3],
            "prev_high": row[4],
            "prev_low": row[5],
            "prev_close": row[6],
            "r1": row[7],
            "s1": row[8],
            "r2": row[9],
            "s2": row[10],
            "r3": row[11],
            "s3": row[12],
            "cpr_shift": row[13],
            "is_narrowing": row[14],
        }

    def get_atr(self, symbol: str, trade_date: str) -> float | None:
        """Get intraday ATR for a specific trading day."""
        row = self.con.execute(
            "SELECT atr FROM atr_intraday WHERE symbol = ? AND trade_date = ?",
            [symbol, trade_date],
        ).fetchone()
        return float(row[0]) if row and row[0] else None

    def get_cpr_threshold(self, symbol: str, trade_date: str) -> float | None:
        """Get rolling CPR width threshold (P50) for a symbol on a date."""
        row = self.con.execute(
            "SELECT cpr_threshold_pct FROM cpr_thresholds WHERE symbol = ? AND trade_date = ?",
            [symbol, trade_date],
        ).fetchone()
        return float(row[0]) if row and row[0] else None

    def get_day_candles(self, symbol: str, trade_date: str) -> pl.DataFrame:
        """All 5-min candles for a single trading day, sorted by time."""
        self._require_data("v_5min")
        return self.con.execute(
            "SELECT candle_time, open, high, low, close, volume "
            "FROM v_5min WHERE symbol = ? AND date = ? ORDER BY candle_time",
            [symbol, trade_date],
        ).pl()

    def get_trade_inspection(
        self,
        run_id: str,
        symbol: str,
        trade_date: str,
        entry_time: str,
        exit_time: str,
    ) -> dict | None:
        """Return daily CPR context and key candles for a saved trade."""
        entry_hhmm = str(entry_time or "")[:5]
        exit_hhmm = str(exit_time or "")[:5]
        trade_row = self.con.execute(
            """
            SELECT
                run_id,
                symbol,
                trade_date::VARCHAR,
                direction,
                entry_time,
                exit_time,
                entry_price,
                exit_price,
                sl_price,
                target_price,
                profit_loss,
                profit_loss_pct,
                exit_reason,
                atr,
                position_size,
                position_value
            FROM backtest_results
            WHERE run_id = ?
              AND symbol = ?
              AND trade_date = ?
              AND (
                    (entry_time = ? AND exit_time = ?)
                 OR (SUBSTR(entry_time, 1, 5) = ? AND SUBSTR(exit_time, 1, 5) = ?)
              )
            ORDER BY
                CASE
                    WHEN entry_time = ? AND exit_time = ? THEN 0
                    ELSE 1
                END,
                entry_time,
                exit_time
            LIMIT 1
            """,
            [
                run_id,
                symbol,
                trade_date,
                str(entry_time or ""),
                str(exit_time or ""),
                entry_hhmm,
                exit_hhmm,
                str(entry_time or ""),
                str(exit_time or ""),
            ],
        ).fetchone()
        if not trade_row:
            fallback_rows = self.con.execute(
                """
                SELECT
                    run_id,
                    symbol,
                    trade_date::VARCHAR,
                    direction,
                    entry_time,
                    exit_time,
                    entry_price,
                    exit_price,
                    sl_price,
                    target_price,
                    profit_loss,
                    profit_loss_pct,
                    exit_reason,
                    atr,
                    position_size,
                    position_value
                FROM backtest_results
                WHERE run_id = ?
                  AND symbol = ?
                  AND trade_date = ?
                ORDER BY entry_time, exit_time
                LIMIT 2
                """,
                [run_id, symbol, trade_date],
            ).fetchall()
            if len(fallback_rows) == 1:
                trade_row = fallback_rows[0]
        if not trade_row:
            return None

        cpr_row = self.con.execute(
            """
            SELECT
                m.prev_date::VARCHAR,
                c.prev_high,
                c.prev_low,
                m.prev_close,
                m.pivot,
                m.bc,
                m.tc,
                m.r1,
                m.s1,
                m.atr,
                m.cpr_width_pct,
                m.cpr_shift,
                m.is_narrowing,
                m.cpr_threshold_pct,
                m.gap_pct_open,
                m.open_915,
                m.or_close_5,
                s.open_side,
                s.open_to_cpr_atr,
                s.gap_abs_pct,
                s.or_atr_5,
                s.direction_5
            FROM market_day_state m
            LEFT JOIN cpr_daily c
              ON c.symbol = m.symbol
             AND c.trade_date = m.trade_date
            LEFT JOIN strategy_day_state s
              ON s.symbol = m.symbol
             AND s.trade_date = m.trade_date
            WHERE m.symbol = ? AND m.trade_date = ?
            LIMIT 1
            """,
            [symbol, trade_date],
        ).fetchone()

        params_json = self.con.execute(
            "SELECT params_json FROM run_metadata WHERE run_id = ? LIMIT 1",
            [run_id],
        ).fetchone()
        params: dict[str, object] = {}
        if params_json and params_json[0]:
            try:
                params = json.loads(str(params_json[0]))
            except json.JSONDecodeError:
                params = {}

        signal_times = {"09:15", str(trade_row[4])[:5], str(trade_row[5])[:5]}
        candles = self.get_day_candles(symbol, trade_date)
        candle_map: dict[str, dict[str, float | str]] = {}
        if not candles.is_empty():
            for row in candles.iter_rows(named=True):
                candle_time = row["candle_time"]
                if hasattr(candle_time, "strftime"):
                    hhmm = candle_time.strftime("%H:%M")
                else:
                    hhmm = str(candle_time)[11:16]
                if hhmm not in signal_times or hhmm in candle_map:
                    continue
                candle_map[hhmm] = {
                    "time": hhmm,
                    "open": float(row.get("open") or 0.0),
                    "high": float(row.get("high") or 0.0),
                    "low": float(row.get("low") or 0.0),
                    "close": float(row.get("close") or 0.0),
                    "volume": float(row.get("volume") or 0.0),
                }

        cpr_levels: dict[str, object] = {}
        derived: dict[str, object] = {}
        if cpr_row:
            cpr_levels = {
                "prev_date": cpr_row[0],
                "prev_high": float(cpr_row[1] or 0.0),
                "prev_low": float(cpr_row[2] or 0.0),
                "prev_close": float(cpr_row[3] or 0.0),
                "pivot": float(cpr_row[4] or 0.0),
                "bc": float(cpr_row[5] or 0.0),
                "tc": float(cpr_row[6] or 0.0),
                "r1": float(cpr_row[7] or 0.0),
                "s1": float(cpr_row[8] or 0.0),
                "atr": float(cpr_row[9] or 0.0),
                "cpr_width_pct": float(cpr_row[10] or 0.0),
                "cpr_shift": str(cpr_row[11] or ""),
                "is_narrowing": int(cpr_row[12] or 0),
                "cpr_threshold_pct": float(cpr_row[13] or 0.0),
                "gap_pct_open": float(cpr_row[14] or 0.0),
                "open_915": float(cpr_row[15] or 0.0),
                "or_close_5": float(cpr_row[16] or 0.0),
                "open_side": str(cpr_row[17] or ""),
                "open_to_cpr_atr": float(cpr_row[18] or 0.0),
                "gap_abs_pct": float(cpr_row[19] or 0.0),
                "or_atr_5": float(cpr_row[20] or 0.0),
                "direction_5": str(cpr_row[21] or ""),
            }

            direction = str(trade_row[3] or "")
            buffer_pct = float(params.get("buffer_pct") or 0.0005)
            cpr_cfg = params.get("cpr_levels")
            cpr_min_close_atr = 0.0
            if isinstance(cpr_cfg, dict):
                cpr_min_close_atr = float(cpr_cfg.get("cpr_min_close_atr") or 0.0)
            atr = float(cpr_levels["atr"] or 0.0)
            bc = float(cpr_levels["bc"] or 0.0)
            tc = float(cpr_levels["tc"] or 0.0)
            cpr_lower = min(tc, bc)
            cpr_upper = max(tc, bc)
            if direction == "LONG":
                trigger_price = cpr_upper * (1.0 + buffer_pct)
                min_signal_close = max(trigger_price, cpr_upper + cpr_min_close_atr * atr)
                setup_rule = "09:15 close above the upper CPR boundary, then first close from 09:20 onward above long threshold."
                target_label = "R1"
            else:
                trigger_price = cpr_lower * (1.0 - buffer_pct)
                min_signal_close = min(trigger_price, cpr_lower - cpr_min_close_atr * atr)
                setup_rule = "09:15 close below the lower CPR boundary, then first close from 09:20 onward below short threshold."
                target_label = "S1"
            derived = {
                "buffer_pct": buffer_pct,
                "cpr_min_close_atr": cpr_min_close_atr,
                "trigger_price": trigger_price,
                "min_signal_close": min_signal_close,
                "setup_rule": setup_rule,
                "target_label": target_label,
                "signal_time": "09:15",
                "entry_scan_start": "09:20",
            }

        return {
            "trade": {
                "run_id": trade_row[0],
                "symbol": trade_row[1],
                "trade_date": trade_row[2],
                "direction": trade_row[3],
                "entry_time": trade_row[4],
                "exit_time": trade_row[5],
                "entry_price": float(trade_row[6] or 0.0),
                "exit_price": float(trade_row[7] or 0.0),
                "sl_price": float(trade_row[8] or 0.0),
                "target_price": float(trade_row[9] or 0.0),
                "profit_loss": float(trade_row[10] or 0.0),
                "profit_loss_pct": float(trade_row[11] or 0.0),
                "exit_reason": str(trade_row[12] or ""),
                "atr": float(trade_row[13] or 0.0),
                "position_size": int(trade_row[14] or 0),
                "position_value": float(trade_row[15] or 0.0),
            },
            "params": params,
            "daily_cpr": cpr_levels,
            "derived": derived,
            "candles": candle_map,
        }

    def get_liquid_symbols(
        self,
        start_date: str,
        end_date: str,
        *,
        limit: int = 51,
        min_price: float = 0.0,
    ) -> list[str]:
        """Get top symbols by average daily traded value over a date range.

        Uses cpr_daily (materialized, has prev_close/prev_volume) when available
        and falls back to v_daily for legacy builds.
        """
        if not self._has_daily:
            return []
        lim = max(0, int(limit))
        limit_sql = f"LIMIT {lim}" if lim > 0 else ""
        self.ensure_data_quality_table()

        # Use cpr_daily turnover columns when available; fallback to v_daily.
        # This keeps prepare resilient even before cpr_daily is rebuilt with prev_volume.
        use_cpr_daily = self._table_exists("cpr_daily") and self._table_has_column(
            "cpr_daily", "prev_volume"
        )

        if use_cpr_daily:
            rows = self.con.execute(
                f"""
                SELECT symbol
                FROM (
                    SELECT
                        d.symbol AS symbol,
                        AVG(d.prev_close * d.prev_volume) AS avg_turnover,
                        COUNT(*) AS bars
                    FROM cpr_daily d
                    WHERE d.trade_date >= ?::DATE
                      AND d.trade_date <= ?::DATE
                      AND d.prev_close IS NOT NULL
                      AND d.prev_volume IS NOT NULL
                      AND d.prev_volume > 0
                      AND (? <= 0 OR d.prev_close >= ?)
                      AND d.symbol NOT IN (
                          SELECT symbol
                          FROM data_quality_issues
                          WHERE issue_code = 'MISSING_5MIN_PARQUET'
                            AND is_active = TRUE
                      )
                    GROUP BY d.symbol
                ) ranked
                ORDER BY avg_turnover DESC NULLS LAST, bars DESC, symbol
                {limit_sql}
                """,
                [start_date, end_date, min_price, min_price],
            ).fetchall()
            return [r[0] for r in rows if r and r[0]]

        # Fallback path for legacy cpr_daily without prev_volume.
        rows = self.con.execute(
            f"""
            SELECT symbol
            FROM (
                SELECT
                    d.symbol AS symbol,
                    AVG(d.close * d.volume) AS avg_turnover,
                    COUNT(*) AS bars
                FROM v_daily d
                WHERE d.date >= ?::DATE
                  AND d.date <= ?::DATE
                  AND d.close IS NOT NULL
                  AND d.volume IS NOT NULL
                  AND d.volume > 0
                  AND (? <= 0 OR d.close >= ?)
                  AND d.symbol NOT IN (
                      SELECT symbol
                      FROM data_quality_issues
                      WHERE issue_code = 'MISSING_5MIN_PARQUET'
                        AND is_active = TRUE
                  )
                GROUP BY d.symbol
            ) ranked
            ORDER BY avg_turnover DESC NULLS LAST, bars DESC, symbol
            {limit_sql}
            """,
            [start_date, end_date, min_price, min_price],
        ).fetchall()
        return [r[0] for r in rows if r and r[0]]

    def get_available_symbols(self, force_refresh: bool = False) -> list[str]:
        """List symbols available for backtesting."""
        if not self._has_daily:
            return []
        now = time.monotonic()
        if (
            not force_refresh
            and self._available_symbols_cache is not None
            and (now - self._available_symbols_cache_time) < self._metadata_cache_ttl_sec
        ):
            return list(self._available_symbols_cache)

        self.ensure_data_quality_table()
        try:
            source_table = "cpr_daily" if self._table_exists("cpr_daily") else "v_daily"
            date_column = "trade_date" if source_table == "cpr_daily" else "date"
            rows = self.con.execute(
                f"""
                SELECT DISTINCT symbol
                FROM {source_table}
                WHERE {date_column} IS NOT NULL
                  AND symbol IS NOT NULL
                  AND symbol NOT IN (
                    SELECT symbol
                    FROM data_quality_issues
                    WHERE issue_code = 'MISSING_5MIN_PARQUET'
                      AND is_active = TRUE
                )
                ORDER BY symbol
                """
            ).fetchall()
            symbols = [r[0] for r in rows if r and r[0]]
            self._available_symbols_cache = symbols
            self._available_symbols_cache_time = now
            return list(symbols)
        except Exception as e:
            logger.exception(
                "Failed to fetch available symbols from v_daily/data_quality_issues: %s", e
            )
            return list(self._available_symbols_cache or [])

    def get_date_range(self, symbol: str) -> tuple[str, str] | None:
        """Min and max dates for a symbol in the daily dataset."""
        if not self._has_daily:
            return None
        row = self.con.execute(
            "SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM v_daily WHERE symbol = ?",
            [symbol],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return (row[0], row[1])

    def get_all_date_ranges(self, force_refresh: bool = False) -> dict[str, dict[str, str]]:
        """All symbols with their min/max dates in a single query (avoids N+1)."""
        if not self._has_daily:
            return {}
        now = time.monotonic()
        if (
            not force_refresh
            and self._all_date_ranges_cache is not None
            and (now - self._all_date_ranges_cache_time) < self._metadata_cache_ttl_sec
        ):
            return dict(self._all_date_ranges_cache)
        try:
            rows = self.con.execute(
                "SELECT symbol, MIN(date)::VARCHAR, MAX(date)::VARCHAR "
                "FROM v_daily GROUP BY symbol ORDER BY symbol"
            ).fetchall()
            ranges = {r[0]: {"start": r[1], "end": r[2]} for r in rows}
            self._all_date_ranges_cache = ranges
            self._all_date_ranges_cache_time = now
            return dict(ranges)
        except Exception as e:
            logger.exception("Failed to fetch all date ranges from v_daily: %s", e)
            return dict(self._all_date_ranges_cache or {})

    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        """System status: what tables exist and how much data is loaded."""
        table_names = [
            "cpr_daily",
            "atr_intraday",
            "cpr_thresholds",
            "virgin_cpr_flags",
            "or_daily",
            "market_day_state",
            "strategy_day_state",
            "intraday_day_pack",
            "dataset_meta",
            "data_quality_issues",
            "backtest_universe",
        ]
        tables = dict.fromkeys(table_names, 0)
        existing_tables = [table for table in table_names if self._table_exists(table)]
        if existing_tables:
            union_sql = "\nUNION ALL\n".join(
                f"SELECT '{self._escape_sql_literal(table)}' AS table_name, COUNT(*) AS row_count FROM {table}"
                for table in existing_tables
            )
            try:
                rows = self.con.execute(union_sql).fetchall()
                for row in rows:
                    table_name = str(row[0])
                    tables[table_name] = int(row[1] or 0)
            except Exception as e:
                logger.debug("Failed to collect table counts in one pass: %s", e)
                for table in existing_tables:
                    try:
                        count_row = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        tables[table] = int(count_row[0] or 0) if count_row else 0
                    except Exception as inner:
                        logger.debug("Failed to count table %s: %s", table, inner)

        status: dict[str, object] = {
            "parquet_5min": self._has_5min,
            "parquet_daily": self._has_daily,
            "tables": tables,
        }
        logger.debug(f"Database status: tables={tables}")

        # Use pre-built dataset_meta (instant) instead of scanning Parquet
        try:
            meta_row = self.con.execute(
                "SELECT symbol_count, min_date, max_date FROM dataset_meta"
            ).fetchone()
            if meta_row:
                status["symbols"] = meta_row[0]
                status["date_range"] = f"{meta_row[1]} to {meta_row[2]}"
        except Exception as e:
            logger.debug("dataset_meta status lookup failed; falling back to v_daily: %s", e)
            # Fallback to v_daily if dataset_meta not built yet
            if self._has_daily:
                try:
                    fallback_row = self.con.execute(
                        "SELECT COUNT(DISTINCT symbol), "
                        "MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM v_daily"
                    ).fetchone()
                    if fallback_row:
                        status["symbols"] = fallback_row[0]
                        status["date_range"] = f"{fallback_row[1]} to {fallback_row[2]}"
                except Exception as fallback_err:
                    logger.debug("Fallback v_daily status lookup failed: %s", fallback_err)

        return status

    def execute_sql(
        self, query: str, params: list | dict | None = None
    ) -> duckdb.DuckDBPyConnection:
        """
        Execute a raw SQL query with optional parameters.

        Provides a consistent interface for dynamic SQL queries that cannot be
        easily expressed through the higher-level query methods. Prefer using
        the specific query methods (get_available_symbols, get_backtest_trades, etc.)
        when possible.

        Args:
            query: SQL query string (can use ? for positional or $name for named params)
            params: Optional parameters list (for ?) or dict (for $name)

        Returns:
            DuckDBPyRelation result (call .fetchall(), .pl(), .df(), etc.)

        Example:
            rows = db.execute_sql("SELECT * FROM symbols WHERE name = ?", ['SBIN']).fetchall()
            df = db.execute_sql("SELECT * FROM runs WHERE date > $date", {'date': '2020-01-01'}).pl()
        """
        if params:
            return self.con.execute(query, params)
        return self.con.execute(query)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception as e:
            logger.debug("DuckDB close ignored: %s", e)
        if not self.read_only:
            _release_write_lock(self._lock_path)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Module-level singleton (thread-safe)
# ---------------------------------------------------------------------------
_db: MarketDB | None = None
_db_lock = threading.Lock()
_db_atexit_registered = False


def get_db() -> MarketDB:
    """
    Return the global MarketDB instance (creates on first call).

    Thread-safe: uses double-checked locking pattern to prevent race conditions
    when multiple FastAPI request handlers call this concurrently.
    """
    global _db, _db_atexit_registered
    if _db is None:
        with _db_lock:
            # Double-check: another thread may have created _db while we waited
            if _db is None:
                replica_dir = REPLICA_DIR
                replica_dir.mkdir(parents=True, exist_ok=True)
                sync = ReplicaSync(DUCKDB_FILE, replica_dir, min_interval_sec=10.0)
                _db = MarketDB(replica_sync=sync)
                if not _db_atexit_registered:
                    atexit.register(close_db)
                    _db_atexit_registered = True
    return _db


def close_db() -> None:
    global _db
    if _db is not None:
        _db.close()
        _db = None


# ---------------------------------------------------------------------------
# Read-only singleton for dashboard (can coexist with write connection)
# ---------------------------------------------------------------------------
_dashboard_db: MarketDB | None = None
_dashboard_consumer: ReplicaConsumer | None = None
_dashboard_lock = threading.Lock()
_dashboard_atexit_registered = False
_live_market_db: MarketDB | None = None
_live_market_lock = threading.Lock()
_live_market_atexit_registered = False


def get_dashboard_db() -> MarketDB:
    """Return a read-only MarketDB instance for dashboard use.

    Uses the latest versioned replica so dashboard reads never contend
    with the write connection. Raises RuntimeError if no replica exists.
    """
    global _dashboard_db, _dashboard_consumer, _dashboard_atexit_registered
    REPLICA_DIR.mkdir(parents=True, exist_ok=True)
    if _dashboard_consumer is None:
        _dashboard_consumer = ReplicaConsumer(REPLICA_DIR, DUCKDB_FILE.stem)
    if _dashboard_db is None:
        with _dashboard_lock:
            if _dashboard_db is None:
                replica_path = _dashboard_consumer.get_replica_path()
                if replica_path is None:
                    raise RuntimeError(
                        f"No market replica found in {REPLICA_DIR}. "
                        "Run 'pivot-build' or 'pivot-refresh' to create one."
                    )
                _dashboard_db = MarketDB(
                    db_path=replica_path,
                    read_only=True,
                )
                if not _dashboard_atexit_registered:
                    atexit.register(close_dashboard_db)
                    _dashboard_atexit_registered = True
    else:
        replica_path = _dashboard_consumer.get_replica_path()
        if replica_path is not None and Path(_dashboard_db.db_path) != replica_path:
            with _dashboard_lock:
                if _dashboard_db is not None and Path(_dashboard_db.db_path) != replica_path:
                    _dashboard_db.close()
                    _dashboard_db = MarketDB(db_path=replica_path, read_only=True)
    return _dashboard_db


def _use_market_read_replica() -> bool:
    return str(os.getenv("PIVOT_MARKET_READ_REPLICA", "")).strip() == "1"


def get_live_market_db() -> MarketDB:
    """Return a read-only MarketDB instance pinned to the source market DB.

    Live/replay runtime setup reads should not depend on the dashboard replica
    selection contract. This accessor names that ownership explicitly while
    keeping the connection read-only.
    """
    global _live_market_db, _live_market_atexit_registered
    if _use_market_read_replica():
        return get_dashboard_db()
    if _db is not None:
        logger.info("Live market DB reusing existing source connection source=%s", _db.db_path)
        return _db
    if _live_market_db is None:
        with _live_market_lock:
            if _live_market_db is None:
                _live_market_db = MarketDB(db_path=DUCKDB_FILE, read_only=True)
                logger.info("Live market DB opened source=%s read_only=True", DUCKDB_FILE)
                if not _live_market_atexit_registered:
                    atexit.register(close_live_market_db)
                    _live_market_atexit_registered = True
    return _live_market_db


def close_dashboard_db() -> None:
    global _dashboard_db
    if _dashboard_db is not None:
        _dashboard_db.close()
        _dashboard_db = None


def close_live_market_db() -> None:
    global _live_market_db
    if _live_market_db is not None:
        _live_market_db.close()
        _live_market_db = None
