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
from db.duckdb_lock import (
    acquire_write_lock as _acquire_write_lock,
)
from db.duckdb_lock import (
    release_write_lock as _release_write_lock,
)
from db.duckdb_table_ops import (
    incremental_delete as _incremental_delete,
)
from db.duckdb_table_ops import (
    skip_if_table_fully_covered as _skip_if_table_fully_covered,
)
from db.duckdb_table_ops import (
    sql_symbol_list as _sql_symbol_list,
)
from db.duckdb_table_ops import (
    symbol_scoped_upsert as _symbol_scoped_upsert,
)
from db.duckdb_validation import (
    date_window_clause as _date_window_clause,
)
from db.duckdb_validation import (
    prepare_date_window as _prepare_date_window,
)
from db.duckdb_validation import (
    validate_symbols as _validate_symbols,
)
from db.duckdb_validation import (
    validate_table_identifier as _validate_table_identifier,
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


class MarketDB(DuckDBBacktestResultsMixin, MarketDataQualityMixin):
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
    # Materialized tables — build once, reuse forever
    # ------------------------------------------------------------------

    def build_cpr_table(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
        next_trading_date: str | None = None,
    ) -> int:
        """
        Pre-compute CPR levels + floor pivot levels for every trading day.

        Uses daily Parquet (more reliable than deriving from 5-min).
        CPR for trade_date = previous day's OHLC.

        next_trading_date: When provided (or auto-detected), used as the trade_date
        for the last available daily parquet row via COALESCE(LEAD(date), next_trading_date).
        This enables pre-market CPR computation for today when today's daily data has not
        been ingested yet (e.g. building April 15 CPR from April 13 OHLC when April 14
        was a holiday and April 15 is a live session not yet in parquet).
        Auto-detected when since_date == until_date and that date has no daily parquet rows.

        Includes:
            - Core CPR: Pivot, TC, BC, cpr_width_pct
            - Floor pivots: R1, S1, R2, S2, R3, S3
            - CPR value shift: HIGHER/LOWER/OVERLAP vs previous day
            - Narrowing flag: is_narrowing (width < previous width)

        Args:
            symbols: If provided, only upsert rows for these symbols.
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        self._require_data("v_daily")

        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)

        # Auto-detect next_trading_date: when a single target date is requested and that
        # date has no daily parquet rows (pre-market live day or holiday gap), use it as
        # the COALESCE fallback so the last available parquet date generates a CPR row.
        if next_trading_date is None and since_date_iso and since_date_iso == until_date_iso:
            has_data = self.con.execute(
                "SELECT COUNT(*) FROM v_daily WHERE date::DATE = ?::DATE",
                [since_date_iso],
            ).fetchone()[0]
            if has_data == 0:
                next_trading_date = since_date_iso
                logger.debug(
                    "build_cpr_table: no daily data for %s — using as next_trading_date "
                    "for pre-market LEAD COALESCE",
                    since_date_iso,
                )

        lead_expr = "LEAD(date) OVER (PARTITION BY symbol ORDER BY date)"
        if next_trading_date:
            lead_expr = (
                f"COALESCE(LEAD(date) OVER (PARTITION BY symbol ORDER BY date), "
                f"'{next_trading_date}'::DATE)"
            )
            print(f"  [cpr] pre-market mode: LEAD COALESCE → {next_trading_date}")

        target_symbols = sorted(set(_validate_symbols(symbols))) if symbols else None
        symbol_filter_sql = ""
        if target_symbols:
            symbol_filter_sql = f"AND symbol IN ({_sql_symbol_list(target_symbols)})"
        window_filter_sql = _date_window_clause("trade_date", since_date_iso, until_date_iso)

        # For incremental builds, push a lower-bound date filter into raw_daily so DuckDB
        # only scans Parquet rows near the target window instead of the full 10-year history.
        # LAG in with_shift only needs one prior row per symbol, so a 7-calendar-day lookback
        # (covers weekends + holidays) is sufficient: we get date=T-1 (OHLC → trade_date T
        # via LEAD) and date=T-2 (provides LAG prev_tc for trade_date T).
        # Not applied on full rebuilds (force=True / no since_date) where all history is needed.
        parquet_date_filter_sql = ""
        if since_date_iso and not force:
            parquet_date_filter_sql = (
                f"AND date::DATE >= ('{since_date_iso}'::DATE - INTERVAL '7 days')"
            )

        table_exists = self._table_exists("cpr_daily")
        insert_sql = f"""
            WITH raw_daily AS (
                SELECT
                    symbol,
                    date::DATE AS date,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, date::DATE
                        ORDER BY high DESC, low ASC, close DESC, volume DESC
                    ) AS rn
                FROM v_daily
                WHERE 1=1
                {symbol_filter_sql}
                {parquet_date_filter_sql}
            ),
            daily AS (
                SELECT
                    symbol,
                    date,
                    high,
                    low,
                    close,
                    volume
                FROM raw_daily
                WHERE rn = 1
            ),
            base AS (
                SELECT
                    symbol,
                    date,
                    high,
                    low,
                    close,
                    volume,
                    {lead_expr} AS trade_date
                FROM daily
            ),
            with_levels AS (
                SELECT
                    symbol,
                    trade_date,
                    date                                                   AS prev_date,
                    high                                                   AS prev_high,
                    low                                                    AS prev_low,
                    close                                                  AS prev_close,
                    volume                                                 AS prev_volume,
                    (high + low + close) / 3.0                             AS "pivot",
                    (high + low) / 2.0                                     AS bc,
                    2.0 * (high + low + close) / 3.0 - (high + low) / 2.0 AS tc,
                    ABS(
                        2.0 * (high + low + close) / 3.0 - (high + low) / 2.0
                        - (high + low) / 2.0
                    ) / NULLIF((high + low + close) / 3.0, 0) * 100       AS cpr_width_pct,
                    -- Floor pivot levels
                    2.0 * (high + low + close) / 3.0 - low                 AS r1,
                    2.0 * (high + low + close) / 3.0 - high                AS s1,
                    (high + low + close) / 3.0 + (high - low)              AS r2,
                    (high + low + close) / 3.0 - (high - low)              AS s2,
                    high + 2.0 * ((high + low + close) / 3.0 - low)        AS r3,
                    low - 2.0 * (high - low)                               AS s3
                FROM base
                WHERE trade_date IS NOT NULL
            ),
            with_shift AS (
                SELECT wl.*,
                    LAG(tc) OVER (PARTITION BY symbol ORDER BY trade_date)            AS prev_tc,
                    LAG(bc) OVER (PARTITION BY symbol ORDER BY trade_date)            AS prev_bc,
                    LAG(cpr_width_pct) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_width
                FROM with_levels wl
            )
            SELECT
                symbol, trade_date, prev_date, prev_high, prev_low, prev_close, prev_volume,
                "pivot", bc, tc, cpr_width_pct,
                r1, s1, r2, s2, r3, s3,
                CASE
                    WHEN prev_tc IS NULL THEN 'OVERLAP'
                    WHEN bc > prev_tc THEN 'HIGHER'
                    WHEN tc < prev_bc THEN 'LOWER'
                    ELSE 'OVERLAP'
                END AS cpr_shift,
                CASE WHEN prev_width IS NOT NULL AND cpr_width_pct < prev_width
                     THEN 1 ELSE 0 END AS is_narrowing
            FROM with_shift
            WHERE trade_date IS NOT NULL
            {window_filter_sql}
        """

        if target_symbols is not None:
            self.con.execute("BEGIN TRANSACTION")
            try:
                if table_exists:
                    delete_parts = [f"symbol IN ({_sql_symbol_list(target_symbols)})"]
                    if since_date_iso:
                        delete_parts.append(f"trade_date >= '{since_date_iso}'::DATE")
                    if until_date_iso:
                        delete_parts.append(f"trade_date <= '{until_date_iso}'::DATE")
                    self.con.execute("DELETE FROM cpr_daily WHERE " + " AND ".join(delete_parts))
                self.con.execute(
                    f"INSERT INTO cpr_daily {insert_sql}"
                    if table_exists
                    else f"CREATE TABLE cpr_daily AS {insert_sql}"
                )
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise
            n = self.con.execute("SELECT COUNT(*) FROM cpr_daily").fetchone()[0]
            scope = f"symbols={len(target_symbols)}"
            if since_date_iso:
                scope += (
                    f", window={since_date_iso}{f'..{until_date_iso}' if until_date_iso else ''}"
                )
            print(f"cpr_daily refreshed: {n:,} rows ({scope})")
            self._publish_replica(force=True)
            return n

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            if table_exists:
                self.con.execute("BEGIN TRANSACTION")
                try:
                    _incremental_delete(
                        self.con,
                        table="cpr_daily",
                        since_date=since_date_iso,
                        until_date=until_date_iso,
                        log_prefix="cpr",
                    )
                    self.con.execute(f"INSERT INTO cpr_daily {insert_sql}")
                    self.con.execute("COMMIT")
                except Exception:
                    self.con.execute("ROLLBACK")
                    raise
                n = self.con.execute("SELECT COUNT(*) FROM cpr_daily").fetchone()[0]
                window_label = since_date_iso
                if until_date_iso and until_date_iso != since_date_iso:
                    window_label = f"{since_date_iso}..{until_date_iso}"
                print(f"cpr_daily refreshed: {n:,} rows (incremental since {window_label})")
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM cpr_daily").fetchone()[0]
                if n > 0:
                    print(f"cpr_daily: {n:,} rows already built. Use force=True to rebuild.")
                    return n
            except Exception as e:
                logger.debug("Failed to probe existing cpr_daily row count: %s", e)

        self._invalidate_metadata_caches()
        self.con.execute("DROP TABLE IF EXISTS cpr_daily")
        self.con.execute(f"CREATE TABLE cpr_daily AS {insert_sql}")
        self.con.execute("DROP INDEX IF EXISTS idx_cpr_symbol_date")
        self.con.execute("DROP INDEX IF EXISTS idx_cpr_symbol_date_unique")
        self.con.execute(
            "CREATE UNIQUE INDEX idx_cpr_symbol_date_unique ON cpr_daily(symbol, trade_date)"
        )
        n = self.con.execute("SELECT COUNT(*) FROM cpr_daily").fetchone()[0]
        print(f"cpr_daily built: {n:,} rows (with R1-S3, cpr_shift, narrowing)")
        self._publish_replica(force=True)
        return n

    def build_atr_table(
        self,
        periods: int = 12,
        force: bool = False,
        symbols: list[str] | None = None,
        batch_size: int | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Pre-compute intraday ATR from last N five-minute candles of each trading day.
        ATR from day D is stored as the ATR *for* day D+1 (next trading day).

        periods=12 = last 1 hour of previous trading day (12 × 5-min candles).

        Uses pre-computed true_range from Parquet if available (added by pivot-convert).
        Falls back to computing True Range from OHLC if the column is not present
        (e.g. for Parquet files converted before this feature was added).

        symbols: if provided, only build for these symbols (faster for testing).

        Args:
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        self._require_data("v_5min")

        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)

        if symbols:
            target_symbols = sorted(_validate_symbols(symbols))
            manifest: dict[str, list[str]] | None = None
        else:
            # Build file manifest once — avoids 16K-file glob discovery per batch.
            manifest = self._build_5min_file_manifest()
            if manifest:
                # Use manifest keys instead of querying v_5min (avoids glob scan).
                target_symbols = sorted(manifest.keys())
            else:
                # Manifest empty — fall back to v_5min symbol discovery.
                logger.warning("Manifest empty; falling back to v_5min symbol discovery")
                target_symbols = [
                    r[0]
                    for r in self.con.execute(
                        "SELECT DISTINCT symbol FROM v_5min ORDER BY symbol"
                    ).fetchall()
                ]

        if not target_symbols:
            print("atr_intraday: no symbols resolved; nothing to build.")
            return 0

        table_exists = self._table_exists("atr_intraday")

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            if table_exists:
                skip_n = _skip_if_table_fully_covered(
                    self.con,
                    table="atr_intraday",
                    date_col="trade_date",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    build_symbols=target_symbols,
                    label="atr",
                )
                if skip_n is not None:
                    return skip_n
                _incremental_delete(
                    self.con,
                    table="atr_intraday",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols if symbols else None,
                    log_prefix="atr",
                )
            # Fall through to normal insert logic with since_date filtering
        elif not force and symbols is None:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM atr_intraday").fetchone()[0]
                if n > 0:
                    print(f"atr_intraday: {n:,} rows already built. Use force=True to rebuild.")
                    return n
            except Exception as e:
                logger.debug("Failed to probe existing atr_intraday row count: %s", e)

        # Use stored true_range from Parquet if available — faster and ensures
        # consistency with the values stored per candle.
        try:
            self.con.execute("SELECT true_range FROM v_5min LIMIT 0")
            use_stored_tr = True
        except Exception as e:
            logger.debug(
                "v_5min.true_range column not available, deriving true range on the fly: %s", e
            )
            use_stored_tr = False

        # Build the CTE body WITHOUT the leading WITH keyword — it gets prepended
        # in the final CREATE TABLE ... AS WITH ... SELECT statement.
        def _build_batch_sql(
            batch_symbols: list[str],
            *,
            trade_date_since: str | None = None,
            trade_date_until: str | None = None,
            source_from: str | None = None,
        ) -> str:
            source = source_from or "v_5min"
            symbol_list = ",".join(f"'{s}'" for s in _validate_symbols(batch_symbols))
            symbol_filter = f"AND symbol IN ({symbol_list})"
            # For incremental mode, filter the final output to trade_date >= since
            # but include all source candles so LEAD() computes correctly
            trade_date_filter = _date_window_clause(
                "trade_date", trade_date_since, trade_date_until
            )
            if use_stored_tr:
                cte_body = f"""
                ranked AS (
                    SELECT symbol, date, true_range,
                        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY candle_time DESC) AS rn
                    FROM {source}
                    WHERE true_range IS NOT NULL {symbol_filter}
                ),
                day_atr AS (
                    SELECT symbol, date, AVG(true_range) AS atr
                    FROM ranked
                    WHERE rn <= {periods}
                    GROUP BY symbol, date
                    HAVING COUNT(*) >= {periods // 2}
                )
            """
            else:
                cte_body = f"""
                candles AS (
                    SELECT symbol, date, candle_time, high, low, close,
                        LAG(close) OVER (PARTITION BY symbol, date ORDER BY candle_time) AS prev_close
                    FROM {source}
                    WHERE 1=1 {symbol_filter}
                ),
                true_ranges AS (
                    SELECT symbol, date, candle_time,
                        GREATEST(
                            high - low,
                            ABS(high - COALESCE(prev_close, close)),
                            ABS(low  - COALESCE(prev_close, close))
                        ) AS true_range,
                        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY candle_time DESC) AS rn
                    FROM candles
                ),
                day_atr AS (
                    SELECT symbol, date, AVG(true_range) AS atr
                    FROM true_ranges
                    WHERE rn <= {periods}
                    GROUP BY symbol, date
                    HAVING COUNT(*) >= {periods // 2}
                )
            """
            return f"""
                WITH
                    {cte_body},
                    with_next AS (
                        SELECT symbol, date, atr,
                            LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS trade_date
                        FROM day_atr
                    )
                SELECT symbol, trade_date, date AS prev_date, atr
                FROM with_next
                WHERE trade_date IS NOT NULL
                {trade_date_filter}
            """

        if use_stored_tr:
            print(f"  [ATR] Using stored true_range from Parquet (periods={periods})")
        else:
            print(
                "  [ATR] Computing True Range from OHLC (run pivot-convert --overwrite to add stored TR)"
            )

        batch_size = max(1, int(batch_size or 0)) if batch_size else 0
        use_batches = batch_size > 0 and len(target_symbols) > batch_size

        # In incremental mode the DELETE already removed stale rows; skip table recreation
        if since_date_iso and not force and table_exists:
            pass  # table is ready; rows already deleted above
        elif target_symbols and table_exists:
            symbol_list = _sql_symbol_list(target_symbols)
            self.con.execute("DROP TABLE IF EXISTS tmp_atr_intraday_keep")
            self.con.execute(
                f"""
                CREATE TEMP TABLE tmp_atr_intraday_keep AS
                SELECT *
                FROM atr_intraday
                WHERE symbol NOT IN ({symbol_list})
                """
            )
            self.con.execute("DROP TABLE atr_intraday")
            self.con.execute("""
                CREATE TABLE atr_intraday (
                    symbol VARCHAR,
                    trade_date DATE,
                    prev_date DATE,
                    atr DOUBLE
                )
            """)
            self.con.execute("INSERT INTO atr_intraday SELECT * FROM tmp_atr_intraday_keep")
            self.con.execute("DROP TABLE tmp_atr_intraday_keep")
        else:
            self._invalidate_metadata_caches()
            self.con.execute("DROP TABLE IF EXISTS atr_intraday")
            self.con.execute("""
                CREATE TABLE atr_intraday (
                    symbol VARCHAR,
                    trade_date DATE,
                    prev_date DATE,
                    atr DOUBLE
                )
            """)

        if use_batches:
            batches = self._iter_symbol_batches(target_symbols, batch_size)
            total_batches = len(batches)
            started = time.time()
            print(
                f"  [ATR] batched rebuild start: symbols={len(target_symbols):,} "
                f"batch_size={batch_size} batches={total_batches}",
                flush=True,
            )
            for idx, batch in enumerate(batches, start=1):
                batch_started = time.time()
                batch_source = (
                    self._build_manifest_source_sql(batch, manifest)
                    if manifest is not None
                    else self._build_parquet_source_sql(batch)
                )
                batch_sql = _build_batch_sql(
                    batch,
                    trade_date_since=since_date_iso,
                    trade_date_until=until_date_iso,
                    source_from=batch_source,
                )
                self.con.execute(f"INSERT INTO atr_intraday {batch_sql}")
                done = min(idx * batch_size, len(target_symbols))
                elapsed = time.time() - started
                eta_min = ((elapsed / idx) * (total_batches - idx) / 60.0) if idx else 0.0
                print(
                    f"  [ATR] batch {idx}/{total_batches} | symbols={done}/{len(target_symbols)} "
                    f"| batch={time.time() - batch_started:.1f}s | elapsed={elapsed:.0f}s | ETA={eta_min:.1f}min",
                    flush=True,
                )
        else:
            batch_source = (
                self._build_manifest_source_sql(target_symbols, manifest)
                if manifest is not None
                else self._build_parquet_source_sql(target_symbols)
            )
            batch_sql = _build_batch_sql(
                target_symbols,
                trade_date_since=since_date_iso,
                trade_date_until=until_date_iso,
                source_from=batch_source,
            )
            self.con.execute(f"INSERT INTO atr_intraday {batch_sql}")

        self.con.execute("DROP INDEX IF EXISTS idx_atr_symbol_date")
        self.con.execute("CREATE INDEX idx_atr_symbol_date ON atr_intraday(symbol, trade_date)")
        n = self.con.execute("SELECT COUNT(*) FROM atr_intraday").fetchone()[0]
        print(f"atr_intraday built: {n:,} rows  (ATR-{periods})")
        self._publish_replica(force=True)
        return n

    def build_cpr_thresholds(
        self,
        percentile: float = 50.0,
        force: bool = False,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Pre-compute per-symbol rolling CPR width threshold.

        Uses 252-day rolling window (=1 trading year) to compute the Nth
        percentile of CPR widths. This is the dynamic filter used in
        check_entry_setup() to identify narrow-CPR (trending) days.
        symbols: if provided, only build for these symbols (faster for testing).

        Args:
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
                        The rolling window computation uses ALL cpr_daily rows for correctness,
                        but only rows >= since_date are inserted.
            until_date: Optional upper bound for a bounded refresh window.
        """
        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)
        target_symbols: list[str] = []
        if symbols:
            _validate_symbols(symbols)
            target_symbols = list(symbols)
        symbol_filter_sql = ""
        if target_symbols:
            symbol_filter_sql = f"AND symbol IN ({_sql_symbol_list(target_symbols)})"
        window_filter_sql = _date_window_clause("trade_date", since_date_iso, until_date_iso)

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            table_exists = self._table_exists("cpr_thresholds")
            if table_exists:
                _incremental_delete(
                    self.con,
                    table="cpr_thresholds",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols or None,
                    log_prefix="thresholds",
                )
                # Recompute over full cpr_daily (rolling window needs history)
                # but only INSERT rows >= since_date
                pct = percentile / 100.0
                self.con.execute(f"""
                    INSERT INTO cpr_thresholds
                    SELECT symbol, trade_date, cpr_threshold_pct
                    FROM (
                        SELECT
                            symbol,
                            trade_date,
                            QUANTILE_CONT(cpr_width_pct, {pct})
                                OVER (
                                    PARTITION BY symbol
                                    ORDER BY trade_date
                                    ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                                ) AS cpr_threshold_pct
                        FROM cpr_daily
                        WHERE 1=1
                        {symbol_filter_sql}
                    ) sub
                    WHERE 1=1
                    {window_filter_sql}
                """)
                n = self.con.execute("SELECT COUNT(*) FROM cpr_thresholds").fetchone()[0]
                print(
                    f"cpr_thresholds refreshed: {n:,} rows"
                    f" (incremental since {since_date_iso}"
                    + (
                        f"..{until_date_iso}"
                        if until_date_iso and until_date_iso != since_date_iso
                        else ""
                    )
                    + f", P{percentile:.0f})"
                )
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM cpr_thresholds").fetchone()[0]
                if n > 0:
                    print(f"cpr_thresholds: {n:,} rows already built. Use force=True to rebuild.")
                    return n
            except Exception as e:
                logger.debug("Failed to probe existing cpr_thresholds row count: %s", e)

        pct = percentile / 100.0
        table_exists = self._table_exists("cpr_thresholds")

        threshold_query = f"""
            SELECT
                symbol,
                trade_date,
                QUANTILE_CONT(cpr_width_pct, {pct})
                    OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                        ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                    ) AS cpr_threshold_pct
            FROM cpr_daily
        """

        if target_symbols and table_exists:
            symbol_list = _sql_symbol_list(target_symbols)
            self.con.execute(f"DELETE FROM cpr_thresholds WHERE symbol IN ({symbol_list})")
            self.con.execute(f"""
                INSERT INTO cpr_thresholds
                {threshold_query}
                WHERE symbol IN ({symbol_list})
                {window_filter_sql}
            """)
        else:
            self._invalidate_metadata_caches()
            self.con.execute("DROP TABLE IF EXISTS cpr_thresholds")
            self.con.execute(
                f"CREATE TABLE cpr_thresholds AS {threshold_query} WHERE 1=1 {symbol_filter_sql} {window_filter_sql}"
            )
            self.con.execute("DROP INDEX IF EXISTS idx_thresh_symbol_date")
            self.con.execute("DROP INDEX IF EXISTS idx_thresh_symbol_date_unique")
            self.con.execute(
                "CREATE UNIQUE INDEX idx_thresh_symbol_date_unique "
                "ON cpr_thresholds(symbol, trade_date)"
            )
        n = self.con.execute("SELECT COUNT(*) FROM cpr_thresholds").fetchone()[0]
        print(f"cpr_thresholds built: {n:,} rows  (P{percentile:.0f})")
        self._publish_replica(force=True)
        return n

    def build_virgin_cpr_flags(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Identify Virgin CPR days: trading days where price never touched the CPR zone.

        A Virgin CPR occurs when no 5-min candle's [low, high] range overlaps with
        [min(TC, BC), max(TC, BC)] for that day. These untouched zones carry forward
        as strong support/resistance for the next trading session.

        Creates a separate virgin_cpr_flags table (not altering cpr_daily) so it can
        be rebuilt independently.

        Args:
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        # Prefer intraday_day_pack (materialized arrays) over v_5min (175M-row Parquet scan)
        use_day_pack = self._table_exists("intraday_day_pack")
        if not use_day_pack:
            self._require_data("v_5min")

        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)
        target_symbols: list[str] = []
        if symbols:
            _validate_symbols(symbols)
            target_symbols = list(symbols)

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            table_exists = self._table_exists("virgin_cpr_flags")
            if table_exists:
                _incremental_delete(
                    self.con,
                    table="virgin_cpr_flags",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols or None,
                    log_prefix="virgin",
                )
                # Build virgin query scoped to since_date
                date_filter = _date_window_clause("trade_date", since_date_iso, until_date_iso)
                if use_day_pack:
                    touched_sql = """
                        SELECT DISTINCT cz.symbol, cz.trade_date
                        FROM cpr_zones cz
                        JOIN intraday_day_pack p
                          ON p.symbol = cz.symbol AND p.trade_date = cz.trade_date
                        WHERE list_max(p.high_arr) >= cz.cpr_bottom
                          AND list_min(p.low_arr)  <= cz.cpr_top
                    """
                else:
                    touched_sql = """
                        SELECT DISTINCT cz.symbol, cz.trade_date
                        FROM cpr_zones cz
                        JOIN v_5min v
                          ON v.symbol = cz.symbol AND v.date = cz.trade_date
                        WHERE v.high >= cz.cpr_bottom
                          AND v.low  <= cz.cpr_top
                    """
                self.con.execute(f"""
                    INSERT INTO virgin_cpr_flags
                    WITH cpr_zones AS (
                        SELECT
                            symbol,
                            trade_date,
                            LEAST(tc, bc)    AS cpr_bottom,
                            GREATEST(tc, bc) AS cpr_top
                        FROM cpr_daily
                        WHERE 1=1 {date_filter}
                    ),
                    touched AS (
                        {touched_sql}
                    )
                    SELECT
                        cz.symbol,
                        cz.trade_date,
                        CASE WHEN t.trade_date IS NULL THEN TRUE ELSE FALSE END AS is_virgin_cpr
                    FROM cpr_zones cz
                    LEFT JOIN touched t
                      ON t.symbol = cz.symbol AND t.trade_date = cz.trade_date
                """)
                n = self.con.execute("SELECT COUNT(*) FROM virgin_cpr_flags").fetchone()[0]
                virgin_n = self.con.execute(
                    "SELECT COUNT(*) FROM virgin_cpr_flags WHERE is_virgin_cpr = TRUE"
                ).fetchone()[0]
                print(
                    f"virgin_cpr_flags refreshed: {n:,} rows ({virgin_n:,} virgin)"
                    f" (incremental since {since_date_iso}"
                    + (
                        f"..{until_date_iso}"
                        if until_date_iso and until_date_iso != since_date_iso
                        else ""
                    )
                    + ")"
                )
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force and symbols is None:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM virgin_cpr_flags").fetchone()[0]
                if n > 0:
                    print(f"virgin_cpr_flags: {n:,} rows already built. Use force=True to rebuild.")
                    return n
            except Exception as e:
                logger.debug("Failed to probe existing virgin_cpr_flags row count: %s", e)

        symbol_filter = (
            f"AND symbol IN ({_sql_symbol_list(target_symbols)})" if target_symbols else ""
        )
        date_filter = _date_window_clause("trade_date", since_date_iso, until_date_iso)

        if use_day_pack:
            touched_sql = """
                SELECT DISTINCT cz.symbol, cz.trade_date
                FROM cpr_zones cz
                JOIN intraday_day_pack p
                  ON p.symbol = cz.symbol AND p.trade_date = cz.trade_date
                WHERE list_max(p.high_arr) >= cz.cpr_bottom
                  AND list_min(p.low_arr)  <= cz.cpr_top
            """
        else:
            touched_sql = """
                SELECT DISTINCT cz.symbol, cz.trade_date
                FROM cpr_zones cz
                JOIN v_5min v
                  ON v.symbol = cz.symbol AND v.date = cz.trade_date
                WHERE v.high >= cz.cpr_bottom
                  AND v.low  <= cz.cpr_top
            """

        virgin_query = f"""
            WITH cpr_zones AS (
                SELECT
                    symbol,
                    trade_date,
                    LEAST(tc, bc)    AS cpr_bottom,
                    GREATEST(tc, bc) AS cpr_top
                FROM cpr_daily
                WHERE 1=1 {symbol_filter} {date_filter}
            ),
            touched AS (
                {touched_sql}
            )
            SELECT
                cz.symbol,
                cz.trade_date,
                CASE WHEN t.trade_date IS NULL THEN TRUE ELSE FALSE END AS is_virgin_cpr
            FROM cpr_zones cz
            LEFT JOIN touched t
              ON t.symbol = cz.symbol AND t.trade_date = cz.trade_date
        """

        table_exists = self._table_exists("virgin_cpr_flags")

        if target_symbols and table_exists:
            sym_list = ",".join(f"'{s}'" for s in target_symbols)
            self.con.execute(f"DELETE FROM virgin_cpr_flags WHERE symbol IN ({sym_list})")
            self.con.execute(f"INSERT INTO virgin_cpr_flags {virgin_query}")
        else:
            self._invalidate_metadata_caches()
            self.con.execute("DROP TABLE IF EXISTS virgin_cpr_flags")
            self.con.execute(f"CREATE TABLE virgin_cpr_flags AS {virgin_query}")
            self.con.execute(
                "CREATE INDEX IF NOT EXISTS idx_virgin_cpr ON virgin_cpr_flags(symbol, trade_date)"
            )
        n = self.con.execute("SELECT COUNT(*) FROM virgin_cpr_flags").fetchone()[0]
        virgin_n = self.con.execute(
            "SELECT COUNT(*) FROM virgin_cpr_flags WHERE is_virgin_cpr = TRUE"
        ).fetchone()[0]
        print(f"virgin_cpr_flags built: {n:,} rows ({virgin_n:,} virgin days)")
        self._publish_replica(force=True)
        return n

    def build_or_table(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Pre-compute Opening Range (OR) candle aggregates per (symbol, date).

        Stores the first 6 OR candle slots (09:15 -- 09:40) so that setup queries
        can join a DuckDB table instead of scanning v_5min with a strftime() filter.

        strftime() filters CANNOT be pushed into Parquet row-group statistics, so
        every v_5min query with a time filter pays a ~25s metadata/scan overhead.
        This table eliminates that cost entirely for CPR_LEVELS and FBR setup queries.

        Schema per row (one row per symbol x trading day):
            o0915, v0915          -- open and volume from the 9:15 candle
            h09XX, l09XX, c09XX   -- high, low, close for each OR slot

        Args:
            symbols: If provided, only upsert rows for these symbols (DELETE+INSERT).
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        self._require_data("v_5min")

        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)
        table_exists = self._table_exists("or_daily")
        target_symbols: list[str] = []
        symbol_list = ""
        if symbols:
            target_symbols = sorted(_validate_symbols(symbols))
            symbol_list = _sql_symbol_list(target_symbols)

        def _or_select_sql(
            *, symbol_filter: str = "", since_date: str | None = None, until_date: str | None = None
        ) -> str:
            date_filter = _date_window_clause("date::DATE", since_date, until_date)
            return f"""
                SELECT
                    symbol,
                    date::DATE AS trade_date,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:15' THEN open   END) AS o0915,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:15' THEN volume END) AS v0915,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:15' THEN high  END) AS h0915,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:15' THEN low   END) AS l0915,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:15' THEN close END) AS c0915,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:20' THEN high  END) AS h0920,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:20' THEN low   END) AS l0920,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:20' THEN close END) AS c0920,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:25' THEN high  END) AS h0925,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:25' THEN low   END) AS l0925,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:25' THEN close END) AS c0925,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:30' THEN high  END) AS h0930,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:30' THEN low   END) AS l0930,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:30' THEN close END) AS c0930,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:35' THEN high  END) AS h0935,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:35' THEN low   END) AS l0935,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:35' THEN close END) AS c0935,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:40' THEN high  END) AS h0940,
                    MIN(CASE WHEN strftime(candle_time,'%H:%M')='09:40' THEN low   END) AS l0940,
                    MAX(CASE WHEN strftime(candle_time,'%H:%M')='09:40' THEN close END) AS c0940
                FROM v_5min
                WHERE strftime(candle_time, '%H:%M') IN
                    ('09:15','09:20','09:25','09:30','09:35','09:40')
                    {symbol_filter}
                    {date_filter}
                GROUP BY symbol, date
                HAVING c0915 IS NOT NULL
            """

        # ── Symbol-scoped upsert ───────────────────────────────────────────
        if target_symbols:
            if table_exists:
                deleted = self.con.execute(
                    f"DELETE FROM or_daily WHERE symbol IN ({symbol_list})"
                ).rowcount
                print(
                    f"  [or] symbol upsert: deleted {deleted:,} rows for {len(target_symbols)} symbols"
                )
            else:
                self.con.execute(
                    f"CREATE TABLE or_daily AS {_or_select_sql(symbol_filter=f'AND symbol IN ({symbol_list})', since_date=since_date_iso, until_date=until_date_iso)}"
                )
                self.con.execute(
                    "CREATE INDEX IF NOT EXISTS idx_or_daily ON or_daily(symbol, trade_date)"
                )
                n = self.con.execute("SELECT COUNT(*) FROM or_daily").fetchone()[0]
                syms = self.con.execute("SELECT COUNT(DISTINCT symbol) FROM or_daily").fetchone()[0]
                print(f"or_daily built: {n:,} rows across {syms:,} symbols")
                return n
            self.con.execute(
                f"INSERT INTO or_daily {_or_select_sql(symbol_filter=f'AND symbol IN ({symbol_list})', since_date=since_date_iso, until_date=until_date_iso)}"
            )
            n = self.con.execute("SELECT COUNT(*) FROM or_daily").fetchone()[0]
            syms = self.con.execute("SELECT COUNT(DISTINCT symbol) FROM or_daily").fetchone()[0]
            print(
                f"or_daily refreshed: {n:,} rows across {syms:,} symbols (upserted {len(target_symbols)} symbols)"
            )
            self._publish_replica(force=True)
            return n
        # ──────────────────────────────────────────────────────────────────

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            if table_exists:
                deleted = _incremental_delete(
                    self.con,
                    table="or_daily",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols or None,
                    log_prefix="or",
                )
                self.con.execute(
                    f"INSERT INTO or_daily {_or_select_sql(since_date=since_date_iso, until_date=until_date_iso)}"
                )
                n = self.con.execute("SELECT COUNT(*) FROM or_daily").fetchone()[0]
                syms = self.con.execute("SELECT COUNT(DISTINCT symbol) FROM or_daily").fetchone()[0]
                print(
                    f"or_daily refreshed: {n:,} rows across {syms:,} symbols"
                    f" (incremental since {since_date_iso}"
                    + (
                        f"..{until_date_iso}"
                        if until_date_iso and until_date_iso != since_date_iso
                        else ""
                    )
                    + ")"
                )
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM or_daily").fetchone()[0]
                if n > 0:
                    print(f"or_daily: {n:,} rows already built. Use force=True to rebuild.")
                    return n
            except Exception as e:
                logger.debug("Failed to probe existing or_daily row count: %s", e)

        self._invalidate_metadata_caches()
        self.con.execute("DROP TABLE IF EXISTS or_daily")
        self.con.execute(f"CREATE TABLE or_daily AS {_or_select_sql()}")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_or_daily ON or_daily(symbol, trade_date)")
        n = self.con.execute("SELECT COUNT(*) FROM or_daily").fetchone()[0]
        syms = self.con.execute("SELECT COUNT(DISTINCT symbol) FROM or_daily").fetchone()[0]
        print(f"or_daily built: {n:,} rows across {syms:,} symbols")
        self._publish_replica(force=True)
        return n

    def _market_day_state_select_sql(
        self,
        symbols: list[str] | None = None,
        cpr_max_width_pct: float = 2.0,
        since_date: str | None = None,
        until_date: str | None = None,
        virgin_exists: bool = True,
    ) -> str:
        """Build SELECT SQL used for market_day_state create/refresh operations."""
        symbol_filter = ""
        if symbols:
            symbol_list = ",".join(f"'{s}'" for s in symbols)
            symbol_filter = f"AND c.symbol IN ({symbol_list})"
        date_filter = _date_window_clause("c.trade_date", since_date, until_date)
        return f"""
            WITH base AS (
                SELECT
                    c.symbol,
                    c.trade_date::DATE AS trade_date,
                    c.prev_date::DATE AS prev_date,
                    c.prev_close,
                    c.tc,
                    c.bc,
                    c."pivot",
                    c.cpr_width_pct,
                    c.r1,
                    c.s1,
                    c.r2,
                    c.s2,
                    c.r3,
                    c.s3,
                    c.cpr_shift,
                    c.is_narrowing,
                    COALESCE(t.cpr_threshold_pct, {cpr_max_width_pct}) AS cpr_threshold_pct,
                    a.atr,
                    {"COALESCE(v.is_virgin_cpr, FALSE)" if virgin_exists else "FALSE"} AS prev_is_virgin,
                    o.o0915 AS open_915,
                    o.v0915 AS volume_915,
                    o.h0915, o.l0915, o.c0915,
                    o.h0920, o.l0920, o.c0920,
                    o.h0925, o.l0925, o.c0925,
                    o.h0930, o.l0930, o.c0930,
                    o.h0935, o.l0935, o.c0935,
                    o.h0940, o.l0940, o.c0940
                FROM cpr_daily c
                -- ASOF JOIN: finds the most recent ATR row with trade_date <= c.trade_date.
                -- atr_intraday is forward-shifted at build time (LEAD): the row keyed by
                -- trade_date=T stores ATR computed from prev_date=T-1 candles. So for any
                -- given trade date T, matching on <= correctly finds the T row whose ATR
                -- represents yesterday's volatility — exactly what is available pre-market.
                -- Zero-ATR rows (circuit filter, no trades) are excluded so the join
                -- reaches back to the nearest valid prior-day ATR instead.
                ASOF JOIN (SELECT * FROM atr_intraday WHERE atr > 0) a
                  ON a.symbol = c.symbol AND a.trade_date <= c.trade_date
                LEFT JOIN cpr_thresholds t
                  ON t.symbol = c.symbol AND t.trade_date = c.trade_date
                LEFT JOIN or_daily o
                  ON o.symbol = c.symbol AND o.trade_date = c.trade_date
                {"LEFT JOIN virgin_cpr_flags v ON v.symbol = c.symbol AND v.trade_date = c.prev_date" if virgin_exists else ""}
                -- Keep rows where 9:15 data exists (historical) OR where or_daily has no
                -- match at all (today pre-market: LEFT JOIN returns NULL for o.symbol).
                WHERE (o.c0915 IS NOT NULL OR o.symbol IS NULL)
                {symbol_filter}
                {date_filter}
            ),
            derived AS (
                SELECT
                    *,
                    h0915 AS or_high_5,
                    l0915 AS or_low_5,
                    c0915 AS or_close_5,
                    GREATEST(h0915, h0920) AS or_high_10,
                    LEAST(l0915, l0920) AS or_low_10,
                    c0920 AS or_close_10,
                    GREATEST(h0915, h0920, h0925) AS or_high_15,
                    LEAST(l0915, l0920, l0925) AS or_low_15,
                    c0925 AS or_close_15,
                    GREATEST(h0915, h0920, h0925, h0930, h0935, h0940) AS or_high_30,
                    LEAST(l0915, l0920, l0925, l0930, l0935, l0940) AS or_low_30,
                    c0940 AS or_close_30
                FROM base
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, trade_date
                        ORDER BY prev_date DESC NULLS LAST
                    ) AS rn
                FROM derived
            )
            SELECT
                symbol,
                trade_date,
                prev_date,
                prev_close,
                tc,
                bc,
                "pivot",
                cpr_width_pct,
                r1,
                s1,
                r2,
                s2,
                r3,
                s3,
                cpr_shift,
                is_narrowing,
                cpr_threshold_pct,
                atr,
                prev_is_virgin,
                open_915,
                volume_915,
                or_high_5,
                or_low_5,
                or_close_5,
                or_high_10,
                or_low_10,
                or_close_10,
                or_high_15,
                or_low_15,
                or_close_15,
                or_high_30,
                or_low_30,
                or_close_30,
                CASE
                    WHEN prev_close > 0 AND open_915 IS NOT NULL
                    THEN ((open_915 - prev_close) / prev_close) * 100
                    ELSE NULL
                END AS gap_pct_open
            FROM ranked
            WHERE rn = 1
        """

    def build_market_day_state(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        cpr_max_width_pct: float = 2.0,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Build one-row-per-day strategy state used by runtime setup filtering.

        This joins CPR/ATR/threshold/OR/virgin metadata into a single read model
        so runtime does not need raw Parquet scans for setup evaluation.

        Args:
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)

        target_symbols = sorted(set(_validate_symbols(symbols))) if symbols else None
        table_exists = self._table_exists("market_day_state")
        virgin_exists = self._table_exists("virgin_cpr_flags")

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            if table_exists:
                _incremental_delete(
                    self.con,
                    table="market_day_state",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols,
                    log_prefix="state",
                )
                select_sql = self._market_day_state_select_sql(
                    symbols=target_symbols,
                    cpr_max_width_pct=cpr_max_width_pct,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    virgin_exists=virgin_exists,
                )
                self.con.execute(f"INSERT INTO market_day_state {select_sql}")
                n = self.con.execute("SELECT COUNT(*) FROM market_day_state").fetchone()[0]
                print(
                    f"market_day_state refreshed: {n:,} rows (incremental since {since_date_iso}"
                    + (
                        f"..{until_date_iso}"
                        if until_date_iso and until_date_iso != since_date_iso
                        else ""
                    )
                    + ")"
                )
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force and target_symbols is None and table_exists:
            n = self.con.execute("SELECT COUNT(*) FROM market_day_state").fetchone()[0]
            if n > 0:
                print(f"market_day_state: {n:,} rows already built. Use force=True to rebuild.")
                return n

        select_sql = self._market_day_state_select_sql(
            symbols=target_symbols,
            cpr_max_width_pct=cpr_max_width_pct,
            since_date=since_date_iso,
            until_date=until_date_iso,
            virgin_exists=virgin_exists,
        )

        if target_symbols and table_exists:
            _symbol_scoped_upsert(
                self.con,
                table="market_day_state",
                select_sql=select_sql,
                symbols=target_symbols,
            )
        else:
            self._invalidate_metadata_caches()
            self.con.execute("DROP TABLE IF EXISTS market_day_state")
            self.con.execute(f"CREATE TABLE market_day_state AS {select_sql}")

        self.con.execute("DROP INDEX IF EXISTS idx_market_day_state")
        self.con.execute("DROP INDEX IF EXISTS idx_market_day_state_unique")
        self.con.execute(
            "CREATE UNIQUE INDEX idx_market_day_state_unique "
            "ON market_day_state(symbol, trade_date)"
        )
        n = self.con.execute("SELECT COUNT(*) FROM market_day_state").fetchone()[0]
        if target_symbols:
            print(
                f"market_day_state refreshed for {len(target_symbols)} symbols. total rows now: {n:,}",
                flush=True,
            )
        else:
            print(f"market_day_state built: {n:,} rows")
        self._publish_replica(force=True)
        return n

    def _strategy_day_state_select_sql(
        self,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> str:
        """Build SELECT SQL used for strategy_day_state create/refresh operations."""
        symbol_filter = ""
        if symbols:
            symbol_list = ", ".join(f"'{s}'" for s in _validate_symbols(symbols))
            symbol_filter = f"AND symbol IN ({symbol_list})"
        date_filter = _date_window_clause("trade_date", since_date, until_date)

        return f"""
            WITH source AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, trade_date
                        ORDER BY prev_date DESC NULLS LAST
                    ) AS rn
                FROM market_day_state
                WHERE 1=1
                  {symbol_filter}
                  {date_filter}
            )
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN open_915 < LEAST(tc, bc) THEN 'BELOW'
                    WHEN open_915 > GREATEST(tc, bc) THEN 'ABOVE'
                    ELSE 'INSIDE'
                END AS open_side,
                CASE
                    WHEN atr > 0 AND open_915 IS NOT NULL AND tc IS NOT NULL AND bc IS NOT NULL THEN
                        CASE
                            WHEN open_915 < LEAST(tc, bc) THEN ABS(LEAST(tc, bc) - open_915) / atr
                            WHEN open_915 > GREATEST(tc, bc) THEN ABS(open_915 - GREATEST(tc, bc)) / atr
                            ELSE 0.0
                        END
                    ELSE 0.0
                END AS open_to_cpr_atr,
                CASE WHEN gap_pct_open IS NULL THEN 0.0 ELSE ABS(gap_pct_open) END AS gap_abs_pct,
                CASE WHEN atr > 0 THEN (or_high_5 - or_low_5) / atr ELSE 0.0 END AS or_atr_5,
                CASE WHEN atr > 0 THEN (or_high_10 - or_low_10) / atr ELSE 0.0 END AS or_atr_10,
                CASE WHEN atr > 0 THEN (or_high_15 - or_low_15) / atr ELSE 0.0 END AS or_atr_15,
                CASE WHEN atr > 0 THEN (or_high_30 - or_low_30) / atr ELSE 0.0 END AS or_atr_30,
                CASE
                    WHEN or_close_5 > GREATEST(tc, bc) THEN 'LONG'
                    WHEN or_close_5 < LEAST(tc, bc) THEN 'SHORT'
                    ELSE 'NONE'
                END AS direction_5,
                CASE
                    WHEN or_close_10 > GREATEST(tc, bc) THEN 'LONG'
                    WHEN or_close_10 < LEAST(tc, bc) THEN 'SHORT'
                    ELSE 'NONE'
                END AS direction_10,
                CASE
                    WHEN or_close_15 > GREATEST(tc, bc) THEN 'LONG'
                    WHEN or_close_15 < LEAST(tc, bc) THEN 'SHORT'
                    ELSE 'NONE'
                END AS direction_15,
                CASE
                    WHEN or_close_30 > GREATEST(tc, bc) THEN 'LONG'
                    WHEN or_close_30 < LEAST(tc, bc) THEN 'SHORT'
                    ELSE 'NONE'
                END AS direction_30
            FROM source
            WHERE rn = 1
        """

    def build_strategy_day_state(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> int:
        """
        Build strategy-specific derived day state used for SQL setup pushdown filters.

        One row per (symbol, trade_date) with precomputed open-side, gap, and OR/ATR
        metrics so runtime setup queries can reduce candidate rows before simulation.

        Args:
            since_date: Incremental mode -- only refresh rows for trade_date >= since_date.
            until_date: Optional upper bound for a bounded refresh window.
        """
        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)

        target_symbols = sorted(set(_validate_symbols(symbols))) if symbols else None
        table_exists = self._table_exists("strategy_day_state")

        # ── Incremental mode ──────────────────────────────────────────────
        if since_date_iso and not force:
            if table_exists:
                _incremental_delete(
                    self.con,
                    table="strategy_day_state",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols,
                    log_prefix="strategy",
                )
                select_sql = self._strategy_day_state_select_sql(
                    symbols=target_symbols, since_date=since_date_iso, until_date=until_date_iso
                )
                self.con.execute(f"INSERT INTO strategy_day_state {select_sql}")
                n = self.con.execute("SELECT COUNT(*) FROM strategy_day_state").fetchone()[0]
                print(
                    f"strategy_day_state refreshed: {n:,} rows (incremental since {since_date_iso}"
                    + (
                        f"..{until_date_iso}"
                        if until_date_iso and until_date_iso != since_date_iso
                        else ""
                    )
                    + ")"
                )
                self._publish_replica(force=True)
                return n
        # ──────────────────────────────────────────────────────────────────

        if not force and target_symbols is None and table_exists:
            n = self.con.execute("SELECT COUNT(*) FROM strategy_day_state").fetchone()[0]
            if n > 0:
                print(f"strategy_day_state: {n:,} rows already built. Use force=True to rebuild.")
                return n

        select_sql = self._strategy_day_state_select_sql(
            symbols=target_symbols, since_date=since_date_iso, until_date=until_date_iso
        )

        if target_symbols and table_exists:
            _symbol_scoped_upsert(
                self.con,
                table="strategy_day_state",
                select_sql=select_sql,
                symbols=target_symbols,
            )
        else:
            self._invalidate_metadata_caches()
            self.con.execute("DROP TABLE IF EXISTS strategy_day_state")
            self.con.execute(f"CREATE TABLE strategy_day_state AS {select_sql}")

        self.con.execute("DROP INDEX IF EXISTS idx_strategy_day_state")
        self.con.execute("DROP INDEX IF EXISTS idx_strategy_day_state_unique")
        self.con.execute(
            "CREATE UNIQUE INDEX idx_strategy_day_state_unique "
            "ON strategy_day_state(symbol, trade_date)"
        )
        n = self.con.execute("SELECT COUNT(*) FROM strategy_day_state").fetchone()[0]
        if target_symbols:
            print(
                f"strategy_day_state refreshed for {len(target_symbols)} symbols. total rows now: {n:,}",
                flush=True,
            )
        else:
            print(f"strategy_day_state built: {n:,} rows")
        self._publish_replica(force=True)
        return n

    def _missing_symbols_in_runtime_table(self, table: str, symbols: list[str]) -> list[str]:
        """Return requested symbols missing from the given runtime table."""
        target = sorted(set(_validate_symbols(symbols)))
        if not target:
            return []

        if not self._table_exists(table):
            return target

        placeholders = ", ".join("?" for _ in target)
        rows = self.con.execute(
            f"SELECT DISTINCT symbol FROM {table} WHERE symbol IN ({placeholders})",
            target,
        ).fetchall()
        existing = {str(r[0]) for r in rows if r and r[0]}
        return [symbol for symbol in target if symbol not in existing]

    def get_missing_market_day_state_symbols(self, symbols: list[str]) -> list[str]:
        """Return requested symbols missing from market_day_state."""
        return self._missing_symbols_in_runtime_table("market_day_state", symbols)

    def get_missing_intraday_day_pack_symbols(self, symbols: list[str]) -> list[str]:
        """Return requested symbols missing from intraday_day_pack."""
        return self._missing_symbols_in_runtime_table("intraday_day_pack", symbols)

    def get_missing_strategy_day_state_symbols(self, symbols: list[str]) -> list[str]:
        """Return requested symbols missing from strategy_day_state."""
        return self._missing_symbols_in_runtime_table("strategy_day_state", symbols)

    def get_missing_runtime_symbol_coverage(self, symbols: list[str]) -> dict[str, list[str]]:
        """Return runtime-table symbol coverage gaps for a requested symbol list."""
        target = sorted(set(_validate_symbols(symbols)))
        if not target:
            return {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []}

        return {
            "market_day_state": self.get_missing_market_day_state_symbols(target),
            "strategy_day_state": self.get_missing_strategy_day_state_symbols(target),
            "intraday_day_pack": self.get_missing_intraday_day_pack_symbols(target),
        }

    def get_runtime_trade_date_coverage(
        self, symbols: list[str], trade_date: str
    ) -> dict[str, list[str]]:
        """Return symbols missing for a specific trade date across required runtime tables.

        Symbols are only flagged as missing from market_day_state / strategy_day_state
        if they have a valid 09:15 opening candle in intraday_day_pack (minute=555).
        Symbols whose first candle is after 09:15 never produce a CPR setup and are
        legitimately absent from the state tables — they are not counted as gaps.
        """
        target = sorted(set(_validate_symbols(symbols)))
        if not target:
            return {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []}

        sym_list = ", ".join(f"'{s}'" for s in target)

        # Symbols that have a 09:15 candle (minute=555) in intraday_day_pack for this date.
        # These are the only ones that can produce a setup, so they're the only ones
        # that should be in market_day_state / strategy_day_state.
        try:
            rows = self.con.execute(
                f"""
                SELECT DISTINCT symbol FROM intraday_day_pack
                WHERE trade_date = '{trade_date}'::DATE
                  AND symbol IN ({sym_list})
                  AND minute_arr[1] = 555
                """
            ).fetchall()
            setup_capable = {str(r[0]) for r in rows}
        except Exception:
            setup_capable = set(target)

        missing_mds = [
            s
            for s in self._symbols_missing_for_trade_date("market_day_state", target, trade_date)
            if s in setup_capable
        ]
        missing_sds = [
            s
            for s in self._symbols_missing_for_trade_date("strategy_day_state", target, trade_date)
            if s in setup_capable
        ]
        return {
            "market_day_state": missing_mds,
            "strategy_day_state": missing_sds,
            "intraday_day_pack": self._symbols_missing_for_trade_date(
                "intraday_day_pack", target, trade_date
            ),
        }

    def get_symbols_with_parquet_data(self, trade_dates: list[str]) -> set[str]:
        """Return symbols that have 5min parquet data for any of the given trade dates."""
        if not trade_dates or not self._has_5min:
            return set()
        placeholders = ", ".join("?" for _ in trade_dates)
        rows = self.con.execute(
            f"SELECT DISTINCT symbol FROM v_5min WHERE date IN ({placeholders})",
            [*trade_dates],
        ).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}

    def get_table_max_trade_dates(self, tables: list[str]) -> dict[str, str | None]:
        """Return the latest trade_date value for each requested table."""
        result: dict[str, str | None] = {}
        for raw_table in tables:
            table = _validate_table_identifier(raw_table)
            if not self._table_exists(table):
                result[table] = None
                continue
            try:
                row = self.con.execute(f"SELECT MAX(trade_date)::VARCHAR FROM {table}").fetchone()
                result[table] = str(row[0]) if row and row[0] is not None else None
            except Exception as e:
                logger.debug("Failed to read max trade_date for %s: %s", table, e)
                result[table] = None
        return result

    def _symbols_missing_for_trade_date(
        self, table: str, symbols: list[str], trade_date: str
    ) -> list[str]:
        table = _validate_table_identifier(table)
        if not symbols:
            return []
        if not self._table_exists(table):
            return sorted(set(_validate_symbols(symbols)))
        placeholders = ", ".join("?" for _ in symbols)
        rows = self.con.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {table}
            WHERE symbol IN ({placeholders})
              AND trade_date = ?::DATE
            """,
            [*symbols, trade_date],
        ).fetchall()
        existing = {str(r[0]) for r in rows if r and r[0]}
        return [
            symbol for symbol in sorted(set(_validate_symbols(symbols))) if symbol not in existing
        ]

    def _table_exists(self, table: str) -> bool:
        """True if a table exists in the current DuckDB catalog."""
        row = self.con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()
        exists = row is not None
        if exists:
            self._table_exists_cache.add(table)
        else:
            self._table_exists_cache.discard(table)
        return exists

    def _table_has_column(self, table: str, column: str) -> bool:
        """True if a column exists in the current DuckDB catalog."""
        cache_key = (table, column)
        row = self.con.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table, column],
        ).fetchone()
        has_column = row is not None
        if has_column:
            self._table_has_column_cache.add(cache_key)
            self._table_exists_cache.add(table)
        else:
            self._table_has_column_cache.discard(cache_key)
        return has_column

    def _resolve_pack_symbols(self, symbols: list[str] | None) -> list[str]:
        """Resolve symbol universe for intraday_day_pack builds."""
        if symbols:
            return sorted(set(_validate_symbols(symbols)))

        # Prefer the runtime state table when available (matches setup universe exactly).
        try:
            rows = self.con.execute(
                "SELECT DISTINCT symbol FROM market_day_state ORDER BY symbol"
            ).fetchall()
            if rows:
                return _validate_symbols([r[0] for r in rows if r and r[0]])
        except Exception as e:
            logger.debug(
                "Falling back to v_5min symbols because market_day_state lookup failed: %s", e
            )

        rows = self.con.execute("SELECT DISTINCT symbol FROM v_5min ORDER BY symbol").fetchall()
        return _validate_symbols([r[0] for r in rows if r and r[0]])

    def _split_symbols_with_5min_data(self, symbols: list[str]) -> tuple[list[str], list[str]]:
        """Split symbols by whether local 5-min parquet files exist."""
        five_min_root = self._parquet_dir / "5min"
        available_dirs: set[str] = set()
        try:
            available_dirs = {
                path.name
                for path in five_min_root.iterdir()
                if path.is_dir() and next(path.glob("*.parquet"), None) is not None
            }
        except Exception as e:
            logger.debug("Failed to pre-scan 5-min parquet directories: %s", e)

        present: list[str] = []
        missing: list[str] = []
        for symbol in symbols:
            has_parquet = symbol in available_dirs
            if has_parquet:
                present.append(symbol)
            else:
                missing.append(symbol)
        return present, missing

    def _iter_symbol_batches(self, symbols: list[str], batch_size: int) -> list[list[str]]:
        """Split symbol list into fixed-size batches."""
        size = max(1, int(batch_size))
        return [symbols[i : i + size] for i in range(0, len(symbols), size)]

    @staticmethod
    def _escape_sql_literal(value: str) -> str:
        """Escape a string for safe single-quoted SQL literal usage."""
        return value.replace("'", "''")

    def _build_5min_file_manifest(self) -> dict[str, list[str]]:
        """Build a mapping of {symbol: [file_paths]} from the 5-min parquet directory.

        Uses pathlib for filesystem traversal instead of DuckDB's glob discovery,
        which re-scans ~16K files on every SQL query referencing v_5min.
        Takes ~1-2s vs ~5s per DuckDB glob per batch.
        """
        five_min_root = self._parquet_dir / "5min"
        if not five_min_root.is_dir():
            raise FileNotFoundError(f"5-min parquet directory not found: {five_min_root}")
        manifest: dict[str, list[str]] = {}
        for symbol_dir in sorted(five_min_root.iterdir()):
            if not symbol_dir.is_dir():
                continue
            parquet_files = sorted(symbol_dir.glob("*.parquet"))
            if parquet_files:
                manifest[symbol_dir.name] = [f.as_posix() for f in parquet_files]
        logger.info(
            "5-min file manifest: %d symbols, %d total files",
            len(manifest),
            sum(len(v) for v in manifest.values()),
        )
        return manifest

    def _build_manifest_source_sql(
        self,
        batch_symbols: list[str],
        manifest: dict[str, list[str]],
    ) -> str:
        """Build read_parquet() SQL using explicit file lists from the manifest."""
        all_paths: list[str] = []
        for symbol in batch_symbols:
            if symbol in manifest:
                all_paths.extend(manifest[symbol])
            else:
                glob_path = (self._parquet_dir / "5min" / symbol / "*.parquet").as_posix()
                all_paths.append(glob_path)
        if not all_paths:
            raise RuntimeError(f"No parquet files found for batch symbols: {batch_symbols[:5]}...")
        escaped = ",".join(f"'{self._escape_sql_literal(p)}'" for p in all_paths)
        return f"read_parquet([{escaped}], hive_partitioning=false)"

    def _build_parquet_source_sql(
        self,
        symbols: list[str],
        *,
        prefer_view: bool = False,
        manifest: dict[str, list[str]] | None = None,
    ) -> str:
        """Build a batch source SQL for intraday_day_pack."""
        if not symbols:
            raise RuntimeError("No symbols resolved for intraday_day_pack batch")
        if manifest is not None:
            return self._build_manifest_source_sql(symbols, manifest)

        if prefer_view:
            symbol_list = ",".join(f"'{self._escape_sql_literal(symbol)}'" for symbol in symbols)
            return f"(SELECT * FROM v_5min WHERE symbol IN ({symbol_list}))"

        globs: list[str] = []
        for symbol in symbols:
            glob_path = (self._parquet_dir / "5min" / symbol / "*.parquet").as_posix()
            globs.append(f"'{self._escape_sql_literal(glob_path)}'")
        return f"read_parquet([{','.join(globs)}], hive_partitioning=false)"

    def build_intraday_day_pack(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        rvol_lookback_days: int = 10,
        batch_size: int = 64,
        since_date: str | None = None,
        until_date: str | None = None,
        resume: bool = False,
    ) -> int:
        """
        Build packed per-day intraday arrays used by runtime simulation.

        One row per (symbol, trade_date) with aligned LIST columns:
        minute-of-day/open/high/low/close/volume/rvol_baseline.

        Phase 7 compaction:
        - New builds store `minute_arr` (`SMALLINT[]`) instead of `time_arr` (`VARCHAR[]`).
        - Numeric arrays are stored as `REAL[]` instead of `DOUBLE[]`.
        - Runtime remains backward-compatible with legacy `time_arr` day-pack rows.

        Large universes are processed in symbol batches so users see intermediate
        progress and memory pressure stays bounded.

        Args:
            since_date: Incremental mode — only insert/replace rows for dates >= since_date.
                        Deletes existing rows for that date range first, then inserts new ones.
                        Skips the DROP TABLE step entirely. Use after adding new Parquet data.
                        Example: "2025-04-01" to add April 2025 onwards.
            until_date: Optional upper bound for a bounded refresh window.
        """

        log_path = Path(self.db_path).parent / "pack_build.log"

        def _log(message: str) -> None:
            print(message, flush=True)
            try:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(message + "\n")
            except Exception as e:
                logger.debug("Failed to append intraday_day_pack build log file: %s", e)

        # Clear log from previous run
        try:
            log_path.write_text("", encoding="utf-8")
        except Exception as e:
            logger.debug("Failed to reset intraday_day_pack build log file: %s", e)

        self._require_data("v_5min")

        target_symbols = self._resolve_pack_symbols(symbols)
        if not target_symbols:
            _log("intraday_day_pack: no symbols resolved; nothing to build.")
            return 0

        build_symbols, missing_parquet = self._split_symbols_with_5min_data(target_symbols)
        if missing_parquet:
            preview = ", ".join(missing_parquet[:5])
            suffix = "..." if len(missing_parquet) > 5 else ""
            _log(
                "intraday_day_pack: skipping "
                f"{len(missing_parquet)} symbols with no 5-min parquet "
                f"({preview}{suffix})"
            )
            self._begin_replica_batch()
            try:
                self.upsert_data_quality_issues(
                    missing_parquet,
                    "MISSING_5MIN_PARQUET",
                    "Symbol exists in daily parquet but 5-min parquet is missing",
                )
            finally:
                self._end_replica_batch()
        if not build_symbols:
            _log("intraday_day_pack: no symbols with 5-min parquet found; nothing to build.")
            self._publish_replica(force=True)
            return 0

        # Build file manifest once — avoids 16K-file glob discovery per batch
        manifest = self._build_5min_file_manifest() if symbols is None else None

        # ── Resume mode ─────────────────────────────────────────────────────
        # Skip symbols already present in the table.  Used to continue a
        # build that was interrupted mid-way (each batch commits independently).
        if resume and self._table_exists("intraday_day_pack"):
            already_built = {
                r[0]
                for r in self.con.execute(
                    "SELECT DISTINCT symbol FROM intraday_day_pack"
                ).fetchall()
            }
            before = len(build_symbols)
            build_symbols = [s for s in build_symbols if s not in already_built]
            skipped = before - len(build_symbols)
            _log(
                f"  [pack] resume mode: {skipped:,} symbols already built,"
                f" {len(build_symbols):,} remaining"
            )
            if not build_symbols:
                n = self.con.execute("SELECT COUNT(*) FROM intraday_day_pack").fetchone()[0]
                _log(f"intraday_day_pack: all symbols already built ({n:,} rows). Nothing to do.")
                return n
        # ─────────────────────────────────────────────────────────────────────

        since_date_iso, until_date_iso = _prepare_date_window(since_date, until_date)
        window_filter_sql = _date_window_clause("date::DATE", since_date_iso, until_date_iso)
        # _date_window_clause embeds dates as SQL literals (no ? placeholders),
        # so window_params is always empty — the filtering is in window_filter_sql.
        window_params: list[object] = []

        # ── Incremental mode (--since) ────────────────────────────────────────
        # Skip DROP TABLE; just delete rows >= since_date and re-insert them.
        if since_date_iso and not force:
            table_exists = self._table_exists("intraday_day_pack")
            if table_exists:
                skip_n = _skip_if_table_fully_covered(
                    self.con,
                    table="intraday_day_pack",
                    date_col="trade_date",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    build_symbols=build_symbols,
                    label="pack",
                )
                if skip_n is not None:
                    return skip_n

                deleted = _incremental_delete(
                    self.con,
                    table="intraday_day_pack",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=build_symbols,
                    log_prefix="pack",
                )
                _log("  [pack] incremental mode: re-inserting...")
            else:
                _log(
                    f"  [pack] incremental mode: table missing, creating from scratch (since {since_date_iso})"
                )
        # ─────────────────────────────────────────────────────────────────────

        table_exists = self._table_exists("intraday_day_pack")
        if not force and not resume and since_date_iso is None and symbols is None and table_exists:
            n = self.con.execute("SELECT COUNT(*) FROM intraday_day_pack").fetchone()[0]
            if n > 0:
                _log(f"intraday_day_pack: {n:,} rows already built. Use force=True to rebuild.")
                return n

        if force:
            if symbols and table_exists:
                symbol_list = ",".join(f"'{s}'" for s in build_symbols)
                del_started = time.time()
                deleted = self.con.execute(
                    f"DELETE FROM intraday_day_pack WHERE symbol IN ({symbol_list})"
                ).rowcount
                _log(
                    f"  [pack] deleted {deleted:,} rows for {len(build_symbols)} target symbols"
                    f" in {time.time() - del_started:.2f}s"
                )
            else:
                drop_started = time.time()
                self._invalidate_metadata_caches()
                self.con.execute("DROP TABLE IF EXISTS intraday_day_pack")
                table_exists = False
                _log(f"  [pack] dropped existing table in {time.time() - drop_started:.2f}s")

        if not table_exists:
            self.con.execute("""
                CREATE TABLE intraday_day_pack (
                    symbol VARCHAR,
                    trade_date DATE,
                    minute_arr SMALLINT[],
                    open_arr DOUBLE[],
                    high_arr DOUBLE[],
                    low_arr DOUBLE[],
                    close_arr DOUBLE[],
                    volume_arr DOUBLE[],
                    rvol_baseline_arr DOUBLE[],
                    PRIMARY KEY (symbol, trade_date)
                )
            """)
        use_compact_schema = self._table_has_column("intraday_day_pack", "minute_arr")

        lookback = max(1, int(rvol_lookback_days))
        batch_size = max(1, int(batch_size))
        batches = self._iter_symbol_batches(build_symbols, batch_size)
        total_batches = len(batches)
        started = time.time()
        _log(
            "intraday_day_pack build start:"
            f" symbols={len(build_symbols):,} lookback={lookback}"
            f" batch_size={batch_size} batches={total_batches}"
            f" source={'manifest' if manifest is not None else 'parquet_globs'}"
        )

        phase_times = {
            "delete": 0.0,
            "source": 0.0,
            "insert": 0.0,
            "commit": 0.0,
            "index": 0.0,
        }

        # Execute each batch as an independent transaction so long builds can resume
        # from already-committed batches after a failure or interruption.
        for idx, batch in enumerate(batches, start=1):
            batch_started = time.time()
            batch_phase = "start"
            done_before = min((idx - 1) * batch_size, len(build_symbols))
            done_after = min(done_before + len(batch), len(build_symbols))
            tx_open = False
            _log(
                f"  [pack] batch {idx}/{total_batches} START"
                f" | symbols={done_before + 1:,}-{done_after:,}/{len(build_symbols):,}"
                f" | count={len(batch):,}"
                f" | first={batch[0]} last={batch[-1]}"
            )
            try:
                batch_phase = "begin"
                self.con.execute("BEGIN TRANSACTION")
                tx_open = True

                batch_phase = "delete"
                delete_started = time.time()
                _log(f"  [pack] batch {idx}/{total_batches} DELETE start")
                placeholders = ", ".join("?" for _ in batch)
                if since_date_iso:
                    delete_result = self.con.execute(
                        f"""
                        DELETE FROM intraday_day_pack
                        WHERE symbol IN ({placeholders})
                          AND trade_date >= ?::DATE
                        """,
                        [*batch, since_date_iso],
                    )
                elif symbols or force:
                    delete_result = self.con.execute(
                        f"DELETE FROM intraday_day_pack WHERE symbol IN ({placeholders})",
                        batch,
                    )
                else:
                    delete_result = None
                delete_elapsed = time.time() - delete_started
                phase_times["delete"] += delete_elapsed
                deleted_rows = delete_result.rowcount if delete_result is not None else 0
                deleted_display = f"{deleted_rows:,}" if deleted_rows >= 0 else "unknown"
                _log(
                    f"  [pack] batch {idx}/{total_batches} DELETE done"
                    f" | rows={deleted_display} | elapsed={delete_elapsed:.1f}s"
                )

                batch_phase = "source"
                source_started = time.time()
                _log(f"  [pack] batch {idx}/{total_batches} SOURCE start")
                source_sql = (
                    self._build_parquet_source_sql(batch, manifest=manifest)
                    if manifest is not None
                    else self._build_parquet_source_sql(batch)
                )
                source_elapsed = time.time() - source_started
                phase_times["source"] += source_elapsed
                _log(
                    f"  [pack] batch {idx}/{total_batches} SOURCE done"
                    f" | elapsed={source_elapsed:.1f}s"
                )
                batch_phase = "insert"
                insert_started = time.time()
                _log(f"  [pack] batch {idx}/{total_batches} INSERT start")
                if use_compact_schema:
                    self.con.execute(
                        f"""
                        INSERT INTO intraday_day_pack
                        WITH candles AS (
                            SELECT
                                symbol,
                                date::DATE AS trade_date,
                                candle_time,
                                (CAST(strftime(candle_time, '%H') AS SMALLINT) * 60)
                                    + CAST(strftime(candle_time, '%M') AS SMALLINT) AS candle_minute,
                                open,
                                high,
                                low,
                                close,
                                volume,
                                AVG(volume) OVER (
                                    PARTITION BY symbol, strftime(candle_time, '%H:%M')
                                    ORDER BY date
                                    ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
                                ) AS rvol_baseline
                            FROM {source_sql}
                            WHERE strftime(candle_time, '%H:%M') BETWEEN '09:15' AND '15:30'
                            {window_filter_sql}
                        )
                        SELECT
                            symbol,
                            trade_date,
                            LIST(candle_minute ORDER BY candle_time) AS minute_arr,
                            LIST(CAST(open AS DOUBLE) ORDER BY candle_time) AS open_arr,
                            LIST(CAST(high AS DOUBLE) ORDER BY candle_time) AS high_arr,
                            LIST(CAST(low AS DOUBLE) ORDER BY candle_time) AS low_arr,
                            LIST(CAST(close AS DOUBLE) ORDER BY candle_time) AS close_arr,
                            LIST(CAST(volume AS DOUBLE) ORDER BY candle_time) AS volume_arr,
                            LIST(CAST(rvol_baseline AS DOUBLE) ORDER BY candle_time) AS rvol_baseline_arr
                        FROM candles
                        GROUP BY symbol, trade_date
                    """,
                        window_params,
                    )
                else:
                    self.con.execute(
                        f"""
                        INSERT INTO intraday_day_pack
                        WITH candles AS (
                            SELECT
                                symbol,
                                date::DATE AS trade_date,
                                candle_time,
                                strftime(candle_time, '%H:%M') AS time_str,
                                open,
                                high,
                                low,
                                close,
                                volume,
                                AVG(volume) OVER (
                                    PARTITION BY symbol, strftime(candle_time, '%H:%M')
                                    ORDER BY date
                                    ROWS BETWEEN {lookback} PRECEDING AND 1 PRECEDING
                                ) AS rvol_baseline
                            FROM {source_sql}
                            WHERE strftime(candle_time, '%H:%M') BETWEEN '09:15' AND '15:30'
                            {window_filter_sql}
                        )
                        SELECT
                            symbol,
                            trade_date,
                            LIST(time_str ORDER BY candle_time) AS time_arr,
                            LIST(open ORDER BY candle_time) AS open_arr,
                            LIST(high ORDER BY candle_time) AS high_arr,
                            LIST(low ORDER BY candle_time) AS low_arr,
                            LIST(close ORDER BY candle_time) AS close_arr,
                            LIST(volume ORDER BY candle_time) AS volume_arr,
                            LIST(rvol_baseline ORDER BY candle_time) AS rvol_baseline_arr
                        FROM candles
                        GROUP BY symbol, trade_date
                    """,
                        window_params,
                    )
                insert_elapsed = time.time() - insert_started
                phase_times["insert"] += insert_elapsed
                _log(
                    f"  [pack] batch {idx}/{total_batches} INSERT done"
                    f" | elapsed={insert_elapsed:.1f}s"
                )

                batch_phase = "commit"
                commit_started = time.time()
                _log(f"  [pack] batch {idx}/{total_batches} COMMIT start")
                self.con.execute("COMMIT")
                commit_elapsed = time.time() - commit_started
                phase_times["commit"] += commit_elapsed
                tx_open = False
                _log(
                    f"  [pack] batch {idx}/{total_batches} COMMIT done"
                    f" | elapsed={commit_elapsed:.1f}s"
                )
            except Exception as e:
                if tx_open:
                    self.con.execute("ROLLBACK")
                _log(f"  [pack] batch {idx}/{total_batches} FAILED phase={batch_phase}")
                logger.exception("Failed while building intraday_day_pack batch: %s", e)
                raise

            batch_elapsed = time.time() - batch_started
            elapsed = time.time() - started
            avg_per_batch = elapsed / idx
            remaining_batches = total_batches - idx
            eta_s = avg_per_batch * remaining_batches
            eta_min = eta_s / 60
            _log(
                f"  [pack] batch {idx}/{total_batches} DONE"
                f" | symbols={done_after:,}/{len(build_symbols):,}"
                f" | batch={batch_elapsed:.1f}s"
                f" | elapsed={elapsed:.0f}s"
                f" | ETA={eta_min:.1f}min"
            )

        _log("  [pack] index build start...")
        index_started = time.time()
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_intraday_day_pack ON intraday_day_pack(symbol, trade_date)"
        )
        try:
            self.con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_intraday_day_pack_unique "
                "ON intraday_day_pack(symbol, trade_date)"
            )
        except Exception as e:
            logger.warning(
                "Could not enforce intraday_day_pack uniqueness; existing duplicates may need cleanup: %s",
                e,
            )
        phase_times["index"] = time.time() - index_started
        _log(f"  [pack] index build done in {phase_times['index']:.2f}s")
        n = self.con.execute("SELECT COUNT(*) FROM intraday_day_pack").fetchone()[0]
        elapsed = time.time() - started
        _log(f"intraday_day_pack built: {n:,} rows in {elapsed:.1f}s")
        _log(
            "intraday_day_pack phase timings:"
            f" delete={phase_times['delete']:.2f}s"
            f" source={phase_times['source']:.2f}s"
            f" insert={phase_times['insert']:.2f}s"
            f" commit={phase_times['commit']:.2f}s"
            f" index={phase_times['index']:.2f}s"
        )
        self._publish_replica(force=True)
        return n

    def build_all(
        self,
        force: bool = False,
        symbols: list[str] | None = None,
        atr_periods: int = 12,
        cpr_percentile: float = 50.0,
        atr_batch_size: int | None = None,
        pack_batch_size: int = 64,
        pack_rvol_lookback_days: int = 10,
        pack_since_date: str | None = None,
        since_date: str | None = None,
        until_date: str | None = None,
    ) -> None:
        """Build all materialized tables. Run after pivot-convert.
        symbols: if provided, only build for these symbols (faster for testing).
        since_date: if provided, incremental refresh for all tables (trade_date >= since_date).
        until_date: optional upper bound for a bounded refresh window.
        pack_since_date: legacy alias -- if since_date is set, it takes precedence.

        Tables built:
            cpr_daily          -- CPR levels per symbol per trading day
            atr_intraday       -- ATR per trading day (from prior-day intraday candles)
            cpr_thresholds     -- Rolling Pxx CPR width threshold per symbol
            or_daily           -- Opening-range slot aggregates
            market_day_state   -- Runtime setup state (single-row-per-day contract)
            strategy_day_state -- Strategy-specific derived setup state
            intraday_day_pack  -- Runtime candle arrays (single-row-per-day contract)
            virgin_cpr_flags   -- Virgin CPR markers (uses intraday_day_pack if available)

        Note: virgin_cpr_flags is built AFTER intraday_day_pack so it can use
        the materialized arrays instead of scanning 175M v_5min rows.
        """
        # since_date takes precedence; fall back to pack_since_date for backward compat
        effective_since = since_date or pack_since_date
        effective_until = until_date

        print("Building runtime materialized tables...")
        if effective_since:
            if effective_until:
                print(
                    f"Incremental refresh: trade_date between {effective_since} and {effective_until}"
                )
            else:
                print(f"Incremental refresh: trade_date >= {effective_since}")
        if symbols:
            print(
                f"Limited to {len(symbols)} symbols: "
                f"{', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}"
            )
        self._begin_replica_batch()
        try:
            self.build_cpr_table(
                force=force, symbols=symbols, since_date=effective_since, until_date=effective_until
            )
            self.build_atr_table(
                periods=atr_periods,
                force=force,
                symbols=symbols,
                batch_size=atr_batch_size,
                since_date=effective_since,
                until_date=effective_until,
            )
            # Flush WAL after ATR build (largest batch-insert stage)
            try:
                self.con.execute("CHECKPOINT")
            except Exception as e:
                logger.debug("Post-ATR CHECKPOINT failed (best-effort): %s", e)
            self.build_cpr_thresholds(
                percentile=cpr_percentile,
                force=force,
                symbols=symbols,
                since_date=effective_since,
                until_date=effective_until,
            )
            self.build_or_table(
                force=force, symbols=symbols, since_date=effective_since, until_date=effective_until
            )
            self.build_market_day_state(
                force=force, symbols=symbols, since_date=effective_since, until_date=effective_until
            )
            self.build_strategy_day_state(
                force=force, symbols=symbols, since_date=effective_since, until_date=effective_until
            )
            self.build_intraday_day_pack(
                force=force,
                symbols=symbols,
                rvol_lookback_days=pack_rvol_lookback_days,
                batch_size=pack_batch_size,
                since_date=effective_since,
                until_date=effective_until,
            )
            # Flush WAL after pack build (second largest batch-insert stage)
            try:
                self.con.execute("CHECKPOINT")
            except Exception as e:
                logger.debug("Post-pack CHECKPOINT failed (best-effort): %s", e)
            # Build virgin_cpr_flags AFTER intraday_day_pack so it can use materialized arrays
            # instead of scanning 175M v_5min rows (15-30 min faster on full builds)
            self.build_virgin_cpr_flags(
                force=force, symbols=symbols, since_date=effective_since, until_date=effective_until
            )
            self._build_dataset_meta()
            # Flush WAL to main file — prevents 5-30s hang on next get_db() call
            try:
                self.con.execute("CHECKPOINT")
            except Exception as e:
                logger.debug("DuckDB CHECKPOINT failed (best-effort): %s", e)
            self._invalidate_metadata_caches()
        finally:
            self._end_replica_batch()
        self._publish_replica(force=True)
        print("Done -- market.duckdb is ready for backtesting.")

    def drop_and_rebuild(self) -> None:
        """Drop all materialized tables and rebuild from Parquet. Use after importing new data."""
        print("Dropping and rebuilding all materialized tables...")
        for table in [
            "cpr_daily",
            "atr_intraday",
            "cpr_thresholds",
            "backtest_results",
            "virgin_cpr_flags",
            "or_daily",
            "market_day_state",
            "strategy_day_state",
            "intraday_day_pack",
            "dataset_meta",
            "data_quality_issues",
            "backtest_universe",
            "run_daily_pnl",
            "run_metrics",
        ]:
            self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self._invalidate_metadata_caches()
        self.build_all(force=True)

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


def close_dashboard_db() -> None:
    global _dashboard_db
    if _dashboard_db is not None:
        _dashboard_db.close()
        _dashboard_db = None
