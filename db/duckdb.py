"""
DuckDB connection and market data layer.

DuckDB handles ALL market data analytics:
  - 5-min OHLCV candles  → queried directly from Parquet (zero import)
  - Daily OHLCV          → queried directly from Parquet
  - CPR levels           → pre-computed materialized table (built once)
  - ATR values           → pre-computed materialized table (built once)
  - Backtest results     → stored in DuckDB file

PostgreSQL handles ONLY: agent_sessions, signals, alert_log.

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
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from db.replica import ReplicaSync
from db.replica_consumer import ReplicaConsumer
from engine.constants import parse_iso_date

logger = logging.getLogger(__name__)

# Symbol name validation: NSE symbols are uppercase letters, digits, spaces, &, ., and - (e.g., M&M, BAJAJ-AUTO, JK AGRI)
_SYMBOL_RE = re.compile(r"^[A-Z0-9& .-]{1,32}$")
_UNIVERSE_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

# Threshold for using simple DELETE+INSERT vs complex temp table rebuild
# For small symbol sets, DELETE+INSERT is faster than temp table pattern
_INCREMENTAL_BUILD_THRESHOLD = 100


def _validate_symbols(symbols: list[str]) -> list[str]:
    """Validate symbol names against a strict regex to prevent SQL injection."""
    for s in symbols:
        if not isinstance(s, str):
            raise ValueError(f"Invalid symbol name type: {type(s)!r}")
        if not _SYMBOL_RE.match(s):
            raise ValueError(f"Invalid symbol name: '{s}'. Must match {_SYMBOL_RE.pattern}")
        # Defense-in-depth: explicitly reject SQL meta characters/patterns.
        if any(token in s for token in ("'", '"', "\\", ";", "--", "/*", "*/")):
            raise ValueError(f"Invalid symbol name: '{s}'. Contains forbidden characters")
    return symbols


def _validate_universe_name(name: str) -> str:
    """Validate saved universe names used in metadata table."""
    if not _UNIVERSE_RE.match(name):
        raise ValueError(f"Invalid universe name: '{name}'. Must match {_UNIVERSE_RE.pattern}")
    return name


def _date_window_clause(
    column: str,
    since_date: str | None = None,
    until_date: str | None = None,
) -> str:
    """Return an AND clause for an inclusive date window."""
    clauses: list[str] = []
    if since_date:
        clauses.append(f"{column} >= '{since_date}'::DATE")
    if until_date:
        clauses.append(f"{column} <= '{until_date}'::DATE")
    return f"AND {' AND '.join(clauses)}" if clauses else ""


def _prepare_date_window(
    since_date: str | None,
    until_date: str | None,
) -> tuple[str | None, str | None]:
    """Normalize and return (since_iso, until_iso) for incremental builds."""
    return (
        parse_iso_date(since_date) if since_date else None,
        parse_iso_date(until_date) if until_date else None,
    )


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DUCKDB_FILE = DATA_DIR / "market.duckdb"
REPLICA_DIR = DATA_DIR / "market_replica"


def _sql_symbol_list(symbols: list[str]) -> str:
    """Build a quoted, comma-separated SQL symbol list for IN clauses."""
    return ",".join(f"'{s}'" for s in symbols)


def _incremental_delete(
    con: Any,
    *,
    table: str,
    since_date: str,
    until_date: str | None = None,
    symbols: list[str] | None = None,
    log_prefix: str,
) -> int:
    """Delete rows matching the incremental window and return deleted count."""
    delete_parts = ["trade_date >= ?::DATE"]
    delete_params: list[object] = [since_date]
    if until_date:
        delete_parts.append("trade_date <= ?::DATE")
        delete_params.append(until_date)
    if symbols:
        delete_parts.append(f"symbol IN ({_sql_symbol_list(symbols)})")
    deleted = con.execute(
        f"DELETE FROM {table} WHERE " + " AND ".join(delete_parts),
        delete_params,
    ).rowcount
    print(
        f"  [{log_prefix}] incremental: deleted {deleted:,} rows"
        f" for trade_date >= {since_date}" + (f" and <= {until_date}" if until_date else "")
    )
    return deleted


def _symbol_scoped_upsert(
    con: Any,
    *,
    table: str,
    select_sql: str,
    symbols: list[str],
) -> None:
    """Upsert rows for a symbol subset using DELETE+INSERT (small) or temp-table swap (large).

    Wraps the operation in a transaction. Uses simple DELETE+INSERT for fewer than
    _INCREMENTAL_BUILD_THRESHOLD symbols, and a temp-table keep/refresh/swap pattern
    for larger sets to avoid scanning the entire table.
    """
    symbol_list = _sql_symbol_list(symbols)
    use_simple_path = len(symbols) < _INCREMENTAL_BUILD_THRESHOLD
    con.execute("BEGIN TRANSACTION")
    tx_open = True
    try:
        if use_simple_path:
            con.execute(f"DELETE FROM {table} WHERE symbol IN ({symbol_list})")
            con.execute(f"INSERT INTO {table} {select_sql}")
        else:
            con.execute(f"DROP TABLE IF EXISTS tmp_{table}_keep")
            con.execute(f"DROP TABLE IF EXISTS tmp_{table}_refresh")
            con.execute(
                f"""
                CREATE TEMP TABLE tmp_{table}_keep AS
                SELECT * FROM {table} WHERE symbol NOT IN ({symbol_list})
                """
            )
            con.execute(f"CREATE TEMP TABLE tmp_{table}_refresh AS {select_sql}")
            con.execute(f"DROP TABLE {table}")
            con.execute(f"""
                CREATE TABLE {table} AS
                SELECT * FROM tmp_{table}_keep
                UNION ALL
                SELECT * FROM tmp_{table}_refresh
            """)
            con.execute(f"DROP TABLE tmp_{table}_keep")
            con.execute(f"DROP TABLE tmp_{table}_refresh")
        con.execute("COMMIT")
        tx_open = False
    except Exception as e:
        if tx_open:
            con.execute("ROLLBACK")
        logger.exception("Failed to refresh %s for symbol subset: %s", table, e)
        raise


def _is_pid_alive(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        process = kernel32.OpenProcess(0x1000, False, pid)
        if process:
            kernel32.CloseHandle(process)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_lock_payload(lock_path: Path) -> dict[str, object] | None:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"pid": int(raw)}
        except ValueError:
            return None
    return payload if isinstance(payload, dict) else None


def _acquire_write_lock(lock_path: Path) -> None:
    """Acquire a PID-based write lock, failing fast if another writer is alive."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": os.getpid(),
        "acquired_at": time.time(),
        "lock_path": str(lock_path),
    }
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing = _read_lock_payload(lock_path) or {}
            existing_pid = existing.get("pid")
            if isinstance(existing_pid, int) and _is_pid_alive(existing_pid):
                kill_cmd = (
                    f"taskkill //F //PID {existing_pid}"
                    if os.name == "nt"
                    else f"kill {existing_pid}"
                )
                raise SystemExit(
                    f"Another DuckDB write process is running (PID {existing_pid}).\n"
                    f"Kill it:  {kill_cmd}\n"
                    "Only one write connection is allowed at a time."
                ) from None
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise SystemExit(
                    f"Failed to clear stale DuckDB write lock at {lock_path}: {exc}"
                ) from exc
            continue
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
                handle.flush()
            return
        except Exception:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise


def _release_write_lock(lock_path: Path) -> None:
    """Release the write lock if it belongs to this process."""
    try:
        payload = _read_lock_payload(lock_path) or {}
        if payload.get("pid") == os.getpid():
            lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def _skip_if_table_fully_covered(
    con: Any,
    *,
    table: str,
    date_col: str,
    since_date: str,
    until_date: str | None,
    build_symbols: list[str],
    label: str,
) -> int | None:
    """Check if *table* already covers all parquet dates for the given window.

    Returns the table's total row count when fully covered (caller should
    return early).  Returns ``None`` when the table is partial or empty
    (caller should proceed with rebuild).
    """
    until_filter = f" AND {date_col} <= '{until_date}'::DATE" if until_date else ""
    parquet_until = f" AND date::DATE <= '{until_date}'::DATE" if until_date else ""

    table_dates = int(
        con.execute(
            f"SELECT COUNT(DISTINCT {date_col}) FROM {table}"
            f" WHERE {date_col} >= '{since_date}'::DATE{until_filter}"
        ).fetchone()[0]
        or 0
    )
    parquet_dates = int(
        con.execute(
            f"SELECT COUNT(DISTINCT date::DATE) FROM v_5min"
            f" WHERE date::DATE >= '{since_date}'::DATE{parquet_until}"
        ).fetchone()[0]
        or 0
    )

    if parquet_dates == 0 or table_dates < parquet_dates:
        return None

    threshold = max(1, int(len(build_symbols) * 0.99))
    min_syms = int(
        con.execute(
            f"SELECT MIN(cnt) FROM ("
            f"  SELECT COUNT(DISTINCT symbol) AS cnt FROM {table}"
            f"  WHERE {date_col} >= '{since_date}'::DATE{until_filter}"
            f"  GROUP BY {date_col}"
            f") t"
        ).fetchone()[0]
        or 0
    )

    if min_syms >= threshold:
        n = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        print(
            f"  [{label}] already covers all {parquet_dates} dates since"
            f" {since_date} (min {min_syms:,} symbols/date,"
            f" {n:,} total rows). Skipping rebuild. Use --force to override.",
            flush=True,
        )
        return n

    print(
        f"  [{label}] partial coverage: {table_dates} dates present but"
        f" min symbols/date={min_syms:,} < threshold={threshold:,}. Rebuilding.",
        flush=True,
    )
    return None


class MarketDB:
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
    ) -> int:
        """
        Pre-compute CPR levels + floor pivot levels for every trading day.

        Uses daily Parquet (more reliable than deriving from 5-min).
        CPR for trade_date = previous day's OHLC.

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
        target_symbols = sorted(set(_validate_symbols(symbols))) if symbols else None
        symbol_filter_sql = ""
        if target_symbols:
            symbol_filter_sql = f"AND symbol IN ({_sql_symbol_list(target_symbols)})"
        window_filter_sql = _date_window_clause("trade_date", since_date_iso, until_date_iso)
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
                    LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS trade_date
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
                _incremental_delete(
                    self.con,
                    table="cpr_daily",
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    log_prefix="cpr",
                )
                self.con.execute(f"INSERT INTO cpr_daily {insert_sql}")
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
        target_symbols = (
            sorted(_validate_symbols(symbols))
            if symbols
            else [
                row[0]
                for row in self.con.execute(
                    "SELECT DISTINCT symbol FROM v_5min ORDER BY symbol"
                ).fetchall()
            ]
        )
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
        ) -> str:
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
                    FROM v_5min
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
                    FROM v_5min
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
                batch_sql = _build_batch_sql(
                    batch, trade_date_since=since_date_iso, trade_date_until=until_date_iso
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
            batch_sql = _build_batch_sql(
                target_symbols, trade_date_since=since_date_iso, trade_date_until=until_date_iso
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
                JOIN atr_intraday a
                  ON a.symbol = c.symbol AND a.trade_date = c.trade_date
                LEFT JOIN cpr_thresholds t
                  ON t.symbol = c.symbol AND t.trade_date = c.trade_date
                LEFT JOIN or_daily o
                  ON o.symbol = c.symbol AND o.trade_date = c.trade_date
                {"LEFT JOIN virgin_cpr_flags v ON v.symbol = c.symbol AND v.trade_date = c.prev_date" if virgin_exists else ""}
                WHERE o.c0915 IS NOT NULL
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
        for table in tables:
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

    def _build_parquet_source_sql(self, symbols: list[str], *, prefer_view: bool = False) -> str:
        """Build a batch source SQL for intraday_day_pack."""
        if not symbols:
            raise RuntimeError("No symbols resolved for intraday_day_pack batch")

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
                    rvol_baseline_arr DOUBLE[]
                )
            """)
        use_compact_schema = self._table_has_column("intraday_day_pack", "minute_arr")

        lookback = max(1, int(rvol_lookback_days))
        batch_size = max(1, int(batch_size))
        batches = self._iter_symbol_batches(build_symbols, batch_size)
        total_batches = len(batches)
        log_every = 1 if total_batches <= 2 else 2
        started = time.time()
        prefer_view_source = symbols is None
        _log(
            "intraday_day_pack build start:"
            f" symbols={len(build_symbols):,} lookback={lookback}"
            f" batch_size={batch_size} batches={total_batches}"
            f" source={'v_5min' if prefer_view_source else 'parquet_globs'}"
        )

        phase_times = {
            "delete": 0.0,
            "insert": 0.0,
            "index": 0.0,
        }

        # Execute each batch as an independent transaction so long builds can resume
        # from already-committed batches after a failure or interruption.
        for idx, batch in enumerate(batches, start=1):
            batch_started = time.time()
            tx_open = False
            try:
                self.con.execute("BEGIN TRANSACTION")
                tx_open = True

                # Refresh only the requested symbols when doing a partial build.
                if symbols:
                    delete_started = time.time()
                    placeholders = ", ".join("?" for _ in batch)
                    if since_date_iso:
                        self.con.execute(
                            f"""
                            DELETE FROM intraday_day_pack
                            WHERE symbol IN ({placeholders})
                              AND trade_date >= ?::DATE
                            """,
                            [*batch, since_date_iso],
                        )
                    else:
                        self.con.execute(
                            f"DELETE FROM intraday_day_pack WHERE symbol IN ({placeholders})",
                            batch,
                        )
                    phase_times["delete"] += time.time() - delete_started

                source_sql = self._build_parquet_source_sql(batch, prefer_view=prefer_view_source)
                insert_started = time.time()
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
                phase_times["insert"] += time.time() - insert_started

                self.con.execute("COMMIT")
                tx_open = False
            except Exception as e:
                if tx_open:
                    self.con.execute("ROLLBACK")
                logger.exception("Failed while building intraday_day_pack batch: %s", e)
                raise

            batch_elapsed = time.time() - batch_started
            if idx == 1 or idx == total_batches or idx % log_every == 0:
                elapsed = time.time() - started
                done = min(idx * batch_size, len(build_symbols))
                avg_per_batch = elapsed / idx
                remaining_batches = total_batches - idx
                eta_s = avg_per_batch * remaining_batches
                eta_min = eta_s / 60
                _log(
                    f"  [pack] batch {idx}/{total_batches}"
                    f" | symbols={done:,}/{len(build_symbols):,}"
                    f" | batch={batch_elapsed:.1f}s"
                    f" | elapsed={elapsed:.0f}s"
                    f" | ETA={eta_min:.1f}min"
                )

        _log("  [pack] index build start...")
        index_started = time.time()
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_intraday_day_pack ON intraday_day_pack(symbol, trade_date)"
        )
        phase_times["index"] = time.time() - index_started
        _log(f"  [pack] index build done in {phase_times['index']:.2f}s")
        n = self.con.execute("SELECT COUNT(*) FROM intraday_day_pack").fetchone()[0]
        elapsed = time.time() - started
        _log(f"intraday_day_pack built: {n:,} rows in {elapsed:.1f}s")
        _log(
            "intraday_day_pack phase timings:"
            f" delete={phase_times['delete']:.2f}s"
            f" insert={phase_times['insert']:.2f}s"
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

    def ensure_data_quality_table(self) -> None:
        """Create data quality issue registry table if it does not exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_issues (
                symbol VARCHAR,
                issue_code VARCHAR,
                severity VARCHAR DEFAULT 'WARNING',
                details VARCHAR,
                is_active BOOLEAN DEFAULT TRUE,
                first_seen TIMESTAMP DEFAULT now(),
                last_seen TIMESTAMP DEFAULT now(),
                PRIMARY KEY (symbol, issue_code)
            )
        """)
        # Migrate: add severity column if the table was created without it
        try:
            self.con.execute(
                "ALTER TABLE data_quality_issues ADD COLUMN IF NOT EXISTS severity VARCHAR DEFAULT 'WARNING'"
            )
        except Exception:
            pass

    def upsert_data_quality_issues(
        self,
        symbols: list[str],
        issue_code: str,
        details: str,
        severity: str = "WARNING",
    ) -> int:
        """Insert or reactivate data quality issues for symbols."""
        target = sorted(set(_validate_symbols(symbols)))
        if not target:
            return 0

        self.ensure_data_quality_table()
        payload = pl.DataFrame(
            {
                "symbol": target,
                "issue_code": [issue_code] * len(target),
                "severity": [severity] * len(target),
                "details": [details] * len(target),
            }
        )
        self.con.register("_tmp_dq_issues", payload.to_arrow())
        try:
            self.con.execute(
                """
                INSERT INTO data_quality_issues
                    (symbol, issue_code, severity, details, is_active, first_seen, last_seen)
                SELECT
                    symbol,
                    issue_code,
                    severity,
                    details,
                    TRUE,
                    now(),
                    now()
                FROM _tmp_dq_issues
                ON CONFLICT (symbol, issue_code)
                DO UPDATE SET
                    severity = excluded.severity,
                    details = excluded.details,
                    is_active = TRUE,
                    last_seen = now()
                """
            )
        finally:
            self.con.unregister("_tmp_dq_issues")
        self._publish_replica(force=True)
        return len(target)

    def deactivate_data_quality_issue(self, issue_code: str, keep_symbols: list[str]) -> int:
        """Deactivate issue rows that are no longer present in current scan results."""
        self.ensure_data_quality_table()
        keep = sorted(set(_validate_symbols(keep_symbols))) if keep_symbols else []
        if keep:
            keep_sql = ",".join(f"'{s}'" for s in keep)
            self.con.execute(
                "UPDATE data_quality_issues "
                "SET is_active = FALSE, last_seen = now() "
                "WHERE issue_code = ? AND is_active = TRUE "
                f"AND symbol NOT IN ({keep_sql})",
                [issue_code],
            )
        else:
            self.con.execute(
                "UPDATE data_quality_issues "
                "SET is_active = FALSE, last_seen = now() "
                "WHERE issue_code = ? AND is_active = TRUE",
                [issue_code],
            )

        row = self.con.execute(
            "SELECT COUNT(*) FROM data_quality_issues WHERE issue_code = ? AND is_active = TRUE",
            [issue_code],
        ).fetchone()
        active = int(row[0]) if row and row[0] is not None else 0
        self._publish_replica(force=True)
        return active

    def refresh_data_quality_issues(self) -> dict[str, int]:
        """Refresh active issue registry using current dataset state."""
        self._begin_replica_batch()
        try:
            self.ensure_data_quality_table()
            issue_code = "MISSING_5MIN_PARQUET"

            if not self._has_daily:
                self.deactivate_data_quality_issue(issue_code, keep_symbols=[])
                return {"missing_5min": 0, "active_issues": 0}

            # Use cpr_daily (materialized, sub-ms) when available, fallback to v_daily
            if self._table_exists("cpr_daily"):
                daily_rows = self.con.execute("SELECT DISTINCT symbol FROM cpr_daily").fetchall()
            else:
                daily_rows = self.con.execute("SELECT DISTINCT symbol FROM v_daily").fetchall()
            daily_symbols = {r[0] for r in daily_rows if r and r[0]}

            min_symbols: set[str] = set()
            if self._has_5min:
                base_dir = self._parquet_dir / "5min"
                if base_dir.exists():
                    for entry in base_dir.iterdir():
                        if entry.is_dir() and any(entry.glob("*.parquet")):
                            min_symbols.add(entry.name)

            missing_5min = sorted(daily_symbols - min_symbols)
            self.upsert_data_quality_issues(
                missing_5min,
                issue_code,
                "Symbol exists in daily parquet but 5-min parquet is missing",
            )
            active = self.deactivate_data_quality_issue(issue_code, keep_symbols=missing_5min)
            return {"missing_5min": len(missing_5min), "active_issues": active}
        finally:
            self._end_replica_batch()
            # Skip replica publish here — the caller (CLI or comprehensive scan)
            # handles it.  Publishing now would block for a 5.6 GB copy only to
            # be immediately followed by another copy after run_comprehensive_dq_scan.
            self._sync.mark_dirty() if self._sync else None

    def run_comprehensive_dq_scan(self) -> dict[str, int]:
        """Run comprehensive 5-min data quality checks and store results in data_quality_issues.

        Checks performed (2 consolidated SQL passes over v_5min):

        Pass 1 — candle-level checks (single scan, all computed in parallel):
        - OHLC_VIOLATION    : high < low, high < open/close, low > open/close
        - NULL_PRICE        : any OHLC column is null
        - ZERO_PRICE        : open or close = 0
        - TIMESTAMP_INVALID : candle_time outside 09:15-15:30 IST window
        - EXTREME_CANDLE    : candle range (high-low) > 50% of open (likely bad data)

        Pass 2 — day-level aggregation checks (one scan, parallel aggregations):
        - DUPLICATE_CANDLE  : duplicate (symbol, date, candle_time) rows
        - DATE_GAP          : gap > 7 calendar days between consecutive trading dates
        - ZERO_VOLUME_DAY   : full trading day with zero total volume

        Returns a dict {issue_code: affected_symbol_count}.
        Caller is responsible for the DB write lock.
        """
        if not self._has_5min:
            return {}

        self.ensure_data_quality_table()
        summary: dict[str, int] = {}

        def _upsert_batch(
            results: dict[str, list[tuple]],
            severities: dict[str, str],
            detail_fns: dict[str, Callable[[int, str], str]],
        ) -> None:
            """Upsert a batch of check results and deactivate resolved rows."""
            self._begin_replica_batch()
            try:
                for code, rows in results.items():
                    affected: list[str] = []
                    for row in rows:
                        sym = str(row[0]) if row[0] else None
                        if not sym:
                            continue
                        cnt = int(row[1]) if row[1] is not None else 0
                        extra = str(row[2]) if len(row) > 2 and row[2] is not None else ""
                        if cnt == 0:
                            continue
                        affected.append(sym)
                        fn = detail_fns[code]
                        self.upsert_data_quality_issues(
                            [sym],
                            code,
                            fn(cnt, extra),
                            severity=severities[code],  # type: ignore[operator]
                        )
                    self.deactivate_data_quality_issue(code, keep_symbols=affected)
                    summary[code] = len(affected)
            finally:
                self._end_replica_batch()

        # ── Pass 1: candle-level checks ─────────────────────────────────────
        # All checks computed in ONE scan of v_5min.  DuckDB vectorises the
        # CASE expressions across columns in a single physical table scan.
        print("  Pass 1/2: scanning candles for OHLC/null/zero/timestamp/extreme...", flush=True)
        try:
            pass1_rows = self.con.execute("""
                SELECT
                    symbol,
                    -- OHLC violations
                    SUM(CASE WHEN high < low
                              OR (open  > 0 AND high < open)
                              OR (close > 0 AND high < close)
                              OR (open  > 0 AND low  > open)
                              OR (close > 0 AND low  > close)
                         THEN 1 ELSE 0 END) AS ohlc_cnt,
                    MIN(CASE WHEN high < low
                              OR (open  > 0 AND high < open)
                              OR (close > 0 AND high < close)
                              OR (open  > 0 AND low  > open)
                              OR (close > 0 AND low  > close)
                         THEN date END)::VARCHAR AS ohlc_first,
                    -- Null prices
                    SUM(CASE WHEN open IS NULL OR high IS NULL
                                  OR low IS NULL OR close IS NULL
                         THEN 1 ELSE 0 END) AS null_cnt,
                    MIN(CASE WHEN open IS NULL OR high IS NULL
                                  OR low IS NULL OR close IS NULL
                         THEN date END)::VARCHAR AS null_first,
                    -- Zero prices
                    SUM(CASE WHEN open = 0 OR close = 0 OR high = 0
                         THEN 1 ELSE 0 END) AS zero_cnt,
                    MIN(CASE WHEN open = 0 OR close = 0 OR high = 0
                         THEN date END)::VARCHAR AS zero_first,
                    -- Invalid timestamps (outside 09:15-15:30 IST)
                    SUM(CASE WHEN HOUR(candle_time) < 9
                                  OR HOUR(candle_time) > 15
                                  OR (HOUR(candle_time) = 9  AND MINUTE(candle_time) < 15)
                                  OR (HOUR(candle_time) = 15 AND MINUTE(candle_time) > 30)
                         THEN 1 ELSE 0 END) AS ts_cnt,
                    MIN(CASE WHEN HOUR(candle_time) < 9
                                  OR HOUR(candle_time) > 15
                                  OR (HOUR(candle_time) = 9  AND MINUTE(candle_time) < 15)
                                  OR (HOUR(candle_time) = 15 AND MINUTE(candle_time) > 30)
                         THEN candle_time END)::VARCHAR AS ts_example,
                    -- Extreme candle range (H-L > 50% of open)
                    SUM(CASE WHEN open > 0 AND (high - low) / open > 0.5
                         THEN 1 ELSE 0 END) AS extreme_cnt,
                    ROUND(MAX(CASE WHEN open > 0 THEN (high - low) / open END) * 100, 1)
                         ::VARCHAR AS extreme_max_pct
                FROM v_5min
                GROUP BY symbol
            """).fetchall()
        except Exception as exc:
            logger.warning("DQ pass-1 scan failed: %s", exc)
            pass1_rows = []

        p1_results: dict[str, list[tuple]] = {
            "OHLC_VIOLATION": [],
            "NULL_PRICE": [],
            "ZERO_PRICE": [],
            "TIMESTAMP_INVALID": [],
            "EXTREME_CANDLE": [],
        }
        for row in pass1_rows:
            sym = row[0]
            if row[1]:
                p1_results["OHLC_VIOLATION"].append((sym, row[1], row[2]))
            if row[3]:
                p1_results["NULL_PRICE"].append((sym, row[3], row[4]))
            if row[5]:
                p1_results["ZERO_PRICE"].append((sym, row[5], row[6]))
            if row[7]:
                p1_results["TIMESTAMP_INVALID"].append((sym, row[7], row[8]))
            if row[9]:
                p1_results["EXTREME_CANDLE"].append((sym, row[9], row[10]))

        print("  Pass 1/2: upserting results...", flush=True)
        _upsert_batch(
            p1_results,
            severities={
                "OHLC_VIOLATION": "CRITICAL",
                "NULL_PRICE": "CRITICAL",
                "ZERO_PRICE": "WARNING",
                "TIMESTAMP_INVALID": "CRITICAL",
                "EXTREME_CANDLE": "WARNING",
            },
            detail_fns={
                "OHLC_VIOLATION": lambda c, d: (
                    f"{c} candles with H<L or price outside H/L (first: {d})"
                ),
                "NULL_PRICE": lambda c, d: f"{c} candles with null OHLC (first: {d})",
                "ZERO_PRICE": lambda c, d: f"{c} candles with zero open/close/high (first: {d})",
                "TIMESTAMP_INVALID": lambda c, ex: (
                    f"{c} candles outside 09:15-15:30 IST (e.g. {ex})"
                ),
                "EXTREME_CANDLE": lambda c, mx: f"{c} candles with range >50% of open (max: {mx}%)",
            },
        )

        # ── Pass 2: day-level checks ─────────────────────────────────────────
        # One scan computing DISTINCT (symbol, date) pairs with aggregations.
        print("  Pass 2/2: scanning for duplicates/date-gaps/zero-volume...", flush=True)
        try:
            pass2_rows = self.con.execute("""
                WITH day_stats AS (
                    SELECT
                        symbol,
                        date,
                        COUNT(*) AS candle_count,
                        SUM(CASE WHEN volume = 0 THEN 1 ELSE 0 END) AS zero_vol_candles,
                        SUM(volume) AS day_vol,
                        COUNT(*) - COUNT(DISTINCT candle_time) AS dup_candles
                    FROM v_5min
                    GROUP BY symbol, date
                ),
                sym_stats AS (
                    SELECT
                        symbol,
                        -- duplicate candles
                        SUM(CASE WHEN dup_candles > 0 THEN 1 ELSE 0 END) AS dup_days,
                        MIN(CASE WHEN dup_candles > 0 THEN date END)::VARCHAR AS dup_first,
                        -- zero volume days
                        SUM(CASE WHEN day_vol = 0 THEN 1 ELSE 0 END) AS zero_vol_days,
                        MIN(CASE WHEN day_vol = 0 THEN date END)::VARCHAR AS zero_vol_first
                    FROM day_stats
                    GROUP BY symbol
                ),
                gap_stats AS (
                    SELECT
                        symbol,
                        COUNT(*) AS gap_count,
                        MAX(gap_days)::VARCHAR AS max_gap
                    FROM (
                        SELECT symbol,
                               DATEDIFF('day',
                                   LAG(date) OVER (PARTITION BY symbol ORDER BY date),
                                   date) AS gap_days
                        FROM (SELECT DISTINCT symbol, date FROM v_5min)
                    )
                    WHERE gap_days > 7
                    GROUP BY symbol
                )
                SELECT s.symbol,
                       s.dup_days, s.dup_first,
                       s.zero_vol_days, s.zero_vol_first,
                       COALESCE(g.gap_count, 0), COALESCE(g.max_gap, '0')
                FROM sym_stats s
                LEFT JOIN gap_stats g USING (symbol)
                WHERE s.dup_days > 0 OR s.zero_vol_days > 0 OR g.gap_count > 0
            """).fetchall()
        except Exception as exc:
            logger.warning("DQ pass-2 scan failed: %s", exc)
            pass2_rows = []

        p2_results: dict[str, list[tuple]] = {
            "DUPLICATE_CANDLE": [],
            "ZERO_VOLUME_DAY": [],
            "DATE_GAP": [],
        }
        for row in pass2_rows:
            sym = row[0]
            if row[1]:
                p2_results["DUPLICATE_CANDLE"].append((sym, row[1], row[2]))
            if row[3]:
                p2_results["ZERO_VOLUME_DAY"].append((sym, row[3], row[4]))
            if row[5]:
                p2_results["DATE_GAP"].append((sym, row[5], row[6]))

        _upsert_batch(
            p2_results,
            severities={
                "DUPLICATE_CANDLE": "CRITICAL",
                "ZERO_VOLUME_DAY": "INFO",
                "DATE_GAP": "WARNING",
            },
            detail_fns={
                "DUPLICATE_CANDLE": lambda c, d: (
                    f"{c} days with duplicate candle times (first: {d})"
                ),
                "ZERO_VOLUME_DAY": lambda c, d: f"{c} days with zero total volume (first: {d})",
                "DATE_GAP": lambda c, mx: f"{c} gaps >7 calendar days (max gap: {mx} days)",
            },
        )

        active_row = self.con.execute(
            "SELECT COUNT(*) FROM data_quality_issues WHERE is_active = TRUE"
        ).fetchone()
        active_total = int(active_row[0]) if active_row and active_row[0] else 0
        summary["total_active_issues"] = active_total

        total_affected = sum(v for k, v in summary.items() if k != "total_active_issues")
        logger.info(
            "DQ scan complete: 2 passes, %d issue types, %d affected symbols, %d active total",
            len(summary) - 1,
            total_affected,
            active_total,
        )
        print("  Publishing replica...", flush=True)
        self._publish_replica(force=True)
        return summary

    def get_data_quality_summary(self) -> dict[str, object]:
        """Return issue counts grouped by issue_code and severity for dashboard display."""
        if not self.read_only:
            self.ensure_data_quality_table()
        try:
            rows = self.con.execute("""
                SELECT issue_code,
                       severity,
                       COUNT(*) AS symbol_count
                FROM data_quality_issues
                WHERE is_active = TRUE
                GROUP BY issue_code, severity
                ORDER BY
                    CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
                    symbol_count DESC
            """).fetchall()
        except Exception:
            rows = []

        total = 0
        critical = 0
        issues: list[dict[str, object]] = []
        for row in rows:
            code, sev, cnt = str(row[0]), str(row[1]), int(row[2])
            total += cnt
            if sev == "CRITICAL":
                critical += cnt
            issues.append({"code": code, "severity": sev, "symbol_count": cnt})

        return {
            "total_affected": total,
            "critical_count": critical,
            "by_issue": issues,
        }

    def get_data_quality_issues(
        self,
        *,
        active_only: bool = True,
        issue_code: str | None = None,
    ) -> list[dict[str, object]]:
        """Return data quality issue rows for reporting/debugging."""
        self.ensure_data_quality_table()
        where_parts: list[str] = []
        params: list[object] = []
        if active_only:
            where_parts.append("is_active = TRUE")
        if issue_code:
            where_parts.append("issue_code = ?")
            params.append(issue_code)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        rows = self.con.execute(
            f"""
            SELECT symbol, issue_code, severity, details, is_active, first_seen, last_seen
            FROM data_quality_issues
            {where_sql}
            ORDER BY
                CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
                issue_code, symbol
            """,
            params,
        ).fetchall()
        return [
            {
                "symbol": r[0],
                "issue_code": r[1],
                "severity": r[2] or "WARNING",
                "details": r[3],
                "is_active": bool(r[4]),
                "first_seen": str(r[5]) if r[5] is not None else None,
                "last_seen": str(r[6]) if r[6] is not None else None,
            }
            for r in rows
        ]

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
                wf_run_id VARCHAR,
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
            "wf_run_id",
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
        wf_run_id: str | None = None,
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
                    param_signature, execution_mode, session_id, source_session_id, wf_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    wf_run_id,
                ],
            )
        except Exception as e:
            logger.exception("Failed to insert run_metadata for run_id=%s: %s", run_id, e)

    def get_run_ids_for_wf_run_ids(self, wf_run_ids: list[str]) -> list[str]:
        """Return DuckDB run_ids tagged to one or more walk-forward parent IDs."""
        self.ensure_run_metadata_table()
        ids = sorted({str(run_id).strip() for run_id in wf_run_ids if str(run_id).strip()})
        if not ids:
            return []
        placeholders = ", ".join("?" for _ in ids)
        rows = self.con.execute(
            f"""
            SELECT DISTINCT run_id
            FROM run_metadata
            WHERE wf_run_id IN ({placeholders})
            ORDER BY run_id
            """,
            ids,
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0]]

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

        return self.con.execute(
            f"SELECT * FROM backtest_results {where} ORDER BY symbol, trade_date",
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
