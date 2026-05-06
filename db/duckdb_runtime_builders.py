"""Runtime state and intraday pack builders for MarketDB."""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

from db.duckdb_table_ops import incremental_delete as _incremental_delete
from db.duckdb_table_ops import incremental_replace as _incremental_replace
from db.duckdb_table_ops import skip_if_table_fully_covered as _skip_if_table_fully_covered
from db.duckdb_table_ops import sql_symbol_list as _sql_symbol_list
from db.duckdb_table_ops import symbol_scoped_upsert as _symbol_scoped_upsert
from db.duckdb_validation import date_window_clause as _date_window_clause
from db.duckdb_validation import prepare_date_window as _prepare_date_window
from db.duckdb_validation import validate_symbols as _validate_symbols
from db.duckdb_validation import validate_table_identifier as _validate_table_identifier

logger = logging.getLogger(__name__)


class DuckDBRuntimeBuilderMixin:
    """Opening-range, market state, strategy state, and day-pack builders."""

    con: Any
    db_path: Any
    _has_5min: bool
    _parquet_dir: Any
    _table_exists_cache: set[str]
    _table_has_column_cache: set[tuple[str, str]]

    def _require_data(self, view: str = "v_5min") -> None:
        raise NotImplementedError

    def _invalidate_metadata_caches(self) -> None:
        raise NotImplementedError

    def _begin_replica_batch(self) -> None:
        raise NotImplementedError

    def _end_replica_batch(self) -> None:
        raise NotImplementedError

    def _publish_replica(self, *, force: bool = False) -> None:
        raise NotImplementedError

    def _build_dataset_meta(self) -> None:
        raise NotImplementedError

    def build_cpr_table(self, *args: Any, **kwargs: Any) -> int:
        return cast(Any, super()).build_cpr_table(*args, **kwargs)

    def build_atr_table(self, *args: Any, **kwargs: Any) -> int:
        return cast(Any, super()).build_atr_table(*args, **kwargs)

    def build_cpr_thresholds(self, *args: Any, **kwargs: Any) -> int:
        return cast(Any, super()).build_cpr_thresholds(*args, **kwargs)

    def build_virgin_cpr_flags(self, *args: Any, **kwargs: Any) -> int:
        return cast(Any, super()).build_virgin_cpr_flags(*args, **kwargs)

    def upsert_data_quality_issues(self, *args: Any, **kwargs: Any) -> int:
        return cast(Any, super()).upsert_data_quality_issues(*args, **kwargs)

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
                select_sql = self._market_day_state_select_sql(
                    symbols=target_symbols,
                    cpr_max_width_pct=cpr_max_width_pct,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    virgin_exists=virgin_exists,
                )
                _incremental_replace(
                    self.con,
                    table="market_day_state",
                    select_sql=select_sql,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols,
                    log_prefix="state",
                )
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
                select_sql = self._strategy_day_state_select_sql(
                    symbols=target_symbols, since_date=since_date_iso, until_date=until_date_iso
                )
                _incremental_replace(
                    self.con,
                    table="strategy_day_state",
                    select_sql=select_sql,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols,
                    log_prefix="strategy",
                )
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
        calc_since_date_iso = since_date_iso
        if since_date_iso:
            calc_lookback_days = max(45, max(1, int(rvol_lookback_days)) * 4)
            calc_since_date_iso = (
                date.fromisoformat(since_date_iso) - timedelta(days=calc_lookback_days)
            ).isoformat()
        window_filter_sql = _date_window_clause("date::DATE", calc_since_date_iso, until_date_iso)
        insert_window_filter_sql = _date_window_clause(
            "trade_date", since_date_iso, until_date_iso
        )
        # _date_window_clause embeds dates as SQL literals (no ? placeholders),
        # so window_params is always empty — the filtering is in window_filter_sql.
        window_params: list[object] = []

        # ── Incremental mode (--since) ────────────────────────────────────────
        # Skip DROP TABLE; just delete rows >= since_date and re-insert them.
        if since_date_iso and not force:
            table_exists = self._table_exists("intraday_day_pack")
            if table_exists and symbols is None:
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

            if table_exists:
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
        if since_date_iso and calc_since_date_iso != since_date_iso:
            _log(
                "  [pack] incremental RVOL source window:"
                f" reading from {calc_since_date_iso}, inserting from {since_date_iso}"
                + (f" through {until_date_iso}" if until_date_iso else "")
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
                        WHERE 1=1
                        {insert_window_filter_sql}
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
                        WHERE 1=1
                        {insert_window_filter_sql}
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
