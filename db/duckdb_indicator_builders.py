"""Indicator materialization builders for MarketDB."""

from __future__ import annotations

import logging
import time
from typing import Any

from db.duckdb_table_ops import incremental_delete as _incremental_delete
from db.duckdb_table_ops import incremental_replace as _incremental_replace
from db.duckdb_table_ops import skip_if_table_fully_covered as _skip_if_table_fully_covered
from db.duckdb_table_ops import sql_symbol_list as _sql_symbol_list
from db.duckdb_validation import date_window_clause as _date_window_clause
from db.duckdb_validation import prepare_date_window as _prepare_date_window
from db.duckdb_validation import validate_symbols as _validate_symbols

logger = logging.getLogger(__name__)


class DuckDBIndicatorBuilderMixin:
    """CPR, ATR, thresholds, and virgin-CPR materialization helpers."""

    con: Any

    def _require_data(self, view: str = "v_5min") -> None:
        raise NotImplementedError

    def _invalidate_metadata_caches(self) -> None:
        raise NotImplementedError

    def _table_exists(self, table: str) -> bool:
        raise NotImplementedError

    def _build_5min_file_manifest(self) -> dict[str, list[str]]:
        raise NotImplementedError

    def _build_manifest_source_sql(
        self, batch_symbols: list[str], manifest: dict[str, list[str]]
    ) -> str:
        raise NotImplementedError

    def _build_parquet_source_sql(
        self,
        symbols: list[str],
        *,
        prefer_view: bool = False,
        manifest: dict[str, list[str]] | None = None,
    ) -> str:
        raise NotImplementedError

    def _iter_symbol_batches(self, symbols: list[str], batch_size: int) -> list[list[str]]:
        raise NotImplementedError

    def _publish_replica(self, *, force: bool = False) -> None:
        raise NotImplementedError

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
            self.con.execute("DROP TABLE IF EXISTS tmp_cpr_daily_refresh")
            self.con.execute(f"CREATE TEMP TABLE tmp_cpr_daily_refresh AS {insert_sql}")
            duplicate = self.con.execute("""
                SELECT symbol, trade_date, COUNT(*) AS n
                FROM tmp_cpr_daily_refresh
                GROUP BY symbol, trade_date
                HAVING COUNT(*) > 1
                LIMIT 1
            """).fetchone()
            if duplicate:
                raise RuntimeError(
                    "cpr_daily targeted refresh produced duplicate row "
                    f"symbol={duplicate[0]} trade_date={duplicate[1]} count={duplicate[2]}"
                )
            if table_exists:
                delete_parts = [f"symbol IN ({_sql_symbol_list(target_symbols)})"]
                if since_date_iso:
                    delete_parts.append(f"trade_date >= '{since_date_iso}'::DATE")
                if until_date_iso:
                    delete_parts.append(f"trade_date <= '{until_date_iso}'::DATE")
                self.con.execute("DELETE FROM cpr_daily WHERE " + " AND ".join(delete_parts))
                self.con.execute("INSERT INTO cpr_daily SELECT * FROM tmp_cpr_daily_refresh")
            else:
                self.con.execute(
                    "CREATE TABLE cpr_daily AS SELECT * FROM tmp_cpr_daily_refresh"
                )
            self.con.execute("DROP TABLE tmp_cpr_daily_refresh")
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
                _incremental_replace(
                    self.con,
                    table="cpr_daily",
                    select_sql=insert_sql,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    log_prefix="cpr",
                )
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
                # Recompute over full cpr_daily (rolling window needs history)
                # but only INSERT rows >= since_date
                pct = percentile / 100.0
                threshold_refresh_sql = f"""
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
                """
                _incremental_replace(
                    self.con,
                    table="cpr_thresholds",
                    select_sql=threshold_refresh_sql,
                    since_date=since_date_iso,
                    until_date=until_date_iso,
                    symbols=target_symbols or None,
                    log_prefix="thresholds",
                )
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
