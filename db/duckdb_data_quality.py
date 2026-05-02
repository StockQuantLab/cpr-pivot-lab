"""Data-quality registry and scan helpers for :class:`db.duckdb.MarketDB`."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import polars as pl

from db.duckdb_validation import validate_symbols as _validate_symbols

logger = logging.getLogger(__name__)


class MarketDataQualityMixin:
    """Mixin for market data-quality tables and scan workflows."""

    con: Any
    read_only: bool
    _has_5min: bool
    _has_daily: bool
    _parquet_dir: Any
    _sync: Any

    def _begin_replica_batch(self) -> None:
        raise NotImplementedError

    def _end_replica_batch(self) -> None:
        raise NotImplementedError

    def _publish_replica(self, *, force: bool = False) -> None:
        raise NotImplementedError

    def _table_exists(self, table: str) -> bool:
        raise NotImplementedError

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
            if self._sync:
                self._sync.mark_dirty()

    def run_comprehensive_dq_scan(self) -> dict[str, int]:
        """Run comprehensive 5-min data quality checks and store results."""
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
                            severity=severities[code],
                        )
                    self.deactivate_data_quality_issue(code, keep_symbols=affected)
                    summary[code] = len(affected)
            finally:
                self._end_replica_batch()

        print("  Pass 1/2: scanning candles for OHLC/null/zero/timestamp/extreme...", flush=True)
        try:
            pass1_rows = self.con.execute("""
                SELECT
                    symbol,
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
                    SUM(CASE WHEN open IS NULL OR high IS NULL
                                  OR low IS NULL OR close IS NULL
                         THEN 1 ELSE 0 END) AS null_cnt,
                    MIN(CASE WHEN open IS NULL OR high IS NULL
                                  OR low IS NULL OR close IS NULL
                         THEN date END)::VARCHAR AS null_first,
                    SUM(CASE WHEN open = 0 OR close = 0 OR high = 0
                         THEN 1 ELSE 0 END) AS zero_cnt,
                    MIN(CASE WHEN open = 0 OR close = 0 OR high = 0
                         THEN date END)::VARCHAR AS zero_first,
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
                        SUM(CASE WHEN dup_candles > 0 THEN 1 ELSE 0 END) AS dup_days,
                        MIN(CASE WHEN dup_candles > 0 THEN date END)::VARCHAR AS dup_first,
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
