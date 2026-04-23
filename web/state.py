"""Shared state management for NiceGUI dashboard.

- Singleton read-only DuckDB connection (won't block backtests)
- Small read executor for non-blocking async DB calls
- TTL caching: in-memory (30 s runs, 120 s symbols) + disk (5 min status)
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import polars as pl

from db.backtest_db import get_dashboard_backtest_db
from db.duckdb import MarketDB, close_dashboard_db, get_dashboard_db
from db.postgres import (
    PaperOrder,
    PaperPosition,
    get_active_sessions,
    get_feed_state,
    get_session,
    get_session_orders,
    get_session_positions,
)
from engine.paper_runtime import summarize_paper_positions

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------


class _DashboardDBProxy:
    """Always resolve the current dashboard DuckDB connection on access.

    `get_dashboard_db()` can swap to a newer replica connection when the
    pointer file changes. Keeping a direct module-level DB object would leave
    dashboard readers stuck on the old connection until process restart.
    """

    def __getattr__(self, name: str) -> object:
        return getattr(get_dashboard_db(), name)


db: MarketDB = _DashboardDBProxy()  # type: ignore[assignment]
_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="db-worker")
_shutdown_lock = threading.Lock()
_shutdown_done = False

# ---------------------------------------------------------------------------
# Cache state
# ---------------------------------------------------------------------------
_RUNS_TTL = 30.0
_SYMBOLS_TTL = 120.0
_STATUS_TTL = 60.0
_STATUS_CACHE_FILE = Path.home() / ".cache" / "cpr_dashboard_status.json"

_runs_cache: list[dict] | None = None
_runs_cache_time: float = 0
_runs_cache_lock = threading.Lock()

_symbols_cache: list[str] | None = None
_symbols_cache_time: float = 0

_status_cache: dict | None = None
_status_cache_time: float = 0

# Data Quality tab query caches (10-minute TTL)
_DQ_CACHE_TTL = 600.0

_symbol_coverage_cache: list[dict] | None = None
_symbol_coverage_cache_time: float = 0

_date_coverage_cache: list[dict] | None = None
_date_coverage_cache_time: float = 0

_top_gaps_cache: list[dict] | None = None
_top_gaps_cache_time: float = 0

_freshness_buckets_cache: list[dict] | None = None
_freshness_buckets_cache_time: float = 0

_dq_issues_detail_cache: list[dict] | None = None
_dq_issues_detail_cache_time: float = 0

_data_quality_detail_cache: dict | None = None
_data_quality_detail_cache_time: float = 0

_market_breadth_cache: dict[int, tuple[float, pl.DataFrame]] = {}


# ---------------------------------------------------------------------------
# Sync helpers (run inside executor)
# ---------------------------------------------------------------------------
def _fetch_runs_sync(force: bool = False, execution_mode: str = "BACKTEST") -> list[dict]:
    global _runs_cache, _runs_cache_time
    if execution_mode.upper() != "BACKTEST":
        try:
            result = get_dashboard_backtest_db().get_runs_with_metrics(
                execution_mode=execution_mode.upper()
            )
            return result if result else []
        except Exception as e:
            logger.debug("Failed to fetch %s run metrics: %s", execution_mode, e)
            return []

    now = time.monotonic()
    if not force and _runs_cache is not None and (now - _runs_cache_time) < _RUNS_TTL:
        return list(_runs_cache)

    with _runs_cache_lock:
        now = time.monotonic()
        if not force and _runs_cache is not None and (now - _runs_cache_time) < _RUNS_TTL:
            return list(_runs_cache)
        try:
            result = get_dashboard_backtest_db().get_runs_with_metrics(execution_mode="BACKTEST")
            _runs_cache = result if result else []
            if _runs_cache:
                for row in _runs_cache:
                    direction = str(row.get("direction_filter") or "BOTH").upper()
                    row["direction_filter"] = (
                        direction if direction in {"LONG", "SHORT", "BOTH"} else "BOTH"
                    )
        except Exception as e:
            logger.debug("Failed to fetch backtest run metrics: %s", e)
            _runs_cache = _runs_cache or []
        _runs_cache_time = now
    return list(_runs_cache or [])


def _fetch_symbols_sync(force: bool = False) -> list[str]:
    global _symbols_cache, _symbols_cache_time
    now = time.monotonic()
    if not force and _symbols_cache is not None and (now - _symbols_cache_time) < _SYMBOLS_TTL:
        return _symbols_cache
    try:
        _symbols_cache = db.get_available_symbols()
    except Exception as e:
        logger.debug("Failed to fetch symbol list: %s", e)
        _symbols_cache = _symbols_cache or []
    _symbols_cache_time = now
    return list(_symbols_cache or [])


def _fetch_status_sync(lite: bool = False) -> dict:
    global _status_cache, _status_cache_time
    now = time.monotonic()
    if _status_cache is not None and (now - _status_cache_time) < _STATUS_TTL:
        return _status_cache

    # Fast path: load from disk (lite mode for first render)
    if lite and _STATUS_CACHE_FILE.exists():
        try:
            cached = json.loads(_STATUS_CACHE_FILE.read_text())
            _status_cache = cached
            _status_cache_time = now
            return _status_cache
        except Exception as e:
            logger.debug("Failed to read status disk cache: %s", e)

    try:
        status = db.get_status()
        logger.info(
            "Dashboard status fetched — db_path=%s, tables=%s",
            getattr(db, "db_path", "?"),
            {k: v for k, v in status.get("tables", {}).items() if v > 0} if status else {},
        )
        _status_cache = status
        _status_cache_time = now
        _STATUS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATUS_CACHE_FILE.write_text(json.dumps(status, default=str))
    except Exception as e:
        logger.debug("Failed to refresh dashboard status: %s", e)
        _status_cache = _status_cache or {}

    return dict(_status_cache or {})


def _fetch_trades_sync(run_id: str) -> pl.DataFrame:
    try:
        trades = get_dashboard_backtest_db().get_backtest_trades(run_id)
        if not trades.is_empty():
            return trades
        paper_trades = get_dashboard_backtest_db().get_backtest_trades(
            run_id,
            execution_mode="PAPER",
        )
        return paper_trades if not paper_trades.is_empty() else trades
    except Exception as e:
        logger.debug("Failed to fetch trades for run_id=%s: %s", run_id, e)
        return pl.DataFrame()


def _fetch_trade_inspection_sync(
    run_id: str,
    symbol: str,
    trade_date: str,
    entry_time: str,
    exit_time: str,
) -> dict:
    try:
        backtest_db = get_dashboard_backtest_db()
        market_db = get_dashboard_db()

        entry_hhmm = str(entry_time or "")[:5]
        exit_hhmm = str(exit_time or "")[:5]

        trade_row = backtest_db.con.execute(
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
            fallback_rows = backtest_db.con.execute(
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
            return {}

        cpr_row = market_db.con.execute(
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

        params_json = backtest_db.con.execute(
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
        candles = market_db.get_day_candles(symbol, trade_date)
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
                setup_rule = (
                    "09:15 close above the upper CPR boundary, then first close from 09:20 onward "
                    "above long threshold."
                )
                target_label = "R1"
            else:
                trigger_price = cpr_lower * (1.0 - buffer_pct)
                min_signal_close = min(trigger_price, cpr_lower - cpr_min_close_atr * atr)
                setup_rule = (
                    "09:15 close below the lower CPR boundary, then first close from 09:20 onward "
                    "below short threshold."
                )
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
    except Exception as e:
        logger.debug(
            "Failed to fetch trade inspection for run_id=%s symbol=%s date=%s: %s",
            run_id,
            symbol,
            trade_date,
            e,
        )
        return {}


def _fetch_run_metadata_sync(run_id: str) -> dict:
    from db.backtest_db import close_dashboard_backtest_db

    try:
        row = (
            get_dashboard_backtest_db()
            .con.execute(
                """
            SELECT
                strategy,
                label,
                start_date::VARCHAR,
                end_date::VARCHAR,
                symbols_json,
                params_json,
                execution_mode,
                session_id,
                created_at::VARCHAR
            FROM run_metadata
            WHERE run_id = ?
            LIMIT 1
            """,
                [run_id],
            )
            .fetchone()
        )
    except Exception as e:
        logger.warning(
            "Failed to fetch run_metadata for run_id=%s (will retry with fresh connection): %s",
            run_id,
            e,
        )
        close_dashboard_backtest_db()
        try:
            row = (
                get_dashboard_backtest_db()
                .con.execute(
                    """
                SELECT strategy, label, start_date::VARCHAR, end_date::VARCHAR,
                       symbols_json, params_json, execution_mode, session_id, created_at::VARCHAR
                FROM run_metadata WHERE run_id = ? LIMIT 1
                """,
                    [run_id],
                )
                .fetchone()
            )
        except Exception as e2:
            logger.error("Retry also failed for run_id=%s: %s", run_id, e2)
            return {}

    if not row:
        return {}

    symbols: list[str] = []
    params: dict = {}
    if row[4]:
        try:
            parsed = json.loads(str(row[4]))
            if isinstance(parsed, list):
                symbols = [str(s) for s in parsed]
        except (TypeError, ValueError):
            symbols = []
    if row[5]:
        try:
            parsed = json.loads(str(row[5]))
            if isinstance(parsed, dict):
                params = parsed
        except (TypeError, ValueError):
            params = {}

    return {
        "strategy": str(row[0] or ""),
        "label": str(row[1] or ""),
        "start_date": str(row[2] or "")[:10],
        "end_date": str(row[3] or "")[:10],
        "symbols": symbols,
        "params": params,
        "execution_mode": str(row[6] or "BACKTEST").upper(),
        "session_id": str(row[7] or ""),
        "created_at": str(row[8] or ""),
    }


def _fetch_scan_snapshot_sync(limit_days: int = 120) -> pl.DataFrame:
    try:
        return db.con.execute(
            """
            SELECT
                s.trade_date::VARCHAR AS trade_date,
                COUNT(*) AS symbols,
                SUM(CASE WHEN s.direction_5 = 'LONG' THEN 1 ELSE 0 END) AS long_bias,
                SUM(CASE WHEN s.direction_5 = 'SHORT' THEN 1 ELSE 0 END) AS short_bias,
                SUM(CASE WHEN s.direction_5 = 'NONE' THEN 1 ELSE 0 END) AS neutral_bias,
                SUM(CASE WHEN m.is_narrowing = 1 THEN 1 ELSE 0 END) AS narrowing_symbols,
                ROUND(AVG(s.or_atr_5), 4) AS avg_or_atr_5,
                ROUND(AVG(ABS(m.gap_pct_open)), 4) AS avg_abs_gap_pct,
                ROUND(AVG(m.cpr_width_pct), 4) AS avg_cpr_width_pct
            FROM strategy_day_state s
            JOIN market_day_state m
              ON m.symbol = s.symbol
             AND m.trade_date = s.trade_date
            GROUP BY s.trade_date
            ORDER BY s.trade_date DESC
            LIMIT ?
            """,
            [int(limit_days)],
        ).pl()
    except Exception as e:
        logger.debug("Failed to fetch scan snapshot: %s", e)
        return pl.DataFrame()


def _fetch_market_breadth_snapshot_sync(limit_days: int = 180) -> pl.DataFrame:
    try:
        rows = int(limit_days)
    except (TypeError, ValueError):
        rows = 180
    if rows <= 0:
        rows = 1

    cached = _market_breadth_cache.get(rows)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _DQ_CACHE_TTL:
        return cached[1]

    try:
        df = db.con.execute(
            """
            WITH candidate_days AS (
                SELECT trade_date::DATE AS trade_date
                FROM strategy_day_state
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT ?
            ),
            daily_stats AS (
                SELECT
                    symbol,
                    date::DATE AS trade_date,
                    close,
                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY date::DATE
                        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
                    ) AS ma40,
                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY date::DATE
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS ma20
                FROM v_daily
            ),
            universe AS (
                SELECT
                    s.trade_date::DATE AS trade_date,
                    s.symbol,
                    s.direction_5,
                    COALESCE(m.is_narrowing, 0) AS is_narrowing,
                    COALESCE(m.cpr_width_pct, 0.0) AS cpr_width_pct,
                    COALESCE(s.gap_abs_pct, 0.0) AS gap_abs_pct,
                    COALESCE(s.or_atr_5, 0.0) AS or_atr_5,
                    ds.close,
                    ds.ma40,
                    ds.ma20
                FROM strategy_day_state s
                JOIN market_day_state m
                  ON m.symbol = s.symbol
                 AND m.trade_date = s.trade_date
                LEFT JOIN daily_stats ds
                  ON ds.symbol = s.symbol
                 AND ds.trade_date = s.trade_date
                WHERE s.trade_date IN (SELECT trade_date FROM candidate_days)
            )
            SELECT
                trade_date::VARCHAR AS trade_date,
                COUNT(*) AS symbols,
                SUM(CASE WHEN direction_5 = 'LONG' THEN 1 ELSE 0 END) AS long_bias,
                SUM(CASE WHEN direction_5 = 'SHORT' THEN 1 ELSE 0 END) AS short_bias,
                SUM(CASE WHEN direction_5 = 'NONE' THEN 1 ELSE 0 END) AS neutral_bias,
                SUM(CASE WHEN is_narrowing = 1 THEN 1 ELSE 0 END) AS narrowing_symbols,
                ROUND(AVG(or_atr_5), 4) AS avg_or_atr_5,
                ROUND(AVG(gap_abs_pct), 4) AS avg_abs_gap_pct,
                ROUND(AVG(cpr_width_pct), 4) AS avg_cpr_width_pct,
                SUM(CASE WHEN close > ma40 THEN 1 ELSE 0 END) AS above_40_dma_count,
                ROUND(
                    SUM(CASE WHEN close > ma40 THEN 1 ELSE 0 END)
                    * 100.0 / NULLIF(COUNT(*), 0),
                    2,
                ) AS pct_above_40_dma,
                SUM(CASE WHEN close > ma20 THEN 1 ELSE 0 END) AS above_ma20_count,
                ROUND(
                    SUM(CASE WHEN close > ma20 THEN 1 ELSE 0 END)
                    * 100.0 / NULLIF(COUNT(*), 0),
                    2,
                ) AS pct_above_ma20,
                CASE
                    WHEN SUM(CASE WHEN direction_5 = 'SHORT' THEN 1 ELSE 0 END) = 0
                    THEN NULL
                    ELSE ROUND(
                        SUM(CASE WHEN direction_5 = 'LONG' THEN 1 ELSE 0 END)::DOUBLE
                        / NULLIF(SUM(CASE WHEN direction_5 = 'SHORT' THEN 1 ELSE 0 END), 0),
                        2,
                    )
                END AS ratio_5d
            FROM universe
            GROUP BY trade_date
            ORDER BY trade_date DESC
            """,
            [rows],
        ).pl()
        _market_breadth_cache[rows] = (now, df)
        return df
    except Exception as e:
        logger.debug("Failed to fetch market monitor snapshot: %s", e)
        return pl.DataFrame()


def _fetch_runtime_coverage_sync() -> pl.DataFrame:
    rows: list[dict] = []
    for table in ("market_day_state", "strategy_day_state", "intraday_day_pack"):
        try:
            row = db.con.execute(
                f"""
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT symbol) AS symbols,
                    MIN(trade_date)::VARCHAR AS min_date,
                    MAX(trade_date)::VARCHAR AS max_date
                FROM {table}
                """
            ).fetchone()
            rows.append(
                {
                    "table": table,
                    "rows": int(row[0] or 0),
                    "symbols": int(row[1] or 0),
                    "min_date": str(row[2] or ""),
                    "max_date": str(row[3] or ""),
                }
            )
        except Exception as e:
            logger.debug("Failed runtime coverage check for %s: %s", table, e)
            rows.append(
                {
                    "table": table,
                    "rows": 0,
                    "symbols": 0,
                    "min_date": "",
                    "max_date": "",
                }
            )
    return pl.DataFrame(rows)


def _fetch_data_quality_detail_sync() -> dict:
    """Detailed data quality metrics for the Data Quality page."""
    from datetime import date as date_cls

    global _data_quality_detail_cache, _data_quality_detail_cache_time
    now = time.monotonic()
    if (
        _data_quality_detail_cache is not None
        and (now - _data_quality_detail_cache_time) < _DQ_CACHE_TTL
    ):
        logger.info(
            "DQ detail: returning cached data (age=%.1fs)", now - _data_quality_detail_cache_time
        )
        return _data_quality_detail_cache

    today = date_cls.today()
    result: dict = {
        "parquet_symbol_count": 0,
        "tradeable_symbol_count": 0,
        "tradeable_covered_count": 0,
        "freshness": [],
        "history_dist": {},
        "short_history_count": 0,
        "short_history_symbols": [],
    }

    # Fetch tradeable set first so we can compute the exact coverage intersection.
    tradeable: set[str] | None = None
    try:
        from engine.kite_ingestion import tradeable_symbols

        tradeable = tradeable_symbols()
        result["tradeable_symbol_count"] = len(tradeable) if tradeable else 0
    except Exception as e:
        logger.debug("tradeable_symbols failed: %s", e)

    try:
        symbol_source = "cpr_daily" if db._table_exists("cpr_daily") else "v_daily"
        rows = db.con.execute(f"SELECT DISTINCT symbol FROM {symbol_source}").fetchall()
        parquet_symbols = {r[0] for r in rows if r[0]}
        result["parquet_symbol_count"] = len(parquet_symbols)
        # tradeable_covered_count = tradeable symbols that actually have parquet data.
        # Using raw parquet_count for the gap would be wrong when dead symbols are
        # present: parquet_count >= tradeable_count even if some tradeable symbols
        # are absent, producing a false-green coverage signal.
        if tradeable:
            result["tradeable_covered_count"] = len(tradeable & parquet_symbols)
        else:
            result["tradeable_covered_count"] = len(parquet_symbols)
    except Exception as e:
        logger.debug("parquet symbol count failed: %s", e)

    _freshness_tables = [
        "market_day_state",
        "strategy_day_state",
        "intraday_day_pack",
        "cpr_daily",
        "atr_intraday",
        "cpr_thresholds",
    ]
    freshness = []
    existing_freshness_tables = [table for table in _freshness_tables if db._table_exists(table)]
    freshness_rows: dict[str, str] = {}
    if existing_freshness_tables:
        union_sql = "\nUNION ALL\n".join(
            f"SELECT '{db._escape_sql_literal(table)}' AS table_name, MAX(trade_date)::VARCHAR AS max_date FROM {table}"
            for table in existing_freshness_tables
        )
        try:
            rows = db.con.execute(union_sql).fetchall()
            freshness_rows = {str(row[0]): str(row[1] or "")[:10] for row in rows if row and row[0]}
        except Exception as e:
            logger.debug("data-quality freshness union failed: %s", e)
            for table in existing_freshness_tables:
                try:
                    row = db.con.execute(f"SELECT MAX(trade_date)::VARCHAR FROM {table}").fetchone()
                    freshness_rows[table] = str(row[0] or "")[:10] if row and row[0] else ""
                except Exception:
                    freshness_rows[table] = ""
    for table in _freshness_tables:
        max_date = freshness_rows.get(table) or "—"
        if max_date != "—":
            d = date_cls.fromisoformat(max_date)
            days_since = (today - d).days
        else:
            days_since = -1
        freshness.append({"table": table, "max_date": max_date, "days_since": days_since})
    result["freshness"] = freshness

    try:
        row = db.con.execute("""
            WITH sym_days AS (
                SELECT symbol, COUNT(DISTINCT trade_date) AS days
                FROM market_day_state
                GROUP BY symbol
            )
            SELECT
                SUM(CASE WHEN days >= 1260 THEN 1 ELSE 0 END),
                SUM(CASE WHEN days >= 504 AND days < 1260 THEN 1 ELSE 0 END),
                SUM(CASE WHEN days >= 252 AND days < 504 THEN 1 ELSE 0 END),
                SUM(CASE WHEN days < 252 THEN 1 ELSE 0 END)
            FROM sym_days
        """).fetchone()
        result["history_dist"] = {
            "5yr+": int(row[0] or 0),
            "2-5yr": int(row[1] or 0),
            "1-2yr": int(row[2] or 0),
            "<1yr": int(row[3] or 0),
        }
        result["short_history_count"] = int(row[3] or 0)
    except Exception as e:
        logger.debug("history distribution failed: %s", e)

    try:
        rows = db.con.execute("""
            SELECT symbol, COUNT(DISTINCT trade_date) AS days,
                   MIN(trade_date)::VARCHAR AS first_date
            FROM market_day_state
            GROUP BY symbol
            HAVING days < 252
            ORDER BY days ASC
            LIMIT 300
        """).fetchall()
        result["short_history_symbols"] = [
            {"symbol": str(r[0]), "days": int(r[1]), "first_date": str(r[2] or "")[:10]}
            for r in rows
            if r
        ]
    except Exception as e:
        logger.debug("short history symbols failed: %s", e)

    # DQ issue summary (grouped counts for dashboard display)
    try:
        result["dq_summary"] = db.get_data_quality_summary()
    except Exception as e:
        logger.debug("dq_summary failed: %s", e)
        result["dq_summary"] = {"total_affected": 0, "critical_count": 0, "by_issue": []}

    _data_quality_detail_cache = result
    _data_quality_detail_cache_time = now
    logger.info(
        "DQ detail: fresh query done — parquet=%d, tradeable=%d, freshness=%d tables, dq_issues=%s",
        result.get("parquet_symbol_count", 0),
        result.get("tradeable_symbol_count", 0),
        len(result.get("freshness", [])),
        list(result.get("dq_summary", {}).keys()) if result.get("dq_summary") else "none",
    )
    return result


def _fetch_symbol_coverage_sync() -> list[dict]:
    """Per-symbol coverage % with gap estimates from v_daily."""
    global _symbol_coverage_cache, _symbol_coverage_cache_time
    now = time.monotonic()
    if _symbol_coverage_cache is not None and (now - _symbol_coverage_cache_time) < _DQ_CACHE_TTL:
        return _symbol_coverage_cache
    try:
        rows = db.con.execute("""
            SELECT symbol,
                   MIN(date)::VARCHAR AS first_date,
                   MAX(date)::VARCHAR AS last_date,
                   COUNT(*)::INT AS total_rows,
                   COUNT(DISTINCT date)::INT AS distinct_days,
                   (DATEDIFF('day', MIN(date), MAX(date)) + 1)::INT AS calendar_span
            FROM v_daily GROUP BY symbol ORDER BY symbol
        """).fetchall()
        result = []
        for r in rows:
            span = max(r[5], 1)
            expected = span * 5 / 7 * 0.96  # weekday estimate with ~10 holidays/yr
            cov = min(round(r[4] / max(expected, 1) * 100, 1), 100.0)
            gap_est = max(0, round(expected) - r[4])
            result.append(
                {
                    "symbol": r[0],
                    "first_date": r[1],
                    "last_date": r[2],
                    "total_rows": r[3],
                    "distinct_days": r[4],
                    "calendar_span": span,
                    "coverage_pct": cov,
                    "gap_estimate": gap_est,
                }
            )
        _symbol_coverage_cache = result
        _symbol_coverage_cache_time = now
    except Exception as e:
        logger.debug("symbol coverage query failed: %s", e)
        result = _symbol_coverage_cache or []
    return list(result)


def _fetch_date_coverage_sync() -> list[dict]:
    """Symbols reporting per date (for coverage line chart)."""
    global _date_coverage_cache, _date_coverage_cache_time
    now = time.monotonic()
    if _date_coverage_cache is not None and (now - _date_coverage_cache_time) < _DQ_CACHE_TTL:
        return _date_coverage_cache
    try:
        rows = db.con.execute("""
            SELECT date::VARCHAR AS trading_date,
                   COUNT(DISTINCT symbol)::INT AS symbol_count
            FROM v_daily GROUP BY date ORDER BY date
        """).fetchall()
        result = [{"trading_date": r[0], "symbol_count": r[1]} for r in rows]
        _date_coverage_cache = result
        _date_coverage_cache_time = now
    except Exception as e:
        logger.debug("date coverage query failed: %s", e)
        result = _date_coverage_cache or []
    return list(result)


def _fetch_top_gaps_sync(limit: int = 200) -> list[dict]:
    """Top gaps > 5 calendar days across all symbols in v_daily."""
    global _top_gaps_cache, _top_gaps_cache_time
    now = time.monotonic()
    if _top_gaps_cache is not None and (now - _top_gaps_cache_time) < _DQ_CACHE_TTL:
        return _top_gaps_cache
    try:
        rows = db.con.execute(f"""
            WITH symbol_dates AS (
                SELECT symbol, date,
                       LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_date
                FROM v_daily
            )
            SELECT symbol, prev_date::VARCHAR AS gap_start, date::VARCHAR AS gap_end,
                   DATEDIFF('day', prev_date, date)::INT AS gap_days
            FROM symbol_dates
            WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, date) > 5
            ORDER BY gap_days DESC
            LIMIT {int(limit)}
        """).fetchall()
        result = [
            {"symbol": r[0], "gap_start": r[1], "gap_end": r[2], "gap_days": r[3]} for r in rows
        ]
        _top_gaps_cache = result
        _top_gaps_cache_time = now
    except Exception as e:
        logger.debug("top gaps query failed: %s", e)
        result = _top_gaps_cache or []
    return list(result)


def _fetch_symbol_gaps_sync(symbol: str) -> list[dict]:
    """Per-symbol gap drill-down (>3 calendar days). No cache — per-lookup."""
    try:
        rows = db.con.execute(
            """
            WITH sym_dates AS (
                SELECT date,
                       LAG(date) OVER (ORDER BY date) AS prev_date
                FROM v_daily WHERE symbol = ?
            )
            SELECT prev_date::VARCHAR AS gap_start, date::VARCHAR AS gap_end,
                   DATEDIFF('day', prev_date, date)::INT AS gap_days
            FROM sym_dates
            WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, date) > 3
            ORDER BY gap_days DESC
        """,
            [symbol],
        ).fetchall()
        return [{"gap_start": r[0], "gap_end": r[1], "gap_days": r[2]} for r in rows]
    except Exception as e:
        logger.debug("symbol gaps query failed for %s: %s", symbol, e)
        return []


def _fetch_freshness_buckets_sync() -> list[dict]:
    """Freshness bucket distribution with sample symbols per bucket."""
    global _freshness_buckets_cache, _freshness_buckets_cache_time
    now = time.monotonic()
    if (
        _freshness_buckets_cache is not None
        and (now - _freshness_buckets_cache_time) < _DQ_CACHE_TTL
    ):
        return _freshness_buckets_cache
    try:
        rows = db.con.execute("""
            WITH last_dates AS (
                SELECT symbol, MAX(date) AS last_date FROM v_daily GROUP BY symbol
            ),
            bucketed AS (
                SELECT symbol, last_date,
                       CASE
                           WHEN CURRENT_DATE - last_date <= 7 THEN 'Fresh (<7d)'
                           WHEN CURRENT_DATE - last_date <= 30 THEN 'Recent (7-30d)'
                           WHEN CURRENT_DATE - last_date <= 90 THEN 'Stale (30-90d)'
                           ELSE 'Very Stale (>90d)'
                       END AS bucket,
                       CASE
                           WHEN CURRENT_DATE - last_date <= 7 THEN 1
                           WHEN CURRENT_DATE - last_date <= 30 THEN 2
                           WHEN CURRENT_DATE - last_date <= 90 THEN 3
                           ELSE 4
                       END AS sort_key
                FROM last_dates
            )
            SELECT bucket, COUNT(*)::INT AS count, sort_key,
                   LIST(symbol ORDER BY symbol)[:50] AS sample_symbols
            FROM bucketed GROUP BY bucket, sort_key ORDER BY sort_key
        """).fetchall()
        result = [{"bucket": r[0], "count": r[1], "symbols": r[3]} for r in rows]
        _freshness_buckets_cache = result
        _freshness_buckets_cache_time = now
    except Exception as e:
        logger.debug("freshness buckets query failed: %s", e)
        result = _freshness_buckets_cache or []
    return list(result)


def _fetch_dq_issues_detail_sync(limit: int = 500) -> list[dict]:
    """Active DQ issue rows from the pre-computed data_quality_issues table."""
    global _dq_issues_detail_cache, _dq_issues_detail_cache_time
    now = time.monotonic()
    if _dq_issues_detail_cache is not None and (now - _dq_issues_detail_cache_time) < _DQ_CACHE_TTL:
        return _dq_issues_detail_cache
    try:
        rows = db.con.execute(f"""
            SELECT symbol, issue_code, severity, details,
                   COALESCE(last_seen::VARCHAR, '') AS last_seen
            FROM data_quality_issues
            WHERE is_active = TRUE
            ORDER BY
                CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
                issue_code, symbol
            LIMIT {int(limit)}
        """).fetchall()
        result = [
            {
                "symbol": r[0],
                "issue": r[1],
                "severity": r[2] or "WARNING",
                "detail": r[3],
                "last_seen": str(r[4])[:10] if r[4] else "",
            }
            for r in rows
        ]
        _dq_issues_detail_cache = result
        _dq_issues_detail_cache_time = now
    except Exception as e:
        logger.debug("DQ issues detail query failed: %s", e)
        result = _dq_issues_detail_cache or []
    return list(result)


def _fetch_symbol_profile_sync(symbol: str) -> dict | None:
    """Full per-symbol profile: daily + 5-min + market_day_state stats + gaps."""
    try:
        row = db.con.execute(
            """
            SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR,
                   COUNT(*)::INT, COUNT(DISTINCT date)::INT,
                   (DATEDIFF('day', MIN(date), MAX(date)) + 1)::INT
            FROM v_daily WHERE symbol = ?
        """,
            [symbol],
        ).fetchone()
        if not row or row[2] == 0:
            return None
        span = max(row[4], 1)
        expected = span * 5 / 7 * 0.96
        cov = min(round(row[3] / max(expected, 1) * 100, 1), 100.0)

        fivemin_row = db.con.execute(
            """
            SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR,
                   COUNT(*)::INT, COUNT(DISTINCT date)::INT
            FROM v_5min WHERE symbol = ?
        """,
            [symbol],
        ).fetchone()

        mds_row = db.con.execute(
            """
            SELECT COUNT(*)::INT, MIN(trade_date)::VARCHAR, MAX(trade_date)::VARCHAR
            FROM market_day_state WHERE symbol = ?
        """,
            [symbol],
        ).fetchone()

        gaps = _fetch_symbol_gaps_sync(symbol)

        return {
            "symbol": symbol,
            "daily_first": row[0],
            "daily_last": row[1],
            "daily_rows": row[2],
            "daily_distinct_days": row[3],
            "daily_coverage_pct": cov,
            "fivemin_first": fivemin_row[0] if fivemin_row and fivemin_row[2] else None,
            "fivemin_last": fivemin_row[1] if fivemin_row and fivemin_row[2] else None,
            "fivemin_rows": fivemin_row[2] if fivemin_row else 0,
            "fivemin_days": fivemin_row[3] if fivemin_row else 0,
            "mds_rows": mds_row[0] if mds_row else 0,
            "mds_first": mds_row[1] if mds_row else None,
            "mds_last": mds_row[2] if mds_row else None,
            "gaps": gaps,
        }
    except Exception as e:
        logger.debug("symbol profile query failed for %s: %s", symbol, e)
        return None


def _fetch_run_ledger_sync(run_id: str, execution_mode: str = "BACKTEST") -> pl.DataFrame:
    try:
        return get_dashboard_backtest_db().get_backtest_trades(
            run_id,
            execution_mode=execution_mode,
        )
    except Exception as e:
        logger.debug("Failed to fetch run ledger for run_id=%s: %s", run_id, e)
        return pl.DataFrame()


def _fetch_run_daily_pnl_sync(run_id: str) -> pl.DataFrame:
    try:
        db_conn = get_dashboard_backtest_db().con
        df = db_conn.execute(
            """
            SELECT
                trade_date::VARCHAR AS trade_date,
                day_pnl,
                cum_pnl
            FROM run_daily_pnl
            WHERE run_id = ?
            ORDER BY trade_date
            """,
            [run_id],
        ).pl()
        if not df.is_empty():
            return df
    except Exception as e:
        logger.debug("run_daily_pnl lookup failed for run_id=%s: %s", run_id, e)

    # Fallback for older DBs where run_daily_pnl may not be populated.
    try:
        return db_conn.execute(
            """
            WITH daily AS (
                SELECT
                    trade_date::VARCHAR AS trade_date,
                    SUM(profit_loss) AS day_pnl
                FROM backtest_results
                WHERE run_id = ?
                GROUP BY trade_date
            )
            SELECT
                trade_date,
                day_pnl,
                SUM(day_pnl) OVER (ORDER BY trade_date ROWS UNBOUNDED PRECEDING) AS cum_pnl
            FROM daily
            ORDER BY trade_date
            """,
            [run_id],
        ).pl()
    except Exception as e:
        logger.debug("Fallback daily pnl query failed for run_id=%s: %s", run_id, e)
        return pl.DataFrame()


def _warm_cache_sync(force: bool = False) -> dict[str, int]:
    """Warm all major dashboard caches in one executor task."""
    runs = _fetch_runs_sync(force=force)
    symbols = _fetch_symbols_sync(force=force)
    status = _fetch_status_sync(lite=False)
    return {
        "runs": len(runs),
        "symbols": len(symbols),
        "tables": len((status or {}).get("tables", {})),
    }


def _warm_home_cache_sync(force: bool = False) -> dict[str, int]:
    """Warm only the caches needed by Home page first render (runs + status)."""
    runs = _fetch_runs_sync(force=force)
    status = _fetch_status_sync(lite=True)
    return {
        "runs": len(runs),
        "tables": len((status or {}).get("tables", {})),
    }


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------
async def aget_runs(force: bool = False, execution_mode: str = "BACKTEST") -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_runs_sync(force, execution_mode))


async def aget_compare_breakdown(run_a: str, run_b: str) -> dict:
    loop = asyncio.get_running_loop()

    def _go() -> dict:
        db = get_dashboard_backtest_db()
        return db.get_compare_breakdown(run_a, run_b) if db else {}

    return await loop.run_in_executor(_executor, _go)


async def aget_setup_funnel(run_id: str) -> list[dict]:
    """Return setup funnel steps for a run: [{filter_step, count}]."""
    loop = asyncio.get_running_loop()

    def _go() -> list[dict]:
        db = get_dashboard_backtest_db()
        if not db:
            return []
        try:
            rows = db.con.execute(
                "SELECT filter_step, count FROM setup_funnel WHERE run_id = ? ORDER BY count DESC",
                [run_id],
            ).fetchall()
            return [{"filter_step": r[0], "count": int(r[1])} for r in rows]
        except Exception:
            return []

    return await loop.run_in_executor(_executor, _go)


async def aget_cross_run_trades(runs: list[dict]) -> pl.DataFrame:
    """Aggregate trades across all BACKTEST runs (excludes PAPER). Returns combined Polars DF."""
    loop = asyncio.get_running_loop()

    def _go() -> pl.DataFrame:
        db = get_dashboard_backtest_db()
        if not db:
            return pl.DataFrame()
        bt_runs = [r for r in runs if str(r.get("execution_mode") or "BACKTEST").upper() != "PAPER"]
        if not bt_runs:
            return pl.DataFrame()
        ids = [r["run_id"] for r in bt_runs if r.get("run_id")]
        if not ids:
            return pl.DataFrame()
        placeholders = ",".join(["?"] * len(ids))
        try:
            return db.con.execute(
                f"SELECT * FROM backtest_results WHERE run_id IN ({placeholders})",
                ids,
            ).pl()
        except Exception:
            return pl.DataFrame()

    return await loop.run_in_executor(_executor, _go)


async def aget_symbols(force: bool = False) -> list[str]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_symbols_sync(force))


async def aget_status(lite: bool = False) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_status_sync(lite))


async def aget_trades(run_id: str) -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_trades_sync(run_id))


async def aget_trade_inspection(
    run_id: str,
    symbol: str,
    trade_date: str,
    entry_time: str,
    exit_time: str,
) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: _fetch_trade_inspection_sync(
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            exit_time=exit_time,
        ),
    )


async def aget_run_metadata(run_id: str) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_run_metadata_sync(run_id))


async def aget_scan_snapshot(limit_days: int = 120) -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: _fetch_scan_snapshot_sync(limit_days=limit_days),
    )


async def aget_market_breadth_snapshot(limit_days: int = 180) -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: _fetch_market_breadth_snapshot_sync(limit_days=limit_days),
    )


async def aget_runtime_coverage() -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_runtime_coverage_sync)


async def aget_data_quality_detail() -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_data_quality_detail_sync)


async def aget_symbol_coverage() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_symbol_coverage_sync)


async def aget_date_coverage() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_date_coverage_sync)


async def aget_top_gaps(limit: int = 200) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_top_gaps_sync(limit))


async def aget_symbol_gaps(symbol: str) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_symbol_gaps_sync(symbol))


async def aget_freshness_buckets() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_freshness_buckets_sync)


async def aget_dq_issues_detail(limit: int = 500) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_dq_issues_detail_sync(limit))


async def aget_symbol_profile(symbol: str) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_symbol_profile_sync(symbol))


async def aget_run_ledger(run_id: str, execution_mode: str = "BACKTEST") -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor, lambda: _fetch_run_ledger_sync(run_id, execution_mode=execution_mode)
    )


async def aget_run_daily_pnl(run_id: str) -> pl.DataFrame:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_run_daily_pnl_sync(run_id))


async def aget_paper_session_positions(
    session_id: str,
    *,
    symbol: str | None = None,
    statuses: list[str] | None = None,
) -> list[PaperPosition]:
    return await get_session_positions(session_id, symbol=symbol, statuses=statuses)


async def aget_paper_session_orders(
    session_id: str,
    *,
    symbol: str | None = None,
    status: str | None = None,
    limit: int = 25,
) -> list[PaperOrder]:
    orders = await get_session_orders(session_id, symbol=symbol)
    if status is not None:
        wanted_status = status.upper()
        orders = [
            order for order in orders if str(getattr(order, "status", "")).upper() == wanted_status
        ]
    return orders[: max(0, int(limit))]


async def aget_paper_session_feed_state(session_id: str) -> object | None:
    return await get_feed_state(session_id)


async def aget_paper_session_summary(session_id: str) -> dict[str, object]:
    session, positions, orders, feed_state = await asyncio.gather(
        get_session(session_id),
        aget_paper_session_positions(session_id),
        aget_paper_session_orders(session_id),
        aget_paper_session_feed_state(session_id),
    )
    if session is None:
        return {"session_id": session_id, "missing": True}
    summary = summarize_paper_positions(session, positions, feed_state)
    summary["orders"] = len(orders)
    summary["positions"] = len(positions)
    return summary


async def aget_paper_session_snapshot(session_id: str) -> dict[str, object]:
    session, positions, orders, feed_state = await asyncio.gather(
        get_session(session_id),
        aget_paper_session_positions(session_id),
        aget_paper_session_orders(session_id),
        aget_paper_session_feed_state(session_id),
    )
    if session is None:
        return {"session_id": session_id, "missing": True}
    summary = summarize_paper_positions(session, positions, feed_state)
    summary["orders"] = len(orders)
    return {
        "session": session,
        "positions": positions,
        "orders": orders,
        "feed_state": feed_state,
        "summary": summary,
    }


async def aget_paper_active_sessions() -> list[dict[str, object]]:
    sessions = await get_active_sessions()
    if not sessions:
        return []
    snapshots = await asyncio.gather(*(aget_paper_session_snapshot(s.session_id) for s in sessions))
    results: list[dict[str, object]] = []
    for snapshot in snapshots:
        session = snapshot.get("session")
        summary = snapshot.get("summary") or {}
        if session is None:
            continue
        results.append(
            {
                "session": session,
                "summary": summary,
                "positions": snapshot.get("positions") or [],
                "orders": snapshot.get("orders") or [],
                "feed_state": snapshot.get("feed_state"),
            }
        )
    return results


async def aget_paper_archived_runs(force: bool = False) -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor, lambda: _fetch_runs_sync(force, execution_mode="PAPER")
    )


async def aget_paper_daily_summary() -> list[dict]:
    """Daily aggregate of paper session trades (LONG/SHORT counts, wins, P/L)."""
    loop = asyncio.get_running_loop()

    def _fetch() -> list[dict]:
        try:
            cols = [
                "trade_date",
                "long_trades",
                "long_wins",
                "long_pnl",
                "short_trades",
                "short_wins",
                "short_pnl",
                "total_trades",
                "total_wins",
                "total_pnl",
            ]
            rows = get_dashboard_backtest_db().get_paper_daily_summary()
            return [dict(zip(cols, r, strict=True)) for r in rows]
        except Exception as e:
            logger.debug("Failed to fetch paper daily summary: %s", e)
            return []

    return await loop.run_in_executor(_executor, _fetch)


async def awarm_cache(force: bool = False) -> dict[str, int]:
    """Warm run, symbol, and status caches for faster first-page interactions."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _warm_cache_sync(force=force))


async def awarm_home_cache(force: bool = False) -> dict[str, int]:
    """Warm run/status caches used by Home page to reduce first-render DB roundtrips."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: _warm_home_cache_sync(force=force))


def invalidate_run_cache(
    run_id: str | None = None,
) -> None:
    global _runs_cache, _runs_cache_time
    with _runs_cache_lock:
        if _runs_cache is None:
            _runs_cache_time = 0
            return
        if run_id is None:
            _runs_cache = None
        else:
            target_run_id = str(run_id)
            filtered = [row for row in _runs_cache if str(row.get("run_id") or "") != target_run_id]
            _runs_cache = filtered if filtered else None
        _runs_cache_time = 0


def shutdown_state() -> None:
    """Gracefully tear down dashboard state resources on process exit."""
    global _shutdown_done
    with _shutdown_lock:
        if _shutdown_done:
            return
        _shutdown_done = True
        try:
            _executor.shutdown(wait=True, cancel_futures=True)
        except Exception as e:
            logger.debug("Ignoring dashboard executor shutdown error: %s", e)
        try:
            close_dashboard_db()
        except Exception as e:
            logger.debug("Ignoring dashboard DB close error: %s", e)


atexit.register(shutdown_state)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _format_run_updated_at(value: object) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    text = str(value).strip()
    if not text:
        return "unknown"
    if " " in text:
        return text[:16]
    if "T" in text:
        return text.replace("T", " ")[:16]
    return text[:16]


def _universe_size_from_json(symbols_json: object) -> int:
    """Extract universe size from symbols_json array (total symbols, not just traded)."""
    text = str(symbols_json or "").strip()
    if not text or text == "None":
        return 0
    try:
        import json

        symbols = json.loads(text)
        return len(symbols) if isinstance(symbols, list) else 0
    except (json.JSONDecodeError, ValueError):
        return 0


def build_run_options(runs: list[dict]) -> dict[str, str]:
    """Build label -> run_id mapping for dropdowns (most recent first).

    Uses the DB label from run_metadata when available (has correct universe
    size), falling back to a reconstructed label from individual fields.
    """
    options: dict[str, str] = {}
    for r in runs:
        rid = str(r.get("run_id") or "")
        ts = _format_run_updated_at(r.get("updated_at"))
        tot_ret = float(r.get("total_return_pct") or 0.0)
        total_pnl = float(r.get("total_pnl") or 0.0)
        trades = int(r.get("trade_count") or 0)
        start = str(r.get("start_date") or "")[:10]
        end = str(r.get("end_date") or "")[:10]

        # Always reconstruct from fields — DB labels vary in quality
        direction = str(r.get("direction_filter") or "BOTH").upper()
        sizing = "risksize" if _as_bool(r.get("risk_based_sizing")) else "slotsize"
        compound = "compound" if _as_bool(r.get("compound_equity")) else "daily-reset"
        strategy = str(r.get("strategy_code") or r.get("strategy") or "").lower()
        rvol_tag = (
            "rvoloff"
            if _as_bool(r.get("skip_rvol_check"))
            else (f"rvol{float(r.get('rvol_threshold') or 1.0):g}")
        )
        atr_gate = float(r.get("cpr_min_close_atr") or 0.0)
        atr_tag = f"atr{atr_gate:g}" if atr_gate > 0 else ""
        universe_size = _universe_size_from_json(r.get("symbols_json"))
        parts = [strategy, direction.lower(), sizing, compound, rvol_tag]
        if atr_tag:
            parts.append(atr_tag)
        if universe_size:
            parts.append(f"u{universe_size}")
        tag = "-".join(p for p in parts if p)
        label = (
            f"{rid[:12]} | {ts} | {tag} | {start}→{end} | "
            f"TotRet {tot_ret:.1f}% | P/L ₹{total_pnl:,.0f} | Trades {trades:,}"
        )
        options[label] = rid
    return options


def build_paper_session_options(runs: list[dict]) -> dict[str, str]:
    """Build label -> run_id for paper-session dropdowns.

    Paper run_ids are descriptive (e.g. ``CPR_LEVELS_LONG-2026-04-23-live-kite``)
    so we use them directly as labels.  TMP_* diagnostic runs are excluded.
    """
    options: dict[str, str] = {}
    for r in runs:
        rid = str(r.get("run_id") or "")
        if rid.startswith("TMP_"):
            continue

        total_pnl = float(r.get("total_pnl") or 0.0)
        trades = int(r.get("trade_count") or 0)
        start = str(r.get("start_date") or "")[:10]
        end = str(r.get("end_date") or "")[:10]

        # rid already contains strategy, direction, date, mode, feed
        label = f"{rid} | {start}→{end} | P/L ₹{total_pnl:,.0f} | Trades {trades:,}"
        options[label] = rid
    return options
