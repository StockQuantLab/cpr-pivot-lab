"""Daily paper runtime preparation helpers.

These helpers make explicit the bootstrap step required before replay/live paper runs:

1. resolve trade date and symbol list
2. (re)build runtime tables in DuckDB
3. validate symbol/date coverage for the requested trading date
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from config.settings import get_settings
from db.duckdb import get_db
from engine.command_lock import acquire_command_lock
from engine.constants import normalize_symbol

IST = ZoneInfo("Asia/Kolkata")


def _open_runtime_db(*, read_only: bool):
    # Paper workflows need the canonical market DB, not the dashboard replica:
    # the replica can lag behind immediately after ingestion / snapshot writes.
    return get_db()


def _resolve_latest_source_trade_date(*, read_only: bool) -> str:
    """Return the latest available source trade date from parquet-backed views.

    Prefer `v_5min` because live paper depends on intraday history. Fall back to
    `v_daily` so the helper still works in setups that only have daily history.
    """
    db = _open_runtime_db(read_only=read_only)
    for view in ("v_5min", "v_daily"):
        try:
            row = db.con.execute(f"SELECT MAX(date)::VARCHAR FROM {view}").fetchone()
        except Exception:
            continue
        if row and row[0]:
            return str(row[0])
    raise RuntimeError("Unable to resolve the latest source trade date from v_5min/v_daily")


def _refresh_live_runtime_tables(*, symbols: list[str], refresh_date: str) -> dict[str, Any]:
    """Refresh runtime DuckDB tables for the latest completed source trade date."""
    requested_symbols = sorted(set(symbols))
    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        db = _open_runtime_db(read_only=False)
        db.build_all(
            force=False,
            symbols=requested_symbols or None,
            atr_batch_size=64,
            pack_batch_size=64,
            pack_rvol_lookback_days=10,
            since_date=refresh_date,
            until_date=refresh_date,
        )
    return {
        "refreshed": True,
        "refresh_date": refresh_date,
        "requested_symbols": requested_symbols,
    }


def resolve_trade_date(value: str | None) -> str:
    """Resolve a trade date from command input.

    Accepts:
    - None -> today in Asia/Kolkata timezone
    - "today" / "now" (case-insensitive)
    - YYYY-MM-DD ISO date
    """
    if value is None or not str(value).strip():
        return datetime.now(IST).date().isoformat()

    normalized = str(value).strip()
    if normalized.lower() in {"today", "now"}:
        return datetime.now(IST).date().isoformat()

    parsed = datetime.fromisoformat(normalized)
    return parsed.date().isoformat()


def _parse_symbols_csv(value: str | None) -> list[str]:
    if not value:
        return []
    symbols: list[str] = []
    for raw_symbol in value.split(","):
        raw_symbol = raw_symbol.strip()
        if not raw_symbol:
            continue
        symbols.append(normalize_symbol(raw_symbol))
    return symbols


def _resolve_all_local_symbols(*, read_only: bool = True) -> list[str]:
    """Resolve the local executable symbol universe for paper workflows.

    Prefer the actual 5-minute symbol universe because paper/live execution
    depends on intraday data. Falling back to `get_available_symbols`
    keeps older tests and legacy setups working if the 5-minute view is not
    available yet.

    When the NSE tradeable master is available, intersect with it to exclude
    delisted symbols that have historical parquet but no current market data.
    """
    from engine.kite_ingestion import tradeable_symbols

    db = _open_runtime_db(read_only=read_only)
    try:
        rows = db.con.execute("SELECT DISTINCT symbol FROM v_5min ORDER BY symbol").fetchall()
        candidates = {normalize_symbol(row[0]) for row in rows if row and row[0]}
        if candidates:
            tradeable = tradeable_symbols()
            if tradeable:
                candidates &= {normalize_symbol(s) for s in tradeable}
            return sorted(candidates)
    except Exception:
        pass
    return sorted(normalize_symbol(s) for s in db.get_available_symbols(force_refresh=True))


def resolve_prepare_symbols(
    symbols: list[str] | None,
    symbols_csv: str | None,
    *,
    universe_name: str | None = None,
    all_symbols: bool = False,
    read_only: bool = True,
) -> list[str]:
    """Resolve symbols for paper runtime preparation.

    Precedence:
    1. explicit list or CSV input
    2. named saved universe
    3. settings.paper_default_symbols
    """
    merged: list[str] = []
    if symbols:
        merged.extend(symbols)
    merged.extend(_parse_symbols_csv(symbols_csv))

    if not merged:
        if universe_name:
            merged.extend(load_universe_symbols(universe_name, read_only=read_only))
        elif all_symbols:
            merged.extend(_resolve_all_local_symbols(read_only=read_only))
        else:
            merged.extend(_parse_symbols_csv(get_settings().paper_default_symbols))

    return sorted({normalize_symbol(symbol) for symbol in merged})


def load_universe_symbols(universe_name: str, *, read_only: bool = True) -> list[str]:
    """Load a saved universe from backtest_universe."""
    db = _open_runtime_db(read_only=read_only)
    return db.get_universe_symbols(universe_name)


def snapshot_candidate_universe(
    universe_name: str,
    symbols: list[str],
    *,
    trade_date: str,
    source: str = "paper-daily-prepare",
    notes: str = "",
) -> int:
    """Persist the resolved candidate universe for the day."""
    db = get_db()
    return db.upsert_universe(
        universe_name,
        symbols,
        start_date=trade_date,
        end_date=trade_date,
        source=source,
        notes=notes,
    )


def validate_daily_runtime_coverage(
    *,
    trade_date: str,
    symbols: list[str],
    read_only: bool = True,
) -> dict[str, Any]:
    """Return per-table coverage gaps for the requested symbols and trade date."""
    db = _open_runtime_db(read_only=read_only)
    coverage = db.get_runtime_trade_date_coverage(symbols, trade_date)
    missing_counts = {table: len(values) for table, values in coverage.items()}
    missing_total = sum(missing_counts.values())
    ready = all(count == 0 for count in missing_counts.values())
    return {
        "trade_date": trade_date,
        "requested_symbols": sorted(set(symbols)),
        "coverage": coverage,
        "missing_counts": missing_counts,
        "missing_total": missing_total,
        "coverage_ready": ready,
    }


def validate_live_runtime_coverage(*, trade_date: str, symbols: list[str]) -> dict[str, Any]:
    """Return prior-day prerequisite coverage for live paper trading.

    Live paper does not need same-day `market_day_state` or `intraday_day_pack`
    before the market opens. It needs enough local history to derive the day
    setup from:

    - the latest previous daily bar
    - the latest previous 5-minute bar set for ATR

    Both sources should resolve to the same previous trade date per symbol.
    """
    db = _open_runtime_db(read_only=True)
    requested_symbols = sorted(set(symbols))
    missing_by_symbol: dict[str, list[str]] = {}
    warning_by_symbol: dict[str, list[str]] = {}
    latest_history_by_symbol: dict[str, dict[str, str | None]] = {}

    # Batch query: get max date per symbol for both views in one pass
    placeholders = ",".join("?" for _ in requested_symbols)
    params = [*requested_symbols, trade_date]

    daily_rows = db.con.execute(
        f"SELECT symbol, MAX(date)::VARCHAR FROM v_daily WHERE symbol IN ({placeholders}) AND date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()
    five_min_rows = db.con.execute(
        f"SELECT symbol, MAX(date)::VARCHAR FROM v_5min WHERE symbol IN ({placeholders}) AND date < ?::DATE GROUP BY symbol",
        params,
    ).fetchall()

    daily_map = {row[0]: str(row[1]) if row[1] is not None else None for row in daily_rows}
    five_min_map = {row[0]: str(row[1]) if row[1] is not None else None for row in five_min_rows}

    for symbol in requested_symbols:
        prev_daily = daily_map.get(symbol)
        prev_5min = five_min_map.get(symbol)
        latest_history_by_symbol[symbol] = {
            "prev_daily_date": prev_daily,
            "prev_5min_date": prev_5min,
        }

        missing: list[str] = []
        if prev_daily is None:
            missing.append("v_daily")
        if prev_5min is None:
            missing.append("v_5min")
        if prev_daily is not None and prev_5min is not None and prev_daily != prev_5min:
            warning_by_symbol[symbol] = ["date_mismatch"]
        if missing:
            missing_by_symbol[symbol] = missing

    return {
        "trade_date": trade_date,
        "requested_symbols": requested_symbols,
        "coverage": latest_history_by_symbol,
        "missing_by_symbol": missing_by_symbol,
        "warning_by_symbol": warning_by_symbol,
        "missing_counts": {symbol: len(missing) for symbol, missing in missing_by_symbol.items()},
        "warning_counts": {symbol: len(warnings) for symbol, warnings in warning_by_symbol.items()},
        "missing_total": sum(len(missing) for missing in missing_by_symbol.values()),
        "warning_total": sum(len(warnings) for warnings in warning_by_symbol.values()),
        "coverage_ready": not missing_by_symbol,
    }


def prepare_runtime_for_daily_paper(
    *,
    trade_date: str,
    symbols: list[str],
    mode: str = "replay",
) -> dict[str, Any]:
    """Prepare runtime table coverage for one trading date.

    Replay mode stays read-only and only validates coverage.
    Live mode refreshes the runtime tables for the latest source trade date first,
    then validates the requested live trade date readiness.
    """
    requested_symbols = sorted(set(symbols))
    if not requested_symbols:
        return {
            "trade_date": trade_date,
            "requested_symbols": [],
            "coverage_ready": False,
            "error": "no symbols provided",
        }

    normalized_mode = str(mode or "replay").strip().lower()

    if normalized_mode == "live":
        # Validate-only — never auto-build inside daily-live.
        # Run `pivot-refresh --since <prev_trading_date>` pre-market before starting.
        coverage = validate_live_runtime_coverage(
            trade_date=trade_date,
            symbols=requested_symbols,
        )
    else:
        coverage = validate_daily_runtime_coverage(
            trade_date=trade_date,
            symbols=requested_symbols,
            read_only=True,
        )

    return {
        "trade_date": trade_date,
        "requested_symbols": requested_symbols,
        "coverage_ready": coverage["coverage_ready"],
        "mode": normalized_mode,
        "coverage": coverage,
        "runtime_refresh": None,
    }


def pre_filter_symbols_for_strategy(
    trade_date: str,
    symbols: list[str],
    strategy: str,
    strategy_params: dict[str, Any],
    *,
    require_trade_date_rows: bool = False,
) -> list[str]:
    """Pre-filter symbols before live/replay starts.

    CPR_LEVELS uses Phase 0 Stage A SQL on cpr_daily + cpr_thresholds so live
    can filter before market-day-state is available for the same date.
    """
    if not symbols:
        return []

    from engine.paper_runtime import _MARKET_DB_READ_LOCK

    db = _open_runtime_db(read_only=True)
    strategy_upper = (strategy or "CPR_LEVELS").upper()
    overrides = dict(strategy_params or {})
    min_price = float(overrides.get("min_price", 0.0))

    placeholders = ", ".join("?" for _ in symbols)

    if strategy_upper == "CPR_LEVELS":
        with _MARKET_DB_READ_LOCK:
            prefilter_row = db.con.execute(
                "SELECT MAX(trade_date)::VARCHAR FROM cpr_daily WHERE trade_date <= ?::DATE",
                [trade_date],
            ).fetchone()
        prefilter_date = str(prefilter_row[0])[:10] if prefilter_row and prefilter_row[0] else None
        if not prefilter_date:
            if require_trade_date_rows:
                raise RuntimeError(
                    "Live pre-filter prerequisites missing: no cpr_daily rows on or before "
                    f"{trade_date}. Run pivot-refresh + daily-prepare before daily-live."
                )
            return list(symbols)

        stage_a_sql = f"""
            SELECT c.symbol
            FROM cpr_daily c
            LEFT JOIN cpr_thresholds t
              ON t.symbol = c.symbol
             AND t.trade_date = c.trade_date
            WHERE c.trade_date = ?::DATE
              AND c.symbol IN ({placeholders})
              AND c.cpr_width_pct < COALESCE(t.cpr_threshold_pct, 2.0)
              AND c.prev_close >= ?
            ORDER BY c.symbol
        """
        with _MARKET_DB_READ_LOCK:
            rows = db.con.execute(
                stage_a_sql,
                [prefilter_date, *symbols, min_price],
            ).fetchall()
        return [str(row[0]) for row in rows]

    # Paper execution is CPR-only. For any other strategy value, keep the input
    # list unchanged instead of maintaining legacy parallel prefilter logic.
    return list(symbols)


__all__ = [
    "load_universe_symbols",
    "pre_filter_symbols_for_strategy",
    "prepare_runtime_for_daily_paper",
    "resolve_prepare_symbols",
    "resolve_trade_date",
    "snapshot_candidate_universe",
    "validate_daily_runtime_coverage",
    "validate_live_runtime_coverage",
]
