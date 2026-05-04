"""Reusable table operation helpers for DuckDB market data builds."""

from __future__ import annotations

import logging
from typing import Any

from db.duckdb_validation import validate_table_identifier

logger = logging.getLogger(__name__)

# For small symbol sets, DELETE+INSERT is faster than temp table rebuild.
INCREMENTAL_BUILD_THRESHOLD = 100


def sql_symbol_list(symbols: list[str]) -> str:
    """Build a quoted, comma-separated SQL symbol list for IN clauses."""
    return ",".join(f"'{s}'" for s in symbols)


def incremental_delete(
    con: Any,
    *,
    table: str,
    since_date: str,
    until_date: str | None = None,
    symbols: list[str] | None = None,
    log_prefix: str,
) -> int:
    """Delete rows matching the incremental window and return matched row count."""
    table = validate_table_identifier(table)
    delete_parts = ["trade_date >= ?::DATE"]
    delete_params: list[object] = [since_date]
    if until_date:
        delete_parts.append("trade_date <= ?::DATE")
        delete_params.append(until_date)
    if symbols:
        delete_parts.append(f"symbol IN ({sql_symbol_list(symbols)})")
    where_sql = " AND ".join(delete_parts)
    matched = int(
        con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where_sql}",
            delete_params,
        ).fetchone()[0]
        or 0
    )
    con.execute(f"DELETE FROM {table} WHERE {where_sql}", delete_params)
    remaining = int(
        con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where_sql}",
            delete_params,
        ).fetchone()[0]
        or 0
    )
    print(
        f"  [{log_prefix}] incremental: deleted {matched:,} matched rows"
        f" for trade_date >= {since_date}" + (f" and <= {until_date}" if until_date else "")
    )
    if remaining:
        raise RuntimeError(
            f"{table} incremental delete left {remaining:,} rows in refresh window "
            f"for trade_date >= {since_date}" + (f" and <= {until_date}" if until_date else "")
        )
    return matched


def incremental_replace(
    con: Any,
    *,
    table: str,
    select_sql: str,
    since_date: str,
    until_date: str | None = None,
    symbols: list[str] | None = None,
    log_prefix: str,
) -> int:
    """Idempotently replace rows in an incremental window using DELETE + INSERT OR REPLACE."""
    table = validate_table_identifier(table)
    con.execute("BEGIN TRANSACTION")
    tx_open = True
    try:
        deleted = incremental_delete(
            con,
            table=table,
            since_date=since_date,
            until_date=until_date,
            symbols=symbols,
            log_prefix=log_prefix,
        )
        con.execute(f"INSERT OR REPLACE INTO {table} {select_sql}")
        con.execute("COMMIT")
        tx_open = False
        return deleted
    except Exception as e:
        if tx_open:
            con.execute("ROLLBACK")
        logger.exception("Failed to incrementally replace %s: %s", table, e)
        raise


def symbol_scoped_upsert(
    con: Any,
    *,
    table: str,
    select_sql: str,
    symbols: list[str],
) -> None:
    """Upsert rows for a symbol subset using DELETE+INSERT or temp-table swap."""
    table = validate_table_identifier(table)
    symbol_list = sql_symbol_list(symbols)
    use_simple_path = len(symbols) < INCREMENTAL_BUILD_THRESHOLD
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


def skip_if_table_fully_covered(
    con: Any,
    *,
    table: str,
    date_col: str,
    since_date: str,
    until_date: str | None,
    build_symbols: list[str],
    label: str,
) -> int | None:
    """Return row count when a table already covers all parquet dates in the window."""
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
