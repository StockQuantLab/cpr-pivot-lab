from __future__ import annotations

from datetime import date

import duckdb

from db import duckdb_table_ops as ops


def test_incremental_delete_filters_date_window_and_symbols() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE sample(symbol TEXT, trade_date DATE, value INT)")
    con.execute(
        """
        INSERT INTO sample VALUES
        ('AAA', '2026-04-01', 1),
        ('AAA', '2026-04-02', 2),
        ('BBB', '2026-04-02', 3),
        ('AAA', '2026-04-03', 4)
        """
    )

    deleted = ops.incremental_delete(
        con,
        table="sample",
        since_date="2026-04-02",
        until_date="2026-04-02",
        symbols=["AAA"],
        log_prefix="test",
    )

    assert deleted == 1
    rows = con.execute("SELECT * FROM sample ORDER BY symbol, trade_date").fetchall()
    assert rows == [
        ("AAA", date(2026, 4, 1), 1),
        ("AAA", date(2026, 4, 3), 4),
        ("BBB", date(2026, 4, 2), 3),
    ]


def test_incremental_replace_replaces_existing_unique_keys() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE target(symbol TEXT, trade_date DATE, value INT)")
    con.execute("CREATE UNIQUE INDEX idx_target_unique ON target(symbol, trade_date)")
    con.execute(
        """
        INSERT INTO target VALUES
        ('AAA', '2026-05-03', 1),
        ('AAA', '2026-05-04', 2),
        ('BBB', '2026-05-04', 3)
        """
    )
    con.execute(
        """
        CREATE TABLE source AS
        SELECT * FROM (VALUES
          ('AAA', DATE '2026-05-04', 20),
          ('CCC', DATE '2026-05-04', 40)
        ) AS t(symbol, trade_date, value)
        """
    )

    deleted = ops.incremental_replace(
        con,
        table="target",
        select_sql="SELECT * FROM source",
        since_date="2026-05-04",
        until_date="2026-05-04",
        log_prefix="test",
    )

    assert deleted == 2
    rows = con.execute("SELECT * FROM target ORDER BY symbol, trade_date").fetchall()
    assert rows == [
        ("AAA", date(2026, 5, 3), 1),
        ("AAA", date(2026, 5, 4), 20),
        ("CCC", date(2026, 5, 4), 40),
    ]


def test_symbol_scoped_upsert_uses_simple_delete_insert_path() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE target(symbol TEXT, trade_date DATE, value INT)")
    con.execute("CREATE TABLE source(symbol TEXT, trade_date DATE, value INT)")
    con.execute("INSERT INTO target VALUES ('AAA', '2026-04-01', 1), ('BBB', '2026-04-01', 2)")
    con.execute("INSERT INTO source VALUES ('AAA', '2026-04-01', 10)")

    ops.symbol_scoped_upsert(
        con,
        table="target",
        select_sql="SELECT * FROM source WHERE symbol = 'AAA'",
        symbols=["AAA"],
    )

    rows = con.execute("SELECT * FROM target ORDER BY symbol").fetchall()
    assert rows == [
        ("AAA", date(2026, 4, 1), 10),
        ("BBB", date(2026, 4, 1), 2),
    ]


def test_symbol_scoped_upsert_uses_temp_swap_path(monkeypatch) -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE target(symbol TEXT, trade_date DATE, value INT)")
    con.execute("CREATE TABLE source(symbol TEXT, trade_date DATE, value INT)")
    con.execute(
        """
        INSERT INTO target VALUES
        ('AAA', '2026-04-01', 1),
        ('BBB', '2026-04-01', 2),
        ('CCC', '2026-04-01', 3)
        """
    )
    con.execute(
        """
        INSERT INTO source VALUES
        ('AAA', '2026-04-01', 10),
        ('BBB', '2026-04-01', 20)
        """
    )
    monkeypatch.setattr(ops, "INCREMENTAL_BUILD_THRESHOLD", 1)

    ops.symbol_scoped_upsert(
        con,
        table="target",
        select_sql="SELECT * FROM source WHERE symbol IN ('AAA','BBB')",
        symbols=["AAA", "BBB"],
    )

    rows = con.execute("SELECT * FROM target ORDER BY symbol").fetchall()
    assert rows == [
        ("AAA", date(2026, 4, 1), 10),
        ("BBB", date(2026, 4, 1), 20),
        ("CCC", date(2026, 4, 1), 3),
    ]


def test_skip_if_table_fully_covered_returns_total_rows_for_complete_table() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE v_5min(symbol TEXT, date TIMESTAMP)")
    con.execute("CREATE TABLE runtime(symbol TEXT, trade_date DATE)")
    con.execute(
        """
        INSERT INTO v_5min VALUES
        ('AAA', '2026-04-01 09:15:00'),
        ('BBB', '2026-04-01 09:15:00'),
        ('AAA', '2026-04-02 09:15:00'),
        ('BBB', '2026-04-02 09:15:00')
        """
    )
    con.execute(
        """
        INSERT INTO runtime VALUES
        ('AAA', '2026-04-01'),
        ('BBB', '2026-04-01'),
        ('AAA', '2026-04-02'),
        ('BBB', '2026-04-02')
        """
    )

    total_rows = ops.skip_if_table_fully_covered(
        con,
        table="runtime",
        date_col="trade_date",
        since_date="2026-04-01",
        until_date="2026-04-02",
        build_symbols=["AAA", "BBB"],
        label="runtime",
    )

    assert total_rows == 4


def test_skip_if_table_fully_covered_returns_none_for_partial_table() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE v_5min(symbol TEXT, date TIMESTAMP)")
    con.execute("CREATE TABLE runtime(symbol TEXT, trade_date DATE)")
    con.execute(
        """
        INSERT INTO v_5min VALUES
        ('AAA', '2026-04-01 09:15:00'),
        ('BBB', '2026-04-01 09:15:00'),
        ('CCC', '2026-04-01 09:15:00')
        """
    )
    con.execute("INSERT INTO runtime VALUES ('AAA', '2026-04-01')")

    total_rows = ops.skip_if_table_fully_covered(
        con,
        table="runtime",
        date_col="trade_date",
        since_date="2026-04-01",
        until_date=None,
        build_symbols=["AAA", "BBB", "CCC"],
        label="runtime",
    )

    assert total_rows is None
