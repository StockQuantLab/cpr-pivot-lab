from __future__ import annotations

import sys
from datetime import datetime

import duckdb

import scripts.data_quality as data_quality
from db.duckdb_data_quality import MarketDataQualityMixin, timestamp_invalid_condition_sql
from scripts.paper_prepare import resolve_trade_date


def test_timestamp_invalid_condition_allows_configured_special_sessions():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE candles(date DATE, candle_time TIMESTAMP)
        """)
        con.execute("""
            INSERT INTO candles VALUES
                (DATE '2024-11-01', TIMESTAMP '2024-11-01 18:15:00'),
                (DATE '2024-11-01', TIMESTAMP '2024-11-01 20:00:00'),
                (DATE '2025-01-02', TIMESTAMP '2025-01-02 09:10:00'),
                (DATE '2025-01-02', TIMESTAMP '2025-01-02 09:15:00')
        """)
        rows = con.execute(
            f"""
            SELECT candle_time
            FROM candles
            WHERE {timestamp_invalid_condition_sql()}
            ORDER BY candle_time
            """
        ).fetchall()
    finally:
        con.close()

    assert rows == [
        (datetime(2024, 11, 1, 20, 0),),
        (datetime(2025, 1, 2, 9, 10),),
    ]


def test_baseline_window_report_fails_on_missing_runtime_rows(monkeypatch):
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0]

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            assert params == ["2025-01-01", "2026-05-05", "MAMATA", "SBIN"]
            if "missing_symbol_days" in sql:
                return _FakeResult([(200, 2)])
            if "SELECT src.symbol" in sql:
                return _FakeResult([("MAMATA", "2025-01-01"), ("SBIN", "2025-01-02")])
            raise AssertionError(f"Unexpected SQL: {sql}")

    class _FakeDb:
        def __init__(self):
            self.con = _FakeCon()

        def get_universe_symbols(self, name):
            assert name == "canonical_full"
            return ["SBIN", "MAMATA"]

    monkeypatch.setattr(
        data_quality,
        "_BASELINE_WINDOW_CHECKS",
        (("intraday_day_pack", "v_5min", "date"),),
    )

    report = data_quality.build_baseline_window_report(
        universe_name="canonical_full",
        start_date="2025-01-01",
        end_date="2026-05-05",
        db=_FakeDb(),
    )

    assert report["ready"] is False
    assert report["symbol_count"] == 2
    assert report["checks"][0]["source_symbol_days"] == 200
    assert report["checks"][0]["missing_symbol_days"] == 2
    assert report["checks"][0]["samples"][0] == {
        "symbol": "MAMATA",
        "trade_date": "2025-01-01",
    }


def test_pack_rvol_all_null_symbols_requires_prior_pack_context():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE intraday_day_pack(
                symbol VARCHAR,
                trade_date DATE,
                rvol_baseline_arr DOUBLE[]
            )
        """)
        con.execute("""
            INSERT INTO intraday_day_pack VALUES
                ('SBIN', DATE '2026-05-05', [100.0]),
                ('SBIN', DATE '2026-05-06', [NULL, NULL]),
                ('TCS', DATE '2026-05-05', [50.0]),
                ('TCS', DATE '2026-05-06', [50.0, NULL]),
                ('NEWLIST', DATE '2026-05-06', [NULL, NULL])
        """)

        class _Db:
            pass

        db = _Db()
        db.con = con
        symbols = data_quality._pack_rvol_all_null_symbols(
            db,
            symbols=["SBIN", "TCS", "NEWLIST"],
            pack_date="2026-05-06",
        )
    finally:
        con.close()

    assert symbols == ["SBIN"]


def test_baseline_window_report_fails_on_all_null_pack_rvol(monkeypatch):
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE intraday_day_pack(
                symbol VARCHAR,
                trade_date DATE,
                rvol_baseline_arr DOUBLE[]
            )
        """)
        con.execute("""
            INSERT INTO intraday_day_pack VALUES
                ('SBIN', DATE '2026-05-05', [100.0]),
                ('SBIN', DATE '2026-05-06', [NULL, NULL]),
                ('TCS', DATE '2026-05-05', [50.0]),
                ('TCS', DATE '2026-05-06', [50.0, NULL])
        """)

        class _Db:
            def __init__(self):
                self.con = con

            def get_universe_symbols(self, name):
                assert name == "canonical_full"
                return ["SBIN", "TCS"]

        monkeypatch.setattr(data_quality, "_BASELINE_WINDOW_CHECKS", ())

        report = data_quality.build_baseline_window_report(
            universe_name="canonical_full",
            start_date="2026-05-05",
            end_date="2026-05-06",
            db=_Db(),
        )
    finally:
        con.close()

    assert report["ready"] is False
    assert report["quality_checks"][0]["issue_code"] == "PACK_RVOL_ALL_NULL"
    assert report["quality_checks"][0]["failing_symbol_days"] == 1
    assert report["quality_checks"][0]["samples"] == [
        {"symbol": "SBIN", "trade_date": "2026-05-06"}
    ]


def test_baseline_window_atr_source_excludes_zero_volume_index_days(monkeypatch):
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE v_5min(
                symbol VARCHAR,
                date DATE,
                true_range DOUBLE,
                volume BIGINT
            )
        """)
        con.execute("""
            CREATE TABLE atr_intraday(
                symbol VARCHAR,
                trade_date DATE,
                prev_date DATE,
                atr DOUBLE
            )
        """)
        con.executemany(
            "INSERT INTO v_5min VALUES (?, ?, ?, ?)",
            [
                ("SBIN", "2026-05-05", 10.0, 100),
                ("SBIN", "2026-05-05", 11.0, 100),
                ("SBIN", "2026-05-05", 12.0, 100),
                ("SBIN", "2026-05-05", 13.0, 100),
                ("SBIN", "2026-05-05", 14.0, 100),
                ("SBIN", "2026-05-05", 15.0, 100),
                ("SBIN", "2026-05-06", 10.0, 100),
                ("SBIN", "2026-05-06", 11.0, 100),
                ("SBIN", "2026-05-06", 12.0, 100),
                ("SBIN", "2026-05-06", 13.0, 100),
                ("SBIN", "2026-05-06", 14.0, 100),
                ("SBIN", "2026-05-06", 15.0, 100),
                ("NIFTY 50", "2026-05-05", 10.0, 0),
                ("NIFTY 50", "2026-05-05", 11.0, 0),
                ("NIFTY 50", "2026-05-05", 12.0, 0),
                ("NIFTY 50", "2026-05-05", 13.0, 0),
                ("NIFTY 50", "2026-05-05", 14.0, 0),
                ("NIFTY 50", "2026-05-05", 15.0, 0),
                ("NIFTY 50", "2026-05-06", 10.0, 0),
                ("NIFTY 50", "2026-05-06", 11.0, 0),
                ("NIFTY 50", "2026-05-06", 12.0, 0),
                ("NIFTY 50", "2026-05-06", 13.0, 0),
                ("NIFTY 50", "2026-05-06", 14.0, 0),
                ("NIFTY 50", "2026-05-06", 15.0, 0),
            ],
        )

        class _Db:
            def __init__(self):
                self.con = con

            def get_universe_symbols(self, name):
                assert name == "canonical_full"
                return ["SBIN", "NIFTY 50"]

        monkeypatch.setattr(
            data_quality,
            "_BASELINE_WINDOW_CHECKS",
            (("atr_intraday", "v_5min", "date"),),
        )

        report = data_quality.build_baseline_window_report(
            universe_name="canonical_full",
            start_date="2026-05-05",
            end_date="2026-05-06",
            db=_Db(),
        )
    finally:
        con.close()

    assert report["checks"][0]["source_symbol_days"] == 1
    assert report["checks"][0]["missing_symbol_days"] == 1
    assert report["checks"][0]["samples"] == [
        {"symbol": "SBIN", "trade_date": "2026-05-06"}
    ]


def test_comprehensive_dq_scan_registers_all_null_pack_rvol():
    class _Db(MarketDataQualityMixin):
        def __init__(self):
            self.con = duckdb.connect(":memory:")
            self.read_only = False
            self._has_5min = True
            self._has_daily = True
            self._parquet_dir = None
            self._sync = None
            self.publish_count = 0

        def _begin_replica_batch(self) -> None:
            pass

        def _end_replica_batch(self) -> None:
            pass

        def _publish_replica(self, *, force: bool = False) -> None:
            self.publish_count += int(force)

        def _table_exists(self, table: str) -> bool:
            row = self.con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = ?
                """,
                [table],
            ).fetchone()
            return bool(row and row[0])

    db = _Db()
    try:
        db.con.execute("""
            CREATE TABLE v_5min(
                symbol VARCHAR,
                date DATE,
                candle_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
        """)
        db.con.execute("""
            CREATE TABLE intraday_day_pack(
                symbol VARCHAR,
                trade_date DATE,
                rvol_baseline_arr DOUBLE[]
            )
        """)
        db.con.execute("""
            INSERT INTO intraday_day_pack VALUES
                ('SBIN', DATE '2026-05-05', [100.0]),
                ('SBIN', DATE '2026-05-06', [NULL, NULL]),
                ('TCS', DATE '2026-05-05', [50.0]),
                ('TCS', DATE '2026-05-06', [50.0, NULL])
        """)

        summary = db.run_comprehensive_dq_scan()
        rows = db.get_data_quality_issues(issue_code="PACK_RVOL_ALL_NULL")
    finally:
        db.con.close()

    assert summary["PACK_RVOL_ALL_NULL"] == 1
    assert rows[0]["symbol"] == "SBIN"
    assert rows[0]["severity"] == "CRITICAL"


def test_baseline_window_cli_returns_nonzero_when_not_ready(monkeypatch, capsys):
    monkeypatch.setattr(
        data_quality,
        "build_baseline_window_report",
        lambda **_: {
            "ready": False,
            "universe_name": "canonical_full",
            "symbol_count": 2,
            "start_date": "2025-01-01",
            "end_date": "2026-05-05",
            "checks": [
                {
                    "target_table": "intraday_day_pack",
                    "source_table": "v_5min",
                    "source_symbol_days": 200,
                    "missing_symbol_days": 1,
                    "samples": [{"symbol": "MAMATA", "trade_date": "2025-01-01"}],
                }
            ],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-data-quality",
            "--baseline-window",
            "--universe-name",
            "canonical_full",
            "--start",
            "2025-01-01",
            "--end",
            "2026-05-05",
        ],
    )

    assert data_quality.main() == 1

    out = capsys.readouterr().out
    assert "Baseline-window runtime coverage: FAIL" in out
    assert "intraday_day_pack" in out
    assert "MAMATA:2025-01-01" in out


def test_windowed_dq_report_prints_registry_and_window_counts(monkeypatch, capsys):
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0]

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "MIN(details) AS sample_detail" in sql:
                return _FakeResult(
                    [
                        ("OHLC_VIOLATION", "CRITICAL", 2, "sample ohlc detail"),
                        ("ZERO_PRICE", "WARNING", 1, "sample zero detail"),
                    ]
                )
            if "high < low" in sql and "COUNT(*)" in sql:
                return _FakeResult([(3,)])
            if "(open = 0 OR close = 0 OR high = 0)" in sql:
                return _FakeResult([(4,)])
            if "HOUR(candle_time) < 9" in sql and "COUNT(*)" in sql:
                return _FakeResult([(5,)])
            if "(high - low) / open > 0.5" in sql and "COUNT(*)" in sql:
                return _FakeResult([(6,)])
            if "dup_candles" in sql:
                assert params == ["2025-01-01", "2026-03-27"]
                return _FakeResult([(7,)])
            if "day_vol = 0" in sql:
                assert params == ["2025-01-01", "2026-03-27"]
                return _FakeResult([(8,)])
            if "gap_days > 7" in sql:
                assert params == ["2026-03-27", "2025-01-01", "2026-03-27"]
                return _FakeResult([(9,)])
            if "FROM intraday_day_pack p" in sql and "rvol_baseline_arr" in sql:
                assert params == ["2025-01-01", "2026-03-27"]
                return _FakeResult([(10,)])
            raise AssertionError(f"Unexpected SQL: {sql}")

    class _FakeDb:
        def __init__(self):
            self.con = _FakeCon()

        def get_data_quality_issues(self, active_only=True, issue_code=None):
            return [
                {
                    "symbol": "SBIN",
                    "issue_code": "OHLC_VIOLATION",
                    "severity": "CRITICAL",
                    "details": "demo detail",
                    "is_active": True,
                    "first_seen": "2025-01-01",
                    "last_seen": "2025-03-27",
                }
            ]

    monkeypatch.setattr(data_quality, "get_live_market_db", lambda: _FakeDb())
    monkeypatch.setattr(
        "sys.argv",
        [
            "pivot-data-quality",
            "--window-start",
            "2025-01-01",
            "--window-end",
            "2026-03-27",
            "--limit",
            "0",
        ],
    )

    data_quality.main()

    out = capsys.readouterr().out
    assert "Active DQ issues (registry-wide; not window-scoped)" in out
    assert "Window checks (2025-01-01 -> 2026-03-27; read-only, registry not mutated)" in out
    assert "OHLC_VIOLATION" in out
    assert "ZERO_PRICE" in out
    assert "DUPLICATE_CANDLE" in out
    assert "ZERO_VOLUME_DAY" in out
    assert "DATE_GAP" in out
    assert "PACK_RVOL_ALL_NULL" in out
    assert "SBIN" in out


def test_windowed_refresh_full_is_read_only(monkeypatch, capsys):
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0]

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM data_quality_issues" in sql:
                return _FakeResult([])
            return _FakeResult([(0,)])

    class _FakeDb:
        con = _FakeCon()

    monkeypatch.setattr(data_quality, "get_live_market_db", lambda: _FakeDb())
    monkeypatch.setattr(
        data_quality,
        "get_db",
        lambda: (_ for _ in ()).throw(AssertionError("must not open writer db")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-data-quality",
            "--refresh",
            "--full",
            "--window-start",
            "2025-01-01",
            "--window-end",
            "2026-03-27",
        ],
    )

    assert data_quality.main() == 0
    out = capsys.readouterr().out
    assert "read-only and does not mutate" in out


def test_trade_date_dq_report_flags_freshness_and_runtime_gaps(monkeypatch, capsys):
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0]

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "minute_arr[1] = 555" in sql:
                return _FakeResult([("SBIN",), ("TCS",)])
            raise AssertionError(f"Unexpected SQL: {sql}")

    class _FakeDb:
        def __init__(self):
            self.con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            assert trade_dates == ["2026-03-27"]
            return {"SBIN", "TCS", "ICICIBANK"}

        def get_table_max_trade_dates(self, tables):
            assert set(tables) == {
                "or_daily",
                "cpr_daily",
                "cpr_thresholds",
                "market_day_state",
                "strategy_day_state",
                "intraday_day_pack",
            }
            return {
                "or_daily": "2026-03-26",
                "cpr_daily": "2026-03-26",
                "cpr_thresholds": "2026-03-26",
                "market_day_state": "2026-03-26",
                "strategy_day_state": "2026-03-26",
                "intraday_day_pack": "2026-03-27",
            }

        def get_runtime_trade_date_coverage(self, symbols, trade_date):
            assert symbols == ["ICICIBANK", "SBIN", "TCS"]
            assert trade_date == "2026-03-27"
            return {
                "market_day_state": ["SBIN"],
                "strategy_day_state": ["SBIN"],
                "intraday_day_pack": [],
            }

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(
        "sys.argv",
        [
            "pivot-data-quality",
            "--date",
            "2026-03-27",
        ],
    )

    exit_code = data_quality.main()

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "Trade-date readiness (2026-03-27)" in out
    assert "Freshness comparison" in out
    assert "OUT OF SYNC" in out
    assert "no 09:15 candle (info only): 1" in out
    assert "ICICIBANK" in out
    assert "[BLOCKING]" in out
    assert "Readiness:" in out


def test_date_today_keyword_resolves_without_crash(monkeypatch):
    """--date today must not raise ConversionException — resolves to ISO date before DB call."""
    calls = []

    class _FakeDb:
        def get_symbols_with_parquet_data(self, trade_dates):
            calls.append(trade_dates)
            # Must receive an ISO date, never the literal string "today"
            assert trade_dates != ["today"], "raw 'today' keyword reached the DB layer"
            assert len(trade_dates) == 1 and trade_dates[0] != "today"
            return set()

        def get_table_max_trade_dates(self, tables):
            return {}

        def get_runtime_trade_date_coverage(self, symbols, trade_date):
            return {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []}

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(sys, "argv", ["pivot-data-quality", "--date", "today"])

    # Should not raise; exit code 1 is fine (no data)
    try:
        data_quality.main()
    except SystemExit:
        pass

    assert calls, "get_symbols_with_parquet_data was never called"


def test_pre_market_mode_reports_ready_when_previous_day_prereqs_current(monkeypatch, capsys):
    """Pre-market: Ready=YES when previous completed-day live prerequisites exist."""
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
            if "FROM cpr_daily" in sql:
                return _FakeResult([("SBIN",), ("TCS",)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([("NONE", 2)])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()  # no 5-min data yet — normal pre-market

        def get_universe_symbols(self, name):
            return ["SBIN", "TCS"]

        def get_table_max_trade_dates(self, tables):
            return dict.fromkeys(tables, today_iso)

        def get_runtime_trade_date_coverage(self, symbols, trade_date):
            return {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []}

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    # Force pre-market mode regardless of actual clock
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)
    monkeypatch.setattr(sys, "argv", ["pivot-data-quality", "--date", today_iso])

    exit_code = data_quality.main()

    out = capsys.readouterr().out
    assert exit_code == 0, "pre-market with previous-day prerequisites must report Ready=YES"
    assert "PRE-MARKET MODE" in out
    assert "Ready" in out


def test_setup_only_mode_falls_back_to_canonical_universe(monkeypatch):
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
            if "FROM cpr_daily" in sql:
                return _FakeResult([("SBIN",), ("TCS",)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()

        def get_universe_symbols(self, name):
            if name == data_quality._default_full_universe_name(today_iso):
                return []
            if name == data_quality.CANONICAL_FULL_UNIVERSE_NAME:
                return ["SBIN", "TCS"]
            return []

        def get_table_max_trade_dates(self, tables):
            return dict.fromkeys(tables, prev_iso)

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is True
    assert report["symbol_source"] == data_quality.CANONICAL_FULL_UNIVERSE_NAME
    assert report["requested_symbols"] == ["SBIN", "TCS"]


def test_pre_market_mode_blocks_when_previous_day_prereqs_missing(monkeypatch):
    today_iso = resolve_trade_date("today")

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()

        def get_universe_symbols(self, name):
            return ["SBIN", "TCS"]

        def get_table_max_trade_dates(self, tables):
            return dict.fromkeys(tables, today_iso)

        def get_runtime_trade_date_coverage(self, symbols, trade_date):
            return {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []}

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is False
    assert report["coverage"]["v_daily"] == ["SBIN", "TCS"]
    assert report["coverage"]["v_5min"] == ["SBIN", "TCS"]
    assert report["coverage"]["atr_intraday"] == ["SBIN", "TCS"]
    assert report["coverage"]["cpr_thresholds"] == ["SBIN", "TCS"]


def test_pre_market_mode_warns_on_sparse_missing_symbols(monkeypatch):
    today_iso = resolve_trade_date("today")
    symbols = [f"SYM{i:03d}" for i in range(100)]
    present_symbols = symbols[1:]

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([("NONE", len(present_symbols))])
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([(symbol, "2026-04-28") for symbol in present_symbols])
            if "FROM cpr_daily" in sql:
                return _FakeResult([(symbol,) for symbol in present_symbols])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([(symbol, "2026-04-28") for symbol in present_symbols])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()

        def get_universe_symbols(self, name):
            return symbols

        def get_table_max_trade_dates(self, tables):
            return dict.fromkeys(tables, today_iso)

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is True
    assert report["coverage"]["v_daily"] == ["SYM000"]
    assert report["coverage_status"]["v_daily"] == "warning"


def test_live_prereq_coverage_uses_runtime_pack_for_5min_prereq():
    queries: list[str] = []

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            queries.append(sql)
            if "FROM v_daily" in sql:
                return _FakeResult([("SBIN", "2026-05-04")])
            if "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", "2026-05-04")])
            if "FROM atr_intraday" in sql:
                return _FakeResult([("SBIN", "2026-05-04")])
            if "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", "2026-05-04")])
            raise AssertionError(f"Unexpected query: {sql}")

    class _FakeDb:
        con = _FakeCon()

    coverage = data_quality._live_prereq_coverage(_FakeDb(), ["SBIN"], "2026-05-05")

    assert coverage["v_5min"] == []
    assert any("FROM intraday_day_pack" in query for query in queries)
    assert not any("FROM v_5min" in query for query in queries)


def test_setup_only_mode_next_day_state_at_trade_date_is_valid(monkeypatch):
    """Ready=YES when state tables are at trade_date and pack at prev_day.

    This is the expected post-EOD state: cpr_daily/thresholds/market_day_state/strategy_day_state
    are built for tomorrow (trade_date) while intraday_day_pack is still at today (prev_day).
    This must NOT be flagged as 'UNEXPECTED FUTURE STATE'.
    """
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM cpr_daily" in sql and "SELECT COUNT(*)" not in sql:
                return _FakeResult([("SBIN",)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([("NONE", 1)])
            # COUNT queries: next-day setup rows are present (2032 rows for trade_date).
            if "SELECT COUNT(*)" in sql and (
                "FROM market_day_state" in sql or "FROM cpr_daily" in sql
            ):
                return _FakeResult([(2032,)])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()  # no 5-min data = setup_only_mode

        def get_universe_symbols(self, name):
            return ["SBIN"]

        def get_table_max_trade_dates(self, tables):
            # State tables at trade_date (next-day setup built by EOD), pack at prev_iso.
            return {
                table: today_iso
                if table
                in {"cpr_daily", "cpr_thresholds", "market_day_state", "strategy_day_state"}
                else prev_iso
                for table in tables
            }

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is True, "Post-EOD state (state@trade_date, pack@prev) must be ready"
    assert report["freshness_blocking"] is False
    assert not any("UNEXPECTED FUTURE STATE" in row["status"] for row in report["freshness_rows"])
    assert all(
        "OK next-day" in row["status"] or "OK" in row["status"]
        for row in report["freshness_rows"]
        if row["table"] in {"market_day_state", "strategy_day_state", "cpr_daily", "cpr_thresholds"}
    )


def test_setup_only_mode_blocks_state_rows_beyond_trade_date(monkeypatch):
    """Ready=NO when state tables are built for a date BEYOND trade_date — genuinely unexpected."""
    today_iso = resolve_trade_date("today")
    import datetime
    from zoneinfo import ZoneInfo

    tomorrow_iso = (
        datetime.datetime.now(ZoneInfo("Asia/Kolkata")).date() + datetime.timedelta(days=1)
    ).isoformat()
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM cpr_daily" in sql and "SELECT COUNT(*)" not in sql:
                return _FakeResult([("SBIN",)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()

        def get_universe_symbols(self, name):
            return ["SBIN"]

        def get_table_max_trade_dates(self, tables):
            # State tables at TOMORROW (beyond today = trade_date) — genuinely unexpected.
            return {
                table: tomorrow_iso
                if table in {"market_day_state", "strategy_day_state"}
                else prev_iso
                for table in tables
            }

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is False, "State rows beyond trade_date must block"
    assert report["freshness_blocking"] is True
    assert any("UNEXPECTED FUTURE STATE" in row["status"] for row in report["freshness_rows"])


def test_setup_only_fails_when_next_day_market_day_state_missing(monkeypatch):
    """Ready=NO when market_day_state has 0 rows for the target trade_date (Apr30 failure mode)."""
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM cpr_daily" in sql and "SELECT COUNT(*)" not in sql:
                return _FakeResult([("SBIN",)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([("NONE", 1)])
            # Both market_day_state and cpr_daily COUNT queries return 0 — missing next-day rows.
            if "SELECT COUNT(*)" in sql and (
                "FROM market_day_state" in sql or "FROM cpr_daily" in sql
            ):
                return _FakeResult([(0,)])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()  # no 5-min data = setup_only_mode

        def get_universe_symbols(self, name):
            return ["SBIN"]

        def get_table_max_trade_dates(self, tables):
            return dict.fromkeys(tables, prev_iso)  # prev-day data is fresh — not the issue

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is False, "Must block when market_day_state is 0 for trade_date"
    assert report["next_day_rows_missing"] is True
    assert report["next_day_mds_count"] == 0


def test_setup_only_passes_when_next_day_state_exists_without_pack(monkeypatch):
    """Ready=YES when state tables are at trade_date and intraday_day_pack is at prev_day.

    Exact post-EOD state: cpr/thresholds/state/strategy built for tomorrow,
    pack not yet available (tomorrow's candles don't exist). Must be ready.
    """
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql or "FROM intraday_day_pack" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM atr_intraday" in sql or "FROM cpr_thresholds" in sql:
                return _FakeResult([("SBIN", prev_iso)])
            if "FROM strategy_day_state" in sql and "GROUP BY 1" in sql:
                return _FakeResult([("NONE", 1)])
            # COUNT queries: next-day setup rows present.
            if "SELECT COUNT(*)" in sql and (
                "FROM market_day_state" in sql or "FROM cpr_daily" in sql
            ):
                return _FakeResult([(2032,)])
            if "FROM cpr_daily" in sql:
                return _FakeResult([("SBIN",)])
            return _FakeResult([])

    class _FakeDb:
        con = _FakeCon()

        def get_symbols_with_parquet_data(self, trade_dates):
            return set()  # no 5-min data = setup_only_mode

        def get_universe_symbols(self, name):
            return ["SBIN"]

        def get_table_max_trade_dates(self, tables):
            # Real post-EOD state: setup tables at trade_date, pack at prev_iso.
            return {
                table: today_iso
                if table
                in {"cpr_daily", "cpr_thresholds", "market_day_state", "strategy_day_state"}
                else prev_iso
                for table in tables
            }

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is True, (
        "Post-EOD: state@trade_date + pack@prev_day + COUNT>0 must be ready"
    )
    assert report["next_day_rows_missing"] is False
    assert report["next_day_mds_count"] == 2032
    assert report["freshness_blocking"] is False


def test_sync_replica_verify_reads_replica_file_not_source(monkeypatch, tmp_path):
    """pivot-sync-replica --verify must open the replica file, not the source db."""
    import scripts.sync_replica as sync_replica_mod

    fake_replica_file = tmp_path / "market_replica_v1.duckdb"
    fake_replica_file.write_bytes(b"")  # placeholder

    latest_pointer = tmp_path / "market_replica_latest"
    latest_pointer.write_text("v1")

    opened_paths: list[str] = []

    class _FakeSync:
        def __init__(self, db_path, replica_dir):
            self.latest_pointer = latest_pointer
            self._db_path = db_path

        def force_sync(self, source_conn=None):
            pass  # succeed silently

    class _FakeReplicaCon:
        def execute(self, sql, params=None):
            return self

        def fetchone(self):
            return (1500,)  # rows present

        def close(self):
            pass

    def _fake_duckdb_connect(path, read_only=False):
        opened_paths.append(path)
        return _FakeReplicaCon()

    class _FakeSourceDb:
        con = object()

    # All these are module-level imports in sync_replica.py — patchable directly.
    monkeypatch.setattr(sync_replica_mod, "ReplicaSync", _FakeSync)
    monkeypatch.setattr(
        sync_replica_mod, "duckdb", type("m", (), {"connect": staticmethod(_fake_duckdb_connect)})()
    )
    monkeypatch.setattr(sync_replica_mod, "REPLICA_DIR", tmp_path)
    monkeypatch.setattr(sync_replica_mod, "DUCKDB_FILE", tmp_path / "market.duckdb")
    monkeypatch.setattr(sync_replica_mod, "get_db", lambda: _FakeSourceDb())  # type: ignore[attr-defined]
    monkeypatch.setattr(
        sync_replica_mod,
        "_get_trade_date",
        lambda raw: "2026-04-30",
    )

    import sys

    monkeypatch.setattr(
        sys, "argv", ["pivot-sync-replica", "--verify", "--trade-date", "2026-04-30"]
    )
    sync_replica_mod.main()

    # The only duckdb.connect call should be to the replica file, not the source db.
    assert len(opened_paths) == 1, f"Expected 1 connection opened, got: {opened_paths}"
    assert str(fake_replica_file) in opened_paths[0], (
        f"Expected replica file {fake_replica_file} to be opened, got: {opened_paths}"
    )
