from __future__ import annotations

import sys

import scripts.data_quality as data_quality
from scripts.paper_prepare import resolve_trade_date


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

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
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
    assert "Active DQ issues (2025-01-01 -> 2026-03-27)" in out
    assert "Window checks (2025-01-01 -> 2026-03-27)" in out
    assert "OHLC_VIOLATION" in out
    assert "ZERO_PRICE" in out
    assert "SBIN" in out


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
            assert tables == [
                "or_daily",
                "market_day_state",
                "strategy_day_state",
                "intraday_day_pack",
            ]
            return {
                "or_daily": "2026-03-26",
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
            if "FROM v_daily" in sql or "FROM v_5min" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
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
            if "FROM v_daily" in sql or "FROM v_5min" in sql:
                return _FakeResult([("SBIN", prev_iso), ("TCS", prev_iso)])
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
            if "FROM v_daily" in sql or "FROM v_5min" in sql:
                return _FakeResult([(symbol, "2026-04-28") for symbol in present_symbols])
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


def test_setup_only_mode_blocks_future_state_rows(monkeypatch):
    today_iso = resolve_trade_date("today")
    prev_iso = "2026-04-28"

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeCon:
        def execute(self, sql, params=None):
            if "FROM v_daily" in sql or "FROM v_5min" in sql:
                return _FakeResult([("SBIN", prev_iso)])
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
            return {
                table: today_iso
                if table in {"market_day_state", "strategy_day_state"}
                else prev_iso
                for table in tables
            }

    monkeypatch.setattr(data_quality, "get_db", lambda: _FakeDb())
    monkeypatch.setattr(data_quality, "_is_pre_market", lambda _date: True)

    report = data_quality.build_trade_date_readiness_report(today_iso)

    assert report["ready"] is False
    assert report["freshness_blocking"] is True
    assert any("UNEXPECTED FUTURE STATE" in row["status"] for row in report["freshness_rows"])
