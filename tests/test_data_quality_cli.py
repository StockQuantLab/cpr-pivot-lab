from __future__ import annotations

import scripts.data_quality as data_quality


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
