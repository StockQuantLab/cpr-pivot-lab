"""Tests for dashboard state helpers."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

import web.state as web_state
from db.backtest_db import BacktestDB
from db.duckdb import MarketDB


@pytest.mark.asyncio
async def test_aget_paper_session_snapshot_uses_postgres_live_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    async def fake_get_session(session_id: str):
        calls.append(("session", session_id))
        return SimpleNamespace(session_id=session_id, strategy="CPR_LEVELS", status="ACTIVE")

    async def fake_get_session_positions(session_id: str, symbol: str | None = None, statuses=None):
        calls.append(("positions", session_id))
        return [
            SimpleNamespace(position_id=1, symbol="SBIN"),
            SimpleNamespace(position_id=2, symbol="TCS"),
        ]

    async def fake_get_session_orders(
        session_id: str,
        symbol: str | None = None,
        status: str | None = None,
        limit: int = 25,
    ):
        calls.append(("orders", session_id))
        return [SimpleNamespace(order_id=11), SimpleNamespace(order_id=12)]

    async def fake_get_feed_state(session_id: str):
        calls.append(("feed", session_id))
        return SimpleNamespace(status="OK", stale_reason=None, last_price=101.5)

    monkeypatch.setattr(web_state, "get_session", fake_get_session)
    monkeypatch.setattr(web_state, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(web_state, "get_session_orders", fake_get_session_orders)
    monkeypatch.setattr(web_state, "get_feed_state", fake_get_feed_state)
    monkeypatch.setattr(
        web_state,
        "summarize_paper_positions",
        lambda session, positions, feed_state: {
            "session_id": session.session_id,
            "feed_status": feed_state.status,
            "open_positions": len(positions),
        },
    )

    snapshot = await web_state.aget_paper_session_snapshot("paper-1")

    assert calls == [
        ("session", "paper-1"),
        ("positions", "paper-1"),
        ("orders", "paper-1"),
        ("feed", "paper-1"),
    ]
    assert snapshot["session"].session_id == "paper-1"
    assert len(snapshot["positions"]) == 2
    assert len(snapshot["orders"]) == 2
    assert snapshot["summary"]["feed_status"] == "OK"
    assert snapshot["summary"]["orders"] == 2
    assert snapshot["summary"]["open_positions"] == 2


@pytest.mark.asyncio
async def test_aget_paper_active_sessions_aggregates_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_get_active_sessions():
        return [
            SimpleNamespace(session_id="paper-1"),
            SimpleNamespace(session_id="paper-2"),
        ]

    async def fake_snapshot(session_id: str):
        return {
            "session": SimpleNamespace(session_id=session_id, strategy="CPR_LEVELS"),
            "summary": {"session_id": session_id, "orders": 1},
            "positions": [],
            "orders": [],
            "feed_state": None,
        }

    monkeypatch.setattr(web_state, "get_active_sessions", fake_get_active_sessions)
    monkeypatch.setattr(web_state, "aget_paper_session_snapshot", fake_snapshot)

    active_sessions = await web_state.aget_paper_active_sessions()

    assert [payload["session"].session_id for payload in active_sessions] == ["paper-1", "paper-2"]
    assert all(payload["summary"]["orders"] == 1 for payload in active_sessions)


def test_fetch_live_readiness_uses_dashboard_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import data_quality

    fake_db = object()
    monkeypatch.setattr(web_state, "get_dashboard_db", lambda: fake_db)

    def fake_report(trade_date: str, *, db=None):
        assert trade_date == "2026-04-30"
        assert db is fake_db
        return {
            "trade_date": trade_date,
            "ready": False,
            "requested_symbols": ["SBIN", "TCS"],
            "coverage_status": {"market_day_state": "blocking", "v_5min": "warning"},
            "missing_counts": {"market_day_state": 2, "v_5min": 1},
        }

    monkeypatch.setattr(data_quality, "build_trade_date_readiness_report", fake_report)

    result = web_state._fetch_live_readiness_sync("2026-04-30")

    assert result["requested_count"] == 2
    assert result["blocking_missing_counts"] == {"market_day_state": 2}


def test_fetch_live_readiness_defaults_to_prepared_runtime_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import data_quality

    class FakeDB:
        def get_table_max_trade_dates(self, tables):
            return {
                "intraday_day_pack": "2026-04-30",
                "cpr_daily": "2026-05-04",
                "cpr_thresholds": "2026-05-04",
                "market_day_state": "2026-05-04",
                "strategy_day_state": "2026-05-04",
            }

    fake_db = FakeDB()
    monkeypatch.setattr(web_state, "get_dashboard_db", lambda: fake_db)

    def fake_report(trade_date: str, *, db=None):
        assert trade_date == "2026-05-04"
        assert db is fake_db
        return {
            "trade_date": trade_date,
            "ready": True,
            "requested_symbols": ["SBIN"],
            "coverage_status": {},
            "missing_counts": {},
        }

    monkeypatch.setattr(data_quality, "build_trade_date_readiness_report", fake_report)

    result = web_state._fetch_live_readiness_sync(None)

    assert result["trade_date"] == "2026-05-04"
    assert result["date_source"] == "prepared_runtime"
    assert result["requested_count"] == 1


def test_fetch_live_readiness_adds_operator_status_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import data_quality

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class FakeCon:
        def execute(self, sql, params=None):
            if "FROM cpr_daily" in sql:
                return FakeResult([(2018,)])
            if "FROM cpr_thresholds" in sql:
                return FakeResult([(2018,)])
            if "FROM market_day_state" in sql:
                return FakeResult([(1993,)])
            if "FROM strategy_day_state" in sql:
                return FakeResult([(1993,)])
            return FakeResult([(0,)])

    class FakeDB:
        con = FakeCon()

    fake_db = FakeDB()
    monkeypatch.setattr(web_state, "get_dashboard_db", lambda: fake_db)

    def fake_report(trade_date: str, *, db=None):
        assert trade_date == "2026-05-04"
        assert db is fake_db
        return {
            "trade_date": trade_date,
            "ready": True,
            "requested_symbols": ["SBIN", "TCS"],
            "freshness_rows": [
                {
                    "table": "market_day_state",
                    "max_trade_date": "2026-05-04",
                    "status": "OK next-day (2026-05-04)",
                },
                {
                    "table": "intraday_day_pack",
                    "max_trade_date": "2026-04-30",
                    "status": "OK",
                },
            ],
            "coverage_status": {"v_daily": "ok", "v_5min": "warning"},
            "missing_counts": {"v_daily": 0, "v_5min": 1},
        }

    monkeypatch.setattr(data_quality, "build_trade_date_readiness_report", fake_report)
    monkeypatch.setattr(web_state, "_today_ist_iso", lambda: "2026-05-04")

    result = web_state._fetch_live_readiness_sync("2026-05-04")

    assert {row["status"] for row in result["setup_table_status_rows"]} == {"OK"}
    assert result["freshness_status_rows"][0]["status"] == "OK"
    assert result["freshness_status_rows"][0]["detail"] == ("OK current trade date (2026-05-04)")
    coverage_rows = {row["table"]: row for row in result["coverage_status_rows"]}
    assert coverage_rows["v_5min"] == {
        "table": "v_5min",
        "value": "1 missing",
        "status": "OK",
        "detail": "WARNING",
    }


def test_live_readiness_freshness_detail_labels_next_trade_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_state, "_today_ist_iso", lambda: "2026-05-04")

    detail = web_state._live_readiness_freshness_detail(
        max_trade_date="2026-05-05",
        raw_status="OK next-day (2026-05-05)",
        trade_date="2026-05-05",
    )

    assert detail == "OK next trade date (2026-05-05)"


@pytest.mark.asyncio
async def test_aqueue_paper_admin_command_writes_command_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = await web_state.aqueue_paper_admin_command(
        session_id="CPR_LEVELS_LONG-2026-04-24-live-kite",
        action="close_positions",
        symbols=["sbin", "reliance"],
        reason="dashboard_test",
        requester="pytest",
    )

    command_path = Path(str(result["command_file"]))
    payload = json.loads(command_path.read_text())
    assert payload["action"] == "close_positions"
    assert payload["symbols"] == ["SBIN", "RELIANCE"]
    assert payload["reason"] == "dashboard_test"


@pytest.mark.asyncio
async def test_aqueue_paper_admin_command_accepts_pause_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = await web_state.aqueue_paper_admin_command(
        session_id="CPR_LEVELS_LONG-2026-04-24-live-kite",
        action="pause_entries",
        reason="dashboard_test",
        requester="pytest",
    )

    payload = json.loads(Path(str(result["command_file"])).read_text())
    assert payload["action"] == "pause_entries"


@pytest.mark.asyncio
async def test_aflatten_both_paper_sessions_queues_long_and_short(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    fake_db = SimpleNamespace(
        get_active_sessions=lambda: [
            SimpleNamespace(
                session_id="CPR_LEVELS_LONG-2026-04-24-live-kite",
                direction="LONG",
                trade_date="2026-04-24",
            ),
            SimpleNamespace(
                session_id="CPR_LEVELS_SHORT-2026-04-24-live-kite",
                direction="SHORT",
                trade_date="2026-04-24",
            ),
            SimpleNamespace(
                session_id="CPR_LEVELS_LONG-2026-04-23-live-kite",
                direction="LONG",
                trade_date="2026-04-23",
            ),
        ]
    )
    monkeypatch.setattr(web_state, "get_dashboard_paper_db", lambda: fake_db)

    result = await web_state.aflatten_both_paper_sessions(
        trade_date="2026-04-24",
        reason="risk_off",
        requester="pytest",
    )

    commands = result["commands"]
    assert len(commands) == 2
    assert result["directions"] == ["LONG", "SHORT"]
    for command in commands:
        payload = json.loads(Path(str(command["command_file"])).read_text())
        assert payload["action"] == "close_all"
        assert payload["reason"] == "risk_off"


@pytest.mark.asyncio
async def test_aget_runs_refreshes_cold_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class _FakeDB:
        def get_runs_with_metrics(self, execution_mode: str = "BACKTEST") -> list[dict]:
            calls.append(execution_mode)
            return [
                {
                    "run_id": "run-1",
                    "strategy": "CPR_LEVELS",
                    "direction_filter": "long",
                }
            ]

    monkeypatch.setattr(web_state, "get_dashboard_backtest_db", lambda: _FakeDB())
    monkeypatch.setattr(web_state, "_runs_cache", None, raising=False)
    monkeypatch.setattr(web_state, "_runs_cache_time", 0.0, raising=False)

    runs = await web_state.aget_runs(force=False, execution_mode="BACKTEST")

    assert calls == ["BACKTEST"]
    assert len(runs) == 1
    assert runs[0]["direction_filter"] == "LONG"


@pytest.mark.asyncio
async def test_aget_trades_falls_back_to_paper_execution_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str | None] = []

    class _FakeDB:
        def get_backtest_trades(
            self, run_id: str, symbols=None, execution_mode: str | None = "BACKTEST"
        ):
            calls.append(execution_mode)
            if execution_mode == "BACKTEST":
                return pl.DataFrame()
            return pl.DataFrame([{"run_id": run_id, "execution_mode": "PAPER", "symbol": "SBIN"}])

    monkeypatch.setattr(web_state, "get_dashboard_backtest_db", lambda: _FakeDB())

    trades = await web_state.aget_trades("paper-1")

    assert calls == ["BACKTEST", "PAPER"]
    assert trades.height == 1
    assert trades["execution_mode"][0] == "PAPER"


def test_invalidate_run_cache_can_drop_a_single_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        web_state,
        "_runs_cache",
        [
            {"run_id": "run-1", "strategy": "CPR_LEVELS"},
            {"run_id": "run-2", "strategy": "FBR"},
        ],
        raising=False,
    )
    monkeypatch.setattr(web_state, "_runs_cache_time", 42.0, raising=False)

    web_state.invalidate_run_cache("run-1")

    assert web_state._runs_cache == [{"run_id": "run-2", "strategy": "FBR"}]
    assert web_state._runs_cache_time == 0


def test_build_run_options_includes_rvol_state() -> None:
    options = web_state.build_run_options(
        [
            {
                "run_id": "run-1",
                "strategy": "CPR_LEVELS",
                "direction_filter": "LONG",
                "updated_at": "2026-04-12 20:50:00",
                "start_date": "2025-01-01",
                "end_date": "2026-03-27",
                "annual_return_pct": 45.0,
                "total_return_pct": 58.3,
                "total_pnl": 582592,
                "trade_count": 2060,
                "rvol_threshold": 1.0,
                "cpr_min_close_atr": 0.35,
                "skip_rvol_check": False,
            },
            {
                "run_id": "run-2",
                "strategy": "CPR_LEVELS",
                "direction_filter": "SHORT",
                "updated_at": "2026-04-11 18:15:00",
                "start_date": "2025-01-01",
                "end_date": "2026-03-27",
                "annual_return_pct": 41.5,
                "total_return_pct": 51.2,
                "total_pnl": 511200,
                "trade_count": 1800,
                "rvol_threshold": 1.0,
                "cpr_min_close_atr": 0.0,
                "skip_rvol_check": True,
            },
            {
                "run_id": "run-3",
                "strategy": "FBR",
                "direction_filter": "LONG",
                "updated_at": "2026-04-10 09:00:00",
                "start_date": "2025-01-01",
                "end_date": "2026-03-27",
                "annual_return_pct": 12.5,
                "total_return_pct": 18.2,
                "total_pnl": 182000,
                "trade_count": 900,
                "rvol_threshold": 1.1,
                "failure_window": 10,
                "skip_rvol_check": False,
            },
        ]
    )

    labels = list(options)
    assert labels[0].startswith(
        "run-1 | 2026-04-12 20:50 | cpr_levels-long-slotsize-daily-reset-rvol1-atr0.35 | 2025-01-01→2026-03-27 | "
        "TotRet 58.3% | P/L ₹582,592 | Trades 2,060"
    ), f"Got: {labels[0]}"
    assert labels[1].startswith(
        "run-2 | 2026-04-11 18:15 | cpr_levels-short-slotsize-daily-reset-rvoloff | 2025-01-01→2026-03-27 | "
        "TotRet 51.2% | P/L ₹511,200 | Trades 1,800"
    ), f"Got: {labels[1]}"


def test_build_run_options_includes_paper_mode_and_feed() -> None:
    options = web_state.build_run_options(
        [
            {
                "run_id": "paper-1",
                "strategy": "CPR_LEVELS",
                "direction_filter": "LONG",
                "updated_at": "2026-04-17 15:20:00",
                "start_date": "2026-04-17",
                "end_date": "2026-04-17",
                "annual_return_pct": 8.5,
                "total_return_pct": 10.2,
                "total_pnl": 1010.0,
                "trade_count": 22,
                "rvol_threshold": 1.0,
                "cpr_min_close_atr": 0.5,
                "skip_rvol_check": False,
                "execution_mode": "PAPER",
                "params_json": ('{"paper_session_mode":"LIVE","paper_feed_source":"local"}'),
            }
        ]
    )

    label = next(iter(options))
    assert "cpr_levels-long-slotsize-daily-reset-rvol1-atr0.5" in label
    assert label.startswith(
        "paper-1 | 2026-04-17 15:20 | cpr_levels-long-slotsize-daily-reset-rvol1-atr0.5 | "
        "2026-04-17→2026-04-17 | TotRet 10.2% | P/L ₹1,010 | Trades 22"
    ), f"Got: {label}"


@pytest.mark.asyncio
async def test_aget_trade_inspection_combines_backtest_and_market_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bt = BacktestDB(db_path=tmp_path / "backtest.duckdb")
    market = MarketDB(db_path=tmp_path / "market.duckdb")
    try:
        bt.con.execute(
            """
            CREATE TABLE backtest_results (
                run_id VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                direction VARCHAR,
                entry_time VARCHAR,
                exit_time VARCHAR,
                entry_price DOUBLE,
                exit_price DOUBLE,
                sl_price DOUBLE,
                target_price DOUBLE,
                profit_loss DOUBLE,
                profit_loss_pct DOUBLE,
                exit_reason VARCHAR,
                atr DOUBLE,
                position_size INTEGER,
                position_value DOUBLE
            )
            """
        )
        bt.con.execute(
            """
            CREATE TABLE run_metadata (
                run_id VARCHAR,
                params_json VARCHAR
            )
            """
        )
        bt.con.execute(
            """
            INSERT INTO backtest_results VALUES
            ('run123', 'TEST', DATE '2026-02-11', 'LONG', '09:20', '09:55',
             11.57, 12.0958333333, 11.4119891667, 12.0333333333,
             7294.36, 4.5448, 'TRAILING_SL', 0.1441666667, 17200, 198964.0)
            """
        )
        bt.con.execute(
            """
            INSERT INTO run_metadata VALUES
            ('run123', '{"buffer_pct":0.0005,"cpr_levels":{"cpr_min_close_atr":0.35}}')
            """
        )

        market.con.execute(
            """
            CREATE TABLE market_day_state (
                symbol VARCHAR,
                trade_date DATE,
                prev_date DATE,
                prev_close DOUBLE,
                "pivot" DOUBLE,
                bc DOUBLE,
                tc DOUBLE,
                cpr_width_pct DOUBLE,
                r1 DOUBLE,
                s1 DOUBLE,
                cpr_shift VARCHAR,
                is_narrowing INTEGER,
                cpr_threshold_pct DOUBLE,
                atr DOUBLE,
                open_915 DOUBLE,
                or_close_5 DOUBLE,
                gap_pct_open DOUBLE
            )
            """
        )
        market.con.execute(
            """
            CREATE TABLE cpr_daily (
                symbol VARCHAR,
                trade_date DATE,
                prev_high DOUBLE,
                prev_low DOUBLE,
                prev_close DOUBLE
            )
            """
        )
        market.con.execute(
            """
            CREATE TABLE strategy_day_state (
                symbol VARCHAR,
                trade_date DATE,
                open_side VARCHAR,
                open_to_cpr_atr DOUBLE,
                gap_abs_pct DOUBLE,
                or_atr_5 DOUBLE,
                direction_5 VARCHAR
            )
            """
        )
        market.con.execute(
            """
            CREATE TABLE candles_5min (
                symbol VARCHAR,
                date DATE,
                candle_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        market.con.execute("CREATE OR REPLACE VIEW v_5min AS SELECT * FROM candles_5min")
        market._has_5min = True
        market.con.execute(
            """
            INSERT INTO market_day_state VALUES
            ('TEST', DATE '2026-02-11', DATE '2026-02-10', 11.50,
             11.4566666667, 11.435, 11.4783333333, 0.3782368344,
             12.0333333333, 10.9233333333, 'HIGHER', 1, 0.6160345170,
             0.1441666667, 11.50, 11.57, 0.0)
            """
        )
        market.con.execute(
            """
            INSERT INTO cpr_daily VALUES
            ('TEST', DATE '2026-02-11', 11.99, 10.88, 11.50)
            """
        )
        market.con.execute(
            """
            INSERT INTO strategy_day_state VALUES
            ('TEST', DATE '2026-02-11', 'ABOVE', 0.1502890173, 0.0, 1.8728323699, 'LONG')
            """
        )
        market.con.execute(
            """
            INSERT INTO candles_5min VALUES
            ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:15:00', 11.50, 11.73, 11.46, 11.57, 1000),
            ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:20:00', 11.57, 11.57, 11.45, 11.56, 1200),
            ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:55:00', 11.84, 12.34, 11.84, 12.24, 2000)
            """
        )

        monkeypatch.setattr(web_state, "get_dashboard_backtest_db", lambda: bt)
        monkeypatch.setattr(web_state, "get_dashboard_db", lambda: market)

        details = await web_state.aget_trade_inspection(
            "run123", "TEST", "2026-02-11", "09:20", "09:55"
        )

        assert details["trade"]["direction"] == "LONG"
        assert details["daily_cpr"]["prev_date"] == "2026-02-10"
        assert details["daily_cpr"]["tc"] == pytest.approx(11.4783333333)
        assert details["daily_cpr"]["r1"] == pytest.approx(12.0333333333)
        assert details["derived"]["trigger_price"] == pytest.approx(11.4840725)
        assert details["derived"]["min_signal_close"] == pytest.approx(11.528791666645)
        assert details["candles"]["09:15"]["close"] == pytest.approx(11.57)
        assert details["candles"]["09:20"]["open"] == pytest.approx(11.57)
        assert details["candles"]["09:55"]["high"] == pytest.approx(12.34)
    finally:
        bt.close()
        market.close()
