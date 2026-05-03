from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

import scripts.paper_prepare as paper_prepare
from scripts.paper_prepare import pre_filter_symbols_for_strategy


def test_resolve_trade_date_accepts_iso_and_keywords(monkeypatch):
    class _FakeDateTime:
        @staticmethod
        def now(tz=None):
            return datetime(2024, 3, 10, 9, 30, tzinfo=tz)

        @staticmethod
        def fromisoformat(value: str):
            return datetime.fromisoformat(value)

    monkeypatch.setattr(paper_prepare, "datetime", _FakeDateTime)

    assert paper_prepare.resolve_trade_date("2024-03-11") == "2024-03-11"
    assert paper_prepare.resolve_trade_date("today") == "2024-03-10"
    assert paper_prepare.resolve_trade_date(None) == "2024-03-10"


def test_resolve_prepare_symbols_uses_explicit_values_before_defaults(monkeypatch):
    monkeypatch.setattr(
        paper_prepare,
        "get_settings",
        lambda: SimpleNamespace(paper_default_symbols="HDFCBANK, INFY"),
    )

    assert paper_prepare.resolve_prepare_symbols(["sbin", "TCS"], None) == ["SBIN", "TCS"]
    assert paper_prepare.resolve_prepare_symbols(None, "reliance, icicibank") == [
        "ICICIBANK",
        "RELIANCE",
    ]
    assert paper_prepare.resolve_prepare_symbols(None, None) == ["HDFCBANK", "INFY"]


def test_resolve_prepare_symbols_supports_all_local_symbols(monkeypatch):
    monkeypatch.setattr(
        paper_prepare,
        "load_universe_symbols",
        lambda universe_name, read_only=True: [],
    )
    monkeypatch.setattr(paper_prepare, "_largest_saved_full_universe", lambda read_only=True: [])
    monkeypatch.setattr(
        paper_prepare,
        "_resolve_all_local_symbols",
        lambda read_only=True: ["SBIN", "RELIANCE"],
    )

    assert paper_prepare.resolve_prepare_symbols(None, None, all_symbols=True) == [
        "RELIANCE",
        "SBIN",
    ]


def test_resolve_prepare_symbols_prefers_canonical_universe(monkeypatch):
    monkeypatch.setattr(
        paper_prepare,
        "load_universe_symbols",
        lambda universe_name, read_only=True: (
            ["SBIN", "TCS"] if universe_name == paper_prepare.CANONICAL_FULL_UNIVERSE_NAME else []
        ),
    )
    monkeypatch.setattr(
        paper_prepare,
        "_resolve_all_local_symbols",
        lambda read_only=True: (_ for _ in ()).throw(AssertionError("must not shrink")),
    )

    assert paper_prepare.resolve_prepare_symbols(None, None, all_symbols=True) == [
        "SBIN",
        "TCS",
    ]


def test_resolve_prepare_symbols_supports_saved_universe(monkeypatch):
    monkeypatch.setattr(
        paper_prepare,
        "load_universe_symbols",
        lambda universe_name, read_only=True: (
            ["SBIN", "TCS"] if universe_name == "full_2026_04_24" else []
        ),
    )

    assert paper_prepare.resolve_prepare_symbols(
        None,
        None,
        universe_name="full_2026_04_24",
    ) == [
        "SBIN",
        "TCS",
    ]


def test_resolve_all_local_symbols_prefers_v5min_universe(monkeypatch):
    class _FakeConn:
        def execute(self, query: str):
            assert "FROM v_5min" in query

            class _Result:
                @staticmethod
                def fetchall():
                    return [("SBIN",), ("RELIANCE",), ("SBIN",)]

            return _Result()

    class _FakeDB:
        con = _FakeConn()

        @staticmethod
        def get_available_symbols(*, force_refresh: bool = False):
            raise AssertionError("fallback should not be used when v_5min is available")

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    assert paper_prepare._resolve_all_local_symbols() == ["RELIANCE", "SBIN"]


def test_ensure_canonical_universe_bootstraps_from_union(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeDB:
        def __init__(self):
            self.saved = {
                "full_2026_04_28": ["SBIN", "TCS"],
                "full_2026_04_29": ["SBIN"],
            }

        def get_universe_symbols(self, name):
            return [] if name == paper_prepare.CANONICAL_FULL_UNIVERSE_NAME else self.saved[name]

        def list_universes(self):
            return [
                {"name": "full_2026_04_28", "symbol_count": 2, "end_date": "2026-04-28"},
                {"name": "full_2026_04_29", "symbol_count": 1, "end_date": "2026-04-29"},
            ]

        def upsert_universe(self, name, symbols, **kwargs):
            calls["name"] = name
            calls["symbols"] = list(symbols)
            calls["kwargs"] = kwargs
            return len(symbols)

    fake_db = _FakeDB()
    monkeypatch.setattr(paper_prepare, "get_db", lambda: fake_db)
    monkeypatch.setattr(
        paper_prepare,
        "_resolve_all_local_symbols",
        lambda read_only=True: ["RELIANCE", "SBIN"],
    )

    symbols, created = paper_prepare.ensure_canonical_universe(trade_date="2026-04-30")

    assert symbols == ["RELIANCE", "SBIN", "TCS"]
    assert created is True
    assert calls["name"] == paper_prepare.CANONICAL_FULL_UNIVERSE_NAME
    assert calls["symbols"] == ["RELIANCE", "SBIN", "TCS"]


def test_validate_daily_runtime_coverage_reports_missing_rows(monkeypatch):
    coverage = {
        "market_day_state": [],
        "strategy_day_state": ["SBIN"],
        "intraday_day_pack": ["SBIN", "TCS"],
    }
    seen: dict[str, object] = {}

    class _FakeDB:
        def get_runtime_trade_date_coverage(self, symbols: list[str], trade_date: str):
            seen["symbols"] = list(symbols)
            seen["trade_date"] = trade_date
            return coverage

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.validate_daily_runtime_coverage(
        trade_date="2024-03-11",
        symbols=["TCS", "SBIN"],
    )

    assert seen == {"symbols": ["TCS", "SBIN"], "trade_date": "2024-03-11"}
    assert payload["coverage_ready"] is False
    assert payload["missing_counts"] == {
        "market_day_state": 0,
        "strategy_day_state": 1,
        "intraday_day_pack": 2,
    }
    assert payload["missing_total"] == 3
    assert payload["coverage"] == coverage


def test_prepare_runtime_for_daily_paper_validates_only_no_builds(monkeypatch):
    """prepare_runtime_for_daily_paper is read-only: it validates coverage but never builds tables."""
    coverage = {
        "market_day_state": [],
        "strategy_day_state": [],
        "intraday_day_pack": [],
    }
    build_calls: list[str] = []

    class _FakeDB:
        def build_market_day_state(self, **kwargs):
            build_calls.append("market")

        def build_strategy_day_state(self, **kwargs):
            build_calls.append("strategy")

        def build_intraday_day_pack(self, **kwargs):
            build_calls.append("pack")

        def get_runtime_trade_date_coverage(self, symbols: list[str], trade_date: str):
            return coverage

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.prepare_runtime_for_daily_paper(
        trade_date="2024-03-11",
        symbols=["SBIN", "TCS"],
        mode="replay",
    )

    assert payload["coverage_ready"] is True
    assert payload["requested_symbols"] == ["SBIN", "TCS"]
    assert payload["mode"] == "replay"
    # No build calls must have been made — this function is validate-only.
    assert build_calls == []


def test_snapshot_candidate_universe_uses_market_db(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeDB:
        @staticmethod
        def upsert_universe(name, symbols, **kwargs):
            calls["name"] = name
            calls["symbols"] = list(symbols)
            calls["kwargs"] = kwargs
            return len(symbols)

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    saved = paper_prepare.snapshot_candidate_universe(
        "full_2026_04_24",
        ["sbin", "reliance"],
        trade_date="2026-04-24",
        source="paper-daily-prepare",
        notes="snapshot from test",
    )

    assert saved == 2
    assert calls == {
        "name": "full_2026_04_24",
        "symbols": ["sbin", "reliance"],
        "kwargs": {
            "start_date": "2026-04-24",
            "end_date": "2026-04-24",
            "source": "paper-daily-prepare",
            "notes": "snapshot from test",
        },
    }


def test_validate_live_runtime_coverage_uses_prior_market_history(monkeypatch):
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if (
                "FROM intraday_day_pack" in query
                or "FROM market_day_state" in query
                or "FROM strategy_day_state" in query
            ):
                return SimpleNamespace(fetchone=lambda: (0,))
            if "FROM v_daily" in query:
                rows = [("SBIN", "2024-03-08"), ("TCS", "2024-03-08")]
            elif "FROM v_5min" in query:
                rows = [("SBIN", "2024-03-08"), ("TCS", None)]
            elif "FROM atr_intraday" in query:
                rows = [("SBIN", "2024-03-08"), ("TCS", "2024-03-08")]
            elif "FROM cpr_thresholds" in query:
                rows = [("SBIN", "2024-03-08"), ("TCS", "2024-03-08")]
            else:
                rows = [("SBIN",), ("TCS",)]
            return SimpleNamespace(fetchall=lambda: rows)

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.validate_live_runtime_coverage(
        trade_date="2024-03-11",
        symbols=["SBIN", "TCS"],
    )

    assert payload["coverage_ready"] is False
    assert payload["coverage"]["SBIN"] == {
        "prev_daily_date": "2024-03-08",
        "prev_5min_date": "2024-03-08",
        "prev_atr_date": "2024-03-08",
        "prev_cpr_threshold_date": "2024-03-08",
    }
    assert payload["missing_by_symbol"]["TCS"] == ["v_5min"]


def test_validate_live_runtime_coverage_allows_state_rows_without_pack(monkeypatch):
    """Having market_day_state rows with no intraday_day_pack is valid after a correct EOD run.

    The old 'unexpected_trade_date_state_rows' check was removed because the EOD pipeline now
    intentionally builds next-day market_day_state rows while leaving pack empty (tomorrow's
    candles don't exist yet). coverage_ready should be True in this scenario.
    """

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if "FROM intraday_day_pack" in query:
                return SimpleNamespace(fetchone=lambda: (0,))
            if "FROM market_day_state" in query:
                return SimpleNamespace(fetchone=lambda: (2,))
            if "FROM strategy_day_state" in query:
                return SimpleNamespace(fetchone=lambda: (2,))
            if (
                "FROM v_daily" in query
                or "FROM v_5min" in query
                or "FROM atr_intraday" in query
                or "FROM cpr_thresholds" in query
            ):
                return SimpleNamespace(fetchall=lambda: [("SBIN", "2024-03-08")])
            return SimpleNamespace(fetchall=lambda: [])

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.validate_live_runtime_coverage(
        trade_date="2024-03-11",
        symbols=["SBIN"],
    )

    # State rows without pack = expected post-EOD state. Must NOT block.
    assert payload["coverage_ready"] is True
    assert "unexpected_trade_date_state_rows" not in payload
    assert payload["exact_trade_date_counts"] == {
        "intraday_day_pack": 0,
        "market_day_state": 2,
        "strategy_day_state": 2,
    }


def test_validate_live_runtime_coverage_treats_date_mismatch_as_warning(monkeypatch):
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if (
                "FROM intraday_day_pack" in query
                or "FROM market_day_state" in query
                or "FROM strategy_day_state" in query
            ):
                return SimpleNamespace(fetchone=lambda: (0,))
            if "FROM v_daily" in query:
                rows = [("SBIN", "2024-03-08")]
            elif "FROM v_5min" in query:
                rows = [("SBIN", "2024-03-07")]
            elif "FROM atr_intraday" in query:
                rows = [("SBIN", "2024-03-08")]
            elif "FROM cpr_thresholds" in query:
                rows = [("SBIN", "2024-03-08")]
            else:
                rows = [("SBIN",)]
            return SimpleNamespace(fetchall=lambda: rows)

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.validate_live_runtime_coverage(
        trade_date="2024-03-11",
        symbols=["SBIN"],
    )

    assert payload["coverage_ready"] is True
    assert payload["missing_by_symbol"] == {}
    assert payload["warning_by_symbol"] == {"SBIN": ["v_5min_date_mismatch"]}
    assert payload["warning_total"] == 1


def test_validate_live_runtime_coverage_allows_sparse_symbol_day_gaps(monkeypatch):
    symbols = [f"SYM{i:03d}" for i in range(100)]
    present_symbols = symbols[1:]

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if (
                "FROM intraday_day_pack" in query
                or "FROM market_day_state" in query
                or "FROM strategy_day_state" in query
            ):
                return SimpleNamespace(fetchone=lambda: (0,))
            if (
                "FROM v_daily" in query
                or "FROM v_5min" in query
                or "FROM atr_intraday" in query
                or "FROM cpr_thresholds" in query
            ):
                if "SELECT symbol, MAX" in query:
                    rows = [(symbol, "2024-03-08") for symbol in present_symbols]
                else:
                    rows = [(symbol,) for symbol in present_symbols]
            else:
                rows = []
            return SimpleNamespace(fetchall=lambda: rows)

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    payload = paper_prepare.validate_live_runtime_coverage(
        trade_date="2024-03-11",
        symbols=symbols,
    )

    assert payload["coverage_ready"] is True
    assert payload["missing_by_symbol"] == {}
    assert payload["sparse_missing_by_symbol"]["SYM000"] == [
        "v_daily",
        "v_5min",
        "atr_intraday",
        "cpr_thresholds",
    ]
    assert payload["sparse_missing_total"] == 4


def test_prepare_runtime_for_daily_paper_live_refreshes_runtime_tables(monkeypatch):
    monkeypatch.setattr(
        paper_prepare,
        "validate_live_runtime_coverage",
        lambda **kwargs: {
            "trade_date": kwargs["trade_date"],
            "requested_symbols": kwargs["symbols"],
            "coverage_ready": True,
            "coverage": {},
            "missing_by_symbol": {},
            "missing_counts": {},
            "missing_total": 0,
        },
    )

    payload = paper_prepare.prepare_runtime_for_daily_paper(
        trade_date="2024-03-11",
        symbols=["SBIN", "TCS"],
        mode="live",
    )

    assert payload["coverage_ready"] is True
    assert payload["mode"] == "live"
    assert payload["runtime_refresh"] is None


def test_pre_filter_symbols_for_strategy_can_return_empty(monkeypatch):
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if "MAX(trade_date)::VARCHAR FROM cpr_daily" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-03-11",))
            return SimpleNamespace(fetchall=lambda: [])

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    filtered = pre_filter_symbols_for_strategy(
        trade_date="2024-03-11",
        symbols=["SBIN", "TCS"],
        strategy="CPR_LEVELS",
        strategy_params={"min_price": 50.0, "cpr_min_close_atr": 0.5, "narrowing_filter": True},
    )

    assert filtered == []


def test_pre_filter_symbols_fails_when_exact_cpr_date_missing_for_live(monkeypatch):
    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            if "MAX(trade_date)::VARCHAR FROM cpr_daily" in query:
                return SimpleNamespace(fetchone=lambda: ("2024-03-08",))
            return SimpleNamespace(fetchall=lambda: [])

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(paper_prepare, "get_db", lambda: _FakeDB())

    with pytest.raises(RuntimeError, match="latest cpr_daily row is 2024-03-08"):
        pre_filter_symbols_for_strategy(
            trade_date="2024-03-11",
            symbols=["SBIN"],
            strategy="CPR_LEVELS",
            strategy_params={},
            require_trade_date_rows=True,
        )
