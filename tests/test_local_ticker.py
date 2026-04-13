"""Tests for LocalTickerAdapter — DuckDB-backed mock of KiteTickerAdapter."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

import polars as pl
import pytest

from engine.cpr_atr_strategy import DayPack
from engine.live_market_data import IST, FiveMinuteCandleBuilder


def _make_pack(n_bars: int = 3, base_price: float = 100.0, base_vol: float = 1000.0) -> DayPack:
    """Build a synthetic DayPack with n_bars of 5-min candles starting at 09:15."""
    times = [f"{9 + (15 + i * 5) // 60:02d}:{(15 + i * 5) % 60:02d}" for i in range(n_bars)]
    return DayPack(
        time_str=times,
        opens=[base_price + i for i in range(n_bars)],
        highs=[base_price + i + 2 for i in range(n_bars)],
        lows=[base_price + i - 1 for i in range(n_bars)],
        closes=[base_price + i + 1 for i in range(n_bars)],
        volumes=[base_vol] * n_bars,
    )


def _mock_load(symbols_and_packs: dict[str, DayPack]):
    """Create a mock for _load_day_packs that returns the given symbol->DayPack mapping."""

    def _load(trade_date, symbols):
        return {s: symbols_and_packs[s] for s in symbols if s in symbols_and_packs}

    return _load


def test_load_day_packs_uses_dashboard_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.local_ticker_adapter as lta

    called = {"dashboard": 0}

    class _FakeRelation:
        def pl(self):
            return pl.DataFrame(
                {
                    "symbol": ["SBIN"],
                    "trade_date": ["2024-01-15"],
                    "pack_time_arr": [[555, 560]],
                    "open_arr": [[100.0, 101.0]],
                    "high_arr": [[102.0, 103.0]],
                    "low_arr": [[99.0, 100.0]],
                    "close_arr": [[101.0, 102.0]],
                    "volume_arr": [[1000.0, 1100.0]],
                    "rvol_baseline_arr": [[1.5, 1.6]],
                }
            )

    class _FakeCon:
        def execute(self, query: str, params: dict[str, object]):
            del query, params
            return _FakeRelation()

    class _FakeDB:
        con = _FakeCon()

        def _table_has_column(self, table: str, column: str) -> bool:
            del table
            return column in {"minute_arr", "rvol_baseline_arr"}

    def fake_get_dashboard_db():
        called["dashboard"] += 1
        return _FakeDB()

    monkeypatch.setattr(lta, "get_dashboard_db", fake_get_dashboard_db)

    packs = lta._load_day_packs("2024-01-15", ["SBIN"])

    assert called["dashboard"] == 1
    assert list(packs.keys()) == ["SBIN"]
    assert packs["SBIN"].time_str == ["09:15", "09:20"]


@pytest.fixture
def patch_load():
    """Fixture to patch _load_day_packs in local_ticker_adapter module."""

    def _patch(packs: dict[str, DayPack]):
        return patch.object(
            __import__("engine.local_ticker_adapter", fromlist=["_load_day_packs"]),
            "_load_day_packs",
            _mock_load(packs),
        )

    return _patch


class TestLocalTickerAdapterBasic:
    def test_has_local_feed_marker(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(3)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])
        assert adapter._local_feed is True
        assert adapter.is_connected is True
        assert adapter.reconnect_count == 0

    def test_fail_fast_on_no_data(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        with patch_load({}):
            with pytest.raises(RuntimeError, match="No intraday_day_pack data"):
                LocalTickerAdapter(trade_date="2024-01-15", symbols=["MISSING"])

    def test_drain_closed_returns_one_bar_per_call(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(3)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        builder = FiveMinuteCandleBuilder()
        adapter.register_session("sess1", ["SBIN"], builder)

        # First drain: bar 0
        closed = adapter.drain_closed("sess1")
        assert len(closed) == 1
        assert closed[0].symbol == "SBIN"
        assert closed[0].open == 100.0
        assert closed[0].close == 101.0

        # Second drain: bar 1
        closed = adapter.drain_closed("sess1")
        assert len(closed) == 1
        assert closed[0].open == 101.0

        # Third drain: bar 2
        closed = adapter.drain_closed("sess1")
        assert len(closed) == 1

        # Fourth drain: exhausted
        closed = adapter.drain_closed("sess1")
        assert closed == []

    def test_bar_timing_matches_replay_semantics(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(1)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("s", ["SBIN"], FiveMinuteCandleBuilder())
        closed = adapter.drain_closed("s")

        assert len(closed) == 1
        # time_str "09:15" is the candle close time (bar_end)
        expected_bar_end = datetime(2024, 1, 15, 9, 15, tzinfo=IST)
        expected_bar_start = expected_bar_end - timedelta(minutes=5)
        assert closed[0].bar_end == expected_bar_end
        assert closed[0].bar_start == expected_bar_start

    def test_ohlcv_parity_with_source_data(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        pack = _make_pack(2, base_price=200.0, base_vol=5000.0)
        packs = {"TCS": pack}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["TCS"])

        adapter.register_session("s", ["TCS"], FiveMinuteCandleBuilder())
        closed = adapter.drain_closed("s")

        assert len(closed) == 1
        assert closed[0].open == pack.opens[0]
        assert closed[0].high == pack.highs[0]
        assert closed[0].low == pack.lows[0]
        assert closed[0].close == pack.closes[0]
        assert closed[0].volume == pack.volumes[0]

    def test_tick_count_and_last_tick_ts(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(2), "TCS": _make_pack(2)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN", "TCS"])

        adapter.register_session("s", ["SBIN", "TCS"], FiveMinuteCandleBuilder())

        assert adapter.tick_count == 0
        assert adapter.last_tick_ts is None

        adapter.drain_closed("s")
        assert adapter.tick_count == 2  # SBIN + TCS
        assert adapter.last_tick_ts is not None

    def test_get_last_ltp(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(1, base_price=500.0)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("s", ["SBIN"], FiveMinuteCandleBuilder())
        adapter.drain_closed("s")

        assert adapter.get_last_ltp("SBIN") == 501.0  # base_price + 1
        assert adapter.get_last_ltp("MISSING") is None


class TestLocalTickerAdapterMultiSession:
    def test_fan_out_to_multiple_sessions(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(3), "TCS": _make_pack(3)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN", "TCS"])

        builder_a = FiveMinuteCandleBuilder()
        builder_b = FiveMinuteCandleBuilder()
        adapter.register_session("A", ["SBIN"], builder_a)
        adapter.register_session("B", ["SBIN", "TCS"], builder_b)

        # Session A drains: advances cursor to bar 0, fans out to both sessions
        closed_a = adapter.drain_closed("A")
        assert len(closed_a) == 1
        assert closed_a[0].symbol == "SBIN"

        # Session B drains: returns its pending bar 0 data (queued during A's drain)
        closed_b = adapter.drain_closed("B")
        assert len(closed_b) == 2  # 2 symbols at bar 0
        assert sorted(c.symbol for c in closed_b) == ["SBIN", "TCS"]

    def test_multi_session_exhaustion(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(1)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("A", ["SBIN"], FiveMinuteCandleBuilder())
        adapter.register_session("B", ["SBIN"], FiveMinuteCandleBuilder())

        # One drain advances the cursor, fans out to both
        closed_a = adapter.drain_closed("A")
        assert len(closed_a) == 1

        closed_b = adapter.drain_closed("B")
        assert len(closed_b) == 1

        # Both are exhausted now
        assert adapter.drain_closed("A") == []
        assert adapter.drain_closed("B") == []

    def test_update_symbols(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(2), "TCS": _make_pack(2)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN", "TCS"])

        adapter.register_session("s", ["SBIN", "TCS"], FiveMinuteCandleBuilder())
        adapter.update_symbols("s", ["SBIN"])

        closed = adapter.drain_closed("s")
        assert len(closed) == 1  # Only SBIN now
        assert closed[0].symbol == "SBIN"

    def test_unregister_session(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(2)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("s", ["SBIN"], FiveMinuteCandleBuilder())
        adapter.unregister_session("s")

        closed = adapter.drain_closed("s")
        assert closed == []  # Session removed, no data


class TestLocalTickerAdapterExhaustion:
    def test_exhaustion_after_last_bar(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(2)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("s", ["SBIN"], FiveMinuteCandleBuilder())

        # Drain all bars
        adapter.drain_closed("s")
        adapter.drain_closed("s")

        # Exhausted
        assert adapter.drain_closed("s") == []
        assert adapter.drain_closed("s") == []  # Stays exhausted
        assert adapter.last_tick_ts is not None  # Still has last tick info

    def test_close_is_noop(self, patch_load):
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(1)}
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.close()  # Should not raise


class TestLocalTickerAdapterPartialData:
    def test_missing_symbol_skipped(self, patch_load):
        """If a symbol has no data for the requested date, it's simply skipped."""
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {"SBIN": _make_pack(2)}
        # Request SBIN + MISSING, but only SBIN has data
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN"])

        adapter.register_session("s", ["SBIN", "MISSING"], FiveMinuteCandleBuilder())
        closed = adapter.drain_closed("s")
        assert len(closed) == 1
        assert closed[0].symbol == "SBIN"

    def test_symbols_with_different_bar_counts(self, patch_load):
        """Symbols may have different numbers of bars. Adapter uses union of all bar times."""
        from engine.local_ticker_adapter import LocalTickerAdapter

        packs = {
            "SBIN": _make_pack(3),  # 3 bars
            "TCS": _make_pack(2),   # 2 bars (missing last bar)
        }
        with patch_load(packs):
            adapter = LocalTickerAdapter(trade_date="2024-01-15", symbols=["SBIN", "TCS"])

        adapter.register_session("s", ["SBIN", "TCS"], FiveMinuteCandleBuilder())

        # Bar 0: both symbols
        closed = adapter.drain_closed("s")
        assert len(closed) == 2

        # Bar 1: both symbols
        closed = adapter.drain_closed("s")
        assert len(closed) == 2

        # Bar 2: only SBIN (TCS has no data for this time)
        closed = adapter.drain_closed("s")
        assert len(closed) == 1
        assert closed[0].symbol == "SBIN"

        # Exhausted
        assert adapter.drain_closed("s") == []
