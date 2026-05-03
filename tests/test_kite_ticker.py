from __future__ import annotations

import sys
from datetime import datetime
from types import ModuleType, SimpleNamespace
from typing import ClassVar

from engine.kite_ticker_adapter import KiteTickerAdapter
from engine.live_market_data import IST, FiveMinuteCandleBuilder


class _FakeKiteTicker:
    instances: ClassVar[list[object]] = []

    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None
        self.on_reconnect = None
        self.subscribe_calls: list[list[int]] = []
        self.unsubscribe_calls: list[list[int]] = []
        self.mode_calls: list[tuple[str, list[int]]] = []
        self.closed = False
        self.MODE_QUOTE = "quote"
        self.__class__.instances.append(self)

    def connect(self, threaded: bool = True) -> None:
        _ = threaded
        if self.on_connect is not None:
            self.on_connect(self, {"ok": True})

    def subscribe(self, tokens: list[int]) -> None:
        self.subscribe_calls.append(list(tokens))

    def unsubscribe(self, tokens: list[int]) -> None:
        self.unsubscribe_calls.append(list(tokens))

    def set_mode(self, mode: str, tokens: list[int]) -> None:
        self.mode_calls.append((mode, list(tokens)))

    def close(self) -> None:
        self.closed = True


class _FakeKiteConnect:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.access_token = None

    def set_access_token(self, access_token: str) -> None:
        self.access_token = access_token

    def instruments(self, exchange: str):
        assert exchange == "NSE"
        return [
            {"exchange": "NSE", "tradingsymbol": "SBIN", "instrument_token": 1},
            {"exchange": "NSE", "tradingsymbol": "TCS", "instrument_token": 2},
        ]


def _install_fake_kite(monkeypatch):
    _FakeKiteTicker.instances.clear()
    kite_module = ModuleType("kiteconnect")
    kite_module.KiteConnect = _FakeKiteConnect
    kite_module.KiteTicker = _FakeKiteTicker
    monkeypatch.setitem(sys.modules, "kiteconnect", kite_module)
    monkeypatch.setattr(
        "engine.kite_ticker_adapter.get_settings",
        lambda: SimpleNamespace(kite_api_key="key", kite_access_token="token"),
    )


def test_kite_ticker_adapter_fans_out_ticks(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder_a = FiveMinuteCandleBuilder(interval_minutes=5)
    builder_b = FiveMinuteCandleBuilder(interval_minutes=5)

    adapter.register_session("A", ["SBIN"], builder_a)
    adapter.register_session("B", ["SBIN", "TCS"], builder_b)

    adapter._on_ticks(
        None,
        [
            {
                "instrument_token": 1,
                "last_price": 100.0,
                "volume_traded": 1000,
                "exchange_timestamp": datetime(2024, 1, 1, 9, 15, tzinfo=IST),
            }
        ],
    )
    adapter.synthesize_quiet_symbols("A", ["SBIN"], datetime(2024, 1, 1, 9, 20, tzinfo=IST))
    closed = adapter.drain_closed("A")

    assert adapter.is_connected is True
    assert adapter.tick_count >= 1
    assert adapter.get_last_ltp("SBIN") == 100.0
    assert len(closed) == 1
    assert closed[0].symbol == "SBIN"
    adapter.close()


def test_kite_ticker_adapter_uses_timestamp_fallback(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)

    adapter.register_session("A", ["SBIN"], builder)

    adapter._on_ticks(
        None,
        [
            {
                "instrument_token": 1,
                "last_price": 100.0,
                "volume_traded": 1000,
                # Some SDK payloads provide `timestamp` even when
                # `exchange_timestamp` is absent. We should still anchor the
                # candle to the exchange-side timestamp instead of wall clock.
                "timestamp": datetime(2024, 1, 1, 9, 15, tzinfo=IST),
            }
        ],
    )
    adapter.synthesize_quiet_symbols("A", ["SBIN"], datetime(2024, 1, 1, 9, 20, tzinfo=IST))
    closed = adapter.drain_closed("A")

    assert len(closed) == 1
    assert closed[0].symbol == "SBIN"
    assert closed[0].bar_start == datetime(2024, 1, 1, 9, 15, tzinfo=IST)
    assert closed[0].bar_end == datetime(2024, 1, 1, 9, 20, tzinfo=IST)
    adapter.close()


def test_kite_ticker_adapter_replays_scripted_tick_stream(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)

    adapter.register_session("A", ["SBIN"], builder)

    scripted_ticks = [
        {
            "instrument_token": 1,
            "last_price": 100.0,
            "volume_traded": 1000,
            "exchange_timestamp": datetime(2024, 1, 1, 9, 15, tzinfo=IST),
        },
        {
            "instrument_token": 1,
            "last_price": 101.0,
            "volume_traded": 1005,
            "exchange_timestamp": datetime(2024, 1, 1, 9, 17, tzinfo=IST),
        },
        {
            "instrument_token": 1,
            "last_price": 102.0,
            "volume_traded": 1010,
            "exchange_timestamp": datetime(2024, 1, 1, 9, 20, tzinfo=IST),
        },
        {
            "instrument_token": 1,
            "last_price": 103.0,
            "volume_traded": 1015,
            "exchange_timestamp": datetime(2024, 1, 1, 9, 22, tzinfo=IST),
        },
        {
            "instrument_token": 1,
            "last_price": 104.0,
            "volume_traded": 1020,
            "exchange_timestamp": datetime(2024, 1, 1, 9, 25, tzinfo=IST),
        },
    ]

    for tick in scripted_ticks:
        adapter._on_ticks(None, [tick])

    closed = adapter.drain_closed("A")

    assert [c.bar_start for c in closed] == [
        datetime(2024, 1, 1, 9, 15, tzinfo=IST),
        datetime(2024, 1, 1, 9, 20, tzinfo=IST),
    ]
    assert [c.bar_end for c in closed] == [
        datetime(2024, 1, 1, 9, 20, tzinfo=IST),
        datetime(2024, 1, 1, 9, 25, tzinfo=IST),
    ]
    assert [c.close for c in closed] == [101.0, 103.0]
    assert adapter.tick_count >= 5
    adapter.close()


def test_kite_ticker_adapter_updates_subscriptions(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)
    adapter.register_session("A", ["SBIN", "TCS"], builder)
    ticker = adapter._ticker
    assert ticker is not None
    assert ticker.subscribe_calls

    adapter.update_symbols("A", ["SBIN"])

    unsubscribed = [tok for call in ticker.unsubscribe_calls for tok in call]
    assert 2 in unsubscribed
    adapter.unregister_session("A")
    assert ticker.closed is True


def test_kite_ticker_adapter_does_not_mark_failed_subscribe_as_active(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)
    adapter.register_session("A", ["SBIN"], builder)
    ticker = adapter._ticker
    assert ticker is not None

    original_subscribe = ticker.subscribe
    subscribe_attempts: list[list[int]] = []

    def fail_subscribe(tokens: list[int]) -> None:
        subscribe_attempts.append(list(tokens))
        if tokens == [2]:
            raise RuntimeError("subscribe failed")
        original_subscribe(tokens)

    ticker.subscribe = fail_subscribe
    adapter.update_symbols("A", ["SBIN", "TCS"])

    assert 2 not in adapter._subscribed_tokens
    adapter._reconcile_subscriptions()
    assert subscribe_attempts[-2:] == [[2], [2]]
    adapter.close()


def test_kite_ticker_adapter_batches_snapshots_per_builder(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()

    class _BatchRecorderBuilder:
        def __init__(self) -> None:
            self.batches: list[list[str]] = []
            self.ingest_calls = 0

        def ingest_many(self, snapshots) -> list[object]:
            self.batches.append([snapshot.symbol for snapshot in snapshots])
            return []

        def ingest(self, snapshot) -> list[object]:
            _ = snapshot
            self.ingest_calls += 1
            return []

    builder_a = _BatchRecorderBuilder()
    builder_b = _BatchRecorderBuilder()

    adapter.register_session("A", ["SBIN"], builder_a)  # type: ignore[arg-type]
    adapter.register_session("B", ["SBIN", "TCS"], builder_b)  # type: ignore[arg-type]
    adapter._on_ticks(
        None,
        [
            {
                "instrument_token": 1,
                "last_price": 100.0,
                "volume_traded": 1000,
                "exchange_timestamp": datetime(2024, 1, 1, 9, 15, tzinfo=IST),
            },
            {
                "instrument_token": 2,
                "last_price": 3500.0,
                "volume_traded": 800,
                "exchange_timestamp": datetime(2024, 1, 1, 9, 15, tzinfo=IST),
            },
        ],
    )

    assert builder_a.batches == [["SBIN"]]
    assert builder_b.batches == [["SBIN", "TCS"]]
    assert builder_a.ingest_calls == 0
    assert builder_b.ingest_calls == 0


def test_kite_ticker_adapter_recover_connection_recreates_client(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)
    adapter.register_session("A", ["SBIN"], builder)

    first_ticker = adapter._ticker
    assert first_ticker is not None
    adapter._on_close(None, 100, "network drop")
    with adapter._lock:
        adapter._last_close_ts = datetime(2024, 1, 1, 9, 0, tzinfo=IST)

    result = adapter.recover_connection(
        now=datetime(2024, 1, 1, 9, 1, tzinfo=IST),
        reconnect_after_sec=30.0,
        cooldown_sec=0.0,
    )

    assert result["action"] == "recovered"
    assert adapter.is_connected is True
    assert adapter._ticker is not None
    assert adapter._ticker is not first_ticker
    assert first_ticker.closed is True
    assert len(_FakeKiteTicker.instances) >= 2


def test_kite_ticker_adapter_recover_connection_reports_connect_failure(monkeypatch) -> None:
    _install_fake_kite(monkeypatch)
    adapter = KiteTickerAdapter()
    builder = FiveMinuteCandleBuilder(interval_minutes=5)
    adapter.register_session("A", ["SBIN"], builder)
    adapter._on_close(None, 100, "network drop")
    with adapter._lock:
        adapter._last_close_ts = datetime(2024, 1, 1, 9, 0, tzinfo=IST)

    def fake_connect(symbols: list[str]) -> None:
        assert symbols == ["SBIN"]
        raise RuntimeError("connect boom")

    monkeypatch.setattr(adapter, "connect", fake_connect)

    result = adapter.recover_connection(
        now=datetime(2024, 1, 1, 9, 1, tzinfo=IST),
        reconnect_after_sec=30.0,
        cooldown_sec=0.0,
    )

    assert result["action"] == "failed"
    assert result["reason"] == "connect_failed"
    assert "connect boom" in str(result["error"])
