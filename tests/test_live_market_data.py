"""Tests for the live market-data adapter and candle builder."""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from types import ModuleType, SimpleNamespace

import pytest

from db.postgres import FeedState
from engine.live_market_data import FiveMinuteCandleBuilder, MarketSnapshot
from scripts.paper_live import LiveSessionDeps, run_live_session


@pytest.fixture(autouse=True)
def _disable_live_alert_dispatcher(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_async() -> None:
        return None

    monkeypatch.setattr("scripts.paper_live._start_alert_dispatcher", lambda: None)
    monkeypatch.setattr("scripts.paper_live.maybe_shutdown_alert_dispatcher", _noop_async)
    monkeypatch.setattr(
        "scripts.paper_live.get_paper_db",
        lambda: SimpleNamespace(
            get_feed_state=lambda session_id: FeedState(
                session_id=session_id,
                status="OK",
                stale_reason=None,
                last_price=None,
                last_event_ts=None,
                last_bar_ts=None,
                raw_state={},
                updated_at=datetime(2024, 1, 1, 9, 15),
            )
        ),
    )


def test_five_minute_candle_builder_emits_closed_bars() -> None:
    builder = FiveMinuteCandleBuilder(interval_minutes=5)

    snapshots = [
        MarketSnapshot(
            symbol="SBIN",
            ts=datetime(2024, 1, 1, 9, 15),
            last_price=100.0,
            volume=10.0,
        ),
        MarketSnapshot(
            symbol="SBIN",
            ts=datetime(2024, 1, 1, 9, 17),
            last_price=101.5,
            volume=11.0,
        ),
        MarketSnapshot(
            symbol="SBIN",
            ts=datetime(2024, 1, 1, 9, 20),
            last_price=102.0,
            volume=12.0,
        ),
    ]

    assert builder.ingest(snapshots[0]) == []
    assert builder.ingest(snapshots[1]) == []

    closed = builder.ingest(snapshots[2])
    assert len(closed) == 1
    candle = closed[0]
    assert candle.symbol == "SBIN"
    assert candle.bar_start == datetime(2024, 1, 1, 9, 15)
    assert candle.bar_end == datetime(2024, 1, 1, 9, 20)
    assert candle.open == pytest.approx(100.0)
    assert candle.high == pytest.approx(101.5)
    assert candle.low == pytest.approx(100.0)
    assert candle.close == pytest.approx(101.5)
    assert candle.volume == pytest.approx(11.0)


def test_five_minute_candle_builder_flushes_partial_bar() -> None:
    builder = FiveMinuteCandleBuilder(interval_minutes=5)

    builder.ingest(
        MarketSnapshot(
            symbol="SBIN",
            ts=datetime(2024, 1, 1, 9, 15),
            last_price=100.0,
            volume=10.0,
        )
    )

    flushed = builder.flush()
    assert len(flushed) == 1
    candle = flushed[0]
    assert candle.symbol == "SBIN"
    assert candle.bar_start == datetime(2024, 1, 1, 9, 15)
    assert candle.bar_end == datetime(2024, 1, 1, 9, 20)
    assert candle.close == pytest.approx(100.0)


def test_kite_quote_adapter_batches_symbol_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    batches: list[list[str]] = []

    class FakeKiteConnect:
        def __init__(self, api_key: str):
            self.api_key = api_key
            self.access_token = None

        def set_access_token(self, access_token: str) -> None:
            self.access_token = access_token

        def ltp(self, keys):
            batch = list(keys)
            batches.append(batch)
            return {
                key: {"last_price": 100.0 + idx, "volume": 10.0 + idx}
                for idx, key in enumerate(batch)
            }

    kiteconnect_module = ModuleType("kiteconnect")
    kiteconnect_module.KiteConnect = FakeKiteConnect
    monkeypatch.setitem(sys.modules, "kiteconnect", kiteconnect_module)
    monkeypatch.setattr(
        "engine.live_market_data.get_settings",
        lambda: SimpleNamespace(
            kite_api_key="kite-key",
            kite_access_token="kite-token",
            paper_live_quote_batch_size=3,
        ),
    )

    from engine.live_market_data import KiteQuoteAdapter

    adapter = KiteQuoteAdapter()
    snapshots = adapter.poll([f"SBIN{i}" for i in range(7)])

    assert [len(batch) for batch in batches] == [3, 3, 1]
    assert [snapshot.symbol for snapshot in snapshots] == [f"SBIN{i}" for i in range(7)]


@pytest.mark.asyncio
async def test_run_live_session_marks_stale_then_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []
    feed_states: list[dict[str, object]] = []
    processed_candles: list[dict[str, object]] = []
    load_count = {"n": 0}
    sessions = [
        SimpleNamespace(
            session_id="sess-1",
            status="ACTIVE",
            symbols=["SBIN"],
            strategy="CPR_LEVELS",
            strategy_params={},
            stale_feed_timeout_sec=30,
        ),
    ]

    async def fake_session_loader(session_id: str):
        await asyncio.sleep(0)
        load_count["n"] += 1
        calls.append(("load", session_id))
        return sessions[min(load_count["n"] - 1, len(sessions) - 1)]

    async def fake_session_updater(session_id: str, **kwargs):
        await asyncio.sleep(0)
        calls.append(("update", kwargs.get("status")))
        return sessions[-1]

    async def fake_feed_writer(**kwargs):
        await asyncio.sleep(0)
        feed_states.append(kwargs)
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        await asyncio.sleep(0)
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class FakeAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            self.calls += 1
            if self.calls == 2:
                return []
            return [
                MarketSnapshot(
                    symbol="SBIN",
                    ts=datetime(2024, 1, 1, 9, 15 if self.calls == 1 else 20),
                    last_price=100.0 if self.calls == 1 else 101.0,
                    volume=10.0 if self.calls == 1 else 15.0,
                ),
            ]

    adapter = FakeAdapter()

    async def fake_sleep(_: float) -> None:
        await asyncio.sleep(0)

    async def fake_risk_enforcer(**kwargs):
        await asyncio.sleep(0)
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    async def fake_evaluate_candle(**kwargs):
        await asyncio.sleep(0)
        processed_candles.append(
            {
                "symbol": kwargs["candle"].symbol,
                "bar_end": kwargs["candle"].bar_end,
                "close": kwargs["candle"].close,
            }
        )
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)
    monkeypatch.setattr("scripts.paper_live.evaluate_candle", fake_evaluate_candle)

    result = await run_live_session(
        session_id="sess-1",
        adapter=adapter,
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=3,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: (
                datetime(2024, 1, 1, 9, 15, 10)
                if adapter.calls <= 1
                else (
                    datetime(2024, 1, 1, 9, 16, 0)
                    if adapter.calls == 2
                    else datetime(2024, 1, 1, 9, 20, 5)
                )
            ),
        ),
    )

    assert result["cycles"] == 3
    assert result["quote_events"] == 2
    assert result["closed_bars"] >= 1
    assert any(state["status"] == "OK" for state in feed_states)
    assert processed_candles[0]["symbol"] == "SBIN"
    assert processed_candles[0]["bar_end"] == datetime(2024, 1, 1, 9, 20)


@pytest.mark.asyncio
async def test_run_live_session_fails_closed_when_finalization_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="sess-finalize",
        status="ACTIVE",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        stale_feed_timeout_sec=30,
    )
    updates: list[dict[str, object]] = []
    feed_states: list[dict[str, object]] = []

    async def fake_session_loader(session_id: str):
        assert session_id == "sess-finalize"
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        assert session_id == "sess-finalize"
        updates.append(dict(kwargs))
        if "status" in kwargs:
            session.status = str(kwargs["status"])
        return session

    async def fake_feed_writer(**kwargs):
        feed_states.append(kwargs)
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class OneShotAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            self.calls += 1
            if self.calls == 1:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 15),
                        last_price=100.0,
                        volume=10.0,
                    )
                ]
            if self.calls == 2:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 20),
                        last_price=101.0,
                        volume=12.0,
                    )
                ]
            return []

    async def fake_sleep(_: float) -> None:
        return None

    async def fake_risk_enforcer(**kwargs):
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    async def fake_evaluate_candle(**kwargs):
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_complete_session(**kwargs):
        raise RuntimeError("finalize failed")

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)
    monkeypatch.setattr("scripts.paper_live.evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(
        "scripts.paper_live.paper_session_driver.complete_session", fake_complete_session
    )

    result = await run_live_session(
        session_id="sess-finalize",
        adapter=OneShotAdapter(),
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=2,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: datetime(2024, 1, 1, 9, 15, 10),
        ),
    )

    assert result["final_status"] == "FAILED"
    assert any(update.get("status") == "FAILED" for update in updates)
    assert feed_states


@pytest.mark.asyncio
async def test_run_live_session_promotes_flatten_time_stop_to_completed_when_flat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="sess-stop-flat",
        status="ACTIVE",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        stale_feed_timeout_sec=30,
    )
    updates: list[dict[str, object]] = []
    complete_calls: list[dict[str, object]] = []
    archived: list[str] = []

    async def fake_session_loader(session_id: str):
        assert session_id == "sess-stop-flat"
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        assert session_id == "sess-stop-flat"
        updates.append(dict(kwargs))
        if "status" in kwargs:
            session.status = str(kwargs["status"])
        return session

    async def fake_feed_writer(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class OneShotAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            self.calls += 1
            if self.calls == 1:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 15),
                        last_price=100.0,
                        volume=10.0,
                    )
                ]
            if self.calls == 2:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 20),
                        last_price=101.0,
                        volume=12.0,
                    )
                ]
            return []

    async def fake_sleep(_: float) -> None:
        return None

    async def fake_risk_enforcer(**kwargs):
        return {"triggered": True, "daily_pnl_used": 0.0, "reasons": ["flatten_time:15:15:00"]}

    async def fake_evaluate_candle(**kwargs):
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_complete_session(**kwargs):
        complete_calls.append(dict(kwargs))
        if kwargs["complete_on_exit"]:
            await fake_session_updater(kwargs["session_id"], status="COMPLETED")

    def fake_archive_completed_session(session_id: str, paper_db=None):
        archived.append(session_id)
        return {"session_id": session_id, "archived": True}

    async def fake_get_session_positions(session_id: str, symbol=None, statuses=None):
        return []

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)
    monkeypatch.setattr("scripts.paper_live.evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(
        "scripts.paper_live.paper_session_driver.complete_session", fake_complete_session
    )
    monkeypatch.setattr(
        "scripts.paper_live.archive_completed_session", fake_archive_completed_session
    )
    monkeypatch.setattr("scripts.paper_live.force_paper_db_sync", lambda _db: None)
    monkeypatch.setattr("scripts.paper_live.get_session_positions", fake_get_session_positions)

    result = await run_live_session(
        session_id="sess-stop-flat",
        adapter=OneShotAdapter(),
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=3,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: datetime(2024, 1, 1, 9, 15, 10),
        ),
    )

    assert result["final_status"] == "COMPLETED"
    assert complete_calls and complete_calls[0]["complete_on_exit"] is True
    assert any(update.get("status") == "COMPLETED" for update in updates)
    assert archived == ["sess-stop-flat"]


@pytest.mark.asyncio
async def test_run_live_session_uses_websocket_path_when_ticker_adapter_is_provided() -> None:
    session = SimpleNamespace(
        session_id="sess-ws",
        status="ACTIVE",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        stale_feed_timeout_sec=0,
    )

    class FakeTickerAdapter:
        def __init__(self):
            self.register_calls: list[tuple[str, list[str]]] = []
            self.drain_calls: list[str] = []
            self.unregister_calls: list[str] = []
            self.close_called = False

        @property
        def is_connected(self) -> bool:
            return True

        @property
        def tick_count(self) -> int:
            return 0

        @property
        def last_tick_ts(self):
            return None

        @property
        def reconnect_count(self) -> int:
            return 0

        def register_session(self, session_id, symbols, builder):
            del builder
            self.register_calls.append((session_id, list(symbols)))

        def synthesize_quiet_symbols(self, session_id, symbols, now):
            del session_id, symbols, now

        def drain_closed(self, session_id):
            self.drain_calls.append(session_id)
            return []

        def update_symbols(self, session_id, symbols):
            del session_id, symbols

        def unregister_session(self, session_id):
            self.unregister_calls.append(session_id)

        def close(self):
            self.close_called = True

    ticker_adapter = FakeTickerAdapter()

    async def fake_session_loader(session_id: str):
        assert session_id == "sess-ws"
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        assert session_id == "sess-ws"
        return session

    async def fake_feed_writer(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_sleep(_: float) -> None:
        return None

    result = await run_live_session(
        session_id="sess-ws",
        ticker_adapter=ticker_adapter,
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=1,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            sleep_fn=fake_sleep,
            now_fn=lambda: datetime(2024, 1, 1, 9, 15),
        ),
    )

    assert ticker_adapter.register_calls == [("sess-ws", ["SBIN"])]
    assert ticker_adapter.drain_calls == ["sess-ws"]
    assert ticker_adapter.unregister_calls == ["sess-ws"]
    assert ticker_adapter.close_called is False
    assert result["poll_interval_sec"] == 1.0


@pytest.mark.asyncio
async def test_run_live_session_fails_closed_when_bar_processing_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="sess-bar-fail",
        status="ACTIVE",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        stale_feed_timeout_sec=30,
    )
    updates: list[dict[str, object]] = []

    async def fake_session_loader(session_id: str):
        assert session_id == "sess-bar-fail"
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        assert session_id == "sess-bar-fail"
        updates.append(dict(kwargs))
        if "status" in kwargs:
            session.status = str(kwargs["status"])
        return session

    async def fake_feed_writer(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class OneShotAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            self.calls += 1
            if self.calls == 1:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 15),
                        last_price=100.0,
                        volume=10.0,
                    )
                ]
            return []

    async def fake_sleep(_: float) -> None:
        return None

    async def fake_risk_enforcer(**kwargs):
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    async def fake_evaluate_candle(**kwargs):
        return {
            "symbol": kwargs["candle"].symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "pending",
            "candidate": None,
            "advance_result": None,
            "setup_row": None,
        }

    async def fake_process_closed_bar_group(**kwargs):
        raise RuntimeError("bar processing failed")

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)
    monkeypatch.setattr("scripts.paper_live.evaluate_candle", fake_evaluate_candle)
    monkeypatch.setattr(
        "scripts.paper_live.paper_session_driver.process_closed_bar_group",
        fake_process_closed_bar_group,
    )

    result = await run_live_session(
        session_id="sess-bar-fail",
        adapter=OneShotAdapter(),
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=2,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: datetime(2024, 1, 1, 9, 15, 10),
        ),
    )

    assert result["final_status"] == "FAILED"
    assert any(update.get("status") == "FAILED" for update in updates)


@pytest.mark.asyncio
async def test_run_live_session_applies_stage_b_direction_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    session = SimpleNamespace(
        session_id="sess-3",
        status="ACTIVE",
        symbols=["SBIN", "TCS", "INFY"],
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "LONG"},
        stale_feed_timeout_sec=60,
    )

    async def fake_session_loader(session_id: str):
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        return session

    async def fake_feed_writer(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class FakeAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            self.calls += 1
            calls.append(list(symbols))
            if self.calls == 1:
                snapshots: list[MarketSnapshot] = []
                for symbol in symbols:
                    snapshots.append(
                        MarketSnapshot(
                            symbol=symbol,
                            ts=datetime(2024, 1, 1, 9, 15),
                            last_price=100.0,
                            volume=10.0,
                        )
                    )
                    snapshots.append(
                        MarketSnapshot(
                            symbol=symbol,
                            ts=datetime(2024, 1, 1, 9, 20),
                            last_price=101.0 if symbol == "SBIN" else 99.0,
                            volume=11.0,
                        )
                    )
                return snapshots
            if self.calls == 2:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 25),
                        last_price=102.0,
                        volume=12.0,
                    )
                ]
            return []

    async def fake_sleep(_: float) -> None:
        return None

    async def fake_risk_enforcer(**kwargs):
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    async def fake_evaluate_candle(**kwargs):
        symbol = kwargs["candle"].symbol
        state = kwargs["runtime_state"].symbols.setdefault(symbol, SimpleNamespace(setup_row=None))
        # Pre-computed directions matching the candle data:
        # SBIN: close=101.0 > tc=99.0 → LONG
        # TCS/INFY: close=99.0 < tc=100.0 → SHORT
        direction = "LONG" if symbol == "SBIN" else "SHORT"
        state.setup_row = {
            "tc": 99.0 if symbol == "SBIN" else 100.0,
            "bc": 100.0,
            "atr": 1.0,
            "setup_source": "test",
            "direction": direction,
        }
        return {
            "symbol": symbol,
            "action": "SKIP",
            "reason": "setup_ready",
            "setup_status": "candidate" if direction == "LONG" else "rejected",
            "candidate": None,
            "advance_result": None,
            "setup_row": state.setup_row,
        }

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)
    monkeypatch.setattr("scripts.paper_live.evaluate_candle", fake_evaluate_candle)

    result = await run_live_session(
        session_id="sess-3",
        adapter=FakeAdapter(),
        poll_interval_sec=0,
        candle_interval_minutes=5,
        max_cycles=2,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: datetime(2024, 1, 1, 9, 18),
        ),
    )

    assert calls[0] == ["SBIN", "TCS", "INFY"]
    assert calls[1] == ["SBIN"]
    assert result["quote_events"] == 7


@pytest.mark.asyncio
async def test_run_live_session_breaks_after_repeated_empty_polls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="sess-2",
        status="ACTIVE",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        stale_feed_timeout_sec=60,
    )

    async def fake_session_loader(session_id: str):
        return session

    async def fake_session_updater(session_id: str, **kwargs):
        return session

    async def fake_feed_writer(**kwargs):
        return SimpleNamespace(**kwargs)

    async def fake_feed_reader(session_id: str):
        return FeedState(
            session_id=session_id,
            status="OK",
            last_event_ts=None,
            last_bar_ts=None,
            last_price=None,
            stale_reason=None,
            raw_state={},
            updated_at=datetime(2024, 1, 1, 9, 15),
        )

    class EmptyAdapter:
        def __init__(self):
            self.calls = 0

        def poll(self, symbols):
            del symbols
            self.calls += 1
            if self.calls == 1:
                return [
                    MarketSnapshot(
                        symbol="SBIN",
                        ts=datetime(2024, 1, 1, 9, 15),
                        last_price=100.0,
                        volume=10.0,
                    )
                ]
            return []

    async def fake_sleep(_: float) -> None:
        return None

    async def fake_risk_enforcer(**kwargs):
        return {"triggered": False, "daily_pnl_used": 0.0, "reasons": []}

    monkeypatch.setattr("scripts.paper_live.enforce_session_risk_controls", fake_risk_enforcer)

    adapter = EmptyAdapter()

    result = await run_live_session(
        session_id="sess-2",
        adapter=adapter,
        poll_interval_sec=0,
        candle_interval_minutes=5,
        deps=LiveSessionDeps(
            session_loader=fake_session_loader,
            session_updater=fake_session_updater,
            feed_writer=fake_feed_writer,
            feed_reader=fake_feed_reader,
            sleep_fn=fake_sleep,
            now_fn=lambda: (
                datetime(2024, 1, 1, 9, 15, 10)
                if adapter.calls <= 1
                else (
                    datetime(2024, 1, 1, 9, 25, 10)
                    if adapter.calls == 2
                    else datetime(2024, 1, 1, 9, 26, 10)
                )
            ),
        ),
    )

    assert result["cycles"] == 3
    assert result["final_status"] == "STALE"
