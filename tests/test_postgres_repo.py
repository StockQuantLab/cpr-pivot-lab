"""Tests for db/postgres.py helpers that do not require a live database."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import db.postgres as postgres
from db.postgres import FeedState, _feed_state_from_row, split_sql_statements


class _FakeResult:
    def __init__(
        self,
        *,
        row: dict[str, object] | None = None,
        rows: list[dict[str, object]] | None = None,
        scalar: object | None = None,
    ) -> None:
        self.row = row
        self.rows = rows or []
        self.scalar = scalar

    def mappings(self) -> _FakeResult:
        return self

    def one(self) -> dict[str, object]:
        assert self.row is not None
        return self.row

    def one_or_none(self) -> dict[str, object] | None:
        return self.row

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)

    def scalar_one(self) -> object:
        if self.scalar is not None:
            return self.scalar
        if self.row and "id" in self.row:
            return self.row["id"]
        raise AssertionError("scalar_one() called without a scalar response")


class _FakeSession:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, object] | None]] = []
        self.last_position_update: dict[str, object] = {}

    async def execute(self, statement, params=None):
        sql = str(statement)
        payload = dict(params or {})
        self.executed.append((sql, payload))

        if "INSERT INTO paper_trading_sessions" in sql:
            return _FakeResult(
                row={
                    "session_id": payload["session_id"],
                    "name": payload.get("name"),
                    "strategy": payload["strategy"],
                    "symbols": ["SBIN", "TCS"],
                    "strategy_params": {"risk": "strict"},
                    "created_by": payload.get("created_by"),
                    "flatten_time": payload["flatten_time"],
                    "stale_feed_timeout_sec": payload["stale_feed_timeout_sec"],
                    "max_daily_loss_pct": payload["max_daily_loss_pct"],
                    "max_positions": payload["max_positions"],
                    "max_position_pct": payload["max_position_pct"],
                    "daily_pnl_used": 0,
                    "latest_candle_ts": None,
                    "stale_feed_at": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "started_at": payload["started_at"],
                    "ended_at": payload["ended_at"],
                    "status": payload["status"],
                    "notes": payload.get("notes"),
                }
            )

        if "INSERT INTO paper_positions" in sql:
            return _FakeResult(
                row={
                    "position_id": 1,
                    "session_id": payload["session_id"],
                    "symbol": payload["symbol"],
                    "direction": payload["direction"],
                    "status": payload["status"],
                    "quantity": payload["quantity"],
                    "entry_price": payload["entry_price"],
                    "opened_at": payload["opened_at"],
                    "opened_by": payload.get("opened_by"),
                    "stop_loss": payload.get("stop_loss"),
                    "target_price": payload.get("target_price"),
                    "trail_state": {"step": 1},
                    "closed_at": None,
                    "close_price": None,
                    "realized_pnl": None,
                    "current_qty": None,
                    "last_price": None,
                    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "signal_id": payload.get("signal_id"),
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            )

        if "UPDATE paper_positions" in sql:
            self.last_position_update = payload
            return _FakeResult(
                row={
                    "position_id": payload["position_id"],
                    "session_id": "paper-1",
                    "symbol": "SBIN",
                    "direction": "LONG",
                    "status": payload.get("status", "OPEN"),
                    "quantity": 10,
                    "entry_price": 100.0,
                    "opened_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "opened_by": None,
                    "stop_loss": payload.get("stop_loss"),
                    "target_price": payload.get("target_price"),
                    "trail_state": {"step": 1},
                    "closed_at": payload.get("closed_at"),
                    "close_price": payload.get("close_price"),
                    "realized_pnl": payload.get("realized_pnl"),
                    "current_qty": payload.get("current_qty"),
                    "last_price": payload.get("last_price"),
                    "updated_at": payload["updated_at"],
                    "signal_id": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            )

        if "FROM paper_positions" in sql:
            position_update = self.last_position_update
            return _FakeResult(
                row={
                    "position_id": payload["position_id"],
                    "session_id": "paper-1",
                    "symbol": "SBIN",
                    "direction": "LONG",
                    "status": position_update.get("status", "OPEN"),
                    "quantity": 10,
                    "entry_price": 100.0,
                    "opened_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "opened_by": None,
                    "stop_loss": position_update.get("stop_loss"),
                    "target_price": position_update.get("target_price"),
                    "trail_state": {"step": 1},
                    "closed_at": position_update.get("closed_at"),
                    "close_price": position_update.get("close_price"),
                    "realized_pnl": position_update.get("realized_pnl"),
                    "current_qty": position_update.get("current_qty"),
                    "last_price": position_update.get("last_price"),
                    "updated_at": payload.get("updated_at", datetime(2024, 1, 1, tzinfo=UTC)),
                    "signal_id": None,
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            )

        if "INSERT INTO paper_orders" in sql:
            return _FakeResult(
                row={
                    "order_id": 7,
                    "session_id": payload["session_id"],
                    "position_id": payload.get("position_id"),
                    "signal_id": payload.get("signal_id"),
                    "symbol": payload["symbol"],
                    "side": payload["side"],
                    "order_type": payload["order_type"],
                    "status": payload["status"],
                    "requested_qty": payload["requested_qty"],
                    "request_price": payload.get("request_price"),
                    "fill_qty": payload.get("fill_qty"),
                    "fill_price": payload.get("fill_price"),
                    "requested_at": payload["requested_at"],
                    "filled_at": None,
                    "exchange_order_id": payload.get("exchange_order_id"),
                    "notes": payload.get("notes"),
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            )

        if "INSERT INTO paper_feed_state" in sql:
            return _FakeResult(
                row={
                    "session_id": payload["session_id"],
                    "status": payload["status"],
                    "last_event_ts": payload.get("last_event_ts"),
                    "last_bar_ts": payload.get("last_bar_ts"),
                    "last_price": payload.get("last_price"),
                    "stale_reason": payload.get("stale_reason"),
                    "raw_state": {"last_prices": {"SBIN": 101.0}},
                    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            )

        if "UPDATE paper_trading_sessions" in sql:
            return _FakeResult(
                row={
                    "session_id": payload["session_id"],
                    "name": "Paper Session",
                    "strategy": "FBR",
                    "symbols": ["SBIN", "TCS"],
                    "strategy_params": {"risk": "strict"},
                    "created_by": "tester",
                    "flatten_time": None,
                    "stale_feed_timeout_sec": 180,
                    "max_daily_loss_pct": 0.05,
                    "max_drawdown_pct": 0.10,
                    "max_positions": 4,
                    "max_position_pct": 0.12,
                    "daily_pnl_used": 0.0,
                    "latest_candle_ts": payload.get("latest_candle_ts"),
                    "stale_feed_at": payload.get("stale_feed_at"),
                    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "updated_at": payload.get("updated_at", datetime(2024, 1, 1, tzinfo=UTC)),
                    "started_at": datetime(2024, 1, 1, tzinfo=UTC),
                    "ended_at": payload.get("ended_at"),
                    "status": payload.get("status", "ACTIVE"),
                    "notes": payload.get("notes"),
                    "mode": "replay",
                }
            )

        raise AssertionError(f"Unexpected SQL: {sql}")


@asynccontextmanager
async def _fake_db_session(fake_session: _FakeSession):
    yield fake_session


def test_split_sql_statements_ignores_empty_chunks() -> None:
    sql = """
    CREATE TABLE a (id INT);

    CREATE TABLE b (id INT);
    """

    assert split_sql_statements(sql) == [
        "CREATE TABLE a (id INT)",
        "CREATE TABLE b (id INT)",
    ]


def test_split_sql_statements_keeps_do_block_intact() -> None:
    sql = """
    CREATE TABLE a (id INT);
    DO $$
    BEGIN
        IF EXISTS (SELECT 1) THEN
            PERFORM 1;
        END IF;
    END $$;
    CREATE TABLE b (id INT);
    """

    assert split_sql_statements(sql) == [
        "CREATE TABLE a (id INT)",
        "DO $$\n    BEGIN\n        IF EXISTS (SELECT 1) THEN\n            PERFORM 1;\n        END IF;\n    END $$",
        "CREATE TABLE b (id INT)",
    ]


def test_feed_state_from_row_parses_json_raw_state() -> None:
    row = {
        "session_id": "paper-1",
        "status": "OK",
        "last_event_ts": None,
        "last_bar_ts": None,
        "last_price": 100.25,
        "stale_reason": None,
        "raw_state": '{"symbol_last_prices":{"SBIN":101.0,"RELIANCE":196.5}}',
        "updated_at": "2024-01-01T10:00:00Z",
    }
    state = _feed_state_from_row(row)
    assert isinstance(state, FeedState)
    assert state.raw_state == {"symbol_last_prices": {"SBIN": 101.0, "RELIANCE": 196.5}}


@pytest.mark.asyncio
async def test_paper_session_helpers_use_paper_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = _FakeSession()

    fake_settings = SimpleNamespace(
        paper_default_strategy="FBR",
        paper_flatten_time="15:20:00",
        paper_stale_feed_timeout_sec=180,
        paper_max_daily_loss_pct=0.05,
        paper_max_drawdown_pct=0.10,
        paper_max_positions=4,
        paper_max_position_pct=0.12,
    )

    monkeypatch.setattr(postgres, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(postgres, "get_db_session", lambda: _fake_db_session(fake_session))

    async def fake_get_session(session_id: str):
        return postgres.PaperSession(
            session_id=session_id,
            name="Paper Session",
            strategy="FBR",
            status="COMPLETED",
            symbols=["SBIN", "TCS"],
            strategy_params={"risk": "strict"},
            created_by="tester",
            flatten_time=None,
            stale_feed_timeout_sec=180,
            max_daily_loss_pct=0.05,
            max_drawdown_pct=0.10,
            max_positions=4,
            max_position_pct=0.12,
            daily_pnl_used=0.0,
            latest_candle_ts=None,
            stale_feed_at=None,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at=datetime(2024, 1, 1, tzinfo=UTC),
            started_at=datetime(2024, 1, 1, tzinfo=UTC),
            ended_at=datetime(2024, 1, 2, tzinfo=UTC),
            notes="updated",
        )

    monkeypatch.setattr(postgres, "get_session", fake_get_session)

    session = await postgres.create_paper_session(
        session_id="paper-1",
        name="Paper Session",
        strategy=None,
        symbols=["sbin", "tcs"],
        status="ACTIVE",
        strategy_params={"risk": "strict"},
        created_by="tester",
        notes="bootstrap",
    )
    assert session.strategy == "FBR"
    assert session.max_positions == 4
    assert session.max_position_pct == 0.12

    updated = await postgres.update_session_state(
        "paper-1",
        status="COMPLETED",
        notes="done",
    )
    assert updated is not None
    assert updated.status == "COMPLETED"

    position = await postgres.open_position(
        session_id="paper-1",
        symbol="SBIN",
        direction="LONG",
        quantity=10,
        entry_price=100.0,
        stop_loss=99.0,
        target_price=102.0,
        trail_state={"step": 1},
        signal_id=11,
        opened_by="system",
    )
    assert position.position_id == 1
    assert position.symbol == "SBIN"
    assert position.trail_state == {"step": 1}

    closed = await postgres.close_position(1, close_price=101.5, realized_pnl=15.0)
    assert closed is not None
    assert closed.status == "CLOSED"

    order = await postgres.append_order_event(
        session_id="paper-1",
        symbol="SBIN",
        side="BUY",
        requested_qty=10,
        position_id=1,
        signal_id=11,
        order_type="MARKET",
        request_price=100.0,
        fill_qty=10,
        fill_price=100.2,
        status="FILLED",
        exchange_order_id="oid-1",
        notes="entry",
    )
    assert order.order_id == 7
    assert order.status == "FILLED"

    feed = await postgres.upsert_feed_state(
        session_id="paper-1",
        status="OK",
        last_price=101.0,
        raw_state={"symbol_last_prices": {"SBIN": 101.0}},
    )
    assert feed.session_id == "paper-1"
    assert feed.raw_state == {"last_prices": {"SBIN": 101.0}}

    assert any("paper_trading_sessions" in sql for sql, _ in fake_session.executed)
    assert any("paper_positions" in sql for sql, _ in fake_session.executed)
    assert any("paper_orders" in sql for sql, _ in fake_session.executed)
    assert any("paper_feed_state" in sql for sql, _ in fake_session.executed)


@pytest.mark.asyncio
async def test_write_signal_uses_session_signal_key_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, dict[str, object]]] = []

    class _SignalSession:
        async def execute(self, statement, params=None):
            sql = str(statement)
            payload = dict(params or {})
            executed.append((sql, payload))
            return _FakeResult(scalar=11)

    @asynccontextmanager
    async def fake_get_db_session():
        yield _SignalSession()

    monkeypatch.setattr(postgres, "get_db_session", fake_get_db_session)

    signal_id = await postgres.write_signal(
        session_id="paper-1",
        symbol="SBIN",
        signal_type="BUY",
        trigger_price=100.0,
        current_price=101.0,
        direction="LONG",
        strategy="CPR_LEVELS",
        signal_key="signal-1",
        source_type="alert",
        source_id="signal-row-1",
        is_active=True,
    )

    assert signal_id == 11
    assert executed
    assert executed[0][1]["session_id"] == "paper-1"
    assert executed[0][1]["signal_key"] == "signal-1"
    assert any("ON CONFLICT (session_id, signal_key)" in sql for sql, _ in executed)
