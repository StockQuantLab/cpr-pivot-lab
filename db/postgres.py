"""PostgreSQL repository helpers.

Used for agent sessions, signals, alerts, and paper-trading operational state.
Market data stays in DuckDB + Parquet.
"""

from __future__ import annotations

import atexit
import json
import re
import threading
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from datetime import time as dt_time
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from sqlalchemy import event, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config.settings import get_settings
from db.paper_db import get_dashboard_paper_db

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_sync_connection_local = threading.local()
_sync_connection_registry: set[psycopg.Connection[Any]] = set()
_sync_connection_registry_lock = threading.Lock()


class PostgresError(RuntimeError):
    """Base error for PostgreSQL helper failures."""


class PostgresConnectionError(PostgresError):
    """Raised when PostgreSQL connection setup or transport fails."""


class PostgresQueryError(PostgresError):
    """Raised when a PostgreSQL query fails."""


class PaperSessionStatus(StrEnum):
    """Paper trading session statuses."""

    PLANNING = "PLANNING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class PositionStatus(StrEnum):
    """Position statuses."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


class OrderStatus(StrEnum):
    """Order statuses."""

    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class FeedStatus(StrEnum):
    """Feed connection statuses."""

    OK = "OK"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    PAUSED = "PAUSED"


# Legacy sets for backward compatibility - will be deprecated
PAPER_SESSION_STATUSES = {s.value for s in PaperSessionStatus}
POSITION_STATUSES = {s.value for s in PositionStatus}
ORDER_STATUSES = {s.value for s in OrderStatus}
FEED_STATUSES = {s.value for s in FeedStatus}
_ACTIVE_SESSION_STATUSES = {
    PaperSessionStatus.ACTIVE.value,
    PaperSessionStatus.PAUSED.value,
    PaperSessionStatus.STOPPING.value,
}

_LEGACY_POSITION_ID_LOCK = threading.Lock()
_LEGACY_POSITION_ID_SEQ = 0
_LEGACY_POSITION_ID_BY_REAL: dict[str, int] = {}
_REAL_POSITION_ID_BY_LEGACY: dict[int, str] = {}
_LEGACY_ORDER_ID_LOCK = threading.Lock()
_LEGACY_ORDER_ID_SEQ = 0
_LEGACY_ORDER_ID_BY_REAL: dict[str, int] = {}
_REAL_ORDER_ID_BY_LEGACY: dict[int, str] = {}


def _register_legacy_position_id(real_position_id: str) -> int:
    global _LEGACY_POSITION_ID_SEQ
    real_position_id = str(real_position_id)
    with _LEGACY_POSITION_ID_LOCK:
        legacy = _LEGACY_POSITION_ID_BY_REAL.get(real_position_id)
        if legacy is not None:
            return legacy
        _LEGACY_POSITION_ID_SEQ += 1
        legacy = _LEGACY_POSITION_ID_SEQ
        _LEGACY_POSITION_ID_BY_REAL[real_position_id] = legacy
        _REAL_POSITION_ID_BY_LEGACY[legacy] = real_position_id
        return legacy


def _resolve_real_position_id(position_id: int | str) -> str:
    if isinstance(position_id, int):
        with _LEGACY_POSITION_ID_LOCK:
            real_id = _REAL_POSITION_ID_BY_LEGACY.get(position_id)
        if real_id is not None:
            return real_id
    return str(position_id)


def _legacy_position_id(position_id: Any) -> Any:
    if isinstance(position_id, str):
        with _LEGACY_POSITION_ID_LOCK:
            legacy = _LEGACY_POSITION_ID_BY_REAL.get(position_id)
        if legacy is not None:
            return legacy
    return position_id


def _register_legacy_order_id(real_order_id: str) -> int:
    global _LEGACY_ORDER_ID_SEQ
    real_order_id = str(real_order_id)
    with _LEGACY_ORDER_ID_LOCK:
        legacy = _LEGACY_ORDER_ID_BY_REAL.get(real_order_id)
        if legacy is not None:
            return legacy
        _LEGACY_ORDER_ID_SEQ += 1
        legacy = _LEGACY_ORDER_ID_SEQ
        _LEGACY_ORDER_ID_BY_REAL[real_order_id] = legacy
        _REAL_ORDER_ID_BY_LEGACY[legacy] = real_order_id
        return legacy


def _legacy_order_id(order_id: Any) -> Any:
    if isinstance(order_id, str):
        with _LEGACY_ORDER_ID_LOCK:
            legacy = _LEGACY_ORDER_ID_BY_REAL.get(order_id)
        if legacy is not None:
            return legacy
    return order_id


def _resolve_real_order_id(order_id: int | str) -> str:
    if isinstance(order_id, int):
        with _LEGACY_ORDER_ID_LOCK:
            real_id = _REAL_ORDER_ID_BY_LEGACY.get(order_id)
        if real_id is not None:
            return real_id
    return str(order_id)


_PAPER_SESSION_COLS = """
    session_id,
    name,
    strategy,
    COALESCE(symbols, '[]'::jsonb) AS symbols,
    COALESCE(strategy_params, '{{}}'::jsonb) AS strategy_params,
    created_by,
    flatten_time,
    stale_feed_timeout_sec,
    max_daily_loss_pct,
    max_drawdown_pct,
    max_positions,
    max_position_pct,
    daily_pnl_used,
    latest_candle_ts,
    stale_feed_at,
    created_at,
    updated_at,
    started_at,
    ended_at,
    status,
    notes,
    COALESCE(mode, 'replay') AS mode
"""

_PAPER_SESSION_SELECT = f"SELECT {_PAPER_SESSION_COLS} FROM paper_trading_sessions"


def _validate_session_status(status: str) -> None:
    if status not in PAPER_SESSION_STATUSES:
        allowed = ", ".join(sorted(PAPER_SESSION_STATUSES))
        raise ValueError(f"Invalid session status '{status}'. Allowed: {allowed}")


_PAPER_POSITION_SELECT = """
    SELECT
        position_id,
        session_id,
        symbol,
        direction,
        status,
        quantity,
        entry_price,
        opened_at,
        opened_by,
        stop_loss,
        target_price,
        trail_state,
        closed_at,
        close_price,
        realized_pnl,
        current_qty,
        last_price,
        updated_at,
        signal_id,
        created_at
    FROM paper_positions
"""

_PAPER_ORDER_SELECT = """
    SELECT
        order_id,
        session_id,
        position_id,
        signal_id,
        symbol,
        side,
        order_type,
        status,
        requested_qty,
        request_price,
        fill_qty,
        fill_price,
        requested_at,
        filled_at,
        exchange_order_id,
        notes,
        created_at,
        updated_at
    FROM paper_orders
"""

_PAPER_FEED_STATE_SELECT = """
    SELECT
        session_id,
        status,
        last_event_ts,
        last_bar_ts,
        last_price,
        stale_reason,
        raw_state,
        updated_at
    FROM paper_feed_state
"""


@dataclass(slots=True)
class PaperSession:
    session_id: str
    name: str | None
    strategy: str
    status: str
    symbols: list[str]
    strategy_params: dict[str, Any]
    created_by: str | None
    flatten_time: dt_time | None
    stale_feed_timeout_sec: int
    max_daily_loss_pct: float
    max_drawdown_pct: float
    max_positions: int
    max_position_pct: float
    daily_pnl_used: float | None
    latest_candle_ts: datetime | None
    stale_feed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    notes: str | None
    mode: str = "replay"


@dataclass(slots=True)
class PaperPosition:
    position_id: int
    session_id: str
    symbol: str
    direction: str
    status: str
    quantity: float
    entry_price: float
    opened_at: datetime
    opened_by: str | None
    stop_loss: float | None
    target_price: float | None
    trail_state: dict[str, Any]
    closed_at: datetime | None
    close_price: float | None
    realized_pnl: float | None
    current_qty: float | None
    last_price: float | None
    updated_at: datetime
    signal_id: int | None
    created_at: datetime
    exit_reason: str | None = None


@dataclass(slots=True)
class PaperOrder:
    order_id: int
    session_id: str
    position_id: int | None
    signal_id: int | None
    symbol: str
    side: str
    order_type: str
    status: str
    requested_qty: float
    request_price: float | None
    fill_qty: float | None
    fill_price: float | None
    requested_at: datetime
    filled_at: datetime | None
    exchange_order_id: str | None
    notes: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class FeedState:
    session_id: str
    status: str
    last_event_ts: datetime | None
    last_bar_ts: datetime | None
    last_price: float | None
    stale_reason: str | None
    raw_state: dict[str, Any]
    updated_at: datetime


def _get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        settings = get_settings()
        pg_url: URL = settings.get_pg_url()
        _engine = create_async_engine(
            pg_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_recycle=settings.db_pool_recycle_sec,
            pool_pre_ping=True,
            echo=False,
        )

        @event.listens_for(_engine.sync_engine, "connect")
        def _set_search_path(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("SET search_path TO cpr_pivot, public")
            cursor.close()

    return _engine


def _get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            _get_engine(), class_=AsyncSession, expire_on_commit=False
        )
    return _session_factory


def _get_sync_connection() -> psycopg.Connection[Any]:
    conn = getattr(_sync_connection_local, "conn", None)
    if conn is not None and not getattr(conn, "closed", False):
        return conn

    settings = get_settings()
    conn = psycopg.connect(
        settings.get_pg_sync_url(mask_password=False),
        row_factory=dict_row,
        autocommit=True,
    )
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO cpr_pivot, public")
    _sync_connection_local.conn = conn
    with _sync_connection_registry_lock:
        _sync_connection_registry.add(conn)
    return conn


def _reset_sync_connection() -> None:
    conn = getattr(_sync_connection_local, "conn", None)
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
    with _sync_connection_registry_lock:
        _sync_connection_registry.discard(conn)
    _sync_connection_local.conn = None


def _close_sync_connections() -> None:
    with _sync_connection_registry_lock:
        connections = list(_sync_connection_registry)
        _sync_connection_registry.clear()
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass


atexit.register(_close_sync_connections)


def _run_sync_query(
    query: str,
    params: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    try:
        conn = _get_sync_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})
            return [dict(row) for row in cursor.fetchall()]
    except psycopg.Error as exc:
        _reset_sync_connection()
        raise PostgresQueryError("Failed to execute PostgreSQL query") from exc


def _run_sync_query_one(
    query: str,
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    try:
        conn = _get_sync_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})
            row = cursor.fetchone()
            return dict(row) if row is not None else None
    except psycopg.Error as exc:
        _reset_sync_connection()
        raise PostgresQueryError("Failed to execute PostgreSQL query") from exc


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """Async context manager for a PostgreSQL session."""
    async with _get_session_factory()() as session:
        try:
            await session.execute(text("SET search_path TO cpr_pivot, public"))
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _coerce_time(value: str | dt_time | None) -> dt_time | None:
    if value is None:
        return None
    if isinstance(value, dt_time):
        return value
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Unsupported time format: {value!r}")


def _coerce_json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_symbols(value: list[str] | None) -> list[str]:
    if not value:
        return []
    return [symbol.strip().upper() for symbol in value if symbol and symbol.strip()]


def _json_dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL into executable statements for idempotent bootstrap."""
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None
    i = 0

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            current.append(ch)
            i += 1
            if ch == "\n":
                in_line_comment = False
            continue

        if in_block_comment:
            current.append(ch)
            i += 1
            if ch == "*" and nxt == "/":
                current.append(nxt)
                i += 1
                in_block_comment = False
            continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(ch)
            i += 1
            continue

        if in_single:
            current.append(ch)
            i += 1
            if ch == "'" and nxt == "'":
                current.append(nxt)
                i += 1
            elif ch == "'":
                in_single = False
            continue

        if in_double:
            current.append(ch)
            i += 1
            if ch == '"' and nxt == '"':
                current.append(nxt)
                i += 1
            elif ch == '"':
                in_double = False
            continue

        if ch == "-" and nxt == "-":
            current.append(ch)
            current.append(nxt)
            i += 2
            in_line_comment = True
            continue

        if ch == "/" and nxt == "*":
            current.append(ch)
            current.append(nxt)
            i += 2
            in_block_comment = True
            continue

        if ch == "'":
            current.append(ch)
            i += 1
            in_single = True
            continue

        if ch == '"':
            current.append(ch)
            i += 1
            in_double = True
            continue

        if ch == "$":
            match = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if match:
                dollar_tag = match.group(0)
                current.append(dollar_tag)
                i += len(dollar_tag)
                continue

        if ch == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


async def initialize_schema() -> None:
    """Run db/init_pg.sql against the connected PostgreSQL database."""
    sql_path = Path(__file__).with_name("init_pg.sql")
    statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))

    async with get_db_session() as session:
        for statement in statements:
            await session.execute(text(statement))
        await session.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE constraint_schema = current_schema()
                          AND table_name = 'signals'
                          AND constraint_name = 'fk_signals_session_id'
                    ) THEN
                        ALTER TABLE signals
                            ADD CONSTRAINT fk_signals_session_id
                            FOREIGN KEY (session_id)
                            REFERENCES paper_trading_sessions(session_id)
                            ON DELETE SET NULL
                            NOT VALID;
                    END IF;
                END $$;
                """
            )
        )


def _paper_session_from_row(row: Mapping[str, Any]) -> PaperSession:
    values = dict(row)
    return PaperSession(
        session_id=values["session_id"],
        name=values.get("name"),
        strategy=values["strategy"],
        status=values["status"],
        symbols=values.get("symbols") or [],
        strategy_params=values.get("strategy_params") or {},
        created_by=values.get("created_by"),
        flatten_time=values.get("flatten_time"),
        stale_feed_timeout_sec=int(values.get("stale_feed_timeout_sec") or 0),
        max_daily_loss_pct=float(values.get("max_daily_loss_pct") or 0.0),
        max_drawdown_pct=float(values.get("max_drawdown_pct") or 0.0),
        max_positions=int(values.get("max_positions") or 0),
        max_position_pct=float(values.get("max_position_pct") or 0.0),
        daily_pnl_used=values.get("daily_pnl_used"),
        latest_candle_ts=values.get("latest_candle_ts"),
        stale_feed_at=values.get("stale_feed_at"),
        created_at=values["created_at"],
        updated_at=values["updated_at"],
        started_at=values.get("started_at"),
        ended_at=values.get("ended_at"),
        notes=values.get("notes"),
        mode=str(values.get("mode") or "replay"),
    )


def _paper_position_from_row(row: Mapping[str, Any]) -> PaperPosition:
    values = dict(row)
    return PaperPosition(
        position_id=int(values["position_id"]),
        session_id=values["session_id"],
        symbol=values["symbol"],
        direction=values["direction"],
        status=values["status"],
        quantity=float(values["quantity"]),
        entry_price=float(values["entry_price"]),
        opened_at=values["opened_at"],
        opened_by=values.get("opened_by"),
        stop_loss=float(values["stop_loss"]) if values.get("stop_loss") is not None else None,
        target_price=float(values["target_price"])
        if values.get("target_price") is not None
        else None,
        trail_state=values.get("trail_state") or {},
        closed_at=values.get("closed_at"),
        close_price=float(values["close_price"]) if values.get("close_price") is not None else None,
        realized_pnl=float(values["realized_pnl"])
        if values.get("realized_pnl") is not None
        else None,
        current_qty=float(values["current_qty"]) if values.get("current_qty") is not None else None,
        last_price=float(values["last_price"]) if values.get("last_price") is not None else None,
        updated_at=values["updated_at"],
        signal_id=values.get("signal_id"),
        created_at=values["created_at"],
    )


def _paper_order_from_row(row: Mapping[str, Any]) -> PaperOrder:
    values = dict(row)
    return PaperOrder(
        order_id=int(values["order_id"]),
        session_id=values["session_id"],
        position_id=values.get("position_id"),
        signal_id=values.get("signal_id"),
        symbol=values["symbol"],
        side=values["side"],
        order_type=values["order_type"],
        status=values["status"],
        requested_qty=float(values["requested_qty"]),
        request_price=values.get("request_price"),
        fill_qty=values.get("fill_qty"),
        fill_price=values.get("fill_price"),
        requested_at=values["requested_at"],
        filled_at=values.get("filled_at"),
        exchange_order_id=values.get("exchange_order_id"),
        notes=values.get("notes"),
        created_at=values["created_at"],
        updated_at=values["updated_at"],
    )


def _feed_state_from_row(row: Mapping[str, Any]) -> FeedState:
    values = dict(row)
    return FeedState(
        session_id=values["session_id"],
        status=values["status"],
        last_event_ts=values.get("last_event_ts"),
        last_bar_ts=values.get("last_bar_ts"),
        last_price=values.get("last_price"),
        stale_reason=values.get("stale_reason"),
        raw_state=_coerce_json_object(values.get("raw_state")),
        updated_at=values["updated_at"],
    )


def _paper_session_to_postgres(session: Any) -> PaperSession:
    if isinstance(session, Mapping):
        values = dict(session)
        return PaperSession(
            session_id=values["session_id"],
            name=values.get("name"),
            strategy=values["strategy"],
            status=values["status"],
            symbols=values.get("symbols") or [],
            strategy_params=values.get("strategy_params") or {},
            created_by=values.get("created_by"),
            flatten_time=_coerce_time(values.get("flatten_time")),
            stale_feed_timeout_sec=int(values.get("stale_feed_timeout_sec") or 0),
            max_daily_loss_pct=float(values.get("max_daily_loss_pct") or 0.0),
            max_drawdown_pct=float(values.get("max_drawdown_pct") or 0.0),
            max_positions=int(values.get("max_positions") or 0),
            max_position_pct=float(values.get("max_position_pct") or 0.0),
            daily_pnl_used=values.get("daily_pnl_used"),
            latest_candle_ts=values.get("latest_candle_ts"),
            stale_feed_at=values.get("stale_feed_at"),
            created_at=values["created_at"],
            updated_at=values["updated_at"],
            started_at=values.get("started_at"),
            ended_at=values.get("ended_at"),
            notes=values.get("notes"),
            mode=str(values.get("mode") or "replay"),
        )
    return PaperSession(
        session_id=session.session_id,
        name=getattr(session, "name", None),
        strategy=session.strategy,
        status=session.status,
        symbols=list(getattr(session, "symbols", []) or []),
        strategy_params=dict(getattr(session, "strategy_params", {}) or {}),
        created_by=getattr(session, "created_by", None),
        flatten_time=_coerce_time(getattr(session, "flatten_time", None)),
        stale_feed_timeout_sec=int(getattr(session, "stale_feed_timeout_sec", 0) or 0),
        max_daily_loss_pct=float(getattr(session, "max_daily_loss_pct", 0.0) or 0.0),
        max_drawdown_pct=float(getattr(session, "max_drawdown_pct", 0.0) or 0.0),
        max_positions=int(getattr(session, "max_positions", 0) or 0),
        max_position_pct=float(getattr(session, "max_position_pct", 0.0) or 0.0),
        daily_pnl_used=getattr(session, "daily_pnl_used", None),
        latest_candle_ts=getattr(session, "latest_candle_ts", None),
        stale_feed_at=getattr(session, "stale_feed_at", None),
        created_at=getattr(session, "created_at", _utcnow()),
        updated_at=getattr(session, "updated_at", _utcnow()),
        started_at=getattr(session, "started_at", None),
        ended_at=getattr(session, "ended_at", None),
        notes=getattr(session, "notes", None),
        mode=str(getattr(session, "mode", "replay") or "replay"),
    )


def _paper_position_to_postgres(position: Any) -> PaperPosition:
    if isinstance(position, Mapping):
        values = dict(position)
        return PaperPosition(
            position_id=values.get("position_id", ""),
            session_id=values.get("session_id", ""),
            symbol=values.get("symbol", ""),
            direction=values.get("direction", ""),
            status=values.get("status", "OPEN"),
            quantity=float(values.get("quantity", 0.0) or 0.0),
            entry_price=float(values.get("entry_price", 0.0) or 0.0),
            opened_at=values.get("opened_at", _utcnow()),
            opened_by=values.get("opened_by", None),
            stop_loss=values.get("stop_loss", None),
            target_price=values.get("target_price", None),
            trail_state=dict(values.get("trail_state", {}) or {}),
            closed_at=values.get("closed_at"),
            close_price=values.get("close_price") or values.get("exit_price"),
            realized_pnl=values.get("realized_pnl")
            if values.get("realized_pnl") is not None
            else values.get("pnl"),
            current_qty=values.get("current_qty"),
            last_price=values.get("last_price"),
            updated_at=values.get("updated_at", _utcnow()),
            signal_id=values.get("signal_id"),
            created_at=values.get("created_at", _utcnow()),
            exit_reason=values.get("exit_reason") or values.get("closed_by"),
        )
    quantity = getattr(position, "quantity", None)
    if quantity is None:
        quantity = getattr(position, "qty", 0) or 0
    current_qty = getattr(position, "current_qty", None)
    if current_qty is None:
        current_qty = quantity
    entry_time = getattr(position, "opened_at", None) or getattr(position, "entry_time", None)
    exit_time = getattr(position, "closed_at", None) or getattr(position, "exit_time", None)
    return PaperPosition(
        position_id=_legacy_position_id(getattr(position, "position_id", "")),
        session_id=getattr(position, "session_id", ""),
        symbol=getattr(position, "symbol", ""),
        direction=getattr(position, "direction", ""),
        status=getattr(position, "status", "OPEN"),
        quantity=float(quantity or 0.0),
        entry_price=float(getattr(position, "entry_price", 0.0) or 0.0),
        opened_at=entry_time or _utcnow(),
        opened_by=getattr(position, "opened_by", None),
        stop_loss=getattr(position, "stop_loss", None),
        target_price=getattr(position, "target_price", None),
        trail_state=dict(getattr(position, "trail_state", {}) or {}),
        closed_at=exit_time,
        close_price=getattr(position, "close_price", None) or getattr(position, "exit_price", None),
        realized_pnl=getattr(position, "realized_pnl", None)
        if getattr(position, "realized_pnl", None) is not None
        else getattr(position, "pnl", None),
        current_qty=current_qty,
        last_price=getattr(position, "last_price", None),
        updated_at=getattr(position, "updated_at", _utcnow()),
        signal_id=getattr(position, "signal_id", None),
        created_at=getattr(position, "created_at", _utcnow()),
        exit_reason=getattr(position, "exit_reason", None) or getattr(position, "closed_by", None),
    )


def _paper_order_to_postgres(order: Any) -> PaperOrder:
    return PaperOrder(
        order_id=_legacy_order_id(getattr(order, "order_id", "")),
        session_id=getattr(order, "session_id", ""),
        position_id=getattr(order, "position_id", None),
        signal_id=getattr(order, "signal_id", None),
        symbol=getattr(order, "symbol", ""),
        side=getattr(order, "side", ""),
        order_type=getattr(order, "order_type", "MARKET"),
        status=getattr(order, "status", "FILLED"),
        requested_qty=float(getattr(order, "requested_qty", 0.0) or 0.0),
        request_price=getattr(order, "request_price", None),
        fill_qty=getattr(order, "fill_qty", None),
        fill_price=getattr(order, "fill_price", None),
        requested_at=getattr(order, "requested_at", _utcnow()),
        filled_at=getattr(order, "filled_at", None),
        exchange_order_id=getattr(order, "exchange_order_id", None),
        notes=getattr(order, "notes", None),
        created_at=getattr(order, "created_at", _utcnow()),
        updated_at=getattr(order, "updated_at", _utcnow()),
    )


def _paper_feed_state_to_postgres(feed_state: Any) -> FeedState:
    if isinstance(feed_state, Mapping):
        values = dict(feed_state)
        return FeedState(
            session_id=values.get("session_id", ""),
            status=values.get("status", "IDLE"),
            last_event_ts=values.get("last_event_ts"),
            last_bar_ts=values.get("last_bar_ts"),
            last_price=values.get("last_price"),
            stale_reason=values.get("stale_reason"),
            raw_state=dict(values.get("raw_state", {}) or {}),
            updated_at=values.get("updated_at", _utcnow()),
        )
    return FeedState(
        session_id=getattr(feed_state, "session_id", ""),
        status=getattr(feed_state, "status", "IDLE"),
        last_event_ts=getattr(feed_state, "last_event_ts", None),
        last_bar_ts=getattr(feed_state, "last_bar_ts", None),
        last_price=getattr(feed_state, "last_price", None),
        stale_reason=getattr(feed_state, "stale_reason", None),
        raw_state=dict(getattr(feed_state, "raw_state", {}) or {}),
        updated_at=getattr(feed_state, "updated_at", _utcnow()),
    )


async def get_session(session_id: str) -> PaperSession | None:
    session = get_dashboard_paper_db().get_session(session_id)
    return _paper_session_to_postgres(session) if session else None


async def get_active_sessions() -> list[PaperSession]:
    sessions = get_dashboard_paper_db().get_active_sessions()
    return [_paper_session_to_postgres(session) for session in sessions]


async def create_paper_session(
    *,
    session_id: str | None = None,
    name: str | None = None,
    strategy: str | None = None,
    symbols: list[str] | None = None,
    status: str = "PLANNING",
    strategy_params: Mapping[str, Any] | None = None,
    created_by: str | None = None,
    flatten_time: str | dt_time | None = None,
    stale_feed_timeout_sec: int | None = None,
    max_daily_loss_pct: float | None = None,
    max_drawdown_pct: float | None = None,
    max_positions: int | None = None,
    max_position_pct: float | None = None,
    mode: str = "replay",
    notes: str | None = None,
) -> PaperSession:
    settings = get_settings()
    _validate_session_status(status)

    session_id = session_id or f"paper-{uuid4().hex}"
    flattened_time = _coerce_time(flatten_time or settings.paper_flatten_time)
    async with get_db_session() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                    INSERT INTO paper_trading_sessions (
                        session_id, name, strategy, symbols, strategy_params,
                        status, mode, created_by, flatten_time,
                        stale_feed_timeout_sec, max_daily_loss_pct,
                        max_drawdown_pct, max_positions, max_position_pct,
                        daily_pnl_used, latest_candle_ts, stale_feed_at,
                        created_at, updated_at, started_at, ended_at,
                        notes
                    ) VALUES (
                        :session_id, :name, :strategy, :symbols, :strategy_params,
                        :status, :mode, :created_by, :flatten_time,
                        :stale_feed_timeout_sec, :max_daily_loss_pct,
                        :max_drawdown_pct, :max_positions, :max_position_pct,
                        :daily_pnl_used, :latest_candle_ts, :stale_feed_at,
                        :created_at, :updated_at, :started_at, :ended_at,
                        :notes
                    )
                    RETURNING *
                    """
                    ),
                    {
                        "session_id": session_id,
                        "name": name,
                        "strategy": strategy or settings.paper_default_strategy,
                        "symbols": _coerce_symbols(symbols),
                        "strategy_params": dict(strategy_params or {}),
                        "status": status,
                        "mode": mode,
                        "created_by": created_by,
                        "flatten_time": flattened_time,
                        "stale_feed_timeout_sec": (
                            stale_feed_timeout_sec
                            if stale_feed_timeout_sec is not None
                            else settings.paper_stale_feed_timeout_sec
                        ),
                        "max_daily_loss_pct": (
                            max_daily_loss_pct
                            if max_daily_loss_pct is not None
                            else settings.paper_max_daily_loss_pct
                        ),
                        "max_drawdown_pct": (
                            max_drawdown_pct
                            if max_drawdown_pct is not None
                            else settings.paper_max_drawdown_pct
                        ),
                        "max_positions": (
                            max_positions
                            if max_positions is not None
                            else settings.paper_max_positions
                        ),
                        "max_position_pct": (
                            max_position_pct
                            if max_position_pct is not None
                            else settings.paper_max_position_pct
                        ),
                        "daily_pnl_used": 0.0,
                        "latest_candle_ts": None,
                        "stale_feed_at": None,
                        "created_at": _utcnow(),
                        "updated_at": _utcnow(),
                        "started_at": None,
                        "ended_at": None,
                        "notes": notes,
                    },
                )
            )
            .mappings()
            .one()
        )
    return _paper_session_to_postgres(row)


create_session = create_paper_session


async def update_session(
    session_id: str,
    *,
    status: str | None = None,
    latest_candle_ts: datetime | None = None,
    clear_latest_candle_ts: bool = False,
    stale_feed_at: datetime | None = None,
    clear_stale_feed_at: bool = False,
    daily_pnl_used: float | None = None,
    notes: str | None = None,
) -> PaperSession | None:
    if status is not None:
        _validate_session_status(status)
    sets = ["updated_at = :updated_at"]
    params: dict[str, Any] = {
        "session_id": session_id,
        "updated_at": _utcnow(),
    }
    if status is not None:
        sets.append("status = :status")
        params["status"] = status
    if latest_candle_ts is not None:
        sets.append("latest_candle_ts = :latest_candle_ts")
        params["latest_candle_ts"] = latest_candle_ts
    elif clear_latest_candle_ts:
        sets.append("latest_candle_ts = NULL")
    if stale_feed_at is not None:
        sets.append("stale_feed_at = :stale_feed_at")
        params["stale_feed_at"] = stale_feed_at
    elif clear_stale_feed_at:
        sets.append("stale_feed_at = NULL")
    if daily_pnl_used is not None:
        sets.append("daily_pnl_used = :daily_pnl_used")
        params["daily_pnl_used"] = daily_pnl_used
    if notes is not None:
        sets.append("notes = :notes")
        params["notes"] = notes
    async with get_db_session() as session:
        row = (
            (
                await session.execute(
                    text(
                        f"""
                    UPDATE paper_trading_sessions
                    SET {", ".join(sets)}
                    WHERE session_id = :session_id
                    RETURNING *
                    """
                    ),
                    params,
                )
            )
            .mappings()
            .one_or_none()
        )
    return _paper_session_to_postgres(row) if row else None


update_session_state = update_session


# ── Position / Order / Feed CRUD ─────────────────────────────────────────────


async def open_position(
    *,
    session_id: str,
    symbol: str,
    direction: str,
    quantity: float,
    entry_price: float,
    stop_loss: float | None = None,
    target_price: float | None = None,
    trail_state: Mapping[str, Any] | None = None,
    signal_id: int | None = None,
    opened_by: str | None = None,
    opened_at: datetime | None = None,
) -> PaperPosition:
    if direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")
    qty_value = round(quantity)
    opened_ts = opened_at or _utcnow()
    async with get_db_session() as session:
        result = await session.execute(
            text(
                """
                INSERT INTO paper_positions (
                    session_id, symbol, direction, status,
                    quantity, entry_price, stop_loss, target_price,
                    trail_state, opened_at, signal_id, opened_by,
                    current_qty, last_price
                ) VALUES (
                    :session_id, :symbol, :direction, :status,
                    :quantity, :entry_price, :stop_loss, :target_price,
                    :trail_state, :opened_at, :signal_id, :opened_by,
                    :current_qty, :last_price
                )
                RETURNING *
                """
            ),
            {
                "session_id": session_id,
                "symbol": symbol,
                "direction": direction,
                "status": "OPEN",
                "quantity": qty_value,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target_price": target_price,
                "trail_state": _json_dump(dict(trail_state or {})),
                "opened_at": opened_ts,
                "signal_id": signal_id,
                "opened_by": opened_by,
                "current_qty": float(quantity),
                "last_price": entry_price,
            },
        )
        row = result.mappings().one()
    paper_position = _paper_position_from_row(row)
    _register_legacy_position_id(str(paper_position.position_id))
    return _paper_position_to_postgres(paper_position)


async def get_open_positions(session_id: str, symbol: str | None = None) -> list[PaperPosition]:
    return await get_session_positions(session_id, symbol=symbol, statuses=["OPEN"])


async def get_session_positions(
    session_id: str,
    symbol: str | None = None,
    statuses: list[str] | None = None,
) -> list[PaperPosition]:
    positions = get_dashboard_paper_db().get_session_positions(
        session_id, symbol=symbol, statuses=statuses
    )
    return [_paper_position_to_postgres(p) for p in positions]


async def update_position(
    position_id: int,
    *,
    status: str | None = None,
    stop_loss: float | None = None,
    target_price: float | None = None,
    trail_state: Mapping[str, Any] | None = None,
    current_qty: float | None = None,
    last_price: float | None = None,
    close_price: float | None = None,
    realized_pnl: float | None = None,
    closed_by: str | None = None,
) -> PaperPosition | None:
    if status is not None:
        if status not in POSITION_STATUSES:
            raise ValueError(f"Invalid position status '{status}'.")
    updated_at = _utcnow()
    sets = ["updated_at = NOW()"]
    params: dict[str, Any] = {"position_id": position_id, "updated_at": updated_at}
    if status is not None:
        sets.append("status = :status")
        params["status"] = status
        if status in ("CLOSED", "FLATTENED"):
            sets.append("closed_at = :closed_at")
            params["closed_at"] = updated_at
    if stop_loss is not None:
        sets.append("stop_loss = :stop_loss")
        params["stop_loss"] = stop_loss
    if target_price is not None:
        sets.append("target_price = :target_price")
        params["target_price"] = target_price
    if trail_state is not None:
        sets.append("trail_state = :trail_state")
        params["trail_state"] = _json_dump(dict(trail_state or {}))
    if current_qty is not None:
        sets.append("current_qty = :current_qty")
        params["current_qty"] = current_qty
    if last_price is not None:
        sets.append("last_price = :last_price")
        params["last_price"] = last_price
    if close_price is not None:
        sets.append("close_price = :close_price")
        params["close_price"] = close_price
    if realized_pnl is not None:
        sets.append("realized_pnl = :realized_pnl")
        params["realized_pnl"] = realized_pnl
    if closed_by is not None:
        sets.append("closed_by = :closed_by")
        params["closed_by"] = closed_by
    async with get_db_session() as session:
        result = await session.execute(
            text(
                f"UPDATE paper_positions SET {', '.join(sets)} WHERE position_id = :position_id RETURNING *"
            ),
            params,
        )
        row = result.mappings().one_or_none()
    return _paper_position_to_postgres(_paper_position_from_row(row)) if row else None


async def close_position(
    position_id: int,
    close_price: float,
    realized_pnl: float | None = None,
    closed_by: str | None = None,
) -> PaperPosition | None:
    return await update_position(
        position_id,
        status="CLOSED",
        close_price=close_price,
        realized_pnl=realized_pnl,
        closed_by=closed_by,
    )


async def append_order_event(
    *,
    session_id: str,
    symbol: str,
    side: str,
    requested_qty: float,
    position_id: int | None = None,
    signal_id: int | None = None,
    order_type: str = "MARKET",
    request_price: float | None = None,
    fill_qty: float | None = None,
    fill_price: float | None = None,
    status: str = "NEW",
    exchange_order_id: str | None = None,
    notes: str | None = None,
) -> PaperOrder:
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if order_type not in {"MARKET", "LIMIT", "STOP", "STOP_LIMIT"}:
        raise ValueError("Unsupported order_type")
    if status not in ORDER_STATUSES:
        raise ValueError(f"Invalid order status '{status}'.")
    requested_at = _utcnow()
    filled_at = requested_at if status == "FILLED" else None
    async with get_db_session() as session:
        result = await session.execute(
            text(
                """
                INSERT INTO paper_orders (
                    session_id, position_id, signal_id, symbol, side,
                    order_type, requested_qty, request_price, fill_qty,
                    fill_price, status, requested_at, filled_at,
                    exchange_order_id, notes
                ) VALUES (
                    :session_id, :position_id, :signal_id, :symbol, :side,
                    :order_type, :requested_qty, :request_price, :fill_qty,
                    :fill_price, :status, :requested_at, :filled_at,
                    :exchange_order_id, :notes
                )
                RETURNING *
                """
            ),
            {
                "session_id": session_id,
                "position_id": position_id,
                "signal_id": signal_id,
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "requested_qty": requested_qty,
                "request_price": request_price,
                "fill_qty": fill_qty,
                "fill_price": fill_price,
                "status": status,
                "requested_at": requested_at,
                "filled_at": filled_at,
                "exchange_order_id": exchange_order_id,
                "notes": notes,
            },
        )
        row = result.mappings().one()
    order = _paper_order_from_row(row)
    _register_legacy_order_id(str(order.order_id))
    return _paper_order_to_postgres(order)


async def get_session_orders(session_id: str, symbol: str | None = None) -> list[PaperOrder]:
    orders = get_dashboard_paper_db().get_session_orders(session_id, symbol=symbol)
    return [_paper_order_to_postgres(order) for order in orders]


async def set_signal_state(
    *,
    signal_id: int,
    is_active: bool,
    current_price: float | None = None,
    profit_loss: float | None = None,
    exit_price: float | None = None,
) -> None:
    async with get_db_session() as session:
        await session.execute(
            text(
                """
                UPDATE signals
                SET
                    is_active = :is_active,
                    current_price = COALESCE(:current_price, current_price),
                    profit_loss = COALESCE(:profit_loss, profit_loss),
                    exit_price = COALESCE(:exit_price, exit_price),
                    closed_at = CASE WHEN :is_active = FALSE THEN NOW() ELSE closed_at END
                WHERE id = :signal_id
                """
            ),
            {
                "signal_id": signal_id,
                "is_active": is_active,
                "current_price": current_price,
                "profit_loss": profit_loss,
                "exit_price": exit_price,
            },
        )


async def write_signal(
    *,
    session_id: str | None,
    symbol: str,
    signal_type: str,
    trigger_price: float,
    current_price: float | None = None,
    direction: str | None = None,
    strategy: str | None = None,
    signal_key: str | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    is_active: bool = True,
    stop_loss: float | None = None,
    profit_loss: float | None = None,
    entry_price: float | None = None,
    exit_price: float | None = None,
) -> int:
    if signal_type not in {"BUY", "SELL"}:
        raise ValueError("signal_type must be BUY or SELL")
    if direction is not None and direction not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")

    payload = {
        "session_id": session_id,
        "symbol": symbol,
        "signal_type": signal_type,
        "trigger_price": trigger_price,
        "current_price": current_price,
        "direction": direction,
        "strategy": strategy,
        "signal_key": signal_key,
        "source_type": source_type,
        "source_id": source_id,
        "is_active": is_active,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "stop_loss": stop_loss,
        "profit_loss": profit_loss,
        "created_at": _utcnow(),
    }

    async with get_db_session() as session:
        if session_id and signal_key:
            row = (
                await session.execute(
                    text(
                        """
                        INSERT INTO signals (
                            session_id,
                            symbol,
                            signal_type,
                            direction,
                            strategy,
                            signal_key,
                            source_type,
                            source_id,
                            trigger_price,
                            current_price,
                            entry_price,
                            exit_price,
                            stop_loss,
                            profit_loss,
                            is_active,
                            created_at
                        )
                        VALUES (
                            :session_id,
                            :symbol,
                            :signal_type,
                            :direction,
                            :strategy,
                            :signal_key,
                            :source_type,
                            :source_id,
                            :trigger_price,
                            :current_price,
                            :entry_price,
                            :exit_price,
                            :stop_loss,
                            :profit_loss,
                            :is_active,
                            :created_at
                        )
                        ON CONFLICT (session_id, signal_key)
                        DO UPDATE SET
                            trigger_price = EXCLUDED.trigger_price,
                            current_price = EXCLUDED.current_price,
                            direction = EXCLUDED.direction,
                            strategy = EXCLUDED.strategy,
                            source_type = EXCLUDED.source_type,
                            source_id = EXCLUDED.source_id,
                            exit_price = EXCLUDED.exit_price,
                            stop_loss = EXCLUDED.stop_loss,
                            profit_loss = EXCLUDED.profit_loss,
                            is_active = EXCLUDED.is_active,
                            closed_at = CASE
                                WHEN EXCLUDED.is_active = FALSE THEN NOW()
                                ELSE signals.closed_at
                            END,
                            created_at = EXCLUDED.created_at
                        RETURNING id
                        """
                    ),
                    payload,
                )
            ).scalar_one()
            return int(row)

        row = (
            await session.execute(
                text(
                    """
                    INSERT INTO signals (
                        session_id,
                        symbol,
                        signal_type,
                        direction,
                        strategy,
                        signal_key,
                        source_type,
                        source_id,
                        trigger_price,
                        current_price,
                        entry_price,
                        exit_price,
                        stop_loss,
                        profit_loss,
                        is_active,
                        created_at
                    )
                    VALUES (
                        :session_id,
                        :symbol,
                        :signal_type,
                        :direction,
                        :strategy,
                        :signal_key,
                        :source_type,
                        :source_id,
                        :trigger_price,
                        :current_price,
                        :entry_price,
                        :exit_price,
                        :stop_loss,
                        :profit_loss,
                        :is_active,
                        :created_at
                    )
                    RETURNING id
                    """
                ),
                payload,
            )
        ).scalar_one()
    return int(row)


async def upsert_feed_state(
    *,
    session_id: str,
    status: str,
    last_event_ts: datetime | None = None,
    last_bar_ts: datetime | None = None,
    last_price: float | None = None,
    stale_reason: str | None = None,
    raw_state: Mapping[str, Any] | None = None,
) -> FeedState:
    if status not in FEED_STATUSES:
        allowed = ", ".join(sorted(FEED_STATUSES))
        raise ValueError(f"Invalid feed status '{status}'. Allowed: {allowed}")
    async with get_db_session() as session:
        row = (
            (
                await session.execute(
                    text(
                        """
                    INSERT INTO paper_feed_state (
                        session_id, status, last_event_ts, last_bar_ts,
                        last_price, stale_reason, raw_state
                    ) VALUES (
                        :session_id, :status, :last_event_ts, :last_bar_ts,
                        :last_price, :stale_reason, :raw_state
                    )
                    ON CONFLICT (session_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        last_event_ts = EXCLUDED.last_event_ts,
                        last_bar_ts = EXCLUDED.last_bar_ts,
                        last_price = EXCLUDED.last_price,
                        stale_reason = EXCLUDED.stale_reason,
                        raw_state = EXCLUDED.raw_state,
                        updated_at = NOW()
                    RETURNING *
                    """
                    ),
                    {
                        "session_id": session_id,
                        "status": status,
                        "last_event_ts": last_event_ts,
                        "last_bar_ts": last_bar_ts,
                        "last_price": last_price,
                        "stale_reason": stale_reason,
                        "raw_state": _json_dump(dict(raw_state or {})),
                    },
                )
            )
            .mappings()
            .one()
        )
    return _paper_feed_state_to_postgres(_feed_state_from_row(row))


async def get_feed_state(session_id: str) -> FeedState | None:
    feed_state = get_dashboard_paper_db().get_feed_state(session_id)
    return _paper_feed_state_to_postgres(feed_state) if feed_state else None


async def mark_signal_stale(signal_id: int) -> None:
    await set_signal_state(signal_id=signal_id, is_active=False)


async def consume_signal(signal_id: int) -> None:
    await set_signal_state(signal_id=signal_id, is_active=False)


__all__ = [
    "FeedState",
    "PaperOrder",
    "PaperPosition",
    "PaperSession",
    "append_order_event",
    "close_position",
    "consume_signal",
    "create_paper_session",
    "create_session",
    "get_active_sessions",
    "get_db_session",
    "get_feed_state",
    "get_open_positions",
    "get_session",
    "get_session_orders",
    "get_session_positions",
    "initialize_schema",
    "mark_signal_stale",
    "open_position",
    "set_signal_state",
    "split_sql_statements",
    "update_position",
    "update_session",
    "update_session_state",
    "upsert_feed_state",
    "write_signal",
]
