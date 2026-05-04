"""
Paper trading state in data/paper.duckdb.

Handles both daily-sim and daily-live paper trading state.
Tables: paper_sessions, paper_positions, paper_orders, paper_feed_state, alert_log.

This replaces the PostgreSQL paper_trading_sessions/positions/orders/feed_state tables.
PostgreSQL is retained only for agent_sessions and signals.
"""

from __future__ import annotations

import atexit
import json
import logging
import re as _re
import subprocess as _subprocess
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from db.replica import ReplicaSync
from db.replica_consumer import ReplicaConsumer
from engine.execution_defaults import (
    DEFAULT_MAX_POSITION_PCT,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_PORTFOLIO_VALUE,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PAPER_DUCKDB_FILE = DATA_DIR / "paper.duckdb"
REPLICA_DIR = DATA_DIR / "paper_replica"
DEFERRED_SYNC_MARKER = REPLICA_DIR / "deferred_sync_pending.flag"


def _loads_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, dict | list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return default


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _write_deferred_sync_marker() -> None:
    try:
        REPLICA_DIR.mkdir(parents=True, exist_ok=True)
        DEFERRED_SYNC_MARKER.write_text(
            json.dumps({"created_at": _utcnow_iso()}, separators=(",", ":")),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.debug("Failed to write paper deferred-sync marker: %s", exc)


def _clear_deferred_sync_marker() -> None:
    try:
        DEFERRED_SYNC_MARKER.unlink(missing_ok=True)
    except Exception as exc:
        logger.debug("Failed to clear paper deferred-sync marker: %s", exc)


def _recover_deferred_sync_if_needed() -> None:
    if not DEFERRED_SYNC_MARKER.exists():
        return
    try:
        sync = ReplicaSync(PAPER_DUCKDB_FILE, REPLICA_DIR, min_interval_sec=0.0)
        recovery_db = PaperDB(replica_sync=sync)
        try:
            recovery_db.force_sync()
        finally:
            recovery_db.close()
    except Exception as exc:
        logger.warning("Paper deferred-sync recovery skipped: %s", exc)


# ---------------------------------------------------------------------------
# Data classes (match the shapes the engine expects)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PaperSession:
    session_id: str
    strategy: str
    status: str
    symbols: list[str] = field(default_factory=list)
    strategy_params: dict[str, Any] = field(default_factory=dict)
    execution_mode: str = "LIVE"
    trade_date: str = ""
    direction: str = "BOTH"
    name: str | None = None
    created_by: str | None = None
    stale_feed_timeout_sec: int = 30
    portfolio_value: float = DEFAULT_PORTFOLIO_VALUE
    max_daily_loss_pct: float = 0.03
    max_drawdown_pct: float = 0.10
    max_positions: int = DEFAULT_MAX_POSITIONS
    max_position_pct: float = DEFAULT_MAX_POSITION_PCT
    flatten_time: str = "15:00"
    daily_pnl_used: float = 0.0
    total_pnl: float = 0.0
    latest_candle_ts: datetime | None = None
    stale_feed_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    notes: str | None = None
    mode: str = "replay"


@dataclass(slots=True)
class PaperPosition:
    position_id: str
    session_id: str
    symbol: str
    direction: str
    status: str = "OPEN"
    entry_price: float = 0.0
    stop_loss: float | None = None
    target_price: float | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    pnl: float = 0.0
    qty: int = 0
    trail_state: dict[str, Any] = field(default_factory=dict)
    entry_time: datetime | None = None
    exit_time: datetime | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    opened_by: str | None = None
    quantity: float | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    close_price: float | None = None
    realized_pnl: float | None = None
    current_qty: float | None = None
    last_price: float | None = None
    signal_id: int | None = None
    closed_by: str | None = None


@dataclass(slots=True)
class PaperOrder:
    order_id: str
    session_id: str
    position_id: str | None = None
    signal_id: int | None = None
    symbol: str = ""
    side: str = ""
    order_type: str = "MARKET"
    requested_qty: int = 0
    request_price: float | None = None
    fill_price: float | None = None
    fill_qty: int = 0
    status: str = "FILLED"
    requested_at: datetime | None = None
    filled_at: datetime | None = None
    exchange_order_id: str | None = None
    idempotency_key: str | None = None
    broker_mode: str | None = None
    broker_payload: str | None = None
    notes: str | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass(slots=True)
class FeedState:
    session_id: str
    status: str = "IDLE"
    last_event_ts: datetime | None = None
    last_bar_ts: datetime | None = None
    last_price: float | None = None
    stale_reason: str | None = None
    raw_state: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass(slots=True)
class FeedAudit:
    session_id: str
    trade_date: str
    feed_source: str
    transport: str
    symbol: str
    bar_start: datetime
    bar_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_snapshot_ts: datetime | None = None
    last_snapshot_ts: datetime | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# Lock diagnostic
# ---------------------------------------------------------------------------


def _diagnose_paper_db_lock(db_path: Path, exc: Exception) -> None:
    """Print an actionable banner when paper.duckdb is locked by another process.

    DuckDB embeds the holding PID in its IOException message:
      "File is already open in <exe> (PID N)"
    We extract it, resolve the command line via PowerShell, and tell the user
    exactly which taskkill command to run before retrying.
    """
    msg = str(exc)
    pid_match = _re.search(r"\(PID\s+(\d+)\)", msg)
    exe_match = _re.search(r"already open in\s*\n?\s*(.+?)\s*\(PID", msg, _re.DOTALL)

    pid = pid_match.group(1) if pid_match else None
    exe = (exe_match.group(1).strip() if exe_match else None) or "unknown"

    cmd_line = ""
    if pid:
        try:
            result = _subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            cmd_line = result.stdout.strip()
        except Exception:
            pass

    kill_cmd = f"taskkill //F //PID {pid}" if pid else "tasklist — PID not found in error"
    logger.error(
        "[STARTUP BLOCKED] %s is locked by PID %s. Fix: %s",
        db_path.name,
        pid or "unknown",
        kill_cmd,
    )
    print(
        f"\n{'=' * 60}\n"
        f"[STARTUP BLOCKED] {db_path.name} is locked!\n"
        f"  Holding PID : {pid or 'unknown'}\n"
        f"  Executable  : {exe}\n"
        f"  Command     : {cmd_line or '(could not determine)'}\n"
        f"  Fix         : {kill_cmd}\n"
        f"  Then retry  : doppler run -- uv run pivot-paper-trading daily-live ...\n"
        f"{'=' * 60}\n",
        flush=True,
    )


# ---------------------------------------------------------------------------
# PaperDB
# ---------------------------------------------------------------------------


class PaperDB:
    """Paper trading state in paper.duckdb.

    Synchronous API — paper_runtime.py is sync. The async wrapper in
    web/state.py uses ThreadPoolExecutor to call these from NiceGUI.
    """

    def __init__(
        self,
        db_path: Path = PAPER_DUCKDB_FILE,
        replica_sync: ReplicaSync | None = None,
        read_only: bool = False,
    ):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._sync = replica_sync
        self.read_only = read_only
        self._sync_deferred = False  # When True, _after_write skips sync
        self._sync_dirty = False  # Tracks whether writes happened while deferred
        try:
            self.con = duckdb.connect(str(db_path), read_only=read_only)
        except duckdb.IOException as exc:
            if "being used by another process" in str(exc):
                _diagnose_paper_db_lock(db_path, exc)
            raise
        if not read_only:
            self._ensure_all_tables()

    def _ensure_all_tables(self) -> None:
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS paper_sessions (
                session_id     VARCHAR(50) PRIMARY KEY,
                strategy       VARCHAR(20) NOT NULL,
                direction      VARCHAR(10) NOT NULL DEFAULT 'BOTH',
                name           VARCHAR(100),
                symbols        VARCHAR(500),
                strategy_params VARCHAR(2000),
                created_by     VARCHAR(100),
                status         VARCHAR(20) NOT NULL DEFAULT 'PLANNING'
                                CHECK (status IN ('PLANNING','ACTIVE','PAUSED',
                                    'STOPPING','COMPLETED','FAILED','CANCELLED')),
                trade_date     VARCHAR(10),
                execution_mode VARCHAR(10) NOT NULL DEFAULT 'LIVE',
                stale_feed_timeout_sec INT DEFAULT 120,
                portfolio_value DOUBLE DEFAULT 1000000,
                max_daily_loss_pct DOUBLE DEFAULT 0.03,
                max_drawdown_pct DOUBLE DEFAULT 0.10,
                max_positions  INT DEFAULT 5,
                max_position_pct DOUBLE DEFAULT 0.20,
                flatten_time   VARCHAR(10) DEFAULT '15:00',
                daily_pnl_used DOUBLE DEFAULT 0,
                total_pnl      DOUBLE DEFAULT 0,
                latest_candle_ts TIMESTAMPTZ,
                stale_feed_at  TIMESTAMPTZ,
                started_at     TIMESTAMPTZ,
                ended_at       TIMESTAMPTZ,
                notes          VARCHAR(500),
                mode           VARCHAR(20) DEFAULT 'replay',
                created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS paper_positions (
                position_id    VARCHAR(50) PRIMARY KEY,
                session_id     VARCHAR(50) NOT NULL,
                symbol         VARCHAR(20) NOT NULL,
                direction      VARCHAR(10) NOT NULL,
                status         VARCHAR(20) NOT NULL DEFAULT 'OPEN'
                                CHECK (status IN ('OPEN','CLOSED','FLATTENED')),
                entry_price    DOUBLE,
                stop_loss      DOUBLE,
                target_price   DOUBLE,
                exit_price     DOUBLE,
                exit_reason    VARCHAR(50),
                pnl            DOUBLE DEFAULT 0,
                qty            INT DEFAULT 0,
                trail_state    VARCHAR(2000),
                entry_time     TIMESTAMPTZ,
                exit_time      TIMESTAMPTZ,
                opened_by      VARCHAR(100),
                current_qty    DOUBLE,
                last_price     DOUBLE,
                signal_id      INT,
                closed_by      VARCHAR(50),
                created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_pp_session ON paper_positions(session_id)")
        self.con.execute(
            """
            DROP INDEX IF EXISTS idx_pp_session_symbol_status
            """
        )
        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pp_session_symbol_status_lookup
            ON paper_positions(session_id, symbol, status)
            """
        )

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS paper_orders (
                order_id       VARCHAR(50) PRIMARY KEY,
                session_id     VARCHAR(50) NOT NULL,
                position_id    VARCHAR(50),
                signal_id      INT,
                symbol         VARCHAR(20) NOT NULL,
                side           VARCHAR(10) NOT NULL,
                order_type     VARCHAR(10) NOT NULL DEFAULT 'MARKET',
                requested_qty  INT DEFAULT 0,
                request_price  DOUBLE,
                fill_price     DOUBLE,
                fill_qty       INT DEFAULT 0,
                status         VARCHAR(20) NOT NULL DEFAULT 'FILLED'
                                CHECK (status IN ('PENDING','FILLED','CANCELLED','REJECTED')),
                requested_at   TIMESTAMPTZ,
                filled_at      TIMESTAMPTZ,
                exchange_order_id VARCHAR(80),
                idempotency_key VARCHAR(200),
                broker_mode    VARCHAR(30),
                broker_payload VARCHAR(4000),
                notes          VARCHAR(500),
                created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.con.execute(
            "ALTER TABLE paper_orders ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(200)"
        )
        self.con.execute(
            "ALTER TABLE paper_orders ADD COLUMN IF NOT EXISTS broker_mode VARCHAR(30)"
        )
        self.con.execute(
            "ALTER TABLE paper_orders ADD COLUMN IF NOT EXISTS broker_payload VARCHAR(4000)"
        )
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_po_session ON paper_orders(session_id)")
        self.con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_po_idempotency ON paper_orders(idempotency_key)"
        )

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS paper_feed_state (
                session_id     VARCHAR(50) PRIMARY KEY,
                status         VARCHAR(20) NOT NULL DEFAULT 'IDLE',
                last_event_ts  TIMESTAMPTZ,
                last_bar_ts    TIMESTAMPTZ,
                last_price     DOUBLE,
                stale_reason   VARCHAR(100),
                raw_state      VARCHAR(4000),
                updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS paper_feed_audit (
                session_id         VARCHAR(50) NOT NULL,
                trade_date         VARCHAR(10) NOT NULL,
                feed_source        VARCHAR(20) NOT NULL,
                transport          VARCHAR(20) NOT NULL,
                symbol             VARCHAR(20) NOT NULL,
                bar_start          TIMESTAMPTZ NOT NULL,
                bar_end            TIMESTAMPTZ NOT NULL,
                open               DOUBLE,
                high               DOUBLE,
                low                DOUBLE,
                close              DOUBLE,
                volume             DOUBLE,
                first_snapshot_ts  TIMESTAMPTZ,
                last_snapshot_ts   TIMESTAMPTZ,
                created_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (session_id, symbol, bar_end)
            )
        """)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pfa_trade_date ON paper_feed_audit(trade_date, feed_source, session_id)"
        )
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pfa_session ON paper_feed_audit(session_id)"
        )

        self.con.execute("""
            CREATE SEQUENCE IF NOT EXISTS alert_log_seq START 1
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS alert_log (
                id             BIGINT PRIMARY KEY DEFAULT nextval('alert_log_seq'),
                alert_type     VARCHAR(50) NOT NULL,
                alert_level    VARCHAR(10) NOT NULL DEFAULT 'INFO'
                                CHECK (alert_level IN ('INFO','WARN','ERROR','CRITICAL')),
                subject        VARCHAR(200),
                body           TEXT,
                channel        VARCHAR(20) NOT NULL DEFAULT 'BOTH'
                                CHECK (channel IN ('TELEGRAM','EMAIL','BOTH','LOG')),
                status         VARCHAR(20) DEFAULT 'queued'
                                CHECK (status IN ('sent','failed','queued')),
                error_msg      TEXT,
                created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _after_write(self) -> None:
        if self._sync:
            self._sync.mark_dirty()
            if self._sync_deferred:
                self._sync_dirty = True
            else:
                self._sync.maybe_sync(self.con)

    def force_sync(self) -> None:
        """Bypass the debounce and publish a replica snapshot immediately.

        Use after a batch of startup writes (e.g. pre-creating PLANNING sessions)
        where the 5-second debounce would otherwise leave the dashboard with a
        partial view until the first trading event fires.
        """
        if self._sync:
            self._sync.mark_dirty()
            try:
                self._sync.force_sync(self.con)
            except Exception as exc:
                logger.warning("Paper replica force_sync skipped after error: %s", exc)
            else:
                _clear_deferred_sync_marker()

    def defer_sync(self) -> None:
        """Defer replica sync until flush_deferred_sync() is called.

        Use during high-throughput bar processing (16+ position events) to avoid
        triggering maybe_sync() on every individual write. Call before processing
        a bar group, then call flush_deferred_sync() after the bar is complete.
        """
        if self._sync:
            _write_deferred_sync_marker()
        self._sync_deferred = True
        self._sync_dirty = False

    def flush_deferred_sync(self) -> None:
        """Flush any deferred replica sync and resume normal per-write sync.

        Called at the end of a bar group to publish a single replica snapshot
        covering all position updates within that bar.
        """
        self._sync_deferred = False
        if self._sync_dirty and self._sync:
            try:
                self._sync.force_sync(self.con)
            except Exception as exc:
                logger.warning("Paper deferred replica sync skipped after error: %s", exc)
            else:
                _clear_deferred_sync_marker()
            self._sync_dirty = False
        elif not self._sync_dirty:
            _clear_deferred_sync_marker()

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    def create_session(
        self,
        *,
        session_id: str | None = None,
        name: str | None = None,
        strategy: str = "CPR_LEVELS",
        direction: str = "BOTH",
        symbols: list[str] | None = None,
        status: str = "PLANNING",
        trade_date: str = "",
        execution_mode: str = "LIVE",
        strategy_params: dict | None = None,
        created_by: str | None = None,
        stale_feed_timeout_sec: int = 30,
        portfolio_value: float = DEFAULT_PORTFOLIO_VALUE,
        max_daily_loss_pct: float = 0.03,
        max_drawdown_pct: float = 0.10,
        max_positions: int = DEFAULT_MAX_POSITIONS,
        max_position_pct: float = DEFAULT_MAX_POSITION_PCT,
        flatten_time: str = "15:00",
        mode: str = "replay",
        notes: str | None = None,
    ) -> PaperSession:
        sid = session_id or f"paper-{uuid.uuid4().hex[:8]}"
        now = _utcnow_iso()
        started_at = now if status in {"ACTIVE", "PAUSED"} else None
        ended_at = now if status in {"COMPLETED", "FAILED", "CANCELLED"} else None
        self.con.execute(
            """
            INSERT OR REPLACE INTO paper_sessions (
                session_id, name, strategy, direction, symbols, strategy_params,
                status, trade_date, execution_mode, created_by,
                stale_feed_timeout_sec, portfolio_value,
                max_daily_loss_pct, max_drawdown_pct, max_positions,
                max_position_pct, flatten_time, mode,
                notes, created_at, updated_at, started_at, ended_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                sid,
                name,
                strategy,
                direction,
                json.dumps(symbols or []),
                json.dumps(strategy_params or {}),
                status,
                trade_date,
                execution_mode,
                created_by,
                stale_feed_timeout_sec,
                portfolio_value,
                max_daily_loss_pct,
                max_drawdown_pct,
                max_positions,
                max_position_pct,
                flatten_time,
                mode,
                notes,
                now,
                now,
                started_at,
                ended_at,
            ],
        )
        self._after_write()
        return self.get_session(sid)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> PaperSession | None:
        row = self.con.execute(
            "SELECT session_id, strategy, direction, name, symbols, strategy_params, "
            "created_by, status, trade_date, execution_mode, stale_feed_timeout_sec, "
            "portfolio_value, max_daily_loss_pct, max_drawdown_pct, max_positions, "
            "max_position_pct, flatten_time, daily_pnl_used, total_pnl, "
            "latest_candle_ts, stale_feed_at, started_at, ended_at, notes, mode, "
            "created_at, updated_at "
            "FROM paper_sessions WHERE session_id = ?",
            [session_id],
        ).fetchone()
        if not row:
            return None
        return self._row_to_session(row)

    def get_active_sessions(self) -> list[PaperSession]:
        rows = self.con.execute(
            "SELECT session_id, strategy, direction, name, symbols, strategy_params, "
            "created_by, status, trade_date, execution_mode, stale_feed_timeout_sec, "
            "portfolio_value, max_daily_loss_pct, max_drawdown_pct, max_positions, "
            "max_position_pct, flatten_time, daily_pnl_used, total_pnl, "
            "latest_candle_ts, stale_feed_at, started_at, ended_at, notes, mode, "
            "created_at, updated_at "
            "FROM paper_sessions WHERE status IN ('PLANNING', 'ACTIVE', 'PAUSED', 'STOPPING') "
            "ORDER BY created_at DESC"
        ).fetchall()
        return [self._row_to_session(r) for r in rows]

    def update_session(
        self,
        session_id: str,
        *,
        status: str | None = None,
        latest_candle_ts: datetime | None = None,
        clear_latest_candle_ts: bool = False,
        stale_feed_at: datetime | None = None,
        clear_stale_feed_at: bool = False,
        daily_pnl_used: float | None = None,
        total_pnl: float | None = None,
        notes: str | None = None,
    ) -> PaperSession | None:
        sets: list[str] = ["updated_at = ?"]
        params: list[Any] = [_utcnow_iso()]
        if status is not None:
            sets.append("status = ?")
            params.append(status)
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                sets.append("ended_at = ?")
                params.append(params[0])
            elif status == "ACTIVE":
                sets.append("started_at = ?")
                params.append(params[0])
        if clear_latest_candle_ts:
            sets.append("latest_candle_ts = ?")
            params.append(None)
        elif latest_candle_ts is not None:
            sets.append("latest_candle_ts = ?")
            params.append(latest_candle_ts)
        if clear_stale_feed_at:
            sets.append("stale_feed_at = ?")
            params.append(None)
        elif stale_feed_at is not None:
            sets.append("stale_feed_at = ?")
            params.append(stale_feed_at)
        if daily_pnl_used is not None:
            sets.append("daily_pnl_used = ?")
            params.append(daily_pnl_used)
        if total_pnl is not None:
            sets.append("total_pnl = ?")
            params.append(total_pnl)
        if notes is not None:
            sets.append("notes = ?")
            params.append(notes)
        params.append(session_id)
        self.con.execute(
            f"UPDATE paper_sessions SET {', '.join(sets)} WHERE session_id = ?",
            params,
        )
        self._after_write()
        return self.get_session(session_id)

    def _row_to_session(self, row: tuple) -> PaperSession:
        cols = [
            "session_id",
            "strategy",
            "direction",
            "name",
            "symbols",
            "strategy_params",
            "created_by",
            "status",
            "trade_date",
            "execution_mode",
            "stale_feed_timeout_sec",
            "portfolio_value",
            "max_daily_loss_pct",
            "max_drawdown_pct",
            "max_positions",
            "max_position_pct",
            "flatten_time",
            "daily_pnl_used",
            "total_pnl",
            "latest_candle_ts",
            "stale_feed_at",
            "started_at",
            "ended_at",
            "notes",
            "mode",
            "created_at",
            "updated_at",
        ]
        d = dict(zip(cols, row, strict=True))
        return PaperSession(
            session_id=d.get("session_id", ""),
            name=d.get("name"),
            strategy=d.get("strategy", ""),
            direction=d.get("direction", "BOTH"),
            symbols=_loads_json(d.get("symbols"), []),
            strategy_params=_loads_json(d.get("strategy_params"), {}),
            status=d.get("status", "PLANNING"),
            trade_date=d.get("trade_date", ""),
            execution_mode=d.get("execution_mode", "LIVE"),
            created_by=d.get("created_by"),
            stale_feed_timeout_sec=d.get("stale_feed_timeout_sec", 120) or 120,
            portfolio_value=d.get("portfolio_value", DEFAULT_PORTFOLIO_VALUE),
            max_daily_loss_pct=d.get("max_daily_loss_pct", 0.03),
            max_drawdown_pct=d.get("max_drawdown_pct", 0.10),
            max_positions=d.get("max_positions", DEFAULT_MAX_POSITIONS),
            max_position_pct=d.get("max_position_pct", DEFAULT_MAX_POSITION_PCT),
            flatten_time=d.get("flatten_time", "15:00"),
            daily_pnl_used=d.get("daily_pnl_used", 0.0),
            total_pnl=d.get("total_pnl", 0.0),
            latest_candle_ts=d.get("latest_candle_ts"),
            stale_feed_at=d.get("stale_feed_at"),
            created_at=d.get("created_at", _utcnow()),
            updated_at=d.get("updated_at", _utcnow()),
            started_at=d.get("started_at"),
            ended_at=d.get("ended_at"),
            notes=d.get("notes"),
            mode=d.get("mode", "replay"),
        )

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def open_position(
        self,
        *,
        session_id: str,
        symbol: str,
        direction: str,
        qty: int | None = None,
        quantity: float | None = None,
        entry_price: float,
        stop_loss: float | None = None,
        target_price: float | None = None,
        trail_state: dict | None = None,
        entry_time: datetime | None = None,
        opened_by: str | None = None,
        signal_id: int | None = None,
        current_qty: float | None = None,
        last_price: float | None = None,
        opened_at: datetime | None = None,
    ) -> PaperPosition:
        pid = f"pos-{uuid.uuid4().hex[:8]}"
        now = _utcnow_iso()
        qty_value = round(quantity if quantity is not None else (qty or 0))
        current_qty_value = current_qty if current_qty is not None else float(qty_value)
        entry_ts = opened_at or entry_time or now
        existing = self.con.execute(
            """
            SELECT
                position_id, session_id, symbol, direction, status,
                entry_price, stop_loss, target_price, exit_price, exit_reason, pnl,
                qty, trail_state, entry_time, exit_time, opened_by, current_qty,
                last_price, signal_id, closed_by, created_at, updated_at
            FROM paper_positions
            WHERE session_id = ? AND symbol = ? AND status = 'OPEN'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            [session_id, symbol],
        ).fetchone()
        if existing:
            return self._row_to_position(existing)
        try:
            self.con.execute(
                """
                INSERT INTO paper_positions (
                    position_id, session_id, symbol, direction, status,
                    entry_price, stop_loss, target_price, pnl, qty,
                    trail_state, entry_time, exit_time, opened_by, current_qty,
                    last_price, signal_id, closed_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'OPEN', ?, ?, ?, 0, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?, ?)
                """,
                [
                    pid,
                    session_id,
                    symbol,
                    direction,
                    entry_price,
                    stop_loss,
                    target_price,
                    qty_value,
                    json.dumps(trail_state or {}),
                    entry_ts,
                    opened_by,
                    current_qty_value,
                    last_price,
                    signal_id,
                    now,
                    now,
                ],
            )
        except duckdb.ConstraintException as exc:
            raise RuntimeError(
                f"paper position insert failed for session={session_id} symbol={symbol}: {exc}"
            ) from exc
        self._after_write()
        return PaperPosition(
            position_id=pid,
            session_id=session_id,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            qty=qty_value,
            trail_state=trail_state or {},
            entry_time=entry_time,
            opened_by=opened_by,
            quantity=quantity if quantity is not None else float(qty_value),
            opened_at=entry_ts,
            current_qty=current_qty_value,
            last_price=last_price,
            signal_id=signal_id,
        )

    def get_open_positions(self, session_id: str, symbol: str | None = None) -> list[PaperPosition]:
        return self.get_session_positions(session_id, symbol=symbol, statuses=["OPEN"])

    def get_session_positions(
        self,
        session_id: str,
        symbol: str | None = None,
        statuses: list[str] | None = None,
    ) -> list[PaperPosition]:
        where = ["session_id = ?"]
        params: list[Any] = [session_id]
        if symbol:
            where.append("symbol = ?")
            params.append(symbol)
        if statuses:
            ph = ", ".join("?" for _ in statuses)
            where.append(f"status IN ({ph})")
            params.extend(statuses)
        rows = self.con.execute(
            "SELECT position_id, session_id, symbol, direction, status, "
            "entry_price, stop_loss, target_price, exit_price, "
            "exit_reason, pnl, qty, trail_state, entry_time, exit_time, "
            "opened_by, current_qty, last_price, signal_id, closed_by, "
            "created_at, updated_at "
            f"FROM paper_positions WHERE {' AND '.join(where)} ORDER BY entry_time ASC",
            params,
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def update_position(
        self,
        position_id: str,
        *,
        status: str | None = None,
        stop_loss: float | None = None,
        target_price: float | None = None,
        trail_state: dict | None = None,
        current_qty: float | None = None,
        last_price: float | None = None,
        close_price: float | None = None,
        exit_price: float | None = None,
        exit_reason: str | None = None,
        realized_pnl: float | None = None,
        pnl: float | None = None,
        closed_by: str | None = None,
        closed_at: datetime | None = None,
    ) -> None:
        sets: list[str] = ["updated_at = ?"]
        params: list[Any] = [_utcnow_iso()]
        if status is not None:
            current = self.con.execute(
                "SELECT status FROM paper_positions WHERE position_id = ?",
                [position_id],
            ).fetchone()
            if current:
                current_status = str(current[0] or "").upper()
                next_status = str(status or "").upper()
                terminal_statuses = {"CLOSED", "FLATTENED"}
                if current_status in terminal_statuses and next_status != current_status:
                    raise RuntimeError(
                        f"Invalid paper position status transition "
                        f"{current_status}->{next_status} for {position_id}"
                    )
                if current_status != "OPEN" and next_status in terminal_statuses:
                    raise RuntimeError(
                        f"Invalid paper position status transition "
                        f"{current_status}->{next_status} for {position_id}"
                    )
            sets.append("status = ?")
            params.append(status)
            if status in ("CLOSED", "FLATTENED"):
                sets.append("exit_time = ?")
                params.append((closed_at or _utcnow()).isoformat())
        if stop_loss is not None:
            sets.append("stop_loss = ?")
            params.append(stop_loss)
        if target_price is not None:
            sets.append("target_price = ?")
            params.append(target_price)
        if trail_state is not None:
            sets.append("trail_state = ?")
            params.append(json.dumps(trail_state))
        if current_qty is not None:
            sets.append("current_qty = ?")
            params.append(current_qty)
        if last_price is not None:
            sets.append("last_price = ?")
            params.append(last_price)
        final_exit_price = exit_price if exit_price is not None else close_price
        if final_exit_price is not None:
            sets.append("exit_price = ?")
            params.append(final_exit_price)
        if exit_reason is not None:
            sets.append("exit_reason = ?")
            params.append(exit_reason)
        final_pnl = pnl if pnl is not None else realized_pnl
        if final_pnl is not None:
            sets.append("pnl = ?")
            params.append(final_pnl)
        if closed_by is not None:
            sets.append("closed_by = ?")
            params.append(closed_by)
        params.append(position_id)
        self.con.execute(
            f"UPDATE paper_positions SET {', '.join(sets)} WHERE position_id = ?",
            params,
        )
        self._after_write()

    def close_position(
        self,
        position_id: str,
        *,
        exit_price: float,
        exit_reason: str,
        pnl: float,
        closed_by: str | None = None,
    ) -> None:
        self.update_position(
            position_id,
            status="CLOSED",
            exit_price=exit_price,
            exit_reason=exit_reason,
            pnl=pnl,
            closed_by=closed_by,
        )

    def _row_to_position(self, row: tuple) -> PaperPosition:
        cols = [
            "position_id",
            "session_id",
            "symbol",
            "direction",
            "status",
            "entry_price",
            "stop_loss",
            "target_price",
            "exit_price",
            "exit_reason",
            "pnl",
            "qty",
            "trail_state",
            "entry_time",
            "exit_time",
            "opened_by",
            "current_qty",
            "last_price",
            "signal_id",
            "closed_by",
            "created_at",
            "updated_at",
        ]
        d = dict(zip(cols, row, strict=True))
        trail = _loads_json(d.get("trail_state"), {})
        quantity = d.get("qty", 0) or 0
        current_qty = d.get("current_qty")
        return PaperPosition(
            position_id=d.get("position_id", ""),
            session_id=d.get("session_id", ""),
            symbol=d.get("symbol", ""),
            direction=d.get("direction", ""),
            status=d.get("status", "OPEN"),
            entry_price=d.get("entry_price", 0.0) or 0.0,
            stop_loss=d.get("stop_loss"),
            target_price=d.get("target_price"),
            exit_price=d.get("exit_price"),
            exit_reason=d.get("exit_reason"),
            pnl=d.get("pnl", 0.0) or 0.0,
            qty=int(quantity),
            trail_state=trail,
            entry_time=d.get("entry_time"),
            exit_time=d.get("exit_time"),
            opened_by=d.get("opened_by"),
            quantity=float(quantity),
            opened_at=d.get("entry_time"),
            closed_at=d.get("exit_time"),
            close_price=d.get("exit_price"),
            realized_pnl=d.get("pnl"),
            current_qty=current_qty,
            last_price=d.get("last_price"),
            signal_id=d.get("signal_id"),
            closed_by=d.get("closed_by"),
        )

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def append_order_event(
        self,
        *,
        session_id: str,
        position_id: str | None = None,
        signal_id: int | None = None,
        symbol: str = "",
        side: str = "",
        order_type: str = "MARKET",
        requested_qty: int = 0,
        request_price: float | None = None,
        fill_price: float | None = None,
        fill_qty: int = 0,
        status: str = "FILLED",
        requested_at: datetime | None = None,
        filled_at: datetime | None = None,
        exchange_order_id: str | None = None,
        idempotency_key: str | None = None,
        broker_mode: str | None = None,
        broker_payload: str | None = None,
        notes: str | None = None,
    ) -> str:
        oid = f"ord-{uuid.uuid4().hex[:8]}"
        now = _utcnow_iso()
        if idempotency_key:
            self.con.execute(
                """
                INSERT OR IGNORE INTO paper_orders (
                    order_id, session_id, position_id, signal_id, symbol, side,
                    order_type, requested_qty, request_price, fill_price, fill_qty,
                    status, requested_at, filled_at, exchange_order_id, idempotency_key,
                    broker_mode, broker_payload, notes,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    oid,
                    session_id,
                    position_id,
                    signal_id,
                    symbol,
                    side,
                    order_type,
                    requested_qty,
                    request_price,
                    fill_price,
                    fill_qty,
                    status,
                    requested_at or now,
                    filled_at,
                    exchange_order_id,
                    idempotency_key,
                    broker_mode,
                    broker_payload,
                    notes,
                    now,
                    now,
                ],
            )
            existing = self.con.execute(
                "SELECT order_id FROM paper_orders WHERE idempotency_key = ? LIMIT 1",
                [idempotency_key],
            ).fetchone()
            if existing and existing[0]:
                self._after_write()
                return str(existing[0])

        self.con.execute(
            """
            INSERT INTO paper_orders (
                order_id, session_id, position_id, signal_id, symbol, side,
                order_type, requested_qty, request_price, fill_price, fill_qty,
                status, requested_at, filled_at, exchange_order_id, idempotency_key,
                broker_mode, broker_payload, notes,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                oid,
                session_id,
                position_id,
                signal_id,
                symbol,
                side,
                order_type,
                requested_qty,
                request_price,
                fill_price,
                fill_qty,
                status,
                requested_at or now,
                filled_at,
                exchange_order_id,
                idempotency_key,
                broker_mode,
                broker_payload,
                notes,
                now,
                now,
            ],
        )
        self._after_write()
        return oid

    def get_session_orders(self, session_id: str, symbol: str | None = None) -> list[PaperOrder]:
        where = ["session_id = ?"]
        params: list[Any] = [session_id]
        if symbol:
            where.append("symbol = ?")
            params.append(symbol)
        rows = self.con.execute(
            "SELECT order_id, session_id, position_id, signal_id, symbol, side, "
            "order_type, requested_qty, request_price, fill_price, fill_qty, "
            "status, requested_at, filled_at, exchange_order_id, idempotency_key, "
            "broker_mode, broker_payload, notes, "
            "created_at, updated_at "
            f"FROM paper_orders WHERE {' AND '.join(where)} ORDER BY created_at ASC",
            params,
        ).fetchall()
        return [self._row_to_order(r) for r in rows]

    def _row_to_order(self, row: tuple) -> PaperOrder:
        cols = [
            "order_id",
            "session_id",
            "position_id",
            "signal_id",
            "symbol",
            "side",
            "order_type",
            "requested_qty",
            "request_price",
            "fill_price",
            "fill_qty",
            "status",
            "requested_at",
            "filled_at",
            "exchange_order_id",
            "idempotency_key",
            "broker_mode",
            "broker_payload",
            "notes",
            "created_at",
            "updated_at",
        ]
        d = dict(zip(cols, row, strict=True))
        return PaperOrder(
            order_id=d.get("order_id", ""),
            session_id=d.get("session_id", ""),
            position_id=d.get("position_id"),
            signal_id=d.get("signal_id"),
            symbol=d.get("symbol", ""),
            side=d.get("side", ""),
            order_type=d.get("order_type", "MARKET"),
            requested_qty=d.get("requested_qty", 0) or 0,
            request_price=d.get("request_price"),
            fill_price=d.get("fill_price"),
            fill_qty=d.get("fill_qty", 0) or 0,
            status=d.get("status", "FILLED"),
            requested_at=d.get("requested_at"),
            filled_at=d.get("filled_at"),
            exchange_order_id=d.get("exchange_order_id"),
            idempotency_key=d.get("idempotency_key"),
            broker_mode=d.get("broker_mode"),
            broker_payload=d.get("broker_payload"),
            notes=d.get("notes"),
            created_at=d.get("created_at", _utcnow()),
            updated_at=d.get("updated_at", _utcnow()),
        )

    # ------------------------------------------------------------------
    # Feed state
    # ------------------------------------------------------------------

    def upsert_feed_state(
        self,
        *,
        session_id: str,
        status: str = "IDLE",
        last_event_ts: datetime | None = None,
        last_bar_ts: datetime | None = None,
        last_price: float | None = None,
        stale_reason: str | None = None,
        raw_state: dict | None = None,
    ) -> None:
        now = _utcnow_iso()
        self.con.execute(
            """
            INSERT INTO paper_feed_state (
                session_id, status, last_event_ts, last_bar_ts, last_price,
                stale_reason, raw_state, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (session_id) DO UPDATE SET
                status = EXCLUDED.status,
                last_event_ts = EXCLUDED.last_event_ts,
                last_bar_ts = EXCLUDED.last_bar_ts,
                last_price = EXCLUDED.last_price,
                stale_reason = EXCLUDED.stale_reason,
                raw_state = EXCLUDED.raw_state,
                updated_at = EXCLUDED.updated_at
            """,
            [
                session_id,
                status,
                last_event_ts,
                last_bar_ts,
                last_price,
                stale_reason,
                json.dumps(raw_state or {}),
                now,
            ],
        )
        self._after_write()

    def upsert_feed_audit_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        columns = [
            "session_id",
            "trade_date",
            "feed_source",
            "transport",
            "symbol",
            "bar_start",
            "bar_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "first_snapshot_ts",
            "last_snapshot_ts",
        ]
        df = pl.DataFrame(rows).select(columns)
        self.con.register("_tmp_paper_feed_audit", df.to_arrow())
        try:
            self.con.execute("BEGIN TRANSACTION")
            self.con.execute(
                f"""
                INSERT OR REPLACE INTO paper_feed_audit (
                    {", ".join(columns)}
                )
                SELECT {", ".join(columns)} FROM _tmp_paper_feed_audit
                """
            )
            self.con.execute("COMMIT")
            self._after_write()
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        finally:
            try:
                self.con.unregister("_tmp_paper_feed_audit")
            except Exception:
                pass
        return len(rows)

    def get_feed_audit_rows(
        self,
        *,
        trade_date: str | None = None,
        session_id: str | None = None,
        feed_source: str | None = None,
    ) -> list[FeedAudit]:
        where: list[str] = []
        params: list[Any] = []
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(trade_date)
        if session_id is not None:
            where.append("session_id = ?")
            params.append(session_id)
        if feed_source is not None:
            where.append("feed_source = ?")
            params.append(feed_source)
        sql = (
            "SELECT session_id, trade_date, feed_source, transport, symbol, bar_start, bar_end, "
            "open, high, low, close, volume, first_snapshot_ts, last_snapshot_ts, created_at, updated_at "
            "FROM paper_feed_audit"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY session_id, bar_end, symbol"
        rows = self.con.execute(sql, params).fetchall()
        result: list[FeedAudit] = []
        for row in rows:
            result.append(
                FeedAudit(
                    session_id=str(row[0] or ""),
                    trade_date=str(row[1] or ""),
                    feed_source=str(row[2] or ""),
                    transport=str(row[3] or ""),
                    symbol=str(row[4] or ""),
                    bar_start=row[5],
                    bar_end=row[6],
                    open=float(row[7] or 0.0),
                    high=float(row[8] or 0.0),
                    low=float(row[9] or 0.0),
                    close=float(row[10] or 0.0),
                    volume=float(row[11] or 0.0),
                    first_snapshot_ts=row[12],
                    last_snapshot_ts=row[13],
                    created_at=row[14] or _utcnow(),
                    updated_at=row[15] or _utcnow(),
                )
            )
        return result

    def cleanup_feed_audit_older_than(self, retention_days: int) -> int:
        """Delete feed audit rows older than the retention window.

        The audit table is only meant to preserve a short rolling trail for
        parity investigation. We keep it bounded so the live paper DB does not
        grow indefinitely.
        """

        retention_days = int(retention_days or 0)
        if retention_days <= 0:
            return 0

        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        cutoff_expr = """
            COALESCE(
                bar_end,
                last_snapshot_ts,
                first_snapshot_ts,
                CAST(trade_date AS TIMESTAMPTZ),
                created_at
            )
        """
        count_row = self.con.execute(
            f"SELECT COUNT(*) FROM paper_feed_audit WHERE {cutoff_expr} < ?",
            [cutoff],
        ).fetchone()
        deleted = int(count_row[0] or 0) if count_row else 0
        if deleted == 0:
            return 0

        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(f"DELETE FROM paper_feed_audit WHERE {cutoff_expr} < ?", [cutoff])
            self.con.execute("COMMIT")
            self._after_write()
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return deleted

    def cleanup_alert_log_older_than(self, retention_days: int) -> int:
        """Delete alert_log rows older than the retention window."""

        retention_days = int(retention_days or 0)
        if retention_days <= 0:
            return 0

        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        count_row = self.con.execute(
            "SELECT COUNT(*) FROM alert_log WHERE created_at < ?",
            [cutoff],
        ).fetchone()
        deleted = int(count_row[0] or 0) if count_row else 0
        if deleted == 0:
            return 0

        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute("DELETE FROM alert_log WHERE created_at < ?", [cutoff])
            self.con.execute("COMMIT")
            self._after_write()
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return deleted

    def get_feed_state(self, session_id: str) -> FeedState | None:
        row = self.con.execute(
            "SELECT * FROM paper_feed_state WHERE session_id = ?", [session_id]
        ).fetchone()
        if not row:
            return None
        cols = [
            "session_id",
            "status",
            "last_event_ts",
            "last_bar_ts",
            "last_price",
            "stale_reason",
            "raw_state",
            "updated_at",
        ]
        d = dict(zip(cols, row, strict=True))
        return FeedState(
            session_id=d.get("session_id", ""),
            status=d.get("status", "IDLE"),
            last_event_ts=d.get("last_event_ts"),
            last_bar_ts=d.get("last_bar_ts"),
            last_price=d.get("last_price"),
            stale_reason=d.get("stale_reason"),
            raw_state=_loads_json(d.get("raw_state"), {}),
            updated_at=d.get("updated_at", _utcnow()),
        )

    # ------------------------------------------------------------------
    # Alert log
    # ------------------------------------------------------------------

    def log_alert(
        self,
        alert_type: str,
        subject: str,
        body: str = "",
        *,
        alert_level: str = "INFO",
        channel: str = "BOTH",
        status: str = "queued",
        error_msg: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO alert_log (
                alert_type, alert_level, subject, body,
                channel, status, error_msg, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [alert_type, alert_level, subject, body, channel, status, error_msg],
        )
        self._after_write()

    def get_alerts(
        self,
        *,
        since_id: int = 0,
        limit: int = 100,
        alert_type: str | None = None,
    ) -> list[dict]:
        where = ["id > ?"]
        params: list[Any] = [since_id]
        if alert_type:
            where.append("alert_type = ?")
            params.append(alert_type)
        params.append(limit)
        rows = self.con.execute(
            f"SELECT id, alert_type, alert_level, subject, body, channel, "
            f"status, error_msg, created_at FROM alert_log "
            f"WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [
            {
                "id": r[0],
                "alert_type": r[1],
                "alert_level": r[2],
                "subject": r[3],
                "body": r[4],
                "channel": r[5],
                "status": r[6],
                "error_msg": r[7],
                "created_at": str(r[8]) if r[8] else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, int]:
        tables = [
            "paper_sessions",
            "paper_positions",
            "paper_orders",
            "paper_feed_state",
            "paper_feed_audit",
            "alert_log",
        ]
        result: dict[str, int] = {}
        try:
            union = "\nUNION ALL\n".join(
                f"SELECT '{t}' AS tn, COUNT(*) AS rc FROM {t}" for t in tables
            )
            rows = self.con.execute(union).fetchall()
            for r in rows:
                result[str(r[0])] = int(r[1] or 0)
        except Exception as e:
            logger.debug("Paper status query failed: %s", e)
        return result

    def delete_all_rows(self) -> dict[str, int]:
        """Delete every paper-session row from paper.duckdb.

        This keeps the dashboard replica in sync because the operation goes
        through the normal writer connection and ends with a forced replica
        publication.
        """
        counts = self.get_status()
        tables = [
            "paper_positions",
            "paper_orders",
            "paper_feed_state",
            "paper_feed_audit",
            "paper_sessions",
            "alert_log",
        ]
        self.con.execute("BEGIN TRANSACTION")
        try:
            for table in tables:
                if table != "alert_log":
                    self.con.execute(f"DELETE FROM {table}")
            self.con.execute("DROP TABLE IF EXISTS alert_log")
            self.con.execute("DROP SEQUENCE IF EXISTS alert_log_seq")
            self.con.execute("CREATE SEQUENCE alert_log_seq START 1")
            self.con.execute("""
                CREATE TABLE alert_log (
                    id             BIGINT PRIMARY KEY DEFAULT nextval('alert_log_seq'),
                    alert_type     VARCHAR(50) NOT NULL,
                    alert_level    VARCHAR(10) NOT NULL DEFAULT 'INFO'
                                    CHECK (alert_level IN ('INFO','WARN','ERROR','CRITICAL')),
                    subject        VARCHAR(200),
                    body           TEXT,
                    channel        VARCHAR(20) NOT NULL DEFAULT 'BOTH'
                                    CHECK (channel IN ('TELEGRAM','EMAIL','BOTH','LOG')),
                    status         VARCHAR(20) DEFAULT 'queued'
                                    CHECK (status IN ('sent','failed','queued')),
                    error_msg      TEXT,
                    created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.con.execute("COMMIT")
            if self._sync is not None:
                self._sync.mark_dirty()
                self._sync.force_sync(self.con)
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return counts

    def delete_sessions_by_trade_date(self, trade_date: str) -> dict[str, int]:
        """Delete paper-session rows for a specific trade date.

        Filters paper_sessions by trade_date, then cascades to positions,
        orders, feed_state, and alert_log. Returns row counts before deletion.
        """
        session_ids_rows = self.con.execute(
            "SELECT session_id FROM paper_sessions WHERE trade_date = ?",
            [trade_date],
        ).fetchall()
        session_ids = [str(r[0]) for r in session_ids_rows if r and r[0]]
        if not session_ids:
            return {
                "paper_sessions": 0,
                "paper_positions": 0,
                "paper_orders": 0,
                "paper_feed_state": 0,
                "paper_feed_audit": 0,
                "alert_log": 0,
                "matched_sessions": 0,
            }

        placeholders = ", ".join("?" for _ in session_ids)
        counts: dict[str, int] = {"matched_sessions": len(session_ids)}
        self.con.execute("BEGIN TRANSACTION")
        try:
            # alert_log has no session_id FK — skip it in the cascade
            for table in (
                "paper_positions",
                "paper_orders",
                "paper_feed_state",
                "paper_feed_audit",
            ):
                row = self.con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE session_id IN ({placeholders})",
                    session_ids,
                ).fetchone()
                counts[table] = int(row[0] or 0) if row else 0
                self.con.execute(
                    f"DELETE FROM {table} WHERE session_id IN ({placeholders})",
                    session_ids,
                )
            counts["alert_log"] = 0
            row = self.con.execute(
                "SELECT COUNT(*) FROM paper_sessions WHERE trade_date = ?",
                [trade_date],
            ).fetchone()
            counts["paper_sessions"] = int(row[0] or 0) if row else 0
            self.con.execute("DELETE FROM paper_sessions WHERE trade_date = ?", [trade_date])
            self.con.execute("COMMIT")
            if self._sync is not None:
                self._sync.mark_dirty()
                self._sync.force_sync(self.con)
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return counts

    def cleanup_stale_sessions(self) -> int:
        """Mark abandoned STOPPING/PLANNING sessions as CANCELLED on startup."""
        stale_planning_cutoff = "CURRENT_TIMESTAMP - INTERVAL '1 day'"
        count = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM paper_sessions
                WHERE (
                    status = 'STOPPING'
                    AND updated_at < CURRENT_TIMESTAMP - INTERVAL '15 minutes'
                ) OR (
                    status = 'PLANNING'
                    AND updated_at < """
                + stale_planning_cutoff
                + """
                )
                """
            ).fetchone()[0]
            or 0
        )
        if not count:
            return 0
        self.con.execute(
            """
            UPDATE paper_sessions
            SET status = 'CANCELLED',
                notes = 'auto-cancelled: stale session from previous run',
                updated_at = CURRENT_TIMESTAMP
            WHERE (status = 'STOPPING' AND updated_at < CURRENT_TIMESTAMP - INTERVAL '15 minutes')
               OR (status = 'PLANNING' AND updated_at < """
            + stale_planning_cutoff
            + """)
        """
        )
        self._after_write()
        return count

    def execute_sql(
        self, query: str, params: list | dict | None = None
    ) -> duckdb.DuckDBPyConnection:
        if params:
            return self.con.execute(query, params)
        return self.con.execute(query)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception as e:
            logger.debug("PaperDB close ignored: %s", e)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Module-level singleton (thread-safe)
# ---------------------------------------------------------------------------
_paper_db: PaperDB | None = None
_dashboard_paper_db: PaperDB | None = None
_dashboard_paper_consumer: ReplicaConsumer | None = None
_paper_db_lock = threading.Lock()
_dashboard_paper_lock = threading.Lock()
_paper_db_atexit = False
_dashboard_paper_atexit = False


def get_paper_db() -> PaperDB:
    """Return the global PaperDB instance (creates on first call)."""
    global _paper_db, _paper_db_atexit
    if _paper_db is None:
        with _paper_db_lock:
            if _paper_db is None:
                replica_dir = REPLICA_DIR
                replica_dir.mkdir(parents=True, exist_ok=True)
                sync = ReplicaSync(PAPER_DUCKDB_FILE, replica_dir, min_interval_sec=2.0)
                _paper_db = PaperDB(replica_sync=sync)
                if not _paper_db_atexit:
                    atexit.register(close_paper_db)
                    _paper_db_atexit = True
    return _paper_db


def get_dashboard_paper_db() -> PaperDB:
    """Return a read-only PaperDB instance backed by the latest replica.

    Raises RuntimeError if no replica exists — the dashboard must never
    open the live paper.duckdb directly.
    """
    global _dashboard_paper_db, _dashboard_paper_consumer, _dashboard_paper_atexit
    REPLICA_DIR.mkdir(parents=True, exist_ok=True)
    _recover_deferred_sync_if_needed()
    if _dashboard_paper_consumer is None:
        _dashboard_paper_consumer = ReplicaConsumer(REPLICA_DIR, PAPER_DUCKDB_FILE.stem)
    replica_path = _dashboard_paper_consumer.get_replica_path()
    if _dashboard_paper_db is None:
        with _dashboard_paper_lock:
            if _dashboard_paper_db is None:
                if replica_path is None:
                    raise RuntimeError(
                        f"No paper replica found in {REPLICA_DIR}. "
                        "Run a paper trading session to create one."
                    )
                _dashboard_paper_db = PaperDB(
                    db_path=replica_path,
                    read_only=True,
                )
                if not _dashboard_paper_atexit:
                    atexit.register(close_dashboard_paper_db)
                    _dashboard_paper_atexit = True
    elif replica_path is not None and _dashboard_paper_db.db_path != replica_path:
        with _dashboard_paper_lock:
            if _dashboard_paper_db is not None and _dashboard_paper_db.db_path != replica_path:
                _dashboard_paper_db.close()
                _dashboard_paper_db = PaperDB(db_path=replica_path, read_only=True)
    return _dashboard_paper_db


def close_paper_db() -> None:
    global _paper_db
    if _paper_db is not None:
        _paper_db.close()
        _paper_db = None


def close_dashboard_paper_db() -> None:
    global _dashboard_paper_db
    if _dashboard_paper_db is not None:
        _dashboard_paper_db.close()
        _dashboard_paper_db = None
