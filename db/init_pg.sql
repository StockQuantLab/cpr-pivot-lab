-- CPR Pivot Lab — PostgreSQL Schema
-- Operational data ONLY: sessions, signals, alerts
-- Market data lives in DuckDB + Parquet

CREATE SCHEMA IF NOT EXISTS cpr_pivot;

SET search_path TO cpr_pivot, public;

-- ─── Phidata agent sessions ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id   VARCHAR(64)  PRIMARY KEY,
    agent_id     VARCHAR(64),
    user_data    JSONB,
    agent_data   JSONB,
    session_data JSONB,
    memory       JSONB,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id          BIGSERIAL    PRIMARY KEY,
    session_id  VARCHAR(64)  NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    role        VARCHAR(20)  NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content     TEXT,
    tool_name   VARCHAR(100),
    tool_input  JSONB,
    tool_output TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_msgs_session ON agent_messages(session_id, created_at DESC);

-- ─── Real-time trading signals ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signals (
    id            BIGSERIAL    PRIMARY KEY,
    symbol        VARCHAR(20)  NOT NULL,
    signal_type   VARCHAR(10)  NOT NULL CHECK (signal_type IN ('BUY', 'SELL')),
    session_id    VARCHAR(64),
    strategy      VARCHAR(20),
    direction     VARCHAR(6),
    signal_key    VARCHAR(64),
    source_type   VARCHAR(30),
    source_id     VARCHAR(100),
    trigger_price NUMERIC(12, 4) NOT NULL,
    current_price NUMERIC(12, 4),
    entry_price   NUMERIC(12, 4),
    exit_price    NUMERIC(12, 4),
    stop_loss     NUMERIC(12, 4),
    profit_loss   NUMERIC(12, 4),
    is_active     BOOLEAN      DEFAULT TRUE,
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    closed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_signals_session ON signals(session_id, is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_active  ON signals(is_active, symbol);
CREATE INDEX IF NOT EXISTS idx_signals_symbol  ON signals(symbol, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_session_key
ON signals(session_id, signal_key);

-- ─── Email alert log ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS alert_log (
    id          BIGSERIAL   PRIMARY KEY,
    alert_type  VARCHAR(50) NOT NULL,
    subject     VARCHAR(200),
    recipient   VARCHAR(200),
    status      VARCHAR(20) DEFAULT 'sent' CHECK (status IN ('sent', 'failed', 'queued')),
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_type ON alert_log(alert_type, created_at DESC);

-- ─── Paper trading mutable state ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS paper_trading_sessions (
    session_id            VARCHAR(64) PRIMARY KEY,
    name                  VARCHAR(200),
    strategy              VARCHAR(32) NOT NULL DEFAULT 'CPR_LEVELS',
    symbols               JSONB       DEFAULT '[]'::jsonb,
    strategy_params       JSONB       DEFAULT '{}'::jsonb,
    status                VARCHAR(20) NOT NULL DEFAULT 'PLANNING'
        CHECK (status IN ('PLANNING', 'ACTIVE', 'PAUSED', 'STOPPING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    mode                  VARCHAR(20) NOT NULL DEFAULT 'replay'
        CHECK (mode IN ('replay', 'live', 'manual')),
    created_by            VARCHAR(64),
    flatten_time          TIME,
    stale_feed_timeout_sec INTEGER      NOT NULL DEFAULT 120,
    max_daily_loss_pct     NUMERIC(8,4) NOT NULL DEFAULT 0.03,
    max_drawdown_pct       NUMERIC(8,4) NOT NULL DEFAULT 0.10,
    max_positions         INTEGER      NOT NULL DEFAULT 10,
    max_position_pct      NUMERIC(8,4) NOT NULL DEFAULT 0.10,
    daily_pnl_used        NUMERIC(20,4) DEFAULT 0,
    latest_candle_ts      TIMESTAMPTZ,
    stale_feed_at         TIMESTAMPTZ,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW(),
    started_at            TIMESTAMPTZ,
    ended_at              TIMESTAMPTZ,
    notes                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_paper_sessions_status
ON paper_trading_sessions(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS paper_positions (
    position_id   BIGSERIAL PRIMARY KEY,
    session_id    VARCHAR(64) NOT NULL,
    symbol        VARCHAR(20) NOT NULL,
    direction     VARCHAR(6)  NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    status        VARCHAR(20) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED')),
    quantity      NUMERIC(18,4) NOT NULL,
    entry_price   NUMERIC(12,4) NOT NULL,
    stop_loss     NUMERIC(12,4),
    target_price  NUMERIC(12,4),
    trail_state   JSONB       DEFAULT '{}'::jsonb,
    opened_at     TIMESTAMPTZ NOT NULL,
    closed_at     TIMESTAMPTZ,
    close_price   NUMERIC(12,4),
    realized_pnl  NUMERIC(20,4),
    signal_id     BIGINT,
    current_qty   NUMERIC(18,4),
    last_price    NUMERIC(12,4),
    opened_by     VARCHAR(100),
    closed_by     VARCHAR(100),
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (session_id) REFERENCES paper_trading_sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (signal_id)  REFERENCES signals(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_positions_session
ON paper_positions(session_id, status, symbol);
CREATE INDEX IF NOT EXISTS idx_paper_positions_symbol
ON paper_positions(symbol, status);

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id         BIGSERIAL PRIMARY KEY,
    session_id       VARCHAR(64) NOT NULL,
    position_id      BIGINT,
    signal_id        BIGINT,
    symbol           VARCHAR(20) NOT NULL,
    side             VARCHAR(10) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    order_type       VARCHAR(12) NOT NULL DEFAULT 'MARKET',
    requested_qty    NUMERIC(18,4) NOT NULL,
    request_price    NUMERIC(12,4),
    fill_qty         NUMERIC(18,4),
    fill_price       NUMERIC(12,4),
    status           VARCHAR(20) NOT NULL DEFAULT 'NEW' CHECK (
        status IN ('NEW', 'SUBMITTED', 'PARTIAL', 'FILLED', 'REJECTED', 'CANCELLED')
    ),
    exchange_order_id VARCHAR(128),
    requested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filled_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    notes            TEXT,
    FOREIGN KEY (session_id)  REFERENCES paper_trading_sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (position_id) REFERENCES paper_positions(position_id) ON DELETE SET NULL,
    FOREIGN KEY (signal_id)   REFERENCES signals(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_orders_session
ON paper_orders(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_paper_orders_position
ON paper_orders(position_id, status);
CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol
ON paper_orders(symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS paper_feed_state (
    session_id      VARCHAR(64) PRIMARY KEY,
    status          VARCHAR(20) NOT NULL DEFAULT 'OK'
        CHECK (status IN ('OK', 'STALE', 'DISCONNECTED', 'PAUSED')),
    last_event_ts   TIMESTAMPTZ,
    last_bar_ts     TIMESTAMPTZ,
    last_price      NUMERIC(12,4),
    stale_reason    TEXT,
    raw_state       JSONB       DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (session_id) REFERENCES paper_trading_sessions(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_paper_feed_state_status
ON paper_feed_state(status, updated_at DESC);

-- ─── Idempotent migrations for existing installs ──────────────────────────

ALTER TABLE paper_trading_sessions
    ADD COLUMN IF NOT EXISTS mode VARCHAR(20) NOT NULL DEFAULT 'replay';

ALTER TABLE paper_trading_sessions
    ADD COLUMN IF NOT EXISTS max_drawdown_pct NUMERIC(8,4) NOT NULL DEFAULT 0.10;
