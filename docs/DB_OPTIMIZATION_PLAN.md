# Database Optimization Plan

**Status:** Draft  
**Created:** 2026-05-04  
**Scope:** market data, runtime setup tables, backtests, paper/live sessions, real-order audit, logs, dashboard reads, and archival analytics  
**Primary objective:** keep the project on exactly two local database roles: one transactional database and one analytics database. No Docker, no Postgres, no server database, no extra service to operate.

---

## 1. Executive Summary

The project currently uses DuckDB for almost everything: market runtime tables, backtest results,
paper sessions, positions, orders, feed state, alert logs, and dashboard replicas. This worked well
while the system was mostly analytics and paper trading, but the recent live-paper and real-order
pilot work exposed a clear boundary:

- DuckDB is excellent for analytics, scans, backtests, slices, reports, and dashboard aggregates.
- DuckDB is a poor primary store for small, frequent, operational writes during live execution.
- SQLite WAL is a better local store for operational state when we want transactions without
  running a database server.

The recommended direction is not "move only broker_ledger to SQLite." The deeper answer is:

- keep **analytics and historical facts** in DuckDB
- move **mutable operational state and append-only event logs** to SQLite WAL
- periodically mirror/compact completed operational data into DuckDB for analytics

The target is deliberately simple:

```text
data/ops.sqlite       = the only transactional/operational database
data/analytics.duckdb = the only analytical/reporting database
Parquet files         = raw market source files, not an operational database
```

This keeps the setup local and simple while reducing DuckDB writer-lock risk during live sessions.

---

## 2. Design Principles

1. **No service dependency**
   - No Docker.
   - No Docker Compose.
   - No Postgres.
   - No server database.
   - A fresh checkout should run with local files only.

2. **Exactly two database roles**
   - SQLite WAL is the transaction DB.
   - DuckDB is the analytics DB.
   - Parquet remains raw/historical source files, not live operational state.

3. **Operational state must survive crashes**
   - Real order state, active sessions, positions, commands, and alerts must be committed quickly.
   - Writes should be small, idempotent, and immediately durable.

4. **Dashboard must not block live writers**
   - Dashboard reads should use read-only SQLite connections and DuckDB replicas/snapshots.
   - Dashboard write actions should enqueue commands, not directly mutate live state.

5. **DuckDB remains the reporting source**
   - Backtest and archived paper/live results stay queryable from DuckDB.
   - SQLite operational rows are mirrored into DuckDB only after completion or by explicit sync.

---

## 3. Docker/Postgres Removal Policy

This plan explicitly removes Docker and Postgres from the future CPR Pivot Lab database architecture.
This is the target state, not a claim that the current code has no Postgres callers.

Current Postgres dependency to retire:

| Area | Current code path | Migration target |
|---|---|---|
| Dashboard paper/live reads | `web/state.py` imports `db.postgres` | route to DuckDB legacy reads first, then SQLite |
| Agent paper tools | `agent/tools/backtest_tools.py` imports `db.postgres` | route to DuckDB legacy reads first, then SQLite |
| Reset tooling | `scripts/reset_run_history.py` uses `get_db_session` | replace with DuckDB/SQLite cleanup paths |
| Schema bootstrap | `scripts/init_postgres.py` | delete after no callers remain |
| Phidata storage | `agent/llm_agent.py` uses `PgAgentStorage` | replace with local file/SQLite-backed storage or remove persistence |
| Tests/fixtures | `tests/test_postgres_repo.py`, feed-state fixtures | replace with SQLite/DuckDB storage tests |

Migration rule:

- do not delete Postgres code first
- first remove dashboard/agent/runtime callers
- then remove Postgres bootstrap scripts, tests, docs, and Doppler references
- final acceptance is `rg "db.postgres|PgAgentStorage|POSTGRES|postgres"` showing only historical docs or no hits

Target state:

- no `docker compose` dependency for normal local development, dashboard use, paper trading, live paper trading, or broker audit
- no Postgres runtime dependency
- no Postgres migrations for the trading/dashboard path
- no Postgres connection settings required in Doppler for the local trading stack
- no new Docker service just to support broker audit, active sessions, agents, alerts, or command queues

Migration implication:

- existing Docker/Postgres docs and configuration should be retired from the operational runbooks once the SQLite/DuckDB migration is implemented
- existing compatibility code with Postgres naming should be renamed, isolated, or removed when it no longer has callers
- future agent features, if added later, must use `data/ops.sqlite` for transactional state and `data/analytics.duckdb` for reporting, not a third database

Reason:

The project needs predictable local operation during market hours. Docker/Postgres adds another
service lifecycle, another backup path, another credential surface, and another failure mode. SQLite
WAL plus DuckDB is enough for the current scale and keeps the system simple.

---

## 4. Current Storage Inventory

| Area | Current store | Current fit | Issue |
|---|---:|---|---|
| Raw daily/5-min market data | Parquet | Good | Keep |
| Runtime setup tables | DuckDB | Good | Mostly analytical/precomputed |
| Backtest results | DuckDB | Good | Keep |
| Baseline registry metadata | YAML + DuckDB | Good | Keep |
| Paper sessions | DuckDB | Mixed | Active lifecycle is operational, archive is analytical |
| Paper positions | DuckDB | Mixed | Open positions are operational; closed trades are analytical |
| Paper orders | DuckDB | Weak for real orders | Needs broker lifecycle, status sync, audit trail |
| Paper feed state | DuckDB | Weak | High-frequency mutable heartbeat/state |
| Paper feed audit | DuckDB | Weak | High-frequency bar audit writes; should be operational while hot |
| Alert log | DuckDB | Mixed | Audit/event log is operational; reporting can mirror |
| Admin commands | Files | Acceptable short-term | Needs visibility, expiry, idempotency |
| Real-order broker events | DuckDB draft | Weak | Should be append-only operational ledger |
| Dashboard reads | DuckDB replica | Good for analytics | Operational pages need SQLite read path |

---

## 5. Current Actual Files And Table Counts

Snapshot taken on 2026-05-04 from this workspace.

### 5.1 Current Top-Level DuckDB Files

| File | Role today | Size | Target |
|---|---|---:|---|
| `data/market.duckdb` | market runtime/setup tables plus some legacy run tables | 7.25 GB | fold into `data/analytics.duckdb` |
| `data/backtest.duckdb` | backtest run storage | 688 MB | fold into `data/analytics.duckdb` |
| `data/paper.duckdb` | active paper/live sessions, orders, positions, alerts, feed state | 499 MB | migrate active state to `data/ops.sqlite`; archive to `data/analytics.duckdb` |

There are currently 10 DuckDB files under `data/` when replica DuckDB version files are included:

- 3 top-level DuckDB files
- 2 backtest replica DuckDB files
- 3 market replica DuckDB files
- 2 paper replica DuckDB files

There are also 3 replica pointer files:

- `data/backtest_replica/backtest_replica_latest`
- `data/market_replica/market_replica_latest`
- `data/paper_replica/paper_replica_latest`

Replica counts are dynamic. The dashboard DB catalog should compute current counts at runtime rather
than relying on this snapshot.

Current SQLite database count under `data/`: 0.

Current physical table/view count across the three top-level DuckDB files:

- 27 base-table instances
- 2 views
- 22 unique base-table names after accounting for duplicated run tables in `market.duckdb` and `backtest.duckdb`

### 5.2 Current DuckDB Tables And Views

| Database | Base tables | Views | Tables/views |
|---|---:|---:|---|
| `data/market.duckdb` | 16 | 2 | `atr_intraday`, `backtest_results`, `backtest_universe`, `cpr_daily`, `cpr_thresholds`, `data_quality_issues`, `dataset_meta`, `intraday_day_pack`, `market_day_state`, `or_daily`, `run_daily_pnl`, `run_metadata`, `run_metrics`, `setup_funnel`, `strategy_day_state`, `virgin_cpr_flags`, `v_5min`, `v_daily` |
| `data/backtest.duckdb` | 5 | 0 | `backtest_results`, `run_daily_pnl`, `run_metadata`, `run_metrics`, `setup_funnel` |
| `data/paper.duckdb` | 6 | 0 | `alert_log`, `paper_feed_audit`, `paper_feed_state`, `paper_orders`, `paper_positions`, `paper_sessions` |

The market compatibility views `v_5min` and `v_daily` are read-model conveniences, not a separate
storage role. Older or freshly rebuilt DBs may differ, so the dashboard catalog must detect views
dynamically.

### 5.3 Target File Count

Final target:

```text
data/ops.sqlite       = one transaction DB
data/analytics.duckdb = one analytics DB
```

No Postgres database. No Docker-managed database.

Replica fate:

- paper DuckDB replicas should be retired after active paper/live state moves to SQLite
- analytics DuckDB may keep a dashboard-safe read replica/snapshot while long backtests or builds hold the writer
- replicas are implementation detail only, not another logical database role

---

## 6. Recommended Target Architecture

### 6.1 Keep Parquet For Raw Source Data

Use Parquet for immutable/raw market history:

- `data/raw`, daily candles, 5-minute candles
- Kite ingestion outputs
- long-term source-of-truth market data

Reason: columnar files are ideal for raw historical market data and are easy to rebuild/validate.

### 6.2 Consolidate DuckDB Into One Analytics Database

Target file:

```text
data/analytics.duckdb
```

This should eventually replace the current split between market and backtest DuckDB files. During
migration, existing files can remain, but the target architecture is one analytics database.

Cutover decision:

- use `data/market.duckdb` as the starting point because it contains the large runtime tables
- rename/copy it to `data/analytics.duckdb` during a maintenance window and update connection strings
- attach/copy the authoritative `data/backtest.duckdb` run tables into `data/analytics.duckdb`
- do not rebuild `intraday_day_pack` just to rename the analytics database
- do not run `pivot-build --table pack --force` or any full pack rebuild as part of this migration
- only choose a full rebuild if integrity checks fail, because that needs extra disk and a larger maintenance window
- retire duplicate legacy run tables already present in `market.duckdb`; do not migrate stale duplicates over authoritative `backtest.duckdb` rows

Analytical DuckDB should own:

- runtime setup tables
  - `cpr_daily`
  - `cpr_thresholds`
  - `intraday_day_pack`
  - `or_daily`
  - `market_day_state`
  - `strategy_day_state`
  - data-quality aggregate tables
- backtest results
- baseline runs
- archived paper/live trade rows
- archived broker-order summaries
- dashboard analytical summaries

Reason: these are scan-heavy, aggregate-heavy, and mostly batch-updated.

### 6.3 Add SQLite WAL For Operational State

Create:

```text
data/ops.sqlite
```

Enable:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
```

SQLite WAL should own:

- active paper/live sessions
- open positions
- order intents
- broker order events
- broker order status snapshots
- feed state / heartbeat
- feed audit bars while hot
- alert dispatch log
- admin command queue
- process/supervisor heartbeats

Reason: these are small operational writes where transactional correctness matters more than analytical scan speed.

SQLite write rule:

- every operational write should use short explicit transactions
- `--multi` sessions may contend for the single SQLite writer at candle close; this is expected
- keep `busy_timeout = 5000` initially, then measure 4-session candle-close bursts before Phase 3
- if contention appears in logs, increase timeout and batch same-session writes into one transaction
- retain a per-process write lock around SQLite writes, similar to the current paper DB write lock
- treat SQLite `busy_timeout` as the cross-process contention safety net, not as the main in-process serialization model
- dashboard command writes and live-session writes must still go through short transactions and explicit command queues

---

## 7. Why Not Only `broker_ledger.sqlite`?

Moving only broker order rows to SQLite helps, but it leaves the same class of problem in adjacent tables:

1. **Paper sessions are operational while active**
   - status changes: `PLANNING -> ACTIVE -> STOPPING -> COMPLETED/FAILED`
   - risk budget updates
   - feed stale state
   - finalization and recovery markers

2. **Open positions are operational**
   - trail state changes
   - partial exits
   - manual close
   - stale/flatten recovery
   - current quantity and mark price

3. **Order rows are coupled to positions and sessions**
   - a real order belongs to a session and possibly a position
   - reconciliation needs to compare broker order state to local session/position state atomically

4. **Alert logs are part of operational evidence**
   - whether `TRADE_OPENED`, `TRADE_CLOSED`, `FLATTEN_EOD`, and `SESSION_COMPLETED` were sent
   - retries and failure reasons
   - dedupe keys

5. **Admin commands are operational**
   - command received
   - command processed
   - command failed
   - command expired

If only broker orders move to SQLite while sessions/positions remain in DuckDB, reconciliation must constantly cross two transactional boundaries. That creates ambiguity exactly where we need clarity.

Therefore the better boundary is:

```text
SQLite WAL = active operational truth
DuckDB     = historical analytics and archived reporting truth
```

---

## 8. Proposed SQLite Schema

Timestamp convention:

- store all timestamps as ISO 8601 TEXT with timezone, for example `2026-05-04T09:20:24.123+05:30`
- do not mix `strftime` formats and `datetime.isoformat()` output without timezone
- text comparisons and dashboard filters depend on this consistency

SQLite JSON policy:

- SQLite JSON support is available, but JSON must be used as an audit/detail field, not as the primary query model.
- Core operational fields must be first-class columns when they are filtered, joined, sorted, reconciled, or displayed in dashboard tables.
- JSON columns must be `TEXT` with `CHECK (json_valid(column))` for required JSON or `CHECK (column IS NULL OR json_valid(column))` for optional JSON.
- Dashboard v1 filters must not depend on JSON path queries.
- Do not store only a raw JSON blob when the code needs stable columns such as `session_id`, `symbol`, `status`, `side`, `quantity`, `price`, `latency_ms`, or `created_at`.

Use JSON for:

- raw broker request/response payloads
- Kite orderbook rows and status snapshots
- strategy parameter snapshots
- admin command payload details
- alert metadata and provider responses
- error context and exception details

Do not use JSON for:

- order status
- position quantity
- filled quantity
- average price
- PnL
- session status
- timestamps used by filters
- symbol/session/date fields used by dashboard queries

### 8.1 `ops_sessions`

One row per active or recent live/paper session.

Key columns:

- `session_id TEXT PRIMARY KEY`
- `strategy TEXT NOT NULL`
- `direction TEXT`
- `trade_date TEXT`
- `mode TEXT`
- `feed_source TEXT`
- `broker_mode TEXT` (`PAPER`, `REAL_DRY_RUN`, `LIVE`)
- `status TEXT`
- `status_reason TEXT`
- `session_storage_version INTEGER NOT NULL DEFAULT 2`
- `portfolio_value REAL`
- `max_positions INTEGER`
- `max_position_pct REAL`
- `scheduled_flatten_time TEXT`
- `flattened_at TEXT`
- `strategy_params_json TEXT CHECK (strategy_params_json IS NULL OR json_valid(strategy_params_json))`
- `started_at TEXT`
- `ended_at TEXT`
- `created_at TEXT`
- `updated_at TEXT`

Indexes:

- `(status, trade_date)`
- `(trade_date, strategy, direction)`

### 8.2 `ops_positions`

One row per local strategy position.

Key columns:

- `position_id TEXT PRIMARY KEY`
- `session_id TEXT NOT NULL REFERENCES ops_sessions(session_id)`
- `symbol TEXT NOT NULL`
- `direction TEXT NOT NULL`
- `status TEXT NOT NULL`
- `entry_time TEXT`
- `entry_price REAL`
- `quantity INTEGER NOT NULL`
- `current_qty INTEGER NOT NULL`
- `stop_loss REAL`
- `target_price REAL`
- `last_price REAL`
- `realized_pnl REAL`
- `unrealized_pnl REAL`
- `exit_time TEXT`
- `exit_price REAL`
- `exit_reason TEXT`
- `trail_state_json TEXT CHECK (trail_state_json IS NULL OR json_valid(trail_state_json))`
- `created_at TEXT`
- `updated_at TEXT`

Indexes:

- `(session_id, status)`
- `(session_id, symbol, status)`

### 8.3 `ops_order_intents`

One row per intended broker/paper order.

Key columns:

- `intent_id TEXT PRIMARY KEY`
- `idempotency_key TEXT UNIQUE NOT NULL`
- `session_id TEXT NOT NULL`
- `position_id TEXT`
- `symbol TEXT NOT NULL`
- `side TEXT NOT NULL`
- `quantity INTEGER NOT NULL`
- `product TEXT`
- `order_type TEXT`
- `price REAL`
- `trigger_price REAL`
- `reference_price REAL`
- `reference_price_age_sec REAL`
- `role TEXT`
- `requested_at TEXT`
- `payload_json TEXT NOT NULL CHECK (json_valid(payload_json))`
- `status TEXT` (`CREATED`, `SUBMITTED`, `ACKED`, `REJECTED`, `CANCELLED`, `FILLED`, `FAILED`)
- `created_at TEXT`
- `updated_at TEXT`

Indexes:

- `(session_id, requested_at)`
- `(symbol, requested_at)`
- `(status, requested_at)`

### 8.4 `ops_broker_events`

Append-only broker event ledger. Do not update rows in this table.

Key columns:

- `event_id TEXT PRIMARY KEY`
- `intent_id TEXT`
- `session_id TEXT`
- `broker TEXT` (`ZERODHA`)
- `broker_order_id TEXT`
- `exchange_order_id TEXT`
- `event_type TEXT`
  - `PLACE_REQUEST`
  - `PLACE_RESPONSE`
  - `ORDERBOOK_SYNC`
  - `POSITION_SYNC`
  - `CANCEL_REQUEST`
  - `CANCEL_RESPONSE`
  - `ERROR`
- `broker_status TEXT`
- `filled_quantity INTEGER`
- `average_price REAL`
- `status_message TEXT`
- `latency_ms REAL`
- `event_time TEXT`
- `payload_json TEXT CHECK (payload_json IS NULL OR json_valid(payload_json))`
- `created_at TEXT`

Indexes:

- `(intent_id, event_time)`
- `(broker_order_id, event_time)`
- `(session_id, event_time)`
- `(event_type, event_time)`

### 8.5 `ops_order_current`

Materialized/current order state, updated from `ops_broker_events`.

Key columns:

- `intent_id TEXT PRIMARY KEY`
- `broker_order_id TEXT`
- `exchange_order_id TEXT`
- `broker_status TEXT`
- `local_status TEXT`
- `filled_quantity INTEGER`
- `average_price REAL`
- `status_message TEXT`
- `last_event_time TEXT`
- `updated_at TEXT`

This gives the dashboard fast reads without scanning the full event log.

Update invariant:

- application code must call `sync_order_current(intent_id, conn)` whenever it inserts an `ops_broker_events` row
- the event insert and current-state UPSERT must happen in the same SQLite transaction
- `ops_order_current` is cache/state derived from the append-only event log; if drift is suspected, rebuild it from `ops_broker_events`
- expected event volume is low: roughly 2-10 broker events per trade/order lifecycle, plus explicit orderbook syncs

Rebuild sketch:

```sql
DELETE FROM ops_order_current;

INSERT INTO ops_order_current (
  intent_id,
  broker_order_id,
  exchange_order_id,
  broker_status,
  local_status,
  filled_quantity,
  average_price,
  status_message,
  last_event_time,
  updated_at
)
SELECT
  intent_id,
  broker_order_id,
  exchange_order_id,
  broker_status,
  CASE
    WHEN broker_status IN ('COMPLETE') THEN 'FILLED'
    WHEN broker_status IN ('REJECTED') THEN 'REJECTED'
    WHEN broker_status IN ('CANCELLED') THEN 'CANCELLED'
    WHEN broker_status IS NOT NULL THEN 'ACKED'
    ELSE 'SUBMITTED'
  END AS local_status,
  filled_quantity,
  average_price,
  status_message,
  event_time,
  event_time
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY intent_id
      ORDER BY event_time DESC, created_at DESC
    ) AS rn
  FROM ops_broker_events
  WHERE intent_id IS NOT NULL
)
WHERE rn = 1;
```

### 8.6 `ops_alert_events`

Append-only alert dispatch log.

Key columns:

- `alert_id TEXT PRIMARY KEY`
- `session_id TEXT`
- `event_type TEXT`
- `channel TEXT`
- `subject TEXT`
- `body_text TEXT`
- `body_html TEXT`
- `status TEXT`
- `dedupe_key TEXT`
- `metadata_json TEXT CHECK (metadata_json IS NULL OR json_valid(metadata_json))`
- `provider_response_json TEXT CHECK (provider_response_json IS NULL OR json_valid(provider_response_json))`
- `error_message TEXT`
- `latency_ms REAL`
- `created_at TEXT`

### 8.7 `ops_admin_commands`

Replace or mirror file-based admin commands.

Key columns:

- `command_id TEXT PRIMARY KEY`
- `session_id TEXT`
- `action TEXT`
- `symbols_json TEXT CHECK (symbols_json IS NULL OR json_valid(symbols_json))`
- `payload_json TEXT CHECK (payload_json IS NULL OR json_valid(payload_json))`
- `reason TEXT`
- `requester TEXT`
- `status TEXT` (`QUEUED`, `PROCESSING`, `DONE`, `FAILED`, `EXPIRED`, `CANCELLED`)
- `created_at TEXT`
- `processed_at TEXT`
- `error_message TEXT`

Keep file-based commands until SQLite command queue is proven.

### 8.8 `ops_feed_audit`

Hot feed/bar audit rows written during live/replay execution.

Key columns:

- `session_id TEXT NOT NULL REFERENCES ops_sessions(session_id)`
- `trade_date TEXT NOT NULL`
- `feed_source TEXT NOT NULL`
- `transport TEXT NOT NULL`
- `symbol TEXT NOT NULL`
- `bar_start TEXT NOT NULL`
- `bar_end TEXT NOT NULL`
- `open REAL`
- `high REAL`
- `low REAL`
- `close REAL`
- `volume REAL`
- `first_snapshot_ts TEXT`
- `last_snapshot_ts TEXT`
- `quality_status TEXT`
- `metadata_json TEXT CHECK (metadata_json IS NULL OR json_valid(metadata_json))`
- `created_at TEXT`
- `updated_at TEXT`

Primary key:

- `(session_id, symbol, bar_end)`

Indexes:

- `(session_id, bar_end)`
- `(trade_date, symbol)`
- `(quality_status, bar_end)`

Archive rule:

- keep recent hot rows in SQLite for operational debugging
- compact completed-session audit rows into DuckDB for historical parity analysis
- retain SQLite rows until the DuckDB archive confirms success

### 8.9 `ops_process_heartbeats`

Operational visibility for live runners and dashboard.

Key columns:

- `process_id TEXT PRIMARY KEY`
- `pid INTEGER`
- `role TEXT`
- `session_id TEXT`
- `command_line TEXT`
- `status TEXT`
- `last_heartbeat_at TEXT`
- `started_at TEXT`
- `ended_at TEXT`

### 8.10 Future Agent State Is Out Of Scope

Do not implement agent tables as part of this migration. If agent orchestration is added later, it
must still follow the same two-database rule:

- live mutable agent state in `data/ops.sqlite`
- completed agent analytics in `data/analytics.duckdb`
- no Postgres, Docker service, or third local database

---

## 9. Sync From SQLite To DuckDB

DuckDB should receive completed/archived data, not every hot operational mutation.

### 9.1 Archive Timing

Mirror from SQLite to DuckDB when:

- session reaches `COMPLETED`, `FAILED`, or `CANCELLED`
- operator runs an explicit archive/sync command
- EOD pipeline runs post-market

### 9.2 DuckDB Archive Tables

Keep or extend:

- `backtest_results` for archived trades
- `run_metadata` for paper/live session summaries
- `paper_order_archive` or equivalent for broker order audit summaries

Do not make DuckDB the live source of truth for open positions or in-flight broker orders.

### 9.3 Reconciliation Rule

If SQLite and DuckDB disagree for an active/recent session:

- SQLite wins for operational state.
- DuckDB can be rebuilt from SQLite/archive sync.

---

## 10. Dashboard Read Model

### 10.1 Operational Pages Read SQLite

These pages should read SQLite directly:

- Active Sessions
- Broker Orders
- Live feed health
- Admin command queue
- Alert delivery status
- Process/supervisor heartbeat

During Phase 3, operational dashboard pages must support a temporary multi-source merge:

- SQLite for new `session_storage_version = 2` sessions
- legacy paper DuckDB replica for old sessions that started before the cutover
- Postgres only until Phase 0a removes remaining active Postgres read paths

Merge rules:

- normalize rows into one dashboard DTO before rendering
- include `storage_source` and `session_storage_version` in debug/catalog views
- never switch a live session from one storage source to another mid-session
- if the same `session_id` appears in multiple sources, prefer SQLite, then DuckDB, then Postgres, and mark the duplicate as a migration warning

### 10.2 Analytical Pages Read DuckDB

These pages should continue reading DuckDB:

- Backtest Results
- Compare
- Trades analytics
- Daily summary
- Strategy analysis
- Data quality
- Symbols

### 10.3 Broker Orders Page Contract

Status: an initial Broker Orders page already exists. The migration changes its local data source
from paper DuckDB replica reads to SQLite operational reads; the page remains read-only for order
placement.

The Broker Orders dashboard should show:

- local intent id
- idempotency key
- session id
- symbol / side / qty
- product / order type / price / trigger
- local requested timestamp
- broker submit latency
- Kite order id
- exchange order id
- broker status
- filled qty / average price
- rejection/cancel message
- raw request/response expandable
- current local-vs-broker match status

Dashboard must be read-only for placement. Buttons should be limited to:

- refresh local
- fetch broker orderbook
- run reconciliation/sync if explicitly allowed

No place/modify/cancel buttons until operational controls are separately reviewed.

### 10.4 Database Catalog And Safe Data Browser

Add a dashboard page for storage visibility and controlled read-only browsing.

The page should show:

- current logical database roles
  - transaction DB: `data/ops.sqlite`
  - analytics DB: `data/analytics.duckdb`
  - raw source files: Parquet
- current physical database files
  - file path
  - role
  - size
  - last modified time
  - replica/snapshot status
  - lock/readiness status when available
- table inventory per database
  - table/view name
  - type
  - owning database
  - target owner after migration
  - row count or last-known row count
  - last refreshed/archived timestamp when available
- schema browser
  - column names
  - data types
  - nullable/default metadata when available
  - indexes for SQLite tables
- safe row preview
  - default `LIMIT 100`
  - operator-selected table only
  - simple filters for date/session/symbol/status where possible
  - no writes
  - no free-form SQL in the first version

Market-hour safety rules:

- DuckDB browsing must use read-only replica/snapshot connections where available.
- SQLite browsing must use read-only connections.
- Large `COUNT(*)` scans must not run automatically for big analytical tables; use cached metadata or an explicit "refresh count" action.
- Every preview query must have a hard limit.
- The page must clearly label current state versus target migration state.

This page answers operator questions like:

- how many database files exist now
- how many logical database roles exist now versus target
- how many tables are in each database
- which tables are moving to SQLite
- which tables stay in DuckDB
- whether dashboard data is coming from live operational state or archived analytics

---

## 11. Data Management, Backups, And History Policy

Current state:

- DuckDB replicas are dashboard read-safety snapshots, not durable historical backups.
- Replica retention is intentionally small and generated locally.
- There is no single documented automatic backup policy yet for `paper.duckdb`, `backtest.duckdb`, `market.duckdb`, or future `ops.sqlite`.

Target policy:

- `data/ops.sqlite` is backed up automatically after EOD checkpoint.
- `data/analytics.duckdb` is backed up after important batch events and before risky migrations.
- Parquet raw market files remain the rebuild source for market history.
- DuckDB replicas are never treated as backups.

### 11.1 SQLite Operational Backups

At EOD, after all sessions are completed or explicitly carried forward:

1. Run `PRAGMA wal_checkpoint(TRUNCATE)`.
2. Copy `data/ops.sqlite`.
3. Copy `data/ops.sqlite-wal` and `data/ops.sqlite-shm` only if they still exist after checkpoint.
4. Write a small manifest with:
   - backup timestamp
   - source file sizes
   - source file modified times
   - application git commit when available
   - latest session ids included
   - archive-to-DuckDB status

Suggested location:

```text
data/backups/ops/YYYY/MM/DD/ops_YYYYMMDD_HHMMSS.sqlite
data/backups/ops/YYYY/MM/DD/manifest.json
```

Retention:

- keep daily operational backups for 30 trading days
- keep weekly operational backups for 6 months
- never delete an operational backup for a date whose sessions/orders have not archived successfully to DuckDB

Ownership:

- EOD pipeline should run this automatically after session archive/sync status is known.
- A manual backup command should also exist for pre-migration and pre-maintenance snapshots.
- Dashboard may display latest backup status, but must not be the backup executor.

### 11.2 DuckDB Analytics Backups

Back up `data/analytics.duckdb`:

- after baseline promotions
- after archived paper/live sessions are compacted
- before schema migrations
- before any cleanup that deletes run rows
- weekly if no other backup was taken

Suggested location:

```text
data/backups/analytics/YYYY/MM/DD/analytics_YYYYMMDD_HHMMSS.duckdb
data/backups/analytics/YYYY/MM/DD/manifest.json
```

Retention:

- keep the latest backup from each week for 6 months
- keep backups tied to canonical baseline promotions indefinitely unless explicitly pruned
- keep pre-migration backups until the migration has been verified and accepted

Ownership:

- baseline promotion command should trigger an analytics backup before changing canonical references
- archive/compaction command should trigger a backup before destructive cleanup
- migration scripts should require a fresh analytics backup before schema/file consolidation
- weekly backup can be a manual operator command first; schedule automation later only if needed

### 11.3 Replica Policy

Replica directories are operational cache only:

- `data/market_replica/`
- `data/backtest_replica/`
- `data/paper_replica/`
- future analytics replica directory, if retained

Rules:

- replicas can be deleted and regenerated
- replicas are not synced between machines
- replicas are not historical reference backups
- dashboard should show replica age/status, but recovery docs should point to real backups and raw Parquet

### 11.4 Hot/Cold Historical Analytics Split

Most current strategy work runs from `2025-01-01` onward. Keeping only 2025+ runtime rows in the hot
analytics database may reduce:

- `analytics.duckdb` size
- DuckDB replica copy time
- dashboard catalog scan cost
- backup size
- maintenance-window duration

But it also adds risk:

- older backtests become a special workflow
- dashboard comparisons across older runs need an archive attach path
- baseline reproducibility can suffer if old data is split before the access path is tested
- another always-mounted DuckDB would weaken the "one analytics DB" operational model

Policy decision:

- do not split historical runtime tables during the SQLite migration
- first add the DB catalog page so table sizes, row counts, date ranges, and replica timings are visible
- then measure how much data is pre-2025 and how much it affects replica/build/dashboard time
- if splitting is still useful, keep `data/analytics.duckdb` as the hot 2025+ analytics DB and treat older data as an offline archive artifact, not a normal dashboard dependency

Possible future archive shape:

```text
data/analytics.duckdb          = hot analytics, default 2025-01-01 onward
data/archive/pre2025.duckdb    = optional offline archive, attached only for explicit old-history analysis
Parquet raw files              = full rebuild source for all dates
```

This optional archive must not be introduced until there is an operator command and dashboard label
that makes the hot/cold boundary explicit.

---

## 12. Migration Plan

### Phase 0. Document And Stabilize Current State

Status: current draft.

Rough duration: 1-2 weeks.

Tasks:

1. Keep DuckDB fixes already made for dashboard lock noise and broker audit visibility.
2. Do not expand real-order automation until order ledger is clean.
3. Keep one-share manual pilot gated.
4. Add the dashboard DB catalog/data-browser design to the plan before implementation.
5. Document JSON usage, backup policy, and hot/cold history decision before implementing SQLite.

Acceptance:

- current paper trading remains functional
- Broker Orders page can show rejected/filled broker status
- no new local database role beyond SQLite transaction DB and DuckDB analytics DB
- operators can see the current DB/table inventory target in the plan
- JSON, backup, retention, and history-split decisions are documented

### Phase 0a. Remove Active Postgres Read Paths

Rough duration: 1 week.

Goal: collapse the current Postgres dual-read situation before introducing SQLite as the new
operational store.

Tasks:

1. Replace `db.postgres` imports in `web/state.py` with current DuckDB paper/read helpers.
2. Replace `db.postgres` imports in `agent/tools/backtest_tools.py` with current DuckDB paper/read helpers.
3. Replace `scripts/reset_run_history.py` Postgres cleanup calls with DuckDB/paper cleanup paths.
4. Decide whether `agent/llm_agent.py` needs persistence; if yes, move it to local file/SQLite storage, otherwise remove `PgAgentStorage`.
5. Retire or quarantine `scripts/init_postgres.py`.
6. Replace Postgres-focused tests with DuckDB legacy-path tests that will later become SQLite tests.
7. Update docs/runbooks so no normal operator path requires Postgres.

Acceptance:

- dashboard paper/live pages work without importing `db.postgres`
- agent paper inspection tools work without importing `db.postgres`
- reset/cleanup tooling no longer opens a Postgres session
- no runtime code path requires Docker or Postgres for paper/live/dashboard workflows
- remaining `postgres` text is limited to historical docs or explicitly deprecated compatibility code

### Phase 1. Add SQLite WAL Infrastructure

Rough duration: 1-2 weeks.

Tasks:

1. Add `db/ops_sqlite.py`.
2. Create `data/ops.sqlite` lazily.
3. Apply WAL pragmas on open.
4. Add schema initialization with `CREATE TABLE IF NOT EXISTS`.
5. Add focused tests using temp SQLite files.
6. Add DB catalog helpers that can list SQLite tables/indexes and DuckDB tables/views safely.
7. Add backup helpers for `ops.sqlite` checkpoint and copy.

Acceptance:

- schema initializes cleanly
- multiple read connections work while one writer writes
- concurrent writes from 2-4 sessions serialize without `database is locked` errors in a burst test
- crash during one transaction does not corrupt prior rows
- DB catalog can report database file count and table counts without opening a DuckDB writer
- SQLite JSON constraints reject malformed JSON payloads
- EOD backup helper can produce an `ops.sqlite` backup and manifest from a temp DB

Crash/recovery tests:

- kill a worker process during an uncommitted SQLite transaction and verify committed rows survive while partial rows do not
- reopen a DB with WAL files present after an unclean shutdown and verify schema/data integrity
- verify backup helper refuses or clearly labels a backup if checkpoint fails

Performance tests:

- simulate 2-4 concurrent sessions writing candle-close updates
- target no `database is locked` errors
- target p95 write latency under 100 ms for ordinary operational writes
- target one 4-session, 10-position candle-close burst to finish in under 2 seconds on the local Windows machine

### Phase 2. Move Real-Order Ledger To SQLite

Rough duration: 1-2 weeks.

Tasks:

1. Write real order intents to `ops_order_intents`.
2. Write `PLACE_REQUEST` before calling Kite.
3. Write `PLACE_RESPONSE` with latency and broker order id.
4. Write `ORDERBOOK_SYNC` events after polling Kite.
5. Maintain `ops_order_current` through `sync_order_current(intent_id, conn)` in the same transaction as each event insert.
6. Keep DuckDB `paper_orders` compatibility bridge temporarily.

Acceptance:

- rejected ITC-style order is fully visible from SQLite
- local status sync is idempotent
- raw Kite response is preserved
- dashboard can display order lifecycle without opening DuckDB writer
- DB catalog labels broker/order operational tables as SQLite-owned

### Phase 3. Move Active Paper/Live Sessions To SQLite

Rough duration: 2-3 weeks. Highest-risk phase.

Tasks:

1. Move session lifecycle writes from legacy paper DuckDB storage to SQLite.
2. Move active position state from legacy paper DuckDB storage to SQLite.
3. Keep closed trade archival into DuckDB at session end.
4. Keep adapter layer so dashboard/state code does not care about storage during transition.
5. Add `session_storage_version` routing.
6. Migrate new sessions only; sessions created before the cutover finish on the legacy DuckDB path.
7. Dashboard reads merge SQLite new sessions and legacy paper DuckDB sessions by `session_storage_version` until legacy active sessions are gone.
8. Keep old-session DuckDB write/read path available until every pre-cutover active session has completed or been explicitly archived.

Acceptance:

- active sessions page reads SQLite
- live process can update open positions without DuckDB writer lock
- completed sessions still archive to DuckDB analytics
- no live session changes storage backend mid-session
- active sessions page clearly labels storage source during migration

### Phase 4. Move Feed State, Alerts, And Commands To SQLite

Rough duration: 1-2 weeks.

Tasks:

1. Move feed state heartbeat to SQLite.
2. Move feed audit hot rows to `ops_feed_audit`.
3. Move alert log to SQLite append-only table.
4. Mirror file command queue into SQLite while leaving file commands active.
5. Add dashboard command queue visibility.
6. Retire file commands only after SQLite command queue has at least 2 weeks of production use.

Acceptance:

- operators can see queued/processed/failed commands
- alert delivery and failures are visible
- stale/feed recovery evidence is queryable without log scraping
- feed audit rows are visible for active sessions without opening a DuckDB writer

### Phase 5. DuckDB Archive And Compaction

Rough duration: 2-3 weeks.

Tasks:

1. Add explicit `ops-archive-to-duckdb` job.
2. Archive completed sessions/orders/alerts into DuckDB reporting tables.
3. Keep SQLite retention policy for hot operational rows.
4. Validate dashboard analytical pages against archived data.
5. Rename/copy `market.duckdb` to `analytics.duckdb` and copy authoritative `backtest.duckdb` run tables.
6. Retire duplicate legacy run tables in `market.duckdb`.
7. Retire paper DuckDB replicas after paper/live operational reads are fully SQLite-backed.
8. Keep analytics DuckDB replica/snapshot support if dashboard reads still need lock isolation during backtests/builds.
9. Update DB catalog target labels to show the final two-database state.
10. Add analytics backup command before DuckDB consolidation.

Acceptance:

- completed sessions appear in historical dashboards
- active operational pages no longer require legacy paper DuckDB storage
- SQLite hot DB remains small and fast
- no planned full rebuild of `intraday_day_pack` is required for the rename cutover
- dashboard DB catalog shows one transaction DB and one analytics DB as the final logical state
- analytics backup exists before consolidation starts

---

## 13. What Not To Do

1. Do not move market runtime tables to SQLite.
   - They are analytical and scan-heavy.

2. Do not move backtest results to SQLite.
   - Backtest analysis needs columnar query performance.

3. Do not put live operational writes into the analytics DuckDB.
   - That database should remain analytics/reporting only.

4. Do not add Docker, Postgres, a server database, or another local database role.
   - The target is one transaction DB and one analytics DB.

5. Do not make the dashboard the real-order placement surface yet.
   - Dashboard should observe and reconcile first.

6. Do not rebuild large pack/runtime tables just to rename or consolidate DuckDB files.
   - Use rename/copy/attach and integrity checks; full rebuild is a repair path only.

7. Do not add a free-form SQL console to the dashboard in the first DB catalog version.
   - Start with table inventory, schema, filters, and limited previews.

8. Do not treat DuckDB replicas as historical backups.
   - They are short-lived dashboard read snapshots.

9. Do not split pre-2025 history into a second always-mounted analytics DB during this migration.
   - Measure first; if needed later, make it an explicit offline archive workflow.

10. Do not depend on SQLite `busy_timeout` as the only serialization mechanism.
    - Keep a per-process write lock and use busy timeout for cross-process contention.

---

## 14. Resolved Migration Decisions

1. Migrate broker ledger first, then active sessions/positions.
2. Mirror file commands into SQLite first; retire file commands only after 2 weeks of production use.
3. Keep 30 days hot by default, but never delete sessions, orders, alerts, or closed positions until archive-to-DuckDB has succeeded.
4. Add EOD backup after `PRAGMA wal_checkpoint(TRUNCATE)`: copy `ops.sqlite`, `ops.sqlite-wal`, and `ops.sqlite-shm` when present.
5. Do not migrate active sessions mid-session; new sessions use the new storage version after cutover.
6. DuckDB consolidation should not trigger pack rebuilds. Rename/copy existing `market.duckdb` content and attach/copy authoritative backtest tables instead.
7. Add a read-only dashboard DB catalog/data browser so operators can inspect DB counts, table counts, schemas, and limited samples safely.
8. Use SQLite JSON only for raw payloads, snapshots, metadata, and error context; keep query/reconciliation fields as normal columns.
9. Add automatic EOD backups for `ops.sqlite`; add event-based backups for `analytics.duckdb`.
10. Do not split hot/cold historical analytics during this migration. Revisit only after DB catalog measurements prove the benefit.
11. Remove active Postgres dashboard/agent/runtime read paths before introducing SQLite as the new operational store.
12. Move `paper_feed_audit` hot writes to SQLite and archive completed-session audit rows to DuckDB.
13. Keep a per-process write lock for SQLite writes; `busy_timeout` is a cross-process safety net.

---

## 15. Recommended Decision

Use a strict two-database local model:

```text
SQLite WAL             = the only transaction DB
DuckDB                 = the only analytics DB
Parquet source files   = raw market history, not a database role
```

Completely remove Docker/Postgres from the database plan. Do not introduce Docker, Postgres, a
server DB, or extra local DBs.

Do not limit SQLite to only `broker_ledger`; move the whole active operational surface there over phases. This gives better transactional boundaries and avoids mixed-state reconciliation across unrelated stores.

The first implementation should be small: remove active Postgres read paths, add SQLite WAL
infrastructure, then move the real-order broker ledger first. After that, migrate active
sessions/positions once the broker ledger proves stable. Existing DuckDB files can remain during
migration, but the target is one consolidated analytics DuckDB.
