# Database Architecture

Last updated: 2026-04-07

---

## Systems Overview

```
Parquet Files (immutable source)
  data/parquet/5min/{SYMBOL}/{YEAR}.parquet   <- 5-min OHLCV candles
  data/parquet/daily/{SYMBOL}/all.parquet     <- daily baseline OHLCV
  data/parquet/daily/{SYMBOL}/kite.parquet    <- daily incremental overlay
      |
      v
DuckDB market.duckdb (~500 MB)
  Views: v_5min, v_daily (query Parquet directly)
  Runtime tables: cpr_daily, atr_intraday, intraday_day_pack, ...
      |           \
      v            v
DuckDB backtest.duckdb (~50-200 MB)    DuckDB paper.duckdb (~1 MB)
  backtest_results, run_metrics,        paper_sessions, paper_positions,
  run_daily_pnl, run_metadata,          paper_orders, paper_feed_state,
  setup_funnel                          alert_log
      |                                 |
      v                                 v
  backtest_replica/                  paper_replica/
  (ReplicaSync -> ReplicaConsumer for dashboard reads)
```

**Rule of thumb**: Market data -> DuckDB `market.duckdb`. Backtest results -> DuckDB `backtest.duckdb`.
Paper trading state -> DuckDB `paper.duckdb`. All dashboard reads go through replicas.

---

## MCP Access (.mcp.json)

| Server | Command | Mode | Use |
|--------|---------|------|-----|
| `duckdb` | `uvx mcp-server-motherduck` | Read-write | Full DuckDB access |
| `duckdb-ro` | `uvx mcp-server-motherduck --ephemeral-connections` | Read-only | Safe dashboard/query access |
| `docker` | `npx @modelcontextprotocol/server-docker` | - | Container management (PostgreSQL) |

PostgreSQL is not directly exposed via MCP.

---

## 1. Parquet Files (Source of Truth)

### 5-Minute Candles
- **Path**: `data/parquet/5min/{SYMBOL}/{YEAR}.parquet`
- **Columns**: symbol, date, open, high, low, close, volume, true_range
- **Coverage**: ~2,105 NSE symbols, 2015-2026
- **Total**: ~175M rows across all files
- **Created by**: `pivot-convert` (one-time CSV -> Parquet)

### Daily Candles
- **Paths**:
  - `data/parquet/daily/{SYMBOL}/all.parquet`
  - `data/parquet/daily/{SYMBOL}/kite.parquet`
- **Columns**: symbol, date, open, high, low, close, volume
- **Coverage**:
  - `all.parquet`: baseline full history
  - `kite.parquet`: recent incremental overlap / catch-up days
- **Used for**: CPR pivot calculation (H+L+C)/3
- **Read contract**: `v_daily` scans both and prefers `kite.parquet` on duplicate `(symbol, date)`

### Known Issue
Friday daily data for 2015-2024 is stored with Sunday dates (+2 day shift).
This is a Zerodha CSV artifact from conversion. Does not affect backtest correctness
(engine matches by symbol+date across tables), but makes day-of-week analysis unreliable.

---

## 2. DuckDB (data/market.duckdb)

### Views (Not Materialized - Scan Parquet on Read)

| View | Source | Purpose |
|------|--------|---------|
| `v_5min` | `data/parquet/5min/**/*.parquet` | 5-minute OHLCV candles |
| `v_daily` | `data/parquet/daily/**/*.parquet` | Daily OHLCV candles, deduped with `kite.parquet` overlay precedence |

### Runtime Tables (Built by `pivot-build`)

These are materialized from Parquet views. Rebuild with `pivot-build --force` or
incrementally with `pivot-build --refresh-since YYYY-MM-DD`.

| Table | Key Columns | Purpose | Build Order |
|-------|-------------|---------|-------------|
| `cpr_daily` | symbol, trade_date, pivot, tc, bc, cpr_width_pct, floor_r1..s3, cpr_shift, is_narrowing | CPR levels + floor pivots per day | 1 |
| `atr_intraday` | symbol, trade_date, atr | ATR from last 12 x 5-min candles of prev day | 2 |
| `cpr_thresholds` | symbol, trade_date, cpr_threshold_pct | Rolling 252-day CPR width percentile | 3 |
| `or_daily` | symbol, trade_date, o0915, v0915, ... | Opening range data (LEGACY) | 4 |
| `market_day_state` | symbol, trade_date, prev_close, gap_pct, cpr_width_pct, atr, ... | Joined daily state for all symbols | 5 |
| `strategy_day_state` | symbol, trade_date, direction_5, or_atr_5, gap_abs_pct, open_side | Per-day strategy inputs | 6 |
| `intraday_day_pack` | symbol, trade_date, time_arr[], open_arr[], high_arr[], low_arr[], close_arr[], vol_arr[] | Full intraday candles as columnar arrays | 7 |
| `virgin_cpr_flags` | symbol, trade_date, virgin_cpr | Whether CPR zone was untouched | 8 |
| `dataset_meta` | symbol_count, min_date, max_date | Quick metadata | 9 |

**Dependency chain**:
```
v_daily -> cpr_daily -> cpr_thresholds
                |
v_5min  -> atr_intraday -> market_day_state -> strategy_day_state
        -> or_daily ----/                   \-> intraday_day_pack -> virgin_cpr_flags
```

### Result Tables (Written by Backtest/Walk-Forward)

**Moved to `backtest.duckdb`** — see section 3 below.

### Utility Tables

| Table | Purpose |
|-------|---------|
| `backtest_universe` | Named symbol lists (e.g. gold_51) for reproducible runs |
| `data_quality_issues` | Tracked data problems by symbol |

---

## 3. DuckDB (data/backtest.duckdb)

Separate from `market.duckdb` so the dashboard can read market data (read-only)
while the engine writes backtest results to a different file.

### Result Tables (Written by Backtest Engine)

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `backtest_results` | run_id, symbol, trade_date, direction, entry/exit prices, profit_loss, exit_reason, gross_pnl, total_costs, reached_1r, reached_2r, max_r | Trade-level results |
| `run_metrics` | run_id, strategy, trade_count, win_rate, total_pnl, profit_factor, calmar, max_dd_pct, annual_return_pct | Aggregated run performance |
| `run_daily_pnl` | run_id, trade_date, day_pnl, cum_pnl | Daily equity curve data |
| `setup_funnel` | run_id, strategy, universe_count, after_cpr_width, ..., entry_triggered | Filter stage pass counts |
| `run_metadata` | run_id, strategy, symbols_json, params_json, execution_mode, wf_run_id | Run configuration registry |

---

## 4. DuckDB (data/paper.duckdb)

Paper trading live state, replacing the former PostgreSQL paper tables.
Handled by `db/paper_db.py` (`PaperDB`).

### Paper Trading Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `paper_sessions` | session_id, strategy, symbols JSON, status (PLANNING->ACTIVE->COMPLETED), mode (replay/live), flatten_time, daily_pnl_used | Master session record |
| `paper_positions` | position_id, session_id, symbol, direction, status (OPEN/CLOSED), entry_price, stop_loss, target_price, trail_state JSON | Position tracking |
| `paper_orders` | order_id, session_id, position_id, side (BUY/SELL), fill_price, status | Order execution log |
| `paper_feed_state` | session_id, status (OK/STALE/DISCONNECTED), last_event_ts, last_price | Data feed health |
| `alert_log` | id BIGINT PK (sequence), alert_type, alert_level, subject, body, channel (TELEGRAM/EMAIL/BOTH/LOG), status (sent/failed/queued), error_msg, created_at | Alert delivery audit trail |

---

## 5. PostgreSQL (Docker, port 5433)

**Database**: `cpr_pivot`, **Schema**: `cpr_pivot`
**Container**: `cpr-pivot-postgres` (postgres:18-alpine)
**Initialized by**: `pivot-db-init` (runs `db/init_pg.sql`)

PostgreSQL is used **only** for agent sessions and walk-forward validation.
Paper trading state has migrated to `paper.duckdb`. No market data, backtest results,
or paper trading state is stored in PostgreSQL.

### Agent Tables

| Table | Purpose |
|-------|---------|
| `agent_sessions` | Phidata agent session state (memory, user_data, agent_data) |
| `agent_messages` | Chat history per session (role, content, tool interactions) |

### Walk-Forward Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `walk_forward_runs` | wf_run_id, strategy, start/end_date, validation_engine, gate_key, decision (PASS/FAIL/INCONCLUSIVE), decision_reasons, summary_json | WF run header + gate result |
| `walk_forward_folds` | wf_run_id, fold_index, trade_date, reference_run_id, total_trades, total_pnl, parity_status | Per-fold results |

### Signal Tables

| Table | Purpose |
|-------|---------|
| `signals` | Real-time trading signals (BUY/SELL, prices, active status) |

---

## 6. Replica System (Windows File-Lock Avoidance)

DuckDB uses exclusive file locking on Windows — only one process can hold a write connection
at a time. The replica system decouples dashboard reads from engine writes so both can run
concurrently.

### Architecture

```
Engine (writer)                     Dashboard (reader)
    │                                    │
    ▼                                    ▼
data/market.duckdb               get_dashboard_db()
data/backtest.duckdb             get_dashboard_backtest_db()
data/paper.duckdb                get_dashboard_paper_db()
    │                                    │
    ▼                                    ▼
ReplicaSync (db/replica.py)      ReplicaConsumer (db/replica_consumer.py)
    │                                    │
    ▼                                    ▼
data/backtest_replica/           Reads versioned snapshots
  *_v15.duckdb                   via pointer file (*_latest)
  *_v16.duckdb
  *_latest                       Auto-reconnects when version changes
data/paper_replica/
  *_v372.duckdb
  *_v373.duckdb
  *_latest
```

### How It Works

1. **Writer side** (`ReplicaSync.maybe_sync()`): After writes, the engine copies its DuckDB
   file to a versioned snapshot (e.g., `paper_replica_v373.duckdb`) and updates a pointer
   file (`paper_replica_latest` → `373`). Sync is debounced — won't re-sync within
   `min_interval_sec`.

2. **Reader side** (`ReplicaConsumer`): The dashboard checks the pointer file for version
   changes. When a new version appears, it reconnects to the new snapshot file. Old
   connections are closed gracefully.

3. **Cleanup** (`_cleanup_old_versions()`): After every sync, replica files with version
   `<= current - MAX_REPLICA_VERSIONS` are deleted. Default retention: **2 versions** per DB.

### Three Replica Channels

| Canonical DB | Replica directory | Sync interval | Dashboard function | Source code |
|---|---|---|---|---|
| `market.duckdb` (~500 MB) | `data/backtest_replica/` | 10s debounce | `get_dashboard_db()` | `db/duckdb.py:5117` |
| `backtest.duckdb` (~50-200 MB) | `data/backtest_replica/` | 30s debounce | `get_dashboard_backtest_db()` | `db/backtest_db.py:1203` |
| `paper.duckdb` (~1 MB) | `data/paper_replica/` | 5s debounce | `get_dashboard_paper_db()` | `db/paper_db.py:1119` |

### What Lives Where

| Dashboard page / data | Source table | Canonical DB | Read via |
|---|---|---|---|
| Home, Symbols, Data Quality | All runtime tables + `data_quality_issues` | `market.duckdb` | Market replica |
| Backtest detail, Trades, Compare | `backtest_results`, `run_metrics`, `run_daily_pnl` | `backtest.duckdb` | Backtest replica |
| Paper Ledger, Paper Live | `paper_sessions`, `paper_positions`, `paper_orders`, `alert_log` | `paper.duckdb` | Paper replica |

### Key Properties

- **`source_conn` requirement**: All callers of `maybe_sync()` MUST pass the existing DB
  connection (`source_conn`). On Windows, `maybe_sync()` runs the copy synchronously on
  the writer's connection to avoid opening a second connection to the same file.
- **MAX_REPLICA_VERSIONS = 2**: Only the 2 most recent snapshots are kept. Old versions
  are pruned on every sync call.
- **Dashboard can stay running during**: live paper trading, daily-sim, daily-replay
- **Dashboard must be killed before**: `pivot-backtest`, `pivot-build` (these write
  `market.duckdb`, and the dashboard holds a read connection to the market replica which
  can conflict with full rebuilds)
- **Checkpoint cleanup**: `ReplicaSync` uses DuckDB `CHECKPOINT` with `checkpoint_cleanup=True`
  on ephemeral copies to minimize replica file size

---

## 7. Current State (2026-04-07)

```
market.duckdb Runtime Tables:
  cpr_daily           4,059,475 rows   2,105 symbols
  atr_intraday        3,324,624 rows   2,105 symbols
  cpr_thresholds      4,059,475 rows   2,105 symbols
  virgin_cpr_flags    4,059,475 rows   2,105 symbols
  or_daily            2,910,000 rows   2,105 symbols
  market_day_state    2,907,946 rows   2,105 symbols
  strategy_day_state  2,907,946 rows   2,105 symbols
  intraday_day_pack   3,332,229 rows   2,105 symbols
  dataset_meta                1 row

Replica snapshots:
  data/backtest_replica/  (market + backtest)  ~2 versions, auto-cleaned
  data/paper_replica/                           ~2 versions, auto-cleaned

backtest.duckdb Result Tables:
  backtest_results       67,473 rows   4 runs (full universe, net of costs)
  run_daily_pnl          ~8,800 rows
  run_metrics                 4 rows

paper.duckdb Paper State:
  paper_sessions             varies   active + completed sessions
  paper_positions            varies   per-session positions
  alert_log                  varies   alert delivery audit trail
```

---

## 8. Operational Commands

```bash
# Build all runtime tables (market.duckdb)
doppler run -- uv run pivot-build

# Incremental refresh (new data only)
doppler run -- uv run pivot-build --refresh-since 2026-03-20

# Check table status
doppler run -- uv run pivot-build --status

# Initialize PostgreSQL schema (agent sessions / walk-forward only)
doppler run -- uv run pivot-db-init

# One-time: migrate backtest results from market.duckdb to backtest.duckdb
doppler run -- uv run python -m scripts.migrate_split --split-backtest
```
