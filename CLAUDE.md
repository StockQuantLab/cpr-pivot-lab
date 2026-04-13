# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

AI-powered CPR-ATR strategy backtesting framework for NSE stocks (2105 symbols, 10 years of 5-min data).

## Architecture

```
Raw CSV (Zerodha)
    ↓  pivot-convert  (one-time, ~2 min per 50 stocks × 10 years)
data/parquet/5min/SYMBOL/YEAR.parquet   (OHLCV + true_range)
data/parquet/daily/SYMBOL/all.parquet   (daily OHLC for CPR)
    ↓  DuckDB  (zero import — queries Parquet directly)
data/market.duckdb  (materialized CPR + ATR + volume tables)
    ↓  Polars + TrailingStop  (vectorized backtest engine)
Backtest results  (stored in DuckDB, append-only per run_id)
    ↓  NiceGUI + Phidata AI Agent
Web Dashboard (http://127.0.0.1:9999)

Paper Trading (same engine, different I/O):
DuckDB (read: market_day_state, strategy_day_state, intraday_day_pack)
    ↓  engine/cpr_atr_shared.py (pure decision logic)
    ↓  engine/paper_runtime.py  (position management + alert dispatch)
DuckDB paper.duckdb (paper sessions, positions, orders, feed state, alerts)
    ↓  scripts/paper_archive.py
DuckDB backtest.duckdb (execution_mode="PAPER")
```

Paper replay/live/sim strategy parameters must stay aligned with backtest. The paper
layer should not inject hidden strategy defaults such as implicit `min_price`,
`risk_based_sizing`, or `narrowing_filter` values. Use the same explicit flags or JSON
overrides that the matching backtest run used.

`max_positions=10` limits concurrent open positions only. It is not a daily trade cap.

Replay logging stays deliberately sparse:
- candle-progress logs show the current 5-minute bar only for the first symbol of each date
- trade open/close/partial logs are emitted only when a trade actually happens, and the
  event time is included in the log line
- replay remains symbol-major today, so trade logs may lag the candle heartbeat unless we
  refactor the loop to be bar-major across the whole date

For the current baseline compare work, stay on `CPR_LEVELS` only. FBR is not required
for these reruns and should not be mixed into the CPR baseline set. When you do need FBR,
use the named presets `FBR_RISK_LONG` and `FBR_RISK_SHORT` rather than spelling the full flag bundle.

Apr 2026 CPR parity note:
- `daily-replay` starts a fresh session per trade date.
- Matching params are required, but executed rows can still diverge from a multi-day backtest window.
- On 2026-04-01 we observed paper trades for `GANESHCP` and `ASTERDM` that did not appear in the full baseline rows.
- When this happens, compare setup rows and intraday day packs first, then diff replay entry logic against the backtest path before changing filters.
- The fix direction is to unify the CPR entry search itself. Keep replay/live as the candle-by-candle operational model, and align backtest to the same shared search so trade existence matches everywhere.

**PostgreSQL** (port 5433, Docker) handles: `agent_sessions`, `agent_messages`, `signals`,
  and walk-forward validation: `walk_forward_runs`, `walk_forward_folds`.
**Market data** (OHLCV, CPR, ATR) → DuckDB `market.duckdb` + Parquet.
**Backtest results** → DuckDB `backtest.duckdb`.
**Paper trading state** → DuckDB `paper.duckdb` (sessions, positions, orders, feed state, alert_log).

### Batch Fetch Architecture (Apr 2026 — current)

The engine uses `intraday_day_pack`: a DuckDB table storing full intraday candle arrays per (symbol, trade_date) as columnar arrays (time_arr, open_arr, high_arr, ...). This replaces the old `or_daily` approach.

- **`intraday_day_pack`**: built by `pivot-build --table pack`. Must be present for ALL symbols before backtesting. See `STATUS.md` for current coverage — run `pivot-build --table pack --force` before full run.
- **`market_day_state`**: per-day state (prev_close, gap_pct, etc.) — 2105 symbols.
- Result: 51 symbols × 10 years runs in ~21–30s wall time (engine time <10s, day-pack fetch ~8–15s). Full universe (2105 syms) ~3–5 min.

---

## Essential Commands

```bash
# Setup
uv sync
docker-compose up -d

# Linting (ruff, line-length=100, py313 target)
uv run ruff check .
uv run ruff format .

# Tests
uv run pytest
uv run pytest tests/test_foo.py -k test_name

# Data ingestion (Kite — NSE holidays/weekends auto-skipped)
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE
doppler run -- uv run pivot-kite-ingest --from 2026-03-21 --to 2026-03-23
doppler run -- uv run pivot-kite-ingest --from 2026-03-21 --to 2026-03-23 --5min --resume
# Backfill ALL tradeable NSE symbols (use when new symbols missing from parquet)
doppler run -- uv run pivot-kite-ingest --universe current-master --from 2015-01-01 --to 2025-12-01 --resume --skip-existing
# After ingestion: rebuild + validate
doppler run -- uv run pivot-build --refresh-since 2026-03-21 --batch-size 64
doppler run -- uv run pivot-data-validate

# Data hygiene / quality
doppler run -- uv run pivot-hygiene --dry-run                  # preview dead symbols
doppler run -- uv run pivot-hygiene --purge --confirm          # delete dead symbol data
doppler run -- uv run pivot-data-quality --refresh --full      # full scan (1-5 min)
doppler run -- uv run pivot-data-quality --date 2026-03-27     # trade-date readiness gate

# Build runtime tables (REQUIRED before first backtest)
doppler run -- uv run pivot-build                          # build all if not exists
doppler run -- uv run pivot-build --force                  # force rebuild everything
doppler run -- uv run pivot-build --table pack --force     # rebuild only intraday_day_pack
doppler run -- uv run pivot-build --status                 # check table row counts

# Run backtest — use --universe-name for a saved universe, --all for dynamic liquid
doppler run -- uv run pivot-backtest --symbol SBIN --start 2020-01-01 --end 2024-12-31
doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31
doppler run -- uv run pivot-backtest --all --universe-size 0 --start 2015-01-01 --end 2024-12-31
# Production CPR_LEVELS run:
doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2023-01-01 --end 2023-12-31 \
  --strategy CPR_LEVELS --cpr-min-close-atr 0.35 --min-price 50 --narrowing-filter --skip-rvol
# FBR (tuned: --failure-window 10 is best — Calmar 4.08 vs baseline 3.93)
doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 \
  --strategy FBR --failure-window 10 --skip-rvol

# Parameter sweep / campaign / refresh / signal alerts / parity check
doppler run -- uv run pivot-sweep sweeps/cpr_width_rvol.yaml        # YAML-driven sweep
doppler run -- uv run pivot-campaign --universe-name gold_51 --start 2015-01-01 --end 2024-12-31
doppler run -- uv run pivot-refresh --since 2026-04-01              # daily build + paper prep
doppler run -- uv run pivot-signal-alert --symbols SBIN --condition narrow-cpr
doppler run -- uv run pivot-parity-check --expected-run-id <bt> --actual-run-id <paper>

# Maintenance
uv run pivot-data-validate                          # validate all symbols
uv run pivot-clean                                  # cleanup caches/logs (never touches data/)
doppler run -- uv run pivot-reset-history           # reset run history
doppler run -- uv run pivot-kite-token              # one-time token exchange per session
doppler run -- uv run pivot-db-init                 # init PostgreSQL schema (once)

# AI Agent / Dashboard
doppler run -- uv run pivot-agent -q "Show me SBIN backtest results for 2023"
doppler run -- uv run pivot-dashboard               # http://127.0.0.1:9999
taskkill //IM python.exe //F                        # kill dashboard (holds market.duckdb lock)
```

---

## Strategies (2 active)

| Strategy | Default | Edge |
|----------|---------|------|
| **CPR_LEVELS** | Yes | SL at Pivot, target at R1/S1 floor pivots |
| **FBR** | No | 86% ORB failure → trade the reversal |
See `STRATEGY.md` for full specification of baseline strategies.

### CPR_LEVELS (7 rules)
1. FILTER: Narrow CPR day (cpr_width_pct < rolling P50)
2. DIRECTION: 9:15 close > TC → LONG, < BC → SHORT
3. ENTRY: TC/BC touch — first candle closing above TC+buffer (LONG) or below BC-buffer (SHORT); scan starts 09:15
4. STOP LOSS: At BC − ATR buffer (LONG) or TC + ATR buffer (SHORT) — CPR zone edge
5. TARGET: R1 (LONG) or S1 (SHORT)
6. AFTER T1: Trail stop at 1×ATR from best price toward R2/S2
7. TIME EXIT: 15:15

**Why TC/BC instead of ORB**: CPR width ≈ 0.3–0.5% on narrow days → SL ≈ 3–5 pts, target (R1) ≈ 15–20 pts → effective RR 3–6:1. ORB entry put SL at Pivot (~10 pts below) with R1 only 5 pts above → 0.47:1 effective RR (losing).

---

## Paper Trading

Paper trading validates strategies against live or historical data. Operational runbook: `docs/PAPER_TRADING_RUNBOOK.md`.

**Model**: 2 primary sessions per day — CPR_LEVELS LONG and SHORT. FBR LONG/SHORT are opt-in.
Walk-forward validation exists but is NOT part of daily flow — paper trading IS the validation.

When rerunning the four CPR dashboard baselines, keep every parameter identical to the
stored CPR row and change only the dates. Prefer named presets such as
`CPR_LEVELS_RISK_LONG` and `CPR_LEVELS_RISK_SHORT` instead of spelling the flag bundle.
Use `--risk-based-sizing` as the canonical term; `--standard-sizing` is a legacy opt-out
alias and should not appear in baseline docs.

Canonical CPR replay/live command:
`doppler run -- uv run pivot-paper-trading daily-replay --multi --strategy CPR_LEVELS --trade-date <date> --all-symbols --no-alerts`
Do not repeat `--min-price 50 --cpr-min-close-atr 0.5 --narrowing-filter` unless you are
intentionally overriding the canonical preset. For FBR, use `--preset FBR_RISK_LONG`
or `--preset FBR_RISK_SHORT` instead of hand-spelling the legacy flag bundle.

CPR entry rule: a trade only opens when the effective reward/risk at entry meets
`min_effective_rr` (default `2.0`). `rr_ratio` is the target multiple used by the
trade model; it is not the gate. For CPR SHORT presets, `skip_rvol_check=True`
means the numeric `rvol_threshold` is ignored at runtime even if the dashboard
still displays it.

**daily-sim** (historical, preferred): Polars vectorized engine, ~25s/variant, no alerts, direct to `backtest.duckdb`.
**daily-replay** (historical, audit): `paper_runtime.py` candle-by-candle, full alert dispatch. Use `--multi` for all CPR variants and keep the canonical preset defaults unless overriding intentionally.
**daily-live** (current day only): Kite quotes, candle building, alert dispatch, archive on completion.
**--multi**: all 4 variants in one process (single DuckDB writer, ref-counted alert dispatcher, shared Kite poll cache).
**--strategy filter**: `--multi --strategy CPR_LEVELS` runs only LONG+SHORT.

**Pre-market** (before daily-live): `pivot-refresh --since <prev_date>` then `pivot-paper-trading daily-prepare --trade-date today --all-symbols`.
**Cleanup**: `pivot-paper-trading cleanup --trade-date YYYY-MM-DD --apply` for a specific date, or `cleanup --apply` for all. Neither touches baseline backtest results.

**Paper engine parity (Apr 2026 — fixed):**
- One entry per symbol per day enforced via `SymbolRuntimeState.position_closed_today`
- CPR_LEVELS fill price uses `max(trigger, candle_open)` to mirror backtest stop-order simulation

Session statuses: `PLANNING → ACTIVE → PAUSED → STOPPING → COMPLETED | FAILED | CANCELLED`

---

## LLM Integration

- **Provider**: Ollama (local or cloud) via `agent/llm/ollama_provider.py`
- **Framework**: Phidata with `phi.model.ollama.ChatOllama`
- **Session storage**: PostgreSQL `cpr_pivot.agent_sessions` via `PgAgentStorage`

Required Doppler secrets:
```
OLLAMA_MODEL          # model name (default: llama3.2)
OLLAMA_BASE_URL       # Ollama endpoint (default: http://localhost:11434)
OLLAMA_API_KEY        # API key (Ollama Cloud only; omit for local)
POSTGRES_PASSWORD     # PostgreSQL password
```

---

## Default Parameters (Current CLI Defaults)

Non-obvious defaults — see `engine/run_backtest.py` argparse for the full list:

| Param | Default | Note |
|-------|---------|------|
| `--cpr-percentile` | 33 | Bottom-third CPR width |
| `--rvol` | 1.0 | Relative volume; used for LONG only. SHORT uses `--skip-rvol` (rvol check disabled). |
| `--max-sl-atr-ratio` | 2.0 | Tighter SL cap |
| `--or-atr-min` / `--or-atr-max` | 0.3 / 2.5 | Filter tiny/exhausted ORs |
| `--max-gap-pct` | 1.5 | Skip large opening gaps |
| `--failure-window` | 8 | FBR only (use 10 for better Calmar) |
| `--breakeven-r` | 1.0 | Move SL to entry at this R |
| `--min-effective-rr` | 2.0 | CPR entry gate; must be met before taking the trade |
| `--direction` | BOTH | CPR_LEVELS: direct. FBR: LONG=failed breakdown, SHORT=failed breakout |
| `--runtime-batch-size` | 512 | Symbols per runtime fetch batch |
| `--commission-model` | zerodha | Brokerage cost model |

FBR direction mapping: `--direction LONG` → `fbr_setup_filter BREAKDOWN`, `--direction SHORT` → `BREAKOUT`.

---

## DuckDB Schema (Three-File Architecture)

```
data/market.duckdb     (~500MB) — market data, CPR, ATR, runtime tables
data/backtest.duckdb   (~50-200MB) — backtest results, run metrics
data/paper.duckdb      (~1MB) — paper trading live state, alert log
```

### market.duckdb

```sql
-- Views (no data stored — query Parquet directly)
v_5min       → data/parquet/5min/*/*.parquet
v_daily      → data/parquet/daily/*/all.parquet

-- Materialized tables (built via pivot-build)
cpr_daily          (symbol, trade_date, pivot, tc, bc, cpr_width_pct, r1..s3, cpr_shift, ...)
atr_intraday       (symbol, trade_date, atr)
cpr_thresholds     (symbol, trade_date, cpr_threshold_pct)       -- rolling P50 per symbol
virgin_cpr_flags   (symbol, trade_date, is_virgin_cpr)           -- virgin CPR markers
or_daily           (symbol, trade_date, ...)                     -- LEGACY, kept for compatibility
market_day_state   (symbol, trade_date, prev_close, gap_pct, tc, bc, pivot, atr, ...)
strategy_day_state (symbol, trade_date, direction, open_side, or_close_5, or_atr_5, ...)
intraday_day_pack  (symbol, trade_date, time_arr[], open_arr[], high_arr[], low_arr[], close_arr[], vol_arr[])
                    -- replaces or_daily; required by engine; 2105 symbols
dataset_meta       (symbol_count, min_date, max_date)
backtest_universe  (universe_name PK, symbols JSON, created_at)
data_quality_issues (symbol, trade_date, issue_code, severity, details JSON, ...)
```

### backtest.duckdb

`backtest_results` (run_id, symbol, trade_date, direction, entry/exit, pnl, exit_reason, ...)
`run_metrics` (run_id, strategy, start_date, end_date, trade_count, win_rate, total_pnl, profit_factor, max_dd_pct, annual_return_pct, calmar, ...)
`run_metadata` (run_id PK, strategy, symbols, start_date, end_date, params JSON, execution_mode)
`run_daily_pnl` (run_id, symbol, trade_date, cumulative_pnl, ...)
`setup_funnel` (run_id, symbol, trade_date, filter_step, count, ...)

### paper.duckdb

`paper_sessions` (session_id PK, strategy, symbols JSON, status, ...)
`paper_positions` (position_id PK, session_id FK, symbol, direction, status, entry_price, stop_loss, target_price, trail_state JSON, ...)
`paper_orders` (order_id PK, session_id FK, position_id FK, ...)
`paper_feed_state` (session_id PK FK, status, last_event_ts, ...)
`alert_log` (id BIGINT PK, alert_type, alert_level, subject, body, channel, status, error_msg, created_at)

### Replica System (Windows File-Lock Avoidance)

Engine writes to canonical DuckDB files. `ReplicaSync` (`db/replica.py`) publishes versioned snapshots to
`data/backtest_replica/` and `data/paper_replica/`. Dashboard reads from replicas via `ReplicaConsumer`
(`db/replica_consumer.py`). This avoids DuckDB file-lock conflicts on Windows when the dashboard is running.
Paper trading uses its own replica (`data/paper_replica/`) — the dashboard can run alongside live paper sessions
without needing to be killed.

**Windows constraint**: DuckDB uses exclusive file locking — a second `duckdb.connect()` to the same file in the same
process raises `IOException`. All callers of `maybe_sync()` MUST pass `source_conn` (the existing DB connection) so the
sync uses `COPY FROM DATABASE` on the live connection instead of opening a second one. Never call `maybe_sync()` without
`source_conn` from within a process that already has the DB file open.

Indexes: `idx_cpr_symbol_date`, `idx_atr_symbol_date`, `idx_thresh_symbol_date`, `idx_br_symbol`.

`run_id` is a UUID generated fresh per execution — backtest results are **append-only**. No cache reuse across runs; use `--save` to persist, and the dashboard's compare page to diff runs.

MarketDB singleton: `from db.duckdb import get_db; db = get_db()`. Only one write connection at a time.
BacktestDB: `from db.backtest_db import BacktestDB`. PaperDB: `from db.paper_db import PaperDB`.

---

## Alert System

Best-effort async dispatch via Telegram + Email. Fire-and-forget — never blocks trading.
All alerts logged to `alert_log` in `paper.duckdb`.

Types: `TRADE_OPENED`, `TRADE_CLOSED`, `SL_HIT`, `TARGET_HIT`, `TRAIL_STOP`,
`SESSION_STARTED`, `SESSION_COMPLETED`, `SESSION_ERROR`,
`DAILY_LOSS_LIMIT`, `DRAWDOWN_LIMIT`, `FLATTEN_EOD`, `DAILY_PNL_SUMMARY`

Config: `config/settings.py` — `alert_on_*` toggles, `telegram_bot_token`, `telegram_chat_ids`, SMTP settings.
Doppler secrets: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS`, `SMTP_USER`, `SMTP_PASSWORD`, `ALERT_TO_EMAIL`.

---

## Agent Tools (14)

See `AGENTS.md` for full reference. Tools live in `agent/tools/backtest_tools.py` and are registered in `agent/llm_agent.py` via `_json_tool()` wrapper (converts dict → JSON string for Phidata).

To add a new tool:
1. Add a function to `agent/tools/backtest_tools.py` returning `dict`, with typed args + docstring
2. Import in `agent/llm_agent.py` and add `_json_tool(your_func)` to the `tools = [...]` list
3. Phidata auto-generates the schema from docstring + type hints — no registration needed

---

## Claude Code Hooks

Defined in `.claude/settings.json`. Scripts in `.claude/hooks/`.

| Hook | Trigger | Blocks |
|------|---------|--------|
| `block_bash.py` | PreToolUse → Bash | Any `rm`/`rmdir`/`rd`/`del`/`Remove-Item` targeting `data/`; shell writes to `.env` |
| `block_env_write.py` | PreToolUse → Write/Edit | Writing or editing `.env*` files |
| `ruff_format.py` | PostToolUse → Edit/Write | — (auto-formats `.py` files, never blocks) |

**data/ is fully protected**: No deletion inside `data/` is allowed through Claude — not even `rm data/market.duckdb.wal`. DuckDB WAL issues are prevented by never running parallel connections. If manual cleanup is genuinely needed, do it outside Claude via Explorer or a terminal.

---

## Running Backtests from Claude Code (Automation)

When Claude Code runs `pivot-backtest` as a subprocess (not a real TTY), stdout is block-buffered and tqdm
writes to stderr — both behave differently from an interactive terminal. Use these rules to avoid hangs or
silent failures.

### Rules for non-TTY execution

1. **Always use `--quiet --progress-file <path>`** for runs with more than 100 symbols.
   `--quiet` suppresses tqdm and per-symbol lines (prevents pipe-buffer fill-up that blocks Python).
   `--progress-file` writes NDJSON heartbeat events so Claude Code can monitor progress.
   Without `--progress-file`, `--quiet` is rejected for large runs.

2. **Always set `PYTHONUNBUFFERED=1`** so print() output is not block-buffered:
   ```bash
   PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
     --all --universe-size 0 --start 2025-01-01 --end 2026-03-30 \
     --strategy CPR_LEVELS --direction LONG --skip-rvol --save \
     --quiet --progress-file /tmp/bt_progress.jsonl
   ```

3. **Monitor progress** by reading `/tmp/bt_progress.jsonl` (NDJSON, one event per line).

### Lock management

Two lock files are used at runtime:

| File | Type | Auto-clears? |
|------|------|-------------|
| `data/market.duckdb.writelock` | PID JSON file | Yes — stale (dead-PID) lock is auto-deleted on next acquisition attempt |
| `.tmp_logs/runtime-writer.lock` | OS byte-range lock (msvcrt) | Yes — OS releases on process exit; never goes stale |

If a run is killed and the error says "Another DuckDB write process is running (PID N)", the lock will
auto-clear when you next run `pivot-backtest` — the acquisition code checks PID liveness and deletes the
stale file. You do **not** need to manually delete `data/market.duckdb.writelock`.

If the process is still alive, both lock errors now print the exact kill command:
```
Kill it:  taskkill //F //PID 12345
```

### Performance characteristics

All Apr 2026 query optimizations applied (batch-size, date-filtered setup/pack queries, lazy replica sync). Expected wall times:
- 51 symbols × 15 months: ~5–10 s
- 2105 symbols × 15 months: ~2–5 min
- 2105 symbols × 10 years: ~20–40 min

---

## Windows-Specific Notes

1. Set `asyncio.WindowsSelectorEventLoopPolicy()` BEFORE uvicorn import (done in `web/run_nicedash.py`)
2. Use `pathlib.Path` for all file paths; DuckDB glob paths must use forward slashes (`.replace("\\", "/")`)
3. Both CLI entry points wrap stdout/stderr for UTF-8 encoding on Windows
4. Replica system avoids DuckDB file-lock conflicts when dashboard is running alongside the engine

---

## Known Issues

| Issue | Solution |
|-------|----------|
| "No 5-min Parquet found" | Run `pivot-convert` or `pivot-kite-ingest` first |
| "No valid setups found" / empty results | Run `pivot-build --force` (runtime tables missing) |
| DuckDB locked / WAL file | Never run parallel write connections. If a `.wal` file persists after a crash, delete it manually via Explorer — Claude's `data/` hook will block `rm data/market.duckdb.wal` |
| `data/market.duckdb.writelock` persists after kill | Auto-cleared on next `pivot-backtest` start (PID liveness check). Error message shows `taskkill //F //PID N` when live process holds it. |
| Port 9999 stuck | `netstat -ano \| findstr 9999` then kill PID |
| PostgreSQL connection failed | `docker-compose up -d` (only needed for agent sessions / walk-forward, not daily paper trading) |
| First year has no CPR threshold | Expected — 252-day warm-up; falls back to `cpr_max_width_pct = 2.0%` |
| New NSE symbols missing from parquet | Run `pivot-kite-ingest --universe current-master --from 2015-01-01 --to <date> --resume --skip-existing` |
| Short-history symbols (<1yr) in dashboard | Symbols recently added to pipeline need backfill — use `--universe current-master` |
| Dead symbols in data | Run `pivot-hygiene --purge --confirm` (see `/data_quality` for preview) |
| `tradeable_symbols()` returns wrong count | Requires `data/NSE_EQUITY_SYMBOLS.csv` (NSE equity allowlist, SERIES=EQ). File is not tracked in git — copy from `nse-momentum-lab/data/` or download fresh from NSE website. Without it, filter falls back to `segment=NSE` (~9,356). |
