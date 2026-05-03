# CPR Pivot Lab

AI-powered intraday backtest system for NSE stocks with a clean production baseline.

Current production strategy set:

- `CPR_LEVELS`
- `FBR`

Strategy rules, defaults, and parameter examples live in [STRATEGY.md](STRATEGY.md).

Default operating rule: use a named preset for every canonical backtest, replay, live, or paper
run. Only spell out explicit flags for ad hoc analysis, hypothesis testing, or deliberate
override validation.

**Stack:** DuckDB + Parquet, PostgreSQL (sessions/signals), NiceGUI, Phidata + Ollama.

## Quick Setup

### 1) Prerequisites

- Python 3.10+ with `uv`
- Doppler CLI configured
- Docker running (PostgreSQL for AI agent storage)

### 2) Environment

```bash
uv sync
docker-compose up -d
doppler secrets set OLLAMA_MODEL "llama3.2"
doppler secrets set OLLAMA_BASE_URL "http://localhost:11434"
doppler secrets set POSTGRES_PASSWORD "your-password"
```

### 3) Data and runtime tables

```bash
# Convert source CSV files
doppler run -- uv run pivot-convert

# Build core runtime tables from full local history (explicit full-history confirmation required)
doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild --duckdb-threads 4 --duckdb-max-memory 24GB --batch-size 128

# Rebuild intraday pack only for a recent catch-up window
doppler run -- uv run pivot-build --table pack --refresh-since 2026-03-21 --pack-lookback 10

# Refresh the issue registry and gate a specific trade date after rebuild
doppler run -- uv run pivot-data-quality --refresh
doppler run -- uv run pivot-data-quality --date 2026-03-27
```

### 4) Backtest workflow

```bash
# Single symbol
doppler run -- uv run pivot-backtest --symbol RELIANCE --strategy CPR_LEVELS --start 2025-01-01 --end 2026-03-09 --save

# Gold universe (reproducible benchmark)
doppler run -- uv run pivot-gold prepare --name gold_51 --start 2015-01-01 --end 2025-03-31 --universe-size 51
doppler run -- uv run pivot-backtest --universe-name gold_51 --strategy CPR_LEVELS --start 2025-01-01 --end 2026-03-09 --save

# Long-only and short-only sensitivity
doppler run -- uv run pivot-backtest --symbol RELIANCE --strategy CPR_LEVELS --direction LONG --start 2025-01-01 --end 2026-03-09 --save
doppler run -- uv run pivot-backtest --symbol RELIANCE --strategy CPR_LEVELS --direction SHORT --start 2025-01-01 --end 2026-03-31 --save
```

When you are creating or extending a baseline, prefer the named preset path over hand-spelled
flags. That keeps backtest, replay, and live parity tied to one canonical config bundle.

### 5) Canonical rerun recipe

Use this when you want a clean slate in the tables and the four dashboard backtests rerun from the same date range.
Run these from the host Windows shell with Doppler authenticated.

```bash
# Wipe DuckDB backtest history and PostgreSQL paper-trading state
doppler run -- uv run pivot-reset-history --apply

# CPR_LEVELS LONG: RVOL on
doppler run -- uv run pivot-backtest --all --universe-size 0 \
  --start 2025-01-01 --end 2026-03-30 \
  --strategy CPR_LEVELS --direction LONG \
  --rvol 1.0 \
  --cpr-min-close-atr 0.5 --min-price 50 --narrowing-filter \
  --save --force-rerun --quiet

# CPR_LEVELS SHORT: RVOL off
doppler run -- uv run pivot-backtest --all --universe-size 0 \
  --start 2025-01-01 --end 2026-03-30 \
  --strategy CPR_LEVELS --direction SHORT \
  --skip-rvol \
  --cpr-min-close-atr 0.5 --min-price 50 --narrowing-filter \
  --save --force-rerun --quiet

# FBR LONG: RVOL off
doppler run -- uv run pivot-backtest --all --universe-size 0 \
  --start 2025-01-01 --end 2026-03-30 \
  --strategy FBR --direction LONG \
  --failure-window 10 --skip-rvol \
  --save --force-rerun --quiet

# FBR SHORT: RVOL off
doppler run -- uv run pivot-backtest --all --universe-size 0 \
  --start 2025-01-01 --end 2026-03-30 \
  --strategy FBR --direction SHORT \
  --failure-window 10 --skip-rvol \
  --save --force-rerun --quiet
```

RVOL policy:

- CPR_LEVELS LONG uses RVOL 1.0
- CPR_LEVELS SHORT skips RVOL
- FBR LONG skips RVOL
- FBR SHORT skips RVOL
- `--risk-based-sizing` is a strategy override. Use it only when the matching backtest run used it.
- Leave it out when reproducing a baseline that ran with standard sizing.

`--force-rerun` recomputes the same deterministic `run_id` and overwrites that row. Use `--fresh-run`
when you want a new historical row for comparison while leaving the old one intact.

Backtest is the source of truth for strategy defaults. `daily-sim`, `daily-replay`, and
`daily-live` use the same resolved strategy params unless you pass explicit overrides.

### 6) Canonical long-run campaign

```bash
# production order = FBR -> CPR_LEVELS
doppler run -- uv run pivot-campaign --full-universe --start 2015-01-01 --end 2025-03-31

# or run a single leg explicitly
doppler run -- uv run pivot-backtest --all --universe-size 0 --strategy CPR_LEVELS --start 2015-01-01 --end 2025-03-31 --save

# cleanup transient artifacts before rerun
doppler run -- uv run pivot-clean

# inspect writer locks before retrying failed writers
uv run pivot-lock-status

# prune bloated local logs/caches safely (dry-run first)
uv run pivot-clean --dry-run --include-tmp-logs --older-than-days 14
uv run pivot-clean --include-tmp-logs --older-than-days 14
```

### 7) Analysis apps

```bash
doppler run -- uv run pivot-dashboard
doppler run -- uv run pivot-agent
doppler run -- uv run pivot-agent -q "Show SBIN win rate for 2023"
```

Dashboard URL: `http://127.0.0.1:9999`

### 8) Daily paper-trading workflow

Run 2 primary paper sessions each trading day: CPR_LEVELS LONG and CPR_LEVELS SHORT.
Paper trading IS the validation. See `docs/PAPER_TRADING_RUNBOOK.md` for the full operating guide.
When you compare a specific paper run to a backtest baseline, use the same explicit strategy flags
and sizing override. `--risk-based-sizing` must be present on both sides if that baseline used it;
otherwise leave it off.

For canonical runs, prefer named presets such as `CPR_LEVELS_RISK_LONG` and
`CPR_LEVELS_RISK_SHORT` instead of spelling out the full flag bundle. Reserve explicit flags for
ad hoc analysis only.

Before validation, make sure `KITE_ACCESS_TOKEN` is current in Doppler, then refresh local market
data from Kite into this repo's parquet files. The step-by-step ingestion runbook is in
`docs/KITE_INGESTION.md`.

Daily parquet now uses a baseline + overlay layout per symbol:

- `data/parquet/daily/<SYMBOL>/all.parquet`
- `data/parquet/daily/<SYMBOL>/kite.parquet`

`pivot-kite-ingest` writes recent daily catch-up rows into `kite.parquet`; DuckDB `v_daily`
dedupes and prefers that overlay automatically.

When run without an explicit symbol list, `pivot-kite-ingest` also refreshes the three major NSE
indexes used for regime checks: `NIFTY 50`, `NIFTY 100`, and `NIFTY 500`. Use
`--major-indexes-only` if you want only those index series.

The rebuild and validation steps below work against whatever local parquet history is already on
disk. They do not start a new Kite backfill from a fixed year like 2015.

On Windows, launch long `pivot-build` and `pivot-paper-trading daily-live` jobs in the
background and poll their log files under `.tmp_logs/` rather than blocking the interactive
terminal for the full runtime.

Operational automation:

```bash
# Read-only EOD readiness report before rerunning ingestion/builds
doppler run -- uv run pivot-eod-status --date today --trade-date <next_trading_date>

# Canonical replay validation bundle for one historical date
doppler run -- uv run pivot-paper-validate --trade-date <YYYY-MM-DD>

# Supervised live-paper launch with heartbeat/stdout/stderr files
doppler run -- uv run pivot-paper-supervisor -- \
  --multi --strategy CPR_LEVELS --trade-date today

# Dashboard HTTP smoke test once pivot-dashboard is running
uv run pivot-dashboard-smoke --base-url http://127.0.0.1:9999

# Validate the active 8-run CPR baseline registry
uv run pivot-baseline-registry --check-db
```

```bash
# One-time or occasional instrument refresh
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE

# First validation catch-up window: Tuesday, March 10, 2026 through Friday, March 20, 2026
doppler run -- uv run pivot-kite-ingest --from 2026-03-10 --to 2026-03-20 --symbols SBIN,RELIANCE
doppler run -- uv run pivot-kite-ingest --from 2026-03-10 --to 2026-03-20 --symbols SBIN,RELIANCE --5min --resume
doppler run -- uv run pivot-build --table pack --refresh-since 2026-03-10
```

```bash
# Monday live paper session
doppler run -- uv run pivot-paper-trading daily-live \
  --trade-date 2026-03-23 \
  --symbols SBIN,RELIANCE \
  --strategy CPR_LEVELS

# Historical replay with alerts and candle-by-candle audit trail
doppler run -- uv run pivot-paper-trading daily-replay \
  --trade-date 2026-03-23 \
  --symbols SBIN,RELIANCE \
  --strategy CPR_LEVELS \
  --direction LONG

# Paper-only cleanup before a fresh rerun (preserves baseline backtests)
doppler run -- uv run pivot-paper-trading cleanup --apply
```

Live-paper operator controls:

```bash
# Close selected symbols immediately using latest live mark/LTP; session keeps running.
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <session-id> --action close_positions --symbols SBIN,RELIANCE

# Flatten one running session.
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <session-id> --action close_all --reason operator_flatten

# Flatten both LONG and SHORT sessions for the trade date.
doppler run -- uv run pivot-paper-trading flatten-both --trade-date today --reason risk_off

# Reduce future-entry budget for one running session. Existing open positions are not resized.
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <session-id> --action set_risk_budget \
  --portfolio-value 500000 --max-positions 5 --reason reduce_short_risk

# Optional explicit diagnostic gate. Live-loop operator exits reconcile automatically.
doppler run -- uv run pivot-paper-trading reconcile --session-id <session-id> --strict
```

Manual/operator exits are immediate market-style paper exits using latest live LTP when available.
Strategy exits remain completed-5-minute-candle driven. Running live sessions reconcile automatically
after operator close/flatten actions; the standalone `reconcile` command is for diagnostics and gates.
Risk-budget changes affect future entries only; reduce current exposure with close commands.

Important distinctions:

- Replay and live paper execution remain on `Paper Sessions` at `/paper_ledger`.
- `/backtest` and `Strategy Analysis` remain backtest-only views.
- The local agent can inspect paper sessions and archived ledgers.

For full history cleanup before a fresh backtest or paper rerun, use:

```bash
doppler run -- uv run pivot-reset-history --apply
```

Use `pivot-paper-trading cleanup --apply` when you want to clear only paper sessions,
paper orders, alerts, feed state, and PAPER analytics rows. That is the safe rerun path
when you need to preserve baseline backtest runs.

## Architecture

```text
raw CSV -> pivot-convert -> parquet -> duckdb views/tables -> backtest engine -> dashboard cache -> reports
```

Core runtime tables:

- `market_day_state`
- `strategy_day_state`
- `intraday_day_pack`
- `cpr_daily`, `cpr_thresholds`, `atr_intraday`
- `backtest_results`, `run_metrics`

Shared portfolio execution model:

- `portfolio_value`
- `max_positions`
- `max_position_pct`

Paper trading is implemented end-to-end for live session state, replay/live validation, live feed adapters, strategy execution, flatten/close workflows, and archival. The operator runbook is in `docs/PAPER_TRADING_RUNBOOK.md`, and the implementation details are documented in `docs/DESIGN.md`.

## Troubleshooting

- `No 5-min Parquet found` -> run `pivot-convert`
- `Missing intraday_day_pack` -> for a recent catch-up use `pivot-build --table pack --refresh-since <YYYY-MM-DD>` (alias: `--since`); use `--force --allow-full-pack-rebuild` only when you intentionally want to rebuild the entire pack table
- Full-history runtime rebuilds now require both `--full-history` and `--staged-full-rebuild`; use the staged rebuild runbook in `docs/RUNTIME_REBUILD.md`
- Dashboard errors or stuck DB -> close dashboard before any new write run
- Dashboard data looks stale or empty -> restart `pivot-dashboard` in the foreground and make sure
  only one dashboard process is listening on `9999`

## Documentation

- [docs/README.md](docs/README.md) - documentation index and ownership map
- [STRATEGY.md](STRATEGY.md) - strategy definitions and parameters
- [docs/PAPER_TRADING_RUNBOOK.md](docs/PAPER_TRADING_RUNBOOK.md) - live-session operator runbook
- [docs/SETUP.md](docs/SETUP.md) - clean setup runbook and command profile
- [docs/KITE_INGESTION.md](docs/KITE_INGESTION.md) - local Kite refresh and parquet catch-up runbook
- [docs/RUNTIME_REBUILD.md](docs/RUNTIME_REBUILD.md) - safe full-history and repair rebuild runbook
- [docs/CODEMAP.md](docs/CODEMAP.md) - project architecture map
