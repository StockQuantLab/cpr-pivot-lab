# Kite Ingestion Runbook

Use this workflow to keep `cpr-pivot-lab` self-contained after March 9, 2026 without copying parquet or DuckDB files from `nse-momentum-lab`.

## Source-of-truth rules

- `pivot-backtest`, `pivot-paper-trading daily-replay`, and `pivot-paper-trading walk-forward` must read only local parquet and DuckDB runtime tables.
- Kite API is used only for:
  - request-token exchange / token refresh
  - instrument master refresh
  - daily historical ingestion
  - 5-minute historical ingestion
  - `pivot-paper-trading daily-live`

## Major NSE indexes

The daily ingest pipeline now auto-includes the three broad market indexes used for regime checks:

- `NIFTY 50`
- `NIFTY 100`
- `NIFTY 500`

These are ingested alongside the normal NSE stock universe when you run `pivot-kite-ingest` without an explicit symbol list.

Use `--major-indexes-only` when you want to backfill or refresh just those index series.
This keeps index data available for market-direction gates without pulling every other NSE index into the default workflow.

## Files written

- Instrument cache: `data/raw/kite/instruments/NSE.csv`
- NSE equity allowlist: `data/NSE_EQUITY_SYMBOLS.csv` (**not in git** — copy from `nse-momentum-lab/data/` or download from NSE website; required for correct tradeable symbol count ~2,105)
- Optional raw snapshots: `data/raw/kite/daily/...` and `data/raw/kite/5min/...`
- Resume checkpoints: `data/raw/kite/checkpoints/*.json`
- Daily baseline parquet: `data/parquet/daily/<SYMBOL>/all.parquet`
- Daily Kite overlay parquet: `data/parquet/daily/<SYMBOL>/kite.parquet`
- 5-minute parquet: `data/parquet/5min/<SYMBOL>/<YEAR>.parquet`

The parquet schema matches the existing repo contract:

- Daily: `symbol,date,open,high,low,close,volume`
- 5-minute: `candle_time,open,high,low,close,volume,true_range,date,symbol`

## Daily parquet storage contract

`5min` is already partitioned by symbol and year:

- `data/parquet/5min/<SYMBOL>/<YEAR>.parquet`

`daily` now uses a two-file per-symbol layout:

- `data/parquet/daily/<SYMBOL>/all.parquet`
  - historical baseline
  - typically populated by CSV conversion or `copy_from_nse`
- `data/parquet/daily/<SYMBOL>/kite.parquet`
  - incremental overlay written by `pivot-kite-ingest`
  - safe append/replace target for recent Kite catch-up windows

DuckDB `v_daily` reads both files and dedupes on `(symbol, date)`, preferring
`kite.parquet` when the same date exists in both places. Processing code should
query `v_daily` or materialized runtime tables, not individual daily parquet files.

Compaction is a separate maintenance step:

```bash
doppler run -- uv run pivot-kite-ingest --compact-daily
```

That command merges `kite.parquet` back into `all.parquet` for the resolved symbols
and deletes the overlay file. Use it only when no repo readers are active.

## Windows reader/writer rule

On Windows, even read-only DuckDB/parquet readers can block atomic parquet file
replacement at the filesystem level. Do not overlap these with ingestion or rebuilds:

- `pivot-dashboard`
- `pivot-paper-trading`
- `pivot-backtest`
- `pivot-agent`
- `pivot-build`

`pivot-kite-ingest` and `pivot-build` now fail fast with a preflight error if they
detect conflicting repo processes.

## Step 1: Refresh the Kite token in Doppler

Before any ingestion run, make sure these secrets already exist in Doppler:

- `KITE_API_KEY`
- `KITE_API_SECRET`

If `KITE_ACCESS_TOKEN` has expired or is missing, refresh it with the CLI entry point
that exchanges a `request_token` for a new access token:

```bash
doppler run -- uv run pivot-kite-token --apply-doppler
```

Alias:

```bash
doppler run -- uv run pivot-kite-get-token --apply-doppler
```

Flow:

1. The command prints the Kite login URL.
2. Sign in to Kite in the browser.
3. Copy either the full redirected callback URL or just the `request_token`.
4. Paste it back into the terminal prompt.
5. The command exchanges the token and writes `KITE_ACCESS_TOKEN` to Doppler.

If you prefer a manual update, run the command without `--apply-doppler`, then persist the
token yourself:

```bash
doppler secrets set KITE_ACCESS_TOKEN '<access-token>'
```

If you already have a valid `KITE_ACCESS_TOKEN`, you can skip this step.

## Step 2: Refresh the instrument master

Refresh the local instrument master before the first ingestion run or whenever Zerodha changes the symbol map:

```bash
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE
```

## Step 3: Ingest the missing date window

Always start from the day after the last loaded date. For example, if the repo is already
loaded through `2026-03-20`, resume at `2026-03-21`.

### Daily bars

```bash
doppler run -- uv run pivot-kite-ingest \
  --from 2026-03-21 \
  --to 2026-03-23
```

If an earlier daily run was interrupted, rerun the same command with `--resume`.
The checkpoint lets the ingester recover only the unfinished symbol tail.

### 5-minute bars

`--resume` keeps a checkpoint under `data/raw/kite/checkpoints/` and skips already completed
symbols on rerun.

```bash
doppler run -- uv run pivot-kite-ingest \
  --from 2026-03-21 \
  --to 2026-03-23 \
  --5min \
  --resume
```

Optional raw CSV snapshots:

```bash
doppler run -- uv run pivot-kite-ingest \
  --from 2026-03-21 \
  --to 2026-03-23 \
  --5min \
  --resume \
  --save-raw
```

## Backfilling missing symbols (current-master mode)

The default `local-first` universe only updates symbols that already have local parquet.
New symbols listed on NSE — or symbols that were never ingested historically — are
invisible to the default ingester.

Use `--universe current-master` to resolve symbols directly from the Kite instrument
master instead of local parquet directories. This is required to backfill symbols that
have zero or incomplete local history.

```bash
# Check what's missing first (dashboard → /data_quality, Short History section)
doppler run -- uv run pivot-hygiene --check-stale

# Backfill daily bars for ALL tradeable NSE symbols
doppler run -- uv run pivot-kite-ingest \
  --universe current-master \
  --from 2015-01-01 \
  --to 2025-12-01 \
  --resume \
  --skip-existing

# Backfill 5-minute bars (takes several hours — use --resume for safe restart)
doppler run -- uv run pivot-kite-ingest \
  --universe current-master \
  --5min \
  --from 2015-01-01 \
  --to 2025-12-01 \
  --resume \
  --skip-existing
```

**How it works:**
- `--universe current-master` resolves symbols by cross-referencing two sources:
  1. `data/raw/kite/instruments/NSE.csv` (Kite instrument master, segment=NSE)
  2. `data/NSE_EQUITY_SYMBOLS.csv` (NSE equity allowlist, SERIES=EQ)
  The intersection gives ~2,105 true NSE equity stocks, excluding ETFs, REITs, bonds, and
  restricted instruments. Without the allowlist, `segment=NSE` alone returns ~9,356 rows.
- `--skip-existing` skips symbols whose parquet already covers the target date, making
  the run safe to re-execute incrementally
- `--resume` writes a checkpoint under `data/raw/kite/checkpoints/current-master_*.json`
  (namespaced separately from local-first checkpoints to prevent resume conflicts)

**Refreshing the NSE equity allowlist:**

`data/NSE_EQUITY_SYMBOLS.csv` is downloaded from NSE's official equity listing page
(`nseindia.com → Market Data → Equity → Securities in F&O → fo_mktlot.csv`, or the
equity bhavcopy). Refresh it when new companies list or you see coverage gaps:

1. Download the latest CSV from NSE's website.
2. Replace `data/NSE_EQUITY_SYMBOLS.csv` with the new file (must have `SYMBOL` and `SERIES` columns).
3. Run `pivot-kite-ingest --universe current-master --refresh-instruments` to pick up new symbols.

**After backfill, rebuild runtime tables:**

```bash
doppler run -- uv run pivot-build \
  --force \
  --full-history \
  --staged-full-rebuild \
  --batch-size 128
```

`pivot-build --force` alone is no longer sufficient -- it also requires
`--full-history --staged-full-rebuild` (and optionally `--allow-full-history-rebuild`
when no `--refresh-since` window is supplied) to acknowledge the full-history scan.
Prefer `--refresh-since` for incremental date windows instead.

**Impact on backtesting and paper trading:**

| Scenario | Impact |
|---|---|
| `--universe-name gold_51` | None — saved symbol list unchanged |
| `--symbols RELIANCE,...` | None — explicit list |
| `--all --universe-size N` | Minimal — new liquid symbols could enter top-N, but NSE top-51 by traded value are stable large-caps already in the system |
| `--all --universe-size 0` | More symbols available; all existing cached run_ids are unaffected (keyed by parameter hash) |
| Paper trading replay/walk-forward | None unless new symbols added to `--symbols` explicitly |
| Short-history symbols (< 1yr data) | Handled correctly — no entries in `market_day_state` for pre-ingestion dates, so backtest/paper trading simply skip those dates |

## Step 4: Rebuild local runtime tables

After daily and 5-minute parquet are updated, rebuild local DuckDB runtime tables:

These rebuild commands operate on the parquet history already present in this repo. They do not
call Kite and they do not assume a fixed start year like 2015; the effective date range is
whatever local parquet exists at the time you run them.

```bash
doppler run -- uv run pivot-build --refresh-since 2026-03-21
```

Do **not** use `--table pack` for the normal EOD/live-prep path. `--table pack` only refreshes
`intraday_day_pack`; it does not advance `cpr_daily`, `atr_intraday`, `cpr_thresholds`,
`market_day_state`, or `strategy_day_state`. Live paper does **not** need future-dated setup rows;
it needs the latest completed trading day's daily, 5-minute, and ATR data so the live runtime can
derive today's CPR setup from previous-day data plus live opening-range candles.

Only use a full-history rebuild when runtime state is inconsistent. That rebuild scans all local
parquet history already in this repo, not just the newly ingested Kite window. Bare `--force` alone
is rejected -- you must also pass `--full-history --staged-full-rebuild`, and when no
`--refresh-since` date is provided, add `--allow-full-history-rebuild` to acknowledge the cost:

```bash
doppler run -- uv run pivot-build \
  --force \
  --full-history \
  --staged-full-rebuild \
  --allow-full-history-rebuild \
  --duckdb-threads 4 \
  --duckdb-max-memory 24GB \
  --batch-size 128
```

If you are running the daily EOD pipeline for paper/live prep, use the guarded one-command path.
This prevents agents/operators from skipping `--refresh-instruments` or running the build before
today's candles are ingested:

```bash
doppler run -- uv run pivot-refresh \
  --eod-ingest \
  --date <today> \
  --trade-date <next_trading_date>
```

For example, after the market closes on 2026-04-29:

```bash
doppler run -- uv run pivot-refresh \
  --eod-ingest \
  --date 2026-04-29 \
  --trade-date 2026-04-30
```

`--eod-ingest` always runs this exact order and stops on the first failure:

1. `pivot-kite-ingest --refresh-instruments --exchange NSE`
2. `pivot-kite-ingest --from <today> --to <today> --skip-existing`
3. `pivot-kite-ingest --from <today> --to <today> --5min --resume --skip-existing`
4. `pivot-build --refresh-since <today>`
5. `pivot-paper-trading daily-prepare --trade-date <next_trading_date> --all-symbols`
6. `pivot-data-quality --date <next_trading_date>`

The final command must return `Ready YES`. Do not hand-run the individual steps unless you are
debugging a failed stage.
When redirected to a file, `pivot-refresh --eod-ingest` streams child command output directly, so
the log should show Kite ingestion progress instead of staying silent until a stage exits.
The command is rerun-safe by default: daily and 5-minute ingestion pass `--skip-existing`, so
already-covered parquet symbols are logged as skipped instead of fetched again. Use
`--force-ingest` only when you intentionally want to refetch existing candles.

If today's candles are already ingested and you only need to rebuild runtime tables, use
`pivot-refresh --prepare-paper` as the build/prepare gate:

```bash
doppler run -- uv run pivot-refresh \
  --since <today> \
  --prepare-paper \
  --trade-date <next_trading_date>
```

`pivot-refresh --prepare-paper` first runs `daily-prepare`. If previous completed-day
prerequisites are already present it skips the build; otherwise it runs the full runtime-table
refresh and re-runs `daily-prepare`. This is the preferred automation path because it fails closed
before live trading without asking operators to build future-date rows.

## Step 5: Validate the loaded data

Run the coverage check before backtests or paper trading:

`pivot-data-validate` reports the min/max dates that exist in the local runtime tables. It is a
coverage check over loaded data, not a command that re-ingests history from a specific year.

```bash
doppler run -- uv run pivot-data-validate
```

## Step 6: Refresh data quality issues

Run the full DQ scan after every ingestion + build cycle to keep the dashboard `/data_quality`
page current and to surface any OHLC violations, timestamp anomalies, or extreme candles
introduced by the new data:

```bash
doppler run -- uv run pivot-data-quality --refresh --full
```

This takes 1–5 minutes and writes results to `data_quality_issues` in `market.duckdb`.
The dashboard reads from this table on the `/data_quality` page.

For a quick parquet-presence-only check (faster, no OHLC scan):

```bash
doppler run -- uv run pivot-data-quality --refresh
```

## Step 7: Pre-filter symbols for next trading day

After ingestion and validation, run the paper readiness gate for the next trading day:

```bash
doppler run -- uv run pivot-paper-trading daily-prepare \
  --trade-date <next_trading_date> --all-symbols
```

This saves the dated universe snapshot and fails unless live setup prerequisites exist. For a
future/current live date, those prerequisites are previous completed-day data, not future rows:

- previous-trading-day `v_daily` history for the requested symbols
- previous-trading-day `v_5min` history for the requested symbols
- previous-trading-day `atr_intraday` history for the requested symbols
- previous-trading-day `cpr_thresholds` history for the requested symbols

Universe policy is stable by default. `daily-prepare --all-symbols` uses `canonical_full` as the
source of truth, creating it once from the broad local universe if needed, then copies the same list
to `full_YYYY_MM_DD` for the requested trade date. Do not shrink `canonical_full` or the dated
snapshot just because a few symbols have no data for that day.
If the dated snapshot already exists with a different symbol list, `daily-prepare` fails instead of
silently overwriting it. Use `--refresh-universe-snapshot` only after explicitly confirming the
canonical universe count and intended repair.

Full-universe runs intentionally tolerate sparse symbol/day gaps. If a few symbols have no
previous-day data or are suspended, the gate treats them as warnings and live/backtest skips those
symbols for that day. Broad gaps still fail closed: if more than 5% of the requested universe is
missing required previous-day data, treat it as an ingestion/runtime-table issue and fix it before
live.

Pre-market does not require today's intraday candles and must not require tomorrow's/today's
materialized state rows. A successful `daily-prepare` is now the automated gate; do not start
`daily-live` if it fails.
If current/future-date `market_day_state` or `strategy_day_state` rows exist while same-day
`intraday_day_pack` is absent, readiness fails closed because those rows are accidental
future-state data.

Run the fast readiness check as the final close-out gate:

```bash
doppler run -- uv run pivot-data-quality --date <next_trading_date>
```

Required result: `Ready YES`.

## Background execution on Windows

For long `pivot-build` runs, start the command in the background and poll the log instead of
keeping the terminal blocked for the entire rebuild:

```powershell
$logDir = Join-Path (Get-Location) '.tmp_logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$out = Join-Path $logDir 'pack_rebuild.out.log'
$err = Join-Path $logDir 'pack_rebuild.err.log'
Start-Process -FilePath 'C:\Program Files\PowerShell\7\pwsh.exe' `
  -ArgumentList '-Command', 'Set-Location ''C:\Users\kanna\github\cpr-pivot-lab''; uv run pivot-build --table pack --refresh-since 2026-03-21' `
  -RedirectStandardOutput $out `
  -RedirectStandardError $err
```

Poll with:

```powershell
Get-Content .tmp_logs\pack_rebuild.out.log -Tail 40
Get-Content .tmp_logs\pack_rebuild.err.log -Tail 40
```

## First validation sequence

1. Refresh the Kite token in Doppler if needed.
2. Refresh the instrument master.
3. Ingest daily bars for the missing date window.
4. Ingest 5-minute bars for the same window.
5. Optionally compact finished daily overlays back into `all.parquet` once no repo readers are active:

```bash
doppler run -- uv run pivot-kite-ingest --compact-daily
```

6. Refresh all local runtime tables with `pivot-build --refresh-since <window-start>`.
7. Run `pivot-data-validate`.
8. Run `pivot-data-quality --refresh --full` to update the DQ issue table.
9. Run the live setup gate for the next trading day's live paper session:

```bash
doppler run -- uv run pivot-paper-trading daily-prepare \
  --trade-date <next_trading_date> --all-symbols
doppler run -- uv run pivot-data-quality --date <next_trading_date>
```

10. If validation looks acceptable, run walk-forward validation:

```bash
doppler run -- uv run pivot-paper-trading walk-forward \
  --start-date 2026-03-21 \
  --end-date 2026-03-23 \
  --symbols SBIN,RELIANCE \
  --strategy CPR_LEVELS
```

11. If validation looks acceptable, start the live paper session:

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --trade-date today \
  --all-symbols \
  --strategy CPR_LEVELS --direction LONG \
  --min-price 50 --cpr-min-close-atr 0.5 --narrowing-filter
```

## CLI Reference

### pivot-kite-ingest

| Flag | Default | Description |
|------|---------|-------------|
| `--today` | off | Use today (Asia/Kolkata) as the ingestion date |
| `--date YYYY-MM-DD` | -- | Single trading date (shorthand for `--from` / `--to` on the same day) |
| `--symbols SYM1,SYM2` | -- | Comma-separated symbol list; bypasses universe resolution |
| `--symbols-file PATH` | -- | Text file with one symbol per line (`#` comments and blank lines ignored) |
| `--missing` | off | Auto-detect tradeable symbols with no local parquet and ingest them |
| `--no-filter-tradeable` | off | Include all parquet symbols even if absent from the current instrument master |
| `--update-features` | off | Run a full local runtime-table rebuild after ingestion completes |
| `--chunk-days N` | 60 | 5-minute ingestion chunk size in calendar days |
| `--daily-chunk-days N` | 2000 | Daily ingestion chunk size in calendar days (under Kite's 2000-candle limit) |

For the full flag list see `scripts/kite_ingest.py` argparse (universe, resume, skip-existing, save-raw, etc.).

### pivot-build

| Flag | Default | Description |
|------|---------|-------------|
| `--refresh-date YYYY-MM-DD` | -- | Exact-day incremental build; limits refresh to one trade date. Combine with `--symbols`, `--symbols-file`, or `--missing` for symbol-scoped builds |
| `--symbols SYM1,SYM2` | -- | Rebuild state/strategy/pack for listed symbols only |
| `--symbols-file PATH` | -- | Text file with one symbol per line |
| `--missing` | off | Auto-detect symbols in parquet that are absent from `market_day_state` and rebuild them |
| `--universe-name NAME` | -- | Use symbols from a saved backtest universe (e.g. `gold_51`). Requires `--table` |
| `--status` | off | Print current table row counts and exit |
| `--table CHOICE` | all | Build only one table. Choices: `cpr`, `atr`, `thresholds`, `virgin`, `or`, `state`, `strategy`, `pack`, `meta` |
| `--resume` | off | Resume an interrupted pack build; skips symbols already present in `intraday_day_pack` |
| `--pack-lookback N` | 10 | RVOL lookback days for `intraday_day_pack` build |
| `--allow-full-history-rebuild` | off | Required for `--force --staged-full-rebuild` when no `--refresh-since` is supplied |

For the full flag list see `scripts/build_tables.py` argparse (`--force`, `--refresh-since`, `--full-history`, `--staged-full-rebuild`, `--batch-size`, `--duckdb-threads`, `--duckdb-max-memory`, etc.).

### pivot-data-quality

| Flag | Default | Description |
|------|---------|-------------|
| `--date YYYY-MM-DD` | -- | Trade-date readiness gate: checks whether runtime tables are populated for the given date. Alias: `--trade-date` |
| `--window-start YYYY-MM-DD` | -- | Window start for a lightweight bounded DQ report |
| `--window-end YYYY-MM-DD` | -- | Window end for a lightweight bounded DQ report |
| `--issue-code CODE` | -- | Filter displayed issues by code (e.g. `OHLC_VIOLATION`, `TIMESTAMP_INVALID`) |
| `--limit N` | 100 | Max rows to print (0 = all) |

For the full flag list see `scripts/data_quality.py` argparse (`--refresh`, `--full`, `--show-inactive`).
