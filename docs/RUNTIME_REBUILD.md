# Data Rebuild Runbook

This runbook covers every scenario where DuckDB runtime tables or parquet data need to be
rebuilt. Follow the matching scenario exactly. Each scenario is self-contained and repeatable.

---

## What Lives Where

```
data/
  raw/          ← NEVER DELETE — original Zerodha CSV exports (source of truth)
    5min/       ← 5-minute OHLCV CSVs by symbol
    daily/      ← Daily OHLCV CSVs by symbol
    kite/       ← Kite API incremental downloads

  parquet/      ← Derived from raw/. Safe to delete and regenerate.
    5min/       ← SYMBOL/YEAR.parquet  (naive IST datetime, no timezone)
    daily/      ← SYMBOL/all.parquet + SYMBOL/kite.parquet  (date32 column only)

  market.duckdb ← Derived from parquet/. Safe to delete and regenerate.
                  Contains: cpr_daily, atr_intraday, cpr_thresholds, or_daily,
                  market_day_state, strategy_day_state, intraday_day_pack,
                  virgin_cpr_flags, backtest_results, run_metrics, …
```

### DuckDB Table Build Order and Dependencies

```
cpr_daily          ← daily parquet (prev-day H/L/C → CPR for next trade_date)
atr_intraday       ← 5-min parquet (last 12 candles of prior day)
cpr_thresholds     ← cpr_daily (rolling P50 per symbol)
or_daily           ← 5-min parquet (opening-range slot aggregates)
market_day_state   ← cpr_daily + atr + thresholds + or_daily + virgin_cpr_flags*
strategy_day_state ← market_day_state + 5-min parquet
intraday_day_pack  ← 5-min parquet (full candle arrays per day)
virgin_cpr_flags   ← intraday_day_pack (built LAST — uses pack arrays, not raw parquet)
```

> **Note on virgin**: `market_day_state` is built before `virgin_cpr_flags` (because virgin
> needs `intraday_day_pack` first). On a fresh build, `prev_is_virgin` is initially `FALSE`.
> After the full build completes, run `--table state` once more to backfill correct values.
> See [Post-Build Step](#post-build-step-refresh-virgin-flags) below.

---

## How to Check Current Status

```bash
doppler run -- uv run pivot-build --status
```

Expected output when everything is healthy:

```
 cpr_daily             ~4,050,000 rows   2105 symbols
 atr_intraday          ~3,320,000 rows   2105 symbols
 cpr_thresholds        ~4,050,000 rows   2105 symbols
 or_daily              ~2,910,000 rows   2105 symbols
 market_day_state      ~2,900,000 rows   2105 symbols
 strategy_day_state    ~2,900,000 rows   2105 symbols
 intraday_day_pack     ~3,320,000 rows   2105 symbols
 virgin_cpr_flags      ~4,050,000 rows   2105 symbols
 dataset_meta                  1 row
```

---

## Scenario 1 — Full Reset (parquet + DuckDB both wrong)

**Use when:** date logic was wrong, CSV re-export needed, or parquet is corrupted.
**Prerequisite:** raw CSVs in `data/raw/` are intact and correct.

### Step 1 — Stop all running processes

```bash
# Kill dashboard (holds DuckDB file lock on Windows)
taskkill /IM python.exe /F
```

### Step 2 — Delete parquet and DuckDB

```powershell
# PowerShell — preserves raw/ CSVs
Remove-Item "data\parquet\5min"  -Recurse -Force
Remove-Item "data\parquet\daily" -Recurse -Force
Remove-Item "data\market.duckdb"     -Force -ErrorAction SilentlyContinue
Remove-Item "data\market.duckdb.wal" -Force -ErrorAction SilentlyContinue
Remove-Item "data\market.duckdb.tmp" -Force -ErrorAction SilentlyContinue
```

Verify CSVs survived:

```powershell
(Get-ChildItem "data\raw" -Recurse -Filter "*.csv").Count   # must be > 3000
(Get-ChildItem "data\parquet" -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue).Count  # must be 0
```

### Step 3 — Reconvert CSVs to parquet

```bash
doppler run -- uv run pivot-convert --overwrite
```

Expected: `DONE ✓ 175,000,000+ total 5-min candles converted`

Verify a sample file has naive IST timestamps (no timezone):

```powershell
doppler run -- uv run python -c "
import pyarrow.parquet as pq
f = pq.read_table('data/parquet/5min/RELIANCE/2015.parquet')
print(f.schema.field('candle_time'))       # must be: timestamp[us]  (no tz)
print(f.column('candle_time')[0].as_py())  # must be: 2015-04-01 09:15:00  (not 03:45)
"
```

### Step 4 — Build recent tables first (fast, usable in minutes)

```bash
doppler run -- uv run pivot-build --refresh-since 2025-01-01 --batch-size 64
```

Expected tables after this step: all 8 tables built for dates ≥ 2025-01-01.
Approximate time: 20–40 minutes (pack is the slowest).

### Step 5 — Refresh virgin flags (post-build correction)

```bash
doppler run -- uv run pivot-build --table state --refresh-since 2025-01-01
```

This rebuilds `market_day_state` now that `virgin_cpr_flags` exists, so `prev_is_virgin`
is correct. See [Post-Build Step](#post-build-step-refresh-virgin-flags).

### Step 6 — Verify status

```bash
doppler run -- uv run pivot-build --status
```

### Step 7 — Schedule full-history rebuild (optional, background)

```bash
doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild --batch-size 64 ^
  2>&1 | Tee-Object ".tmp_logs\full_rebuild.log"
```

This extends all tables back to 2015. Run overnight. Use `--resume-from <table>` if interrupted.

---

## Scenario 2 — Tables-Only Reset (parquet is correct, DuckDB is wrong)

**Use when:** `market.duckdb` is corrupted, WAL is stuck, or a build was interrupted
mid-way and left tables in an inconsistent state.

### Step 1 — Stop all running processes

```bash
taskkill /IM python.exe /F
```

### Step 2 — Delete only the DuckDB file

```powershell
Remove-Item "data\market.duckdb"     -Force -ErrorAction SilentlyContinue
Remove-Item "data\market.duckdb.wal" -Force -ErrorAction SilentlyContinue
Remove-Item "data\market.duckdb.tmp" -Force -ErrorAction SilentlyContinue
```

### Step 3 — Rebuild recent tables

```bash
doppler run -- uv run pivot-build --refresh-since 2025-01-01 --batch-size 64
```

### Step 4 — Refresh virgin flags

```bash
doppler run -- uv run pivot-build --table state --refresh-since 2025-01-01
```

### Step 5 — Verify

```bash
doppler run -- uv run pivot-build --status
```

---

## Scenario 3 — Incremental Update (new data ingested via Kite)

**Use when:** new dates were added via `pivot-build --table pack --refresh-since` or Kite
ingestion, and you need to extend all runtime tables to cover them.

```bash
# Replace YYYY-MM-DD with the first new trade date
doppler run -- uv run pivot-build --refresh-since YYYY-MM-DD --batch-size 64

# Refresh virgin flags after pack is updated
doppler run -- uv run pivot-build --table state --refresh-since YYYY-MM-DD
```

---

## Scenario 4 — Single Table Rebuild

Rebuild one table without touching others:

```bash
# Available table names: cpr, atr, thresholds, virgin, or, state, strategy, pack, meta
doppler run -- uv run pivot-build --table <name> --refresh-since YYYY-MM-DD --batch-size 64
```

Examples:

```bash
# Rebuild only CPR daily (e.g. after correcting pivot formula)
doppler run -- uv run pivot-build --table cpr --refresh-since 2025-01-01

# Rebuild only intraday_day_pack for gold_51 symbols
doppler run -- uv run pivot-build --table pack --refresh-since 2025-01-01 \
  --universe-name gold_51 --batch-size 64

# Rebuild only market_day_state after virgin_cpr_flags was updated
doppler run -- uv run pivot-build --table state --refresh-since 2025-01-01
```

---

## Post-Build Step: Refresh Virgin Flags

`build_all` builds `virgin_cpr_flags` last (it needs `intraday_day_pack` to avoid scanning
175M raw candles). This means on a fresh build, `market_day_state.prev_is_virgin` is `FALSE`
for all rows. After the full build completes, always run:

```bash
doppler run -- uv run pivot-build --table state --refresh-since <same-since-date>
```

This takes 1–2 minutes and ensures `prev_is_virgin` reflects actual virgin CPR days.

---

## Full-History Rebuild (Background, Overnight)

Extends all tables back to 2015 (or however far parquet goes).
Use `--staged-full-rebuild` so you can resume from the failed table if interrupted.

```bash
doppler run -- uv run pivot-build \
  --force \
  --full-history \
  --staged-full-rebuild \
  --batch-size 64 \
  2>&1 | Tee-Object ".tmp_logs\full_rebuild.log"
```

Resume after interruption:

```bash
doppler run -- uv run pivot-build \
  --force \
  --full-history \
  --staged-full-rebuild \
  --resume-from pack \
  --batch-size 64 \
  2>&1 | Tee-Object ".tmp_logs\full_rebuild_resume.log"
```

---

## Rules

- **Never** delete `data/raw/`. Those CSVs are the source of truth.
- **Never** run `pivot-build` and `pivot-backtest` at the same time (DuckDB write lock).
- **Never** use `pivot-build --table pack --force` without `--allow-full-pack-rebuild`
  (it drops the entire pack table).
- **Always** run `--table state` after any virgin or pack rebuild to refresh `prev_is_virgin`.
- Prefer `--refresh-since` over `--force` for routine updates — it is incremental and safe.
- After a full reset (Scenario 1), always start with `--refresh-since 2025-01-01` before
  attempting a full-history rebuild — this gives you a working backtest environment quickly.
