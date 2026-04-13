# Backtest Performance Tuning

Status date: 2026-03-07

Goal: keep runs stable and fast by enforcing explicit full-history materialization before benchmark/backtest execution.

## Implemented Status (Redesign Complete)

- Strict run path is active for the Gold CLI workflow.
  - `pivot-gold prepare` now persists universe metadata and validates runtime coverage only.
  - `pivot-gold benchmark` fails fast if runtime tables are missing coverage for the saved universe.
- Hidden warm/rebuild behavior has been removed from the Gold entry points.
- Stability defaults remain in place:
  - bounded dynamic mode (`pivot-backtest --all` defaults to `--universe-size 51`)
  - data-quality exclusions for missing 5-min symbols
  - progress heartbeats via `--progress-file`

## Prior Failure Mode and Fix

Before redesign:
- Cold benchmark/backtest runs could silently trigger missing-symbol rebuilds for `intraday_day_pack` and `market_day_state`.
- This looked like hangs, caused large transient memory pressure, and could end in OOM on wide universes.

After redesign:
- Runtime materialization is explicit and front-loaded via `pivot-build`.
- Gold prepare/benchmark now validate coverage and surface gaps immediately.
- Benchmark timing reflects simulation work, not hidden rebuild work.

## Strict Runbook

### 1) Full Materialization (once per data refresh)

```bash
uv run pivot-build --force --full-history --staged-full-rebuild --duckdb-threads 4 --duckdb-max-memory 24GB --batch-size 64
uv run pivot-data-quality --refresh --limit 50

uv run pivot-gold prepare \
  --name gold_51 \
  --start 2015-01-01 \
  --end 2025-03-27 \
  --universe-size 51
```

Expected behavior:
- `prepare` saves the universe and prints coverage for `market_day_state` and `intraday_day_pack`.
- If coverage is incomplete, it prints explicit rebuild instructions and does not auto-rebuild.

### 2) Benchmark Gate (strict, no hidden rebuild)

```bash
uv run pivot-gold benchmark \
  --name gold_51 \
  --start 2015-01-01 \
  --end 2025-03-27 \
  --target-minutes 10 \
  --fail-on-breach \
  --progress-file data/progress/gold_51_benchmark.ndjson
```

If runtime coverage is incomplete, this command exits with a precondition error.

### 3) Production Backtests (run in yearly chunks)

```bash
uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2015-12-31 --progress-file data/progress/bt_2015.ndjson
uv run pivot-backtest --universe-name gold_51 --start 2016-01-01 --end 2016-12-31 --progress-file data/progress/bt_2016.ndjson
uv run pivot-backtest --universe-name gold_51 --start 2017-01-01 --end 2017-12-31 --progress-file data/progress/bt_2017.ndjson
```

Repeat yearly through the target horizon. Keep runs sequential (DuckDB single-writer).

## Cross-References

- Operational workflow: `docs/backtest-medallion-workflow.md`

## Update (2026-03-07): Pending Performance Work Implemented

- Fixed gold universe resolver regression:
  - Root cause: `get_liquid_symbols()` attempted to use `cpr_daily.volume` (column absent in legacy schema), swallowed exception, and returned an empty list.
  - Fix: dual path in `get_liquid_symbols()`.
    - Fast path: `cpr_daily` when `prev_volume` exists.
    - Fallback path: `v_daily` for legacy schemas.
- Added `prev_volume` to `cpr_daily` build output for fast turnover ranking from materialized data.
- Added materialized `run_metrics` table for dashboard/read paths.
  - Populated incrementally on run save.
  - `get_runs_with_metrics()` now reads from `run_metrics`.
  - Removes repeated per-request recomputation overhead.

### Operational Note

For the fastest liquid-universe resolution path, rebuild CPR once so `prev_volume` is present in `cpr_daily`:

```bash
uv run pivot-build --table cpr --force
```

If you skip this rebuild, the system still works via fallback to `v_daily` (correctness first, slower path).
