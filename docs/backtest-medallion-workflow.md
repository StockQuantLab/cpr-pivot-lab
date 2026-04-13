# Backtest Medallion Workflow

Purpose: make CPR Pivot Lab operate with the same discipline as `nse-momentum-lab`.

## Layers

### Bronze
- Raw CSV to parquet conversion.
- Command:

```bash
uv run pivot-convert
```

Outputs:
- `data/parquet/5min/<SYMBOL>/<YEAR>.parquet`
- `data/parquet/daily/<SYMBOL>/all.parquet`
- `data/parquet/daily/<SYMBOL>/kite.parquet`

### Silver
- Full runtime feature/state materialization in DuckDB.
- Commands:

```bash
uv run pivot-build --force --full-history --staged-full-rebuild --duckdb-threads 4 --duckdb-max-memory 24GB --batch-size 64
uv run pivot-data-quality --refresh --limit 50
uv run pivot-data-quality --date 2026-03-27
```

Silver tables:
- `cpr_daily`
- `atr_intraday`
- `cpr_thresholds`
- `virgin_cpr_flags`
- `or_daily`
- `market_day_state`
- `intraday_day_pack`
- `dataset_meta`
- `data_quality_issues`

### Gold
- Fixed reproducible universe plus strict runtime-coverage validation and benchmark gate.
- Commands:

```bash
# 1) Persist a fixed universe and validate runtime coverage
uv run pivot-gold prepare \
  --name gold_51 \
  --start 2015-01-01 \
  --end 2025-03-27 \
  --universe-size 51

# 2) Inspect saved universes
uv run pivot-gold status --show-symbols

# 3) Run benchmark gate (target 10 min, strict precondition)
uv run pivot-gold benchmark \
  --name gold_51 \
  --start 2015-01-01 \
  --end 2025-03-27 \
  --target-minutes 10 \
  --fail-on-breach \
  --progress-file data/progress/gold_benchmark.ndjson
```

## Runtime Preconditions

- Backtest and benchmark runs are expected to start only after Silver materialization is complete.
- `pivot-gold prepare` validates symbol coverage in `market_day_state` and `intraday_day_pack` and reports gaps.
- `pivot-gold benchmark` fails fast when runtime coverage is incomplete.
- Gold workflow does not auto-rebuild missing runtime subsets during `prepare` or `benchmark`.

## Backtest Execution Modes

- Reproducible fixed-universe mode:

```bash
uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2025-03-27 --progress-file data/progress/backtest.ndjson
```

- Dynamic liquid-universe mode (bounded default):

```bash
uv run pivot-backtest --all --universe-size 51 --start 2015-01-01 --end 2025-03-27 --progress-file data/progress/backtest.ndjson
```

For long windows, prefer yearly chunks for operational stability.

## Readiness Checklist

System is ready for fast repeated runs when:
- Bronze parquet exists (`parquet_5min=true`, `parquet_daily=true`).
- Silver tables are fully materialized (staged `pivot-build --force --full-history --staged-full-rebuild ...` completed).
- Data quality is refreshed and the target trade date passes readiness checks (`pivot-data-quality --refresh` + `pivot-data-quality --date <trade-date>`).
- Gold universe is persisted and coverage-validated (`pivot-gold prepare`).
- Benchmark gate passes for target window (`pivot-gold benchmark --fail-on-breach`).

## Notes

- `data_quality_issues` tracks `MISSING_5MIN_PARQUET` and excludes these symbols from default `--all` selection.
- Run backtests sequentially (DuckDB single-writer). Do not launch two `pivot-backtest` writes at the same time.
- `--force-rerun` recomputes the same deterministic `run_id`; use `--fresh-run` when you want a new
  historical row without touching the prior result.
- Keep this document aligned with `docs/performance-tuning.md`.
