# Backtest Optimization Plan

Date: 2026-03-08
Owner: CPR Pivot Lab
Status: Planning (pre-implementation)

## 1) Scope

This document captures the full optimization plan before code changes start.

In scope:
- Backtest runtime performance and memory stability
- Strategy setup filtering efficiency (CPR_LEVELS, FBR, VIRGIN_CPR)
- Metrics correctness and consistency (`run_metrics` vs runtime summary)
- Save-path efficiency and reliability

Out of scope (for this phase):
- Strategy alpha changes or parameter tuning
- UI/dashboard redesign
- New data ingestion pipelines

## 2) Problem Statement

Feature tables were built with a long preprocessing run, but full-universe backtests remained slow and unstable. This is expected with the current architecture because the main bottleneck shifted from feature generation to runtime materialization and simulation.

Current observed behavior:
- `intraday_day_pack` and runtime tables exist and are usable.
- `gold_51` runs are workable.
- Full-universe runs can degrade system performance and fail from memory pressure.

## 3) Root-Cause Findings

### F1. Runtime loads too much data at once

The current run path fetches setups for all symbols, then prefetches all day packs for all setups before simulation.

Code evidence:
- `run()` setup fetch: `engine/cpr_atr_strategy.py` (`all_setups_df = ...`)
- `run()` day-pack prefetch: `engine/cpr_atr_strategy.py` (`_prefetch_day_pack_batch(all_setups_df)`)
- Symbol partitioning of full setup frame: `engine/cpr_atr_strategy.py`

Impact:
- Peak memory scales with universe size and setup count.
- Runtime instability on large universes.

### F2. `intraday_day_pack` decode is Python-heavy

`_prefetch_day_pack_batch()` converts list columns to Python lists, then rebuilds a Polars DataFrame per `(symbol, trade_date)`.

Code evidence:
- `engine/cpr_atr_strategy.py`: `_prefetch_day_pack_batch()`
- Per-column `.to_list()` calls
- Per-day `pl.DataFrame(...)` reconstruction

Impact:
- High object churn and heap growth.
- Significant CPU overhead before strategy logic even begins.

### F3. Strategy rejection happens too late

FBR and VIRGIN_CPR setup filtering is only partially pushed to SQL. Many candidates are rejected only after full day data is loaded and scanned.

Code evidence:
- Setup query path: `engine/cpr_atr_strategy.py` (`_get_all_setups_batch`)
- FBR runtime rejection: `engine/cpr_atr_strategy.py` (`_simulate_day_fbr`)
- VCPR runtime rejection: `engine/cpr_atr_strategy.py` (`_simulate_day_virgin_cpr`)

Impact:
- Unnecessary day-pack fetch + simulation for invalid candidates.

### F4. Metrics basis mismatch (correctness issue)

Runtime advanced metrics normalize by `params.capital`; `run_metrics` normalizes by `symbol_count * 100000`.

Code evidence:
- Runtime: `engine/cpr_atr_strategy.py` (`_advanced_metrics`, `capital = ...`)
- Persisted metrics: `db/duckdb.py` (`refresh_run_metrics`, `symbol_count * 100000.0`)

Impact:
- Drawdown %, annual return %, and Calmar can disagree between summary and DB metrics for multi-symbol runs.

### F5. `run_metrics.strategy` currently mixes label and strategy code

`run_metrics.strategy` uses `COALESCE(rm.label, rm.strategy, rb.run_id)`.

Code evidence:
- `db/duckdb.py`: `refresh_run_metrics()`

Impact:
- Hard to filter/group reliably by strategy.

### F6. Save path does extra heavyweight post-processing

`store_backtest_results()` performs delete/insert and immediately recomputes `run_metrics`.

Code evidence:
- `db/duckdb.py`: `store_backtest_results()`, `refresh_run_metrics([run_id])`

Impact:
- Extra end-of-run latency, especially for large runs.

### F7. Pack schema is wide and memory-expensive (secondary)

`intraday_day_pack` stores `VARCHAR[]` time arrays and `DOUBLE[]` numeric arrays, including RVOL baseline arrays.

Code evidence:
- `db/duckdb.py`: `build_intraday_day_pack()` schema and insert SQL

Impact:
- Larger IO and memory footprint than necessary.
- Not the primary blocker, but relevant at scale.

## 4) Objectives and Success Metrics

Primary objectives:
- Stabilize full-universe runs (no memory allocation failures).
- Reduce full-universe runtime materially without strategy behavior changes.
- Make metrics consistent and auditable.

Performance targets (phase-wise):
- Phase 1-2: reduce peak memory by 50%+ and runtime by 30%+ on large runs.
- Phase 3: reduce setup candidate volume for FBR/VCPR by 30-60% pre-simulation.
- Phase 4: zero mismatch between runtime summary metrics and `run_metrics` basis.

Correctness targets:
- Identical trade outputs for unchanged strategy rules after runtime refactor.
- Deterministic run IDs for same params.

## 5) Implementation Plan (Exact Tasks)

## Phase 0: Guardrails and Baseline Instrumentation

### T0.1 Add runtime telemetry markers
- Files:
  - `engine/cpr_atr_strategy.py`
- Changes:
  - Log setup row count, day-pack row count, batch timing, per-phase timing.
  - Track memory-safe progress points at chunk boundaries.
- Expected impact:
  - Faster diagnosis and safer rollout.

### T0.2 Add plan docs index pointer
- Files:
  - `STATUS.md`
- Changes:
  - Link to this plan under current optimization status.
- Expected impact:
  - Team visibility.

## Phase 1: Runtime Batching (Highest Priority)

### T1.1 Add runtime batch parameter
- Files:
  - `engine/cpr_atr_strategy.py`
  - `engine/run_backtest.py`
  - `tests/test_cli.py`
- Changes:
  - Add `runtime_batch_size` to `BacktestParams` (default `32`).
  - Add CLI flag `--runtime-batch-size`.
  - Validation: must be integer >= 1.
  - Include in run hash.
- Expected impact:
  - Enables bounded memory execution.

### T1.2 Refactor `run()` to chunk symbol execution
- Files:
  - `engine/cpr_atr_strategy.py`
  - `tests/test_strategy.py`
- Changes:
  - Replace all-at-once setup/day-pack prefetch with per-batch pipeline:
    - fetch setups for batch
    - prefetch day-pack for batch
    - simulate batch
    - release batch data
  - Keep cache-hit behavior unchanged.
- Expected impact:
  - Major reduction in peak memory.
  - Full-universe stability improvement.

## Phase 2: Day-Pack Decode Refactor (Array-First Data Shape)

### T2.1 Introduce `DayPack` runtime container
- Files:
  - `engine/cpr_atr_strategy.py`
- Changes:
  - Add dataclass:
    - `times`, `opens`, `highs`, `lows`, `closes`, `volumes`, `rvol_baseline`
  - Store arrays per day without constructing `pl.DataFrame`.
- Expected impact:
  - Significant reduction in object churn.

### T2.2 Rewrite `_prefetch_day_pack_batch()` output format
- Files:
  - `engine/cpr_atr_strategy.py`
  - `tests/test_strategy.py`
- Changes:
  - Return `dict[symbol][trade_date] -> DayPack`.
  - Remove per-day DataFrame reconstruction.
  - RVOL baseline remains index-aligned array.
- Expected impact:
  - Faster prefetch and lower memory.

### T2.3 Remove tuple-key RVOL dict path
- Files:
  - `engine/cpr_atr_strategy.py`
- Changes:
  - Replace `(trade_date, time)` lookup map with direct per-candle index access.
- Expected impact:
  - Lower memory and faster RVOL checks.

## Phase 3: Hot Path Strategy Simulation Refactor

### T3.1 Replace `iter_rows(named=True)` setup iteration
- Files:
  - `engine/cpr_atr_strategy.py`
- Changes:
  - Iterate setups by column arrays / index view, not dict-per-row.
- Expected impact:
  - Lower Python overhead per setup.

### T3.2 Add shared array scan helpers
- Files:
  - `engine/cpr_atr_strategy.py`
  - `engine/cpr_atr_utils.py`
- Changes:
  - Helpers for:
    - time-window index bounds
    - first close above/below threshold
    - first intersection conditions
- Expected impact:
  - Removes repetitive DataFrame filters.

### T3.3 Port CPR/FBR/VCPR simulation to `DayPack`
- Files:
  - `engine/cpr_atr_strategy.py`
  - `tests/test_strategy.py`
- Changes:
  - Refactor:
    - `_simulate_day_cpr_levels`
    - `_simulate_day_fbr`
    - `_simulate_day_virgin_cpr`
    - `_simulate_trade`
  - Preserve exact strategy logic.
- Expected impact:
  - Major CPU savings in simulation loop.

## Phase 4: Strategy-Specific Precompute and SQL Pushdown

### T4.1 Add `strategy_day_state` runtime table
- Files:
  - `db/duckdb.py`
  - `scripts/build_tables.py`
  - `tests` (DB build tests as applicable)
- Changes:
  - Materialize strategy-useful derived fields:
    - OR/ATR ratio
    - gap %
    - open-side vs CPR
    - open-to-CPR ATR distance
    - first close beyond TC/BC markers
    - optional first breakout/touch indices
- Expected impact:
  - Shrinks candidate rows before simulation.

### T4.2 Tighten `_get_all_setups_batch()` by strategy
- Files:
  - `engine/cpr_atr_strategy.py`
- Changes:
  - Join `market_day_state` with `strategy_day_state`.
  - Push more FBR and VCPR eligibility checks into SQL.
  - For trend filter off-path, reduce unnecessary date scan range.
- Expected impact:
  - Lower setup row volume and less wasted prefetch.

## Phase 5: Metrics and Reporting Correctness (Must-Fix)

### T5.1 Unify capital basis across runtime and DB metrics
- Files:
  - `engine/cpr_atr_strategy.py`
  - `db/duckdb.py`
- Changes:
  - Decide and enforce one basis:
    - recommended: `capital_per_symbol` with `total_allocated_capital`.
  - Remove hardcoded denominator assumptions from `refresh_run_metrics`.
- Expected impact:
  - Reliable DD/return/Calmar comparison.

### T5.2 Use requested backtest window for annualization
- Files:
  - `engine/cpr_atr_strategy.py`
  - `db/duckdb.py`
- Changes:
  - Annual return should use run start/end params, not first/last trade date.
- Expected impact:
  - Prevents sparse-strategy overstatement.

### T5.3 Split strategy identity from label
- Files:
  - `db/duckdb.py`
  - UI/query code paths as needed
- Changes:
  - Add `strategy_code` and `label` fields distinctly in `run_metrics`.
- Expected impact:
  - Correct grouping/filtering behavior.

## Phase 6: Save-Path Efficiency and Atomicity

### T6.1 Wrap delete+insert+metrics refresh in one transaction
- Files:
  - `db/duckdb.py`
- Changes:
  - Ensure atomic save path in `store_backtest_results`.
- Expected impact:
  - Better integrity and slightly reduced overhead.

### T6.2 Add `run_daily_pnl` materialization
- Files:
  - `db/duckdb.py`
- Changes:
  - Persist daily run-level PnL.
  - Compute drawdown and related metrics from daily series when possible.
- Expected impact:
  - Faster metrics refresh on large trade tables.

## Phase 7: Optional Storage Compaction

### T7.1 Compact day-pack schema
- Files:
  - `db/duckdb.py`
  - migration/build scripts
- Changes:
  - Consider replacing `VARCHAR[]` time with minute-index array.
  - Evaluate `REAL[]` for OHLCV/rvol arrays where precision is acceptable.
- Expected impact:
  - Reduced storage and IO footprint.
  - Secondary priority.

## 6) Rollout Sequence (PR-by-PR)

PR1:
- T1.1, T1.2
- Tests for batching parity

PR2:
- T2.1, T2.2, T2.3
- Decode and runtime container refactor

PR3:
- T3.1, T3.2, T3.3
- Hot path strategy simulation refactor

PR4:
- T5.1, T5.2, T5.3
- Metrics correctness and schema cleanup

PR5:
- T6.1, T6.2
- Save-path + metrics refresh optimization

PR6:
- T4.1, T4.2
- Strategy-specific precompute

PR7 (optional):
- T7.1
- Storage compaction

## 7) Validation and QA Plan

Unit/regression validation:
- Existing tests:
  - `tests/test_strategy.py`
  - `tests/test_cli.py`
  - `tests/test_cpr_utils.py`
- Add parity tests ensuring no behavior change after refactor:
  - same inputs -> same trade count/win/loss/exit reasons/PnL

Performance validation:
- Compare before/after for:
  - peak memory
  - stage timings:
    - setup fetch
    - day-pack prefetch
    - simulation
    - save+metrics
- Run on:
  - smoke subset
  - `gold_51`
  - full universe (once stable)

Correctness validation:
- Runtime summary metrics and `run_metrics` must align within rounding.
- Strategy filter/grouping in dashboard/query paths must use `strategy_code`.

## 8) Risks and Mitigations

Risk:
- Array-first refactor can introduce behavior drift.
Mitigation:
- Golden parity tests on representative fixtures before and after each phase.

Risk:
- Schema changes may break existing dashboards/queries.
Mitigation:
- Backward-compatible columns during migration window.

Risk:
- Precompute table increases build time.
Mitigation:
- Keep derived fields minimal and strategy-focused; batch builds.

## 9) Decisions Required Before Coding

1. Capital model:
- Choose one and enforce globally.
- Recommendation: `capital_per_symbol` + explicit `total_allocated_capital`.

2. Priority order:
- Recommendation: PR1 -> PR2 -> PR3 -> PR4 before any new strategy tuning.

3. Compatibility policy:
- Confirm whether old `run_metrics` rows should be migrated or rebuilt from trades.

## 10) Definition of Done

Optimization phase is complete when:
- Full-universe run is stable (no memory allocation failures).
- Runtime performance improves materially against baseline.
- Metrics are internally consistent and auditable.
- Strategy behavior remains unchanged except where explicitly intended.
- Documentation and operational steps are updated.

## 11) Implementation Progress Log

### 2026-03-08 - Phase 1 Implemented (Runtime Batching)

Implemented:
- Added `runtime_batch_size` in `BacktestParams` and run hash key.
- Added CLI flag `--runtime-batch-size` with validation.
- Refactored `run()` to fetch/simulate symbols in batches instead of all-at-once fetch.
- Added tests for CLI flag presence/validation and batched setup-fetch behavior.

Validation:
- `uv run ruff check engine/cpr_atr_strategy.py engine/run_backtest.py tests/test_strategy.py tests/test_cli.py`
- `uv run pytest tests/test_strategy.py tests/test_cli.py -q`

Gold baseline benchmark (same command, forced rerun):
- Command:
  - `doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 --strategy CPR_LEVELS --skip-rvol --save --force-rerun`
- Before change:
  - `ELAPSED_SEC=52.45`
  - `run_id=14eaa2750d48`
  - `trades=2262`, `P/L=+891,199.98`
- After Phase 1:
  - `ELAPSED_SEC=24.80`
  - `run_id=fb9d879547e9`
  - `trades=2262`, `P/L=+891,199.98`

Result:
- Wall-clock improved by `27.65s` (`~52.7%` faster) with identical trade outcomes.

### 2026-03-08 - Phase 2 Implemented (DayPack + RVOL Map Removal)

Implemented:
- Added `DayPack` runtime container in strategy engine.
- Refactored day-pack prefetch to return `DayPack` objects instead of prebuilt per-day Polars DataFrames.
- Removed tuple-key RVOL map path (`(trade_date, time) -> avg_vol`) and switched RVOL lookup to `DayPack.baseline_for_time(...)`.
- Updated simulation wiring to pass `DayPack` into strategy methods.
- Added tests for `DayPack` baseline lookup and frame materialization.

Validation:
- `uv run ruff check engine/cpr_atr_strategy.py tests/test_strategy.py`
- `uv run pytest tests/test_strategy.py tests/test_cli.py -q`

Gold baseline benchmark (same command, forced rerun):
- Command:
  - `doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 --strategy CPR_LEVELS --skip-rvol --save --force-rerun`
- Before any optimization:
  - `ELAPSED_SEC=52.45`
- After Phase 1:
  - `ELAPSED_SEC=24.80`
- After Phase 2:
  - `ELAPSED_SEC=28.98`
  - `run_id=fb9d879547e9`
  - `trades=2262`, `P/L=+891,199.98`

Result:
- Phase 2 remains materially faster than original baseline (`~44.7%` faster vs pre-optimization).
- Phase 2 is slower than Phase 1 alone (`+4.18s`), which is expected until Phase 3 removes DataFrame-heavy strategy execution.

### 2026-03-08 - Phase 3 Implemented (Array-First Simulation Path)

Implemented:
- Replaced setup iteration hot path from `iter_rows(named=True)` to tuple-based row iteration.
- Added index-based helpers and range lookups:
  - `DayPack.index_of(...)`
  - `DayPack.range_indices(...)`
  - `_find_first_close_idx(...)`
- Refactored CPR_LEVELS/FBR/VIRGIN_CPR runtime scans to use `DayPack` arrays and indices instead of repeated DataFrame `filter(...)` calls.
- Refactored `_simulate_trade()` to consume `DayPack` + `start_idx` directly (no filtered `trade_df` allocation).
- Removed now-unused FBR DataFrame failure-check dependency in strategy engine path.

Validation:
- `uv run ruff check engine/cpr_atr_strategy.py tests/test_strategy.py tests/test_cli.py`
- `uv run pytest tests/test_strategy.py tests/test_cli.py -q`

Gold baseline benchmark (same command, forced rerun):
- Command:
  - `doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 --strategy CPR_LEVELS --skip-rvol --save --force-rerun`
- Before any optimization:
  - `ELAPSED_SEC=52.45`
- After Phase 1:
  - `ELAPSED_SEC=24.80`
- After Phase 2:
  - `ELAPSED_SEC=28.98`
- After Phase 3:
  - `ELAPSED_SEC=19.33`
  - `run_id=fb9d879547e9`
  - `trades=2262`, `P/L=+891,199.98`

Result:
- Phase 3 recovered and improved runtime significantly:
  - `~63.1%` faster vs pre-optimization baseline
  - `~22.1%` faster vs Phase 1
  - `~33.3%` faster vs Phase 2
- Trade outputs remained identical for the gold baseline.

## 12) Current Status (Completed vs Pending)

Completed:
- Phase 1:
  - T1.1 Add runtime batch parameter and CLI flag
  - T1.2 Refactor `run()` for batch fetch/sim
- Phase 2:
  - T2.1 Introduce `DayPack`
  - T2.2 Refactor `_prefetch_day_pack_batch()` to return `DayPack`
  - T2.3 Remove tuple-key RVOL map path
- Phase 3:
  - T3.1 Replace `iter_rows(named=True)` setup iteration
  - T3.2 Add shared index/range scan helpers
  - T3.3 Port CPR/FBR/VCPR + shared trade simulation to array-first runtime path
- Phase 4:
  - T4.1 Add `strategy_day_state`
  - T4.2 Push strategy-specific filtering into SQL
- Phase 5:
  - T5.1 Capital basis unification
  - T5.2 Annualization window correction
  - T5.3 Strategy code vs label separation
- Phase 6:
  - T6.1 Atomic save transaction
  - T6.2 `run_daily_pnl` materialization
- Phase 7 (optional):
  - T7.1 Pack schema compaction
- Test/validation updates for the above

Pending:
- None

### 2026-03-08 - Operational Improvement (Chunked Progress + Resume)

Implemented:
- Added chunked execution controls to `pivot-backtest`:
  - `--chunk-by none|year|month`
  - `--resume/--no-resume` (default resume enabled)
- Added human-readable chunk progress logs with timestamps and completion percentages:
  - `chunk_plan`
  - `running_year` / `running_month`
  - `year_complete` / `month_complete`
  - `chunk_skip` (resume path)
- Added resumable checkpoint behavior using `run_metadata`:
  - completed chunk detection is based on presence of chunk `run_id` in `run_metadata`
  - works for zero-trade chunks when `--save` is enabled
- Added guardrail:
  - `--chunk-by year/month` with `--resume` now requires `--save`
- Added end-of-run combined summary across all chunks.

Validation:
- `uv run ruff check engine/run_backtest.py tests/test_cli.py`
- `uv run pytest tests/test_cli.py -q`

Notes:
- Chunk-level save now happens per chunk only when running multi-chunk windows.
- Single-window runs preserve existing save flow and output behavior.

### 2026-03-08 - Phase 5/6 Implemented (Metrics Correctness + Save-Path Materialization)

Implemented:
- Phase 5:
  - Unified capital basis across runtime summary and materialized DB metrics:
    - denominator now uses `symbol_count * capital_per_symbol`
    - `capital_per_symbol` is read from `run_metadata.params_json.capital` (fallback `100000`)
  - Annualization now uses requested run window (`run_metadata.start_date/end_date`) with fallback to trade span.
  - Split strategy identity from display label in `run_metrics`:
    - added `strategy_code` and `label`
    - `strategy` now stores strategy code for backward compatibility.
- Phase 6:
  - Added `run_daily_pnl` materialization table (`run_id`, `trade_date`, `day_pnl`, `cum_pnl`) and refresh path.
  - `refresh_run_metrics()` now computes drawdown from `run_daily_pnl` instead of re-windowing full trade rows each time.
  - Wrapped `store_backtest_results()` delete+insert+metric-refresh flow in a single transaction.

Validation:
- `uv run ruff check db/duckdb.py engine/cpr_atr_strategy.py scripts/build_tables.py tests/test_strategy.py tests/test_cli.py`
- `uv run pytest tests/test_strategy.py tests/test_cli.py -q`
- Added regression tests:
  - runtime advanced-metrics capital/window basis
  - DB `run_metrics` capital/window + `strategy_code`/`label` behavior.

### 2026-03-08 - Phase 4 Data Integrity Fix (Duplicate Key Elimination)

Implemented:
- Eliminated duplicate `(symbol, trade_date)` propagation at build time:
  - `cpr_daily`: dedupe `v_daily` source with windowed `ROW_NUMBER()` before CPR derivation.
  - `market_day_state`: dedupe final derived rows by `(symbol, trade_date)`.
  - `strategy_day_state`: retained dedupe logic and normalized to a unique index.
- Enforced uniqueness constraints via indexes:
  - `cpr_daily(symbol, trade_date)` unique
  - `cpr_thresholds(symbol, trade_date)` unique
  - `market_day_state(symbol, trade_date)` unique
  - `strategy_day_state(symbol, trade_date)` unique
- Dropped legacy non-unique index names before creating unique indexes.

Rebuild and validation:
- Rebuilt tables in dependency order:
  - `pivot-build --table cpr --force`
  - `pivot-build --table thresholds --force`
  - `pivot-build --table state --force`
  - `pivot-build --table strategy --force`
- Post-rebuild DB checks:
  - `cpr_daily` dups: `0`
  - `cpr_thresholds` dups: `0`
  - `market_day_state` dups: `0`
  - `strategy_day_state` dups: `0`

Gold baseline rerun (same command as earlier phases):
- Command:
  - `pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 --strategy CPR_LEVELS --skip-rvol --save --force-rerun`
- Result after integrity fix:
  - trades=`2253`, P/L=`+886,198.34`, elapsed=`14.0s`
  - duplicate trade keys in saved results: `0`

Interpretation:
- Previous drift (`2278`) was confirmed as join amplification on duplicate day-state keys.
- The corrected baseline is now duplicate-free and should be used for future optimization comparisons.

### 2026-03-09 - Post-Fix Gold_51 Comparison Rerun (Phase 4 Validation Closed)

Executed sequentially on `gold_51` (`2015-01-01` to `2024-12-31`, `--skip-rvol --save --force-rerun`):
- `FBR --failure-window 10` -> `6a1c4a0218c5`
- `VIRGIN_CPR --rr-ratio 0.6 --vcpr-scan-start 09:45` -> `6318bda030a6`
- `CPR_LEVELS --cpr-min-close-atr 0.3` -> `841740773b02`

Canonical post-fix reference set:
- `CPR baseline`: `fb9d879547e9` -> trades=`2253`, pnl=`+886,198.34`, calmar=`3.35`
- `CPR 0.3 ATR`: `841740773b02` -> trades=`1878`, pnl=`+1,015,521.58`, calmar=`4.50`
- `FBR fw=10`: `6a1c4a0218c5` -> trades=`608`, pnl=`+240,749.31`, calmar=`3.47`
- `VCPR 09:45`: `6318bda030a6` -> trades=`4543`, pnl=`+98,228.92`, calmar=`0.38`

Validation checks:
- Duplicate saved trade keys (`run_id`,`symbol`,`trade_date`) for all four runs: `0`
- `run_metrics` reflects these run IDs and is now the source of truth for dashboard/compare pages.

Status update:
- Phase 4 validation + drift audit is complete and documented.
- Remaining optimization backlog: none.

### 2026-03-09 - Post-Fix CPR Distance Sweep (gold_51)

Executed:
- `CPR_LEVELS --cpr-min-close-atr 0.25` -> `12bc660c858d`
- `CPR_LEVELS --cpr-min-close-atr 0.35` -> `0ae617fa5611`
- Compared against post-fix `0.30` reference run `841740773b02`

Results:
- `0.25`: trades=`2002`, pnl=`+975,767.34`, pf=`2.02`, calmar=`3.83`
- `0.30`: trades=`1878`, pnl=`+1,015,521.58`, pf=`2.15`, calmar=`4.50`
- `0.35`: trades=`1750`, pnl=`+1,029,891.84`, pf=`2.29`, calmar=`5.25`

Conclusion:
- Post-fix CPR improves monotonically through `0.35`.
- New CPR candidate: `--cpr-min-close-atr 0.35`.

### 2026-03-09 - Phase 7 Implemented (Pack Schema Compaction)

Implemented:
- `build_intraday_day_pack` now compacts new builds to:
  - `minute_arr SMALLINT[]` (replaces `time_arr VARCHAR[]`)
  - `REAL[]` for open/high/low/close/volume/rvol baseline arrays
- Insert path supports both schema variants:
  - compact insert for tables with `minute_arr`
  - legacy insert fallback for existing `time_arr` tables
- Runtime day-pack fetch is backward-compatible:
  - auto-detects `minute_arr` schema
  - converts minute-of-day to `HH:MM` in engine before simulation

Validation:
- `uv run ruff check db/duckdb.py engine/cpr_atr_strategy.py tests/test_strategy.py`
- `uv run pytest tests/test_strategy.py -q`
- Added tests:
  - minute-to-time conversion
  - compact-schema mode detection and legacy fallback
- Smoke-validated compact build on temporary DB (`SBIN` only):
  - schema created with `minute_arr SMALLINT[]` + `FLOAT[]` arrays

Activation note:
- Main `data/market.duckdb` remains on legacy `time_arr/DOUBLE[]` until `pivot-build --table pack --force` is executed.
- Runtime is backward-compatible, so no immediate rebuild is required for correctness.

### 2026-03-10 - Post-Phase Refactor (Canonical Strategy Config Model)

Implemented:
- Removed flat strategy-specific fields from `BacktestParams` and made grouped strategy configs canonical:
  - `cpr_levels_config: CPRLevelsParams`
  - `fbr_config: FBRParams`
  - `virgin_cpr_config: VirginCPRParams`
- `BacktestParams.cpr_levels / fbr / virgin_cpr` now return canonical grouped config objects directly.
- `BacktestParams.apply_strategy_configs(...)` now updates grouped config objects (no flat-field mirroring).
- Updated CLI parameter construction to build grouped strategy configs explicitly and apply them in one place.
- Updated run-id hash key to split narrowing flags by strategy config:
  - `cpr_use_narrowing_filter`
  - `fbr_use_narrowing_filter`
- Deduplicated agent tool `BacktestParams` construction through a shared helper.

Validation:
- `uv run ruff check engine/cpr_atr_strategy.py engine/run_backtest.py agent/tools/backtest_tools.py tests/test_strategy.py tests/test_backtest_tools.py tests/test_cli.py`
- `uv run pytest -q`
  - Result: `71 passed`

Impact:
- Cleaner configuration model with reduced parameter drift risk between CLI/agent/runtime.
- Safer future strategy extensions without reintroducing flat-field duplication.

### 2026-03-10 - Code Review Remediation (Round 2)

Implemented:
- Removed remaining bare `except Exception:` usage across `agent/db/engine/scripts/web/tests` scope.
- Added DB-layer metadata caching for dashboard-heavy lookups:
  - `MarketDB.get_available_symbols(force_refresh=False)`
  - `MarketDB.get_all_date_ranges(force_refresh=False)`
  - short TTL cache with invalidation hook (`_invalidate_metadata_caches`).
- Removed mutable theme/color global dict state in dashboard components:
  - replaced mutable `THEME` / `COLORS` with read-only live mapping views.
- Reduced first-render dashboard query pressure:
  - added `awarm_home_cache()` and used it in Home page before rendering.
- Eliminated `# type: ignore` usage in repository source/tests.

Validation:
- `uv run ruff check db/duckdb.py engine/constants.py engine/run_backtest.py engine/cli_setup.py scripts/build_tables.py scripts/prune_runs.py web/state.py web/components/__init__.py web/pages/home.py web/pages/run_detail.py`
- `uv run pytest -q`
  - Result: `71 passed`

Status snapshot after this round:
- Bare `except Exception:` count in repo scan: `0`
- `_preview` helper duplication: `resolved` (single implementation in `scripts/gold_pipeline.py`)
- `# type: ignore` count: reduced to `0` (from prior 26)

### 2026-03-11 - Execution Workflow Finalized

Implemented execution-focused operating defaults in code:
- Added `pivot-campaign` (`scripts/run_campaign.py`) to standardize long-window runs.
- Fixed campaign run order:
  - `FBR --failure-window 10 --skip-rvol`
  - `CPR_LEVELS --cpr-min-close-atr 0.35 --skip-rvol`
  - `VIRGIN_CPR --rr-ratio 0.6 --vcpr-scan-start 09:45 --skip-rvol` (optional via `--exclude-vcpr`)
- Enforced resumable monthly workflow:
  - `--chunk-by month --resume --save --quiet` for every campaign run.
- Checkpoint policy codified:
  - aggregate run retained, chunk checkpoint run IDs pruned (default `pivot-backtest` behavior; campaign does not pass `--keep-chunk-runs`).
- Cleanup cadence codified:
  - optional `--clean-before` and `--clean-after` (with `--clean-progress-after` for `data/progress`).

Validation:
- Added CLI tests for campaign help and dry-run planning path in `tests/test_cli.py`.
- README updated with canonical campaign commands.
