# Live Readiness, Data Ingestion, and Resiliency Hardening Plan

**Status:** Draft for review — no implementation implied by this document  
**Created:** 2026-04-30  
**Scope:** EOD ingestion, pre-market readiness, live-paper startup, runtime-table builds, dashboard status, agent/runbook safety, and incident recovery  
**Primary objective:** prevent another live-paper trading day from being lost because data readiness looked green while the live-facing setup contract was incomplete.

---

## 1. Executive Summary

The system needs one enforced operating contract:

1. EOD ingestion must produce the next live trading day's setup surface.
2. Pre-market validation must verify that exact setup surface before live can start.
3. Live startup must fail closed before session creation if setup is missing or stale.
4. Repairs must be targeted and idempotent, never broad rebuilds during live hours.
5. The dashboard must show a clear, actionable readiness state instead of requiring log spelunking.
6. Agents must follow a decision tree that prevents confusing source data dates, setup trade dates, and intraday candle dates.

This document intentionally separates **planning** from **implementation**. The implementation should happen only after review.

---

## 2. Problem Statement

### 2.1 What failed

The project had fresh previous-day market data, but the live-facing trade-date setup rows were missing or not validated correctly. Readiness checks reported OK because they validated source freshness, not the complete setup surface required by live.

### 2.2 Why this is costly

The live-paper process can connect to Kite and receive ticks, but still be unable to evaluate trades if CPR/ATR/setup rows are absent or mismatched. This creates a silent or late-detected failure during the entry window.

### 2.3 Main root cause category

The project has multiple partially overlapping readiness contracts:

- EOD ingestion contract
- `pivot-build` materialization contract
- `daily-prepare` contract
- `pivot-data-quality` contract
- `daily-live` startup contract
- dashboard display contract
- agent/runbook instructions

These contracts are not currently enforced from one canonical source of truth.

---

## 3. Date Semantics That Must Become Canonical

This is the most important rule set.

### 3.1 Source data date

The date on raw market data:

- `v_daily.date`
- `v_5min.date`
- daily/5-minute parquet files

Example: after market close on 2026-04-29, source rows for `2026-04-29` should exist.

### 3.2 Setup trade date

The date the setup row is used for trading:

- `cpr_daily.trade_date`
- `cpr_thresholds.trade_date`
- `market_day_state.trade_date`
- `strategy_day_state.trade_date`

Example: after 2026-04-29 EOD, setup rows for `2026-04-30` may be valid because they are derived from completed 2026-04-29 data.

### 3.3 Intraday execution date

The date on actual same-day intraday candles and packed arrays:

- `intraday_day_pack.trade_date`
- live 09:15 candle
- opening range candles
- live bar stream

Example: before market open on 2026-04-30, `intraday_day_pack.trade_date = 2026-04-30` should normally be absent. Live bars will arrive from Kite during the session.

### 3.4 Correct pre-market expectation

Before live starts on `T`:

| Table / Input | Expected date | Required before live? | Notes |
|---|---:|---:|---|
| `v_daily` | `T-1 completed trading day` | Yes | Previous completed daily OHLC |
| `v_5min` | `T-1 completed trading day` | Yes | Needed for ATR/derived runtime |
| `atr_intraday` | `T-1` or setup-equivalent source | Yes | Must match previous completed setup source |
| `cpr_daily` | `T` | Yes | Trade-date row derived from `T-1` daily OHLC |
| `cpr_thresholds` | `T` | Yes | Trade-date threshold row derived from historical CPR |
| `market_day_state` | `T` | Yes | Setup row; can have same-day OR fields null before live candles |
| `strategy_day_state` | `T` | Yes if live path depends on it; otherwise explicitly optional | Must be clearly defined |
| `intraday_day_pack` | `T` | No pre-market | Same-day intraday data does not exist yet |

Any validation or documentation that says "same-day rows should be zero" must qualify that statement as **same-day intraday rows**, not setup rows.

---

## 4. Non-Negotiable Invariants

### 4.1 EOD invariants

After EOD completes for source date `D` and next trading date `T`:

- Instrument refresh status is recorded.
- Daily ingestion for `D` is either completed or explicitly skipped because rows already exist.
- 5-minute ingestion for `D` is either completed or explicitly skipped because rows already exist.
- Runtime tables for `D` are up to date where required.
- Setup-only tables for `T` exist:
  - `cpr_daily`
  - `cpr_thresholds`
  - `market_day_state`
  - `strategy_day_state`, if live requires it
- Same-day intraday pack for `T` is not required.
- Final EOD gate records a machine-readable pass/fail result.

### 4.2 Pre-market invariants

Before `daily-live` starts for trade date `T`:

- Dated universe `full_YYYY_MM_DD` exists.
- Dated universe equals `canonical_full`, unless an explicit override was approved.
- Previous completed trading day source data exists for all tradeable symbols, allowing documented sparse skips.
- Trade-date setup rows exist for all eligible symbols or sparse gaps are explicitly classified.
- No broad runtime writer is active.
- No stale partial pack repair is in progress.
- No live-date intraday pack is required before market open.

### 4.3 Live startup invariants

Before creating or reusing a live-paper session:

- Validate pre-market invariants.
- Validate preset fingerprint against canonical CPR risk parameters.
- Validate dated universe selection.
- Validate no conflicting active session with mismatched config exists.
- Fail closed with one actionable command if any blocking check fails.

### 4.4 Repair invariants

Repair commands must be targeted:

- Missing setup rows: build setup-only tables for the trade date.
- Missing pack rows: repair only missing pack symbols/dates.
- Missing raw data: run Kite ingestion only for missing date/symbol scope.
- Locks: report PID and command line; do not kill without operator decision.
- Never use broad `pivot-build --refresh-since` as the first live-day repair.

---

## 5. Required Project Audit Areas

The implementation should include a line-by-line review of these areas.

### 5.1 Build/materialization layer

Files:

- `db/duckdb.py`
- `scripts/build_tables.py`
- `scripts/refresh.py`
- `scripts/data_hygiene.py`

Review questions:

- Does each table builder clearly distinguish source date from trade date?
- Does `--refresh-date T` build setup rows for `T` without building invalid same-day pack?
- Does `--refresh-since D` accidentally rebuild too much during live hours?
- Are skip checks table-specific and symbol-specific, or can they falsely pass on date coverage only?
- Are partial builds detectable after interruption?
- Are all long builds batch logged at phase level?

Risks to specifically inspect:

- `build_cpr_table` next-trading-date behavior.
- `build_all` calling pack for dates where same-day pack should not be built.
- `_skip_if_table_fully_covered` using date-level coverage that hides symbol gaps.
- Incremental delete/insert behavior when a process is killed between batches.

### 5.2 Kite ingestion layer

Files:

- `engine/kite_ingestion.py`
- `scripts/kite_ingest.py`
- `scripts/refresh.py`

Review questions:

- Is `--skip-existing` deterministic and visible in logs?
- Does checkpoint/resume skip only completed symbols?
- Are daily and 5-minute ingestion statuses separately recorded?
- Are NSE holidays/weekends/empty Kite responses classified correctly?
- Does ingestion know the intended next trading date, or does it rely on calendar guessing?

### 5.3 Data quality/readiness layer

Files:

- `scripts/data_quality.py`
- `scripts/data_validate.py`
- tests under `tests/test_data_quality_cli.py`

Review questions:

- Does `pivot-data-quality --date T` validate the live setup contract for `T`?
- Does it fail if `cpr_daily T` is missing?
- Does it fail if `market_day_state T` is missing?
- Does it avoid requiring `intraday_day_pack T` before market?
- Does it classify sparse symbol/day gaps separately from systemic gaps?
- Does it print exact fix commands?

### 5.4 Daily prepare and universe layer

Files:

- `scripts/paper_prepare.py`
- `scripts/paper_trading.py`
- `db/duckdb.py`

Review questions:

- Does `daily-prepare` validate trade-date setup rows?
- Does it guarantee dated universe equals `canonical_full` unless explicit override is passed?
- Does it refuse to overwrite dated snapshots with different symbol lists?
- Does it classify missing symbols as sparse skips rather than shrinking the universe?
- Does `pre_filter_symbols_for_strategy` use the correct setup date?

### 5.5 Live runtime layer

Files:

- `scripts/paper_live.py`
- `engine/paper_runtime.py`
- `engine/paper_session_driver.py`
- `engine/bar_orchestrator.py`

Review questions:

- Can live start without trade-date setup rows?
- Is fallback from previous-day data safe at startup, or does it require live candles first?
- Does fallback create a circular dependency?
- Are live 09:15/opening-range candles incorporated deterministically?
- Are missing setup rows logged as hard errors before session creation?
- Are session completion, archive, alert, and PnL flush paths fail-closed?

### 5.6 Paper DB/session layer

Files:

- `db/paper_db.py`
- `scripts/paper_trading.py`
- `scripts/paper_supervisor.py`

Review questions:

- Are stale active sessions cleaned or surfaced safely?
- Are resume rules strict enough?
- Are mismatched configs refused?
- Are final archive and alert paths idempotent?
- Does the supervisor clearly record child status, exit code, tail logs, and heartbeat?

### 5.7 Dashboard layer

Files to identify and review:

- dashboard page routing files
- paper session page
- data quality page
- ops/status page
- `web/state.py`
- `web/*`

Review questions:

- Can the operator see `READY` / `NOT READY` for today's live session?
- Is the failing table/date obvious?
- Is the exact recommended fix command shown?
- Are active writer locks shown?
- Is the latest EOD run status visible?
- Can the dashboard distinguish warning sparse gaps from blocking gaps?

### 5.8 Backtest/replay/parity layer

Files:

- `engine/run_backtest.py`
- `engine/cpr_atr_strategy.py`
- `scripts/paper_replay.py`
- `scripts/parity_check.py`
- `engine/strategy_presets.py`
- `engine/execution_defaults.py`

Review questions:

- Are backtest/replay/live using the same canonical parameters?
- Are date semantics consistent between backtest and live?
- Does replay require same setup rows as live where appropriate?
- Do missing symbol/day sparse gaps produce deterministic skips?
- Are baseline runs blocked if runtime tables are partial?

### 5.9 Agent/docs layer

Files:

- `AGENTS.md`
- `.codex/skills/*`
- `docs/KITE_INGESTION.md`
- `docs/PAPER_TRADING_RUNBOOK.md`
- `docs/PARAMETER_UNIFORMITY.md`
- `docs/ISSUES.md`

Review questions:

- Do all docs use the same date semantics?
- Do all docs forbid broad rebuilds during live hours?
- Do all docs explain setup-only repair?
- Do agent instructions require read-only diagnosis before repair?
- Are commands copied from docs safe and current?

---

## 6. Proposed Command Contract

### 6.1 One EOD command

Target command:

```bash
doppler run -- uv run pivot-refresh --eod-ingest \
  --date <source_completed_trading_date> \
  --trade-date <next_trading_date>
```

This command should internally perform:

1. Refresh instruments.
2. Ingest daily candles for source date.
3. Ingest 5-minute candles for source date.
4. Build source-date runtime tables needed for historical completeness.
5. Build next-trading-date setup-only tables.
6. Save/verify dated universe.
7. Run final data quality gate.
8. Write a machine-readable EOD manifest.

### 6.2 Setup-only repair command

Target command:

```bash
doppler run -- uv run pivot-refresh --repair-setup \
  --trade-date <live_trade_date> \
  --source-date <previous_completed_trading_date>
```

This should build only:

- `cpr_daily`
- `cpr_thresholds`
- `market_day_state`
- `strategy_day_state`

It must not build:

- `intraday_day_pack` for live date
- broad all-history tables
- broad `--refresh-since` table sets

### 6.3 Pre-market readiness command

Target command:

```bash
doppler run -- uv run pivot-paper-trading readiness --trade-date today --strict
```

This should be the single read-only command used by humans, agents, and dashboard.

Expected output sections:

- Universe
- Previous completed source data
- Trade-date setup rows
- Same-day intraday expectations
- Writer locks
- Active sessions
- Recommended action

### 6.4 Live startup command

Target command remains:

```bash
doppler run -- uv run pivot-paper-supervisor -- \
  --multi --strategy CPR_LEVELS --trade-date today
```

But this command must internally call the strict readiness contract before session creation.

---

## 7. Dashboard Readiness View

### 7.1 Required panel

Add a "Live Readiness" panel to the paper session or ops page.

Top-level state:

- `READY`
- `NOT READY`
- `WARNING`
- `UNKNOWN`

### 7.2 Fields to display

| Field | Example | Blocking? |
|---|---:|---:|
| Trade date | `2026-04-30` | Yes |
| Previous completed date | `2026-04-29` | Yes |
| Canonical universe count | `2038` | Yes |
| Dated universe count | `2038` | Yes |
| Universe diff | `0` | Yes if non-zero |
| Previous daily rows | `2014` | Yes if systemic gap |
| Previous 5-min rows | `2014 symbols` | Yes if systemic gap |
| Previous ATR rows | `2014` | Yes if systemic gap |
| Trade-date CPR rows | `2014` | Yes |
| Trade-date thresholds rows | `2014` | Yes |
| Trade-date market state rows | `2014` | Yes |
| Trade-date strategy state rows | `2014` or explicitly optional | Depends on contract |
| Trade-date pack rows | `0 pre-market` | No pre-market |
| Active runtime writer | PID / none | Yes if writer active |
| Last EOD manifest | path/status | Warning if missing |
| Recommended fix | command | N/A |

### 7.3 Dashboard actions

For safety, initial version should be read-only:

- Show commands, do not execute them.
- Show PID, do not kill it.
- Show log path, do not tail automatically unless safe.

Optional later version:

- Button to copy fix command.
- Button to open logs.
- Button to run read-only readiness check.

---

## 8. Idempotency Requirements

### 8.1 Kite ingestion

- If raw daily data exists, skip unless `--force-ingest`.
- If raw 5-minute data exists, skip unless `--force-ingest`.
- If partial checkpoint exists, resume exactly from missing symbols.
- Logs must show:
  - symbols requested
  - symbols skipped
  - symbols fetched
  - symbols failed
  - checkpoint path

### 8.2 Runtime builds

- Setup-only build should upsert/delete+insert only setup rows for the target trade date.
- Pack repair should operate only on missing symbols/dates.
- Skip checks must include symbol coverage, not only date coverage.
- Every builder must emit:
  - table
  - date window
  - symbol count
  - rows before
  - rows deleted
  - rows inserted
  - rows after
  - elapsed time

### 8.3 Daily prepare

- Re-running with the same canonical universe should be idempotent.
- Re-running with a different resolved universe should fail unless an explicit refresh flag is passed.
- It must not silently shrink the dated universe.

### 8.4 Live sessions

- Reusing an existing session must compare config fingerprint.
- Resume must preserve open positions.
- New session creation must be blocked if a same strategy/date session already exists with conflicting config.

---

## 9. Guardrails To Add

### 9.1 Broad build guard

Block or require explicit confirmation for:

```bash
pivot-build --refresh-since <recent_date>
```

when:

- current time is during live/premarket window
- command would include `intraday_day_pack`
- a live session is active
- a dashboard or writer lock is active

### 9.2 Setup-only guard

If live-date setup rows are missing, recommended repair must be setup-only.

The message must not recommend broad `--refresh-since`.

### 9.3 Future-row guard

Validation must distinguish:

- allowed future/live-date setup rows
- disallowed same-day intraday pack rows before source candles exist

### 9.4 Partial pack guard

Detect when `intraday_day_pack` has source 5-minute rows but missing pack rows.

Output:

- affected symbol count
- missing symbol-day count
- generated missing-symbols file path
- recommended targeted repair command

### 9.5 Agent guard

Agent instructions must require:

1. Run read-only SQL/status check.
2. Identify data class:
   - source raw data
   - setup rows
   - intraday pack
   - paper session state
   - lock/process
3. Propose targeted fix.
4. Do not execute destructive or broad repair without explicit approval.

---

## 10. Incident Response Decision Tree

### 10.1 Live readiness says NOT READY

1. Check if source data for previous completed trading day exists.
2. Check if trade-date setup rows exist.
3. Check if universe mismatch exists.
4. Check if writer lock exists.
5. Run only the targeted repair command recommended by readiness output.

### 10.2 Missing source data

Action:

```bash
doppler run -- uv run pivot-refresh --eod-ingest \
  --date <missing_source_date> \
  --trade-date <next_trading_date>
```

If only one side is missing:

- daily only: targeted daily ingest
- 5-minute only: targeted 5-minute ingest

### 10.3 Missing setup rows

Action:

```bash
doppler run -- uv run pivot-refresh --repair-setup \
  --source-date <previous_completed_trading_date> \
  --trade-date <live_trade_date>
```

Do not run broad `--refresh-since` first.

### 10.4 Missing pack rows

Action:

1. Generate missing symbol list from SQL.
2. Run targeted pack repair with small batch size.
3. Verify missing pack rows are zero.

Do not run full pack rebuild.

### 10.5 Active writer lock

Action:

1. Show PID, process name, command line, start time, memory.
2. Determine if process is expected.
3. If it is an active live session, do not kill.
4. If it is a stale build, operator decides whether to stop.

### 10.6 Live session started but no trades

Action:

1. Check live log for setup prefetch count.
2. Check setup rows for trade date.
3. Check universe and candidate count.
4. Check live candle arrival.
5. Check strategy filters.
6. Do not assume "no trades" is a data issue until setup count and candle count are known.

---

## 11. SQL Verification Library

These should become reusable commands or functions, not ad hoc snippets.

### 11.1 Universe equality

Checks:

- `canonical_full` count
- dated `full_YYYY_MM_DD` count
- symbols in canonical not dated
- symbols in dated not canonical

### 11.2 Previous source coverage

For previous completed date `D`:

- `v_daily` rows/symbols
- `v_5min` rows/symbols
- `atr_intraday` rows/symbols

### 11.3 Trade-date setup coverage

For trade date `T`:

- `cpr_daily`
- `cpr_thresholds`
- `market_day_state`
- `strategy_day_state`

### 11.4 Same-day intraday guard

For trade date `T` before market:

- `intraday_day_pack T` should not be a readiness requirement.
- If present, classify whether it is legitimate replay/backfill or accidental pre-market build.

### 11.5 Duplicate key checks

For every materialized table:

- group by `(symbol, trade_date)`
- fail if count > 1

### 11.6 Missing pack where source exists

Compare:

- distinct `(symbol, date)` from `v_5min`
- distinct `(symbol, trade_date)` from `intraday_day_pack`

---

## 12. Test Plan

### 12.1 Unit tests

Add or expand tests for:

- next-trading-date setup build
- setup-only repair command planning
- DQ pass when setup exists and same-day pack is absent
- DQ fail when setup rows are missing
- DQ fail on universe mismatch
- DQ warning on sparse source gaps
- DQ fail on systemic source gaps
- daily-prepare fail when setup rows are missing
- daily-live fail before session creation when readiness fails
- broad build guard blocks unsafe command in live window

### 12.2 Integration tests

Simulate:

- EOD for date `D`, trade date `T`
- pre-market readiness for `T`
- live startup for `T` with no same-day pack
- missing setup rows and repair recommendation
- partial pack rebuild interruption and targeted recovery

### 12.3 Regression tests for current incident

Add a fixture where:

- `v_daily D` exists
- `v_5min D` exists
- `cpr_daily T` missing
- `market_day_state T` missing

Expected:

- `pivot-data-quality --date T` fails.
- `daily-prepare --trade-date T` fails.
- `daily-live --trade-date T` fails before session creation.
- Recommended command is setup-only repair, not broad refresh.

---

## 13. Logging And Observability Plan

### 13.1 EOD manifest

Write a JSON manifest per EOD run:

```text
.tmp_logs/eod/YYYYMMDD_<run_id>.json
```

Fields:

- source date
- trade date
- command args
- git commit
- start/end time
- step statuses
- row counts before/after
- skipped counts
- error details
- final readiness result

### 13.2 Long-running command logs

Every long-running command must have:

- stdout log
- stderr log
- structured progress file
- last heartbeat timestamp

### 13.3 Dashboard log links

Dashboard should expose:

- latest EOD manifest
- latest readiness report
- latest live supervisor heartbeat
- latest live stdout/stderr log

---

## 14. Documentation Updates Required

### 14.1 `AGENTS.md`

Add hard rules:

- Do not run broad build repair during live hours.
- Do not confuse setup trade date with source data date.
- Always run read-only readiness before repair.
- If missing setup rows, use setup-only repair.

### 14.2 `docs/KITE_INGESTION.md`

Update EOD section:

- state exact date semantics
- show single command
- show setup-only table build
- show final readiness expectations

### 14.3 `docs/PAPER_TRADING_RUNBOOK.md`

Update pre-market section:

- readiness dashboard
- readiness command
- setup rows required
- same-day pack not required
- do-not-run command list

### 14.4 `docs/PARAMETER_UNIFORMITY.md`

Ensure canonical CPR risk defaults are linked to live/backtest/replay commands.

### 14.5 `docs/ISSUES.md`

Log:

- current setup-row validation incident
- how validation missed it
- permanent guard to be added
- recovery command
- tests required

---

## 15. Implementation Phases

### Phase A — Read-only audit

Deliverables:

- full file-by-file audit notes
- issue matrix
- list of current unsafe docs/commands
- no behavior changes

### Phase B — Canonical readiness function

Deliverables:

- one library function that computes readiness
- one CLI command that prints it
- tests for all readiness states

### Phase C — EOD and setup repair commands

Deliverables:

- setup-only repair command
- EOD manifest
- idempotent skip behavior
- tests for skip/build/fail paths

### Phase D — Live startup enforcement

Deliverables:

- `daily-live` calls readiness before session creation
- supervisor surfaces readiness failure clearly
- existing sessions are not mutated by failed startup

### Phase E — Dashboard readiness view

Deliverables:

- read-only status panel
- blocking/warning separation
- recommended command display
- process lock visibility

### Phase F — Docs and agent hardening

Deliverables:

- updated runbooks
- updated `AGENTS.md`
- updated skills if needed
- issue log cross-links

### Phase G — End-to-end validation

Deliverables:

- simulated EOD
- simulated pre-market
- live local-feed dry run
- replay parity smoke
- dashboard screenshot/manual check

---

## 16. Acceptance Criteria

The hardening is complete only when all are true:

1. A missing live-date setup row fails pre-market readiness.
2. A missing live-date setup row fails `daily-prepare`.
3. A missing live-date setup row fails `daily-live` before session creation.
4. The recommended fix for missing setup rows is setup-only repair.
5. Same-day intraday pack is not required before market open.
6. Universe mismatch fails with exact diff counts.
7. Partial pack rebuild is detectable and repairable with a generated missing-symbol file.
8. Dashboard shows ready/not-ready and exact reason.
9. EOD command writes a manifest proving what was done/skipped.
10. Agents and docs all use the same date semantics.

---

## 17. Open Design Questions For Review

1. Should `strategy_day_state T` be required before live, or should live derive strategy direction from live opening-range candles and only require `market_day_state T`?
2. Should setup rows for `T` intentionally have null OR fields before market, or should those fields live in a separate table?
3. Should setup-only rows be stored in current `market_day_state`, or split into `market_setup_state` and `market_intraday_state` to prevent date confusion?
4. Should `pivot-build --refresh-date T` default to setup-only when `T` has no same-day intraday source data?
5. Should dashboard be allowed to trigger read-only readiness checks, or only display persisted reports?
6. What is the acceptable sparse-gap tolerance for live universe readiness?
7. Should broad build commands require an interactive confirmation token during market hours?

---

## 18. Immediate Review Checklist

Before implementation starts, review and decide:

- Is the table/date contract in Section 3 correct?
- Should live require `strategy_day_state T`, or should it compute direction live?
- Should setup state and intraday state be split in schema?
- What command name should be used for setup-only repair?
- Where should the dashboard readiness panel live?
- Which sparse gaps are warnings vs blockers?

No code changes should be made until these decisions are confirmed.
