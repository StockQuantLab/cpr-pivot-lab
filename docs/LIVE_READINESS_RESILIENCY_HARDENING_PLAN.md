# Live Readiness, Data Ingestion, and Resiliency Hardening Plan

**Status:** Revision 1 after Claude review  
**Created:** 2026-04-30  
**Last updated:** 2026-04-30  
**Scope:** EOD ingestion, pre-market readiness, live-paper startup, dashboard readiness, source/replica separation, agent/runbook safety, and incident recovery  
**Primary objective:** prevent another live-paper trading day from being lost because source data looked fresh while the live-facing setup contract was incomplete.

---

## 1. Revision Notes From Claude Review

Claude's first review returned `VERDICT: REVISE`. This revision addresses the actionable findings:

| Review finding | Revision |
|---|---|
| Plan assumed too much was missing | Added current-state inventory and changed plan to remaining deltas only |
| Incident root cause was too generic | Added the concrete 2026-04-30 failure chain |
| `strategy_day_state` requirement was unresolved | Marked it required because current live setup prefetch depends on it |
| `--repair-setup` could hide missing source data | Replaced initial new CLI proposal with existing table-specific commands plus mandatory source-date validation |
| Clock-based broad-build guard could block recovery | Changed guard to active-session/writer-state based, not clock-only |
| EOD manifest could become stale documentation | Deferred manifest until it has automatic consumers |
| Missing EOD failure alert | Promoted EOD failure alert to P1 |
| Too many open questions | Split blockers from deferrable questions |

---

## 2. Current State Already Implemented

These items already exist in the codebase and should not be rebuilt from scratch:

1. `pivot-refresh --eod-ingest --date D --trade-date T` runs an ordered EOD pipeline.
2. EOD now includes next-day setup table stages:
   - `pivot-build --table cpr --refresh-date T`
   - `pivot-build --table thresholds --refresh-date T`
   - `pivot-build --table state --refresh-date T`
   - `pivot-build --table strategy --refresh-date T`
3. `pivot-sync-replica --verify --trade-date T` exists and verifies the actual market replica file.
4. `pivot-data-quality --date T` has setup-only mode and checks next-day setup rows.
5. `daily-prepare --trade-date T` fails if required setup rows for `T` are missing.
6. `daily-live` has startup fail-fast checks for missing trade-date setup rows in the market read DB.
7. Windows encoding failure in `pivot-data-quality` is fixed by configuring stdio and using ASCII readiness markers.
8. Tests exist for data-quality setup-only readiness, paper prepare behavior, EOD stage order, and replica verification.

The remaining work is not a rewrite. It is a hardening pass to remove ambiguity, improve operator visibility, and prevent agents/operators from choosing unsafe repair paths.

---

## 3. Actual 2026-04-30 Incident Chain

The incident was not simply "Kite data missing." The failure chain was:

1. Trade-date setup rows were missing or stale for the live date.
2. Validation focused on previous completed source freshness and did not sufficiently prove the exact live-facing setup surface.
3. Live startup could connect to Kite and receive ticks while setup coverage was zero.
4. Manual repair was confused by source-date vs setup trade-date semantics.
5. Live market reads used the market replica through `get_dashboard_db()`, so source DB repair did not help live unless the replica was also synced and verified.
6. Console encoding then caused final readiness output to fail even after readiness itself was OK.

The code fixes already close several points in this chain. This plan covers the remaining design and operational gaps.

---

## 4. Canonical Date Semantics

These semantics must be used in code, docs, dashboard, and agent instructions.

### 4.1 Source Data Date

The date on raw/completed market data:

- `v_daily.date`
- `v_5min.date`
- daily parquet
- 5-minute parquet

Example: after market close on 2026-04-30, source rows for `2026-04-30` should exist.

### 4.2 Setup Trade Date

The date the setup row is used for trading:

- `cpr_daily.trade_date`
- `cpr_thresholds.trade_date`
- `market_day_state.trade_date`
- `strategy_day_state.trade_date`

Example: after 2026-04-30 EOD, setup rows for `2026-05-04` are valid because they are derived from completed 2026-04-30 source data.

### 4.3 Intraday Execution Date

The date on same-day intraday execution data:

- `intraday_day_pack.trade_date`
- live 09:15 candle
- opening-range candles
- live bar stream

Example: before market open on 2026-05-04, `intraday_day_pack.trade_date = 2026-05-04` is not required.

### 4.4 Pre-Market Requirement For Trade Date `T`

| Table / Input | Expected date | Required before live? | Notes |
|---|---:|---:|---|
| `v_daily` | previous completed trading day `D` | Yes | Previous completed daily OHLC |
| `v_5min` | previous completed trading day `D` | Yes | Needed for ATR/runtime state |
| `atr_intraday` | setup source equivalent for `T` | Yes | Derived from completed 5-minute data |
| `cpr_daily` | `T` | Yes | Trade-date row derived from `D` |
| `cpr_thresholds` | `T` | Yes | Needed for narrowing/threshold filters |
| `market_day_state` | `T` | Yes | Live setup surface |
| `strategy_day_state` | `T` | Yes | Current live setup prefetch depends on it |
| `intraday_day_pack` | `T` | No pre-market | Same-day candles do not exist yet |

Any check saying "same-day rows should be zero" must specify same-day intraday rows, not setup rows.

---

## 5. Source DB, Live Reads, And Replicas

### 5.1 Current Problem

Live currently uses `get_dashboard_db()` for read-only market setup queries. That accessor reads the latest file from `data/market_replica/`.

This is lock-safe for DuckDB on Windows, but the ownership is wrong:

- Dashboard replicas exist so dashboard/ad-hoc reads do not affect operational paths.
- Live trading should not silently depend on an implicit "latest dashboard replica."
- If `market.duckdb` is repaired but the replica is stale, live can still see old data unless guards catch it.

### 5.2 Target Design

1. `market.duckdb` remains the source of truth.
2. Live startup reads source DB directly for validation/preload where feasible, then closes it before the trading loop.
3. Dashboard and ad-hoc queries continue to use replicas.
4. If a copy is required for lock isolation, create an explicit verified and pinned live snapshot:
   - `live_market_snapshot_<trade_date>.duckdb`
   - created by EOD or pre-market readiness
   - verified for row counts and source commit/version
   - pinned to the session at startup
5. Replace live use of `get_dashboard_db()` with an explicit accessor:
   - `get_live_market_db(trade_date)`
   - logs source path/snapshot path
   - logs row counts for all required setup tables
   - fails if snapshot/source is stale

### 5.3 Required Audit

Audit and classify every `get_dashboard_db()` call:

- Dashboard/UI read: keep replica.
- Replay/local simulation read: decide explicitly, document behavior.
- Live startup/readiness read: migrate to `get_live_market_db(trade_date)`.
- Live loop read after preload: avoid DB reads if possible.

Files:

- `engine/paper_runtime.py`
- `scripts/paper_live.py`
- `engine/local_ticker_adapter.py`
- `db/duckdb.py`

---

## 6. Remaining Work By Priority

### P0 — Consolidate Readiness Contract

Goal: one canonical readiness result used by CLI, live startup, dashboard, and agents.

Actions:

1. Treat `build_trade_date_readiness_report(trade_date)` as the canonical base.
2. Extend it only where gaps remain; do not duplicate readiness SQL elsewhere.
3. Ensure the report includes:
   - source date `D`
   - trade date `T`
   - universe name/count/diff
   - required setup table counts for `T`
   - prior source table counts for `D`
   - same-day pack status classified as "not required pre-market"
   - active writer/live sessions
   - market source/replica/snapshot path used for validation
   - exact recommended action
4. Make `daily-live` call the same readiness contract before session creation.
5. Ensure `daily-prepare` and `pivot-data-quality --date T` agree on pass/fail semantics.

Acceptance:

- A fixture with `v_daily D` and `v_5min D` present but `market_day_state T` missing fails in all three places:
  - `pivot-data-quality --date T`
  - `daily-prepare --trade-date T`
  - `daily-live --trade-date T`
- All three print the same targeted setup repair command bundle.

### P0 — Live Market DB Refactor

Goal: live must not depend on `get_dashboard_db()` / implicit latest dashboard replica.

Actions:

1. Add `get_live_market_db(trade_date)` or equivalent explicit accessor.
2. Decide implementation:
   - preferred first pass: open source `market.duckdb` read-only during startup/preload and close it
   - fallback: explicit pinned live snapshot, not dashboard replica
3. Log the selected DB path/version and setup row counts at startup.
4. Fail if the source/snapshot does not contain required `T` setup rows.
5. Keep dashboard on dashboard replica.

Acceptance:

- Grep shows no live startup path calling `get_dashboard_db()` directly.
- Startup log clearly shows the market DB source used by live.
- Test proves stale dashboard replica cannot make live fail if source DB is valid, or cannot be used without explicit pinned verification.

### P1 — EOD Failure Alert

Goal: operator gets notified at EOD failure time, not next morning.

Actions:

1. Add Telegram/email alert for `pivot-refresh --eod-ingest` failure.
2. Include:
   - source date
   - trade date
   - failed stage
   - log path
   - next safe command
3. Send success alert only after final readiness gate passes.

Acceptance:

- Simulated failed EOD stage emits `EOD_FAILED`.
- Successful EOD emits `EOD_READY` with `Ready YES`.

### P1 — Dashboard Live Readiness Panel

Goal: operator sees readiness without reading logs.

Actions:

1. Add read-only "Live Readiness" panel to paper/ops dashboard.
2. Show:
   - `READY`, `NOT READY`, `WARNING`, or `UNKNOWN`
   - source date `D`
   - trade date `T`
   - universe count/diff
   - setup table row counts
   - market DB source/snapshot path
   - last EOD status
   - active writer/live process state
   - exact recommended fix command
3. No destructive buttons in first version.

Acceptance:

- Dashboard shows `Ready YES` for 2026-05-04 after Apr30 EOD.
- Dashboard shows missing table and fix command in a fixture with missing setup rows.

### P1 — Source Completeness Before Setup Repair

Goal: setup repair must not build rows from stale/missing source data.

Actions:

1. Before any setup-only repair command bundle, validate source date `D`:
   - daily source exists
   - 5-minute source exists
   - expected symbol coverage is within sparse-gap tolerance
2. If source is missing, print "run EOD ingestion for D first."
3. Use existing table-specific commands initially:

```bash
doppler run -- uv run pivot-build --table cpr --refresh-date <trade_date>
doppler run -- uv run pivot-build --table thresholds --refresh-date <trade_date>
doppler run -- uv run pivot-build --table state --refresh-date <trade_date>
doppler run -- uv run pivot-build --table strategy --refresh-date <trade_date>
doppler run -- uv run pivot-sync-replica --verify --trade-date <trade_date>
```

Do not add `pivot-refresh --repair-setup` until this validation is proven and documented. A wrapper can be added later if it only orchestrates the same validated steps.

Acceptance:

- Missing source date blocks setup repair recommendation.
- Present source date allows setup repair recommendation.

### P2 — Active-Session Build Guard

Goal: prevent broad repairs from damaging live state without blocking legitimate recovery.

Actions:

1. Do not use clock-only blocking.
2. Guard broad `pivot-build --refresh-since` / no-table repair when:
   - active live session exists
   - runtime writer lock exists
   - command includes `intraday_day_pack`
3. Allow targeted setup-table repair if source completeness passes and no writer conflict exists.
4. Print exact reason and safer command.

Acceptance:

- Active live session blocks broad build.
- No active live session allows operator-approved repair even pre-market.

### P2 — Historical Sparse Gap Classification

Goal: distinguish structural sparse gaps from systemic data holes.

Actions:

1. Classify first-trading-day IPO gaps as INFO/non-blocking.
2. Investigate 2019 anomaly separately.
3. Baseline promotion gate should fail only on blocking gaps within the backtest window.

Acceptance:

- IPO first-day gaps do not block live readiness.
- Systemic missing source/runtime gaps still block baseline promotion.

### P2 — Baseline Runtime Fingerprints

Goal: make promoted baseline comparisons reproducible without storing a full DuckDB/parquet
snapshot for every run.

Actions:

1. Store lightweight fingerprints on every saved run:
   - git commit and strategy preset/config hash
   - saved universe name, symbol count, and symbol-list hash
   - source parquet manifest hash or source file fingerprint
   - runtime table row counts, min/max trade dates, and last build timestamp/hash
2. Store compact CPR audit rows only for promoted baselines:
   - setup direction, CPR bounds, OR close, OR/ATR, effective RR, quality score
   - candidate rank, selected/skipped status, skip reason, slot count/open slots
3. Do not create full data snapshots for every experiment.
4. Allow an explicit frozen runtime DB copy only for rare major baseline promotions or release
   checkpoints, not routine strategy sweeps.
5. Baseline promotion must rerun the same params/universe/window and fail if drift cannot be
   explained as universe, runtime-state, candidate-selection, or execution drift.

Acceptance:

- A promoted run can explain future drift without needing the old mutable `market.duckdb`.
- Storage growth is bounded to hashes plus compact candidate/setup audit rows.
- Full DB snapshots are manual, named release artifacts only.

### P3 — EOD Manifest Only If Consumed

Goal: avoid stale artifact bloat.

Actions:

1. Add manifest only if at least one consumer reads it:
   - dashboard readiness panel
   - readiness CLI
   - EOD alert
2. Write atomically: temp file then rename.
3. Do not log secrets or environment values; command args only.

Acceptance:

- Corrupt/truncated manifest does not crash readiness.
- Dashboard/CLI clearly marks manifest missing or stale as warning, not source of truth.

---

## 7. Test Plan

### 7.1 Regression Tests For The Incident

Fixture:

- `v_daily D` exists.
- `v_5min D` exists.
- `cpr_daily T` missing.
- `market_day_state T` missing.
- `strategy_day_state T` missing.
- `intraday_day_pack T` absent.

Expected:

- `pivot-data-quality --date T` fails.
- `daily-prepare --trade-date T` fails.
- `daily-live --trade-date T` fails before session creation.
- Recommended command is setup-table repair, not broad `--refresh-since`.

### 7.2 Live DB Source Tests

Cases:

1. Source DB has `T` setup rows; dashboard replica is stale.
2. Source DB missing `T` setup rows; dashboard replica has old rows.
3. Pinned live snapshot is missing or stale.

Expected:

- Live uses only the explicitly approved live market source.
- Startup logs the source path.
- Stale implicit dashboard replica cannot pass live readiness.

### 7.3 EOD Alert Tests

Cases:

1. EOD stage fails during daily ingest.
2. EOD stage fails during setup build.
3. EOD final readiness fails.
4. EOD succeeds.

Expected:

- Failure sends `EOD_FAILED` with stage/log/action.
- Success sends `EOD_READY`.

### 7.4 Dashboard Tests

Cases:

1. Ready state after successful EOD.
2. Missing setup rows.
3. Missing source rows.
4. Active writer lock.
5. Warning-only sparse gaps.

Expected:

- UI state and recommended action match readiness report.

### 7.5 Performance Budget

Measure and document:

- readiness CLI: target under 10 seconds
- setup-table repair with source already present: target under 2 minutes
- dashboard readiness refresh: target under 5 seconds from cached/persisted result

---

## 8. Docs And Agent Updates

Update these after P0/P1 changes:

- `AGENTS.md`
- `docs/PAPER_TRADING_RUNBOOK.md`
- `docs/KITE_INGESTION.md`
- `docs/PARAMETER_UNIFORMITY.md`
- `.codex/skills/daily-refresh/SKILL.md`
- `.codex/skills/daily-paper/SKILL.md`
- `.codex/skills/data-validate/SKILL.md`
- `docs/ISSUES.md`

Required docs changes:

1. Explain source date vs setup trade date vs intraday execution date.
2. State that `strategy_day_state T` is currently required.
3. State that same-day intraday pack is not required pre-market.
4. Forbid broad `--refresh-since` as first live-day repair.
5. Require read-only readiness diagnosis before repair.
6. Explain live source DB / live snapshot / dashboard replica separation.

---

## 9. Implementation Order

### Phase 1 — P0 Incident Closure

1. Consolidate readiness contract.
2. Refactor live market DB source away from dashboard replica.
3. Add/adjust incident regression tests.
4. Update minimal docs/runbook for changed contract.

### Phase 2 — P1 Operator Visibility

1. Add EOD failure/success alerts.
2. Add dashboard readiness panel.
3. Add source completeness check before setup repair recommendation.

### Phase 3 — P2/P3 Hardening

1. Add active-session broad-build guard.
2. Classify sparse gaps.
3. Add consumed-only EOD manifest if still useful.
4. Expand agent skill/runbook decision trees.

If only two things can be done next, do:

1. Live market DB source refactor.
2. Dashboard readiness panel backed by the canonical readiness report.

---

## 10. Blocking Decisions

These must be decided before implementation:

1. Should live open source `market.duckdb` briefly at startup, or should EOD create a pinned `live_market_snapshot_<trade_date>.duckdb`?
2. Should the first dashboard readiness panel read live readiness on demand or only display a persisted readiness result?
3. What sparse-gap threshold is acceptable for live readiness vs baseline promotion?

Deferred/non-blocking:

- Final dashboard placement.
- Whether to add a wrapper `pivot-refresh --repair-setup`.
- Whether to add EOD manifest after alerts/dashboard are implemented.

---

## 11. Definition Of Done

The plan is complete when:

1. Live startup cannot use an implicit dashboard replica for market setup.
2. A stale replica/source mismatch is visible before session creation.
3. `pivot-data-quality`, `daily-prepare`, dashboard readiness, and `daily-live` agree on readiness.
4. EOD failure sends an alert the same evening.
5. Operator can see readiness and exact fix from dashboard.
6. Agents have one safe decision tree and do not recommend broad repairs first.
7. Regression tests cover the Apr30 incident class.
