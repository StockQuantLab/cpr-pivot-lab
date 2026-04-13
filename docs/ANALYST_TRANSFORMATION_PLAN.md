# CPR Pivot Lab — Analyst-Ready Transformation Plan

> Product/backlog doc, not live operator guidance. The live runbook is
> `docs/PAPER_TRADING_RUNBOOK.md`.

Last updated: 2026-03-25

---

## Product Goal

Transform CPR Pivot Lab from a developer-operated toolkit into an **analyst decision platform**:

- an analyst can launch a run safely without knowing CLI details
- compare a candidate run against a baseline
- understand *why* a run passed or failed
- inspect regime and exit diagnostics, not just top-line PnL
- promote or reject experiments with a clear audit trail

This document is intentionally product-oriented. It is not just a UI feature list; it defines
the analyst workflows the system should support. Engine-side dependencies and validation rules
are described in [`docs/ENGINE_OPTIMIZATION_PLAN.md`](./ENGINE_OPTIMIZATION_PLAN.md).

---

## What “Analyst-Ready” Means

The target user is not a developer who memorizes flags. The target user is a strategy analyst
who needs to answer questions such as:

- Is this run better than the approved baseline after costs?
- Did the new CPR regime filter improve quality or only reduce trade count?
- Are breakeven exits too aggressive?
- Why did this walk-forward run fail?
- Which parameter experiment is currently approved for paper trading?

If the product cannot answer those questions clearly, it is not yet analyst-ready.

---

## Current State Assessment

| Area | Score | Main gap |
|---|---:|---|
| Visualization / historical analysis | 9/10 | Strong read-only reporting |
| Safe run execution | 2/10 | No guided runner, presets, or validation guardrails |
| Decision support | 3/10 | Weak explanation of pass/fail, exit mix, or regime effects |
| Experiment management | 2/10 | No audit trail for presets, approvals, or comparisons |
| Automation | 7/10 | Good CLI orchestration, weak analyst-facing scheduling |
| Onboarding | 3/10 | Too CLI- and env-driven |

---

## Product Principles

1. **Decision support beats raw control**
   - The UI should explain outcomes, not just expose more buttons.

2. **Safe defaults before flexibility**
   - Analysts should start from validated presets and guarded ranges, not empty forms.

3. **Every run should be explainable**
   - A run should show data freshness, cost model, validation state, and pass/fail reasons.

4. **Experiment history is a product feature**
   - Parameter changes, approvals, and promotions need auditability.

5. **Execution UX must respect DuckDB constraints**
   - Jobs, reads, and background tasks should be designed around Windows file locking.

---

## Core Analyst Workflows to Support

### Workflow A — Launch a safe backtest

The analyst should be able to:

1. choose a preset or approved baseline
2. adjust a small number of validated parameters
3. see warnings before launch
4. run the job
5. review progress and logs
6. land directly on the results comparison page

### Workflow B — Diagnose a run

The analyst should be able to answer:

- What changed vs baseline?
- Did CPR width filtering materially change setup coverage?
- Did zero-PnL exits increase?
- Which folds passed, failed, or were inconclusive?
- Were costs applied?
- How many candidates were considered and where were they rejected? (setup funnel)

### Workflow C — Compare and approve experiments

The analyst should be able to:

1. group runs into an experiment
2. compare candidate vs baseline side-by-side
3. add a decision note
4. mark a candidate as approved / rejected
5. preserve the full history of that decision

### Workflow D — Operate routine jobs safely

The analyst should be able to:

1. refresh data incrementally (not rebuild from 2015) with progress visibility
2. run a Monday validation workflow
3. start paper-trading preparation
4. review job history and failures
5. trust that the UI will not corrupt or deadlock the environment

---

## Roadmap Overview

### Phase 1 — Safe execution foundation

Build the minimum product surface that lets analysts run workflows reliably:

- DuckDB-safe query and job model
- run builder with presets and safe defaults
- job history and status
- validation badges and pre-flight checks

### Phase 2 — Diagnostics & decision support

Expose the strategy-quality questions analysts actually need to answer:

- CPR-width distributions and regime filtering impact
- zero-PnL / breakeven exit mix
- fold-level pass/fail reasons
- baseline vs candidate comparison
- parameter experiment tracking

### Phase 3 — Automation & alerts

Turn routine analyst operations into scheduled, visible workflows:

- scheduler
- data refresh pipeline
- alert configuration
- Telegram / webhook notifications

### Phase 4 — Adoption & polish

Reduce training overhead and improve daily usability:

- settings UI
- onboarding wizard
- exports / reports
- optional chat assistant

---

## Phase 1 — Safe Execution Foundation

### 1.1 DuckDB Concurrency — The Windows Single-Process Lock Problem

#### The problem

On Windows, DuckDB enforces exclusive file-level locking. Today the dashboard opens a
persistent connection at module load, which means:

**dashboard running = no backtests, no builds, no paper-trading writes.**

Any analyst-facing execution workflow will be fragile until this is fixed.

#### Solution options

##### Option A: Connect-on-demand (recommended foundation)

Open and close DuckDB for each query instead of holding a singleton connection for the entire
dashboard lifetime.

```python
# BEFORE
db = get_dashboard_db()

# AFTER
def _query(sql: str, params=None) -> list[dict]:
    db = MarketDB(read_only=True)
    try:
        return db.con.execute(sql, params).fetchdf().to_dict("records")
    finally:
        db.close()
```

**Trade-offs:**
- ✅ simplest architecture change
- ✅ file lock held only briefly
- ✅ works well with existing TTL caches
- ⚠️ small per-query overhead
- ⚠️ query may occasionally collide with a running job

##### Option B: Job-aware lifecycle

Use a job runner that launches subprocesses with lock awareness and gracefully handles the
brief windows where the database is unavailable.

**Trade-offs:**
- ✅ clearer job status model
- ✅ gives analysts a proper “running / completed / failed” workflow
- ⚠️ adds orchestration complexity

##### Option C: Client-server database access (future)

Move to a server-mediated model later if the product outgrows file-based access patterns.

#### Recommended approach

Use **Option A + Option B**:

1. connect-on-demand for dashboard reads
2. a `JobRunner` for subprocess execution
3. graceful UI messaging when the file is temporarily locked
4. TTL cache to absorb most repeat reads

This is the minimum viable foundation for an analyst-facing product on Windows.

---

### 1.2 Backtest Runner Workspace (`/run`)

This should be more than a form. It should be a **guided run builder**.

**Required capabilities:**
- preset selector:
  - Approved baseline
  - Quick symbol smoke test
  - Full-universe validation run
  - strategy-specific templates
- constrained parameter controls with analyst-safe ranges
- inline help for each parameter
- pre-flight summary before launch
- progress state and post-run redirect

**Guardrails:**
- hide advanced parameters behind an “Expert” section
- prefill cost model and known-good defaults
- warn when trade-date range is too small for the requested analysis
- warn when the candidate differs materially from the last approved baseline

---

### 1.3 Validation Badges & Pre-Flight Checks

Before a run is launched, the product should show analyst-readable badges such as:

- **Data Freshness:** current / stale / unknown
- **Costs Applied:** yes / no
- **Walk-Forward Enabled:** yes / no
- **Approved Preset:** yes / modified / no
- **Safe Range Validation:** pass / warning / blocked
- **Comparable to Baseline:** yes / no baseline selected

These badges turn hidden technical assumptions into visible analyst context.

**Pre-flight checks should block or warn on:**
- missing or stale data
- unsupported parameter combinations
- invalid date windows for rolling metrics
- no baseline selected for experiment runs
- turning off costs or walk-forward on non-exploratory runs

---

### 1.4 Job Runner, Job History, and Failure Visibility

Analysts need confidence that jobs are manageable and auditable.

**Required capabilities:**
- run jobs as tracked background tasks
- show queued / running / completed / failed state
- capture stdout/stderr logs
- store launch parameters and who launched the run
- support retry from last configuration

**Job history view should show:**
- job type
- start/end time
- duration
- outcome
- linked run or artifact
- failure reason

This is also the base for scheduled automation later.

---

### 1.5 Paper Trading Control Surface

Paper trading controls should stay conservative and analyst-safe.

**Minimum controls:**
- Start session
- Pause
- Flatten
- Stop
- last validation badge shown beside launch controls

**Guardrails:**
- only allow session start from an approved or explicitly acknowledged config
- show latest gate result inline
- require visible acknowledgment when starting from a modified preset

---

## Phase 2 — Diagnostics & Decision Support

This phase is the biggest gap between “dashboard” and “analyst platform.”

### 2.1 Run Comparison View (`/compare`)

Analysts should be able to compare:

- baseline vs candidate
- candidate A vs candidate B
- same strategy across two time windows

**Comparison panels should include:**
- headline metrics
- cost-aware net metrics
- trade count / turnover
- exit mix
- walk-forward fold outcomes
- parameter diff

**Required explanation cards:**
- “Why candidate improved”
- “Why candidate failed”
- “What changed operationally” (trade count, exposure, coverage)

Example explanations:

- “Candidate failed because 3 of 8 folds were inconclusive and net Calmar fell after costs.”
- “Candidate improved PnL, but BE zero-exit rate rose from 14% to 28%.”

---

### 2.2 CPR Regime Diagnostics

The product should help analysts understand regime filtering, not just enable it.

**Required diagnostics:**
- CPR-width distribution over time
- percentile threshold history
- absolute-width threshold overlays
- setup counts filtered in/out by:
  - percentile rule
  - hard cap
  - narrowing flag
- strategy split: CPR_LEVELS vs FBR

**Analyst questions this should answer:**
- Are we already filtering most wide-CPR days?
- Did the absolute-width rule improve setup quality or just reduce coverage?
- Is FBR benefitting from the same regime filter as CPR_LEVELS?

This directly supports the engine roadmap item on narrow CPR regime refinement.

---

### 2.3 Exit-Quality Diagnostics

This is the analyst-facing counterpart to engine-side exit instrumentation.

**Required diagnostics:**
- exit-reason mix
- zero-PnL / breakeven stopout share
- percent of trades that reached `>= 1R`
- percent of `>= 1R` trades that still missed target
- post-breakeven MFE distribution
- exit mix by strategy and by symbol

**Required views:**
- run-level summary
- symbol-level drilldown
- fold-level comparison

**Primary analyst question:**
- Is the strategy failing because entries are weak, or because exit management is too tight?

This directly supports the engine roadmap item on breakeven / zero-PnL exit optimization.

---

### 2.4 Walk-Forward Decision Support

A badge that says `PASS` or `FAIL` is not enough.

**Required outputs:**
- fold-by-fold table with pass / fail / inconclusive
- reasons for each state
- minimum-trade threshold visibility
- aggregated gate summary
- visual marker showing which folds are out-of-sample

**“Why this run passed/failed” panel should summarize:**
- insufficient trades
- negative net PnL
- profit factor below threshold
- drawdown breach
- unstable exit mix or trade count collapse

---

### 2.5 Parameter Experiments & Audit Trail

Analysts need experiment management, not just individual run pages.

**Required capabilities:**
- group runs into an experiment
- attach a hypothesis
- tag a baseline
- capture notes and decisions
- mark approved / rejected / exploratory
- preserve parameter diffs and result diffs

**Audit trail should answer:**
- Who changed the preset?
- Which run became the approved baseline?
- Why was a candidate rejected?
- Which parameters were in force for paper trading on a given date?

---

### 2.6 Setup Selection Funnel View

Analysts need to understand not just why trades executed, but **how stocks were selected**
and **where candidates were rejected**. Today the dashboard explains executed trades well
but gives zero visibility into the filtering pipeline.

**Required view — daily setup funnel:**

Show a per-day waterfall / funnel chart with counts at each filter stage:

```
Universe (51 symbols)
  └─ Passed CPR width filter: 28
      └─ Passed gap filter: 25
          └─ Passed OR/ATR filter: 22
              └─ Passed min-price: 20
                  └─ Direction determined: 14
                      └─ Passed narrowing: 8
                          └─ Entry triggered: 3 trades taken
```

**Required capabilities:**
- daily funnel breakdown (which day rejected how many at which stage)
- aggregate funnel over a date range (which filter is the binding constraint overall)
- filter-by-filter rejection details (e.g. "15 symbols rejected by CPR width — show which ones")
- comparison mode: show funnel side-by-side for two runs with different filter settings
- strategy split: separate funnels for CPR_LEVELS vs FBR

**Analyst questions this should answer:**
- How selective is the CPR width filter on a typical day?
- Is the narrowing filter rejecting too many otherwise-good setups?
- Did the new absolute-width filter reduce opportunity without improving quality?
- Which symbols are consistently rejected, and at which stage?

**Data source:** Engine-side `setup_funnel` output described in
[`docs/ENGINE_OPTIMIZATION_PLAN.md`](./ENGINE_OPTIMIZATION_PLAN.md) §0.6.

**Depends on:** Engine §0.6 (setup funnel diagnostics) must be implemented first.

---

## Phase 3 — Automation & Alerting

### 3.1 Monday Workflow / Daily Operations

Create an analyst-friendly orchestrated workflow:

1. check data freshness
2. run walk-forward validation
3. review result
4. optionally trigger paper-trading prep

This should be a visible workflow with progress states, not a hidden command chain.

---

### 3.2 Scheduler

Add an in-product scheduler for:

- pre-market refresh
- post-ingest rebuild
- walk-forward validation
- daily summary reporting

**Scheduler UI should show:**
- next run
- last run
- status
- recent failures
- linked logs and outputs

---

### 3.3 Alerts & Notification Channels

Keep alerting analyst-centric:

- alert config page
- recipient and channel management
- dry-run / preview
- delivery test

**Channel order:**
1. Telegram first
2. Webhook / Slack / Discord next
3. WhatsApp later if operationally justified

Alerts should link back to the relevant run, setup page, or comparison screen whenever
possible.

---

### 3.4 Data Pipeline Freshness & Incremental Refresh

Analysts should be able to refresh data without rebuilding everything from 2015.

**Required capabilities:**
- **freshness dashboard**: show last-ingested date per symbol, staleness warnings
- **one-click refresh**: trigger incremental build for recent dates only
- **progress visibility**: show which tables are rebuilding and estimated time remaining
- **refresh history**: log of past refreshes with duration and outcome

**Workflow:**
1. Analyst sees "data is 3 days stale" badge on dashboard
2. Clicks "Refresh to today" → triggers `pivot-build --refresh-since <last_date + 1>`
3. Progress bar shows table-by-table rebuild status
4. Badge updates to "current" when complete

**Depends on:** Engine §0.5 (fast incremental build pipeline) — all tables must support
`--refresh-since` before this can offer a clean one-click refresh.

**Cross-link:** See [`docs/ENGINE_OPTIMIZATION_PLAN.md`](./ENGINE_OPTIMIZATION_PLAN.md) §0.5
for the underlying incremental build implementation.

---

## Phase 4— Adoption & Polish

### 4.1 Settings UI

Expose analyst-safe settings without forcing env-var edits. Keep sensitive secrets outside the
normal analyst path.

### 4.2 Guided Onboarding

Provide:

- first-run checklist
- prerequisite validation
- “run your first backtest” wizard
- contextual help on metrics and badges

### 4.3 Export & Reporting

Support:

- CSV / Excel export
- scheduled summary reports
- printable comparison reports

### 4.4 Optional Chat Assistant

Use only after core workflows are stable. Chat is helpful, but it should not substitute for
structured diagnostics and comparison tools.

---

## Guardrails for Execution UX

These are non-negotiable if the product is meant for analysts:

### Presets
- approved baseline presets
- quick-test presets
- strategy-specific templates

### Safe defaults
- costs on by default
- walk-forward on by default for validation runs
- analyst-safe parameter ranges

### Job history
- every run tied to a job record
- visible logs and launch parameters

### Validation badges
- data freshness
- costs
- walk-forward
- approved / modified state
- gate status

### Experiment audit trail
- who changed what
- when
- from which preset
- decision note
- approval state

Without these guardrails, the UI becomes a prettier way to make avoidable mistakes.

---

## Architecture Changes

### New / expanded modules

```text
engine/
  notifications/
    base.py
    email_notifier.py
    telegram_notifier.py
    whatsapp_notifier.py
    webhook_notifier.py

web/
  job_runner.py              # background jobs, lock-aware execution
  pages/
    run_backtest.py          # guided run builder
    compare_runs.py          # baseline vs candidate diagnostics
    diagnostics.py           # CPR-width + exit-quality analysis
    alerts.py                # alert configuration
    scheduler.py             # analyst scheduler view
    settings.py              # analyst-safe settings

db/
  pg_models.py              # presets, job history, experiment audit trail
```

### New / expanded PostgreSQL tables

```sql
CREATE TABLE analyst_presets (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    strategy      TEXT NOT NULL,
    params        JSONB NOT NULL,
    is_approved   BOOLEAN DEFAULT false,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    updated_by    TEXT
);

CREATE TABLE job_runs (
    id             TEXT PRIMARY KEY,
    job_type       TEXT NOT NULL,
    launched_by    TEXT,
    status         TEXT NOT NULL,
    params         JSONB,
    linked_run_id  TEXT,
    started_at     TIMESTAMPTZ DEFAULT now(),
    completed_at   TIMESTAMPTZ,
    output         TEXT,
    error          TEXT
);

CREATE TABLE experiment_groups (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    hypothesis     TEXT,
    baseline_run   TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    created_by     TEXT
);

CREATE TABLE experiment_decisions (
    id             TEXT PRIMARY KEY,
    experiment_id  TEXT REFERENCES experiment_groups(id),
    run_id         TEXT NOT NULL,
    decision       TEXT NOT NULL, -- approved | rejected | exploratory
    note           TEXT,
    decided_at     TIMESTAMPTZ DEFAULT now(),
    decided_by     TEXT
);
```

---

## Recommended Delivery Sequence

### Sprint 1 — Safe execution
1. DuckDB connect-on-demand
2. `JobRunner`
3. run builder with presets
4. validation badges
5. job history

### Sprint 2 — Decision support
6. run comparison page
7. walk-forward pass/fail explanations
8. CPR-width diagnostics
9. zero-PnL / exit-mix diagnostics
10. experiment grouping + audit trail
11. setup selection funnel view

### Sprint 3 — Operations
12. Monday workflow
13. scheduler
14. alert config
15. Telegram integration
16. data freshness dashboard + one-click refresh

### Sprint 4 — Adoption
17. settings UI
18. onboarding
19. exports / reports
20. optional chat assistant

---

## Cross-Reference to Engine Work

This product roadmap depends on the following engine-side work being exposed cleanly:

- cost-aware backtests
- walk-forward carry-forward and better gate states
- CPR regime filter experiment metadata
- exit diagnostics for breakeven / zero-PnL analysis
- fast incremental build pipeline for one-click data refresh
- setup selection funnel data for filter-impact analysis

See [`docs/ENGINE_OPTIMIZATION_PLAN.md`](./ENGINE_OPTIMIZATION_PLAN.md) for those underlying
changes.
