# CPR Pivot Lab — Engine Optimization Plan

> Planning doc, not operator guidance. The live runbook is `docs/PAPER_TRADING_RUNBOOK.md`.
> Use this file for strategy and engine backlog context only.

Last updated: 2026-03-27

---

## Objective

Improve the engine in the right order:

1. **Make results more truthful** via cost-aware and walk-forward-correct validation
2. **Make strategy-quality work measurable** before changing live logic
3. **Tune strategy behavior only after diagnostics exist**
4. **Reduce operational drag** so experiments and daily refreshes stay fast

This plan complements the analyst-facing roadmap in
[`docs/ANALYST_TRANSFORMATION_PLAN.md`](./ANALYST_TRANSFORMATION_PLAN.md). The engine plan
defines what must be measured and implemented; the analyst plan defines how those findings
should be exposed and controlled in product workflows.

---

## Guiding Principles & Validation Requirements

Engine changes should not be accepted because they “look better” on one backtest. Every
material change should satisfy these rules:

1. **Cost-aware first**
   - Strategy tuning must be judged on backtests that include realistic brokerage, taxes,
     and slippage.
   - Any “improvement” that disappears after costs is not a real improvement.

2. **Walk-forward before promotion**
   - Use walk-forward and out-of-sample evaluation before adopting new defaults.
   - Prefer consistency across folds over one strong in-sample year.

3. **Exit-mix reporting is mandatory**
   - Any change that touches entries, exits, stops, or filters must report:
     - exit reason mix
     - zero-PnL / breakeven exit rate
     - percent of trades reaching `>= 1R`
     - percent of `>= 1R` trades that still fail to reach target
     - MFE / MAE by exit reason where available

4. **Diagnostics before logic changes**
   - Before changing stop rules or regime filters, first add instrumentation to show where
     expectancy is being lost.
   - “We think breakeven is too aggressive” is not enough; we need measured evidence.

5. **Promote changes strategy-by-strategy**
   - CPR_LEVELS and FBR should not automatically share every new filter or exit rule.
   - Start with CPR_LEVELS for narrow-CPR refinement; extend to FBR only if data supports it.

6. **Preserve reproducibility**
   - Every experiment must record parameter values, cost assumptions, validation window,
     and baseline comparison.

### Minimum validation bundle for any strategy-quality change

At minimum, report these deltas versus the current baseline:

| Category | Required outputs |
|---|---|
| Performance | Net PnL, Calmar, max drawdown, profit factor, win rate |
| Trade quality | Trade count, average R, median R, expectancy |
| Exit behavior | Exit-reason mix, zero-PnL exit rate, `>=1R but no target` rate |
| Robustness | Walk-forward fold summary, pass/fail/inconclusive counts |
| Coverage | Symbols traded, setups per month, regime concentration |

---

## Current Findings That Change Priorities

### 1. Narrow CPR is already partially implemented

Current code computes:

- `pivot = (H + L + C) / 3`
- `bc = (H + L) / 2`
- `tc = 2 * pivot - bc`
- `cpr_width_pct = abs(tc - bc) / pivot * 100`
- `is_narrowing = current cpr_width_pct < previous day's cpr_width_pct`
- `cpr_threshold_pct = rolling percentile over prior 252 sessions`

Important implications:

- Setup selection already requires
  `cpr_width_pct < LEAST(cpr_threshold, cpr_max_width_pct)` for **both** CPR_LEVELS and FBR
- `--narrowing-filter` only adds a second filter: `is_narrowing = 1`
- FBR is therefore **already indirectly filtered by CPR width percentile and hard cap** even
  when `use_narrowing_filter` is false
- The commonly referenced ChartInk-style rule is effectively
  `abs(TC - BC) < Close * 0.001`, i.e. **0.1% of close**, not 1%

This means the next step is **not** “add narrow CPR.” The next step is to determine whether
the current dynamic percentile filter is sufficient, or whether a more explicit absolute-width
regime filter adds signal quality.

### 2. Breakeven policy likely explains many zero-PnL exits

The trailing-stop phases are currently:

- **PROTECT:** initial SL
- **BREAKEVEN:** move SL to entry after price reaches `breakeven_r * initial risk`
- **TRAIL:** only after `rr_ratio` target

Current defaults:

- `breakeven_r = 1.0`
- `rr_ratio = 2.0`

Because the stop moves to exact entry at 1R, trades that prove themselves initially and then
mean-revert often finish as `BREAKEVEN_SL` at zero PnL. That may be correct behavior — or it
may be prematurely cutting trades that still had enough structure to reach target.

This makes **breakeven diagnostics** a higher priority than additional exit features.

---

## Phase 0 — Measurement & Validation Foundation

These items should happen before major strategy retuning. Otherwise we risk optimizing on
distorted results.

### 0.1 Slippage & Commission Model

**Status:** DONE
**Priority:** P0 — directly affects net expectancy

**Problem:** Current backtests assume zero slippage and zero brokerage. Reported PnL and
Calmar are therefore optimistic, especially for strategies with many smaller outcomes.

**Implementation:**
1. Add a `SlippageModel` / cost configuration to the engine
2. Support at least:
   - fixed brokerage per trade
   - slippage in bps per side
   - STT / exchange / regulatory charges
3. Deduct costs in trade-close accounting
4. Add CLI flags such as:
   - `--commission-model zerodha|zero`
   - `--slippage-bps N`
5. Default new runs to a realistic broker model, not zero-cost mode

**Validation:**
- Re-run core baselines with costs on
- Report pre-cost vs post-cost deltas
- Re-rank strategies using **net** metrics, not gross metrics

**Why this comes first:** Strategy tuning without costs will bias us toward fragile,
high-turnover behavior.

---

### 0.2 Walk-Forward Equity Carry-Forward

**Status:** DONE
**Priority:** P0 — needed for realistic fold-to-fold behavior

**Problem:** Walk-forward folds currently reset capital instead of carrying forward actual
equity, which understates compounding effects and drawdown path dependence.

**Implementation:**
1. Add `--carry-forward-equity`
2. Pass ending fold equity into the next fold as starting capital
3. Store both:
   - per-fold gate metrics
   - cumulative equity across folds
4. Keep per-day gate logic separable from cumulative equity presentation if needed

**Validation:**
- Compare walk-forward cumulative equity to full backtest equity
- Confirm any remaining divergence is explainable by fold boundaries, not capital resets

**Depends on:** none

---

### 0.3 Exit Diagnostics & Run Attribution

**Status:** DONE
**Priority:** P0 — prerequisite for breakeven and exit-policy tuning

**Problem:** We currently know headline outcomes, but not enough about **how** trades evolve.
Without richer exit attribution, changes to stops or filters are guesswork.

**Implementation:**
1. Extend trade-level outputs with diagnostics such as:
   - `reached_1r`
   - `reached_2r`
   - `max_r`
   - `mfe_after_breakeven`
   - `mae_before_breakeven`
   - `exit_phase` (`PROTECT`, `BREAKEVEN`, `TRAIL`, `TIME_EXIT`, etc.)
2. Add run-level aggregates:
   - zero-PnL exit rate
   - share of trades that reached `>=1R` but did not hit target
   - expectancy by exit reason
   - average/max MFE lost after breakeven activation
3. Store these metrics in a way the analyst UI can compare side-by-side across runs

**Validation:**
- Manual spot-check on sample trades to ensure phase attribution is correct
- Confirm aggregate counts reconcile with total trades and exit reason totals

**Cross-link:** This is the engine-side prerequisite for the analyst diagnostics tier in
[`docs/ANALYST_TRANSFORMATION_PLAN.md`](./ANALYST_TRANSFORMATION_PLAN.md).

---

### 0.4 Walk-Forward Gate Criteria Improvements

**Status:** DONE
**Priority:** P1 — prevents weak promotions and misleading failures

**Problem:** Current gate logic is too binary (`PnL >= 0`). It does not express insufficient
sample size, and it does not explain *why* a run passed or failed.

**Implementation:**
1. Add minimum trade-count thresholds
2. Support three states:
   - `PASS`
   - `FAIL`
   - `INCONCLUSIVE`
3. Include supporting metrics per fold:
   - trade count
   - net PnL
   - profit factor
   - max drawdown
   - exit mix summary
4. Add optional multi-metric gate rules:
   - PnL threshold
   - profit-factor floor
   - drawdown ceiling
5. Persist fold-level explanations so the dashboard can show “why this run failed”

**Validation:**
- Re-score historical walk-forward runs and inspect classification changes
- Confirm low-sample folds move to `INCONCLUSIVE` instead of false `PASS` / `FAIL`

---

### 0.5 Fast Incremental Build Pipeline

**Status:** DONE
**Priority:** P0 — daily operations pay full rebuild costs; blocks routine refresh

**Problem:** When new market data arrives (e.g. 5 new trading days from Kite), the operator
must rebuild ALL runtime tables from 2015 history even though only recent rows changed.
Only `intraday_day_pack` currently supports `--refresh-since`. Every other table requires
a full DROP + CREATE, costing 60–90 minutes for a monthly update cycle.

**Current state by table:**

| Table | Incremental? | Full rebuild time | Notes |
|---|---|---|---|
| `cpr_daily` | ❌ | ~5 min | Reads daily Parquet; static unless new data ingested |
| `atr_intraday` | ❌ | ~5–10 min | Symbol-batched but no date filter |
| `cpr_thresholds` | ❌ | ~10 min | 252-day rolling window; needs lookback logic |
| `or_daily` | ❌ | ~5 min | Expensive `strftime` scan on v_5min |
| `market_day_state` | ❌ | ~10–20 min | Joins 5+ upstream tables |
| `strategy_day_state` | ❌ | ~10 min | Derived from market_day_state |
| `intraday_day_pack` | ✅ | ~30s for 50 symbols | DELETE + INSERT by date; best-in-class |
| `virgin_cpr_flags` | ❌ | ~5 min | Full CPR zone vs intraday scan |

**Table dependency chain (build order):**

```
v_daily (Parquet) ──→ cpr_daily ──→ cpr_thresholds (252-day rolling)
                          │                │
v_5min (Parquet) ──→ atr_intraday ─────────┤
                 ──→ or_daily ─────────────┤
                                           ▼
                                   market_day_state
                                           │
                                   strategy_day_state
                                           │
                                   intraday_day_pack ──→ virgin_cpr_flags
```

**Implementation — extend `--since` to all tables:**

The pattern proven by `intraday_day_pack` should be applied uniformly:

1. **DELETE rows** where `trade_date >= since_date`
2. **Re-query source** with date filter (plus lookback where needed)
3. **INSERT** new rows

Table-specific considerations:

- **`cpr_daily`**: Straightforward DELETE + INSERT for `trade_date >= since_date`.
  Only needs daily Parquet which is append-only.
- **`atr_intraday`**: DELETE + INSERT. Include `since_date - 1` for prev_close
  cross-day dependency.
- **`cpr_thresholds`**: DELETE + INSERT. Must rebuild from `since_date - 252 trading days`
  to maintain rolling-window correctness, but only INSERT rows `>= since_date`.
- **`or_daily`**: DELETE + INSERT. Add date filter to the v_5min strftime query.
- **`market_day_state`**: DELETE + INSERT. Add `since_date` filter to inner JOINs.
- **`strategy_day_state`**: DELETE + INSERT. Same pattern as market_day_state.
- **`virgin_cpr_flags`**: DELETE + INSERT. Add date filter to CPR zone scan.

**Target workflow after implementation:**

```bash
# Ingest recent data (Kite or manual Parquet drop)
doppler run -- uv run pivot-kite-ingest --5min --from 2026-03-20 --to 2026-03-25

# Rebuild ONLY the affected date range across ALL tables
doppler run -- uv run pivot-build --refresh-since 2026-03-20

# Time: ~10–18 min instead of 60–90 min (75–80% faster)
```

**Validation:**
- Rebuild a recent window incrementally and compare row counts + checksums to a full rebuild
- Target daily refresh time ≤ 30 seconds for 1–5 new days
- Target monthly refresh time ≤ 18 minutes for ~22 new days
- Ensure rolling-window tables (`cpr_thresholds`) produce identical results

**Depends on:** none (can start immediately)

**Cross-link:** This replaces the less detailed §2.2 below, which is retained as a
reference but should be considered superseded by this section.

---

### 0.6 Setup Selection Funnel Diagnostics

**Status:** DONE
**Priority:** P0 — prerequisite for understanding filter impact

**Problem:** The engine applies a multi-stage filter pipeline to select trading setups:

1. Universe definition (CLI symbols, `--all`, or `--universe-name`)
2. CPR width filter (`cpr_width_pct < LEAST(cpr_threshold, cpr_max_width_pct)`)
3. Gap filter (`gap_abs_pct <= max_gap_pct`)
4. Opening Range ATR filter (`or_atr_5 BETWEEN or_atr_min AND or_atr_max`)
5. Minimum price filter (`close >= min_price`)
6. Direction filter (9:15 close vs TC/BC)
7. Optional narrowing filter (`is_narrowing = 1`)
8. Optional CPR shift filter (`cpr_shift IN (...)`)
9. Entry rules (TC/BC touch for CPR_LEVELS, OR failure for FBR)

Today, only the final traded setups are visible. There is no record of how many candidates
entered the funnel, how many were rejected at each stage, or which filter is the binding
constraint. This makes it impossible to evaluate whether a regime filter is helping or
merely reducing opportunity.

**Implementation:**
1. Add a `setup_funnel` output to the backtest engine that records per-day counts:
   - `universe_count`: symbols in the starting universe
   - `after_cpr_width`: passed CPR width filter
   - `after_gap`: passed gap filter
   - `after_or_atr`: passed OR/ATR filter
   - `after_min_price`: passed min-price filter
   - `after_direction`: passed direction determination
   - `after_narrowing`: passed narrowing filter (if enabled)
   - `after_shift`: passed CPR shift filter (if enabled)
   - `entry_triggered`: entry rules met → trade taken
2. Store funnel data alongside run results (new DuckDB table or JSONB column in run_metrics)
3. Surface funnel summary in CLI output after each run
4. Expose to dashboard for visual funnel analysis (see analyst plan §2.6)

**Validation:**
- Funnel counts must reconcile: each stage count ≤ previous stage count
- `entry_triggered` count must equal actual trade count for the day
- Spot-check sample days manually against SQL queries on raw setup data

**Cross-link:** The analyst-facing funnel view is defined in
[`docs/ANALYST_TRANSFORMATION_PLAN.md`](./ANALYST_TRANSFORMATION_PLAN.md) §2.6.

---

## Phase 1 — Strategy-Quality Experiments

Do these after Phase 0 instrumentation is in place.

### 1.1 Narrow CPR Regime Filter Refinement

**Status:** NOT STARTED  
**Priority:** P1 — promising, but should be run as a measured experiment

**Problem:** The project already applies CPR width percentile and a hard max-width cap for
both CPR_LEVELS and FBR. The open question is whether signal quality improves further with a
more explicit **absolute-width regime filter** or a refined combination of filters.

**Recommended framing:** Treat this as a **regime-filter experiment**, not as a missing
feature.

**Experiment tracks:**
1. **Baseline (current):**
   - percentile threshold + hard cap
2. **Absolute-width only:**
   - ChartInk-style thresholds such as `abs(TC - BC) < Close * 0.001`
   - test nearby values as sensitivity bands, e.g. 0.10%, 0.15%, 0.20%
3. **Hybrid filter:**
   - percentile threshold **and** absolute-width threshold
4. **Optional narrowing add-on:**
   - current filter plus `is_narrowing = 1`

**Recommendation on sequencing:**
1. Run this on **CPR_LEVELS first**
2. Promote to FBR only if:
   - net metrics improve after costs
   - trade coverage does not collapse
   - walk-forward remains stable

**Implementation:**
1. Add an explicit absolute-width filter parameter
2. Keep dynamic percentile and hard cap available independently
3. Record which regime filter combination was used in run metadata
4. Surface setup counts filtered in/out by each regime rule

**Validation:**
- Compare baseline vs candidate on:
  - net Calmar
  - drawdown
  - trade count
  - hit rate
  - monthly consistency
  - exit-mix changes
- Segment results by year and by strategy
- Reject candidates that only improve by drastically reducing trade count without improving
  robustness

---

### 1.2 Breakeven / Zero-PnL Exit Optimization

**Status:** NOT STARTED  
**Priority:** P1 — likely high leverage, but diagnostics must come first

**Problem:** With `breakeven_r = 1.0` and SL moved to exact entry, many trades that reach
1R may later exit at zero as `BREAKEVEN_SL`. This may be reducing expectancy and obscuring
whether entry quality or stop logic is the main issue.

**Do not change logic blindly.** First quantify the current behavior.

**Required baseline measurements before tuning:**
- percent of trades reaching `>= 1R`
- percent of `>= 1R` trades exiting at zero
- zero-PnL exit rate as share of all trades
- post-breakeven MFE distribution
- outcome distribution for trades that touched 1R but not 2R

**Candidate experiment directions:**
1. **Delayed breakeven**
   - move to entry at `1.25R` or `1.5R` instead of `1.0R`
2. **Lock-in epsilon**
   - move stop to `entry + costs` or a small positive offset, not exact entry
3. **Structure-based breakeven**
   - move SL under/over the last swing rather than flat entry price
4. **Volatility-aware breakeven**
   - require ATR-normalized progress before BE activation
5. **Partial exits before BE**
   - take a small partial at 1R, then give remainder more room

**Implementation:**
1. Parameterize breakeven behavior rather than hard-coding one policy
2. Add new config options for:
   - breakeven trigger
   - breakeven offset
   - structure-based mode
   - partial-exit interaction
3. Keep the current exact-entry 1R rule as the baseline comparator

**Validation:**
- Compare candidates against baseline on:
  - net expectancy
  - zero-PnL exit rate
  - `% trades >=1R but no target`
  - exit-mix shifts
  - drawdown impact
  - walk-forward stability
- Start with CPR_LEVELS first unless FBR diagnostics show the same pathology

**Success condition:** lower avoidable zero-PnL exits without creating materially larger
average losses or worse drawdowns.

### 1.2.1 Post-Parity Baseline Snapshot (2026-04-11)

**Status:** Observed, not yet changed

These notes capture the current measured state after the parity and sizing fixes. They are
documentation for the next tuning pass, not a request to change live logic yet.

Current interpretation:

- The active CPR_LEVELS reference baseline is the exact 8-run table in
  `docs/PAPER_TRADING_RUNBOOK.md`.
- `compound-standard` scales with equity as expected.
- `compound-risk` uses the shared current-equity allocator and is part of the approved
  reference set. Treat any future drift as a regression against the current May 5 baseline
  family in `docs/PAPER_TRADING_RUNBOOK.md`.
- Strategy tuning is still separate from parity work.

#### Current daily-reset reference runs

| Run | Calmar | WR | PF | P/L |
|---|---:|---:|---:|---:|
| STD_LONG `d9bfe51d2d49` | 204 | 33.6% | 3.41 | ₹1.724M |
| RISK_LONG `dccce0e0ada6` | 207 | 33.6% | 3.46 | ₹1.718M |
| STD_SHORT `05fd7c6f7184` | 88 | 30.0% | 2.57 | ₹1.615M |
| RISK_SHORT `f6e520155aa7` | 94 | 30.2% | 2.62 | ₹1.632M |

#### What this means

- STD and RISK are now nearly identical after the shared sizing fixes.
- The older RISK outperformance was inflated by oversized positions and dust fills.
- Risk sizing is now behaving like a control rule, not a hidden profit amplifier.
- Compound STD and compound RISK are not the same thing:
  - STD compound scales notional with current equity because it derives allocation from the
    growing slot capital.
  - RISK compound now threads the current session capital base into the shared sizing helper
    when `compound_equity=True`, so its risk budget can grow with equity instead of staying
    anchored to the fixed `params.capital` seed.
  - Daily-reset RISK still uses the fixed seed capital, which is the correct apples-to-apples
    reference for paper replay/live.

#### Pending validation

- Keep the 2026-05-06 `full_2026_05_06` reference baseline fixed unless a future change is
  explicitly intended to alter the reference behavior.
- Use the current reference baseline set for any future regression checks.

#### Measured follow-up hypotheses

1. **Breakeven is a likely high-leverage knob**
   - With `breakeven_r = 1.0`, a large share of trades are converted to `BREAKEVEN_SL`.
   - The current buckets show:
     - `1,121` LONG and `1,602` SHORT trades exit at breakeven.
     - `556` LONG and `991` SHORT trades never reach `1R`.
     - `350` LONG and `470` SHORT trades reach past `1R` but still exit at initial SL.
   - That last bucket is suspicious and may indicate same-candle timing or exit precedence issues.

2. **Late entries look weaker**
   - 10:xx entries have lower win rate and lower average P/L than 09:xx entries.
   - This supports an entry-window sweep, but it is not a change recommendation yet.

3. **CPR width appears to matter**
   - Medium-width CPR days outperform ultra-narrow days on per-trade economics.
   - The current percentile gate admits a lot of ultra-narrow setups, which may be diluting quality.

4. **Any future sweep must stay shared**
   - If breakeven, entry window, CPR width, or sizing is changed, the change must apply
     identically to backtest, paper replay, and live.
   - Baselines must be rerun after any such change.

5. **Compound-risk validation COMPLETE (2026-05-01)**
   - Backtest compound-risk now uses the same shared `SessionPositionTracker.compute_position_qty()`
     sizing helper as paper/live.
   - Active compound-risk baselines: LONG `480a14f8aa26`, SHORT `f377d33a9157`.
   - Earlier compound-risk runs created before the sizing-path fix were deleted and must not be
     used as references.

6. **Compound risk sizing now has explicit semantics**
   - Compound risk mode uses the current session capital base.
   - Daily-reset risk mode uses the fixed seed capital.
   - Any further tuning must keep those two modes separate in the documentation and rerun
     the relevant baselines after changing either one.

---

### 1.3 Scaled Exits (80/20 Split)

**Status:** NOT STARTED  
**Priority:** P2 — only after breakeven measurement exists

**Problem:** All-or-nothing exits may under-capture strong extensions, but scaled exits add
meaningful complexity and can hide weak core signal quality if done too early.

**Implementation:**
1. Add scaled-exit configuration:
   - `t1_exit_pct`
   - `t2_trail_pct`
   - `t2_trail_atr`
2. Support partial fill accounting and sub-position tracking
3. Add new exit reasons for first target and runner behavior

**Validation:**
- Compare against the best non-scaled baseline, not just the current engine default
- Report whether improvement comes from higher tail capture or from simply reducing realized
  variance

**Sequencing note:** Do this **after** the breakeven track, otherwise attribution becomes
confused.

### 1.5 Tie-break scoring experiment

**Status:** NOT STARTED

Current tie-break is deterministic symbol order for reproducibility. If we later want to
rank same-bar candidates by a quality score such as effective RR, that must be treated as a
strategy change, documented, and baseline-rerun across backtest, replay, and live.

---

### 1.4 FBR Direction Filter Semantic Cleanup

**Status:** DONE  
**Priority:** P1 — correctness and analyst clarity

**Problem:** `direction_filter` is semantically confusing for FBR because breakout direction
and trade direction are opposite. This has already caused labeling bugs.

**What is already fixed:**
- Walk-forward matrix labels were corrected
- `pivot-backtest` accepts `--trade-direction-filter` as an alias for `--direction`
- Run-detail dashboards now label the control as a setup filter and surface the actual LONG/SHORT trade split

**What remains:**
1. Clean up historical labeling where feasible
2. Ensure any future analyst views continue to show actual trade direction, not ambiguous setup direction

**Validation:**
- Run regression tests around FBR long/short labeling
- Verify dashboard summaries and run metadata agree with actual executed trade direction

---

## Phase 2 — Operational & Platform Foundations

These items do not directly improve signal quality, but they determine how quickly we can
iterate and how reliably analysts can use the system.

### 2.1 DuckDB Read-Only Mode for Dashboard

**Status:** DONE  
**Priority:** P0 — blocks concurrent dashboard + backtest usage

**Problem:** On Windows, the dashboard's persistent DuckDB connection blocks writes and forces
manual process killing before backtests or builds.

**Solution direction:** Use connect-on-demand query patterns as documented in
[`docs/ANALYST_TRANSFORMATION_PLAN.md`](./ANALYST_TRANSFORMATION_PLAN.md).

**Implementation:**
1. Replace the module-level connection singleton with per-query open/close
2. Add graceful handling when a backtest temporarily owns the lock
3. Keep existing cache layers so UX stays fast

**Current state:** Dashboard state uses a dedicated read-only DuckDB handle via
`db.get_dashboard_db()`, so dashboard reads can coexist with backtests and builders.

**Validation:**
- Verify dashboard reads, backtests, and builds can alternate without manual intervention

---

### 2.2 Incremental Build for All Tables

**Status:** SUPERSEDED — promoted to §0.5 in Phase 0

This item has been elevated to Phase 0 (§0.5) because daily operations are blocked by
full-rebuild costs. See §0.5 for the detailed table-by-table implementation plan.

---

### 2.3 Single `pivot-refresh` Command

**Status:** DONE  
**Priority:** P1 — simplifies routine operations

**Problem:** Daily refresh is currently spread across multiple commands and mental steps.

**Implementation:**
1. Create a single command that orchestrates:
   - new data detection
   - optional conversion
   - incremental builds
   - optional paper-trading preparation
2. Auto-detect the refresh start date where possible
3. Emit a clear summary of what changed

**Current state:** `pivot-refresh` now auto-detects the next refresh date from the
runtime tables, runs `pivot-build --refresh-since`, and can optionally launch
`daily-prepare` for the refreshed trade date.

**Depends on:** 2.2 Incremental build for all tables

**Validation:**
- Prove the command can handle “no-op”, one-day refresh, and multi-day catch-up cases

---

### 2.4 Scanner-First Live Paper Startup

**Status:** DONE  
**Priority:** P0 — live paper must support full-universe scanning without brute-force polling

**Problem:** `daily-live --all-symbols` needs to scan the opening bar across the full universe,
but it should not keep polling rejected symbols for the rest of the day. The live path was
effectively using the whole universe as a perpetual watchlist.

**Implementation:**
1. Chunk Kite quote polling to the broker batch limit
2. Classify each symbol after the first completed 5-minute candle
3. Retire symbols that do not produce a valid setup after the opening scan
4. Keep only candidate symbols and open positions in the live watchlist
5. Archive no-candidate sessions cleanly once the active universe is exhausted

**Validation:**
- Full-universe live start should stay responsive on Monday morning
- The polling universe should collapse from the opening scan to the candidate shortlist
- Repeated empty polling should stop once there are no active symbols left

**Depends on:** none

---

### 2.5 Walk-Forward and Live Runtime Hardening

**Status:** DONE  
**Priority:** P0 — prevents false failures and live event-loop stalls

**Problem:** A few operational edge cases were still making the Monday flow brittle:

1. Walk-forward trade-date selection could fall back to raw weekdays when the primary
   market-session source was unavailable, which could count holidays incorrectly.
2. DuckDB persistence failures during fast validation could abort the whole run even when the
   backtest fold itself was valid.
3. Live paper setup resolution was still doing synchronous DuckDB work inside the async loop.

**Implementation:**
1. Prefer data-backed trade dates from `v_5min`, then `v_daily`, then runtime tables before
   falling back to weekday-only iteration.
2. Treat walk-forward DuckDB persistence failures as warnings and continue the validation run.
3. Offload live setup loading to a worker thread so the paper loop does not block the event loop.

**Validation:**
- Holiday gaps should not inflate walk-forward denominators when market session data exists.
- WAL-lock style persistence failures should leave the validator runnable end-to-end.
- Live session startup should still create and update a session while scanning the opening bar.

**Depends on:** none

---

## Phase 3 — Portfolio Risk & Exploratory Analysis

### 3.1 Portfolio-Level Correlation & Sector Risk

**Status:** NOT STARTED  
**Priority:** P2 — risk containment

**Problem:** Position slots can concentrate into one sector, creating correlated drawdowns.

**Implementation:**
1. Add sector mapping data
2. Enforce optional concentration limits before entry
3. Report sector exposure and rejected trades

**Validation:**
- Compare drawdown and trade opportunity cost with and without sector limits

---

### 3.2 Time-of-Day PnL Analysis

**Status:** DONE  
**Priority:** P2 — exploratory optimization

**Problem:** We do not yet know which entry windows contribute most of the edge for each
strategy.

**Implementation:**
1. Bucket entries by time of day
2. Aggregate PnL, win rate, and expectancy by bucket
3. Support optional entry-window restrictions for experiments

**Current state:** Backtest summaries now include hourly entry buckets with trade count,
PnL, and win rate so we can spot time-of-day edge concentration quickly.

**Validation:**
- Compare performance across entry windows without overfitting to one calendar segment

---

## Recommended Execution Sequence

The old flat priority list is not enough; the work should be executed in phases.

### Phase 0 — Trust the metrics
1. Slippage & commission model
2. Walk-forward equity carry-forward
3. Exit diagnostics & run attribution
4. Walk-forward gate improvements
5. Fast incremental build pipeline
6. Setup selection funnel diagnostics

### Phase 1 — Improve strategy quality
7. Narrow CPR regime filter refinement
8. Breakeven / zero-PnL exit optimization
9. Scaled exits
10. FBR direction semantic cleanup

### Phase 2 — Remove operational friction
11. DuckDB read-only dashboard access
12. Incremental build for all tables (see §0.5 for detailed plan)
13. Single `pivot-refresh` command

### Phase 3 — Strengthen robustness
14. Sector-risk controls
15. Time-of-day analysis

### Why this ordering

- **Costs and walk-forward correctness** must precede tuning
- **Diagnostics** must precede stop-policy changes
- **CPR filter refinement** should be tested before more complex exit features
- **Operational speedups** matter, but not at the expense of optimizing on misleading metrics

---

## Immediate Next 3 Actions

If work starts now, the recommended first sprint is:

1. **Extend `--refresh-since` to all runtime tables** (§0.5)
   - Unblocks fast daily refresh without 60–90 min full rebuilds
2. **Add setup selection funnel diagnostics** (§0.6)
   - Shows where setups are rejected, which filter is binding
3. **Implement cost model** (§0.1) **+ run CPR_LEVELS experiment pack** (§1.1)
   - Answers: is current narrow-CPR logic good enough after costs?

That sequence delivers operational speed, diagnostic visibility, and strategy-quality
answers within the first sprint.

---

## Cross-References

- Analyst product roadmap:
  [`docs/ANALYST_TRANSFORMATION_PLAN.md`](./ANALYST_TRANSFORMATION_PLAN.md)
- Paper trading operations: `docs/PAPER_TRADING_RUNBOOK.md`
- Data ingestion: `docs/KITE_INGESTION.md`
