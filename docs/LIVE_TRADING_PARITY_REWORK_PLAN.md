# Live Trading Parity Rework Plan

## Objective

Make `daily-replay`, `daily-live --feed-source local`, and `pivot-backtest`
produce the same trades, exits, PnL, and archived rows for the same
symbol/date/params.

Paper-mode parity (replay/live/local-live) is verified on 2026-04-09.
The active CPR_LEVELS reference baseline is the exact 8-run table in
`docs/PAPER_TRADING_RUNBOOK.md`.
Keep correctness fixes separate from optimization; any future strategy sweep
belongs in the optimization plan, not the parity rework plan.

Hard requirements:
- no hidden lifecycle differences between modes
- no transport-specific stop conditions that alter trades
- no double portfolio filtering (batch path applies constraints twice)
- no cash-release timing differences between paper and backtest
- if the same candle stream is replayed, the outcome must be identical

## Apr 20 Parity Drift Follow-up

The Apr 20 runs showed that shared engine code is necessary but not sufficient for parity.
The strategy logic is shared, but parity also depends on identical runtime inputs and an
idempotent symbol-resolution path.

### Goal

Make `pivot-backtest`, `daily-replay`, `daily-live --feed-source local`, and
`daily-live --feed-source kite` comparable on the same trade date by ensuring they all resolve the
same candidate symbols from the same inputs, then compare the resulting feed audit and trade
outputs.

### What the Apr 20 audit showed

- Backtest SHORT: 12 trades, `INR 5,168.24`
- Local-live SHORT: 14 trades, `INR 7,318.53`
- Kite-live SHORT: 38 trades, `INR 461.00`
- Local-live LONG: 5 trades, `INR 1,261.53`
- Kite-live LONG: 30 trades, `INR 13,520.00`

Stored feed audit rows are available and should be treated as the canonical tape for transport
comparison:

- live-local LONG: 843 symbols, 4,776 rows, `09:15` -> `10:15`
- live-kite LONG: 844 symbols, 4,975 rows, `09:20` -> `13:25`
- live-local SHORT: 843 symbols, 7,995 rows, `09:20` -> `15:05`
- live-kite SHORT: 844 symbols, 9,193 rows, `09:20` -> `15:15`

### Remediation Plan

#### Phase 1: Define the idempotent resolution contract

Create one symbol-resolution contract that all modes must use for a given `trade_date` and
parameter bundle. The contract should be dynamic, not hardcoded, but must return the same ordered
symbol list when the inputs are the same.

Contract inputs to record:

- `trade_date`
- `strategy`
- preset and direction
- full strategy parameters after preset expansion
- `min_price`
- CPR snapshot date chosen for prefiltering
- tradeable-universe source
- data-quality exclusions
- feed source and candle interval

Contract outputs to record:

- resolved symbol list
- symbol count
- stable resolution hash
- source rows used to resolve the list

The output is not a hand-maintained list. It is a deterministic result of the same inputs.

#### Phase 2: Make every mode call the same resolver

Backtest, replay, local-live, and kite-live should all resolve symbols through the same contract
instead of each mode assembling its own near-equivalent list.

- Backtest should not choose a separate universe path just because it is batch mode.
- Replay should not silently diverge from live-prefilter behavior.
- Local-live and kite-live should log the exact inputs that produced their symbol list.
- If the tradeable universe or CPR tables differ, that is an input drift problem, not a strategy
  change.

This keeps the behavior dynamic while still being idempotent for a given day and parameter set.

#### Phase 3: Canonicalize feed/tape comparison

Use the stored feed audit table as the bar-level compare surface:

- compare by `session_id`, `symbol`, and `bar_end`
- require identical OHLCV rows for replay/local-live parity
- allow only explicitly documented transport timing differences for kite-live
- treat missing, duplicated, or late bars as a feed regression, not a strategy regression

The compare contract should key on `bar_end` only. Wall-clock arrival time is an audit field,
not the canonical candle identity.

#### Phase 4: Extend the parity checker

Update the compare flow to fail fast when any of the following differ:

- resolution hash or symbol count
- `feed_source`
- candle interval
- preset-derived parameters
- CPR snapshot date
- bar coverage window

After those gates pass, compare:

1. run-level metrics
2. trade keys
3. setup funnel
4. feed-audit rows

#### Phase 5: Re-run and validate Apr 20

Re-run the Apr 20 backtest slice and the archived paper sessions after the resolver change.
Acceptance criteria:

- local-live and backtest match on LONG and SHORT trade keys
- local-live and backtest match on P/L within normal rounding tolerance
- kite-live is either equal or has a documented transport exception with a proven feed-audit cause
- no mode is allowed to silently choose a different symbol universe

## Pre-Market Profit Analysis

The two profit-related issues in `docs/ISSUES.md` are analysis-first items for the next two hours.
They are not code-change tasks yet. The goal is to estimate whether either one is likely to
improve realized profit and drawdown before we spend time implementing anything.

### Issue A: SHORT regime bleed

Documented in `docs/ISSUES.md` as the 10:15 intraday win-rate checkpoint.

Observed problem:
- SHORT sessions keep taking trades even after they are clearly losing intraday edge.
- The session keeps bleeding while the market continues to whipsaw.
- The same behavior is less harmful on good LONG days because the trailing phase later in the day
  can still recover profit.

Analysis-only questions to answer before any implementation:
- What was the SHORT intraday win rate at 10:15 on the recent losing days?
- If we flatten all open SHORT positions at 10:15 on those days, how much P/L do we preserve?
- Does the checkpoint improve Calmar without materially hurting strong SHORT days?
- Is there a threshold band where the checkpoint is clearly beneficial, or is the effect too noisy?

Backtest-only evidence needed:
- per-day SHORT equity curve up to 10:15
- closed-trade win rate by 10:15
- remaining open P/L at 10:15
- projected end-of-day P/L with and without checkpoint flattening
- effect on LONG when the same checkpoint logic is evaluated separately

Decision rule:
- If the checkpoint consistently reduces drawdown and improves risk-adjusted return on bad SHORT
  days without destroying good LONG/SHORT days, it is a candidate for implementation.
- If the effect is mixed or only helps one isolated day, keep it as an investigation note only.

Backtest analysis results from the retained baselines:
- SHORT baseline `b5da636ec81a` regressed at every tested threshold:
  - `0.20`: `-INR 41,225.91`
  - `0.25`: `-INR 52,440.14`
  - `0.30`: `-INR 74,314.49`
  - `0.33`: `-INR 77,322.99`
  - `0.40`: `-INR 89,300.47`
- LONG baseline `6eb4ea65763f` improved modestly:
  - `0.20`: `+INR 19,762.14`
  - `0.25`: `+INR 22,177.23`
  - `0.30`: `+INR 4,091.68`
  - `0.33`: `+INR 7,814.95`
  - `0.40`: `+INR 25,684.42`
- Conclusion: the checkpoint is not a good SHORT fix on the retained backtest set. If revisited at all,
  it should be treated as a possible LONG-side risk-off filter, not a general profit lever.

### Issue B: Trailing stop activates too late

Documented in `docs/ISSUES.md` as the progressive trail stop between 1R and T1.

Observed problem:
- The current trail leaves a dead zone between breakeven and target.
- Winning trades can give back a large unrealized gain before the ATR trail actually starts.
- This is especially visible when price moves strongly, stalls before T1, and then reverses.

Analysis-only questions to answer before any implementation:
- For the recent profitable trades, how many reached 1.5R or 2.0R before reversing?
- How many exited at breakeven or initial SL after briefly becoming meaningful winners?
- Would a 1.5R or 2.0R ratchet have locked more profit on the losing-to-flat reversals?
- Does the ratchet help SHORT more than LONG, or is the benefit symmetric?

Backtest-only evidence needed:
- max favorable excursion by trade
- time spent between 1R and T1
- count of trades that would have benefited from earlier trail activation
- estimated P/L delta under candidate ratchet levels:
  - 1.25R
  - 1.5R
  - 2.0R
- impact on Calmar and profit factor versus the current baseline

Decision rule:
- If the ratchet improves profit capture on stalled winners without cutting off the best trend
  trades, it becomes a candidate for implementation.
- If the improvement comes mostly from hindsight on one or two sessions, defer it.

Backtest analysis results from the retained baselines:
- SHORT baseline `b5da636ec81a` improved materially at every tested level:
  - `1.25R`: `+INR 4,23,117.71`, Calmar `132.58`
  - `1.50R`: `+INR 4,57,574.22`, Calmar `144.19`
  - `1.75R`: `+INR 4,51,191.41`, Calmar `134.25`
  - `2.00R`: `+INR 4,44,463.31`, Calmar `139.81`
- LONG baseline `6eb4ea65763f` also improved materially:
  - `1.25R`: `+INR 3,07,838.52`, Calmar `549.21`
  - `1.50R`: `+INR 3,44,779.57`, Calmar `534.53`
  - `1.75R`: `+INR 3,54,791.21`, Calmar `535.64`
  - `2.00R`: `+INR 3,43,732.59`, Calmar `500.49`
- Conclusion: the ratchet is the stronger candidate of the two. `1.50R` is the best SHORT raw P/L point,
  `1.75R` is the best LONG raw P/L point, and `1.75R` is the best combined raw P/L rung across the two
  retained baselines by a small margin. `1.25R` remains strongest on profit factor / win-rate.

Full ladder policy simulation (`1.25R -> 1.50R -> 1.75R -> 2.00R -> ATR`):
- SHORT: `INR 12,28,167.76` (`+INR 1,69,047.27`), win rate `72.0%`, Calmar `179.46`
- LONG: `INR 13,11,379.06` (`+INR 3,18,617.47`), win rate `75.9%`, Calmar `615.46`
- Conclusion: the full ladder is the feature we should talk about, not a single rung. It improves both
  retained baselines and preserves the current ATR trail after `2.0R`.

### Pre-market execution order

1. Run the SHORT checkpoint analysis first, because it addresses the current bleeding problem.
2. Run the trail-ratchet analysis second, because it is about profit protection rather than trade
   admission.
3. Keep both analysis runs separate from parity work so we do not confuse regime questions with
   feed/universe questions.
4. If both show a clear uplift, write the implementation plan later. Do not code before the market
   starts.

## Current State

### What is shared (DONE)

All three paper modes (replay, Kite live, local-live) call the same shared driver:
- `engine/paper_session_driver.process_closed_bar_group()` — per-bar exits/entries/pruning/risk
- `engine/paper_session_driver.complete_session()` — terminal status helper
- `engine/paper_session_driver.apply_stage_b_direction_filter()` — CPR direction pruning
- `engine/paper_runtime.evaluate_candle()` — single-candle entry/exit evaluation
- `engine/paper_runtime.execute_entry()` — position opening with tracker sizing
- `engine/bar_orchestrator.SessionPositionTracker` — shared position/cash tracking
- `engine/bar_orchestrator.select_entries_for_bar()` — alphabetical tie-breaking
- `engine/bar_orchestrator.should_process_symbol()` — entry window check
- `engine.paper_runtime.set_alert_sink()` — no-op / recorded alert sink for tests
- `engine/cpr_atr_shared.scan_cpr_levels_entry()` — shared CPR entry scan
- `engine/cpr_atr_shared.simulate_trade_lifecycle()` — shared trailing stop simulation

Shared sizing policy:
- per-position notional is capped by the slot capital
- dust allocations are skipped instead of opening 1-share / 2-share positions
- the current dust floor is 5% of slot capital with a hard minimum of Rs.1,000
- `compound_equity=False` uses the fixed session seed capital
- `compound_equity=True` uses current session equity for sizing and the overlay pass

### What remains outside the shared driver

| Aspect | Paper (replay/live) | Backtest (batch) |
|--------|---------------------|-------------------|
| Flatten | `enforce_session_risk_controls()` → `flatten_session_positions()` in `paper_runtime.py` | No flatten concept (backtest simulates to exit) |
| Archival | `archive_completed_session()` in both `paper_live.py:883` and `paper_replay.py:635` | `save_to_db()` in `BacktestResult` |
| Session status | Transitions in `paper_session_driver.py` + wrapper scripts | No session concept |
| Edge cases | Stale feed, late start, mid-session resume in `paper_live.py` | Not applicable |

### Paper-mode parity: VERIFIED

On 2026-04-09:
- `daily-replay` and `daily-live --feed-source local` match each other on SHORT trades
- All SHORT trades match by symbol, entry time, exit time, and prices
- 6 SHORT trades produced by paper, 5 by backtest (DHARMAJ missing)

### Current validation status

- The active CPR_LEVELS reference baseline is the exact 8-run table in
  `docs/PAPER_TRADING_RUNBOOK.md`
- `compound-standard` and `compound-risk` are both part of that approved set
- The April 11 CPR_LEVELS runs were retired after the review cycle
- Any new strategy sweep belongs in `docs/ENGINE_OPTIMIZATION_PLAN.md`
- Replay/local-live parity only proves the candle-based driver matches
  `intraday_day_pack`; it does not prove the live quote/tick path is equivalent.
  The 2026-04-13 `MANOMAY` incident showed that a live fill can still drift from
  the stored candle tape even when replay and local-live match each other.

## Root Cause Analysis: Backtest Divergence

The batch path (`_simulate_cpr_levels_batch` at `cpr_atr_strategy.py:1576`) and
the paper path (`process_closed_bar_group` in `paper_session_driver.py`) share
the same entry scan and trade simulation functions, but differ in two critical
ways:

### 1. Double Portfolio Filtering (HIGH — likely primary cause)

The batch path applies portfolio constraints **twice**:

**First pass** (in-bar, line 1674): `tracker.compute_position_qty()` checks
`cash_available` and `slot_capital`. This is the same as the paper path.

**Second pass** (post-hoc, line 760): `_apply_portfolio_constraints()` re-simulates
the entire portfolio, potentially skipping trades or reducing quantities that the
in-bar tracker already accepted.

The paper path only applies portfolio constraints once (in-bar tracker only).
This means the batch path can **lose trades** that the paper path keeps.

**Fix**: Skip `_apply_portfolio_constraints()` for CPR_LEVELS batch trades.
The in-bar tracker already handles sizing. The post-hoc overlay is only needed
for the old per-symbol path (FBR/VIRGIN_CPR) that has no in-bar tracker.

### 2. DHARMAJ Entry Time Discrepancy (MEDIUM — hypothesis, not confirmed)

Both paper and backtest SHOULD enter DHARMAJ SHORT at 09:45. The 09:45 candle
(close=260.10) clears all gates:
- close (260.10) < trigger (261.37)
- close (260.10) < cpr_min_close_atr gate (260.53)

What actually happened:
- **Paper entered at 09:50** (one bar late) — root cause not yet proven.
  `evaluate_candle()` loads `setup_row` on the first bar a symbol is processed
  (typically 09:15), so lazy loading alone does not explain a 09:50 delay.
  The delay may be caused by a different issue (e.g., setup-row validity,
  direction-filter timing, or entry-candidate filter cascade). Prefetching
  setup rows in replay mode (Step 6) is a reasonable hardening step, but the
  exact cause must be confirmed via per-bar tracing (Step 4).
- **Backtest missed DHARMAJ entirely** — hypothesized as the double portfolio
  filtering (Root Cause #1): the in-bar tracker accepts DHARMAJ at 09:45, but
  the post-hoc overlay rejects it because another trade consumed the slot.

The correct entry time is 09:45. Both are bugs requiring investigation.

**Fix**:
- Paper: Add per-bar tracing (Step 4) to confirm why DHARMAJ triggered at 09:50
  instead of 09:45. Prefetch setup rows in replay (Step 6) as hardening.
- Backtest: Fix double portfolio filtering (Step 5). If DHARMAJ still missing
  after the fix, use per-bar tracing (Step 4) to identify the exact rejection.

### 3. Live feed fidelity gap on 2026-04-13 `MANOMAY` (HIGH — confirmed)

The 2026-04-13 `MANOMAY` short trade exposed a live-feed fidelity gap:

- live alert / live log: entry `221.49`, exit `216.31`, `TARGET`
- current `intraday_day_pack`: `09:25` close `220.90`
- replay/local-live: match the candle tape, not the historical live fill

This is not a CPR setup bug. The symbol qualified either way. The divergence is in
the price source used at entry time.

Likely explanation:
- live uses real-time ticks/quotes aggregated into 5-minute candles
- replay/local-live use stored 5-minute candles from `intraday_day_pack`
- the historical candle tape does not preserve the same fill snapshot that the live
  session saw

Operational implication:
- treat replay/local-live as candle-driver parity checks
- treat live-vs-EOD fidelity as a separate audit problem
- `paper_feed_audit` now captures the compact live-feed rows, and
  `pivot-paper-trading feed-audit` compares them against `intraday_day_pack`
  after EOD build for daily assurance

## Implementation Steps

### Step 1: Extract shared driver (DONE)

`engine/paper_session_driver.py` created. Replay and live both delegate to it.

### Step 2: Make replay call the shared driver (DONE)

`daily-replay` calls `process_closed_bar_group()` and `finalize_session_state()`.

### Step 3: Make live call the same shared driver (DONE)

`daily-live` calls the same driver. Kite live and local-live differ only in
candle source.

### Step 4: Per-bar tracing observability (DONE)

Thread-local reject reason codes added to `find_cpr_levels_entry()` in `cpr_atr_shared.py`.
Per-bar debug logging added to `_simulate_cpr_levels_batch()` in `cpr_atr_strategy.py`.

15 reason codes: `INVALID_DIRECTION`, `DIRECTION_FILTER`, `CPR_WIDTH`, `NARROWING`,
`MIN_PRICE`, `OR_ATR_RATIO`, `GAP_SIZE`, `SHORT_OPEN_TO_CPR_ATR`, `SCAN_WINDOW`,
`RVOL`, `TRIGGER_NOT_HIT`, `CPR_MIN_CLOSE_ATR`, `SL_NORMALIZE_FAILED`,
`TARGET_BEHIND_ENTRY`, `MIN_EFFECTIVE_RR`.

### Step 5: Fix double portfolio filtering (DONE)

For `CPR_LEVELS` with `compound_equity=False` (daily reset — the default),
`_apply_portfolio_constraints()` is now skipped. The in-bar `SessionPositionTracker`
already applies portfolio constraints (max_positions + max_position_pct).

### Step 6: Harden paper replay setup-row prefetch (DONE)

Setup rows are now prefetched in `scripts/paper_replay.py` before the bar loop starts,
matching the pattern in `paper_live.py`. `state.trade_date` is pre-set to prevent
`_reset_symbol_state_for_trade_date` from clearing the prefetched rows.

### Step 7: Parity validation (DONE — 2026-04-09 SHORT VERIFIED)

Single-date parity check for 2026-04-09 CPR_LEVELS_RISK_SHORT:

| Metric | Backtest (66336e2d8646) | Paper (CPR_LEVELS_SHORT-2026-04-09) |
|--------|------------------------|--------------------------------------|
| Trade count | 6 | 6 |
| Symbols | BELLACASA, DHARMAJ, EUREKAFORB, JAYAGROGN, SSDL, TRF | Same |
| Entry times | 09:35, 09:50, 09:30, 09:40, 09:45, 10:05 | Same |
| Exit times | 10:45, 10:25, 09:50, 10:10, 11:10, 13:40 | Same |
| Entry prices | Identical | Identical |
| Exit prices | Identical | Identical |
| Position sizes | 1740, 625, 1069, 1, 1, 1000 | Same |
| Exit reasons | BREAKEVEN_SL, INITIAL_SL, TARGET, TARGET, BREAKEVEN_SL, TARGET | Same |
| PnL total | Rs.+9,567.58 | Rs.+9,567.49 (0.01% diff) |

**DHARMAJ is now present in both paths.** The 09:50 entry matches exactly.
Sub-rupee PnL deltas (BELLACASA ₹0.07, TRF ₹0.03) are from floating-point
rounding in the cost model calculation paths — acceptable.

Full baseline reruns are complete. The current daily-reset set is treated as the
frozen reference; compound-risk remains under review.

### Step 7 (old): detailed validation specs (superseded by DONE above)

<details>
<summary>Original validation spec (collapsed)</summary>

1. **Single-date parity check** for 2026-04-09:
   - Run `daily-replay --multi --strategy CPR_LEVELS --trade-date 2026-04-09 --all-symbols --no-alerts`
   - Run `pivot-backtest --all --universe-size 0 --start 2026-04-09 --end 2026-04-09 --preset CPR_LEVELS_RISK_SHORT --save`
   - Compare: trade count, symbols, entry/exit times, prices, quantities, exit_reason,
     gross_pnl, total_costs, profit_loss must all match

2. **Full baseline rerun** for all 4 non-compound presets:
   - Standard long/short, risk long/short
   - Window: 2025-01-01 to 2026-04-09
   - Compare against paper-equivalent runs

3. **If DHARMAJ still missing**: Use the per-bar tracing from Step 4 to identify
   the exact rejection point. The trace output will show:
   - Was DHARMAJ a candidate at bar 09:45?
   - Was it rejected by `find_cpr_levels_entry()` (and which filter)?
   - Or was it rejected by slot contention in `_apply_portfolio_constraints()`?

4. **Setup source verification**: Parity validation must confirm `setup_source ==
   market_day_state` only, with no live fallback setup rows involved.

</details>

### Step 8: Terminal state machine (DONE)

Unify the terminal flow across paper modes. The shared helper now lives in
`paper_session_driver.complete_session()` and both paper scripts call it.

The remaining terminal behavior is still intentionally mode-specific for:
- Flatten: `enforce_session_risk_controls()` in `paper_runtime.py`
- Status: `finalize_session_state()` in `paper_session_driver.py`
- Archive: `archive_completed_session()` in both wrapper scripts

Extract into a single `complete_session()` function in `paper_session_driver.py`:

```
PLANNING → ACTIVE → [bar loop] → STOPPING → COMPLETED
                            ↓          ↑
                     [risk trigger]  [final bar]
```

Terminal triggers (all must flatten before COMPLETED):
1. `flatten_time` reached → flatten → COMPLETED
2. Entry window closed, no open positions → NO_TRADES → COMPLETED
3. No active symbols → NO_ACTIVE_SYMBOLS → COMPLETED
4. Risk control (daily loss/drawdown limit) → flatten → STOPPING → COMPLETED
5. Local feed exhausted → flatten → COMPLETED
6. Stale feed timeout (real-time only) → flatten → COMPLETED

Idempotency rules:
- Flatten must be callable multiple times without duplicate alerts or DB writes
- Archival must be callable multiple times without duplicate `backtest_results` rows
- Session status transitions must be one-way: COMPLETED never goes back to ACTIVE

### Step 9: Adapter contract tests (DONE)

Add tests for edge cases that can cause paper-mode divergence:

| Edge case | Test |
|-----------|------|
| Duplicate closed bar (same symbol + bar_end) | Session driver drops duplicates |
| Out-of-order candles (bar_end < last processed) | Session driver rejects silently |
| Missing final bar (no 15:15 candle) | Flatten at session cutoff regardless |
| Mid-session resume with open positions | `seed_open_positions()` called before bar loop |
| Symbols with different bar counts | Shorter symbol contributes fewer candles |
| LocalTickerAdapter exhaustion | Repeated drain returns empty, `last_tick_ts` preserved |

These are unit-level tests — no alert dispatch, no DB writes.

Coverage is in:
- `tests/test_paper_runtime.py`
- `tests/test_local_ticker.py`
- `tests/test_paper_live_polling.py`
- `tests/test_paper_trading_workflow.py`

### Step 10: No-op alert sink (DONE)

Add an injected alert sink interface for automated tests. Tests inject a no-op
sink that records alerts without dispatching. This prevents accidental
Telegram/email dispatch during test runs, removing reliance on `--no-alerts`
CLI discipline alone.

Implemented via `engine.paper_runtime.set_alert_sink()`.

### Step 11: Stage B versioned migration (DONE)

The Stage B filter change (9:20 candle-close → pre-computed direction from
`strategy_day_state.direction_5`) is a versioned strategy migration.

Migration steps:
1. Rerun baselines with the new semantics (DONE — 8 baselines complete)
2. Publish a one-time diff of days where 09:15 and 09:20 direction disagree
3. Annotate dashboard/reporting outputs with the migration version
4. Add a migration note to `STRATEGY.md` or the runbook

### Step 12: Cleanup (DONE)

After all parity work is verified:
- Removed dead code: `_parse_hhmm()` in `paper_session_driver.py`
- Added comment at `local_ticker_adapter.py` explaining `_idx_by_time.get()` vs `index_of()`
- Added comment at `cpr_atr_strategy.py` documenting the dual-path dispatch for CPR_LEVELS vs FBR/VIRGIN_CPR
- Added skipped/invalid setup-row counters from `_prefetch_setup_rows` into session feed state

## What is complete

- Shared paper lifecycle driver
- Replay/live/local-live paper parity
- Daily-reset backtest parity path
- Dashboard run labels include `updated_at`
- Legacy duplicate reruns have been cleaned up
- `select_entries_for_bar()` still uses the canonical alphabetical tie-break by design; it is not a parity bug.

## What is pending

- Any further strategy-quality tuning (breakeven, entry window, CPR width)
- Scaled exits

## Acceptance Criteria

1. Replay and historical local-live match exactly for the same day/params. (DONE)
2. Kite live and replay match on all candle-generated trades that both modes observe.
3. Open positions are never left behind at terminal cutoff.
4. Session completion/archival state is identical across paper modes.
5. **Backtest and replay produce identical trades** for the same symbol/date/params
   (non-compound, daily-reset): same symbols, same entry times, same exit times,
   same prices, same quantities, same exit_reason, same gross_pnl, same total_costs,
   same profit_loss. DHARMAJ-type gaps must be resolved. (DONE — verified 2026-04-09)
6. Daily-reset baseline reruns are frozen and documented; compound-risk rows are
   still under review for exact old-vs-new reconciliation.
7. Terminal operations (flatten, archive) are idempotent across all modes.
8. Adapter edge cases have explicit test coverage.
9. Parity tests cover the CPR_LEVELS batch path (not just the old per-symbol path).
10. Setup row sourcing is `market_day_state` only — no live fallback involved in parity tests.

## Out of Scope

- changing strategy parameters
- relaxing parity to tolerate different trade counts
- using local-live as a separate simulation engine
- adding paper-only defaults that do not exist in backtest

## Compound Equity

Already implemented: `compound_equity=False` is the default (`StrategyConfig` line 189).
Both paper and backtest reset equity to `portfolio_value` daily.
The batch path uses `portfolio_value` per date (correct for daily reset).
If `compound_equity=True` is needed, the batch path must carry equity forward and
thread that capital base into the shared risk-sizing helper. That compound mode is
separate from the daily-reset parity set and should be compared only against other
compound runs.

## Review Constraint

The shared driver lives in `engine/paper_session_driver.py`.
Do not split lifecycle logic back into the scripts.
Do not add alternate stop/completion paths outside the shared driver.
