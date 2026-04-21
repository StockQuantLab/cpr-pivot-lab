# Progressive Trail Ratchet Plan

This document is a standalone implementation plan for the pre-target trail ratchet feature
documented in `docs/ISSUES.md`.

Goal:
- reduce giveback on trades that move well past 1R but stall before T1
- preserve the shared engine contract across backtest, replay, local-live, and kite-live
- keep the stop logic deterministic and parity-safe

## Current Status

This feature is paused pending a post-close retest.

What we learned:
- archived retained-baseline counterfactuals looked positive for both LONG and SHORT
- the full `2025-01-01 -> 2026-04-20` daily-reset-risk rerun improved LONG but regressed SHORT
- the code was reverted back to the pre-ratchet ATR trail before market open

Decision:
- do not ship the ratchet yet
- keep the old behavior for today's session
- revisit after market close with a narrower experiment, likely starting from the live-risk daily-reset pair only

## Problem Summary

The current trail has a dead zone:
- at `1R`, stop moves to breakeven
- at `T1`, the ATR trail starts
- between `1R` and `T1`, the stop can sit flat too long

That means a trade can be meaningfully profitable and still end as a scratch or small loser if
price reverses before the ATR trail starts.

## Analysis Results

The retained baseline runs were simulated without code changes using archived trade rows and the
stored market tape.

### Retained SHORT baseline `b5da636ec81a`

- Baseline: `4,663` trades, `INR 10,59,120.49`, win rate `33.5%`, Calmar `72.85`
- `1.25R` ratchet: `+INR 4,23,117.71`, Calmar `132.58`
- `1.50R` ratchet: `+INR 4,57,574.22`, Calmar `144.19`
- `1.75R` ratchet: `+INR 4,51,191.41`, Calmar `134.25`
- `2.00R` ratchet: `+INR 4,44,463.31`, Calmar `139.81`

### Retained LONG baseline `6eb4ea65763f`

- Baseline: `3,146` trades, `INR 9,92,761.59`, win rate `35.2%`, Calmar `155.01`
- `1.25R` ratchet: `+INR 3,07,838.52`, Calmar `549.21`
- `1.50R` ratchet: `+INR 3,44,779.57`, Calmar `534.53`
- `1.75R` ratchet: `+INR 3,54,791.21`, Calmar `535.64`
- `2.00R` ratchet: `+INR 3,43,732.59`, Calmar `500.49`

### Readout

- The ratchet improves both directions materially.
- `1.50R` is the best raw P/L point on the retained SHORT baseline.
- `1.75R` is the best raw P/L point on the retained LONG baseline.
- `1.75R` is the best combined raw P/L rung across the two retained baselines by a small margin.
- `1.25R` is the strongest profit-factor / win-quality rung.

### Combined ladder policy

The actual feature is the full ladder:
- `1.25R -> 1.50R -> 1.75R -> 2.00R -> ATR`

Counterfactual results:
- SHORT: `INR 12,28,167.76` (`+INR 1,69,047.27`), win rate `72.0%`, Calmar `179.46`
- LONG: `INR 13,11,379.06` (`+INR 3,18,617.47`), win rate `75.9%`, Calmar `615.46`

Interpretation:
- The full ladder is the right implementation target.
- It improves both retained baselines while keeping the existing ATR trail as the last phase.
- This is a cleaner feature definition than trying to choose one rung as the default.

## Proposed Rule

Use a ratchet ladder with activation thresholds:
- `1.25R`
- `1.50R`
- `1.75R`
- `2.00R`

Definitions:
- `R` is the initial per-trade risk, computed from entry price and initial stop price
- activation threshold means "when favorable excursion reaches this many R, tighten the stop"
- locked profit means the amount of R retained if the trade reverses after the trigger

Recommended behavior:
- once a rung is reached, raise the stop to the corresponding locked-profit level
- never loosen the stop
- if the ATR trail is tighter than the ratchet stop, let the ATR trail win
- if the ratchet stop is tighter, keep the ratchet stop

## Stop Math

For a long trade:
- entry = `100`
- initial stop = `98`
- risk = `2`

Ratchet levels:
- `1.00R` reached at `102.00` -> stop to `100.00`
- `1.25R` reached at `102.50` -> stop to `100.50`
- `1.50R` reached at `103.00` -> stop to `101.00`
- `1.75R` reached at `103.50` -> stop to `101.50`
- `2.00R` reached at `104.00` -> stop to `102.00`

For a short trade:
- entry = `100`
- initial stop = `102`
- risk = `2`

Ratchet levels:
- `1.00R` reached at `98.00` -> stop to `100.00`
- `1.25R` reached at `97.50` -> stop to `99.50`
- `1.50R` reached at `97.00` -> stop to `99.00`
- `1.75R` reached at `96.50` -> stop to `98.50`
- `2.00R` reached at `96.00` -> stop to `98.00`

## What "ATR trail takes over" means

It does not mean the ratchet is discarded.

It means:
- the existing ATR trail continues to update as the trade evolves
- the active stop is always the tighter of the ratchet stop and the ATR stop
- a stop can only move in the protective direction

Practical rule:
- for a long trade, the tighter stop is the higher one
- for a short trade, the tighter stop is the lower one

So the trade first gets protection from the fixed R ladder, then the ATR trail can continue
tightening if it becomes more protective.

## Example

Short trade example:
- entry: `100`
- initial stop: `102`
- risk: `2`

If price falls to:
- `97.50`, the trade has reached `1.25R`, so the stop ratchets to `99.50`
- `97.00`, the trade has reached `1.50R`, so the stop ratchets to `99.00`
- `96.50`, the trade has reached `1.75R`, so the stop ratchets to `98.50`
- `96.00`, the trade has reached `2.00R`, so the stop ratchets to `98.00`

If price then bounces and the ATR trail calculates a stop at `98.30`, the ratchet stop of `98.00`
is still tighter for a short, so the active stop remains `98.00`.

If the ATR trail later tightens to `97.70`, then the ATR stop becomes the active stop because it is
more protective.

## Implementation Plan

### Phase 1: Shared stop calculator

Add one shared ratchet-stop helper used by all execution modes.

Responsibilities:
- compute the current locked-profit stop from entry, initial stop, direction, and rung list
- compare that stop against the ATR trail stop
- return the tighter stop only
- preserve deterministic ordering and rounding

Likely touchpoints:
- `engine/cpr_atr_utils.py` for the shared stop math and ATR/tick rounding behavior
- `engine/cpr_atr_shared.py` for lifecycle integration and stop-phase transitions
- `engine/cpr_atr_strategy.py` for batch backtest wiring through the shared lifecycle
- `engine/paper_runtime.py` for live/replay trail-state updates

### Phase 2: Backtest integration

Wire the helper into the backtest trade lifecycle.

Requirements:
- the backtest must apply the same ladder logic as live and replay
- the result must remain deterministic for a fixed input tape
- existing runs should still save cleanly into `backtest_results`

### Phase 3: Paper/live integration

Use the same helper in replay, local-live, and kite-live.

Requirements:
- do not branch on feed source for the stop math itself
- only the candle source differs; stop evaluation logic stays shared
- alert and archival behavior must remain unchanged

### Phase 4: Configuration

Expose the ladder as explicit configuration, not hardcoded magic.

Suggested shape:
- `trail_ratchet_rungs = [1.25, 1.50, 1.75, 2.00]`
- keep the feature disabled unless the preset enables it
- support direction-specific overrides later if needed

Likely touchpoints:
- preset / params parsing in the strategy config layer
- `run_metadata.params_json` persistence so archived runs can be audited
- any CLI preset mapping used by backtest, replay, and daily-live entry points

### Phase 5: Validation

Run the following before any live enablement:

1. compare the modified backtest against the current retained SHORT baseline
2. compare the modified backtest against the current retained LONG baseline
3. run a replay/local-live parity check on the same date
4. verify that the final trade keys and prices stay aligned when the same tape is replayed

Test matrix:
- `tests/test_cpr_utils.py`
  - verify the new rung ladder math for LONG and SHORT
  - verify stop monotonicity
  - verify ATR stop remains tighter when it should
- `tests/test_cpr_atr_shared.py`
  - verify the shared lifecycle uses the new helper without changing exit ordering
  - verify `TRAILING_SL` / `BREAKEVEN_SL` transitions still map correctly
- `tests/test_paper_runtime.py`
  - verify live/replay trail state serializes and restores correctly
  - verify no session-state regression in late-bar stop updates
- `tests/test_parity.py`
  - verify backtest vs replay trade alignment still holds after the stop change
- `tests/test_strategy.py`
  - verify config/preset parsing exposes the new rung list cleanly

## Acceptance Criteria

- the ladder never loosens a stop
- the same input tape produces the same stop sequence in backtest and paper modes
- both retained baselines should improve or remain within an explicitly approved tolerance
- the feature should preserve the current ATR trail after the ladder has been activated
- the archived run metadata must record the rung configuration for later audits

## Recommendation

If we need one initial rung-specific fallback for comparison, `1.75R` is the best balanced single
threshold, but the ladder itself is the correct feature. `1.50R` remains the conservative SHORT-first
choice if we ever need a narrower pilot.

## Open Questions

- Should the ladder be direction-specific in the first release, or symmetric for LONG and SHORT?
- Should the ATR trail be allowed to overtake the ratchet immediately, or only after a minimum
  buffer?
- Do we want a single default rung or the full four-step ladder as the initial release?

## Related Documents

- `docs/ISSUES.md`
- `docs/LIVE_TRADING_PARITY_REWORK_PLAN.md`
