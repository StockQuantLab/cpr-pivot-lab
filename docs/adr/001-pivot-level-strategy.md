# ADR-001: Add CPR_LEVELS Strategy Using Floor Pivot Levels

**Status**: Accepted
**Date**: 2026-02-27

## Context

After exhaustive backtesting of 4 strategies across 10 NSE stocks over 2015-2024:

- **ORB (Opening Range Breakout)**: 0/10 stocks profitable. 86% initial SL rate. MFE peaks at ~0.3R — breakouts fail before reaching target. **Dead strategy.**
- **CPR_FADE (Mean Reversion at TC/BC)**: 8.3% win rate. Rejection detection too loose even at `wick_pct=0.6`. 52% initial SL rate. **Structurally broken.**
- **FBR (Failed Breakout Reversal)**: 10/10 profitable. 56.7% win rate, +Rs.8.48L. Exploits the 86% ORB failure rate. **Keep.**
- **VCPR (Virgin CPR Break-through)**: 10/10 profitable. 64.4% win rate, +Rs.1.91L. Sub-1R target optimal. **Keep.**

The codebase computes only Pivot/TC/BC — using ~30% of CPR theory. Professional CPR trading uses R1/S1/R2/S2/R3/S3 floor pivot levels as institutional targets. The current fixed R:R target (e.g., 2.0) doesn't match where the market actually offers support/resistance.

## Decision

1. **Add CPR_LEVELS strategy** — SL at Pivot, target at R1/S1, trail toward R2/S2.
2. **Keep FBR and VCPR** as proven profitable alternatives.
3. **Remove ORB and CPR_FADE** — both are dead strategies adding code complexity with no edge.

## Consequences

- The codebase drops from 4 strategies to 3 (CPR_LEVELS, FBR, VCPR)
- `calculate_cpr()` and `cpr_daily` table grow to include R1-S3 (6 new levels)
- CPR_LEVELS uses market-derived S/R levels instead of fixed R:R — targets are structurally meaningful
- ORB's `_simulate_day()` is replaced by CPR_LEVELS version
- CPR_FADE code (`detect_cpr_rejection`, `_simulate_day_cpr_fade`, `_get_valid_setups_cpr_fade`) is removed
- BacktestParams loses `rejection_wick_pct` and `cpr_approach_atr` fields
