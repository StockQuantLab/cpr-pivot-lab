# ADR-002: Extend CPR with Full Floor Pivot Framework

**Status**: Accepted
**Date**: 2026-02-27

## Context

`calculate_cpr()` returns only 5 values: `pivot, tc, bc, cpr_width, cpr_width_pct`. The `cpr_daily` DuckDB table stores only these same fields plus prev_high/low/close.

Professional CPR trading uses 13+ values:
- **Core CPR**: Pivot, TC, BC (already implemented)
- **Floor Pivot Levels**: R1, S1, R2, S2, R3, S3 (not implemented)
- **CPR Value Shift**: HIGHER/LOWER/OVERLAP vs previous day (not implemented)
- **Narrowing Pattern**: Consecutive days where CPR narrows (not implemented)

Floor pivot levels are where institutional traders place orders. R1/S1 are the most frequently hit intraday levels after the pivot itself. Using them as targets instead of arbitrary R:R ratios aligns the strategy with actual market structure.

## Decision

Extend the CPR framework with:

### Floor Pivot Levels (from previous day's OHLC)

```
R1 = 2 * Pivot - Low
S1 = 2 * Pivot - High
R2 = Pivot + (High - Low)
S2 = Pivot - (High - Low)
R3 = High + 2 * (Pivot - Low)
S3 = Low - 2 * (High - Low)
```

### CPR Value Shift

Compares today's CPR zone vs yesterday's:
- **HIGHER**: Today's BC > Yesterday's TC (bullish shift)
- **LOWER**: Today's TC < Yesterday's BC (bearish shift)
- **OVERLAP**: CPR zones overlap (neutral)

### Narrowing Streak

Boolean `is_narrowing = cpr_width_pct < prev_day_cpr_width_pct`. Consecutive narrowing days often precede trend days.

## Implementation

1. `calculate_cpr()` in `engine/cpr_atr_utils.py` adds R1-S3 to return dict
2. `build_cpr_table()` SQL adds R1-S3, `cpr_shift`, `is_narrowing` columns
3. `get_cpr()` returns all new columns
4. New CLI flags: `--cpr-shift HIGHER|LOWER|OVERLAP|ALL`

## Consequences

- `cpr_daily` table grows from 10 to 18 columns — still fits in DuckDB with no performance concern
- Existing strategies (FBR, VCPR) are unaffected — they don't query R1-S3
- CPR_LEVELS strategy uses R1/S1 as primary targets and Pivot as SL anchor
- Run `uv run pivot-build --force` after upgrade to repopulate runtime tables with new columns

