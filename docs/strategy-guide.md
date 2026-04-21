# Strategy Guide: CPR Levels vs FBR

This guide answers the “what is the difference between CPR_LEVELS and FBR?” question for operators, analysts, and reviewers.

## Data Basis (both strategies)

- Uses previous trading day **1D** pivot structure (`pivot`, `TC`, `BC`, `R1`, `S1`).
- Uses current-day **5-minute** candles for setup detection, fill simulation, and exits.
- Directional bias is derived at the market-opening 09:15 observation candle.

```text
Direction rule:
close_915 > TC   => LONG bias
close_915 < BC   => SHORT bias
else             => no setup
```

When opening behavior is neither clearly above TC nor below BC, both strategies do not open a trade.

## CPR_LEVELS (baseline directional setup)

**Idea:** wait for price interaction with the daily CPR boundary and target the same-day floor pivot extension.

- Determine direction from 09:15 close.
- Scan from `entry_scan_start` (default `09:20`) for a candle close through boundary + buffer:
  - LONG: close >= `TC + buffer`
  - SHORT: close <= `BC - buffer`
- Set stop at the opposite CPR boundary with ATR noise buffer.
- Set target at `R1` (LONG) / `S1` (SHORT) and apply standard trailing logic.
- Short-side `cpr_hold_confirm` was tested as a stricter variant, but it is not a good default:
  - it helped on a two-day short-only slice,
  - but on the full `2025-01-01 → 2026-03-30` short run it reduced return from `112.32%` to `12.45%`.
- Short-side `cpr_confirm_entry` was also tested and rejected:
  - `2,823` trades fell to `468`, with return dropping to `6.96%`.
  - It filtered out too many good shorts.
- Raising `or_atr_min` to `0.5` for shorts was also tested and was not an improvement:
  - return moved from `112.32%` to `107.40%`.
- Adding a short-only `open_to_cpr_atr` floor was also tested and rejected:
  - `short_open_to_cpr_atr_min = 0.5` reduced the full-period short run to `1,455` trades and `37.84%` return (`₹378,441.41`, `PF 2.05`).
  - The promoted shared CPR close-clearance default is now `cpr_min_close_atr = 0.5`.
  - It removed too much of the profitable baseline.
- Adding an 80/20 CPR scale-out from `R1/S1` to `R2/S2` was also tested and rejected:
  - `cpr_scale_out_pct = 0.8` moved the long run from `43.44%` to `43.07%` and the short run from `128.08%` to `117.85%`.
  - Keep it opt-in only unless a narrower variant proves better.
- Keep the default CPR_LEVELS path on the 5-minute signal unless you are explicitly testing a new short-side filter.
- Fails setup if:
  - entry fill would be invalid (`target` behind fill),
  - effective RR < minimum required (`min_effective_rr`, default `2.0`),
  - or risk filters fail (`OR/ATR`, `gap`, optional RVOL, CPR width checks).

RR semantics:

- `rr_ratio` is the target multiple used by the trade model.
- `min_effective_rr` is the entry gate. A CPR trade only opens when the effective reward/risk at entry is at least this value.
- In other words, the engine does not open the trade just because a target exists; the target must be far enough away from the stop.

Direction-specific RVOL defaults:

- CPR_LEVELS LONG: `rvol_threshold = 1.0`
- CPR_LEVELS SHORT: `skip_rvol = true`
- FBR LONG: `skip_rvol = true`
- FBR SHORT: `skip_rvol = true`

When `skip_rvol = true`, the numeric `rvol_threshold` stored in the config is ignored at runtime.

Long RVOL sweep summary:

| Run ID | RVOL | Trades | Return | P/L | PF | Max DD |
|---|---:|---:|---:|---:|---:|---:|
| `deb2b5638274` | `1.0` | `1,092` | `50.50%` | `₹504,950.88` | `3.27` | `0.7178%` |
| `590337d33a19` | `1.1` | `1,005` | `46.91%` | `₹469,075.34` | `3.31` | `0.6623%` |
| `b80ca74b2f34` | `1.2` | `911` | `43.44%` | `₹434,407.98` | `3.38` | `0.6028%` |
| `fe252837dc83` | `1.3` | `831` | `40.19%` | `₹401,886.48` | `3.47` | `0.5971%` |

The promoted long default is `rvol_threshold = 1.0`.

## FBR (Failed Breakout Reversal)

**Idea:** a fast breakout that fails often reverses; trade the counter-move rather than the breakout direction.

- Determine initial breakout direction from the same 09:15 OR context (`direction_5`).
- Find the first breakout close beyond OR:
  - LONG breakout: close crosses above OR high + buffer
  - SHORT breakout: close crosses below OR low - buffer
- Within `failure_window`, look for a close that re-enters enough of the OR range (`fbr_failure_depth`):
  - LONG breakout -> LONG fails => reversal SHORT
  - SHORT breakout -> SHORT fails => reversal LONG
- Enter on that failure close.
- Stop uses the failed breakout extreme + ATR/reversal buffer.
- Target uses configured `rr_ratio`, then trailing rules.

## Why both can show different edge

- `CPR_LEVELS` is a directional momentum test (break of a CPR boundary).
- `FBR` is a reversal-contingency test (momentum failed and turned).
- They are complementary. Running LONG-only vs SHORT-only can isolate when one side fails in specific market regimes.

## Trailing Stop Mechanics

Both strategies use the same three-phase `TrailingStop` engine.  After entry a trade
advances through PROTECT → BREAKEVEN → TRAIL as price moves in your favour.

For candle-by-candle examples, same-bar edge cases, and the full exit matrix, see
[`docs/trailing-stop-explained.md`](docs/trailing-stop-explained.md). The summary below is
the operator-level version.

| Phase | SL sits at | Advance condition |
|---|---|---|
| **PROTECT** | Original SL (CPR boundary ± ATR buffer) | Candle close ≥ entry + 1R |
| **BREAKEVEN** | Entry price | Candle HIGH or CLOSE ≥ entry + 2R (mirror rule for SHORT uses LOW) |
| **TRAIL** | Highest close since entry − 1× ATR | Ratchet — only moves in your favour |

**Key behaviours that operators should know:**

- **1R** is the SL distance at entry, not a fixed rupee amount.  For CPR\_LEVELS LONG the SL
  is at `BC − ATR buffer`, so 1R varies by symbol and day.
- **2R** is twice the SL distance.  `min_effective_rr = 2.0` ensures the target (R1/S1) is
  always at least 2R away from entry, so TRAIL activation and the exit target are reachable.
- LONG trail activation can use the candle HIGH to arm TRAIL once the bar closes.  SHORT uses
  the mirror rule with LOW.  This is still bar-close processing, not intrabar execution: we only
  know the full OHLC after the 5-minute candle completes.
- When TRAIL activates, the stop is tightened after the 5-minute bar closes, but the triggering
  candle itself is still evaluated against the pre-update stop. This avoids assuming whether the
  2R touch happened before or after the reversal inside the candle.
- The LONG trail anchor (`highest_since_entry`) tracks the **completed bar high** once the bar has
  proven 2R. The stop still only becomes active after the bar closes, so the triggering candle is
  never retroactively re-evaluated.
- SHORT can use the same bar-touch logic, but the payoff profile is different: short selloffs
  often snap back faster, so the same trigger tends to convert targets into trailing exits more
  quickly.  When that happened, we tuned SHORT separately with `short_trail_atr_multiplier = 1.25`.
- LONG keeps the default `trail_atr_multiplier = 1.0` because the high-aware trigger was the
  profit-improving change we wanted to preserve for the morning baseline set.
- TIME\_EXIT at 15:15 takes priority — any open position is force-closed regardless of phase.

For candle-by-candle worked examples (including the April 2026 intraday-high bug and its fix)
see `docs/trailing-stop-explained.md`.

## Where to inspect behavior for any run

- **High-level run summary + params:** Dashboard `Run Results` (`/backtest`) header and KPI cards.
- `risk_based_sizing` is an opt-in experimental sizing variant for reproducing alternate sizing paths.
  Leave it off for the canonical baseline runs; the shared portfolio overlay is the default.
- **Per-trade trace:** In `Trade List` / `Top Winners` / `Top Losers`, click a row to open inspector.
  - You will see:
    - Daily CPR source values (previous-day pivot context)
    - 09:15 signal candle
    - entry candle, exit candle
    - why the setup was picked and why it exited
- **Trade-by-trade TradingView checklist:** included in the inspector as a step-by-step check list.
