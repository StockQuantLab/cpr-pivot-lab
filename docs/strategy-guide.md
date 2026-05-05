> This guide answers the “what is the difference between CPR_LEVELS and FBR?” question for
> operators, analysts, and reviewers. It is written so a trader can understand the choices
> without reading code.

## Quick Glossary

| Term | Trader meaning | Simple example |
|---|---|---|
| `TC` / `BC` | Top and bottom of the CPR zone from yesterday's candle. Treat it like the day's decision zone. | Price above `TC` means buyers are stronger; below `BC` means sellers are stronger. |
| `R1` / `S1` | First upside/downside pivot target. | LONG exits at `R1`; SHORT exits at `S1`. |
| `R2` / `S2` | Farther second target. | Tested, but rejected for the current CPR strategy. |
| `OR` | Opening range from the first 5-minute candle. | A quiet opening bar means weak momentum; a huge opening bar means the move may already be done. |
| `ATR` | Normal daily movement size for that stock. | A ₹500 stock with ATR ₹10 normally moves about ₹10 per day. |
| `RR` | Reward divided by risk. | Risk ₹5 to target ₹10 profit = 2.0 RR. |
| `RVOL` | Current volume compared with normal volume for that time of day. | `1.0` means normal volume; `1.5` means 50% above normal. |

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

Example:

- Yesterday's CPR zone is `BC = ₹98`, `TC = ₹100`.
- Today's 09:15 candle closes at `₹101`.
- Because `₹101 > TC`, the day has a LONG bias for CPR_LEVELS.
- If the candle closes at `₹99`, it is inside the CPR zone, so there is no setup.

## CPR_LEVELS (baseline directional setup)

**Idea:** wait for price interaction with the daily CPR boundary and target the same-day floor pivot extension.

- Determine direction from 09:15 close.
- Scan from `entry_scan_start` (default `09:20`) for a candle close through boundary + buffer:
  - LONG: close >= `TC + buffer`
  - SHORT: close <= `BC - buffer`
- Set stop at the opposite CPR boundary with ATR noise buffer.
- Set target at `R1` (LONG) / `S1` (SHORT) and apply standard trailing logic.

Plain example:

- Yesterday's CPR zone is `BC = ₹98`, `TC = ₹100`.
- Today's first candle closes at `₹101`, so the stock has LONG bias.
- The strategy waits for a valid candle close above `TC + buffer`.
- If entry is near `₹101`, stop is near `BC`, and target is `R1`.
- For SHORT, mirror the same logic: close below `BC`, stop near `TC`, target `S1`.
<!-- REJECTED_VARIANTS -->
- **cpr_hold_confirm (SHORT):** Helped on a two-day slice, but full-period return dropped from `112.32%` → `12.45%`.
- **cpr_confirm_entry (SHORT):** Trades fell from `2,823` → `468`, return `6.96%`. Filtered out too many good shorts.
- **or_atr_min = 0.5 (SHORT):** Return moved from `112.32%` → `107.40%`. Not worth the trade count reduction.
- **open_to_cpr_atr floor (SHORT):** `short_open_to_cpr_atr_min = 0.5` → `1,455` trades, `37.84%` return (`₹378K`, `PF 2.05`). The shared `cpr_min_close_atr = 0.5` default is the promoted version.
- **CPR scale-out / R2-S2 target experiments:** rejected. Corrected 2026-05-05 tests showed
  `scale_out_pct = 0.5` and `0.8`, full-position R2/S2 targets, and R2/S2 RR-gate variants all
  underperformed canonical R1/S1 full exits. Keep `scale_out_pct = 0.0`, `target_level = FIRST`,
  and `rr_gate_target_level = AUTO` unless starting a new explicit experiment.
<!-- /REJECTED_VARIANTS -->

- Keep the default CPR_LEVELS path on the 5-minute signal unless you are explicitly testing a new short-side filter.
- Fails setup if:
  - entry fill would be invalid (`target` behind fill),
  - effective RR < minimum required (`min_effective_rr`, default `2.0`),
  - or risk filters fail (`OR/ATR`, `gap`, optional RVOL, CPR width checks).

### Plain Trader Examples: Targets and Scale-Out

Assume a simple LONG trade:

- Buy 100 shares at `₹100`
- Stop loss is `₹95`
- First target `R1` is `₹110`
- Second target `R2` is `₹120`

These examples explain the experiment settings in trader language. The current default remains the
first row: sell the full position at R1/S1.

| Setting | What happens | If price reaches R1 then reverses | If price reaches R2 | Current verdict |
|---|---|---:|---:|---|
| `scale_out_pct = 0.0` | Sell all 100 shares at R1 (`₹110`) | `₹1,000` profit; trade done | Not applicable; already exited | **Default** |
| `scale_out_pct = 0.5` | Sell 50 shares at R1, keep 50 for R2 | `₹500` profit if runner exits near entry | `₹1,500` total profit | Rejected |
| `scale_out_pct = 0.8` | Sell 80 shares at R1, keep 20 for R2 | `₹800` profit if runner exits near entry | `₹1,200` total profit | Rejected |
| `target_level = SECOND` | Skip R1 exit; sell all 100 shares only at R2 | Can give back the R1 profit if price reverses | `₹2,000` profit | Rejected |
| `rr_gate_target_level = SECOND` | Only enter trades whose R2 distance looks good enough | Fewer/lower-quality R1 exits may remain | Higher win-rate filter, but lower total P/L in tests | Rejected |

Why rejected: scale-out looks safer because it locks some profit early, but corrected full-period
tests showed it reduced total strategy P/L. The canonical CPR edge is to take the full position at
R1/S1 rather than reserve part of the trade for R2/S2.

### Plain Trader Examples: Entry Filters

Assume the same LONG setup: `TC = ₹100`, `BC = ₹98`, ATR = `₹4`, entry around `₹102`, target
`R1 = ₹110`.

| Option | What it asks in trader language | Example pass/fail |
|---|---|---|
| `cpr_percentile = 33` | Is today's CPR zone narrow enough compared with this stock's own history? | Narrow CPR passes; wide CPR skips. |
| `cpr_min_close_atr = 0.5` | Did the 09:15 close move far enough beyond CPR, not just barely touch it? | Need at least `0.5 × ATR = ₹2` beyond `TC`; close at `₹102.20` passes, `₹100.50` fails. |
| `narrowing_filter = true` | Is today's CPR tighter than yesterday's CPR? | Today width `0.20%`, yesterday `0.35%` passes. |
| `buffer_pct = 0.05%` | Add a tiny cushion beyond CPR before accepting entry. | `TC = ₹100`, trigger is about `₹100.05`; close at `₹100.02` fails. |
| `min_effective_rr = 2.0` | Is the target at least twice as far as the stop risk? | Risk `₹4`, reward `₹8.50` passes; reward `₹6` fails. |
| `or_atr_min = 0.3` | Did the first candle show enough movement? | ATR `₹4`, first candle range must be at least `₹1.20`. |
| `or_atr_max = 2.5` | Was the first candle too large already? | ATR `₹4`, first candle range above `₹10` skips. |
| `max_gap_pct = 1.5` | Did the stock gap too much before we could enter? | Previous close `₹100`, open above `₹101.50` or below `₹98.50` skips. |
| `rvol_threshold = 1.0` | Is volume at least normal for this time of day? | Normal 09:25 volume is 10,000 shares; current volume must be at least 10,000. |

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

Plain example:

- Opening range high is `₹105`, low is `₹100`.
- Price first breaks above `₹105`, so it looks like a LONG breakout.
- A few candles later it falls back inside the opening range, for example to `₹103`.
- FBR treats that as a failed LONG breakout and looks for a SHORT reversal trade.
- If price never comes back inside the range within the allowed window, FBR skips it.

## Why both can show different edge

- `CPR_LEVELS` is a directional momentum test (break of a CPR boundary).
- `FBR` is a reversal-contingency test (momentum failed and turned).
- They are complementary. Running LONG-only vs SHORT-only can isolate when one side fails in specific market regimes.

Trader translation:

- CPR_LEVELS says: "The stock opened strong/weak around CPR; follow that direction."
- FBR says: "The first breakout failed; trade the reversal."
- A trend day can favor CPR_LEVELS. A trap/reversal day can favor FBR.

## Trailing Stop Mechanics

Both strategies use the same three-phase `TrailingStop` engine.  After entry a trade
advances through PROTECT → BREAKEVEN → TRAIL as price moves in your favour.

For candle-by-candle examples, same-bar edge cases, and the full exit matrix, see
[`trailing-stop-explained.md`](trailing-stop-explained.md). The summary below is
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
- TIME\_EXIT at 15:00 takes priority — any open position is force-closed regardless of phase.

Plain example:

- Buy at `₹100`, stop `₹95`; risk is `₹5`, so `1R = ₹5`.
- At `₹105`, the trade has reached `1R`; stop can move to breakeven near `₹100`.
- At `₹110`, the trade has reached `2R`; trailing can start.
- If the stock keeps rising, the stop follows upward. If it reverses, the trailing stop exits.

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

---

## Parameter Reference

Every parameter shown in the **Run Parameters** panel on the Run Results page is explained
below. Parameters are grouped the same way as the panel. Hover over any label in the
dashboard to see a one-line tooltip; this page provides the full explanation and examples.

### Strategy Config

**Strategy** · `CPR_LEVELS` or `FBR`
Which strategy the engine ran. CPR_LEVELS = daily pivot boundary touch entry.
FBR = failed opening breakout reversal.
Example: choose `CPR_LEVELS` when testing "follow the CPR break"; choose `FBR` when testing
"fade a failed opening breakout".

**Direction** · `LONG`, `SHORT`, or `BOTH`
Filters which side of the market the engine trades.
- `LONG`: only buy setups (price breaks above TC).
- `SHORT`: only sell setups (price breaks below BC).
- `BOTH`: takes whichever direction the 09:15 bar signals.
Example: if market conditions are bearish, run only `SHORT`; if bullish, run only `LONG`;
if neutral, run both as separate sessions.

**Execution Mode** · `BACKTEST`, `PAPER`, `LIVE`
How the run was executed. Affects commission model and position sizing source.
Example: `BACKTEST` is historical research, `PAPER` is live simulation, `LIVE` is real-time operation.

**Commission Model** · `zerodha` or `zero`
`zerodha` applies the actual Zerodha brokerage + STT + exchange charges.
At ₹100 stock with 100 shares (~₹10,000 position), commission ≈ ₹85 round-trip.
`zero` is used for pure strategy analysis without cost drag.
Example: use `zerodha` for realistic P/L; use `zero` only to understand raw strategy behavior.

---

### Entry Rules

**CPR Percentile** · default `33`
*What:* Only trade on days when a symbol's CPR width is in the bottom N-th percentile of
its own rolling 252-day distribution.
*Formula:* `cpr_width_pct < rolling_P{N}(symbol)` where `cpr_width_pct = |TC - BC| / pivot × 100`.
*Example:* FIEMIND on Apr 24 had `cpr_width_pct = 0.075%`. Its rolling P33 = 0.12%.
0.075 < 0.12 → passes. Narrow CPR = tighter S/R zone = cleaner breakout signal.
*Why it matters:* Wide CPR days (news, earnings) have wide noise bands that produce
false breakouts. This filter keeps the strategy on its best-behaved days.

**CPR Min Close ATR** · default `0.5`
*What:* The 09:15 bar's close must be at least `N × ATR` away from the CPR zone edge it
broke through. Prevents entries where price barely grazed the boundary.
*Formula (SHORT):* `(BC - or_close_5) / ATR ≥ 0.5`
*Example:* AIAENG on Apr 24 — BC = 4066, or_close = 4051.3, ATR = 23.5.
Distance = (4066 − 4051.3) / 23.5 = **0.63 ATR** → passes (≥ 0.5).
*If it fails:* `CPR_MIN_CLOSE_ATR` reject. Symbol not in setup list that day.

**Narrowing Filter** · `ON` or `OFF`
*What:* Additionally requires that today's CPR width is narrower than the previous day's
CPR width (the CPR is "contracting" into today). Extra confirmation that the range is
compressing before a potential breakout.
*Note:* In the current baselines this is `ON`. It reduces trade count ~10% but improves
quality slightly by avoiding days where CPR widened overnight.

**Buffer Pct** · default `0.05%`
*What:* Small price buffer added to the CPR boundary for the entry trigger.
*Formula (SHORT):* `trigger = BC × (1 − buffer_pct)`
*Example:* BC = 452.42. trigger = 452.42 × 0.9995 = **452.19**.
Fill = `max(trigger, next_bar_open)` to simulate a stop-limit order.
*Why:* Avoids entering on a candle that merely touched BC at the wick — requires the
full bar to close through the boundary.

**Failure Window** · default `8` (FBR only)
*What:* Number of bars after the initial breakout to look for a reversal that re-enters
the Opening Range. Only relevant when `Strategy = FBR`.
*Best value:* 10 bars (Calmar 4.08 vs 3.93 at 8). See ISSUES.md tuning results.
*Trader example:* If a breakout happens at 09:25 and `failure_window = 8`, FBR watches roughly
the next 40 minutes for the breakout to fail.

---

### Risk Management

**RR Ratio** · default `2`
*What:* The reward-to-risk multiple that determines the **target price**.
*Formula:* `target = entry − (SL_distance × rr_ratio)` (SHORT).
*Example:* Entry = 529.4, SL = 534.33, distance = 4.93. Target = 529.4 − (4.93 × 2) = **519.54**.
Actual S1 = 517.63 → target is set to S1 (whichever is further in your favour).
*Important:* This is NOT the entry gate — `min_effective_rr` is. `rr_ratio` just sets
where the target lands.

**Min Effective RR** · default `2.0`
*What:* The **entry gate**. A trade only opens if the actual reward/risk ratio at the
moment of entry is ≥ this value.
*Formula:* `effective_rr = (entry − target) / (entry − sl_price)` (SHORT — all positive).
*Example:* MAGADSUGAR Apr 24 — entry 529.4, SL 534.33, target 517.63.
Effective RR = (529.4 − 517.63) / (534.33 − 529.4) = 11.77 / 4.93 = **2.39** → opens (≥ 2.0).
*If it fails:* Trade is skipped even though the setup passed all other filters. Protects
against entries where R1/S1 is too close to the entry price (thin CPR day).

**Max SL ATR Ratio** · default `2.0`
*What:* The stop-loss distance (entry to SL) cannot exceed `N × ATR`. Prevents very wide
stops on volatile symbols.
*Formula:* `|entry − sl_price| / ATR ≤ max_sl_atr_ratio`
*Example:* ATR = 4.93. Max SL distance = 4.93 × 2.0 = 9.86. If the CPR zone would put
SL 12 points away → trade is skipped.

**Breakeven R** · default `1.0`
*What:* When a trade reaches `1R` profit (i.e., price moved 1× the original SL distance
in your favour), the stop is moved to the entry price (breakeven).
*Result:* Position can only lose commission from this point forward. Activates the
BREAKEVEN phase of the trailing stop.
*Example:* Entry `₹100`, stop `₹95`, risk `₹5`. Once price reaches `₹105`, breakeven logic can
move the stop near `₹100`.

**Risk-Based Sizing** · `ON` or `OFF`
*What:* When ON, position size is calculated to risk exactly `risk_pct` (default 1%) of
slot capital per trade, bounded by `max_position_pct` (20% in the canonical CPR presets).
*Formula:* `qty = floor(capital × risk_pct / sl_distance_in_₹)` capped by the slot allocation.
*Example:* Capital ₹200,000. Risk 1% = ₹2,000. SL = 4.93/share.
qty = floor(2,000 / 4.93) = **405** shares, then capped by the ₹2L slot if needed.
*When OFF (standard sizing):* qty = floor(portfolio × max_position_pct / entry_price) — fixed slot capital per trade regardless of SL distance.

**Max Positions** · default `5`
*What:* Maximum number of positions that can be open **concurrently** at any moment.
*This is NOT a daily trade cap.* Once a position closes (SL, target, breakeven), a new
one can open. On a typical day with many early BREAKEVEN exits, 20-35 total trades with
max 5 concurrent is possible.
*Live vs Backtest:* Competing entries now use the same quality-sort in both engines, with
symbol tie-breaks for determinism. Remaining live/backtest differences mostly come from the
input candles themselves, not the slot-allocation rule.

**Live Operator Controls** · paper/live only
Operators can reduce a running session's future-entry budget with `send-command --action set_risk_budget`.
They can also pause future entries with `pause_entries`, resume them with `resume_entries`, or clear
unprocessed admin intents with `cancel_pending_intents`. These are not strategy signals and do not
resize already-open positions. Existing positions keep their normal SL/target/trailing management;
new entries use the current operator budget/entry-gate state after the command is processed. If
current open notional already consumes a reduced budget, new entries are disabled until exposure falls.
Example: if market suddenly turns choppy, `pause_entries` stops new trades but keeps monitoring
open positions. `set_risk_budget` can reduce future entries without manually closing current trades.

---

### Filters

**Min Price** · default `₹50`
*What:* Skip any symbol whose previous-day close is below ₹50.
*Why:* Sub-₹50 stocks have wide percentage spreads and very large position sizes (10,000+
shares at ₹10), making commission costs disproportionate and execution slippage high.

**RVOL Threshold** · default `1.0` for LONG, `OFF` for SHORT
*What:* Relative Volume filter. For LONG only — requires that the current bar's volume is
at least `rvol_threshold × baseline_volume` before allowing entry.
*Baseline:* 10-day rolling average of volume at the same 5-minute bar of day.
*Why OFF for SHORT:* Adding RVOL to SHORT reduces trade count substantially (−46% at
RVOL=1.0) with net negative PnL impact. See strategy-guide RVOL sweep table.
*Live note:* Live RVOL uses tick-accumulated volume; backtest uses REST API volume.
Small differences exist but do not cause binary filter flips (unlike OR ATR).

**Max Gap Pct** · default `1.5%`
*What:* Skip if the overnight gap from previous close to today's 09:15 open exceeds 1.5%.
*Formula:* `|open_915 − prev_close| / prev_close × 100 > max_gap_pct → skip`
*Why:* Large gap stocks have already made their move before the opening bar. The CPR
boundary levels (computed from yesterday's OHLC) may be far from today's price, making
SL distances very wide or the target unreachable.

**OR ATR Min** · default `0.3`
*What:* Skip if the opening 5-minute bar (09:15-09:20) range is less than 0.3× ATR.
*Formula:* `or_atr_5 = (bar_high − bar_low) / ATR_prev_day`
*Example:* VISAKAIND Apr 24 — bar range = 65.46 − 65.38 = 0.08. ATR = 0.365.
or_atr = 0.08 / 0.365 = **0.22** → skip (< 0.3). No energy = no direction.
*Why:* A near-flat opening bar suggests the stock is illiquid or circuit-filtered.
Entering on a CPR signal with no momentum behind it produces false breakouts.

**OR ATR Max** · default `2.5`
*What:* Skip if the opening 5-minute bar range exceeds 2.5× ATR. The opposite of Min —
the stock already made its big move in the first bar and is likely to consolidate or reverse.
*Formula:* `or_atr_5 = (bar_high − bar_low) / ATR_prev_day`
*Example:* AGARIND Apr 24 — historical REST API bar: high=454.85, low=445.1.
or_atr = (454.85 − 445.1) / 1.633 = **5.97** → backtest skips. Live tick bar: high=453.5,
low=448.5. or_atr = 5.0 / 1.633 = **3.06** → backtest still skips, but live candle
looked different enough at entry time that it entered.

⚠️ **Live vs Backtest divergence (key parity issue):**
The Kite WebSocket runs in `MODE_QUOTE` (one tick per price change, not one per trade).
In the opening 5 minutes, 50 trades can occur in 200ms while Kite delivers 2-3 ticks.
The live tick-built 09:15 bar has a **30-90% smaller range** than the Kite REST API
historical bar for the same period. This causes many stocks to pass `or_atr_max=2.5` in
live (tick range narrow) but fail in backtest (REST range wide). On Apr 24 SHORT, this
explained 19 of 27 live-only symbols. Quality-sort removes the separate max-position
ordering mismatch, but it does not eliminate this feed-source gap. See ISSUES.md →
"2026-04-18 — Live WebSocket OR values differ from EOD parquet" → 2026-04-25 follow-up
for the full quantification.
**Calibration note:** If `or_atr_max` is raised to `5.0` in the backtest, the symbol set
would better match what live actually sees (since live's effective threshold ≈ 2.5 on
tick data ≈ 4.0-5.0 on REST data).

**Time Exit** · default `15:00`
*What:* All open positions are force-closed at this time regardless of phase.
*Why 15:00 not later:* Keeps MIS positions away from Zerodha's intraday auto-square-off window
and leaves time for operator intervention if an exit order is rejected.
*Example:* A trade opened at 10:00 is still open at 15:00. Even if target/SL did not hit, the
system exits it at the 15:00 candle close.

---

## Audit Feed — What It Is and When to Use It

`paper_feed_audit` is a DuckDB table in `paper.duckdb` that records every live OHLCV
bar seen by the live session as it was built from WebSocket ticks.

### What it stores

For every 5-minute bar that closed during a live session, it stores:

- `session_id`, `symbol`, `bar_start`, `bar_end`
- `open`, `high`, `low`, `close`, `volume` — as seen from **live ticks**
- `first_snapshot_ts`, `last_snapshot_ts` — when the bar was first and last updated

### Why it matters

The difference between `paper_feed_audit` and `intraday_day_pack`:

| Source | Data origin | OR range | Use case |
|--------|-------------|----------|----------|
| `intraday_day_pack` | Kite REST API (post-market) | Full trade-by-trade | Backtest |
| `paper_feed_audit` | Kite WebSocket ticks (live) | 30-90% narrower | Parity analysis |

The live engine uses `paper_feed_audit` data to build its bars. The backtest uses
`intraday_day_pack`. They are the same stock, same time window, different data sources.

Plain example:

- Historical REST data might say the 09:15 candle high-low range was `₹10`.
- Live WebSocket ticks might only see `₹6` of that movement.
- A filter like `or_atr_max` can therefore reject the historical backtest but allow the live
  session. The audit feed lets us replay the exact live bars later.

### Three uses

1. **Post-hoc parity debugging:** Compare live or_atr vs historical or_atr per symbol.
   The Apr 24 investigation (why did AGARIND enter live but not backtest?) was solved
   entirely by querying `paper_feed_audit`.

2. **Exact-feed backtest:** Pass `--pack-source paper_feed_audit --pack-source-session-id <id>`
   to `pivot-backtest`. The engine replaces `intraday_day_pack` candles with audit candles.
   This gives the closest possible backtest to live execution:

   ```bash
   doppler run -- uv run pivot-backtest \
     --all --universe-size 0 --yes-full-run \
     --start 2026-04-24 --end 2026-04-24 \
     --preset CPR_LEVELS_RISK_SHORT \
     --pack-source paper_feed_audit \
     --pack-source-session-id CPR_LEVELS_SHORT-2026-04-24-live-kite \
     --save
   ```

3. **Exact-feed replay:** `daily-replay` also supports `--pack-source paper_feed_audit`.
   Replay + exact-feed gives the same result as exact-feed backtest (both were verified
   equal after the Apr 22 sparse-tape terminal close fix).

### Expected overlap

| Comparison | Symbol overlap | Why not 100% |
|------------|---------------|--------------|
| Live vs historical backtest | ~16% | OR filter + different data source |
| Live vs exact-feed backtest | **~47-61%** | Orchestration ordering only |
| Replay vs exact-feed backtest | **~100%** | Same engine, same data |

The live vs exact-feed gap is reduced by the shared quality selector, but can still appear when
feed timing differs. Canonical CPR now uses `max_positions=5`; live/replay/backtest all rank
same-bar candidates through the shared quality selector before filling slots. PnL comparison at
the session level remains more reliable than exact trade matching for Kite live data.
