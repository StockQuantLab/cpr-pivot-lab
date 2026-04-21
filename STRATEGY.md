# STRATEGY.md — CPR Pivot Lab Strategy Specification

## Scope and Policy

CPR Pivot Lab production runs currently use two strategies:

- `CPR_LEVELS`
- `FBR`

`VIRGIN_CPR` is intentionally excluded from default campaigns and baseline dashboards. Keep it as a research branch only.

For a quick high-level read, use:

- [docs/strategy-guide.md](docs/strategy-guide.md) — side-by-side behavior (long/short) and execution flow
- Run-level inspection in dashboard (`/backtest`) for per-trade evidence (`Run Details → click trade row`)

## Documentation Map

Use these docs in this order when you need a clean mental model:

- [docs/strategy-guide.md](docs/strategy-guide.md) — operator-level overview of CPR_LEVELS vs FBR
- [docs/trailing-stop-explained.md](docs/trailing-stop-explained.md) — canonical exit lifecycle, same-bar edge cases, and scale-out behavior
- [docs/ISSUES.md](docs/ISSUES.md) — dated experiment log for accepted/rejected ideas and incident notes
- [docs/PAPER_TRADING_RUNBOOK.md](docs/PAPER_TRADING_RUNBOOK.md) — canonical presets and live/replay operating commands
- [docs/PROGRESSIVE_TRAIL_RATCHET_PLAN.md](docs/PROGRESSIVE_TRAIL_RATCHET_PLAN.md) — closed history of the rejected pre-target ratchet hypothesis
- [docs/LIVE_TRADING_PARITY_REWORK_PLAN.md](docs/LIVE_TRADING_PARITY_REWORK_PLAN.md) — parity work, not strategy tuning

## Data and Signal Source

- Data source: NSE 5-minute candles and daily bars in `data/parquet`
- CPR and pivots are computed from previous trading day:
  - `pivot = (H + L + C) / 3`
  - `BC = (H + L) / 2`
  - `TC = (pivot + BC) / 2`
  - `R1 = 2 * pivot - L`, `S1 = 2 * pivot - H`
- Intraday ATR uses previous-day 5-min candles (`atr_periods = 12`)
- Execution uses shared portfolio model (`portfolio_value`, `max_positions`, `max_position_pct`)
- `risk_based_sizing` is the default sizing mode for paper trading. Keep it off for backtest
  canonical baselines to avoid inflating historical comparisons.

### Sizing note

`risk_based_sizing` is meant to control how much money each trade can lose if the stop is hit.
That is different from "always lower drawdown".

Simple rule:

- tight stop -> larger position can fit inside the same risk budget
- wide stop -> smaller position is needed to stay inside the same risk budget

That can still increase max drawdown at the portfolio level if the strategy takes more trades,
uses larger size on many signals, or keeps several correlated positions open at once. It is
better described as "risk controlled per trade", not "guaranteed lower drawdown".

Example:

- account size = `₹1,00,000`
- risk budget = `1%` per trade = `₹1,000`
- stop distance = `₹5`
- max size = `₹1,000 / ₹5 = 200 shares`

If another trade has a `₹2` stop:

- max size = `₹1,000 / ₹2 = 500 shares`

So the risk budget stays the same, but the actual rupee exposure can get much larger on tight
stops. That is why the setting can improve profit while also raising drawdown.

Sizing formula:

- `risk budget = capital × risk_pct`
- `shares = risk budget / stop_distance`

Example:

- `capital = ₹1,00,000`
- `risk_pct = 1%`
- `risk budget = ₹1,000`
- `entry_price = ₹81.90`
- `stop_price = ₹81.305`
- `stop_distance = ₹0.595`
- `shares = ₹1,000 / ₹0.595 ≈ 1,679`

The share count is computed after the actual fill price is known. That prevents a gap-open entry
from being sized off a tighter trigger price than the trade actually paid.

Why realized loss can be higher:

- the stop can still be wider than the nominal risk budget once ATR guardrails and stop
  normalization are applied
- commissions and slippage are included in the final P&L
- the fill can happen after price has moved through the stop in a fast candle or gap

Concrete example from the risk-based long run:

- `NITCO` now sizes from the actual fill price and lands near `1,679` shares on the fresh rerun
- the historical `3,337`-share row came from the pre-fix sizing order and should be treated as
  stale data, not the intended model

So `risk_based_sizing` should be read as "target per-trade risk control", not "a hard maximum
realized loss per trade".

## Plain-English Parameter Glossary

Use this as the quick reference for what each common parameter means.

| Parameter | Plain English | Example |
|---|---|---|
| `strategy` | Which rule set to run. | `CPR_LEVELS` for CPR breakouts, `FBR` for failed breakout reversals. |
| `direction_filter` | Which side to trade. | `LONG` means buy-only; `SHORT` means sell-only; `BOTH` means take both sides. |
| `risk_based_sizing` | Size trades by stop-loss risk instead of the normal capped capital slot model. | `true` means a tight stop may get a larger position, `false` means use the standard portfolio overlay. |
| `portfolio_value` | Starting account value used for sizing and compounding. | `₹10,00,000` means the engine thinks it is managing a 10 lakh account. |
| `max_positions` | Maximum number of trades allowed at the same time. | `10` means up to ten open positions can exist together. |
| `max_position_pct` | Hard cap on how much of the account one trade can use. | `0.10` means one trade cannot use more than 10% of equity. |
| `rvol_threshold` | Minimum relative volume needed before a trade is allowed. | `1.0` means volume must be at least average; `1.2` means 20% above average. |
| `skip_rvol` | Turn the RVOL filter off completely. | `true` means ignore RVOL and take the setup without a volume check. |
| `cpr_percentile` | How strict the CPR narrowing filter should be. | `33` means only narrow CPR days qualify, based on the chosen percentile rule. |
| `cpr_min_close_atr` | How far price must close beyond CPR, measured in ATR. | `0.5` means the close must clear CPR by half an ATR before the trade is valid. |
| `narrowing_filter` | Whether to require CPR to be narrow enough before trading. | `true` means only narrow CPR days are eligible. |
| `min_price` | Skip cheap stocks below this price. | `50` means ignore stocks trading under 50 rupees. |
| `buffer_pct` | Small extra distance used to avoid taking borderline entries. | `0.0005` adds a tiny buffer above or below the CPR boundary. |
| `or_minutes` | Opening range length in minutes. | `5` means the OR window is the first 5 minutes of trade. Note: CPR_LEVELS presets use `15`; code default is `5`. |
| `or_atr_min` | Minimum opening-range size, measured in ATR. | `0.3` means the opening range must be at least 0.3 ATR wide. |
| `or_atr_max` | Maximum opening-range size, measured in ATR. | `2.5` means very huge opening ranges are rejected. |
| `time_exit` | Forced exit time if no stop or target was hit. | `15:15` means flatten any open trade near market close. |
| `failure_window` | FBR only: how many candles after breakout we watch for failure. | `10` means the setup must fail within 10 candles. |
| `rr_ratio` | FBR only: reward-to-risk target multiplier. | `2.0` means aim for 2x the stop distance. |
| `reversal_buffer_pct` | FBR only: small buffer used when entering the reversal. | `0.001` means 0.1% extra room around the failure level. |
| `fbr_min_or_atr` | FBR only: minimum opening-range size filter. | `0.5` means ignore tiny OR breakouts. |
| `fbr_failure_depth` | FBR only: how deep the breakout must fail before reversal. | `0.3` means the breakout must reverse enough to count as a real failure. |
| `cpr_scale_out_pct` | Optional scale-out split between first target and runner. | `0.0` means no scale-out (opt-in; tested at 0.8 but rejected as default). |

Rule of thumb:

- If the parameter changes *which trades qualify*, it is an entry filter.
- If the parameter changes *how much to buy or sell*, it is a sizing rule.
- If the parameter changes *when to exit*, it is an exit rule.

## Full Parameter Reference

Parameters below are code-level defaults from `BacktestParams` and `CPRParams` dataclasses. Presets and CLI flags may override these at runtime.

| Parameter | Default | Description |
|---|---|---|
| `cpr_max_width_pct` | `2.0` | Hard cap on CPR width; skip days wider than this. |
| `entry_window_end` | `"10:15"` | Stop looking for CPR entries after this time. |
| `min_sl_atr_ratio` | `0.5` | Minimum SL distance as a fraction of ATR. |
| `max_sl_atr_ratio` | `2.0` | Maximum SL distance as a fraction of ATR. |
| `breakeven_r` | `1.0` | R-multiple at which SL moves to entry price. |
| `atr_sl_buffer` | `0.0` | ATR multiplier added as noise buffer beyond the stop level. |
| `risk_pct` | `0.01` | Fraction of capital risked per trade (used with `risk_based_sizing`). |
| `compound_equity` | `False` | Carry forward equity across days. `True` restores old compounding behavior. |
| `rvol_lookback_days` | `10` | Lookback window for relative volume calculation. |
| `runtime_batch_size` | `512` | Symbols per day-pack fetch batch (memory vs latency tradeoff). |
| `max_gap_pct` | `1.5` | Skip symbols with opening gap larger than this percentage. |
| `commission_model` | `"zerodha"` | Brokerage cost model (`"zerodha"` or `"zero"`). |
| `slippage_bps` | `0.0` | Slippage in basis points per side. |
| `min_effective_rr` | `2.0` | CPR entry gate; reward/risk must meet this threshold before taking the trade. |
| `cpr_shift_filter` | `"ALL"` | Filter by CPR shift direction (`"ALL"`, `"UP"`, `"DOWN"`). |
| `cpr_entry_start` | `""` | Override CPR entry scan start time (empty string = derive from `or_minutes`). |
| `scale_out_pct` | `0.0` | Fraction of position to close at first target; remainder trails to R2/S2. |

## Strategy 1: CPR_LEVELS

### Thesis
On narrow CPR days, the normalized CPR band controls direction and R1/S1 becomes the anchor target.

### Rules

- `narrowing_filter` determines valid CPR days.
- Define the CPR band as `upper = max(TC, BC)` and `lower = min(TC, BC)`.
- Direction is inferred from `09:15` close vs the normalized band:
  - `close_915 > upper` ⇒ setup `LONG`
  - `close_915 < lower` ⇒ setup `SHORT`
- Entry is first valid 5-min candle (from `entry_scan_start`, default `09:20`) crossing the band with buffer:
  - LONG: close `>= upper + buffer`
  - SHORT: close `<= lower - buffer`
- SL is on the opposite CPR edge with ATR guardrails:
  - LONG stop below `lower`
  - SHORT stop above `upper`
- Target is R1 (for LONG) or S1 (for SHORT).
- Trade can be filtered by `cpr_min_close_atr` (minimum additional close-clearing distance).
- Breakeven and trailing phases are standard for all strategies.
- Exit at forced `time_exit` if no stop/target.
- Short-side `cpr_hold_confirm` was tested and rejected as a default:
  - It helped on the isolated `2026-03-27` / `2026-03-30` short slice.
  - On the full `2025-01-01 → 2026-03-30` short run it fell from `112.32%` return / `₹1,123,236.61` P&L / `PF 2.21` to `12.45%` / `₹124,508.36` / `PF 1.26`.
  - Keep it off by default until a narrower short-side filter proves better.
- Short-side `cpr_confirm_entry` was also tested and rejected as a default:
  - It cut trades from `2,823` to `468` and reduced return to `6.96%`.
  - It removed too many good short winners along with the bad early fakeouts.
- Raising `or_atr_min` to `0.5` for shorts was also tested and did not improve the full-period result:
  - `2,772` trades, `107.40%` return, `PF 2.20` versus the current `112.32%` / `PF 2.21`.
  - Keep the baseline `or_atr_min = 0.3` unless a narrower short-side filter proves better.
- Adding a short-only `open_to_cpr_atr` floor was also tested and rejected as a default:
  - `short_open_to_cpr_atr_min = 0.5` cut the full-period short run to `1,455` trades and `37.84%` return (`₹378,441.41`, `PF 2.05`).
  - It filtered too aggressively and hurt the strong baseline short campaign.
- Adding an 80/20 CPR scale-out from `R1/S1` to `R2/S2` was also tested and rejected as a default:
  - `cpr_scale_out_pct = 0.8` moved the long run from `43.44%` to `43.07%` and the short run from `128.08%` to `117.85%`.
  - Keep it opt-in only unless a narrower variant proves better.

### Recommended baseline params (production)

> **Note:** The values below include several that are set by named presets (`CPR_LEVELS_RISK_LONG`,
> `CPR_LEVELS_RISK_SHORT`, `CPR_LEVELS_STANDARD_LONG`, `CPR_LEVELS_STANDARD_SHORT`) and are **not**
> code defaults. Specifically: `min_price=50`, `cpr_min_close_atr=0.5`, and
> `use_narrowing_filter=True` are preset overrides. The code defaults are `min_price=0`,
> `cpr_min_close_atr=0.0`, and `use_narrowing_filter=False`.

- `cpr_percentile = 33.0`
- `or_atr_min = 0.3`
- `or_atr_max = 2.5`
- `or_minutes = 15`
- `buffer_pct = 0.0005`
- `cpr_min_close_atr = 0.5`
- `min_price = 50`
- `narrowing_filter = true`
- `time_exit = 15:15`
- `failure_window` is ignored by CPR_LEVELS

Direction-specific RVOL defaults:

- CPR_LEVELS LONG: `rvol_threshold = 1.0`
- CPR_LEVELS SHORT: `skip_rvol = true`
- FBR LONG: `skip_rvol = true`
- FBR SHORT: `skip_rvol = true`

### Long RVOL sweep (2025-01-01 → 2026-03-30)

The long CPR book was tested across four RVOL points before settling on `1.0` as the default.

| Run ID | RVOL | Trades | Total Return | P/L | Win Rate | PF | Max DD |
|---|---:|---:|---:|---:|---:|---:|---:|
| `deb2b5638274` | `1.0` | `1,092` | `50.50%` | `₹504,950.88` | `39.4%` | `3.27` | `0.7178%` |
| `590337d33a19` | `1.1` | `1,005` | `46.91%` | `₹469,075.34` | `40.0%` | `3.31` | `0.6623%` |
| `b80ca74b2f34` | `1.2` | `911` | `43.44%` | `₹434,407.98` | `40.5%` | `3.38` | `0.6028%` |
| `fe252837dc83` | `1.3` | `831` | `40.19%` | `₹401,886.48` | `40.7%` | `3.47` | `0.5971%` |

Interpretation:

- `1.0` delivered the highest absolute P/L and total return.
- `1.1` was the best middle ground if you want a slightly cleaner book.
- `1.3` had the best PF / drawdown profile but gave up too much profit.
- The promoted long default is `rvol_threshold = 1.0`.

## Strategy 2: FBR (Failed Breakout Reversal)

### Thesis
ORB breakouts that fail quickly often reverse; capture the reversal leg after defined failure conditions.

### Rules

- Observe breakout over OR window.
- Setup direction is from `strategy_day_state.direction_5`:
  - LONG breakout is above OR High
  - SHORT breakout is below OR Low
- `breakout_idx` finds first breakout candle in the OR window.
- Look for failure within `failure_window` candles:
  - LONG breakout failure if close falls below `OR_High - depth` (opens reversal SHORT)
  - SHORT breakout failure if close rises above `OR_Low + depth` (opens reversal LONG)
- Reversal entry is taken from the first failure candle close.
- SL = beyond failed breakout extreme with ATR + reversal buffer.
- Target = `rr_ratio × SL distance` from entry (then trailing).
- Exit on SL/TP and `time_exit`.
- Same shared portfolio and trailing framework as CPR_LEVELS.

### Long vs Short at a glance

| Strategy | Long setup | Short setup |
|---|---|---|
| CPR_LEVELS | 09:15 close above the upper CPR boundary, then re-cross above upper+buffer and hold/confirm | 09:15 close below the lower CPR boundary, then re-cross below lower-buffer and hold/confirm |
| FBR | Breakout setup is LONG, then failed breakout turns SHORT | Breakout setup is SHORT, then failed breakout turns LONG |

### Recommended baseline params (production)

- `failure_window = 10`
- `rr_ratio = 2.0`
- `reversal_buffer_pct = 0.001`
- `fbr_min_or_atr = 0.5`
- `fbr_failure_depth = 0.3`
- `skip_rvol = true` for campaign baselines
- `risk_based_sizing = false` for backtest campaign baselines
- `risk_based_sizing = true` for daily paper trading
- `time_exit = 15:15`

## Execution Semantics

- Backtests are run in cache-aware mode by `run_id`.
- Two symbol-level decisions are supported:
  - shared portfolio (default)
  - `direction_filter = LONG` or `SHORT` for long/short-only diagnostics

## Exit Reasons

- `INITIAL_SL`
- `BREAKEVEN_SL`
- `TRAILING_SL`
- `TARGET`
- `TIME`
- `CANDLE_EXIT`

## Dashboard Explanation Surfaces

- **Run-wide summary:** Dashboard `/backtest` shows strategy + direction filter (BOTH/LONG/SHORT), KPI cards, and KPIs.
- **Per-trade decision path:** Click any row in `Trade List`, `Top Winners`, or `Top Losers` to open a plain-English inspection.
- **How to verify in TradingView:** the trade inspector includes a checklist for previous-day CPR and 09:15 / entry / exit candles.
