# Paper Trading Runbook

## Daily Live Trading — Canonical Startup Command

Preferred supervised launch for live-paper days:

```bash
doppler run -- uv run pivot-paper-supervisor -- \
  --multi --strategy CPR_LEVELS --trade-date today
```

This starts `pivot-paper-trading daily-live` as a child process and writes:
- heartbeat JSONL: `.tmp_logs/supervisor/live_YYYYMMDD_HHMMSS.heartbeat.jsonl`
- child stdout: `.tmp_logs/supervisor/live_YYYYMMDD_HHMMSS.stdout.log`
- child stderr: `.tmp_logs/supervisor/live_YYYYMMDD_HHMMSS.stderr.log`

Use the direct command below only when you intentionally do not need process-level exit
diagnostics:

```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS --trade-date today \
  >> .tmp_logs/live_YYYYMMDD.log 2>&1
```

**Rules — read before every session:**
- `--multi` is the ONLY safe way to run LONG + SHORT concurrently on Windows. DuckDB
  exclusive locking means two separate `daily-live` processes will always fail on the second.
- `--multi` uses `PAPER_STANDARD_MATRIX` → `CPR_CANONICAL_PARAMS` (= `CPR_LEVELS_RISK_LONG`
  overrides). Risk-based sizing is already baked in. Do NOT add `--preset` with `--multi`.
- `pivot-paper-supervisor` sets `PYTHONUNBUFFERED=1` and `PYTHONFAULTHANDLER=1` for the child.
  If the child exits silently, inspect the heartbeat JSONL first; it records PID, return code,
  elapsed time, log sizes, log tails, and Windows memory counters when available.
- `PYTHONUNBUFFERED=1` is required for real-time log visibility when using the direct command.
- Pre-market (8:30–9:10 AM): `daily-prepare --trade-date today --all-symbols` must pass
  before live starts. If EOD was not completed, run `pivot-refresh --since <prev_trading_date>
  --prepare-paper --trade-date today` and wait for it to complete.
- `pivot-data-quality --date today` must print `Ready YES` before live starts. In pre-market
  mode this checks today's saved universe against the previous completed trading day's
  `v_daily`, `v_5min`, `atr_intraday`, and `cpr_thresholds` data; it does not require today's
  intraday candles or future-dated `market_day_state` rows.
- After market open, readiness/status checks must be read-only: use only
  `doppler run -- uv run pivot-data-quality --date today`, `pivot-paper-trading status`,
  dashboard Live Readiness, and log monitoring. Do not run `daily-prepare`, `pivot-build`,
  `pivot-refresh`, or EOD/Kite ingest after market open unless the operator explicitly approves
  a recovery action. `daily-prepare --status` is not a valid status command and should not be
  used by agents.
- Start `daily-live --feed-source kite` at/after 09:16 IST. The CLI now fails fast before
  09:16 unless `--wait-for-open` is explicitly supplied; do not use the wait mode for normal ops.
- CPR exits open positions at 15:00 IST by default. Zerodha publishes intraday auto square-off
  timings at/after 15:25 for equity/cash and says timings may change with volatility; exiting at
  15:00 keeps CPR out of the broker RMS auto-square-off window.
- Do not run backtests while live is running — `market.duckdb` write lock will block startup.
- Reproducibility: `daily-prepare --all-symbols` uses the stable `canonical_full` universe and
  saves that same list as `full_YYYY_MM_DD` in `backtest_universe` inside the canonical
  `market.duckdb`.
  You can still override the name with `--snapshot-universe-name full_YYYY_MM_DD` if needed,
  and then reuse that exact list with `--universe-name full_YYYY_MM_DD` in live / replay /
  daily-sim / baseline commands.
- `daily-live`, `daily-replay`, and `daily-sim` now default to the dated saved universe when
  `--symbols`, `--all-symbols`, and `--universe-name` are all omitted, falling back to
  `canonical_full` only if the dated snapshot is missing.
- Snapshot rows live in DuckDB, not as files on disk. If you want to trim old ad hoc
  snapshots, use `pivot-paper-trading universes --prune-before YYYY-MM-DD --apply`. Do not
  prune the dated archive if you still need it for audit comparisons.
- Inspect saved snapshots with `pivot-paper-trading universes` or
  `pivot-paper-trading universes --name full_YYYY_MM_DD`.

## Today's Live-Paper Dry-Run Checklist

Use this compact checklist on live-paper days when validating real-trading safety behaviour.
The commands below are paper/dry-run only; real Zerodha order placement remains disabled unless
you explicitly launch `daily-live --real-orders` and all Doppler real-order gates are enabled.

**Pre-market gate:**
```bash
doppler run -- uv run pivot-eod-status --date <previous_trading_date> --trade-date today
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols
doppler run -- uv run pivot-data-quality --date today
doppler run -- uv run python scripts/test_kite_websocket.py
doppler run -- uv run pivot-paper-trading status
doppler run -- uv run pivot-paper-trading universes
uv run pivot-lock-status
```

Required result:
- `daily-prepare` succeeds and saves/uses today's `full_YYYY_MM_DD` universe.
- `pivot-data-quality` prints `Ready YES` for previous completed-day live prerequisites.
- Kite REST and WebSocket both print `OK`.
- `status` shows no unexpected active sessions before launch.
- `universes` includes today's `full_YYYY_MM_DD` snapshot.
- `pivot-lock-status` shows no live writer PID before startup.

**After market open status-only commands:**
```bash
doppler run -- uv run pivot-data-quality --date today
doppler run -- uv run pivot-paper-trading status
uv run pivot-lock-status --json
```

Do not run `daily-prepare`, `pivot-refresh`, `pivot-build`, or ingestion commands after market
open unless this is an explicit operator-approved recovery. Those commands are pre-market/EOD
operations, not live status checks. `daily-prepare --status` is not a supported command.

**Historical replay validation bundle:**
```bash
doppler run -- uv run pivot-paper-validate --trade-date <YYYY-MM-DD>
```

This runs paper cleanup, daily-prepare, canonical CPR LONG+SHORT `daily-replay --no-alerts`,
feed audit, and writes `.tmp_logs/paper_validate_<date>.json`.

**Pre-market broker dry-run checks:**
```bash
doppler run -- uv run pivot-paper-trading pilot-check \
  --symbols SBIN \
  --order-quantity 1 \
  --estimated-notional 5000 \
  --acknowledgement I_ACCEPT_REAL_ORDER_RISK \
  --strict

doppler run -- uv run pivot-paper-trading real-dry-run-order \
  --session-id premarket-dryrun-YYYY-MM-DD \
  --symbol SBIN \
  --side BUY \
  --quantity 1 \
  --role premarket_probe \
  --order-type LIMIT \
  --price 800 \
  --event-time YYYY-MM-DDT06:25:00+05:30
```

Expected result:
- `pilot-check` returns `ok=true` and `real_orders_enabled=false`.
- `real-dry-run-order` prints a Zerodha payload with `mode=REAL_DRY_RUN`.
- Do not run `broker-reconcile` against this standalone premarket dry-run id unless you also
  created a matching paper session row. Without a session row, `SESSION_MISSING` is expected.

**Start live paper:**
```bash
doppler run -- uv run pivot-paper-supervisor -- \
  --multi --strategy CPR_LEVELS --trade-date today \
  --simulate-real-orders
```

Use `--simulate-real-orders` during the paper-to-live transition window. It keeps the session
paper-only, but routes every paper entry/exit intent through the Zerodha `REAL_DRY_RUN` order
adapter so broker-intent payloads and order latency are recorded without calling Kite
`place_order`.

**In-session safety drills after LONG/SHORT session IDs exist:**
```bash
doppler run -- uv run pivot-paper-trading reconcile --session-id <LONG_SESSION_ID> --strict
doppler run -- uv run pivot-paper-trading reconcile --session-id <SHORT_SESSION_ID> --strict

doppler run -- uv run pivot-paper-trading send-command \
  --session-id <SESSION_ID> --action pause_entries --reason dry_run_drill
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <SESSION_ID> --action resume_entries --reason dry_run_drill

doppler run -- uv run pivot-paper-trading send-command \
  --session-id <SESSION_ID> --action set_risk_budget \
  --portfolio-value 500000 --max-positions 5 --reason dry_run_reduce_risk
```

If an open position exists and you intentionally want to test manual exit parity:
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <SESSION_ID> --action close_positions \
  --symbols <SYMBOL> --reason dry_run_close_one
```

For emergency/market-regime drills only:
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id <SESSION_ID> --action close_all --reason dry_run_flatten_one

doppler run -- uv run pivot-paper-trading flatten-both \
  --trade-date today --reason dry_run_flatten_both
```

Operational notes:
- `close_positions`, `close_all`, and `flatten-both` close using the latest live mark/LTP when
  available; they do not wait for the next 5-minute strategy candle.
- `set_risk_budget` changes future entries only. Existing open positions keep normal exits unless
  you also send `close_positions` or `close_all`.
- Dashboard `/paper_ledger` exposes the same controls and refreshes active paper state every 3s.

### Universe policy (`canonical_full` → `full_YYYY_MM_DD`)

`canonical_full` is the stable full-universe source of truth. Daily snapshots must not shrink just
because a few symbols are suspended, delisted, or missing data on one date. `daily-prepare
--all-symbols` creates `canonical_full` once if missing, then copies it to the dated
`full_YYYY_MM_DD` snapshot for that trade date. Date-specific readiness/pre-filtering skips symbols
that lack required rows for that day; broad gaps still fail closed.

Rerun guard: if `full_YYYY_MM_DD` already exists and differs from `canonical_full`,
`daily-prepare --all-symbols` refuses to overwrite it. Repair requires an explicit operator action:
`--refresh-universe-snapshot`. Normal repeated `daily-prepare` runs are idempotent when the dated
snapshot already matches canonical.

**The date in the name is the TRADE date — the day you will trade on, not the day the data came from.**

| What ran | Universe saved | Use it for |
|----------|---------------|-----------|
| Pre-market `daily-prepare --trade-date 2026-04-27` (ran this morning) | `full_2026_04_27` | Live trading today; backtest parity for 2026-04-27 |
| EOD `daily-prepare --trade-date 2026-04-28` (ran tonight) | `full_2026_04_28` | Live trading tomorrow; backtest parity for 2026-04-28 |

**Why this naming, not the data-date?**
The date you'd put in `--start`/`--end` for a parity backtest is always the trade date, and the
universe name must match it. Using the data-date (prev day) would force you to subtract one day
every time you build a backtest command — a constant source of off-by-one errors.

**Practical rules:**

1. **Normal live trading tomorrow** — omit the universe flag entirely:
   ```bash
   doppler run -- uv run pivot-paper-trading daily-live --multi --strategy CPR_LEVELS --trade-date 2026-04-28
   ```
   Auto-resolves to `full_2026_04_28`.

2. **Emergency mid-session relaunch (same day)** — pass today's trade date explicitly so the
   relaunch uses the identical symbol list as the original launch:
   ```bash
   doppler run -- uv run pivot-paper-trading daily-live --multi --strategy CPR_LEVELS \
     --trade-date 2026-04-27 --universe-name full_2026_04_27
   ```

3. **Backtest parity check for today's paper session** — use today's universe, not tomorrow's:
   ```bash
   doppler run -- uv run pivot-backtest \
     --universe-name full_2026_04_27 \
     --start 2026-04-27 --end 2026-04-27 \
     --preset CPR_LEVELS_RISK_SHORT --save
   ```
   The date in `--universe-name` always matches `--start`/`--end`.

4. **Never use `--all-symbols` for live** — use the dated saved snapshot or default resolution.

---

## Operating Model

Run **2 primary paper sessions every trading day** (CPR_LEVELS LONG and SHORT).
FBR execution is disabled in paper workflows to keep CPR parity strict.

The goal is observation: accumulate per-session performance over time and find which
variant works in which market regime (uptrend vs downtrend).

### Parity Contract

`StrategyConfig` is the canonical config object used by backtest, replay, and live.
Backtest, replay, and live must all consume the same explicit strategy params and
execution rules; none of them is exempt from parity.

- Daily-reset CPR_LEVELS is the current frozen comparison set.
- Compound-risk CPR_LEVELS is still under row-by-row review against the preserved old
  baselines, so do not treat its temporary reruns as the reference set.
- When comparing runs in the dashboard, use the timestamp portion of the label to
  distinguish old vs new rows. The label renders `updated_at` when available and falls
  back to the run creation time when `updated_at` is missing.

- `daily-sim` runs the backtest engine directly.
- `daily-replay` and `daily-live` use the same shared `StrategyConfig` defaults as backtest.
- Paper does **not** inject extra paper-only strategy defaults.
- If you want a paper run to match a specific backtest run, pass the same explicit flags or JSON overrides.
- Default rule: every canonical backtest, replay, live, or paper run must start from a named preset.
  Use hand-spelled flags only for ad hoc analysis, hypothesis testing, or deliberate override validation.
- Prefer named presets such as `CPR_LEVELS_RISK_LONG` and `CPR_LEVELS_RISK_SHORT` instead of hand-spelling the full flag bundle.
- Live finalization is defensive: final flush is guarded, abnormal exits auto-flatten, and a
  `SESSION_ERROR` alert is emitted if terminal cleanup fails. A clean shutdown still requires
  the DB terminal status stamp to land successfully.
- Manual/operator exits are execution controls, not strategy signals. In live paper, `close_positions`,
  `close_all`, sentinel flatten, and final abnormal-exit flatten close immediately using the latest
  in-memory LTP/tick mark when available; they do not wait for the next 5-minute strategy candle.
  Strategy SL/target/trailing/time exits remain completed-5-minute-candle driven.
- Paper order events now pass through a process-local execution safety layer: default 8 orders/sec
  governor plus order idempotency keys. This is below the 10 orders/sec regulatory threshold and is
  deliberately conservative for future real-broker mode.
- Real-broker dry-run is available for payload validation only. `real-dry-run-order` builds the
  Zerodha order payload, passes through the same governor/idempotency layer, records the payload in
  `paper_orders.broker_payload`, and never calls Kite `place_order`.
- Live paper reconciles order/position/session invariants after each bar and after admin close
  commands. If a critical mismatch is detected, new entries are disabled, current open positions
  continue to be monitored for exits, and the session note is stamped with
  `ENTRY_DISABLED_RECONCILIATION`.
- When multiple symbols qualify on the same bar, the current shared selector uses a
  deterministic symbol-order tie-break via `select_entries_for_bar()`. That keeps replay,
  live, and backtest reproducible. It is not a profitability optimizer. If you want to rank
  same-bar candidates by a score such as effective RR, that is a strategy change and must be
  baseline-rerun like any other rule change.
- `profit_loss_pct` is the trade's net return on entry notional (`position_value`), not the
  percentage of account equity. A small-share trade can therefore show a large percentage loss
  even when the rupee loss is modest.
- Canonical CPR replay/live command (no extra CPR flags unless you are intentionally overriding):
  `doppler run -- uv run pivot-paper-trading daily-replay --multi --strategy CPR_LEVELS --trade-date 2026-04-02 --no-alerts`
- For CPR_LEVELS, a trade only opens when the effective reward/risk at entry meets `min_effective_rr` (default `2.0`).
  `rr_ratio` is the target multiple used by the trade model; it is not the entry gate.

### Canonical 4 CPR Baselines (Backtest)

Use these 4 presets as the canonical CPR baseline matrix. Keep every parameter fixed and only
change the date window when extending baselines.

| Variant | Preset | Legacy label meaning |
|---------|--------|----------------------|
| CPR LONG standard | `CPR_LEVELS_STANDARD_LONG` | `cpr-levels-long-rvol1-atr0.5` |
| CPR SHORT standard | `CPR_LEVELS_STANDARD_SHORT` | `cpr-levels-short-rvoloff-atr0.5` |
| CPR LONG risk sizing | `CPR_LEVELS_RISK_LONG` | `cpr-levels-long-risksize-rvol1-atr0.5` |
| CPR SHORT risk sizing | `CPR_LEVELS_RISK_SHORT` | `cpr-levels-short-risksize-rvoloff-atr0.5` |

Canonical rerun commands (only change `--start`/`--end`):

```bash
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-24 --preset CPR_LEVELS_STANDARD_LONG  --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-24 --preset CPR_LEVELS_STANDARD_SHORT --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-24 --preset CPR_LEVELS_RISK_LONG      --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-24 --preset CPR_LEVELS_RISK_SHORT     --save --quiet
```

Reference sets (Apr 2026):

- **Pre-fix baselines (v1 — kept for history, do not use as new comparison target):**

| Mode | Run ID | Start → End | P/L |
|------|--------|-------------|-----|
| Compound Risk SHORT | `06ea445a0108` | 2025-01-01 → 2026-04-09 | ₹2,480,614 |
| Compound Risk LONG | `7e3feac35fd6` | 2025-01-01 → 2026-04-09 | ₹1,597,668 |
| Compound Std SHORT | `fdea70184c2c` | 2025-01-01 → 2026-04-09 | ₹2,470,887 |
| Compound Std LONG | `7d3b85732419` | 2025-01-01 → 2026-04-09 | ₹1,598,267 |
| Daily Reset Risk SHORT | `4a2cc2485a6d` | 2025-01-01 → 2026-04-09 | ₹1,035,723 |
| Daily Reset Risk LONG | `f1644ae9ce0e` | 2025-01-01 → 2026-04-09 | ₹818,525 |
| Daily Reset Std SHORT | `1d6e5e93618e` | 2025-01-01 → 2026-04-09 | ₹1,041,450 |
| Daily Reset Std LONG | `84a85d954f99` | 2025-01-01 → 2026-04-09 | ₹827,381 |

**Current CPR baselines (2026-05-04 `full_2026_04_30` / 2038-symbol all-8 rerun through 2026-05-04):**

SHORT presets now use `short_trail_atr_multiplier = 1.25`. LONG keeps `trail_atr_multiplier = 1.0`.
Canonical sizing is now `max_positions=5`, `capital=200000`, `max_position_pct=0.2`.
Daily-reset risk is the live-paper sizing reference.
The May 4 extension is now the canonical comparison target for all eight CPR baselines:

| Mode | Preset | Run ID | Start → End | P/L | Calmar |
|------|--------|--------|-------------|-----|--------|
| Daily Reset | `CPR_LEVELS_STANDARD_LONG` | `1089ce2684e3` | 2025-01-01 → 2026-05-04 | ₹1,728,353 | 207 |
| Daily Reset | `CPR_LEVELS_STANDARD_SHORT` | `f9eacc07c317` | 2025-01-01 → 2026-05-04 | ₹1,602,288 | 88 |
| Daily Reset | `CPR_LEVELS_RISK_LONG` | `785a0ae8bc76` | 2025-01-01 → 2026-05-04 | ₹1,721,793 | 209 |
| Daily Reset | `CPR_LEVELS_RISK_SHORT` | `49488023a79d` | 2025-01-01 → 2026-05-04 | ₹1,619,565 | 94 |
| Compound | `CPR_LEVELS_STANDARD_LONG` | `eb3c979cbae2` | 2025-01-01 → 2026-05-04 | ₹5,579,446 | 402 |
| Compound | `CPR_LEVELS_STANDARD_SHORT` | `5e5d105ee842` | 2025-01-01 → 2026-05-04 | ₹4,926,210 | 172 |
| Compound | `CPR_LEVELS_RISK_LONG` | `521c0fad74af` | 2025-01-01 → 2026-05-04 | ₹1,729,140 | 207 |
| Compound | `CPR_LEVELS_RISK_SHORT` | `eeae3af65dd1` | 2025-01-01 → 2026-05-04 | ₹1,632,929 | 85 |

The 2026-05-04 promotion supersedes the prior 2026-04-30-ended comparison set, the
2026-05-03 comparison set, and the older deleted 2026-04-28 `u2029` baseline rows. The
retired rows should not be used as comparison targets. The only added trading date versus the
previous current set is 2026-05-04; 2026-05-01 was a market holiday and 2026-05-02/03 were
weekend days.
Use `full_2026_04_30` explicitly for reproducible reruns; it matched `canonical_full` at promotion
time but the dated name is the durable reference.

When extending the v2 set to a later end date, rerun these same eight presets and
compare the overlapping window only. The incremental window should be the only source
of P/L delta unless the operator explicitly accepts a universe/runtime-surface migration. Do not
infer the reference set from label prefixes alone.

Dashboard note:
- Home page `Recent Backtest Runs` now has a one-click copy icon for `run_id`.
- Run Detail (`/backtest`) header now has `run_id` + copy button.

### CPR Parity Learnings

For daily CPR compares, treat strategy filters and session state as separate checks:

- `daily-replay` starts a fresh paper session for the requested date.
- Matching params are necessary, but they do not by themselves guarantee identical executed rows across a one-day replay and a multi-day backtest window.
- Historical trigger case: the 2026-04-01 CPR compare on `GANESHCP` and `ASTERDM` drove the parity rework.
- The current backtest, replay, and live/local-live CPR paths share the same entry search. If that compare ever reappears, treat it as a regression and diff the shared input rows first.
- Capital / sizing may change quantity and P&L, but a trade appearing or disappearing is a separate parity problem.
- Replay/live should keep the candle-by-candle operational behavior, and backtest should remain on the same shared entry evaluation so all paths agree.

Operational controls remain separate from strategy params:

- `flatten_time`
- `max_daily_loss_pct`
- `max_positions`
- `max_position_pct`
- alert dispatch
- live quote adapter and feed state

### Primary sessions (daily default)

| Session | Strategy | Direction | RVOL | Sizing | Trade type |
|---------|----------|-----------|------|--------|------------|
| CPR_LEVELS LONG | CPR_LEVELS | LONG | ON (1.0) | Same as backtest | Buy at TC touch, target R1 |
| CPR_LEVELS SHORT | CPR_LEVELS | SHORT | OFF | Same as backtest | Sell at BC touch, target S1 |

**RVOL note**: RVOL 1.0 is the shared backtest default for CPR LONG. CPR SHORT uses
`skip_rvol_check=True` in the canonical preset bundle.

**RVOL display note**: when a preset has `skip_rvol_check=True`, the stored numeric `rvol_threshold`
is ignored at runtime. The dashboard may still display the saved threshold value, but it is not an
active filter for that run.

**Sizing note**: sizing is a strategy override, not a paper-only default. Paper and backtest both
default to `risk_based_sizing=False` unless you explicitly pass `--risk-based-sizing` or use a
named risk preset such as `CPR_LEVELS_RISK_LONG`.

**Risk-sizing cap note**: when `risk_based_sizing=True`, the tracker still respects
`max_position_pct` and remaining cash. Risk sizing changes the share count, but it does not
remove the per-position capital cap.

**Compound note**: when `compound_equity=True`, the sizing base follows the current session
equity, not the fixed starting capital. That keeps compound backtest, replay, and live aligned
on the same evolving capital base.

**Dust-floor note**: the engine skips tiny allocations that would otherwise open a 1-share or
similar dust position. The shared minimum trade notional is 5% of the per-position slot capital
with a hard floor of Rs.1,000.

**Position-limit note**: canonical CPR presets now use `max_positions=5`,
`portfolio_value=₹10L`, and `max_position_pct=0.20`, so each slot can allocate up to ₹2L. This is
a cap on concurrent open positions, not a cap on total trades in a day. A session can take more
than 5 trades if earlier positions close and later signals open new ones.

**Filter note**: paper replay/live do not add a paper-only narrowing filter. If the matching
backtest run used `--narrowing-filter`, pass it here too.

Paper-only cleanup before a fresh rerun:

```bash
# Clear paper sessions, orders, feed state, alerts, and PAPER analytics rows only.
# This does NOT delete baseline BACKTEST runs.
# Do not delete backtest baseline rows without explicit confirmation when a compare
# is still in progress.
doppler run -- uv run pivot-paper-trading cleanup --trade-date YYYY-MM-DD --apply
```

Use this when you want a clean paper rerun from storage but need to preserve backtest baselines.
If you only need to archive one finished session, use `stop --complete` instead of a full cleanup.

The same RVOL policy is used in backtest, `daily-sim`, and `daily-live` unless a flag overrides it:

- CPR_LEVELS LONG uses RVOL 1.0
- CPR_LEVELS SHORT skips RVOL

If you need a clean rerun from storage, use `pivot-reset-history --apply` from the host shell.
That wipes DuckDB `backtest_results`, `run_daily_pnl`, `run_metrics`, `run_metadata`,
`setup_funnel` (in `backtest.duckdb`), plus `paper_sessions`, `paper_positions`,
`paper_orders`, `paper_feed_state`, and `alert_log` (in `paper.duckdb`).
It is the supported table cleanup path for a full rerun.

**Walk-forward fold testing is not used.** Paper trading is the validation.
Sessions accumulate their own performance history; compare them over time to find
market-regime correlations.

---

## Two Modes: Live vs Simulation

| Mode | Command | When to use | Speed |
|------|---------|-------------|-------|
| **daily-live** | `pivot-paper-trading daily-live` | Current trading day, real Kite quotes | Real-time |
| **daily-sim** | `pivot-paper-trading daily-sim` | Any historical date, full universe | ~25s per variant |

`daily-sim` uses the backtest engine directly and stores results as `execution_mode='PAPER'`
in DuckDB. It executes the same strategy logic as backtest, but writes paper results and skips
alert dispatch.
Use `daily-sim` for historical backfill and validation; `daily-live` for today's market.
`daily-replay` still exists, but it is now an advanced parity/debugging path rather than the
default operator command for historical checks.

Replay logging is intentionally light:
- candle-progress logs show the current 5-minute time being processed, but only for the first
  symbol in each replayed date so the log does not explode
- trade lifecycle logs are emitted only when a trade opens, partially exits, or closes, and
  they include the candle time the event occurred on
- replay is bar-major with concurrent CPR LONG/SHORT sessions; trade logs are interleaved by event timing

## Single Writer Policy

DuckDB is still single-writer. The CLI commands below now take the shared `runtime-writer`
lock before they mutate DuckDB state. If another writer is active, they fail fast instead of
leaving a stale `.writelock` file behind:

- `pivot-backtest`
- `pivot-build`
- `pivot-paper-trading daily-sim`
- `pivot-paper-trading daily-replay`
- `pivot-paper-trading replay`
- `pivot-data-quality --refresh`
- `pivot-reset-history`
- `pivot-campaign` runtime coverage auto-fix

The lock is command-level only. Read-only commands still run without it.

---

## Daily Sequence

### 1. Ingest fresh market data (EOD or pre-market)

Use the guarded single-command EOD path by default. It prevents skipped instrument refreshes and
out-of-order build/prepare steps:

```bash
doppler run -- uv run pivot-refresh \
  --eod-ingest \
  --date <today> \
  --trade-date <next_trading_date>
```

For example, after close on 2026-04-29:

```bash
doppler run -- uv run pivot-refresh \
  --eod-ingest \
  --date 2026-04-29 \
  --trade-date 2026-04-30
```

The command runs: refresh instruments → daily ingest → 5-min ingest → runtime build →
daily-prepare → final `pivot-data-quality --date <next_trading_date>`. The final gate must show
`Ready YES`. Do not hand-run the individual steps unless debugging a failed stage.
Ingestion stages are rerun-safe by default: the wrapper passes `--skip-existing` to daily and
5-minute Kite ingestion, so existing parquet is logged as skipped. Add `--force-ingest` only for
an intentional refetch.

### 1b. Pre-market setup and readiness check (8:30–9:10 AM)

**CRITICAL**: Do these steps **before** starting `daily-live` for best parity and stability.
`daily-live` does not auto-build runtime tables.

```bash
# Step 1 — Complete the previous trading day's EOD pipeline if EOD did not finish.
doppler run -- uv run pivot-refresh --since <prev_trading_date> --prepare-paper --trade-date today

# Step 2 — Verify previous-day live prerequisites (must pass before starting live)
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols

# Step 3 — Scan for today's narrow-CPR candidates (informational — 8:45–9:10 AM)
doppler run -- uv run pivot-signal-alert --universe gold_51 --condition narrow-cpr
# Or full universe:
doppler run -- uv run pivot-signal-alert --all-symbols --condition narrow-cpr
```

**Live setup contract (fixed Apr 2026)**: do not build future-date setup rows before market open.
Live uses the latest completed trading day's CPR/ATR inputs and resolves today's direction from
the live 9:15 opening-range candle. `--allow-late-start-fallback` is no longer required for normal
startup.
If `market_day_state` or `strategy_day_state` rows exist for a live/current trade date while
same-day `intraday_day_pack` is absent, `daily-prepare` / `pivot-data-quality --date today` now
fail closed. Those rows are treated as accidental future-state data and must be cleaned instead of
used by live.

**Dashboard note**: The dashboard can remain open during live sessions — the `paper.duckdb` replica avoids
file-lock conflicts.

**Post-open agent boundary**: after market open, agents must not run `daily-prepare`,
`pivot-refresh`, `pivot-build`, or ingestion as a readiness/status check. Use
`pivot-data-quality --date today`, `pivot-paper-trading status`, dashboard Live Readiness, and
logs only. Any rebuild/refresh after open is a recovery action and requires explicit operator
approval.

**If `daily-prepare` fails with "Runtime coverage incomplete":**
- Re-run `pivot-refresh --since <prev_trading_date> --prepare-paper --trade-date today` and check for errors
- Do NOT start `daily-live` until `daily-prepare` reports coverage ready

**Kite WebSocket STALE recovery**: Current sessions emit `FEED_STALE` when the session-wide feed
stops progressing and `FEED_RECOVERED` when data resumes. A single quiet symbol is normal and does
not count as stale by itself. Connected WebSocket sessions allow a longer quiet period before stale
is raised; disconnected sessions still use the shorter session timeout. The session stays alive
for up to 10 minutes so KiteConnect can reconnect automatically before terminating.
If you still see the old `SESSION_ERROR market_data_stale` wording, that session was started
before this rollout and must be restarted to pick up the new alert contract. Only terminate
manually if you see stale for > 10 min past entry window with no active positions.

**How to tell real feed trouble from normal quiet symbols**:
- A few symbols being quiet is normal in low-liquidity names.
- Treat stale as a **session-wide** transport problem, not a per-symbol trading signal.
- If bars keep closing and `LIVE_BAR`/trade alerts keep flowing, the session is healthy even if
  some symbols are silent.
- Use highly liquid symbols such as `SBIN` and `RELIANCE` as canaries only if they are already
  part of that day’s tradable universe. If they are not in the session symbol list, use a separate
  health-check probe and do not force them into the trading list.
- Do not let a single quiet symbol force a session restart.

**Direction Readiness & Session Startup**:

At session startup, the engine logs a `LIVE_DIRECTION_PREFLIGHT` line showing how many symbols
have resolved vs pending direction:

```
LIVE_DIRECTION_PREFLIGHT session=CPR_LEVELS_SHORT-2026-04-15 resolved=5 pending=592
  with_setup=597 missing=1498 coverage=0%
```

- `resolved` — symbols whose `strategy_day_state.direction` is LONG or SHORT.
- `pending` — symbols whose direction is NONE or not yet populated. These are **not rejected**;
  the session stays alive and retries resolution on every bar via
  `refresh_pending_setup_rows_for_bar()`.
- `missing` — symbols with no exact-date materialized setup row at startup. In live Kite mode
  these are retried from previous-day data plus live candles; they are excluded only if fallback
  cannot resolve a valid setup row.
- There is no 80% hard gate on direction coverage at startup. Stage B direction filter applies
  once setup rows exist; symbols without a resolved direction pass through and get retried.
- `evaluate_candle()` calls `refresh_pending_setup_rows_for_bar()` on each 5-minute bar, which
  re-queries `strategy_day_state` for any symbol still at `direction_pending=True`. Once the
  live 9:15 candle data is available, the direction resolves and the symbol becomes tradeable.
- `TICKER_HEALTH` telemetry is logged per-bar when a WebSocket adapter is active, showing:
  `connected`, `ticks`, `last_tick_age`, `closes`, `reconnects`, `subs`, `coverage`, `stale`,
  and `missing` counts. Use this to diagnose whether direction resolution failures are caused
  by a transport problem or a data gap.
- `MARKET_READY_HHMM = "09:16"` — default live Kite startup now fails fast before 09:16 IST
  instead of sleeping inside the process. This avoids hidden pre-market wait/retry loops on
  Windows. Use `--wait-for-open` only for an intentional supervised early launch.

**Failure drills for no-surprises live ops**:
- `market_data_stale`: use `daily-live --feed-source local` and inject a gap in the feeder, or
  monkeypatch the ticker adapter in `tests/test_live_market_data.py` so `last_tick_ts` stops
  advancing. Expect `FEED_STALE` / `FEED_RECOVERED` on new sessions.
- `bar_processing_error`: monkeypatch `paper_session_driver.process_closed_bar_group()` to raise.
  The session should emit `SESSION_ERROR reason=bar_processing_error` and fail closed.
- `session_finalize_failed`: monkeypatch the terminal `complete_session()` update path to raise.
  The session should emit `SESSION_ERROR reason=session_finalize_failed` and stamp a fallback
  terminal status.
- `auto_flatten_failed`: force a stale/failed exit with open positions, then monkeypatch
  `flatten_session_positions()` to raise. The session should emit
  `SESSION_ERROR reason=auto_flatten_failed`.
- `prefetch missing setup`: run `daily-live` before `pivot-refresh` / `daily-prepare`, or stub
  `load_setup_row()` to return `None` in a test. The session should fail fast before trading.

**WebSocket Recovery**:

The Kite ticker adapter includes automatic reconnect logic when the WebSocket disconnects:

- After 20 seconds of being disconnected, a watchdog triggers `recover_connection()` which
  recreates the WebSocket client and resubscribes to all active symbols.
- If reconnect attempts reach 3 (configurable via `_WEBSOCKET_RECONNECT_ALERT_ATTEMPTS`),
  a `SESSION_ERROR reason=websocket_reconnect_stalled` alert is dispatched once. The session
  continues running; the alert is informational, not terminal.
- On successful recovery, a `FEED_RECOVERED` alert is dispatched with the downtime duration
  and reconnect count.
- If recovery fails (e.g. Kite API is down), a `SESSION_ERROR reason=websocket_reconnect_failed`
  alert is dispatched. Repeated disconnect alerts are throttled with a 5-minute cooldown
  (`_stale_alert_cooldown_sec = 300`) to avoid alert fatigue.
- The session stays alive through recovery attempts. It only exits if the overall session-wide
  stale timeout is reached (see "Kite WebSocket STALE recovery" above).

**Auto-restart scope**: current live code can auto-flatten abnormal exits, stamp terminal
status, and alert on feed/session failures. It does **not** auto-spawn a replacement
`daily-live` process. If you want zero-manual-intervention day trading, add an external
supervisor/launcher that notices a failed/stopped session and restarts it only after
reconciliation.

**Session identity**: for the same trading day, the safest recovery path is usually to resume the
same `session_id` after reconciling DB state so open positions keep the same history. Use a new
`session_id` only when you are intentionally starting a fresh day or after the old session has
been fully closed and archived.

**What the signal scan tells you:**
- Which symbols have narrow CPR today (eligible for CPR_LEVELS entry)
- CPR width vs threshold, TC/BC levels, pivot — gives you situational awareness
- Direction (LONG or SHORT) is determined by the **9:15 candle close** relative to TC/BC —
  unknown until market opens. The engine resolves this live; the scan is for pre-market awareness only.
- `--all-symbols` runs the full local tradable universe and overrides the dated saved-universe
  default. The engine still watches all symbols during the live session — you do **not** need
  to restrict `--symbols` to the candidates list.

**Why not pre-filter symbols for daily-live?**
The Kite adapter batches 500 quotes per API call, so 2105 symbols = 5 calls per poll.
Restricting to 50–100 candidates saves only 4 calls/poll at the cost of missing late movers.
Use the dated saved universe for daily-live by default; use `--all-symbols` only when you
intentionally want the current dynamic universe for comparison.

### 2a. Live paper trading - primary sessions (current trading day)

Use the canonical CPR preset bundle by default. Start from a named preset for every canonical run;
only use explicit flags when you are intentionally doing ad hoc analysis. The dated saved universe
is the default for canonical live/replay/sim runs; `--all-symbols` scans the dynamic full universe;
the Kite adapter batches 500 symbols per API call, which stays within rate limits for the full
tradable set. `max_positions=5` caps concurrent open trades, not total trades per day.
The snapshot-universe path does not change EOD ingestion; it only adds an optional
post-prepare freeze/reuse layer for reproducible live runs.

```bash
# CPR LONG + SHORT concurrently (preferred)
doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS \
  --trade-date today
```

`--allow-late-start-fallback` is no longer required for normal startup. Live does not build or
require future-date `market_day_state` rows; it derives setup from the previous completed trading
day and resolves direction from the live 9:15 candle.

Add `--no-alerts` only for silent validation runs. Add explicit CPR flags only when you are
intentionally overriding the canonical preset.

`--skip-coverage`: bypasses the runtime coverage validation step and starts the live session
immediately even if `intraday_day_pack` or other runtime tables are incomplete. Use only when
you know coverage is acceptable (e.g. known missing symbols that are suspended/delisted) and
you do not want the pre-flight check to block startup. Do not use `--skip-coverage` as a
substitute for running `pivot-build` and `daily-prepare` — missing pack data will cause
silent no-trade sessions.

Host-shell note: these commands assume a working Doppler login on the Windows host. If `doppler run`
fails with a keyring or token error, fix Doppler auth on the host first and rerun the command.

### 2a-multi. Concurrent multi-variant live sessions

Use `--multi` to run the canonical CPR variants (LONG + SHORT) concurrently in a single process:

```bash
# CPR_LEVELS LONG + SHORT concurrently
doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS --trade-date today
```

Each variant gets its own session, pre-filtered symbol list, and RVOL policy. The alert dispatcher uses
reference counting — it only shuts down when all sessions complete. Stale sessions from previous crashes
are auto-cleaned on startup.

**Multi-variant auto-retry**: `daily-live --multi` wraps each variant in a retry loop. If a variant
exits prematurely with `FAILED`, `STALE`, `NO_ACTIVE_SYMBOLS`, or any exception, the launcher
automatically restarts it up to 5 times with a 10-second linear backoff (10s, 20s, 30s, ...).
Completed variants (after entry window close) are not retried. Retries stop after 14:30 IST (EOD
cutoff). This handles transient issues like brief WebSocket disconnections or early direction-resolution
races without manual intervention.

### 2a-resume. Mid-day session recovery (STALE or FAILED with open positions)

If a live session goes STALE or FAILED mid-day and positions are still open, resume it:

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --resume --session-id CPR_LEVELS_SHORT-2026-04-15 \
  >> .tmp_logs/live_resume_2026-04-15.log 2>&1 &
```

Behavior:
- Reads open positions from `paper.duckdb` and calls `run_live_session()` directly.
- Only `FAILED`, `STALE`, `STOPPING`, or `CANCELLED` sessions can be resumed (avoids hijacking a running session).
- Sessions with no open positions are rejected — nothing to manage.
- No new entry signals are evaluated; the resume path only manages existing open positions through to EOD.
- Closed/flattened positions from the original session are seeded into both
  `tracker._closed_today` (bar_orchestrator) and `state.position_closed_today`
  (paper_runtime) so that symbols already traded are never re-entered during the
  resumed session.
- Do **not** use `--multi` with `--resume`; the resume path targets a single named session.

**DB status mapping**: `STALE` maps to `FAILED` in the DB (CHECK constraint does not include STALE).
Internally the logic still uses STALE to decide auto-flatten behavior. The DB always gets `FAILED`
with notes `"stale_exit: ..."` so the constraint is never violated.

**FLATTEN_EOD dedup**: `flatten_session_positions()` checks `alert_log` before sending the EOD
Telegram alert. If the session already sent a `FLATTEN_EOD` (from the original process before stale),
the resumed process skips it — only one EOD alert per session.

### 2b. Historical replay — exact live parity with alerts (any past date)

Use `daily-replay` to validate the alert pipeline and exact live parity before going live.
Use `--multi --strategy CPR_LEVELS` to run LONG and SHORT concurrently in a single process —
same `asyncio.gather` concurrency as `daily-live --multi`, with a shared day-pack fetch.

```bash
# CPR LONG + SHORT concurrently (preferred)
doppler run -- uv run pivot-paper-trading daily-replay \
  --multi --strategy CPR_LEVELS \
  --trade-date 2026-04-06 \
  --all-symbols
```

Add `--no-alerts` when you want a silent replay. Use explicit CPR flags only when you
intend to override the canonical CPR preset.

Each session takes ~10–15 min (candle-by-candle, with real alert dispatch to Telegram/email).
Results are archived to `backtest.duckdb` automatically on completion.

### 2b-alt. Historical local-live — websocket-shaped driver test

Use `daily-live --feed-source local` when you want to test the live session controller
against historical `intraday_day_pack` data without Kite.

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --feed-source local \
  --multi --strategy CPR_LEVELS \
  --trade-date 2026-04-06 \
  --all-symbols --no-alerts
```

Important:
- this path is a closed-candle broadcaster built from saved day packs, not a real tick stream
- it should match replay on closed-candle decisions, quantities, exits, and final session state
- if it diverges, treat it as a session-driver bug first
- use it to verify pruning, completion, and end-of-day flattening before trusting Kite
- keep alerts enabled for the live rerun unless you explicitly want a silent test
- on normal completion, the live path emits the EOD summary alert (`FLATTEN_EOD`) even when all positions already closed earlier
- local-live does **not** prove live quote fidelity; it only proves the candle-based
  session driver agrees with `intraday_day_pack`
- if a symbol behaves differently in real Kite live but not in replay/local-live,
  the likely cause is quote/tick timing or fill-source drift, not the CPR gate

The end-of-day close rule is explicit: positions should be flattened by the configured
session cutoff, not left open until 15:30 simply because the transport is still running.

### Live feed audit recommendation

If you need to detect live-vs-EOD drift every day, use the compact
`paper_feed_audit` table in `paper.duckdb` instead of relying only on the paper ledger:

- one row per `session_id + symbol + 5-minute bucket`
- capture OHLCV as the live driver saw it
- record `first_snapshot_ts` and `last_snapshot_ts` so the bucket can be audited
- compare the stored live bars against the EOD-built `intraday_day_pack` after close

Important compare contract:

- `daily-live` rows from Kite are stamped at candle close, but the audit compare must key
  them against the candle bucket start time (`bar_start`) because the pack stores the
  source candle bucket, not the close timestamp.
- `daily-replay` and `daily-live --feed-source local` should continue to key against
  `bar_end`, because those paths already emit the pack bucket timestamp as the candle time.
- The audit is diagnostic, not a trade parity check. A session with zero trades can still
  produce useful feed audit rows.

This is the right daily guard because `intraday_day_pack` is built from EOD/pre-market
ingestion, while live paper uses real-time ticks/quotes. A candle-level mismatch can
change the entry price even when the setup rows are identical.

Daily operator command:

```bash
doppler run -- uv run pivot-paper-trading feed-audit --trade-date 2026-04-13 --feed-source kite
```

Use `--feed-source local` or `--feed-source replay` for local-feed/replay investigations.

### Live signal audit recommendation

`daily-live` and `daily-replay` also write `paper_signal_audit`, one compact decision row per
symbol per closed bar. This is the decision tape to use when feed bars match but trades still
do not:

- `ENTRY_SKIP`: symbol was not eligible for evaluation, with the skip reason
- `ENTRY_EVALUATED`: symbol was evaluated but did not become an entry candidate
- `ENTRY_CANDIDATE`: candidate fields before portfolio selection
- `ENTRY_RANKED`: candidate rank and selected/not-selected result
- `ENTRY_EXECUTED`: final paper entry execution result and selected rank

After the Kite live session completes, replay the captured Kite tape and compare both audits:

```bash
doppler run -- uv run pivot-paper-trading daily-replay \
  --strategy CPR_LEVELS \
  --trade-date today \
  --pack-source paper_feed_audit \
  --pack-source-session-id <LIVE_SESSION_ID> \
  --session-id compare-kite-audit-<direction>-YYYY-MM-DD-v1 \
  --no-alerts

doppler run -- uv run pivot-paper-trading signal-audit \
  --session-id <LIVE_SESSION_ID> \
  --compare-session-id compare-kite-audit-<direction>-YYYY-MM-DD-v1 \
  --trade-date YYYY-MM-DD
```

Use `signal-audit` first for trade-selection mismatch. Use `feed-audit` first for OHLCV/source
drift.

Running variants as separate commands (without `--multi`) executes them sequentially — each
command acquires the `runtime-writer` lock exclusively. Use `--multi` to avoid this.

### 2c. Historical simulation (any past date — fast, no alerts)

Use `daily-sim` for historical backfill when alert pipeline testing is not needed.

```bash
doppler run -- uv run pivot-paper-trading daily-sim \
  --trade-date 2026-03-27 --all-symbols
```

`daily-sim` stores results directly in DuckDB `backtest.duckdb` as `execution_mode='PAPER'` — no
separate session management overhead. Results appear in the dashboard `/paper_ledger` like any
archived paper session.

### 3. Monitor and archive live sessions

```bash
doppler run -- uv run pivot-paper-trading status
doppler run -- uv run pivot-paper-trading stop --session-id <id> --complete
```

Review archived results in the dashboard at `/paper_ledger`.

During an active `daily-live` run, `/paper_ledger` also exposes the operator
control plane. Use it to close selected symbols, flatten one selected session,
flatten both LONG and SHORT sessions for a date, pause/resume future entries,
cancel unprocessed admin intents, reduce future-entry budget/caps, or run
reconciliation. These buttons write the same admin command files as the CLI
commands below; the dashboard does not directly mutate live positions.

### 3a. Recovery: missed FLATTEN_EOD alert

If the EOD summary was not delivered (network outage at EOD, stale exit before 15:00,
or process crash), re-send it after the session is COMPLETED:

```bash
doppler run -- uv run pivot-paper-trading resend-eod \
  --session-id CPR_LEVELS_LONG-2026-04-23-live-kite
```

- Queries all CLOSED positions, builds the EOD summary, sends directly via Telegram.
- Writes to `alert_log` synchronously (bypasses the in-memory dedup guard).
- Safe to run multiple times — each call writes a fresh `alert_log` entry.
- Requires the session to have at least 1 closed position.
- Does not change session status, open/close positions, or affect `backtest_results`.

### 3b. Recovery: session FAILED with open positions (orphaned)

When a session dies STALE/FAILED with positions still OPEN in the DB:

```bash
# 1. Verify no live process is still running (DuckDB exclusive lock)
# 2. Flatten the open position at last known price and mark session COMPLETED:
doppler run -- uv run pivot-paper-trading flatten \
  --session-id CPR_LEVELS_LONG-2026-04-23-live-kite \
  --notes "stale_exit_manual"
# 3. Re-send the EOD summary:
doppler run -- uv run pivot-paper-trading resend-eod \
  --session-id CPR_LEVELS_LONG-2026-04-23-live-kite
```

**Dashboard shows wrong trade count / PnL after manual flatten**: The `backtest_results`
archive was written at stale-exit time and does not include the manually flattened position.
Fix: delete the stale archive and re-archive from the live DB.

```python
# From a doppler run -- uv run python shell:
import duckdb
sid = "CPR_LEVELS_LONG-2026-04-23-live-kite"
bcon = duckdb.connect("data/backtest.duckdb")
for t in ["backtest_results", "run_metrics", "run_metadata", "run_daily_pnl", "setup_funnel"]:
    bcon.execute(f"DELETE FROM {t} WHERE run_id=?", [sid])
bcon.commit(); bcon.close()
from scripts.paper_archive import archive_completed_session
archive_completed_session(sid)
```

**Illiquid positions and early session death**: When only 1 illiquid symbol remains open
after the entry window closes, and it stops ticking for 10+ minutes, the stale watchdog
terminates the session before the 15:00 TIME exit. This is correct behaviour — the FEED_STALE
alert includes the open position details (`entry`, `SL`, `target`, `qty`) so you can act
manually if needed. Use `flatten` + `resend-eod` as above to close the position cleanly.

---

## Prepare Runtime Tables

`daily-live` and `daily-replay` **automatically validate** DuckDB coverage before running.
If tables are missing they exit immediately with the exact symbols and a fix command:

```
Runtime coverage incomplete for 2026-04-06 (mode=replay).
  intraday_day_pack: 4 missing — EXCEL, PREMIERPOL, SANGHIIND, SHIVAUM

Note: if missing symbols are non-tradeable/delisted, no rebuild needed —
they are excluded automatically from --all-symbols.
Single-symbol gaps can also be expected on suspended or non-trading names for a specific date;
do not rerun the ingestion pipeline just because one symbol is absent if the daily readiness
check is otherwise green.
Fix:
  doppler run -- uv run pivot-build --table pack --force
```

They do **not** build tables automatically — that is an intentional separation so that
table builds (which require a DuckDB write lock) are never triggered from within a live
trading session.

### Before running pivot-build — read the error first

**The error message names missing symbols when count ≤ 20.** Check before rebuilding:

| Situation | Action |
|-----------|--------|
| Missing symbols are delisted / non-tradeable | **Do not rebuild.** The `--all-symbols` filter now excludes non-tradeable symbols automatically. Re-run the command — it will pass. |
| Missing 1-2 symbols because they were suspended / did not trade that day | **Do not rebuild.** Treat this as expected data sparsity unless the readiness check shows a broader runtime gap. |
| Missing symbols are legitimate tradeable stocks | Run the suggested `pivot-build` fix command, then re-run. |
| Missing count is large (> 20 symbols) | Run `pivot-build` — something is genuinely incomplete. |

**Pack build has a built-in date guard.** When called with `--refresh-since` (no `--force`),
`pivot-build --table pack` checks if `intraday_day_pack` already has rows for every parquet
date in that window. If it does, it prints "already covers all N dates — skipping" and exits
immediately. You only pay the delete+reinsert cost when there is genuinely new data to add.
Use `--force` only when you intentionally want to wipe and rebuild.

To check readiness manually before market open:

```bash
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols
```

For today's date (no intraday data yet), `daily-prepare` reports against the prior trading
date. All tables must be current through yesterday before the session starts.

---

## State Model

- **Active live sessions**: DuckDB `paper.duckdb` (`paper_sessions`, `paper_positions`, `paper_orders`, `paper_feed_state`, `alert_log`)
- **Archived live sessions**: DuckDB `backtest.duckdb` `backtest_results` (`execution_mode='PAPER'`) after `stop --complete`
- **Simulation results** (`daily-sim`): DuckDB `backtest.duckdb` (`execution_mode='PAPER'`) — stored directly
- **Dashboard**: Active data from versioned paper replica; archived/sim ledgers from versioned backtest replica

---

## Recovery Procedures

### Stale Feed

**Preferred recovery path**: use `daily-live --resume --session-id <id>` (see section
"2a-resume. Mid-day session recovery"). This is the recommended operator workflow — it
loads open and closed positions from DB, seeds re-entry guards, and manages positions to EOD
without requiring a new session.

If the session was started from a canonical named preset, `daily-live --resume` can also infer
the deterministic session id from `--strategy`/`--preset` and `--trade-date` when
`--session-id` is omitted.

The `pause` and `resume` commands below are low-level DB operations that change session
status and feed state directly. They do not restart the live session loop or reconnect the
WebSocket. Use them only when you need to manipulate DB state manually (e.g. to reset a
session to PLANNING before a fresh start).

```bash
doppler run -- uv run pivot-paper-trading pause --session-id paper-001
doppler run -- uv run pivot-paper-trading resume --session-id paper-001
```

### Early Exit / Emergency Close (market conditions)

Use when you need to close open positions immediately due to market conditions, operator judgment,
or end-of-day. While `daily-live` is still running, prefer the admin command queue wrappers below.
They do not compete with the live process for the DuckDB writer lock.

**Close specific symbols in one running session:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --action close_positions \
  --symbols SBIN,RELIANCE \
  --reason operator_close
```

The session keeps running after `close_positions`.

**Flatten one running session:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_SHORT-2026-04-27-live-kite \
  --action close_all \
  --reason market_conditions
```

`close_all` marks that session complete. The sibling LONG/SHORT session is unaffected.

**Flatten both LONG and SHORT sessions for a date:**
```bash
doppler run -- uv run pivot-paper-trading flatten-both \
  --trade-date today \
  --reason risk_off
```

**Reduce budget for future entries only:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_SHORT-2026-04-27-live-kite \
  --action set_risk_budget \
  --portfolio-value 500000 \
  --max-positions 5 \
  --reason reduce_short_risk
```

This does not resize existing open positions. It changes the in-memory live tracker used for
future entries. If existing open notional already consumes the reduced budget, new entries are
disabled until those positions close. To reduce current exposure immediately, also use
`close_positions` or `close_all`.

**Pause future entries while still monitoring open-position exits:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --action pause_entries \
  --reason market_regime_pause
```

**Resume future entries for the original session universe:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --action resume_entries \
  --reason market_regime_recovered
```

**Cancel unprocessed admin intents for one session:**
```bash
doppler run -- uv run pivot-paper-trading send-command \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --action cancel_pending_intents \
  --reason operator_reset_queue
```

`cancel_pending_intents` deletes only command files that the live loop has not processed yet.
It does not cancel already-filled paper exits or broker orders; real broker order cancellation
belongs to the future broker-adapter phase.

Flatten commands reconcile automatically inside the running live loop after positions are closed and
alerts are queued. If reconciliation reports critical findings after `close_all` or sentinel flatten,
the session fails closed instead of being marked as cleanly completed.

Use the standalone reconcile command when you want an explicit operator gate or diagnostic readout:
```bash
doppler run -- uv run pivot-paper-trading reconcile \
  --session-id CPR_LEVELS_SHORT-2026-04-27-live-kite \
  --strict
```

`reconcile --strict` exits non-zero on critical order/position/session mismatches.

**Real-broker dry-run payload check (no order placement):**
```bash
doppler run -- uv run pivot-paper-trading real-dry-run-order \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --symbol SBIN \
  --side BUY \
  --quantity 10 \
  --role entry \
  --event-time 2026-04-27T09:20:00+05:30
```

This prints and records the Zerodha payload with `broker_mode=REAL_DRY_RUN`.
It is safe for payload validation because the adapter does not call Kite `place_order`.

**Manual real-order pilot path:**

Manual Zerodha order placement is available through the `real-order` CLI command. It is off by
default and requires both a CLI confirmation flag and Doppler environment gates:

```bash
CPR_ZERODHA_REAL_ORDERS_ENABLED=true
CPR_ZERODHA_REAL_ORDER_ACK=I_UNDERSTAND_REAL_MONEY_ORDERS
CPR_ZERODHA_REAL_MAX_QTY=1
CPR_ZERODHA_REAL_MAX_NOTIONAL=10000
CPR_ZERODHA_REAL_ALLOWED_PRODUCTS=MIS
CPR_ZERODHA_REAL_ALLOWED_ORDER_TYPES=LIMIT,SL,SL-M
```

Default real-order safety posture:

- real orders are disabled unless `CPR_ZERODHA_REAL_ORDERS_ENABLED=true`
- the acknowledgement string must exactly match `I_UNDERSTAND_REAL_MONEY_ORDERS`
- default pilot scope is max quantity `1`, max notional `₹10,000`, `MIS`, and `LIMIT/SL/SL-M`
- `MARKET` is not allowed unless explicitly added to `CPR_ZERODHA_REAL_ALLOWED_ORDER_TYPES`
- every real order requires fresh `--reference-price` and `--reference-price-age-sec`
- real placement also requires `--confirm-real-order`

Example manual real LIMIT buy:

```bash
doppler run -- uv run pivot-paper-trading real-order \
  --session-id manual-pilot-2026-05-04 \
  --symbol SBIN \
  --side BUY \
  --quantity 1 \
  --role manual \
  --order-type LIMIT \
  --price 700 \
  --reference-price 700 \
  --reference-price-age-sec 1 \
  --confirm-real-order
```

This calls Kite `place_order` only after all gates pass and records the broker payload/order id in
`paper_orders` with `broker_mode=LIVE`.

**Automated real-order CPR pilot path:**

Normal `daily-live` remains paper-only. Automated Zerodha routing starts only when the command
includes `--real-orders` and the same Doppler gates above are enabled. The pilot intentionally
blocks `--multi --real-orders` and `--resume --real-orders`; run one LONG or one SHORT session
first.

1-share market connectivity test:

```bash
# Doppler must also allow MARKET temporarily:
# CPR_ZERODHA_REAL_ALLOWED_ORDER_TYPES=MARKET,LIMIT,SL,SL-M
doppler run -- uv run pivot-paper-trading daily-live \
  --strategy CPR_LEVELS \
  --preset CPR_LEVELS_RISK_LONG \
  --trade-date today \
  --symbols SBIN \
  --real-orders \
  --real-order-fixed-qty 1 \
  --real-order-max-positions 1 \
  --real-order-cash-budget 10000 \
  --real-entry-order-type MARKET
```

Safer first live strategy pilot after market-order connectivity is proven:

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --strategy CPR_LEVELS \
  --preset CPR_LEVELS_RISK_LONG \
  --trade-date today \
  --real-orders \
  --real-order-fixed-qty 1 \
  --real-order-max-positions 1 \
  --real-order-cash-budget 10000 \
  --real-entry-order-type LIMIT \
  --real-entry-max-slippage-pct 0.5 \
  --real-exit-max-slippage-pct 2
```

Automated pilot behaviour:

- paper-live is unchanged unless `--real-orders` is present
- real quantity is fixed by `--real-order-fixed-qty`; it does not use paper position size
- default max real-routed open positions is `1`
- real entries are cash-budgeted: cumulative open real-order notional must stay within
  `--real-order-cash-budget`, and startup rejects the session if that budget exceeds Kite
  reported available equity cash
- entries can be `LIMIT` or `MARKET`, but `MARKET` requires the Doppler order-type allow-list
- exits, stop exits, admin close, global flatten, and EOD flatten use protected LIMIT orders
- partial scale-out is blocked for real routing until it has a dedicated reconciliation path
- every real order row is recorded in `paper_orders` with `broker_mode=LIVE` and the Kite order id

**Real-order safety model:**

The accepted exit/flatten shape is protected LIMIT, not raw MARKET:

```text
LONG position emergency exit:
  side = SELL
  order_type = LIMIT
  reference_price = latest fresh LTP/mark
  limit_price = reference_price * (1 - max_slippage_pct)

SHORT position emergency exit:
  side = BUY
  order_type = LIMIT
  reference_price = latest fresh LTP/mark
  limit_price = reference_price * (1 + max_slippage_pct)
```

Default safety assumptions in code:

- zero, negative, NaN, and infinite prices are rejected before payload generation
- SL/SL-M require a trigger price
- MARKET/SL-M require Zerodha `market_protection`
- exit/close/flatten/emergency roles must use protected LIMIT with a fresh reference price
- stale reference prices are rejected
- real order placement remains blocked if either the code gate or Doppler gate is off

Example protected dry-run flatten payload:

```bash
doppler run -- uv run pivot-paper-trading real-dry-run-order \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --symbol SBIN \
  --side SELL \
  --quantity 250 \
  --role manual_flatten \
  --order-type LIMIT \
  --price 784 \
  --reference-price 800 \
  --reference-price-age-sec 1 \
  --max-slippage-pct 2 \
  --event-time 2026-04-27T10:30:00+05:30
```

Dashboard visibility:

- `/paper_ledger` shows active sessions, open/closed positions, orders, feed state, and archived ledgers.
- Active sessions show `PAPER LIVE` or `ZERODHA LIVE` in the session selector and broker chip.
- Real-routed order rows show `broker_mode=LIVE` and the Zerodha exchange order id.
- `/paper_ledger` can queue paper controls: close selected symbols, flatten one session, flatten LONG+SHORT, pause/resume entries, cancel pending intents, set risk budget, and reconcile.
- `/paper_ledger` does **not** currently show a dedicated real-broker payload preview or broker snapshot comparison panel. Use the CLI dry-run and `broker-reconcile` commands for that.
- No dashboard control starts real Zerodha trading; real routing starts only from CLI `daily-live --real-orders`.

**Broker reconciliation check (read-only):**
```bash
doppler run -- uv run pivot-paper-trading broker-reconcile \
  --session-id CPR_LEVELS_LONG-2026-04-27-live-kite \
  --broker-orders-json broker_orders.json \
  --broker-positions-json broker_positions.json \
  --strict
```

`broker-reconcile` compares local paper orders/positions against supplied broker snapshots. It does
not call Kite and does not place/cancel orders. The read-only adapter path can map Kite
`orders()` / `positions()` responses into the same snapshot format for future supervised drills.

**Real-pilot guardrail check (does not enable real orders):**
```bash
doppler run -- uv run pivot-paper-trading pilot-check \
  --symbols SBIN \
  --order-quantity 1 \
  --estimated-notional 5000 \
  --acknowledgement I_ACCEPT_REAL_ORDER_RISK \
  --strict
```

The guardrail allows at most 2 symbols, quantity 1, up to Rs10,000 notional, MIS product, and
LIMIT orders. Even when it passes, the payload reports `real_orders_enabled=false`; it is a readiness
gate, not a switch that enables Zerodha order placement.

**Offline fallback if the live process is already dead or DB status is stale:**
```bash
doppler run -- uv run pivot-paper-trading flatten-all --trade-date today --notes "market_conditions"
```

Use `flatten` / `flatten-all` only when the live process is no longer holding the writer lock.
During an active live session, use `send-command` or `flatten-both`.

All manual/operator flatten paths close at latest live mark when available:
- Kite/live mode: latest `KiteTickerAdapter.get_last_ltp(symbol)`.
- Local-feed mode: latest local feed LTP.
- Fallback: last recorded feed-state symbol price, then position last price, then entry price.

### Selective session flatten while live is still running

**Option A — Session sentinel (close all positions in one session):**

```powershell
New-Item -ItemType File -Path .tmp_logs\flatten_CPR_LEVELS_LONG-2026-04-25-live-kite.signal -Force | Out-Null
```

The running `daily-live` process checks for `.tmp_logs/flatten_<session_id>.signal` on every
poll cycle (~1s). When present, that session closes all open positions, sends `FLATTEN_EOD`,
and marks itself `COMPLETED`. The sibling session is unaffected.

**Option B — Admin command queue (close specific symbols or all, from any caller):**

Drop a JSON file into `.tmp_logs/cmd_<session_id>/`. The live loop processes it within ~1s
without requiring a DB lock, then deletes it.

```powershell
# Close two specific symbols — session keeps running:
$cmd = '{"action":"close_positions","symbols":["SBIN","RELIANCE"],"reason":"manual","requester":"operator"}'
$ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
New-Item -ItemType Directory -Path ".tmp_logs\cmd_CPR_LEVELS_SHORT-2026-04-25-live-kite" -Force | Out-Null
$cmd | Out-File ".tmp_logs\cmd_CPR_LEVELS_SHORT-2026-04-25-live-kite\${ts}_close.json" -Encoding utf8

# Close all — equivalent to sentinel but also works for the agent/dashboard:
$cmd = '{"action":"close_all","reason":"market_conditions","requester":"operator"}'
$cmd | Out-File ".tmp_logs\cmd_CPR_LEVELS_SHORT-2026-04-25-live-kite\${ts}_closeall.json" -Encoding utf8
```

Prefer the CLI wrappers above over hand-written JSON. The agent can trigger the same queue via:
`paper_send_command(session_id, "close_positions", symbols=[...])`.

Each closed position sends a `TRADE_CLOSED` alert. Dashboard active-session state
refreshes from the paper replica every 3s by default.
The session stays `ACTIVE` after `close_positions`; only `close_all` marks it terminal.
Both paths run reconciliation after the close. Critical reconciliation findings after
`close_positions` disable new entries and continue exit monitoring for existing positions.

### Flatten All Positions (legacy single-session)

```bash
doppler run -- uv run pivot-paper-trading flatten --session-id paper-001
```

### Session Recovery After Crash

1. `pivot-paper-trading status --session-id <id>` to confirm DuckDB paper state
2. Restart live loop with the same session id
3. If already complete, archive with `stop --complete`

Stale ACTIVE/PAUSED/STOPPING sessions are automatically cancelled on next `pivot-paper-trading` startup
via `cleanup_stale_sessions()`. You will see: `Cleaned up N stale session(s) from previous run(s)`.

### Paper-only cleanup before a rerun

```bash
doppler run -- uv run pivot-paper-trading cleanup --trade-date YYYY-MM-DD --apply
```

This clears only PAPER rows and paper-session state. It does **not** touch baseline backtests.
Use `pivot-reset-history --apply` only when you intentionally want to clear backtest history too.

---

## Parity Check

Compare a paper session (or sim run) against its equivalent backtest run:

```bash
doppler run -- uv run pivot-parity-check --expected-run-id <backtest-run> --actual-run-id <paper-run>
```

The `daily-sim` run_ids are printed in the command output. For live sessions, get the archived
run_id from `pivot-paper-trading status --session-id <id>`.

When a paper session and backtest differ on trade count, compare the capital overlay first.
The shared paper driver can be correct while the backtest overlay still diverges on
same-bar cash reuse or candidate rejection. Treat that as a parity bug, not as expected drift.

---

## Setup (first time)

```bash
docker-compose up -d                          # only needed for AI agent
doppler run -- uv run pivot-db-init            # PostgreSQL schema (agent sessions, signals)
doppler run -- uv run python -m scripts.migrate_split --split-backtest  # one-time: split backtest tables
```

Paper trading state is fully in DuckDB (`paper.duckdb`). No PostgreSQL setup needed for daily paper flow.
