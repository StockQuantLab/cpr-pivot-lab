# Paper Trading Runbook

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
- When multiple symbols qualify on the same bar, the current shared selector uses a
  deterministic symbol-order tie-break via `select_entries_for_bar()`. That keeps replay,
  live, and backtest reproducible. It is not a profitability optimizer. If you want to rank
  same-bar candidates by a score such as effective RR, that is a strategy change and must be
  baseline-rerun like any other rule change.
- `profit_loss_pct` is the trade's net return on entry notional (`position_value`), not the
  percentage of account equity. A small-share trade can therefore show a large percentage loss
  even when the rupee loss is modest.
- Canonical CPR replay/live command (no extra CPR flags unless you are intentionally overriding):
  `doppler run -- uv run pivot-paper-trading daily-replay --multi --strategy CPR_LEVELS --trade-date 2026-04-02 --all-symbols --no-alerts`
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
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-09 --preset CPR_LEVELS_STANDARD_LONG  --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-09 --preset CPR_LEVELS_STANDARD_SHORT --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-09 --preset CPR_LEVELS_RISK_LONG      --save --quiet
uv run pivot-backtest --all --universe-size 0 --yes-full-run --start 2025-01-01 --end 2026-04-09 --preset CPR_LEVELS_RISK_SHORT     --save --quiet
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

- **Current v2 baselines (TrailingStop intraday-high fix — use these for all future comparisons):**
- **Current v3 baselines (TrailingStop fix + SHORT trail tuning — use these for all future comparisons):**

SHORT presets now use `short_trail_atr_multiplier = 1.25`. LONG keeps `trail_atr_multiplier = 1.0`.

| Mode | Preset | Run ID | Start → End | P/L | Calmar |
|------|--------|--------|-------------|-----|--------|
| Daily Reset | `CPR_LEVELS_RISK_LONG` | `3898e767c6a9` | 2025-01-01 → 2026-04-17 | ₹893,968 | 143 |
| Daily Reset | `CPR_LEVELS_RISK_SHORT` | `3c139d78214a` | 2025-01-01 → 2026-04-17 | ₹1,053,952 | 73 |
| Daily Reset | `CPR_LEVELS_STANDARD_LONG` | `e4f3123e8ad7` | 2025-01-01 → 2026-04-17 | ₹905,186 | 145 |
| Daily Reset | `CPR_LEVELS_STANDARD_SHORT` | `ab10eca1e9c9` | 2025-01-01 → 2026-04-17 | ₹1,057,696 | 66 |
| Compound | `CPR_LEVELS_STANDARD_LONG` | `206283c94744` | 2025-01-01 → 2026-04-17 | ₹1,827,199 | 181 |
| Compound | `CPR_LEVELS_STANDARD_SHORT` | `b7688096ded7` | 2025-01-01 → 2026-04-17 | ₹2,539,392 | 104 |
| Compound | `CPR_LEVELS_RISK_LONG` | `dcb0f8fd2ddf` | 2025-01-01 → 2026-04-17 | ₹1,826,581 | 181 |
| Compound | `CPR_LEVELS_RISK_SHORT` | `c2fcdfa605ef` | 2025-01-01 → 2026-04-17 | ₹2,536,852 | 104 |

The v2 fix (2026-04-19) makes TRAIL activate on intraday HIGH ≥ 2R (not just close).
The v3 short-tuning step lifts SHORT by another ~₹42K daily-reset / ~₹173K compound.
LONG still benefits strongly from the fix (+₹80K daily-reset, +₹234K compound). See
`docs/ISSUES.md` and `docs/trailing-stop-explained.md` for the full explanation.

When extending the v2 set to a later end date, rerun these same eight presets and
compare the overlapping window only. The incremental window should be the only source
of P/L delta. Do not infer the reference set from label prefixes alone.

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

**Position-limit note**: `max_positions=10` is a cap on concurrent open positions, not a cap on
total trades in a day. A session can take more than 10 trades if earlier positions close and later
signals open new ones.

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

```bash
doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE
doppler run -- uv run pivot-kite-ingest --from 2026-03-30 --to 2026-03-30
doppler run -- uv run pivot-kite-ingest --from 2026-03-30 --to 2026-03-30 --5min --resume
doppler run -- uv run pivot-build --refresh-since 2026-03-30 --batch-size 64
```

### 1b. Pre-market setup and readiness check (8:30–9:10 AM)

**CRITICAL**: Do these steps **before** starting `daily-live` for best parity and stability.
`daily-live` does not auto-build runtime tables.

```bash
# Step 1 — Build today's setup rows from yesterday's close data (8:30–8:45 AM)
# Replace <prev_trading_date> with the last NSE trading day (e.g. 2026-04-10)
doppler run -- uv run pivot-refresh --since <prev_trading_date>

# Step 2 — Verify runtime table coverage for prior trading date (must pass before starting live)
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols

# Step 3 — Scan for today's narrow-CPR candidates (informational — 8:45–9:10 AM)
doppler run -- uv run pivot-signal-alert --universe gold_51 --condition narrow-cpr
# Or full universe:
doppler run -- uv run pivot-signal-alert --all-symbols --condition narrow-cpr
```

**Pre-market build (fixed Apr 2026)**: `market_day_state` is now buildable pre-market.
`pivot-refresh` uses an ASOF JOIN for ATR (finds the most recent prior-day ATR when today's
bars don't yet exist) so today's CPR rows appear in `market_day_state` pre-market.
`--allow-late-start-fallback` is no longer required; direction is resolved from the live
9:15 candle when the DB row has `or_close_5 = NULL`.

**Dashboard note**: The dashboard can remain open during live sessions — the `paper.duckdb` replica avoids
file-lock conflicts.

**If `daily-prepare` fails with "Runtime coverage incomplete":**
- Re-run `pivot-refresh --since <prev_trading_date>` and check for errors
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
- `missing` — symbols with no setup row at all (excluded from trading).
- There is no 80% hard gate on direction coverage at startup. Stage B direction filter applies
  once setup rows exist; symbols without a resolved direction pass through and get retried.
- `evaluate_candle()` calls `refresh_pending_setup_rows_for_bar()` on each 5-minute bar, which
  re-queries `strategy_day_state` for any symbol still at `direction_pending=True`. Once the
  live 9:15 candle data is available (or the DB row is updated by a parallel build), the
  direction resolves and the symbol becomes tradeable.
- `TICKER_HEALTH` telemetry is logged per-bar when a WebSocket adapter is active, showing:
  `connected`, `ticks`, `last_tick_age`, `closes`, `reconnects`, `subs`, `coverage`, `stale`,
  and `missing` counts. Use this to diagnose whether direction resolution failures are caused
  by a transport problem or a data gap.
- `MARKET_READY_HHMM = "09:16"` — the WebSocket subscription is delayed until 09:16 IST
  (market open + 1 minute) so the 9:15 candle has time to close before direction resolution
  uses it. Before 09:16 the startup code sleeps; after 09:16 it subscribes immediately.

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
- `--all-symbols` runs the full 2105-symbol universe. The engine still watches all symbols
  during the live session — you do **not** need to restrict `--symbols` to the candidates list.

**Why not pre-filter symbols for daily-live?**
The Kite adapter batches 500 quotes per API call, so 2105 symbols = 5 calls per poll.
Restricting to 50–100 candidates saves only 4 calls/poll at the cost of missing late movers.
Keep `--all-symbols` for daily-live; use the signal scan for situational awareness only.

### 2a. Live paper trading - primary sessions (current trading day)

Use the canonical CPR preset bundle by default. Start from a named preset for every canonical run;
only use explicit flags when you are intentionally doing ad hoc analysis. `--all-symbols` scans the full universe;
the Kite adapter batches 500 symbols per API call so 2106 symbols = 5 calls per poll,
well within rate limits. `max_positions=10` caps concurrent open trades, not total trades per day.

```bash
# CPR LONG + SHORT concurrently (preferred)
doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS \
  --trade-date today --all-symbols
```

`--allow-late-start-fallback` is no longer required. `pivot-refresh` now builds
`market_day_state` pre-market using prev-day ATR (ASOF JOIN). Direction is resolved from
the live 9:15 candle automatically when `or_close_5` is NULL in the DB row.

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
  --multi --strategy CPR_LEVELS --trade-date today --all-symbols
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
  doppler run -- uv run pivot-build --table pack --force --batch-size 64
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

### Flatten All Positions

```bash
doppler run -- uv run pivot-paper-trading flatten --session-id paper-001
doppler run -- uv run pivot-paper-trading stop --session-id paper-001 --complete
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
