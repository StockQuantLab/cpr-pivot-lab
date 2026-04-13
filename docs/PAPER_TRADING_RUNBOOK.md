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
- Prefer named presets such as `CPR_LEVELS_RISK_LONG` and `CPR_LEVELS_RISK_SHORT` instead of hand-spelling the full flag bundle.
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

- Latest set including `2026-04-09`:
  - `1993f32beea7` (`CPR_LEVELS_STANDARD_LONG`)
  - `6d1afd502546` (`CPR_LEVELS_STANDARD_SHORT`)
  - `89b702775d22` (`CPR_LEVELS_RISK_LONG`)
  - `e8a7de2d3258` (`CPR_LEVELS_RISK_SHORT`)
- Rebuilt `2026-04-08` set:
  - `8e930145831f` (`CPR_LEVELS_STANDARD_LONG`)
  - `eef97528c63f` (`CPR_LEVELS_STANDARD_SHORT`)
  - `ee81feba3dbe` (`CPR_LEVELS_RISK_LONG`)
  - `8f865018cea1` (`CPR_LEVELS_RISK_SHORT`)

Dashboard note:
- Home page `Recent Backtest Runs` now has a one-click copy icon for `run_id`.
- Run Detail (`/backtest`) header now has `run_id` + copy button.

### CPR Parity Learnings

For daily CPR compares, treat strategy filters and session state as separate checks:

- `daily-replay` starts a fresh paper session for the requested date.
- Matching params are necessary, but they do not by themselves guarantee identical executed rows across a one-day replay and a multi-day backtest window.
- The 2026-04-01 CPR compare exposed an open mismatch: paper replay archived trades for `GANESHCP` and `ASTERDM`, while the full backtest runs for the same date did not show those rows.
- When this happens, compare the setup rows and intraday day packs first, then diff the replay entry path against the backtest entry path before changing the filters.
- Capital / sizing may change quantity and P&L, but a trade appearing or disappearing is a separate parity problem.
- The intended fix direction is to unify the CPR entry search itself. Replay/live should keep the candle-by-candle operational behavior, and backtest should be brought into the same shared entry evaluation so all paths agree.

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
doppler run -- uv run pivot-paper-trading cleanup --apply
```

Use this when you want a clean paper rerun from storage but need to preserve backtest baselines.
If you only need to archive one finished session, use `stop --complete` instead of a full cleanup.

The same RVOL policy is used in backtest, `daily-sim`, and `daily-live` unless a flag overrides it:

- CPR_LEVELS LONG uses RVOL 1.0
- CPR_LEVELS SHORT skips RVOL

If you need a clean rerun from storage, use `pivot-reset-history --apply` from the host shell.
That wipes DuckDB `backtest_results`, `run_daily_pnl`, `run_metrics`, `run_metadata`,
`setup_funnel` (in `backtest.duckdb`), plus `paper_sessions`, `paper_positions`,
`paper_orders`, `paper_feed_state`, and `alert_log` (in `paper.duckdb`),
and PostgreSQL `walk_forward_runs`, `walk_forward_folds`.
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
- `pivot-paper-trading walk-forward`
- `pivot-paper-trading walk-forward-matrix`
- `pivot-paper-trading walk-forward-replay`
- `pivot-paper-trading walk-forward-cleanup --apply`
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
# Replace <prev_trading_date> with the last NSE trading day (e.g. 2026-04-07)
doppler run -- uv run pivot-refresh --since <prev_trading_date>

# Step 2 — Verify runtime table coverage for today (must pass before starting live)
doppler run -- uv run pivot-paper-trading daily-prepare --trade-date today --all-symbols

# Step 3 — Scan for today's narrow-CPR candidates (informational — 8:45–9:10 AM)
doppler run -- uv run pivot-signal-alert --universe gold_51 --condition narrow-cpr
# Or full universe:
doppler run -- uv run pivot-signal-alert --all-symbols --condition narrow-cpr
```

**Dashboard note**: The dashboard can remain open during live sessions — the `paper.duckdb` replica avoids
file-lock conflicts.

**If `daily-prepare` fails with "Runtime coverage incomplete":**
- Re-run `pivot-refresh --since <prev_trading_date>` and check for errors
- Do NOT start `daily-live` until `daily-prepare` reports coverage ready

**Late start / restart note (after market open):**
- Strict setup parity is enforced for invalid setup rows. Missing same-day `market_day_state`
  rows are tolerated and skipped, so a small gap does not trigger another build cycle.
- If you intentionally want candle-derived recovery for a late start, use:
  `--allow-late-start-fallback`
- For normal parity runs, keep the fallback off and use `--no-alerts` only when you are
  comparing paper paths and do not want Telegram/email noise.

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

Use the canonical CPR preset bundle by default. `--all-symbols` scans the full universe;
the Kite adapter batches 500 symbols per API call so 2106 symbols = 5 calls per poll,
well within rate limits. `max_positions=10` caps concurrent open trades, not total trades per day.

```bash
# CPR LONG + SHORT concurrently (preferred)
doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS \
  --trade-date today --all-symbols
```

Add `--no-alerts` only for silent validation runs. Add explicit CPR flags only when you are
intentionally overriding the canonical preset.

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

The end-of-day close rule is explicit: positions should be flattened by the configured
session cutoff, not left open until 15:30 simply because the transport is still running.

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
doppler run -- uv run pivot-paper-trading cleanup --apply
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
docker-compose up -d                          # only needed for AI agent / walk-forward
doppler run -- uv run pivot-db-init            # PostgreSQL schema (agent sessions, walk-forward)
doppler run -- uv run python -m scripts.migrate_split --split-backtest  # one-time: split backtest tables
```

Paper trading state is fully in DuckDB (`paper.duckdb`). No PostgreSQL setup needed for daily paper flow.
