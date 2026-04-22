# Issues and Fixes Log

This is the single consolidated record of all bugs, incidents, and fixes for the live
paper trading system. Every new issue or fix must be documented here going forward.

Supersedes: `docs/PARITY_INCIDENT_LOG.md` (contents migrated below).

---

## 2026-04-23 — PERF: CPR incremental build scans full 10-year Parquet history

**Status:** FIXED — `db/duckdb.py` `build_cpr_table()`
**Severity:** Medium — EOD pipeline Step 4 (CPR refresh) takes ~5 min for a single-date refresh instead of ~5s

### Root Cause
`build_cpr_table()` builds a CTE chain: `raw_daily → daily → base → with_levels → with_shift → SELECT`.
The date filter (`window_filter_sql`) was only applied on the final `SELECT`. All inner CTEs
(especially `raw_daily FROM v_daily` and the `LAG/LEAD` window functions in `with_shift`) scanned
the full 10-year Parquet history (~3.9M daily rows, ~2035 symbols) even for a single-date incremental refresh.

### Fix Applied
`db/duckdb.py` `build_cpr_table()` — added `parquet_date_filter_sql` pushed into `raw_daily`:

```python
if since_date_iso and not force:
    parquet_date_filter_sql = (
        f"AND date::DATE >= ('{since_date_iso}'::DATE - INTERVAL '7 days')"
    )
```

The 7-day calendar lookback covers weekends + holidays. For `trade_date = T`:
- `raw_daily.date = T-1` provides the OHLC → via LEAD becomes trade_date T
- `raw_daily.date = T-2` provides the LAG prev_tc/prev_bc for the T row

DuckDB now reads ~4K rows (2 per symbol) instead of 3.9M for a single-date refresh.
Not applied when `force=True` or no `since_date` (full rebuild needs all history).

### Expected Impact
CPR step in EOD pipeline: ~5 min → ~5–10 s for a single-date incremental refresh.

### Related Notes
- `cpr_thresholds`: genuinely needs 252 prior rows (rolling quantile), reads from `cpr_daily`
  (materialized, fast) — no further optimization possible there
- `atr_intraday`: batched by symbol groups, already reasonably fast
- `market_day_state` / `strategy_day_state`: already have early date filters, not affected

---

## 2026-04-22 — FEATURE: Need ability to flatten one session while keeping the other live

**Status:** FIXED — `scripts/paper_live.py` (sentinel file IPC, Option A)
**Severity:** Medium — operator cannot surgically stop SHORT on an up-day without also killing LONG

### Observation
On up-days (Apr 21, Apr 22) SHORT bleeds while LONG produces trailing winners. Ideal response
is to flatten all open SHORT positions mid-session and let LONG continue uninterrupted.
Currently not possible because both sessions share one process and one DuckDB exclusive lock —
killing the process kills both.

### Required Capability
A way to close all open positions for one session_id while the sibling session keeps running,
without requiring a process kill. Options:

**Option A — in-process signal/command**
Add a lightweight IPC mechanism (e.g. a sentinel file or named pipe) that the running process
polls each bar. If `.tmp_logs/flatten_<session_id>.signal` exists, flatten that session and
mark it COMPLETED, then delete the signal file. The sibling session continues unaffected.

**Option B — separate writer process with DuckDB WAL**
Allow a secondary process to write position closes to `paper.duckdb` while the primary holds
the write lock — not feasible with DuckDB exclusive locking on Windows.

**Option C — kill + flatten SHORT + resume LONG (current workaround)**
1. `taskkill //F //PID <live_pid>`
2. `doppler run -- uv run pivot-paper-trading flatten --session-id CPR_LEVELS_SHORT-<date>-live-kite`
3. `doppler run -- uv run pivot-paper-trading daily-live --resume --session-id CPR_LEVELS_LONG-<date>-live-kite`
Downside: `--resume` mode takes no new LONG entries after restart.

**Recommendation:** Option A (sentinel file) is the cleanest — zero new dependencies,
consistent with the existing single-writer model, and reversible.

### Fix Applied
`scripts/paper_live.py` — bar loop now checks for `.tmp_logs/flatten_<session_id>.signal`
after each bar group. If the file exists: log the event, delete the file, set
`final_status=COMPLETED / complete_on_exit=True`, break the loop. The normal exit path then
calls `flatten_session_positions()` to close all open positions and sends FLATTEN_EOD alert.
The sibling session's bar loop is unaffected (different `session_id`, different signal file).

**Usage:**
```bash
# Flatten SHORT session while LONG keeps running:
touch .tmp_logs/flatten_CPR_LEVELS_SHORT-2026-04-23-live-kite.signal
# Engine picks it up at the next bar boundary (~5 sec), closes all SHORT positions, exits cleanly.
```

### Context
First identified: 2026-04-22. Two consecutive up-days (Apr 21, Apr 22) where SHORT lost
while LONG won. The need became clear when manual intervention was desired at 09:50 IST
but the mechanism didn't exist.

---

## 2026-04-22 — BUG: PLANNING→ACTIVE transition also lost in replica debounce

**Status:** FIXED — `scripts/paper_live.py`
**Severity:** Low — cosmetic; sessions are genuinely ACTIVE; dashboard shows stale PLANNING label

### Symptom
After both sessions go ACTIVE at 09:16, the dashboard continued to show `SHORT = PLANNING`
and `Feed = UNKNOWN` until the first 9:20 candle triggered a replica sync. `LONG` transitioned
visibly first (its ACTIVE write happened to land in the sync window); `SHORT` did not.

### Root Cause
Same 5-second debounce as the pre-create race. `LONG.update(ACTIVE)` → `_after_write()` →
sync fires (v4526, SHORT still PLANNING). `SHORT.update(ACTIVE)` milliseconds later →
`_after_write()` → debounce blocks. No events fire for ~3 min until first candle.

Replica v4526 confirmed: `LONG=ACTIVE (09:16 IST)`, `SHORT=PLANNING (03:04 IST stale)`.

### Fix
`paper_live.py` `run_live_session()` should call `_pdb().force_sync()` after setting
`status=ACTIVE`, OR the periodic-sync Option B from the pre-create issue would cover both
cases automatically.

### Fix Applied
`scripts/paper_live.py` — added `force_paper_db_sync(get_paper_db())` immediately after
`_update_session(status="ACTIVE")`. Both LONG and SHORT sessions now publish a consistent
replica snapshot as they go ACTIVE, so the dashboard reflects ACTIVE status before the
first candle arrives.

---

## 2026-04-22 — WARN: GILLANDERS skipped every session — ATR=0.0000

**Status:** FIXED — `db/duckdb.py`
**Severity:** Low — single symbol excluded; no trading impact

### Symptom
Both LONG and SHORT sessions emit on every startup:
```
Setup prefetch skipped 1 invalid rows on 2026-04-22 (critical fields <= 0):
GILLANDERS(tc=92.7767, bc=92.5300, atr=0.0000)
```
CPR levels are present but ATR is zero, so the symbol is correctly excluded from trading.

### Root Cause (hypothesis)
`atr_intraday` for GILLANDERS on 2026-04-22 is either missing or computed as zero.
Likely cause: symbol had no 5-min trades on the reference date used for ATR calculation
(suspended, circuit-filtered, or very thin liquidity day).

### Root Cause (confirmed)
`atr_intraday` for GILLANDERS on 2026-04-21 is 0.0 (circuit filter / no trades). The ASOF JOIN
in `market_day_state` was joining to `atr_intraday` without filtering zero-ATR rows, so it
found Apr 21 (atr=0.0) as the "most recent" row for Apr 22 instead of looking further back to
Apr 20 (atr=0.245).

### Fix Applied
`db/duckdb.py` line 1553 — changed ASOF JOIN target from `atr_intraday` to
`(SELECT * FROM atr_intraday WHERE atr > 0)` so zero-ATR circuit-filter days are skipped and
the join reaches back to the nearest valid ATR. Takes effect on next `pivot-build --refresh-since`.

---

## 2026-04-22 — WARN: SUMIT missing from Kite instrument token map (recurring)

**Status:** FIXED — symbol purged via `pivot-hygiene --purge --confirm`
**Severity:** Low — symbol silently excluded from WebSocket subscription; no trading impact

### Symptom
Every `daily-live` session startup logs:
```
Instrument tokens not found for 1 symbols: ['SUMIT']
Instrument tokens missing for 1 symbols (not in cached map): ['SUMIT']
```
Appears on both LONG and SHORT adapter instances.

### Root Cause
`SUMIT` is in the strategy candidate universe (passes CPR/ATR filters) but is absent from
the Kite instrument master (`--refresh-instruments` map). Likely suspended, renamed, or
moved to a different exchange segment since the last instrument refresh.

### Fix Applied
`pivot-kite-ingest --refresh-instruments --exchange NSE` ran — SUMIT confirmed absent from
current Kite master (genuinely delisted). `pivot-hygiene --purge --confirm` removed SUMIT
and 72 other dead symbols (107 MB freed, 867K rows deleted).

---

## 2026-04-22 — BUG: Monitor grep too broad — Telegram HTTP lines flood notifications

**Status:** FIXED (same session) — tighter pattern applied; CLAUDE.md + AGENTS.md updated
**Severity:** Low — cosmetic; no trading impact; just noisy during 9:20 trade-open burst

### Symptom
The live session monitor uses a grep pattern that matches `session` and general log lines,
which causes every `httpx: HTTP Request: POST ...sendMessage "HTTP/1.1 200 OK"` line to
fire a notification during the Telegram alert burst at 9:20 open (~15–20 alerts back-to-back).
Makes it hard to spot actual trades and errors in the notification stream.

### Fix
Tighten the monitor grep to exclude raw `httpx`/`HTTP Request` lines and focus on:
- Trade opens/closes: `paper trade open|paper trade close|TRADE|TARGET|SL_HIT|TRAIL|PARTIAL`
- Bar heartbeat: `LIVE_BAR|TICKER_HEALTH`
- Errors: `STALE|ERROR|Exception|Traceback|WARNING scripts.paper`

Updated pattern (already applied to current session monitor):
```
tail -f .tmp_logs/live_20260422.log | grep --line-buffered -E \
  "trade open|trade close|TRADE|TARGET|SL_HIT|TRAIL|PARTIAL|LIVE_BAR|TICKER_HEALTH|STALE|ERROR|Exception|Traceback|WARNING scripts.paper"
```

---

## 2026-04-22 — BUG: SHORT session missing from dashboard during pre-market PLANNING phase

**Status:** FIXED — `scripts/paper_trading.py` (`_pdb().force_sync()` after pre-create loop, line 1215)
**Severity:** Low — operator may think SHORT session failed to start

### Symptom
After `daily-live --multi --strategy CPR_LEVELS` starts before market open, the dashboard
shows only `CPR_LEVELS LONG` in PLANNING. `CPR_LEVELS SHORT` is absent until the WebSocket
connects and the first candle event fires at or just after 09:16.

Observed: 2026-04-22 pre-market. Log confirmed both sessions pre-created:
```
[pre-create] CPR_LEVELS_LONG-2026-04-22-live-kite  (PLANNING)
[pre-create] CPR_LEVELS_SHORT-2026-04-22-live-kite (PLANNING)
```
Paper replica `v4524` had zero Apr 22 sessions; `v4525` had only LONG. SHORT was in
`paper.duckdb` but the replica sync landed in the ~millisecond gap between the two
sequential `INSERT` calls.

### Root Cause
The replica sync (`maybe_sync()`) is **event-driven** — it fires on meaningful trading
events (candle processed, position opened, etc.). During the pre-market wait (~40 min),
no events fire after session pre-creation. If the sync happens to execute between the LONG
and SHORT `INSERT` calls, the dashboard gets an incomplete snapshot and sees no further
update until 09:16.

### Proposed Fix (two options — pick one)

**Option A — explicit post-create sync (simplest)**
After all sessions are pre-created in `_cmd_daily_live_multi` (before `_wait_until_market_ready`),
call `paper_db.maybe_sync(source_conn=paper_db.con)` once to guarantee a consistent
snapshot of all sessions.

**Option B — periodic background sync**
Add a lightweight background thread in `PaperDB` (or `ReplicaSync`) that publishes a new
replica snapshot every 30 s regardless of trading events. Keeps dashboard live during any
future quiet periods (e.g., between trades on a slow day). Slightly more complex but
eliminates the whole class of "stale dashboard" issues.

### Files to Change
- `scripts/paper_trading.py` — Option A: add `maybe_sync` call after pre-create loop
- `db/replica.py` / `db/paper_db.py` — Option B: periodic sync thread

---

## 2026-04-22 — EXPERIMENT (HIGH PRIORITY): Backtest `entry_window_start_short=09:35`

**Status:** PENDING — queue after EOD; gold_51 × 15-month window
**Severity:** Informational — strategy improvement candidate

### Motivation
Two consecutive live days (Apr 21 up-day, Apr 22 down-day) show the same pattern:
- SHORT entries at 9:20–9:30 disproportionately hit INITIAL_SL or BREAKEVEN_SL within 1–3 bars
- The culprit is **gap-open + intraday bounce**: market gaps down, SHORT entry triggers at 9:20,
  then a recovery move into 9:30–9:35 sweeps the SL
- Entries at 9:35+ (post-bounce) on both days had materially better hit rates

Apr 22 evidence:
| Entry time | SHORT outcomes |
|------------|----------------|
| 9:20 | 10 opened → 7 BREAKEVEN or INITIAL_SL, 1 TARGET (DMART), 2 still open at that point |
| 9:25–9:30 | 5 opened → 4 SL, 0 targets in window |
| 9:35–9:45 | 9 opened → CANTABIL TARGET, LT TARGET, VERANDA TARGET, FEDFINA TARGET; others mostly BREAKEVEN |

Apr 21 (same pattern): early shorts reversed by 9:35 bounce; SHORT P&L dragged positive by later entries.

### Hypothesis
Delaying the SHORT entry window to 09:35 eliminates the bounce-SL cluster while retaining
entries into confirmed intraday down-moves. The 9:20 bar (OR open) still sets direction —
we just wait for the bounce to exhaust before entering.

### Backtest Command
```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --universe-name gold_51 --start 2024-10-01 --end 2026-03-31 \
  --preset CPR_LEVELS_RISK_SHORT \
  --strategy-params '{"entry_window_start_short": "09:35"}' \
  --save --quiet --progress-file .tmp_logs/bt_short_entry35.jsonl
```

### Accept Criteria
- SHORT trade count stays ≥ 70% of baseline (some early trades lost is acceptable)
- SHORT Calmar holds or improves vs baseline `23376249a6ca`
- Win rate improves (fewer early SL hits)

### Risk
If the 09:35 window also misses the real downward continuation (market falls early, bounces
by 09:35 then stalls), we may lose both ends. Check: does LONG benefit from the same delay?
(Probably not — LONG entries benefit from early momentum.)

---

## 2026-04-22 — OBSERVATION: Gap-open reversal kills early SHORT entries (2-day pattern)

**Status:** INFORMATIONAL — informs `entry_window_start_short` experiment above
**Severity:** Low — expected market behaviour; not a bug

### Pattern
On both Apr 21 (up-day) and Apr 22 (down-day by NIFTY close), the same intraday structure:
1. **09:15–09:20**: CPR direction fires SHORT for many symbols (gap-down open or intraday weakness)
2. **09:20–09:30**: Sharp reversal / bounce off day lows. Early SHORT entries get swept.
3. **09:35+**: Bounce exhausts, move resumes in original direction. Later entries profitable.

This pattern is NOT visible in daily-reset backtests because backtests use vectorised 5-min
bars that don't capture the 09:15–09:25 tick-level bounce that triggers intraday SL sweeps.
Paper live replay captures it because candle-by-candle SL evaluation uses OHLC intra-bar.

### Implication
The strategy's 09:15 entry window (first bar after open) is aggressive for SHORT. The SL at
`BC + ATR_buffer` is narrow enough that a 1-bar bounce can sweep it even on a genuine down-day.
LONG is less affected because up-gaps tend to hold in the first few bars more often than
down-gaps (buying-dip-fast is a common institutional pattern).

### Action
Backtest `entry_window_start_short=09:35`. See experiment above.

---

## 2026-04-22 — INFO: WebSocket watchdog auto-recovery validated in live session

**Status:** INFORMATIONAL — system behaved correctly; no fix needed
**Severity:** Low

### Incident (10:20 IST)
TCP connection dropped with error 1006 (`peer dropped TCP without WebSocket closing handshake`).
Feed had been degrading: `last_tick_age=170s`, `stale=22` before the drop.

### Recovery sequence
1. `KiteTicker` fired `on_close` → internal reconnect initiated
2. Watchdog fired ~21s post-drop → attempted reconnect, got `connect_failed` (Kite side not ready)
3. ~35s later: `KiteTicker` internal reconnect succeeded
4. Post-recovery: only subscribed to symbols with open positions (`active=10, subs=20`)
   — reduced subscription mode is correct behaviour, not a bug
5. Total gap: ~52s. Neither LONG nor SHORT had any position or SL/target events in the gap window.

### Key validation
- Both sessions recovered seamlessly with `connected=True, stale=0, coverage=100%` within 2 bars
- No orphaned positions, no double-triggers, no missed exits
- The `closes=1` counter accurately tracked the single disconnect event throughout the session

### Note for future debugging
`closes > 0` is a normal counter — it does not mean the session is unhealthy now.
Only `connected=False` or persistent `stale > 5` across multiple bars warrants action.

---

## 2026-04-22 — OBSERVATION: BREAKEVEN_SL commission drain on slow trending days

**Status:** PENDING EXPERIMENT — `breakeven_r=1.5` backtest queued (lower priority than `entry_window_start_short`)
**Severity:** Low

### Observation
Apr 22 session had 28 BREAKEVEN_SL closes (15 SHORT + 13 LONG) at ~-₹83 each = ~-₹2,324 in
commission drain. Each represents a position that moved to +1R (breakeven SL moved to entry)
then reversed back to entry. Net: paid commission on both legs with zero directional gain.

### Why it happens
The breakeven rule (`breakeven_r=1.0`) is aggressive — SL moves to entry as soon as +1R is
reached. On choppy mid-session bars the position oscillates between entry and +1R, eventually
stopping out at entry. This is by design: the alternative (not moving SL to entry) risks
converting a +1R position into a full SL loss if the move reverses sharply.

### Potential improvement
Test `breakeven_r=1.5` on the 15-month gold_51 window: does higher breakeven threshold reduce
commission drain while not materially increasing full-SL losses? The trade-off: fewer BREAKEVEN
exits but each breakeven failure costs more.

Command (future experiment, lower priority than `entry_window_start_short`):
```bash
doppler run -- uv run pivot-backtest \
  --universe-name gold_51 --start 2024-10-01 --end 2026-03-31 \
  --preset CPR_LEVELS_RISK_SHORT \
  --breakeven-r 1.5 --save
```

---

## 2026-04-22 — EXPERIMENT: Time-stop for slow-bleed INITIAL_SL trades

**Status:** PENDING — queue after `entry_window_start_short` experiment; gold_51 × 15-month window
**Severity:** Informational — strategy improvement candidate; reduces capital locked in dead trades

### Observation
Apr 22 live session had 3 trades that held the original INITIAL_SL for 30+ bars with no
meaningful progress, eventually stopping out for a full loss:
- BHAGYANGR: 37 bars at INITIAL_SL, −₹1,010
- ICIL: 44 bars at INITIAL_SL, −₹804
- GANECOS: 53 bars at INITIAL_SL, −₹946

These "slow-bleed" trades entered on a valid signal, never triggered breakeven (+1R), but also
never hit SL quickly. They tied up position slots for hours on stocks that effectively went flat
post-entry, eventually leaking to a loss via spread/commission or tiny adverse drift.

### Hypothesis
A time-stop rule — exit if price has not reached +0.5R within N bars after entry — would cut
these dead-money trades early and free the slot for other entries. The cost: occasionally exiting
a trade that would have eventually moved in the desired direction.

### Backtest Command
```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --universe-name gold_51 --start 2024-10-01 --end 2026-03-31 \
  --preset CPR_LEVELS_RISK_SHORT \
  --strategy-params '{"time_stop_bars": 12}' \
  --save --quiet --progress-file .tmp_logs/bt_timestop_12.jsonl
```

### Accept Criteria
- INITIAL_SL trade count reduces
- Calmar holds or improves vs baseline
- Trade count does not drop by more than 10% (time-stop exits do not reduce new entries)

---

## 2026-04-22 — EXPERIMENT: Momentum confirmation filter (early exit if no direction in bar 1)

**Status:** PENDING — queue after `entry_window_start_short` experiment; companion to time-stop above
**Severity:** Informational — strategy improvement candidate; addresses commission drain from rapid BREAKEVEN_SL losses

### Observation (from Apr 21 + Apr 22 paper data)

Queried all SHORT positions across both live days (52 trades):

**By entry bar — win rate:**
| Entry bar | Apr 21 trades | Apr 21 WR | Apr 22 trades | Apr 22 WR |
|-----------|---------------|-----------|---------------|-----------|
| 09:20 | 4 | 25% | 10 | 10% |
| 09:25 | 6 | 0% | 2 | 0% |
| 09:30 | 2 | 0% | 3 | 0% |
| 09:35 | 1 | 0% | 5 | 40% |
| 09:40+ | 9 | ~33% | 10 | ~30% |

The 09:20–09:30 cluster (27 of 52 trades, 52%) produced a combined ~7% win rate and accounted
for nearly all the session losses. Entries at 09:35+ had materially better outcomes on both days.

**Loss anatomy (Apr 21 + Apr 22 combined):**
- **BREAKEVEN_SL**: ~25 trades at −₹83 each ≈ −₹2,075 — rapid entry → first bar moves to +1R →
  immediately reverses, stopped at entry. Commission drain on zero directional progress.
- **INITIAL_SL (fast, 1–3 bars)**: ~15 trades at −₹250 to −₹700 — price moved hard against entry
  within 1–3 bars (gap reversal, morning auction bounce).
- **INITIAL_SL (slow bleed, 30+ bars)**: 4 trades (GANECOS −₹946, ICIL −₹804, INDOTHAI −₹541,
  SPLPETRO −₹590) — stock went flat post-entry, held all day, finally stopped out.

### Two distinct problems, two filters

**Problem 1 — Gap bounce SL sweep (fast, bars 1–3):** Market opens with SHORT setup, but a
recovery move in the first 1–2 bars sweeps the SL before price continues down. Fix: delay entry
window (`entry_window_start_short=09:35`) so the bounce has exhausted. Already queued above.

**Problem 2 — Dead-money entries (no momentum):** After entry, price neither confirms direction
(doesn't reach +0.5R within N bars) nor hits SL quickly. Stock drifts flat for 30–50 bars,
eventually leaking to a loss. Fix: time-stop or momentum confirmation exit. Queued above.

**Problem 3 — BREAKEVEN_SL commission drain (mid-speed, bars 1–10):** Entry confirms briefly
(moves to +1R, SL moves to entry) then reverses. Stopped at entry for −₹83 commission cost.
This is the dominant volume loss: 25 trades × −₹83 = −₹2,075 across 2 days.

### Momentum confirmation filter concept

After entry, if the next bar's close is **adverse** (i.e. moves against the trade direction
relative to entry), exit at the bar after that — do not wait for SL or breakeven.

Rule: `if close_bar1 < entry_price` (for SHORT: `close_bar1 > entry_price`), exit at
`open_bar2`. Rationale: a genuine short signal should have bar 1 closing below entry.
An adverse bar 1 close is the earliest signal that the entry was a false positive.

Alternative framing (threshold-based): exit at bar 2 open if favorable excursion in bar 1
did not reach a minimum of `+0.25R`. Requires less strict filtering than full reversal.

### Why this is distinct from time-stop

The time-stop checks momentum over N bars (12 bars = 1 hour) and exits if +0.5R not reached.
It targets Problem 2 (slow bleed over hours).

The momentum confirmation filter checks **bar 1 only** and exits within 10 minutes of entry.
It targets Problem 3 (BREAKEVEN_SL cluster) and the fast-SL-sweep trades from Problem 1
that are NOT caught by the delayed entry window.

### What a backtest test would look like

The momentum confirmation filter requires bar-level candle access after entry — the vectorised
backtest engine does not currently support intra-trade candle inspection after entry. It would
need to be implemented in the engine before a backtest can be run.

**Implementation sketch** (not yet built):
```python
# In the trade lifecycle, after entry bar:
if direction == "SHORT" and bar1_close > entry_price + 0.1 * atr:
    exit_at_bar2_open = True  # momentum did not confirm, exit early
```

### Priority and sequencing

1. Run `entry_window_start_short=09:35` backtest first (delayed window — no code changes needed)
2. Run time-stop backtest second (N=12 bars)
3. If both of those are insufficient, design and implement momentum confirmation filter

The momentum filter requires engine changes, so it is lowest priority. Document here for later.

---

## 2026-04-22 — EXPERIMENT: CPR_LEVELS `scale_out_pct=0.5` rerun on daily-reset baselines → REJECTED

**Status:** REJECTED — explicit `--cpr-scale-out-pct 0.5` reruns underperformed the current daily-reset baselines
**Severity:** Informational — exit-side hypothesis only; no engine bug

### Context
We reran only the two daily-reset CPR_LEVELS baselines with an explicit 50% scale-out override:

- LONG: `--strategy CPR_LEVELS --direction LONG --risk-based-sizing --min-price 50 --narrowing-filter --cpr-min-close-atr 0.5 --cpr-scale-out-pct 0.5`
- SHORT: `--strategy CPR_LEVELS --direction SHORT --risk-based-sizing --skip-rvol --min-price 50 --narrowing-filter --cpr-min-close-atr 0.5 --short-trail-atr-multiplier 1.25 --cpr-scale-out-pct 0.5`

The reruns were valid after the explicit-override path was used, but they did not improve the baseline set enough to keep as a candidate.

### Backtest Outcomes

| Side | Scale-out run | Baseline | Delta |
|---|---:|---:|---:|
| LONG | ₹891,256.62 | ₹992,934.74 | −₹101,678.12 |
| SHORT | ₹813,562.37 | ₹1,060,744.05 | −₹247,181.68 |

### Conclusion
Scale-out at `0.5` is knocked off for now. Keep `scale_out_pct = 0.0` in the canonical baselines unless a later experiment provides a materially better result.

---

## 2026-04-22 — ANALYSIS: ATR look-ahead claim — INCORRECT, reverted

**Status:** CLOSED — no code change needed; original `<=` join was correct
**Severity:** Informational — incorrect diagnosis investigated and retracted

### Claim (incorrect)
Hypothesis raised 2026-04-22: the ASOF JOIN `a.trade_date <= c.trade_date` in `market_day_state`
causes look-ahead bias because post-EOD it finds same-day ATR computed from same-day 5-min bars,
while live pre-market uses only prev-day ATR.

`<=` was briefly changed to `<` in `db/duckdb.py`. This was wrong and immediately reverted.

### Why the claim was wrong

`atr_intraday` is **forward-shifted at build time** (`db/duckdb.py` line 928):

```sql
LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS trade_date
SELECT symbol, trade_date, date AS prev_date, atr
```

The row keyed by `trade_date = T` stores ATR computed from `prev_date = T-1` candles. It does NOT
store same-day (T) ATR. So the `<=` join on `trade_date` is correct in all modes:

- **Pre-market live**: today's `atr_intraday[T]` row is present (built by yesterday's EOD); join lands on it — prev-day ATR ✓
- **Post-EOD backtest**: same `atr_intraday[T]` row is present; join lands on same row — same prev-day ATR ✓

Changing to `<` would skip the intended row and use `atr_intraday[T-1]` — ATR from T-2, an extra unintended lag.

### Actual parity risk (confirmed by existing ISSUES.md)
ATR is consistent across modes. The real divergence source is **first-bar OR values**:
- Live uses WebSocket-built 9:15/9:20 OHLC before EOD Parquet exists
- Replay/backtest after EOD use authoritative packed candles from `intraday_day_pack`
- Any binary filter that depends on `or_atr_5`, `or_close_5`, or direction is susceptible to this drift

---

## 2026-04-21 — BUG: `flatten` command drops alerts and leaves sessions at STOPPING/CANCELLED

**Status:** FIXED — `scripts/paper_trading.py`
**Severity:** Medium — positions close correctly but operator had no confirmation; sessions not COMPLETED

### Context
2026-04-21 early-close test: both live sessions were hard-killed (taskkill /F) then
`pivot-paper-trading flatten` was run on each session to close 8 remaining open positions.

### Bugs Found

**1. TRADE_CLOSED alerts not sent for MANUAL_FLATTEN positions**
`flatten_session_positions()` calls `_dispatch_alert(AlertType.TRADE_CLOSED, ...)` for each
position but the `_cmd_flatten` handler exits immediately after without calling
`maybe_shutdown_alert_dispatcher()`. The async dispatcher never flushes — all 8 alerts dropped.
Fix: call `await maybe_shutdown_alert_dispatcher()` at the end of `_cmd_flatten` in `scripts/paper_trading.py`.

**2. FLATTEN_EOD alert not sent**
Same root cause — the EOD summary is queued by `flatten_session_positions()` but the process
exits before the dispatcher flushes. Operator has no Telegram confirmation the session is done.
Fix: same as above — `maybe_shutdown_alert_dispatcher()` at end of `_cmd_flatten`.

**3. Sessions stuck at STOPPING/CANCELLED instead of COMPLETED**
`flatten_session_positions()` sets status to STOPPING but never transitions to COMPLETED.
Hard kill left LONG as ACTIVE → startup cleanup in the next `flatten` call marked it CANCELLED.
SHORT remains STOPPING permanently.
Fix: `_cmd_flatten` should call `complete_session(session_id)` after `flatten_session_positions()`.

### UX Gap: No single "close all and exit" command
Current early-close procedure requires 3 manual steps:
1. Find and kill the live process PID
2. `pivot-paper-trading flatten --session-id LONG`
3. `pivot-paper-trading flatten --session-id SHORT`

The delay executing these steps cost ~₹2,400 vs the projected close-now price (market moved against positions while researching the mechanism).

**Required:** Add `pivot-paper-trading flatten-all --trade-date today` command that:
- Flattens all ACTIVE/STOPPING sessions for the given trade date in one call
- Calls `maybe_shutdown_alert_dispatcher()` before exit
- Marks all sessions COMPLETED
- Works even while the live process is still running (or documents that kill is required first)

### Root Cause (found in fix)
`_cmd_flatten` did not call `register_session_start()` + `_start_alert_dispatcher()`. The alert
consumer task was never started, so `_dispatch_alert()` queued items that nothing processed.
`maybe_shutdown_alert_dispatcher()` drained an empty queue — alerts silently dropped.

### Files Changed
- `scripts/paper_trading.py` — `_cmd_flatten`: added `register_session_start()`, `_start_alert_dispatcher()`, `update_session_state(COMPLETED)`, `maybe_shutdown_alert_dispatcher()`
- `scripts/paper_trading.py` — added `flatten-all` subcommand + `_cmd_flatten_all()` with same dispatcher wiring

### Test Results (2026-04-21)
- Positions: all 8 closed correctly at last-known feed price ✅
- TRADE_CLOSED alerts: 0 of 8 delivered ❌
- FLATTEN_EOD alert: not sent ❌
- Session status: LONG=CANCELLED, SHORT=STOPPING (neither COMPLETED) ❌
- Final P&L: LONG +₹3,856 / SHORT −₹2,748 / Combined +₹1,108

---

## 2026-04-21 — ANALYSIS: Losing trade patterns — tight SLs, early entries, SHORT bias on up-day

**Status:** ANALYSIS COMPLETE — experiments run 2026-04-21; both Pattern 1 and 2 filters rejected by backtest
**Severity:** Informational — no live bug; trading outcome review for strategy refinement

### Session Summary
- 28 closed positions: LONG 16, SHORT 12
- **All 5 wins were LONG. Zero SHORT wins.**
- 14 INITIAL_SL hits, 9 BREAKEVEN_SL exits (−₹83 each), 5 TRAILING_SL wins

### Pattern 1: Tight SLs stopped by opening-range noise (strongest signal)

| Group | Avg SL distance | Min | Max |
|-------|----------------|-----|-----|
| Winners | 1.16% | 0.45% | 1.56% |
| Losers | 0.38% | 0.16% | 0.83% |

Winners had ~3× wider SLs than losers. Every trade with SL < 0.45% was stopped out.
NSE opening-range noise is ~0.3–0.5% on mid-caps — SLs inside that band have no survival room.
SL distance is determined by the CPR zone width + ATR buffer; naturally narrow CPR days produce the tightest SLs.

**Experiment**: add `min_sl_distance_pct = 0.5%` filter — skip entries where `|entry − SL| / entry < 0.005`.
Estimated impact: ~10 of today's 14 INITIAL_SL losses would have been skipped.

### Pattern 2: 64% of losses entered at 09:20–09:25 (first two bars)

9 of 14 INITIAL_SL hits entered at 09:20 or 09:25 — the most volatile period post-open.
All 5 winning trades entered at 09:30 or later.
CPR_LEVELS entry scan starts at 09:15, so first-bar TC/BC touches carry the highest noise-to-signal ratio.

**Experiment**: test `entry_window_start = 09:30` — defer first-bar entries.
Risk: may reduce trade count; backtest to confirm Calmar impact before applying.

### Pattern 3: SHORT side completely failed — market trended up all day

NIFTY climbed steadily from open. Every SHORT entry was against the day's trend:
- MANGLMCEM, GLAND, SAREGAMA, ADVANCE, RAMAPHO, BHEL → all INITIAL_SL
- 6 more SHORTs → BREAKEVEN_SL (brief move right, then reversed with market)

Direction is resolved at 09:15 from `or_close_5`. On a strongly trending day, the initial
9:15 bar can still print below BC (triggering SHORT direction) before the trend is established.

**Experiment**: add a Nifty trend gate — skip SHORT entries when Nifty is already up >0.3% from open
by 09:30. This is a market-regime filter, not an individual-stock filter.
Backtest required before any live use. Risk of data-mining; validate out-of-sample.
The same idea could be mirrored for LONG as a down-day gate in principle, but that is a separate
hypothesis. The current note is SHORT-only because the observed bleed happened on an up-day.

### Pattern 4: BREAKEVEN protection is working correctly — but reveals choppy session

9 trades reached +1R (BE trigger), then reversed to entry. The system behaved correctly.
This pattern indicates a **choppy, mean-reverting session** — initial momentum faded for most trades.
Indistinguishable from a real move in real-time; no change recommended here.

### Backtest Outcomes (SQL analysis on DR-Risk baselines, 2025-01-01 → 2026-04-21)

Baselines: LONG `c543038a648a` (3,165 trades, ₹992,935), SHORT `804f589a2fc7` (4,669 trades, ₹1,060,744)

#### Pattern 1 — min_sl_distance_pct = 0.5% → REJECTED

| | LONG | SHORT |
|---|---|---|
| Tight-SL trades (<0.5%) | 2,271 / 3,146 (72%) | 3,121 / 4,663 (67%) |
| Tight-SL P&L | ₹713,923 (72% of total) | ₹649,732 (61% of total) |
| Tight-SL WR | 36.0% | 32.6% |
| Wide-SL WR | 33.1% (lower!) | 35.4% |

**Result: Do NOT implement.** Tight-SL trades are the majority of the book and their WR matches or
exceeds wide-SL trades across 15 months. CPR_LEVELS intentionally trades narrow CPR days — tight
SLs are by design, not noise. The Apr 21 observation was a single choppy session, not a pattern.

#### Pattern 2 — entry_window_start = 09:30 → REJECTED

| Bar | LONG trades | LONG avg P&L | SHORT trades | SHORT avg P&L |
|---|---|---|---|---|
| 09:20 | 725 | **₹422** (best) | 1,891 | **₹289** (best) |
| 09:25 | 422 | ₹302 | 634 | ₹227 |
| 09:30 | 332 | ₹325 | 416 | ₹237 |

Skipping 09:20+09:25 would lose −43.6% of LONG P&L and −65.2% of SHORT P&L.
Even skipping 09:25 alone costs −12.8% LONG and −13.6% SHORT.

**Result: Do NOT implement.** First-bar entries (09:20) have the best average P&L across 15 months.
The Apr 21 pattern (early entries all losing) was a single strongly-trending up-day where SHORT
direction was simply wrong — not a structural noise problem with first-bar entries.

#### Pattern 3 — Nifty trend gate for SHORT / mirrored LONG gate → BACKTESTED

This was the next live hypothesis after the Apr 21 session review: use a broad market proxy to
skip new entries on strong one-sided index days rather than forcing an immediate liquidation rule.
The regime gate is still an entry filter only; it does **not** close open trades on reversal.

We first ran SQL sweeps on `NIFTY 500` daily moves from `2025-01-01` to `2026-04-21`:

| Threshold | Up days | Down days | Abs days |
|---|---:|---:|---:|
| 0.10% | 60 | 86 | 146 |
| 0.20% | 23 | 31 | 54 |
| 0.30% | 51 | 75 | 126 |
| 0.50% | 23 | 31 | 54 |
| 1.00% | 4 | 2 | 6 |
| 2.00% | 1 | 0 | 1 |
| 3.00% | 0 | 0 | 0 |

Takeaway: `1%+` is too sparse to be a useful day filter here. `2%` and `3%` are basically dead
thresholds for this dataset. The useful search space is closer to `0.3%`-`0.5%`.

We then ran the actual full-universe daily-reset risk baselines with
`--regime-index-symbol 'NIFTY 500' --regime-min-move-pct 0.5`:

| Side | Baseline run | Gate run | Trades | PnL | WR | PF | Calmar |
|---|---|---|---:|---:|---:|---:|---:|
| LONG | `c543038a648a` | `4655d9f75806` | 3165 → 3027 | ₹992,934.74 → ₹967,311.25 | 35.1% → 35.6% | 2.70 → 2.77 | 167.84 → 161.43 |
| SHORT | `804f589a2fc7` | `fc0ae028635c` | 4669 → 4459 | ₹1,060,744.05 → ₹1,089,065.18 | 33.5% → 34.4% | 2.12 → 2.24 | 72.83 → 79.13 |

### Conclusion
The symmetric `0.5%` gate helps SHORT and hurts LONG. That means NIFTY 500 is useful, but not as a
forced long/short mirror at the same threshold. The most plausible next deployment is SHORT-only
at `0.5%`, with LONG left ungated until a separate long-side threshold proves itself.

For live use, keep the gate as an extra opt-in parameter and do **not** turn it into a default
liquidation rule. If we want earlier influence on the first entry window, that should be a separate
snapshot-time experiment, not a reversal-close rule.

### Follow-up: early snapshots for SHORT at `0.5%`

Because most CPR trades enter at `09:20` and `09:25`, we tested the SHORT gate again using earlier
regime snapshots:

| Snapshot | Run ID | Trades | PnL | WR | PF | Calmar |
|---|---|---:|---:|---:|---:|---:|
| 09:20 | `6d5b0009115e` | 4614 | ₹1,058,358.25 | 33.66% | 2.13 | 75.66 |
| 09:25 | `619bb64aaa70` | 4605 | ₹1,058,921.19 | 33.72% | 2.13 | 75.78 |
| Baseline SHORT | `804f589a2fc7` | 4669 | ₹1,060,744.05 | 33.50% | 2.12 | 72.83 |

### Follow-up conclusion
The earlier snapshots improved the SHORT gate slightly versus the 09:30 version, but they still
did not beat the daily-reset SHORT baseline. Keep the market-direction gate disabled for today's
live session and revisit later only if we want another threshold/search pass.

#### Pattern 4 — BREAKEVEN protection → No change needed

Working as designed. Single-session observation only.

---

## 2026-04-21 — BUG: SESSION_STARTED alert never dispatched on daily-live startup

**Status:** FIXED — `engine/paper_runtime.py`, `scripts/paper_live.py`
**Severity:** Low — no trading impact; operator receives no confirmation that sessions are live

### Root Cause
`AlertType.SESSION_STARTED` was defined in the enum (alongside `SESSION_COMPLETED`,
`FEED_STALE`, etc.) but no dispatch function existed for it and no call site wired it.
Operators received `TRADE_OPENED` alerts at 09:20 as the first sign the session was alive,
with no upfront confirmation that both LONG and SHORT sessions had subscribed to the feed.

### Fix
1. Added `dispatch_session_started_alert()` to `engine/paper_runtime.py` following the
   same pattern as `dispatch_feed_stale_alert` / `dispatch_session_error_alert`.
2. Called it in `scripts/paper_live.py` inside `run_live_session()` immediately after
   `direction_filter` is resolved (after session is confirmed ACTIVE and symbols filtered),
   so the alert includes strategy, direction, symbol count, and trade date.

Alert format:
```
Subject : SESSION_STARTED CPR_LEVELS LONG 2026-04-21
Body    : Session: CPR_LEVELS_LONG-2026-04-21
          Strategy: CPR_LEVELS  Direction: LONG
          Symbols: 615  Date: 2026-04-21
```

### Files Changed
- `engine/paper_runtime.py` — `dispatch_session_started_alert()`
- `scripts/paper_live.py` — import + call after session goes ACTIVE

---

## 2026-04-21 — ENHANCEMENT: Dashboard shows no session until 09:16 IST

**Status:** FIXED — `scripts/paper_trading.py`, `db/paper_db.py`
**Severity:** Low — cosmetic; sessions appear as soon as the WebSocket subscribes

### Observation
When `daily-live --multi` starts before market open (e.g. 09:05 IST), it waits ~435s in
`_wait_until_market_ready()` before subscribing to Kite WebSocket. Session rows were only
written to `paper_sessions` inside `_ensure_daily_session()`, which is called *after* the
wait ends. The dashboard therefore showed no sessions for today until 09:16 IST.

### Fix Applied
Pre-create sessions with `status="PLANNING"` in `_cmd_daily_live_multi` before
`_wait_until_market_ready()`. Sessions transition PLANNING → ACTIVE automatically when
`run_live_session` starts (`if session.status != "ACTIVE": update to ACTIVE` at paper_live.py:766).
`get_active_sessions()` in `db/paper_db.py` updated to include PLANNING status so the dashboard
renders them immediately. Dashboard shows all zeros (no positions yet) with status="PLANNING".

### Files Changed
- `scripts/paper_trading.py` — pre-compute `variant_setup` and pre-create sessions with PLANNING
  before kite wait; skips pre-creation if session already exists (safe for resume/restart)
- `db/paper_db.py` — `get_active_sessions()` query now includes PLANNING in status filter

---

## 2026-04-21 — BUG: paper.duckdb startup lock from stale Python process

**Status:** FIXED — `db/paper_db.py`
**Severity:** Medium — blocks `daily-live` session start; requires manual kill

### Root Cause
On Windows, DuckDB acquires an exclusive write lock on `paper.duckdb` for the process
lifetime. Any stale Python process (failed startup, Codex test run, killed session that
was not reaped) that had the file open blocks the next `daily-live` start with an
`IOException`. The error message included the holding PID, but it was buried deep in the
traceback with no actionable guidance.

Observed: Codex ran a `daily-live` test as part of the ingestion improvement plan and left
PID 32004 running. Next morning's fresh start failed silently because PID 32004 held the
lock. Required manual `tasklist` → `taskkill` cycle before the session could start.

### Symptom
```
_duckdb.IOException: IO Error: Cannot open file "data/paper.duckdb": The process cannot
access the file because it is being used by another process.
File is already open in python.exe (PID XXXXX)
```

### Fix
`PaperDB.__init__` now wraps `duckdb.connect()` in a `try/except duckdb.IOException`.
On failure it calls `_diagnose_paper_db_lock()` which:
1. Extracts the holding PID and executable from DuckDB's own error text via regex
2. Runs a PowerShell one-liner to resolve the full command line for that PID
3. Prints a `[STARTUP BLOCKED]` banner with the exact `taskkill` fix command
4. Re-raises so the caller still sees the error (no silent swallow)

### Files Changed
- `db/paper_db.py` — `_diagnose_paper_db_lock()` + try/except in `PaperDB.__init__`

### Prevention
Before starting `daily-live`, kill any stale paper-trading processes:
```powershell
Get-CimInstance Win32_Process | Where-Object {$_.CommandLine -like "*pivot-paper-trading*"} | ForEach-Object { taskkill //F //PID $_.ProcessId }
```

---

## 2026-04-20 — BUG: Apr 20 parity drift across backtest, local-live, and live-kite

**Status:** INVESTIGATING
**Severity:** High — the same CPR_LEVELS preset does not produce the same symbol set, trade count,
or P/L across backtest, local-live, and live-kite

### Symptoms
On 2026-04-20 the archived runs diverged materially:

- Backtest SHORT (`b5da636ec81a`): 12 trades, `INR 5,168.24`
- Paper local-live SHORT (`CPR_LEVELS_SHORT-2026-04-20-live-local`): 14 trades, `INR 7,318.53`
- Paper live-kite SHORT (`CPR_LEVELS_SHORT-2026-04-20-live-kite`): 38 trades, `INR 461.00`
- Backtest LONG (`6eb4ea65763f`): 5 trades, `INR 1,261.50`
- Paper local-live LONG (`CPR_LEVELS_LONG-2026-04-20-live-local`): 5 trades, `INR 1,261.53`
- Paper live-kite LONG (`CPR_LEVELS_LONG-2026-04-20-live-kite`): 30 trades, `INR 13,520.00`

LONG is effectively in parity with local-live on the Apr 20 slice, but SHORT and live-kite drift
remain large.

### Evidence
- Backtest archived with a 2043-symbol universe for the Apr 20 run.
- The paper sessions were archived with a prefiltered `u844` universe.
- SHORT trade differences are mostly symbol-set drift:
  - Paper-local had `EXCELINDUS`, `HINDCOMPOS`, `THEINVEST`
  - Backtest had `VIYASH`
- Shared trade keys are mostly matched, and the mismatches are small on matching rows.
- Stored feed audit rows differ by source and bar window:
  - live-local LONG: 843 symbols, 4,776 rows, `09:15` -> `10:15`
  - live-kite LONG: 844 symbols, 4,975 rows, `09:20` -> `13:25`
  - live-local SHORT: 843 symbols, 7,995 rows, `09:20` -> `15:05`
  - live-kite SHORT: 844 symbols, 9,193 rows, `09:20` -> `15:15`

### Root Cause Hypothesis
1. The candidate universe is not frozen as a first-class artifact per trade date.
2. `pre_filter_symbols_for_strategy()` is shared, but the inputs are not identical across modes
   because the tradeable universe, CPR snapshot, and feed-source timing are resolved at runtime.
3. `live-kite` and `live-local` use different transports and bar-finalization timing, so the feed
   tape itself can diverge even when the strategy engine is shared.
4. The current compare flow validates output rows, but it does not enforce equality of the
   upstream universe manifest or feed-audit tape.

### Required Fix
- Define one deterministic symbol-resolution contract for a trade date and parameter bundle, then
  make backtest, replay, local-live, and live-kite call that same resolver.
- Record the resolver inputs and outputs for audit, but do not hand-maintain a separate symbol list.
- Compare feed-audit rows by `symbol + bar_end` as part of the parity gate.
- Treat differences in `feed_source`, `candle_interval`, resolution hash, or bar window as parity
  drift unless they are explicitly documented and accepted.
- Re-run Apr 20 comparisons after the manifest and feed-tape checks are in place.

---

## 2026-04-20 — BUG: Telegram 429 rate-limit drops alert permanently at burst open

**Status:** FIXED — `engine/alert_dispatcher.py`
**Severity:** Medium — 1 TRADE_OPENED alert lost (MVGJL SHORT); no trading impact

### Root Cause
At 09:25, the 9:25 bar fired a burst of ~20 alerts simultaneously (10 SL_HIT + new TRADE_OPENED
for both LONG and SHORT sessions). Telegram's bot API limit is **20 messages/minute per chat**.
MVGJL SHORT OPENED (alert_log ID 1042) was message 21, got a `429 Too Many Requests`.

The existing retry logic used fixed backoff: 1s → 2s → 4s (7s total). Telegram's response body
specified `retry_after: 26` — all 3 retries fired within the cooldown window and also got 429.
Alert was permanently dropped (`status=failed`, `channel=BOTH`).

### Fix
`AlertDispatcher._send_with_retry` now extracts the server-specified wait from the 429 response:
1. Checks `response.json()["parameters"]["retry_after"]` (Telegram-specific field)
2. Falls back to standard `Retry-After` HTTP header
3. If found, sleeps exactly that duration before retry; logs a WARNING with the wait time
4. If not found, falls back to the existing fixed `RETRY_BACKOFF`

### Files Changed
- `engine/alert_dispatcher.py` — `_send_with_retry` + `_telegram_retry_after` static helper

---

## 2026-04-20 — BUG: Duplicate FLATTEN_EOD Telegram alert at EOD

**Status:** FIXED — `engine/paper_runtime.py`
**Severity:** Medium — double Telegram noise, no trading impact

### Root Cause
`flatten_session_positions` deduped via `SELECT COUNT(*) FROM alert_log WHERE alert_type='FLATTEN_EOD'`.
`_dispatch_alert` is fire-and-forget — `log_alert` writes to `alert_log` AFTER the Telegram POST
completes, inside the async consumer loop. Any two concurrent (or near-simultaneous) callers both
see 0 before either write commits, so both dispatch.

On 2026-04-20, `CPR_LEVELS_SHORT` had EMKAY close at TARGET at 15:10 as the last open position.
The session exit triggered `flatten_session_positions` with the dedup window still open, resulting
in two FLATTEN_EOD items queued (alert_log IDs 1157 and 1158, sent 360ms apart).

### Fix
Replaced DB dedup with a module-level `_flatten_eod_sent: set[str]` in `paper_runtime.py`.
The set is updated synchronously (`_flatten_eod_sent.add(session_id)`) before `_dispatch_alert`
is called, making it immune to async timing. `_flatten_eod_sent` persists for the process lifetime
so cross-session dedup also works (e.g., resume sessions).

---

## 2026-04-20 — FEATURE: Progressive trail stop between 1R and T1 (pre-target ratchet)

**Status:** REJECTED — single-rung ratchet (`--trail-after-r 1.5`) tested on retained baselines 2026-04-21: SHORT −₹218K, LONG −₹15K. See `docs/PROGRESSIVE_TRAIL_RATCHET_PLAN.md`.
**Severity:** Enhancement — prevents giving back unrealised gains when position stalls before target

### Problem
Current trail design has a dead zone between BREAKEVEN (1R) and TARGET (T1):
- At 1R: SL moves to entry (scratch protection only)
- At T1: ATR trail begins
- **Between 1R and T1: SL sits flat at entry regardless of how far price has moved**

Example (2026-04-20 EMKAY SHORT):
- Entry 241.16, SL 243.44, Target 232.15 (3.93R away)
- Price reached 237.7 (+1.51R, unrealised +₹1,428)
- If price reverses to entry → BREAKEVEN_SL fires → -₹83 scratch
- Position gave back entire ₹1,428 unrealised gain for a scratch exit

On days where SHORT entries move 1–1.5R but never reach T1, every trade ends as a scratch
or initial SL. The trail never activates because the target is too far. LONG avoids this
on good days because T1 gets hit early and the ATR trail captures the extension.

### Proposed Solution: ratchet trail after 1R

Add `trail_after_r` parameter — once position reaches N×R, start trailing at 1×ATR from
best price immediately (instead of waiting for T1):

```
Entry 241.16, risk = 2.28/share
  1.0R (238.88) → SL to entry (current — BREAKEVEN)
  1.5R (237.72) → SL to +0.5R locked  ← new ratchet step
  2.0R (236.56) → SL to +1.0R locked  ← new ratchet step
  T1 hit        → ATR trail (current behaviour continues)
```

### Implementation Plan
1. **SQL analysis first**: query retained baselines and estimate the P&L delta under candidate
   ratchet levels.
2. **Backtest flag**: `--trail-after-r 1.5` on `pivot-backtest` (default disabled).
3. **Sweep**: test 1.0 / 1.25 / 1.5 / 2.0 thresholds. Check Calmar vs baseline.
4. **Paper parity**: add to both engines before enabling in live.

### Backtest analysis results

Retained SHORT baseline `b5da636ec81a`:
- Baseline: `4,663` trades, `INR 10,59,120.49`, win rate `33.5%`, Calmar `72.85`
- `1.25R` ratchet: `INR 14,82,238.20` (`+INR 4,23,117.71`), win rate `71.9%`, Calmar `132.58`
- `1.50R` ratchet: `INR 15,16,694.71` (`+INR 4,57,574.22`), win rate `65.0%`, Calmar `144.19`
- `1.75R` ratchet: `INR 15,10,311.90` (`+INR 4,51,191.41`), win rate `58.9%`, Calmar `134.25`
- `2.00R` ratchet: `INR 15,03,583.80` (`+INR 4,44,463.31`), win rate `54.3%`, Calmar `139.81`

Retained LONG baseline `6eb4ea65763f`:
- Baseline: `3,146` trades, `INR 9,92,761.59`, win rate `35.2%`, Calmar `155.01`
- `1.25R` ratchet: `INR 13,00,600.11` (`+INR 3,07,838.52`), win rate `75.9%`, Calmar `549.21`
- `1.50R` ratchet: `INR 13,37,541.16` (`+INR 3,44,779.57`), win rate `70.0%`, Calmar `534.53`
- `1.75R` ratchet: `INR 13,47,552.80` (`+INR 3,54,791.21`), win rate `64.2%`, Calmar `535.64`
- `2.00R` ratchet: `INR 13,36,494.18` (`+INR 3,43,732.59`), win rate `58.7%`, Calmar `500.49`

Combined ladder policy `1.25R -> 1.50R -> 1.75R -> 2.00R -> ATR`:
- SHORT: `INR 12,28,167.76` (`+INR 1,69,047.27`), win rate `72.0%`, Calmar `179.46`
- LONG: `INR 13,11,379.06` (`+INR 3,18,617.47`), win rate `75.9%`, Calmar `615.46`

### Interpretation
- The ratchet is not just a SHORT-only fix; it also materially improves the LONG baseline.
- `1.50R` remains the best SHORT P/L candidate, while `1.75R` is the best LONG P/L candidate.
- Across both retained baselines combined, `1.75R` is the best raw P/L rung by a small margin.
- `1.25R` has the best profit factor on both baselines if we care more about win quality than raw P/L.
- The full ladder is the actual feature, and it improves both retained baselines while keeping the
  ATR phase intact after `2.0R`.
- This analysis is still counterfactual. It uses archived trade rows plus the stored market tape, but it is not an engine implementation.

### Notes
- Only affects trades that pass BREAKEVEN but don't reach T1 (the "stalled" trades)
- Risk: cutting winners short if 1.5R dip is just noise before the big move to T1
- Asymmetric impact: SHORT benefits more than LONG on range-bound days
- Pairs well with the 10:15 win-rate checkpoint (complementary, not redundant)

---

## 2026-04-20 — FEATURE: 10:15 intraday win-rate checkpoint to flatten losing sessions early

**Status:** REJECTED — regresses SHORT baseline at all thresholds tested; marginal LONG uplift not worth the added complexity
**Severity:** Enhancement — reduces drawdown on bad market days

### Problem
On bearish days (like 2026-04-20), the SHORT session opens many trades that go against it.
By 10:15 (entry window close), the session's intraday win rate is already below its
long-run baseline (~33%). Remaining open positions have unrealized profit but keep bleeding
as the market whipsaws. Continuing to 15:15 turns a small loss into a large one.

Simply closing all sessions at 10:15 is too blunt — on a good day (LONG today), the
trailing phase (10:15→15:15) generates the biggest winners (INTERARCH +₹2,326, THERMAX
+₹1,652). We need a per-session, market-adaptive signal.

### Proposed Solution: intraday win-rate checkpoint

At 10:15 (entry window close), independently evaluate each session:

```
intraday_win_rate = winning_closed_trades / total_closed_trades (up to 10:15)

if intraday_win_rate < checkpoint_winrate_threshold:
    flatten all remaining open positions at current market price
    emit CHECKPOINT_FLATTEN alert
else:
    continue to 15:15 TIME_EXIT as normal
```

**Why win rate vs rupee threshold:**
- Win rate is relative to the strategy's known edge — it's market-condition adaptive
- A rupee threshold is position-size dependent (breaks with risk-based sizing)
- Win rate < baseline (33%) = "strategy edge not present today" = clear exit signal
- Each session (LONG, SHORT) evaluated independently — good session continues, bad exits

### Example (2026-04-20 SHORT at 10:15):
- 32 closed trades, win rate likely ~20-25% (several INITIAL_SL hits vs targets)
- Threshold = 30% → flatten 6 remaining open positions at ~10:15 price
- Locks unrealized +₹1,954 instead of watching it bleed to 15:15

### Implementation Plan
1. **SQL analysis first** (no code changes): query `backtest_results` for existing baseline
   run, simulate checkpoint at different thresholds (0.20, 0.25, 0.30, 0.33, 0.40).
   Compare Calmar vs baseline. Do SHORT first (today's pain), then LONG.

2. **Engine flag** (if SQL shows Calmar improvement > 10%):
   - `--checkpoint-winrate-threshold 0.30` on both `pivot-backtest` and `daily-live`
   - In `paper_session_driver.py`: `on_entry_window_close()` hook
   - In backtest engine: mid-day session-level aggregation at 10:15 bar
   - Must be added to BOTH engines for paper parity

3. **Sweep** with `pivot-sweep` YAML across threshold values per direction.

### Notes
- Default = disabled (current behavior preserved)
- Backtest parity required before enabling in live — do not add paper-only
- Only valid at entry window close (10:15) — not a rolling intraday check

### Backtest analysis results

Retained SHORT baseline `b5da636ec81a`:
- Baseline: `4,663` trades, `INR 10,59,120.49`, win rate `33.5%`, Calmar `72.85`
- Threshold `0.20`: `INR 10,17,894.58` (`-INR 41,225.91`)
- Threshold `0.25`: `INR 10,06,680.35` (`-INR 52,440.14`)
- Threshold `0.30`: `INR 9,84,806.00` (`-INR 74,314.49`)
- Threshold `0.33`: `INR 9,81,797.50` (`-INR 77,322.99`)
- Threshold `0.40`: `INR 9,69,820.02` (`-INR 89,300.47`)
- Result: every tested threshold regressed profit on the retained SHORT baseline.

Retained LONG baseline `6eb4ea65763f`:
- Baseline: `3,146` trades, `INR 9,92,761.59`, win rate `35.2%`, Calmar `155.01`
- Threshold `0.20`: `INR 10,12,523.73` (`+INR 19,762.14`)
- Threshold `0.25`: `INR 10,14,938.82` (`+INR 22,177.23`)
- Threshold `0.30`: `INR 9,96,853.27` (`+INR 4,091.68`)
- Threshold `0.33`: `INR 10,00,576.54` (`+INR 7,814.95`)
- Threshold `0.40`: `INR 10,18,446.01` (`+INR 25,684.42`)
- Result: the checkpoint is mildly positive on the retained LONG baseline, but it is not a SHORT fix.

### Interpretation
- The checkpoint should not be prioritized as a generic profit fix for the current SHORT bleed.
- If we revisit it later, it should be framed as a possible LONG-side drawdown control with tighter gating, not a universal rule.

---

## 2026-04-20 — `get_stats` AttributeError crashes both sessions at market open

**Status:** FIXED (same day, ~9:18 AM)
**Severity:** Critical — both LONG and SHORT sessions crashed on first candle bar

### Symptom
Both CPR_LEVELS LONG and SHORT sessions connected to WebSocket successfully at 9:16 AM,
processed the first Stage B direction filter, then immediately crashed with:
```
AttributeError: 'KiteTickerAdapter' object has no attribute 'get_stats'.
Did you mean: 'health_stats'?
```
Both variants entered the retry loop (2/5, 3/5...) but all retries failed with the same error.
Sessions were in ACTIVE state in paper.duckdb but not processing any bars.

### Root Cause
`scripts/paper_live.py:1168` called `ticker_adapter.get_stats()` inside the zombie-check
block (stale WebSocket detection). The method was renamed to `health_stats()` in
`engine/kite_ticker_adapter.py` but the call site in `paper_live.py` was not updated.

```python
# Before (broken):
tick_age = (ticker_adapter.get_stats() or {}).get("last_tick_age_sec") or 0
# After (fixed):
tick_age = (ticker_adapter.health_stats() or {}).get("last_tick_age_sec") or 0
```

### Fix Applied
`scripts/paper_live.py:1168`: `get_stats()` → `health_stats()`.
Session restarted at 9:18 AM, both variants reconnected, caught the 9:20 entry bar.

### Impact
9:15 and first half of 9:20 bar missed. No trades lost — entry window runs to 10:15 AM
and the 9:20 bar was caught in full after restart.

---

## 2026-04-20 — Daily startup confusion: separate LONG/SHORT processes fail on Windows

**Status:** Documented (process issue, not code bug)
**Severity:** Operational — wasted ~15 minutes before market open

### Symptom
Attempting to start two separate `daily-live` processes (one `--preset CPR_LEVELS_RISK_LONG`,
one `--preset CPR_LEVELS_RISK_SHORT`) fails: the second process crashes immediately with
`IOException: paper.duckdb is already open by PID N`.

### Root Cause
DuckDB on Windows uses OS-level exclusive file locking. Two separate `daily-live` processes
both try to open `paper.duckdb` in read-write mode. The second process always fails.

### Fix / Canonical Command
Always use `--multi` — runs both LONG and SHORT in one process with a single DB writer.
`--multi` uses `PAPER_STANDARD_MATRIX` which hardcodes `CPR_CANONICAL_PARAMS` (= RISK sizing).
`--risk-based-sizing` flag is redundant with `--multi` but harmless.

```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS --trade-date today --all-symbols \
  >> .tmp_logs/live_YYYYMMDD.log 2>&1
```

---

## 2026-04-16 — OR fields NULL→0.0 kills all entry evaluations

**Status:** FIXED (same day)
**Severity:** Critical — 0 trades for entire session

### Symptom
Both CPR_LEVELS LONG and SHORT sessions ran for 6 hours with 0 trades. No errors
or warnings in the log. Entry window closed with `NO_TRADES_ENTRY_WINDOW_CLOSED`.

### Root Cause
`market_day_state` is built pre-market (before the 9:15 candle exists). Three OR
OHLCV fields are always NULL at session start: `or_high_5`, `or_low_5`, `open_915`.

`_hydrate_setup_row_from_market_row` coerced these nulls to `0.0` via
`float(row[N] or 0.0)`. The live direction fallback (added after Apr 15) only
updated `direction` and `or_close_5` from the live 9:15 candle but never touched
the OR fields — they stayed at 0.0.

In `find_cpr_levels_entry` (`engine/cpr_atr_shared.py`), the first filter is:
```python
or_atr_ratio = (0.0 - 0.0) / atr = 0.0
if or_atr_ratio < 0.3:  # True → OR_ATR_RATIO rejection
```
Every symbol, every bar, silently rejected. `OR_ATR_RATIO` is a legitimate filter
code for genuinely tiny ORs so the code never warned.

### Fix Applied
`_hydrate_setup_row_from_market_row` now captures `live_intraday` from
`_build_intraday_summary(live_candles)` and backfills OR fields when DB values are 0:
```python
or_high_5 = _db_or_high or float(live_intraday.get("or_high_5") or 0.0)
or_low_5  = _db_or_low  or float(live_intraday.get("or_low_5")  or 0.0)
open_915  = _db_open    or float(live_intraday.get("open_915")   or 0.0)
```

### Files Changed
- `engine/paper_runtime.py` — `_hydrate_setup_row_from_market_row` (~line 903)

---

## 2026-04-16 — Zombie WebSocket causes early flatten (12:20 instead of 3:30 EOD)

**Status:** FIXED
**Severity:** Medium — positions closed early, session data valid but incomplete

### Symptom
Both sessions auto-flattened at 12:19 with 1 open position each. Feed had been
silent for 1685s (28 min) before termination, but no FEED_STALE alert was sent.

### Root Cause
The stale detection in `scripts/paper_live.py:1151` uses a 4× multiplier when the
WebSocket socket is connected:
```python
if ticker_adapter.is_connected:
    stale = elapsed > max(stale_timeout * 4, 120)  # very lenient
else:
    stale = True
```
NSE ticks stopped at ~11:51 AM but the Kite TCP socket stayed alive (no close event
from the exchange). While `is_connected=True`, the session tolerated unlimited tick
silence. At ~12:17 Kite finally closed the socket (`closes=2`). The stale check
then found 1685s elapsed and terminated immediately — no chance to send FEED_STALE
or recover.

### Proposed Fix
Cap the connected-session multiplier using `last_tick_age_sec`. A socket that is
connected but has delivered no ticks for 5 minutes is a zombie connection:

```python
# scripts/paper_live.py ~line 1151
if ticker_adapter.is_connected:
    tick_age = (ticker_adapter.get_stats() or {}).get("last_tick_age_sec") or 0
    if tick_age > 300:
        stale = elapsed > stale_timeout          # treat as disconnected
    else:
        stale = elapsed > max(stale_timeout * 4, 120)
```

Minimal one-line version — cap the multiplied threshold at 300s:
```python
if ticker_adapter.is_connected:
    stale = elapsed > min(max(stale_timeout * 4, 120), 300)
```

### Files Changed
- `scripts/paper_live.py` — stale detection block (~line 1151): added `tick_age > 300` zombie check

---

## 2026-04-16 — Dashboard LONG shows doubled trades and PnL (22 trades, ₹1,762)

**Status:** FIXED (data corrected + dedup guard added)
**Severity:** High — dashboard showed factually wrong numbers

### Symptom
Dashboard "Archived Sessions" showed LONG as 22 trades / ₹1,762 P&L.
Correct values: 11 trades / ₹881.24.
SHORT was correct (12 trades / −₹2,174).

### Root Cause
`store_backtest_results` in `db/backtest_db.py` is a plain `INSERT INTO` with no
deduplication. When `archive_completed_session` is called twice for the same PAPER
session, all rows are doubled. LONG was archived twice; SHORT happened to be
archived once only.

Backtest run_ids are UUIDs (unique per run), so double-archiving is impossible for
them. But PAPER run_ids are session_ids (fixed strings like
`CPR_LEVELS_LONG-2026-04-16`), so a second call silently duplicates all rows.

### Fix Applied
1. **Data corrected** — deleted duplicate rows and re-archived both sessions cleanly:
   - LONG: 11 trades, +₹881.24 ✓
   - SHORT: 12 trades, −₹2,174.40 ✓

2. **Dedup guard added** in `db/backtest_db.py store_backtest_results`:
   When `execution_mode == 'PAPER'`, delete existing rows for that `run_id` before
   inserting, making re-archive idempotent. Backtest runs are unaffected (UUID
   run_ids never collide).

### Files Changed
- `db/backtest_db.py` — `store_backtest_results` (~line 446)

---

## 2026-04-16 — FAILED sessions not archived, missing from dashboard dropdown

**Status:** FIXED (manually archived same day)
**Severity:** Low — data intact in paper.duckdb, just not visible in dashboard

### Symptom
Dashboard "Archived Sessions" dropdown only showed zero-trade restarted sessions.
The original FAILED sessions (23 trades total) were invisible.

### Root Cause
`archive_completed_session` is only triggered at normal COMPLETED exit. FAILED exits
(stale, crash) do not archive. The dashboard reads from `backtest.duckdb`
(`run_metadata` with `execution_mode='PAPER'`) so FAILED sessions never appeared.

### Fix Applied
Manually called `archive_completed_session` for both FAILED sessions:
- `CPR_LEVELS_LONG-2026-04-16` → 11 trades archived
- `CPR_LEVELS_SHORT-2026-04-16` → 12 trades archived

### Longer-term Fix Applied
`paper_live.py` now archives on both COMPLETED and FAILED terminal status.
`store_backtest_results` has PAPER dedup guard so re-archiving is idempotent.

### Files Changed
- `scripts/paper_live.py` — archive condition `status in ("COMPLETED", "FAILED")`

---

## 2026-04-16 — FLATTEN_EOD alert shows wrong PnL and trade count

**Status:** FIXED
**Severity:** High — Telegram EOD summary was factually incorrect (sign wrong on SHORT)

### Symptom
Telegram FLATTEN_EOD alert showed only the last auto-flattened position's PnL, not
the full session PnL. Trade count was also inflated by 1 (double-counted the
force-flattened position).

| Session | Telegram (wrong) | DB / Dashboard (correct) |
|---------|-----------------|--------------------------|
| LONG | +₹463.12, 12 trades | +₹881.24, 11 trades |
| SHORT | +₹496.22, 13 trades | −₹2,174.40, 12 trades |

The SHORT session actually had a net loss but the alert showed a gain.

### Root Cause
In `flatten_session_positions` (`engine/paper_runtime.py`), after force-closing open
positions via `update_position(status="CLOSED")`, the code re-fetched all closed
positions into `all_closed`. Because `update_position` commits before the re-fetch,
the just-flattened positions are already included in `all_closed`. But the code:
1. Added `len(closed)` again → double-counted the flattened positions
2. Only summed `total_realized` for force-closed positions; the `if not closed`
   guard meant already-closed positions were never included when a force-close ran

```python
# Bug:
total_trades = len(all_closed) + len(closed)   # double-count
if not closed and all_closed:                  # never true when force-close ran
    total_realized = sum(...)
```

### Fix Applied
```python
# Fix in engine/paper_runtime.py ~line 1325:
all_closed = await get_session_positions(session_id, statuses=["CLOSED"])
total_trades = len(all_closed)
total_realized = sum(float(p.realized_pnl or 0) for p in all_closed)
```

### Files Changed
- `engine/paper_runtime.py` — `flatten_session_positions` (~line 1325)

---

## 2026-04-16 — Double EOD summary (LONG + SHORT x 2)

**Status:** FIXED
**Severity:** Low

### Symptom
Four EOD alerts sent: two from FAILED original sessions (FLATTEN_EOD with position
PnL), two from restarted sessions (SESSION_COMPLETED with 0 trades).

### Root Cause
`--multi` auto-restarts failed variants with a new `session_id` suffix. Restarted
sessions start after 10:15, find entry window closed, exit immediately, and send
their own SESSION_COMPLETED alert.

### Fix Applied
Inside `flatten_session_positions`: if `total_trades == 0 and not closed`, skip
the FLATTEN_EOD dispatch entirely — zero-trade restart sessions have nothing to
report. Also fixed the dedup check to match on `body LIKE session_id[:24]` instead
of subject (subject is date-only and doesn't contain the session_id).

### Files Changed
- `engine/paper_runtime.py` — `flatten_session_positions` dedup + zero-trade guard

---

## 2026-04-16 — Apr 15 Telegram alerts all failed (error_msg='None')

**Status:** CLOSED — root cause was missing Doppler secret (operational); AlertDispatcher startup logging added to make this detectable going forward
**Severity:** Medium — no operational impact on Apr 15 but alert system is critical

### Symptom
All `alert_log` rows for 2026-04-15 sessions have `status='failed'` and
`error_msg='None'` (the string, not SQL NULL). Zero Telegram messages received.

### Root Cause Analysis
`error_msg = str(last_error) if status == "failed" else None`

`str(None) = 'None'` means `last_error` was never assigned — i.e., no send was
attempted at all. This only happens when `telegram.enabled = False` AND
`email.enabled = False`. `telegram.enabled = bool(bot_token and chat_ids)`, so
either `TELEGRAM_BOT_TOKEN` or `TELEGRAM_CHAT_IDS` was empty/missing at session
start on Apr 15.

Note: changing the chat ID to an invalid value would produce a Telegram API error
exception, not a silent no-attempt. `error_msg='None'` rules out a wrong-chat-ID
scenario.

### Current State
Apr 16 restart (12:20) used updated Doppler config — all 15 Telegram sends returned
HTTP 200. Alert system confirmed working.

### Fix Applied
`AlertDispatcher.start()` now logs:
```
AlertDispatcher started: telegram.enabled=True chat_ids=1 email.enabled=False
```
This makes alert config visible in the log at session start for post-mortem diagnosis.

### Files Changed
- `engine/alert_dispatcher.py` — `start()` method

---

## 2026-04-15 — Direction resolution race — SHORT died at 09:25 with NO_ACTIVE_SYMBOLS

**Status:** FIXED (before 2026-04-16 session)
**Severity:** Critical — SHORT session lost entire day

### Summary
`CPR_LEVELS_SHORT-2026-04-15` completed with `NO_ACTIVE_SYMBOLS` at 09:25.
LONG survived by coincidence (5 early-tick resolutions all happened to be LONG).

### Root Cause
`strategy_day_state` had `direction_5 = 'NONE'` for all 2095 symbols — pre-market
state build did not populate directions. At session prefetch time (09:20:29-30,
after three crashes and restarts), Kite WebSocket ticks were still sparse:
- 5 symbols resolved (all LONG from closed-above-TC)
- 0 symbols resolved SHORT
- 592 symbols stayed NONE → rejected by `should_process_symbol`

The session had been started before the pre-market WebSocket segment flip
(09:10 → 09:15), causing sparse ticks at direction-resolution time.

### Fix Applied
`MARKET_READY_HHMM = "09:16"` auto-sleep in `scripts/paper_trading.py:1051`.
Sessions started before 09:16 now wait until exactly 09:16, after the segment flip,
ensuring the 9:15 candle is closed before direction resolution runs.

### Remaining Open Actions (from original incident log)
1. **Direction-readiness gate** — delay Stage B until coverage ≥80% or bar 09:25
2. **EOD pipeline** — guarantee `strategy_day_state` for T+1 is built with ATR
   every EOD (never left to morning-of refresh)
3. **Kite WebSocket auto-reconnect** — `_on_close(None, None)` silently clears
   `_connected`. Add reconnect with backoff, per-minute health log, tick-coverage
   alerts (`engine/kite_ticker_adapter.py:328-330`)
4. **DQ readiness gate** — extend `pivot-data-quality --date today` to verify
   `strategy_day_state` direction coverage, not just table presence

### Related Files
- `engine/paper_runtime.py:818-833` — direction fallback chain
- `engine/paper_session_driver.py:44-86` — Stage B filter
- `engine/paper_session_driver.py:250-263` — Step 5 prune (NONE → rejected)
- `engine/kite_ticker_adapter.py:328-330` — silent `_on_close`
- `scripts/paper_live.py:922-940` — NO_ACTIVE_SYMBOLS exit path

---

## 2026-04-13 — MANOMAY live vs replay fill divergence

**Status:** CLOSED — architectural; root cause fully documented in 2026-04-15/16 OHLC drift entry. Residual ~15–25% OHLC divergence between WebSocket (MODE_QUOTE) and historical pack is inherent — not fixable without polling Kite historical candles.
**Severity:** High — same symbol, same session, different PnL outcome

### Summary
`MANOMAY` SHORT on 2026-04-13:
- Live session alert: `221.49 → 216.31`, TARGET, `+₹2,253`
- Replay/local-backtest after cleanup: `220.90 → 220.90`, BREAKEVEN_SL, `-₹83.49`

### Root Cause Hypothesis
Live WebSocket path builds 5-min candles from real ticks. Replay/local-live use
candles from `intraday_day_pack` (EOD-built). The historical pack does not
preserve the exact live fill snapshot, so fill prices and bar shapes can diverge.
A single-bar fill drift is enough to flip a TARGET win into a BREAKEVEN loss.

### Fix Applied
`paper_feed_audit` table now stores one compact row per
`session_id + symbol + 5-min bucket`. Compare via:
```bash
pivot-paper-trading feed-audit --trade-date YYYY-MM-DD --feed-source kite
```

### Pending Analysis
- Verify whether WebSocket path falls back to receive-time when
  `exchange_timestamp` is missing or late
- Confirm live volume reconstruction from cumulative `volume_traded` is correct
- Decide whether feed-audit tolerates OHLC drift or whether the live candle
  builder needs a stronger timestamp contract before trusting live-vs-pack parity
- 2026-04-15 audit: large real drift vs `intraday_day_pack` confirmed, mostly in
  volume with some OHLC mismatches (after fixing the bar_start vs bar_end join key)

---

## 2026-04-16 — paper_positions.exit_reason is NULL for all positions

**Status:** FIXED
**Severity:** Low — data still usable but exit reason audit was missing

### Symptom
All 23 positions from today's LONG and SHORT sessions have `exit_reason=None` in
`paper_positions`. Exit reasons (SL_HIT, TARGET_HIT, TRAIL_STOP, MANUAL_FLATTEN,
BREAKEVEN_SL) were not being persisted to the DB column.

### Root Cause
`update_position` in `db/paper_db.py` accepts two separate keyword arguments:
`exit_reason` (writes `exit_reason` column) and `closed_by` (writes `closed_by` column).
All four close call-sites in `engine/paper_runtime.py` only passed `closed_by=exit_reason`
— the `exit_reason=` keyword was never passed, so the column always stayed NULL.

The `paper_positions` schema has both columns (`exit_reason VARCHAR(50)` and
`closed_by VARCHAR(50)`) — both exist, but only `closed_by` was being populated.

### Fix Applied
Added `exit_reason=<reason>` to all four `update_position` close call-sites:
1. Manual flatten path (`closed_by="MANUAL_FLATTEN"`) — `engine/paper_runtime.py` ~line 1280
2. SL hit path (`closed_by=exit_reason`) — ~line 1717
3. Scale-out TARGET path (`closed_by="TARGET"`) — ~line 1792
4. Resolved exit path (`closed_by=resolved_exit_reason`) — ~line 1930

### Files Changed
- `engine/paper_runtime.py` — four `update_position` call-sites

---

## 2026-04-15 / 2026-04-16 — Live candle OHLCV diverges from intraday_day_pack

**Status:** ROOT CAUSE CONFIRMED — volume bug fixed, residual OHLC drift is architectural
**Severity:** Medium — volume bug caused false mismatches; residual OHLC drift is inherent and
understood. Does not affect SL/target monitoring (uses LTP directly). May cause occasional
entry fill differences vs replay.

### Symptom

Apr 15 feed audit (`pivot-paper-trading feed-audit --trade-date 2026-04-15 --feed-source kite`):
- Total rows: 2433 | Exact matches: 55 | Unmatched: 2342
- Field mismatches: open=1391, high=1262, low=1278, close=1548, volume=2333
- Volume virtually always wrong; OHLC also largely wrong

Apr 16 feed audit (bar-by-bar breakdown):

| Bar | Syms | PriceOK% | VolOK% | Vol Inflated |
|-----|------|----------|--------|--------------|
| 10:10 (first — session started at 10:07, mid-bar) | 1573 | 20% | 0% | 89% |
| 10:15 (first complete bar) | 1596 | 41% | 33% | ~0% |
| 10:20 (steady state begins) | 880 | 76% | 86% | 0% |
| 10:25+ (steady state) | ~877 | 20–100% | 10–100% | 0% |
| 12:25 (flush on stale terminate) | 1581 | 74% | 0% | 87% |

### Root Cause — Two Separate Problems

#### Problem 1: First-tick volume inflation (BUG — now FIXED)

`volume_traded` from Kite WebSocket is cumulative since market open. When the session
connects (e.g. at 10:07), the first tick for each symbol has `prev = None` so the delta
computation fell back to `max(0.0, cumulative)` — attributing ALL pre-session volume
(from 9:15 to 10:07) to the first live bar. This produced volumes 10–1000× higher than
the historical pack. Accounts for the vast majority of Apr 15 volume mismatches (2333/2433).

Same inflation happens at session terminate: flushed partial bars inherit stale
`_prev_cumulative_vol` from the last real tick, so the 12:25 flush bar also had 87%
inflated volume. That data is discarded anyway.

**Fix applied** (`engine/live_market_data.py`): when `prev is None`, leave
`bar_volume_delta = 0.0` instead of using the full cumulative.

#### Problem 2: Residual OHLC drift (~15–25% at steady state) — ARCHITECTURAL, NOT A BUG

Even after the volume fix, ~15–25% of OHLC values differ from the historical pack.
This is an inherent difference between the two data sources — **not a Kite bug, not our bug**.

**Why live WebSocket and Kite historical differ even though both come from Kite:**

| | Kite WebSocket (live) | Kite Historical API (pack) |
|--|--|--|
| What it sends | LTP quote updates — one tick per price change | Full tick reconstruction — every trade aggregated server-side |
| Bar open | First `last_price` tick received after bar start | First actual trade of the bar on exchange |
| Bar high/low | Max/min of ticks WE received | True high/low of ALL trades in bar |
| Bar volume | Cumulative delta between ticks we received | Exact per-bar volume from exchange |

We subscribe in **MODE_QUOTE** (`kite_ticker_adapter.py`). This delivers one WebSocket
update per price change — not every individual trade. In a liquid symbol, 50 trades can
happen in 200ms at different prices; Kite delivers 2–3 ticks covering that burst. We only
see the last price of the burst, not the extremes.

Consequences:
- **Open**: Our first tick after 9:20:00 may arrive at 9:20:02 and may not be the actual
  first trade. The historical API has the exact first trade at 9:20:00.
- **High/Low**: We miss intra-burst price extremes between consecutive WebSocket deliveries.
- **Close**: Usually matches well — last tick before bar end is typically captured.

This is why low-liquidity symbols nearly match (CANFINHOME: entry 857.72 vs 857.72 — exact)
while liquid symbols diverge (ABFRL: our vol=1,788,322 vs historical 33,096 before fix).

**The residual ~15–25% mismatch after the volume fix is not fixable** without switching
the candle source to Kite historical 1-min candles polled every 1–2 minutes, which would
replace WebSocket-built candles with authoritative data.

#### Problem 3: First bar always incomplete (structural limitation)

When the session connects mid-bar (e.g. at 10:07 during the 10:05–10:10 bar):
- OHLC open = first tick received at 10:07, NOT the actual open at 10:05
- All prior trades (10:05–10:07) are invisible

For Apr 16 this affected the 10:10 bar (89% vol inflated, 20% price match). This is
unavoidable — we cannot reconstruct trades we didn't observe. Since entry window is
9:20–10:15 and sessions start before 9:16, in normal operation the first bar affected
is the 9:20 bar (session connects at ~9:07, the 9:15 bar starts at 9:15:00 — we should
be connected before the first trade).

### Impact on Trading

- **SL and target monitoring**: Not affected. SL/target checks use LTP directly from
  WebSocket, not candle OHLC. No fix needed here.
- **Entry signals**: Entry fires on candle close. A 0.1–0.5% open/close divergence may
  occasionally push a symbol across or below the entry threshold compared to historical.
  This explains why the same symbol can show a different entry price or miss entry
  entirely between live and local-live sessions.
- **PnL divergence**: Entry price differences cascade into PnL differences. Expected.
  Example: HGS LONG — live entry 410.75 (TRAILING_SL +₹463), local-live 411.15 (TIME +₹1,089).

### Fixes Applied

1. **Volume inflation** (`engine/live_market_data.py`): First-tick `bar_volume_delta = 0.0`
   instead of `max(0.0, cumulative)`.

2. **exchange_timestamp fallback logged** (`engine/kite_ticker_adapter.py`): DEBUG log
   per batch when receive-time fallback fires. Lets us monitor how often exchange_timestamp
   is missing (missed timestamps near bar boundaries cause bucket misassignment).

3. **Audit tooling** (commits `fcba0ca`, `e3aba01`): `scripts/paper_feed_audit.py` reports
   per-field mismatches (open, high, low, close, volume separately) and is feed-source aware
   (Kite joins on `bar_start`; replay/local joins on `bar_end`).

### Remaining / Future Work

- **Potential improvement**: Poll Kite historical 1-min candles every 60s as authoritative
  candle source for entry decisions; keep WebSocket for LTP-only SL/target monitoring.
  This would eliminate residual OHLC drift at the cost of 60s entry latency.
- Monitor exchange_timestamp fallback rate tomorrow via DEBUG logs.
- Apr 15 zombie gap (11:51–12:17 — no ticks): missing ticks never recovered after
  reconnect — pack has full data, live has a gap. Contributes to the Apr 15 divergence
  count on top of the volume bug.

### Related Files
- `scripts/paper_feed_audit.py` — field-level drift reporting
- `tests/test_paper_feed_audit.py` — field-level counter assertions
- `engine/live_market_data.py` — `FiveMinuteCandleBuilder._ingest_locked` first-tick fix
- `engine/kite_ticker_adapter.py` — `_on_ticks` exchange_timestamp fallback log + MODE_QUOTE subscription

---

## 2026-04-17 — Zombie stale check is event-driven; never fires when ticks stop

**Status:** FIXED (2026-04-17)
**Severity:** High — stale detection silently inactive for 22 minutes; positions exposed

### Symptom
Feed went dead at ~11:08 IST. STALE exit fired at 11:30 (1334s stale, 22 minutes).
Apr 16 zombie fix (tick_age > 300 cap) was confirmed in code but did NOT fire.
Both sessions auto-flattened 2 open positions each and restarted unnecessarily.

### Root Cause
The stale watchdog was still running in the session supervision loop, but the
`stale_timeout > 0` guard prevented the zombie path from running when
`stale_feed_timeout_sec` was `NULL` or `0` in the live session row.

That meant a connected-but-silent WebSocket could stay alive far longer than intended
because the "zombie" check was effectively disabled by config.

### Proposed Fix
Keep the watchdog in the existing supervision loop, but always evaluate the WebSocket
zombie path even when `stale_timeout` is unset.

```python
# scripts/paper_live.py — evaluate zombie sockets even if stale_timeout is NULL/0
if use_websocket and ticker_adapter is not None and ticker_adapter.is_connected:
    tick_age = (ticker_adapter.get_stats() or {}).get("last_tick_age_sec") or 0
    if tick_age > 300:
        stale = elapsed > 600
    elif stale_timeout > 0:
        stale = elapsed > max(stale_timeout * 4, 120)
elif stale_timeout > 0 and elapsed > stale_timeout:
    stale = True
```

### Files to Change
- `scripts/paper_live.py` — keep the watchdog in the supervision loop and remove the stale_timeout gate

---

## 2026-04-17 — Auto-restart creates new sessions instead of resuming

**Status:** FIXED (2026-04-17)
**Severity:** High — open positions auto-flattened unnecessarily; new session is useless post-entry-window

### Symptom
After STALE exit at 11:30, `--multi` restarted both variants with new session IDs:
- `CPR_LEVELS_SHORT-2026-04-17-63b13c` — completed immediately, 0 trades
- `CPR_LEVELS_LONG-2026-04-17-401b74` — completed immediately, 0 trades

Original sessions had open positions (AWFIS, DIXON, VENTIVE, ALGOQUANT) that were
auto-flattened at the stale exit tick price (11:08 IST) instead of monitoring them
to their natural SL/target/time-exit. Dashboard shows the suffixed sessions as the
"latest" but they have 0 trades — confusing and incomplete.

### Root Cause
`paper_live.py` restart logic (line ~1050) always creates a fresh session on restart.
It does not check whether the failed session had open positions that should be resumed.
`--resume --session-id <id>` exists in the CLI but is not used by the auto-restart path.

### Proposed Fix
In the `--multi` restart loop, check if the failed session has OPEN positions in DB.
If yes: use `--resume --session-id <original_id>` instead of creating a new session.
If no (all closed): proceed with a new session as today (entry window check still applies).

```python
# scripts/paper_live.py — in variant restart logic
open_count = await count_open_positions(session_id)
if open_count > 0:
    # resume the existing session — no new entries, just monitor to EOD
    await run_variant_resume(session_id, ...)
else:
    # no open positions and entry window likely closed — skip restart
    logger.info(f"[{session_id}] No open positions and entry window closed — skipping restart")
    return
```

This avoids the premature flatten + useless empty-session restart pattern.

### Files to Change
- `scripts/paper_live.py` — `_run_multi` restart logic

---

## 2026-04-17 — Dashboard P&L does not match Telegram EOD summary

**Status:** FIXED — indirectly resolved by the auto-restart fix
**Severity:** Medium — was creating confusion about actual daily P&L

### Symptom
Dashboard showed ~₹1,603 mid-session. Telegram EOD showed:
- SHORT: +₹1,208.32 (21 trades)
- LONG: +₹1,432.85 (25 trades)
- Combined: +₹2,641.17

### Root Cause / Fix
The mismatch came from restarted suffix sessions (`CPR_LEVELS_SHORT-2026-04-17-63b13c`,
`CPR_LEVELS_LONG-2026-04-17-401b74`) being archived separately, causing the dashboard
to show 0-trade restart sessions instead of the full original session results.

Fixed indirectly by the auto-restart fix (2026-04-17): retries now reuse the original
`session_id` instead of creating a new suffixed one. Since the restart creates no new
session_id, the dashboard per-session P&L naturally matches the Telegram EOD total —
there is only one LONG and one SHORT session per day with the complete trade history.

---

## 2026-04-17 — Breakeven exits leaking unrealized profit (strategy observation)

**Status:** FIXED (2026-04-28) — intraday high now triggers TRAIL activation
**Severity:** Low — not a bug; expected behavior but suboptimal P&L capture

### Symptom
Multiple trades hit 1R (triggering `breakeven_r=1.0` → SL moves to entry), ran
further into profit showing large unrealized gains, then reversed back through entry
and exited at ~₹-83 (commission only). Dashboard appeared to show large P&L swings
(₹4,500 → ₹1,603) as these unrealized gains evaporated.

Examples from 2026-04-17: CHEMCON, COSMOFIRST, ASHIANA, DIGITIDE, SBILIFE all
hit 1R, were seen running in profit, then exited BREAKEVEN_SL at ~₹-83.

### Root Cause (Fixed)
`TrailingStop.update()` only checked the candle CLOSE to trigger BREAKEVEN→TRAIL
transition. A candle whose HIGH reaches 2R (trail target) but whose CLOSE stays below
2R never activated trailing. On the next candle, if price reversed below entry, the
trade exited BREAKEVEN_SL despite having had profitable unrealized gains.

Average MFE on BREAKEVEN_SL trades was 2.58R — meaning intraday highs were routinely
reaching/exceeding 2R without activating TRAIL.

### Fix Applied (2026-04-19)
`engine/cpr_atr_utils.py` — `TrailingStop.update()`:

1. **Added `candle_high`/`candle_low` params** — call sites pass full OHLC data.
2. **Trail trigger uses `max(close, candle_high)`** (LONG) or `min(close, candle_low)`
   (SHORT) to detect intraday 2R crossings.
3. **Deferred SL — both trigger paths** — regardless of whether TRAIL was activated by
   candle close or intraday high, the SL stays at entry price for the activation bar's
   `is_hit()` check and tightens on the next bar. This avoids a same-bar ordering
   assumption: a candle that closes through 2R could also have reversed back through the
   tighter stop level within the same 5-minute bar (OHLC order is unknown).
4. **Separate `if` for multi-phase transitions** — changed `elif self.phase == "BREAKEVEN"`
   to a standalone `if`, allowing PROTECT→BREAKEVEN AND BREAKEVEN→TRAIL to both fire on
   the same bar when a single candle crosses both 1R close and 2R high simultaneously.

Call sites updated:
- `engine/cpr_atr_shared.py:445` — `ts.update(close, candle_high=high, candle_low=low)`
- `engine/paper_runtime.py:1709` — same, using candle dict fields

### Backtest Impact (baseline comparison 2025-01-01 → 2026-04-17, full 2044 symbols)

| Metric | Old LONG | New LONG | Old SHORT | New SHORT |
|---|---|---|---|---|
| run_id | `a267ead61ffa` | `3898e767c6a9` | `23376249a6ca` | `3c139d78214a` |
| Total P&L | ₹813,889 | ₹893,968 | ₹1,014,394 | ₹1,053,952 |
| BREAKEVEN_SL | 1,130 | 1,107 (−23) | 1,608 | 1,576 (−32) |
| TARGET exits | 741 | 561 (−180) | 1,025 | 1,346 (+321) |
| TRAILING_SL | 264 | 467 (+203) | 322 | 34 (−288) |
| Calmar | 117.2 | 143.1 | 70.0 | 73.1 |

LONG improved significantly (+₹80K, Calmar 117→143). SHORT improved after the
`short_trail_atr_multiplier = 1.25` tuning (+₹41.7K, Calmar 70→73) because more
trades were allowed to continue to the fixed target instead of being clipped by the
trail. See `docs/trailing-stop-explained.md` for the mechanism and the candle-by-candle
examples.

Remaining BREAKEVEN_SL trades (1,107 LONG / 1,575 SHORT) are structurally correct —
trades that reached 1R breakeven but price never went on to reach 2R (high OR close),
so they reversed to entry. These are expected capital-protection outcomes.

### All-preset impact (2025-01-01 → 2026-04-17)

| Preset | Old P&L | New P&L | Δ | Old Calmar | New Calmar |
|---|---|---|---|---|---|
| DR-Std LONG | ₹824,702 | ₹905,186 | +₹80K (+9.8%) | 119 | 145 |
| DR-Std SHORT | ₹1,017,785 | ₹1,057,696 | +₹42K (+3.9%) | 61 | 66 |
| DR-Risk LONG | ₹813,889 | ₹893,968 | +₹80K (+9.8%) | 117 | 143 |
| DR-Risk SHORT | ₹1,014,394 | ₹1,053,952 | +₹42K (+3.9%) | 70 | 73 |
| Cmp-Std LONG | ₹1,593,174 | ₹1,827,199 | +₹234K (+14.7%) | 145 | 181 |
| Cmp-Std SHORT | ₹2,393,497 | ₹2,539,392 | +₹146K (+6.1%) | 93 | 104 |
| Cmp-Risk LONG | ₹1,592,321 | ₹1,826,581 | +₹234K (+14.7%) | 145 | 181 |
| Cmp-Risk SHORT | ₹2,354,156 | ₹2,536,852 | +₹183K (+7.8%) | 92 | 104 |

### Current v4 baseline run IDs

| Preset | Run ID |
|---|---|
| DR-Risk LONG | `f0bfbf9074ce` |
| DR-Risk SHORT | `be37c0ae2111` |
| DR-Std LONG | `6d3635b36ca3` |
| DR-Std SHORT | `3fba5456e120` |
| Cmp-Std LONG | `38a0b809d8a2` |
| Cmp-Std SHORT | `f1386c54ca7f` |
| Cmp-Risk LONG | `99e9d2beca78` |
| Cmp-Risk SHORT | `22d8ca089901` |

These rows replaced the retired v3 baselines in `backtest.duckdb` and are the only
canonical CPR_LEVELS baseline rows now kept in the archive.

### Files Changed
- `engine/cpr_atr_utils.py` — `TrailingStop.update()` rewrite
- `engine/cpr_atr_shared.py` — updated `ts.update()` call at line 445
- `engine/paper_runtime.py` — updated `ts.update()` call at line 1709
- `tests/test_cpr_utils.py` — 4 new unit tests + 2 updated existing tests

### 2026-04-19 RCA: LONG profit regression after trail-timing refactor

After the intraday-high trail fix landed in `95ae448`, the morning LONG baselines
(`e4f3123e8ad7`, `3898e767c6a9`, `206283c94744`, `dcb0f8fd2ddf`) no longer
matched when rerun later in the day. The reruns were still directionally correct,
but they shifted a large number of LONG winners from `TRAILING_SL` back to
`TARGET`, which reduced profit and Calmar.

On 2026-04-20, the baseline set was refreshed again after the shared exit helper
centralization. The new canonical rows are:
`6d3635b36ca3`, `3fba5456e120`, `f0bfbf9074ce`, `be37c0ae2111`,
`38a0b809d8a2`, `f1386c54ca7f`, `99e9d2beca78`, `22d8ca089901`.
These are the only baseline rows retained in `backtest.duckdb`.

  The important nuance is not that intraday-high activation is wrong. The issue is
  the timing of when the tightened stop becomes active relative to the completed
  5-minute bar. The working recovery path is:

  1. Let the completed bar arm TRAIL when its HIGH touches 2R.
  2. Use that completed-bar HIGH as the LONG trail anchor.
  3. Apply the tightened stop immediately after the bar closes.
  4. Make that new stop affect only future bars, not the triggering candle itself.

  That preserves the profit-improving LONG behavior while keeping the engine
  conservative about same-bar OHLC ordering.

  Operational note:
  - Backtest runs can show `save_complete` in the logs and still hold the DuckDB
    writer lock briefly afterwards. The dashboard replica sync is already done at
    that point; the remaining delay is process cleanup / file-handle release.
  - Treat that as a cleanup issue, not a data-sync issue. Investigate the lingering
    writer lifecycle separately from the strategy parity work.
- The completed-candle exit ordering is now centralized in a shared helper used by
  both backtest and paper/live so the bar-close trailing behavior stays aligned in
  one place.

---

## 2026-04-20 — Backtest vs live-paper drift on Apr 20 needs follow-up compare

**Status:** OPEN
**Severity:** High

### Symptom

On Apr 20, the backtest slice for CPR_LEVELS reports far fewer trades than the live
sessions, even though the same date is being compared:

- Backtest daily-reset risk: `6eb4ea65763f` on `2026-04-20` shows only `5` trades for the day
- Backtest daily-reset standard: `4d1f4e1b7873` on `2026-04-20` shows only `12` trades for the day
- Archived paper live-kite on Apr 20: `CPR_LEVELS_LONG-2026-04-20-live-kite` shows `30` trades
- Archived paper live-kite on Apr 20: `CPR_LEVELS_SHORT-2026-04-20-live-kite` shows `38` trades

The current working hypothesis is that the live universe / ordering / feed path is still
materially different from the historical backtest slice. The universe counts also differ:

- backtest run label: `u2043`
- live-kite run label: `u844`

### Follow-up

After the 20-Apr baseline reruns complete, run `daily-live --feed-source local` for
`2026-04-20` and compare the paper-local slice against the matching Apr 20 backtest
slice first. Once that is pinned down, compare live-kite separately and analyze the
remaining delta.

### Notes

This is a long-run parity item, not a pre-open blocker. The backtest behavior should be
understood against live over time, but the immediate next step is to get the 20-Apr
backtest slice and the 20-Apr paper-local slice compared cleanly.

---

## 2026-04-20 — Dashboard crash: Quasar formatter TypeError on Daily Summary

**Status:** FIXED — `web/components/__init__.py` (commit `bddd4b8`)
**Severity:** Medium

### Symptom

The dashboard emitted a client-side render error:

```text
vue.esm-browser.prod.js:5 TypeError: oe.format is not a function
    at te (quasar.umd.prod.js:17:210112)
```

### Root Cause / Fix

Column definitions passed `"format": "currency"` (a string) directly to Quasar's
`columns` prop. Quasar's native formatter expects a JS function, not a string —
hence the TypeError.

Fixed by adding `_vue_display_expr()` in `web/components/__init__.py`: `paginated_table`
now pops the `"format"` key before passing columns to Quasar and instead attaches a
`body-cell-{name}` Vue slot that renders the value through a locale-aware JS expression
(currency ₹, percent %, int with commas, decimal:N). Quasar's native sort still operates
on raw numbers; only the display rendering uses the slot.

### Files Changed
- `web/components/__init__.py` — `_vue_display_expr()` + `paginated_table` slot wiring

---

## 2026-04-18 — Live WebSocket OR values differ from EOD parquet, causing symbol divergence

**Status:** OPEN (root cause confirmed, fix under discussion)
**Severity:** High — 11% symbol overlap between Kite live and local feed; PnL comparison unreliable

### Symptom
Kite live (Apr 17): LONG +₹1,432 / SHORT +₹1,208 = +₹2,641
Local feed (`daily-live --feed-source local`, run after EOD): LONG +₹5,178 / SHORT +₹843 = +₹6,020
Only 5 out of 46 symbols overlapped between the two sessions.

### Root Cause (Confirmed)

**ATR is consistent** — verified all 2030 symbols in `market_day_state` match
`atr_intraday[trade_date=Apr 17]` exactly (ATR from Apr 16's last 14 five-min candles).
The pre-market `pivot-refresh` picks up this row because it was built during Apr 16's EOD
pipeline. ATR is NOT the divergence cause.

The actual root cause: **live WebSocket OR values differ from EOD parquet OR values**.

The Kite live session starts at 09:16 with pre-market `market_day_state` (NULL `or_high_5`,
`or_low_5`, `or_close_5` — no 9:15 candle data yet). At the 09:20 bar (after 9:15 candle
completes), `refresh_pending_setup_rows_for_bar()` resolves direction from live candles and
the fallback in `_hydrate_setup_row_from_market_row()` provides OR values from live
WebSocket ticks.

The Kite WebSocket operates in MODE_QUOTE — one update per price change, not per trade.
In liquid symbols, 50 trades can occur in 200ms at different prices while Kite delivers
only 2-3 ticks. The live 9:15 candle has a **smaller high-low range** than the EOD parquet
(which captures every trade). This produces a **lower or_atr_ratio**, allowing more symbols
to pass the `or_atr_ratio ≤ 2.5` filter.

| Symbol | EOD or_atr_ratio | Filter | Kite live |
|--------|-----------------|--------|-----------|
| CORONA | 4.11 | FAIL | Traded (live ratio ≤ 2.5) |
| NOCIL | 2.90 | FAIL | Traded (live ratio ≤ 2.5) |
| SUNDARMFIN | 2.82 | FAIL | Traded (live ratio ≤ 2.5) |
| ALGOQUANT | 2.28 | PASS | Traded (same) |

This is a manifestation of the known OHLCV drift issue (#7), amplified by the or_atr_ratio
filter which converts small OHLCV differences into binary PASS/FAIL decisions.

### Why Local Feed Uses Different OR Values
The local feed runs AFTER EOD pipeline rebuilds `market_day_state` with actual 9:15 candle
data from `intraday_day_pack` (EOD parquet). These values include all trades, producing
larger OR ranges and higher or_atr_ratio values. The `intraday_day_pack` and
`market_day_state` have identical OR values (verified diff=0.0).

### Parity Requirement
Backtest, paper replay, and paper live should produce the same results for the same date.
The live session's OR values (from WebSocket ticks) are inherently different from EOD
parquet values. This is an architectural limitation of MODE_QUOTE WebSocket feeds.

### Fix Options
1. **Poll Kite historical API during live session**: Replace WebSocket-built candles with
   authoritative 1-min candles polled every 60s. Eliminates drift at the cost of 60s entry
   latency. Keeps SL/target monitoring on WebSocket LTP.
2. **Store live OR values for post-hoc comparison**: After the live session, save the
   OR values used. Local feed can then replay with these exact values.
3. **Accept the difference**: Live trades symbols that the backtest filters out. For
   reliable comparison, compare at the strategy level (overall P&L, WR, Calmar) rather
   than symbol-by-symbol.

### Related Issues
- #7 (OHLCV drift) — same underlying cause, this issue is a specific manifestation
- #1 (OR fields NULL → 0.0) — the Apr 16 fix enabled the live OR fallback
- #8 (live candle OHLCV diverges from intraday_day_pack) — same architectural limitation

---

## Deferred Parity Follow-Ups

**Status:** OPEN
**Severity:** Medium

These items are intentionally deferred until after the current market-open window. They are
important for long-term parity hygiene, but they are not required to complete the immediate
pre-open workflow.

### 1) Shared daily candidate universe snapshot

Backtest and paper/live still derive their candidate universes from different runtime flows.
The long-term fix is to persist a daily candidate-universe snapshot and have all CPR runtimes
consume the same dated list.

### 2) Backtest vs paper parity re-check for 2026-04-17

Current paper replay/local parity is stable, but the 2026-04-17 paper runs still differ from
the backtest day slice. This needs a follow-up compare after the universe-source alignment is
settled.

### 3) Review lingering backtest writer-lock shutdown

We observed that some long baseline reruns finish saving results before the DuckDB writer lock
fully releases. That does not change results, but it is worth a later cleanup so baseline
reruns exit more predictably.

### 4) DuckDB RAM / thread tuning for ingestion and baseline reruns

Current tuning uses a conservative DuckDB memory cap and default thread heuristic. On the
96 GB / 16-core host, it may be worth benchmarking a higher-memory, moderate-thread profile
(for example 8 threads / 36 GB) to see whether build and rerun wall time improves without
creating additional spill or contention issues. This is a performance follow-up only and does
not change strategy behavior.

---

## Template for New Issues

```
## YYYY-MM-DD — Short title

**Status:** OPEN | FIXED | INVESTIGATING | PARTIALLY ADDRESSED
**Severity:** Critical | High | Medium | Low

### Symptom

### Root Cause

### Fix Applied / Proposed Fix

### Files Changed / Files to Change
```
