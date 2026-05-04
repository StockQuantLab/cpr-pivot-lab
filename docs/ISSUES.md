# Issues and Fixes Log

This is the single consolidated record of all bugs, incidents, and fixes for the live
paper trading system. Every new issue or fix must be documented here going forward.

Supersedes: `docs/PARITY_INCIDENT_LOG.md` (contents migrated below).

---

## 2026-05-04 — FIXED: LIVE: final paper archive used read-only dashboard replica

**Status:** FIXED
**Severity:** High

### Symptom

The 2026-04-30 local-feed live validation completed both paper sessions, but final archive
raised `DuckDB InvalidInputException: Cannot execute UPDATE on read-only ... paper_replica`.
That left the session completed in PostgreSQL/DuckDB paper state while the PAPER analytics
archive was missing.

### Root Cause

`scripts/paper_live.py` passed `get_dashboard_paper_db()` into
`archive_completed_session()`. The archive path updates `paper_sessions.total_pnl`, so it
must use the live writable paper DB connection, not the dashboard read-only replica.

### Fix

Final live archiving now uses `get_paper_db()`. Archive failure is also caught and returned
as an explicit archive payload error so a completed live session does not crash into a retry
loop after trading has already finished.

### Related

Found during `daily-live --multi --strategy CPR_LEVELS --trade-date 2026-04-30
--feed-source local --no-alerts` validation.

---

## 2026-05-03 — FIXED: UI: live readiness hid OK/NOT OK prerequisite details

**Status:** FIXED
**Severity:** Medium

### Symptom

Operators had to remember why exact trade-date setup rows such as `cpr_daily`,
`cpr_thresholds`, `market_day_state`, and `strategy_day_state` were valid for next-day live
startup. The Paper Sessions readiness panel showed aggregate counts and only displayed
non-OK freshness rows, so a green result did not explain which prerequisites were satisfied.

### Root Cause

`web/pages/ops_pages.py` filtered the readiness freshness table down to failures only and
did not render exact trade-date setup table statuses. `web/state.py` returned the raw
readiness report without dashboard-friendly OK/NOT OK rows.

### Fix

`web/state.py` now adds `setup_table_status_rows`, `freshness_status_rows`, and
`coverage_status_rows` to the readiness payload. `web/pages/ops_pages.py` renders those rows
with explicit `OK` / `NOT OK` status labels in the Live Readiness tab.
Dashboard freshness details now relabel exact selected-date rows from `OK next-day (...)` to
`OK current trade date (...)`, so the operator view is not confusing on the live trade date.

### Related

Focused verification: `uv run pytest tests/test_web_state.py -q`.

---

## 2026-05-03 — FIXED: UI: paper readiness checked Sunday calendar date by default

**Status:** FIXED
**Severity:** Medium

### Symptom

On Sunday 2026-05-03, the Paper Sessions dashboard showed `Live Readiness` for
`2026-05-03` and refreshed it every 3 seconds inside the Active Sessions tab. Operators
expected the readiness check to show the prepared next live date, `2026-05-04`, and to run
only when explicitly refreshed.

### Root Cause

`web/pages/ops_pages.py` initialized the readiness trade-date input with
`datetime.now().date()`, and `_load_active()` fetched readiness together with active sessions
on the near-real-time refresh loop. `web/state.py` also treated an empty readiness date as
literal `today` instead of preferring the prepared next-day runtime setup date.

### Fix

Moved readiness to its own `Live Readiness` tab with manual refresh. The Active Sessions
3-second timer now refreshes only active-session state and only while that tab is selected.
`web/state.py` now defaults empty readiness requests to the prepared runtime setup date when
next-day setup tables are ahead of `intraday_day_pack`.

### Related

Focused verification: `uv run pytest tests/test_web_state.py -q`.

---

## 2026-05-03 — FIXED: LIVE: daily-live could bypass the stronger data-quality readiness gate

**Status:** FIXED
**Severity:** High

### Symptom

The dashboard and `pivot-data-quality --date <trade_date>` could report a live trade date as
not ready because next-day setup rows were missing, while `pivot-paper-trading daily-live`
still used the weaker `prepare_runtime_for_daily_paper(mode="live")` prerequisite check.

### Root Cause

The CLI live launcher and dashboard/data-quality page enforced different readiness contracts.
`daily-live` validated prior-day prerequisites but did not call the setup-row readiness report
that catches missing next-day `market_day_state` / `cpr_daily` rows.

### Fix

Added a `daily-live` startup gate that calls `build_trade_date_readiness_report()` before
pre-filtering or session creation. A red data-quality readiness report now blocks both single
and `--multi` live starts unless the operator explicitly passes `--skip-coverage`.

### Related

Focused verification: `uv run pytest tests/test_paper_trading_cli.py::test_enforce_live_readiness_gate_blocks_when_data_quality_not_ready -q`.

---

## 2026-05-03 — FIXED: LIVE: startup fallback allowed an empty selected setup universe

**Status:** FIXED
**Severity:** High

### Symptom

Live startup could continue when `_prefetch_setup_rows()` loaded zero setup rows for the
requested active symbols if the broader `market_day_state` table had any rows for that
trade date.

### Root Cause

`scripts/paper_live.py` treated global table presence as a fallback signal. That is unsafe:
the live session must validate setup coverage for the exact candidate universe it is about
to trade, not just prove that some rows exist somewhere in the table.

### Fix

`run_live_session()` now fails closed whenever active symbols were requested and zero setup
rows were loaded for that selected universe. The existing regression test now covers the
default live fallback setting instead of only the explicit no-fallback path.

### Related

Focused verification: `uv run pytest tests/test_live_market_data.py::test_run_live_session_fails_closed_when_setup_prefetch_loads_no_rows -q`.

---

## 2026-05-03 — FIXED: PARITY: local-feed multi-session replay skipped the first bar for late-registered variants

**Status:** FIXED
**Severity:** High

### Symptom

The 2026-04-30 local-feed live validation produced exact LONG parity against the one-day
backtest slice, but SHORT had 1 paper-only trade and 3 backtest-only trades. Logs showed
the LONG local-feed session started at the 09:15 bar while the SHORT local-feed session first
processed 09:20.

### Root Cause

`engine/local_ticker_adapter.py` used one global bar cursor shared by all sessions. In
`daily-live --feed-source local --multi`, the first variant could register and drain the
09:15 bar before the second variant registered, causing the second variant to start from
09:20 and invalidating local-feed parity evidence.

### Fix

Changed `LocalTickerAdapter` to maintain per-session bar cursors. Each registered session now
starts from the first historical bar independently, even when variant coroutines register a few
scheduler ticks apart. Added a regression test for late session registration.

### Related

Focused verification: `uv run ruff check engine/local_ticker_adapter.py tests/test_local_ticker.py`
and `uv run pytest tests/test_local_ticker.py -q`.

---

## 2026-05-03 — FIXED: BUG: local-feed live replay failed on stale CPR helper import

**Status:** FIXED
**Severity:** High

### Symptom

`pivot-paper-trading daily-live --feed-source local --multi --strategy CPR_LEVELS --trade-date
2026-04-30 --all-symbols --no-alerts --complete-on-exit` failed during setup prefetch for both
LONG and SHORT variants:

`ImportError: cannot import name 'resolve_cpr_direction' from 'engine.paper_runtime'`

### Root Cause

`scripts/paper_live.py` still imported `resolve_cpr_direction` and `_build_intraday_summary`
from `engine.paper_runtime` inside the batch setup hydration path. Those helpers now live in
`engine.cpr_atr_utils` and `engine.paper_setup_loader`, so the local-feed/live setup path
crashed before any bar processing or parity comparison could run.

### Fix

Updated the setup hydration imports in `scripts/paper_live.py` to use the current shared CPR
direction helper and setup-loader intraday summary helper. Added a regression test covering
batch setup prefetch direction hydration.

### Related

Focused verification: `uv run ruff check scripts/paper_live.py tests/test_live_market_data.py`
and `uv run pytest tests/test_live_market_data.py -q`.

---

## 2026-05-03 — FIXED: INFRA: Documentation roots and relative links drifted

**Status:** FIXED
**Severity:** Low

### Symptom

Legacy durable docs (`CODEMAP.md`, `METRICS_POLICY.md`, `OPTIMIZATION_PLAN.md`) still lived at
the repository root, while several links inside `docs/` used `docs/...` targets that resolve
incorrectly from GitHub-rendered files under `docs/`.

### Root Cause

Older project docs predated the current documentation layout, and Git ignore rules only prevented
new local agent files from being added; they did not organize already-tracked docs or validate
relative Markdown links.

### Fix

Moved the root project docs under `docs/`, added `docs/README.md` as the documentation index,
refreshed stale status headers, corrected same-directory Markdown links, and tightened local
agent ignore rules for case-sensitive environments.

### Related

Verified with a local Markdown link scan across tracked and untracked Markdown files.

---

## 2026-05-03 — FIXED: LIVE: CPR time exit too close to Zerodha MIS auto square-off

**Status:** FIXED
**Severity:** Critical

### Symptom

CPR live/paper/backtest defaults exited open intraday positions at 15:15 IST. For real MIS
trading this left too little room before broker RMS auto square-off, extra charges, and failure
cases such as circuit-limit or connectivity issues.

### Root Cause

The strategy default was optimized against NSE close, not broker intraday square-off operations.
`StrategyConfig.time_exit`, paper session `flatten_time`, settings, and the backtest CLI all
defaulted to 15:15.

### Fix

Changed the production default EOD/time exit to 15:00 across `engine/constants.py`,
`engine/cpr_atr_models.py`, `engine/run_backtest.py`, `config/settings.py`, and `db/paper_db.py`.
Updated tests and current operator docs so backtest, replay, live paper, and real-routed live
sessions share the earlier exit.

Follow-up in the same safety batch: automated real-order routing now enforces a cash-only budget.
`daily-live --real-orders` rejects startup when `--real-order-cash-budget` exceeds Kite-reported
available equity cash, and each entry is blocked if cumulative open real-order notional would
exceed that cash budget.

### Related

Focused verification: `uv run pytest tests/test_settings.py tests/test_strategy.py
tests/test_live_market_data.py tests/test_paper_replay.py tests/test_paper_runtime.py
tests/test_paper_trading_cli.py -q`. LONG/SHORT CPR risk baselines must be rerun because this
changes strategy behaviour.

---

## 2026-05-02 — FIXED: LIVE: Broker order intents lacked hard price-safety guards

**Status:** FIXED
**Severity:** Critical

### Symptom

Future real-order enablement could inherit dry-run payload defaults that allowed unsafe broker
intents, including zero-priced LIMIT exits, SL/SL-M orders without trigger prices, and raw
MARKET flatten roles without a fresh reference price or slippage bound.

### Root Cause

`BrokerOrderIntent` validated only basic symbol/side/quantity/order-type shape. The Zerodha
adapter intentionally blocked real placement, but the payload builder did not encode the
real-money safety contract that must hold before a future real mode can call Kite.

### Fix

`engine/broker_adapter.py` now validates broker intents before Zerodha dry-run/real paths:
prices, triggers, reference prices, quote ages, slippage, and market protection must be finite
and in range; SL/SL-M require triggers; MARKET/SL-M require `market_protection`; and
exit/close/flatten/emergency roles must use protected LIMIT orders based on a fresh reference
price. Added `build_protected_flatten_intent()` to construct bounded marketable LIMIT exits from
latest LTP. `scripts/paper_trading.py real-dry-run-order` now accepts trigger/reference/slippage
fields for payload drills.

Follow-up audit hardening centralized admin-command validation in `engine/paper_runtime.py`:
invalid actions, unsafe symbols, oversized risk-budget values, and control characters in
reason/requester text are rejected or sanitized at the low-level queue writer. The real-pilot
readiness guard now accepts only LIMIT orders, not raw MARKET orders.

### Related

Focused verification: `uv run pytest tests/test_broker_adapter.py tests/test_execution_safety.py -q`,
`uv run ruff check engine/broker_adapter.py scripts/paper_trading.py tests/test_broker_adapter.py`,
and `uv run mypy engine/broker_adapter.py scripts/paper_trading.py --no-error-summary`.

Follow-up audit verification: `uv run pytest tests/test_paper_admin_commands.py
tests/test_broker_reconciliation.py tests/test_broker_adapter.py tests/test_execution_safety.py -q`,
`uv run ruff check engine/broker_adapter.py engine/broker_reconciliation.py engine/paper_runtime.py
scripts/paper_trading.py tests/test_broker_adapter.py tests/test_broker_reconciliation.py
tests/test_paper_admin_commands.py`, and `uv run mypy engine/broker_adapter.py
engine/broker_reconciliation.py engine/paper_runtime.py scripts/paper_trading.py --no-error-summary`.

---

## 2026-05-02 — FIXED: BUG: Review batch correctness and paper-runtime hardening

**Status:** FIXED
**Severity:** High

### Symptom

The 2026-05-02 project review identified multiple high-priority drift and integrity risks:
live setup fallback used a stale CPR TC formula, SHORT trailing stops ignored candle lows,
entry selection truncated candidates before validating cash, CPR/setup-funnel refreshes used
DELETE+INSERT without transactions, `resend-eod` could summarize sessions with OPEN positions,
and supervisor watch mode could relaunch after the trading day.

### Root Cause

Several live/replay/backtest decision paths had duplicated or stale logic, and some operator
retry paths were missing idempotency, transaction, or cutoff guards. The paper-control surface
also allowed mutation commands without an explicit environment gate.

### Fix

Implemented the review batch across `engine/`, `db/`, `scripts/`, `agent/`, and `web/`:
shared min-notional constants, fixed CPR fallback TC, corrected SHORT trailing anchors,
cash-validates all same-bar candidates before slot truncation, transaction-wrapped critical
DELETE+INSERT paths, added replay date-boundary reset, added EOD resume and supervisor cutoff,
bounded alert-log retention, hardened admin command paths and agent mutation ACL, escaped
Telegram HTML fields, pinned the dashboard host to `127.0.0.1`, and added focused regression
coverage.

Follow-up P2 completion added direct `AlertDispatcher` tests, explicit alert dedupe reset
coverage, live fallback query consolidation, SNTP drift warning in the supervisor, supported
MCP query limits/read-only defaults, active `or_daily` wording, and the Doppler parent-process
contract note.

### Related

`docs/REVIEW_2026-05-02.md`; focused tests: 173 passed. Full CPR baseline verification
completed and promoted on 2026-05-02:
`e811f5bb01e5`, `9a2ccbd93c5b`, `638b343959ad`, `307c3e175a16`,
`8bbabe422f9c`, `a700bb027f24`, `480a14f8aa26`, `f377d33a9157`.

---

## 2026-05-01 — OPEN: CPR risk baseline reference is not reproducible after runtime rebuild

**Status:** OPEN — baseline promotion guard required before accepting new CPR baseline family
**Severity:** High — baseline comparisons can be misleading if the runtime setup surface changed

### Symptom

Rerunning the daily-reset CPR risk baselines with the same saved universe and same visible
strategy parameters did not reproduce the previous LONG reference run:

- Old LONG reference `82b6b8c1e3fa` (`full_2026_04_27`, 2025-01-01→2026-04-28):
  3,284 trades, ₹1,047,927 PnL.
- Repro LONG run `8ebdb3c25632` with the same universe/window/sizing:
  2,851 trades, ₹907,099 PnL.
- Old SHORT reference `f7f1a698788f` reproduced closely against `12c5b346efa4`, so the
  large drift is concentrated in the LONG path.

### Evidence

- Old and repro runs both used `full_2026_04_27` (2,029 symbols).
- Visible sizing/strategy parameters matched: `capital=100000`, `max_positions=10`,
  `max_position_pct=0.1`, `risk_based_sizing=True`, `compound_equity=False`,
  `direction_filter=LONG/SHORT`, `or_minutes=5`, `entry_window_end=10:15`.
- Quality sort was already present before the old reference run; `select_entries_for_bar()`
  quality ordering dates to 2026-04-25 and the old reference was created on 2026-04-28.
- The current runtime state marks 363 of the 602 old-only LONG trades as
  `strategy_day_state.direction_5 = 'NONE'`; those rows cannot pass the current setup query.
- Sample contradictions include old archived LONG trades where the current 09:15 close is still
  inside the CPR band, e.g. `BAJAJHLDNG` 2025-01-02 and `HATSUN` 2025-01-02.
- `strategy_day_state.direction_5` builder logic was not recently changed; it still uses
  `or_close_5 > GREATEST(tc, bc)` for LONG and `< LEAST(tc, bc)` for SHORT.

### Root Cause

The old baseline was generated against a different runtime setup surface than the one now in
`market.duckdb`. The archived baseline rows do not persist enough setup/candidate diagnostics
to reconstruct which `strategy_day_state` values, candidate ranks, and selected/skipped
same-bar contenders existed at run time.

This is a baseline reproducibility gap, not proof that quality sort reduced profits. The
current comparison is mixing an old archived result with a rebuilt runtime state.

### Fix Needed

1. Persist a baseline data fingerprint with every promoted run:
   - saved universe name and symbol hash,
   - market/runtime table build timestamp or manifest hash,
   - `market_day_state` / `strategy_day_state` row counts and max dates,
   - relevant source parquet manifest hash.
2. Persist CPR candidate diagnostics for promoted baselines:
   - setup direction, CPR bounds, OR close, OR/ATR, effective RR, quality score,
   - candidate rank, selected/skipped status, skip reason, slot count/open slots.
3. Add a baseline promotion gate:
   - rerun the same params against the same universe/window before promotion,
   - fail if trade count/PnL drift exceeds tolerance,
   - print whether drift is due to universe, setup-state, candidate-selection, or execution.
4. DONE 2026-05-01: accepted the current rebuilt runtime surface as the new baseline family,
   promoted the `full_2026_04_30` 8-run set, and deleted the old 2026-04-28 reference rows.
   The fingerprint/candidate-diagnostics guard is still required before future promotions.

---

## 2026-05-01 — FIXED IN CODE: Compound-risk CPR batch path used raw risk quantity before overlay

**Status:** FIXED IN CODE — targeted tests pass; fixed compound-risk reruns completed
**Severity:** High for compound-risk baselines; low for daily-reset live paper

### Symptom

The 2026-04-30 current-runtime baseline rerun showed daily-reset and compound-standard improving
with the new 5-position / ₹2L slot configuration, but compound-risk regressed sharply:

- `RISK_LONG_CMP`: old ₹23.53L → new ₹15.05L, trades 3,282 → 1,109.
- `RISK_SHORT_CMP`: old ₹29.99L → new ₹17.86L, trades 4,853 → 1,651.

That was inconsistent with the standard compound result and indicated an implementation issue,
not a valid strategy conclusion.

### Root Cause

In the CPR batch path, `compound_equity=True` plus `risk_based_sizing=True` used the raw
risk-sized quantity from `scan_cpr_levels_entry()` before the final portfolio overlay. The
tracker recorded that raw notional during candidate simulation, so cash/slot availability for
later same-day candidates was distorted before the final overlay later capped allocation.

Daily-reset CPR was not affected because it does not use the compound overlay path.

### Fix

`engine/cpr_atr_strategy.py` now uses `SessionPositionTracker.compute_position_qty()` for
compound-risk as well as standard/risk daily-reset paths, passing `capital_base` so compound
slot sizing can grow with equity while still capping notional before the tracker records the
open position.

Regression coverage was updated in `tests/test_strategy.py` so compound-risk batch sizing is
capped before tracking instead of preserving raw risk quantity.

### Follow-up

Fixed reruns on `full_2026_04_30` were superseded by the 2026-05-02 review-batch canonical
promotion:

- `CPR_LEVELS_RISK_LONG` compound: `480a14f8aa26`, 2,376 trades, ₹1,710,760 PnL.
- `CPR_LEVELS_RISK_SHORT` compound: `f377d33a9157`, 3,116 trades, ₹1,652,271 PnL.

The earlier post-fix candidate runs `18c9f0587fd7` and `46b91b4c2842` should not be promoted.

---

## 2026-04-30 — FIXED: `pivot-data-quality` Windows encoding failure on readiness checkmark

**Status:** FIXED — non-data bug; final readiness passed when rerun with UTF-8 output
**Severity:** Medium — can make a successful EOD pipeline exit non-zero on Windows

### Symptom

During `pivot-refresh --eod-ingest --date 2026-04-30 --trade-date 2026-05-04`, stages 1–10
completed and `daily-prepare` printed `Ready YES`, but stage 11 failed while printing the
final data-quality report:

`UnicodeEncodeError: 'charmap' codec can't encode character '\u2713'`

The failure was caused by the `✓` readiness marker in `scripts/data_quality.py` under a
Windows `cp1252` console. Rerunning with `PYTHONIOENCODING=utf-8` returned exit code 0 and
confirmed `Ready YES`.

### Fix

`scripts/data_quality.py` now configures Windows stdio via `configure_windows_stdio()` and
uses ASCII `[OK]` / `[MISSING - BLOCKING]` markers for next-day setup rows. The EOD pipeline
should not fail after a successful readiness calculation because of console encoding.

---

## 2026-04-30 — FIXED: Live path reads market data through dashboard replica accessor

**Status:** FIXED
**Severity:** High — live trading should not depend on an implicit "latest dashboard replica"

### Problem

The live paper path currently uses `get_dashboard_db()` for read-only market setup queries
(`market_day_state`, `cpr_daily`, `atr_intraday`, `intraday_day_pack`, and related views).
That function reads the latest versioned file under `data/market_replica/`.

This is lock-safe for DuckDB on Windows, but the naming and ownership are wrong for live
trading. Dashboard replicas exist so dashboard/ad-hoc readers do not affect critical paths.
Live trading should not silently depend on a dashboard-oriented "latest replica" contract.

### Risk

If `market.duckdb` is repaired or rebuilt but the replica is stale, live can start from old
setup rows unless startup gates catch it. The new row-count and replica verification guards
reduce this risk, but the architecture is still ambiguous.

### Target design

- `market.duckdb` remains the source of truth.
- Live startup should read the source DB directly for trade-date setup validation/preload,
  then close the source DB before the trading loop starts; dashboard/ad-hoc queries continue
  to use replicas.
- If a copied DB is needed for live lock isolation, create an explicit verified and pinned
  `live_market_snapshot_<trade_date>.duckdb`, not the dashboard replica.
- Replace live uses of `get_dashboard_db()` with a clearly named live market accessor such as
  `get_live_market_db(trade_date)`.
- Startup logs must print source/snapshot path, snapshot version if any, trade date, created
  time, and CPR/state row counts before allowing the session to run.

### Required follow-up

`db/duckdb.py` now exposes `get_live_market_db()` as an explicit read-only source-DB accessor.
`engine/paper_setup_loader.py`, `scripts/paper_live.py`, and `engine/local_ticker_adapter.py`
now use it for live/replay runtime setup and local-feed pack reads instead of the dashboard
replica accessor.

---

## 2026-04-30 — OPEN: Historical sparse gaps in runtime tables (deferred post-EOD)

**Status:** OPEN — do not fix until after today's EOD ingestion
**Severity:** Low — does not block live trading or baseline reruns (gaps are on first-day IPO dates)

### Counts (as of 2026-04-30)

| Table | Missing symbol-days | Affected symbols | Date range |
|-------|--------------------|--------------------|------------|
| `atr_intraday` | 3,247 | 1,482 | 2015-02-02 → 2026-04-22 |
| `market_day_state` | 10,653 | 1,548 | 2015-02-02 → 2026-04-22 |
| `strategy_day_state` | 10,653 | 1,548 | 2015-02-02 → 2026-04-22 |

**Note:** `cpr_daily` gaps not separately counted (market_day_state gaps are superset).

### Root cause (primary — ~90% of gaps)

Newly-listed symbols on their **first trading day** (IPO date). The CPR build uses
`LEAD(date) OVER (PARTITION BY symbol ORDER BY date)` to assign `trade_date`. For the first
row in v_daily (no prior-day OHLC), `LEAD()` returns NULL → no CPR row → no `market_day_state`
row → `intraday_day_pack` has candle but runtime table is absent.

Recent examples (2024): DIGIDRIVE, EPACK, EXICOM, NOVAAGRI, RKSWAMY — all IPO first days.

This is a **structural limitation** for the first trading day of any new symbol. These gaps
are safe to accept: the live engine skips symbols without CPR data (entry gate requires
valid tc/bc/pivot), so no trades fire on IPO day anyway.

### 2019 anomaly (requires investigation)

2019 has 3,554 missing symbol-days across 211 symbols — disproportionately large vs other years
(which have 100-400 each). Possible causes: batch ingestion gap, instrument master change,
or a pivot-build run that was interrupted. Investigate after EOD is stable.

### Fix plan (post-EOD)

1. **IPO first-day gaps**: Mark as acceptable in `data_quality_issues` with `severity=INFO`
   and `issue_code=FIRST_DAY_IPO_NO_PREV_OHLC`. Suppressed from blocking checks.
   Implement via: `pivot-data-quality --classify-first-day-gaps` (new command needed).

2. **2019 anomaly**: Run `pivot-data-quality --date <affected-dates>` to identify the date
   range, then `pivot-build --refresh-since 2019-01-01 --until 2019-12-31` to rebuild if
   the source parquet exists.

3. **Baseline promotion gate**: Before promoting any baseline run, require
   `pivot-data-quality --baseline-window --universe-name canonical_full --start 2025-01-01`
   to pass with zero blocking gaps. (Tracked in 2026-04-29 DQ GAP issue above.)

---

## 2026-04-30 — INCIDENT: Zero trades on live session due to missing Apr30 CPR rows + stale replica

**Status:** FIXED (code), DOCUMENTED (root cause), PROCESS UPDATE REQUIRED
**Severity:** Critical — complete washout, zero trades taken for the full trading day

### Symptom

`daily-live --multi --strategy CPR_LEVELS` launched at 09:16 IST with:
- `with_setup=0 missing=2038 coverage=0%` at LIVE_STARTUP_READY
- Zero trades for the entire 09:16–10:15 entry window
- `closes=0` in every TICKER_HEALTH log line

### Root Cause Chain (4 layers)

**Layer 1 — EOD pipeline did not build Apr30 CPR rows:**
`pivot-refresh --eod-ingest --date 2026-04-29 --trade-date 2026-04-30` was not run on Apr29 evening.
Without this, `cpr_daily` and `market_day_state` had no rows for `trade_date=2026-04-30`.
The `cpr_daily` build uses `LEAD(date) OVER (PARTITION BY symbol ORDER BY date)` to determine
the next trading date. Since v_daily has no Apr30 rows at EOD, `LEAD()` returns NULL for Apr29
rows → no Apr30 CPR rows are created. The `--trade-date 2026-04-30` flag to EOD is what tells
the build to use `COALESCE(LEAD, '2026-04-30'::DATE)` for this purpose.

**Layer 2 — Pre-launch validation did not detect the gap:**
`daily-prepare` and `pivot-data-quality` check `max(trade_date) = previous_trading_day`. Since
Apr29 data was fresh, both reported OK. Neither checked `market_day_state has rows for today`.

**Layer 3 — `allow_live_setup_fallback` circular dependency:**
`_load_live_setup_row` (the designed fallback for missing market_day_state rows) requires
`live_candles != []` to work. At session startup, `state.candles` is always empty. The fallback
returns `None` for all symbols → all 2038 symbols marked `missing` → zero setups forever.
Code: `engine/paper_runtime.py:892` — `if not live_candles: return None`.

**Layer 4 — market_replica not synced after pivot-build:**
After manually running `pivot-build --refresh-date 2026-04-30` to repair the CPR data,
the live session still showed `with_setup=0` because `get_dashboard_db()` reads from
`data/market_replica/` (versioned snapshot) not `data/market.duckdb` directly. DuckDB on
Windows uses exclusive file locking — no second process can open the same file even read-only.
The replica was not updated by the manual build because: (a) the build process was killed before
completing its replica sync step, and (b) the live session held market.duckdb exclusively,
blocking any subsequent sync attempt.

### Fixes Applied (2026-04-30)

**Fix 1 — `--skip-coverage` bypass in `--multi` path (scripts/paper_trading.py):**
`_enforce_kite_live_setup_gate` was called unconditionally in `_cmd_daily_live_multi`
and in `_run_multi_variants`. Both call sites now respect `--skip-coverage`.

**Fix 2 — Startup fail-fast when market_day_state has 0 rows (scripts/paper_live.py):**
After `LIVE_STARTUP_READY` with `with_setup=0`, the engine now queries the replica directly
for `market_day_state` row count. If count=0, session fails immediately with the exact
`pivot-build --refresh-date <date>` + `pivot-sync-replica` fix commands.

**Fix 3 — Remove circular dependency in `_load_live_setup_row` (engine/paper_runtime.py):**
Removed `if not live_candles: return None` guard. CPR/ATR are computed from `v_daily` +
`atr_intraday` without needing candles. Direction stays NONE (pending) until first candle.
This allows the fallback to work at startup even with empty candle buffers.

**Fix 4 — EOD pipeline builds next-day CPR/state rows (scripts/refresh.py):**
Added two targeted stages after `build_runtime`:
- `build_next_day_cpr` → `pivot-build --table cpr --refresh-date <trade_date>`
- `build_next_day_state` → `pivot-build --table state --refresh-date <trade_date>`
These use `COALESCE(LEAD, trade_date)` to create Apr30 CPR rows from Apr29 OHLC.
Also added `sync_replica` stage → `pivot-sync-replica --verify --trade-date <trade_date>`.

**Fix 5 — `daily-prepare` asserts next-day rows exist (scripts/paper_trading.py):**
When in live mode (no intraday data), `daily-prepare` now queries `market_day_state` and
`cpr_daily` row counts for trade_date. Raises `SystemExit(1)` with exact fix commands if
either is 0.

**Fix 6 — `data_quality` readiness gate includes next-day row check (scripts/data_quality.py):**
In `setup_only_mode`, the readiness report now checks `market_day_state` and `cpr_daily`
row counts for the target trade_date. `ready=False` if either is 0. Printed in the report
as "Next-day setup rows (YYYY-MM-DD): [count] [OK / MISSING - BLOCKING]".

**Fix 7 — `pivot-sync-replica` CLI command (scripts/sync_replica.py + pyproject.toml):**
New command: `doppler run -- uv run pivot-sync-replica --verify --trade-date <date>`
Syncs `market_replica/` with current `market.duckdb`, then verifies row counts for the
given trade_date. Exits 1 with fix commands if rows are missing.

### Required Fixes (not yet implemented)

**Fix 2 — Startup fail-fast when market_day_state has 0 rows for trade_date:**
In `scripts/paper_live.py`, when `with_setup=0` at `LIVE_STARTUP_READY`, add an explicit
DB query `SELECT COUNT(*) FROM market_day_state WHERE trade_date = ?` and fail fast with
the fix command if count=0:
```
[STARTUP BLOCKED] No market_day_state rows for 2026-04-30.
Fix: doppler run -- uv run pivot-build --refresh-date 2026-04-30
     doppler run -- uv run python -c "from db.duckdb import ...; sync_replica()"
Then restart daily-live.
```

**Fix 3 — Pre-launch validation: assert today's market_day_state rows exist:**
`daily-prepare` and `pivot-data-quality --date <trade_date>` must check:
`SELECT COUNT(*) FROM market_day_state WHERE trade_date = ?` > 0.
Currently only checks max_date freshness, not forward-looking row existence.

**Fix 4 — Remove circular dependency in `_load_live_setup_row`:**
`engine/paper_runtime.py:892` — remove `if not live_candles: return None`. The function
already computes CPR from `v_daily` and ATR from `atr_intraday` without needing candles.
Direction would resolve as "NONE" (pending) on first candle. This makes the fallback
work at startup even with empty candle state.

**Fix 5 — Replica sync as part of pre-market checklist:**
After any `pivot-build`, `pivot-refresh`, or manual data repair, explicitly sync the
market replica before starting `daily-live`:
```bash
doppler run -- uv run python -c "
from db.duckdb import get_db, DUCKDB_FILE, REPLICA_DIR
from db.replica import ReplicaSync
db = get_db()
sync = ReplicaSync(DUCKDB_FILE, REPLICA_DIR)
sync.mark_dirty()
sync.maybe_sync(source_conn=db.con)
print('Replica synced')
"
```
Or add a `pivot-sync-replica` CLI command.

**Fix 6 — EOD pipeline guard: verify cpr_daily has next-day rows after build:**
`_run_eod_ingestion` in `scripts/refresh.py` should assert after `pivot-build`:
`SELECT COUNT(*) FROM cpr_daily WHERE trade_date = ?::DATE` > 0.
If 0, re-run `pivot-build --refresh-date <trade_date>` before proceeding.

### Pre-market Checklist Update (add these steps)

Before running `daily-live` each morning, verify:
```bash
# Must be > 0 (not just max_date freshness):
doppler run -- uv run python -c "
from db.duckdb import get_db
db = get_db()
import datetime
today = datetime.date.today().isoformat()
r = db.con.execute(f\"SELECT COUNT(*) FROM market_day_state WHERE trade_date = '{today}'\").fetchone()
print(f'market_day_state rows for {today}:', r[0])
assert r[0] > 1000, f'MISSING market_day_state rows for {today} — run pivot-build --refresh-date {today}'
"
# Then sync replica:
doppler run -- uv run python -c "
from db.duckdb import get_db, DUCKDB_FILE, REPLICA_DIR
from db.replica import ReplicaSync
db = get_db()
sync = ReplicaSync(DUCKDB_FILE, REPLICA_DIR)
sync.mark_dirty()
sync.maybe_sync(source_conn=db.con)
print('Replica synced — safe to start daily-live')
"
```

---

## 2026-04-29 — DQ GAP: EOD readiness does not validate full baseline-window runtime coverage

**Status:** OPEN
**Severity:** Medium — live readiness can pass while historical baseline reruns still have runtime-table gaps

### Symptom

After Apr29 EOD ingestion and Apr30 readiness showed green, a read-only baseline preflight for
`canonical_full` (`2038` symbols) over `2025-01-01 -> 2026-04-29` still found missing runtime
symbol-days where `v_5min` data exists:

- `intraday_day_pack`: 3 missing source symbol-days
- `atr_intraday`: 838 missing source symbol-days
- `cpr_daily`: 170 missing source symbol-days
- `market_day_state`: 30,212 missing source symbol-days
- `strategy_day_state`: 30,212 missing source symbol-days

### Root Cause

Current `pivot-data-quality --date <trade_date>` is a live-readiness gate. It validates today's or
tomorrow's operational readiness and sparse symbol/day gaps, but it does not validate historical
baseline-window completeness for a named universe.

### Required Fix

Add a baseline preflight mode, for example:

```bash
pivot-data-quality --baseline-window \
  --universe-name canonical_full \
  --start 2025-01-01 \
  --end <current_date>
```

The gate must fail when runtime tables are missing rows for `(symbol, date)` pairs where source
parquet (`v_5min` / `v_daily`) exists. This should run before any canonical baseline rerun.

---

## 2026-04-29 — BUG: live resilience gaps in live loop + alert dedupe (tracking)

**Status:** IMPLEMENTED (high-priority)
**Severity:** High — affects crash response and multi-session containment

### Scope

The live-paper hardening review identified still-open resilience gaps:

- Global kill signal is currently per-session-only (`flatten_<session_id>.signal`) and does not close all active sessions from one file.
- Admin command queue files can persist and execute after restart because there is no stale-file expiry.
- Reconciliation runs on every bar group, increasing per-bar latency under large universes.
- WebSocket stale detection mixes `last_snapshot_ts` with tick-age signals and can produce false positives on quiet feeds.
- `FLATTEN_EOD` dedupe for alerts is in-memory only during long-lived process restarts.
- Session `PLANNING` rows can linger long after the day starts.

### Implemented Fixes

- Added session-wide flatten signal path: `.tmp_logs/flatten_all.signal`.
- Added stale admin-command cleanup based on file mtime.
- Throttled in-loop reconciliation to trigger events and 15-minute boundaries.
- Switched websocket stale-freshness to websocket tick timestamps when present.
- Added persisted `FLATTEN_EOD` dedupe guard in `alert_log` (in addition to existing in-memory guard).
- Added periodic `paper_feed_audit` cleanup in live loop (every 30 minutes).
- Added `cleanup_stale_sessions()` handling for `PLANNING` rows older than 1 day.
- Added external process supervision in `scripts/paper_supervisor.py`:
  - `--watch` mode relaunches `daily-live` when no active session exists for target trade date.
  - Added explicit signal handling to terminate active child sessions on SIGINT/SIGTERM/SIGBREAK.

### Validation

- Added/updated tests:
  - `tests/test_paper_live_polling.py` (global signal + stale admin command expiry + feed-audit retention scheduling).
  - `tests/test_paper_runtime.py` (DB-backed `FLATTEN_EOD` dedupe).
  - `tests/test_bar_orchestrator.py` (multi-entry cumulative cash capacity regression).
  - `tests/test_paper_db.py` (stale `PLANNING` sessions auto-cancelled).
  - `tests/test_paper_supervisor.py` (`trade_date` parsing, active session detection, and defaults).

### Deferred / Out-of-Scope

- **H2 (external supervisor/watchdog):** implemented via `pivot-paper-supervisor --watch`.

### Pending / Skipped with rationale

- **L1 (TrailingStop reconstruction on every candle):** implemented via session-position `TrailingStop` cache in `engine/paper_runtime.py` to avoid re-instantiation during per-candle advances.
- **L2 (FEED_STALE rate-limit window):** implemented in `scripts/paper_live.py` as a shared cooldown constant (`PIVOT_FEED_STALE_ALERT_COOLDOWN_SEC`) to make repeated alerts less noisy and configurable.
- **L3 (archive zero-trade `archive_completed_session` fragility):** fixed to use persisted closed-position count (via `get_session_positions`) instead of suffix heuristics.
- **L4 (compute_position_qty float precision):** N/A — partial-share execution is intentionally out of scope for this environment. Paper sizing remains integer as expected by current broker/order path and risk policy.
- **M5 (market DB lock contention):** low-priority and no clear improvement without measurable benchmark evidence; left as is.

### 2026-04-29 Follow-up Decisions

- Confirmed all critical/high items (C1–C4, H1–H3, H4, H5, H1/2/3 etc.) are implemented.
- Confirmed mid-priority resilience fixes M1–M4 are implemented.
- Kept M5 deferred by design; L1/L2/L3 moved to implemented after this pass.
- Additional parity hardening (2026-04-29 follow-up): removed redundant CPR entry prefilter logic in
  `CPRATRBacktest._simulate_day_cpr_levels()` so OR/ATR and gap screening uses the same
  shared `scan_cpr_levels_entry()` path as live/replay. This removes the last backtest-only
  duplicate gate in the parity-critical entry path.
- Follow-up regression checks run locally (with writable cache override):
  - `$env:UV_CACHE_DIR = Join-Path (Get-Location) '.tmp_logs/uv-cache'; uv run pytest tests/test_paper_supervisor.py tests/test_bar_orchestrator.py tests/test_paper_live_polling.py tests/test_paper_db.py -q`
  - `$env:UV_CACHE_DIR = Join-Path (Get-Location) '.tmp_logs/uv-cache'; uv run ruff check scripts/paper_supervisor.py scripts/paper_live.py engine/paper_runtime.py engine/bar_orchestrator.py db/paper_db.py tests/test_paper_supervisor.py tests/test_paper_live_polling.py tests/test_bar_orchestrator.py tests/test_paper_db.py`

### Notes

- Constraints for idempotent paper orders and single-open-position-per-symbol are now enforced in code:
  `idx_po_idempotency` is unique, and `idx_pp_session_symbol_status` enforces the open-symbol/session
  uniqueness contract in DuckDB-compatible form (unique session+symbol+status).

### 2026-04-29 Follow-up: parity/config parity sweep

- **Status:** FIXED IN CODE — historical Apr-29 live sessions remain non-canonical evidence
- **Severity:** Medium
- **Scope:** live-vs-replay-vs-backtest configuration uniformity and parity precision

#### Findings

1) `CPR_LEVELS_LONG-2026-04-29-live-kite` and `CPR_LEVELS_SHORT-2026-04-29-live-kite`:
   - Marked `_canonical_preset=CPR_LEVELS_RISK_LONG / CPR_LEVELS_RISK_SHORT`
   - Session-level DB columns still show `max_positions=10`, `max_position_pct=0.10`, `portfolio_value=1000000`
   - Session strategy fingerprint in `paper_sessions.strategy_params` still resolves to:
     - `capital=100000`
     - `risk_pct=0.01`
     - `max_positions=10`
     - `max_position_pct=0.1`
   - Expected canonical sizing for CPR risk presets is:
     - `capital=200000`
     - `max_positions=5`
     - `max_position_pct=0.20`
   - Impact: these two sessions are not replay/parity comparable to canonical backtest presets despite preset labels.

2) 2026-04-27 replay-vs-backtest canonical validation:
   - `CPR_LEVELS_LONG-2026-04-27-replay-historical-8e14c9` vs `04f545178f9b`
   - `11` expected / `11` actual trades matched; `matched_within_eps=9` with two penny-level row drifts
     (`±₹0.01`, `total_abs_delta=₹0.0200`, `2` rows).
   - Impact: residual is operationally negligible but should be tracked as a parity guard threshold to prevent silent expansion.

3) Same-day backtest target for live 2026-04-29 is still missing.
   - No `run_metadata`/`backtest_results` run currently matched `trade_date=2026-04-29` for direct backtest parity.
   - Impact: complete-day live-vs-backtest parity for 2026-04-29 remains blocked until a canonical backtest run for that date exists.

#### Action

- Fixed future-session enforcement:
  - Single `--preset` paper runs now stamp `_canonical_preset` only when resolved params exactly
    match the named preset.
  - `_with_resolved_strategy_metadata()` now expands `_canonical_preset` from
    `engine/strategy_presets.py` and refuses stale embedded sizing overrides.
  - A future session can no longer be labelled canonical while carrying old sizing such as
    `capital=100000`, `max_positions=10`, or `max_position_pct=0.10`.
- Remaining parity work:
  - Treat the two Apr-29 live sessions as non-canonical historical sessions.
  - Run a fresh canonical replay/live/backtest comparison after the next clean session.
  - Live-vs-backtest parity for 2026-04-29 is still blocked until a canonical same-day backtest
    target exists.

---

## 2026-04-28 — BUG: pivot-baselines parent process blocks child backtests

**Status:** FIXED
**Severity:** Medium — baseline refresh can fail all 8 variants before doing any strategy work

### Symptom

`pivot-baselines --start 2025-01-01 --end 2026-04-28` failed every variant immediately with:

```text
Another DuckDB write process is running (PID ...)
Only one write connection is allowed at a time.
```

### Root Cause

`pivot-baselines` closed the parent market DB handle before entering the run loop, but
`_build_backtest_args()` reopened `market.duckdb` to check whether `full_<end_date>` existed and
kept that handle open while spawning `pivot-backtest`. The child process then correctly refused to
start because the parent process still held the market writer lock.

### Fix Applied 2026-04-28

- `engine/baselines_cli.py`: `_build_backtest_args()` now closes the parent market DB handle in a
  `finally` block before the child process is spawned.
- `tests/test_baselines_cli.py`: added regression coverage that verifies `_build_backtest_args()`
  releases the parent market DB connection.

Validation:
- `uv run pytest tests/test_baselines_cli.py::test_build_backtest_args_closes_parent_market_db tests/test_baselines_cli.py::test_build_backtest_args_includes_progress_file tests/test_baselines_cli.py::test_build_backtest_args_prefers_saved_universe_snapshot -q` -> `3 passed`
- `uv run ruff check engine/baselines_cli.py tests/test_baselines_cli.py` -> clean

---

## 2026-04-28 — BASELINE: extending CPR baselines with a new dated universe changes prior-window trades

**Status:** OPEN — old baselines were preserved; do not promote the 2026-04-28 rerun until policy is decided
**Severity:** Medium — baseline comparison can look like strategy drift even when the strategy code is unchanged

### Symptom

The 2026-04-28 canonical CPR baseline rerun completed all 8 variants, but the overlap check against
the existing 2026-04-27 baselines did not reproduce the `2025-01-01..2026-04-27` rows exactly.

Examples:

- `STD_LONG`: overlap trade delta `-13`, overlap P&L delta `-₹1,931.59`
- `STD_SHORT`: overlap trade delta `-16`, overlap P&L delta `-₹10,527.01`
- `RISK_SHORT_CMP`: overlap trade delta `-17`, overlap P&L delta `-₹65,351.00`

### Root Cause / Finding

The previous baseline runs used a 2029-symbol universe. The 2026-04-28 saved universe
`full_2026_04_28` contains 2015 symbols. These 14 symbols were present in the old run metadata but
absent from the new dated universe:

```text
GLOBECIVIL, KHAICHEM, MANAKSTEEL, NAGREEKCAP, NINSYS, OSWALAGRO, PENINLAND,
RUDRA, SADHNANIQ, SARVESHWAR, SEJALLTD, SHEKHAWATI, SHYAMTEL, VAISHALI
```

Six of those symbols had historical trades before 2026-04-28, and removing them can also change
same-bar slot filling/ranking outcomes for other symbols. Therefore the total P&L delta is not only
the 2026-04-28 trading day.

### Required Follow-Up

- Decide baseline policy before cleanup:
  - continuity baseline: rerun 2026-04-28 using the exact old 2029-symbol reference universe, then
    compare overlap again;
  - current-tradeable baseline: accept `full_2026_04_28`, but document that old/new deltas include
    universe churn and are not a pure daily delta.
- Add an automated overlap gate to `pivot-baselines` so promotion/cleanup is blocked when the prior
  window does not match within tolerance.

---

## 2026-04-28 — POLICY/BUG: Canonical universe must not shrink daily because a few symbols are missing

**Status:** FIXED IN CODE — needs next EOD/live validation and baseline rerun
**Severity:** High — shrinking the baseline/live universe makes backtests non-comparable and can hide live-trading coverage gaps

### Symptom

The 2026-04-28 dated universe `full_2026_04_28` had 2015 symbols, while the existing canonical
baseline universe had 2029 symbols. This caused historical baseline deltas to change before Apr-28,
even though the strategy overlap should have been identical.

Daily universe shrink is the wrong default for canonical baselines. A few symbols being delisted,
suspended, or missing records on a day should not cause the entire canonical universe to be reduced
and historical runs to become non-comparable.

### Policy Implemented 2026-04-29

- `canonical_full` is now the stable full-universe source of truth.
- `daily-prepare --all-symbols` creates `canonical_full` once if missing, then copies that same
  list to the dated `full_YYYY_MM_DD` snapshot.
- `daily-live`, `daily-replay`, and `daily-sim` default to the dated snapshot and fall back to
  `canonical_full` only if the dated snapshot is missing.
- `pivot-data-quality --date <trade_date>` also falls back from `full_YYYY_MM_DD` to
  `canonical_full` for setup-only readiness.
- `_resolve_all_local_symbols()` no longer intersects with the current Kite instrument master;
  the universe is not reduced because one day's instrument/data availability changed.
- Repeated `daily-prepare --all-symbols` is now guarded: if the dated `full_YYYY_MM_DD` snapshot
  already exists and differs from `canonical_full`, the command fails unless the operator passes
  `--refresh-universe-snapshot`.
- Default live/replay/sim universe resolution now refuses a mismatched dated snapshot instead of
  silently using a smaller universe than `canonical_full`.
- Sparse symbol/day gaps remain warnings; broad gaps still fail readiness.

### Remaining Follow-Up

- Validate the next EOD `daily-prepare` preserves the canonical count in `full_YYYY_MM_DD`.
- Rerun baselines only after the baseline promotion/overlap gate policy is finalized.

### Required Code Behaviour

- Backtest and live preparation load the canonical universe and skip only symbols that lack
  the required setup/candle rows for the specific day being processed.
- Live/paper continues with the available symbols when a few symbols are missing data.
- Missing-symbol/day counts are logged and surfaced in readiness/quality output.
- Baseline comparison should store and display the universe name and symbol count clearly.
- `pivot-baselines` should block promotion if the new run uses a different universe from the
  previous canonical baseline unless the operator explicitly opts into a universe migration.

---

## 2026-04-28 — PARITY BUG: paper/live ignored CPR momentum-confirm exit while backtest applied it

**Status:** FIXED IN CODE — needs next replay/live validation
**Severity:** High — live/paper exits can diverge from backtest even when params both say `momentum_confirm=true`

### Symptom

Apr-28 promoted risk baselines and archived Kite paper sessions had poor trade parity:

- LONG backtest `82b6b8c1e3fa`: `31` trades, `-₹2,065.71`
- LONG live paper `CPR_LEVELS_LONG-2026-04-28-live-kite`: `26` trades, `+₹267.44`
- SHORT backtest `f7f1a698788f`: `17` trades, `+₹1,261.28`
- SHORT live paper `CPR_LEVELS_SHORT-2026-04-28-live-kite`: `21` trades, `+₹4,732.57`

The archived run metadata showed both backtest and live/paper had `momentum_confirm=true`, but only
the backtest rows produced `MOMENTUM_FAIL` exits. Live/paper had no `MOMENTUM_FAIL` exits.

### Root Cause

Backtest calls `simulate_trade_lifecycle(... momentum_confirm=True)` in `engine/cpr_atr_shared.py`.
That shared lifecycle exits at the next bar's open when the first post-entry candle closes adverse.

Paper/live uses `_advance_open_position()` in `engine/paper_runtime.py` and calls
`resolve_completed_candle_trade_step()`, but it did not persist or apply the
`momentum_exit_pending` state. Result: paper/live silently ignored the momentum-confirm exit rule.

### Fix Applied

- `engine/paper_runtime.py`: stores `momentum_exit_pending` in `trail_state` when bar 1 closes
  adverse and closes at bar 2 open with `exit_reason="MOMENTUM_FAIL"`.
- `tests/test_paper_runtime.py`: added regression test for bar-1 adverse close -> bar-2 open
  `MOMENTUM_FAIL` close.

Validation:
- `.venv\Scripts\python.exe -m pytest tests/test_paper_runtime.py::test_advance_open_position_honors_momentum_confirm_exit tests/test_paper_runtime.py::test_advance_open_position_preserves_initial_sl_for_trail_transition -q` -> `2 passed`
- `.venv\Scripts\python.exe -m ruff check engine/paper_runtime.py tests/test_paper_runtime.py` -> clean

### Remaining Apr-28 Parity Blockers

This fix does not make the already-archived Apr-28 Kite sessions clean parity targets:

- The live feed audit starts at 09:30, while backtest has 09:20 and 09:25 entries.
- LONG session metadata shows a 13:40 restart/flatten lifecycle and is not a clean uninterrupted
  live session.
- Live sessions used a 749-symbol prefiltered runtime universe; promoted baselines use the `u2029`
  stable baseline universe and then apply setup/entry filters.

Next validation should use a fresh replay or tomorrow's supervised live session after this fix.

---

## 2026-04-28 — PROCESS: costly baseline reruns started before isolating the mismatch

**Status:** OPEN — process guard needed
**Severity:** Medium — wastes time and creates confusing partial runs during active trading-day analysis

### Symptom

After the first 2026-04-28 baseline rerun showed unexpected old-vs-new deltas, a continuity rerun
using the old 2029-symbol universe was started to isolate Apr-28 impact. The rerun was technically
useful, but it was started before confirming with the operator and before narrowing the investigation
to the two daily-reset risk variants.

This created confusing partial runs such as:

- `920f14ee4ea7` — `STD_LONG_CONT`
- `026505f8d6c1` — `STD_SHORT_CONT`
- `82b6b8c1e3fa` — `RISK_LONG_CONT`

### Root Cause / Finding

The correct diagnostic sequence should have been:

1. Compare one old/new pair on the overlapping window through the prior end date.
2. If overlap drifts, inspect run metadata, universe, and changed strategy code before rerunning.
3. If a rerun is still needed, run only `RISK_LONG daily-reset` and `RISK_SHORT daily-reset` first.
4. Run the full 8-baseline campaign only after the operator explicitly approves it.

The pairwise check later showed that the promoted `82b6b8c1e3fa` RISK LONG daily-reset run
matched its retired Apr-27 predecessor exactly through `2026-04-27`; its total delta was only
the Apr-28 LONG result:

- overlap trade delta: `0`
- overlap P&L delta: `₹0.00`
- Apr-28 trades: `31`
- Apr-28 P&L: `-₹2,065.71`

### Required Follow-Up

- Add a `pivot-baselines` pre-promotion overlap gate that compares prior-window trades/P&L before
  allowing cleanup or documentation promotion.
- Add a lightweight command or runbook step for single-pair validation:
  `RISK_LONG daily-reset` and `RISK_SHORT daily-reset` only.
- Do not start full 8-run reruns without explicit operator approval.
- Reconcile Apr-28 LONG paper-vs-backtest separately; the live Kite paper LONG had fewer trades and
  a different P&L than the Apr-28 backtest day.

---

## 2026-04-28 — BUG: Cleanup can update live DuckDB but leave dashboard replica stale

**Status:** FIXED — replica pointer/file publication now retries transient Windows locks
**Severity:** Medium — dashboard can show deleted drill sessions after cleanup even though live DB rows are gone

### Symptom

After deleting drill paper sessions and archived PAPER runs, the dashboard could still show stale
sessions/runs until a later sync or dashboard restart.

### Root Cause

Cleanup writes to the live DuckDB files and then publishes versioned dashboard replicas.
On Windows, the dashboard can briefly hold the replica pointer file while polling it. If
`os.replace()` hits that exact moment, replica publication raises `PermissionError`.

For `paper.duckdb`, `PaperDB.force_sync()` intentionally catches sync exceptions to avoid crashing
live trading. That made cleanup appear successful while the dashboard kept reading the previous
replica version.

### Fix Applied 2026-04-28

- `db/replica.py`: added bounded retries around both replica DB file replacement and pointer-file
  replacement.
- `tests/test_backtest_replica_migration.py`: added regression coverage for transient
  `PermissionError` during replacement.

Validation:
- Live and replica DBs both show only the two actual 2026-04-28 Kite sessions after cleanup.
- `uv run pytest tests/test_backtest_replica_migration.py::test_replica_force_sync_publishes_snapshot tests/test_backtest_replica_migration.py::test_replica_replace_retries_transient_permission_error tests/test_backtest_replica_migration.py::test_backtest_delete_runs_forces_replica_publish tests/test_backtest_replica_migration.py::test_market_delete_runs_forces_replica_publish -q` -> `4 passed`
- `uv run ruff check db/replica.py tests/test_backtest_replica_migration.py` -> clean

---

## 2026-04-28 — PARITY: live-local drill exceeded max_positions and is not a valid replay baseline

**Status:** INVESTIGATED — current replay path obeys the cap; archived `CPR_LEVELS_LONG-2026-04-28-live-local` should not be used as a parity baseline
**Severity:** High — historical drill overstates trade count and P&L by allowing more than 10 concurrent LONG positions

### Symptom

The 28-Apr LONG local-feed live drill archived as:

- `CPR_LEVELS_LONG-2026-04-28-live-local`: 45 trades, `+6,506.91`

A fresh 28-Apr LONG replay with the same CPR risk preset archived as:

- `paper-cpr_levels-long-2026-04-28-replay-historical`: 28 trades, `+590.78`

The 28 common trades matched exactly by symbol, entry/exit time, exit reason, and P&L.
The difference was 17 extra `LOCAL_ONLY` trades in the historical live-local drill.

### Root Cause / Finding

The live-local drill breached the configured concurrent cap:

- Session config: `max_positions=10`
- Observed live-local max concurrent open positions: `19`
- Fresh replay max concurrent open positions: `10`

The feed-audit bars for the `LOCAL_ONLY` symbols matched replay bars, so this is not price-feed
drift. The archived live-local drill is therefore an invalid comparison target for replay parity.

### Impact

- Do not compare future replay/backtest results against `CPR_LEVELS_LONG-2026-04-28-live-local`.
- Its `+6,506.91` P&L is inflated by extra entries that should not exist under the 10-position cap.
- Use the fresh replay run or rerun live-local with current code under a new session id if a local-feed
  baseline is needed.

### Validation 2026-04-28

- Deleted extra short local drill: `CPR_LEVELS_SHORT-2026-04-28-live-local`.
- Preserved the long local drill only as forensic evidence.
- Fresh replay command:
  `doppler run -- uv run pivot-paper-trading daily-replay --strategy CPR_LEVELS --preset CPR_LEVELS_RISK_LONG --trade-date 2026-04-28 --no-alerts`
- Comparison result:
  - `MATCH`: 28 trades
  - `LOCAL_ONLY`: 17 trades
  - `REPLAY_ONLY`: 0 trades
  - Feed drift on matched bars: 0
- Focused regression check:
  `uv run pytest tests/test_paper_session_driver.py::test_process_closed_bar_group_skips_duplicate_candle -q` -> `1 passed`

---

## 2026-04-28 — BUG: Paper ledger tab change raises GenericEventArguments.value AttributeError

**Status:** FIXED
**Severity:** Low — dashboard-only; no trading/runtime impact

### Symptom

Switching tabs on `/paper_ledger` logged:

```text
AttributeError: 'GenericEventArguments' object has no attribute 'value'
```

### Root Cause

The new tab handler used `e.value`, but NiceGUI's raw `update:model-value` event passes the selected
tab through `GenericEventArguments.args`, not a direct `.value` attribute.

### Fix Applied 2026-04-28

- `web/pages/ops_pages.py`: tab change handler now reads `e.args["value"]` / 
  `e.args["modelValue"]` first, with a fallback to `e.value`.

## 2026-04-28 — BUG: ADMIN close_all can send partial FLATTEN_EOD

**Status:** FIXED
**Severity:** Medium — EOD summary can overstate/understate session trade count and P&L in the same trading day

### Symptom

When `close_all` (dashboard command file, operator command, or auto-stop signal) was used while
positions were still open, Telegram and dashboard replay could report `FLATTEN_EOD` early with a
partial trade count, then suppress a later terminal summary even after additional closes happened during
session teardown.

### Root Cause

`flatten_session_positions()` always dispatched `FLATTEN_EOD` for every invocation.

In `scripts/paper_live.py`, admin/early-stop paths already call `flatten_session_positions()`
before setting `complete_on_exit`, and the final `finally`/terminal cleanup path also calls it again.
The session can accumulate more closures between the first and final call.

### Fix Applied 2026-04-28

- `engine/paper_runtime.py`: added `emit_summary: bool = True` to `flatten_session_positions()`.
  Summary dispatch is now optional.
- `scripts/paper_live.py`: command paths that only request an early stop (`close_all` and
  `manual_flatten_signal`) now call `flatten_session_positions(..., emit_summary=False)`.
- The terminal finalization path still dispatches the summary once with final closed count.
- Added regression coverage in `tests/test_paper_runtime.py::test_flatten_session_positions_can_disable_summary_dispatch`.

### Validation

- Re-ran the relevant behavior path in the live paper drill:
  `close_all` now closes positions and logs per-trade `TRADE_CLOSED`,
  and only the terminal path emits the final `FLATTEN_EOD` summary.

## 2026-04-28 — BUG: Trade retrieval assumed legacy columns on historical paper runs

**Status:** FIXED
**Severity:** Low — dashboard/order summaries display inconsistencies

### Symptom

Loading legacy-style paper/backtest databases that only had minimal `backtest_results`
columns (`run_id`, `symbol`, `trade_date`, `profit_loss`) failed with:

- `Binder Error: Referenced column "entry_time" not found ...`

and archived ledgers could not render in edge-run databases.

### Root Cause

`get_backtest_trades()` in both `BacktestDB` and `MarketDB` used a fixed
`ORDER BY trade_date, entry_time, exit_time, symbol` clause. Some archived or
legacy DBs did not yet have `entry_time` / `exit_time`.

### Fix Applied

- `db/backtest_db.py` and `db/duckdb.py`: build `ORDER BY` dynamically from
  available columns (`trade_date`, then optional `entry_time`, `exit_time`, `symbol`).
- `web/pages/ops_pages.py`: cumulative calculations now sort trade rows using only
  columns present in the loaded DataFrame.

### Impact

- Legacy paper archives and mixed-schema backtest databases now render safely.
- No change to stored P&L values; only ordering/aggregation stability for UI views.

---

## 2026-04-28 — BUG: Paper ledger archived/daily tabs render empty after tab switch

**Status:** FIXED — keep-alive forced for paper ledger tab panels
**Severity:** Low — dashboard-only; no trading/runtime impact

### Symptom

`/paper_ledger` showed active sessions but archived sessions and daily summary were blank after switching tabs.

### Root Cause

`ui.tab_panels` tab content can be lazily recreated by the client, which can invalidate
`archived_content`/`daily_content` component references used by async loader callbacks.

### Fix Applied 2026-04-28

- `web/pages/ops_pages.py`: set `keep_alive=True` on `ui.tab_panels(...)` so tab panel component
  references remain valid across tab switches.

## 2026-04-28 — BUG: Archived paper session shows non-chronological cumulative P/L

**Status:** FIXED
**Severity:** Medium — dashboard reporting-only; no trading/runtime impact

### Symptom

Some archived rows showed cumulative P/L values that did not align with displayed per-trade sequence on
`/paper_ledger` (for example, first row showing a large negative/positive drift while that trade P/L was small).

### Root Cause

`get_backtest_trades()` returned trades ordered by `symbol` then `trade_date`, so dashboard `cum_sum()`
used the wrong sequence when multiple trades shared the same date.

### Fix Applied 2026-04-28

- `db/backtest_db.py` and `db/duckdb.py`: changed default trade fetch ordering to
  `trade_date, entry_time, exit_time, symbol`.
- `web/pages/ops_pages.py`: ledger renderer sorts trades before computing cumulative P/L
  (`trade_date`, `entry_time`, `exit_time`, `symbol`) so on-screen running P/L is deterministic.
- `tests/test_backtest_replica_migration.py`: added regression coverage for returned trade order on both
  `BacktestDB` and `MarketDB`.

---

## 2026-04-28 — BUG: SHORT session archive fails (CHECK constraint on exit_reason — admin-close reason invalid)

**Status:** FIXED — SHORT trades re-archived successfully; paper.duckdb source data remains unchanged
**Severity:** Medium — session PnL data lost from dashboard/backtest view; no trading harm

### Symptom
At 15:15 IST (EOD archive):
```
ERROR db.backtest_db: Failed to store backtest_results:
Constraint Error: CHECK constraint failed on table backtest_results with expression
CHECK((exit_reason IN ('TARGET', 'INITIAL_SL', 'BREAKEVEN_SL', 'TRAILING_SL', 'TIME',
'REVERSAL', 'CANDLE_EXIT', 'TIME_STOP', 'MOMENTUM_FAIL')))
```
SHORT session archive aborted. 40+ trades not written to `backtest.duckdb`.
LONG session (26 trades, already closed at 11:49) archived successfully.

### Root Cause
ZAGGLE SHORT was closed via admin command queue at 10:55 IST (`close_positions` action).
The admin-command close path writes a non-standard `exit_reason` to `paper_positions`
(e.g. `MANUAL`, `OPERATOR_CLOSE`, `close_positions`, or the reason string from the command JSON).
When `archive_completed_session()` copies `paper_positions.exit_reason` → `backtest_results.exit_reason`,
the non-standard value fails the CHECK constraint.

### Fix Applied 2026-04-28

1. `scripts/paper_archive.py` now normalizes paper-only manual/admin close reasons to
   CHECK-safe analytics values:
   - `MANUAL_CLOSE` → `TIME`
   - `MANUAL_FLATTEN` → `TIME`
   - `MANUAL`, `OPERATOR_CLOSE`, `CLOSE_POSITIONS`, `CLOSE_ALL`, `FLATTEN` → `TIME`
2. Added a regression test for a `MANUAL_CLOSE` paper position archiving into
   `backtest_results.exit_reason='TIME'`.
3. Re-archived `CPR_LEVELS_SHORT-2026-04-28-live-kite`.

Validation:
- `CPR_LEVELS_SHORT-2026-04-28-live-kite`: 21 archived rows, net P&L `+4,732.57`
- Archived exit reasons: `BREAKEVEN_SL=13`, `INITIAL_SL=2`, `TARGET=4`, `TIME=2`

### Impact today
- All SHORT session PnL data is safe in `paper.duckdb` (paper_positions table)
- Dashboard cannot display SHORT run from today until re-archived
- Winners affected: NEOGEN +₹1,244 · PATANJALI +₹840 · FEDERALBNK +₹657 · ONMOBILE +₹1,630 · JAMNAAUTO +₹839

---

## 2026-04-28 — BUG: Spurious SESSION_STARTED + duplicate EOD on relaunch of COMPLETED session

**Status:** FIXED — completed sessions are skipped at live-loop startup
**Severity:** Low — false Telegram alerts only; no PnL or position impact

### Symptom
After v5 relaunch at 13:40 IST, operator received:
1. SESSION_STARTED Telegram alert for `CPR_LEVELS_LONG-2026-04-28-live-kite` — already COMPLETED
   since 11:49 IST (sentinel flatten)
2. SESSION_COMPLETED / FLATTEN_EOD for the same LONG session at 13:45 — duplicate of 11:49 alerts
Also: `backtest.duckdb` received a duplicate archive write (26 rows inserted twice for same session).

### Root Cause
`paper_live.py` checks `_was_already_active = getattr(session, "status", "") == "ACTIVE"`.
This guard suppresses SESSION_STARTED only when the session is ACTIVE. A COMPLETED session
returns `_was_already_active = False` → SESSION_STARTED dispatched incorrectly.

The LONG variant then ran `run_live_session()`, processed one bar (13:45), found no positions
and entry window closed → exited with `NO_TRADES_ENTRY_WINDOW_CLOSED` → archive + EOD alert.

### Fix Applied 2026-04-28

- `scripts/paper_live.py`: `run_live_session()` now returns immediately when the session is already
  `COMPLETED` or `CANCELLED` at startup.
- `SESSION_STARTED` is now suppressed for any already-started non-`PLANNING` session, not just
  `ACTIVE`.

---

## 2026-04-28 — BUG: Replica PermissionError crashes live session on Windows (dashboard file lock)

**Status:** FIXED — paper replica sync errors are non-fatal to the trading loop
**Severity:** High — live session crashed at 13:36 IST; 5-min gap 13:35→13:40; positions unmonitored

### Symptom
Engine logged `ERROR db.replica: Replica sync failed for paper` followed by:
```
PermissionError: [WinError 5] Access is denied:
  'data\paper_replica\paper_replica_latest.latest.tmp' ->
  'data\paper_replica\paper_replica_latest'
```
The v4 process (PID 42712) then entered the `_execute_with_retry` retry loop (holding paper.duckdb
but not processing bars) until killed manually. Sessions remained ACTIVE in DB.

### Root Cause
`flush_deferred_sync()` is called in the `finally` block of every 5-min bar group:
```python
finally:
    get_paper_db().flush_deferred_sync()
```
Inside, `_sync_worker` copies the paper DB to a replica file then atomically renames
`paper_replica_latest.latest.tmp` → `paper_replica_latest` (the version pointer file).

The dashboard (NiceGUI, restarted ~11:50 IST) reads `paper_replica_latest` on every poll cycle
to find which replica version to open. On Windows, if the dashboard has `paper_replica_latest`
open (even for reading), `os.replace()` fails with `WinError 5: Access is denied`.

This PermissionError propagates out of `flush_deferred_sync()` → `finally` block →
`run_live_session()` → caught by `_execute_with_retry` → 5 retries × 30s wait → process exits.

### Why it appeared at 13:36 but not earlier
The dashboard was restarted at ~11:50 IST. Fresh dashboard startup may poll the replica pointer
file more aggressively than an already-warmed dashboard. The issue hadn't occurred in the 4+
hours before the restart.

### Recovery (2026-04-28)
- Killed stuck PID 42712 manually (taskkill //F //PID 42712)
- One launch attempt (bvjnsqoo3) blocked by a surviving retry-loop subprocess (PID 40936)
- Second relaunch (bvjnsqoo3 as PID 40936) connected successfully; SHORT session resumed at 13:40
- Gap: 13:35→13:40 (5 min unmonitored)

### Fix Applied 2026-04-28

- `db/paper_db.py`: `force_sync()` and `flush_deferred_sync()` now catch replica sync exceptions,
  log a warning, and keep the live trading loop running.
- Replica freshness may lag briefly if Windows blocks the pointer file, but trading state remains
  authoritative in `paper.duckdb`.

---

## 2026-04-28 — BUG: Dashboard backtest DB connection loops on 'closed pending query result'

**Status:** FIXED — dashboard DB executor serialized
**Severity:** Medium — WARNING log spam every few seconds; run_metadata shows empty params in UI

### Symptom
Dashboard (`pivot-dashboard`, :9999) repeatedly logs:
```
Failed to fetch run_metadata for run_id=CPR_LEVELS_SHORT-2026-04-27-live-kite
  (will retry with fresh connection): Invalid Input Error:
  Attempting to execute an unsuccessful or closed pending query result
```
Dashboard remains accessible. Affected run_id shows empty params. Live session unaffected.

### Root Cause
`_fetch_run_metadata_sync` (`web/state.py:492`) retries with `close_dashboard_backtest_db()` but
the fresh connection also fails — likely a threading race: two async UI coroutines sharing the same
DuckDB connection object mid-query. The "closed pending query result" error means one task's query
state was clobbered by another task's query on the same connection before `.fetchone()` completed.

### Confirmed (code search 2026-04-28)
`grep -rn "closed pending query"` returns **zero matches** across the entire codebase. The error is
never explicitly caught anywhere — only the generic `except Exception` in `_fetch_run_metadata_sync`
handles it. No thread-safety guard exists on `get_dashboard_backtest_db()`.

### Fix Options
1. Add `threading.Lock` around `get_dashboard_backtest_db()` calls in `web/state.py`.
2. Restart the dashboard PID only (find with `tasklist | grep python`; kill only that PID — NOT
   `taskkill //IM python.exe //F` which kills the live session too).
   **Workaround applied 2026-04-28**: user restarted dashboard; errors stopped.

### Fix Applied 2026-04-28

- `web/state.py`: dashboard DB executor changed from `max_workers=3` to `max_workers=1` so the
  singleton DuckDB replica connections are not queried concurrently.
- `web/pages/ops_pages.py`: `/paper_ledger` now has separate tabs for Active Sessions, Archived
  Sessions, and Daily Summary. The 3-second near-real-time timer refreshes Active Sessions only;
  Archived Sessions and Daily Summary refresh only when opened or manually refreshed.

### Log files preserved for crash analysis
- `.tmp_logs/live_20260428_v2.log` — 261 lines, 9:27→10:05 IST (crashed 10:07, exit code 1)
- `.tmp_logs/live_20260428_v3.log` — 163 lines, 10:10→11:20 IST (crashed 11:24, exit code 1)
- `.tmp_logs/live_20260428_v4.log` — active from 11:25 IST onward
- `.tmp_logs/crash_v2_task_output_bk6zyeyyb.txt` — empty (stdout redirected to v2 log)
- `.tmp_logs/crash_v3_task_output_byeelra2i.txt` — empty (stdout redirected to v3 log)
- `.tmp_logs/crash_startup_task_output_bogp1h9j5.txt` — "PID: 4066" only (double-bg artifact)

---

## 2026-04-28 — BUG: send-command CLI fails while live session is running (paper.duckdb locked)

**Status:** FIXED — file-only command path skips paper.duckdb startup cleanup
**Severity:** Medium — operator cannot use `send-command` CLI during active trading; must fall back to manual file writes

### Symptom
```
doppler run -- uv run pivot-paper-trading send-command --session-id <id> --action close_positions --symbols <SYM>
```
Exits with code 1 immediately:
```
[STARTUP BLOCKED] paper.duckdb is locked by PID 50600
IO Error: Cannot open file "data/paper.duckdb": being used by another process.
```

### Root Cause
`paper_trading.py:main()` calls `_pdb().cleanup_stale_sessions()` unconditionally at startup for ALL
subcommands — including `send-command`, which only needs to write a JSON file to a directory and never
needs a DB connection. The exclusive DuckDB lock held by the live process blocks this.

### Fix Applied 2026-04-28

- `scripts/paper_trading.py`: `send-command` now skips startup `cleanup_stale_sessions()`, so it
  writes directly to the command queue without opening `paper.duckdb`.
- `write_admin_command()` remains the only path needed for this subcommand.

### Workaround (confirmed working 2026-04-28)
Write the command JSON file directly to the command directory:
```bash
mkdir -p ".tmp_logs/cmd_<session_id>"
echo '{"action":"close_positions","symbols":["SYMBOL"],"reason":"manual","requester":"operator"}' \
  > ".tmp_logs/cmd_<session_id>/$(date +%s)_close.json"
```
Engine polls the directory every ~1s, processes the file, closes the position, sends TRADE_CLOSED
Telegram alert, and deletes the file. Session continues with remaining positions unaffected.

### Verified behaviour (2026-04-28 10:55 IST)
- ZAGGLE SHORT closed by admin command within ~25s of file creation ✅
- Telegram TRADE_CLOSED alert delivered (HTTP 200 OK) ✅
- Command file deleted after processing ✅
- Other 5 SHORT positions continued trading uninterrupted ✅

---

## 2026-04-28 — BUG: Silent mid-session crash at 10:07 IST — exit code 1, no traceback, orphaned positions

**Status:** RESUMED — relaunched at 10:10 IST; sessions reused ACTIVE; open positions being monitored
**Severity:** High — 5-min gap (10:05–10:10 IST) with unmonitored open positions; auto-flatten did NOT fire

### Timeline
- 10:05:04 IST — last log line (BAJAJHLDNG SHORT entry, NEOGEN TARGET close, all Telegram 200 OK)
- 10:05–10:07 IST — process died silently, exit code 1, no traceback in log
- 10:07 IST — background task `bk6zyeyyb` notified as failed
- 10:07 IST — paper.duckdb unlocked (no WAL), sessions still ACTIVE in DB (no status update)
- 10:10 IST — relaunch detected ACTIVE sessions, connected KiteTicker, resumed monitoring
- 10:10 IST — no SESSION_STARTED alerts (duplicate dedup fired correctly)

### What we know
- Last TICKER_HEALTH at 10:05: `connected=True, ticks=412987, reconnects=0, coverage=98%`
- No ERROR, Exception, Traceback, or ASYNC_FATAL_EXCEPTION in the log
- Auto-flatten did NOT fire — same failure mode as 2026-04-27 9:30 crash (process killed before `finally` block)
- PnL at crash time: SHORT +₹468 (closed), LONG −₹3,525 (closed); unknown OPEN positions

### Likely root cause (under investigation)
This is the second silent crash in 2 sessions. Both share the same pattern:
- Session was processing normally (no Kite disconnect, no errors visible)
- Process killed abruptly (exit code 1) without entering the `finally` auto-flatten path
- Correlated with a high-event bar earlier (10 simultaneous opens at 9:30 — rate-limited but may have
  caused slow memory accumulation that OOM-killed the process 35 min later)

Alternative: the `_run_multi_variants` retry logic's outer try/except raised SystemExit(1) after some
internal invariant check failed between bars. Needs post-mortem log analysis.

### Open questions for post-session analysis
1. Check if `_should_retry_variant_exit()` with status=COMPLETED before 10:30 caused the retry loop
   to re-enter `run_live_session()` which then tried to create a new session (conflict with ACTIVE)
2. Check RAM at time of crash — OOM kills don't write to stdout/stderr
3. Add process memory logging every 5 min to catch slow leaks

---

## 2026-04-28 — BUG: Second silent crash at 11:24 IST — identical pattern, exit code 1, no traceback

**Status:** RESUMED — v4 relaunched at 11:25 IST; sessions reused ACTIVE; gap 11:20–11:25 (5 min)
**Severity:** High — recurring silent crash; 3rd crash today across 2 process instances

### Timeline
- 11:20 IST — last LIVE_BAR logged for both sessions (healthy: connected=True, coverage=100%)
- 11:20–11:24 IST — process died silently, exit code 1, no traceback, no error in log
- 11:24 IST — background task `byeelra2i` notified as failed; WAL file present at 11:24
- 11:25 IST — v4 relaunched, sessions reused, connected in <1s, resuming

### Pattern (all 3 crashes today)
| Crash | Time | Last healthy log | Gap | Launch |
|-------|------|-----------------|-----|--------|
| v2 | 10:07 IST | 10:05 bar | 5 min | v3 at 10:10 |
| v3 | 11:24 IST | 11:20 bar | 5 min | v4 at 11:25 |

Both crashes:
- Process was healthy at last logged bar (connected=True, 100% coverage, 0 stale)
- No ERROR, Exception, Traceback, or async fatal in the log
- Exit code 1 silently
- Auto-flatten did NOT fire
- Sessions remained ACTIVE in DB (no status update)
- Open positions orphaned for ~5 min then recovered on relaunch

### Suspected cause: `_execute_with_retry` retry loop + tool background-task timeout
The Claude Code Bash tool with `run_in_background: true` may have an internal session-level
timeout that kills the background task subprocess after a certain duration, regardless of
whether the actual process is still running. Both crashes occurred at irregular intervals
(40 min for v2→v3, 74 min for v3→v4) with no visible trigger in the application logs.

Alternative: The `_execute_with_retry` loop detected a variant exit (for an internal reason
not written to stdout, e.g. an exception in a non-logging code path) and exhausted 5 retries.
The retry loop itself exits with `sys.exit(1)` or equivalent.

### Immediate action
On each crash: kill old Monitor, relaunch (no &, run_in_background:true), arm new Monitor on vN log.
The ACTIVE session reuse path works cleanly — resume is reliable even after silent crashes.

### Observability Fix Applied 2026-04-28
`scripts/paper_trading.py` now logs retry decisions and final unhealthy/exceptional variant exits via
`logger.warning` / `logger.error`, not only `print()`. If stdout detaches again, the log still records
the retry-loop reason, attempt number, and final failure status.

`pivot-paper-supervisor` was added as an external parent-process launcher for the next live-paper
session. It starts `daily-live` as a child process with `PYTHONUNBUFFERED=1` and
`PYTHONFAULTHANDLER=1`, writes child stdout/stderr to `.tmp_logs/supervisor/`, and records heartbeat
JSONL events containing:

- child PID and command
- heartbeat timestamps and elapsed seconds
- stdout/stderr byte counts
- Windows process memory counters when available
- final return code and stdout/stderr tails

This does not prove the root cause is fixed. It makes the next recurrence diagnosable.

---

## 2026-04-28 — OPS: Session stuck in PLANNING, no WebSocket connect, no trades (early-launch + stdout-detach)

**Status:** FIXED — Kite live starts now fail fast before 09:16 unless explicitly opted into waiting
**Severity:** High — ~75 minutes of live trading lost (9:15–9:27 IST); no first-bar entries captured

### Timeline (2026-04-28)
- 08:58 IST — `daily-live --multi` launched via Claude Code Bash tool with `run_in_background: true` and `&` in command
- 08:58 IST — Sessions pre-created as PLANNING, process sleeping 1066s until 09:16 IST
- 08:58 IST — Log file frozen at 3 lines (pre-create × 2 + waiting message); paper replica frozen at v5249
- 09:16 IST — Kite connect should have fired; no new log lines, session still PLANNING in DB
- 09:20 IST — Monitoring loop woke up, detected PLANNING (not ACTIVE), log frozen at 08:58
- 09:21 IST — User confirmed: no SESSION_STARTED alerts, no trades, session lost
- 09:21 IST — PID 45520 (1.17 GB RAM, alive) killed manually; no relaunch

### Probable Root Causes (two compounding)

**Root Cause 1 — stdout detachment on Windows with double backgrounding:**
`run_in_background: true` on the Claude Bash tool causes the tool's shell to exit after
spawning the command. The `&` inside the command creates a second layer of background
detachment. On Windows Git Bash, the child process may lose its stdout/stderr file
descriptors when the parent shell exits. Result: the process runs (PID 45520, 1.17 GB),
but no output reaches `.tmp_logs/live_20260428.log` — and more importantly, any crash
or timeout during Kite connect happens silently with no log trace.

**Root Cause 2 — early launch leaves a window for Kite connect failure:**
Launched at 08:58 IST → internal sleep until 09:16 IST (~18 min). Even though the sleep
is shorter than yesterday's 2.5-hour window, any Kite WebSocket connect failure during
the 30s timeout fires silently (stdout detached). With no log and no DB write, the
monitoring loop cannot distinguish "still sleeping" from "connect failed and died."

**Note:** The 30s Kite connect timeout fix (applied 2026-04-27) would cause the process
to raise `ConnectionError` and die — but since stdout/stderr were detached, neither the
log file nor stderr shows anything. The session stays PLANNING in DB (no close write).

### What was NOT the cause
- Data readiness: verified YES at 07:43 and 08:45 IST ✅
- Universe: `full_2026_04_28` (2015 symbols) existed and was correct ✅
- Kite token: confirmed valid at 06:38 IST (REST OK, WebSocket ticks received) ✅
- DuckDB locks: none at launch ✅

### Fix Options

1. **Never use `run_in_background: true` + `&` together on Windows** — pick one:
   - Use `run_in_background: true` without `&` (tool manages background; shell stays alive, output preserved)
   - Use `&` with the tool in foreground, tail the log in a separate Monitor arm
2. **Launch AFTER 9:15 IST** — zero sleep window, Kite connect happens immediately when the
   live 9:15 candle is being built. Any connect failure is visible within 30s. User's note:
   *"should not start early"* — confirmed approach for future sessions.
3. **Add a liveness probe**: Monitor the paper replica timestamp and alarm if sessions stay
   PLANNING for >3 min past 09:16 IST (would have caught this at 09:19 IST).
4. **Log to stderr as well**: Ensure the process writes a startup banner to stderr so even
   if stdout is detached, the tool's stderr capture shows something.

### Complete Root Cause (3 layers — confirmed by code inspection)

**Layer 1 — Kite server overload at market open (primary failure trigger):**
The process slept 1066s from 8:58 AM, waking at exactly 9:15:46 IST — the peak WebSocket
connection moment when thousands of Kite users connect at market open. The 30s connect
timeout (added 2026-04-27 in `6049e59`) fired before Kite's server responded. `ConnectionError`
raised from `KiteTickerAdapter.connect()` at ~9:16:16 IST.
Evidence: Second launch at 9:27 IST (12 min after open, past peak load) connected in <1s.

**Layer 2 — Retry loop kept process alive but retrying (amplifier):**
`retry_on_early_exit=True` in `_run_multi_variants()` (`scripts/paper_trading.py:1381`) catches
all exceptions from `_execute_with_retry()`. `ConnectionError` from layer 1 triggered 5 retry
attempts with wait times 10+20+30+40+50s = 150s between attempts, plus 5×30s connect timeouts
= 300s total. Process alive from 9:15:46 → 9:20:46 IST — exactly when monitoring loop checked
at 9:20 IST and found the 1.17GB PID still alive but sessions still PLANNING.
Code path: `ConnectionError` → caught at `paper_trading.py:1150` → `_should_retry_variant_exit()`
→ retry → all 5 retries fail at peak load → process exits at ~9:20:46 IST.

**Layer 3 — stdout detached by double-backgrounding (masked all evidence):**
First launch used `run_in_background: true` on the Bash tool AND `&` in the command. On
Windows Git Bash, when the tool's shell exits after spawning the `&` background job, the
background process's stdout FILE DESCRIPTOR may be closed or detached from the log file.
Evidence: The pre-create banner (printed BEFORE the sleep) appeared in the log; the "Launching
2 variant(s)..." banner (printed AFTER the sleep at line 1107 with `flush=True`) did NOT appear.
All retry-loop print statements (5 restart messages, 5×30s timeout messages) also absent.
Without stdout, operators had no visibility that 5 retries were in progress.

**Interaction between layers 2 and 3 (new after Apr 27):**
Before `6049e59` (Apr 27), `_connected.wait()` had NO timeout. The process hung indefinitely
on the first Kite connect attempt — retries never triggered. Operators would see a hung process,
kill it, and relaunch. After `6049e59`, the timeout fires, retry loop fires, and the process
appears "busy" (alive, 1.17GB) for ~5 minutes while silently failing — more confusing, not less.

**Why "last week this hasn't happened":**
Last week there was no 30s timeout. The process either (a) connected successfully within the
wait because server load was lower on that particular day, or (b) hung and the operator killed
it manually. After Apr 27's timeout fix, the failure mode changed from "hang" to "silent retry
loop" — same root cause (early launch hitting peak server load), different visible behaviour.

### Lesson for Future Sessions
- **Launch AFTER 9:16 IST** (or as close to 9:16 as possible). `_wait_until_market_ready()`
  returns immediately if `now >= 09:16`, so there is zero pre-market sleep and zero window
  for peak-load Kite connect failures. Today's 9:27 AM launch proved this works.
- **Never use `run_in_background: true` WITH `&`** in the same Bash tool call on Windows.
  Pick one: `run_in_background: true` alone (tool manages background, stdout connected), or
  `&` alone (foreground tool, stdout stays in shell, manually monitor with Monitor tool).
- **Add "still PLANNING after 3 min past 09:16" check** to the monitoring loop so a silent
  retry-loop failure is caught within 3 minutes instead of being discovered at 9:20+.
- Consider increasing `KiteTickerAdapter.connect()` timeout from 30s to 60s to survive the
  peak market-open load window, or add a short pre-connect back-off retry within `connect()`
  itself before propagating `ConnectionError` to the outer retry loop.

### Fix Applied 2026-04-28

- `scripts/paper_trading.py`: `daily-live --feed-source kite` now fails fast before `09:16 IST`
  on the trade date.
- Operators can still intentionally opt into the old sleep behavior with `--wait-for-open`, but
  the default path prevents hidden pre-market wait/retry loops.

---

## 2026-04-28 — BUG: Dashboard archived-paper metadata fetch can fail on concurrent DuckDB reads

**Status:** FIXED
**Severity:** Medium — dashboard page can show error banners even though the dashboard process is still running

### Symptom

Dashboard showed:

```text
Failed to fetch run_metadata for run_id=CPR_LEVELS_SHORT-2026-04-27-live-kite (will retry with fresh connection): Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result
```

The dashboard was still listening on port `8501`; this was not a full process crash.

### Root Cause

The archived paper ledger UI calls `aget_run_ledger()` and `aget_run_metadata()` concurrently via
`asyncio.gather()`. Both calls run in the shared dashboard thread pool and use the same singleton
read-only DuckDB backtest replica connection.

DuckDB connections are not safe for overlapping queries from multiple threads. Concurrent reads can
leave the connection with a closed pending result, which then causes the metadata fetch warning and
retry path.

### Fix Applied 2026-04-28

- `web/state.py`: serialized dashboard DB executor work by changing the shared dashboard executor
  from `max_workers=3` to `max_workers=1`.
- This keeps UI DB reads queued instead of running overlapping queries on the singleton DuckDB
  connections.

Validation:
- `uv run pytest tests/test_web_state.py -q` → `11 passed`
- `uv run ruff check web\state.py` → clean

### Pending

- Consider a larger dashboard data-access refactor after today's live paper:
  - separate per-worker DuckDB read-only connections, or
  - explicit per-connection locks around backtest, market, and paper DuckDB singletons.
- Add a concurrency regression test that calls archived paper ledger + metadata concurrently and
  asserts no DuckDB pending-result failure.

---

## 2026-04-28 — PARITY: backtest portfolio overlay does not persist/use quality rank like live/replay selector

**Status:** PARTIALLY FIXED — backtest portfolio overlay now uses the shared quality-score primitive for same-time slot pressure; candidate-rank persistence still pending
**Severity:** High — affects max-position / slot-count what-if analysis and can create live-vs-backtest selection drift

### Symptom

After moving live/replay same-bar entry selection to quality sorting, a quick SQL analysis of the
2025-01-01 → 2026-04-27 saved CPR backtests could not perfectly answer whether reducing
`max_positions` from 10 to 5 would improve results.

The saved `backtest_results` rows contain only executed trades. They do not persist:

- rejected same-bar candidates
- candidate quality score
- selected entry rank within the bar
- portfolio slot decision reason/order

### Root Cause

The live/replay bar path uses `select_entries_for_bar()` quality sorting:

```text
quality_score = effective_rr / (1 + or_atr_ratio)
```

The saved backtest portfolio overlay previously sorted executed candidate trades by:

```text
trade_date, entry_time, symbol
```

This means a no-rerun SQL approximation can replay the saved max-10 executed rows, but it cannot
reconstruct the true "top 5 by quality" candidate set because the quality rank and non-selected
candidates were never archived.

Additional mistakes found during review:

- The 2026-04-25 issue was marked `FIXED`, but the fix only covered the shared live/replay selector
  and CPR batch simulation. The later global backtest portfolio overlay still had an older
  time+symbol ordering path.
- The Apr 25 issue text claimed backtest was fully aligned, but no test covered the overlay case
  where two same-time trades compete for one portfolio slot and the alphabetically first symbol is
  lower quality.
- `TradeResult` / `backtest_results` persist `or_atr_ratio` but not the effective RR used by
  the selector, so historical SQL cannot exactly reproduce the selector's quality score.

### Impact

- `max_positions=5` vs `max_positions=10` cannot be confirmed exactly from existing saved rows.
- Live/replay and backtest may select different symbols when more same-bar candidates exist than
  available slots.
- Post-hoc SQL reports can under/over-estimate slot-count changes because they only see already
  executed max-10 rows.

### Fix Needed

1. DONE 2026-04-28: align the backtest portfolio overlay slot selection with quality ordering for
   same-time slot pressure.
2. DONE 2026-04-28: expose the shared `entry_quality_score()` primitive and use it from both
   live/replay candidate selection and backtest overlay ordering.
3. PENDING after today's live paper: persist candidate-level diagnostics for CPR runs:
   - `candidate_quality_score`
   - `candidate_rank`
   - `selected_rank`
   - `selection_status` (`EXECUTED`, `SKIPPED_NO_SLOT`, `SKIPPED_NO_CASH`, etc.)
   - `slot_capital`, `open_slot_count`, `max_positions`
4. PENDING after today's live paper: add a parity test where a same-bar candidate set has more candidates than slots and assert
   backtest, replay, and live-local choose the same symbols in the same order.
5. PENDING after candidate diagnostics: rerun or replay the 5-position / 2L-position-size scenario
   from candidate rows instead of approximating from already-executed max-10 trades.

### Current Workaround

For urgent pre-market analysis, use saved-row approximations only as directional evidence:

- synthetic concurrent cap replay from saved max-10 rows
- first-N-per-day SQL cuts
- scaled notional estimates

Treat these as estimates, not proof. Exact confirmation requires either a rerun with
`max_positions=5` / `max_position_pct=0.20` or candidate-rank persistence.

### Fix Applied 2026-04-28

- `engine/bar_orchestrator.py`: exported shared `entry_quality_score()` and
  `candidate_quality_score()` helpers. Live/replay candidate selection still uses
  `select_entries_for_bar()`, but the score formula is no longer duplicated.
- `engine/cpr_atr_strategy.py`: `_apply_portfolio_constraints()` now orders same-time portfolio
  candidates by reconstructed effective RR using the shared `entry_quality_score()` helper before
  symbol tie-break.
- `tests/test_strategy.py`: added a regression test proving the overlay chooses a higher-quality
  same-time trade over an alphabetically earlier lower-quality trade.
- `tests/test_bar_orchestrator.py`: added a regression test proving nested live/replay candidates
  and scalar backtest inputs share the same scoring primitive.

Validation:
- `uv run pytest tests/test_strategy.py::TestPortfolioExecutionOverlay::test_apply_portfolio_constraints_prioritizes_quality_within_same_entry_time tests/test_strategy.py::TestPortfolioExecutionOverlay::test_apply_portfolio_constraints_can_use_risk_based_sizing tests/test_bar_orchestrator.py -q` → `13 passed`
- `uv run ruff check engine\bar_orchestrator.py engine\cpr_atr_strategy.py tests\test_strategy.py tests\test_bar_orchestrator.py` → clean

---

## 2026-04-27 — BUG: Process crashes silently on max-throughput bar (all 10 positions cycling)

**Status:** FIXED — 4 defensive changes applied (rate-limit, batch sync, exception handler, deferred SESSION_STARTED)
**Severity:** High — silent crash with 20 orphaned open positions; auto-flatten did NOT fire

### Symptom

Live process (PID 32900, 1.1 GB RAM) died silently at 09:30 IST after processing the 09:30 bar.
No traceback in log, no error message, no FLATTEN_EOD. Log ends at 09:30:33 (16th Telegram alert).
`paper.duckdb` unlocked, sessions still showing ACTIVE in DB with 20 open positions orphaned.

### Root Cause

**This is the first session where ALL 10 max_positions in both sessions cycled simultaneously in a single bar.**

At the 09:30 bar close:
- 4 LONG INITIAL_SL closes (all 09:25 entries stopped in 1 bar)
- 3 SHORT INITIAL_SL closes + 1 SHORT TARGET close
- 4 LONG new opens + 4 SHORT new opens
= **16 position events in one bar** (the maximum possible)

Each event triggers:
1. A DuckDB write (`paper_positions`)
2. A replica sync attempt (`maybe_sync(source_conn=...)`)
3. A Telegram HTTP POST via `httpx` (async)

All 16 Telegram alerts fired in 30 seconds (09:30:04 → 09:30:33). Combined with:
- 590-symbol tick data held in memory (~1.1 GB baseline)
- DuckDB replica copy of 410MB paper.duckdb triggered on first write post-restart
- 16 async httpx response objects in flight simultaneously

This caused a memory/asyncio overload that killed the process without triggering the `finally` block
(so `auto_flatten_on_abnormal_exit=True` did NOT run — positions were orphaned).

### Why it never happened before (root cause of timing)

Two compounding factors made today the first time this threshold was crossed:

**1. Quality-sort (added 2026-04-25) increased portfolio correlation.**
Before quality-sort, the 10 max_positions slots were filled in alphabetical order — symbols with
no natural relationship, exits spread across many bars. After quality-sort (`effective_rr /
(1 + or_atr_ratio)`), all 10 slots go to the highest-RR setups. These are structurally similar
(narrow CPR + strong OR) and respond to the same intraday market move — so when the market
reverses sharply, they ALL hit INITIAL_SL at the same bar rather than staggering over 10+ bars.

**2. First-bar ISL on all 10 is a low-probability event even with correlation.**
On most days entries are staggered across multiple bars (not all 10 fill at 09:25). Today all
10 filled at the 09:25 bar AND all stopped out at 09:30 — probability is low but no longer zero
once the portfolio is quality-sorted into correlated setups. Normal days with
mixed TARGET/TRAIL/SL exits across 10+ bars never reach the 16-event-per-bar ceiling.

**Short version:** Quality-sort made the worst case more likely. A single strong counter-move at
09:30 was enough to trigger it for the first time.

### Contributing Factor: SESSION_STARTED alert on resume sends duplicate alerts

When relaunched, `run_live_session()` sends SESSION_STARTED alert even for reused ACTIVE sessions.
This is not a crash cause but causes user confusion (appears as a "new session" on Telegram).

### Timeline (2026-04-27)
- 09:23:28 IST — sessions start (ACTIVE), KiteTicker connected
- 09:25 — 10 LONG + 10 SHORT opens (20 Telegram alerts)
- 09:30 — 8 closes + 8 opens = 16 events (16 Telegram alerts)
- 09:30:33 — last log line (16th Telegram alert)
- 09:30–09:38 — process dead, log frozen, DB unlocked, 20 open positions orphaned
- 09:41 — manual relaunch, sessions reused (ACTIVE), KiteTicker reconnected

### Fix Options
1. **Rate-limit Telegram dispatches**: Queue alerts, max 3-4 concurrent HTTP requests per bar.
2. **Batch replica sync**: Collect all bar writes, call `force_sync()` ONCE per bar end (not per write).
3. **Add asyncio exception handler**: `loop.set_exception_handler(...)` to log fatal errors before dying.
4. **Guard `run_live_session` with `auto_flatten` on `finally`**: Ensure FLATTEN_EOD fires even on OOM.
5. **Reduce memory**: Don't cache all 590 tick builders in memory — lazy-init per session symbol set.

### Fixes Applied (2026-04-27)

All 7 fixes landed in the same session. See git diff for full details.

| # | Fix | File | What changed |
|---|-----|------|-------------|
| 1 | Rate-limit Telegram | `engine/alert_dispatcher.py` | Class-level `asyncio.Semaphore(3)` + 200ms `INTER_SEND_DELAY` between sends in `_consumer_loop` — max 3 concurrent HTTP POSTs at any time |
| 2 | Batch replica sync | `db/paper_db.py` | `defer_sync()` / `flush_deferred_sync()` — `_after_write` marks dirty but skips `maybe_sync()` while deferred; single `force_sync()` fires at bar end |
| 2 | Batch replica sync (call site) | `scripts/paper_live.py` | `defer_sync()` before `process_closed_bar_group()`, `flush_deferred_sync()` in `finally` |
| 3 | Async exception handler | `scripts/paper_live.py` | `loop.set_exception_handler()` at `run_live_session()` entry — logs fatal async errors before process death |
| 4 | Duplicate SESSION_STARTED | `scripts/paper_live.py` | `_was_already_active` flag skips alert dispatch when session status is already ACTIVE (resume scenario) |
| 4 | Duplicate SESSION_STARTED (dedup set) | `engine/paper_runtime.py` | `_session_started_sent: set[str]` in-process dedup — same session_id never dispatches twice in same process |
| 5 | flatten-all alert delivery | `engine/paper_runtime.py` | `shutdown_alert_dispatcher` awaits `_background_tasks` with `gather()` instead of cancelling — FLATTEN_EOD/TRADE_CLOSED now deliver |
| 6 | Live PnL accumulation | `engine/paper_runtime.py` | `_accumulate_session_pnl()` called after every `CLOSED` write — `paper_sessions.total_pnl` updates in real-time during live trading |
| 7 | Kite connect timeout | `engine/kite_ticker_adapter.py` | Timeout 15s → 30s; `self.close()` called before raising `ConnectionError` (previously the WebSocket thread was orphaned on timeout) |

**Post-fix regression introduced and fixed same session:**
During fix #1, the `AlertDispatcher.__init__` was accidentally deleted (new class constants inserted
at the same position). The class had no constructor — any instantiation would have raised
`AttributeError` on first attribute access. Caught on review and restored before any trading.
Verified with `uv run python -c "from engine.alert_dispatcher import AlertDispatcher, AlertConfig; d = AlertDispatcher(None, AlertConfig())"`.

**Remaining open:** The 09:35/09:40 bar gap (see separate OPS entry below) is architectural — no
post-hoc fix for today. Missed-bar reconciliation on resume is the planned mitigation.

---

## 2026-04-27 — BUG: flatten-all does not dispatch FLATTEN_EOD or TRADE_CLOSED alerts

**Status:** FIXED — shutdown_alert_dispatcher now awaits background tasks instead of cancelling them
**Severity:** High — operator has no Telegram confirmation that positions were closed

### Symptom
`pivot-paper-trading flatten-all --trade-date today --notes "..."` ran successfully:
- Sessions marked COMPLETED ✅
- archive_completed_session ran (rows written to backtest.duckdb) ✅
- `AlertDispatcher started` logged at 10:30:57 IST ✅
- NO `FLATTEN_EOD` or `TRADE_CLOSED` entries in `alert_log` ✗
- NO Telegram alerts received by operator ✗

### Root Cause (suspected)
`flatten-all` runs in a short-lived `asyncio.run()` process. `flatten_session_positions()`
dispatches alerts as async fire-and-forget HTTP POST tasks (via `httpx`). When `asyncio.run()`
exits after all synchronous work completes, any pending async HTTP tasks are cancelled before
they can deliver. The `alert_log` never receives the write because the DB write is also async.

Unlike the live engine (which runs a long-lived event loop), the CLI flatten command doesn't
await the alert dispatch queue to drain before returning.

### Workaround
After any `flatten-all` or `flatten` command, run:
```bash
doppler run -- uv run pivot-paper-trading resend-eod --session-id CPR_LEVELS_LONG-<date>-live-kite
doppler run -- uv run pivot-paper-trading resend-eod --session-id CPR_LEVELS_SHORT-<date>-live-kite
```
`resend-eod` uses its own event loop and awaits dispatch before returning.

### Files to Fix
- `scripts/paper_trading.py` — `_cmd_flatten`, `_cmd_flatten_all`: await alert queue drain
  before returning (e.g., `await dispatcher.flush()` or `asyncio.gather(*pending_tasks)`)

---

## 2026-04-27 — BUG: Dashboard shows PnL=0 for ACTIVE sessions (no real-time total_pnl update)

**Status:** FIXED — _accumulate_session_pnl updates total_pnl on every position close
**Severity:** High — operator cannot track live session PnL from dashboard during trading

### Symptom
Dashboard showed **Rs0** PnL for both ACTIVE sessions throughout the trading day (09:23–10:30).
After flatten+archive, it correctly showed LONG +Rs6,494 and SHORT −Rs566. The field is correct
at EOD but useless during the session.

### Root Cause
`paper_sessions.total_pnl` is written ONLY by `archive_completed_session()` which runs at
session end. During a live ACTIVE session, no code path updates `total_pnl` on position close.
The dashboard reads this field → shows 0.0 for the entire trading day.

This is a different manifestation of the Apr 24 bug. That bug was about COMPLETED sessions
showing 0.0 because archive wasn't called. Today's bug is about ACTIVE sessions showing 0.0
because the field is never updated in real-time — even when archive DOES eventually run.

### Fix Options
1. **Update `total_pnl` on every position close** in `paper_runtime.py` via an `UPDATE paper_sessions` call.
2. **Dashboard fallback**: when `total_pnl=0.0` AND session is ACTIVE, compute from
   `SUM(paper_positions.pnl) WHERE status='CLOSED'` on the fly.
3. **Add `daily_closed_pnl` live field**: a running counter updated per close event, separate
   from the archive-populated `total_pnl`.

Option 2 is safest (no DB write path changes) and can be added to the dashboard query layer.

---

## 2026-04-27 — OPS: 09:35 and 09:40 bars unmonitored during engine downtime (gap in position management)

**Status:** OPEN — architectural limitation; no post-hoc fix for today
**Severity:** Medium — 20 open positions unmonitored for 11 min; exits deferred to 09:45 bar

### Symptom

Engine dead 09:30:33→09:41:22 IST. The 09:35 and 09:40 bars closed with no engine running.
Any INITIAL_SL, TARGET, or TRAIL hits that should have triggered at those bars were silently
skipped. All 20 open positions held until 09:45, when the restarted engine processed them.

### Impact
- Positions that hit SL at 09:35/09:40 but recovered by 09:45 → false hold (potentially better)
- Positions that hit SL at 09:35/09:40 and continued deteriorating → closed at worse 09:45 price
- Positions that hit target at 09:35/09:40 but fell back → missed profit
- 0 Telegram alerts during gap window (expected — engine not running)

### Alert Tally (full day ~09:55 IST)
| Alert Type | Sent | Expected | Match |
|------------|------|----------|-------|
| SESSION_STARTED | 4 | 4 (2 relaunches × 2 sessions) | ✅ |
| TRADE_OPENED | 38 | 38 (total positions ever opened) | ✅ |
| SL_HIT | 20 | 20 (ISL + BREAKEVEN + TRAIL) | ✅ |
| TRADE_CLOSED | 2 | 2 (TARGET hits only) | ✅ |
| **HTTP 200 total** | **64** | **64** | ✅ |

No alerts dropped or lost. Perceived shortfall was because no events fired during 09:30–09:41 gap.

### Fix Options
1. On restart, back-process missed bars via `intraday_day_pack` for the gap window.
2. On restart, immediately mark any open positions past SL/target as closed at last known price.
3. Add missed-bar reconciliation step to `run_live_session` startup when resuming ACTIVE session.

---

## 2026-04-27 — OPS: Dashboard shows wrong PnL (reads paper_sessions.total_pnl = 0.0)

**Status:** FIXED — same fix as BUG above (_accumulate_session_pnl on every close)
**Severity:** Medium — misleading dashboard display during live session

### Symptom
Dashboard showed **+Rs9,000** while actual closed PnL was **−Rs3,063** (LONG −955 + SHORT −2,108).

### Root Cause
`paper_sessions.total_pnl = 0.0` for both ACTIVE sessions. The field is only written by
`archive_completed_session()`, which runs on session end — not during live trading.
Dashboard reads `paper_sessions.total_pnl` → sees 0.0 → displays incorrectly.
The +9K figure likely came from baseline backtest runs displayed on the same dashboard panel.

### Source of Truth (always correct for live sessions)
```sql
SELECT session_id,
       COUNT(*) FILTER(WHERE status='CLOSED') as closed,
       ROUND(SUM(pnl) FILTER(WHERE status='CLOSED'),0) as closed_pnl
FROM paper_positions WHERE session_id LIKE '%2026-04-27%'
GROUP BY session_id;
```
As of ~09:55 IST: LONG 8 closed −Rs955 | SHORT 14 closed −Rs2,108 | **Combined: −Rs3,063**

### Fix
See 2026-04-24 fix options — add live `total_pnl` accumulation to `paper_sessions` on each
position close, or have dashboard fall back to `paper_positions` sum when `total_pnl=0`.

---

## 2026-04-27 — OPS: daily-live hangs silently on Kite connect after WiFi network change

**Status:** FIXED — KiteTickerAdapter connect timeout increased to 30s with auto-close on timeout
**Severity:** Medium — session misses the first 09:16–09:20 window but recovers on restart

### Symptom

`daily-live --multi` launched at 06:52 IST. Sessions pre-created (PLANNING) successfully.
Process slept until 09:16 IST (8526s). WiFi network changed during the sleep window (~08:xx IST).
After sleep ended, the process attempted Kite WebSocket connection with the new network interface.
Process hung silently — no new log output, no DB writes, no exception. `paper.duckdb` remained
locked by PID 7084 (385MB RAM, alive). Sessions stayed PLANNING in the replica.

Kite API token was valid (HTTP 200 `/user/profile`). The hang occurred in the post-sleep
startup path (`_run_multi_variants` → `prepare_runtime_for_daily_paper` or
`_resolve_cli_symbols`) — no print statements reached.

### Timeline (2026-04-27)
- 06:52 IST — launched, sessions pre-created (PLANNING)
- 06:53 IST — paper.duckdb last modified (session creation writes)
- 09:14-09:16 IST — sleep ended, Kite connect attempted; WiFi had changed
- 09:16-09:21 IST — process hung (7 log lines, PID alive, DB locked, no new writes)
- 09:21 IST — kill attempted (PID 7084 not found — process had crashed on its own)
- 09:21 IST — DB confirmed unlocked; sessions still PLANNING
- 09:21 IST — relaunch with same `--universe-name full_2026_04_27`
- 09:23 IST — sessions transitioned ACTIVE; trading resumed (missed first 09:20 bar)

### Root Cause (suspected)
Network interface change invalidated the active TCP connection used by the Kite WebSocket
handshake. The `asyncio` event loop's TCP connect attempt blocked indefinitely rather than
timing out (no connect timeout configured in `KiteTickerAdapter` or underlying `websocket-client`).
The process eventually crashed without writing a traceback to the log (async context, unhandled).

### Fix Options
1. Add connect timeout to `KiteTickerAdapter` WebSocket initialization.
2. Add explicit `asyncio.wait_for(...)` around the Kite connect call with a 30s timeout.
3. Add a watchdog: if session stays PLANNING for >5 min past 09:16, auto-restart.

### Do NOT
- Launch `daily-live` with an unstable network connection before 09:00 IST.
- Rely on the process staying alive if the network changes during the 09:16 sleep/connect.

---

## 2026-04-25 — PARITY: max_positions slot selection is non-deterministic (quality-sort fix planned)

**Status:** FIXED — quality-sort implemented in `engine/bar_orchestrator.py` with deterministic
symbol tie-break
**Severity:** High — live and backtest systematically selected different symbols when `max_positions=10`
was saturated at the same bar, making trade-level comparison unreliable

### Root Cause (confirmed 2026-04-25)

Two separate problems compound each other:

**Problem 1 — Different data source for 09:15 bar (OR filter flip)**
Backtest uses `intraday_day_pack` (Kite REST API, full trade aggregation). Live uses
`paper_feed_audit` (Kite WebSocket `MODE_QUOTE` ticks, one tick per price change not per trade).
In the first 5 minutes of trading, 50 trades can occur in 200ms while Kite delivers 2-3 ticks.
Result: live tick-built 09:15 bar range is **30-90% smaller** than the REST API bar. Stocks
with large opening moves pass `or_atr_max=2.5` in live (tick range narrow) but are correctly
filtered in backtest (REST range wide). On Apr 24 SHORT: 19 of 27 live-only symbols were
explained by this flip.

**Problem 2 — Different ordering within the 10-position cap (orchestration)**
Both engines cap concurrent positions at `max_positions=10`. When more than 10 symbols
qualify at the same bar close, both must pick 10. Backtest picks alphabetically (deterministic).
Live picks in WebSocket tick-arrival order (non-deterministic, varies by network jitter).
Even with identical candle data, different symbols fill the 10 slots.

This means neither engine is selecting the **best** 10 setups — both are selecting an
**arbitrary** 10 based on ordering that has no strategic value.

### Quantified impact (Apr 24 SHORT, 2026-04-25)

| Comparison | Trades | Symbol overlap | PnL |
|------------|--------|---------------|-----|
| Live kite-live | 32 | — | +₹14,963 |
| Exact-feed backtest (audit feed) | 28 | **47% with live** | +₹11,357 |
| Historical backtest (REST API) | 19 | **16% with live** | +₹15,423 |

The audit feed tripled the symbol overlap (16% → 47%). The remaining 53% gap is purely
Problem 2 (orchestration ordering), not data source.

### Proposed Fix — Quality-Sort Before max_positions Cut

**Core idea:** instead of taking the first N qualifying symbols in arrival/alphabetical order,
rank all candidates in a bar by quality score and take the top N.

**Quality score formula:**
```python
score = effective_rr / (1.0 + or_atr_ratio)
```
- `effective_rr` = (entry − target) / (entry − sl) for SHORT — higher is better
- `or_atr_ratio` = (bar_high − bar_low) / ATR — lower is better

This penalises wide-OR exhausted moves (the primary filter-flip symbols) and rewards tight
opening ranges with good reward/risk. On Apr 24 SHORT, CRAFTSMAN (or_atr=2.39, TARGET +₹1,563)
would outrank AGARIND (or_atr=5.97, INITIAL_SL) and claim a slot first.

**Performance impact on live:** negligible. `entry_candidates` per bar is 10-100 items.
Python's `sorted()` on 100 dicts takes microseconds — the bottleneck is always DB writes
per position open, not iteration.

**Expected parity after fix:**
- Backtest and live both use quality-sort → same symbol selected for the same slot
- With audit feed: expected overlap jumps from ~47% to ~85-95%
- With historical REST feed: still ~40-60% (data source difference remains)

### Files Changed

| File | Change |
|------|--------|
| `engine/paper_session_driver.py` | Sort `entry_candidates` by quality score before `select_entries_for_bar` |
| `engine/cpr_atr_strategy.py` | Same sort in batch backtest per-bar entry selection |
| `engine/cpr_atr_shared.py` | `select_entries_for_bar`: accept pre-sorted list or sort internally |

### Do NOT

- Apply arrival-order or alphabetical sort as the "fix" — either is deterministic but
  still arbitrary. Quality-sort is the only approach that also improves strategy results.
- Change `max_positions` without testing — reducing it with quality-sort may hurt diversification;
  the cap itself is correct, only the selection within it was wrong.

---

## 2026-04-24 — BUG: Sentinel file flatten silently ignored after entry window closes

**Status:** FIXED — `scripts/paper_live.py`
**Severity:** High — operator control path fails silently during live session

### Symptom
`touch .tmp_logs/flatten_CPR_LEVELS_LONG-2026-04-24-live-kite.signal` created, no
"Flatten signal detected" log appeared after 10+ minutes across multiple bar boundaries.
LONG session stayed ACTIVE. SHORT was unaffected (correct).

### Root Cause
The sentinel check was placed **inside `if cycle_closed:`** (16-space indent). It only
executed when closed candles existed in the current polling cycle. After the entry window
closes, `active_symbols` shrinks to open-position symbols only. If those symbols have no
ticks in a polling window (lunch lull, illiquid scrips), `cycle_closed` is empty and the
sentinel is never checked.

### Fix
Moved sentinel block **outside `if cycle_closed:`** to 12-space indent (while loop body).
It now runs on every poll cycle regardless of whether any bars closed. File:
`scripts/paper_live.py` — sentinel block relocated from inside `if cycle_closed:` to
between the `if cycle_closed:` block and the stale detection logic.

---

## 2026-04-24 — BUG: `flatten` / `flatten-all` CLI leaves dashboard showing ACTIVE

**Status:** FIXED — `scripts/paper_trading.py`
**Severity:** Medium — operator sees stale ACTIVE status in dashboard after successful flatten

### Symptom
After `flatten-all --trade-date today` completed successfully (all positions CLOSED,
sessions COMPLETED in DB), restarting the dashboard still showed both sessions as ACTIVE.
No Telegram alerts were received from `flatten-all` (alerts from the live session itself
were never sent either due to the same replica gap).

### Root Cause
`_cmd_flatten` and `_cmd_flatten_all` write session status COMPLETED via `update_session_state()`
200–500ms after the first write (`flatten_session_positions`). The first write fires
`maybe_sync()` in a fresh process (`_last_sync_time=0`) and succeeds. The COMPLETED write
lands within the 5-second debounce window → `maybe_sync()` returns early → the replica
captures positions closed but session still ACTIVE.

`resend-eod` (separate process) does write to `alert_log` → `_after_write()` → `maybe_sync()`,
and since its process has no prior sync it fires immediately — confirming the debounce is
the root cause, not a missing `_after_write()` hook.

### Fix
Added `_pdb().force_sync()` before the final `print()` in both `_cmd_flatten` (line ~1737)
and `_cmd_flatten_all` (line ~1776) in `scripts/paper_trading.py`. `force_sync()` bypasses
the debounce and guarantees the replica is written before the process exits.
`archive_completed_session` already called `force_sync()` on the backtest replica — only
the paper replica was missing this call.

---

## 2026-04-23 — STRATEGY: CPR SHORT underperforms on mild down days — false breakdown pattern

**Status:** OPEN — backtest experiments queued for post-market
**Severity:** High — recurring losses on SHORT sessions, multiple consecutive days (Apr 21–23)

### Observation

| Date | Index | SHORT WR | SHORT PnL | LONG PnL |
|------|-------|----------|-----------|----------|
| Apr 21 | — | — | -₹2,000 est | positive |
| Apr 22 | mildly bullish (LONG worked well) | 16.7% (5/30) | -₹2,447 | +₹7,085 |
| Apr 23 | Nifty 500 -0.61% (−139 pts) | 8% (2/25 closed) | ~-₹5,500 | +₹3,398 |

On Apr 23 EOD: 25 of 30 SHORT positions closed. **INITIAL_SL: 14/25 (56%). TARGET: 2/25 (8%).**
The strategy is backtested profitable every single month — this is NOT a broken strategy.
It is a regime/execution mismatch.

### Current Parameters (LONG and SHORT identical — no differentiation)

| Parameter | Value | Set in |
|-----------|-------|--------|
| `min_sl_atr_ratio` | **0.5** | `engine/cpr_atr_strategy.py:187` (code default, no preset override) |
| `max_sl_atr_ratio` | **2.0** | `engine/cpr_atr_strategy.py:188` (CLAUDE.md default) |
| `skip_rvol_check` | **True** for SHORT | `strategy_presets.py` `CPR_LEVELS_RISK_SHORT` |
| `skip_rvol_check` | **False** for LONG | `strategy_presets.py` `CPR_LEVELS_RISK_LONG` |
| `momentum_confirm` | **True** (both) | All 4 CPR_LEVELS presets |
| `cpr_min_close_atr` | **0.5** (both) | All 4 CPR_LEVELS presets |

`min_sl_atr_ratio=0.5` means the minimum SL distance allowed is `0.5 × ATR`. With a typical
ATR of ₹2–5 on low-priced stocks (₹100–600), and entry near BC on narrow-CPR days, SL distance
can be as tight as **0.1%–0.3%**. This is hair-trigger in live trading.

### Apr 23 Trade-by-Trade SHORT Data

25 closed positions as of analysis (5 still open). All entries at 09:20–10:05.

| Symbol | Entry | SL% | PnL | Exit Reason | Bars to SL |
|--------|-------|-----|-----|-------------|------------|
| ABFRL | 64.91 | 0.145% | -₹229 | INITIAL_SL | **1 bar** (09:20→09:25) |
| ACI | 595.20 | — | -₹84 | BREAKEVEN_SL | — |
| ADVANCE | 113.93 | — | -₹84 | BREAKEVEN_SL | — |
| AGARIND | 453.73 | — | -₹83 | BREAKEVEN_SL | — |
| APLLTD | 765.32 | 0.109% | -₹192 | INITIAL_SL | **1 bar** (09:20→09:25) |
| ARVSMART | 598.85 | 0.139% | -₹222 | INITIAL_SL | **1 bar** (09:20→09:25) |
| ATUL | 6625.19 | 0.266% | -₹348 | INITIAL_SL | **1 bar** (09:20→09:25) |
| BAJAJCON | 467.80 | 0.524% | -₹605 | INITIAL_SL | 3 bars (09:20→09:35) |
| BALMLAWRIE | 178.71 | 0.131% | -₹215 | INITIAL_SL | **1 bar** (09:20→09:25) |
| BHEL | 332.72 | 0.304% | -₹387 | INITIAL_SL | **1 bar** (09:20→09:25) |
| SPORTKING | 140.61 | — | -₹84 | BREAKEVEN_SL | — |
| LGBBROSLTD | 1779.00 | 0.442% | -₹524 | INITIAL_SL | 3 bars (09:25→09:40) |
| PNBHOUSING | 987.00 | — | -₹83 | BREAKEVEN_SL | — |
| IMFA | 1517.20 | — | -₹83 | BREAKEVEN_SL | — |
| AARTIPHARM | 697.90 | 0.245% | -₹328 | INITIAL_SL | 2 bars (09:30→09:40) |
| AGARWALEYE | 458.75 | 0.503% | -₹584 | INITIAL_SL | 2 bars (09:30→09:40) |
| AUBANK | 1041.25 | 0.271% | -₹355 | INITIAL_SL | 3 bars (09:35→09:50) |
| **GARFIBRES** | 648.43 | — | **+₹1,353** | **TARGET** | — |
| ARIHANTCAP | 71.75 | — | -₹84 | BREAKEVEN_SL | — |
| BERGEPAINT | 474.91 | 0.176% | -₹259 | INITIAL_SL | 5 bars (09:40→10:05) |
| **ARTEMISMED** | 232.21 | — | **+₹1,041** | **TARGET** | — |
| BOSCH-HCIL | 1373.31 | — | -₹83 | BREAKEVEN_SL | — |
| MONTECARLO | 545.75 | 0.330% | -₹413 | INITIAL_SL | 3 bars (09:50→10:05) |
| OMAXE | 82.84 | — | -₹84 | BREAKEVEN_SL | — |
| MUFTI | 79.13 | 0.440% | -₹523 | INITIAL_SL | 4 bars (10:05→10:25) |

**BREAKEVEN_SL (SL%=0.000)**: These hit breakeven stop — price initially moved in favor then returned to entry.
The `-₹83/₹84` loss is purely Zerodha commission (not strategy loss).

Key stats:
- **6 of 14 INITIAL_SL hits** happened in exactly **1 bar** (entry 09:20, stop 09:25)
- SL distances for INITIAL_SL trades: 0.109% to 0.524% (median ~0.27%)
- Two genuine winners: GARFIBRES +₹1,353, ARTEMISMED +₹1,041

### Backtest Monthly Data — SHORT (run `9f0e916bbff0`, Jan 2025–Apr 22 2026, ~2044 symbols)

All 16 months profitable. INITIAL_SL rate stable at **14–29%** — far below today's 56%.

| Month | Trades | Win% | PnL | INITIAL_SL% |
|-------|--------|------|-----|-------------|
| 2025-01 | 321 | 34.6% | +₹103,543 | 20.2% |
| 2025-02 | 245 | 37.1% | +₹121,741 | 23.7% |
| 2025-03 | 202 | 34.2% | +₹89,340 | 18.3% |
| 2025-04 | 164 | 28.0% | +₹26,156 | 23.8% |
| 2025-05 | 223 | 27.8% | +₹32,709 | 29.1% |
| 2025-06 | 244 | 27.9% | +₹22,008 | 24.2% |
| 2025-07 | 371 | 33.2% | +₹87,559 | 18.6% |
| 2025-08 | 362 | 35.4% | +₹87,177 | 14.4% |
| 2025-09 | 387 | 27.6% | +₹52,644 | 26.1% |
| 2025-10 | 423 | 33.1% | +₹86,371 | 23.2% |
| 2025-11 | 374 | 33.4% | +₹93,869 | 20.3% |
| 2025-12 | 477 | 28.7% | +₹62,799 | 23.1% |
| 2026-01 | 400 | 34.0% | +₹82,582 | 20.8% |
| 2026-02 | 415 | 25.1% | +₹60,483 | 22.2% |
| 2026-03 | 196 | 37.2% | +₹70,671 | 21.9% |
| 2026-04 (1–22) | 100 | 25.0% | +₹14,464 | 20.0% |
| **TOTAL** | **4,904** | **31.5%** | **+₹1,094,116** | **~21.8% avg** |

### INITIAL_SL Rate Comparison

| Context | INITIAL_SL Rate |
|---------|----------------|
| Backtest 16-month average | 21.8% |
| Backtest worst month (May 2025) | 29.1% |
| Live Apr 22 | ~40% est |
| Live Apr 23 | **56%** |

The 56% live rate is 2.5× the worst backtest month. This gap is NOT explained by tick/candle
precision alone (that adds ~2–5% noise at most). There is a genuine regime problem on certain days.

### Why Live ≠ Backtest (Tick vs Historical Candle)

**Backtest** reads `intraday_day_pack`: Kite REST historical 5-min OHLCV — exchange-confirmed
canonical candles, delivered batch after market close.

**Live** builds candles from KiteTicker L1 ticks in real-time. Differences:

1. **H/L precision**: Live ticks capture every transient spike; historical OHLCV uses
   exchange-confirmed tick-level H/L. They should match, but on fast-moving opens with
   reconnects, live H may briefly spike above historical H.

2. **Bar boundary edge cases**: A tick at 09:24:59.950 ms that arrives at 09:25:00.050 ms
   due to network latency lands in the next bar live, but is in the 09:20 bar in historical.
   This shifts the candle open for the next bar.

3. **Volume**: Live ticks sum LTP-change events; historical uses exchange-confirmed volume.
   Volume differences affect RVOL checks (for LONG) but not SHORT (skip_rvol=True).

4. **SL sensitivity with 0.1% distances**: At SL=0.109% (APLLTD), a candle H discrepancy
   of ±0.15% changes HOLD → INITIAL_SL or vice versa. Backtest's clean OHLCV is forgiving;
   live ticks' transient spikes are not.

**However**, the Apr 23 gap (56% vs 21%) is primarily a regime issue, not a tick issue.
To confirm: run a single-date backtest for Apr 23 and check its INITIAL_SL rate. If it's
also ~50%+, the market microstructure on this specific day is genuinely different. That's
important because it means the historical 21% average was achieved on days where the SL
was not being tested this aggressively — today is an outlier day, not a system failure.

### Root Cause Summary

**False breakdown pattern on mild-down days:** Stocks qualifying for CPR SHORT have already
gapped down at open. They briefly pierce BC (triggering entry at 09:20) but attract immediate
gap-fill buying, reversing through TC+ATR_buffer in 1 bar. On genuine down days (index -1.5%+),
this reversal doesn't happen — stocks keep trending. On -0.5% days, it reverses almost always.

Three compounding factors:
1. **No RVOL gate** (`skip_rvol_check=True`): Low-volume breakdowns are the most prone to reversal
2. **Hair-trigger SL** (`min_sl_atr_ratio=0.5`): At 0.1%–0.3% distance, any tick noise hits stop
3. **Index magnitude too small**: -0.5% Nifty 500 → individual stocks are not in sustained downtrend

### 2026-04-23 follow-up: common factor in the last 3 live SHORT sessions

Directly querying the live-kite paper sessions for `2026-04-21`, `2026-04-22`, and
`2026-04-23` shows the same repeatable shape across the losing SHORT trades:

- losers are concentrated in the opening window (`09:20`–`09:30`)
- they carry very tight stops (median SL distance ~`0.30%`)
- they tend to have high effective RR (median ~`4.1R`)
- they fail quickly as `INITIAL_SL` or recycle to `BREAKEVEN_SL`

Aggregated over the 3 sessions:

| Bucket | Trades | PnL | WR | Median SL % | Median RR | Avg candles |
|--|--|--|--|--|--|--|
| Losers | 69 | `-₹17,604` | — | `0.298%` | `4.076R` | `6.97` |
| Winners | 13 | `+₹11,870` | — | `0.281%` | `3.325R` | `14.62` |

Early-entry concentration is the strongest signal:

- `60.9%` of losers were entered at `09:20` / `09:25` / `09:30`
- only `23.1%` of winners were that early

By day, the early SHORT book was consistently the problem:

| Date | Early (`09:20`–`09:30`) | Late (`09:35+`) |
|--|--|--|
| `2026-04-21` | `12` trades, `-₹1,276`, WR `8.3%` | `10` trades, `-₹1,472`, WR `20.0%` |
| `2026-04-22` | `15` trades, `-₹2,275`, WR `6.7%` | `15` trades, `-₹173`, WR `26.7%` |
| `2026-04-23` | `18` trades, `-₹2,621`, WR `5.6%` | `12` trades, `+₹2,082`, WR `33.3%` |

Interpretation:

- the issue is not simply "SHORT on up days"
- the repeatable failure mode is **early opening-breakdown SHORTs with tiny stops that do not
  get continuation**
- even on days that later offer downside, the first 1–3 bars are the unreliable part of the
  SHORT book
- this could still be amplified by live-vs-backtest first-bar feed drift, so any eventual fix
  must be validated against parity constraints before it is promoted

### Experiments to Run (Post-Market)

**Exp 1 — Single-date backtest Apr 23 (calibration check) — ✅ COMPLETED**

Queried Apr 23 slice from the then-current RISK_SHORT baseline `64c1ded4f9f0`
(retired and replaced by `fd763aa18d54` on 2026-04-24; run already covered this date).
- Backtest SHORT INITIAL_SL rate on Apr 23: **32%** (8/25 trades)
- Live SHORT INITIAL_SL rate on Apr 23: **50%** (15/30 trades)
- Backtest combined PnL Apr 23: +₹16,166 | Live combined PnL: +₹16,917 (nearly identical)
- Entry-time breakdown: 09:20–09:25 = 0% WR (12 trades), 09:30–10:10 = 54% WR (13 trades)
- Conclusion: Mildly bad regime day in backtest (32% vs 21.8% avg). Live gap (50% vs 32%)
  is real — tick/OHLCV divergence amplified the damage but did not cause the regime problem.

**Exp 2 — Re-enable RVOL for SHORT — ✅ REJECTED (2026-04-23)**
```bash
doppler run -- uv run pivot-backtest --all --universe-size 0 --yes-full-run \
  --start 2025-01-01 --end 2026-04-23 \
  --preset CPR_LEVELS_RISK_SHORT --no-skip-rvol --save
```
Hypothesis: Volume confirmation filters low-momentum breakdowns that reverse quickly.
Risk: fewer trades, but higher quality. Key metric: INITIAL_SL rate reduction.

Implementation notes:
- Added `--no-skip-rvol` so preset-backed SHORT runs can re-enable RVOL.
- Fixed a follow-on preset override bug where `--rvol X` was silently ignored under `--preset`;
  only the boolean skip flag was propagating. This made the first `0.5` test invalid.

Results vs the then-current SHORT baseline `64c1ded4f9f0`
(retired and replaced by `fd763aa18d54` on 2026-04-24):

| Variant | Trades | WR | PF | PnL | Calmar |
|--|--|--|--|--|--|
| Baseline (`skip_rvol_check=true`) | 4,927 | 31.3% | 2.245 | ₹1,092,472 | 99.51 |
| RVOL ON @ `1.0` | 2,427 | 36.8% | 3.043 | ₹773,021 | 126.15 |
| RVOL ON @ `0.5` | 3,632 | 35.3% | 2.689 | ₹1,013,119 | 78.67 |

Interpretation:
- RVOL confirmation improves trade quality, but it cuts too much of the profitable SHORT book.
- `1.0` is far too strict.
- `0.5` is materially better than `1.0`, but still underperforms the baseline on both PnL and Calmar.
- Conclusion: keep `skip_rvol_check=True` for the SHORT baseline.

**Exp 3 — Raise min_sl_atr_ratio for SHORT — ✅ REJECTED (2026-04-23)**
```bash
doppler run -- uv run pivot-backtest --all --universe-size 0 --yes-full-run \
  --start 2025-01-01 --end 2026-04-23 \
  --preset CPR_LEVELS_RISK_SHORT --min-sl-atr-ratio 1.0 --save
```
Results vs the then-current baselines `64c1ded4f9f0` / `b6476255aa1c`
(retired and replaced by `fd763aa18d54` / `4eaaa682e79c` on 2026-04-24):

| | SHORT baseline | SHORT min_sl=1.0 | LONG baseline | LONG min_sl=1.0 |
|--|--|--|--|--|
| Trades | 4,927 | 4,497 (−9%) | 3,216 | 2,899 (−10%) |
| WR | 31.3% | 32.9% | 34.0% | 34.1% |
| PF | 2.25 | 2.10 | 3.00 | 2.62 |
| PnL | ₹1,092,472 | ₹1,012,516 | ₹1,036,948 | ₹889,595 |
| Calmar | 99.5 | 77.8 (−22%) | 203 | 140 (−31%) |
| MaxDD | 0.76% | 0.91% | 0.36% | 0.45% |

Tight SL (0.5×ATR) is doing real work — removing it loses net-positive trades. Wider SL
is not the fix; the Apr 23 problem is regime-specific, not SL-distance. **Keep 0.5**.
Stale runs deleted: `c30d95b013d0` (preset-bug duplicate), `f966f4703bfb` (SHORT), `6eb80f21f6b2` (LONG).

Also fixed: `--min-sl-atr-ratio` / `--max-sl-atr-ratio` were silently ignored when using
`--preset` (same bug class as the Apr 22 `--breakeven-r` fix). Fixed in `engine/run_backtest.py`.

**Exp 4 — Combined RVOL + wider SL — SKIPPED** (Exp 3 already rejected wider SL)

**Exp 5 — Regime gate for SHORT — NEXT**
```bash
doppler run -- uv run pivot-backtest --all --universe-size 0 --yes-full-run \
  --start 2025-01-01 --end 2026-04-23 \
  --preset CPR_LEVELS_RISK_SHORT \
  --regime-index-symbol "NIFTY 500" --regime-min-move-pct 0.5 --save
```
Skip SHORT when Nifty 500 is flat or up at 9:45 snapshot. Prior test was on gold_51 —
must rerun on full universe / same date range as baselines for fair comparison.

### Do NOT
- Change the canonical preset without running the backtest comparison first
- Assume that because index is down shorts will work — the magnitude matters (-0.5% is not enough)
- Mix the regime gate result from the prior NIFTY 500 test (gold_51 universe) with the
  current baseline without re-running on the same universe/date range

---

## 2026-04-23 — OPS: LONG session died at 13:50 (ANUHPHR illiquid) — position orphaned, no FLATTEN_EOD

**Status:** FIXED — `scripts/paper_trading.py` (`auto_flatten_on_abnormal_exit` always True for `--multi`)
**Severity:** High — open position left in DB after session death, FLATTEN_EOD never sent for LONG

### Observation

CPR_LEVELS_LONG session died at ~13:50 IST due to ANUHPHR going completely illiquid after 13:37.
The stale watchdog (10-minute timeout) terminated the session with status FAILED.

Timeline:
- `13:37:37` — last tick from ANUHPHR (last_tick_age kept climbing)
- `13:50:15` — FEED_STALE alert fired (streak=4), open positions listed: ANUHPHR LONG entry=81.69 SL=81.40 tgt=82.69 qty=1224
- `~13:50` — session terminated (STALE→FAILED in DB)
- `15:20` — ANUHPHR still OPEN in paper.duckdb, manually flattened via `pivot-paper-trading flatten --session-id CPR_LEVELS_LONG-2026-04-23-live-kite`
- ANUHPHR closed at ₹82.60 (entry ₹81.69, target was ₹82.69 — very close to target)

Consequence: FLATTEN_EOD alert was never sent for the LONG session. Operator saw the last alert at 13:50 and assumed session would recover or hit TIME exit at 15:15. No FLATTEN_EOD = no end-of-day summary for LONG.

LONG session final PnL (all 30 trades including ANUHPHR): **+₹17,540**
- TRAILING_SL: 12 trades, +₹20,655
- BREAKEVEN_SL: 9 trades, -₹751
- INITIAL_SL: 8 trades, -₹3,477
- ANUHPHR: TIME exit (manual flatten) at ₹82.60, +~₹1,100

### Root Cause (Code Bug)

`scripts/paper_trading.py` `_cmd_daily_live_multi`, line 1257–1307:

```python
preserve_open_positions_on_restart = not bool(getattr(args, "complete_on_exit", False))
# ...
auto_flatten_on_abnormal_exit=not preserve_open_positions_on_restart,
```

Since `complete_on_exit` defaults to `False` for a normal `--multi` run:
- `preserve_open_positions_on_restart = True`
- `auto_flatten_on_abnormal_exit = False`

**Every `--multi` session had `auto_flatten_on_abnormal_exit=False`**, meaning STALE/FAILED exits
always hit the "preserving for resume" path instead of auto-flattening. The `preserve_open_positions_on_restart`
flag was designed for the retry-on-early-exit restart path, but it incorrectly controlled whether
abnormal exits flatten positions. These are orthogonal concerns.

### Fix Applied

`scripts/paper_trading.py` — hardcoded `auto_flatten_on_abnormal_exit=True` for the `--multi`
execute_variant path. STALE/FAILED sessions will now always auto-flatten orphaned positions
and send FLATTEN_EOD, regardless of the `complete_on_exit` flag.

The `preserve_open_positions_on_restart` variable is retained for the `retry_on_early_exit` logic only.

### Also: Alert retry fix (same incident)

The three failed alerts at 10:19–10:21 on Apr 22 (`getaddrinfo failed`) were lost because the
dispatcher retried 3 times with (1s, 2s, 4s) = 7 seconds total — insufficient for a 2-minute
DNS outage. Fixed in `engine/alert_dispatcher.py`: network errors now get additional retries
at 30s and 120s, capped at 10 minutes from alert creation. The 10-minute cap prevents stale
"FEED_STALE" alerts from being delivered long after recovery.

---

## 2026-04-23 — UX: Dashboard shows "Feed: UNKNOWN" during session startup (9:16–9:20 AM)

**Status:** FIXED — `scripts/paper_live.py` (initial CONNECTING seed write)
**Severity:** Low — cosmetic, no trading impact

### Observation
Dashboard shows `Feed: UNKNOWN` after both sessions become ACTIVE at 9:16 AM,
even though KiteTicker connected at 09:16:34 and ticks are flowing.

### Root Cause
`paper_feed_state` has one row per session, written only when `latest_raw_state is not None`
(i.e. after the first complete 5-minute candle is assembled from ticks, ~9:20 AM).
Between session ACTIVE (9:16 AM) and the first candle close, no row exists in `paper_feed_state`.
`get_feed_state()` returns None → `summarize_paper_positions` produces `feed_status=None` →
dashboard falls back to `str(None or "UNKNOWN") = "UNKNOWN"`.

### Fix Applied
`scripts/paper_live.py` — after the ticker is registered and before `dispatch_session_started_alert`,
write an initial feed state row with `status="CONNECTING"`:

```python
await _write_feed_state(
    deps,
    session_id=session_id,
    status="CONNECTING",
    ...
    raw_state={"mode": "startup", "symbols": len(active_symbols)},
)
```

Dashboard now shows "CONNECTING" from 9:16 AM until the first candle snapshot (9:20 AM),
then transitions to "OK" normally. The replica syncs immediately via `_after_write`.

---

## 2026-04-24 — FIXED: BUG: paper_sessions.total_pnl = 0.0 after flatten-all (archive step skipped)

**Status:** FIXED — flatten paths now stamp session P&L and archive completed sessions
**Severity:** Medium — dashboard and run_metrics show ₹0 P&L for any day that ended via flatten-all

### Symptom
Both Apr 24 live sessions (`CPR_LEVELS_LONG-2026-04-24-live-kite`,
`CPR_LEVELS_SHORT-2026-04-24-live-kite`) show `total_pnl=0.0` in `paper_sessions` and
`run_metrics` even though all 61 positions are CLOSED with correct per-position PnL:
- SHORT: 32 trades, actual total +₹14,963 from `paper_positions.pnl`
- LONG: 29 trades, actual total −₹8,449 from `paper_positions.pnl`

### Root Cause
`flatten-all` (and `flatten`) calls `update_session_state(status=COMPLETED)` then writes
positions CLOSED, but never calls `archive_completed_session(...)`. The archive step is the
only path that computes and writes the aggregated `total_pnl`, `win_rate`, `max_drawdown_pct`
into `paper_sessions` and inserts the row into `backtest_results` / `run_metrics`.

The `force_sync()` fix (Apr 24 debounce issue) ensured the replica shows COMPLETED promptly,
but cannot fix missing aggregated data that was never computed in the first place.

### Source of Truth During Investigation
Query `paper_positions` grouped by `session_id` — the per-position `pnl` column is always
correct regardless of whether archive ran:
```sql
SELECT session_id, SUM(pnl) AS total_pnl, COUNT(*) AS trades,
       COUNT(*) FILTER (WHERE pnl > 0) * 100.0 / COUNT(*) AS win_rate
FROM paper_positions
WHERE session_id LIKE 'CPR_LEVELS%2026-04-24%'
GROUP BY session_id;
```

### Fix
Implemented in the flatten paths:

- `engine/paper_runtime.py` — `flatten_session_positions()` now recomputes `total_pnl` from all
  CLOSED positions after force-closing open positions and writes it to `paper_sessions`.
- `scripts/paper_trading.py` — `_cmd_flatten()` and `_cmd_flatten_all()` call
  `archive_completed_session()` after marking sessions `COMPLETED`.
- `scripts/paper_archive.py` — `archive_completed_session()` also recomputes and stamps
  `paper_sessions.total_pnl` from CLOSED positions, so re-archive repairs historical sessions too.
- `db/backtest_db.py` — PAPER archive writes are idempotent because `store_backtest_results()`
  deletes existing rows for the run_id before inserting refreshed rows, then refreshes run metrics.
- `tests/test_paper_runtime.py`, `tests/test_paper_archive.py`, and
  `tests/test_paper_trading_workflow.py` cover the P&L stamp and archive calls.

Historical sessions affected before this fix can still be repaired by re-running
`archive_completed_session("<session_id>")` after verifying `paper_positions` has the correct
CLOSED rows.

---

## 2026-04-24 — OPS: replica lag made Apr 24 look unbuilt in the pre-market check

**Status:** FIXED — operational correction only, no strategy impact
**Severity:** Low — confusing pre-market signal, but the live DB / session setup remained valid

### What happened
A pre-market inspection queried the dashboard replica instead of the live `market.duckdb`, so it still showed Apr 23 state. That made it look like the Apr 24 build had not completed, even though yesterday's EOD refresh had already populated the canonical DB.

### Root Cause
The dashboard reads from the versioned replica snapshot, which can lag or point at an older replica version until the writer publishes a new one and the consumer reconnects. The build pipeline is additive/upsert-based, so stopping it mid-way does not lose data that already exists; it only risks leaving a partially refreshed table until the next clean build.

### Resolution
- The unnecessary background build was stopped.
- The dashboard can be restarted after the build window if needed.
- The live session should continue on the original `daily-live --all-symbols` path unless a separate strategy change is intentionally approved.

### Notes
- The universe-snapshot experiment was rolled back before live started, but the opt-in
  snapshot path is now restored: `daily-prepare --all-symbols --snapshot-universe-name ...`
  writes the frozen list to `backtest_universe`, and `daily-live --universe-name ...`
  can reuse it when reproducibility is desired.
- No strategy defaults were changed by this correction.

---

## 2026-04-24 — OPS: session-start Telegram alert arrived without HTML formatting

**Status:** FIXED — `engine/paper_runtime.py` (2026-04-24)
**Severity:** Low — operational visibility issue only

### Symptom
The live session started and the SESSION_STARTED alert arrived, but it used a truncated
session_id (`[:20]` cut), plain text body, and no HTML formatting beyond `<code>` on the
session tag. PAUSED/RESUMED alerts had the same problem.

### Root Cause
`dispatch_session_started_alert` and `dispatch_session_state_alert` did not use the
`_parse_session_label` helper (used by FEED_STALE/FEED_RECOVERED) and had sparse, plain-text
bodies. The `parse_mode: HTML` in the Telegram notifier was correct — the content was the issue.

### Fix Applied
- `dispatch_session_started_alert`: now uses `_parse_session_label`, direction icon (🟢/🔴),
  bold labels, full `session_id`, start time in IST. Subject: `🟢 Session Started — CPR LONG · 24 Apr`
- `dispatch_session_state_alert` (PAUSED/RESUMED): same pattern with ⏸️/🔄 icons.
- Added `dispatch_session_completed_alert`: fires when `stop_is_terminal=True` and
  `final_status=COMPLETED` in `run_live_session` finally block (after FLATTEN_EOD).
  Subject: `✅ Session Completed — CPR SHORT · 24 Apr`

---

## 2026-04-24 — STRATEGY: LONG underperformed on a risk-off index day; future regime work may need sit-out or size reduction

**Status:** OPEN — log for post-close analysis
**Severity:** Medium — today’s LONG side is materially weaker than expected

### Observation
At the time of the live session, NIFTY 500 was down about `0.75%` (`22,639.15`, `-171.70`). The LONG CPR session still traded normally and is currently performing worse than expected.

### Interpretation
- A NIFTY-based regime gate would likely have filtered some LONG entries today.
- The broader response may need to be more than binary skip logic.
- We should evaluate:
  - full sit-out on strongly risk-off days, or
  - reduced capital deployed / smaller position sizing when the index is weak.

### Notes
- No live trading change is being made during the session.
- This observation is being recorded now so we can compare it against backtest and paper-replay later.
- The regime gate remains an opt-in hypothesis, not a default preset change.

---

## 2026-04-24 — OPS: LONG-only flatten could not be executed without stopping the live writer

**Status:** FIXED — `engine/paper_runtime.py` + `scripts/paper_live.py` (admin command queue, 2026-04-24)
**Severity:** Medium — prevented a selective LONG flatten while SHORT remained live

### What happened
A request to flatten only the LONG live session was blocked because `paper.duckdb` is held by the active `daily-live` process. The DB layer is single-writer, so a separate `pivot-paper-trading flatten --session-id CPR_LEVELS_LONG-2026-04-24-live-kite` process could not acquire the lock while the live session was running.

### Implication
- A selective LONG-only manual flatten is not currently available as an out-of-band operation while `daily-live` owns the writer lock.
- Stopping the live process would also interrupt SHORT, which was explicitly not desired.

### Notes
- No live process was interrupted.
- SHORT continues untouched.
- If we want selective long-only flattening in the future, we likely need an in-process control hook or a session-aware management command that talks to the live process instead of opening a second writer.

---

## 2026-04-24 — OPS: sentinel-file LONG flatten did not complete after multiple bar boundaries

**Status:** FIXED — `scripts/paper_live.py` (sentinel moved outside `if cycle_closed:`, 2026-04-24)
**Severity:** Medium — operator control path did not complete as expected

### What happened
The per-session sentinel file `.tmp_logs/flatten_CPR_LEVELS_LONG-2026-04-24-live-kite.signal` was created while the live session was still running. The file exists, but the LONG session remained `ACTIVE` and no `Flatten signal detected` / `manual_flatten_signal` log line appeared after multiple 5-minute boundaries.

### Observed state
- LONG session stayed `ACTIVE` in the dashboard.
- SHORT session kept running normally.
- The signal file is present on disk.
- Live logs continued to show bar processing for LONG up to `11:05`, but no sentinel-consumption log has appeared.

### Hypotheses
- The LONG session is not re-entering the sentinel-check path as expected.
- The signal check may be occurring only in a branch that is no longer being reached once the LONG symbol set shrinks.
- There may be a session-state / bar-loop interaction preventing `complete_on_exit` from being applied.

### Notes
- This is not a filename typo.
- This is not a DuckDB writer-lock issue.
- The issue is that the in-process sentinel path is not completing the LONG flatten in practice, even though the file exists.

---

## 2026-04-23 — UX: `pivot-data-quality` reports "Ready: NO" falsely in pre-market

**Status:** FIXED — `scripts/data_quality.py` (pre-market auto-detect)
**Severity:** Low — misleading output, no functional impact on live trading

### Observation
Running `pivot-data-quality --date <today>` before market open (e.g. 8:42 AM IST) returns:

```
Readiness:
  Ready      NO
  5-min symbols on date: 0
  Suggested fix: doppler run -- uv run pivot-build --refresh-since <today>
```

The tool gates readiness on the presence of 5-min intraday candles for the requested date.
Pre-market, today's candles do not exist yet — they are built live as the market runs.
The state tables that actually matter for live trading (`market_day_state`, `strategy_day_state`)
are correctly shown as AHEAD (built with prev-day ATR via ASOF JOIN) with 0 missing symbols,
but the overall "Ready" flag is still set to NO.

### Root Cause
The readiness gate `5-min symbols on date: 0` is appropriate for post-market validation
(confirming ingestion ran) but incorrect for pre-market checks (confirming live trading can start).
The suggested fix command (`pivot-build --refresh-since <today>`) is also wrong pre-market —
rebuilding `intraday_day_pack` for a date with no candle data would produce an empty pack.

### Fix Applied
`scripts/data_quality.py` — `_is_pre_market()` helper detects if the trade date is today and
current IST time is before 09:15. In pre-market mode:
- `ready` is computed from `not freshness_blocking and not coverage_blocking` only (no parquet check)
- Output header shows `[PRE-MARKET MODE]`
- "Suggested fix" changes from the wrong `pivot-build --refresh-since` to `daily-prepare`
- The "5-min symbols on date: 0" line is suppressed with an explanatory note

---

## 2026-04-23 — UX: Feed shows STALE for 5 min after a 2-second WebSocket reconnect

**Status:** FIXED — `scripts/paper_live.py` (immediate OK write on stale→connected transition)
**Severity:** Low — cosmetic dashboard confusion, no trading impact

### Observation
WebSocket code 1006 drops happened twice (12:57 and 13:21). Both auto-reconnected in 2–3 seconds.
The dashboard showed `Feed: STALE` for up to 5 minutes after recovery because `_write_feed_state`
only writes OK when `latest_raw_state is not None` — which requires a bar-close snapshot. The next
bar close is up to 5 minutes away.

### Fix Applied
In the `else:` branch of the stale watchdog in `paper_live.py`, captured `_was_stale = no_snapshot_streak > 0`
before the reset, then added an additional path:
```python
elif _was_stale and use_websocket and ticker_adapter is not None and ticker_adapter.is_connected:
    await _write_feed_state(..., status="OK", raw_state={"mode": "reconnected", "connected": True})
```
This writes OK immediately when the supervision loop detects stale→connected transition, clearing
the STALE status without waiting for the next 5-minute candle snapshot.

---

## 2026-04-23 — OPS: bare `daily-live` (no `--multi`, no direction) fails silently with direction=BOTH

**Status:** FIXED — `scripts/paper_trading.py` (explicit direction guard in `_cmd_daily_live`)
**Severity:** Medium — operator confusion, failed session attempts go unnoticed

### Observation
Five `SESSION_STARTED CPR_LEVELS BOTH` alerts fired after market hours on Apr 22 (18:02–23:11 IST).
All had `status=failed`, `error_msg=None`. These were manual retries of a bare `daily-live` command
that defaulted to `direction_filter=BOTH` (no preset, no `--multi` flag).

### Root Cause
`_resolve_paper_strategy_params` returns no `direction_filter` when no preset is given.
`paper_live.py:824` falls back to `"BOTH"`, which fails downstream validation. No error was shown
to the operator — the session attempt failed after network I/O, so the operator kept retrying.

### Fix Applied
`scripts/paper_trading.py` — `_cmd_daily_live` now checks direction immediately after resolving params:
```python
direction = (strategy_params.get("direction_filter") or "BOTH").upper()
if direction == "BOTH":
    raise SystemExit("daily-live requires an explicit direction ... use --multi ...")
```
The error message shows the correct `--multi` command and preset alternatives.

---

## 2026-04-22 — OPS: WebSocket reconnect failure on SHORT session (alert undelivered)

**Status:** INVESTIGATED — root cause: DNS/network outage, not a code bug
**Severity:** Low — session ran fine, operator impact only

### Observation
`SESSION_ERROR CPR_LEVELS_SHORT websocket_reconnect_failed` fired at 10:20 AM IST.
Alert `status=failed` in `alert_log`. Session recovered and completed normally at 15:15.
Several other alerts also failed around the same time.

### Root Cause
`alert_log.error_msg = "[Errno 11001] getaddrinfo failed"` on the alerts that captured errors.
This is a DNS lookup failure — the machine briefly lost network connectivity at ~10:20 AM,
which simultaneously caused the WebSocket drop AND the alert delivery failure. No code fix needed.
The fire-and-forget alert design is correct — never block trading on alert delivery.

### Note on `error_msg=None` entries
Some `TRADE_OPENED` and `SESSION_STARTED` alerts show `status=failed` with `error_msg=None`.
This means the exception was caught but not stored. Low priority — the trade state itself is
correctly written to DB regardless of alert delivery.

---

## 2026-04-22 — OPS: Post-market SESSION_STARTED (BOTH direction) attempts, all failed

**Status:** FIXED — see "bare daily-live direction=BOTH" entry above (Apr 23)
**Severity:** Low — all attempts failed cleanly, no bad state written

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

**Status:** REJECTED — backtest data (15 months, 4,669 SHORT trades, run `804f589a2fc7`) definitively contradicts the 2-day live hypothesis. Do NOT implement.
**Severity:** Informational — hypothesis retired

### Motivation (original, from 2 live days)
Two consecutive live days (Apr 21 up-day, Apr 22 down-day) showed early SHORT entries
(09:20–09:30) hitting SL disproportionately vs later entries at 09:35+.

Apr 22 live evidence (52 trades):
| Entry time | SHORT outcomes |
|------------|----------------|
| 9:20 | 10 opened → 7 BREAKEVEN or INITIAL_SL, 1 TARGET, 2 open |
| 9:25–9:30 | 5 opened → 4 SL, 0 targets |
| 9:35–9:45 | 9 opened → 4 targets; others mostly BREAKEVEN |

### Backtest refutation (Apr 22, 2026 — SQL analysis on 15-month run)

Queried `backtest_results` for SHORT run `804f589a2fc7` (2025-01-01→2026-04-21, 2040 symbols):

| Time | Trades | Trade% | WR% | Avg PnL | PnL% | TARGET% | InitSL% | BE_SL% |
|------|--------|--------|-----|---------|------|---------|---------|--------|
| **09:20** | **1,891** | **40.5%** | **36.8%** | **₹290** | **51.7%** | 32.0% | 30.9% | 31.1% |
| 09:25 | 635 | 13.6% | 33.2% | ₹226 | 13.5% | 29.9% | 30.9% | 34.5% |
| 09:30 | 416 | 8.9% | 32.5% | ₹233 | 9.1% | 29.1% | 26.7% | 40.9% |
| **09:35** | **321** | **6.9%** | **27.1%** | **₹93** | **2.8%** | **21.5%** | **37.1%** | 34.9% |
| 09:40 | 279 | 6.0% | 31.9% | ₹166 | 4.4% | 27.6% | 37.3% | 30.5% |
| 09:45 | 213 | 4.6% | 29.1% | ₹154 | 3.1% | 24.4% | 30.0% | 38.0% |
| 09:50–10:15 | 856 | 18.3% | ~30% | ~160 | ~17% | ~25% | ~31% | ~36% |

**Key findings:**
- **09:20 is the best SHORT slot**: 40.5% of trades, 51.7% of total PnL, 36.8% WR, ₹290 avg.
  Delaying entry to 09:35 would forfeit more than half the strategy's PnL.
- **09:35 is the worst SHORT slot**: 27.1% WR (lowest), 21.5% TARGET rate (lowest),
  37.1% INITIAL_SL rate (highest), ₹93 avg PnL (lowest). Implementing `entry_window_start_short=09:35`
  would actively move entries from the best slot to the worst.
- **Also checked for LONG** (run `c543038a648a`, 3,165 trades): same conclusion.
  09:20 LONG: 38.4% WR, ₹415 avg, 23.2% of trades. Best LONG slot is 09:40 (41.8%, ₹472)
  but early entries are broadly good. No case for delayed start.
- **Gap analysis**: On large gap-down days (gap < −0.5%), early SHORT WR = 31.7% avg ₹202 —
  still better than late SHORT on the same days (WR 29.3%, avg ₹136).

### Why live data showed the opposite (2 days ≠ 4,669 trades)
The 2-day live sessions captured a specific market regime (Apr 21–22 bounce pattern).
At 52 live SHORT trades, any 2-day cluster of adverse entries is within normal variance.
The 15-month backtest across 2040 symbols provides 90× the sample size and definitively
shows 09:20 as the alpha source, not a liability.

Note: `entry_window_start_short` is not implemented in the engine — `CPRLevelsParams`
has only `entry_window_end`. The `--strategy-params` JSON override in the original command
would have been silently ignored.

---

## 2026-04-22 — OBSERVATION: Gap-open reversal kills early SHORT entries (2-day pattern)

**Status:** INFORMATIONAL — hypothesis NOT confirmed by backtest; see experiment above
**Severity:** Low — the 2-day pattern is real but not structurally significant at scale

### Pattern (observed in live, 2 days)
On both Apr 21 (up-day) and Apr 22 (down-day by NIFTY close), the same intraday structure:
1. **09:15–09:20**: CPR direction fires SHORT for many symbols (gap-down open or intraday weakness)
2. **09:20–09:30**: Sharp reversal / bounce off day lows. Early SHORT entries get swept.
3. **09:35+**: Bounce exhausts, move resumes in original direction. Later entries profitable.

### Backtest verdict
This pattern is real on individual days but does NOT dominate the 15-month distribution.
Across 4,669 SHORT trades, 09:20 gap-down entries still outperform 09:35 entries.
The bounce SL-sweep that was visible in live is absorbed into the backtest's INITIAL_SL rate
(30.9% at 09:20), which is lower than the INITIAL_SL rate at 09:35 (37.1%). In aggregate,
the early entries are still better despite the gap-bounce effect.

**Do not act on this observation for entry window changes.**

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

## 2026-04-22 — EXPERIMENT: `entry_window_end=10:00` vs baseline `10:15`

**Status:** REJECTED — both directions worse. Keep default `entry_window_end=10:15`.
**Severity:** Informational — hypothesis disproved

### Motivation
Given that late entries (09:35+) underperform, question was whether restricting the entry
window to 09:15–10:00 (cutting the 10:00–10:15 tail) would improve quality by removing
the marginal late-entry slots.

### Experiment (2026-04-22, universe=baseline_apr21_2040, 2035 symbols, 2025-01-01→2026-04-21)

**SHORT direction** — `CPR_LEVELS_RISK_SHORT`:

| Parameter | Run ID | Trades | WR% | PF | PnL | Ann% | MaxDD | Calmar |
|-----------|--------|--------|-----|----|-----|------|-------|--------|
| EW=10:15 (baseline) | `804f589a2fc7` | 4,669 | 33.5% | 2.12 | ₹1,060,744 | 74.2% | 1.0% | 72.83 |
| EW=10:00 | `62e309688892` | 4,275 | 33.7% | 2.15 | ₹997,534 | 70.1% | 1.1% | 66.34 |
| **Δ** | | **-394 (−8.4%)** | +0.2% | +0.03 | **−₹63K (−6.0%)** | **−4.1%** | +0.1% | **−6.5 (−8.9%)** |

**LONG direction** — `CPR_LEVELS_RISK_LONG`:

| Parameter | Run ID | Trades | WR% | PF | PnL | Ann% | MaxDD | Calmar |
|-----------|--------|--------|-----|----|-----|------|-------|--------|
| EW=10:15 (baseline) | `c543038a648a` | 3,165 | 35.1% | 2.70 | ₹992,935 | 69.8% | 0.4% | 167.84 |
| EW=10:00 | `af32eed649e4` | 2,733 | 36.0% | 2.81 | ₹913,011 | 64.5% | 0.8% | 81.36 |
| **Δ** | | **-432 (−13.7%)** | +0.9% | +0.11 | **−₹80K (−8.0%)** | **−5.3%** | +0.4% | **−86.5 (−51.5%)** |

### Verdict
The 10:00–10:15 window contributes net-positive trades in both directions. Cutting it:
- SHORT: −6% P/L, −8.9% Calmar
- LONG: −8% P/L, Calmar halved (167 → 81) because MaxDD also doubled (0.4% → 0.8%)

The marginal late entries are still worth taking. **Keep `entry_window_end=10:15`.**

---

## 2026-04-22 — OBSERVATION: BREAKEVEN_SL commission drain on slow trending days

**Status:** EXPERIMENT COMPLETED (2026-04-22) — `breakeven_r=1.5` tested on 2035-symbol universe, 15 months. **Verdict: REJECT — keep `breakeven_r=1.0`.** Calmar regresses in both directions.
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

### Bug discovered: `--breakeven-r` silently ignored when using `--preset`

When `--preset` is combined with `--breakeven-r`, the CLI's preset code path built
`preset_cli_overrides` without `breakeven_r`, so the flag was silently dropped and the
run used `breakeven_r=1.0` (default) regardless of what was specified.

**Fix (2026-04-22):** `engine/run_backtest.py` lines 923–926 — added non-default guard:
```python
if args.breakeven_r != 1.0:
    preset_cli_overrides["breakeven_r"] = args.breakeven_r
if args.rr_ratio != 2.0:
    preset_cli_overrides["rr_ratio"] = args.rr_ratio
```
Same pattern as `trail_atr_multiplier`. Re-run required to get valid results.

### Experiment results (2026-04-22, universe=baseline_apr21_2040, 2035 symbols, 2025-01-01→2026-04-21)

**SHORT direction** — `CPR_LEVELS_RISK_SHORT`:

| Run | breakeven_r | Run ID | Trades | WR% | PF | PnL | Ann% | MaxDD | Calmar |
|-----|-------------|--------|--------|-----|----|-----|------|-------|--------|
| Baseline | 1.0 | `804f589a2fc7` | 4,669 | 33.5% | 2.12 | ₹1,060,744 | 74.2% | 1.02% | 72.83 |
| BE=1.5 | 1.5 | `45ed0e169528` | 4,512 | **37.5%** | 2.05 | ₹1,097,233 | **76.5%** | 1.09% | 70.48 |
| **Δ** | | | **-157 (−3.4%)** | +4.0pp | −0.07 | **+₹36K (+3.4%)** | +2.3pp | +0.07pp | **−2.35 (−3.2%)** |

**LONG direction** — `CPR_LEVELS_RISK_LONG`:

| Run | breakeven_r | Run ID | Trades | WR% | PF | PnL | Ann% | MaxDD | Calmar |
|-----|-------------|--------|--------|-----|----|-----|------|-------|--------|
| Baseline | 1.0 | `c543038a648a` | 3,165 | 35.1% | 2.70 | ₹992,935 | 69.8% | 0.42% | 167.84 |
| BE=1.5 | 1.5 | `1c731dd72125` | 3,111 | **37.9%** | 2.46 | ₹977,562 | 68.7% | 0.46% | 149.19 |
| **Δ** | | | **-54 (−1.7%)** | +2.8pp | −0.24 | **−₹15K (−1.5%)** | −1.1pp | +0.04pp | **−18.65 (−11.1%)** |

### Verdict: REJECT `breakeven_r=1.5`

Both directions show the same pattern: WR rises (fewer BREAKEVEN_SL scratch exits),
but PF and Calmar both fall because trades that previously scratched at entry (small loss)
now either reach target OR become full INITIAL_SL losses (larger loss). The net effect:

- **SHORT**: P/L gains +₹36K (+3.4%) but Calmar drops 72.83 → 70.48 (−3.2%). Trade count
  also drops by ~157 (slot pressure: positions held longer block new entries under `max_positions=10`).
- **LONG**: P/L loses −₹15K (−1.5%) AND Calmar drops 167.84 → 149.19 (−11%). Clear regression.

The original `breakeven_r=1.0` produces better risk-adjusted returns (Calmar) in both directions.
The commission drain from BREAKEVEN_SL exits (~₹2,324/day observation) is not significant enough
to justify the Calmar hit. **Do not change `breakeven_r`.**

---

## 2026-04-22 — EXPERIMENT: Time-stop for slow-bleed INITIAL_SL trades

**Status:** COMPLETED (2026-04-23) — `time_stop_bars=12` tested on 2035-symbol universe, 2025-01-01–2026-04-21. **Verdict: REJECT — neutral to marginally worse; no benefit worth the added complexity.**
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

### Backtest Commands (executed 2026-04-23)
```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --all --universe-size 0 --start 2025-01-01 --end 2026-04-21 \
  --preset CPR_LEVELS_RISK_SHORT --time-stop-bars 12 \
  --save --quiet --progress-file .tmp_logs/bt_short_tsb12.jsonl

PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --all --universe-size 0 --start 2025-01-01 --end 2026-04-21 \
  --preset CPR_LEVELS_RISK_LONG --time-stop-bars 12 \
  --save --quiet --progress-file .tmp_logs/bt_long_tsb12.jsonl
```

### Results

| Config | Run ID | Trades | WR | PF | PnL | Ann% | MaxDD | Calmar |
|--------|--------|--------|----|----|-----|------|-------|--------|
| **RISK_SHORT baseline** | `804f589a2fc7` | 4,669 | 33.5% | 2.120 | ₹1,060,744 | 74.2% | 1.0% | 72.83 |
| **RISK_SHORT TSB=12** | `52c22b1e45f1` | 4,666 | 33.3% | 2.120 | ₹1,048,454 | 73.4% | 1.0% | 71.72 |
| **RISK_LONG baseline** | `c543038a648a` | 3,165 | 35.1% | 2.700 | ₹992,935 | 69.8% | 0.4% | 167.84 |
| **RISK_LONG TSB=12** | `e8ed5ccbd87a` | 3,158 | 34.9% | 2.750 | ₹993,670 | 69.8% | 0.4% | 167.79 |

### Analysis

- **SHORT**: -3 trades, WR -0.2pp, PF unchanged (2.120), PnL -₹12K, Calmar 71.72 vs 72.83 (-1.5%). Marginally worse.
- **LONG**: -7 trades, WR -0.2pp, PF +0.05, PnL +₹735, Calmar essentially flat (167.79 vs 167.84). Neutral.
- The ~800 "slow-bleed" trades identified in the original SQL analysis (505 SHORT + 295 LONG) do not concentrate P&L drag as hypothesised. They fall within normal SL-range losses; early exit offers no edge.
- TIME_STOP exits do NOT free position slots for re-entry (unlike momentum_confirm) — the symbol slot is locked for the full day regardless, so trade count barely changes.

**Verdict: REJECT.** No material improvement in either direction. Adds engine complexity for zero gain.

---

## 2026-04-22 — EXPERIMENT: Momentum confirmation filter (early exit if no direction in bar 1)

**Status:** COMPLETED (2026-04-23) — `momentum_confirm=True` tested on 2035-symbol universe, 2025-01-01–2026-04-21. **Verdict: ACCEPT — meaningful Calmar improvement (+38% SHORT, +20% LONG) with higher PF and lower MaxDD. Recommend enabling in CPR_LEVELS_RISK presets.**
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

### Backtest Commands (executed 2026-04-23)
```bash
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --all --universe-size 0 --start 2025-01-01 --end 2026-04-21 \
  --preset CPR_LEVELS_RISK_SHORT --momentum-confirm \
  --save --quiet --progress-file .tmp_logs/bt_short_mc.jsonl

PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --all --universe-size 0 --start 2025-01-01 --end 2026-04-21 \
  --preset CPR_LEVELS_RISK_LONG --momentum-confirm \
  --save --quiet --progress-file .tmp_logs/bt_long_mc.jsonl
```

### Results

| Config | Run ID | Trades | WR | PF | PnL | Ann% | MaxDD | Calmar |
|--------|--------|--------|----|----|-----|------|-------|--------|
| **RISK_SHORT baseline** | `804f589a2fc7` | 4,669 | 33.5% | 2.120 | ₹1,060,744 | 74.2% | 1.0% | 72.83 |
| **RISK_SHORT MC=True** | `81bc928ec7ad` | 4,885 | 31.5% | 2.260 | ₹1,094,892 | 76.4% | 0.8% | 100.18 |
| **RISK_LONG baseline** | `c543038a648a` | 3,165 | 35.1% | 2.700 | ₹992,935 | 69.8% | 0.4% | 167.84 |
| **RISK_LONG MC=True** | `83b0a8535295` | 3,201 | 33.8% | 2.960 | ₹1,017,690 | 71.4% | 0.4% | 200.66 |

### Analysis

- **SHORT**: +216 more trades (exits free up slots for re-entry), WR -2.0pp (MOMENTUM_FAIL exits are losses), PF +0.14 (+6.6%), PnL +₹34K (+3.2%), MaxDD -0.2pp, **Calmar 100.18 vs 72.83 (+37.5%)**.
- **LONG**: +36 more trades, WR -1.3pp, **PF +0.26 (+9.6%)**, PnL +₹25K (+2.5%), MaxDD unchanged, **Calmar 200.66 vs 167.84 (+19.6%)**.
- The mechanism is clear: MOMENTUM_FAIL exits at bar 2 open cut the immediate reversal entries before they can develop into full SL hits. This reduces the severity of losing trades (lower MaxDD) while the freed slots occasionally allow better entries (trade count increases, PnL improves despite lower WR).
- WR dips because MOMENTUM_FAIL exits are counted as losses (exit at bar 2 open for a small loss or commission), but these small losses replace the deeper INITIAL_SL losses they prevent.
- Calmar improvement is driven primarily by MaxDD compression — SHORT MaxDD drops from 1.0% to 0.8%, a meaningful reduction in worst-case drawdown.

### Implementation
Engine changes made in this session:
- `engine/cpr_atr_shared.py`: `simulate_trade_lifecycle` — `momentum_exit_pending` flag, exit at bar 2 open with `exit_reason="MOMENTUM_FAIL"`
- `engine/cpr_atr_strategy.py`: `CPRLevelsParams.momentum_confirm: bool = False` + call-site wired
- `engine/run_backtest.py`: `--momentum-confirm` flag (default False, preset path uses non-default guard)
- `engine/strategy_presets.py`: `momentum_confirm` propagated in `cpr_levels_config` fields
- `db/backtest_db.py`: `exit_reason` CHECK constraint updated to include `MOMENTUM_FAIL`, `TIME_STOP`

**Verdict: ACCEPT.** Both directions show clear Calmar improvement with higher PnL and lower MaxDD. The trade-off (lower WR) is expected and acceptable — MOMENTUM_FAIL exits are small losses that prevent deeper SL hits.

### 2026-05-01 follow-up: `full_2026_04_30` CPR baseline promotion

The 2026-05-01 `full_2026_04_30` runs below supersede the 2026-04-28 `u2029` rows as the active
CPR comparison set. The old 2026-04-28 rows were deleted after the runtime rebuild made their setup
surface non-reproducible. This promotion also adopts canonical 5-position / ₹2L sizing:
`max_positions=5`, `capital=200000`, `max_position_pct=0.2`.

Superseded by the 2026-05-02 review-batch canonical CPR baseline set:

| Mode | Preset | Run ID | Window | P/L | Calmar |
|------|--------|--------|--------|-----|--------|
| Daily Reset | `CPR_LEVELS_STANDARD_LONG` | `e811f5bb01e5` | 2025-01-01 → 2026-04-30 | ₹1,710,015 | 207 |
| Daily Reset | `CPR_LEVELS_STANDARD_SHORT` | `9a2ccbd93c5b` | 2025-01-01 → 2026-04-30 | ₹1,626,569 | 91 |
| Daily Reset | `CPR_LEVELS_RISK_LONG` | `638b343959ad` | 2025-01-01 → 2026-04-30 | ₹1,706,953 | 210 |
| Daily Reset | `CPR_LEVELS_RISK_SHORT` | `307c3e175a16` | 2025-01-01 → 2026-04-30 | ₹1,643,730 | 96 |
| Compound | `CPR_LEVELS_STANDARD_LONG` | `8bbabe422f9c` | 2025-01-01 → 2026-04-30 | ₹5,459,694 | 401 |
| Compound | `CPR_LEVELS_STANDARD_SHORT` | `a700bb027f24` | 2025-01-01 → 2026-04-30 | ₹5,028,099 | 178 |
| Compound | `CPR_LEVELS_RISK_LONG` | `480a14f8aa26` | 2025-01-01 → 2026-04-30 | ₹1,710,760 | 207 |
| Compound | `CPR_LEVELS_RISK_SHORT` | `f377d33a9157` | 2025-01-01 → 2026-04-30 | ₹1,652,271 | 87 |

Future universe changes are migrations and must be labelled separately. Do not compare future
universe totals against this `full_2026_04_30` family as a daily extension.

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

**Status:** OPEN — root cause confirmed; quality-sort fix planned (see 2026-04-25 entry below)
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

### 2026-04-22 follow-up: quantified trade-count impact on Apr 20-21

The Apr 18 analysis established the mechanism. A direct symbol-by-symbol compare on the
retained daily-reset risk baselines now shows the impact is large enough to invalidate
`live-kite` as a parity reference for entry qualification.

Compared against the retained baselines:

- `2026-04-17` replay stayed close:
  - LONG: backtest `26` vs replay `25`
  - SHORT: backtest `5` vs replay `5`
- `2026-04-20` local-live also stayed close:
  - LONG: backtest `5` vs local-live `5`
  - SHORT: backtest `12` vs local-live `14`
- `2026-04-20` kite-live diverged materially:
  - LONG: backtest `5` vs kite-live `30`
  - SHORT: backtest `12` vs kite-live `38`
- `2026-04-21` kite-live diverged materially:
  - LONG: backtest `21` vs kite-live `29`
  - SHORT: backtest `6` vs kite-live `22`

Across the `2026-04-20` and `2026-04-21` kite-live sessions:

- `99` symbols were **paper-only** (traded in kite-live, absent from baseline backtest)
- `79 / 99` (`79.8%`) were explained by a **setup filter flip**
- the dominant flip was `OR_ATR_RATIO`: the packed 09:15 candle failed the filter, while the
  live-kite 09:15 candle passed it because the live OR range was much smaller

Representative examples:

| Date | Side | Symbol | Packed `or_atr_5` | Live-kite `or_atr_5` | Effect |
|------|------|--------|-------------------|----------------------|--------|
| 2026-04-20 | SHORT | `20MICRONS` | `10.627` | `1.117` | backtest rejects, kite-live trades |
| 2026-04-20 | LONG | `ACUTAAS` | `6.976` | `1.831` | backtest rejects, kite-live trades |
| 2026-04-20 | SHORT | `BAJAJHCARE` | `8.608` | `1.678` | backtest rejects, kite-live trades |
| 2026-04-21 | LONG | `DEEPAKFERT` | `3.048` | `2.341` | backtest rejects, kite-live trades |
| 2026-04-21 | SHORT | `BAJAJHLDNG` | `3.426` | `1.325` | backtest rejects, kite-live trades |

The drift also cuts the other way:

- `24` symbols were **baseline-only** (traded in backtest, absent from kite-live)
- `9 / 24` (`37.5%`) were also explained by setup-filter flips, again mostly `OR_ATR_RATIO`
- in some symbols the kite-live first bar collapsed to zero range:

| Date | Side | Symbol | Packed `or_atr_5` | Live-kite `or_atr_5` | Effect |
|------|------|--------|-------------------|----------------------|--------|
| 2026-04-20 | LONG | `KRITI` | `1.845` | `0.000` | baseline trades, kite-live rejects |
| 2026-04-20 | LONG | `VHL` | `1.490` | `0.000` | baseline trades, kite-live rejects |
| 2026-04-20 | SHORT | `CONTROLPR` | `2.199` | `0.000` | baseline trades, kite-live rejects |

Conclusion:

- replay/local-live parity still validates the shared candle-based engine
- `daily-live --feed-source kite` is **not** a valid parity reference for entry qualification
  while entries depend on WebSocket-built 09:15 OHLC
- if real-money live trading is the acceptance standard, the live money path must consume an
  entry signal source that can be reproduced after the fact

Practical implication:

- Zerodha WebSocket is still appropriate for live transport and LTP monitoring
- the parity problem is not "WebSocket should never be used"
- the problem is that `MODE_QUOTE` snapshots are not an authoritative bar source for binary
  first-bar filters such as `or_atr_5`, `or_close_5`, and derived direction
- any future parity fix must either:
  1. derive entry qualification from an authoritative candle source, or
  2. replay from the exact captured live feed (`paper_feed_audit`) instead of comparing against
     `intraday_day_pack`

### 2026-04-22 follow-up: exact-feed backtest path implemented, parity still open

An opt-in exact-feed source path now exists for single-session analysis:

- backtest can use `pack_source=paper_feed_audit`
- replay can use `pack_source=paper_feed_audit`
- both require `pack_source_session_id=<archived session_id>`
- default behavior remains `intraday_day_pack`

This allows backtest/replay to consume the exact archived bars from `paper_feed_audit`
instead of the EOD-packed candle tape.

First proof run against archived kite-live sessions (`2026-04-20` through `2026-04-22`):

| Session | Exact-feed backtest | Archived live | Delta |
|---------|---------------------|---------------|-------|
| `CPR_LEVELS_LONG-2026-04-20-live-kite` | `26` trades / `₹10,516.22` | `30` / `₹13,520.31` | `-4` trades |
| `CPR_LEVELS_SHORT-2026-04-20-live-kite` | `28` / `₹1,602.46` | `38` / `₹460.64` | `-10` |
| `CPR_LEVELS_LONG-2026-04-21-live-kite` | `17` / `₹3,064.07` | `29` / `₹3,856.14` | `-12` |
| `CPR_LEVELS_SHORT-2026-04-21-live-kite` | `21` / `-₹2,407.50` | `22` / `-₹2,748.10` | `-1` |
| `CPR_LEVELS_LONG-2026-04-22-live-kite` | `19` / `₹7,663.09` | `34` / `₹7,084.62` | `-15` |
| `CPR_LEVELS_SHORT-2026-04-22-live-kite` | `23` / `-₹942.80` | `30` / `-₹2,447.39` | `-7` |

Notes:

- Apr 20 exact-feed runs had to drop 2 symbols from the parity set because runtime coverage
  was missing for `HILINFRA` and `JKIPL`
- Apr 21 dropped `KECL` for the same reason
- even after exact-feed loading, full trade-key overlap is still not closed
- symbol+direction overlap improved versus the packed-candle compare, but remained partial:
  - `2026-04-20 LONG`: `16` shared / `10` backtest-only / `14` live-only
  - `2026-04-20 SHORT`: `20` / `8` / `18`
  - `2026-04-21 LONG`: `10` / `7` / `19`
  - `2026-04-21 SHORT`: `15` / `6` / `7`
  - `2026-04-22 LONG`: `8` / `11` / `26`
  - `2026-04-22 SHORT`: `13` / `10` / `17`

Conclusion:

- the first-bar candle-source mismatch was a real and important problem
- fixing the candle source alone is **not sufficient** to reproduce live-kite exactly
- remaining parity drift is now narrower: session orchestration / setup hydration / candidate
  ordering still differ between exact-feed backtest and archived live
- next investigation should compare exact-feed backtest against `daily-replay` driven from the
  same `paper_feed_audit` session to isolate backtest-vs-paper engine differences from feed
  differences

### 2026-04-22 follow-up: exact-feed replay vs exact-feed backtest still diverge

That follow-up compare was run on the same archived live-kite sessions using the new
`paper_feed_audit` source on both paths.

Temporary exact-feed replay sessions:

- `TMP_EXACT_REPLAY_CPR_LEVELS_LONG-2026-04-20-live-kite`
- `TMP_EXACT_REPLAY_CPR_LEVELS_SHORT-2026-04-20-live-kite`
- `TMP_EXACT_REPLAY_CPR_LEVELS_LONG-2026-04-21-live-kite`
- `TMP_EXACT_REPLAY_CPR_LEVELS_SHORT-2026-04-21-live-kite`
- `TMP_EXACT_REPLAY_CPR_LEVELS_LONG-2026-04-22-live-kite`
- `TMP_EXACT_REPLAY_CPR_LEVELS_SHORT-2026-04-22-live-kite`

Observed counts / PnL:

- `2026-04-20 LONG`
  - live: `30` trades, `₹13,520.31`
  - exact-feed replay: `23` trades, `₹9,689.13`
  - exact-feed backtest: `26` trades, `₹10,516.22`
- `2026-04-20 SHORT`
  - live: `38` trades, `₹460.64`
  - exact-feed replay: `22` trades, `₹1,006.15`
  - exact-feed backtest: `28` trades, `₹1,602.46`
- `2026-04-21 LONG`
  - live: `29` trades, `₹3,856.14`
  - exact-feed replay: `7` trades, `₹1,071.85`
  - exact-feed backtest: `17` trades, `₹3,064.07`
- `2026-04-21 SHORT`
  - live: `22` trades, `-₹2,748.10`
  - exact-feed replay: `14` trades, `-₹4,562.35`
  - exact-feed backtest: `21` trades, `-₹2,407.50`
- `2026-04-22 LONG`
  - live: `34` trades, `₹7,084.62`
  - exact-feed replay: `11` trades, `₹5,869.45`
  - exact-feed backtest: `19` trades, `₹7,663.09`
- `2026-04-22 SHORT`
  - live: `30` trades, `-₹2,447.39`
  - exact-feed replay: `17` trades, `-₹1,215.13`
  - exact-feed backtest: `23` trades, `-₹942.80`

Important structural result:

- replay did **not** match backtest even when both used the same captured `paper_feed_audit`
  bars
- replay was a strict subset of backtest on all six sessions
- replay-vs-backtest overlap:
  - `2026-04-20 LONG`: shared `23`, replay-only `0`, backtest-only `3`
  - `2026-04-20 SHORT`: shared `22`, replay-only `0`, backtest-only `6`
  - `2026-04-21 LONG`: shared `7`, replay-only `0`, backtest-only `10`
  - `2026-04-21 SHORT`: shared `14`, replay-only `0`, backtest-only `7`
  - `2026-04-22 LONG`: shared `11`, replay-only `0`, backtest-only `8`
  - `2026-04-22 SHORT`: shared `17`, replay-only `0`, backtest-only `6`

Examples of backtest-only trades that replay did not open:

- `2026-04-20 LONG`: `AMBUJACEM 10:15`, `BBOX 09:35`, `TATACONSUM 09:45`
- `2026-04-20 SHORT`: `AEROENTER 09:25`, `CEATLTD 09:20`, `GSPL 09:20`
- `2026-04-21 LONG`: `BALMLAWRIE 09:20`, `DEEPAKFERT 09:20`, `MANYAVAR 09:20`
- `2026-04-22 LONG`: `ADOR 09:20`, `ATLASCYCLE 09:20`, `GRAPHITE 09:20`

Interpretation:

- the first-bar WebSocket candle source is a real live-vs-historical problem
- but it is **not** the only parity blocker
- after removing candle-source drift, a second gap remains between the paper/replay driver
  and the batch backtest engine itself
- priority should shift to replay-vs-backtest orchestration and entry-selection parity before
  spending more time on live WebSocket candle reconstruction

Likely investigation area:

- `engine/paper_session_driver.py`
- `engine/paper_runtime.py`
- `engine/cpr_atr_strategy.py`

Specifically compare how replay and batch backtest handle:

- setup-row hydration timing
- bar-major candidate evaluation
- entry selection under `max_positions`
- position-close cash release timing across later bars

### 2026-04-22 resolution: replay/backtest gap was sparse-tape terminal close handling

Root cause is now confirmed and fixed in replay.

What was happening:

- archived `paper_feed_audit` is sparse by design because live shrinks the active symbol list
  during the session
- some exact-feed counterfactual trades opened on symbols whose captured tape stopped early
  (`CEATLTD`, `GSPL`, etc.)
- batch backtest already handled that case by falling back to a `TIME` exit at `15:15`
  using the last available captured close
- replay did not; it left those positions `OPEN`, so they never archived into
  `backtest_results`

Evidence:

- on clean exact-feed replay reruns, the prior replay-vs-backtest trade deficit matched the
  count of stranded `OPEN` paper positions exactly:
  - `2026-04-20 LONG`: diff `3`, open `3`
  - `2026-04-20 SHORT`: diff `6`, open `6`
  - `2026-04-21 LONG`: diff `10`, open `10`
  - `2026-04-21 SHORT`: diff `7`, open `7`
  - `2026-04-22 LONG`: diff `8`, open `8`
  - `2026-04-22 SHORT`: diff `6`, open `6`

Fix:

- replay now synthesizes one terminal candle at `params.time_exit` for any still-open symbol,
  using that symbol's last known captured close
- this reuses the normal paper exit path and closes the position as `TIME`, matching the
  batch backtest contract on sparse tapes

Validation after the fix:

- `2026-04-20 LONG`: replay `26`, backtest `26`, open `0`
- `2026-04-20 SHORT`: replay `28`, backtest `28`, open `0`
- `2026-04-21 LONG`: replay `17`, backtest `17`, open `0`
- `2026-04-21 SHORT`: replay `21`, backtest `21`, open `0`
- `2026-04-22 LONG`: replay `19`, backtest `19`, open `0`
- `2026-04-22 SHORT`: replay `23`, backtest `23`, open `0`

Implication:

- replay and backtest now agree on the same sparse exact-feed tape
- the remaining live-vs-backtest parity problem is back in the live-input domain:
  first-bar feed drift and live-session symbol/tape sparsity, not replay-vs-backtest logic

### 2026-04-25 follow-up: Apr 24 kite-live vs baseline backtest — most detailed quantification yet

Compared `CPR_LEVELS_SHORT-2026-04-24-live-kite` (32 trades) against backtest slice from
`fd763aa18d54` on 2026-04-24 (19 trades). Symbol-level diff and live `paper_feed_audit`
OR data extracted directly from DuckDB.

**Trade count:**

| Session | Trades | PnL | INITIAL_SL | TARGET | BREAKEVEN_SL |
|---------|--------|-----|-----------|--------|-------------|
| Live kite-live | 32 | +₹14,963 | 4 | 10 | 10 |
| Baseline backtest | 19 | +₹15,423 | 2 | 10 | 7 |

Only **5 symbols** overlapped (20MICRONS, EUROPRATIK, MWL, SANATHAN, REMSONSIND). The
live session filled `max_positions=10` at 09:20 with wide-OR symbols, blocking 14 clean
backtest-qualified symbols (FIEMIND +₹1,539, MAGADSUGAR +₹2,129, STALLION +₹1,348,
UFBL +₹1,563, RGL +₹1,062, GARUDA +₹1,368, KPEL +₹3,590).

**OR filter classification for live-only symbols (27 total):**

| Group | Count | live or_atr | hist or_atr | Filter result |
|-------|-------|-------------|-------------|---------------|
| LIVE_PASS / HIST_FAIL | 19 | ≤ 2.5 | > 2.5 | Live accepts, backtest rejects |
| LIVE_PASS / HIST_PASS | 2 (AIAENG, ASALCBR) | ≤ 2.5 | ≤ 2.5 | Both should accept |
| LIVE_FAIL / HIST_FAIL | 6 (CARERATING, AWL, GRPLTD, AGARIND, COROMANDEL, AJMERA) | > 2.5 | > 2.5 | Both should reject — anomalous entries |

The 19 LIVE_PASS/HIST_FAIL symbols confirm the MODE_QUOTE tick sparsity mechanism:

| Symbol | Live or_atr | Hist or_atr | Ratio (hist/live) |
|--------|-------------|-------------|-------------------|
| INDOTHAI | 0.477 | 4.291 | 9.0× |
| AVTNPL | 0.910 | 4.332 | 4.8× |
| ARIS | 1.861 | 6.436 | 3.5× |
| PSPPROJECT | 1.305 | 5.871 | 4.5× |
| COROMANDEL | 4.658 | 10.118 | 2.2× |

Live tick-built OR ranges are typically **30–90% narrower** than the REST API candle, causing
binary filter flips from HIST_FAIL → LIVE_PASS.

**6 anomalous entries (LIVE_FAIL but still traded):**
These 6 have live audit `or_atr > 2.5` (fail even by tick data) yet entered the session.
Likely mechanism: the `setup_row` was cached at an **earlier bar** (before the full 09:15
range was established in the live feed), with a narrower partial-bar OR that passed the
filter. Once cached, the setup_row is not revalidated with later bars. Needs a specific
targeted investigation; the 6 trades were net-positive (+₹798 EUROPRATIK, +₹1,044 GRPLTD,
etc.) so no correctness regression, but the entry should not have happened.

**Practical implication for daily monitoring:**
Do not compare live-kite trade count against baseline backtest daily. Expected kite-live
count = baseline × 1.5–2× on typical open days. Actual P&L will be comparable only at the
session level (not trade level) because extra trades tend to be breakeven/small-loss while
the missed clean setups would have been targets.

---

## Deferred Parity Follow-Ups

**Status:** OPEN
**Severity:** Medium

These items are intentionally deferred until after the current market-open window. They are
important for long-term parity hygiene, but they are not required to complete the immediate
pre-open workflow.

### 0) Quality-sort for max_positions selection (DONE — implemented)

See standalone issue "2026-04-25 — PARITY: max_positions slot selection is non-deterministic"
for historical context.

`engine/bar_orchestrator.py` now computes a shared candidate quality score
(`effective_rr / (1 + or_atr_ratio)`), sorts same-bar candidates by descending score,
and then by symbol for deterministic ties. `engine/cpr_atr_strategy.py` uses the same
ordering to preserve parity across backtest/replay execution.

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

## 2026-04-27 — EXECUTION SAFETY: paper live operator controls, idempotency, and reconciliation

**Status:** FIXED / IMPLEMENTED
**Severity:** High — prerequisite before any real-broker dry run

### Symptom

Paper-live was usable for strategy validation, but the real-trading safety surface was incomplete:

- operator flatten documentation still assumed killing the live process before flattening
- paper order events had no idempotency key for retry-safe order writes
- manual/admin flatten pricing could fall back to stale closed-candle marks instead of latest live LTP
- no explicit reconciliation command existed for order/position/session invariants
- no automatic live-loop behavior existed for critical reconciliation failures

### Root Cause

The system evolved from paper validation into live-operation rehearsal. Paper execution state was
still modelled as direct DB writes instead of broker-like order intents with throttling, idempotency,
and reconciliation.

### Fix Applied

- Added `engine/execution_safety.py` with a default 8 orders/sec `OrderRateGovernor` and
  deterministic order idempotency key builder.
- Added `paper_orders.idempotency_key` and duplicate suppression in `db/paper_db.py`.
- Routed paper order events through the governor in `engine/paper_runtime.py`.
- Added idempotency keys for entry, normal exit, partial exit, position flatten, and session flatten.
- Added live-paper immediate manual flatten pricing from latest in-memory LTP via
  `KiteTickerAdapter.get_last_ltp()` / local-feed LTP, with fallbacks to feed-state/position marks.
- Added `pivot-paper-trading send-command` for active-session close-one / close-all commands.
- Added `pivot-paper-trading flatten-both` to queue close-all for both LONG and SHORT sessions.
- Added `send-command --action set_risk_budget` to reduce/adjust one running session's future-entry
  budget and slot caps without resizing existing open positions.
- Added `send-command --action pause_entries`, `resume_entries`, and `cancel_pending_intents` for
  explicit entry gating and admin-queue cleanup.
- Added dashboard `/paper_ledger` controls for close symbols, flatten session, flatten LONG+SHORT,
  pause/resume entries, cancel pending intents, risk-budget update, and reconciliation.
- Added `engine/paper_reconciliation.py` and `pivot-paper-trading reconcile --strict`.
- Added `engine/broker_adapter.py` with `BrokerAdapter`, `PaperBrokerAdapter`, and
  `ZerodhaBrokerAdapter(mode="REAL_DRY_RUN")`.
- Added `pivot-paper-trading real-dry-run-order` to generate and record Zerodha payloads without
  calling Kite `place_order`.
- Live loop now runs reconciliation after bars, admin close commands, and sentinel flatten. Critical
  findings after `close_positions` disable new entries while preserving exit monitoring for open
  positions. Critical findings after `close_all` / sentinel flatten fail the session closed instead
  of marking it cleanly completed.
- Replaced executable `datetime.utcnow()` usage in `db/paper_db.py` with timezone-aware UTC helpers.

### Validation

- `uv run pytest tests/test_execution_safety.py tests/test_paper_reconciliation.py tests/test_paper_live_polling.py tests/test_live_market_data.py tests/test_paper_admin_commands.py tests/test_web_state.py -q` → `49 passed`
- `uv run pytest tests/test_broker_adapter.py tests/test_execution_safety.py tests/test_paper_db.py tests/test_paper_reconciliation.py tests/test_paper_admin_commands.py tests/test_paper_trading_cli.py -q` → `55 passed`
- `uv run ruff check engine\paper_runtime.py agent\tools\backtest_tools.py scripts\paper_trading.py scripts\paper_live.py web\state.py web\pages\ops_pages.py tests\test_paper_admin_commands.py tests\test_web_state.py tests\test_paper_live_polling.py` → clean
- `uv run ruff check engine\broker_adapter.py db\paper_db.py scripts\paper_trading.py tests\test_broker_adapter.py` → clean
- Local-feed live-paper smoke:
  `daily-live --trade-date 2026-04-24 --feed-source local --preset CPR_LEVELS_RISK_LONG --symbols SBIN,RELIANCE,TCS --session-id safety-smoke-20260424-long --max-cycles 25 --no-alerts --skip-coverage`
  completed with `final_status=COMPLETED`, `terminal_reason=NO_TRADES_ENTRY_WINDOW_CLOSED`, no open positions.
- `pivot-paper-trading reconcile --session-id safety-smoke-20260424-long --strict` → `ok=true`.

### Operator Contract

- Strategy exits remain completed-5-minute-candle driven.
- Manual/operator/emergency exits are immediate market-style exits using latest live mark/LTP when
  available.
- Risk-budget reductions apply to future entries only. Existing open positions continue to be
  managed unless the operator also sends `close_positions` or `close_all`.
- During active live sessions, use `send-command` / `flatten-both`, not direct `flatten-all`, to avoid
  DuckDB writer-lock contention. Reconciliation is automatic in the live loop; the standalone
  `reconcile --strict` command is for explicit diagnostics or gates.

---

## 2026-04-28 — EXECUTION SAFETY: broker reconciliation and pilot guardrails

**Status:** FIXED / IMPLEMENTED
**Severity:** High — final dry-run gate before any supervised real-order pilot

### Symptom

The paper safety layer could generate Zerodha dry-run payloads, but there was no broker-state
comparison contract or explicit pilot scope gate.

### Fix Applied

- Added `engine/broker_reconciliation.py` with normalized broker order and position snapshots.
- Added local-vs-broker reconciliation for missing broker orders, symbol/side/quantity mismatches,
  missing broker positions, and untracked broker positions.
- Added read-only `ZerodhaBrokerAdapter.fetch_order_snapshots()` and
  `fetch_position_snapshots()`; tests verify they do not call `place_order`.
- Added `pivot-paper-trading broker-reconcile --strict` for supplied broker snapshot JSON.
- Added `PilotGuardrails` and `pivot-paper-trading pilot-check --strict`.
- Pilot guardrails require max 2 symbols, quantity 1, max Rs10,000 notional, MIS product, MARKET
  order type, and explicit `I_ACCEPT_REAL_ORDER_RISK` acknowledgement.
- Passing pilot guardrails still returns `real_orders_enabled=false`; real order placement remains
  disabled.

### Validation

- `uv run pytest tests/test_broker_adapter.py tests/test_broker_reconciliation.py -q` → `10 passed`
- `uv run pytest tests/test_broker_reconciliation.py tests/test_broker_adapter.py tests/test_paper_reconciliation.py -q` → `13 passed`
- `uv run ruff check engine\broker_adapter.py engine\broker_reconciliation.py scripts\paper_trading.py tests\test_broker_adapter.py tests\test_broker_reconciliation.py` → clean
- `uv run pivot-paper-trading broker-reconcile --help` → command registered
- `uv run pivot-paper-trading pilot-check --help` → command registered

---

## 2026-04-29 — INCIDENT: live sessions started with zero setup rows after false data-readiness pass

**Status:** FIXED IN CODE — needs next market-day validation
**Severity:** Critical — one live-paper trading day lost because sessions were connected but unable to evaluate trades

### Symptom

`CPR_LEVELS_LONG` and `CPR_LEVELS_SHORT` live sessions for 2026-04-29 started and received live
bars, but setup prefetch had zero usable trade-date rows. The live loop skipped symbols instead
of evaluating entries, so the entry window was lost.

### Root Cause

Pre-market validation checked the wrong contract. Today's intraday candles are not expected before
market open, and future/current-day materialized setup rows are not required either. Live CPR setup
must be derived from the latest completed trading day's daily/ATR inputs plus the live 09:15
opening-range candle. `pivot-data-quality --date today` could pass with an empty same-day
5-minute symbol set, while `daily-live` still failed because startup incorrectly required
exact-date `market_day_state` rows.

### Fix Applied

- `pivot-data-quality --date today` now uses the dated saved universe in pre-market/setup-only
  mode and fails unless previous completed-day `v_daily`, `v_5min`, `atr_intraday`, and
  `cpr_thresholds` cover that universe. Sparse symbol/day gaps up to 5% are warnings; broad gaps
  still fail closed.
- `daily-prepare` live validation now checks the same previous completed-day live prerequisites
  and does not require future-date `cpr_daily`, `market_day_state`, or `strategy_day_state` rows.
- Live CPR pre-filter keeps the full universe when only an older completed `cpr_daily` date exists,
  avoiding a false block before the live day starts.
- `daily-live` no longer fails solely because exact-date `market_day_state` rows are absent; it
  retries setup resolution from previous completed-day data plus live opening-range candles.
- Added `engine/execution_defaults.py` as the shared sizing source for backtest, replay, live,
  paper DB sessions, settings, and agent backtest tools.
- Updated canonical CPR presets to `portfolio_value=₹10L`, `capital=₹2L`, `risk_pct=1%`,
  `max_positions=5`, and `max_position_pct=0.20`; risk per position is therefore ₹2,000.
- Paper `--multi` now verifies LONG/SHORT params against the named backtest presets and persists
  a resolved strategy-config fingerprint in `paper_sessions.strategy_params`.
- Existing live sessions are refused if their stored strategy fingerprint differs from the
  requested preset/config, preventing silent live/backtest drift such as a missing
  `momentum_confirm`.
- Ingestion docs now require `pivot-refresh --prepare-paper --trade-date <next_trading_date>` and
  previous completed-day DQ readiness as the automated live-readiness gate.
- Added `pivot-refresh --eod-ingest --date <today> --trade-date <next_trading_date>` as the
  guarded single-command EOD path. It enforces refresh-instruments -> daily ingest -> 5-min ingest
  -> build -> daily-prepare -> final DQ in one process and stops on first failure.
- `pivot-refresh --eod-ingest` now passes `--skip-existing` to daily and 5-minute Kite ingestion
  by default, logs skipped/rerun behavior clearly, and exposes `--force-ingest` for deliberate
  refetches.
- `pivot-refresh` now streams child stdout/stderr directly, so redirected EOD logs show live Kite
  ingestion/build progress instead of staying silent until a subprocess exits.

### Validation

- `uv run pytest tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_paper_trading_workflow.py tests/test_live_market_data.py tests/test_paper_trading_cli.py tests/test_strategy.py tests/test_backtest_tools.py tests/test_settings.py -q` → `158 passed`
- `uv run pytest tests/test_data_quality_cli.py tests/test_paper_prepare.py -q` → `20 passed` after sparse full-universe tolerance
- `uv run pytest tests/test_refresh.py -q` → `6 passed`
- `uv run pytest tests/test_refresh.py tests/test_data_quality_cli.py tests/test_paper_prepare.py -q` → `25 passed`
- `uv run ruff check engine/execution_defaults.py engine/strategy_presets.py engine/cpr_atr_strategy.py engine/run_backtest.py engine/cpr_atr_utils.py agent/tools/backtest_tools.py config/settings.py db/paper_db.py scripts/data_quality.py scripts/paper_prepare.py scripts/paper_trading.py scripts/paper_live.py tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_paper_trading_workflow.py tests/test_live_market_data.py tests/test_paper_trading_cli.py tests/test_strategy.py tests/test_backtest_tools.py tests/test_settings.py` → clean
- `uv run ruff check scripts/data_quality.py scripts/paper_prepare.py tests/test_data_quality_cli.py tests/test_paper_prepare.py docs/KITE_INGESTION.md docs/ISSUES.md` → clean
- `uv run ruff check scripts/refresh.py scripts/data_quality.py scripts/paper_prepare.py tests/test_refresh.py tests/test_data_quality_cli.py tests/test_paper_prepare.py` → clean
- `uv run ruff check scripts/refresh.py tests/test_refresh.py` → clean
- `uv run pytest tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_live_market_data.py tests/test_paper_trading_workflow.py tests/test_paper_trading_cli.py tests/test_refresh.py -q` → `93 passed`
- `uv run ruff check scripts/data_quality.py scripts/paper_prepare.py scripts/paper_trading.py scripts/paper_live.py scripts/refresh.py tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_live_market_data.py tests/test_paper_trading_workflow.py tests/test_paper_trading_cli.py tests/test_refresh.py` → clean
- `uv run pytest tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_paper_runtime.py tests/test_live_market_data.py tests/test_paper_trading_workflow.py tests/test_paper_trading_cli.py tests/test_refresh.py -q` → `120 passed`
- `uv run ruff check engine/paper_runtime.py scripts/data_quality.py scripts/paper_prepare.py scripts/paper_trading.py scripts/paper_live.py scripts/refresh.py tests/test_data_quality_cli.py tests/test_paper_prepare.py tests/test_paper_runtime.py tests/test_live_market_data.py tests/test_paper_trading_workflow.py tests/test_paper_trading_cli.py tests/test_refresh.py` → clean

---

## 2026-04-30 — DATA GAP FOLLOW-UP: historical CPR/ATR/state sparse gaps remain after pack repair

**Status:** OPEN — deferred; not blocking 2026-04-30 live paper
**Severity:** Medium — baseline-window completeness follow-up

### Symptom

After the interrupted canonical-full runtime rebuild was repaired, `intraday_day_pack` coverage
was fully restored for the `canonical_full` universe, but small historical gaps still remain in
derived CPR/ATR/state tables for sparse symbol-days.

### Current Verified State

- `intraday_day_pack` is repaired for the baseline window `2025-01-01` → `2026-04-29`.
- Missing pack rows where source 5-minute data exists: `0`.
- Duplicate pack `(symbol, trade_date)` rows: `0`.
- No future `2026-04-30+` runtime rows were created.
- Remaining derived-table gaps where source 5-minute data exists:
  - `atr_intraday`: `573` symbol-days across `491` symbols.
  - `cpr_daily`: `170` symbol-days across `166` symbols.
  - `market_day_state`: `552` symbol-days across `448` symbols.
  - `strategy_day_state`: `552` symbol-days across `448` symbols.

### Follow-Up

Investigate whether the remaining derived-table gaps are expected sparse symbol-days
(suspended/no daily candle/no ATR lookback) or repairable runtime gaps. Do this before final
baseline promotion, but do not rerun broad Kite ingestion unless raw parquet coverage is proven
missing.

---

## 2026-05-03 — FIXED: RISK: enforce_session_risk_controls uses only realized PnL, ignoring open losses

**Status:** FIXED
**Severity:** High

### Symptom

A live session with large open unrealized losses never triggers the daily-loss or max-drawdown flatten, because `enforce_session_risk_controls` evaluates only closed-position PnL.

### Root Cause

`engine/paper_runtime.py:892-907` — `enforce_session_risk_controls()` calls `_risk_limit_reasons(session, as_of, realized_pnl)` where `realized_pnl` is taken from `summary["realized_pnl"]` only. Open/unrealized position PnL from `summary["open_pnl"]` or mark-to-market is never included. `engine/paper_risk.py:38-46` then compares that realized-only value against the daily-loss and drawdown thresholds.

### Impact

A session that has ₹0 realized PnL but ₹50K open losses will not trigger the `max_daily_loss_pct` flatten even if the threshold is 2%. The risk gate is effectively blind to open positions.

### Location

`engine/paper_runtime.py:900-907`, `engine/paper_risk.py:37-47`

### Fix

`enforce_session_risk_controls()` now gates on `summary["net_pnl"]`, which includes open
unrealized P&L, and stores that risk P&L in `daily_pnl_used`. Regression coverage:
`tests/test_paper_runtime.py::test_enforce_session_risk_controls_includes_open_unrealized_loss`.

---

## 2026-05-03 — FIXED: RISK: malformed flatten_time string silently skips all risk checks

**Status:** FIXED
**Severity:** High

### Symptom

If `flatten_time` stored on a session is an empty string or any value without a `:` separator, `_risk_limit_reasons()` returns an empty list immediately, bypassing daily-loss limit and max-drawdown checks entirely.

### Root Cause

`engine/paper_risk.py:25-28`:
```python
if isinstance(flatten_time, str):
    parts = flatten_time.split(":")
    if len(parts) < 2:
        return reasons   # early return skips all remaining checks
```
The `len(parts) < 2` guard is placed before the daily-loss and drawdown blocks, so a blank or malformed string short-circuits the entire function.

### Impact

All session-level risk limits are silently inactive if `flatten_time` is blank, null-string, or malformed in the DB.

### Location

`engine/paper_risk.py:25-28`

### Fix

Malformed `flatten_time` is ignored only for the time-exit check; daily loss and drawdown
checks still run. Regression coverage:
`tests/test_paper_runtime.py::test_enforce_session_risk_controls_malformed_flatten_time_keeps_loss_checks`.

---

## 2026-05-03 — OPEN: STRATEGY: min_effective_rr gate evaluated against runner target (R2/S2), not first target (R1/S1)

**Status:** OPEN
**Severity:** High

### Symptom

When `scale_out_pct > 0`, trades that would fail the `min_effective_rr` check against R1/S1 are admitted because the check uses the farther R2/S2 as the target price.

### Root Cause

`engine/cpr_atr_shared.py:277`:
```python
target_price = runner_target_price if use_scale_out else first_target_price
```
This line sets `target_price` to the runner target (R2/S2) before the `min_effective_rr` gate on line 306-310. The RR is therefore computed as `|runner_target - fill| / sl_distance`, which is always larger than the first-target RR. Trades with insufficient reward at R1/S1 pass the gate when R2 is used.

### Impact

Trade quality is overstated when scale-out is active. Trades that do not meet the RR threshold at the first take-profit level are admitted.

### Location

`engine/cpr_atr_shared.py:277`, `306-310`

---

## 2026-05-03 — FIXED: RUNTIME: partial exit leaves in-memory position tracker with stale qty and exposure

**Status:** FIXED
**Severity:** High

### Symptom

After a scale-out (PARTIAL) exit, `SessionPositionTracker` still holds the full position quantity and exposure. Subsequent `can_open_new()` and equity calculations treat the partial exit as if no size was reduced.

### Root Cause

`engine/paper_runtime.py:1602-1603` — when `advance.get("action") == "PARTIAL"`, only cash is credited:
```python
position_tracker.credit_cash(float(advance.get("exit_value") or 0.0))
```
The tracker's `_open[symbol].current_qty` is never reduced. `record_close()` is not called (position remains open), and there is no `update_trail_state()` call that would reduce the in-memory qty. The DB is correctly updated at line 1260, but the in-memory mirror diverges.

### Impact

`current_equity()` overstates equity (cash credited without reducing open exposure). Downstream position sizing for the same bar uses stale equity.

### Location

`engine/paper_runtime.py:1245-1263`, `1602-1603`, `engine/bar_orchestrator.py:177-194`

### Fix

`SessionPositionTracker.record_partial()` now credits partial-exit cash and reduces cached
`current_qty`; the paper runtime calls it after PARTIAL exits. Regression coverage:
`tests/test_bar_orchestrator.py::test_session_position_tracker_partial_reduces_qty_and_credits_cash`.

---

## 2026-05-03 — FIXED: RUNTIME: partial exit with real-order routing raises RuntimeError, crashing candle loop

**Status:** FIXED
**Severity:** High

### Symptom

If `real_order_router` is enabled and a PARTIAL scale-out decision occurs, the candle evaluation raises a `RuntimeError` that is not caught, terminating the entire session's candle processing loop.

### Root Cause

`engine/paper_runtime.py:1211-1212`:
```python
if decision.action == "PARTIAL" and real_order_router is not None and real_order_router.enabled:
    raise RuntimeError("automated real-order routing does not support partial scale-out exits")
```
This is placed inside the candle evaluation coroutine with no local error handler. The exception propagates up and the session exits abnormally.

### Impact

Any live session with `real_order_router` enabled and `scale_out_pct > 0` will crash as soon as the first partial exit is triggered.

### Location

`engine/paper_runtime.py:1211-1212`

### Fix

`scripts/paper_live.py` now blocks startup when real-order routing is enabled with
`cpr_levels.scale_out_pct > 0`, marks the session `FAILED`, and returns
`real_order_partial_scale_out_unsupported` before any candle loop can crash.

---

## 2026-05-03 — FIXED: ARCHIVE: FLATTENED positions excluded from paper session archive

**Status:** FIXED
**Severity:** High

### Symptom

When a session is manually flattened using `flatten --session-id` or `flatten-all`, positions are marked with `status=FLATTENED`. The subsequent archive writes zero trades to `backtest_results` even though trades exist.

### Root Cause

`scripts/paper_archive.py:107`:
```python
closed_positions = paper_db.get_session_positions(session_id, statuses=["CLOSED"])
```
`FLATTENED` is a distinct status used for manually closed positions. It is not included in the status filter. The archive therefore sees zero positions for sessions that ended via manual flatten.

### Impact

Archived trade count, PnL, and win rate are understated or zero for all manually-flattened sessions. Dashboard archived view shows no trades.

### Location

`scripts/paper_archive.py:107`

### Fix

`archive_completed_session()` now reads both `CLOSED` and `FLATTENED` positions. Regression
coverage: `tests/test_paper_archive.py::test_archive_completed_session_includes_flattened_positions`.

---

## 2026-05-03 — FIXED: ARCHIVE: metadata and results written without a transaction boundary

**Status:** FIXED
**Severity:** High

### Symptom

If `store_backtest_results()` fails after `store_run_metadata()` succeeds, `backtest.duckdb` contains orphaned metadata rows with no corresponding trade rows.

### Root Cause

`scripts/paper_archive.py:156-169` — the two writes are independent calls with no wrapping transaction:
```python
backtest_db.store_run_metadata(...)   # call 1
if rows:
    backtest_db.store_backtest_results(pl.DataFrame(rows))  # call 2 — independent
```
A crash or DuckDB error between calls leaves the DB in a partial state. Re-running the archive appends a second metadata row rather than cleaning up.

### Impact

`run_metrics` and `backtest_results` are out of sync. Dashboard compare page shows phantom sessions with no trades.

### Location

`scripts/paper_archive.py:156-170`

### Fix

Archive failures now clean up partial PAPER rows via `delete_runs([session_id])` before
re-raising, preventing orphaned archive metadata from surviving a failed result write.

---

## 2026-05-03 — FIXED: ARCHIVE: zero PnL treated as missing, recomputed from prices

**Status:** FIXED
**Severity:** Medium

### Symptom

A break-even trade (entry == exit) stored with `pnl=0.0` is recomputed during archiving instead of using the stored value. If `exit_price` is `None` or inaccurate, the recomputed PnL will be wrong.

### Root Cause

`scripts/paper_archive.py:25`:
```python
if not pnl:   # catches 0.0 as falsy
    pnl = (exit_price - entry_price) * qty ...
```
`if not pnl` is true for `None`, `0`, `0.0`, and `False`. A scratch trade stored with `pnl=0.0` is overwritten. Should be `if pnl is None:`.

### Impact

Break-even trades can have their PnL silently recalculated. If `exit_price` defaulted to `entry_price` (line 22), the result is the same, but if the stored exit_price differs, the archived value is wrong.

### Location

`scripts/paper_archive.py:25-30`

---

## 2026-05-03 — FIXED: DB: ConstraintException in open_position masks all schema/integrity errors

**Status:** FIXED
**Severity:** High

### Symptom

When `open_position()` raises any `duckdb.ConstraintException` — not just the expected duplicate-open violation — the handler silently swallows the error, queries for an existing OPEN row, and either returns stale data or re-raises with no context.

### Root Cause

`db/paper_db.py:775-792` — the exception handler catches the broad `duckdb.ConstraintException` class. DuckDB can raise this for primary key, foreign key, check constraint, and uniqueness violations. The handler only knows how to recover from the uniqueness case (duplicate OPEN row). All other constraint failures follow the same code path and produce misleading log output.

### Impact

Unrelated data integrity errors (bad position_id, check constraint violation) are misattributed as "duplicate open" and can return wrong position data to the caller.

### Location

`db/paper_db.py:775-792`

### Fix

`open_position()` now performs the duplicate-OPEN lookup before insertion and re-raises
unrelated DuckDB constraint failures with session/symbol context instead of treating every
constraint as duplicate-open recovery.

---

## 2026-05-03 — FIXED: DB: update_position has no state-transition guard

**Status:** FIXED
**Severity:** High

### Symptom

Any caller can set `status=OPEN` on an already-CLOSED position, or set `status=CLOSED` on a position that was never OPEN. There is no enforcement of the intended OPEN → CLOSED/FLATTENED lifecycle.

### Root Cause

`db/paper_db.py:840-902` — `update_position()` builds an UPDATE from whatever kwargs are passed and executes it unconditionally. There is no check of the current status before applying the new status.

### Impact

Bugs in calling code (double-close, stale reference, async reordering) can leave positions in invalid states without any error. The UNIQUE INDEX on `(session_id, symbol, status)` provides partial protection for duplicate OPEN rows, but does not prevent status regressions.

### Location

`db/paper_db.py:840-902`

### Fix

`update_position()` now rejects status regressions from terminal states back to `OPEN` and
rejects invalid terminal transitions. Regression coverage:
`tests/test_paper_db.py::test_update_position_rejects_reopening_closed_position`.

---

## 2026-05-03 — FIXED: DB: UNIQUE index on (session_id, symbol, status) blocks re-entries and multiple manual closes

**Status:** FIXED
**Severity:** High

### Symptom

If a symbol is re-entered in the same session (which `position_closed_today` prevents in normal flow but can happen in error recovery), `open_position()` raises a ConstraintException and returns the stale prior OPEN row. Additionally, two manual `FLATTENED` closes for the same symbol in the same session (e.g., after a bug leaves a ghost position) are blocked by the unique constraint.

### Root Cause

`db/paper_db.py:353-355`:
```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_pp_session_symbol_status
ON paper_positions(session_id, symbol, status)
```
This prevents multiple rows with the same (session, symbol, status) triple. While intentional for OPEN rows, it also prevents two CLOSED or two FLATTENED rows for the same session/symbol, making error recovery writes fail.

### Impact

Error recovery paths that try to close or flatten an already-closed position (e.g., a delayed duplicate flatten command) raise ConstraintExceptions instead of idempotently succeeding.

### Location

`db/paper_db.py:353-355`

### Fix

The old unique index is dropped during table initialization and replaced with a non-unique
lookup index. Duplicate OPEN protection is enforced explicitly in `open_position()`, while
multiple historical CLOSED/FLATTENED rows for the same session/symbol are allowed. Regression
coverage:
`tests/test_paper_db.py::test_open_position_allows_multiple_closed_rows_for_same_symbol`.

---

## 2026-05-03 — FIXED: KITE: subscription state updated even when subscribe/unsubscribe raises exception

**Status:** FIXED
**Severity:** High

### Symptom

After a Kite WebSocket subscribe or unsubscribe call fails, `self._subscribed_tokens` is still updated to the intended set. On the next reconciliation, the adapter believes those symbols are already subscribed and does not retry, silently missing market data for those symbols.

### Root Cause

`engine/kite_ticker_adapter.py:386-398` — the `_subscribed_tokens` assignment happens unconditionally after the try/except blocks:
```python
if to_sub:
    try:
        ticker.subscribe(to_sub)
        ticker.set_mode(...)
    except Exception:
        logger.exception(...)
with self._lock:
    self._subscribed_tokens = needed_tokens   # runs even if subscribe raised
```

### Impact

Symbols that failed to subscribe are silently dropped from price feeds. Live sessions lose data for affected symbols without any ongoing alert or retry.

### Location

`engine/kite_ticker_adapter.py:386-398`

### Fix

`_subscribed_tokens` now updates only for subscribe/unsubscribe calls that succeed, so failed
subscriptions remain pending and are retried on the next reconciliation. Regression coverage:
`tests/test_kite_ticker.py::test_kite_ticker_adapter_does_not_mark_failed_subscribe_as_active`.

---

## 2026-05-03 — WON'T FIX: ALERT: alert dispatcher can double-dispatch alerts during shutdown drain

**Status:** WON'T FIX — reviewed as false positive
**Severity:** High

### Symptom

Alerts sent near session end can be delivered twice — once by the still-running consumer task, and again by the shutdown drain loop.

### Root Cause

`engine/alert_dispatcher.py:183-210` — the shutdown path sets `self._running = False`, waits for the consumer task to finish, then drains remaining queue items. However, the consumer task may still be processing the last item when the drain runs. The same alert is dispatched by both code paths.

### Impact

Duplicate Telegram/email alerts for TRADE_CLOSED or FLATTEN_EOD near session end.

### Location

`engine/alert_dispatcher.py:183-210`

### Triage

Reviewed 2026-05-04. This is not a real duplicate-dispatch path as written: the consumer
removes an event from the queue with `get()` before sending it, and shutdown drains only
items still remaining in the queue after waiting for the consumer. An in-flight event is not
present in the queue to be drained a second time.

---

## 2026-05-03 — FIXED: ORCHESTRATOR: record_open silently overwrites existing position and double-deducts cash

**Status:** FIXED
**Severity:** High

### Symptom

If `record_open()` is called twice for the same symbol (e.g., due to a duplicate open event or bug), the second call overwrites the first tracked position and deducts cash twice, understating available capital.

### Root Cause

`engine/bar_orchestrator.py:177-194`:
```python
self._open[symbol] = tracked        # overwrites without checking
self.cash_available -= max(0.0, float(position_value or 0.0))  # always deducts
```
There is no guard against `symbol` already being present in `self._open`.

### Impact

Double cash deduction can prevent legitimate new entries (`can_open_new()` fails early). The overwritten position tracker loses the original entry price, SL, and trail state.

### Location

`engine/bar_orchestrator.py:177-194`

### Fix

`record_open()` now rejects duplicate opens for the same symbol instead of overwriting the
tracked position and deducting cash twice. Regression coverage:
`tests/test_bar_orchestrator.py::test_session_position_tracker_rejects_duplicate_open_for_symbol`.

---

## 2026-05-03 — FIXED: SUPERVISOR: global variables not declared in main(), breaking signal handling

**Status:** FIXED
**Severity:** High

### Symptom

`paper_supervisor.py` signal handlers reference `_child_heartbeat_path` and `_run_loop_active` module globals. These are assigned in `main()` without `global` declarations, creating local variables that shadow the module-level names. The signal handlers always see the original module-level values (None / False).

### Root Cause

`scripts/paper_supervisor.py:400-405` — assignments like:
```python
_child_heartbeat_path = heartbeat_path
_run_loop_active = True
```
are missing `global _child_heartbeat_path` and `global _run_loop_active` declarations. Python treats these as local variable assignments rather than mutations of the module globals.

### Impact

SIGINT/SIGTERM handlers cannot write to the heartbeat log or stop the watch-relaunch loop reliably.

### Location

`scripts/paper_supervisor.py:400-405`

### Fix

`main()` now declares both `_child_heartbeat_path` and `_run_loop_active` as global before
assigning them, so signal handlers see the active heartbeat path and watch-loop flag.

---

## 2026-05-03 — FIXED: LIVE: session startup force-sets status to ACTIVE, clobbering PAUSED/STOPPING

**Status:** FIXED
**Severity:** High

### Symptom

If an operator sets a session to PAUSED or STOPPING, and then the live runner restarts (resume path), the session status is unconditionally overwritten to ACTIVE on startup.

### Root Cause

`scripts/paper_live.py:699-701`:
```python
if session.status != "ACTIVE":
    session = await _update_session(session_id, deps, status="ACTIVE", notes=notes)
```
Any non-ACTIVE status is forced to ACTIVE without checking whether that transition is valid. A session intentionally paused or being stopped by an operator is silently promoted back to ACTIVE.

### Impact

Operator-initiated pause/stop commands are overridden on runner restart. Combined with the sentinel-file or admin-command flat logic, this can produce unexpected re-activation.

### Location

`scripts/paper_live.py:699-701`

### Fix

`run_live_session()` now treats startup `STOPPING` as terminal and preserves startup
`PAUSED` instead of force-promoting it to ACTIVE. PLANNING/FAILED resume paths can still
activate normally.

---

## 2026-05-03 — FIXED: LIVE: deferred sync window leaves replica stale on crash

**Status:** FIXED
**Severity:** High

### Symptom

If the live process crashes between `defer_sync()` and `flush_deferred_sync()` — e.g., due to an exception in `process_closed_bar_group()` — DB writes completed within that window are never synced to the paper replica. The dashboard shows stale data until the next manual sync or restart.

### Root Cause

`scripts/paper_live.py:1111-1143`:
```python
get_paper_db().defer_sync()
try:
    driver_result = await paper_session_driver.process_closed_bar_group(...)
finally:
    get_paper_db().flush_deferred_sync()
```
`flush_deferred_sync()` is in a `finally` block, so it runs on normal exceptions. However, a hard process exit (SIGKILL, OOM, Windows force-close) bypasses Python's finally handlers entirely.

### Impact

Dashboard shows old position data after an abnormal exit. Operators may not see newly opened/closed positions until the next DB access flushes the pending sync.

### Location

`scripts/paper_live.py:1111-1143`

### Fix

`db/paper_db.py` now writes a `paper_replica/deferred_sync_pending.flag` marker when sync is
deferred, clears it after a successful flush/force-sync, and makes dashboard replica access
attempt a recovery force-sync if a marker survived an abnormal process exit.

---

## 2026-05-03 — FIXED: LIVE: flatten-all includes CANCELLED sessions; flatten-both misses STOPPING/FAILED

**Status:** FIXED
**Severity:** High

### Symptom

`flatten-all` tries to flatten `CANCELLED` sessions (no-op or unintended). `flatten-both` misses `STOPPING` and `FAILED` sessions that still have open positions.

### Root Cause

Two separate filter bugs:

1. `scripts/paper_trading.py:1746` — `flatten-all` query includes `'CANCELLED'` in the status filter. Cancelled sessions have no open positions to close and should be excluded.

2. `scripts/paper_trading.py:1832` — `flatten-both` (sentinel/admin-command path) only targets `ACTIVE` and `PAUSED`. A session in `STOPPING` or `FAILED` state can still have OPEN positions that need flattening.

### Impact

`flatten-all` generates spurious "closed 0 position(s)" entries for cancelled sessions. `flatten-both` silently skips sessions stuck in STOPPING/FAILED with open positions, leaving them unflattened.

### Location

`scripts/paper_trading.py:1746`, `1832`

### Fix

`flatten-all` no longer includes `CANCELLED`; `flatten-both` now includes `STOPPING` and
`FAILED` sessions so stuck sessions with open positions can still receive `close_all`.
Regression coverage in `tests/test_paper_trading_workflow.py` and
`tests/test_paper_admin_commands.py`.

---

## 2026-05-03 — FIXED: PRE-FILTER: date mismatch returns unfiltered full universe instead of empty/failing

**Status:** FIXED
**Severity:** High

### Symptom

When the live pre-filter cannot find CPR rows for the requested `trade_date` and falls back to an older date, and `require_trade_date_rows=True`, it returns the **full unrestricted universe** instead of an empty list or an error.

### Root Cause

`scripts/paper_prepare.py:532-533` (and mirrored in `engine/paper_setup_loader.py:532-533`):
```python
if require_trade_date_rows and prefilter_date != trade_date:
    return list(symbols)   # returns all symbols unfiltered
```
The intent appears to be a passthrough when the date is unavailable, but instead of returning an empty list or raising, the code returns the full input symbol list. This can cause the live session to attempt to trade all ~2100 symbols with no CPR filter applied.

### Impact

A live session started when `cpr_daily` rows are not yet available for the trade date runs with no CPR pre-filtering, wasting Kite API quota and potentially opening positions without valid CPR levels.

### Location

`scripts/paper_prepare.py:532-533`, `engine/paper_setup_loader.py:532-533`

### Fix

Live pre-filter now fails closed when `require_trade_date_rows=True` and the latest
`cpr_daily` row does not match the requested trade date. Regression coverage:
`tests/test_paper_prepare.py::test_pre_filter_symbols_fails_when_exact_cpr_date_missing_for_live`.

---

## 2026-05-03 — FIXED: REPLAY: date filters ignored when preloaded_days pack is supplied

**Status:** FIXED
**Severity:** Medium

### Symptom

When `paper_replay.py` is invoked with a `preloaded_days` pack, the replay processes all dates in the pack regardless of `start_date` / `end_date` parameters. Sessions can include out-of-range dates, producing incorrect PnL attribution.

### Root Cause

`scripts/paper_replay.py:638-640`:
```python
if preloaded_days is not None:
    replay_symbols_set = set(replay_symbols)
    replay_days = [d for d in preloaded_days if d.symbol in replay_symbols_set]
```
The filter is symbol-only. No date range check is applied when using the preloaded pack.

### Impact

Multi-date replay with date-range constraints silently includes extra dates when a preloaded pack is provided. Callers (e.g., parity comparison tools) can see inflated trade counts.

### Location

`scripts/paper_replay.py:638-640`

### Fix

`replay_session()` now applies `start_date` / `end_date` filters even when a caller supplies
`preloaded_days`. Regression coverage:
`tests/test_paper_replay.py::test_replay_session_streams_candles_and_archives_completed_session`.

---

## 2026-05-03 — FIXED: LIVE: admin command file deleted even when processing fails

**Status:** FIXED
**Severity:** Medium

### Symptom

If an admin command JSON file is malformed or causes a processing error, the file is still deleted in the `finally` block, making the command unrecoverable.

### Root Cause

`scripts/paper_live.py:1271-1291` — command file is deleted in the `finally` after a try/except that catches parsing and processing errors. A partially-written or corrupted file is silently discarded rather than moved to a `.failed` location for inspection.

### Impact

Lost admin commands during error conditions. Operators cannot tell whether a command was processed or silently dropped.

### Location

`scripts/paper_live.py:1271-1291`

### Fix

Admin command files are now deleted only after parsing and processing complete successfully.
Malformed or failed command files remain in `.tmp_logs/cmd_<session_id>/` for retry or
operator inspection.

---

## 2026-05-03 — FIXED: LIVE: stage-B direction filter applied as one-way latch, can freeze universe too early

**Status:** FIXED
**Severity:** Medium

### Symptom

Once stage-B direction filtering runs for a CPR_LEVELS session, `stage_b_applied = True` is set and the filter never runs again, even if the setup universe changes (e.g., symbols are added by a late-registering variant or the direction signal changes).

### Root Cause

`engine/paper_session_driver.py:265-289`:
```python
if not stage_b_applied and normalized_strategy == "CPR_LEVELS":
    active_symbols = apply_stage_b_direction_filter(...)
    stage_b_applied = True
```
The latch is set on the first successful run. It is not re-evaluated if `active_symbols` is empty (no direction signal yet) or if the 9:15 bar arrives while the driver has only a partial symbol set.

### Impact

A session that applies stage-B before the full symbol universe is loaded may trade with a truncated universe for the rest of the day.

### Location

`engine/paper_session_driver.py:265-289`

### Fix

`engine/paper_session_driver.py` now re-evaluates the CPR Stage-B direction filter on each
bar instead of treating `stage_b_applied` as a one-way gate. Newly added or late-resolved
symbols are filtered against the current direction state.

---

## 2026-05-03 — FIXED: LIVE: resume_entries admin command restores original universe, ignoring post-open pruning

**Status:** FIXED
**Severity:** Medium

### Symptom

When an operator sends a `resume_entries` admin command to re-enable entries mid-session, the symbol universe is restored to the original pre-filter list, ignoring stage-B filtering, symbols already traded, and symbols excluded after open-position limits were hit.

### Root Cause

`scripts/paper_live.py:1470-1473`:
```python
elif _action == "resume_entries":
    entries_disabled = False
    active_symbols = list(entry_universe_symbols)
```
`entry_universe_symbols` is the full pre-filter output from session start. Any runtime pruning (stage-B, `max_positions` cap, `position_closed_today`) accumulated since then is discarded.

### Impact

Re-activating entries can attempt to open positions in symbols that were already excluded for valid strategic or risk reasons during the current session.

### Location

`scripts/paper_live.py:1470-1473`

### Fix

`scripts/paper_live.py` now preserves the latest filtered entry universe in
`entry_resume_symbols` and uses that set on `resume_entries`, rather than restoring the
original Stage-A universe.

---

## 2026-05-03 — FIXED: SUPERVISOR: watch-mode relaunch cutoff uses host local timezone instead of IST

**Status:** FIXED
**Severity:** Medium

### Symptom

On a server in a non-IST timezone (e.g., UTC), the `_is_within_trading_hours()` cutoff used by the supervisor watch-mode relaunch logic fires at the wrong wall-clock time, potentially relaunching the session outside NSE trading hours.

### Root Cause

`scripts/paper_supervisor.py:45-47`:
```python
current = now or datetime.now().astimezone()
return current.timetz().replace(tzinfo=None) < WATCH_RELAUNCH_CUTOFF
```
`datetime.now().astimezone()` uses the host's local timezone. If the host is not in IST (UTC+5:30), `timetz()` returns a local time, not an IST time, producing an incorrect comparison against the IST-based `WATCH_RELAUNCH_CUTOFF`.

### Impact

On UTC servers, the supervisor will try to relaunch paper sessions up to 5.5 hours outside the intended IST cutoff window.

### Location

`scripts/paper_supervisor.py:45-47`

### Fix

`_watch_relaunch_allowed()` now evaluates the 15:00 IST relaunch cutoff in `Asia/Kolkata`,
including when passed an aware timestamp from another timezone. Regression coverage:
`tests/test_paper_supervisor.py::test_supervisor_watch_relaunch_cutoff_uses_ist_for_aware_time`.

---

## 2026-05-03 — FIXED: ALERT: FLATTEN_EOD and SESSION_STARTED dedup guards are not thread-safe

**Status:** FIXED
**Severity:** Medium

### Symptom

Rare duplicate FLATTEN_EOD or SESSION_STARTED alerts can be dispatched when two coroutines reach the dedup check simultaneously before either updates the in-memory set.

### Root Cause

`engine/paper_runtime.py:414-428` and `733-756` — both use a pattern of `if session_id in _set: return` followed by `_set.add(session_id)` then `_dispatch_alert(...)`. In async code running in a single-threaded event loop these are non-atomic: `await _dispatch_alert(...)` yields control between the check and the add, allowing a second caller to pass the guard.

### Impact

Occasional duplicate Telegram alerts for session start and EOD summary. Not harmful but noisy and confusing.

### Location

`engine/paper_runtime.py:414-428`, `733-756`

### Fix

FLATTEN_EOD already adds to `_flatten_eod_sent` before dispatch. `SESSION_STARTED` now also
adds the session id before dispatch, closing the re-entrant duplicate window.

---

## 2026-05-03 — FIXED: ALERT: close alert formatting crashes on zero entry_price or invalid date string

**Status:** FIXED
**Severity:** Medium

### Symptom

Two separate crash paths in alert body formatting:
1. Division by zero when `entry_price == 0.0` in `pnl_pct` calculation.
2. Unhandled `ValueError` when parsing a malformed 10-character `trade_date` string.

### Root Cause

`engine/paper_alerts.py:78-82`:
```python
pnl_pct = (
    ((close_price - entry_price) / entry_price * 100)  # ZeroDivisionError if entry_price==0
    ...
)
```

`engine/paper_alerts.py:123-125`:
```python
if date_str and len(date_str) == 10:
    date_str = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%b-%Y")
    # no exception handler — ValueError if date_str is 10 chars but not a valid date
```

### Impact

Any position with a corrupt or zero entry price causes the close alert to fail entirely, suppressing the Telegram/email notification.

### Location

`engine/paper_alerts.py:78-82`, `123-125`

### Fix

Close-alert formatting now reports `0.00%` when entry price is zero. Risk/EOD summary
formatting now ignores invalid trade-date strings instead of raising. Regression coverage:
`tests/test_paper_runtime.py::test_format_close_alert_handles_zero_entry_and_invalid_event_time`
and `tests/test_paper_runtime.py::test_format_risk_alert_ignores_invalid_trade_date`.

---

## 2026-05-03 — OPEN: STRATEGY: CPR entry boundary allows equality (close == trigger) when exclusive crossing intended

**Status:** OPEN
**Severity:** Medium

### Symptom

A candle whose close price exactly equals the CPR trigger (TC with buffer for LONG, BC minus buffer for SHORT) is accepted as an entry, even though the intended rule is "close **beyond** the trigger."

### Root Cause

`engine/cpr_atr_shared.py:239-261`:
```python
# LONG
if current_close < trigger:   # reject if below; accept if at or above
    return None
# SHORT
if current_close > trigger:   # reject if above; accept if at or below
    return None
```
Exact equality is not rejected. For the LONG case, `close == trigger` means the candle closed exactly at the TC+buffer level, which is ambiguous — it could be a rejection wick rather than a confirmed breakout.

### Impact

Marginal entries at the exact trigger boundary may have lower follow-through probability. Impact is small in practice since exact equality at two decimal places is rare, but it is a parity divergence from a strict "close beyond" rule.

### Location

`engine/cpr_atr_shared.py:239`, `258`

---

## 2026-05-03 — FIXED: KITE: tick timestamp source silently discarded, receive-time fallbacks mixed with exchange time

**Status:** FIXED
**Severity:** Medium

### Symptom

Bar timing accuracy cannot be audited after the fact. Bars built under high-latency or reconnect conditions may have receive-time timestamps silently substituted for exchange timestamps without any indicator in stored data.

### Root Cause

`engine/kite_ticker_adapter.py:439-447` — when a tick's `exchange_timestamp` is unavailable, the adapter falls back to `timestamp` (SDK time) or receive time (`datetime.now(IST)`). All three cases produce a `source="websocket"` label with no distinction. The `fallback_count` metric is accumulated but never stored with the bar or surfaced in session diagnostics.

### Impact

Parity comparisons and latency audits cannot distinguish bars built from exchange time vs. estimated receive time. On reconnect bursts where many fallback ticks arrive simultaneously, bar open/close attribution can shift by 200ms–2s.

### Location

`engine/kite_ticker_adapter.py:439-447`

### Fix

`engine/kite_ticker_adapter.py` now tracks cumulative and last-batch timestamp-source counts
for `exchange_timestamp`, SDK `timestamp`, `last_trade_time`, and receive-time fallbacks.
`scripts/paper_live_helpers.py` includes exchange/fallback/receive-time counts in
`TICKER_HEALTH` logs.

---

## 2026-05-03 — FIXED: DB: cleanup_feed_audit uses created_at instead of trade/feed date for retention

**Status:** FIXED
**Severity:** Medium

### Symptom

Feed audit record cleanup based on `created_at` can delete freshly ingested historical records that have a recent `created_at` but old feed dates, or retain old audit records that were created recently via a late backfill.

### Root Cause

`db/paper_db.py:1299-1315`:
```python
DELETE FROM paper_feed_audit WHERE created_at < ?
```
The cutoff should be based on the feed/trade date (`bar_time` or `trade_date`), not the DB insertion timestamp. Late-inserted historical bars (e.g., from a replay) share a recent `created_at` and survive the cleanup unnecessarily.

### Impact

`paper_feed_audit` table grows unbounded for replay-heavy usage. Targeted purge by trade date is not possible with this schema approach.

### Location

`db/paper_db.py:1299-1315`

### Fix

Feed-audit cleanup now evaluates retention against feed/bar timestamps first
(`bar_end`, `last_snapshot_ts`, `first_snapshot_ts`, `trade_date`) and only falls back to
`created_at` when no feed timestamp exists. Regression coverage:
`tests/test_paper_db.py::test_cleanup_feed_audit_uses_bar_date_not_insert_date`.

---

## 2026-05-03 — OPEN: ALERT: email channel never configured or tested; both channels are parallel not sequential

**Status:** OPEN
**Severity:** Low

### Symptom

Email alerts have never been delivered. Despite the comment "Email (backup)", email is not a fallback triggered only when Telegram fails — both channels are attempted **simultaneously on every retry attempt**. However since the three required Doppler secrets have never been set, `email.enabled` is always `False` and email is completely inactive.

### Root Cause

`engine/notifiers/email.py:36-37` — `email.enabled` is `True` only when all three Doppler secrets are present: `SMTP_USER` (sender address + SMTP login), `SMTP_PASSWORD`, and `ALERT_TO_EMAIL` (recipient address). None of these have been configured.

`engine/alert_dispatcher.py:262-271` — the `_send_with_retry` loop tries Telegram and email independently on every attempt:
```python
if self.telegram.enabled and not telegram_ok:
    await self.telegram.send(...)    # attempted every iteration until success
if self.email.enabled and not email_ok:
    await self.email.send(...)       # also attempted every iteration — parallel, NOT fallback
```
If both are enabled, every alert goes to both channels. The "backup" label in the old comment was misleading.

### Impact

No email alerts are ever delivered. If Telegram is down, there is no secondary alert channel. The email path is untested end-to-end.

### To Activate

Set Doppler secrets `SMTP_USER`, `SMTP_PASSWORD`, and `ALERT_TO_EMAIL`. Use a Gmail app password. The SMTP defaults are `smtp.gmail.com:587` (STARTTLS).

### Location

`engine/notifiers/email.py:36-37`, `engine/alert_dispatcher.py:262-271`

---

## 2026-05-03 — FIXED: LOCAL-FEED/REPLAY: missing bars silently skipped; diverges from Kite live flat-candle behavior

**Status:** FIXED
**Severity:** Medium

### Symptom

In Kite live trading, an illiquid symbol that has no new trade ticks for a 5-minute slot still emits a candle with OHLC = last known close and volume = 0 (a "flat candle"). In local-feed (`daily-live --feed-source local`) and paper replay, the same symbol simply **does not appear** in that bar's candle list. The bar is silently skipped entirely for that symbol.

This means open positions on illiquid symbols are not evaluated for trailing stop, time-exit, or risk checks during quiet bars in local-feed and replay, while live would evaluate them with the flat candle.

### Root Cause

All three non-live modes skip consistently:

- **Local-feed** (`engine/local_ticker_adapter.py:263-265`): iterates the global union of bar timestamps and emits only symbols that have data at that slot. Missing → `continue`.
- **Paper replay** (`engine/local_ticker_adapter.py:263-265`): same code path — preloaded packs carry only recorded candles, no fill for quiet bars.
- **Backtest** (`engine/cpr_atr_strategy.py`): vectorized engine also skips missing candle indices.

The root cause is that `intraday_day_pack` only stores bars with actual recorded data. There is no fill step that injects flat candles for symbols that traded in a prior bar but were quiet in the current one.

### Impact

**Trailing stop evaluation** — a position open on a quiet symbol is not evaluated for `TRAIL_STOP`, `TIME_EXIT`, or `DAILY_LOSS_LIMIT` during the silent bars. In live, the trailing stop would advance (or hold) using the flat-candle close. This difference is small in practice (quiet symbol → no meaningful price move) but it is a correctness gap.

**Time exit** — if all bars for a position's symbol are quiet from 14:00–15:15, the TIME_EXIT at 15:15 is not triggered in replay/local-feed. In live, the flat-candle at 15:15 triggers it.

**Parity** — local-feed is used for "closest-to-live" simulation. Any divergence from live behavior undermines its validation purpose.

### Fix Direction

In `local_ticker_adapter.py`, after building the candle list for a bar, inject flat candles for any symbol that has an open position in the session tracker but was not present in the current bar. Use `last known close = self._last_ltp[symbol]` as the flat OHLC with volume=0. This matches Kite live behavior and ensures all three evaluation paths (stop, time-exit, risk) are run for open positions every bar.

Backtest already doesn't need this fix — its vectorized engine handles missing bars by design and results are the reference. The fix is specifically for `local_ticker_adapter.py` (covers both local-feed and replay paths since they share the same adapter).

### Location

`engine/local_ticker_adapter.py:250-289`, `engine/bar_orchestrator.py` (where flat-candle injection should occur after `get_closed_bars()`)

### Fix

`LocalTickerAdapter.drain_closed()` now emits a flat carry-forward candle with volume `0`
when a session symbol is missing for the current bar but has a prior last traded price. This
matches the live quiet-symbol behavior used by the WebSocket path. Regression coverage:
`tests/test_local_ticker.py::TestLocalTickerAdapterPartialData::test_symbols_with_different_bar_counts`.

---

## 2026-05-03 — FIXED: LIVE: flatten_session_positions uses original qty instead of remaining qty for partial positions

**Status:** FIXED
**Severity:** High

### Symptom

After a partial exit (scale-out at first target), the remaining runner is force-flattened via `flatten-all`, admin command, or EOD flatten. The realized P&L is computed for the **original full quantity** instead of the **remaining runner quantity**, producing an inflated or wrong-sign P&L. The order event also records the wrong fill quantity.

### Root Cause

`engine/paper_runtime.py:627` — `_realized_pnl_for_close(position, close_price, params=params)` is called without passing `qty`. The function defaults to `position.quantity` (the original full qty from entry). After a partial exit, `position.current_qty` holds the reduced runner quantity but this field is never passed.

Similarly, lines 648 and 652 record `float(position.quantity)` in the order event instead of `float(position.current_qty or position.quantity)`.

`flatten_positions_subset` (line 806+) has the same issue.

### Impact

- P&L overstated by the ratio `original_qty / runner_qty` for every force-flattened runner
- Order events record wrong fill quantities
- Dashboard and EOD summary show incorrect session totals

### Fix Direction

Pass `qty=float(position.current_qty or position.quantity)` to `_realized_pnl_for_close`, and use `current_qty` in order event `requested_qty`/`fill_qty` fields.

### Location

`engine/paper_runtime.py:627-652`, `engine/paper_runtime.py:806-830`

### Fix

`flatten_session_positions()` and `flatten_positions_subset()` now close
`position.current_qty` when present, and write the same remaining quantity to order-event
`requested_qty` / `fill_qty`. Regression coverage:
`tests/test_paper_runtime.py::test_flatten_session_positions_uses_remaining_qty_after_partial_exit`.

---

## 2026-05-03 — FIXED: SIM: paper_session_driver uses credit_cash instead of record_partial for PARTIAL exits

**Status:** FIXED
**Severity:** High

### Symptom

In `daily-sim` mode, after a partial exit (scale-out), the `SessionPositionTracker`'s `current_qty` is never updated. Subsequent entries use inflated equity because the partially-exited position's full original notional is still counted.

### Root Cause

`engine/paper_session_driver.py:205` — for `PARTIAL` action, calls `tracker.credit_cash(exit_value)` which adds cash but does NOT update `TrackedPosition.current_qty`. The canonical path in `paper_runtime.py:1609` correctly calls `tracker.record_partial(symbol, exit_value, remaining_qty)`.

### Impact

- `current_open_notional()` returns stale value (original qty, not reduced qty)
- `current_equity()` returns stale value
- Incorrect compound-equity sizing for subsequent entries when `--compound-equity` is used
- Does not affect `slots_available()` (counts positions, not qty)

### Fix Direction

Replace `tracker.credit_cash(...)` with `tracker.record_partial(candle.symbol, exit_value, remaining_qty)` at line 205.

### Location

`engine/paper_session_driver.py:204-205`

### Fix

`engine.paper_session_driver.process_closed_bar_group()` now calls
`tracker.record_partial(symbol, exit_value, remaining_qty)` for PARTIAL exits. Regression
coverage:
`tests/test_paper_session_driver.py::test_process_closed_bar_group_updates_tracker_on_partial_exit`.

---

## 2026-05-03 — FIXED: ALERT: log_alert passes invalid status="skipped_no_loop" violating CHECK constraint

**Status:** FIXED
**Severity:** High

### Symptom

In `daily-sim` or any paper mode where `asyncio.get_running_loop()` raises `RuntimeError` (no event loop running), every alert dispatch attempt throws a `ConstraintException` at the DB level. The outer try/except catches it silently, so the alert is lost and the caller gets no useful feedback.

### Root Cause

`engine/paper_runtime.py:275` — calls `_db().log_alert(..., status="skipped_no_loop")`. The `alert_log` table has `CHECK (status IN ('sent','failed','queued'))` at `db/paper_db.py:453`. The value `"skipped_no_loop"` is not in the allowed set.

### Impact

- Every alert in sim mode silently fails with a DB constraint violation
- Alert log has no record of the attempt
- Masks whatever the original alert was trying to communicate

### Fix Direction

Change `status="skipped_no_loop"` to `status="failed"` with `error_msg="no_event_loop"`.

### Location

`engine/paper_runtime.py:275`, `db/paper_db.py:453`

### Fix

No-loop alert fallback now writes `channel="LOG"`, `status="failed"`, and
`error_msg="no_event_loop"`, which satisfies the alert-log CHECK constraint. Regression
coverage:
`tests/test_paper_runtime.py::test_dispatch_alert_without_running_loop_logs_valid_failed_status`.

---

## 2026-05-03 — FIXED: LIVE: global flatten signal file never deleted after detection

**Status:** FIXED
**Severity:** High

### Symptom

After triggering a global flatten via `.tmp_logs/flatten_all.signal`, all subsequent `run_live_session` invocations immediately flatten and exit. This includes watch-mode restarts, resume attempts, and concurrent sibling sessions in `--multi`.

### Root Cause

`scripts/paper_live.py:868-899` — detects the signal file and flattens all positions, but never deletes the file afterward. The per-session sentinel file at line 1237 correctly calls `_signal_file.unlink()`, but the global signal has no equivalent cleanup.

### Impact

- Supervisor's watch-mode retry loop is useless after a single global flatten
- All restart attempts exit within one cycle
- Operator must remember to manually delete `.tmp_logs/flatten_all.signal`

### Fix Direction

Add `Path(_GLOBAL_FLATTEN_SIGNAL).unlink(missing_ok=True)` before the `break` at line 899.

### Location

`scripts/paper_live.py:868-899`

### Fix

After a global flatten signal is handled, `run_live_session()` now best-effort deletes
`.tmp_logs/flatten_all.signal` so future watch-mode restarts and resumes do not immediately
flatten again.

---

## 2026-05-03 — FIXED: ALERT: EmailNotifier.send swallows all exceptions — retry logic bypassed

**Status:** FIXED
**Severity:** High

### Symptom

When email delivery fails (wrong credentials, SMTP server down, network partition), `_send_with_retry` never sees the exception. The dispatcher records `email_ok=True` in the alert log even though the email never reached the recipient.

### Root Cause

`engine/notifiers/email.py:50-66` — the `send` method catches **all** exceptions and only logs them, never re-raising. The retry loop in `alert_dispatcher.py:275` expects exceptions to trigger retries, but `send()` always returns normally.

### Impact

- Email retry logic (30s + 120s) is completely bypassed
- Alert log shows email as "sent" on failure
- Not currently triggered (email secrets not configured), but will manifest when activated

### Fix Direction

Remove the blanket try/except from `send()`. Let network exceptions propagate to `_send_with_retry` which is designed to handle them. Alternatively, catch only non-transient exceptions (e.g., `SMTPAuthenticationError`) and re-raise network errors.

### Location

`engine/notifiers/email.py:50-66`, `engine/alert_dispatcher.py:262-275`

### Fix

`EmailNotifier.send()` now logs and re-raises send failures so `AlertDispatcher._send_with_retry()`
can retry and record accurate alert status. Regression coverage:
`tests/test_alert_dispatcher.py::test_email_notifier_reraises_send_failure`.

---

## 2026-05-03 — FIXED: ARCHIVE: uses position.qty (INT) instead of position.quantity (float)

**Status:** FIXED
**Severity:** High

### Symptom

Archived paper session rows have slightly wrong `position_size`, `position_value`, and `pnl_pct` values for positions where risk-based sizing produced a non-integer quantity.

### Root Cause

`scripts/paper_archive.py:20` — `quantity = float(position.qty or 0.0)` reads `PaperPosition.qty` which is the `INT` DB column (`qty INT DEFAULT 0`). The engine's `PaperPosition.quantity` field carries the precise `float`. `paper_db.py:963` stores `qty=int(quantity)` (truncated) while `quantity=float(quantity)` preserves the float. The archive reads the truncated INT field.

### Impact

- `position_size`, `position_value`, `pnl_pct` in archived `backtest_results` use truncated integer
- Dashboard analytics and run comparisons are slightly inaccurate for risk-sized positions
- The `realized_pnl` column is unaffected (uses separate calculation path)

### Fix Direction

Change line 20 to `quantity = float(position.quantity or position.qty or 0.0)`.

### Location

`scripts/paper_archive.py:20`

### Fix

Archive conversion now prefers `position.quantity` over the truncated `qty` field when
calculating `position_value`, P&L, and P&L percent. Note: `backtest_results.position_size`
is still an INTEGER schema field, and production NSE share quantities are integer; changing
that analytics schema is separate cleanup, not a Monday live blocker. Regression coverage:
`tests/test_paper_archive.py::test_position_to_trade_row_uses_float_quantity_and_maps_momentum_fail`.

---

## 2026-05-03 — OPEN: STRATEGY: SHORT trailing stop skips SL tightening on BREAKEVEN-to-TRAIL transition

**Status:** OPEN
**Severity:** Medium

### Symptom

When a SHORT position's price reaches the target (S1), the trailing stop phase transitions from BREAKEVEN to TRAIL but the SL is NOT tightened on that bar. For LONG, the SL is tightened immediately on the transition bar. The SHORT SL is one bar late.

### Root Cause

`engine/cpr_atr_utils.py:118-123` (LONG) tightens the SL on BREAKEVEN→TRAIL transition:
```python
self.current_sl = max(self.current_sl, self.highest_since_entry - (self.atr * self.trail_atr_multiplier))
self.phase = "TRAIL"
```

`engine/cpr_atr_utils.py:144-146` (SHORT) only changes the phase:
```python
self.phase = "TRAIL"
```

The comment says "Same deferred-SL logic as LONG" but the SL update is missing entirely.

### Impact

If price reverses sharply on the target-reaching bar, the SHORT position is not protected by the trailing stop for one bar. The SL remains at breakeven (entry price) instead of tightening to `lowest_since_entry + ATR`.

### Fix Direction

Add SL tightening to the SHORT transition, mirroring LONG:
```python
self.current_sl = min(self.current_sl, self.lowest_since_entry + (self.atr * self.trail_atr_multiplier))
self.phase = "TRAIL"
```

### Location

`engine/cpr_atr_utils.py:144-146`

---

## 2026-05-03 — FIXED: ARCHIVE: exit_reason normalization incomplete for MOMENTUM_FAIL

**Status:** FIXED
**Severity:** Medium

### Symptom

A paper session where a position exits with `exit_reason="MOMENTUM_FAIL"` may crash during archiving with a CHECK constraint violation on `backtest_results`.

### Root Cause

`scripts/paper_archive.py:41-51` — the `exit_reason_map` covers known paper-specific values but `MOMENTUM_FAIL` (used in `paper_runtime.py:1140`) is not mapped to a backtest-compatible value. The DuckDB backtest results path may have a narrower CHECK constraint than `backtest_db.py`'s allow-list, causing the insert to fail.

### Impact

Archive crash for sessions with momentum-failure exits. The session remains unarchived and the operator must manually fix or retry.

### Fix Direction

Preserve `MOMENTUM_FAIL` in the archive when the active `backtest_results` schema supports
it, so paper-vs-backtest comparisons keep the same exit reason semantics. Older fallback
constraints should be aligned instead of silently downgrading the reason.

### Location

`scripts/paper_archive.py:41-51`, `paper_runtime.py:1140`

### Fix

Paper archive normalization preserves `MOMENTUM_FAIL`. The 2026-04-30 local-live parity
validation showed matching trade sets and prices, and this fix removes the remaining
paper-archive `CANDLE_EXIT` vs backtest `MOMENTUM_FAIL` semantic diff. Regression coverage:
`tests/test_paper_archive.py::test_position_to_trade_row_uses_float_quantity_and_preserves_momentum_fail`.

---

## 2026-05-03 — FIXED: LIVE: EOD summary suppressed on admin and sentinel flatten paths

**Status:** FIXED
**Severity:** Medium

### Symptom

Operators do not receive FLATTEN_EOD Telegram alerts when using admin `close_all` command or per-session sentinel files to terminate a session with open positions. The CLAUDE.md documentation states this alert should fire even when all positions are already closed.

### Root Cause

`scripts/paper_live.py:883,1250,1313` — the three mid-loop flatten calls (global signal, per-session sentinel, admin `close_all`) all pass `emit_summary=False`. Only the finally-block flatten at line 1736 defaults to `emit_summary=True`, but that path may not reach the FLATTEN_EOD dispatch if the session status was changed by `complete_session`.

### Impact

Operators relying on EOD summary alerts for admin-initiated flattens miss the session's final P&L notification.

### Fix Direction

Pass `emit_summary=True` on the admin `close_all`, sentinel flatten, and global flatten paths.

### Location

`scripts/paper_live.py:883,1250,1313`

### Fix

The mid-loop global signal, per-session sentinel, and admin `close_all` flatten paths now use
the default `emit_summary=True`. Existing in-memory/persisted FLATTEN_EOD dedupe prevents
duplicate summaries if final cleanup calls `flatten_session_positions()` again.

---

## 2026-05-03 — FIXED: LIVE: resume does not reconstruct original feed_source — always defaults to WebSocket

**Status:** FIXED
**Severity:** Medium

### Symptom

Resuming a session that was originally started with `--feed-source local` always creates a fresh `KiteTickerAdapter` (WebSocket), ignoring the original feed source.

### Root Cause

`scripts/paper_trading.py:671-679` — `_cmd_daily_live_resume` passes `poll_interval_sec`, `candle_interval_minutes`, etc. but does not construct a `ticker_adapter` or pass `feed_source`. The default in `run_live_session` creates a `KiteTickerAdapter` when no adapter is provided.

### Impact

Cannot resume local-feed sessions after stale exit. The resumed session tries to connect to Kite WebSocket instead of reading from historical DuckDB data.

### Fix Direction

Read `feed_source` from the session's `strategy_params` (stored in the DB) and reconstruct the appropriate adapter before calling `run_live_session`.

### Location

`scripts/paper_trading.py:671-679`

### Fix

`_cmd_daily_live_resume()` now reads the stored session `strategy_params["feed_source"]` and
reconstructs `LocalTickerAdapter` for local-feed sessions before calling `run_live_session()`.
Regression coverage:
`tests/test_paper_trading_workflow.py::test_cmd_daily_live_resume_reconstructs_local_feed_adapter`.

---

## 2026-05-03 — FIXED: LIVE: final flush uses stale session object loaded at startup

**Status:** FIXED
**Severity:** Medium

### Symptom

If an admin command changed session params mid-run (e.g., `set_risk_budget`), the final flush in the finally block evaluates remaining candles against the original params, not the current ones.

### Root Cause

`scripts/paper_live.py:1690` — the finally block passes `session` (loaded at line 652) to `process_closed_bar_group`. The main loop uses `current_session` (reloaded every cycle at line 863) for the same purpose. The stale `session` object may have outdated `strategy_params`.

### Impact

Final-bar evaluations use wrong risk parameters if an admin budget change occurred during the session. In practice this is a narrow window (session must have remaining bars when the finally block runs).

### Fix Direction

Use `current_session` instead of `session` in the final flush, or reload the session from DB before flushing.

### Location

`scripts/paper_live.py:1690`

### Fix

The final flush now reloads the latest session before draining remaining candles and rebuilds
the flush-time `BacktestParams` from that fresh session object.

---

## 2026-05-03 — FIXED: LIVE: dead code _process_closed_bar_group diverges from canonical driver

**Status:** FIXED
**Severity:** Low

### Symptom

`scripts/paper_live.py:478-584` defines a 107-line `_process_closed_bar_group` function that is never called in production. The actual call goes to `paper_session_driver.process_closed_bar_group`. The dead copy is missing several features: duplicate candle dedup, 64-symbol yield-to-event-loop, risk control checks, stage-B direction filtering, and `should_complete` detection.

### Root Cause

The driver was extracted to `engine/paper_session_driver.py` but the old function was not deleted. Tests that patch `paper_live._process_closed_bar_group` may be testing dead code.

### Impact

Maintenance hazard — a developer reading top-to-bottom edits the dead copy thinking it's the active path, introducing subtle regressions.

### Fix Direction

Delete lines 478-584. Update any test mocks that reference it to patch `engine.paper_session_driver.process_closed_bar_group` instead.

### Location

`scripts/paper_live.py:478-584`

### Fix

Deleted the unused `_process_closed_bar_group()` copy from `scripts/paper_live.py`; live
processing now has a single implementation in `engine.paper_session_driver`.

---

## 2026-05-03 — WON'T FIX: LIVE: _cmd_daily_live_multi does not pass real_order_config to run_live_session

**Status:** WON'T FIX for current pilot; keep as latent cleanup if the pilot block is removed
**Severity:** Low

### Symptom

Multi-live sessions cannot route real orders even when `--real-orders` is requested. The `_execute_variant` closure does not pass `real_order_config` to `run_live_session`, while the single-variant `_cmd_daily_live` does.

### Root Cause

`scripts/paper_trading.py:1258-1269` — the closure passes several kwargs but omits `real_order_config`. A pilot block at line 1126 prevents `--multi --real-orders` today, masking the bug.

### Impact

Latent — removing the pilot block without fixing the pass-through will cause multi-live sessions to silently run in paper mode even when `--real-orders` is requested.

### Fix Direction

Add `real_order_config=real_order_config` to the `_execute_variant` closure's `run_live_session` call.

### Location

`scripts/paper_trading.py:1258-1269`

### Triage

Not a current runtime bug: `_cmd_daily_live_multi()` exits immediately when `--real-orders`
is supplied, so no multi-live real-order session reaches `_execute_variant`. Keep this as a
cleanup note for the future commit that deliberately removes the pilot block.

---

## 2026-05-03 — FIXED: RECONCILE: missing EXIT_UNDERFILLED check for closed positions

**Status:** FIXED
**Severity:** Low

### Symptom

A closed position whose exit order fills total less than the position's original quantity passes reconciliation without any finding.

### Root Cause

`engine/paper_reconciliation.py:129-159` — checks for `ENTRY_UNDERFILLED` and `EXIT_OVERFILLED` but has no corresponding `EXIT_UNDERFILLED` check. A partial exit fill that underfills would go undetected.

### Impact

Real execution gaps (e.g., partial fill on illiquid stock at exit) are not flagged by the reconciliation layer.

### Fix Direction

Add an exit underfill check after the `EXIT_OVERFILLED` check:
```python
if exit_fills and exit_qty + 1e-9 < expected_qty:
    findings.append({"type": "EXIT_UNDERFILLED", ...})
```

### Location

`engine/paper_reconciliation.py:146-159`

### Fix

Closed/flattened positions now emit a CRITICAL `EXIT_UNDERFILLED` finding when filled exit
quantity is below the expected position quantity. Regression coverage:
`tests/test_paper_reconciliation.py::test_reconcile_closed_position_underfilled_exit_order`.

## 2026-05-03 — FIXED: ALERTS: paper_alerts.py used ambiguous Python 2-style comma except syntax

**Status:** FIXED
**Severity:** Low

### Symptom

`engine/paper_alerts.py:148` uses `except ValueError, IndexError:` — Python 2 syntax where the second name was the binding variable for the exception, not a second exception type. In Python 3.14 this compiles to a tuple catch (correct behavior), but this is non-obvious, not PEP 8 compliant, and would have incorrect semantics in Python 2 where `IndexError` would be the variable holding the caught ValueError.

### Root Cause

Legacy Python 2 style code not updated during the Python 3 migration. Should be `except (ValueError, IndexError):`.

### Impact

No runtime breakage on Python 3.14, but a code clarity/maintenance hazard. Future readers may misinterpret the intent.

### Location

`engine/paper_alerts.py:148`
