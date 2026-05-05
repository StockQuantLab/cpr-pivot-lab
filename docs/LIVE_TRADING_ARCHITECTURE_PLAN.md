# Live Trading Architecture Plan

> Historical planning doc. The current operator source of truth is `docs/PAPER_TRADING_RUNBOOK.md`.
> This file is kept for design history and remaining backlog context; when it conflicts with the
> runbook, the runbook wins.

## Goals

1. **Full parity**: Backtest, paper-replay, and paper-live must produce identical
   trade entries, exits, and PnL for the same symbol/date/params. Any divergence
   is a bug. This is the primary correctness requirement.

2. **Full universe**: Run against all 2105 NSE symbols filtered down to qualifying
   ones (narrow CPR + min_price + cpr_min_close_atr). Do NOT restrict to NIFTY 50
   or any fixed list — that reduces edge by excluding symbols that meet the filter.
   Typical qualifying count: 150–400 per direction per day.

3. **Production reliability**: `daily-live` must survive a full trading day (09:15–15:15)
   without crashing, OOM, or lock contention.

---

## Apr 2026 Learnings

The implementation has reached the point where the remaining parity bugs are
mostly about orchestration and lifecycle, not the shared entry/exit math.

- Replay and live already share the same candle evaluation, execution, and risk
  control helpers.
- Replay is still bar-major and deterministic over `intraday_day_pack`.
- Live still owns its own session controller, stop conditions, and terminal
  completion path.
- The local historical feed is not a true tick-by-tick websocket replay. It is a
  closed-candle broadcaster built from `intraday_day_pack`, which is good enough
  for candle parity but does not exercise the same transport mechanics as Kite.
- The remaining drift we observed on `2026-04-09` came from lifecycle handling:
  early stop paths, session completion, and end-of-day flattening, not from the
  CPR/ATR signal math itself.

Practical rule:
- if replay and live disagree on trade count or open positions for the same day,
  treat it as a session-driver bug first.
- only after the driver is unified should transport differences be investigated.

---

## Context and Known Bugs (Fixed and Open)

### Fixed in Apr 2026

- `_load_live_setup_row` used `v_5min` Parquet glob (21,000 files) for ATR → OOM.
  Fixed: now uses `atr_intraday` indexed table.
- `asyncio.to_thread(load_setup_row)` spawned per-symbol threads all calling the
  same DuckDB connection → native crash. Fixed: synchronous call in event loop.
- `alert_log` cascade delete had no `session_id` FK → cleanup errors. Fixed.
- Paper fill price used `candle.close` instead of `max(trigger, candle_open)`. Fixed.
- Re-entry after SL was allowed. Fixed via `position_closed_today` flag.

### Fixed in Apr 2026 (later)

- RVOL display bug: `rvol_threshold` fallback `1.2` → `1.0` in 5 display files.
  Paper sessions now show correct `rvol1.0` instead of stale `rvol1.2`.
- Replay refactored from symbol-major to bar-major loop (`_process_replay_bar_major`).
  Fixes the `max_positions` over-open bug where paper had 24 trades vs backtest's 16.

### Fixed in Apr 2026 (parity round — 2026-04-10)

- TrailingStop state never accumulated across candles: `SessionPositionTracker.get_open_position()`
  cached position at open time; `trail_state` never updated in-memory after DB writes.
  Every candle reconstructed TrailingStop from initial PROTECT phase → all exits were INITIAL_SL.
  Fix: `update_trail_state()` on tracker, called from `evaluate_candle()` after HOLD/PARTIAL.
- Entry window boundary off-by-one: `should_process_symbol()` used `>=` vs `entry_window_end`,
  excluding the 10:15 bar. Backtest's `range_indices` includes it. Fix: changed to `>`.
- SHORT exit_value over-credits cash: Paper used `qty × close_price` as exit_value; correct
  formula is `qty × (2×entry - exit)` (= position_value + gross_pnl). Old formula over-credited
  on SHORT losses by `2×loss`, cascading to wrong sizes on subsequent trades.
  Fix: `_exit_value_for_position()` with direction-aware formula.
- Verified: single-date backtest (`--preset CPR_LEVELS_RISK_SHORT --start X --end X`)
  matches paper replay exactly (all trades, quantities, exit reasons) on Apr 1, 2, 6.

### Open Issues (this plan)

- Phase 5 orchestrator items are now implemented (2026-04-09):
  - In-memory `SessionPositionTracker` replaced per-symbol per-bar DB open-position reads.
  - Replay and live now share `should_process_symbol()` + `select_entries_for_bar()`.
  - Replay bar driver uses canonical `time_str` union (not array index alignment).
  - Entry evaluation was split from execution (`evaluate_candle()` vs `execute_entry()`).
  - Entry scan now evaluates the current bar only (no stale-signal resurrection).
  - Risk controls are checked per bar in replay and live via a shared wrapper.
- Remaining gap after the Apr 2026 parity checks:
  - The outer replay/live/session controller is still split.
  - `daily-live` can exit on transport/session conditions before the end-of-day
    flatten path is guaranteed to run.
  - The local historical feed path is a websocket-shaped adapter, not a true
    tick-stream simulation, so it does not fully validate Kite arrival behavior.
  - The current mismatch on `2026-04-09` is therefore an orchestration bug until
    proven otherwise.
- Active parity investigation:
  - No known unresolved CPR replay-vs-backtest trade-count drift from the earlier
    2026-04-02 short-session mismatch after strict setup-row parity + trailing-stop fix.
- Live cumulative volume: `FiveMinuteCandleBuilder` stores cumulative `volume_traded`
  as bar volume instead of per-bar delta. Live RVOL check uses wrong volume on later
  bars. Fixed in Phase 2 (volume delta computation in `FiveMinuteCandleBuilder`)
- Quiet symbol candle closure: With WebSocket, a symbol with no ticks in a 5-min bucket
  never gets its bar closed by `FiveMinuteCandleBuilder`. Fixed in Phase 2 via
  `synthesize_quiet_symbols()` at bar boundary. (Phase 2)
- Setup row caching risk: `load_setup_row()` fast path may cache a partial/zeroed row
  at prefetch time, permanently rejecting the symbol even if candles would later
  qualify it. (Phase 1/5H)
- No alert on API failure: `paper_live.py` still converts repeated poll failures into
  a stale termination path but does not dispatch an explicit operator alert event yet.
  Keep this as an operational hardening item.

---

## Replay vs Live: Identical Except Data Source

`daily-replay` and `daily-live` are the **same engine** (`engine/paper_runtime.py`)
with one difference: how candle data arrives.

```
daily-replay                         daily-live
────────────────────────────────     ────────────────────────────────
Data:  intraday_day_pack (DuckDB)    Data:  Kite LTP poll → FiveMinuteCandleBuilder
       pre-built historical arrays          (Phase 2: KiteTicker WebSocket ticks)

Speed: faster than real-time         Speed: real-time (1 bar per 5 min)
Bars:  all bars for the date         Bars:  bars arrive as market ticks
Setup: market_day_state (pre-built)  Setup: market_day_state (must run daily-prepare)

Both use:
  ✓ engine/paper_runtime.py — process_closed_candle(), position management, alert dispatch
  ✓ engine/cpr_atr_shared.py — check_entry_setup(), scan_cpr_levels_entry()
  ✓ Same BacktestParams / StrategyConfig
  ✓ Same fill price: max(trigger, candle_open) for LONG, min(trigger, candle_open) for SHORT
  ✓ Same SL/target/trail logic
  ✓ Same one-entry-per-symbol-per-day rule (position_closed_today)
  ✓ Same entry window cutoff (10:15 IST default)
  ✓ Same early completion: if no open positions at entry window close → COMPLETED
```

**If replay and live produce different trades for the same symbol/date/params,
it is a bug.** The only valid source of divergence is the candle OHLCV itself —
live OHLCV is approximate (built from LTP snapshots), while replay uses the exact
`intraday_day_pack` arrays. Phase 2 (WebSocket) eliminates this gap by capturing
every tick, making live OHLCV accurate.

### Corrected Interpretation

The statement above is only true once the outer session driver is unified.
Today, replay and live still differ in:

- session stop conditions
- end-of-day flatten enforcement
- terminal completion/archival
- local historical feed implementation details

So the right parity target is:
- identical candle sequence
- identical per-candle decisions
- identical lifecycle completion rules
- transport-specific code only in the data adapter layer

If the candle decisions match but the session state does not, the bug is in the
driver, not the strategy.

---

## Parity Architecture: Single Shared Entry Evaluator

All three modes (backtest, replay, live) must call the identical entry evaluation
code path:

```
engine/cpr_atr_shared.py
  └─ check_entry_setup(symbol, candle, setup_row, params) → EntrySignal | None

Used by:
  engine/cpr_atr_strategy.py      (backtest)
  engine/paper_runtime.py         (replay + live)
```

**Rule**: If `check_entry_setup()` returns a signal in backtest, it MUST return
the same signal in replay and live given identical candle data and setup row.

**Current parity gap**: Replay scans the full 09:20–10:15 window per symbol
on each bar; backtest uses first-signal-only with different ordering → diverge
on the same date. Fix: both must call `check_entry_setup()` with an explicit
early-exit after first signal.

**Setup row parity**: All three modes must derive CPR levels, ATR, and threshold
from the same source values:
- `cpr_daily.tc`, `cpr_daily.bc`, `cpr_daily.pivot`, `cpr_daily.r1`, `cpr_daily.s1`
- `atr_intraday.atr` (previous day)
- `cpr_thresholds.cpr_threshold_pct` (previous day rolling P50)

Live cannot currently read `cpr_daily` for today's date if `daily-prepare` hasn't
run — it falls back to computing CPR from `v_daily` raw Parquet. After `daily-prepare`
runs (required pre-market step), `market_day_state` has today's row and `load_setup_row()`
takes the fast `market_day_state` path. **Require `daily-prepare` to have run before
`daily-live` starts.** Fail fast (exit 1) if `cpr_daily` has 0 rows for today's date —
any partial coverage silently skews the symbol universe and breaks parity.

---

## Phase 0 — Symbol Pre-Filter at Session Start (Priority: HIGH, ~1 day)

### Problem

`daily-live --all-symbols` starts sessions for 2105 symbols. All are polled every
cycle. The CPR entry filter only runs inside `process_closed_candle` — symbols that
will never qualify (not narrow, wrong price) are polled until 15:15.

### Solution

**Two-stage filter, called before the poll loop starts:**

**Stage A (at session start, before 09:15):**
```sql
-- Read from cpr_daily (NOT v_daily — avoid Parquet scan)
-- Note: column is prev_close, not close
SELECT c.symbol, c.tc, c.bc, c.pivot, c.r1, c.s1, c.r2, c.s2,
       c.cpr_width_pct, c.prev_close
FROM cpr_daily c
JOIN cpr_thresholds t ON t.symbol = c.symbol AND t.trade_date = c.trade_date
WHERE c.trade_date = ?::DATE
  AND c.symbol IN (SELECT UNNEST(?::VARCHAR[]))
  -- Match existing fallback logic in paper_prepare.py:422:
  -- cpr_threshold_pct may be NULL for first-year symbols (warm-up period)
  -- fallback to cpr_max_width_pct = 2.0% as in current code
  AND c.cpr_width_pct < COALESCE(t.cpr_threshold_pct, 2.0)
  AND c.prev_close >= ?                         -- min_price filter
-- NOTE: cpr_min_close_atr is an entry-time gate in check_entry_setup(), not a
-- prefilter. Do NOT add it here — it would silently change the symbol universe
-- and break parity. Keep only the narrow-CPR and min_price filters at this stage.
```
Result: ~150–400 symbols (was 2105). Only these are passed to the session.

**Notes on implementation:**
- Must hold `_MARKET_DB_READ_LOCK` (reentrant RLock) around the execute call,
  same as `_load_live_setup_row()` in `paper_runtime.py:538`.
- **Hard prerequisite**: `pivot-refresh` and `daily-prepare` MUST have run this
  morning. `cpr_daily` for today's `trade_date` only exists after that. Fail fast
  (exit 1 with clear message) if `cpr_daily` has 0 rows for today's date.
- **Single source of truth for prefilter**: Extend `pre_filter_symbols_for_strategy()`
  in `scripts/paper_prepare.py:363` only. Do NOT add parallel symbol-filter logic
  in `paper_live.py` — two filter paths diverge and produce different universes.
  `paper_live.py` calls `paper_prepare.py`'s function; no filtering logic lives in
  the live script itself.
- Read via `get_dashboard_db()` (read-only replica connection), not `get_db()`.

**Stage B (after 09:15 bar closes):**
Apply direction filter from 09:15 candle **close** (not open) against CPR:
```python
# After 09:15 bar has been fully assembled (candle_time == 09:20):
# Direction logic mirrors engine/cpr_atr_shared.py:104 and cpr_atr_utils.py:298
qualifying = [
    sym for sym in stage_a_symbols
    if (direction == "LONG"  and close_9_15[sym] > setup_rows[sym]["tc"]) or
       (direction == "SHORT" and close_9_15[sym] < setup_rows[sym]["bc"])
]
```
- Uses **close** of the 09:15 bar, not open — matches the shared entry evaluator.
- Called after the 09:20 bar timestamp (i.e. first closed bar), not before 09:15.
- Separate in-memory filter; not part of the Stage A SQL.
- Stage A: run at session start (before 09:15). Stage B: run after 09:20 bar close.

**Stage C (after entry window closes, 10:15 IST default):**
Reduce active symbol set to only open positions. If no positions are open,
**complete the session immediately** — there is nothing to manage.

```python
if bar_ts.time() >= entry_window_end:
    open_positions = [p.symbol for p in session.positions if p.status == "OPEN"]
    if not open_positions:
        # No trades opened — session is done. Mark COMPLETED now, not at 15:15.
        mark_session_completed(session)
        send_summary_alert(session, reason="no_trades_entry_window_closed")
        return
    active_symbols = open_positions
    # With WebSocket (Phase 2): ticker.update_symbols(session_id, active_symbols)
```

**Entry window is 09:15–10:15 IST (canonical default).** The noon extension used
on 2026-04-09 was a one-time debugging override — do not make it the default.
The architecture plan uses 10:15 as the standard cutoff.

### Expected Impact

| Phase | Symbols polled | REST calls/bar (1s poll, 60 polls) |
|-------|---------------|-----------------------------------|
| Current (--all-symbols, no filter) | 2105 | oom |
| After Stage A (narrow+price filter) | ~200 | ~200 REST |
| After Stage B (direction filter) | ~100 | ~100 REST |
| After Stage C (post-entry window) | 0–10 | ~10 REST |

---

## Phase 1 — Batch Setup Row Prefetch (Priority: HIGH, ~0.5 days)

### Problem

At first bar close, `_load_live_setup_row` runs per-symbol: 3 DB queries × N symbols
sequentially. First-bar latency: ~30s for 48 symbols.

### Simpler Solution: Extend Existing Fast Path

`load_setup_row()` at `engine/paper_runtime.py:657` already has a `market_day_state`
fast path — a single indexed query per symbol. If `daily-prepare` populates
`market_day_state` for today (required pre-market step), the slow `_load_live_setup_row`
fallback is never reached.

**Primary fix (Phase 0 dependency):** Make `daily-prepare` mandatory. If
`market_day_state` has today's rows for all qualifying symbols, the existing
`load_setup_row()` already resolves them in O(1) per symbol. No new batch function needed.

**Secondary fix (when `market_day_state` missing):** Pre-call `load_setup_row()`
for all qualifying symbols at session start, before the poll loop:

```python
# In paper_live.py, before _run_live_loop():
for symbol in qualifying_symbols:
    state = runtime_states[symbol]
    if state.setup_row is None:
        state.setup_row = load_setup_row(symbol, trade_date, ...)
        # load_setup_row() takes market_day_state fast path if today's row exists
```

This serializes the 30s latency into session startup (acceptable) rather than
spiking at first bar close (unacceptable mid-trading).

**Notes:**
- All `db.con.execute()` calls inside `load_setup_row()` already hold `_MARKET_DB_READ_LOCK`.
- Calls are synchronous — never via `asyncio.to_thread`.
- No new SQL query to write — reuse `load_setup_row()` which already handles
  both the `market_day_state` fast path and the `_load_live_setup_row` fallback.

---

## Phase 2 — KiteTicker WebSocket Adapter (Priority: HIGH, 3–5 days)

### Problem

REST `kite.ltp()` polling misses intra-poll price spikes. With 200 qualifying
symbols, that's ~200 REST calls/bar. OHLCV is approximated, not exact. The adaptive
0.5s–5s poll interval adds unnecessary complexity and API load.

### Critical clarification: KiteTicker tick content

`KiteTicker.MODE_QUOTE` provides:
- `last_price`: current LTP (this is what we use for candle building)
- `ohlc`: **day OHLC since 09:15** — NOT the current 5-minute bar OHLCV
- `volume_traded`: cumulative day volume (NOT per-bar — must compute delta)
- `bid`, `ask`: top of book

**`tick["ohlc"]` is the day high/low, not the bar high/low.** It CANNOT be used
to build a 5-minute candle directly.

`MODE_QUOTE` was verified working during live market (2026-04-09): 9 ticks in
2.6s for 3 symbols. `exchange_timestamp` is empty in MODE_QUOTE — use
`datetime.now(IST)` as fallback (sub-second accuracy is sufficient since we
only act at 5-minute boundaries).

### Design Principles (from rubber-duck critique)

1. **No separate loop** — Do NOT create a new `_run_live_loop_ws()`. Instead,
   make the existing `_run_live_loop` transport-agnostic. WebSocket is just a
   different data source feeding the same supervision infrastructure.

2. **1-second supervision cadence** — Even though we only process candles every
   5 minutes, the main loop must check pause/stop/stale/risk/heartbeat every
   ~1 second. With REST this happened naturally (poll every ~1-5s). With
   WebSocket, ticks arrive passively — but the control plane still needs
   active supervision.

3. **Quiet symbol candle closure** — `FiveMinuteCandleBuilder` only closes a
   bar when a later snapshot arrives in a new bucket. With REST, constant polling
   ensures every symbol gets a snapshot. With WebSocket, a quiet symbol may emit
   no tick for an entire bucket, so the bar never "closes." At each bar boundary,
   synthesize a snapshot from the last-known LTP for symbols with no tick in the
   current bucket.

4. **Volume delta** — `tick["volume_traded"]` is cumulative day volume. Must
   compute per-bar delta (current_cumulative − previous_cumulative). This is a
   pre-existing bug with REST too — fix it in `FiveMinuteCandleBuilder` for both
   adapters in this phase.

### Architecture: Transport-Agnostic Live Loop

```
                ┌─────────────────────────────────────────────┐
                │          _run_live_loop (unchanged)          │
                │                                             │
                │  1s supervision: pause/stop/stale/risk/hb   │
                │  On bar boundary: drain + process candles   │
                └────────────────┬────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
             ┌──────▼──────┐          ┌───────▼───────┐
             │   REST mode  │          │  WebSocket    │
             │  (current)   │          │  mode (new)   │
             │              │          │               │
             │ poll() every │          │ _on_ticks()   │
             │ cycle → snap │          │ in bg thread  │
             │ → ingest     │          │ → ingest      │
             └──────┬──────┘          └───────┬───────┘
                    │                         │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  FiveMinuteCandleBuilder │
                    │  (shared, unchanged)     │
                    │  + volume delta fix      │
                    └─────────────────────────┘
```

The key insight: the live loop already runs at ~1s cadence for supervision. With
REST, each cycle polls and ingests. With WebSocket, ticks arrive passively — the
cycle just checks if any new closed candles appeared and handles supervision.

### Revised Loop Flow (WebSocket mode)

```python
# In _run_live_cycle (transport-agnostic):
if using_websocket:
    # Ticks already ingested by background thread into shared builder
    # At bar boundary: drain closed candles + synthesize for quiet symbols
    now = datetime.now(IST)
    if _is_bar_boundary(now, candle_interval_minutes):
        _synthesize_quiet_symbols(ticker_adapter, builder, active_symbols, now)
        closed_candles = builder.drain_closed_since(last_bar_ts)
        # process closed candles (same as REST path)
        ...
    else:
        # Supervision only: check pause/stop/stale/risk, write heartbeat
        ...
else:
    # REST path: poll → ingest → process (unchanged)
    snapshots = await asyncio.to_thread(market_adapter.poll, active_symbols)
    ...
```

### New File: `engine/kite_ticker_adapter.py`

```python
class KiteTickerAdapter:
    """WebSocket market data adapter.

    Background thread runs KiteTicker. Ticks are ingested into per-session
    FiveMinuteCandleBuilder instances under a lock. The main asyncio loop
    drains closed candles at bar boundaries.

    For --multi: LONG+SHORT share one KiteTickerAdapter (one WebSocket
    connection). Each session registers via register_session() and gets
    its own FiveMinuteCandleBuilder. Subscription is the union of all
    sessions' required symbols — ref-counted so unsubscribe only happens
    when NO session needs a symbol.
    """

    def __init__(self, api_key: str, access_token: str, exchange: str = "NSE"):
        self._api_key = api_key
        self._access_token = access_token
        self._exchange = exchange
        self._ticker: KiteTicker | None = None
        self._lock = threading.Lock()
        self._token_to_symbol: dict[int, str] = {}  # instrument_token → symbol
        self._symbol_to_token: dict[str, int] = {}   # symbol → instrument_token
        self._connected = threading.Event()
        self._last_ltp: dict[str, float] = {}        # symbol → last seen LTP
        self._tick_count = 0
        self._last_tick_ts: datetime | None = None
        self._reconnect_count = 0
        self._max_reconnect_alert = 5

        # Per-session state for --multi
        self._session_builders: dict[str, FiveMinuteCandleBuilder] = {}
        self._session_symbols: dict[str, set[str]] = {}  # session_id → wanted symbols

    def register_session(self, session_id: str, symbols: list[str],
                         builder: FiveMinuteCandleBuilder) -> None:
        """Register a session (e.g., CPR_LEVELS_LONG) with its symbol set."""
        with self._lock:
            self._session_builders[session_id] = builder
            self._session_symbols[session_id] = set(symbols)

    def connect(self, symbols: list[str]) -> None:
        """Resolve tokens, subscribe, start background thread.

        1. Download instrument list (cached for session)
        2. Map symbols → instrument_tokens
        3. Start KiteTicker in daemon thread
        4. Wait for connection event
        """
        self._resolve_tokens(symbols)
        self._ticker = KiteTicker(self._api_key, self._access_token)
        self._ticker.on_ticks = self._on_ticks
        self._ticker.on_connect = self._on_connect
        self._ticker.on_close = self._on_close
        self._ticker.on_error = self._on_error
        self._ticker.on_reconnect = self._on_reconnect

        # KiteTicker.connect(threaded=True) runs reactor in background
        self._ticker.connect(threaded=True)
        if not self._connected.wait(timeout=15):
            raise ConnectionError("KiteTicker did not connect within 15s")

    def _on_ticks(self, ws, ticks) -> None:
        """Called from WebSocket thread. Fan out to all registered builders."""
        with self._lock:
            for tick in ticks:
                token = tick.get("instrument_token")
                sym = self._token_to_symbol.get(token)
                if not sym:
                    continue

                ltp = tick["last_price"]
                ts = datetime.now(IST)  # MODE_QUOTE has no exchange_timestamp
                vol = tick.get("volume_traded")

                self._last_ltp[sym] = ltp
                self._tick_count += 1
                self._last_tick_ts = ts

                snapshot = MarketSnapshot(
                    symbol=sym, ts=ts, last_price=ltp,
                    volume=float(vol) if vol is not None else None,
                    source="websocket",
                )

                # Fan out to every registered session that wants this symbol
                for sid, builder in self._session_builders.items():
                    if sym in self._session_symbols.get(sid, set()):
                        builder.ingest(snapshot)

    def _on_connect(self, ws, response) -> None:
        tokens = list(self._token_to_symbol.keys())
        if tokens:
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_QUOTE, tokens)
        self._connected.set()
        self._reconnect_count = 0
        logger.info("KiteTicker connected — subscribed %d tokens", len(tokens))

    def _on_close(self, ws, code, reason) -> None:
        self._connected.clear()
        logger.warning("KiteTicker closed code=%s reason=%s", code, reason)

    def _on_error(self, ws, code, reason) -> None:
        logger.error("KiteTicker error code=%s reason=%s", code, reason)

    def _on_reconnect(self, ws, attempts) -> None:
        self._reconnect_count = attempts
        logger.warning("KiteTicker reconnecting attempt=%d", attempts)

    def update_symbols(self, session_id: str, symbols: list[str]) -> None:
        """Update a session's symbol set. Adjusts WebSocket subscription
        to the union of all sessions' symbols (ref-counted)."""
        with self._lock:
            self._session_symbols[session_id] = set(symbols)
            needed_syms = set().union(*self._session_symbols.values())
            needed_tokens = {self._symbol_to_token[s] for s in needed_syms
                            if s in self._symbol_to_token}
            current_tokens = set(self._token_to_symbol.keys())

            to_unsub = current_tokens - needed_tokens
            to_sub = needed_tokens - current_tokens

        if to_unsub and self._ticker:
            self._ticker.unsubscribe(list(to_unsub))
            with self._lock:
                for tok in to_unsub:
                    sym = self._token_to_symbol.pop(tok, None)
                    if sym:
                        self._symbol_to_token.pop(sym, None)

        if to_sub and self._ticker:
            self._ticker.subscribe(list(to_sub))
            self._ticker.set_mode(self._ticker.MODE_QUOTE, list(to_sub))

    def synthesize_quiet_symbols(
        self, builder: FiveMinuteCandleBuilder, symbols: list[str]
    ) -> None:
        """At bar boundary, inject a synthetic snapshot for symbols that had
        no tick in the current bucket, using last-known LTP. This forces
        FiveMinuteCandleBuilder to close the bar for quiet symbols."""
        now = datetime.now(IST)
        with self._lock:
            for sym in symbols:
                ltp = self._last_ltp.get(sym)
                if ltp is None:
                    continue  # never seen — skip
                # Check if builder already has a closed candle pending
                state = builder._states.get(sym)
                if state is None:
                    continue  # no open bucket — already closed or never started
                snapshot = MarketSnapshot(
                    symbol=sym, ts=now, last_price=ltp,
                    volume=None, source="websocket_synthetic",
                )
                builder.ingest(snapshot)

    def unregister_session(self, session_id: str) -> None:
        """Remove a session. If no sessions remain, close the WebSocket."""
        with self._lock:
            self._session_builders.pop(session_id, None)
            self._session_symbols.pop(session_id, None)
            remaining = len(self._session_builders)
        if remaining == 0:
            self.close()

    def close(self) -> None:
        if self._ticker:
            self._ticker.close()
            self._ticker = None
        self._connected.clear()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_ts(self) -> datetime | None:
        return self._last_tick_ts

    def get_last_ltp(self, symbol: str) -> float | None:
        return self._last_ltp.get(symbol)

    def _resolve_tokens(self, symbols: list[str]) -> None:
        """Map symbol names to instrument tokens via kite.instruments()."""
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=self._api_key)
        kite.set_access_token(self._access_token)
        instruments = kite.instruments(self._exchange)
        for inst in instruments:
            sym = inst["tradingsymbol"]
            if sym in set(symbols):
                token = inst["instrument_token"]
                self._token_to_symbol[token] = sym
                self._symbol_to_token[sym] = token
        missing = set(symbols) - set(self._symbol_to_token.keys())
        if missing:
            logger.warning("Tokens not found for %d symbols: %s",
                          len(missing), sorted(missing)[:10])
```

### Volume Delta Fix (FiveMinuteCandleBuilder)

**File:** `engine/live_market_data.py`

`FiveMinuteCandleBuilder.ingest()` currently stores `snapshot.volume` directly
as bar volume. For both REST and WebSocket, `volume_traded` from Kite is
**cumulative day volume**. On the 10:00 bar, "volume" would be the entire
day's volume since 09:15, making RVOL meaningless.

**Fix:** Track cumulative volume per symbol and compute delta:

```python
class FiveMinuteCandleBuilder:
    def __init__(self, interval_minutes: int = 5):
        ...
        self._prev_cumulative_vol: dict[str, float] = {}  # symbol → last cumul vol

    def ingest(self, snapshot: MarketSnapshot) -> list[ClosedCandle]:
        # Compute per-bar volume delta
        bar_vol = 0.0
        if snapshot.volume is not None:
            prev = self._prev_cumulative_vol.get(snapshot.symbol, 0.0)
            delta = snapshot.volume - prev
            bar_vol = max(0.0, delta)  # guard against day rollover / reset
            self._prev_cumulative_vol[snapshot.symbol] = snapshot.volume

        # Use bar_vol instead of snapshot.volume in _CandleState
        ...
```

This fix applies to both REST and WebSocket modes since both get cumulative
`volume_traded` from Kite. Replay and backtest are unaffected — they use
`intraday_day_pack` which already has correct per-bar volume.

### Supervision Loop Design

The existing `_run_live_loop` runs at ~1-5s cadence. With WebSocket, we keep
the same cadence but change what happens each cycle:

```python
# Pseudocode — NOT a new function, modifications to existing _run_live_cycle

async def _run_live_cycle(...):
    # 1. Session status check (pause/stop/cancel) — UNCHANGED
    current_session = await _load_session(session_id, deps)
    if current_session.status in {"PAUSED", "STOPPING", "COMPLETED", ...}:
        return ...

    # 2. Data source — TRANSPORT-DEPENDENT
    if using_websocket:
        now = datetime.now(IST)
        # Check if bar boundary crossed since last cycle
        if _bar_boundary_crossed(now, last_bar_ts, candle_interval_minutes):
            # Force-close quiet symbols before draining
            ticker_adapter.synthesize_quiet_symbols(builder, active_symbols)
            # Drain all closed candles from builder
            closed_candles = builder.flush_closed()  # new method: drain without destroying open
        else:
            closed_candles = []  # supervision-only cycle
        # Check WebSocket health for stale detection
        if not ticker_adapter.is_connected:
            no_snapshot_streak += 1
            # ... stale handling same as REST no-snapshot path
        else:
            no_snapshot_streak = 0
    else:
        # REST path — unchanged
        snapshots = await asyncio.to_thread(market_adapter.poll, active_symbols)
        ...

    # 3. Process closed candles — IDENTICAL for both transports
    for candle in closed_candles:
        await process_closed_candle(session, candle, runtime_state, now=candle.bar_end)

    # 4. Risk controls — UNCHANGED
    # 5. Symbol pruning — UNCHANGED
    # 6. Heartbeat/feed_state — UNCHANGED
```

**Key**: The 1s supervision cadence is maintained. On 59 out of 60 cycles,
WebSocket mode does almost nothing (just supervision checks). On the 1 cycle
at the bar boundary, it drains and processes all closed candles.

**Flush-on-any-exit**: The current live loop flushes remaining candles on ANY
loop exit (STOPPING, PAUSED, stale exit, interrupt, market close). This
contract must be preserved for WebSocket mode — `builder.flush()` is called
in the exit path regardless of exit reason, not just at market close.

### `--multi` Handling

```
┌─────────────────────────────────────────────────────┐
│              KiteTickerAdapter (ONE)                  │
│                                                     │
│  WebSocket thread:                                  │
│    _on_ticks() → fan out to ALL registered builders │
│                                                     │
│  session_builders:                                  │
│    "CPR_LEVELS_LONG-2026-04-07"  → builder_long     │
│    "CPR_LEVELS_SHORT-2026-04-07" → builder_short    │
│                                                     │
│  session_symbols:                                   │
│    "CPR_LEVELS_LONG-..."  → {SBIN, RELIANCE, ...}   │
│    "CPR_LEVELS_SHORT-..." → {SBIN, RELIANCE, ...}   │
│    → subscription = UNION = {SBIN, RELIANCE, ...}   │
│                                                     │
│  Lifecycle:                                         │
│    LONG registers → subscribe union                 │
│    SHORT registers → subscribe union (same, no-op)  │
│    LONG drops symbols → unsubscribe only if SHORT   │
│                          doesn't need them either   │
│    LONG finishes → unregister. SHORT keeps running.  │
│    SHORT finishes → unregister. close() WebSocket.   │
└─────────────────────────────────────────────────────┘
```

Each variant's `asyncio.gather` coroutine has its own `builder` and calls
`ticker_adapter.synthesize_quiet_symbols(builder, ...)` independently. No
shared mutable state between variant coroutines except the ticker adapter
(which is thread-safe under `_lock`).

### Error Handling / Reconnection

KiteTicker has built-in reconnect (exponential backoff). Additional handling:

1. **Consecutive reconnect alert**: After 5 reconnect attempts (~30s), send
   Telegram/email alert to operator: "WebSocket unstable — {reconnect_count}
   attempts. Open positions may not be managed timely."

2. **No REST fallback in this phase**: User explicitly said "I don't want API
   at all." REST adapter code stays in the codebase but is not the default.
   Future: add `--market-adapter rest` flag for emergency fallback.

3. **Session survival**: WebSocket disconnects do NOT crash the session.
   Positions remain open. On reconnect, ticks resume and candle building
   continues from the last-known state. The worst case is a missed bar
   where the last-known LTP is used (synthesize_quiet_symbols handles this).

4. **Stale detection**: If `ticker_adapter.last_tick_ts` is older than
   `stale_timeout` seconds, treat as stale (same as REST no-snapshot path).

### Twisted Signal Warning

`KiteTicker` uses `autobahn` + Twisted reactor. When `connect(threaded=True)`
runs the reactor in a background thread, Twisted warns: "signal only works
in main thread." This is cosmetic — ticks flow normally. Suppress with:
```python
import warnings
warnings.filterwarnings("ignore", message=".*signal only works in main thread.*")
```

### Files Changed (Phase 2)

| File | Change |
|------|--------|
| `engine/kite_ticker_adapter.py` | NEW — WebSocket adapter with per-session builders, ref-counted subscriptions |
| `engine/live_market_data.py` | Volume delta fix in `FiveMinuteCandleBuilder.ingest()` |
| `scripts/paper_live.py` | Transport-agnostic `_run_live_cycle` + WebSocket integration |
| `scripts/paper_trading.py` | Create `KiteTickerAdapter` instead of `_SharedCachingAdapter` for `--multi` |
| `tests/test_live_market_data.py` | Volume delta test cases |
| `tests/test_kite_ticker.py` | NEW — unit tests for KiteTickerAdapter (mock ticks) |

---

## Phase 3 — Paper Replay Fixes (Priority: HIGH)

### 3A — LONG+SHORT Concurrency Bug (~0.5 days)

**Symptom:** With `--multi`, one variant finishes and the other fails with
`Invalid Input Error: No open result set`.

**Root cause (to verify first):** `asyncio.gather` in a single-threaded event loop
interleaves coroutines at `await` points — not true parallelism. The error likely
comes from one coroutine advancing a shared DuckDB result set while the other
coroutine tries to `.fetchone()` on it. This is an event-loop interleaving issue,
not a threading issue. `_PAPER_DB_IO_LOCK` (a `threading.RLock`) does NOT protect
against this — two coroutines can both hold an RLock from the same OS thread.

**Fix — primary:** Run LONG and SHORT replay **sequentially** (not `asyncio.gather`)
for `daily-replay`. Replay is already faster than real-time — sequential execution
has no operational cost and eliminates all interleaving. Use `asyncio.gather` only
for `daily-live` where both variants must poll simultaneously.

**Fix — secondary (if gather is kept):** Wrap all `paper.duckdb` execute+fetch pairs
in a coroutine-safe mutex (`asyncio.Lock`, not `threading.RLock`). An `asyncio.Lock`
prevents two coroutines from interleaving within the locked section.

**Action before implementing:** Run `daily-replay --multi` with full Python traceback
enabled and capture the exact line number of the error. Confirm it is in the DuckDB
execute path before applying the fix.

### 3B — Archive Step Reads from Live Writer

**Status: Already fixed.** `paper_replay.py:596-598` calls `archive_completed_session`
with `paper_db=get_dashboard_paper_db()` (reads from `paper_replica/` via
`ReplicaConsumer`). A `force_paper_db_sync(source_conn=pdb.con)` is called at line
589 before archiving. **No action needed.**

### 3C — Backtest vs Replay vs Live Parity (historical trigger case)

**Historical trigger:** Replay opened trades for symbols (e.g., GANESHCP, ASTERDM on 2026-04-01)
that were not present in the then-current baseline backtest run. That compare drove the CPR parity rework,
but it is not a standing open mismatch in the current codebase.

**Existing shared evaluator:** Backtest and paper runtime already share
`scan_cpr_levels_entry()` via `engine/cpr_atr_shared.py:270`, called from
`paper_runtime.py:1661`. The shared evaluator is present. The parity risk is NOT
a missing shared function — it is in one of:

1. **Setup row sourcing**: backtest reads `market_day_state`; replay reads
   `intraday_day_pack` + `market_day_state`; live reads `_load_live_setup_row`.
   If these resolve to different CPR/ATR values, identical candles produce different signals.
   **Fix**: Add assertion in replay/live that `tc`, `bc`, `atr` from their source
   match the `market_day_state` values for the same symbol/date.

2. **Candle construction**: backtest uses `intraday_day_pack` arrays; replay
   builds candles from the same pack; live uses `FiveMinuteCandleBuilder` from
   `engine/live_market_data.py:57`. If `close` of a bar differs by even 0.01 due
   to LTP timing, an entry that triggers on backtest may miss in live.
   **Fix**: For replay, verify candle OHLCV matches `intraday_day_pack` arrays exactly.

3. **State management**: `SymbolRuntimeState.position_closed_today` enforces
   one-entry-per-symbol-per-day in replay/live. Verify backtest applies the same rule.

**Action before implementing:** Run `pivot-parity-check` on a known-divergent date
(e.g., 2026-04-01 for GANESHCP) and log setup rows + candle arrays from both paths.
The divergence will be in one of the three areas above.

**Do not restructure the shared evaluator** — it is already correct. Fix the input
that differs, not the evaluator.

**Parity test:** After fix, run:
```bash
# Backtest baseline
doppler run -- uv run pivot-backtest --symbol SBIN --start 2026-04-01 --end 2026-04-01 \
  --strategy CPR_LEVELS --preset CPR_LEVELS_RISK_SHORT --save

# Replay of same date
doppler run -- uv run pivot-paper-trading daily-replay \
  --multi --strategy CPR_LEVELS --trade-date 2026-04-01 --all-symbols

# Diff: backtest results vs paper session in backtest.duckdb (execution_mode=PAPER)
doppler run -- uv run pivot-parity-check \
  --expected-run-id <bt_run_id> --actual-run-id <paper_run_id>
```
Target: 0 parity divergences on any date in the baseline window.

### 3D — RVOL Display Audit (~0.5 days)

Partly done. Remaining: audit fallback SQL path in `db/backtest_db.py:765`
where `rvol_threshold` may be serialized without `skip_rvol_check` context.
Pattern: wherever `rvol_threshold` is displayed, check `skip_rvol_check` first;
show "OFF" if True.

---

## Phase 4 — Full 2105 Universe in Live (after Phases 0–2)

With Phase 0 pre-filter + Phase 2 WebSocket:
- KiteTicker supports 3000 instruments per connection in `MODE_QUOTE`
- Subscribe all qualifying symbols after Stage A filter (~200–400)
- `FiveMinuteCandleBuilder` accumulates ticks for all subscribed symbols
- Bar close: process 200–400 symbols sequentially in Python (~2–5s, no I/O)
- After entry window: `update_symbols()` reduces subscription to open positions

`--all-symbols` becomes viable without `--symbols` restriction.

---

## Phase 5 — Unified Bar Orchestrator (Completed 2026-04-09 IST)

### Problem

The three execution modes (backtest, replay, live) share entry/exit decision
logic (`cpr_atr_shared.py`) but have separate, divergent **orchestration** —
the per-bar logic that decides which symbols to process, enforces
`max_positions`, manages portfolio cash/slots, and prunes finished symbols.

This causes six concrete parity bugs:

1. **Symbol pruning** — live prunes rejected symbols after each bar (lines 567-581
   in `paper_live.py`), replay processes all 439 symbols for all 75 bars, wasting
   ~27K `process_closed_candle()` calls after the entry window closes.
2. **Tie-break ordering** — when 18 entries fire at 09:20, backtest picks by
   `(date, entry_time, symbol)` (alphabetical within same bar), replay uses
   DayPack load order, live uses `session.symbols` order. Different modes pick
   different 10 out of 18.
3. **Cash/slot sizing** — backtest enforces both `max_positions` AND shared cash /
   slot capital (`_apply_portfolio_constraints` at `cpr_atr_strategy.py:818-857`).
   Paper runtime only checks `max_positions` count and opens full candidate size
   (`paper_runtime.py:1606-1651`). This changes executed qty and PnL even when the
   same symbols trigger.
4. **Exit-before-entry ordering** — backtest sorts trades chronologically and
   releases exited positions before evaluating later entries (line 827-839).
   Replay/live process one symbol at a time and commit immediately. If symbol A
   closes on a bar and symbol B opens on the same bar, the result depends on
   iteration order.
5. **Risk control cadence** — live checks `_check_risk_controls()` every poll
   cycle, replay checks `_check_replay_risk_controls()` only after whole date.
6. **Per-bar DB overhead** — `get_session_positions()` hits paper.duckdb for
   every symbol on every bar (~33K round-trips). With `--multi`, the
   `_PAPER_DB_IO_LOCK` serialization between LONG+SHORT makes this worse.

### Design: In-Memory Position Tracking + Shared Bar Orchestration

**Core insight**: Backtest's speed comes from in-memory state — no DB I/O per bar.
Replay and live should do the same. DuckDB is excellent for analytics (batch queries,
columnar scans) but painful for per-bar operational state. Move position tracking
in-memory, persist to DB only at key events (open, close, session end).

**New file: `engine/bar_orchestrator.py`**

Three responsibilities:
1. `SessionPositionTracker` — in-memory position book (replaces per-bar DB reads)
2. `should_process_symbol()` — shared per-bar filter (replaces divergent pruning)
3. `select_entries_for_bar()` — canonical max_positions + tie-break (replaces
   immediate-commit pattern)

### 5A: SessionPositionTracker

In-memory position book for one session (one variant). Each `--multi` variant
(LONG/SHORT) gets its own tracker — no shared mutable state, no DB contention.

```python
@dataclass
class TrackedPosition:
    position_id: str
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    target_price: float
    entry_time: str
    status: str  # "OPEN" | "CLOSED"

class SessionPositionTracker:
    """In-memory position book for one session (one variant).
    
    Mirrors backtest's _apply_portfolio_constraints() cash/slot model:
    - slot_capital = equity / max_positions
    - cash_available tracks pool as positions open/close
    - max_positions enforced in real-time
    """

    def __init__(self, max_positions: int, portfolio_value: float):
        self.max_positions = max_positions
        self.cash_available = portfolio_value
        self.slot_capital = portfolio_value / max(1, max_positions)
        self._open: dict[str, TrackedPosition] = {}   # symbol → position
        self._closed_today: set[str] = set()           # symbols that traded

    @property
    def open_count(self) -> int:
        return len(self._open)

    def has_open_position(self, symbol: str) -> bool:
        return symbol in self._open

    def has_traded_today(self, symbol: str) -> bool:
        return symbol in self._closed_today

    def can_open_new(self) -> bool:
        return self.open_count < self.max_positions

    def slots_available(self) -> int:
        return max(0, self.max_positions - self.open_count)

    def record_open(self, position: TrackedPosition, position_value: float):
        self._open[position.symbol] = position
        self.cash_available -= position_value

    def record_close(self, symbol: str, exit_value: float):
        self._open.pop(symbol, None)
        self._closed_today.add(symbol)
        self.cash_available += exit_value

    def open_symbols(self) -> set[str]:
        return set(self._open.keys())

    def compute_position_qty(self, entry_price: float,
                              risk_based_sizing: bool,
                              candidate_size: int = 0) -> int:
        """Compute executable qty matching backtest's portfolio sizing."""
        if risk_based_sizing:
            investable = min(candidate_size * entry_price, self.cash_available)
        else:
            investable = min(self.slot_capital, self.cash_available)
        return max(0, int(investable / entry_price)) if entry_price > 0 else 0

    def reset_day(self, portfolio_value: float | None = None):
        self._open.clear()
        self._closed_today.clear()
        if portfolio_value is not None:
            self.cash_available = portfolio_value
            self.slot_capital = portfolio_value / max(1, self.max_positions)
```

**DB writes still happen** for `open_position()` and `update_position()` — those
are event-driven (only when a trade opens/closes). These use the existing
`_PAPER_DB_IO_LOCK` which is fine for rare events (~10-20 per session, not ~33K).

### 5B: should_process_symbol()

Shared per-bar filter called by both replay and live. Replaces the custom pruning
at `paper_live.py:567-581` and the missing pruning in replay.

```python
def should_process_symbol(
    bar_time: str,             # "09:20", "10:30", etc.
    entry_window_end: str,     # "10:15" default
    tracker: SessionPositionTracker,
    symbol: str,
    setup_status: str,         # "candidate" | "rejected" | "pending"
) -> bool:
    """Decide whether to call process_closed_candle for this symbol on this bar.
    
    Uses richer setup_status (not just bool) to match paper_runtime.py:759-765.
    """
    has_position = tracker.has_open_position(symbol)

    # Always process symbols with open positions (SL/target/trail management)
    if has_position:
        return True

    # Past entry window — nothing to manage, nothing to enter
    if bar_time >= entry_window_end:
        return False

    # Setup rejected (wrong direction, no CPR, etc.) — skip
    if setup_status == "rejected":
        return False

    # Already traded and closed today — one entry per symbol per day
    if tracker.has_traded_today(symbol):
        return False

    # Pending (setup_row not yet loaded) or candidate — keep processing
    return True
```

### 5C: select_entries_for_bar()

When multiple symbols trigger entries on the same bar, apply canonical
tie-breaking. Matches backtest's `(trade_date, entry_time, symbol)` sort
— within one bar `entry_time` is identical, so symbol is the effective tie-break.
This is a reproducibility rule, not an optimizer. If you want a profitability-
weighted tie-break (for example, highest effective RR or a custom edge score),
define it explicitly as a strategy change and rerun the baselines.

```python
def select_entries_for_bar(
    candidates: list[dict],     # entry candidates from evaluate_candle()
    tracker: SessionPositionTracker,
) -> list[dict]:
    """Select which entries to execute, respecting max_positions + cash.
    
    Canonical tie-break: alphabetical by symbol (matches backtest).
    """
    if not candidates:
        return []

    available = tracker.slots_available()
    if available <= 0:
        return []

    # Canonical alphabetical sort — same as backtest's third sort key
    sorted_candidates = sorted(candidates, key=lambda c: c["symbol"])
    return sorted_candidates[:available]
```

### 5D: Split process_closed_candle into Evaluate vs Execute

Currently `process_closed_candle()` both evaluates and commits (DB writes) in
one call. We need to split it so the orchestrator can:

1. **Advance all open positions first** (exits free slots + cash)
2. **Evaluate all symbols for entry** (collect candidates)
3. **Apply tie-break + max_positions** (select winners)
4. **Execute only selected entries** (DB writes)

This matches backtest's chronological ordering where exited positions release
slots before new entries are considered.

**Critical fix — stale-candidate resurrection**: The current `process_closed_candle()`
rescans from entry window start to the current bar on every bar. This means a signal
at 09:25 that was blocked by `max_positions` can be "resurrected" at 09:40 when a slot
frees up — using the 09:25 entry price but executing at 09:40. Backtest does NOT do
this — it only fires on the bar that originally triggered. The `evaluate_candle()`
function must only evaluate the **current bar**, not rescan history. Entry signals
are ephemeral: if blocked on their bar, they are gone.

```python
async def evaluate_candle(
    *,
    session: PaperSession,
    candle: Any,
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    now: datetime,
) -> dict[str, Any]:
    """Evaluate one candle for one symbol. Returns action intent, no DB writes
    for entries. Position advances (exits) still write immediately since
    they free slots.
    
    Returns:
        {
            "symbol": str,
            "action": "ADVANCE" | "ENTRY_CANDIDATE" | "SKIP",
            "setup_status": "candidate" | "rejected" | "pending",
            "candidate": dict | None,      # if ENTRY_CANDIDATE
            "advance_result": dict | None,  # if ADVANCE (may have closed)
            "skip_reason": str | None,
        }
    """
```

```python
async def execute_entry(
    *,
    session: PaperSession,
    candidate: dict,
    setup_row: dict,
    params: BacktestParams,
    tracker: SessionPositionTracker,
    now: datetime,
) -> dict[str, Any]:
    """Open a position for a selected entry. Writes to DB + updates tracker."""
    qty = tracker.compute_position_qty(
        entry_price=candidate["entry_price"],
        risk_based_sizing=params.risk_based_sizing,
        candidate_size=candidate.get("position_size", 0),
    )
    if qty < 1:
        return {"action": "SKIP", "reason": "no_cash"}
    
    result = await _open_position_from_candidate(...)
    tracker.record_open(TrackedPosition(...), position_value=qty * candidate["entry_price"])
    return result
```

### 5E: Integrate into Replay

Refactor `_process_replay_bar_major()` in `scripts/paper_replay.py`:

```python
async def _process_replay_bar_major(...):
    tracker = SessionPositionTracker(
        max_positions=session.max_positions,
        portfolio_value=session.portfolio_value,
    )
    # Canonical alphabetical order — matches backtest tie-break
    active_days = sorted(date_items, key=lambda d: d.symbol)
    
    # Canonical time grid — NOT reference symbol's array index
    all_bar_times = sorted({t for d in active_days for t in d.day_pack.time_str})
    entry_window_end = params.entry_window_end  # "10:15"

    for bar_time in all_bar_times:
        # STEP 1: Advance existing positions (exits first, frees slots)
        for day in active_days:
            if not tracker.has_open_position(day.symbol):
                continue
            idx = day.day_pack._idx_by_time.get(bar_time)
            if idx is None:
                continue
            result = await evaluate_candle(...)  # will advance/close position
            if result["advance_result"] and result["advance_result"]["action"] == "CLOSE":
                exit_value = ...  # position_value + pnl
                tracker.record_close(day.symbol, exit_value)

        # STEP 2: Evaluate entry candidates
        entry_candidates = []
        for day in active_days:
            if not should_process_symbol(bar_time, entry_window_end,
                                          tracker, day.symbol, setup_status):
                continue
            if tracker.has_open_position(day.symbol):
                continue  # already advanced in step 1
            idx = day.day_pack._idx_by_time.get(bar_time)
            if idx is None:
                continue
            result = await evaluate_candle(...)
            if result["action"] == "ENTRY_CANDIDATE":
                entry_candidates.append(result)

        # STEP 3: Apply max_positions + tie-break
        selected = select_entries_for_bar(entry_candidates, tracker)
        for entry in selected:
            await execute_entry(...)

        # STEP 4: Prune symbols for next bar
        active_days = [
            d for d in active_days
            if should_process_symbol(bar_time, entry_window_end,
                                      tracker, d.symbol, ...)
        ]

        await asyncio.sleep(0)  # yield for alert dispatch
```

### 5F: Integrate into Live

Replace the custom pruning logic at `paper_live.py:567-581` with shared functions.

```python
async def _process_snapshots(...):
    # Closed candles from FiveMinuteCandleBuilder
    
    # STEP 1: Advance existing positions (exits first)
    for candle in closed_candles:
        if tracker.has_open_position(candle.symbol):
            result = await evaluate_candle(...)
            if closed:
                tracker.record_close(candle.symbol, exit_value)

    # STEP 2: Evaluate entry candidates
    entry_candidates = []
    for candle in closed_candles:
        if not should_process_symbol(bar_time, entry_window_end,
                                      tracker, candle.symbol, setup_status):
            continue
        if tracker.has_open_position(candle.symbol):
            continue  # already advanced
        result = await evaluate_candle(...)
        if result["action"] == "ENTRY_CANDIDATE":
            entry_candidates.append(result)

    # STEP 3: Apply max_positions + tie-break (same function as replay)
    selected = select_entries_for_bar(entry_candidates, tracker)
    for entry in selected:
        await execute_entry(...)

    # STEP 4: Prune active_symbols (replaces lines 567-581)
    active_symbols[:] = [
        sym for sym in active_symbols
        if should_process_symbol(bar_time, entry_window_end,
                                  tracker, sym, ...)
    ]
```

**Live bar batching note**: In live, candle-close events may arrive across
different poll cycles for different symbols. The current `FiveMinuteCandleBuilder`
detects boundary per-symbol independently. For max_positions tie-breaking to work
correctly, all symbols' closed candles for the same `bar_end` should be batched
before entry selection. Accept that live can only achieve approximate parity here
— exact tie-breaking requires all symbols' bars to arrive simultaneously, which
REST polling cannot guarantee. Phase 2 (WebSocket) improves this since ticks
arrive sub-second.

### 5G: Align Risk Controls

Move replay's risk check from per-date to per-bar (match live):

```python
# In the bar loop, after processing entries:
if await check_bar_risk_controls(session, tracker, bar_time, symbol_last_prices):
    break  # flatten and stop
```

Extract shared `check_bar_risk_controls()` callable by both replay and live,
wrapping the existing `enforce_session_risk_controls()`.

### 5H: Fix Setup Row Fallback

`_load_live_setup_row()` (line ~527 in `paper_runtime.py`) omits `rvol_baseline`
when materialized `market_day_state` rows are missing.

**Fix (preferred)**: Require materialized rows. Fail fast with clear error if
`market_day_state` is missing for a symbol on the trade date:
```
ERROR: market_day_state missing for {symbol} on {trade_date}.
Run: doppler run -- uv run pivot-build --refresh-date {trade_date}
```

This is already the expected flow per AGENTS.md — `pivot-build` must run before
live/replay.

**Setup row caching risk**: The Phase 1 startup prefetch calls `load_setup_row()`
for all qualifying symbols before the poll loop. If the fast path returns a
partial/zeroed row (e.g., OR-dependent fields are null during warm-up), it gets
cached in `state.setup_row` permanently and the symbol is rejected forever.
**Guard**: Do NOT cache a fast-path row unless critical fields (`tc`, `bc`, `atr`)
are non-null/non-zero. If incomplete, keep `state.setup_row = None` and let the
fallback path attempt again on first bar close.

### 5I: Replay Bar Alignment

Current replay aligns bars by array index (`bar_idx`), not by `time_str`.
If a symbol has fewer bars, `bar_idx=5` might be "09:40" while for others it's
"09:35".

**Fix**: Drive replay from a canonical time grid built from the union of all
symbols' `time_str` arrays. Align each symbol by looking up the index for each
canonical bar time via `day_pack._idx_by_time`. See Phase 5E pseudocode above.

### 5I-bis: Fix Live Volume (cumulative → per-bar delta)

**Moved to Phase 2.** The volume delta fix is implemented in
`FiveMinuteCandleBuilder.ingest()` as part of the WebSocket integration,
since it affects both REST and WebSocket modes. See Phase 2 — Volume Delta Fix.

### 5J: `--multi` Safety

Each `--multi` variant (LONG/SHORT) gets its own `SessionPositionTracker`.
No shared mutable state between variants. The per-bar orchestration is entirely
per-session.

DuckDB contention is reduced from ~33K reads/session to ~10-20 writes/session:
- `open_position()`: ~10 per session (write, uses `_PAPER_DB_IO_LOCK`)
- `update_position()`: ~10 per session (write, uses `_PAPER_DB_IO_LOCK`)
- `get_session_positions()`: **eliminated** from hot path (in-memory tracker)
- `load_setup_row()`: cached after first bar (unchanged, ~400 reads at startup)

With `asyncio.gather` for `--multi`, LONG and SHORT coroutines interleave at
`await` points. Since DB writes are rare events with the `_PAPER_DB_IO_LOCK`,
the probability of contention drops dramatically.

### 5K: Backtest Parity Verification

Backtest stays vectorized 2-pass for performance. We do NOT assume the 2-pass
approach produces identical results — we **prove** it with parity tests.

**New file: `tests/test_parity.py`**

```python
def test_backtest_replay_parity_known_date():
    """Run same date through backtest and replay, assert identical trades.
    
    Checks:
    - Same number of trades
    - Same symbols traded
    - Same entry times
    - Same entry prices (within float tolerance)
    - Same exit reasons
    - Same exit prices
    - Same qty (portfolio sizing parity)
    - Same PnL (within cost-model tolerance)
    """
```

If parity tests fail, the test output pinpoints exactly which decision diverges
(entry, exit, sizing, tie-break) — making it actionable.

### Expected Impact

| Metric | Before | After |
|--------|--------|-------|
| Per-bar DB reads (replay) | ~33,000 | ~0 (in-memory) |
| Per-bar DB reads (live) | ~33,000 | ~0 (in-memory) |
| Symbols processed after 10:15 | 439 | 0-10 (open positions only) |
| Tie-break parity | ❌ divergent | ✅ alphabetical everywhere |
| Cash/slot sizing parity | ❌ backtest only | ✅ shared tracker |
| Exit-before-entry ordering | ❌ divergent | ✅ shared 2-step |
| Risk control cadence | ❌ live≠replay | ✅ per-bar everywhere |

### Phase 5 Sub-Phase Order

Phases 5A-5K are incremental — each is testable independently:

1. **5A** — `SessionPositionTracker` (new module, no callers yet, unit tests)
2. **5B** — `should_process_symbol()` (new module, no callers yet, unit tests)
3. **5C** — `select_entries_for_bar()` (new module, unit tests)
4. **5D** — Split `evaluate_candle` / `execute_entry` in `paper_runtime.py`
5. **5E** — Integrate into replay (first consumer)
6. **5F** — Integrate into live (second consumer)
7. **5G** — Risk control alignment
8. **5H** — Setup row fallback (fail fast)
9. **5I** — Bar alignment fix
10. **5J** — `--multi` validation
11. **5K** — Parity tests

**Risk**: Phase 5D is the riskiest — splitting `process_closed_candle` into
evaluate + execute touches the hot path. Must preserve all existing behavior.
Test with known dates before and after to confirm identical results.

## File Change Summary

| File | Change | Phase |
|------|--------|-------|
| `scripts/paper_prepare.py` | Extend `pre_filter_symbols_for_strategy()` — Stage A SQL (narrow CPR + min_price) | 0 |
| `scripts/paper_live.py` | Call `pre_filter_symbols_for_strategy()` at startup; apply Stage B direction filter after 09:20 bar | 0 |
| `scripts/paper_live.py` | Entry window reduction — pass open-position symbols to `ticker.update_symbols(session_id, ...)` | 0 |
| `scripts/paper_live.py` | Hard fail-fast at startup if `cpr_daily` has 0 rows for today | 0 |
| `scripts/paper_live.py` | Pre-call `load_setup_row()` for all qualifying symbols before poll loop | 1 |
| `engine/kite_ticker_adapter.py` | New — WebSocket adapter with per-session builders, ref-counted subscriptions, quiet-symbol synthesis | 2 |
| `engine/live_market_data.py` | Volume delta fix in `FiveMinuteCandleBuilder.ingest()` — cumulative → per-bar delta | 2 |
| `scripts/paper_live.py` | Transport-agnostic `_run_live_cycle` with WebSocket support; 1s supervision cadence preserved | 2 |
| `scripts/paper_trading.py` | Create `KiteTickerAdapter` for `--multi` instead of `_SharedCachingAdapter` | 2 |
| `tests/test_live_market_data.py` | Volume delta test cases | 2 |
| `tests/test_kite_ticker.py` | New — unit tests for KiteTickerAdapter (mock ticks, quiet symbols, multi-session) | 2 |
| `scripts/paper_replay.py` | Run LONG+SHORT concurrently (`asyncio.gather`) with bar-major replay ordering | 3A |
| `scripts/paper_live.py` | Add parity assertion logging: log setup row source + candle OHLCV at each bar | 3C |
| `engine/bar_orchestrator.py` | New — `SessionPositionTracker`, `should_process_symbol()`, `select_entries_for_bar()`, `check_bar_risk_controls()` | 5A-C,G |
| `engine/paper_runtime.py` | Split `process_closed_candle()` → `evaluate_candle()` + `execute_entry()`; fail-fast setup row fallback | 5D,H |
| `scripts/paper_replay.py` | Use orchestrator: canonical time grid, shared filter, exits-before-entries, in-memory tracker | 5E,I |
| `scripts/paper_live.py` | Use orchestrator: replace custom pruning (lines 567-581) with shared functions, in-memory tracker | 5F |
| `tests/test_bar_orchestrator.py` | New — unit tests for `SessionPositionTracker`, `should_process_symbol()`, `select_entries_for_bar()` | 5A-C |
| `tests/test_parity.py` | New — cross-mode parity tests (backtest vs replay, same date/params → identical trades) | 5K |
| `engine/cpr_atr_strategy.py` | Add `compound_equity: bool = False` field + daily reset in `_apply_portfolio_constraints` | 6 |
| `engine/strategy_presets.py` | Add `"compound_equity"` to `simple_fields` | 6 |
| `engine/run_backtest.py` | Add `--compound-equity` CLI flag + pass through in preset/non-preset paths | 6 |

**Phase 5 modifies `process_closed_candle` split — the evaluate/execute boundary
is the riskiest change. All other Phase 0-4 notes about core logic stability
apply only to their respective phases.**

---

## Phase 6 — Daily Capital Reset (Completed 2026-04-10)

### Problem

The backtest engine (`_apply_portfolio_constraints`) carried forward accumulated equity across trading days within a single `run()` call. After a profitable month, the backtest traded with 3–4x the initial capital — sizes that paper replay/live could never achieve since they start each session with a fixed ₹1M.

This caused structural parity divergence: the same CPR_LEVELS preset produced different trade quantities (and sometimes different trade counts for risk-based sizing) in backtest vs paper.

### Fix

Added `compound_equity: bool = False` to `StrategyConfig`. When `False` (default), `equity` resets to `portfolio_value` at the start of each trading day in `_apply_portfolio_constraints`. When `True`, equity carries forward as before.

**Files changed:**
- `engine/cpr_atr_strategy.py` — `compound_equity` field on StrategyConfig + daily reset in `_apply_portfolio_constraints`
- `engine/strategy_presets.py` — `"compound_equity"` added to `simple_fields`
- `engine/run_backtest.py` — `--compound-equity` CLI flag

### Baseline Comparison (2025-01-01 → 2026-04-09, 2099 symbols, ₹1M portfolio)

#### RISK_BASED_SIZING = ON (risk-sizing presets)

| Variant | Mode | Run ID | Trades | WR | P/L | Return | PF | Max DD |
|---------|------|--------|--------|----|-----|--------|----|--------|
| RISK_LONG | Compound | `c839350ea00d` | 2,954 | 34.1% | ₹2,467,691 | +246.8% | 3.18 | 0.5% |
| RISK_LONG | Daily Reset | `7e87066d4d51` | 2,415 | 31.6% | ₹1,676,672 | +167.7% | 3.19 | 0.8% |
| RISK_SHORT | Compound | `24175af4f18c` | 4,123 | 32.6% | ₹2,648,801 | +264.9% | 2.67 | 1.5% |
| RISK_SHORT | Daily Reset | `189d89120089` | 3,066 | 27.1% | ₹1,350,785 | +135.1% | 2.42 | 1.3% |

#### RISK_BASED_SIZING = OFF (slot-sizing presets)

| Variant | Mode | Run ID | Trades | WR | P/L | Return | PF | Max DD |
|---------|------|--------|--------|----|-----|--------|----|--------|
| STD_LONG | Compound | `a1c6c74d4a44` | 3,088 | 34.7% | ₹1,604,269 | +160.4% | 2.61 | 0.7% |
| STD_LONG | Daily Reset | `9cd7ad14b771` | 3,085 | 34.7% | ₹828,755 | +82.9% | 2.44 | 0.4% |
| STD_SHORT | Compound | `1cab93ad9aa6` | 4,404 | 34.1% | ₹2,473,820 | +247.4% | 2.36 | 1.4% |
| STD_SHORT | Daily Reset | `fb075f4467af` | 4,403 | 34.1% | ₹1,033,852 | +103.4% | 2.16 | 1.1% |

### Key Observations

1. **Slot-sizing trades barely changed** (3088→3085 LONG, 4404→4403 SHORT). Slot sizing computes `qty = (equity / max_positions) / entry_price`. With ₹1M and max_positions=10, each slot is ₹100K. The universe includes stocks from ₹50 (GKSL) to ₹14,000+ (TVSHLTD). For most stocks, ₹100K buys enough shares — even after compounding doubles the slot to ₹200K, the trade still happens (just with more shares). Only marginal cases where `slot_capital < entry_price` (very expensive stocks with many concurrent positions) get rejected. **Result**: same trades, smaller sizes → P/L halves but count barely changes.

2. **Risk-sizing trades dropped significantly** (2954→2415 LONG, 4123→3066 SHORT). Risk sizing computes `investable = min(candidate_notional, cash_available)` — it gates on the **total portfolio cash pool**, not per-slot limits. When several positions are open, `cash_available` shrinks. With compounding, the pool grows (₹1M → ₹2M+ after a winning streak), allowing more concurrent opens. With daily ₹1M reset, the pool is smaller and extra candidates get rejected at the cash gate. **Result**: fewer trades AND smaller sizes. These aren't different signals — they're the same signals that paper trading can't afford to take.

3. **All 4 variants remain profitable** with daily reset. The old compounding returns were inflated by the equity snowball effect. Daily reset is the honest number that paper trading can achieve.

4. **Regression verified**: Rerunning all 4 variants with `--compound-equity` produced byte-identical P/L to the pre-change baselines (delta ₹0.00 on all 4).

### Parity Guarantee

With `compound_equity=False` (default):
- Backtest multi-day run: each day starts with ₹1M → matches paper replay per-session
- Paper replay: each session starts with ₹1M → matches backtest daily slice
- Paper live: each session starts with ₹1M → matches backtest daily slice
- Single-date backtest: only 1 day → unaffected (always used ₹1M)

The `--portfolio-value` flag allows changing the base capital (e.g., ₹500K on choppy days, ₹2M when confident). This applies to all three modes consistently.

---

## Constraints

- **DuckDB single-writer**: `paper.duckdb` one writer only. Reads via `get_dashboard_db()`.
  Phase 5 reduces write contention from ~33K per-bar reads to ~10-20 event-driven writes.
- **No `asyncio.to_thread` for DuckDB**: All DB calls synchronous in event loop.
- **`maybe_sync()` must pass `source_conn`**: Never call without live connection reference.
- **`_MARKET_DB_READ_LOCK`**: Hold on all `get_dashboard_db().con.execute()` calls.
- **No hidden strategy defaults**: `min_price`, `narrowing_filter`, `risk_based_sizing`
  must come from explicit CLI flags or named presets — never injected by the engine.
- **`max_positions=10`**: Concurrent open positions cap only. Not a daily trade cap.
- **Kite WebSocket**: Same `api_key + access_token` as REST. No new Doppler secrets.
  Uses `MODE_QUOTE` (not `MODE_FULL`). `exchange_timestamp` is empty in MODE_QUOTE
  — use `datetime.now(IST)` as timestamp (sub-second accuracy sufficient for 5-min bars).
- **WebSocket supervision**: 1s loop cadence maintained even with WebSocket — needed
  for pause/stop/stale/risk/heartbeat checks. Candle processing only at bar boundaries.
- **Quiet symbol handling**: At each bar boundary, `synthesize_quiet_symbols()` injects
  a last-known-LTP snapshot for symbols with no tick in the current bucket. Without this,
  `FiveMinuteCandleBuilder` never closes the bar for quiet symbols.
- **All symbols, not NIFTY 50**: Symbol universe = all 2105 NSE EQ symbols filtered
  by CPR conditions. Do not hardcode or restrict to index components.
- **In-memory position tracking**: `SessionPositionTracker` is the single source of
  truth during a session. DB writes are event-driven persistence, not the read path.
  Each `--multi` variant gets its own tracker — no shared mutable state.
- **Exits before entries per bar**: Within one bar, advance/close existing positions
  FIRST, then evaluate and select new entries. Matches backtest's chronological ordering.
- **Daily capital reset**: `compound_equity=False` (default) resets equity to `portfolio_value`
  (₹1M) at the start of each trading day. This matches paper replay/live behavior. Use
  `--compound-equity` to restore old carry-forward compounding. `--portfolio-value` changes
  the base capital for all three modes.

---

## Implementation Status Update (2026-04-09 IST)

### Scope tracked in this execution pass

- Cleanup the two explicit FBR baseline runs from analytics storage:
  - `c2cd9f540972`
  - `c3a53be848a8`
- Rerun two CPR baselines with the exact historical parameter bundle:
  - reference `f64daddf7de6` (CPR SHORT)
  - reference `04b6fcf0a99c` (CPR LONG)
- Cleanup all paper-session state and archived PAPER analytics rows.
- Record late-start live findings and apply logging/runtime fixes.

### Completed

1. FBR run cleanup (explicit IDs)
- `c2cd9f540972` removed from `run_metadata`, `run_metrics`, `run_daily_pnl`, and `backtest_results`.
- `c3a53be848a8` removed from `run_metadata`, `run_metrics`, `run_daily_pnl`, and `backtest_results`.

2. Full paper cleanup
- `paper_sessions`, `paper_positions`, `paper_orders`, `paper_feed_state` cleared.
- Archived PAPER analytics rows cleared:
  - `run_metadata` where `execution_mode='PAPER'`: `0`
  - `backtest_results` where `execution_mode='PAPER'`: `0`

3. CPR baseline reruns (exact-match parity check)
- New run `52f449e804b0` re-ran `f64daddf7de6` parameters.
  - Trade count: `4400` vs `4400` (delta `0`)
  - Total P/L: `₹2,460,259.73` vs `₹2,460,259.73` (delta `0.00`)
  - Total return: `246.03%` vs `246.03%` (delta `0.00%`)
- New run `4419e4acee83` re-ran `04b6fcf0a99c` parameters.
  - Trade count: `3080` vs `3080` (delta `0`)
  - Total P/L: `₹1,608,100.94` vs `₹1,608,100.94` (delta `0.00`)
  - Total return: `160.81%` vs `160.81%` (delta `0.00%`)

4. Live/runtime fixes applied
- Reduced live log volume:
  - `PARITY_TRACE` now env-gated (`PIVOT_LIVE_PARITY_TRACE=1` to enable).
  - Setup parity DB cross-check now env-gated (`PIVOT_LIVE_SETUP_PARITY_CHECK=1` to enable).
  - Added compact bar heartbeat logs: `LIVE_BAR session=... bar_end=... closed=... active=...`.
- Strict setup parity enabled by default for live/replay:
  - `load_setup_row()` fallback from live candles is disabled in replay and disabled in live by default.
  - Missing same-day `market_day_state` rows are tolerated and skipped so small gaps do not
    force a rebuild.
  - Historical note: early drafts allowed candle-derived recovery for a late start via
    `--allow-late-start-fallback`. The current real-order operator flow uses true opening-range
    catch-up from Kite historical `5minute` candles and refuses diagnostic OR-proxy setup rows.

5. CPR-only policy enforced in paper workflows
- `pivot-paper-trading` strategy and preset surface now enforce CPR-only execution.
- Walk-forward matrix pruned to canonical CPR LONG+SHORT only.
- FBR-specific paper CLI flags/help text removed from active workflow surface.

6. RVOL fallback display audit completed
- `db/backtest_db.py` fallback metadata path now infers:
  - `skip_rvol_check` from labels containing `rvoloff`
  - numeric RVOL value from labels containing `rvol<value>`
- Zero-trade PAPER fallback parsing now keeps skip-RVOL context consistent.

7. 2026-04-02 CPR SHORT parity root-cause fix completed
- Root cause A (extra trade): replay session that produced 8 trades used late-fallback setup loading and opened `MHLXMIRU` even though `market_day_state/strategy_day_state` row was missing for that date.
  - Current strict replay behavior (`allow_live_setup_fallback=False`) removes that leak.
- Root cause B (exit drift): `_advance_open_position()` rebuilt `TrailingStop` with `current_sl` as constructor SL.
  - After breakeven this collapsed `sl_distance` and triggered premature `TRAIL` phase promotion.
  - Fixed by reconstructing with immutable `initial_sl`, then overlaying `current_sl`/`phase`.
- Regression test added:
  - `tests/test_paper_runtime.py::test_advance_open_position_preserves_initial_sl_for_trail_transition`
- Validation replay after fix:
  - Session `CPR_LEVELS_SHORT-2026-04-02-RCA2` now archives `7` trades (not `8`)
  - Exit times/reasons align with backtest (HECPROJECT/KRITI/UNIVASTU now `BREAKEVEN_SL`; HARRMALAYA exit at `10:20`).
  - Remaining tiny PnL delta is sizing/cost rounding (`₹2890.74` replay vs `₹2892.44` backtest).

8. Dead-path cleanup in paper execution flow
- Removed unreachable FBR paper execution branches from:
  - `engine/paper_runtime.py` (entry-window/FBR entry scanner path)
  - `scripts/paper_live.py` (FBR-specific entry-window override branch)
  - `scripts/paper_prepare.py` (legacy non-CPR prefilter branch)
- Paper runtime now explicitly rejects non-CPR strategy values with:
  - `reason=unsupported_strategy:<strategy>`
- Added regression coverage for this contract in:
  - `tests/test_paper_runtime.py::test_process_closed_candle_rejects_non_cpr_strategy`

9. Phase 5 orchestrator rollout completed
- Added `engine/bar_orchestrator.py`:
  - `SessionPositionTracker`
  - `should_process_symbol()`
  - `select_entries_for_bar()`
  - `check_bar_risk_controls()`
- `engine/paper_runtime.py` split:
  - `evaluate_candle()` for per-bar decisioning
  - `execute_entry()` for selected-entry execution
  - `process_closed_candle()` retained as compatibility wrapper
- `scripts/paper_replay.py` now runs canonical-time bar orchestration with:
  - exits-first, then entries
  - alphabetical tie-break
  - tracker-driven pruning
  - per-bar risk checks
- `scripts/paper_live.py` now batches by `bar_end` with the same ordering model and tracker-driven Stage C pruning.
- Added unit coverage:
  - `tests/test_bar_orchestrator.py`
- Validation:
  - `uv run pytest tests/ -q` → `383 passed`
  - `uv run mypy engine/ db/ web/ agent/ --no-error-summary` → passed

10. CPR 4-baseline extension to include 2026-04-09
- New canonical preset-based runs (`2025-01-01` → `2026-04-09`):
  - `1993f32beea7` — `CPR_LEVELS_STANDARD_LONG` (legacy: `cpr-levels-long-rvol1-atr0.5`)
  - `6d1afd502546` — `CPR_LEVELS_STANDARD_SHORT` (legacy: `cpr-levels-short-rvoloff-atr0.5`)
  - `89b702775d22` — `CPR_LEVELS_RISK_LONG` (legacy: `cpr-levels-long-risksize-rvol1-atr0.5`)
  - `e8a7de2d3258` — `CPR_LEVELS_RISK_SHORT` (legacy: `cpr-levels-short-risksize-rvoloff-atr0.5`)
- Delta vs prior 2026-04-08 references:
  - LONG standard: `4419e4acee83` → `1993f32beea7`
    - trades `3080 → 3088` (`+8`)
    - P/L `₹1,608,100.94 → ₹1,604,269.05` (`-₹3,831.89`)
    - return `160.81% → 160.43%` (`-0.38%`)
  - SHORT standard: `52f449e804b0` → `6d1afd502546`
    - trades `4400 → 4404` (`+4`)
    - P/L `₹2,460,259.73 → ₹2,473,820.14` (`+₹13,560.41`)
    - return `246.03% → 247.38%` (`+1.35%`)
  - LONG risk: `a21bebd90f1f` → `89b702775d22`
    - trades `2950 → 2954` (`+4`)
    - P/L `₹2,466,787.43 → ₹2,467,690.74` (`+₹903.31`)
    - return `246.68% → 246.77%` (`+0.09%`)
  - SHORT risk: `18bea7db2f40` → `e8a7de2d3258`
    - trades `4117 → 4123` (`+6`)
    - P/L `₹2,638,308.68 → ₹2,648,801.00` (`+₹10,492.32`)
    - return `263.83% → 264.88%` (`+1.05%`)

11. Phase 6 — Daily capital reset implemented and baselines rerun
- Added `compound_equity: bool = False` to `StrategyConfig` (default: reset to ₹1M daily).
- Added `--compound-equity` CLI flag to restore old carry-forward behavior.
- Paper engine unchanged — already uses ₹1M per session.
- 4 compound baselines verified byte-identical to pre-change runs (zero regression).
- 4 daily-reset baselines established as new canonical reference.
- All 8 baseline runs retained in `backtest.duckdb`; all other runs cleaned up.

### Findings

1. Late-start behavior gap (observed)
- Midday live run (after market open) showed startup warnings:
  - `Setup prefetch missing rows ...`
  - repeated `setup_source=unknown` in old verbose parity trace mode.
- Root cause: prefetch happens before first closed bar and without same-day materialized OR context.

2. Logging gap (observed)
- Per-symbol `PARITY_TRACE` at scale flooded logs and obscured trade lifecycle events.
- Operational requirement is bar-ordered heartbeat + trade open/close logs, not full per-symbol dumps.

3. Top-25 websocket smoke (post-fix)
- Session ran with websocket connected and compact `LIVE_BAR` logs.
- No trade opened before entry-window close in that short pre-close test window.
- Final status for both variants: `NO_TRADES_ENTRY_WINDOW_CLOSED`.

4. Replay-vs-backtest short-session mismatch (resolved)
- The previously reported archived mismatch (`CPR_LEVELS_SHORT-2026-04-02`, 8 trades) is now reproducibly resolved with current code.
- Fresh replay (`CPR_LEVELS_SHORT-2026-04-02-RCA2`) produces the canonical 7-trade set for that date.

### Implemented Differently From Original Plan

1. Parity logging policy adjusted
- Original note implied persistent parity assertion logging.
- Implemented as opt-in diagnostic mode via environment flags, with compact default logs.

2. Late-start policy finalized
- Previous temporary continuity fallback was kept to unblock intraday testing.
- Final operational default is now strict for invalid setup rows, while missing same-day
  `market_day_state` rows are skipped instead of forcing another build.
- Historical note: late-start fallback was once CLI-driven (`--allow-late-start-fallback`).
  The current real-order operator flow no longer relies on that path; it uses true Kite historical
  catch-up for unresolved OR rows and blocks entries that still cannot prove true OR/direction.

### Pending / Open (Post Phase 0–5)

1. Operational hardening only
- Add explicit operator alert dispatch for repeated market-data/API failure paths in live.
- Keep parity-monitor replay checks in CI for selected known dates as a regression guard.
