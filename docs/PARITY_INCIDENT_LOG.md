# Parity Incident Log

This file records feed and parity incidents that need follow-up beyond a single
daily rerun.

## 2026-04-15 Direction-resolution race — SHORT died at 09:25 with 0 trades

### Summary

`CPR_LEVELS_SHORT-2026-04-15` completed with `NO_ACTIVE_SYMBOLS` at 09:25. Only
one session survived the morning: LONG ran through the full entry window but
ended with 0 trades. SHORT archived 5 minutes into the scan.

### Root cause

`strategy_day_state` for 2026-04-15 had `direction_5 = 'NONE'` for **all 2095
symbols** — pre-market state build did not populate directions because the
morning refresh (run by Claude Sonnet) only did data refresh, not state
rebuild.

Runtime direction resolution in `engine/paper_runtime.py:818-833` falls back to
the live 9:15 candle when `direction == "NONE" and live_candles`. But at
session prefetch time (09:20:29-30, after three crashes and restarts), Kite
WebSocket ticks were still sparse:

- 5 symbols had enough ticks to resolve → all closed above TC → `direction=LONG`
- 0 symbols resolved to `direction=SHORT`
- 592 symbols stayed at `direction="NONE"`

### Behavior at the 09:25 bar

`apply_stage_b_direction_filter` (`engine/paper_session_driver.py:44-86`) keeps
symbols whose direction is *not* in `{LONG, SHORT}` (the "will be pruned later"
branch). Step 5 `should_process_symbol` then drops every symbol with
`status == "rejected"` (direction ∉ `{LONG, SHORT}`).

| Variant | Stage B keeps      | Step 5 prunes (NONE=rejected) | Result |
|---------|--------------------|-------------------------------|--------|
| LONG    | 5 LONG + 592 NONE  | drop 592 NONE                 | 5 active |
| SHORT   | 0 SHORT + 592 NONE | drop 592 NONE                 | 0 → `NO_ACTIVE_SYMBOLS` |

LONG survived by coincidence: all 5 early-tick resolutions happened to be LONG.

### Follow-up actions

1. **Direction-readiness gate**: delay Stage B until direction coverage ≥80%
   or until bar 09:25, whichever comes first. Retry setup prefetch if coverage
   is low.
2. **EOD + next-day pipeline**: guarantee `strategy_day_state` for
   `trade_date+1` is built with pre-market ATR every EOD, not left to
   morning-of refresh.
3. **Kite WebSocket auto-reconnect**: `_on_close(None, None)` currently just
   clears `_connected` silently. Add explicit reconnect with backoff,
   per-minute health log, and tick-coverage alerts.
4. **Start session at 09:16, not 09:10**: current 09:10 start causes two
   WebSocket cycle events (pre-market close at 09:10, regular open at 09:15).
   Starting at 09:16 eliminates the cycle and ensures the 9:15 candle is
   already closed (so direction resolution has full tick data).
5. **DQ readiness gate**: extend `pivot-data-quality --date today` to verify
   `strategy_day_state` direction coverage, not just table presence.

### Related files

- `engine/paper_runtime.py:818-833` — direction fallback chain
- `engine/paper_session_driver.py:44-86` — Stage B filter (NONE pass-through)
- `engine/paper_session_driver.py:250-263` — Step 5 prune (NONE → rejected)
- `engine/bar_orchestrator.py:211-229` — `should_process_symbol`
- `engine/kite_ticker_adapter.py:328-330` — silent `_on_close`
- `scripts/paper_live.py:922-940` — `NO_ACTIVE_SYMBOLS` exit path
- `.tmp_logs/live_2026-04-15.log` — full session log

## 2026-04-13 MANOMAY feed divergence

### Summary

`MANOMAY` short on `2026-04-13` exposed a live-feed fidelity gap:

- live session alert: `221.49 -> 216.31`, `TARGET`, `+₹2,253`
- archived replay/local-backtest row after cleanup: `220.90 -> 220.90`, `BREAKEVEN_SL`, `-₹83.49`

The symbol qualified in both paths. The divergence is not a CPR filter issue.
It is a feed/fill-source issue.

### What was observed

- `daily-live` on the historical live log opened `MANOMAY` at `221.49`
- the local/replay candle for the same `09:25` bucket was `220.90`
- the current `intraday_day_pack` for `2026-04-13` has:
  - `09:25` close `220.90`
  - `09:30` low `214.51`
  - `09:35` close `217.51`
- the current paper replay/local-live runs therefore reproduce the pack candle, not the original live fill

### Root cause hypothesis

Replay/local-live are candle-based, while the live path is tick/quote-based:

- live websocket path builds 5-minute candles from real ticks/quotes
- local-live and replay emit 5-minute candles directly from `intraday_day_pack`
- the current historical pack does not preserve the same live fill snapshot that produced the original alert

That means the same symbol can qualify in both modes and still produce a different
entry price, quantity, or exit outcome.

### Why this matters

This is more dangerous than a setup-row mismatch:

- strategy parity can be green while live fidelity is still wrong
- a candle-only historical replay cannot prove the broker quote path is identical
- a single-bar fill drift is enough to flip a target win into a breakeven or loss

### Follow-up needed

Implemented:

- `paper_feed_audit` now stores one compact row per `session_id + symbol + 5-minute bucket`
- `pivot-paper-trading feed-audit --trade-date YYYY-MM-DD --feed-source kite` compares the
  stored live bars against the EOD-built `intraday_day_pack`

2026-04-15 audit note:

- The compare contract is feed-source aware.
- Kite live rows are keyed by `bar_start` for the audit join.
- Replay/local rows remain keyed by `bar_end`.
- After fixing the join, the 2026-04-15 Kite audit still shows large real drift
  versus `intraday_day_pack`, mostly in volume and with some OHLC mismatches.

Pending analysis for the next session:

- verify whether the websocket path is falling back to receive-time when
  `exchange_timestamp` is missing or late
- confirm whether live volume is being reconstructed from cumulative
  `volume_traded` correctly
- decide whether feed-audit should stay a diagnostic tool with expected OHLC
  drift tolerance or whether the live candle builder needs a stronger timestamp
  contract before we trust live-vs-pack parity

The audit record keeps the same minimum fields that were proposed below.

Recommended minimum fields:

- `session_id`
- `trade_date`
- `symbol`
- `bar_start`
- `bar_end`
- `open`, `high`, `low`, `close`
- `volume`
- `first_snapshot_ts`
- `last_snapshot_ts`
- `source` / transport mode

For deeper debugging, keep raw tick capture only for traded symbols or on-demand
incident replay. The daily default should be the compact per-bar audit table.
