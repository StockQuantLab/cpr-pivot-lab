# Parity Incident Log

This file records feed and parity incidents that need follow-up beyond a single
daily rerun.

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
