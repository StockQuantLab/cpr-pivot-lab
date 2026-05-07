# Actual Live Broker Order Flow

This page explains how actual live trading works when CPR Pivot Lab routes a live
strategy signal to Zerodha/Kite broker orders. It is written for an operator or trader,
not for code debugging.

Use this with [PAPER_TRADING_RUNBOOK.md](PAPER_TRADING_RUNBOOK.md). The runbook tells
you which command to run. This page explains what the system is doing after the
command starts.

## One-Line Summary

The strategy still decides trades the same way as paper live. The only difference in
actual live mode is that every selected entry or exit must be confirmed by the broker
before the local paper session treats the position as open or closed.

```text
paper live:
  strategy signal -> local paper fill -> local stop/target monitoring

simulated real orders:
  strategy signal -> Zerodha-shaped dry-run order -> simulated broker fill -> local state

actual live orders:
  strategy signal -> Kite order -> broker fill confirmation -> broker stop-loss order
  -> local state follows broker reality
```

## Modes

| Mode | Places Kite Orders? | Purpose |
|------|---------------------|---------|
| Normal paper live | No | Daily paper trading, alerts, dashboard, no broker order payloads |
| `--simulate-real-orders` | No | Paper live with the same broker-intent lifecycle as actual live, using `REAL_DRY_RUN` |
| Manual `real-order` | Yes | One-off controlled broker test, such as 1 share ITC buy/sell |
| `daily-live --real-orders` | Yes | Strategy-routed actual live pilot for one LONG or one SHORT session |

Important current limitation: `--multi --real-orders` is intentionally blocked for the pilot.
Run one real-routed session first, either LONG or SHORT. Normal `--multi` paper live is still
supported. Paper/live multi sessions share an account-level symbol guard: once one direction
opens or reserves a symbol, the sibling direction cannot open that same symbol for the rest of
the trading day.

## Daily Preconditions

Before any actual live order is allowed:

- Kite access token is refreshed for the day.
- Today's outbound public IP is whitelisted in the Kite developer console, or verified against
  the expected IP in `real-readiness`.
- Doppler real-order gates are enabled.
- Account cash check passes unless explicitly skipped.
- Real-order max quantity and max notional guardrails are high enough for the intended pilot.
- Live data readiness passes.
- There is no duplicate live process for the same session id.

Read-only readiness command:

```bash
doppler run -- uv run pivot-paper-trading real-readiness \
  --symbol ITC \
  --quantity 1 \
  --expected-ip <WHITELISTED_PUBLIC_IP> \
  --strict
```

Kite does not provide an API to read your app IP whitelist. The readiness command can only
compare the machine's current public IP against the IP you say is whitelisted.

## What Decides A Trade

The strategy decision is shared across backtest, replay, paper live, simulated-real paper,
and actual live:

1. Load the same CPR_LEVELS preset.
2. Use the saved daily universe.
3. Resolve the opening range and direction.
4. Scan each symbol on closed 5-minute bars.
5. Rank candidates by the shared quality selector.
6. Select entries only if risk, slots, cash, filters, and the account-level symbol guard allow.

Actual live does not use a different entry rule. It only changes the execution step after a
candidate has already been selected.

The account-level symbol guard is important for LONG+SHORT operation. Paper ledgers can show
separate LONG and SHORT sessions, but Zerodha has one account-level position per symbol. If LONG
and SHORT both traded SBIN at the same time, the broker would net or flip the position. The guard
therefore treats a symbol as used for the day after either direction opens it.

## LONG Example

Assume the strategy finds a LONG setup in SBIN:

```text
Signal:
  Buy SBIN around Rs. 800
  Stop loss Rs. 784
  Target Rs. 832
  Quantity 10
```

Actual live flow:

1. The system creates a durable entry intent before calling Kite.
2. It sends a BUY order to Kite.
3. It waits for Kite to confirm the BUY is completely filled.
4. If the BUY is rejected, cancelled, timed out, or partially filled, the local position is not
   opened automatically. Operator reconciliation is required.
5. After the BUY fill is confirmed, the system immediately places a broker-native SELL SL-M
   order for the same risk distance from the actual fill price.
6. Only after entry fill and stop-loss placement does the local paper session show the position
   as open.
7. The local stop, first target, and runner target are shifted from the actual fill price while
   preserving the original rupee distances.
8. During the day, every closed 5-minute bar checks target, stop, trailing stop, time exit,
   manual commands, and risk limits.

For a LONG, exits are SELL orders:

| Event | Broker Action |
|-------|---------------|
| Stop loss hits | Wait for the existing SELL SL-M order to fill |
| Target/time/manual exit | Cancel the pending SELL SL-M first, then place a protected SELL LIMIT |
| Emergency flatten | Place a protected SELL LIMIT using the latest live mark |

Protected LIMIT means the system does not send an unlimited market exit. For a LONG exit,
the sell limit is below the latest reference price by the configured exit slippage buffer.
Example: if SBIN reference is Rs. 800 and exit buffer is 2%, the SELL limit is around
Rs. 784. This is intended to be marketable while still bounding bad fills.

## SHORT Example

Assume the strategy finds a SHORT setup in TCS:

```text
Signal:
  Sell TCS around Rs. 3500
  Stop loss Rs. 3570
  Target Rs. 3360
  Quantity 2
```

Actual live flow:

1. The system creates a durable entry intent before calling Kite.
2. It sends a SELL order to Kite.
3. It waits for Kite to confirm the SELL is completely filled.
4. If the SELL is rejected, cancelled, timed out, or partially filled, the local position is not
   opened automatically. Operator reconciliation is required.
5. After the SELL fill is confirmed, the system immediately places a broker-native BUY SL-M
   order for the same risk distance from the actual fill price.
6. Only after entry fill and stop-loss placement does the local paper session show the position
   as open.
7. The local stop, first target, and runner target are shifted from the actual fill price while
   preserving the original rupee distances.
8. During the day, the live loop monitors the position on every closed 5-minute bar.

For a SHORT, exits are BUY orders:

| Event | Broker Action |
|-------|---------------|
| Stop loss hits | Wait for the existing BUY SL-M order to fill |
| Target/time/manual exit | Cancel the pending BUY SL-M first, then place a protected BUY LIMIT |
| Emergency flatten | Place a protected BUY LIMIT using the latest live mark |

For a SHORT exit, the protected BUY limit is above the latest reference price by the configured
exit slippage buffer. Example: if TCS reference is Rs. 3500 and exit buffer is 2%, the BUY
limit is around Rs. 3570.

## Entry Order Types

### LIMIT Entry

Default for actual live pilots.

For a LONG BUY, the system places a marketable LIMIT above the reference price:

```text
reference Rs. 800, entry buffer 0.5%
BUY LIMIT around Rs. 804
```

For a SHORT SELL, the system places a marketable LIMIT below the reference price:

```text
reference Rs. 3500, entry buffer 0.5%
SELL LIMIT around Rs. 3482.50
```

The order can fill at a better price than the limit. The limit is a worst acceptable price,
not the expected fill price.

### MARKET Entry

Allowed only if Doppler explicitly allows `MARKET` order type. Use this for very small
connectivity tests only. The normal pilot path should use LIMIT.

## Position Sizing

Two real-order sizing modes exist.

### Fixed Quantity

Use exactly `--real-order-fixed-qty` shares per trade.

Example:

```text
fixed qty = 1
SBIN entry = 800
order qty = 1 share
```

### Cash Budget

Use as many shares as fit inside `--real-order-cash-budget`.

Example:

```text
cash budget = Rs. 10,000
protected entry price = Rs. 804
floor(10000 / 804) = 12 shares
```

Doppler `CPR_ZERODHA_REAL_MAX_QTY` must still allow the computed quantity. If the computed
quantity is 12 but Doppler max quantity is 1, the safety gate rejects the order.

## What Happens In Failure Scenarios

| Scenario | What The System Does | Operator Meaning |
|----------|----------------------|------------------|
| Entry rejected/cancelled | Does not open local position | Check Kite order status and logs |
| Entry times out | Does not open local position | Broker state is uncertain; reconcile before retry |
| Entry confirms zero filled quantity | Does not reserve exposure or open local position | Treat as broker anomaly; sync and reconcile |
| Entry partially fills | Blocks automatic local open | Manual reconciliation required |
| Stop-loss order placement fails after entry fill | Attempts immediate protected flatten | If rollback fails, close manually in Kite |
| Target/time exit order fails to fill | Keeps exposure reserved and position not locally closed | Reconcile; do not assume the slot is free |
| Existing broker SL fills | Local close waits for broker SL fill confirmation | Broker is source of truth |
| Manual target exit while broker SL is pending | Cancels SL first, then places exit | Avoids double exit/reverse exposure |
| Duplicate retry after crash | Existing durable intent first tries exact orderbook recovery; if no unique match exists, it blocks blind duplicate order | Reconcile pending intent before retry |
| Feed stale during real flatten | Requires timestamped live mark; fails loud if no mark | Operator may need direct Kite intervention |
| Daily loss/drawdown breached | Blocks new entries and flattens through the live loop | Existing broker state must still reconcile |

## Why The Stop-Loss Is Not In The First Entry Order

Zerodha regular MIS equity orders do not behave like a single bracket order where entry,
target, and stop are safely attached in one atomic instruction for this flow. The current safe
design is:

1. Place entry.
2. Confirm actual filled quantity.
3. Recalculate the stop from the actual fill price using the same rupee risk distance.
4. Place broker-native SL-M for exactly that filled quantity.

This avoids placing a stop for a quantity that was never actually bought or sold. If the entry
does not fill, no stop-loss order should exist. If the entry fills only partly, the system should
not blindly protect the original strategy quantity.

OCO/GTT may be useful for some CNC-style holdings, but it is not the default safety mechanism for
this intraday MIS CPR pilot.

## How Paper Simulates The Same Path

`--simulate-real-orders` is the recommended bridge between paper live and actual live:

```bash
doppler run -- uv run pivot-paper-supervisor -- \
  --multi --strategy CPR_LEVELS --trade-date today \
  --simulate-real-orders
```

This does not place Kite orders. It does:

- create Zerodha-shaped entry and exit intents;
- apply idempotency keys;
- simulate broker fill confirmation;
- simulate protective SL order placement;
- record broker payloads and latency fields in `paper_orders`;
- keep the same strategy scan, ranking, risk, entry, and exit logic as paper live.

This is not a perfect broker test, but it catches most logic mistakes before real money is used.

## What To Watch During A Real Pilot

Dashboard:

- session broker chip should show `ZERODHA LIVE`;
- order rows should show `broker_mode=LIVE`;
- real order rows should show Kite exchange order ids;
- local position quantity should match confirmed broker filled quantity.

CLI checks:

```bash
doppler run -- uv run pivot-paper-trading status

doppler run -- uv run pivot-paper-trading broker-sync-orders \
  --session-id <SESSION_ID>

doppler run -- uv run pivot-paper-trading reconcile \
  --session-id <SESSION_ID> \
  --strict
```

Kite console:

- entry order status should be `COMPLETE`;
- protective SL-M order should be visible after entry fill;
- after target/time/manual exit, the protective SL should be cancelled;
- no unintended open position should remain after session exit.

## Current Pilot Guardrails

- Start with one real-routed session: LONG or SHORT, not `--multi --real-orders`.
- Start with one position.
- Use small capital first.
- Prefer LIMIT entries for the first strategy pilot.
- Do not enable partial scale-out for real routing.
- Do not resume a real-order session automatically after a crash; reconcile first.
- If broker state and local state disagree, broker state wins.

## Safe First Strategy Pilot Shape

Fixed one-share LONG canary:

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

Capital-budget one-position LONG canary:

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --strategy CPR_LEVELS \
  --preset CPR_LEVELS_RISK_LONG \
  --trade-date today \
  --real-orders \
  --real-order-sizing-mode cash-budget \
  --real-order-max-positions 1 \
  --real-order-cash-budget 10000 \
  --real-entry-order-type LIMIT \
  --real-entry-max-slippage-pct 0.5 \
  --real-exit-max-slippage-pct 2
```

For SHORT, use the SHORT preset:

```bash
--preset CPR_LEVELS_RISK_SHORT
```

## Operator Rule

If anything is unclear during actual live trading, do not retry the same order blindly.
First sync broker orders, reconcile, and compare with the Kite console.

```bash
doppler run -- uv run pivot-paper-trading broker-sync-orders --session-id <SESSION_ID>
doppler run -- uv run pivot-paper-trading reconcile --session-id <SESSION_ID> --strict
```
