# Live Execution Safety Plan

Current actual-live broker lifecycle documentation lives in
[REAL_ORDER_LIVE_FLOW.md](REAL_ORDER_LIVE_FLOW.md). This file is the historical safety rollout
plan; use it for design context, not as the current operator command source.

Scope: build and validate the remaining real-trading safety controls in paper mode first. Real Zerodha order placement stays disabled until the paper safety layer has passed live-session tests.

## Readiness Position

Paper trading has run for multiple weeks and many parity/runtime issues have been fixed. That is enough to start execution-safety work, but not enough to send real orders yet. The next phase is to make paper mode behave like a broker-executed system: queued order intents, throttled dispatch, idempotent retries, reconciliation, and explicit kill switches.

## Status Snapshot - 2026-04-28

Implemented and validated in paper mode:

- Paper order governor defaults to 8 orders/sec.
- Paper order idempotency keys dedupe retries.
- Manual/operator flatten uses latest live LTP/local-feed mark when available.
- `send-command` queues active live-loop actions without competing for the DuckDB writer lock.
- `flatten-both` queues close-all for both LONG and SHORT sessions for a date.
- `set_risk_budget` updates future-entry budget/caps for one running session.
- `pause_entries` disables future entries while keeping open-position exit monitoring active.
- `resume_entries` restores the original session universe for future entries.
- `cancel_pending_intents` removes unprocessed admin command files for that session.
- `reconcile --strict` checks order/position/session invariants.
- Live loop reconciles after each bar and after operator close/flatten commands.
- Critical reconciliation findings after partial closes disable new entries while preserving exit monitoring.
- Critical reconciliation findings after full-session flatten fail the session closed instead of marking it clean.
- Dashboard `/paper_ledger` queues `close_positions`, `close_all`, `set_risk_budget`, `pause_entries`, `resume_entries`, `cancel_pending_intents`, and LONG+SHORT flatten through the same live-loop admin command queue.
- Dashboard `/paper_ledger` refreshes the paper replica in-place every 3 seconds by default and shows the latest refresh timestamp.
- `BrokerAdapter` protocol and `ZerodhaBrokerAdapter(mode="REAL_DRY_RUN")` generate Zerodha order payloads without calling `place_order`.
- `real-dry-run-order` records generated Zerodha payloads in `paper_orders.broker_payload` with `broker_mode=REAL_DRY_RUN`.
- Read-only broker snapshot fetchers map Zerodha `orders()` / `positions()` responses into normalized reconciliation snapshots.
- `broker-reconcile --strict` compares local paper orders/positions against supplied broker snapshots.
- `pilot-check --strict` validates minimal real-pilot scope without enabling real orders.

Still pending:

- Real Zerodha order placement remains disabled.
- A controlled real small-size pilot still requires explicit operator approval after live dry-run drills.

## Phase 1 - Paper Execution Gateway

Goal: every paper order goes through the same shape of gateway that a real broker adapter will later use.

- Status: implemented.
- Add a process-wide order governor with a conservative default cap of 8 orders/sec.
- Add idempotency keys to `paper_orders` so retry paths do not duplicate fills.
- Mark order roles in notes/idempotency keys: entry, exit, partial exit, manual close, session flatten, emergency flatten.
- Keep real broker execution absent. This phase only changes paper behavior and observability.

Validation:
- Unit-test token bucket behavior without sleeping.
- Unit-test duplicate idempotency keys return the same paper order id.
- Unit-test burst flatten requests are serialized through the governor.

## Phase 2 - Operator Control Plane

Goal: make every operator action available from CLI, dashboard, and agent command path.

- Status: CLI, agent, and dashboard queue paths implemented for paper live sessions.
- Dashboard paper ledger tabs were updated so Archived and Daily Summary panels refresh from live data and no longer depend on a single preloaded panel lifecycle.
- Close one symbol in one session: `send-command --action close_positions`.
- Close all positions in one session: `send-command --action close_all`.
- Close both LONG and SHORT sessions for a trade date: `flatten-both`.
- Reduce future-entry budget/caps in one running session: `send-command --action set_risk_budget`.
- Pause entries while allowing exits: `send-command --action pause_entries`.
- Resume entries after automatic disable/manual pause: `send-command --action resume_entries`.
- Cancel unprocessed admin command files for one session: `send-command --action cancel_pending_intents`.
- Dashboard `/paper_ledger` exposes selected-session close, selected-session flatten, pause/resume entries, cancel pending intents, risk-budget update, reconcile, and page-level LONG+SHORT flatten.
- Dashboard `/paper_ledger` validation now includes tab-switch behavior: active, archived, and daily summary should render immediately after switching while active sessions keep 3-second near-real-time refresh.
- Emergency kill switch: implemented for paper through idempotent `flatten-both` plus `cancel_pending_intents`.

Validation:
- Run `daily-live --feed-source local` and issue close-one / close-session / flatten-both commands.
- During live paper, close one active position and confirm the session continues.
- During live paper, flatten LONG only and confirm SHORT continues.
- During live paper, flatten both sessions and confirm both archive with EOD alerts.
- During live paper, pause entries and confirm open positions continue to exit but no new positions open.
- During live paper, resume entries and confirm eligible new entries can open again.
- Queue multiple admin commands, run `cancel_pending_intents`, and confirm only already-consumed commands execute.

## Phase 3 - Reconciliation

Goal: continuously compare intended execution state with observed state.

- Paper reconciliation compares `paper_positions` vs `paper_orders`.
- Flag impossible states: open position without entry order, closed position without exit order, overfilled quantity, duplicate exit, session status inconsistent with open positions.
- Status: implemented for paper state.
- Add `reconcile` CLI command with JSON output and non-zero exit on critical mismatches.
- In live loop, if reconciliation fails critically after partial close/bar processing, disable new
  entries and allow exits only.
- If reconciliation fails critically after full-session flatten, fail the session closed instead of
  marking it cleanly completed.

Validation:
- Inject synthetic bad rows in a temp DB and verify critical findings.
- Run reconciliation every bar during local-feed paper.
- Run reconciliation every bar during Kite-live paper.
- Validate `set_risk_budget`: existing positions remain open; future entries use the reduced budget.
- Example: if SHORT starts with ₹10L and 10 slots, then the operator cuts it to ₹5L and 5 slots,
  current SHORT positions continue under normal exits. After those close, the session can use at
  most the reduced budget/slot cap for new entries.

## Phase 4 - Real Broker Adapter, Still Dry-Run

Goal: prepare real execution without placing orders.

- Status: dry-run payload generation implemented; real placement still disabled.
- Added `BrokerAdapter` protocol.
- Added `PaperBrokerAdapter`.
- Added `ZerodhaBrokerAdapter(mode="REAL_DRY_RUN")`.
- Added `REAL_DRY_RUN` mode that builds Zerodha order payloads, sends them through the governor, records what would be placed, but does not call `place_order`.
- Added `pivot-paper-trading real-dry-run-order` for explicit one-order payload generation tests.

Validation:
- Unit-test generated Zerodha payloads for product, exchange, side, quantity, order type, variety, and tag.
- Unit-test `REAL_DRY_RUN` does not call a provided Kite client's `place_order`.
- Unit-test non-dry-run Zerodha mode raises `RealOrderPlacementDisabledError`.
- Unit-test dry-run payload recording and idempotent DB dedupe.

## Phase 5 - Broker Reconciliation and Real Small-Size Pilot

Goal: before scaling, verify broker state is the source of truth.

- Status: reconciliation scaffolding and pilot guardrails implemented; real placement still disabled.
- Read-only `ZerodhaBrokerAdapter.fetch_order_snapshots()` maps Kite `orders()` into normalized broker order snapshots.
- Read-only `ZerodhaBrokerAdapter.fetch_position_snapshots()` maps Kite `positions()` into normalized broker position snapshots.
- `broker-reconcile` compares local paper state against supplied broker order/position snapshots.
- Critical mismatch examples: missing broker order, side/symbol/quantity mismatch, missing broker position, untracked broker position.
- `pilot-check` enforces minimal scope before any future pilot: max 2 symbols, quantity 1, max ₹10,000 notional, MIS + LIMIT only, and explicit acknowledgement.
- Real order placement remains impossible in this phase; pilot guardrails report readiness only and return `real_orders_enabled=false`.

Validation:
- Unit-test broker/local match.
- Unit-test untracked broker position is critical.
- Unit-test missing broker order is critical.
- Unit-test read-only Kite snapshot fetchers do not call `place_order`.
- Unit-test pilot guardrail failure and minimal-scope pass.

## This Week Test Schedule

- Day 1: paper execution gateway, idempotency, governor unit tests.
- Day 2: control-plane CLI/agent commands and local-feed paper tests.
- Day 3: reconciliation command and local-feed fault injection.
- Day 4: Kite-live paper with close-one, close-session, and flatten-all drills.
- Day 5: REAL_DRY_RUN payload generation only, no order placement.

### Test Gap Notes (pending from this round)

- Add negative-path coverage for dashboard interactions that currently rely on runtime UI state:
  - tab switch handlers, empty-state transitions, and stale container handling for `/paper_ledger`.
- Add regression coverage for command/refresh sequencing:
  - `send-command` during locked live writer windows.
  - concurrent archive/ledger fetch paths for same `run_id`.
- Add one end-to-end operator drill in paper:
  - issue `close_positions` for one symbol, then verify remaining position monitoring stays active and new entries can be paused/resumed during the same run.

### Test Items Completed (2026-04-28)

- Added dashboard tab event parser regression coverage for NiceGUI model-value payload shapes.
- Added schema-tolerant legacy backtest row ordering coverage so `get_backtest_trades()` no longer
  assumes `entry_time`/`exit_time`.
- Added additional operator-control replay/unit coverage for:
  - backtest reconciliation and dry-run archive workflows
  - replay orchestration ordering guards
  - paper archive exit-reason normalization and legacy trade-list handling

## Hard Rules

- Default order cap is 8 orders/sec, not 10.
- Exits and flatten intents have priority over entries.
- Real order placement remains impossible by default.
- Any critical reconciliation mismatch disables entries or fails the full-session flatten closed.
- Emergency flatten must be idempotent and safe to retry.
- Risk-budget reductions apply to future entries only. Existing exposure changes only through
  explicit close/flatten commands.
