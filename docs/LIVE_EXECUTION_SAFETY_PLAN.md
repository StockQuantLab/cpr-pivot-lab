# Live Execution Safety Plan

Scope: build and validate the remaining real-trading safety controls in paper mode first. Real Zerodha order placement stays disabled until the paper safety layer has passed live-session tests.

## Readiness Position

Paper trading has run for multiple weeks and many parity/runtime issues have been fixed. That is enough to start execution-safety work, but not enough to send real orders yet. The next phase is to make paper mode behave like a broker-executed system: queued order intents, throttled dispatch, idempotent retries, reconciliation, and explicit kill switches.

## Status Snapshot - 2026-04-27

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

Still pending:

- Real Zerodha order placement remains disabled and out of scope until dry-run validation passes.

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
- Close one symbol in one session: `send-command --action close_positions`.
- Close all positions in one session: `send-command --action close_all`.
- Close both LONG and SHORT sessions for a trade date: `flatten-both`.
- Reduce future-entry budget/caps in one running session: `send-command --action set_risk_budget`.
- Pause entries while allowing exits: `send-command --action pause_entries`.
- Resume entries after automatic disable/manual pause: `send-command --action resume_entries`.
- Cancel unprocessed admin command files for one session: `send-command --action cancel_pending_intents`.
- Dashboard `/paper_ledger` exposes selected-session close, selected-session flatten, pause/resume entries, cancel pending intents, risk-budget update, reconcile, and page-level LONG+SHORT flatten.
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

- Poll Zerodha orders/positions after every order burst.
- Reconcile local state against broker order status and broker positions.
- If broker/local mismatch is critical, disable entries and flatten or alert depending on severity.
- Start with 1-2 symbols and minimal size only after all dry-run checks pass.

## This Week Test Schedule

- Day 1: paper execution gateway, idempotency, governor unit tests.
- Day 2: control-plane CLI/agent commands and local-feed paper tests.
- Day 3: reconciliation command and local-feed fault injection.
- Day 4: Kite-live paper with close-one, close-session, and flatten-all drills.
- Day 5: REAL_DRY_RUN payload generation only, no order placement.

## Hard Rules

- Default order cap is 8 orders/sec, not 10.
- Exits and flatten intents have priority over entries.
- Real order placement remains impossible by default.
- Any critical reconciliation mismatch disables entries or fails the full-session flatten closed.
- Emergency flatten must be idempotent and safe to retry.
- Risk-budget reductions apply to future entries only. Existing exposure changes only through
  explicit close/flatten commands.
