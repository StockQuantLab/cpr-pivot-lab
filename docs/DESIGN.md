# Design Notes — CPR Pivot Lab

> Design/history doc, not live operator guidance. The live runbook is
> `docs/PAPER_TRADING_RUNBOOK.md`.

> **Migration Note (Apr 2026)**: Paper trading state has migrated from PostgreSQL to DuckDB
> (`paper.duckdb`). Backtest results are now in `backtest.duckdb` (separated from `market.duckdb`).
> PostgreSQL is used only for agent sessions and walk-forward validation. The workstream
> descriptions below reflect the original plan; the actual implementation uses DuckDB for all
> mutable paper state via `db/paper_db.py` and `db/backtest_db.py`. A versioned replica system
> (`db/replica.py` + `db/replica_consumer.py`) handles Windows file-lock avoidance.
> Alert dispatch (`engine/alert_dispatcher.py`) provides best-effort Telegram/email notifications.

## Core Design

- **Market data layer**: DuckDB reads Parquet directly, with prebuilt runtime tables for campaign speed.
- **Execution layer**: strategy engine runs on materialized setup + intraday packs.
- **Storage layer**: one run cache (`run_id`) for deterministic re-runs.
- **Analysis layer**: NiceGUI dashboard and LLM agent expose trade-level and aggregate analytics.

## Shared Portfolio Model

All execution paths (including `CPR_LEVELS` and `FBR`) use a shared portfolio model:

- `portfolio_value` defines total capital base
- `max_positions` limits concurrent exposure
- `max_position_pct` caps per-position allocation

## Operational Invariants

1. At most one writer to DuckDB when rebuilding runtime tables.
2. Campaign runs are chunked and resumable.
3. `intraday_day_pack` is mandatory for any backtest run.
4. Run IDs must be deterministic; changing any input parameter creates a new run.

## Paper Trading Plan

### Current Status

Paper trading is implemented end-to-end for session state, replay/live execution, archival, and operator visibility.
Paper state has been migrated from PostgreSQL to DuckDB (`paper.duckdb`). The three-file DuckDB layout
(market / backtest / paper) eliminates the Docker dependency for daily paper flow.

Current operating path:

- Paper trading is the validation. Walk-forward fold testing is not used.
- Operators run 4 daily `daily-live` sessions (CPR_LEVELS LONG/SHORT, FBR LONG/SHORT).
- Historical backfill uses `daily-sim` (fast Polars engine) for past dates.
- Active sessions and archived results surface under `Paper Sessions` at `/paper_ledger`.
- `/backtest` and `Strategy Analysis` remain backtest-only views.
- The local agent currently supports inspection of paper sessions and archived ledgers, but not direct launch of walk-forward.

The repository includes both the live paper-trading runtime and older operator-facing surfaces:

- `web/pages/ops_pages.py` exposes `/scans` and `/paper_ledger`
- `web/pages/walk_forward.py` exposes `/walk_forward`
- PostgreSQL handles agent sessions and walk-forward validation (not paper state)
- `config/settings.py` already includes optional Kite credentials and Telegram/email alert settings
- `engine/alert_dispatcher.py` provides best-effort async alert delivery

The live/replay execution loop now drives active paper sessions through DuckDB (`paper.duckdb`),
while `/paper_ledger` serves as a dual-mode view for replay/live state and `/walk_forward` serves as the validation gate view.

### Design Goal

Add a paper-trading subsystem that reuses the strategy logic and portfolio controls already validated in backtests while keeping mutable intraday execution state in a dedicated DuckDB file (`paper.duckdb`).

The design preserves the current storage split:

- DuckDB + Parquet remain the source of truth for market history, runtime setup tables, and immutable archived trade results
- DuckDB `paper.duckdb` holds mutable operational state (sessions, active positions, orders, feed state, alerts)
- PostgreSQL holds agent sessions and walk-forward validation (not paper state)

### Why This Cannot Be Just a Thin Wrapper Around Backtests

The current backtest engine is optimized for deterministic replay from prebuilt day packs:

- `engine/cpr_atr_strategy.py` consumes `intraday_day_pack` arrays and simulates full sessions in one pass
- `db/duckdb.py` materializes one-row-per-day state in `market_day_state` and `strategy_day_state`
- `web/pages/ops_pages.py` reads saved runs after the fact

Paper trading has different requirements:

- mutable intraday state across market hours
- live or polled price ingestion
- 5-minute bar construction during the session
- crash recovery and stale-feed handling
- timestamp-accurate order and fill audit trails
- strict separation between active positions and archived history

Trying to write live mutable state directly into the current backtest tables would create avoidable coupling, increase DuckDB write contention, and make recovery logic brittle.

### Reusable Building Blocks

The paper-trading implementation should reuse these existing components instead of re-implementing them:

- `BacktestParams` in `engine/cpr_atr_strategy.py` as the shared strategy parameter contract
- `TrailingStop` and position-sizing helpers in `engine/cpr_atr_utils.py`
- `market_day_state` and `strategy_day_state` in `db/duckdb.py` as the morning setup read model
- `run_metadata`, `backtest_results`, and `run_metrics` in `db/duckdb.py` as the long-term archive and reporting layer
- the NiceGUI operator shell in `web/main.py`, `web/state.py`, and `web/pages/ops_pages.py`
- DuckDB paper-session state managed by `db/paper_db.py` (`PaperDB`) and `db/backtest_db.py` (`BacktestDB`)

### Remaining Gaps

The core paper-trading path now exists in code. The main remaining operator-facing gap is agent orchestration.

Current gap list:

1. Stale-feed recovery remains an operator-driven workflow.
2. UI regression coverage is still lighter than runtime/CLI coverage.
3. Walk-forward validation is available from CLI but is not part of the daily paper flow — paper trading IS the validation.

There is also an important correctness boundary: `scripts/signal_alert.py` is useful as an exploratory alerting script, but it is not yet an execution-grade signal pipeline. It should be refactored to reuse the same setup logic and read models as paper trading instead of being treated as the final order trigger path.

### Target Architecture

#### 1. Data Ownership

- DuckDB `market.duckdb`:
  - historical market data
  - CPR, ATR, runtime state, and day packs
- DuckDB `backtest.duckdb`:
  - backtest results
  - archived paper trades after a session closes (execution_mode='PAPER')
  - aggregated reporting across both backtest and paper modes
- DuckDB `paper.duckdb`:
  - paper sessions
  - active positions
  - order attempts and fills
  - feed state and recovery metadata
  - alert log (audit trail for all sent/failed alerts)
- PostgreSQL:
  - agent sessions and messages (Phidata storage)
  - walk-forward validation runs and folds

#### 2. Session Flow

1. Pre-open or market-open bootstrap
	- load today’s universe, CPR state, ATR, and setup metadata from DuckDB
	- create a paper session row in DuckDB `paper.duckdb`
2. Intraday data ingestion
	- subscribe to live quotes or poll broker data
	- build 5-minute candles in memory with durable checkpoints in DuckDB
3. Setup evaluation and signal generation
	- run shared setup checks using the same strategy parameters as backtests
	- persist candidate signals to DuckDB
4. Paper order entry
	- create an order/position row when entry conditions confirm
	- track expected entry price, filled price, quantity, stop, target, and slippage
5. Intraday position management
	- update stops and targets on each new 5-minute candle
	- enforce `max_positions`, `max_position_pct`, and daily risk rules
	- dispatch alerts (Telegram/email) on trade events — best-effort, fire-and-forget
6. Exit and archival
	- close active paper positions at stop, target, or time exit
	- archive closed trades to DuckDB `backtest.duckdb` with `execution_mode = ‘PAPER’`
7. Post-close analytics
	- refresh paper-session KPIs and run-level summaries for dashboard and agent use

#### 3. Strategy Logic Boundary

The most important code-architecture step is to extract strategy decision points from the current backtest loop into reusable functions:

- pre-open setup qualification
- entry confirmation on each 5-minute candle
- stop/target initialization
- stop-update progression
- exit classification

Backtests should continue to call these functions with historical day packs. Paper trading should call the same functions with incrementally built intraday candles.

### Recommended Schema Changes

#### DuckDB (paper.duckdb)

Paper trading mutable state tables:

- `paper_sessions`
  - session id, strategy, symbol set, status, start/end timestamps, risk guardrails, session PnL
- `paper_positions`
  - one row per active paper position, including session id, symbol, direction, entry timestamp, quantity, stop, target, last price, unrealized PnL, and recovery fields
- `paper_orders`
  - order lifecycle rows for submitted, acknowledged, filled, cancelled, rejected
- `paper_feed_state`
  - feed health tracking (OK/STALE/DISCONNECTED)
- `alert_log`
  - alert delivery audit trail (type, subject, channel, status, error_msg)

#### DuckDB (backtest.duckdb)

Extend archived trade/reporting tables so paper results can be analyzed with the same dashboard surfaces used for backtests:

- add `execution_mode` to `backtest_results` and `run_metadata`
- add `source_session_id` to archived trades
- add `entry_timestamp` and `exit_timestamp` for paper-trade precision
- keep `run_id` semantics deterministic for archived paper sessions as a session/run identifier, not as a pure parameter hash

#### PostgreSQL (agent + walk-forward only)

- `walk_forward_runs` and `walk_forward_folds` for walk-forward validation
- `agent_sessions` and `agent_messages` for Phidata agent storage
- `signals` for real-time signal tracking

### File-Level Implementation Plan

#### Phase 1. Extract Shared Strategy Logic

Primary files:

- `engine/cpr_atr_strategy.py`
- `engine/cpr_atr_utils.py`

Tasks:

- extract reusable setup and trade-management helpers from the backtest loop
- define a small live-compatible candle interface so both historical and live callers use the same decision code
- keep the vectorized backtest path intact where performance matters

Exit criteria:

- backtests still produce the same run outputs
- the paper engine can call shared entry/exit logic without importing the full replay loop

#### Phase 2. Add Operational Storage For Paper Sessions

Primary files:

- `db/init_pg.sql`
- `db/postgres.py`
- `config/settings.py`

Tasks:

- add `paper_trading_sessions`, `paper_positions`, and `paper_orders`
- add risk and mode configuration settings such as enable flag, max daily loss, market-close cutoff, and stale-feed timeout
- add repository-level data-access helpers for paper session reads and writes

Exit criteria:

- paper session state can be created, resumed, queried, and closed without touching DuckDB

#### Phase 3. Build The Paper Engine

Primary files:

- `scripts/paper_trading.py` (new)
- `scripts/signal_alert.py`
- `pyproject.toml`

Tasks:

- add a dedicated `pivot-paper-trading` CLI entry point
- build a session orchestrator that loads morning setup state from DuckDB and keeps mutable intraday state in DuckDB `paper.duckdb`
- either refactor `signal_alert.py` into reusable signal-generation helpers or move its useful checks into a shared module
- implement controlled session lifecycle commands: start, pause, resume, stop, flatten

Exit criteria:

- a single market session can run end to end in paper mode and archive trades after close

#### Phase 4. Add Market-Data Adapter And Candle Builder

Primary files:

- a new live-data module under `agent/` or `scripts/`
- `config/settings.py`

Tasks:

- implement Kite or equivalent broker adapter behind a narrow interface
- support reconnects, heartbeats, stale-data detection, and symbol subscription management
- aggregate ticks or quote snapshots into 5-minute bars usable by the shared strategy logic

Exit criteria:

- live paper session receives stable 5-minute bars through market hours
- stale feed or disconnect forces safe session pause or flatten policy

#### Phase 5. Extend Dashboard And Agent Surfaces

Primary files:

- `web/state.py`
- `web/pages/ops_pages.py`
- `web/main.py`
- `agent/tools/backtest_tools.py`
- `agent/llm_agent.py`

Tasks:

- convert `/paper_ledger` from historical-run-only to mode-aware paper session reporting
- add live sections for active positions, pending signals, realized/unrealized PnL, risk limits, and stale-feed state
- add agent tools to start a paper session, inspect active positions, and fetch paper-session ledgers

Exit criteria:

- dashboard operators and the local agent can inspect a paper session without direct database access

#### Phase 6. Verification And Rollout

Primary files:

- `tests/`
- operational docs

Tasks:

- add unit tests for shared decision helpers and candle-builder logic
- add integration tests for DuckDB paper-session persistence
- add replay tests that compare a historical day-pack replay against the paper engine fed candle by candle
- current operator priority is walk-forward validation first, then `daily-live`

Exit criteria:

- shadow-mode parity is acceptable versus archived historical behavior
- operational runbook is explicit about recovery and flatten procedures

### Detailed Execution Plan For GPT Codex

> **DEPRECATION NOTICE**: The workstreams below (WS0–WS9) describe the *original* implementation
> plan, which used PostgreSQL for mutable paper state. The actual implementation (Apr 2026) uses
> DuckDB for all paper state (`paper.duckdb`, `backtest.duckdb`) with a versioned replica system.
> PostgreSQL is retained only for agent sessions and walk-forward validation.
>
> For the current architecture, see:
> - `docs/superpowers/specs/2026-04-05-live-trading-performance-design.md`
> - `docs/DATABASE_ARCHITECTURE.md`
> - `docs/PAPER_TRADING_RUNBOOK.md`
>
> The workstreams are kept here for historical context and design rationale only.

This section breaks the work into implementation-sized batches that can be handed to GPT Codex one at a time.

The goal is to keep each batch independently reviewable and to avoid mixing strategy refactors, schema changes, live-feed plumbing, and UI work in a single large change.

#### Workstream 0. Ground Rules Before Any Code Changes

Objective:

- lock the architectural boundary so future strategy changes apply uniformly where intended

Rules:

- DuckDB remains archive and analytics storage, and is also used for mutable paper-session state (`paper.duckdb`)
- PostgreSQL is used only for agent sessions, walk-forward validation, and signals
- strategy decision logic must be extracted into shared functions before building the paper-session loop
- avoid adding broker-specific code directly inside `engine/cpr_atr_strategy.py`

Required review outcome:

- confirm that all new paper-trading features follow the storage and logic split above

#### Workstream 1. Extract Shared Strategy Decision Logic

Objective:

- make CPR and FBR entry and exit rules reusable by both backtest replay and paper trading

Files expected:

- `engine/cpr_atr_strategy.py`
- `engine/cpr_atr_utils.py`
- optionally a new shared strategy helper module under `engine/`

Implementation tasks:

1. identify the smallest reusable decision points inside the current backtest engine:
	- setup qualification
	- entry confirmation
	- initial stop and target calculation
	- trailing-stop updates
	- exit classification
2. extract those decision points into pure or near-pure helper functions
3. keep the current vectorized batch-replay path intact so backtest performance does not regress unnecessarily
4. define a minimal candle/state input shape that a live paper engine can also provide

Acceptance criteria:

- existing backtests still run with unchanged CLI behavior
- extracted helpers can be called without running the full backtest orchestration
- CPR_LEVELS and FBR still produce equivalent results versus pre-refactor baselines

Tests to require:

- unit tests for extracted helper functions
- regression tests or parity assertions for representative CPR_LEVELS and FBR cases

Suggested Codex task prompt:

"Refactor the strategy engine so CPR_LEVELS and FBR decision logic is reusable outside the backtest loop. Preserve current backtest behavior. Extract helper functions for setup qualification, entry confirmation, stop/target initialization, trailing updates, and exit classification. Add focused tests proving no behavior drift on representative scenarios."

##### Exact Files And Functions To Target First

Start with these existing functions and methods in this order:

1. `engine/cpr_atr_strategy.py`
	 - `CPRATRBacktest._simulate_day_cpr_levels`
	 - `CPRATRBacktest._simulate_day_fbr`
	 - `CPRATRBacktest._simulate_trade`
	 - `CPRATRBacktest._normalize_sl`
	 - `CPRATRBacktest._find_first_close_idx`
	 - `CPRATRBacktest._simulate_from_preloaded`
2. `engine/cpr_atr_utils.py`
	 - `TrailingStop`
	 - `validate_and_adjust_sl_distance`
	 - `calculate_gap_pct`
	 - `calculate_or_atr_ratio`
	 - `calculate_position_size`
	 - `check_failed_breakout`

Do not start by changing:

- `CPRATRBacktest.run`
- DuckDB query shape or runtime table generation
- CLI argument parsing in `engine/run_backtest.py`
- dashboard code

The first refactor should isolate decision logic, not change orchestration, storage, or UI.

##### Concrete Extraction Goal

Create shared strategy helpers that a future paper engine can call with incremental candle data.

The first extraction pass should produce functions along these lines:

- CPR setup and entry helpers
	- compute CPR entry scan start
	- build CPR trigger, stop, and target from setup row
	- validate CPR signal candle and optional confirmation rules
- FBR helpers
	- build breakout trigger from OR levels
	- detect failure and derive reversal entry, stop, and target
- shared trade helpers
	- normalize stop distance
	- simulate trade lifecycle from post-entry candles
	- classify exits and compute trade metrics

The exact function names can vary, but the separation should be explicit enough that a paper session loop can call them without depending on the whole `CPRATRBacktest` object.

##### Preferred Refactor Sequence

1. Leave `CPRATRBacktest.run` and `_simulate_from_preloaded` behavior unchanged.
2. Extract pure helper functions from `_simulate_trade` first because that is the most reusable common path.
3. Extract CPR-specific entry logic from `_simulate_day_cpr_levels`.
4. Extract FBR-specific failure and reversal logic from `_simulate_day_fbr`.
5. Keep `TrailingStop` where it is unless a very small wrapper is needed.
6. Only after helpers exist, rewire `_simulate_day_cpr_levels` and `_simulate_day_fbr` to call them.

##### Concrete Output Expected From Codex

Codex should aim to produce:

- one or more new shared helper functions or a small new module under `engine/`
- smaller `_simulate_day_cpr_levels` and `_simulate_day_fbr` methods that mostly map row data into shared helpers
- unchanged external behavior for `pivot-backtest`
- tests covering extracted logic directly, not only through full CLI runs

##### Specific Constraints To Give Codex

Tell Codex all of the following explicitly:

- preserve current CLI and public API behavior
- preserve backtest cache semantics and `run_id` generation
- do not change DuckDB schema in this workstream
- do not add live-feed or paper-trading code yet
- do not rewrite vectorized fetch paths unless required for extraction
- prefer small pure helpers over a large new class hierarchy
- keep CPR_LEVELS and FBR outputs behaviorally equivalent to current logic

##### Ready-To-Paste Codex Prompt

Use this prompt as the first implementation handoff:

```text
Workstream 1 only: extract shared strategy decision logic from the current backtest engine so future paper trading can reuse it.

Repository context:
- Main engine file: engine/cpr_atr_strategy.py
- Utility file: engine/cpr_atr_utils.py
- Do not touch dashboard, DuckDB schema, PostgreSQL schema, or paper-trading code in this task.

Primary targets in engine/cpr_atr_strategy.py:
- CPRATRBacktest._simulate_day_cpr_levels
- CPRATRBacktest._simulate_day_fbr
- CPRATRBacktest._simulate_trade
- CPRATRBacktest._normalize_sl
- CPRATRBacktest._find_first_close_idx
- CPRATRBacktest._simulate_from_preloaded

Reusable existing helpers in engine/cpr_atr_utils.py:
- TrailingStop
- validate_and_adjust_sl_distance
- calculate_gap_pct
- calculate_or_atr_ratio
- calculate_position_size
- check_failed_breakout

Goal:
- Extract reusable helper functions for setup qualification, entry confirmation, stop/target initialization, trailing-stop driven trade lifecycle, and exit classification.
- Keep the current backtest orchestration intact.
- Keep CPR_LEVELS and FBR behavior equivalent to the current implementation.

Constraints:
- Preserve existing CLI behavior and external interfaces.
- Do not change run_id generation or cache semantics.
- Do not change DuckDB queries or schema.
- Do not add live feed, broker, or paper-trading code yet.
- Prefer small pure helper functions or a very small new helper module under engine/.
- Avoid introducing a large new class hierarchy.

Refactor order:
1. Extract common logic from _simulate_trade first.
2. Extract CPR-specific entry logic from _simulate_day_cpr_levels.
3. Extract FBR-specific breakout failure / reversal logic from _simulate_day_fbr.
4. Rewire the existing methods to call the extracted helpers.
5. Keep CPRATRBacktest.run and _simulate_from_preloaded behaviorally stable.

Tests required:
- Add focused unit tests for the extracted helpers.
- Add regression-style tests for representative CPR_LEVELS and FBR scenarios to prove no behavior drift.
- If needed, add one or two targeted integration tests at the engine level, but do not rely only on full CLI tests.

Deliverable:
- Minimal, reviewable refactor with shared strategy helpers now reusable by a future paper-trading engine.
- Include a short summary of what was extracted, what remained unchanged, and why.
```

##### Review Checklist For This PR

Use this checklist when reviewing Codex output:

- Are CPR and FBR decision rules still implemented in one place each?
- Is `_simulate_trade` materially smaller or more modular?
- Are extracted helpers usable without DuckDB or CLI code?
- Did the change avoid schema, dashboard, and paper-trading scope creep?
- Are there direct tests for helper behavior, not only end-to-end tests?
- Is the refactor small enough to understand in one review session?

### Daily Replay And Live Paper Implementation Plan

This section defines the concrete repository-specific plan for supporting:

1. historical replay from a chosen trade date such as March 10
2. next-day and ongoing daily live paper trading after EOD or pre-market data load
3. walk-forward replay over historical date ranges using the same paper runtime

#### Operating Assumption

New market data can be loaded either at EOD or before market open the next day.

That means the paper-trading workflow should use a preparation step that refreshes DuckDB runtime state for the requested trade date and symbol set before replay or live execution starts.

#### Workflow Split

The target one-click flows are:

1. `daily-prepare`
	- prepare runtime coverage for a given date and symbol set
2. `daily-replay`
	- one-click historical paper replay for a chosen date
3. `daily-live`
	- one-click live paper session after pre-market preparation
4. `walk-forward`
	- day-by-day replay across a date range, archiving each day independently

#### Why This Fits The Existing Code

The current paper runtime loads same-date setup rows from DuckDB through `load_setup_row(symbol, trade_date)` in `engine/paper_runtime.py`.

That boundary should remain intact.

The clean sequence is therefore:

1. load new market data
2. refresh same-day runtime tables in DuckDB
3. create or activate the paper session in DuckDB `paper.duckdb`
4. run replay or live paper execution through the shared paper runtime

This preserves the current architecture:

- DuckDB = historical and setup read model
- DuckDB `paper.duckdb` = mutable paper-session state
- paper runtime = shared execution logic
- replay/live = orchestration

#### Workstream A. Add Daily Preparation Service

Objective:

- make same-day runtime preparation explicit and reusable by both replay and live flows

Status:

- completed

Files:

- new: `scripts/paper_prepare.py`
- existing: `db/duckdb.py`

Functions to add in `scripts/paper_prepare.py`:

1. `resolve_trade_date(value: str | None) -> str`
2. `resolve_prepare_symbols(symbols: list[str] | None, symbols_csv: str | None) -> list[str]`
3. `validate_daily_runtime_coverage(*, trade_date: str, symbols: list[str]) -> dict[str, Any]`
4. `prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], force_refresh: bool = False, pack_since_date: str | None = None) -> dict[str, Any]`

Function to add in `db/duckdb.py`:

1. `get_runtime_trade_date_coverage(self, symbols: list[str], trade_date: str) -> dict[str, list[str]]`

Expected behavior:

- run symbol-scoped refreshes using existing builders:
  - `build_market_day_state(...)`
  - `build_strategy_day_state(...)`
  - `build_intraday_day_pack(...)`
- return symbol-level and requested-date-level readiness
- fail clearly if the requested trade date is still missing after refresh

Tests:

- new: `tests/test_paper_prepare.py`

#### Workstream B. Add Daily Commands To The Paper CLI

Objective:

- expose one-click daily operations through the existing paper CLI

Status:

- completed

File:

- `scripts/paper_trading.py`

New command handlers:

1. `_cmd_daily_prepare(args: argparse.Namespace) -> None`
2. `_cmd_daily_replay(args: argparse.Namespace) -> None`
3. `_cmd_daily_live(args: argparse.Namespace) -> None`
4. `_cmd_walk_forward(args: argparse.Namespace) -> None`

Helper functions to add:

1. `_parse_symbols_arg(value: str | None) -> list[str] | None`
2. `_default_session_id(prefix: str, trade_date: str, strategy: str) -> str`
3. `_ensure_daily_session(...) -> PaperSession`

Parser additions:

1. `daily-prepare`
	- `--trade-date`
	- `--symbols`
	- `--force-refresh`
2. `daily-replay`
	- `--trade-date`
	- `--symbols`
	- `--strategy`
	- `--strategy-params`
	- `--session-id`
	- `--leave-active`
	- `--notes`
3. `daily-live`
	- `--trade-date`
	- `--symbols`
	- `--strategy`
	- `--strategy-params`
	- `--session-id`
	- `--poll-interval-sec`
	- `--candle-interval-minutes`
	- `--notes`
4. `walk-forward`
	- `--start-date`
	- `--end-date`
	- `--symbols`
	- `--strategy`
	- `--strategy-params`
	- `--notes`

Expected behavior:

- `daily-prepare` only prepares and validates coverage
- `daily-replay` does prepare + session create/reuse + replay
- `daily-live` does prepare + session create/reuse + live run
- `walk-forward` loops replay day by day and archives each daily session

Tests:

- extend `tests/test_paper_trading_cli.py`

#### Workstream C. Add Walk-Forward Orchestration

Objective:

- support sequential replay across a date range using the same paper runtime used by daily replay/live

Status:

- completed

Files:

- start inside `scripts/paper_trading.py`
- optionally extract later to `scripts/paper_walk_forward.py`

Functions to add:

1. `iter_trade_dates(start_date: str, end_date: str) -> list[str]`
2. `run_walk_forward_replay(*, start_date: str, end_date: str, symbols: list[str], strategy: str, strategy_params: dict[str, Any], notes: str | None = None) -> dict[str, Any]`

Walk-forward v1 scope:

- replay only
- fixed strategy parameters across the full date range
- one archived paper session per day
- aggregate summary at the end
- no optimizer-style retuning in-loop

Tests:

- new: `tests/test_paper_walk_forward.py`

#### Workstream D. Document The Daily Operator Workflow

Status:

- completed

Files:

- `docs/PAPER_TRADING_RUNBOOK.md`
- this file

Document:

1. EOD or pre-market data load assumption
2. `daily-prepare`
3. `daily-replay`
4. `daily-live`
5. `walk-forward`
6. missing-coverage failure modes

#### Exact Build Order

1. Add `get_runtime_trade_date_coverage()` in `db/duckdb.py`
2. Add `scripts/paper_prepare.py`
3. Add `daily-prepare`
4. Add preparation tests and CLI parser tests
5. Add `daily-replay`
6. Add `daily-live`
7. Add `walk-forward`
8. Update operator docs
9. Run focused tests
10. Run full `uv run pytest`

#### Exact Acceptance Criteria

1. `daily-prepare --trade-date X` succeeds or fails with explicit missing coverage details
2. `daily-replay --trade-date X` runs with no manual preparation step required
3. `daily-live --trade-date today` runs with no manual session bootstrap step required
4. `walk-forward` archives one replay-driven paper session per day
5. replay and live both continue to use the same paper runtime and risk controls

#### Recommended First Slice

Start with:

1. `get_runtime_trade_date_coverage()`
2. `scripts/paper_prepare.py`
3. `daily-prepare`
4. `daily-replay`

That is the shortest path to validating historical dates such as March 10 before adding one-click live orchestration.

#### Workstream 2. Add Paper-Session Schema (DuckDB `paper.duckdb`)

Objective:

- create durable mutable storage for live paper-trading state

Status:

- completed

Files expected:

- `db/init_pg.sql`
- `db/postgres.py`
- `config/settings.py`

Implementation tasks:

1. add `paper_trading_sessions`
2. add `paper_positions`
3. add `paper_orders`
4. extend `signals` with linkage fields needed for session-aware signal handling
5. add settings for:
	- `paper_trading_enabled`
	- `paper_max_daily_loss_pct`
	- `paper_flatten_time`
	- `paper_stale_feed_timeout_sec`
	- optional `paper_default_symbols` or equivalent
6. add typed repository/helper functions in `db/postgres.py` for:
	- creating a session
	- listing active sessions
	- opening and closing positions
	- writing order events
	- updating signal state

Acceptance criteria:

- schema initializes cleanly on a new database
- one paper session can be created and updated without touching DuckDB
- open-position reads are fast and indexed by session and symbol

Tests to require:

- integration tests covering session creation, position lifecycle rows, and order-event persistence

Suggested Codex task prompt:

"Add PostgreSQL schema and access helpers for paper trading. Create session, position, and order tables; extend signal state for session-aware operation; add settings for paper-trading risk controls; and add tests validating the persistence lifecycle. Keep DuckDB untouched in this task."

Ready-to-paste Codex prompt (WS2):

```text
Workstream 2 only: introduce PostgreSQL paper-session state with minimal storage model for active operations.

Repository context:
- db/init_pg.sql
- db/postgres.py
- config/settings.py
- tests/* (existing style and helpers in this repo)

Objective:
- Add durable, recoverable paper session storage in PostgreSQL only.
- Keep DuckDB strictly read-only for live/active paper state.

Primary tables to add/update in db/init_pg.sql:
- paper_trading_sessions
- paper_positions
- paper_orders
- paper_feed_state (or equivalent heartbeat/state table if useful)
Also update/extend:
- signals (session-aware correlation fields, dedupe keys, state transitions)

Required columns baseline (suggested):
- paper_trading_sessions
  - session_id (PK), name/strategy/symbols, start/end_time, status, risk caps, created_by, created_at/updated_at
  - params JSON for full strategy params, operator notes, recovery markers
  - latest_candle_ts, stale_feed_at, flatten_time, daily_pnl_used, locks/flags
- paper_positions
  - position_id (PK), session_id (FK), symbol, direction, qty, entry_price, stop_loss, target, trail_state JSON, opened_at, updated_at, closed_at, state
- paper_orders
  - order_id (PK), session_id (FK), position_id (nullable FK), symbol, signal_id (nullable FK), side, order_type, requested_qty, request_price, fill_price, status, exchange_id, created_at
- signals (extend)
  - session_id (nullable FK) and external ids that identify the same decision path
  - source/strategy metadata to support dedupe and replay.

Code tasks in db/postgres.py:
1. add async repository helpers for:
   - create_session / get_active_sessions / get_session
   - update_session_state
   - open_position / update_position / close_position
   - append_order_event
   - set_signal_state + mark_signal_stale/consumed
2. keep helper functions small and transaction-safe, returning typed objects or dataclasses where practical.
3. add lightweight indexing by session_id and symbol for hot read/write paths.
4. do not add broker code or strategy logic here.

Config in config/settings.py:
- add toggles and caps:
  - paper_trading_enabled
  - paper_max_daily_loss_pct
  - paper_flatten_time
  - paper_stale_feed_timeout_sec
  - paper_default_symbols (optional, list/CSV)
  - paper_default_strategy

Safety constraints:
- Do not alter existing DuckDB schemas/queries.
- Do not add session orchestration/CLI flow here.
- Avoid breaking existing agent_sessions/agent_messages usage.
- Preserve backward compatibility if signals table already has live data from the current signal_alert path.

Acceptance criteria:
- `doppler run -- uv run scripts/build_tables.py` path should initialize new schema on clean PostgreSQL.
- one session lifecycle works: create -> ACTIVE -> flat -> COMPLETED without errors.
- queries by session and symbol are O(log n) from the model/index design on realistic volumes.
- existing signal_alert behavior is not functionally regressed.
```

Review checklist for WS2 PR:

- Are new tables created in a PostgreSQL-only migration path and guarded by existence checks?
- Are all mutable paper state tables (positions/orders/session) PostgreSQL-only?
- Is schema compatible with existing signal rows, and are extension fields backward-safe?
- Are helper functions transaction-safe and idempotent where recoverability is required?
- Are indexes present on session_id/symbol/time columns for the operational hot path?
- Are risks/config defaults validated and typed in settings?
- Are there tests for session creation, open/close position, and order append + state transition?


#### Workstream 3. Extend DuckDB Archive Model For Paper Trades

Objective:

- allow paper trades to reuse existing reporting and dashboard analytics after a session closes

Status:

- completed

Files expected:

- `db/backtest_db.py`
- possibly tests touching archived trade reads

Implementation tasks:

1. extend archived trade metadata with:
	- `execution_mode`
	- `source_session_id`
	- `entry_timestamp`
	- `exit_timestamp`
2. update run metadata to distinguish `backtest` vs `paper`
3. update metrics refresh logic so paper sessions can appear in archive reporting without breaking existing backtest metrics
4. ensure current dashboard queries remain correct for existing historical runs

Acceptance criteria:

- archived paper rows can coexist with existing backtest rows
- existing backtest dashboards continue to work
- paper-mode rows can be filtered without ambiguous joins or broken metrics

Tests to require:

- migration or schema-extension coverage
- metrics refresh coverage for mixed backtest and paper data

Suggested Codex task prompt:

"Extend DuckDB archived trade and run metadata tables so completed paper sessions can be stored and analyzed alongside backtests. Add execution mode and session linkage, preserve compatibility for existing backtests, and add tests for mixed-mode archive reads and metrics refresh."

#### Workstream 4. Build The Paper Session Orchestrator

Objective:

- add a dedicated CLI workflow that runs a paper session using shared strategy logic and DuckDB state (`paper.duckdb`)

Status:

- completed

Files expected:

- `scripts/paper_trading.py` new
- `pyproject.toml`
- possibly helper modules under `scripts/` or `engine/`
- `tests/test_paper_trading_workflow.py`

Implementation tasks:

1. add a `pivot-paper-trading` CLI entry point
2. support commands or flags for:
	- `start`
	- `pause`
	- `resume`
	- `stop`
	- `flatten`
	- `status`
3. session bootstrap should:
	- load selected symbols and strategy params
	- read morning setup state from DuckDB
	- create a DuckDB session row in `paper.duckdb`
4. intraday loop should:
	- consume candle events from a feed adapter
	- evaluate shared strategy helpers
	- create/update positions and order events in DuckDB `paper.duckdb`
5. close flow should:
	- flatten remaining positions
	- archive completed trades to DuckDB

Acceptance criteria:

- session lifecycle is explicit and recoverable
- no live mutable state is written to `market.duckdb` during session operation
- a completed session can be archived and later reported in analytics

Tests to require:

- session lifecycle tests with a fake feed
- archival tests proving completed sessions are moved into DuckDB reporting correctly

Suggested Codex task prompt:

"Create a new paper-trading CLI orchestrator with explicit session lifecycle commands. Use PostgreSQL for active session state and archive completed trades to DuckDB. Reuse shared strategy helpers from the engine refactor. Provide tests using a fake candle feed."

#### Workstream 5. Refactor Signal Generation Into A Reusable Module

Objective:

- prevent `scripts/signal_alert.py` from diverging from paper-trading logic

Status:

- completed

Files expected:

- `scripts/signal_alert.py`
- possibly a new shared signal module under `scripts/` or `engine/`
- `engine/signal_generation.py`
- `tests/test_signal_generation.py`

Implementation tasks:

1. separate alert formatting and transport from signal computation
2. extract reusable signal-generation logic that can be called by:
	- alert CLI
	- paper session orchestrator
	- future agent tools
3. ensure signal rows written to PostgreSQL are session-aware and deduplicated
4. retain current alert behavior where practical

Acceptance criteria:

- alert CLI still works
- paper orchestrator can call the same signal-generation code without importing email logic
- signal rows contain enough metadata to trace why they were generated

Tests to require:

- tests for signal generation and dedupe behavior

Suggested Codex task prompt:

"Refactor the existing signal alert script so signal computation is reusable independent of email delivery. Preserve current alert CLI behavior, but expose shared signal-generation helpers usable by the paper-trading orchestrator. Add tests for signal generation and deduplication."

#### Workstream 6. Add Live Feed Adapter And 5-Minute Candle Builder

Objective:

- provide the paper engine with a broker-agnostic stream of intraday candle events

Status:

- completed

Files expected:

- new live-data module under `scripts/` or `agent/`
- `config/settings.py`
- `scripts/live_market_data.py`
- `scripts/paper_live.py`
- `tests/test_live_market_data.py`

Implementation tasks:

1. define a narrow market-data adapter interface so paper trading is not hard-coded to Kite internals
2. implement a first adapter for Kite using existing credential settings
3. build a 5-minute candle aggregator that:
	- ingests ticks or quotes
	- emits closed bar events
	- preserves enough state for restart recovery if needed
4. add stale-feed detection and session-safe pause behavior
5. make the paper orchestrator consume the adapter interface, not the broker SDK directly

Acceptance criteria:

- live feed module can be replaced without rewriting strategy logic
- candle builder emits deterministic 5-minute bars from test fixtures
- stale feed prevents new entries and raises operator-visible state

Tests to require:

- deterministic candle-builder tests
- stale-feed and reconnect-state tests using mocked adapter events

Suggested Codex task prompt:

"Add a broker-agnostic live market-data adapter and a 5-minute candle builder for paper trading. Implement a first adapter using Kite settings, but keep the paper engine dependent only on the adapter interface. Add deterministic tests for candle construction and stale-feed behavior."

#### Workstream 7. Dashboard Support For Live Paper Sessions

Objective:

- make paper trading operable from the dashboard without depending on slow DuckDB archive queries during the session

Status:

- completed

Files expected:

- `web/state.py`
- `web/pages/ops_pages.py`
- `web/main.py`
- `tests/test_web_state.py`

Implementation tasks:

1. add DuckDB-backed fetchers for:
	- active sessions
	- active positions
	- recent order events
	- realized and unrealized PnL
	- feed-health state
2. keep `/paper_ledger` dual-mode:
	- live session state from DuckDB `paper.duckdb` (via replica)
	- archived session history from DuckDB after close
3. expose explicit operator actions if desired later:
	- refresh
	- pause session
	- flatten all
4. do not route live paper-session views through existing heavy archived-run queries

Acceptance criteria:

- opening the paper-trading dashboard page does not depend on `get_runs_with_metrics()` or historical ledger queries for active sessions
- live paper page remains responsive even if DuckDB archive reads are slow

Tests to require:

- basic UI-state tests if the project already covers those patterns
- state-layer tests for PostgreSQL-backed paper reads

Suggested Codex task prompt:

"Extend the dashboard to support live paper sessions backed by PostgreSQL, not DuckDB. Keep archived session analytics available after close, but ensure active session views use fast operational reads. Update web state helpers and paper ledger UI accordingly."

#### Workstream 8. Agent Support For Paper Sessions

Objective:

- allow the local agent to inspect and manage paper sessions safely

Status:

- completed

Files expected:

- `agent/tools/backtest_tools.py`
- `agent/llm_agent.py`
- `tests/test_backtest_tools.py`
- `tests/test_llm_agent.py`

Implementation tasks:

1. add read-oriented paper tools first:
	- list paper sessions
	- get active positions
	- get session summary
	- get archived paper ledger
2. only after review, add controlled write tools:
	- start session
	- stop session
	- flatten session
3. keep tool contracts explicit about whether they read from PostgreSQL live state or DuckDB archive

Acceptance criteria:

- agent can inspect paper sessions without direct SQL
- tool naming does not blur historical backtests and live paper sessions

Tests to require:

- tool-level tests mirroring the style used for existing backtest tools

Suggested Codex task prompt:

"Add agent tools for inspecting paper-trading sessions, active positions, and archived paper ledgers. Keep read tools separate from write tools. Make storage sources explicit and add tests matching the current backtest tool patterns."

#### Workstream 9. Parity, Safety, And Rollout

Objective:

- prove paper trading is aligned with backtest logic and operationally safe before everyday use

Status:

- completed

Implementation tasks:

1. create replay tests where historical candles are streamed one bar at a time into the paper engine
2. compare outputs against archived backtest expectations for representative days and symbols
3. define staged rollout:
	- signals only
	- paper entries only
	- paper entries plus exits with strict caps
	- full session with operator monitoring
4. document operator runbooks for:
	- stale feed
	- reconnect
	- flatten all
	- session recovery after crash

Acceptance criteria:

- replay-mode paper engine decisions are acceptably close to historical backtest behavior
- operator runbook is explicit and actionable
- first live sessions can run with constrained risk and clear visibility

Suggested Codex task prompt:

"Add replay-parity and safety rollout support for paper trading. Feed historical candles incrementally into the new paper engine, compare behavior to backtest expectations, and document operational procedures for stale feed, recovery, and flatten-all workflows."

### Recommended Pull Request Sequence

Use this order for actual implementation reviews:

1. strategy-logic extraction only
2. PostgreSQL schema and repository helpers only
3. DuckDB archive model extension only
4. paper-trading CLI orchestration with fake feed only
5. signal generation refactor only
6. live feed adapter and candle builder only
7. dashboard live paper support only
8. agent paper tools only
9. parity tests, runbook updates, and rollout controls

This sequence is deliberate:

- it isolates strategy correctness from storage changes
- it isolates storage changes from live-feed integration risk
- it keeps dashboard responsiveness concerns separate from core engine work
- it makes review and rollback practical if one batch goes wrong

PR-by-PR review checklist:

1) strategy-logic extraction
- Decision helpers are extracted as reusable units with minimal orchestration coupling
- No behavior changes to `pivot-backtest` CLI defaults or cache/run_id semantics
- `_simulate_trade`-path is demonstrably simplified through helper usage
- Tests cover helper-level behavior before full engine integration

2) DuckDB paper schema and repository helpers
- New paper tables are in `paper.duckdb` and isolated from `market.duckdb` path
- Session lifecycle fields support recoverability and explicit terminal states
- Indexing supports quick active-session and per-symbol reads
- Helper functions are idempotent for restart-safe control flows

3) DuckDB archive model extension
- Backtest metrics/legacy rows remain unchanged in behavior
- Paper rows are distinguishable by execution mode/session metadata
- Archive refresh includes completed paper trades without contaminating live state
- Legacy dashboard queries continue to pass for historical backtest data

4) paper-trading CLI orchestration with fake feed
- Fake-feed path can execute start/pause/resume/stop/flatten deterministically
- Strategy decisions come from shared helper layer, not duplicated branching
- Active paper run writes only DuckDB `paper.duckdb`; archival path writes to `backtest.duckdb` on completion only
- Replay or session-recovery path is explicit in control flow

5) signal generation refactor
- Signal compute is transport-agnostic (no direct dependency on notification channel)
- Alert flow and paper flow consume same generation helper set
- Dedup/retry semantics are preserved and test-covered

6) live feed adapter and candle builder
- Paper engine consumes a narrow adapter interface
- Candle builder outputs deterministic 5m closed-bars from fixtures
- Stale/missing feed state is captured and prevents unsafe entries

7) dashboard live paper support
- Live paper pages read from DuckDB `paper.duckdb` replica, not `market.duckdb` scans
- Archived sessions remain available through existing analytics
- UI paths remain usable while DuckDB is under load

8) agent paper tools
- Tools are read-first, scoped to live-vs-archive source of truth
- Controlled write actions are explicit and safe
- Tool outputs include enough fields for operator recovery decisions

9) parity tests and rollout controls
- Replay parity checks cover representative CPR_LEVELS and FBR cases
- Runbook defines stale-feed, reconnect, flatten-all, and recovery in operational terms
- Risk gates and staged rollout are encoded in config/workflow, not tribal process

### Non-Goals For The First Release

The first paper-trading release should explicitly avoid these extras:

- real-money order execution
- multi-broker abstraction beyond the first narrow adapter interface
- full portfolio optimization beyond existing shared portfolio controls
- deep charting or advanced visualization before core session correctness is proven
- merging active live state into DuckDB dashboards during market hours

### Minimum Viable Paper Trading Release

The first acceptable release should deliver only this:

- one active paper session at a time
- CPR_LEVELS and FBR using shared strategy logic
- DuckDB-backed live session state (`paper.duckdb`)
- archived completed trades visible in existing analytics surfaces
- dashboard visibility for active positions and session status
- replay tests proving no major logic drift

Anything beyond that is a second-wave enhancement, not part of the core paper-trading milestone.

### Safety Controls Required Before First Use

Paper trading should not be enabled without these guardrails:

- max daily loss cutoff
- max open positions and per-position cap enforcement
- market-close flatten time before exchange close
- stale-feed timeout that pauses new entries
- single active session lock to avoid double-running the same symbols
- full audit trail for signal time, entry time, fill price, exit reason, and manual overrides

### Testing Strategy

The safest validation path is progressive:

1. historical candle replay through the new paper engine with no broker dependency
2. simulated live feed from stored day packs to verify candle aggregation and recovery behavior
3. broker-connected dry run with signal capture only
4. broker-connected paper orders with strict symbol and capital limits
5. production-style daily paper sessions only after multi-day clean runs

### Recommended Order Of Work

Implement in this order:

1. shared strategy extraction
2. PostgreSQL session and position schema
3. paper-session CLI and archival path
4. live feed adapter and bar builder
5. dashboard and agent surfaces
6. staged rollout and parity validation

This order keeps the design aligned with the current codebase: deterministic research stays in DuckDB, mutable operational state stays in DuckDB `paper.duckdb`, and the dashboard evolves from historical analysis toward live operator support without breaking the existing backtest workflow.
