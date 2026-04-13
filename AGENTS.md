# AGENTS.md — CPR Pivot Lab AI Reference

Project instructions for the CPR Pivot Lab AI agent (Phidata + Ollama).

## Claude Code Skills

| Skill | Location | Use For |
|---|---|---|
| `query-trades` | project | Query saved trade rows and run summaries from `backtest_results` |
| `compare-runs` | project | Compare two `run_id`s with metric deltas |
| `performance-optimizer` | user | Slow backtest or query diagnosis |
| `database-specialist` | user | DuckDB query shape and indexing guidance |
| `debugger` | user | Strategy errors and trace debugging |
| `code-reviewer` | user | Pre-merge design/review |
| `test-engineer` | user | Add coverage for CLI, engine, and web |
| `docs-writer` | user | Maintain `STRATEGY.md`, `README.md`, architecture docs |

## Tooling Notes

Use `.codex/` hooks in this repo only for local checks:

- `block_env_write.py` blocks edits to `.env*`
- `ruff_format.py` formats edited `.py` files post-write
- Dashboard data loaders must stay schema-tolerant. Legacy DuckDB files may omit `execution_mode`
  and `exit_reason`; preserve compatibility guards in `db/duckdb.py` and `web/state.py`.
- Keep `configure_windows_stdio()` import-safe under pytest. Do not reintroduce Windows stdio wrapping
  that breaks pytest capture during collection.
- Any command that touches PostgreSQL, walk-forward, paper trading, or runtime writers must be
  launched as `doppler run -- ...`. Do not run those paths with plain `uv run` in the sandbox shell;
  they require host-side Doppler secrets and will fail without them.

`data/` is protected because market.parquet + market.duckdb are expensive to rebuild.

## Quality Gates

Run these before merge or push:

- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy engine/ db/ web/ agent/ --no-error-summary`
- `uv run pytest tests/ -q`

## Running the Agent

```bash
doppler run -- uv run pivot-agent
doppler run -- uv run pivot-agent -q "Run RELIANCE backtest 2025"
doppler run -- uv run pivot-agent --session <session-id>
```

## Daily Ingestion Sequence

After each Kite ingestion window (run in order):

1. `doppler run -- uv run pivot-kite-ingest --refresh-instruments --exchange NSE`
2. `doppler run -- uv run pivot-kite-ingest --from <start> --to <end>`
3. `doppler run -- uv run pivot-kite-ingest --from <start> --to <end> --5min --resume`
4. `doppler run -- uv run pivot-build --refresh-since <start> --batch-size 64`
5. `doppler run -- uv run pivot-data-validate`
6. `doppler run -- uv run pivot-data-quality --refresh --full`
7. `doppler run -- uv run pivot-paper-trading daily-prepare --trade-date <next_trading_date> --all-symbols`

Step 6 refreshes the `data_quality_issues` table served by the `/data_quality` dashboard page.
Step 7 pre-filters symbols for the next trading day's live paper session (uses today's CPR/ATR setup data to reduce ~2100 symbols to ~200 candidates).
NSE holidays and weekends are handled automatically — Kite returns empty data for non-trading days.

## Paper Simulation (end-to-end with alerts)

To test the full paper trading stack including Telegram/email alert dispatch, use `daily-replay`
(not `daily-sim`). `daily-replay` feeds historical candles through `paper_runtime.py` exactly
as live trading does. Use `--multi` to run all variants (LONG + SHORT) in a single process:

```bash
doppler run -- uv run pivot-paper-trading daily-replay \
  --multi --strategy CPR_LEVELS --trade-date 2026-04-02 --all-symbols --no-alerts
```

`daily-sim` is faster (Polars engine) but skips `paper_runtime.py` and does not dispatch alerts.
Use `pivot-paper-trading cleanup --apply` when you need to clear only paper sessions and PAPER
analytics rows before a rerun. It preserves baseline backtest results.

Paper replay/live/sim strategy parameters must match backtest parameters. Do not add
paper-only strategy defaults such as implicit `min_price`, `risk_based_sizing`, or
`narrowing_filter` values; use the same explicit flags or JSON overrides as the matching
backtest run.

Canonical CPR replay/live runs should use the preset-driven config bundle, not a hand-spelled
flag set. In other words, use `--strategy CPR_LEVELS` with the canonical CPR preset defaults
already defined in the codebase; only add explicit CPR flags when you are intentionally testing
an override.

For CPR_LEVELS, a trade only opens when the effective reward/risk at entry meets
`min_effective_rr` (default `2.0`). `rr_ratio` is the target multiple used by the
trade model, not the entry gate. For CPR SHORT presets, `skip_rvol_check=True`
means the stored `rvol_threshold` is ignored at runtime even if the dashboard still
shows a numeric value.

For the current CPR baseline compare flow, stay on `CPR_LEVELS` only. FBR is not required
for this comparison set and should not be mixed into CPR baseline reruns.

Apr 2026 CPR parity note:
- `daily-replay` starts a fresh session per trade date.
- Matching params are required, but executed rows can still diverge from a multi-day backtest window.
- On 2026-04-01 we observed paper trades for `GANESHCP` and `ASTERDM` that did not appear in the full baseline rows.
- When this happens, compare setup rows and intraday day packs first, then diff replay entry logic against the backtest path before changing filters.
- The fix direction is to unify the CPR entry search itself. Keep replay/live as the candle-by-candle operational model, and align backtest to the same shared search so trade existence matches everywhere.

## Monday Operator Priority

Start the week with fast walk-forward validation and then move to live paper trading.

## Walk-Forward Policy

- Use `pivot-paper-trading walk-forward` for the fast DuckDB-backed promotion gate.
- Use `pivot-paper-trading walk-forward-matrix` as the default operator command when you need the standard full-universe validation set. It runs the 2 canonical CPR gates sequentially: `CPR_LEVELS LONG`, `CPR_LEVELS SHORT`. For named presets, prefer `CPR_LEVELS_RISK_LONG` and `CPR_LEVELS_RISK_SHORT`.
- Use `pivot-paper-trading walk-forward-cleanup` to remove wrong/aborted validator runs. Do not hand-write SQL or ad hoc Python deletes for normal cleanup.
- Use `pivot-paper-trading walk-forward-replay` only when you need full paper-session parity or debugging.
- `walk-forward` writes gate lineage into PostgreSQL `walk_forward_runs` and `walk_forward_folds` and also saves DuckDB `backtest_results` rows for the fold runs.
- DuckDB fold runs are tagged with their parent `wf_run_id` in `run_metadata` so cleanup can be done from the CLI.
- `walk-forward-matrix` performs a trade-date coverage preflight before the first fold. If `market_day_state` or `strategy_day_state` is stale for the requested range, it must fail fast instead of returning a zero-trade run.
- The `/walk_forward` dashboard page is the validation surface for those fast-validator runs. Keep `/paper_ledger` for replay/live execution state.
- Each `/walk_forward` fold expands inline to show the underlying trade ledger and a link back to the fold's backtest run.
- Daily replay and daily live should only proceed when the current strategy params match the latest passing fast-validator gate key.
- If you need a fresh validation ledger, use `pivot-paper-trading walk-forward-cleanup` first. Keep `paper_trading_sessions`, archived paper ledgers, and market history intact.
- If `walk_forward_folds` is missing or the schema is stale, run `pivot-db-init` before any walk-forward command.
- On Windows, any CLI entry point that touches PostgreSQL must call the selector-loop setup before `asyncio.run(...)`.

### 1. Walk-Forward Validation First

Human operators should run the fast validator from the CLI, then review it on the dedicated `/walk_forward` dashboard page:

```bash
doppler run -- uv run pivot-paper-trading walk-forward-matrix \
  --start-date 2026-03-10 \
  --end-date 2026-03-31 \
  --all-symbols --force
```

Use this before Monday live-paper starts or before promoting any rule change.
Use `pivot-paper-trading walk-forward-replay` only when you need full per-bar replay parity.
Any explicit `--start-date` / `--end-date` range is valid, including a single-day run where both dates are the same.
The walk-forward, walk-forward-matrix, and walk-forward-replay commands also accept `--start` / `--end` aliases.

### 2. Then Run Daily Live Paper

For the live paper session after validation. Use `--multi` to run all variants concurrently:

```bash
doppler run -- uv run pivot-paper-trading daily-live \
  --multi --strategy CPR_LEVELS --trade-date 2026-04-08 --all-symbols
```

### 3. Where Results Appear

- `/walk_forward` is the validation surface for fast walk-forward runs.
- `/paper_ledger` is the paper-trading surface.
- Active paper-session state is read from PostgreSQL.
- Archived paper-session history is read from DuckDB.
- Fast walk-forward output lands in PostgreSQL `walk_forward_runs` / `walk_forward_folds` plus DuckDB backtest rows.
- Replay and live paper output appears under `Paper Sessions` after each replayed day is archived.
- `/backtest` and `Strategy Analysis` remain backtest-only surfaces.

## Coding Agent Guidance

This section is for coding agents such as GitHub Copilot, Codex, and Claude Code.

- Humans already have the CLI commands needed for fast walk-forward, walk-forward replay, and daily live paper, plus the `/walk_forward` dashboard page for reviewing gates.
- For paper replay/parity checks, prefer `pivot-paper-trading daily-replay`; it is the candle-by-candle alert path.
- Replay logs are intentionally light: candle-progress heartbeats show the current 5-minute bar only for the
  first symbol in each date, while trade lifecycle logs are emitted only for traded symbols and include the
  candle time of the event.
- For paper-only cleanup, prefer `pivot-paper-trading cleanup --apply`; do not suggest `pivot-reset-history --apply` unless the user explicitly wants to erase baseline backtests too.
- When rerunning the four CPR dashboard baselines, keep every parameter identical to the existing CPR row and change only the date window. Prefer named presets such as `CPR_LEVELS_RISK_LONG` and `CPR_LEVELS_RISK_SHORT` rather than spelling the full flag bundle. Use `--risk-based-sizing` as the canonical wording; `--standard-sizing` is only a legacy opt-out alias.
- For default CPR paper replay/live commands, use `doppler run -- uv run pivot-paper-trading daily-replay --multi --strategy CPR_LEVELS --trade-date <date> --all-symbols --no-alerts` and rely on the canonical CPR preset defaults. Do not repeat `--min-price 50 --cpr-min-close-atr 0.5 --narrowing-filter` unless the user explicitly wants an override.
- `max_positions=10` is a concurrent-open-position cap, not a daily trade cap. More than 10 trades/day is possible if positions close and later signals open new ones.
- The local CPR Pivot Lab agent currently has inspection tools for paper sessions and archived ledgers.
- The local CPR Pivot Lab agent does not yet have a dedicated tool that launches `pivot-paper-trading walk-forward` directly.
- Until such a tool exists, coding agents should instruct users to run the CLI commands above instead of implying that the local agent can orchestrate walk-forward itself.
- When documenting paper-trading results, always distinguish live PostgreSQL session state from archived DuckDB paper ledgers.

### Running Backtests from Claude Code (Non-TTY)

When running `pivot-backtest` as a subprocess (not a real TTY), stdout is block-buffered and tqdm uses
stderr — both behave differently from an interactive terminal. Always use these flags:

```bash
# CORRECT — non-TTY safe, progress visible via file
PYTHONUNBUFFERED=1 doppler run -- uv run pivot-backtest \
  --all --universe-size 0 --yes-full-run \
  --start 2025-01-01 --end 2026-03-30 \
  --strategy CPR_LEVELS --direction LONG \
  --min-price 50 --cpr-min-close-atr 0.5 --narrowing-filter \
  --save --quiet --progress-file .tmp_logs/bt_cpr_long.jsonl

# Monitor progress (NDJSON, one event per line)
tail -f .tmp_logs/bt_cpr_long.jsonl
```

Rules:
- `--quiet` suppresses tqdm and per-symbol lines (prevents pipe-buffer fill that blocks Python)
- `--progress-file` is **required** for `--quiet` runs with >100 symbols
- `PYTHONUNBUFFERED=1` prevents Python's own block-buffering from silencing print() output
- Use a timeout of at least 10 minutes for full-universe (2105 sym) runs

### Lock Management for Automation

Two lock files protect against concurrent writers:

| File | Auto-clears? | On process kill |
|------|-------------|----------------|
| `data/market.duckdb.writelock` | Yes — stale PID auto-deleted on next acquisition | Run `pivot-backtest` again; it will self-clear |
| `.tmp_logs/runtime-writer.lock` | Yes — OS releases on process exit | Never goes stale |

When a live process holds the lock, both error messages now print the exact kill command:
```
Kill it:  taskkill //F //PID 12345
```
After killing, simply retry — the next `pivot-backtest` invocation auto-clears the stale writelock.

### Expected Performance (after Apr 2026 query fixes)

Three query bugs were fixed (CLI batch_size default 32→512, setup query missing start-date filter,
pack query missing date-range filter). Expected wall times:

| Universe | Date range | Expected time |
|----------|-----------|--------------|
| 51 symbols | 15 months | ~5–10 s |
| 2105 symbols | 15 months | ~2–5 min |
| 2105 symbols | 10 years | ~20–40 min |

If a run takes >10 min for a 15-month window, check for stale writelock contention first.

## Agent Tools

All tools are registered in `agent/tools/backtest_tools.py` and exposed by `agent/llm_agent.py`.

### 1. `run_backtest`

Run a strategy on one symbol.

- `symbol` (required)
- `start_date` (required)
- `end_date` (required)
- `strategy`: `CPR_LEVELS` or `FBR`
- `direction_filter`: `BOTH`/`LONG`/`SHORT`
- `cpr_percentile`, `rvol_threshold`, `rr_ratio`, `portfolio_value`, `max_positions`, `max_position_pct`, `time_exit`
- `cpr_min_close_atr`, `narrowing_filter`, `failure_window`

### 2. `run_multi_stock_backtest`

Same argument family as `run_backtest`, accepts `symbols: list[str]`.

### 3. `get_backtest_summary`

Aggregate summary across stored `run_id`s and optional `symbol`.

### 4. `get_available_symbols`

Returns symbol coverage and date spans.

### 5. `get_cpr_for_date`

Returns daily CPR/pivot values for one symbol and one trading date.

### 6. `get_data_status`

Returns data readiness state, row counts, and active data ranges.

### 7. `rebuild_indicators`

Rebuild all runtime tables (`--force` supported).

### Paper Session Inspection Tools

The current local agent exposes read-oriented paper-session tools:

- `list_paper_sessions`
- `get_paper_session_summary`
- `get_paper_positions`
- `get_paper_ledger`

These tools support inspection and reporting only. They do not launch walk-forward or daily-live execution.

## Campaign Policy

Canonical paper run order is CPR only:

1) CPR_LEVELS (`--preset CPR_LEVELS_RISK_LONG` / `--preset CPR_LEVELS_RISK_SHORT`)
   Equivalent explicit flags: `--cpr-min-close-atr 0.5 --min-price 50 --narrowing-filter --risk-based-sizing`
   (SHORT variant also adds `--skip-rvol`; LONG does NOT skip rvol)

## Architecture

```
engine/cpr_atr_strategy.py (CPR_LEVELS / FBR)
   → db/duckdb.py (Materialized runtime tables)
      → tests + web + agent tooling
```

## CRITICAL OPERATING RULE

- Never run `pivot-backtest` or `pivot-build` concurrently.
- Never use `pivot-build --force` for a recent Kite catch-up window unless the user explicitly wants a full runtime repair.
- For recent Kite catch-up windows, rebuild `intraday_day_pack` with `doppler run -- uv run pivot-build --table pack --refresh-since <window-start> --batch-size 64` (alias: `--since`).
- For same-day DQ / replay parity, use `doppler run -- uv run pivot-build --refresh-date YYYY-MM-DD` to refresh the full runtime slice for that exact trade date. Add `--symbols`, `--symbols-file`, or `--missing` when only a subset of symbols needs the refresh.
- `--table pack --refresh-since <window-start>` only refreshes `intraday_day_pack`; it does not advance `or_daily`, `market_day_state`, or `strategy_day_state`. If the next step is same-day DQ, `/walk_forward` validation, or replay/live parity on the fresh trade date, run the table-wide `doppler run -- uv run pivot-build --refresh-since <window-start> --batch-size 64` refresh instead.
- A full-universe `doppler run -- uv run pivot-build --table pack --force` rebuild is destructive because it drops the existing pack table first. It now requires `--allow-full-pack-rebuild` and should be treated as an explicit repair path, not the default refresh flow.
- Any no-table `pivot-build --force` run must be executed only as a staged full rebuild:
  `doppler run -- uv run pivot-build --force --full-history --staged-full-rebuild ...`
- After incremental Kite ingestion, prefer targeted rebuilds first and explicitly call out that `pivot-build` scans all local parquet history, not just the newly ingested dates.
- If a staged rebuild is interrupted, treat `data/market.duckdb` as mixed runtime state until a clean staged rebuild is resumed/completed.
- On Windows, long-running `pivot-build` and `pivot-paper-trading walk-forward` commands should be launched in the background with stdout/stderr redirected to log files under `.tmp_logs/`, then polled for status. Do not hold the interactive terminal open for hour-scale rebuilds.
- Run long campaigns with `--resume` and month chunking.
- Use `pivot-clean` for stale transient artifacts.
- Use `--ensure-runtime-coverage` (default in campaign) and do not bypass it unless debugging.
- Run `pivot-dashboard` in a foreground terminal on Windows. Detached launches can exit early, so
  keep that process alive while validating UI changes.

## Keyboard + Docs

CLI entry points:

- `pivot-backtest`, `pivot-build`, `pivot-gold`, `pivot-campaign`, `pivot-dashboard`, `pivot-agent`
- `pivot-data-quality`, `pivot-data-validate`, `pivot-signal-alert`, `pivot-clean`, `pivot-parity-check`
- `pivot-db-init`, `pivot-paper-trading`

Paper-trading references for humans and coding agents:

- `docs/PAPER_TRADING_RUNBOOK.md` is the operator flow reference.
- `docs/SETUP.md` points to the paper-trading runbook from the main setup path.
- `README.md` summarizes the current Monday workflow.

## Paper Trading Architecture Notes

- `--multi` flag: runs the 2 standard CPR variants (CPR_LEVELS LONG/SHORT) concurrently via `asyncio.gather`.
- Alert dispatcher uses reference counting (`register_session_start` / `maybe_shutdown_alert_dispatcher`) — safe for concurrent sessions. Only shuts down when all sessions complete.
- Dashboard reads paper.duckdb via `get_dashboard_paper_db()` (replica) — no file-lock conflict with live sessions. Dashboard can stay open during trading.
- Stale ACTIVE sessions are auto-cancelled on startup via `cleanup_stale_sessions()` in `db/paper_db.py`.
- Symbol pre-filtering (`pre_filter_symbols_for_strategy` in `scripts/paper_prepare.py`) reduces Kite API load from ~2100 to ~200 candidates using `market_day_state` SQL filters (min_price, CPR width, narrowing). In `--multi`, pre-filter runs **once per unique strategy+params** (not per variant) — CPR LONG+SHORT return identical symbols so the DB query is shared.
- Live `--multi` uses `_SharedCachingAdapter` (in `scripts/paper_trading.py`): one `KiteQuoteAdapter` wraps all variants with a `threading.Lock` + 0.3 s TTL cache, halving Kite API round-trips when LONG+SHORT variants poll simultaneously.
- Replay `--multi` calls `load_replay_day_packs()` (public, in `scripts/paper_replay.py`) once for the union of all pre-filtered symbols. Each variant filters its subset from the shared list, avoiding duplicate DuckDB pack reads.
- `exit_time` stored in `paper_positions` is the candle's `bar_end` (IST-aware datetime), not wall-clock. Manual flatten is the only path that uses wall-clock (no associated candle). Fix was applied to `db/paper_db.py` (`update_position` now accepts `closed_at`) and the 3 trade-close call sites in `engine/paper_runtime.py`.
