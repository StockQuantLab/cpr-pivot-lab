# Code Map — CPR Pivot Lab

## Architecture

```text
raw CSV (NSE) -> pivot-convert -> parquet -> duckdb runtime tables -> backtest engine -> dashboard + agent
```

## Directory Map

- `engine/`
  - `cpr_atr_strategy.py` core strategy engine (`CPR_LEVELS`, `FBR`)
  - `cpr_atr_utils.py` trailing stop and helper utilities
  - `constants.py` strategy, direction, and execution constants
  - `run_backtest.py` backtest CLI entry
  - `convert_to_parquet.py` raw CSV conversion
  - `alert_dispatcher.py` best-effort async alert delivery (Telegram, email)
  - `notifiers/` transport implementations (`telegram.py`, `email.py`)
- `db/`
  - `duckdb.py` runtime tables and reporting views (`market.duckdb`)
  - `backtest_db.py` backtest result storage (`backtest.duckdb`)
  - `paper_db.py` paper trading state (`paper.duckdb`)
  - `replica.py` engine-side versioned file replication
  - `replica_consumer.py` dashboard-side replica reader
  - `postgres.py` agent sessions and walk-forward metadata (PostgreSQL)
- `agent/`
  - `llm_agent.py`, `tools/backtest_tools.py` for query tooling
- `web/`
  - `main.py`, `run_nicedash.py`, `state.py`
  - `pages/` and `components/` NiceGUI views
- `scripts/`
  - `build_tables.py`, `data_quality.py`, `data_validate.py`, `gold_pipeline.py`, `run_campaign.py`
- `tests/` unit and integration verification
- `docs/` design docs and ADRs

## Runtime Tables

- `market_day_state` and `strategy_day_state`: setup precompute and prefilter metadata
- `intraday_day_pack`: required intraday arrays per symbol/date
- `cpr_daily`, `cpr_thresholds`, `atr_intraday`: market state (all in `market.duckdb`)
- `backtest_results` and `run_metrics`: persisted outputs (in `backtest.duckdb`)
- `paper_sessions`, `paper_positions`, `paper_orders`: live paper state (in `paper.duckdb`)

## Core CLI Pattern

```bash
doppler run -- uv run pivot-convert
doppler run -- uv run pivot-build --force
doppler run -- uv run pivot-backtest --strategy CPR_LEVELS --universe-name gold_51 --start ... --end ... --save
doppler run -- uv run pivot-dashboard
```

## Campaign Contract

`pivot-campaign` is the standard long-window runner and enforces runtime coverage by default.

- Production order: `FBR -> CPR_LEVELS`
- chunking defaults to monthly windows with resume support
