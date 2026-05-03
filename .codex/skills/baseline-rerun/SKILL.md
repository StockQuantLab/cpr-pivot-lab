---
name: baseline-rerun
description: Rerun and verify canonical CPR/FBR strategy baselines for regression control.
---

# baseline-rerun

## Purpose
Rerun and verify canonical CPR/FBR baselines for regression control.

## Preconditions
- Dashboard process is not holding write lock.
- Runtime tables are already prepared.

## Current Canonical Set

Use the 2026-05-03 15:00 all-8 CPR rerun as the active comparison target. The
previous 15:15 rows were retired after promotion.

| Variant | Run ID |
|---|---|
| STD_LONG | `9a317c93b934` |
| STD_SHORT | `fc5bca650a39` |
| RISK_LONG | `c352c2c9e238` |
| RISK_SHORT | `b4007289822e` |
| STD_LONG_CMP | `91bfdcbcd776` |
| STD_SHORT_CMP | `ca7fa0c63e4b` |
| RISK_LONG_CMP | `b6ac11d6998f` |
| RISK_SHORT_CMP | `121b2b0c6461` |

Canonical sizing is `max_positions=5`, `capital=200000`, `max_position_pct=0.2`.
Daily-reset risk (`RISK_LONG` / `RISK_SHORT`) is the live-paper sizing reference.

## Canonical commands

Prefer `pivot-baselines` so all 8 variants run sequentially with progress logs:

```bash
doppler run -- uv run pivot-baselines --start 2025-01-01 --end 2026-04-30
```

For individual checks, use the dated universe explicitly:

```bash
doppler run -- uv run pivot-backtest --universe-name full_2026_04_30 --yes-full-run --start 2025-01-01 --end 2026-04-30 --preset CPR_LEVELS_RISK_LONG --save --quiet --progress-file .tmp_logs/bt_cpr_risk_long.jsonl
doppler run -- uv run pivot-backtest --universe-name full_2026_04_30 --yes-full-run --start 2025-01-01 --end 2026-04-30 --preset CPR_LEVELS_RISK_SHORT --save --quiet --progress-file .tmp_logs/bt_cpr_risk_short.jsonl
```

## Verification query
```bash
uv run python -c "from db.duckdb import get_dashboard_db as g; db=g(); print(db.con.execute(\"select run_id,strategy_code,total_pnl,total_return_pct,annual_return_pct,calmar,max_dd_pct,trade_count from run_metrics order by updated_at desc limit 8\").fetchall())"
```

## Expected interpretation
- Compare only within the same dated universe/runtime-surface family.
- Do not compare future universe migrations directly against `full_2026_04_30` without labeling
  the migration.
- `5 x 2L` daily-reset risk materially outperformed the current-runtime `10 x 1L` control, but
  both must be rerun after any sizing or entry/exit code change.

