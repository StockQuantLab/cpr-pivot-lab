# Metrics Policy

Date: 2026-03-11
Last reviewed: 2026-05-03

## Run Metrics Source of Truth

`run_metrics` is the canonical source for dashboard and compare pages. It is materialized from:
- `backtest_results`
- `run_daily_pnl`
- `run_metadata`

## Definitions Used

- `total_pnl`: sum of `profit_loss` over all trades in the run.
- `profit_factor`: gross profit / gross loss.
- `max_dd_abs`: worst peak-to-trough drawdown on cumulative P/L.
- `max_dd_pct`: drawdown % from the run equity curve (daily P/L series).
- `annual_return_pct`: CAGR over requested run window.
- `calmar`: `annual_return_pct / max_dd_pct`.

## Why Calmar Can Be High

For broad multi-symbol runs, diversified daily P/L can produce very small drawdown percentages.  
When drawdown denominator is tiny, Calmar can become numerically large even if annual return is modest.

This is not a computation bug by itself; it is a metric interpretation issue for highly diversified, fixed-risk simulations.

## Dashboard Guardrail

Home page "Best Calmar" selection now requires:
- `trade_count >= 10`
- run span `>= 365` days
- `max_dd_pct >= 0.10%`

Fallback remains:
- if no run passes drawdown filter, use the long-window set;
- if no long-window set, use trade-count set.

This prevents near-zero drawdown outliers from dominating the headline KPI.

## Ranking Guidance

For production strategy ranking, use a basket of metrics:
1. `profit_factor`
2. `total_pnl`
3. `max_dd_pct`
4. `calmar` (with the above guardrail context)
