# Documentation Index

Last reviewed: 2026-05-05

This directory is the home for project documentation. Keep the repository root limited to
`README.md`, `STRATEGY.md`, and source-control metadata; operational plans, architecture notes,
and policy documents belong under `docs/`.

## Start Here

- [../README.md](../README.md) - project overview, setup, and common commands
- [../STRATEGY.md](../STRATEGY.md) - canonical strategy specification and parameter policy
- [PAPER_TRADING_RUNBOOK.md](PAPER_TRADING_RUNBOOK.md) - paper replay/live operator workflow
- [REAL_ORDER_LIVE_FLOW.md](REAL_ORDER_LIVE_FLOW.md) - plain-English actual live broker order lifecycle
- [SETUP.md](SETUP.md) - setup, command profile, and remote dashboard access

## Operations

- [KITE_INGESTION.md](KITE_INGESTION.md) - Kite token, instrument, daily, and 5-minute ingestion
- [RUNTIME_REBUILD.md](RUNTIME_REBUILD.md) - staged runtime rebuild and repair flow
- [MULTI_MACHINE_SETUP.md](MULTI_MACHINE_SETUP.md) - multi-machine operating notes
- [ISSUES.md](ISSUES.md) - dated incident, experiment, and fix log
- [PARITY_INCIDENT_LOG.md](PARITY_INCIDENT_LOG.md) - legacy parity incident history

## Architecture And Design

- [CODEMAP.md](CODEMAP.md) - current code and runtime table map
- [DESIGN.md](DESIGN.md) - architecture and paper-trading implementation notes
- [DATABASE_ARCHITECTURE.md](DATABASE_ARCHITECTURE.md) - database layout and data movement
- [backtest-medallion-workflow.md](backtest-medallion-workflow.md) - backtest data workflow
- [NICEGUI_DESIGN_SYSTEM.md](NICEGUI_DESIGN_SYSTEM.md) - dashboard UI patterns

## Strategy And Metrics

- [strategy-guide.md](strategy-guide.md) - operator-level strategy behavior guide
- [trailing-stop-explained.md](trailing-stop-explained.md) - exit lifecycle and trailing stop behavior
- [PARAMETER_UNIFORMITY.md](PARAMETER_UNIFORMITY.md) - strategy parameter consistency policy
- [METRICS_POLICY.md](METRICS_POLICY.md) - run metrics source-of-truth and dashboard guardrails

## Plans And Historical Decisions

- [OPTIMIZATION_PLAN.md](OPTIMIZATION_PLAN.md) - completed backtest optimization plan and implementation log
- [ENGINE_OPTIMIZATION_PLAN.md](ENGINE_OPTIMIZATION_PLAN.md) - engine optimization roadmap
- [ANALYST_TRANSFORMATION_PLAN.md](ANALYST_TRANSFORMATION_PLAN.md) - analyst transformation plan
- [LIVE_EXECUTION_SAFETY_PLAN.md](LIVE_EXECUTION_SAFETY_PLAN.md) - live execution safety design
- [LIVE_READINESS_RESILIENCY_HARDENING_PLAN.md](LIVE_READINESS_RESILIENCY_HARDENING_PLAN.md) - live readiness hardening plan
- [LIVE_TRADING_ARCHITECTURE_PLAN.md](LIVE_TRADING_ARCHITECTURE_PLAN.md) - live trading architecture plan
- [LIVE_TRADING_PARITY_REWORK_PLAN.md](LIVE_TRADING_PARITY_REWORK_PLAN.md) - parity rework history
- [PROGRESSIVE_TRAIL_RATCHET_PLAN.md](PROGRESSIVE_TRAIL_RATCHET_PLAN.md) - rejected trail ratchet experiment
- [performance-tuning.md](performance-tuning.md) - performance tuning notes

## ADRs

- [adr/001-baseline-strategy-policy.md](adr/001-baseline-strategy-policy.md) - baseline strategy policy
- [adr/001-pivot-level-strategy.md](adr/001-pivot-level-strategy.md) - CPR_LEVELS strategy ADR
- [adr/002-cpr-full-framework.md](adr/002-cpr-full-framework.md) - CPR floor pivot extension ADR

## Maintenance Rules

- Put new durable docs under `docs/`; keep local agent files (`AGENTS.md`, `CLAUDE.md`, `.codex/`, `.claude/`) out of Git.
- Prefer links relative to the current file. From a file inside `docs/`, link to `PAPER_TRADING_RUNBOOK.md`, not `docs/PAPER_TRADING_RUNBOOK.md`.
- When code changes alter an operator command, update the relevant runbook in the same change.
- Keep generated reviews and scratch plans out of the docs index unless they become durable project references.
