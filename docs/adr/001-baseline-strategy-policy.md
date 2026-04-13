# ADR 001: Baseline Strategy Policy

## Status

Accepted

## Context

The project exposes two baseline strategies in public CLIs and runbooks:

- `CPR_LEVELS`
- `FBR`

`VIRGIN_CPR` is retained only as internal legacy research code and is not exposed in public CLI workflows.

## Decision

- Treat `CPR_LEVELS` and `FBR` as the only baseline strategies for:
  - `pivot-campaign` default order
  - benchmark reporting used for go/no-go decisions
  - baseline dashboard views and comparison defaults
- Do not expose `VIRGIN_CPR` in operator-facing commands or baseline dashboards.

## Consequences

- Cleaner parameter space for dashboard interpretation and daily runbooks.
- Lower operational ambiguity for production comparisons.
- Historical research code remains in the repository, but baseline backtests and campaigns only use `CPR_LEVELS` and `FBR`.
