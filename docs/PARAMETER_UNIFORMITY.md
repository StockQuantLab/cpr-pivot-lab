# Parameter Uniformity — Backtest vs Paper Replay vs Paper Live

This document verifies that all strategy parameters resolve identically across the three
execution modes: backtest (vectorized Polars), paper replay (candle-by-candle historical),
and paper live (WebSocket/Kite real-time).

**Last verified**: 2026-04-29

---

## Resolution Architecture

All three modes converge on the same `StrategyConfig` dataclass via the same function:

```
                    ┌─────────────────────────────┐
                    │  engine/strategy_presets.py  │
                    │  build_strategy_config_from  │
                    │  _preset() / _overrides()    │
                    └──────────┬──────────────────┘
                               │
                    ┌──────────┴──────────────────┐
                    │      StrategyConfig          │
                    │  (cpr_atr_strategy.py:176)   │
                    └──┬──────────┬────────────┬───┘
                       │          │            │
              Backtest │   Paper  │    Paper   │
               (Polars)│  Replay  │     Live   │
                       │          │            │
              run_backtest.py  paper_trading.py  paper_live.py
```

Key guarantee: `paper_runtime.build_backtest_params_from_overrides` is **aliased** to
`strategy_presets.build_strategy_config_from_overrides` (`paper_runtime.py:827`).
Paper runtime cannot drift — it uses the exact same function.

---

## Parameter Resolution Paths

| Mode | Entry Point | Preset Resolution | Config Object |
|------|------------|-------------------|---------------|
| **Backtest** | `run_backtest.py:1000` | `build_strategy_config_from_preset(preset, cli_overrides)` | `StrategyConfig` |
| **Paper Replay** | `paper_trading.py` → `_resolve_paper_strategy_params()` / `_prepare_paper_multi_strategy_params()` | Same preset → same function | `StrategyConfig` |
| **Paper Live** | `paper_live.py` loads `session.strategy_params` from DB → `paper_runtime.py` `build_backtest_params()` | Same overrides → same function | `StrategyConfig` |

---

## Why "Not in Preset" Is Safe

Presets only override parameters that differ from the `StrategyConfig` dataclass defaults.
Parameters not listed in a preset fall through to the dataclass default, which is shared
across all three modes. Examples:

| Parameter | StrategyConfig Default | In Preset? | Result |
|-----------|----------------------|------------|--------|
| `min_effective_rr` | 2.0 | No | All modes get 2.0 from `CPRLevelsParams` default |
| `entry_window_end` | "10:15" | No | All modes get "10:15" from `StrategyConfig` default |
| `time_exit` | "15:00" | No | All modes get "15:00" from `StrategyConfig` default |
| `or_atr_min` | 0.3 | No | All modes get 0.3 from `StrategyConfig` default |
| `compound_equity` | False | No | All modes get False from `StrategyConfig` default |

If a future preset needs a different value (e.g., `min_effective_rr=1.5`), it must
explicitly include the override. The assertion gate catches accidental omissions.

---

## Full Parameter Table — CPR_LEVELS_RISK_SHORT

### Execution Sizing (from `DEFAULT_EXECUTION_SIZING` in `execution_defaults.py`)

| Parameter | StrategyConfig Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|----------------------|-----------------|----------|-------------|------------|-------|
| `portfolio_value` | 1,000,000 | 1,000,000 | 1M | 1M | 1M | YES |
| `capital` | 200,000 | 200,000 | 200K | 200K | 200K | YES |
| `risk_pct` | 0.01 | 0.01 | 0.01 | 0.01 | 0.01 | YES |
| `max_positions` | 5 | 5 | 5 | 5 | 5 | YES |
| `max_position_pct` | 0.20 | 0.20 | 0.20 | 0.20 | 0.20 | YES |
| `risk_based_sizing` | False | **True** | True | True | True | YES |

### Direction & Filters

| Parameter | Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|---------|-----------------|----------|-------------|------------|-------|
| `direction_filter` | BOTH | **SHORT** | SHORT | SHORT | SHORT | YES |
| `min_price` | 0.0 | **50.0** | 50 | 50 | 50 | YES |
| `skip_rvol_check` | False | **True** | True | True | True | YES |
| `compound_equity` | False | *(default)* | False | False | False | YES |

### Entry & Exit

| Parameter | Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|---------|-----------------|----------|-------------|------------|-------|
| `cpr_percentile` | 33.0 | *(default)* | 33.0 | 33.0 | 33.0 | YES |
| `buffer_pct` | 0.0005 | *(default)* | 0.0005 | 0.0005 | 0.0005 | YES |
| `entry_window_end` | "10:15" | *(default)* | "10:15" | "10:15" | "10:15" | YES |
| `time_exit` | "15:00" | *(default)* | "15:00" | "15:00" | "15:00" | YES |
| `rr_ratio` | 2.0 | *(default)* | 2.0 | 2.0 | 2.0 | YES |
| `breakeven_r` | 1.0 | *(default)* | 1.0 | 1.0 | 1.0 | YES |

### Risk & ATR

| Parameter | Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|---------|-----------------|----------|-------------|------------|-------|
| `min_sl_atr_ratio` | 0.5 | *(default)* | 0.5 | 0.5 | 0.5 | YES |
| `max_sl_atr_ratio` | 2.0 | *(default)* | 2.0 | 2.0 | 2.0 | YES |
| `atr_sl_buffer` | 0.0 | *(default)* | 0.0 | 0.0 | 0.0 | YES |
| `trail_atr_multiplier` | 1.0 | *(default)* | 1.0 | 1.0 | 1.0 | YES |
| `short_trail_atr_multiplier` | 1.0 | **1.25** | 1.25 | 1.25 | 1.25 | YES |
| `atr_periods` | 12 | *(default)* | 12 | 12 | 12 | YES |

### OR & Gap Filters

| Parameter | Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|---------|-----------------|----------|-------------|------------|-------|
| `or_minutes` | 5 | *(default)* | 5 | 5 | 5 | YES |
| `or_atr_min` | 0.3 | *(default)* | 0.3 | 0.3 | 0.3 | YES |
| `or_atr_max` | 2.5 | *(default)* | 2.5 | 2.5 | 2.5 | YES |
| `max_gap_pct` | 1.5 | *(default)* | 1.5 | 1.5 | 1.5 | YES |
| `long_max_gap_pct` | None | *(default)* | None | None | None | YES |

### CPR-Level Sub-Params (`CPRLevelsParams`)

| Parameter | Default | Preset Override | Backtest | Paper Replay | Paper Live | Match |
|-----------|---------|-----------------|----------|-------------|------------|-------|
| `min_effective_rr` | 2.0 | *(default)* | 2.0 | 2.0 | 2.0 | YES |
| `use_narrowing_filter` | False | **True** (via `narrowing_filter`) | True | True | True | YES |
| `cpr_min_close_atr` | 0.0 | **0.5** | 0.5 | 0.5 | 0.5 | YES |
| `momentum_confirm` | False | **True** | True | True | True | YES |
| `scale_out_pct` | 0.0 | *(default)* | 0.0 | 0.0 | 0.0 | YES |
| `cpr_entry_start` | "" | *(default)* | "" | "" | "" | YES |
| `cpr_confirm_entry` | False | *(default)* | False | False | False | YES |
| `cpr_hold_confirm` | False | *(default)* | False | False | False | YES |
| `time_stop_bars` | 0 | *(default)* | 0 | 0 | 0 | YES |
| `cpr_shift_filter` | "ALL" | *(default)* | "ALL" | "ALL" | "ALL" | YES |

### Other Presets

The other three CPR presets (`CPR_LEVELS_RISK_LONG`, `CPR_LEVELS_STANDARD_LONG`,
`CPR_LEVELS_STANDARD_SHORT`) follow the same pattern — they override the same subset of
parameters, differing only in `direction_filter`, `risk_based_sizing`, `skip_rvol_check`,
and `short_trail_atr_multiplier`. All non-overridden parameters inherit from
`StrategyConfig` defaults identically.

---

## Safeguards: Canonical Preset Gates

Paper replay/live has two runtime guards:

1. `_assert_paper_multi_params_match_preset()` verifies `--multi` LONG/SHORT bundles exactly match
   `CPR_LEVELS_RISK_LONG` / `CPR_LEVELS_RISK_SHORT`.
2. `_with_resolved_strategy_metadata()` refuses to persist `_canonical_preset=<name>` if embedded
   strategy values differ from that preset. This prevents a session from being labelled canonical
   while carrying stale sizing such as `capital=100000`, `max_positions=10`, or
   `max_position_pct=0.10`.

The guard:

1. Builds the canonical `StrategyConfig` from the named preset
2. Builds the `StrategyConfig` from the session's resolved params
3. Serializes both to dicts and compares field-by-field
4. **Aborts session creation** if any field differs

```python
if params_config != preset_config:
    diffs = _strategy_config_diffs(params_config, preset_config)
    raise SystemExit(
        f"{label} paper params do not match preset {preset_name}: {diffs}. "
        "Fix the shared preset, not live-only params."
    )
```

This means future paper sessions cannot be marked canonical with mismatched parameters.
Historical sessions created before this guard may still show stale metadata and must be treated as
non-canonical for parity.

---

## Shared Decision Logic

All three modes use the same functions for actual trading decisions:

| Function | Source | Used By |
|----------|--------|---------|
| `scan_cpr_levels_entry()` | `cpr_atr_shared.py` | Backtest + Paper Live |
| `find_cpr_levels_entry()` | `cpr_atr_shared.py` | Backtest + Paper Live |
| `resolve_completed_candle_trade_step()` | `cpr_atr_shared.py` | Backtest + Paper Live |
| `normalize_stop_loss()` | `cpr_atr_shared.py` | Backtest + Paper Live |
| `select_entries_for_bar()` | `bar_orchestrator.py` | Backtest + Paper Live |
| `SessionPositionTracker` | `bar_orchestrator.py` | Backtest + Paper Live |

---

## Regenerating This Table

When parameters change, re-verify by:

1. Check `engine/execution_defaults.py` for shared execution sizing defaults
2. Check `engine/strategy_presets.py` for preset overrides
3. Check `engine/cpr_atr_strategy.py` `StrategyConfig` dataclass for all field defaults
4. Run focused tests:
   - `uv run pytest tests/test_paper_trading_cli.py tests/test_paper_trading_workflow.py -q`
   - `uv run ruff check scripts/paper_trading.py tests/test_paper_trading_cli.py`

---

## Sources

- `engine/execution_defaults.py` — shared execution sizing defaults
- `engine/strategy_presets.py` — preset definitions and `build_strategy_config_from_preset()`
- `engine/cpr_atr_strategy.py:176` — `StrategyConfig` dataclass
- `engine/cpr_atr_strategy.py:134` — `CPRLevelsParams` dataclass
- `engine/paper_runtime.py:796` — `build_backtest_params_from_overrides()` (alias)
- `engine/paper_runtime.py:819` — `build_backtest_params()` (paper live path)
- `scripts/paper_trading.py` — preset resolution, canonical metadata, and assertion gates
- `scripts/paper_live.py` — live session `build_backtest_params()` call
