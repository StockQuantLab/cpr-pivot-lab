"""Tests for engine/cpr_atr_strategy.py — BacktestParams, runtime validation, TradeResult."""

import sys
from typing import Any

import polars as pl
import pytest

from db.duckdb import MarketDB
from engine.cpr_atr_shared import scan_cpr_levels_entry
from engine.cpr_atr_strategy import (
    STRATEGY_VERSION,
    BacktestParams,
    BacktestResult,
    CPRATRBacktest,
    CPRLevelsParams,
    DayPack,
    FBRParams,
    TradeResult,
    VirginCPRParams,
)
from engine.strategy_presets import (
    CPR_LEVELS_PRESETS,
    FBR_PRESETS,
    build_strategy_config_from_preset,
    list_strategy_preset_names,
)


class TestBacktestParams:
    """Verify BacktestParams defaults and removed fields."""

    def test_default_strategy_is_cpr_levels(self):
        p = BacktestParams()
        assert p.strategy == "CPR_LEVELS"

    def test_cpr_shift_filter_default(self):
        p = BacktestParams()
        assert p.cpr_levels.cpr_shift_filter == "ALL"

    def test_no_rejection_wick_pct(self):
        """CPR_FADE params should be removed."""
        p = BacktestParams()
        assert not hasattr(p, "rejection_wick_pct")

    def test_no_cpr_approach_atr(self):
        """CPR_FADE params should be removed."""
        p = BacktestParams()
        assert not hasattr(p, "cpr_approach_atr")

    def test_fbr_params_preserved(self):
        p = BacktestParams()
        assert p.fbr.failure_window == 8
        assert p.fbr.reversal_buffer_pct == 0.001

    def test_vcpr_params_preserved(self):
        p = BacktestParams()
        assert p.virgin_cpr.vcpr_confirm_candles == 1  # proven optimal (was incorrectly 2)
        assert p.virgin_cpr.vcpr_body_pct == 0.0  # proven optimal (was incorrectly 0.5)
        assert p.virgin_cpr.vcpr_sl_mode == "ZONE"
        assert p.virgin_cpr.candle_exit == 0

    def test_common_defaults(self):
        p = BacktestParams()
        assert p.cpr_percentile == 33.0  # tightened from 50.0
        assert p.atr_periods == 12
        assert p.buffer_pct == 0.0005
        assert p.rr_ratio == 2.0
        assert p.breakeven_r == 1.0
        assert p.time_exit == "15:15"
        assert p.direction_filter == "BOTH"
        assert p.cpr_levels.cpr_hold_confirm is False
        assert p.cpr_levels.cpr_min_close_atr == 0.0
        assert p.cpr_levels.scale_out_pct == 0.0
        assert p.short_open_to_cpr_atr_min == 0.0
        assert p.runtime_batch_size == 512
        assert p.long_max_gap_pct is None

    def test_runtime_batch_size_default(self):
        p = BacktestParams()
        assert p.runtime_batch_size == 512

    def test_cpr_risk_presets(self):
        assert "CPR_LEVELS_RISK_LONG" in CPR_LEVELS_PRESETS
        assert "CPR_LEVELS_RISK_SHORT" in CPR_LEVELS_PRESETS
        long_cfg = build_strategy_config_from_preset("CPR_LEVELS_RISK_LONG")
        short_cfg = build_strategy_config_from_preset("CPR_LEVELS_RISK_SHORT")
        assert long_cfg.strategy == "CPR_LEVELS"
        assert long_cfg.direction_filter == "LONG"
        assert long_cfg.risk_based_sizing is True
        assert long_cfg.min_price == 50.0
        assert long_cfg.cpr_levels.cpr_min_close_atr == 0.5
        assert long_cfg.cpr_levels.use_narrowing_filter is True
        assert short_cfg.direction_filter == "SHORT"
        assert short_cfg.risk_based_sizing is True
        assert short_cfg.skip_rvol_check is True
        assert short_cfg.cpr_levels.cpr_min_close_atr == 0.5
        assert short_cfg.cpr_levels.use_narrowing_filter is True
        assert short_cfg.short_trail_atr_multiplier == 1.25

    def test_fbr_risk_presets(self):
        assert "FBR_RISK_LONG" in FBR_PRESETS
        assert "FBR_RISK_SHORT" in FBR_PRESETS
        assert "FBR_RISK_LONG" in list_strategy_preset_names("FBR")
        assert "FBR_RISK_SHORT" in list_strategy_preset_names("FBR")

        long_cfg = build_strategy_config_from_preset("FBR_RISK_LONG")
        short_cfg = build_strategy_config_from_preset("FBR_RISK_SHORT")

        assert long_cfg.strategy == "FBR"
        assert long_cfg.direction_filter == "SHORT"
        assert long_cfg.fbr_setup_filter == "BREAKDOWN"
        assert long_cfg.fbr.failure_window == 10
        assert long_cfg.min_price == 50.0
        assert long_cfg.risk_based_sizing is True
        assert long_cfg.skip_rvol_check is True

        assert short_cfg.strategy == "FBR"
        assert short_cfg.direction_filter == "LONG"
        assert short_cfg.fbr_setup_filter == "BREAKOUT"
        assert short_cfg.fbr.failure_window == 10
        assert short_cfg.min_price == 50.0
        assert short_cfg.risk_based_sizing is True
        assert short_cfg.skip_rvol_check is True

    def test_backtest_cli_preset_not_clobbered_by_argparse_defaults(self):
        """Simulate the full backtest CLI --preset path using argparse defaults.

        This is the regression test for the bug where passing the full strategy_overrides
        dict (with CLI defaults: min_price=0.0, narrowing_filter=False, risk_based_sizing=False,
        direction_filter="BOTH") to build_strategy_config_from_preset would silently override
        the preset's values.  The fix builds a minimal explicit-only overrides dict instead.
        """
        # Mirror the argparse defaults exactly as they appear in run_backtest.py
        # with no explicit flags set (i.e. user typed: pivot-backtest --preset CPR_LEVELS_RISK_LONG)
        preset_cli_overrides: dict[str, Any] = {
            "portfolio_value": 1_000_000,
            "capital": 100_000,
            "max_positions": 10,
            "max_position_pct": 0.1,
            "runtime_batch_size": 512,
            "commission_model": "zerodha",
            "slippage_bps": 5.0,
            "time_exit": "15:15",
            "entry_window_end": "10:15",
            # Strategy semantics intentionally excluded — no explicit flag was set
        }
        cfg = build_strategy_config_from_preset("CPR_LEVELS_RISK_LONG", preset_cli_overrides)
        assert cfg.direction_filter == "LONG", "direction clobbered by BOTH default"
        assert cfg.min_price == 50.0, "min_price clobbered by 0.0 default"
        assert cfg.cpr_levels.use_narrowing_filter is True, "narrowing_filter clobbered by False"
        assert cfg.risk_based_sizing is True, "risk_based_sizing clobbered by False"

        cfg_short = build_strategy_config_from_preset("CPR_LEVELS_RISK_SHORT", preset_cli_overrides)
        assert cfg_short.direction_filter == "SHORT", "direction clobbered"
        assert cfg_short.skip_rvol_check is True, "skip_rvol_check clobbered by False"
        assert cfg_short.short_trail_atr_multiplier == 1.25, "short trail multiplier clobbered"

    def test_custom_params(self):
        p = BacktestParams(
            strategy="FBR",
            rr_ratio=1.5,
            runtime_batch_size=8,
        ).apply_strategy_configs(
            cpr_levels=CPRLevelsParams(
                cpr_shift_filter="HIGHER",
                min_effective_rr=2.0,
                use_narrowing_filter=False,
                cpr_entry_start="",
                cpr_confirm_entry=False,
                cpr_hold_confirm=True,
                cpr_min_close_atr=0.1,
                scale_out_pct=0.8,
            )
        )
        assert p.strategy == "FBR"
        assert p.cpr_levels.cpr_shift_filter == "HIGHER"
        assert p.rr_ratio == 1.5
        assert p.cpr_levels.cpr_hold_confirm is True
        assert p.cpr_levels.cpr_min_close_atr == 0.1
        assert p.cpr_levels.scale_out_pct == 0.8
        assert p.runtime_batch_size == 8

    def test_apply_strategy_configs_merges_grouped_values(self):
        p = BacktestParams().apply_strategy_configs(
            cpr_levels=CPRLevelsParams(
                cpr_shift_filter="HIGHER",
                min_effective_rr=2.5,
                use_narrowing_filter=True,
                cpr_entry_start="09:25",
                cpr_confirm_entry=False,
                cpr_hold_confirm=True,
                cpr_min_close_atr=0.2,
                scale_out_pct=0.75,
            ),
            fbr=FBRParams(
                failure_window=10,
                reversal_buffer_pct=0.002,
                fbr_min_or_atr=0.7,
                fbr_failure_depth=0.4,
                fbr_entry_window_end="10:35",
                use_narrowing_filter=True,
            ),
            virgin_cpr=VirginCPRParams(
                vcpr_confirm_candles=2,
                vcpr_body_pct=0.3,
                vcpr_sl_mode="EDGE",
                candle_exit=4,
                vcpr_scan_start="09:45",
                vcpr_scan_end="12:00",
                vcpr_min_open_dist_atr=0.5,
            ),
        )

        assert p.cpr_levels.cpr_shift_filter == "HIGHER"
        assert p.cpr_levels.min_effective_rr == 2.5
        assert p.cpr_levels.use_narrowing_filter is True
        assert p.cpr_levels.cpr_entry_start == "09:25"
        assert p.cpr_levels.cpr_hold_confirm is True
        assert p.cpr_levels.cpr_min_close_atr == 0.2
        assert p.cpr_levels.scale_out_pct == 0.75
        assert p.fbr.failure_window == 10
        assert p.fbr.fbr_min_or_atr == 0.7
        assert p.fbr.fbr_failure_depth == 0.4
        assert p.fbr.fbr_entry_window_end == "10:35"
        assert p.virgin_cpr.vcpr_confirm_candles == 2
        assert p.virgin_cpr.vcpr_body_pct == 0.3
        assert p.virgin_cpr.vcpr_sl_mode == "EDGE"
        assert p.virgin_cpr.candle_exit == 4
        assert p.virgin_cpr.vcpr_scan_start == "09:45"
        assert p.virgin_cpr.vcpr_scan_end == "12:00"
        assert p.virgin_cpr.vcpr_min_open_dist_atr == 0.5

    def test_long_gap_override_applies_only_to_longs(self):
        p = BacktestParams(max_gap_pct=1.5, long_max_gap_pct=1.0)

        assert p.max_gap_for_direction("LONG") == 1.0
        assert p.max_gap_for_direction("SHORT") == 1.5

    def test_version_removed_from_backtest_params(self):
        """version field should no longer be a BacktestParams dataclass field."""
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(BacktestParams())}
        assert "version" not in field_names

    def test_strategy_version_constant_value(self):
        """STRATEGY_VERSION must be the same string that was in the hash before."""
        assert STRATEGY_VERSION == "cpr-atr-v3"

    def test_make_run_id_is_unique_per_call(self):
        """run_id must be a unique UUID-based ID (12 hex chars), different on each call."""
        params = BacktestParams()
        bt = CPRATRBacktest(params, db=type("DB", (), {})())
        run_id_1 = bt._make_run_id(["SBIN"], "2023-01-01", "2023-01-31")
        run_id_2 = bt._make_run_id(["SBIN"], "2023-01-01", "2023-01-31")
        assert len(run_id_1) == 12
        assert len(run_id_2) == 12
        assert run_id_1 != run_id_2  # same params → different run_ids
        assert all(c in "0123456789abcdef" for c in run_id_1)

    def test_make_param_signature_is_deterministic(self):
        """param_signature must be deterministic for same inputs."""
        params = BacktestParams()
        bt = CPRATRBacktest(params, db=type("DB", (), {})())
        sig_1 = bt._make_param_signature(["SBIN"], "2023-01-01", "2023-01-31")
        sig_2 = bt._make_param_signature(["SBIN"], "2023-01-01", "2023-01-31")
        assert len(sig_1) == 12
        assert sig_1 == sig_2  # same params → same signature
        assert all(c in "0123456789abcdef" for c in sig_1)

    def test_param_signature_changes_with_params(self):
        """param_signature must change when params differ."""
        params_a = BacktestParams()
        params_b = BacktestParams(cpr_percentile=50.0)
        bt_a = CPRATRBacktest(params_a, db=type("DB", (), {})())
        bt_b = CPRATRBacktest(params_b, db=type("DB", (), {})())
        sig_a = bt_a._make_param_signature(["SBIN"], "2023-01-01", "2023-01-31")
        sig_b = bt_b._make_param_signature(["SBIN"], "2023-01-01", "2023-01-31")
        assert sig_a != sig_b


class TestRuntimeValidation:
    class _CoverageDB:
        def __init__(self, coverage: dict[str, list[str]]):
            self.coverage = coverage
            self.last_symbols: list[str] = []

        def get_missing_runtime_symbol_coverage(self, symbols: list[str]) -> dict[str, list[str]]:
            self.last_symbols = list(symbols)
            return self.coverage

    def test_marketdb_runtime_coverage_reports_missing_symbols(self, tmp_path):
        db = MarketDB(db_path=tmp_path / "runtime-coverage.duckdb")
        try:
            initial = db.get_missing_runtime_symbol_coverage(["SBIN", "TCS"])
            assert initial["market_day_state"] == ["SBIN", "TCS"]
            assert initial["strategy_day_state"] == ["SBIN", "TCS"]
            assert initial["intraday_day_pack"] == ["SBIN", "TCS"]

            db.con.execute("""
                CREATE TABLE market_day_state (
                    symbol VARCHAR,
                    trade_date DATE
                )
            """)
            db.con.execute("""
                CREATE TABLE strategy_day_state (
                    symbol VARCHAR,
                    trade_date DATE
                )
            """)
            db.con.execute("""
                CREATE TABLE intraday_day_pack (
                    symbol VARCHAR,
                    trade_date DATE
                )
            """)
            db.con.execute("INSERT INTO market_day_state VALUES ('SBIN', DATE '2023-01-01')")
            db.con.execute("INSERT INTO strategy_day_state VALUES ('INFY', DATE '2023-01-01')")
            db.con.execute("INSERT INTO intraday_day_pack VALUES ('TCS', DATE '2023-01-01')")

            coverage = db.get_missing_runtime_symbol_coverage(["SBIN", "TCS", "INFY"])
            assert coverage["market_day_state"] == ["INFY", "TCS"]
            assert coverage["strategy_day_state"] == ["SBIN", "TCS"]
            assert coverage["intraday_day_pack"] == ["INFY", "SBIN"]
        finally:
            db.close()

    def test_run_fails_fast_when_runtime_coverage_is_missing(self):
        events: list[dict[str, object]] = []
        db = self._CoverageDB(
            {
                "market_day_state": ["SBIN"],
                "strategy_day_state": ["INFY"],
                "intraday_day_pack": ["TCS"],
            }
        )
        bt = CPRATRBacktest(params=BacktestParams(), db=db)

        with pytest.raises(RuntimeError) as exc:
            bt.run(
                symbols=["SBIN", "TCS"],
                start="2023-01-01",
                end="2023-01-31",
                verbose=False,
                use_cache=False,
                progress_hook=events.append,
            )

        msg = str(exc.value)
        assert "market_day_state missing 1 symbol(s): SBIN" in msg
        assert "strategy_day_state missing 1 symbol(s): INFY" in msg
        assert "intraday_day_pack missing 1 symbol(s): TCS" in msg
        assert "uv run pivot-build --force --batch-size 128" in msg
        assert db.last_symbols == ["SBIN", "TCS"]

        event_names = [str(e["event"]) for e in events]
        assert "runtime_validate_start" in event_names
        assert "runtime_validate_done" in event_names
        assert "runtime_validate_failed" in event_names
        assert "runtime_ensure_start" not in event_names

        validate_done = next(e for e in events if e["event"] == "runtime_validate_done")
        assert validate_done["coverage_ok"] is False
        assert validate_done["missing_state"] == 1
        assert validate_done["missing_strategy"] == 1
        assert validate_done["missing_pack"] == 1

    def test_run_emits_runtime_validate_events_when_coverage_is_complete(self):
        events: list[dict[str, object]] = []
        db = self._CoverageDB(
            {
                "market_day_state": [],
                "strategy_day_state": [],
                "intraday_day_pack": [],
            }
        )
        bt = CPRATRBacktest(params=BacktestParams(), db=db)
        bt_any: Any = bt
        bt_any._get_all_setups_batch = lambda symbols, start, end: pl.DataFrame()

        result = bt.run(
            symbols=["SBIN"],
            start="2023-01-01",
            end="2023-01-31",
            verbose=False,
            use_cache=False,
            progress_hook=events.append,
        )

        assert result.df.is_empty()
        assert db.last_symbols == ["SBIN"]

        event_names = [str(e["event"]) for e in events]
        assert "runtime_validate_start" in event_names
        assert "runtime_validate_done" in event_names
        assert "runtime_validate_failed" not in event_names

        validate_done = next(e for e in events if e["event"] == "runtime_validate_done")
        assert validate_done["coverage_ok"] is True
        assert validate_done["missing_state"] == 0
        assert validate_done["missing_strategy"] == 0
        assert validate_done["missing_pack"] == 0

    def test_run_fetches_all_setups_in_one_query(self):
        """Setup is fetched once for all symbols regardless of runtime_batch_size.

        runtime_batch_size now controls day-pack batching only; setup is always
        fetched in a single query for the full symbol list.
        """
        events: list[dict[str, object]] = []
        db = self._CoverageDB(
            {
                "market_day_state": [],
                "strategy_day_state": [],
                "intraday_day_pack": [],
            }
        )
        # batch_size=1 → if setup were still batched, we'd see 2 calls; now expect exactly 1
        bt = CPRATRBacktest(params=BacktestParams(runtime_batch_size=1), db=db)
        seen_batches: list[list[str]] = []

        def fake_get_all_setups(symbols: list[str], start: str, end: str) -> pl.DataFrame:
            seen_batches.append(list(symbols))
            return pl.DataFrame()

        bt_any: Any = bt
        bt_any._get_all_setups_batch = fake_get_all_setups

        result = bt.run(
            symbols=["SBIN", "TCS"],
            start="2023-01-01",
            end="2023-01-31",
            verbose=False,
            use_cache=False,
            progress_hook=events.append,
        )

        assert result.df.is_empty()
        # Single setup query covering all symbols — no batching at setup phase
        assert len(seen_batches) == 1, f"Expected 1 setup call, got {len(seen_batches)}"
        assert set(seen_batches[0]) == {"SBIN", "TCS"}

        # batch_plan reflects the pack-batch count (0 when no setups found)
        plan = next(e for e in events if e["event"] == "batch_plan")
        assert plan["runtime_batch_size"] == 1
        assert plan["batch_count"] == 0  # no symbols had setups

        # Exactly one setup_fetch_start event (not one per batch as before)
        setup_starts = [e for e in events if e["event"] == "setup_fetch_start"]
        assert len(setup_starts) == 1


class TestTradeInspection:
    def test_get_trade_inspection_returns_daily_cpr_and_key_candles(self, tmp_path):
        db = MarketDB(db_path=tmp_path / "trade-inspection.duckdb")
        try:
            db.con.execute("""
                CREATE TABLE backtest_results (
                    run_id VARCHAR,
                    symbol VARCHAR,
                    trade_date DATE,
                    direction VARCHAR,
                    entry_time VARCHAR,
                    exit_time VARCHAR,
                    entry_price DOUBLE,
                    exit_price DOUBLE,
                    sl_price DOUBLE,
                    target_price DOUBLE,
                    profit_loss DOUBLE,
                    profit_loss_pct DOUBLE,
                    exit_reason VARCHAR,
                    atr DOUBLE,
                    position_size INTEGER,
                    position_value DOUBLE
                )
            """)
            db.con.execute("""
                CREATE TABLE run_metadata (
                    run_id VARCHAR,
                    params_json VARCHAR
                )
            """)
            db.con.execute("""
                CREATE TABLE market_day_state (
                    symbol VARCHAR,
                    trade_date DATE,
                    prev_date DATE,
                    prev_close DOUBLE,
                    "pivot" DOUBLE,
                    bc DOUBLE,
                    tc DOUBLE,
                    cpr_width_pct DOUBLE,
                    r1 DOUBLE,
                    s1 DOUBLE,
                    cpr_shift VARCHAR,
                    is_narrowing INTEGER,
                    cpr_threshold_pct DOUBLE,
                    atr DOUBLE,
                    open_915 DOUBLE,
                    or_close_5 DOUBLE,
                    gap_pct_open DOUBLE
                )
            """)
            db.con.execute("""
                CREATE TABLE cpr_daily (
                    symbol VARCHAR,
                    trade_date DATE,
                    prev_high DOUBLE,
                    prev_low DOUBLE,
                    prev_close DOUBLE
                )
            """)
            db.con.execute("""
                CREATE TABLE strategy_day_state (
                    symbol VARCHAR,
                    trade_date DATE,
                    open_side VARCHAR,
                    open_to_cpr_atr DOUBLE,
                    gap_abs_pct DOUBLE,
                    or_atr_5 DOUBLE,
                    direction_5 VARCHAR
                )
            """)
            db.con.execute("""
                CREATE TABLE candles_5min (
                    symbol VARCHAR,
                    date DATE,
                    candle_time TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE
                )
            """)
            db.con.execute("CREATE OR REPLACE VIEW v_5min AS SELECT * FROM candles_5min")
            db._has_5min = True

            db.con.execute("""
                INSERT INTO backtest_results VALUES
                ('run123', 'TEST', DATE '2026-02-11', 'LONG', '09:20', '09:55',
                 11.57, 12.0958333333, 11.4119891667, 12.0333333333,
                 7294.36, 4.5448, 'TRAILING_SL', 0.1441666667, 17200, 198964.0)
            """)
            db.con.execute("""
                INSERT INTO run_metadata VALUES
                ('run123', '{"buffer_pct":0.0005,"cpr_levels":{"cpr_min_close_atr":0.35}}')
            """)
            db.con.execute("""
                INSERT INTO market_day_state VALUES
                ('TEST', DATE '2026-02-11', DATE '2026-02-10', 11.50,
                 11.4566666667, 11.435, 11.4783333333, 0.3782368344,
                 12.0333333333, 10.9233333333, 'HIGHER', 1, 0.6160345170,
                 0.1441666667, 11.50, 11.57, 0.0)
            """)
            db.con.execute("""
                INSERT INTO cpr_daily VALUES
                ('TEST', DATE '2026-02-11', 11.99, 10.88, 11.50)
            """)
            db.con.execute("""
                INSERT INTO strategy_day_state VALUES
                ('TEST', DATE '2026-02-11', 'ABOVE', 0.1502890173, 0.0, 1.8728323699, 'LONG')
            """)
            db.con.execute("""
                INSERT INTO candles_5min VALUES
                ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:15:00', 11.50, 11.73, 11.46, 11.57, 1000),
                ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:20:00', 11.57, 11.57, 11.45, 11.56, 1200),
                ('TEST', DATE '2026-02-11', TIMESTAMP '2026-02-11 09:55:00', 11.84, 12.34, 11.84, 12.24, 2000)
            """)

            details = db.get_trade_inspection("run123", "TEST", "2026-02-11", "09:20", "09:55")

            assert details is not None
            assert details["trade"]["direction"] == "LONG"
            assert details["daily_cpr"]["prev_date"] == "2026-02-10"
            assert details["daily_cpr"]["tc"] == pytest.approx(11.4783333333)
            assert details["daily_cpr"]["r1"] == pytest.approx(12.0333333333)
            assert details["derived"]["trigger_price"] == pytest.approx(11.4840725)
            assert details["derived"]["min_signal_close"] == pytest.approx(11.528791666645)
            assert details["candles"]["09:15"]["close"] == pytest.approx(11.57)
            assert details["candles"]["09:20"]["open"] == pytest.approx(11.57)
            assert details["candles"]["09:55"]["high"] == pytest.approx(12.34)
        finally:
            db.close()


class TestTradeResult:
    """Verify TradeResult dataclass."""

    def test_default_values(self):
        t = TradeResult(run_id="abc123", symbol="SBIN", trade_date="2023-01-01", direction="LONG")
        assert t.profit_loss == 0.0
        assert t.exit_reason == ""
        assert t.mfe_r == 0.0
        assert t.mae_r == 0.0

    def test_full_trade(self):
        t = TradeResult(
            run_id="abc123",
            symbol="SBIN",
            trade_date="2023-01-01",
            direction="LONG",
            entry_price=520.0,
            exit_price=530.0,
            sl_price=515.0,
            target_price=535.0,
            profit_loss=5000.0,
            exit_reason="TARGET",
        )
        assert t.profit_loss == 5000.0
        assert t.exit_reason == "TARGET"


class TestDayPack:
    def test_baseline_for_time_lookup(self):
        pack = DayPack(
            time_str=["09:15", "09:20", "09:25"],
            opens=[100.0, 101.0, 102.0],
            highs=[101.0, 102.0, 103.0],
            lows=[99.0, 100.0, 101.0],
            closes=[100.5, 101.5, 102.5],
            volumes=[1000.0, 1100.0, 1200.0],
            rvol_baseline=[900.0, None, 1300.0],
        )
        assert pack.baseline_for_time("09:15") == 900.0
        assert pack.baseline_for_time("09:20") == 0.0
        assert pack.baseline_for_time("10:00") == 0.0

    def test_to_frame_materializes_expected_columns(self):
        pack = DayPack(
            time_str=["09:15"],
            opens=[100.0],
            highs=[101.0],
            lows=[99.0],
            closes=[100.5],
            volumes=[1000.0],
            rvol_baseline=[900.0],
        )
        df = pack.to_frame()
        assert df.columns == ["time_str", "open", "high", "low", "close", "volume"]
        assert df.height == 1


class TestPhase7PackCompaction:
    def test_minute_to_time_str(self):
        assert CPRATRBacktest._minute_to_time_str(555) == "09:15"
        assert CPRATRBacktest._minute_to_time_str(930) == "15:30"
        assert CPRATRBacktest._minute_to_time_str("560") == "09:20"

    def test_resolve_pack_time_mode_uses_minute_arr_when_available(self):
        class _DB:
            @staticmethod
            def _table_has_column(table: str, column: str) -> bool:
                return table == "intraday_day_pack" and column == "minute_arr"

        bt = CPRATRBacktest(params=BacktestParams(), db=_DB())
        assert bt._resolve_pack_time_mode() == "minute_arr"
        # Cached decision should remain stable on subsequent calls.
        assert bt._resolve_pack_time_mode() == "minute_arr"

    def test_resolve_pack_time_mode_falls_back_to_time_arr_without_probe(self):
        class _DB:
            pass

        bt = CPRATRBacktest(params=BacktestParams(), db=_DB())
        assert bt._resolve_pack_time_mode() == "time_arr"


class TestBacktestResultMetrics:
    def test_advanced_metrics_use_portfolio_base_for_multi_symbol_runs(self):
        df = pl.DataFrame(
            {
                "run_id": ["r1", "r1", "r1"],
                "symbol": ["SBIN", "TCS", "SBIN"],
                "trade_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "profit_loss": [1000.0, -500.0, -500.0],
                "exit_reason": ["TARGET", "INITIAL_SL", "TRAILING_SL"],
            }
        )
        res = BacktestResult(
            run_id="r1",
            params=BacktestParams(capital=100_000),
            _loaded_df=df,
            run_context={"start_date": "2023-01-01", "end_date": "2023-12-31"},
        )

        adv = res._advanced_metrics()
        assert adv["max_dd_abs"] == -1000.0
        # New baseline uses shared portfolio equity (default 10L), not capital x symbols.
        assert adv["max_dd_pct"] == 0.1

    def test_advanced_metrics_annualization_uses_run_context_window(self):
        df = pl.DataFrame(
            {
                "run_id": ["r2"],
                "symbol": ["SBIN"],
                "trade_date": ["2023-06-15"],
                "profit_loss": [1000.0],
                "exit_reason": ["TARGET"],
            }
        )
        res = BacktestResult(
            run_id="r2",
            params=BacktestParams(capital=100_000),
            _loaded_df=df,
            run_context={"start_date": "2023-01-01", "end_date": "2023-12-31"},
        )

        adv = res._advanced_metrics()
        # Full-year denominator now uses the default 10L portfolio base: 1000/10L = 0.1%.
        assert 0.09 <= adv["annual_return_pct"] <= 0.11

    def test_validate_rejects_mixed_run_ids(self):
        df = pl.DataFrame(
            {
                "run_id": ["r2", "r3"],
                "symbol": ["SBIN", "TCS"],
                "trade_date": ["2023-06-15", "2023-06-16"],
                "profit_loss": [1000.0, -250.0],
                "exit_reason": ["TARGET", "INITIAL_SL"],
            }
        )
        res = BacktestResult(run_id="r2", params=BacktestParams(), _loaded_df=df)

        with pytest.raises(ValueError, match="Mixed run_id values"):
            res.validate()

    def test_save_to_db_invalidates_dashboard_run_cache(self, monkeypatch):
        df = pl.DataFrame(
            {
                "run_id": ["r4"],
                "symbol": ["SBIN"],
                "trade_date": ["2023-06-15"],
                "profit_loss": [1000.0],
                "exit_reason": ["TARGET"],
            }
        )
        res = BacktestResult(run_id="r4", params=BacktestParams(), _loaded_df=df)

        calls: list[str] = []

        class _FakeCon:
            def execute(self, *args, **kwargs):
                return None

            def register(self, *args, **kwargs):
                return None

            def unregister(self, *args, **kwargs):
                return None

        class _FakeDB:
            con = _FakeCon()

            def store_run_metadata(self, **kwargs):
                calls.append("metadata")

            def store_backtest_results(
                self,
                results_df,
                execution_mode: str | None = None,
                transactional: bool = True,
            ):
                calls.append(f"results:{execution_mode or 'BACKTEST'}")
                return results_df.height

        monkeypatch.setattr("engine.cpr_atr_strategy.get_db", lambda: _FakeDB())
        monkeypatch.setitem(
            sys.modules,
            "web.state",
            type(
                "_FakeWebStateModule",
                (),
                {
                    "invalidate_run_cache": staticmethod(
                        lambda run_id=None: calls.append(f"invalidate:{run_id}")
                    )
                },
            )(),
        )

        row_count = res.save_to_db()

        assert row_count == 1
        assert calls == ["metadata", "results:BACKTEST", "invalidate:r4"]

    def test_save_to_db_passes_paper_execution_mode(self, monkeypatch):
        df = pl.DataFrame(
            {
                "run_id": ["paper-1"],
                "symbol": ["SBIN"],
                "trade_date": ["2023-06-15"],
                "profit_loss": [1000.0],
                "exit_reason": ["TARGET"],
            }
        )
        res = BacktestResult(run_id="paper-1", params=BacktestParams(), _loaded_df=df)

        calls: list[tuple[str, object]] = []

        class _FakeCon:
            def execute(self, *args, **kwargs):
                return None

            def register(self, *args, **kwargs):
                return None

            def unregister(self, *args, **kwargs):
                return None

        class _FakeDB:
            con = _FakeCon()

            def store_run_metadata(self, **kwargs):
                calls.append(("metadata", kwargs["execution_mode"]))

            def store_backtest_results(
                self,
                results_df,
                execution_mode: str | None = None,
                transactional: bool = True,
            ):
                calls.append(("results", execution_mode))
                assert execution_mode == "PAPER"
                assert results_df["run_id"][0] == "paper-1"
                return results_df.height

        monkeypatch.setattr("engine.cpr_atr_strategy.get_db", lambda: _FakeDB())
        monkeypatch.setitem(
            sys.modules,
            "web.state",
            type(
                "_FakeWebStateModule",
                (),
                {
                    "invalidate_run_cache": staticmethod(
                        lambda run_id=None: calls.append(("invalidate", run_id))
                    )
                },
            )(),
        )

        row_count = res.save_to_db(execution_mode="PAPER")

        assert row_count == 1
        assert calls[0] == ("metadata", "PAPER")
        assert calls[1] == ("results", "PAPER")
        assert calls[2] == ("invalidate", "paper-1")

    def test_summary_includes_time_of_day_breakdown(self):
        df = pl.DataFrame(
            {
                "run_id": ["r5", "r5", "r5", "r5"],
                "symbol": ["SBIN", "SBIN", "TCS", "RELIANCE"],
                "trade_date": ["2023-06-01", "2023-06-02", "2023-06-03", "2023-06-04"],
                "entry_time": ["09:20", "09:45", "10:05", "10:35"],
                "profit_loss": [100.0, -50.0, 250.0, -25.0],
                "exit_reason": ["TARGET", "INITIAL_SL", "TARGET", "TIME"],
            }
        )
        res = BacktestResult(run_id="r5", params=BacktestParams(), _loaded_df=df)

        summary = res.summary()

        assert "TIME OF DAY PNL" in summary
        assert "09:00" in summary
        assert "10:00" in summary
        assert "Rs.+50.00" in summary


class TestPortfolioExecutionOverlay:
    def test_apply_portfolio_constraints_limits_concurrent_positions(self):
        bt = CPRATRBacktest(
            params=BacktestParams(portfolio_value=100_000, max_positions=1),
            db=object(),
        )
        trades = [
            TradeResult(
                run_id="r1",
                symbol="SBIN",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:20",
                exit_time="09:40",
                entry_price=100.0,
                exit_price=102.0,
                sl_price=99.0,
                target_price=102.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=10,
            ),
            TradeResult(
                run_id="r1",
                symbol="TCS",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:25",
                exit_time="09:45",
                entry_price=200.0,
                exit_price=204.0,
                sl_price=198.0,
                target_price=204.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=10,
            ),
        ]

        executed, stats = bt._apply_portfolio_constraints(trades)

        assert len(executed) == 1
        assert executed[0].symbol == "SBIN"
        assert executed[0].position_value == 10_000.0
        assert executed[0].position_size == 100
        assert executed[0].gross_pnl == 200.0
        assert executed[0].total_costs == 50.89
        assert executed[0].profit_loss == 149.11
        assert stats["candidate_trade_count"] == 2
        assert stats["executed_trade_count"] == 1
        assert stats["not_executed_portfolio"] == 1
        assert stats["skipped_no_slots"] == 1

    def test_apply_portfolio_constraints_can_use_risk_based_sizing(self):
        bt = CPRATRBacktest(
            params=BacktestParams(portfolio_value=100_000, max_positions=1, risk_based_sizing=True),
            db=object(),
        )
        trades = [
            TradeResult(
                run_id="r1",
                symbol="SBIN",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:20",
                exit_time="09:40",
                entry_price=100.0,
                exit_price=102.0,
                sl_price=99.0,
                target_price=102.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=10,
            ),
            TradeResult(
                run_id="r1",
                symbol="TCS",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:25",
                exit_time="09:45",
                entry_price=200.0,
                exit_price=204.0,
                sl_price=198.0,
                target_price=204.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=10,
            ),
        ]

        executed, stats = bt._apply_portfolio_constraints(trades)

        assert len(executed) == 1
        assert executed[0].symbol == "SBIN"
        assert executed[0].gross_pnl == 20.0
        assert executed[0].total_costs > 0
        assert executed[0].position_size == 10
        assert executed[0].position_value == 1_000.0
        assert stats["candidate_trade_count"] == 2
        assert stats["executed_trade_count"] == 1
        assert stats["not_executed_portfolio"] == 1
        assert stats["skipped_no_slots"] == 1

    def test_apply_portfolio_constraints_recomputes_profit_loss_pct(self):
        bt = CPRATRBacktest(
            params=BacktestParams(portfolio_value=100_000, max_positions=1),
            db=object(),
        )
        trades = [
            TradeResult(
                run_id="r1",
                symbol="SBIN",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:20",
                exit_time="09:40",
                entry_price=100.0,
                exit_price=102.0,
                sl_price=99.0,
                target_price=102.0,
                profit_loss=2_000.0,
                profit_loss_pct=99.9,
                exit_reason="TARGET",
                position_size=10,
            ),
        ]

        executed, stats = bt._apply_portfolio_constraints(trades)

        assert len(executed) == 1
        assert executed[0].position_size == 100
        assert executed[0].position_value == 10_000.0
        assert executed[0].profit_loss_pct != pytest.approx(99.9)
        assert executed[0].profit_loss_pct == pytest.approx(
            executed[0].profit_loss / executed[0].position_value * 100,
            abs=0.0001,
        )
        assert stats["executed_trade_count"] == 1

    def test_apply_portfolio_constraints_allows_zero_max_position_pct(self):
        bt = CPRATRBacktest(
            params=BacktestParams(
                portfolio_value=100_000,
                max_positions=10,
                max_position_pct=0.0,
            ),
            db=object(),
        )
        trades = [
            TradeResult(
                run_id="r1",
                symbol="SBIN",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:20",
                exit_time="09:40",
                entry_price=100.0,
                exit_price=102.0,
                sl_price=99.0,
                target_price=102.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=10,
            ),
        ]

        executed, stats = bt._apply_portfolio_constraints(trades)

        assert len(executed) == 1
        assert executed[0].position_size == 100
        assert executed[0].position_value == 10_000.0
        assert stats["executed_trade_count"] == 1
        assert stats["not_executed_portfolio"] == 0

    def test_apply_portfolio_constraints_risk_based_sizing_caps_to_slot_capital(self):
        bt = CPRATRBacktest(
            params=BacktestParams(
                portfolio_value=1_000_000,
                max_positions=10,
                max_position_pct=0.10,
                risk_based_sizing=True,
            ),
            db=object(),
        )
        trades = [
            TradeResult(
                run_id="r1",
                symbol="SBIN",
                trade_date="2023-01-02",
                direction="LONG",
                entry_time="09:20",
                exit_time="09:40",
                entry_price=100.0,
                exit_price=102.0,
                sl_price=99.0,
                target_price=102.0,
                profit_loss=2_000.0,
                profit_loss_pct=2.0,
                exit_reason="TARGET",
                position_size=6_000,
            ),
        ]

        executed, stats = bt._apply_portfolio_constraints(trades)

        assert len(executed) == 1
        assert executed[0].symbol == "SBIN"
        assert executed[0].position_size == 1_000
        assert executed[0].position_value == 100_000.0
        assert stats["executed_trade_count"] == 1
        assert stats["not_executed_portfolio"] == 0

    def test_cpr_levels_risk_based_sizing_uses_actual_entry_price(self):
        params = BacktestParams(
            risk_based_sizing=True, capital=100_000, risk_pct=0.01
        ).apply_strategy_configs(
            cpr_levels=CPRLevelsParams(
                cpr_shift_filter="ALL",
                min_effective_rr=2.0,
                use_narrowing_filter=False,
                cpr_entry_start="",
                cpr_confirm_entry=False,
                cpr_hold_confirm=False,
                cpr_min_close_atr=0.0,
                scale_out_pct=0.0,
            )
        )
        bt = CPRATRBacktest(params=params, db=object())

        captured: dict[str, float] = {}

        def fake_simulate_trade(**kwargs):
            captured.update(
                {
                    "entry_price": float(kwargs["entry_price"]),
                    "sl_price": float(kwargs["sl_price"]),
                    "sl_distance": float(kwargs["sl_distance"]),
                    "position_size": float(kwargs["position_size"]),
                }
            )
            return TradeResult(
                run_id=kwargs["run_id"],
                symbol=kwargs["symbol"],
                trade_date=kwargs["trade_date"],
                direction=kwargs["direction"],
                entry_time=kwargs["entry_time"],
                exit_time="09:25",
                entry_price=float(kwargs["entry_price"]),
                exit_price=float(kwargs["sl_price"]),
                sl_price=float(kwargs["sl_price"]),
                target_price=float(kwargs["target_price"]),
                profit_loss=-1.0,
                profit_loss_pct=-1.0,
                exit_reason="INITIAL_SL",
                position_size=int(kwargs["position_size"]),
                position_value=round(
                    float(kwargs["position_size"]) * float(kwargs["entry_price"]), 2
                ),
            )

        bt._simulate_trade = fake_simulate_trade  # type: ignore[method-assign]

        setup_row = {
            "trade_date": "2026-03-20",
            "direction": "LONG",
            "atr": 0.6,
            "cpr_width_pct": 0.1,
            "cpr_threshold": 33.0,
            "high_915": 10.1,
            "low_915": 9.7,
            "open_915": 9.8,
            "tc": 10.0,
            "bc": 9.9,
            "r1": 12.0,
            "s1": 9.0,
            "prev_day_close": 9.75,
        }
        day_pack = DayPack(
            time_str=["09:15", "09:20"],
            opens=[9.8, 10.5],
            highs=[10.1, 10.7],
            lows=[9.7, 10.4],
            closes=[10.05, 10.6],
            volumes=[1000.0, 2000.0],
            rvol_baseline=[1000.0, 2000.0],
        )

        trade = bt._simulate_day_cpr_levels("NITCO", "run-x", setup_row, day_pack)

        assert trade is not None
        assert captured["entry_price"] == pytest.approx(10.5)
        assert captured["sl_price"] == pytest.approx(9.9)
        assert captured["sl_distance"] == pytest.approx(0.6)
        assert captured["position_size"] == pytest.approx(1666.0, abs=1.0)

    def test_cpr_levels_compound_risk_uses_capital_base(self):
        params = BacktestParams(
            risk_based_sizing=True,
            compound_equity=True,
            capital=100_000,
            portfolio_value=1_000_000,
            risk_pct=0.01,
        ).apply_strategy_configs(
            cpr_levels=CPRLevelsParams(
                cpr_shift_filter="ALL",
                min_effective_rr=2.0,
                use_narrowing_filter=False,
                cpr_entry_start="",
                cpr_confirm_entry=False,
                cpr_hold_confirm=False,
                cpr_min_close_atr=0.0,
                scale_out_pct=0.0,
            )
        )
        setup_row = {
            "trade_date": "2026-03-20",
            "direction": "LONG",
            "atr": 0.6,
            "cpr_width_pct": 0.1,
            "cpr_threshold": 33.0,
            "high_915": 10.1,
            "low_915": 9.7,
            "open_915": 9.8,
            "tc": 10.0,
            "bc": 9.9,
            "r1": 12.0,
            "s1": 9.0,
            "prev_day_close": 9.75,
        }
        day_pack = DayPack(
            time_str=["09:15", "09:20"],
            opens=[9.8, 10.5],
            highs=[10.1, 10.7],
            lows=[9.7, 10.4],
            closes=[10.05, 10.6],
            volumes=[1000.0, 2000.0],
            rvol_baseline=[1000.0, 2000.0],
        )

        low_cap = scan_cpr_levels_entry(
            day_pack=day_pack,
            setup_row=setup_row,
            params=params,
            scan_start_idx=0,
            scan_end_idx=1,
            capital_base=100_000.0,
        )
        high_cap = scan_cpr_levels_entry(
            day_pack=day_pack,
            setup_row=setup_row,
            params=params,
            scan_start_idx=0,
            scan_end_idx=1,
            capital_base=200_000.0,
        )

        assert low_cap is not None
        assert high_cap is not None
        assert low_cap["position_size"] == pytest.approx(1666.0, abs=1.0)
        assert high_cap["position_size"] == pytest.approx(3333.0, abs=1.0)
        assert high_cap["position_size"] > low_cap["position_size"]

    def test_cpr_levels_batch_compound_risk_keeps_raw_qty_for_overlay(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        params = BacktestParams(
            risk_based_sizing=True,
            compound_equity=True,
            capital=100_000,
            portfolio_value=1_000_000,
            max_positions=1,
            max_position_pct=0.10,
            risk_pct=0.01,
        ).apply_strategy_configs(
            cpr_levels=CPRLevelsParams(
                cpr_shift_filter="ALL",
                min_effective_rr=2.0,
                use_narrowing_filter=False,
                cpr_entry_start="",
                cpr_confirm_entry=False,
                cpr_hold_confirm=False,
                cpr_min_close_atr=0.0,
                scale_out_pct=0.0,
            )
        )
        bt = CPRATRBacktest(params=params, db=object())

        def fake_scan_cpr_levels_entry(**kwargs):
            return {
                "direction": "LONG",
                "entry_idx": 0,
                "entry_time": "09:20",
                "entry_price": 100.0,
                "sl_price": 95.0,
                "target_price": 110.0,
                "runner_target_price": None,
                "first_target_price": 110.0,
                "scale_out_pct": 0.0,
                "sl_distance": 5.0,
                "position_size": 3000,
                "rr_ratio": 2.0,
                "rvol": 1.0,
                "or_atr_ratio": 1.0,
                "gap_pct": 0.0,
                "cpr_width_pct": 0.1,
                "cpr_threshold": 33.0,
            }

        captured: dict[str, float] = {}

        def fake_simulate_trade(**kwargs):
            captured["position_size"] = float(kwargs["position_size"])
            return TradeResult(
                run_id=kwargs["run_id"],
                symbol=kwargs["symbol"],
                trade_date=kwargs["trade_date"],
                direction=kwargs["direction"],
                entry_time=kwargs["entry_time"],
                exit_time="09:25",
                entry_price=float(kwargs["entry_price"]),
                exit_price=105.0,
                sl_price=float(kwargs["sl_price"]),
                target_price=float(kwargs["target_price"]),
                profit_loss=0.0,
                profit_loss_pct=0.0,
                exit_reason="TARGET",
                position_size=int(kwargs["position_size"]),
                position_value=round(
                    float(kwargs["position_size"]) * float(kwargs["entry_price"]), 2
                ),
            )

        monkeypatch.setattr(
            "engine.cpr_atr_strategy.scan_cpr_levels_entry",
            fake_scan_cpr_levels_entry,
        )
        monkeypatch.setattr(bt, "_simulate_trade", fake_simulate_trade)

        setup_row = {
            "trade_date": "2026-03-20",
            "direction": "LONG",
            "atr": 0.6,
            "cpr_width_pct": 0.1,
            "cpr_threshold": 33.0,
            "high_915": 10.1,
            "low_915": 9.7,
            "open_915": 9.8,
            "tc": 10.0,
            "bc": 9.9,
            "r1": 12.0,
            "s1": 9.0,
            "prev_day_close": 9.75,
        }
        day_pack = DayPack(
            time_str=["09:20"],
            opens=[100.0],
            highs=[101.0],
            lows=[99.0],
            closes=[100.5],
            volumes=[1000.0],
            rvol_baseline=[1000.0],
        )

        out = bt._simulate_cpr_levels_batch(
            run_id="run-x",
            batch_symbols=["SBIN"],
            setups_by_sym={"SBIN": pl.DataFrame([setup_row])},
            candles_by_sym={"SBIN": {"2026-03-20": day_pack}},
        )

        assert out["SBIN"]
        assert captured["position_size"] == pytest.approx(3000.0)

    def test_scan_cpr_levels_entry_skips_early_failed_candidate(self):
        params = BacktestParams(
            min_price=50.0,
            skip_rvol_check=False,
            rvol_threshold=1.0,
            cpr_levels_config=CPRLevelsParams(
                cpr_shift_filter="ALL",
                min_effective_rr=2.0,
                use_narrowing_filter=True,
                cpr_entry_start="",
                cpr_confirm_entry=False,
                cpr_hold_confirm=False,
                cpr_min_close_atr=0.5,
                scale_out_pct=0.0,
            ),
        )
        setup_row = {
            "direction": "LONG",
            "atr": 10.0,
            "cpr_width_pct": 0.1,
            "cpr_threshold": 33.0,
            "high_915": 110.0,
            "low_915": 90.0,
            "open_915": 100.0,
            "tc": 101.0,
            "bc": 99.0,
            "r1": 130.0,
            "s1": 80.0,
            "prev_day_close": 100.0,
            "is_narrowing": True,
            "open_to_cpr_atr": 1.5,
        }
        day_pack = DayPack(
            time_str=["09:20", "09:25"],
            opens=[105.5, 106.1],
            highs=[106.0, 106.6],
            lows=[105.0, 106.0],
            closes=[105.5, 106.1],
            volumes=[1000.0, 1000.0],
            rvol_baseline=[1000.0, 1000.0],
        )

        candidate = scan_cpr_levels_entry(
            day_pack=day_pack,
            setup_row=setup_row,
            params=params,
            scan_start_idx=0,
            scan_end_idx=1,
        )

        assert candidate is not None
        assert candidate["entry_idx"] == 1
        assert candidate["entry_time"] == "09:25"
        assert candidate["entry_price"] == pytest.approx(106.1)


class TestRunMetricsMaterialization:
    def test_run_metrics_uses_metadata_capital_and_window(self, tmp_path):
        db = MarketDB(db_path=tmp_path / "run-metrics.duckdb")
        try:
            run_id = "run-metrics-test"
            db.store_run_metadata(
                run_id=run_id,
                strategy="CPR_LEVELS",
                label="CPR test window",
                symbols=["SBIN", "TCS"],
                start_date="2023-01-01",
                end_date="2023-12-31",
                params={"capital": 200000},
            )
            df = pl.DataFrame(
                {
                    "run_id": [run_id, run_id],
                    "symbol": ["SBIN", "TCS"],
                    "trade_date": ["2023-02-01", "2023-02-02"],
                    "direction": ["LONG", "SHORT"],
                    "entry_time": ["09:20", "09:25"],
                    "exit_time": ["10:00", "10:05"],
                    "entry_price": [100.0, 200.0],
                    "exit_price": [101.0, 199.5],
                    "sl_price": [99.5, 200.5],
                    "target_price": [102.0, 198.0],
                    "profit_loss": [1000.0, -500.0],
                    "profit_loss_pct": [1.0, -0.25],
                    "exit_reason": ["TARGET", "INITIAL_SL"],
                    "sl_phase": ["TRAIL", "PROTECT"],
                    "atr": [2.0, 2.5],
                    "cpr_width_pct": [0.4, 0.5],
                    "mfe_r": [1.5, 0.3],
                    "mae_r": [-0.2, -1.0],
                    "or_atr_ratio": [0.7, 0.9],
                    "gap_pct": [0.2, -0.1],
                }
            )
            db.store_backtest_results(df)

            row = db.con.execute(
                """
                SELECT strategy, strategy_code, label, start_date::VARCHAR, end_date::VARCHAR,
                       allocated_capital, total_pnl, total_return_pct, max_dd_pct, annual_return_pct, calmar
                FROM run_metrics
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()

            assert row is not None
            assert row[0] == "CPR_LEVELS"
            assert row[1] == "CPR_LEVELS"
            assert row[2] == "CPR test window"
            assert str(row[3])[:10] == "2023-01-01"
            assert str(row[4])[:10] == "2023-12-31"
            assert float(row[5]) == 400000.0
            assert float(row[6]) == 500.0
            assert 0.12 <= float(row[7]) <= 0.13
            # 2 symbols x 200000 capital = 400000; max_dd_abs=500 => 0.125% -> rounds 0.13
            assert 0.12 <= float(row[8]) <= 0.13
            # Annualized return over full-year window: 500/400000 ~= 0.125%
            assert 0.12 <= float(row[9]) <= 0.13
            # Calmar ~= annual_return / max_dd_pct ~= 1
            assert 0.95 <= float(row[10]) <= 1.05
        finally:
            db.close()

    def test_run_metrics_prefers_portfolio_value_when_present(self, tmp_path):
        db = MarketDB(db_path=tmp_path / "run-metrics-portfolio.duckdb")
        try:
            run_id = "run-metrics-portfolio"
            db.store_run_metadata(
                run_id=run_id,
                strategy="CPR_LEVELS",
                label="Portfolio test window",
                symbols=["SBIN", "TCS"],
                start_date="2023-01-01",
                end_date="2023-12-31",
                params={"capital": 200000, "portfolio_value": 1000000},
            )
            df = pl.DataFrame(
                {
                    "run_id": [run_id, run_id],
                    "symbol": ["SBIN", "TCS"],
                    "trade_date": ["2023-02-01", "2023-02-02"],
                    "direction": ["LONG", "SHORT"],
                    "entry_time": ["09:20", "09:25"],
                    "exit_time": ["10:00", "10:05"],
                    "entry_price": [100.0, 200.0],
                    "exit_price": [101.0, 199.5],
                    "sl_price": [99.5, 200.5],
                    "target_price": [102.0, 198.0],
                    "profit_loss": [1000.0, -500.0],
                    "profit_loss_pct": [1.0, -0.25],
                    "exit_reason": ["TARGET", "INITIAL_SL"],
                    "sl_phase": ["TRAIL", "PROTECT"],
                    "atr": [2.0, 2.5],
                    "cpr_width_pct": [0.4, 0.5],
                    "mfe_r": [1.5, 0.3],
                    "mae_r": [-0.2, -1.0],
                    "or_atr_ratio": [0.7, 0.9],
                    "gap_pct": [0.2, -0.1],
                }
            )
            db.store_backtest_results(df)

            row = db.con.execute(
                """
                SELECT allocated_capital, total_return_pct, annual_return_pct
                FROM run_metrics
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()

            assert row is not None
            assert float(row[0]) == 1_000_000.0
            assert 0.04 <= float(row[1]) <= 0.06
            assert 0.04 <= float(row[2]) <= 0.06
        finally:
            db.close()

    def test_run_metrics_drawdown_pct_is_floored_to_starting_equity(self, tmp_path):
        db = MarketDB(db_path=tmp_path / "run-metrics-dd.duckdb")
        try:
            run_id = "run-metrics-dd"
            db.store_run_metadata(
                run_id=run_id,
                strategy="FBR",
                label="Drawdown floor test",
                symbols=["SBIN"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                params={"capital": 100000},
            )
            df = pl.DataFrame(
                {
                    "run_id": [run_id],
                    "symbol": ["SBIN"],
                    "trade_date": ["2023-01-03"],
                    "direction": ["LONG"],
                    "entry_time": ["09:20"],
                    "exit_time": ["10:00"],
                    "entry_price": [100.0],
                    "exit_price": [99.0],
                    "sl_price": [99.0],
                    "target_price": [102.0],
                    "profit_loss": [-1000.0],
                    "profit_loss_pct": [-1.0],
                    "exit_reason": ["INITIAL_SL"],
                    "sl_phase": ["PROTECT"],
                    "atr": [2.0],
                    "cpr_width_pct": [0.4],
                    "mfe_r": [0.0],
                    "mae_r": [-1.0],
                    "or_atr_ratio": [0.7],
                    "gap_pct": [0.2],
                }
            )
            db.store_backtest_results(df)

            row = db.con.execute(
                "SELECT max_dd_pct, calmar FROM run_metrics WHERE run_id = ?",
                [run_id],
            ).fetchone()

            assert row is not None
            assert 0.99 <= float(row[0]) <= 1.01
            assert float(row[1]) <= 0.0
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Module-level: BacktestProgress.log_stage()
# ---------------------------------------------------------------------------
from engine.progress import BacktestProgress  # noqa: E402


def test_log_stage_prints_without_tqdm(capsys):
    progress = BacktestProgress(total_symbols=10, verbose=False)
    progress.log_stage("setup fetch done", elapsed_s=1.23, setup_days=500)
    captured = capsys.readouterr()
    assert "[stage] setup fetch done" in captured.out
    assert "elapsed_s=1.23" in captured.out
    assert "setup_days=500" in captured.out
    progress.close()
