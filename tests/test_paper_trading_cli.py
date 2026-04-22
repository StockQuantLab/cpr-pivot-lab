"""Tests for the paper trading CLI parser and helpers."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import polars as pl
import pytest

from scripts.paper_trading import (
    PAPER_STANDARD_MATRIX,
    _cmd_daily_prepare,
    _parse_json,
    _resolve_paper_strategy_params,
    _run_sim_variant,
    build_parser,
)


def test_paper_trading_parser_supports_start_and_status() -> None:
    parser = build_parser()

    start_args = parser.parse_args(["start", "--symbols", "SBIN,TCS"])
    assert start_args.command == "start"
    assert start_args.strategy == "CPR_LEVELS"
    assert start_args.symbols == "SBIN,TCS"
    assert start_args.max_positions == 10
    assert start_args.max_position_pct == 0.1

    status_args = parser.parse_args(["status"])
    assert status_args.command == "status"
    assert status_args.summary is False

    status_summary_args = parser.parse_args(["status", "--summary"])
    assert status_summary_args.summary is True


def test_paper_trading_parser_supports_cleanup() -> None:
    parser = build_parser()

    cleanup_args = parser.parse_args(["cleanup", "--trade-date", "2026-04-01", "--apply"])

    assert cleanup_args.command == "cleanup"
    assert cleanup_args.apply is True
    assert cleanup_args.trade_date == "2026-04-01"


def test_paper_trading_parser_supports_feed_audit() -> None:
    parser = build_parser()

    feed_audit_args = parser.parse_args(
        ["feed-audit", "--trade-date", "2026-04-13", "--feed-source", "kite"]
    )

    assert feed_audit_args.command == "feed-audit"
    assert feed_audit_args.trade_date == "2026-04-13"
    assert feed_audit_args.feed_source == "kite"


def test_paper_trading_parser_supports_replay() -> None:
    parser = build_parser()

    replay_args = parser.parse_args(
        [
            "replay",
            "--session-id",
            "sess-1",
            "--symbols",
            "SBIN,TCS",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ]
    )

    assert replay_args.command == "replay"
    assert replay_args.session_id == "sess-1"
    assert replay_args.leave_active is False


def test_paper_trading_parser_supports_daily_commands() -> None:
    parser = build_parser()

    daily_prepare_args = parser.parse_args(["daily-prepare", "--trade-date", "2024-01-01"])
    assert daily_prepare_args.command == "daily-prepare"
    assert daily_prepare_args.trade_date == "2024-01-01"
    assert daily_prepare_args.all_symbols is False

    daily_replay_args = parser.parse_args(
        [
            "daily-replay",
            "--trade-date",
            "2024-01-02",
            "--symbols",
            "SBIN,RELIANCE",
            "--strategy",
            "CPR_LEVELS",
            "--session-id",
            "paper-test",
            "--multi",
        ],
    )
    assert daily_replay_args.command == "daily-replay"
    assert daily_replay_args.session_id == "paper-test"
    assert daily_replay_args.strategy == "CPR_LEVELS"
    assert daily_replay_args.all_symbols is False
    assert daily_replay_args.multi is True

    daily_live_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN,RELIANCE",
        ]
    )
    assert daily_live_args.command == "daily-live"
    assert daily_live_args.trade_date == "2024-01-03"
    assert daily_live_args.all_symbols is False
    assert daily_live_args.skip_coverage is False
    assert daily_live_args.direction is None
    assert daily_live_args.min_price is None
    assert daily_live_args.regime_index_symbol is None
    assert daily_live_args.regime_min_move_pct is None
    assert daily_live_args.regime_snapshot_minutes == 30
    assert daily_live_args.cpr_min_close_atr is None
    assert daily_live_args.cpr_scale_out_pct is None
    assert daily_live_args.narrowing_filter is False
    assert daily_live_args.standard_sizing is False
    assert daily_live_args.risk_based_sizing is True
    assert daily_live_args.no_alerts is False

    daily_live_no_alerts_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--no-alerts",
        ]
    )
    assert daily_live_no_alerts_args.no_alerts is True

    preset_live_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--preset",
            "CPR_LEVELS_RISK_LONG",
        ]
    )
    assert preset_live_args.preset == "CPR_LEVELS_RISK_LONG"

    daily_live_skip_args = parser.parse_args(
        ["daily-live", "--trade-date", "2024-01-03", "--all-symbols", "--skip-coverage"]
    )
    assert daily_live_skip_args.skip_coverage is True

    tuned_live_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--direction",
            "SHORT",
            "--skip-rvol",
            "--min-price",
            "50",
            "--regime-index-symbol",
            "NIFTY 500",
            "--regime-min-move-pct",
            "0.3",
            "--regime-snapshot-minutes",
            "10",
            "--cpr-min-close-atr",
            "0.35",
            "--cpr-scale-out-pct",
            "0.8",
            "--narrowing-filter",
        ]
    )
    assert tuned_live_args.direction == "SHORT"
    assert tuned_live_args.skip_rvol is True
    assert tuned_live_args.min_price == 50.0
    assert tuned_live_args.regime_index_symbol == "NIFTY 500"
    assert tuned_live_args.regime_min_move_pct == 0.3
    assert tuned_live_args.regime_snapshot_minutes == 10
    assert tuned_live_args.cpr_min_close_atr == 0.35
    assert tuned_live_args.cpr_scale_out_pct == 0.8
    assert tuned_live_args.narrowing_filter is True
    assert tuned_live_args.standard_sizing is False
    assert tuned_live_args.risk_based_sizing is True

    standard_sizing_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--standard-sizing",
        ]
    )
    assert standard_sizing_args.standard_sizing is True
    assert standard_sizing_args.risk_based_sizing is False

    timed_live_args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--or-minutes",
            "10",
            "--entry-window-end",
            "15:00",
            "--time-exit",
            "15:00",
            "--cpr-entry-start",
            "14:00",
        ]
    )
    assert timed_live_args.or_minutes == 10
    assert timed_live_args.entry_window_end == "15:00"
    assert timed_live_args.time_exit == "15:00"
    assert timed_live_args.cpr_entry_start == "14:00"


class _DummyLock:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def test_paper_trading_parser_supports_daily_replay_no_alerts() -> None:
    parser = build_parser()

    replay_args = parser.parse_args(["daily-replay", "--trade-date", "2024-01-02", "--no-alerts"])

    assert replay_args.command == "daily-replay"
    assert replay_args.no_alerts is True


def test_paper_standard_matrix_uses_cpr_canonical_params() -> None:
    cpr_variants = {
        label: params
        for label, strategy, params in PAPER_STANDARD_MATRIX
        if strategy == "CPR_LEVELS"
    }

    assert cpr_variants["CPR_LEVELS_LONG"] == {
        "direction_filter": "LONG",
        "min_price": 50,
        "cpr_min_close_atr": 0.5,
        "narrowing_filter": True,
        "risk_based_sizing": True,
        "skip_rvol_check": False,
    }
    assert cpr_variants["CPR_LEVELS_SHORT"] == {
        "direction_filter": "SHORT",
        "skip_rvol_check": True,
        "min_price": 50,
        "cpr_min_close_atr": 0.5,
        "narrowing_filter": True,
        "risk_based_sizing": True,
    }


def test_parse_json_accepts_powershell_style_object() -> None:
    parsed = _parse_json(
        "{direction_filter:LONG,skip_rvol_check:true,cpr_min_close_atr:0.35,min_price:50}"
    )

    assert parsed == {
        "direction_filter": "LONG",
        "skip_rvol_check": True,
        "cpr_min_close_atr": 0.35,
        "min_price": 50,
    }


def test_resolve_paper_strategy_params_preserves_backtest_defaults() -> None:
    cpr_long = _resolve_paper_strategy_params("CPR_LEVELS", None)
    assert cpr_long["direction_filter"] == "BOTH"
    assert "min_price" not in cpr_long
    assert "cpr_min_close_atr" not in cpr_long
    assert "scale_out_pct" not in cpr_long.get("cpr_levels_config", {})
    assert "narrowing_filter" not in cpr_long
    assert "skip_rvol_check" not in cpr_long
    assert "risk_based_sizing" not in cpr_long

    cpr_short = _resolve_paper_strategy_params("CPR_LEVELS", '{"direction_filter":"SHORT"}')
    assert cpr_short["direction_filter"] == "SHORT"
    assert "narrowing_filter" not in cpr_short
    assert "skip_rvol_check" not in cpr_short

    with pytest.raises(ValueError, match="not supported for paper workflows"):
        _resolve_paper_strategy_params("FBR", '{"direction_filter":"SHORT"}')

    preset = SimpleNamespace(preset="CPR_LEVELS_RISK_LONG")
    preset_params = _resolve_paper_strategy_params("CPR_LEVELS", None, preset)
    assert preset_params["direction_filter"] == "LONG"
    assert preset_params["min_price"] == 50
    assert preset_params["cpr_min_close_atr"] == 0.5
    assert preset_params["narrowing_filter"] is True
    assert preset_params["risk_based_sizing"] is True


def test_resolve_paper_strategy_params_standard_preset_not_clobbered_by_default() -> None:
    """Regression: paper CLI default risk_based_sizing=True must not override STANDARD preset False.

    In the daily-live parser, risk_based_sizing has default=True.  When --preset
    CPR_LEVELS_STANDARD_LONG is used, _collect_strategy_cli_overrides must NOT inject
    risk_based_sizing=True (the default) over the preset's risk_based_sizing=False.
    """
    parser = build_parser()
    args = parser.parse_args(
        [
            "daily-live",
            "--trade-date",
            "2024-01-03",
            "--symbols",
            "SBIN",
            "--preset",
            "CPR_LEVELS_STANDARD_LONG",
        ]
    )
    # Confirm that the parser default is True (no explicit --risk-based-sizing was passed)
    assert args.risk_based_sizing is True
    assert args.standard_sizing is False

    params = _resolve_paper_strategy_params("CPR_LEVELS", None, args)
    assert params["risk_based_sizing"] is False, (
        "STANDARD preset risk_based_sizing=False was overridden by CLI default True"
    )


def test_resolve_paper_strategy_params_rejects_fbr_preset() -> None:
    args = SimpleNamespace(
        preset="FBR_RISK_LONG",
        direction="BOTH",
        skip_rvol=False,
        standard_sizing=False,
        risk_based_sizing=True,
        min_price=None,
        cpr_min_close_atr=None,
        cpr_scale_out_pct=None,
        failure_window=None,
        or_minutes=None,
        entry_window_end=None,
        time_exit=None,
        cpr_entry_start=None,
        fbr_entry_window_end=None,
    )

    with pytest.raises(ValueError, match="not supported for paper workflows"):
        _resolve_paper_strategy_params("FBR", None, args)


def test_resolve_paper_strategy_params_accepts_standard_sizing_cli_override() -> None:
    args = SimpleNamespace(
        direction=None,
        skip_rvol=False,
        standard_sizing=True,
        risk_based_sizing=False,
        min_price=None,
        cpr_min_close_atr=None,
        cpr_scale_out_pct=None,
        failure_window=None,
        or_minutes=None,
        entry_window_end=None,
        time_exit=None,
        cpr_entry_start=None,
        fbr_entry_window_end=None,
    )

    cpr = _resolve_paper_strategy_params("CPR_LEVELS", None, args)
    assert cpr["risk_based_sizing"] is False


def test_resolve_paper_strategy_params_accepts_timing_cli_overrides() -> None:
    args = SimpleNamespace(
        direction="LONG",
        skip_rvol=False,
        min_price=None,
        cpr_min_close_atr=None,
        cpr_scale_out_pct=None,
        failure_window=None,
        or_minutes=15,
        entry_window_end="15:00",
        time_exit="15:05",
        cpr_entry_start="14:00",
    )

    cpr = _resolve_paper_strategy_params("CPR_LEVELS", None, args)
    assert cpr["direction_filter"] == "LONG"
    assert cpr["or_minutes"] == 15
    assert cpr["entry_window_end"] == "15:00"
    assert cpr["time_exit"] == "15:05"
    assert cpr["cpr_levels_config"]["cpr_entry_start"] == "14:00"

    with pytest.raises(ValueError, match="not supported for paper workflows"):
        _resolve_paper_strategy_params("FBR", None, args)


@pytest.mark.asyncio
async def test_cmd_daily_prepare_runs_readiness_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.paper_trading as pt

    calls: list[object] = []

    class _FakeCon:
        def execute(self, query: str, params: list[object]):
            # Simulate intraday data existing for the requested date → mode="replay"
            return SimpleNamespace(fetchone=lambda: (1,))

    class _FakeDB:
        con = _FakeCon()

        def con_execute(self, *a, **kw):
            return SimpleNamespace(fetchone=lambda: (1,))

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls.append(("prepare", trade_date, list(symbols), mode))
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": True,
            "coverage": {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []},
        }

    def fake_build_trade_date_readiness_report(trade_date: str):
        calls.append(("dq", trade_date))
        return {
            "trade_date": trade_date,
            "requested_symbols": ["SBIN"],
            "freshness_rows": [],
            "freshness_comparisons": [],
            "setup_capable_symbols": ["SBIN"],
            "late_starting_symbols": [],
            "setup_query_failed": False,
            "coverage": {"market_day_state": [], "strategy_day_state": [], "intraday_day_pack": []},
            "ready": False,
        }

    def fake_print_trade_date_readiness_report(report: dict[str, object]) -> None:
        calls.append(("print", report["trade_date"]))

    monkeypatch.setattr(pt, "get_db", lambda: _FakeDB())
    monkeypatch.setattr(pt, "get_dashboard_db", lambda: _FakeDB())
    monkeypatch.setattr(pt, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper)
    monkeypatch.setattr(
        pt._data_quality,
        "build_trade_date_readiness_report",
        fake_build_trade_date_readiness_report,
    )
    monkeypatch.setattr(
        pt._data_quality,
        "print_trade_date_readiness_report",
        fake_print_trade_date_readiness_report,
    )
    monkeypatch.setattr(pt, "resolve_trade_date", lambda value: value)
    monkeypatch.setattr(
        pt,
        "resolve_prepare_symbols",
        lambda symbols, raw, all_symbols=False, read_only=True: symbols,
    )

    with pytest.raises(SystemExit) as excinfo:
        await _cmd_daily_prepare(
            SimpleNamespace(
                trade_date="2024-01-01",
                symbols="SBIN",
                all_symbols=False,
            )
        )

    assert excinfo.value.code == 1
    assert calls == [
        ("prepare", "2024-01-01", ["SBIN"], "replay"),
        ("dq", "2024-01-01"),
        ("print", "2024-01-01"),
    ]


@pytest.mark.asyncio
async def test_cmd_daily_replay_restores_alert_suppression_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    calls: list[bool] = []

    class _LockCtx:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(pt, "acquire_command_lock", lambda *args, **kwargs: _LockCtx())
    monkeypatch.setattr(pt, "_resolve_cli_symbols", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(
        pt,
        "_resolve_paper_strategy_params",
        lambda *args, **kwargs: {"direction_filter": "LONG", "skip_rvol_check": False},
    )
    monkeypatch.setattr(pt, "pre_filter_symbols_for_strategy", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(pt, "set_alerts_suppressed", lambda value: calls.append(value))

    async def fake_run_daily_workflow(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(pt, "_run_daily_workflow", fake_run_daily_workflow)

    with pytest.raises(RuntimeError):
        await pt._cmd_daily_replay(
            SimpleNamespace(
                trade_date="2024-01-02",
                symbols="SBIN",
                all_symbols=False,
                strategy="CPR_LEVELS",
                strategy_params=None,
                session_id="paper-test",
                notes=None,
                leave_active=False,
                no_alerts=True,
            )
        )

    assert calls == [True, False]


@pytest.mark.asyncio
@pytest.mark.parametrize("no_alerts, expected_calls", [(False, []), (True, [True, False])])
async def test_cmd_daily_live_local_feed_alert_toggle(
    monkeypatch: pytest.MonkeyPatch,
    no_alerts: bool,
    expected_calls: list[bool],
) -> None:
    import engine.local_ticker_adapter as lta
    import scripts.paper_trading as pt

    calls: list[bool] = []
    wait_calls: list[tuple[str, str]] = []
    workflow_calls: dict[str, object] = {}

    class _FakeLocalTickerAdapter:
        def __init__(
            self,
            *,
            trade_date: str,
            symbols: list[str],
            candle_interval_minutes: int = 5,
        ) -> None:
            self.trade_date = trade_date
            self.symbols = list(symbols)
            self.candle_interval_minutes = candle_interval_minutes
            workflow_calls["local_ticker"] = {
                "trade_date": trade_date,
                "symbols": list(symbols),
                "candle_interval_minutes": candle_interval_minutes,
            }

    async def fake_run_daily_workflow(**kwargs):
        workflow_calls["workflow"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "resolve_trade_date", lambda value: value)
    monkeypatch.setattr(pt, "_resolve_cli_symbols", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(
        pt,
        "_resolve_paper_strategy_params",
        lambda *args, **kwargs: {"direction_filter": "LONG", "skip_rvol_check": False},
    )
    monkeypatch.setattr(pt, "pre_filter_symbols_for_strategy", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(pt, "set_alerts_suppressed", lambda value: calls.append(value))
    monkeypatch.setattr(lta, "LocalTickerAdapter", _FakeLocalTickerAdapter)
    monkeypatch.setattr(pt, "_run_daily_workflow", fake_run_daily_workflow)
    monkeypatch.setattr(
        pt,
        "_wait_until_market_ready",
        lambda trade_date: wait_calls.append(("wait", trade_date)),
    )

    await pt._cmd_daily_live(
        SimpleNamespace(
            trade_date="2026-04-09",
            symbols="SBIN",
            all_symbols=False,
            strategy="CPR_LEVELS",
            strategy_params=None,
            session_id="paper-test",
            notes=None,
            skip_coverage=True,
            poll_interval_sec=1.0,
            candle_interval_minutes=5,
            max_cycles=1,
            complete_on_exit=False,
            feed_source="local",
            no_alerts=no_alerts,
            multi=False,
        )
    )

    assert calls == expected_calls
    assert wait_calls == []
    assert workflow_calls["local_ticker"] == {
        "trade_date": "2026-04-09",
        "symbols": ["SBIN"],
        "candle_interval_minutes": 5,
    }
    assert workflow_calls["workflow"]["strategy_params"]["feed_source"] == "local"
    assert workflow_calls["workflow"]["live_kwargs"]["ticker_adapter"].trade_date == "2026-04-09"


@pytest.mark.asyncio
async def test_cmd_daily_live_kite_feed_waits_until_market_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    calls: list[tuple[str, str]] = []
    workflow_calls: dict[str, object] = {}

    async def fake_wait_until_market_ready(trade_date: str) -> None:
        calls.append(("wait", trade_date))

    async def fake_run_daily_workflow(**kwargs):
        workflow_calls["workflow"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "resolve_trade_date", lambda value: value)
    monkeypatch.setattr(pt, "_resolve_cli_symbols", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(
        pt,
        "_resolve_paper_strategy_params",
        lambda *args, **kwargs: {"direction_filter": "LONG", "skip_rvol_check": False},
    )
    monkeypatch.setattr(pt, "pre_filter_symbols_for_strategy", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(pt, "set_alerts_suppressed", lambda value: None)
    monkeypatch.setattr(pt, "_wait_until_market_ready", fake_wait_until_market_ready)
    monkeypatch.setattr(pt, "_run_daily_workflow", fake_run_daily_workflow)

    await pt._cmd_daily_live(
        SimpleNamespace(
            trade_date="2026-04-09",
            symbols="SBIN",
            all_symbols=False,
            strategy="CPR_LEVELS",
            strategy_params=None,
            session_id="paper-test",
            notes=None,
            skip_coverage=True,
            poll_interval_sec=1.0,
            candle_interval_minutes=5,
            max_cycles=1,
            complete_on_exit=False,
            feed_source="kite",
            no_alerts=False,
            multi=False,
        )
    )

    assert calls == [("wait", "2026-04-09")]
    assert "ticker_adapter" not in workflow_calls["workflow"]["live_kwargs"]


@pytest.mark.asyncio
async def test_run_sim_variant_stamps_paper_execution_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.paper_trading as pt
    from engine.cpr_atr_strategy import BacktestParams

    calls: list[tuple[str, object]] = []

    class _FakeBacktest:
        def __init__(self, params, db):
            self.params = params
            self.db = db

        def run(self, *, symbols, start, end, verbose, use_cache=True):
            calls.append(("run", list(symbols), start, end, verbose, use_cache))
            return SimpleNamespace(
                run_id="bt-paper-base",
                params=BacktestParams(strategy="CPR_LEVELS"),
                df=pl.DataFrame(
                    {
                        "run_id": ["bt-paper-base"],
                        "symbol": ["SBIN"],
                        "trade_date": ["2024-01-02"],
                        "profit_loss": [123.45],
                        "exit_reason": ["TARGET"],
                    }
                ),
                run_context={
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-02",
                    "symbols": ["SBIN"],
                },
            )

    class _FakeDB:
        class _FakeCon:
            def execute(self, *args, **kwargs):
                return None

            def register(self, *args, **kwargs):
                return None

            def unregister(self, *args, **kwargs):
                return None

        con = _FakeCon()

        def store_run_metadata(self, **kwargs):
            calls.append(("metadata", kwargs["execution_mode"]))

        def store_backtest_results(
            self,
            results_df,
            execution_mode: str | None = None,
            transactional: bool = True,
        ):
            calls.append(("results", execution_mode, results_df["run_id"][0]))
            assert execution_mode == "PAPER"
            assert results_df["run_id"][0] != "bt-paper-base"
            return results_df.height

    monkeypatch.setattr(pt, "CPRATRBacktest", _FakeBacktest)
    monkeypatch.setattr(pt, "get_db", lambda: _FakeDB())
    monkeypatch.setitem(
        sys.modules,
        "web.state",
        SimpleNamespace(
            invalidate_run_cache=lambda run_id=None: calls.append(("invalidate", run_id))
        ),
    )

    result = _run_sim_variant(
        trade_date="2024-01-02",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "LONG"},
        force=True,
    )

    assert result["run_id"] != "bt-paper-base"
    assert result["trades"] == 1
    assert calls[0][0] == "run"
    assert calls[1] == ("metadata", "PAPER")
    assert calls[2][0] == "results"
    assert calls[2][1] == "PAPER"
    assert calls[3] == ("invalidate", result["run_id"])


@pytest.mark.asyncio
async def test_cmd_daily_sim_holds_runtime_writer_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.paper_trading as pt

    calls: list[str] = []

    class _LockCtx:
        def __enter__(self):
            calls.append("enter")
            return None

        def __exit__(self, exc_type, exc, tb):
            calls.append("exit")
            return False

    def fake_run_sim_variant(**kwargs):
        calls.append(f"run:{kwargs['strategy']}")
        return {
            "run_id": "paper-123",
            "strategy": kwargs["strategy"],
            "strategy_params": kwargs["strategy_params"],
            "trade_date": kwargs["trade_date"],
            "symbol_count": len(kwargs["symbols"]),
            "trades": 1,
            "wins": 1,
            "win_rate": 100.0,
            "total_pnl": 1.0,
            "elapsed_sec": 0.1,
        }

    monkeypatch.setattr(pt, "acquire_command_lock", lambda *args, **kwargs: _LockCtx())
    monkeypatch.setattr(pt, "_resolve_cli_symbols", lambda *args, **kwargs: ["SBIN"])
    monkeypatch.setattr(
        pt,
        "_resolve_paper_strategy_params",
        lambda *args, **kwargs: {"direction_filter": "LONG", "skip_rvol_check": False},
    )
    monkeypatch.setattr(pt, "_run_sim_variant", fake_run_sim_variant)

    await pt._cmd_daily_sim(
        SimpleNamespace(
            trade_date="2024-01-02",
            symbols=None,
            all_symbols=True,
            strategy="CPR_LEVELS",
            strategy_params=None,
            force=True,
            direction=None,
            skip_rvol=False,
            min_price=None,
            cpr_min_close_atr=None,
            narrowing_filter=False,
            failure_window=None,
            or_minutes=None,
            entry_window_end=None,
            time_exit=None,
            cpr_entry_start=None,
            fbr_entry_window_end=None,
        )
    )

    assert calls == ["enter", "run:CPR_LEVELS", "exit"]


def test_paper_trading_parser_supports_live() -> None:
    parser = build_parser()

    live_args = parser.parse_args(
        ["live", "--session-id", "sess-2", "--max-cycles", "2", "--no-alerts"]
    )

    assert live_args.command == "live"
    assert live_args.session_id == "sess-2"
    assert live_args.max_cycles == 2
    assert live_args.no_alerts is True


def test_paper_trading_main_configures_windows_asyncio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    events: list[str] = []

    async def fake_handler(_args) -> None:
        return None

    class _FakeParser:
        def parse_args(self):
            return SimpleNamespace(handler=fake_handler)

    def fake_run(coro) -> None:
        events.append("run")
        coro.close()

    monkeypatch.setattr(pt, "configure_windows_stdio", lambda **_: events.append("stdio"))
    monkeypatch.setattr(pt, "configure_windows_asyncio", lambda: events.append("asyncio"))
    monkeypatch.setattr(pt, "run_asyncio", fake_run)
    monkeypatch.setattr(pt, "build_parser", lambda: _FakeParser())
    monkeypatch.setattr(
        pt,
        "_pdb",
        lambda: SimpleNamespace(cleanup_stale_sessions=lambda: 0),
    )

    pt.main()

    assert events == ["stdio", "asyncio", "run"]
