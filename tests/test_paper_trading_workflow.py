"""Tests for the daily paper-trading orchestration workflow."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

import scripts.paper_trading as paper_trading
from engine.broker_adapter import OrderSafetyError
from engine.real_order_runtime import build_real_order_router


def test_default_session_id_includes_direction_for_daily_sessions() -> None:
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "CPR_LEVELS",
            {"direction_filter": "LONG"},
            "live",
        )
        == "paper-cpr_levels-long-2024-01-02-live-kite"
    )
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "FBR",
            {"direction_filter": "SHORT"},
            "live",
            "local",
        )
        == "paper-fbr-short-2024-01-02-live-local"
    )
    assert (
        paper_trading._default_session_id(
            "paper",
            "2024-01-02",
            "CPR_LEVELS",
            {"direction_filter": "SHORT"},
            "replay",
        )
        == "paper-cpr_levels-short-2024-01-02-replay-historical"
    )


def test_select_paper_multi_variants_can_select_one_side() -> None:
    long_variants = paper_trading._select_paper_multi_variants(
        SimpleNamespace(direction="LONG", preset=None),
        "CPR_LEVELS",
    )
    short_variants = paper_trading._select_paper_multi_variants(
        SimpleNamespace(direction=None, preset="CPR_LEVELS_RISK_SHORT"),
        "CPR_LEVELS",
    )

    assert [label for label, _, _ in long_variants] == ["CPR_LEVELS_LONG"]
    assert [label for label, _, _ in short_variants] == ["CPR_LEVELS_SHORT"]


def test_select_paper_multi_variants_rejects_conflicting_side() -> None:
    with pytest.raises(SystemExit, match="conflicts"):
        paper_trading._select_paper_multi_variants(
            SimpleNamespace(direction="LONG", preset="CPR_LEVELS_RISK_SHORT"),
            "CPR_LEVELS",
        )


def test_real_order_config_defaults_to_disabled() -> None:
    args = SimpleNamespace(real_orders=False)

    assert (
        paper_trading._build_real_order_config(
            args,
            strategy="CPR_LEVELS",
            strategy_params={"direction_filter": "LONG"},
            feed_source="kite",
        )
        is None
    )


def test_real_order_config_marks_pilot_session_and_blocks_multi() -> None:
    args = SimpleNamespace(
        real_orders=True,
        multi=False,
        resume=False,
        real_order_sizing_mode="fixed-qty",
        real_order_fixed_qty=1,
        real_order_max_positions=1,
        real_order_cash_budget=10_000.0,
        real_order_skip_account_cash_check=False,
        real_entry_order_type="MARKET",
        real_entry_max_slippage_pct=0.5,
        real_exit_max_slippage_pct=2.0,
    )

    config = paper_trading._build_real_order_config(
        args,
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "LONG"},
        feed_source="kite",
    )

    assert config == {
        "enabled": True,
        "sizing_mode": "FIXED_QTY",
        "fixed_quantity": 1,
        "max_positions": 1,
        "cash_budget": 10_000.0,
        "require_account_cash_check": True,
        "entry_order_type": "MARKET",
        "entry_max_slippage_pct": 0.5,
        "exit_max_slippage_pct": 2.0,
        "product": "MIS",
        "exchange": "NSE",
        "adapter_mode": "LIVE",
        "shadow": False,
    }
    assert paper_trading._real_order_notes(None) == "ZERODHA_LIVE_REAL_ORDERS"

    args.multi = True
    with pytest.raises(SystemExit, match="--multi --real-orders"):
        paper_trading._build_real_order_config(
            args,
            strategy="CPR_LEVELS",
            strategy_params={"direction_filter": "LONG"},
            feed_source="kite",
        )


def test_simulated_real_order_config_allows_multi_and_local_feed() -> None:
    args = SimpleNamespace(
        real_orders=False,
        simulate_real_orders=True,
        multi=True,
        resume=False,
        real_order_sizing_mode="fixed-qty",
        real_order_fixed_qty=1,
        real_order_max_positions=1,
        real_order_cash_budget=10_000.0,
        real_order_skip_account_cash_check=False,
        real_entry_order_type="LIMIT",
        real_entry_max_slippage_pct=0.5,
        real_exit_max_slippage_pct=2.0,
    )

    config = paper_trading._build_real_order_config(
        args,
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "SHORT"},
        feed_source="local",
    )

    assert config is not None
    assert config["adapter_mode"] == "REAL_DRY_RUN"
    assert config["shadow"] is True
    assert config["require_account_cash_check"] is False
    assert paper_trading._simulated_real_order_notes(None) == "ZERODHA_REAL_DRY_RUN_ORDERS"


def test_real_order_config_supports_cash_budget_sizing() -> None:
    args = SimpleNamespace(
        real_orders=True,
        multi=False,
        resume=False,
        real_order_sizing_mode="cash-budget",
        real_order_fixed_qty=1,
        real_order_max_positions=1,
        real_order_cash_budget=10_000.0,
        real_order_skip_account_cash_check=False,
        real_entry_order_type="LIMIT",
        real_entry_max_slippage_pct=0.5,
        real_exit_max_slippage_pct=2.0,
    )

    config = paper_trading._build_real_order_config(
        args,
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "LONG"},
        feed_source="kite",
    )

    assert config is not None
    assert config["sizing_mode"] == "CASH_BUDGET"
    assert config["cash_budget"] == 10_000.0


def test_real_order_config_preserves_zero_budget_for_fail_closed_validation() -> None:
    args = SimpleNamespace(
        real_orders=True,
        multi=False,
        resume=False,
        real_order_sizing_mode="cash-budget",
        real_order_fixed_qty=1,
        real_order_max_positions=1,
        real_order_cash_budget=0.0,
        real_order_skip_account_cash_check=True,
        real_entry_order_type="LIMIT",
        real_entry_max_slippage_pct=0.5,
        real_exit_max_slippage_pct=2.0,
    )

    config = paper_trading._build_real_order_config(
        args,
        strategy="CPR_LEVELS",
        strategy_params={"direction_filter": "LONG"},
        feed_source="kite",
    )

    assert config is not None
    assert config["cash_budget"] == 0.0
    with pytest.raises(OrderSafetyError, match="cash budget must be positive"):
        build_real_order_router(config)


def test_reject_early_kite_live_start_aborts_before_market_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    early_start = datetime(2026, 4, 28, 9, 15, 0, tzinfo=paper_trading.IST)

    class _FakeDateTime:
        @staticmethod
        def now(_tz=None) -> datetime:
            return early_start

    monkeypatch.setattr(paper_trading, "datetime", _FakeDateTime)

    with pytest.raises(SystemExit) as exc:
        paper_trading._reject_early_kite_live_start("2026-04-28", wait_for_open=False)

    assert "should be launched at/after 09:16 IST" in str(exc.value)


def test_reject_early_kite_live_start_allows_wait_for_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    early_start = datetime(2026, 4, 28, 9, 15, 0, tzinfo=paper_trading.IST)

    class _FakeDateTime:
        @staticmethod
        def now(_tz=None) -> datetime:
            return early_start

    monkeypatch.setattr(paper_trading, "datetime", _FakeDateTime)

    paper_trading._reject_early_kite_live_start("2026-04-28", wait_for_open=True)


def test_reject_early_kite_live_start_allows_non_trading_day_or_late_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    late_start = datetime(2026, 4, 28, 9, 16, 0, tzinfo=paper_trading.IST)

    class _FakeDateTime:
        @staticmethod
        def now(_tz=None) -> datetime:
            return late_start

    monkeypatch.setattr(paper_trading, "datetime", _FakeDateTime)

    paper_trading._reject_early_kite_live_start("2026-04-27", wait_for_open=False)
    paper_trading._reject_early_kite_live_start("2026-04-28", wait_for_open=False)


def test_variant_exit_summary_and_retry_policy_detects_spurious_early_completion() -> None:
    summary = paper_trading._variant_exit_summary(
        {
            "final_status": "COMPLETED",
            "last_bar_ts": "2026-04-15T09:25:00+05:30",
            "terminal_reason": "spurious_completion",
            "closed_bars": 17,
            "cycles": 9,
        }
    )

    should_retry, reason = paper_trading._should_retry_variant_exit(
        summary,
        current_hhmm="09:26",
        entry_window_closed_hhmm="10:30",
        eod_cutoff_hhmm="14:30",
    )

    assert summary["status"] == "COMPLETED"
    assert summary["last_bar_hhmm"] == "09:25"
    assert summary["closed_bars"] == 17
    assert should_retry is True
    assert reason == "completed early at 09:25"


def test_variant_exit_summary_and_retry_policy_respects_intentional_completion() -> None:
    summary = paper_trading._variant_exit_summary(
        {
            "final_status": "COMPLETED",
            "last_bar_ts": "2026-04-15T09:25:00+05:30",
            "terminal_reason": "complete_on_exit",
        }
    )

    should_retry, reason = paper_trading._should_retry_variant_exit(
        summary,
        current_hhmm="09:26",
        entry_window_closed_hhmm="10:30",
        eod_cutoff_hhmm="14:30",
    )

    assert should_retry is False
    assert reason == "complete_on_exit"


@pytest.mark.asyncio
async def test_run_daily_workflow_replay_uses_shared_preparation_and_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls["prepare"] = {
            "trade_date": trade_date,
            "symbols": symbols,
            "mode": mode,
        }
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": True,
            "coverage": {"paper_trading_sessions": [], "paper_positions": []},
        }

    async def fake_ensure_daily_session(
        *,
        session_id: str | None,
        trade_date: str,
        strategy: str,
        symbols: list[str],
        strategy_params: dict,
        notes: str | None,
        mode: str = "replay",
    ):
        calls["ensure"] = {
            "session_id": session_id,
            "trade_date": trade_date,
            "strategy": strategy,
            "symbols": symbols,
            "strategy_params": strategy_params,
            "notes": notes,
        }
        return SimpleNamespace(session_id="paper-2024-01-02")

    async def fake_replay_session(
        *,
        session_id: str,
        symbols: list[str] | None,
        start_date: str | None,
        end_date: str | None,
        leave_active: bool,
        notes: str | None,
        preloaded_days=None,
        real_order_config=None,
        account_symbol_guard=None,
    ):
        calls["replay"] = {
            "session_id": session_id,
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
            "leave_active": leave_active,
            "notes": notes,
            "real_order_config": real_order_config,
            "account_symbol_guard": account_symbol_guard,
        }
        return {"status": "REPLAYED", "bars": 12}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "replay_session", fake_replay_session)

    payload = await paper_trading._run_daily_workflow(
        mode="replay",
        trade_date="2024-01-02",
        symbols=["SBIN", "RELIANCE"],
        strategy="FBR",
        strategy_params={"failure_window": 10},
        session_id="paper-2024-01-02",
        notes="daily replay",
        replay_kwargs={"leave_active": True},
    )

    assert calls["prepare"] == {
        "trade_date": "2024-01-02",
        "symbols": ["SBIN", "RELIANCE"],
        "mode": "replay",
    }
    assert calls["ensure"] == {
        "session_id": "paper-2024-01-02",
        "trade_date": "2024-01-02",
        "strategy": "FBR",
        "symbols": ["SBIN", "RELIANCE"],
        "strategy_params": {"failure_window": 10},
        "notes": "daily replay",
    }
    assert calls["replay"] == {
        "session_id": "paper-2024-01-02",
        "symbols": ["SBIN", "RELIANCE"],
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
        "leave_active": True,
        "notes": "daily replay",
        "real_order_config": None,
        "account_symbol_guard": None,
    }
    assert payload["session_id"] == "paper-2024-01-02"
    assert payload["preparation"]["coverage_ready"] is True
    assert payload["status"] == "REPLAYED"


@pytest.mark.asyncio
async def test_run_daily_workflow_live_uses_live_session_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls["prepare"] = {
            "trade_date": trade_date,
            "symbols": symbols,
            "mode": mode,
        }
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": True,
            "coverage": {"paper_trading_sessions": [], "paper_positions": []},
        }

    async def fake_ensure_daily_session(
        *,
        session_id: str | None,
        trade_date: str,
        strategy: str,
        symbols: list[str],
        strategy_params: dict,
        notes: str | None,
        mode: str = "replay",
    ):
        calls["ensure"] = {
            "session_id": session_id,
            "trade_date": trade_date,
            "strategy": strategy,
            "symbols": symbols,
            "strategy_params": strategy_params,
            "notes": notes,
        }
        return SimpleNamespace(session_id="paper-live-1")

    async def fake_run_live_session(
        *,
        session_id: str,
        symbols: list[str] | None,
        poll_interval_sec: float | None,
        candle_interval_minutes: int | None,
        max_cycles: int | None,
        complete_on_exit: bool,
        auto_flatten_on_abnormal_exit: bool,
        allow_late_start_fallback: bool,
        real_order_config: dict | None = None,
        notes: str | None = None,
        ticker_adapter: object | None = None,
    ):
        calls["live"] = {
            "session_id": session_id,
            "symbols": symbols,
            "poll_interval_sec": poll_interval_sec,
            "candle_interval_minutes": candle_interval_minutes,
            "max_cycles": max_cycles,
            "complete_on_exit": complete_on_exit,
            "auto_flatten_on_abnormal_exit": auto_flatten_on_abnormal_exit,
            "allow_late_start_fallback": allow_late_start_fallback,
            "real_order_config": real_order_config,
            "notes": notes,
            "ticker_adapter": ticker_adapter,
        }
        return {"status": "LIVE", "cycles": 2}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "run_live_session", fake_run_live_session)

    payload = await paper_trading._run_daily_workflow(
        mode="live",
        trade_date="2024-01-03",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={"rr_ratio": 1.8},
        session_id=None,
        notes=None,
        live_kwargs={
            "poll_interval_sec": 2.5,
            "candle_interval_minutes": 5,
            "max_cycles": 7,
            "complete_on_exit": True,
        },
    )

    assert calls["prepare"] == {
        "trade_date": "2024-01-03",
        "symbols": ["SBIN"],
        "mode": "live",
    }
    assert calls["ensure"] == {
        "session_id": None,
        "trade_date": "2024-01-03",
        "strategy": "CPR_LEVELS",
        "symbols": ["SBIN"],
        "strategy_params": {"rr_ratio": 1.8},
        "notes": None,
    }
    assert calls["live"] == {
        "session_id": "paper-live-1",
        "symbols": ["SBIN"],
        "poll_interval_sec": 2.5,
        "candle_interval_minutes": 5,
        "max_cycles": 7,
        "complete_on_exit": True,
        "auto_flatten_on_abnormal_exit": True,
        "allow_late_start_fallback": False,
        "real_order_config": None,
        "notes": None,
        "ticker_adapter": None,
    }
    assert payload["session_id"] == "paper-live-1"
    assert payload["status"] == "LIVE"


@pytest.mark.asyncio
async def test_cmd_daily_live_multi_uses_bar_major_dispatcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeTicker:
        def close(self) -> None:
            calls["ticker_closed"] = True

    class FakeLocalTickerAdapter:
        def __init__(self, **kwargs):
            calls["ticker_kwargs"] = kwargs

        def close(self) -> None:
            calls["ticker_closed"] = True

    async def fake_ensure_daily_session(**kwargs):
        return SimpleNamespace(session_id=kwargs["session_id"])

    async def fake_run_live_multi_sessions(**kwargs):
        calls["multi"] = kwargs
        return [
            {"session_id": spec.session_id, "final_status": "COMPLETED"} for spec in kwargs["specs"]
        ]

    monkeypatch.setattr(paper_trading, "_apply_default_saved_universe", lambda *_args: None)
    monkeypatch.setattr(paper_trading, "_resolve_cli_symbols", lambda *_args, **_kwargs: ["SBIN"])
    monkeypatch.setattr(paper_trading, "_enforce_live_readiness_gate", lambda *_args, **_kw: None)
    monkeypatch.setattr(
        paper_trading,
        "pre_filter_symbols_for_strategy",
        lambda _date, symbols, _strategy, _params, **_kw: list(symbols),
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "run_live_multi_sessions", fake_run_live_multi_sessions)
    monkeypatch.setattr(paper_trading, "_cleanup_feed_audit_retention", lambda **_kwargs: 0)
    monkeypatch.setattr("engine.local_ticker_adapter.LocalTickerAdapter", FakeLocalTickerAdapter)

    args = SimpleNamespace(
        trade_date="2026-05-04",
        feed_source="local",
        no_alerts=False,
        wait_for_open=False,
        strategy="CPR_LEVELS",
        strategy_params=None,
        skip_coverage=False,
        candle_interval_minutes=5,
        poll_interval_sec=1.0,
        max_cycles=1,
        complete_on_exit=True,
        allow_late_start_fallback=False,
        notes=None,
        simulate_real_orders=False,
        real_orders=False,
        real_order_sizing_mode="fixed-qty",
        real_order_fixed_qty=1,
        real_order_max_positions=1,
        real_order_cash_budget=10_000.0,
        real_order_skip_account_cash_check=False,
        real_entry_order_type="LIMIT",
        real_entry_max_slippage_pct=0.5,
        real_exit_max_slippage_pct=2.0,
    )

    await paper_trading._cmd_daily_live_multi(args)

    multi = calls["multi"]
    assert isinstance(multi, dict)
    assert isinstance(multi["ticker_adapter"], FakeLocalTickerAdapter)
    assert multi["max_cycles"] == 1
    assert multi["complete_on_exit"] is True
    specs = multi["specs"]
    assert len(specs) == 2
    assert [spec.session_id for spec in specs] == [
        "CPR_LEVELS_LONG-2026-05-04-live-local",
        "CPR_LEVELS_SHORT-2026-05-04-live-local",
    ]
    assert calls["ticker_closed"] is True


@pytest.mark.asyncio
async def test_run_daily_workflow_skips_when_coverage_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls.append("prepare")
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": False,
            "coverage": {
                "market_day_state": [],
                "strategy_day_state": ["SBIN"],
                "intraday_day_pack": ["SBIN"],
            },
        }

    async def fake_ensure_daily_session(**kwargs):
        calls.append("ensure")
        return SimpleNamespace(session_id="unused")

    async def fake_replay_session(**kwargs):
        calls.append("replay")
        return {"status": "REPLAYED"}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "replay_session", fake_replay_session)

    with pytest.raises(SystemExit) as exc:
        await paper_trading._run_daily_workflow(
            mode="replay",
            trade_date="2024-01-04",
            symbols=["SBIN"],
            strategy="FBR",
            strategy_params={},
            session_id=None,
            notes=None,
        )

    assert calls == ["prepare"]
    assert "Runtime coverage incomplete" in str(exc.value)
    assert "pivot-build --table strategy --refresh-date <trade-date>" in str(exc.value)
    assert "pivot-build --table pack --refresh-since <trade-date>" in str(exc.value)


@pytest.mark.asyncio
async def test_run_daily_workflow_live_raises_when_coverage_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_prepare_runtime_for_daily_paper(*, trade_date: str, symbols: list[str], mode: str):
        calls.append("prepare")
        return {
            "trade_date": trade_date,
            "requested_symbols": symbols,
            "coverage_ready": False,
            "coverage": {
                "market_day_state": [],
                "strategy_day_state": ["SBIN"],
                "intraday_day_pack": [],
            },
        }

    async def fake_ensure_daily_session(**kwargs):
        calls.append("ensure")
        return SimpleNamespace(session_id="unused")

    async def fake_run_live_session(**kwargs):
        calls.append("live")
        return {"status": "LIVE"}

    monkeypatch.setattr(
        paper_trading, "prepare_runtime_for_daily_paper", fake_prepare_runtime_for_daily_paper
    )
    monkeypatch.setattr(paper_trading, "_ensure_daily_session", fake_ensure_daily_session)
    monkeypatch.setattr(paper_trading, "run_live_session", fake_run_live_session)

    with pytest.raises(SystemExit) as exc:
        await paper_trading._run_daily_workflow(
            mode="live",
            trade_date="2024-01-04",
            symbols=["SBIN"],
            strategy="FBR",
            strategy_params={},
            session_id=None,
            notes=None,
        )

    assert calls == ["prepare"]
    assert "Runtime coverage incomplete" in str(exc.value)


@pytest.mark.asyncio
async def test_ensure_daily_session_reuses_existing_live_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    requested_params = paper_trading._with_resolved_strategy_metadata(
        "CPR_LEVELS", {"rr_ratio": 1.8}
    )
    existing = SimpleNamespace(
        session_id="paper-live-1",
        status="ACTIVE",
        strategy="CPR_LEVELS",
        strategy_params=requested_params,
    )
    calls: list[str] = []

    async def fake_get_session(session_id: str):
        calls.append(session_id)
        return existing

    async def fake_create_paper_session(**kwargs):
        raise AssertionError(f"create_paper_session should not be called: {kwargs}")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    session = await pt._ensure_daily_session(
        session_id="paper-live-1",
        trade_date="2024-01-03",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"rr_ratio": 1.8},
        notes="retry",
        mode="live",
    )

    assert session is existing
    assert calls == ["paper-live-1"]


@pytest.mark.asyncio
async def test_ensure_daily_session_creates_with_strategy_sizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    calls: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        return None

    async def fake_create_paper_session(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(session_id=kwargs["session_id"])

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    await pt._ensure_daily_session(
        session_id="paper-live-1",
        trade_date="2024-01-03",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={
            "capital": 200_000,
            "portfolio_value": 1_000_000,
            "max_positions": 5,
            "max_position_pct": 0.20,
        },
        notes="live",
        mode="live",
    )

    assert calls[0]["portfolio_value"] == 1_000_000
    assert calls[0]["max_positions"] == 5
    assert calls[0]["max_position_pct"] == 0.20
    assert calls[0]["strategy_params"]["_strategy_config_fingerprint"]


@pytest.mark.asyncio
async def test_ensure_daily_session_rejects_live_param_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    existing_params = pt._with_resolved_strategy_metadata(
        "CPR_LEVELS",
        {"rr_ratio": 1.8},
    )
    existing = SimpleNamespace(
        session_id="paper-live-1",
        status="ACTIVE",
        strategy="CPR_LEVELS",
        strategy_params=existing_params,
    )

    async def fake_get_session(session_id: str):
        return existing

    async def fake_create_paper_session(**kwargs):
        raise AssertionError(f"create_paper_session should not be called: {kwargs}")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    with pytest.raises(SystemExit, match="different strategy params"):
        await pt._ensure_daily_session(
            session_id="paper-live-1",
            trade_date="2024-01-03",
            strategy="CPR_LEVELS",
            symbols=["SBIN"],
            strategy_params={"rr_ratio": 2.0},
            notes="retry",
            mode="live",
        )


@pytest.mark.asyncio
async def test_ensure_daily_session_rejects_live_sizing_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    requested_params = pt._with_resolved_strategy_metadata(
        "CPR_LEVELS",
        {
            "portfolio_value": 1_000_000,
            "max_positions": 5,
            "max_position_pct": 0.20,
        },
    )
    existing = SimpleNamespace(
        session_id="paper-live-1",
        status="ACTIVE",
        strategy="CPR_LEVELS",
        strategy_params=requested_params,
        portfolio_value=1_000_000,
        max_positions=10,
        max_position_pct=0.10,
    )

    async def fake_get_session(session_id: str):
        return existing

    async def fake_create_paper_session(**kwargs):
        raise AssertionError(f"create_paper_session should not be called: {kwargs}")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    with pytest.raises(SystemExit, match="execution sizing differs"):
        await pt._ensure_daily_session(
            session_id="paper-live-1",
            trade_date="2024-01-03",
            strategy="CPR_LEVELS",
            symbols=["SBIN"],
            strategy_params=requested_params,
            notes="retry",
            mode="live",
        )


async def test_ensure_daily_session_creates_fallback_for_replay_collisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    existing = SimpleNamespace(session_id="paper-replay-1", status="COMPLETED")
    calls: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        assert session_id == "paper-replay-1"
        return existing

    async def fake_create_paper_session(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(session_id=kwargs["session_id"])

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)
    monkeypatch.setattr(pt, "uuid4", lambda: SimpleNamespace(hex="abcdef123456"))

    session = await pt._ensure_daily_session(
        session_id="paper-replay-1",
        trade_date="2024-01-03",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"rr_ratio": 1.8},
        notes="retry",
        mode="replay",
    )

    assert session.session_id == "paper-replay-1-abcdef"
    assert calls and calls[0]["session_id"] == "paper-replay-1-abcdef"
    assert calls[0]["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_ensure_daily_session_creates_fallback_for_local_live_terminal_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    existing = SimpleNamespace(session_id="paper-live-local-1", status="COMPLETED")
    calls: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        assert session_id == "paper-live-local-1"
        return existing

    async def fake_create_paper_session(**kwargs):
        calls.append(dict(kwargs))
        return SimpleNamespace(session_id=kwargs["session_id"])

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)
    monkeypatch.setattr(pt, "uuid4", lambda: SimpleNamespace(hex="123456abcdef"))

    session = await pt._ensure_daily_session(
        session_id="paper-live-local-1",
        trade_date="2026-05-04",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"direction_filter": "LONG", "feed_source": "local"},
        notes="local drill",
        mode="live",
    )

    assert session.session_id == "paper-live-local-1-123456"
    assert calls and calls[0]["session_id"] == "paper-live-local-1-123456"
    assert calls[0]["status"] == "ACTIVE"


@pytest.mark.parametrize("status", ["FAILED", "COMPLETED", "CANCELLED", "STOPPING", "STOPPED"])
@pytest.mark.asyncio
async def test_ensure_daily_session_rejects_kite_live_terminal_collision(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    import scripts.paper_trading as pt

    existing = SimpleNamespace(session_id="paper-live-kite-1", status=status)

    async def fake_get_session(session_id: str):
        assert session_id == "paper-live-kite-1"
        return existing

    async def fake_create_paper_session(**_kwargs):
        raise AssertionError("terminal Kite live session must not be recreated or reused")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    with pytest.raises(SystemExit, match="refusing to reuse"):
        await pt._ensure_daily_session(
            session_id="paper-live-kite-1",
            trade_date="2026-05-06",
            strategy="CPR_LEVELS",
            symbols=["SBIN"],
            strategy_params={"direction_filter": "LONG", "feed_source": "kite"},
            notes="late test",
            mode="live",
        )


@pytest.mark.parametrize("status", ["PLANNING", "ACTIVE", "PAUSED"])
@pytest.mark.asyncio
async def test_ensure_daily_session_allows_non_terminal_live_collision(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    import scripts.paper_trading as pt

    requested_params = pt._with_resolved_strategy_metadata(
        "CPR_LEVELS",
        {"direction_filter": "LONG", "feed_source": "kite"},
    )
    existing = SimpleNamespace(
        session_id="paper-live-kite-1",
        status=status,
        strategy="CPR_LEVELS",
        strategy_params=requested_params,
        portfolio_value=1000000.0,
        max_daily_loss_pct=0.03,
        max_drawdown_pct=0.1,
        max_positions=5,
        max_position_pct=0.2,
    )

    async def fake_get_session(session_id: str):
        assert session_id == "paper-live-kite-1"
        return existing

    async def fake_create_paper_session(**_kwargs):
        raise AssertionError("non-terminal live session should be reused")

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "create_paper_session", fake_create_paper_session)

    session = await pt._ensure_daily_session(
        session_id="paper-live-kite-1",
        trade_date="2026-05-06",
        strategy="CPR_LEVELS",
        symbols=["SBIN"],
        strategy_params={"direction_filter": "LONG", "feed_source": "kite"},
        notes="late test",
        mode="live",
    )

    assert session is existing


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_reuses_same_session_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-live-1",
        status="FAILED",
        strategy="CPR_LEVELS",
    )
    open_positions = [
        SimpleNamespace(symbol="SBIN"),
        SimpleNamespace(symbol="RELIANCE"),
    ]
    calls: dict[str, object] = {}
    alerts: list[dict[str, object]] = []

    async def fake_get_session(session_id: str):
        calls["get_session"] = session_id
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        calls["get_positions"] = {
            "session_id": session_id,
            "statuses": list(statuses or []),
            "symbol": symbol,
        }
        return open_positions

    async def fake_run_live_session(**kwargs):
        calls["run"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(pt, "run_live_session", fake_run_live_session)
    monkeypatch.setattr(
        pt,
        "dispatch_session_state_alert",
        lambda **kwargs: alerts.append(dict(kwargs)),
    )

    await pt._cmd_daily_live_resume(
        SimpleNamespace(
            session_id="paper-live-1",
            poll_interval_sec=1.5,
            candle_interval_minutes=5,
            max_cycles=9,
            complete_on_exit=False,
            no_alerts=False,
        )
    )

    assert calls["get_session"] == "paper-live-1"
    assert calls["get_positions"] == {
        "session_id": "paper-live-1",
        "statuses": ["OPEN"],
        "symbol": None,
    }
    assert calls["run"]["session_id"] == "paper-live-1"
    assert calls["run"]["symbols"] == ["SBIN", "RELIANCE"]
    assert calls["run"]["poll_interval_sec"] == 1.5
    assert calls["run"]["candle_interval_minutes"] == 5
    assert calls["run"]["max_cycles"] == 9
    assert calls["run"]["auto_flatten_on_abnormal_exit"] is False
    assert alerts and alerts[0]["state"] == "RESUMED"
    assert alerts[0]["session_id"] == "paper-live-1"


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_reconstructs_local_feed_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-live-local",
        status="FAILED",
        strategy="CPR_LEVELS",
        strategy_params={"feed_source": "local"},
        trade_date="2026-04-30",
    )
    calls: dict[str, object] = {}

    class FakeLocalTickerAdapter:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    async def fake_get_session(_session_id: str):
        return session

    async def fake_get_session_positions(_session_id: str, statuses=None, symbol=None):
        return [SimpleNamespace(symbol="SBIN")]

    async def fake_run_live_session(**kwargs):
        calls["run"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr("engine.local_ticker_adapter.LocalTickerAdapter", FakeLocalTickerAdapter)
    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(pt, "run_live_session", fake_run_live_session)
    monkeypatch.setattr(pt, "dispatch_session_state_alert", lambda **kwargs: None)

    await pt._cmd_daily_live_resume(
        SimpleNamespace(
            session_id="paper-live-local",
            trade_date="2026-04-30",
            strategy="CPR_LEVELS",
            preset=None,
            strategy_params=None,
            poll_interval_sec=1.0,
            candle_interval_minutes=5,
            max_cycles=1,
            complete_on_exit=False,
            no_alerts=False,
        )
    )

    adapter = calls["run"]["ticker_adapter"]
    assert isinstance(adapter, FakeLocalTickerAdapter)
    assert adapter.kwargs == {
        "trade_date": "2026-04-30",
        "symbols": ["SBIN"],
        "candle_interval_minutes": 5,
    }


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_infers_session_id_from_preset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-cpr_levels-short-2024-01-03-live-kite",
        status="FAILED",
        strategy="CPR_LEVELS",
    )
    open_positions = [SimpleNamespace(symbol="SBIN")]
    calls: dict[str, object] = {}

    async def fake_get_session(session_id: str):
        calls["get_session"] = session_id
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        calls["get_positions"] = {
            "session_id": session_id,
            "statuses": list(statuses or []),
            "symbol": symbol,
        }
        return open_positions

    async def fake_run_live_session(**kwargs):
        calls["run"] = kwargs
        return {"status": "LIVE"}

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)
    monkeypatch.setattr(pt, "run_live_session", fake_run_live_session)
    monkeypatch.setattr(pt, "dispatch_session_state_alert", lambda **kwargs: None)

    await pt._cmd_daily_live_resume(
        SimpleNamespace(
            session_id=None,
            trade_date="2024-01-03",
            strategy="CPR_LEVELS",
            preset="CPR_LEVELS_RISK_SHORT",
            strategy_params=None,
            poll_interval_sec=1.5,
            candle_interval_minutes=5,
            max_cycles=9,
            complete_on_exit=False,
            no_alerts=True,
        )
    )

    assert calls["get_session"] == "paper-cpr_levels-short-2024-01-03-live-kite"
    assert calls["get_positions"] == {
        "session_id": "paper-cpr_levels-short-2024-01-03-live-kite",
        "statuses": ["OPEN"],
        "symbol": None,
    }
    assert calls["run"]["session_id"] == "paper-cpr_levels-short-2024-01-03-live-kite"
    assert calls["run"]["symbols"] == ["SBIN"]
    assert calls["run"]["poll_interval_sec"] == 1.5
    assert calls["run"]["candle_interval_minutes"] == 5
    assert calls["run"]["max_cycles"] == 9


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name, expected_state",
    [
        ("_cmd_pause", "PAUSED"),
        ("_cmd_resume", "RESUMED"),
    ],
)
async def test_pause_and_resume_commands_emit_state_alerts(
    monkeypatch: pytest.MonkeyPatch,
    handler_name: str,
    expected_state: str,
) -> None:
    import scripts.paper_trading as pt
    from db.paper_db import PaperSession

    alerts: list[dict[str, object]] = []

    def fake_update_session(session_id: str, **kwargs):
        return PaperSession(
            session_id=session_id,
            strategy="CPR_LEVELS",
            status=str(kwargs.get("status") or "ACTIVE"),
        )

    monkeypatch.setattr(pt, "_pdb", lambda: SimpleNamespace(update_session=fake_update_session))
    monkeypatch.setattr(
        pt,
        "dispatch_session_state_alert",
        lambda **kwargs: alerts.append(dict(kwargs)),
    )

    handler = getattr(pt, handler_name)
    await handler(SimpleNamespace(session_id="paper-live-1", notes="manual op"))

    assert alerts and alerts[0]["state"] == expected_state
    assert alerts[0]["session_id"] == "paper-live-1"


@pytest.mark.asyncio
async def test_cmd_flatten_shuts_down_dispatcher_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    calls: list[str] = []

    async def fake_flatten_session_positions(session_id: str, *, notes: str | None = None):
        calls.append("flatten")
        raise RuntimeError("boom")

    async def fake_shutdown():
        calls.append("shutdown")

    monkeypatch.setattr(pt, "flatten_session_positions", fake_flatten_session_positions)
    monkeypatch.setattr(pt, "maybe_shutdown_alert_dispatcher", fake_shutdown)
    monkeypatch.setattr(pt, "register_session_start", lambda: calls.append("register"))
    monkeypatch.setattr(pt, "_start_alert_dispatcher", lambda: calls.append("start"))

    with pytest.raises(RuntimeError, match="boom"):
        await pt._cmd_flatten(SimpleNamespace(session_id="paper-live-1", notes="manual"))

    assert calls == ["register", "start", "flatten", "shutdown"]


@pytest.mark.asyncio
async def test_cmd_flatten_archives_completed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt
    from db.paper_db import PaperSession

    calls: list[tuple[str, object]] = []

    async def fake_flatten_session_positions(session_id: str, *, notes: str | None = None):
        calls.append(("flatten", session_id))
        return {"closed_positions": 2}

    async def fake_update_session_state(session_id: str, **kwargs):
        calls.append(("state", (session_id, kwargs)))

    def fake_archive_completed_session(session_id: str):
        calls.append(("archive", session_id))
        return {"trade_count": 2}

    fake_db = SimpleNamespace(
        get_session=lambda session_id: PaperSession(
            session_id=session_id,
            strategy="CPR_LEVELS",
            status="COMPLETED",
        ),
        force_sync=lambda: calls.append(("force_sync", None)),
    )

    async def fake_shutdown():
        calls.append(("shutdown", None))

    monkeypatch.setattr(pt, "flatten_session_positions", fake_flatten_session_positions)
    monkeypatch.setattr(pt, "update_session_state", fake_update_session_state)
    monkeypatch.setattr(pt, "archive_completed_session", fake_archive_completed_session)
    monkeypatch.setattr(pt, "_pdb", lambda: fake_db)
    monkeypatch.setattr(pt, "maybe_shutdown_alert_dispatcher", fake_shutdown)
    monkeypatch.setattr(pt, "register_session_start", lambda: None)
    monkeypatch.setattr(pt, "_start_alert_dispatcher", lambda: None)

    await pt._cmd_flatten(SimpleNamespace(session_id="paper-live-1", notes="manual"))

    assert calls[:4] == [
        ("flatten", "paper-live-1"),
        ("state", ("paper-live-1", {"status": "COMPLETED", "notes": "manual"})),
        ("archive", "paper-live-1"),
        ("force_sync", None),
    ]
    assert calls[-1] == ("shutdown", None)


@pytest.mark.asyncio
async def test_cmd_flatten_all_archives_each_completed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    calls: list[tuple[str, object]] = []

    class _Rows:
        def fetchall(self):
            return [("paper-long", "ACTIVE"), ("paper-short", "FAILED")]

    def fake_execute(sql: str, *args, **kwargs):
        assert "CANCELLED" not in sql
        return _Rows()

    fake_db = SimpleNamespace(
        con=SimpleNamespace(execute=fake_execute),
        force_sync=lambda: calls.append(("force_sync", None)),
    )

    async def fake_flatten_session_positions(session_id: str, *, notes: str | None = None):
        calls.append(("flatten", session_id))
        return {"closed_positions": 1}

    async def fake_update_session_state(session_id: str, **kwargs):
        calls.append(("state", (session_id, kwargs)))

    def fake_archive_completed_session(session_id: str):
        calls.append(("archive", session_id))
        return {"trade_count": 1}

    async def fake_shutdown():
        calls.append(("shutdown", None))

    monkeypatch.setattr(pt, "resolve_trade_date", lambda value: value)
    monkeypatch.setattr(pt, "_pdb", lambda: fake_db)
    monkeypatch.setattr(pt, "flatten_session_positions", fake_flatten_session_positions)
    monkeypatch.setattr(pt, "update_session_state", fake_update_session_state)
    monkeypatch.setattr(pt, "archive_completed_session", fake_archive_completed_session)
    monkeypatch.setattr(pt, "maybe_shutdown_alert_dispatcher", fake_shutdown)
    monkeypatch.setattr(pt, "register_session_start", lambda: None)
    monkeypatch.setattr(pt, "_start_alert_dispatcher", lambda: None)

    await pt._cmd_flatten_all(SimpleNamespace(trade_date="2026-04-24", notes="manual"))

    assert ("archive", "paper-long") in calls
    assert ("archive", "paper-short") in calls
    assert calls.count(("force_sync", None)) == 1
    assert calls[-1] == ("shutdown", None)


@pytest.mark.asyncio
async def test_cmd_daily_live_resume_rejects_session_without_open_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.paper_trading as pt

    session = SimpleNamespace(
        session_id="paper-live-1",
        status="FAILED",
        strategy="CPR_LEVELS",
    )

    async def fake_get_session(session_id: str):
        assert session_id == "paper-live-1"
        return session

    async def fake_get_session_positions(session_id: str, statuses=None, symbol=None):
        assert session_id == "paper-live-1"
        assert list(statuses or []) == ["OPEN"]
        assert symbol is None
        return []

    monkeypatch.setattr(pt, "get_session", fake_get_session)
    monkeypatch.setattr(pt, "get_session_positions", fake_get_session_positions)

    with pytest.raises(SystemExit, match="No OPEN positions"):
        await pt._cmd_daily_live_resume(
            SimpleNamespace(
                session_id="paper-live-1",
                poll_interval_sec=1.0,
                candle_interval_minutes=5,
                max_cycles=None,
                complete_on_exit=False,
                no_alerts=False,
            )
        )
