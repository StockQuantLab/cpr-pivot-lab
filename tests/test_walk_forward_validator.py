from __future__ import annotations

from types import SimpleNamespace

import polars as pl

import engine.walk_forward_validator as wfv


def test_iter_session_calendar_trade_dates_falls_back_to_daily_rows(
    monkeypatch,
) -> None:
    class _FakeCon:
        def execute(self, query: str, params: list[str]):
            assert params == ["2026-03-01", "2026-03-20"]
            if "FROM v_5min" in query:
                raise RuntimeError("v_5min unavailable")
            if "FROM v_daily" in query:
                return SimpleNamespace(
                    fetchall=lambda: [
                        ("2026-03-02",),
                        ("2026-03-03",),
                        ("2026-03-05",),
                    ]
                )
            raise AssertionError(f"unexpected query: {query}")

    class _FakeDB:
        con = _FakeCon()

    monkeypatch.setattr(wfv, "get_db", lambda: _FakeDB())

    dates = wfv.iter_session_calendar_trade_dates("2026-03-01", "2026-03-20")

    assert dates == ["2026-03-02", "2026-03-03", "2026-03-05"]


def test_run_fast_walk_forward_validation_continues_when_save_to_db_fails(
    monkeypatch,
) -> None:
    class _FakeResult:
        def __init__(self, run_id: str, trade_date: str, should_fail: bool):
            self.run_id = run_id
            self._should_fail = should_fail
            self.df = pl.DataFrame(
                {
                    "run_id": [run_id],
                    "symbol": ["SBIN"],
                    "trade_date": [trade_date],
                    "profit_loss": [100.0],
                }
            )

        def save_to_db(self, db, *, wf_run_id: str | None = None) -> int:
            if self._should_fail:
                raise RuntimeError("WAL locked")
            return int(self.df.height)

    class _FakeBacktest:
        def __init__(self, params, db):
            self.params = params
            self.db = db

        def run(self, *, symbols, start, end, verbose=False, use_cache=True):
            should_fail = start == "2026-03-11"
            return _FakeResult(run_id=f"run-{start}", trade_date=start, should_fail=should_fail)

    monkeypatch.setattr(wfv, "CPRATRBacktest", _FakeBacktest)
    monkeypatch.setattr(
        wfv,
        "iter_session_calendar_trade_dates",
        lambda start_date, end_date: ["2026-03-11", "2026-03-12"],
    )
    monkeypatch.setattr(wfv, "get_db", lambda: SimpleNamespace())

    payload = wfv.run_fast_walk_forward_validation(
        start_date="2026-03-11",
        end_date="2026-03-12",
        symbols=["SBIN"],
        strategy="CPR_LEVELS",
        strategy_params={},
        wf_run_id="wf-1",
        force=False,
    )

    assert [fold["persisted_to_db"] for fold in payload["folds"]] == [False, True]
    assert "WAL locked" in payload["folds"][0]["persist_error"]
    assert payload["folds"][1]["persisted_to_db"] is True
    assert payload["folds"][1].get("persist_error") is None
    assert payload["folds"][1]["trade_date"] == "2026-03-12"
