"""Tests for backtest run detail rendering helpers."""

from __future__ import annotations

import json

from web.pages.run_detail import _effective_run_params, _extract_run_params


def test_extract_run_params_prefers_run_metadata() -> None:
    meta = {"params_json": json.dumps({"risk_based_sizing": False})}
    run_meta = {"params": {"risk_based_sizing": True, "min_price": 50}}

    params = _extract_run_params(meta, run_meta)

    assert params["risk_based_sizing"] is True
    assert params["min_price"] == 50


def test_extract_run_params_falls_back_to_params_json() -> None:
    meta = {"params_json": json.dumps({"risk_based_sizing": False, "min_price": 0})}

    params = _extract_run_params(meta, {})

    assert params["risk_based_sizing"] is False
    assert params["min_price"] == 0


def test_effective_run_params_marks_skipped_rvol_as_off() -> None:
    params = {"skip_rvol_check": True, "rvol_threshold": 1.2, "min_price": 50}

    display = _effective_run_params(params)

    assert display["rvol_threshold"] == "OFF"
    assert display["min_price"] == 50
    assert params["rvol_threshold"] == 1.2
