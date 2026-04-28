"""Tests for dashboard tab event helpers."""

from __future__ import annotations

from types import SimpleNamespace

from web.pages.ops_pages import _extract_tab_value


def test_extract_tab_value_prefers_args_value() -> None:
    assert (
        _extract_tab_value(SimpleNamespace(args={"value": "archived", "modelValue": "daily"}))
        == "archived"
    )


def test_extract_tab_value_prefers_args_model_value() -> None:
    assert _extract_tab_value(SimpleNamespace(args={"modelValue": "daily"})) == "daily"


def test_extract_tab_value_supports_raw_args_string() -> None:
    assert _extract_tab_value(SimpleNamespace(args="archived")) == "archived"


def test_extract_tab_value_falls_back_to_value() -> None:
    assert _extract_tab_value(SimpleNamespace(value="active")) == "active"


def test_extract_tab_value_returns_none_for_unset() -> None:
    assert _extract_tab_value(SimpleNamespace()) is None
    assert _extract_tab_value(object()) is None
    assert _extract_tab_value(None) is None
