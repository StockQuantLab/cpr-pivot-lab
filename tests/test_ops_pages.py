"""Tests for dashboard tab event helpers."""

from __future__ import annotations

from types import SimpleNamespace

from web.pages.ops_pages import _build_session_risk_cards, _extract_tab_value


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


def test_build_session_risk_cards_shows_effective_slot_sizing() -> None:
    cards = _build_session_risk_cards(
        SimpleNamespace(
            portfolio_value=1_000_000,
            max_positions=5,
            max_position_pct=0.20,
        ),
        {"info": "i", "primary": "p", "warning": "w", "success": "s"},
    )

    values = {card["title"]: card["value"] for card in cards}
    subtitles = {card["title"]: card["subtitle"] for card in cards}
    assert values["Capital"] == "₹1,000,000"
    assert values["Max Pos"] == "5"
    assert values["Slot Cap"] == "₹200,000"
    assert values["Sizing"] == "5 x ₹200,000"
    assert subtitles["Slot Cap"] == "20% per position"


def test_build_session_risk_cards_reflects_ten_by_one_lakh_sizing() -> None:
    cards = _build_session_risk_cards(
        SimpleNamespace(
            portfolio_value=1_000_000,
            max_positions=10,
            max_position_pct=0.10,
        ),
        {"info": "i", "primary": "p", "warning": "w", "success": "s"},
    )

    values = {card["title"]: card["value"] for card in cards}
    assert values["Sizing"] == "10 x ₹100,000"
    assert values["Slot Cap"] == "₹100,000"
