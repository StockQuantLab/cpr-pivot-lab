"""Tests for walk-forward dashboard helpers."""

from __future__ import annotations

from web.components import NAV_ITEMS
from web.state import build_walk_forward_run_options


def test_nav_items_include_walk_forward_page() -> None:
    assert any(item.get("path") == "/walk_forward" for item in NAV_ITEMS)


def test_build_walk_forward_run_options_includes_gate_metadata() -> None:
    options = build_walk_forward_run_options(
        [
            {
                "wf_run_id": "wf-fast-cpr_levels-2026-03-10-2026-03-20-926159a4e850",
                "strategy": "CPR_LEVELS",
                "direction_filter": "LONG",
                "start_date": "2026-03-10",
                "end_date": "2026-03-20",
                "decision": "PASS",
                "replayed_days": 9,
                "days_requested": 9,
                "scope_key": "ALL:1819",
                "gate_key": "926159a4e8506c47",
            }
        ]
    )

    label, run_id = next(iter(options.items()))
    assert run_id.startswith("wf-fast-cpr_levels")
    assert "CPR_LEVELS [LONG]" in label
    assert "PASS" in label
    assert "ALL:1819" in label
    assert "926159a4" in label
