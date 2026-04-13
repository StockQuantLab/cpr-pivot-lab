from __future__ import annotations

import sys

import scripts.refresh as refresh


def test_detect_refresh_since_uses_next_day(monkeypatch) -> None:
    class _FakeDB:
        @staticmethod
        def get_table_max_trade_dates(tables: list[str]) -> dict[str, str | None]:
            assert "market_day_state" in tables
            return {
                "cpr_daily": "2026-03-20",
                "market_day_state": "2026-03-21",
                "strategy_day_state": "2026-03-21",
            }

    monkeypatch.setattr(refresh, "get_dashboard_db", lambda: _FakeDB())

    assert refresh._detect_refresh_since() == "2026-03-22"


def test_main_skips_build_when_daily_prepare_is_ready(monkeypatch) -> None:
    captured: list[list[str]] = []

    monkeypatch.setattr(refresh, "_detect_refresh_since", lambda: "2026-03-22")

    def fake_run(cmd, *, dry_run, timeout=3600):
        del dry_run, timeout
        captured.append(cmd)
        if cmd[2:4] == ["scripts.paper_trading", "daily-prepare"]:
            return 0
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(refresh, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-refresh",
            "--since",
            "2026-03-20",
            "--prepare-paper",
            "--trade-date",
            "2026-03-23",
        ],
    )

    refresh.main()

    assert len(captured) == 1
    assert captured[0][2:4] == ["scripts.paper_trading", "daily-prepare"]
    assert captured[0][5] == "2026-03-23"


def test_main_builds_only_when_daily_prepare_needs_refresh(monkeypatch) -> None:
    captured: list[list[str]] = []
    prepare_calls = 0

    monkeypatch.setattr(refresh, "_detect_refresh_since", lambda: "2026-03-22")

    def fake_run(cmd, *, dry_run, timeout=3600):
        nonlocal prepare_calls
        del dry_run, timeout
        captured.append(cmd)
        if cmd[2:4] == ["scripts.paper_trading", "daily-prepare"]:
            prepare_calls += 1
            return 1 if prepare_calls == 1 else 0
        if cmd[2:4] == ["scripts.build_tables", "--refresh-since"]:
            return 0
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(refresh, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-refresh",
            "--since",
            "2026-03-20",
            "--prepare-paper",
            "--trade-date",
            "2026-03-23",
        ],
    )

    refresh.main()

    assert [cmd[2] for cmd in captured] == [
        "scripts.paper_trading",
        "scripts.build_tables",
        "scripts.paper_trading",
    ]
    assert captured[1][4] == "2026-03-20"
    assert captured[2][5] == "2026-03-23"
