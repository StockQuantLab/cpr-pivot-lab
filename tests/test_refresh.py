from __future__ import annotations

import sys
from types import SimpleNamespace

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


def test_eod_ingest_runs_strict_order(monkeypatch) -> None:
    captured: list[list[str]] = []

    def fake_run(cmd, *, dry_run, timeout=3600):
        assert dry_run is True
        assert timeout is None
        captured.append(cmd)
        return 0

    monkeypatch.setattr(refresh, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-refresh",
            "--eod-ingest",
            "--date",
            "2026-04-29",
            "--trade-date",
            "2026-04-30",
            "--dry-run",
        ],
    )

    refresh.main()

    assert [cmd[2] for cmd in captured] == [
        "scripts.kite_ingest",  # 0: refresh_instruments
        "scripts.kite_ingest",  # 1: ingest_daily
        "scripts.kite_ingest",  # 2: ingest_5min
        "scripts.build_tables",  # 3: build_runtime
        "scripts.build_tables",  # 4: build_next_day_cpr
        "scripts.build_tables",  # 5: build_next_day_thresholds
        "scripts.build_tables",  # 6: build_next_day_state
        "scripts.build_tables",  # 7: build_next_day_strategy
        "scripts.sync_replica",  # 8: sync_replica
        "scripts.paper_trading",  # 9: daily_prepare
        "scripts.data_quality",  # 10: data_quality
    ]
    assert captured[0][3:] == ["--refresh-instruments", "--exchange", "NSE"]
    assert captured[1][3:] == [
        "--from",
        "2026-04-29",
        "--to",
        "2026-04-29",
        "--skip-existing",
    ]
    assert captured[2][3:] == [
        "--from",
        "2026-04-29",
        "--to",
        "2026-04-29",
        "--5min",
        "--resume",
        "--skip-existing",
    ]
    assert captured[3][3:] == ["--refresh-since", "2026-04-29", "--batch-size", "128"]
    assert captured[4][3:] == [
        "--table",
        "cpr",
        "--refresh-date",
        "2026-04-30",
        "--batch-size",
        "128",
    ]
    assert captured[5][3:] == [
        "--table",
        "thresholds",
        "--refresh-date",
        "2026-04-30",
        "--batch-size",
        "128",
    ]
    assert captured[6][3:] == [
        "--table",
        "state",
        "--refresh-date",
        "2026-04-30",
        "--batch-size",
        "128",
    ]
    assert captured[7][3:] == [
        "--table",
        "strategy",
        "--refresh-date",
        "2026-04-30",
        "--batch-size",
        "128",
    ]
    assert captured[8][3:] == ["--verify", "--trade-date", "2026-04-30"]
    assert captured[9][3:] == [
        "daily-prepare",
        "--trade-date",
        "2026-04-30",
        "--all-symbols",
    ]
    assert captured[10][3:] == ["--date", "2026-04-30"]


def test_eod_ingest_logs_stage_names(monkeypatch, capsys) -> None:
    def fake_run(cmd, *, dry_run, timeout=3600):
        del cmd, dry_run, timeout
        return 0

    monkeypatch.setattr(refresh, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-refresh",
            "--eod-ingest",
            "--date",
            "2026-04-29",
            "--trade-date",
            "2026-04-30",
        ],
    )

    refresh.main()

    out = capsys.readouterr().out
    assert "START refresh_instruments" in out
    assert "START ingest_daily" in out
    assert "START ingest_5min" in out
    assert "START build_runtime" in out
    assert "START build_next_day_cpr" in out
    assert "START build_next_day_thresholds" in out
    assert "START build_next_day_state" in out
    assert "START build_next_day_strategy" in out
    assert "START sync_replica" in out
    assert "START daily_prepare" in out
    assert "START data_quality" in out
    assert "EOD pipeline complete" in out


def test_eod_ingest_requires_next_trade_date(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["pivot-refresh", "--eod-ingest", "--date", "2026-04-29"],
    )

    try:
        refresh.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover
        raise AssertionError("expected parser error")


def test_eod_ingest_force_ingest_disables_skip_existing(monkeypatch) -> None:
    captured: list[list[str]] = []

    def fake_run(cmd, *, dry_run, timeout=3600):
        del dry_run, timeout
        captured.append(cmd)
        return 0

    monkeypatch.setattr(refresh, "_run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pivot-refresh",
            "--eod-ingest",
            "--date",
            "2026-04-29",
            "--trade-date",
            "2026-04-30",
            "--force-ingest",
        ],
    )

    refresh.main()

    assert "--skip-existing" not in captured[1]
    assert "--skip-existing" not in captured[2]


def test_run_streams_child_output(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        calls["cmd"] = cmd
        calls["kwargs"] = kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(refresh, "close_dashboard_db", lambda: None)
    monkeypatch.setattr(refresh.subprocess, "run", fake_run)

    code = refresh._run(["python", "-m", "demo"], dry_run=False, timeout=None)

    assert code == 0
    assert "capture_output" not in calls["kwargs"]
    assert calls["kwargs"]["cwd"] == str(refresh.PROJECT_ROOT)
    assert calls["kwargs"]["timeout"] is None
    assert "$ python -m demo" in capsys.readouterr().out
