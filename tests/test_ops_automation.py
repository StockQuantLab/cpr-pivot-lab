from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts import baseline_registry, clean_artifacts, lock_status


def test_clean_tmp_logs_preserves_locks_and_control_files(tmp_path: Path) -> None:
    root = tmp_path
    tmp_logs = root / ".tmp_logs"
    tmp_logs.mkdir()
    old_log = tmp_logs / "live_20260501.log"
    active_lock = tmp_logs / "runtime-writer.lock"
    lock_info = tmp_logs / "runtime-writer.lock.info"
    flatten_signal = tmp_logs / "flatten_session.signal"
    cmd_file = tmp_logs / "cmd_session" / "close_all.json"
    cmd_file.parent.mkdir()
    for path in (old_log, active_lock, lock_info, flatten_signal, cmd_file):
        path.write_text("x", encoding="utf-8")
        old_ts = (datetime.now(UTC) - timedelta(days=30)).timestamp()
        os.utime(path, (old_ts, old_ts))

    selected = clean_artifacts._collect_tmp_log_files(root, older_than_days=14)

    assert selected == [old_log]


def test_clean_tmp_logs_missing_directory_returns_empty(tmp_path: Path) -> None:
    assert clean_artifacts._collect_tmp_log_files(tmp_path, older_than_days=14) == []


def test_lock_status_reads_runtime_lock_sidecar(tmp_path: Path, monkeypatch) -> None:
    lock_path = tmp_path / "runtime-writer.lock"
    lock_path.write_text("", encoding="utf-8")
    lock_path.with_suffix(".lock.info").write_text(
        "pid=123\ndetail=runtime writer\nstarted_at=2026-05-02T09:00:00Z\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(lock_status, "_is_pid_alive", lambda pid: pid == 123)
    monkeypatch.setattr(lock_status, "_process_command", lambda pid: "uv run pivot-build")

    payload = lock_status._inspect_lock("runtime_writer", lock_path, kind="runtime")

    assert payload["exists"] is True
    assert payload["pid"] == 123
    assert payload["alive"] is True
    assert payload["detail"] == "runtime writer"
    assert payload["command"] == "uv run pivot-build"


def test_lock_status_marks_dead_pid_stale(tmp_path: Path, monkeypatch) -> None:
    lock_path = tmp_path / "runtime-writer.lock"
    lock_path.write_text("", encoding="utf-8")
    lock_path.with_suffix(".lock.info").write_text("pid=456\n", encoding="utf-8")
    monkeypatch.setattr(lock_status, "_is_pid_alive", lambda pid: False)
    monkeypatch.setattr(lock_status, "_process_command", lambda pid: "should-not-read")

    payload = lock_status._inspect_lock("runtime_writer", lock_path, kind="runtime")

    assert payload["pid"] == 456
    assert payload["alive"] is False
    assert payload["stale"] is True
    assert payload["command"] is None


def test_baseline_registry_validates_eight_unique_variants(tmp_path: Path) -> None:
    variants = [
        {
            "label": f"v{i}",
            "preset": "CPR_LEVELS_RISK_LONG" if i % 2 else "CPR_LEVELS_RISK_SHORT",
            "direction": "LONG" if i % 2 else "SHORT",
            "compound_equity": bool(i % 3),
            "run_id": f"run{i}",
        }
        for i in range(8)
    ]
    registry = {
        "name": "test",
        "universe": "full_2026_04_30",
        "variants": variants,
    }
    path = tmp_path / "registry.yaml"
    import yaml

    path.write_text(yaml.safe_dump(registry), encoding="utf-8")

    payload = baseline_registry.validate_registry(path, check_db=False)

    assert payload["ok"] is True
    assert payload["variant_count"] == 8
    assert json.loads(json.dumps(payload))["run_ids"][0] == "run0"


def test_baseline_registry_rejects_duplicate_run_ids(tmp_path: Path) -> None:
    variants = [
        {
            "label": f"v{i}",
            "preset": "CPR_LEVELS_RISK_LONG" if i % 2 else "CPR_LEVELS_RISK_SHORT",
            "direction": "LONG" if i % 2 else "SHORT",
            "run_id": "dup" if i in {0, 1} else f"run{i}",
        }
        for i in range(8)
    ]
    path = tmp_path / "registry.yaml"
    import yaml

    path.write_text(yaml.safe_dump({"variants": variants}), encoding="utf-8")

    payload = baseline_registry.validate_registry(path, check_db=False)

    assert payload["ok"] is False
    assert "duplicate run_id dup" in payload["errors"]
