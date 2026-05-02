"""Inspect project lock files and their owning processes."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _is_pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _process_command(pid: int) -> str | None:
    if os.name != "nt":
        try:
            return Path(f"/proc/{pid}/cmdline").read_text(encoding="utf-8").replace("\x00", " ")
        except OSError:
            return None
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    command = result.stdout.strip()
    return command or None


def _read_key_value(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep:
                data[key.strip()] = value.strip()
    except OSError:
        pass
    return data


def _read_market_lock(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return {}
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"pid": int(raw)}
        except ValueError:
            return {"raw": raw}
    return payload if isinstance(payload, dict) else {"raw": raw}


def _inspect_lock(name: str, path: Path, *, kind: str) -> dict[str, Any]:
    exists = path.exists()
    payload: dict[str, Any] = {}
    if kind == "market":
        payload = _read_market_lock(path)
    elif kind == "runtime":
        payload = _read_key_value(path.with_suffix(path.suffix + ".info"))
        if not payload:
            payload = _read_key_value(path)

    pid_raw = payload.get("pid")
    pid = int(pid_raw) if str(pid_raw or "").isdigit() else None
    alive = _is_pid_alive(pid) if pid is not None else None
    kill_command = None
    if pid is not None:
        kill_command = f"taskkill //F //PID {pid}" if os.name == "nt" else f"kill {pid}"

    return {
        "name": name,
        "path": str(path),
        "exists": exists,
        "pid": pid,
        "alive": alive,
        "detail": payload.get("detail"),
        "started_at": payload.get("started_at") or payload.get("acquired_at"),
        "command": _process_command(pid) if pid is not None and alive else None,
        "kill_command": kill_command,
        "stale": bool(exists and pid is not None and alive is False),
    }


def collect_lock_status() -> list[dict[str, Any]]:
    return [
        _inspect_lock(
            "market_duckdb_writelock",
            PROJECT_ROOT / "data" / "market.duckdb.writelock",
            kind="market",
        ),
        _inspect_lock(
            "runtime_writer",
            PROJECT_ROOT / ".tmp_logs" / "runtime-writer.lock",
            kind="runtime",
        ),
    ]


def _print_table(rows: list[dict[str, Any]]) -> None:
    print(f"{'Lock':<26} {'Exists':<7} {'PID':<8} {'Alive':<7} Detail")
    print("-" * 90)
    for row in rows:
        pid = "" if row["pid"] is None else str(row["pid"])
        alive = "" if row["alive"] is None else str(row["alive"])
        print(
            f"{row['name']:<26} {row['exists']!s:<7} {pid:<8} {alive:<7} {row.get('detail') or ''}"
        )
        if row.get("command"):
            print(f"  command: {row['command']}")
        if row.get("stale"):
            print("  stale: process is gone; retrying the writer should self-clear when supported")
        elif row.get("alive"):
            print(f"  kill:    {row['kill_command']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect CPR Pivot Lab writer locks.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()
    rows = collect_lock_status()
    if args.json:
        print(json.dumps({"locks": rows}, indent=2, default=str))
    else:
        _print_table(rows)


if __name__ in {"__main__", "__mp_main__"}:
    main()
