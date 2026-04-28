"""External supervisor for live paper trading.

This wrapper exists because in-process retry/finally blocks cannot diagnose a
native crash or abrupt Python process exit. It launches ``daily-live`` as a
child process, redirects logs, and writes process-level heartbeat/exit events.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = PROJECT_ROOT / ".tmp_logs" / "supervisor"


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str, ensure_ascii=True) + "\n")
        handle.flush()


def _tail_lines(path: Path, count: int) -> list[str]:
    if count <= 0 or not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return [f"<failed to read {path}: {exc}>"]
    return lines[-count:]


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _windows_memory_info(pid: int) -> dict[str, int] | None:
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return None

    class ProcessMemoryCountersEx(ctypes.Structure):
        _fields_: ClassVar[list[tuple[str, Any]]] = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    process_query_limited_information = 0x1000
    process_vm_read = 0x0010
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    handle = kernel32.OpenProcess(
        process_query_limited_information | process_vm_read,
        False,
        pid,
    )
    if not handle:
        return None
    try:
        counters = ProcessMemoryCountersEx()
        counters.cb = ctypes.sizeof(ProcessMemoryCountersEx)
        ok = psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            counters.cb,
        )
        if not ok:
            return None
        return {
            "rss_bytes": int(counters.WorkingSetSize),
            "peak_rss_bytes": int(counters.PeakWorkingSetSize),
            "private_bytes": int(counters.PrivateUsage),
            "pagefile_bytes": int(counters.PagefileUsage),
        }
    finally:
        kernel32.CloseHandle(handle)


def _normalize_live_args(raw: list[str]) -> list[str]:
    live_args = list(raw)
    if live_args and live_args[0] == "--":
        live_args = live_args[1:]
    if live_args and live_args[0] == "daily-live":
        live_args = live_args[1:]
    if not live_args:
        live_args = ["--multi", "--strategy", "CPR_LEVELS", "--trade-date", "today"]
    return live_args


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pivot-paper-supervisor",
        description=(
            "Launch pivot-paper-trading daily-live as a child process and record "
            "process-level heartbeats, exit code, and log tails."
        ),
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Optional run name used in log filenames. Default: live_<timestamp>.",
    )
    parser.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help="Directory for supervisor heartbeat/stdout/stderr logs.",
    )
    parser.add_argument(
        "--poll-sec",
        type=float,
        default=30.0,
        help="Heartbeat interval in seconds.",
    )
    parser.add_argument(
        "--tail-lines",
        type=int,
        default=80,
        help="Number of stdout/stderr lines captured in the final exit event.",
    )
    parser.add_argument(
        "--no-faulthandler",
        action="store_true",
        help="Do not set PYTHONFAULTHANDLER=1 for the child process.",
    )
    parser.add_argument(
        "live_args",
        nargs=argparse.REMAINDER,
        help=(
            "Arguments for daily-live. Use '--' before them, for example: "
            "pivot-paper-supervisor -- --multi --strategy CPR_LEVELS --trade-date today"
        ),
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    live_args = _normalize_live_args(args.live_args)
    log_dir = Path(args.log_dir)
    run_name = args.name or f"live_{_stamp()}"
    heartbeat_path = log_dir / f"{run_name}.heartbeat.jsonl"
    stdout_path = log_dir / f"{run_name}.stdout.log"
    stderr_path = log_dir / f"{run_name}.stderr.log"
    command = [sys.executable, "-m", "scripts.paper_trading", "daily-live", *live_args]

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    if not args.no_faulthandler:
        env.setdefault("PYTHONFAULTHANDLER", "1")

    child: subprocess.Popen[str] | None = None

    def _terminate_child(signum: int, _frame: object) -> None:
        if child is not None and child.poll() is None:
            _append_jsonl(
                heartbeat_path,
                {
                    "event": "supervisor_signal",
                    "ts": _now_iso(),
                    "signal": signum,
                    "pid": child.pid,
                    "action": "terminate_child",
                },
            )
            child.terminate()

    signal.signal(signal.SIGTERM, _terminate_child)
    signal.signal(signal.SIGINT, _terminate_child)

    log_dir.mkdir(parents=True, exist_ok=True)
    start_ts = time.time()
    with (
        stdout_path.open("w", encoding="utf-8", buffering=1) as stdout_f,
        stderr_path.open("w", encoding="utf-8", buffering=1) as stderr_f,
    ):
        child = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=stdout_f,
            stderr=stderr_f,
            text=True,
        )
        print(f"Supervisor started child PID {child.pid}", flush=True)
        print(f"Heartbeat: {heartbeat_path}", flush=True)
        print(f"Stdout:    {stdout_path}", flush=True)
        print(f"Stderr:    {stderr_path}", flush=True)
        _append_jsonl(
            heartbeat_path,
            {
                "event": "start",
                "ts": _now_iso(),
                "pid": child.pid,
                "command": command,
                "cwd": str(PROJECT_ROOT),
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
            },
        )

        while True:
            returncode = child.poll()
            elapsed_sec = round(time.time() - start_ts, 1)
            memory = _windows_memory_info(child.pid)
            _append_jsonl(
                heartbeat_path,
                {
                    "event": "heartbeat" if returncode is None else "exit",
                    "ts": _now_iso(),
                    "pid": child.pid,
                    "returncode": returncode,
                    "elapsed_sec": elapsed_sec,
                    "stdout_bytes": _file_size(stdout_path),
                    "stderr_bytes": _file_size(stderr_path),
                    "memory": memory,
                },
            )
            if returncode is not None:
                break
            time.sleep(max(1.0, float(args.poll_sec)))

    final_payload = {
        "event": "exit_detail",
        "ts": _now_iso(),
        "pid": child.pid,
        "returncode": child.returncode,
        "elapsed_sec": round(time.time() - start_ts, 1),
        "stdout_tail": _tail_lines(stdout_path, int(args.tail_lines)),
        "stderr_tail": _tail_lines(stderr_path, int(args.tail_lines)),
    }
    _append_jsonl(heartbeat_path, final_payload)
    if child.returncode != 0:
        print(f"Child exited with return code {child.returncode}", flush=True)
        print(f"See heartbeat: {heartbeat_path}", flush=True)
        print(f"See stderr:    {stderr_path}", flush=True)
    raise SystemExit(child.returncode or 0)


if __name__ == "__main__":
    main()
