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
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime
from datetime import time as dt_time
from pathlib import Path
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

from db.paper_db import get_paper_db
from scripts.paper_prepare import resolve_trade_date

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = PROJECT_ROOT / ".tmp_logs" / "supervisor"
WATCH_RELAUNCH_CUTOFF = dt_time(15, 0)
NTP_DELTA = 2_208_988_800
IST = ZoneInfo("Asia/Kolkata")
_ACTIVE_SESSION_STATUSES = ("ACTIVE", "PAUSED", "STOPPING")
_active_child: subprocess.Popen[str] | None = None
_child_heartbeat_path: Path | None = None
_run_loop_active = True


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _watch_relaunch_allowed(now: datetime | None = None) -> bool:
    current = now.astimezone(IST) if now is not None else datetime.now(IST)
    return current.timetz().replace(tzinfo=None) < WATCH_RELAUNCH_CUTOFF


def _measure_clock_drift_sec(*, host: str = "pool.ntp.org", timeout: float = 2.0) -> float:
    """Return local clock drift versus an SNTP server in seconds."""
    packet = b"\x1b" + (47 * b"\0")
    started = time.time()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(float(timeout))
        sock.sendto(packet, (host, 123))
        data, _addr = sock.recvfrom(48)
    received = time.time()
    if len(data) < 48:
        raise RuntimeError(f"short SNTP response from {host}: {len(data)} bytes")
    unpacked = struct.unpack("!12I", data[:48])
    transmit_sec = unpacked[10] - NTP_DELTA
    transmit_frac = unpacked[11] / 2**32
    remote_time = transmit_sec + transmit_frac
    local_midpoint = started + ((received - started) / 2.0)
    return float(local_midpoint - remote_time)


def _warn_if_clock_drift(
    *,
    warn_threshold_sec: float = 30.0,
    host: str = "pool.ntp.org",
    timeout: float = 2.0,
) -> float | None:
    """Warn when local time is far enough off to affect live bar boundaries."""
    try:
        drift = _measure_clock_drift_sec(host=host, timeout=timeout)
    except Exception as exc:
        print(f"[clock] SNTP drift check skipped: {exc}", flush=True)
        return None
    if abs(drift) > float(warn_threshold_sec):
        print(
            f"[clock] WARNING: local clock drift is {drift:+.1f}s vs {host}; "
            "live bar-boundary decisions may be wrong.",
            flush=True,
        )
    return drift


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str, ensure_ascii=True) + "\n")
        handle.flush()


def _on_supervisor_signal(signum: int, _frame: object) -> None:
    global _run_loop_active
    _run_loop_active = False
    child = _active_child
    heartbeat_path = _child_heartbeat_path
    if child is not None and child.poll() is None:
        if heartbeat_path is not None:
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


def _extract_trade_date(raw: list[str]) -> str:
    if not raw:
        return "today"
    for index, arg in enumerate(raw):
        if arg == "--trade-date":
            if index + 1 < len(raw):
                return raw[index + 1]
            return "today"
        if arg.startswith("--trade-date="):
            return arg.split("=", 1)[1]
    return "today"


def _build_live_command(raw: list[str]) -> list[str]:
    return [sys.executable, "-m", "scripts.paper_trading", "daily-live", *raw]


def _has_active_session_for_trade_date(*, trade_date: str, db=None) -> bool:
    db_obj = get_paper_db() if db is None else db
    placeholders = ",".join(["?"] * len(_ACTIVE_SESSION_STATUSES))
    row = db_obj.con.execute(
        f"SELECT 1 FROM paper_sessions WHERE trade_date = ? AND status IN ({placeholders}) LIMIT 1",
        [trade_date, *_ACTIVE_SESSION_STATUSES],
    ).fetchone()
    return bool(row)


def _run_child_once(
    command: list[str],
    log_dir: Path,
    run_name: str,
    *,
    poll_sec: float,
    tail_lines: int,
    env: dict[str, str],
) -> int:
    heartbeat_path = log_dir / f"{run_name}.heartbeat.jsonl"
    stdout_path = log_dir / f"{run_name}.stdout.log"
    stderr_path = log_dir / f"{run_name}.stderr.log"
    global _active_child, _child_heartbeat_path
    _child_heartbeat_path = heartbeat_path

    with (
        stdout_path.open("w", encoding="utf-8", buffering=1) as stdout_f,
        stderr_path.open("w", encoding="utf-8", buffering=1) as stderr_f,
    ):
        print(f"Supervisor started child command: {' '.join(command)}", flush=True)
        print(f"Heartbeat: {heartbeat_path}", flush=True)
        print(f"Stdout:    {stdout_path}", flush=True)
        print(f"Stderr:    {stderr_f.name}", flush=True)
        _active_child = child = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=stdout_f,
            stderr=stderr_f,
            text=True,
        )
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

        start_ts = time.time()
        while True:
            returncode = child.poll()
            elapsed_sec = round(time.time() - start_ts, 1)
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
                    "memory": _windows_memory_info(child.pid),
                },
            )
            if returncode is not None:
                break
            time.sleep(max(1.0, float(poll_sec)))

    final_payload = {
        "event": "exit_detail",
        "ts": _now_iso(),
        "pid": child.pid,
        "returncode": child.returncode,
        "elapsed_sec": round(time.time() - start_ts, 1),
        "stdout_tail": _tail_lines(stdout_path, tail_lines),
        "stderr_tail": _tail_lines(stderr_path, tail_lines),
    }
    _append_jsonl(heartbeat_path, final_payload)
    _active_child = None
    _child_heartbeat_path = None
    if child.returncode != 0:
        print(f"Child exited with return code {child.returncode}", flush=True)
        print(f"See heartbeat: {heartbeat_path}", flush=True)
        print(f"See stderr:    {stderr_path}", flush=True)
    return int(child.returncode or 0)


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
        "--skip-clock-check",
        action="store_true",
        help="Skip the SNTP clock-drift preflight warning.",
    )
    parser.add_argument(
        "--clock-drift-warn-sec",
        type=float,
        default=30.0,
        help="Warn when SNTP drift exceeds this many seconds (default 30).",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help=(
            "Keep this supervisor alive and launch daily-live when no active session "
            "exists for the target trade date."
        ),
    )
    parser.add_argument(
        "--watch-poll-sec",
        type=float,
        default=300.0,
        help="Watch-loop interval in seconds when checking for active sessions.",
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
    global _child_heartbeat_path, _run_loop_active
    args = _build_parser().parse_args()
    live_args = _normalize_live_args(args.live_args)
    log_dir = Path(args.log_dir)
    command = _build_live_command(live_args)
    run_name = args.name or f"live_{_stamp()}"

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    if not args.no_faulthandler:
        env.setdefault("PYTHONFAULTHANDLER", "1")

    log_dir.mkdir(parents=True, exist_ok=True)
    trade_date = resolve_trade_date(_extract_trade_date(live_args))
    if not args.skip_clock_check:
        _warn_if_clock_drift(warn_threshold_sec=float(args.clock_drift_warn_sec))
    heartbeat_path = log_dir / f"{run_name}.heartbeat.jsonl"
    _child_heartbeat_path = heartbeat_path
    _run_loop_active = True

    for signame in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, signame):
            signal.signal(getattr(signal, signame), _on_supervisor_signal)

    if args.watch:
        print(
            f"Supervisor watch-mode enabled for trade_date={trade_date}; poll interval="
            f"{args.watch_poll_sec}s",
            flush=True,
        )

        heartbeat_tick = args.watch_poll_sec
        current_run = 0
        while _run_loop_active:
            relaunch_allowed = _watch_relaunch_allowed()
            active = _has_active_session_for_trade_date(trade_date=trade_date)
            _append_jsonl(
                heartbeat_path,
                {
                    "event": "watch_tick",
                    "ts": _now_iso(),
                    "trade_date": trade_date,
                    "active_session_exists": bool(active),
                    "relaunch_allowed": bool(relaunch_allowed),
                    "iteration": current_run,
                },
            )
            if not relaunch_allowed:
                print(
                    "[watch] relaunch cutoff reached; supervisor will not start another child",
                    flush=True,
                )
                break
            if active:
                time.sleep(max(5.0, float(args.watch_poll_sec)))
                current_run += 1
                continue

            print(
                f"[watch] no active session found for {trade_date}, starting child #{current_run + 1}",
                flush=True,
            )
            current_run += 1
            instance_name = f"{run_name}_{current_run}"
            return_code = _run_child_once(
                command,
                log_dir=log_dir,
                run_name=instance_name,
                poll_sec=args.poll_sec,
                tail_lines=int(args.tail_lines),
                env=env,
            )
            _append_jsonl(
                heartbeat_path,
                {
                    "event": "watch_child_exit",
                    "ts": _now_iso(),
                    "trade_date": trade_date,
                    "returncode": return_code,
                    "iteration": current_run,
                },
            )
            if return_code != 0:
                print(
                    f"[watch] child exited with {return_code}; will retry after {heartbeat_tick}s",
                    flush=True,
                )
            time.sleep(max(5.0, float(heartbeat_tick)))
        _append_jsonl(
            heartbeat_path,
            {
                "event": "watch_exit",
                "ts": _now_iso(),
                "trade_date": trade_date,
                "iteration": current_run,
            },
        )
        print(f"Supervisor stopped for trade_date={trade_date}", flush=True)
    else:
        return_code = _run_child_once(
            command,
            log_dir=log_dir,
            run_name=run_name,
            poll_sec=args.poll_sec,
            tail_lines=int(args.tail_lines),
            env=env,
        )
        raise SystemExit(return_code)


if __name__ == "__main__":
    main()
