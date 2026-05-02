"""PID-based writer lock helpers for DuckDB files."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path


def is_pid_alive(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        process = kernel32.OpenProcess(0x1000, False, pid)
        if process:
            kernel32.CloseHandle(process)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def read_lock_payload(lock_path: Path) -> dict[str, object] | None:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"pid": int(raw)}
        except ValueError:
            return None
    return payload if isinstance(payload, dict) else None


def acquire_write_lock(lock_path: Path) -> None:
    """Acquire a PID-based write lock, failing fast if another writer is alive."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": os.getpid(),
        "acquired_at": time.time(),
        "lock_path": str(lock_path),
    }
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing = read_lock_payload(lock_path) or {}
            existing_pid = existing.get("pid")
            if isinstance(existing_pid, int) and is_pid_alive(existing_pid):
                kill_cmd = (
                    f"taskkill //F //PID {existing_pid}"
                    if os.name == "nt"
                    else f"kill {existing_pid}"
                )
                raise SystemExit(
                    f"Another DuckDB write process is running (PID {existing_pid}).\n"
                    f"Kill it:  {kill_cmd}\n"
                    "Only one write connection is allowed at a time."
                ) from None
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise SystemExit(
                    f"Failed to clear stale DuckDB write lock at {lock_path}: {exc}"
                ) from exc
            continue
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
                handle.flush()
            return
        except Exception:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise


def release_write_lock(lock_path: Path) -> None:
    """Release the write lock if it belongs to this process."""
    try:
        payload = read_lock_payload(lock_path) or {}
        if payload.get("pid") == os.getpid():
            lock_path.unlink(missing_ok=True)
    except OSError:
        pass
