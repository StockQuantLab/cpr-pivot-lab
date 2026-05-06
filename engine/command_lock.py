"""Cross-process CLI lock helpers for runtime writers."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

if os.name == "nt":
    import msvcrt
else:  # pragma: no cover - exercised on non-Windows CI only.
    import fcntl

F = TypeVar("F", bound=Callable[..., Any])


def _parse_lock_pid(lock_path: Path) -> int | None:
    """Read the PID written by the current lock holder, if any."""
    try:
        for line in lock_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("pid="):
                return int(line.split("=", 1)[1])
    except OSError, ValueError:
        pass
    return None


def _lock_info_path(lock_path: Path) -> Path:
    """Return the sidecar metadata file for a lock file."""
    return lock_path.with_suffix(lock_path.suffix + ".info")


def _write_lock_info(lock_path: Path, *, pid: int, detail: str, started_at: str) -> None:
    """Persist holder metadata outside the locked file so contenders can inspect it."""
    info_path = _lock_info_path(lock_path)
    tmp_path = info_path.with_suffix(info_path.suffix + ".tmp")
    tmp_path.write_text(
        f"pid={pid}\ndetail={detail}\nstarted_at={started_at}\n",
        encoding="utf-8",
    )
    tmp_path.replace(info_path)


def _read_lock_info(lock_path: Path) -> tuple[int | None, str | None, str | None]:
    """Read sidecar metadata for a lock if available."""
    info_path = _lock_info_path(lock_path)
    pid: int | None = None
    detail: str | None = None
    started_at: str | None = None
    try:
        for line in info_path.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            if key == "pid" and value:
                pid = int(value)
            elif key == "detail" and value:
                detail = value
            elif key == "started_at" and value:
                started_at = value
    except OSError, ValueError:
        pass
    return pid, detail, started_at


def _is_pid_alive(pid: int) -> bool:
    """Return True if the process is still running."""
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


@contextmanager
def acquire_command_lock(name: str, *, detail: str) -> Iterator[None]:
    """Acquire an exclusive lock for a long-running CLI writer.

    The lock is backed by a file in `.tmp_logs` so concurrent commands fail fast
    instead of corrupting shared runtime state.

    Lock-before-write design: we acquire the byte-range lock first so that the file
    always contains the **holder's** PID when a competing process reads it on failure.
    The OS auto-releases the lock when the process exits, so stale locks are not a
    problem in practice.
    """

    lock_dir = Path(".tmp_logs")
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{name}.lock"
    started_at = datetime.now(UTC).isoformat()

    try:
        with open(lock_path, "a+", encoding="utf-8") as handle:
            handle.seek(0)

            # Step 1: try to acquire first. The lock file itself no longer stores
            # metadata, so contenders read the sidecar file instead.
            try:
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:  # pragma: no cover - exercised on non-Windows CI only.
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]
            except OSError:
                holder_pid, holder_detail, holder_started_at = _read_lock_info(lock_path)
                if holder_pid is None:
                    holder_pid = _parse_lock_pid(lock_path)
                if holder_pid and _is_pid_alive(holder_pid):
                    kill_cmd = (
                        f"taskkill //F //PID {holder_pid}"
                        if os.name == "nt"
                        else f"kill {holder_pid}"
                    )
                    extra = f" ({holder_detail})" if holder_detail else ""
                    started = f"\nstarted_at={holder_started_at}" if holder_started_at else ""
                    raise SystemExit(
                        f"{detail} is already running (PID {holder_pid}){extra}.{started}\n"
                        f"Kill it:  {kill_cmd}"
                    ) from None
                raise SystemExit(
                    f"{detail} lock is held but holder process not found.\n"
                    f"Delete {lock_path} to force-clear."
                ) from None

            # Step 2: acquired — write holder metadata to a sidecar file.
            _write_lock_info(
                lock_path,
                pid=os.getpid(),
                detail=detail,
                started_at=started_at,
            )
            handle.seek(0)

            try:
                yield
            finally:
                handle.seek(0)
                try:
                    if os.name == "nt":
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:  # pragma: no cover - exercised on non-Windows CI only.
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
                finally:
                    handle.flush()
                    try:
                        _lock_info_path(lock_path).unlink()
                    except FileNotFoundError:
                        pass
    except SystemExit:
        raise
    except OSError as exc:
        raise SystemExit(f"Failed to open lock file {lock_path}: {exc}") from exc


def command_lock(name: str, *, detail: str) -> Callable[[F], F]:
    """Decorator that protects a CLI entry point with an exclusive file lock."""

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any):
            with acquire_command_lock(name, detail=detail):
                return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
