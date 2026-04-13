"""Cleanup helper for intermediate caches/logs/progress artifacts.

Safe defaults clean only non-data transient artifacts. Data progress logs
can be included explicitly via --include-data-progress.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)


@dataclass(frozen=True)
class CleanupTarget:
    rel_path: str
    description: str


DEFAULT_TARGETS: tuple[CleanupTarget, ...] = (
    CleanupTarget(".mypy_cache", "Mypy type-check cache"),
    CleanupTarget(".pytest_cache", "Pytest cache"),
    CleanupTarget(".ruff_cache", "Ruff cache"),
    CleanupTarget("logs", "Local run logs"),
)

OPTIONAL_TARGETS: tuple[CleanupTarget, ...] = (
    CleanupTarget("data/progress", "NDJSON progress logs for long-running jobs"),
)


def _path_size_bytes(path: Path) -> int:
    """Return recursive file size for file/dir path."""
    if not path.exists():
        return 0
    if path.is_file():
        return int(path.stat().st_size)
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += int(child.stat().st_size)
    return total


def _delete_path(path: Path) -> None:
    """Delete file or directory path."""
    if not path.exists():
        return
    if path.is_file():
        path.unlink(missing_ok=True)
        return
    shutil.rmtree(path, ignore_errors=True)


def _format_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def _collect_targets(include_data_progress: bool) -> list[CleanupTarget]:
    targets = list(DEFAULT_TARGETS)
    if include_data_progress:
        targets.extend(OPTIONAL_TARGETS)
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean transient artifacts (caches/logs/progress files)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting anything.",
    )
    parser.add_argument(
        "--include-data-progress",
        action="store_true",
        help="Also delete data/progress NDJSON heartbeat files.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    targets = _collect_targets(include_data_progress=args.include_data_progress)

    rows: list[tuple[str, str, int, bool]] = []
    total_bytes = 0
    for target in targets:
        abs_path = repo_root / target.rel_path
        exists = abs_path.exists()
        size_bytes = _path_size_bytes(abs_path) if exists else 0
        total_bytes += size_bytes
        rows.append((target.rel_path, target.description, size_bytes, exists))

    print(f"{'Path':<24} {'Exists':<8} {'Size':>12} Description")
    print("-" * 90)
    for rel_path, description, size_bytes, exists in rows:
        print(
            f"{rel_path:<24} "
            f"{('yes' if exists else 'no'):<8} "
            f"{_format_mb(size_bytes):>12} "
            f"{description}"
        )
    print("-" * 90)
    print(f"Total candidate cleanup: {_format_mb(total_bytes)}")

    if args.dry_run:
        print("Dry run only. No files were deleted.")
        return

    for rel_path, _description, _size_bytes, exists in rows:
        if not exists:
            continue
        _delete_path(repo_root / rel_path)

    print(f"Cleanup complete. Freed approximately {_format_mb(total_bytes)}.")


if __name__ in {"__main__", "__mp_main__"}:
    main()
