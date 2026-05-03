"""Sweep orchestration engine — runs multiple backtests and auto-compares.

Runs are executed **sequentially** (one subprocess at a time) because DuckDB
uses a single-writer model with an exclusive file lock.  Parallel backtest
processes would conflict on ``data/backtest.duckdb`` and ``data/market.duckdb``.
Each subprocess must complete and release its lock before the next one starts.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_STORE_TRUE_DESTS: frozenset[str] | None = None


def _get_store_true_dests() -> frozenset[str]:
    """Lazy cache of argparse store_true dest names."""
    global _STORE_TRUE_DESTS
    if _STORE_TRUE_DESTS is None:
        from engine.run_backtest import build_parser

        _STORE_TRUE_DESTS = frozenset(
            a.dest for a in build_parser()._actions if type(a).__name__ == "_StoreTrueAction"
        )
    return _STORE_TRUE_DESTS


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class SweepResult:
    """Result of a single sweep combination."""

    run_id: str
    label: str
    params_dict: dict
    exit_code: int = 0
    elapsed_sec: float = 0.0


def _build_label(combo: dict[str, Any]) -> str:
    """Build a human-readable label from a parameter combination."""
    if set(combo).issubset({"preset", "compound_equity"}) and "preset" in combo:
        preset = str(combo["preset"]).upper()
        label_map = {
            "CPR_LEVELS_STANDARD_LONG": "STD_LONG",
            "CPR_LEVELS_STANDARD_SHORT": "STD_SHORT",
            "CPR_LEVELS_RISK_LONG": "RISK_LONG",
            "CPR_LEVELS_RISK_SHORT": "RISK_SHORT",
        }
        base_label = label_map.get(preset)
        if base_label is not None:
            if bool(combo.get("compound_equity")):
                return f"{base_label}_CMP"
            return base_label

    parts = []
    for k, v in combo.items():
        short = k.replace("_", "-")
        parts.append(f"{short}={v}")
    return "-".join(parts)


def _build_manifest(name: str, results: list[SweepResult], *, dry_run: bool = False) -> dict:
    """Build a JSON manifest for a sweep run."""
    return {
        "sweep": name,
        "dry_run": dry_run,
        "total_combinations": len(results),
        "completed": 0 if dry_run else len(results),
        "results": [
            {
                "label": r.label,
                "run_id": r.run_id,
                "exit_code": r.exit_code,
                "elapsed_sec": r.elapsed_sec,
                "params": r.params_dict,
            }
            for r in results
        ],
    }


def _slugify_name(value: str, *, max_length: int = 80) -> str:
    """Return a filesystem-safe short name for logs and manifests."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._-")
    if not slug:
        return "run"
    return slug[:max_length]


def _save_manifest_incremental(
    path: Path, name: str, results: list[SweepResult], total: int
) -> None:
    """Write manifest to disk after each run completes for crash recovery."""
    manifest = {
        "sweep": name,
        "total_combinations": total,
        "completed": sum(1 for r in results if not r.run_id.startswith("(")),
        "results": [
            {
                "label": r.label,
                "run_id": r.run_id,
                "exit_code": r.exit_code,
                "elapsed_sec": r.elapsed_sec,
                "params": r.params_dict,
            }
            for r in results
        ],
    }
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def _load_manifest_for_resume(path: Path) -> dict | None:
    """Load a previously saved manifest for --resume."""
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError, OSError:
        return None


def _build_subprocess_args(
    config: Any,
    combo: dict[str, Any],
    *,
    progress_file: Path | None = None,
) -> list[str]:
    """Build the full CLI args for a single sweep combination subprocess.

    Auto-injects ``--save``, ``--yes-full-run``, and ``--quiet`` so operators
    never need to spell these control flags in the YAML config.
    """
    store_true = _get_store_true_dests()
    args: list[str] = [sys.executable, "-m", "engine.run_backtest"]
    args.append("--save")
    args.append("--yes-full-run")
    args.append("--quiet")
    if progress_file is not None:
        args.extend(["--progress-file", str(progress_file)])
    args.extend(["--strategy", config.strategy])
    # Base params — convert dest-style names (universe_name) to CLI flags (--universe-name)
    for key, value in config.base_params.items():
        flag = f"--{key.replace('_', '-')}"
        if key in store_true:
            if value:
                args.append(flag)
        else:
            args.extend([flag, str(value)])
    # Combination params
    for key, value in combo.items():
        flag = f"--{key.replace('_', '-')}"
        if key in store_true:
            if value:
                args.append(flag)
        else:
            args.extend([flag, str(value)])
    return args


def _run_subprocess_with_logs(
    cmd: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    cwd: str,
) -> subprocess.CompletedProcess[str]:
    """Run a child backtest with stdout/stderr streamed to log files."""
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_f,
        stderr_path.open("w", encoding="utf-8") as stderr_f,
    ):
        return subprocess.run(
            cmd,
            shell=False,
            cwd=cwd,
            stdout=stdout_f,
            stderr=stderr_f,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=3600,
        )


def _extract_run_id(
    stdout_path: Path,
    progress_file: Path,
    completed_stdout: str | None = None,
) -> str:
    """Recover the saved run_id from stdout or the structured progress stream."""
    if completed_stdout:
        for line in reversed(completed_stdout.splitlines()):
            if "run_id:" in line or "cached" in line:
                return line.split("run_id:", 1)[-1].strip().rstrip(".")

    for path in (stdout_path, progress_file):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in reversed(text.splitlines()):
            if "run_id:" in line:
                return line.split("run_id:", 1)[-1].strip().rstrip(".")
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if payload.get("event") in {"cli_run_complete", "cli_save_complete"}:
                run_id = str(payload.get("run_id") or "").strip()
                if run_id:
                    return run_id
    return "(failed)"


def run_sweep(
    config: Any,
    *,
    dry_run: bool = False,
    resume_from: Path | None = None,
) -> list[SweepResult]:
    """Execute all combinations in a sweep config.

    Each combination gets a unique run_id (append-only).

    Runs are strictly sequential — ``subprocess.run`` (synchronous) blocks
    until each backtest exits, which avoids DuckDB file-lock collisions on
    Windows.  Do NOT switch to parallel subprocess execution without a
    connection-pooling or file-lock-aware coordination layer.

    The manifest is saved incrementally after each run so that ``--resume``
    can skip already-completed combinations on re-run.

    Args:
        config: SweepConfig with base params and sweep axes.
        dry_run: Show combinations without executing.
        resume_from: Path to a previous sweep manifest to resume from.

    Returns:
        List of SweepResult with run_ids and labels.
    """
    combos = config.combinations()
    logger.info(
        "Sweep '%s': %d combinations for strategy '%s'",
        config.name,
        len(combos),
        config.strategy,
    )

    if dry_run:
        dry_run_results = []
        for combo in combos:
            label = _build_label(combo)
            logger.info("  [DRY-RUN] %s → %s", label, combo)
            dry_run_results.append(
                SweepResult(
                    run_id="(dry-run)",
                    label=label,
                    params_dict=config.build_params_for(combo),
                )
            )
        return dry_run_results

    # Resolve resume: load completed labels and skip them
    skip_labels: set[str] = set()
    prior_results: list[SweepResult] = []
    if resume_from:
        manifest = _load_manifest_for_resume(resume_from)
        if manifest:
            for entry in manifest.get("results", []):
                if entry.get("exit_code", -1) == 0:
                    skip_labels.add(entry["label"])
                    prior_results.append(
                        SweepResult(
                            run_id=entry["run_id"],
                            label=entry["label"],
                            params_dict=entry.get("params", {}),
                            exit_code=entry.get("exit_code", 0),
                            elapsed_sec=entry.get("elapsed_sec", 0.0),
                        )
                    )
            logger.info(
                "Resuming: %d/%d combinations already completed, skipping.",
                len(skip_labels),
                len(combos),
            )

    logger.info("Releasing parent DuckDB handle before spawning child processes")
    from db.duckdb import close_db

    close_db()

    results: list[SweepResult] = list(prior_results)
    project_root = PROJECT_ROOT
    total = len(combos)

    # Incremental manifest path
    sweep_dir = project_root / "sweeps"
    sweep_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    manifest_path = sweep_dir / f"{config.name}-{timestamp}.json"
    progress_dir = project_root / ".tmp_logs" / "sweeps" / f"{config.name}-{timestamp}"
    progress_dir.mkdir(parents=True, exist_ok=True)

    for idx, combo in enumerate(combos, 1):
        label = _build_label(combo)

        if label in skip_labels:
            logger.info("  %d/%d: %s — SKIPPED (resume)", idx, total, label)
            continue

        params_dict = config.build_params_for(combo)
        progress_file = progress_dir / f"{idx:02d}-{_slugify_name(label)}.jsonl"
        stdout_path = progress_dir / f"{idx:02d}-{_slugify_name(label)}.stdout.log"
        stderr_path = progress_dir / f"{idx:02d}-{_slugify_name(label)}.stderr.log"
        cmd = _build_subprocess_args(config, combo, progress_file=progress_file)
        logger.info("Running %d/%d: %s …", idx, total, label)
        logger.info("  progress: %s", progress_file)
        logger.info("  stdout:   %s", stdout_path)
        logger.info("  stderr:   %s", stderr_path)
        t0 = time.time()
        try:
            completed = _run_subprocess_with_logs(
                cmd,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                cwd=str(project_root),
            )
        except subprocess.TimeoutExpired:
            logger.error("  %s timed out after 3600s", label)
            results.append(
                SweepResult(
                    run_id="(timeout)",
                    label=label,
                    params_dict=params_dict,
                    exit_code=-1,
                    elapsed_sec=3600,
                )
            )
            _save_manifest_incremental(manifest_path, config.name, results, total)
            continue

        elapsed = round(time.time() - t0, 2)
        logger.info("  → exit=%d (%.1fs)", completed.returncode, elapsed)

        # Parse run_id from subprocess stdout
        run_id = _extract_run_id(stdout_path, progress_file, completed.stdout)

        results.append(
            SweepResult(
                run_id=run_id,
                label=label,
                params_dict=params_dict,
                exit_code=completed.returncode,
                elapsed_sec=elapsed,
            )
        )

        # Save manifest after each run for crash recovery
        _save_manifest_incremental(manifest_path, config.name, results, total)

        # Log stderr tail on failure
        if completed.returncode != 0:
            try:
                stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                stderr_text = ""
            tail = stderr_text.strip().splitlines()[-3:]
            for line in tail:
                logger.warning("  stderr: %s", line)

    _save_manifest_incremental(manifest_path, config.name, results, total)

    return results
