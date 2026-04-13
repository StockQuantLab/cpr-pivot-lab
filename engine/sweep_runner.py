"""Sweep orchestration engine — runs multiple backtests and auto-compares."""

from __future__ import annotations

import logging
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


def _build_subprocess_args(
    config: Any,
    combo: dict[str, Any],
) -> list[str]:
    """Build the full CLI args for a single sweep combination subprocess."""
    store_true = _get_store_true_dests()
    args: list[str] = [sys.executable, "-m", "engine.run_backtest"]
    args.append("--save")
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


def run_sweep(
    config: Any,
    *,
    dry_run: bool = False,
) -> list[SweepResult]:
    """Execute all combinations in a sweep config.

    Each combination gets a unique run_id (append-only).

    Args:
        config: SweepConfig with base params and sweep axes.
        dry_run: Show combinations without executing.

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

    logger.info("Releasing parent DuckDB handle before spawning child processes")
    from db.duckdb import close_db

    close_db()

    results: list[SweepResult] = []
    project_root = PROJECT_ROOT

    for combo in combos:
        label = _build_label(combo)
        params_dict = config.build_params_for(combo)
        cmd = _build_subprocess_args(config, combo)
        logger.info("Running %s …", label)
        t0 = time.time()
        try:
            completed = subprocess.run(
                cmd,
                shell=False,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                timeout=3600,
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
            continue

        elapsed = round(time.time() - t0, 2)
        logger.info("  → exit=%d (%.1fs)", completed.returncode, elapsed)

        # Parse run_id from subprocess stdout
        run_id = "(failed)"
        for line in (completed.stdout or "").splitlines():
            if "run_id:" in line or "cached" in line:
                run_id = line.split("run_id:", 1)[-1].strip().rstrip(".")
                break

        results.append(
            SweepResult(
                run_id=run_id,
                label=label,
                params_dict=params_dict,
                exit_code=completed.returncode,
                elapsed_sec=elapsed,
            )
        )

        # Log stderr tail on failure
        if completed.returncode != 0 and completed.stderr:
            tail = completed.stderr.strip().splitlines()[-3:]
            for line in tail:
                logger.warning("  stderr: %s", line)

    return results
