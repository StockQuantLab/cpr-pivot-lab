"""CLI entrypoint for pivot-sweep — YAML-driven backtest parameter sweeps."""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pivot-sweep",
        description="Run parameter sweeps defined in YAML and auto-compare results.",
    )
    parser.add_argument("config", type=str, help="Path to sweep YAML config")
    parser.add_argument("--dry-run", action="store_true", help="Show combinations without running")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    return parser


def main() -> None:
    from engine.cli_setup import configure_windows_stdio

    configure_windows_stdio(line_buffering=True, write_through=True)

    parser = build_parser()
    args = parser.parse_args()

    from engine.sweep_compare import fetch_summaries, format_comparison_table, rank_sweeps
    from engine.sweep_runner import _build_manifest, run_sweep
    from engine.sweep_schema import load_sweep_config

    config = load_sweep_config(Path(args.config))

    logger.info(
        "Sweep: %s (%d combinations, strategy=%s)",
        config.name,
        len(config.combinations()),
        config.strategy,
    )

    results = run_sweep(config, dry_run=args.dry_run)

    # Fetch metrics from DuckDB for non-dry-run results
    ranked = []
    if not args.dry_run:
        valid_run_ids = [r.run_id for r in results if not r.run_id.startswith("(")]
        if valid_run_ids:
            from db.duckdb import get_db

            db = get_db()
            summaries = fetch_summaries(db, valid_run_ids)
            ranked = rank_sweeps(
                summaries,
                metric=config.compare.metric,
                sort=config.compare.sort,
                top_n=config.compare.top_n,
            )

    if args.json:
        output = {
            "sweep": config.name,
            "total_combinations": len(config.combinations()),
            "results": [
                {
                    "label": r.label,
                    "run_id": r.run_id,
                    "exit_code": r.exit_code,
                    "elapsed_sec": r.elapsed_sec,
                }
                for r in results
            ],
            "ranked": [
                {
                    "run_id": s.run_id,
                    "calmar": s.calmar,
                    "win_rate": s.win_rate,
                    "total_pnl": s.total_pnl,
                    "trade_count": s.trade_count,
                }
                for s in ranked
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"\n{'=' * 60}")
        print(f"Sweep: {config.name}")
        print(f"{'=' * 60}")
        print(f"Total combinations: {len(config.combinations())}")
        print(f"Completed: {len(results)}")
        print()
        for r in results:
            status = "OK" if r.exit_code == 0 else f"FAIL({r.exit_code})"
            print(f"  {r.label:40s} → {r.run_id} [{status}] ({r.elapsed_sec:.1f}s)")
        if ranked:
            print(f"\n{'=' * 60}")
            print(f"Ranked Results (top {config.compare.top_n}, by {config.compare.metric}):")
            print(f"{'=' * 60}")
            print(format_comparison_table(ranked))

    # Write JSON manifest
    from engine.sweep_runner import PROJECT_ROOT

    sweep_dir = PROJECT_ROOT / "sweeps"
    sweep_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    manifest_path = sweep_dir / f"{config.name}-{timestamp}.json"
    manifest = _build_manifest(config.name, results, dry_run=args.dry_run)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest: {manifest_path}")


if __name__ == "__main__":
    main()
