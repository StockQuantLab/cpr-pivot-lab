"""Standardized execution workflow for long backtest campaigns.

This CLI codifies the operational run policy:
- chunked monthly execution with resume + save
- per-strategy progress files
- aggregate-only persisted runs (chunk checkpoints pruned by default)
- optional cleanup before/after campaign
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from db.duckdb import close_db, get_db
from engine.cli_setup import configure_windows_stdio
from engine.command_lock import acquire_command_lock


@dataclass(frozen=True)
class CampaignRun:
    strategy: str
    label: str
    extra_args: tuple[str, ...]


DEFAULT_RUN_ORDER: tuple[CampaignRun, ...] = (
    CampaignRun(
        strategy="FBR",
        label="fbr_fw10",
        extra_args=("--failure-window", "10", "--skip-rvol"),
    ),
    CampaignRun(
        strategy="CPR_LEVELS",
        label="cpr_quality_combo",
        extra_args=(
            "--cpr-min-close-atr",
            "0.5",
            "--min-price",
            "50",
            "--narrowing-filter",
            "--skip-rvol",
        ),
    ),
)


def _build_common_args(args: argparse.Namespace) -> list[str]:
    common = [
        "--start",
        args.start,
        "--end",
        args.end,
        "--chunk-by",
        "month",
        "--resume",
        "--save",
        "--quiet",
        "--runtime-batch-size",
        str(args.runtime_batch_size),
    ]
    if args.full_universe:
        common.extend(["--all", "--universe-size", "0"])
    else:
        common.extend(["--universe-name", args.universe_name])
    return common


def _command_for_run(
    run: CampaignRun, *, common_args: list[str], progress_dir: Path, with_progress: bool
) -> list[str]:
    cmd = [sys.executable, "-m", "engine.run_backtest", "--strategy", run.strategy, *common_args]
    cmd.extend(list(run.extra_args))
    if with_progress:
        progress_file = progress_dir / f"{run.label}_{common_args[1]}_{common_args[3]}.ndjson"
        cmd.extend(["--progress-file", str(progress_file)])
    return cmd


def _run_subprocess(cmd: list[str], *, dry_run: bool) -> int:
    pretty = " ".join(cmd)
    print(f"$ {pretty}")
    if dry_run:
        return 0
    # Release any parent-process DuckDB handle before spawning a child writer.
    close_db()
    completed = subprocess.run(cmd, check=False)
    return int(completed.returncode)


def _run_cleanup(*, include_data_progress: bool, dry_run: bool) -> int:
    cmd = [sys.executable, "-m", "scripts.clean_artifacts"]
    if include_data_progress:
        cmd.append("--include-data-progress")
    if dry_run:
        cmd.append("--dry-run")
    return _run_subprocess(cmd, dry_run=dry_run)


def _preview(items: list[str], n: int = 10) -> str:
    """Compact preview for long symbol lists."""
    if not items:
        return ""
    if len(items) <= n:
        return ", ".join(items)
    return f"{', '.join(items[:n])}, ..."


def _resolve_campaign_symbols(args: argparse.Namespace) -> list[str]:
    """Resolve symbol universe exactly as campaign intends to run."""
    db = get_db()
    if args.full_universe:
        # Match pivot-backtest --all behavior: refresh quality issues and use available symbols.
        db.refresh_data_quality_issues()
        db._publish_replica(force=False)
        return db.get_available_symbols(force_refresh=True)
    return db.get_universe_symbols(args.universe_name)


def _ensure_runtime_coverage(
    *,
    symbols: list[str],
    dry_run: bool,
    auto_fix: bool,
    pack_batch_size: int,
) -> None:
    """Validate and optionally auto-fix runtime symbol coverage before campaign."""
    if not symbols:
        raise SystemExit("No symbols resolved for campaign universe.")

    db = get_db()
    coverage = db.get_missing_runtime_symbol_coverage(symbols)
    missing_state = sorted(coverage.get("market_day_state", []))
    missing_strategy = sorted(coverage.get("strategy_day_state", []))
    missing_pack = sorted(coverage.get("intraday_day_pack", []))
    has_gaps = bool(missing_state or missing_strategy or missing_pack)

    print(
        "Runtime coverage preflight: "
        f"market_day_state={len(symbols) - len(missing_state)}/{len(symbols)}, "
        f"strategy_day_state={len(symbols) - len(missing_strategy)}/{len(symbols)}, "
        f"intraday_day_pack={len(symbols) - len(missing_pack)}/{len(symbols)}"
    )
    if not has_gaps:
        return

    if missing_state:
        print(f"  Missing market_day_state ({len(missing_state)}): {_preview(missing_state)}")
    if missing_strategy:
        print(
            f"  Missing strategy_day_state ({len(missing_strategy)}): {_preview(missing_strategy)}"
        )
    if missing_pack:
        print(f"  Missing intraday_day_pack ({len(missing_pack)}): {_preview(missing_pack)}")

    if not auto_fix:
        raise SystemExit(
            "Runtime coverage is incomplete. Re-run with --ensure-runtime-coverage "
            "or materialize tables via: doppler run -- uv run pivot-build --table pack --refresh-since <YYYY-MM-DD>"
        )

    state_targets = missing_state
    strategy_targets = sorted(set(missing_strategy + state_targets))
    pack_targets = sorted(set(missing_pack + strategy_targets))

    print("Auto-fix enabled: rebuilding missing runtime coverage before campaign.")
    if dry_run:
        print(
            "  [dry-run] would rebuild: "
            f"state={len(state_targets)} strategy={len(strategy_targets)} "
            f"pack={len(pack_targets)} (pack_batch_size={pack_batch_size})"
        )
        return

    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        if state_targets:
            db.build_market_day_state(force=True, symbols=state_targets)
        if strategy_targets:
            db.build_strategy_day_state(force=True, symbols=strategy_targets)
        if pack_targets:
            db.build_intraday_day_pack(
                force=True,
                symbols=pack_targets,
                batch_size=pack_batch_size,
            )

    post = db.get_missing_runtime_symbol_coverage(symbols)
    post_state = post.get("market_day_state", [])
    post_strategy = post.get("strategy_day_state", [])
    post_pack = post.get("intraday_day_pack", [])
    if post_state or post_strategy or post_pack:
        raise SystemExit(
            "Runtime coverage still incomplete after auto-fix: "
            f"state={len(post_state)} strategy={len(post_strategy)} pack={len(post_pack)}"
        )
    print("Runtime coverage auto-fix complete.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Run standardized long-window campaign in fixed order: FBR -> CPR_LEVELS.")
    )
    parser.add_argument("--start", required=True, help="Campaign start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Campaign end date (YYYY-MM-DD)")
    parser.add_argument(
        "--full-universe",
        action="store_true",
        help="Run on full available universe (--all --universe-size 0).",
    )
    parser.add_argument(
        "--universe-name",
        default="gold_51",
        help="Universe name when not using --full-universe (default gold_51).",
    )
    parser.add_argument(
        "--runtime-batch-size",
        type=int,
        default=32,
        help="Runtime symbol batch size for pivot-backtest (default 32).",
    )
    parser.add_argument(
        "--ensure-runtime-coverage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Preflight runtime coverage and auto-remediate missing state/pack rows before campaign "
            "(default true). Use --no-ensure-runtime-coverage to skip."
        ),
    )
    parser.add_argument(
        "--pack-batch-size",
        type=int,
        default=64,
        help="Batch size when auto-remediating intraday_day_pack coverage (default 64).",
    )
    parser.add_argument(
        "--progress-dir",
        default="data/progress",
        help="Directory for per-strategy NDJSON progress files (default data/progress).",
    )
    parser.add_argument(
        "--no-progress-file",
        action="store_true",
        help="Do not emit per-strategy --progress-file outputs.",
    )
    parser.add_argument(
        "--clean-before",
        action="store_true",
        help="Run pivot-clean before campaign (non-data targets only).",
    )
    parser.add_argument(
        "--clean-after",
        action="store_true",
        help="Run pivot-clean after campaign (non-data targets only).",
    )
    parser.add_argument(
        "--clean-progress-after",
        action="store_true",
        help="When used with --clean-after, also delete data/progress files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands only; do not execute.",
    )
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.runtime_batch_size < 1:
        raise SystemExit("--runtime-batch-size must be >= 1")
    if args.pack_batch_size < 1:
        raise SystemExit("--pack-batch-size must be >= 1")
    if not args.full_universe and not args.universe_name.strip():
        raise SystemExit("--universe-name cannot be empty when --full-universe is not set")


def _selected_runs(args: argparse.Namespace) -> list[CampaignRun]:
    return list(DEFAULT_RUN_ORDER)


def _resolved_symbols(args: argparse.Namespace) -> list[str]:
    if args.ensure_runtime_coverage and not args.dry_run:
        return _resolve_campaign_symbols(args)
    return []


def _print_campaign_policy(
    *,
    args: argparse.Namespace,
    symbols: list[str],
    selected_runs: list[CampaignRun],
) -> None:
    print("Campaign policy:")
    print("  - Chunking: month")
    print("  - Resume: enabled")
    print("  - Save: enabled")
    print("  - Keep chunk checkpoints: disabled (aggregate run only)")
    print(
        "  - Universe: "
        + ("full-universe (--all --universe-size 0)" if args.full_universe else args.universe_name)
    )
    if symbols:
        print(f"  - Symbols resolved: {len(symbols)}")
    elif args.dry_run:
        print("  - Symbols resolved: (skipped in dry-run)")
    else:
        print("  - Symbols resolved: (not required)")
    print("  - Strategy order: " + " -> ".join(run.strategy for run in selected_runs))


def _run_preflight(args: argparse.Namespace, symbols: list[str]) -> None:
    if not args.ensure_runtime_coverage:
        print("  - Runtime coverage preflight: skipped (--no-ensure-runtime-coverage)")
        return

    print("  - Runtime coverage preflight: enabled")
    if args.dry_run:
        print("    [dry-run] preflight check skipped (no DB read/write in dry-run mode)")
        return

    _ensure_runtime_coverage(
        symbols=symbols,
        dry_run=args.dry_run,
        auto_fix=True,
        pack_batch_size=args.pack_batch_size,
    )


def _run_campaign_steps(
    *,
    args: argparse.Namespace,
    selected_runs: list[CampaignRun],
    common_args: list[str],
    progress_dir: Path,
) -> None:
    if args.clean_before:
        total_steps = len(selected_runs) + int(args.clean_after) + 1
        print(f"\n[1/{total_steps}] Cleanup before campaign")
        code = _run_cleanup(include_data_progress=False, dry_run=args.dry_run)
        if code != 0:
            raise SystemExit(code)

    for idx, run in enumerate(selected_runs, start=1):
        print(f"\n[{idx}/{len(selected_runs)}] Running {run.strategy}")
        cmd = _command_for_run(
            run,
            common_args=common_args,
            progress_dir=progress_dir,
            with_progress=not args.no_progress_file,
        )
        code = _run_subprocess(cmd, dry_run=args.dry_run)
        if code != 0:
            raise SystemExit(code)

    if args.clean_after:
        print("\n[final] Cleanup after campaign")
        code = _run_cleanup(
            include_data_progress=args.clean_progress_after,
            dry_run=args.dry_run,
        )
        if code != 0:
            raise SystemExit(code)


def main() -> None:
    configure_windows_stdio(line_buffering=True, write_through=True)
    parser = build_parser()
    args = parser.parse_args()

    _validate_args(args)

    common_args = _build_common_args(args)
    progress_dir = Path(args.progress_dir)
    symbols = _resolved_symbols(args)
    selected_runs = _selected_runs(args)

    _print_campaign_policy(args=args, symbols=symbols, selected_runs=selected_runs)
    _run_preflight(args, symbols)
    _run_campaign_steps(
        args=args,
        selected_runs=selected_runs,
        common_args=common_args,
        progress_dir=progress_dir,
    )

    print("\nCampaign complete.")


if __name__ in {"__main__", "__mp_main__"}:
    main()
