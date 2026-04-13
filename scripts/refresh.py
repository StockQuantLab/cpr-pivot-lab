"""Single-command refresh orchestration for daily operator workflows."""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path

from db.duckdb import close_dashboard_db, get_dashboard_db
from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

_RUNTIME_TABLES = [
    "cpr_daily",
    "atr_intraday",
    "cpr_thresholds",
    "or_daily",
    "virgin_cpr_flags",
    "market_day_state",
    "strategy_day_state",
    "intraday_day_pack",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _detect_refresh_since() -> str | None:
    db = get_dashboard_db()
    max_dates = db.get_table_max_trade_dates(_RUNTIME_TABLES)
    dates = sorted(
        {
            dt.date.fromisoformat(value)
            for value in max_dates.values()
            if isinstance(value, str) and value
        }
    )
    if not dates:
        return None
    return (dates[-1] + dt.timedelta(days=1)).isoformat()


def _run(cmd: list[str], *, dry_run: bool, timeout: int = 3600) -> int:
    """Run a subprocess with the standard contract.

    Uses sys.executable, shell=False, cwd pinned to project root,
    capture_output=True, text=True, with configurable timeout.
    """
    pretty = " ".join(cmd)
    print(f"$ {pretty}")
    if dry_run:
        return 0
    close_dashboard_db()
    completed = subprocess.run(
        cmd,
        shell=False,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0 and completed.stderr:
        for line in completed.stderr.strip().splitlines()[-3:]:
            print(f"  stderr: {line}")
    return int(completed.returncode)


def _daily_prepare_cmd(*, trade_date: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.paper_trading",
        "daily-prepare",
        "--trade-date",
        trade_date,
        "--all-symbols",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh runtime tables and optional paper prep")
    parser.add_argument(
        "--since",
        default=None,
        help="Inclusive refresh start date (YYYY-MM-DD). Defaults to auto-detected next day.",
    )
    parser.add_argument(
        "--no-auto",
        action="store_true",
        help="Disable auto-detection when --since is omitted.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Batch size forwarded to pivot-build (default 64).",
    )
    parser.add_argument(
        "--prepare-paper",
        action="store_true",
        help="Check daily-prepare first; build only if needed, then validate the refreshed trade date.",
    )
    parser.add_argument(
        "--paper-sim",
        action="store_true",
        help="Check daily-prepare first; build only if needed, then run daily-sim for all 4 variants.",
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="Paper-trading date to prepare. Defaults to the resolved refresh date.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands without executing them.",
    )
    args = parser.parse_args()

    since = args.since
    if since is None and not args.no_auto:
        since = _detect_refresh_since()
    if since is None:
        parser.error("Unable to determine refresh start date. Pass --since explicitly.")

    build_cmd = [
        sys.executable,
        "-m",
        "scripts.build_tables",
        "--refresh-since",
        since,
        "--batch-size",
        str(args.batch_size),
    ]
    build_required = True
    prepare_cmd: list[str] | None = None
    if args.prepare_paper or args.paper_sim:
        trade_date = args.trade_date or since
        prepare_cmd = _daily_prepare_cmd(trade_date=trade_date)
        code = _run(prepare_cmd, dry_run=args.dry_run)
        if code == 0:
            build_required = False
            if not args.dry_run:
                print("daily-prepare reports runtime tables are current; skipping pivot-build.")
        elif not args.dry_run:
            print("daily-prepare reported incomplete coverage; pivot-build will run.")
        if code != 0 and args.dry_run:
            # Preserve the existing dry-run contract: only show the command sequence.
            build_required = True

    if build_required:
        code = _run(build_cmd, dry_run=args.dry_run)
        if code != 0:
            raise SystemExit(code)

        if prepare_cmd is not None:
            code = _run(prepare_cmd, dry_run=args.dry_run)
            if code != 0:
                raise SystemExit(code)

    if args.paper_sim:
        trade_date = args.trade_date or since
        sim_cmd = [
            sys.executable,
            "-m",
            "scripts.paper_trading",
            "daily-sim",
            "--trade-date",
            trade_date,
            "--all-symbols",
        ]
        code = _run(sim_cmd, dry_run=args.dry_run, timeout=600)
        if code != 0:
            raise SystemExit(code)


if __name__ in {"__main__", "__mp_main__"}:
    main()
