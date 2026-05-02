"""Single-command refresh orchestration for daily operator workflows."""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

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

_EOD_STAGE_DESCRIPTIONS = {
    "refresh_instruments": "Refresh Kite instrument master",
    "ingest_daily": "Ingest daily candles for EOD date",
    "ingest_5min": "Ingest 5-minute candles for EOD date",
    "build_runtime": "Build runtime DuckDB tables through EOD date",
    "build_next_day_cpr": "Build next-day CPR rows via COALESCE(LEAD, trade_date)",
    "build_next_day_thresholds": "Build next-day cpr_thresholds (rolling P50 for narrowing filter)",
    "build_next_day_state": "Build next-day market_day_state (ATR via ASOF JOIN)",
    "build_next_day_strategy": "Build next-day strategy_day_state",
    "sync_replica": "Sync market_replica with new next-day rows and verify",
    "daily_prepare": "Create dated universe and validate live prerequisites",
    "data_quality": "Final trade-date readiness gate",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")


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


def _run(cmd: list[str], *, dry_run: bool, timeout: int | None = 3600) -> int:
    """Run a subprocess with the standard contract.

    Uses sys.executable, shell=False, cwd pinned to project root, and streams
    child stdout/stderr directly so redirected wrapper logs show live progress.
    """
    pretty = " ".join(cmd)
    print(f"$ {pretty}", flush=True)
    if dry_run:
        return 0
    close_dashboard_db()
    completed = subprocess.run(
        cmd,
        shell=False,
        cwd=str(PROJECT_ROOT),
        text=True,
        timeout=timeout,
    )
    return int(completed.returncode)


def _run_stage(
    *,
    index: int,
    total: int,
    name: str,
    cmd: list[str],
    dry_run: bool,
) -> None:
    description = _EOD_STAGE_DESCRIPTIONS.get(name, name)
    print(f"\n[{index}/{total}] START {name}: {description}", flush=True)
    started_at = dt.datetime.now(IST)
    code = _run(cmd, dry_run=dry_run, timeout=None)
    elapsed = (dt.datetime.now(IST) - started_at).total_seconds()
    if code != 0:
        print(
            f"[{index}/{total}] FAILED {name}: exit_code={code} elapsed={elapsed:.1f}s", flush=True
        )
        raise SystemExit(code)
    print(f"[{index}/{total}] DONE {name}: elapsed={elapsed:.1f}s", flush=True)


def _build_table_cmd(*, table: str, trade_date: str, batch_size: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.build_tables",
        "--table",
        table,
        "--refresh-date",
        trade_date,
        "--batch-size",
        str(batch_size),
    ]


def _sync_replica_cmd(*, trade_date: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.sync_replica",
        "--verify",
        "--trade-date",
        trade_date,
    ]


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


def _data_quality_cmd(*, trade_date: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.data_quality",
        "--date",
        trade_date,
    ]


def _kite_ingest_cmd(
    *,
    ingest_date: str,
    five_min: bool = False,
    skip_existing: bool = True,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "scripts.kite_ingest",
        "--from",
        ingest_date,
        "--to",
        ingest_date,
    ]
    if five_min:
        cmd.extend(["--5min", "--resume"])
    if skip_existing:
        cmd.append("--skip-existing")
    return cmd


def _refresh_instruments_cmd() -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.kite_ingest",
        "--refresh-instruments",
        "--exchange",
        "NSE",
    ]


def _build_cmd(*, since: str, batch_size: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.build_tables",
        "--refresh-since",
        since,
        "--batch-size",
        str(batch_size),
    ]


def _resolve_date_arg(value: str | None, *, default_today: bool = False) -> str | None:
    if value is None:
        if not default_today:
            return None
        return dt.datetime.now(IST).date().isoformat()
    normalized = str(value).strip().lower()
    today = dt.datetime.now(IST).date()
    if normalized == "today":
        return today.isoformat()
    if normalized == "tomorrow":
        return (today + dt.timedelta(days=1)).isoformat()
    return dt.date.fromisoformat(str(value)).isoformat()


def _run_eod_ingestion(
    *,
    ingest_date: str,
    trade_date: str,
    batch_size: int,
    dry_run: bool,
    skip_existing: bool = True,
    start_from_stage: str | None = None,
) -> None:
    """Run the complete EOD ingestion contract in the only valid order."""
    stages = [
        ("refresh_instruments", _refresh_instruments_cmd()),
        (
            "ingest_daily",
            _kite_ingest_cmd(
                ingest_date=ingest_date,
                five_min=False,
                skip_existing=skip_existing,
            ),
        ),
        (
            "ingest_5min",
            _kite_ingest_cmd(
                ingest_date=ingest_date,
                five_min=True,
                skip_existing=skip_existing,
            ),
        ),
        ("build_runtime", _build_cmd(since=ingest_date, batch_size=batch_size)),
        (
            "build_next_day_cpr",
            _build_table_cmd(table="cpr", trade_date=trade_date, batch_size=batch_size),
        ),
        (
            "build_next_day_thresholds",
            _build_table_cmd(table="thresholds", trade_date=trade_date, batch_size=batch_size),
        ),
        (
            "build_next_day_state",
            _build_table_cmd(table="state", trade_date=trade_date, batch_size=batch_size),
        ),
        (
            "build_next_day_strategy",
            _build_table_cmd(table="strategy", trade_date=trade_date, batch_size=batch_size),
        ),
        ("sync_replica", _sync_replica_cmd(trade_date=trade_date)),
        ("daily_prepare", _daily_prepare_cmd(trade_date=trade_date)),
        ("data_quality", _data_quality_cmd(trade_date=trade_date)),
    ]
    print(
        f"EOD pipeline: ingest_date={ingest_date} -> live_trade_date={trade_date} "
        f"(skip_existing_ingest={skip_existing})",
        flush=True,
    )
    if skip_existing:
        print(
            "EOD idempotency: daily/5-min ingestion stages pass --skip-existing; "
            "use --force-ingest to refetch already-covered symbols.",
            flush=True,
        )
    else:
        print(
            "EOD idempotency: --force-ingest enabled; ingestion will refetch symbols.", flush=True
        )
    print(
        "EOD date contract: --date is the completed market-data date; --trade-date is the "
        "next actual trading day. Pass it explicitly for weekends/holidays.",
        flush=True,
    )
    stage_names = [name for name, _cmd in stages]
    if start_from_stage:
        if start_from_stage not in stage_names:
            valid = ", ".join(stage_names)
            raise SystemExit(
                f"Unknown --start-from-stage {start_from_stage!r}; valid stages: {valid}"
            )
        start_index = stage_names.index(start_from_stage) + 1
        print(
            f"EOD resume: skipping stages before {start_from_stage!r} (stage {start_index}).",
            flush=True,
        )
    else:
        start_index = 1
    for index, (name, cmd) in enumerate(stages, start=1):
        if index < start_index:
            print(f"\n[{index}/{len(stages)}] SKIP {name}: {_EOD_STAGE_DESCRIPTIONS[name]}")
            continue
        _run_stage(
            index=index,
            total=len(stages),
            name=name,
            cmd=cmd,
            dry_run=dry_run,
        )
    print(
        f"\nEOD pipeline complete: ingest_date={ingest_date} prepared_trade_date={trade_date}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh runtime tables and optional paper prep")
    parser.add_argument(
        "--eod-ingest",
        action="store_true",
        help=(
            "Run the full EOD pipeline in order: refresh instruments, daily ingest, "
            "5-min ingest, build, daily-prepare, and final data-quality gate."
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        help="EOD ingestion date (YYYY-MM-DD, today). Required for --eod-ingest unless using today.",
    )
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
        default=128,
        help="Batch size forwarded to pivot-build (default 128).",
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
        "--force-ingest",
        action="store_true",
        help=(
            "With --eod-ingest, fetch daily and 5-min data even when local parquet already "
            "covers the ingestion date. Default is to pass --skip-existing."
        ),
    )
    parser.add_argument(
        "--start-from-stage",
        choices=tuple(_EOD_STAGE_DESCRIPTIONS),
        default=None,
        help="With --eod-ingest, skip earlier stages and resume from this stage name.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands without executing them.",
    )
    args = parser.parse_args()

    if args.eod_ingest:
        ingest_date = _resolve_date_arg(args.date, default_today=True)
        trade_date = _resolve_date_arg(args.trade_date)
        if trade_date is None:
            parser.error("--eod-ingest requires --trade-date <next_trading_date>")
        if trade_date <= ingest_date:
            parser.error("--trade-date must be after --date for --eod-ingest")
        _run_eod_ingestion(
            ingest_date=ingest_date,
            trade_date=trade_date,
            batch_size=args.batch_size,
            dry_run=bool(args.dry_run),
            skip_existing=not bool(args.force_ingest),
            start_from_stage=args.start_from_stage,
        )
        return

    since = args.since
    if since is None and not args.no_auto:
        since = _detect_refresh_since()
    if since is None:
        parser.error("Unable to determine refresh start date. Pass --since explicitly.")

    build_cmd = _build_cmd(since=since, batch_size=args.batch_size)
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
