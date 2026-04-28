"""Dedicated CLI for running the 8 CPR baseline backtests.

Daily operational command that replaces manual sweep YAML for the baseline
refresh ritual.  Pre-flights universe checks, runs all 8 variants sequentially,
auto-compares against previous baselines, and handles cleanup/retry.

Usage:
    pivot-baselines --end 2026-04-23
    pivot-baselines --end 2026-04-23 --notify
    pivot-baselines --end 2026-04-23 --resume
    pivot-baselines --end 2026-04-23 --clean-last
"""

from __future__ import annotations

import argparse
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

# The 8 canonical baseline variants — fixed, not configurable.
BASELINE_VARIANTS: list[dict[str, Any]] = [
    {"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": False, "label": "STD_LONG"},
    {"preset": "CPR_LEVELS_STANDARD_SHORT", "compound_equity": False, "label": "STD_SHORT"},
    {"preset": "CPR_LEVELS_RISK_LONG", "compound_equity": False, "label": "RISK_LONG"},
    {"preset": "CPR_LEVELS_RISK_SHORT", "compound_equity": False, "label": "RISK_SHORT"},
    {"preset": "CPR_LEVELS_STANDARD_LONG", "compound_equity": True, "label": "STD_LONG_CMP"},
    {"preset": "CPR_LEVELS_STANDARD_SHORT", "compound_equity": True, "label": "STD_SHORT_CMP"},
    {"preset": "CPR_LEVELS_RISK_LONG", "compound_equity": True, "label": "RISK_LONG_CMP"},
    {"preset": "CPR_LEVELS_RISK_SHORT", "compound_equity": True, "label": "RISK_SHORT_CMP"},
]

STORE_TRUE_DESTS: frozenset[str] | None = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASELINE_DIR = PROJECT_ROOT / "sweeps" / ".baselines"


def _get_store_true_dests() -> frozenset[str]:
    global STORE_TRUE_DESTS
    if STORE_TRUE_DESTS is None:
        from engine.run_backtest import build_parser

        STORE_TRUE_DESTS = frozenset(
            a.dest for a in build_parser()._actions if type(a).__name__ == "_StoreTrueAction"
        )
    return STORE_TRUE_DESTS


def _slugify_name(value: str, *, max_length: int = 80) -> str:
    """Return a filesystem-safe short name for logs and manifests."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._-")
    if not slug:
        return "run"
    return slug[:max_length]


@dataclass
class BaselineResult:
    label: str
    run_id: str
    exit_code: int
    elapsed_sec: float
    params_dict: dict


def _find_previous_baselines(end_date: str) -> dict[str, str]:
    """Find the most recent baseline run_id for each variant from run_metadata.

    Matches by parameter signature (direction + risk_sizing + compound) since
    older baselines may not have a ``preset`` field in their params_json.
    """
    from db.backtest_db import close_backtest_db, get_backtest_db

    db = get_backtest_db()
    rows = db.con.execute(
        """
        SELECT run_id,
               COALESCE(TRY_CAST(json_extract(params_json, '$.compound_equity') AS BOOLEAN), FALSE) as compound,
               COALESCE(
                   TRY_CAST(json_extract(params_json, '$.risk_based_sizing') AS BOOLEAN),
                   TRY_CAST(json_extract(params_json, '$.legacy_sizing') AS BOOLEAN),
                   FALSE
               ) as risk_sizing,
               UPPER(COALESCE(NULLIF(json_extract_string(params_json, '$.direction_filter'), ''), 'BOTH')) as direction,
               end_date
        FROM run_metadata
        WHERE strategy = 'CPR_LEVELS'
          AND execution_mode = 'BACKTEST'
          AND COALESCE(TRY_CAST(json_extract(params_json, '$.min_price') AS DOUBLE), 0.0) = 50.0
          AND end_date <= $end
        ORDER BY created_at DESC
        """,
        {"end": end_date},
    ).pl()

    result: dict[str, str] = {}
    for row in rows.to_dicts():
        direction = row.get("direction", "")
        compound = str(row.get("compound", "false")).lower() == "true"
        risk = str(row.get("risk_sizing", "false")).lower() == "true"
        run_id = row["run_id"]

        sizing = "RISK" if risk else "STD"
        if direction == "LONG":
            label = f"{sizing}_LONG"
        elif direction == "SHORT":
            label = f"{sizing}_SHORT"
        else:
            continue

        if compound:
            label += "_CMP"

        if label not in result:
            result[label] = run_id

    close_backtest_db()
    return result


def _preflight_universe_check(start: str, end: str) -> int:
    """Check how many symbols the selected universe would yield. Returns symbol count."""
    from db.duckdb import get_db

    db = get_db()
    universe_name = f"full_{end.replace('-', '_')}"
    row = db.con.execute(
        "SELECT symbol_count FROM backtest_universe WHERE universe_name = ? LIMIT 1",
        [universe_name],
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0])

    cnt_df: Any = db.con.execute(
        """
        SELECT COUNT(DISTINCT symbol) as cnt
        FROM market_day_state
        WHERE trade_date BETWEEN $start AND $end
        """,
        {"start": start, "end": end},
    ).pl()
    if cnt_df.height == 0:
        return 0
    return cnt_df.item(0, 0)


def _build_backtest_args(
    start: str,
    end: str,
    variant: dict[str, Any],
    *,
    progress_file: Path | None = None,
) -> list[str]:
    """Build CLI args for a single baseline backtest run."""
    args = [sys.executable, "-m", "engine.run_backtest"]
    args.extend(["--save", "--yes-full-run", "--quiet"])
    if progress_file is not None:
        args.extend(["--progress-file", str(progress_file)])
    args.extend(["--strategy", "CPR_LEVELS"])
    args.extend(["--start", start, "--end", end])
    universe_name = f"full_{end.replace('-', '_')}"
    from db.duckdb import close_db, get_db

    db = get_db()
    try:
        has_saved_universe = bool(
            db.con.execute(
                "SELECT 1 FROM backtest_universe WHERE universe_name = ? LIMIT 1",
                [universe_name],
            ).fetchone()
        )
    finally:
        # The child pivot-backtest process needs the exclusive market DB writer
        # lock. Do not keep the parent pre-flight connection open across spawn.
        close_db()
    if has_saved_universe:
        args.extend(["--universe-name", universe_name])
    else:
        args.extend(["--all", "--universe-size", "0"])
    args.extend(["--preset", variant["preset"]])
    if variant["compound_equity"]:
        args.append("--compound-equity")
    return args


def _run_backtest_with_logs(
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
            if "run_id:" in line:
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


def _save_progress(path: Path, results: list[BaselineResult], total: int) -> None:
    """Write progress manifest after each run."""
    manifest = {
        "completed": len(results),
        "total": total,
        "results": [
            {
                "label": r.label,
                "run_id": r.run_id,
                "exit_code": r.exit_code,
                "elapsed_sec": r.elapsed_sec,
            }
            for r in results
        ],
    }
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def _load_progress(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _cleanup_runs(run_ids: list[str]) -> None:
    """Delete specified runs from backtest DB and sync replica."""
    from db.backtest_db import close_backtest_db, get_backtest_db

    db = get_backtest_db()
    ids_sql = ", ".join(f"'{rid}'" for rid in run_ids)
    for table in ("backtest_results", "run_metrics", "run_daily_pnl", "run_metadata"):
        db.con.execute(f"DELETE FROM {table} WHERE run_id IN ({ids_sql})")
    sync = db._sync
    assert sync is not None
    sync.mark_dirty()
    sync.force_sync(db.con)
    close_backtest_db()
    logger.info("Cleaned up %d runs: %s", len(run_ids), run_ids)


def _format_money(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"Rs {float(value):,.0f}"
    except (TypeError, ValueError):
        return "n/a"


def _format_delta_money(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"Rs {float(value):+,.0f}"
    except (TypeError, ValueError):
        return "n/a"


def _format_percent(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "n/a"


def _format_int(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        value_any: Any = value
        return f"{int(value_any):d}"
    except (TypeError, ValueError):
        return "n/a"


def _fetch_metric_rows(run_ids: list[str]) -> dict[str, dict[str, object]]:
    from db.backtest_db import close_backtest_db, get_backtest_db

    ids = sorted({rid for rid in run_ids if rid})
    if not ids:
        return {}

    db = get_backtest_db()
    ids_sql = ", ".join(f"'{rid}'" for rid in ids)
    rows = db.con.execute(
        f"""
        SELECT
            run_id,
            COALESCE(NULLIF(label, ''), run_id) AS label,
            trade_count,
            win_rate,
            total_pnl,
            calmar,
            annual_return_pct,
            max_dd_pct
        FROM run_metrics
        WHERE run_id IN ({ids_sql})
        """,
    ).pl()
    metrics_map: dict[str, dict[str, object]] = {}
    for row in rows.to_dicts():
        metrics_map[str(row["run_id"])] = row
    close_backtest_db()
    return metrics_map


def _build_baseline_table(results: list[BaselineResult], previous: dict[str, str]) -> str:
    """Build a compact comparison table for the final baseline report."""
    successful = {r.label: r.run_id for r in results if r.exit_code == 0 and r.run_id}
    if not successful:
        return "\nNo successful runs to compare."

    metrics_map = _fetch_metric_rows(list(successful.values()) + list(previous.values()))
    headers = [
        "Variant",
        "New Run",
        "Prev Run",
        "New P/L",
        "Prev P/L",
        "Delta",
        "WR",
        "Trades",
        "Calmar",
    ]

    def md_cell(value: object) -> str:
        text = "n/a" if value is None else str(value)
        return text.replace("|", "\\|")

    lines = ["", "Baseline Comparison", ""]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for variant in BASELINE_VARIANTS:
        label = variant["label"]
        new_id = successful.get(label, "")
        prev_id = previous.get(label, "")
        new_m = metrics_map.get(new_id, {})
        prev_m = metrics_map.get(prev_id, {})

        new_pnl = new_m.get("total_pnl")
        prev_pnl = prev_m.get("total_pnl")
        delta_pnl = None
        if isinstance(new_pnl, int | float) and isinstance(prev_pnl, int | float):
            delta_pnl = float(new_pnl) - float(prev_pnl)

        lines.append(
            "| "
            + " | ".join(
                [
                    md_cell(label),
                    md_cell(new_id[:12] if new_id else "n/a"),
                    md_cell(prev_id[:12] if prev_id else "n/a"),
                    md_cell(_format_money(new_pnl)),
                    md_cell(_format_money(prev_pnl)),
                    md_cell(_format_delta_money(delta_pnl) if delta_pnl is not None else "n/a"),
                    md_cell(_format_percent(new_m.get("win_rate"))),
                    md_cell(_format_int(new_m.get("trade_count"))),
                    md_cell(
                        f"{float(new_m.get('calmar') or 0):.2f}"
                        if new_m.get("calmar") is not None
                        else "n/a"
                    ),
                ]
            )
            + " |"
        )

    return "\n".join(lines)


def _send_notification(
    results: list[BaselineResult],
    previous: dict[str, str],
    end_date: str,
) -> None:
    """Send Telegram notification with baseline results."""
    try:
        from config.settings import get_settings

        settings = get_settings()
        token = settings.telegram_bot_token
        chat_ids = settings.telegram_chat_ids
        if not token or not chat_ids:
            logger.info("No Telegram config — skipping baseline notification.")
            return

        ok_count = sum(1 for r in results if r.exit_code == 0)
        fail_count = sum(1 for r in results if r.exit_code != 0)
        lines = [
            f"<b>Baselines Complete: {end_date}</b>",
            f"Runs: {ok_count} OK, {fail_count} failed",
            "",
        ]
        for r in results:
            status = "OK" if r.exit_code == 0 else "FAIL"
            prev_id = previous.get(r.label, "")
            lines.append(f"  {r.label}: {r.run_id} [{status}] ({r.elapsed_sec:.0f}s)")
            if prev_id and r.exit_code == 0:
                lines.append(f"    prev: {prev_id}")

        import requests  # type: ignore[import-untyped]

        for chat_id in chat_ids:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={
                        "chat_id": chat_id,
                        "text": "\n".join(lines),
                        "parse_mode": "HTML",
                    },
                    timeout=10,
                )
            except Exception:
                pass
        logger.info("Baseline notification sent.")
    except Exception as exc:
        logger.warning("Baseline notification failed: %s", exc)


def _print_delta_report(results: list[BaselineResult], previous: dict[str, str]) -> None:
    """Print a comparison table between new and previous baselines."""
    print(_build_baseline_table(results, previous))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pivot-baselines",
        description=(
            "Run the 8 canonical CPR baseline backtests (4 presets x 2 equity modes). "
            "Daily operational command with pre-flight checks, auto-comparison, and resume."
        ),
    )
    parser.add_argument("--start", default="2025-01-01", help="Start date (default: 2025-01-01)")
    parser.add_argument("--end", required=True, help="End date (required)")
    parser.add_argument(
        "--resume", action="store_true", help="Resume from previous interrupted run"
    )
    parser.add_argument(
        "--notify", action="store_true", help="Send Telegram notification on completion"
    )
    parser.add_argument(
        "--clean-last",
        action="store_true",
        help="Delete the previous baseline set before running (DANGEROUS)",
    )
    return parser


def main() -> None:
    from engine.cli_setup import configure_windows_stdio

    configure_windows_stdio(line_buffering=True, write_through=True)

    parser = build_parser()
    args = parser.parse_args()

    print(f"{'=' * 60}")
    print(f" pivot-baselines: {args.start} → {args.end}")
    print(f"{'=' * 60}")

    # ── Pre-flight: universe check ──────────────────────────────────────
    symbol_count = _preflight_universe_check(args.start, args.end)
    print(f"\nPre-flight: {symbol_count} symbols in universe for {args.start}→{args.end}")
    if symbol_count < 100:
        print(f"ERROR: Only {symbol_count} symbols found. Run pivot-build first.")
        sys.exit(1)

    # ── Find previous baselines for delta comparison ────────────────────
    previous = _find_previous_baselines(args.end)
    print(f"Previous baselines found: {len(previous)} variants")
    for label, rid in previous.items():
        print(f"  {label}: {rid}")

    # ── Clean-last: remove previous baseline set ────────────────────────
    if args.clean_last and previous:
        prev_ids = list(previous.values())
        print(f"\n--clean-last: removing {len(prev_ids)} previous baseline runs...")
        _cleanup_runs(prev_ids)
        previous = {}

    # ── Resume: load progress from previous attempt ─────────────────────
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    progress_path = BASELINE_DIR / f"baselines_{args.end}.json"

    skip_labels: set[str] = set()
    prior_results: list[BaselineResult] = []

    if args.resume:
        progress = _load_progress(progress_path)
        if progress:
            for entry in progress.get("results", []):
                if entry.get("exit_code", -1) == 0:
                    skip_labels.add(entry["label"])
                    prior_results.append(
                        BaselineResult(
                            label=entry["label"],
                            run_id=entry["run_id"],
                            exit_code=entry.get("exit_code", 0),
                            elapsed_sec=entry.get("elapsed_sec", 0.0),
                            params_dict={},
                        )
                    )
            print(f"\n--resume: skipping {len(skip_labels)} already-completed runs")

    # ── Release parent DuckDB handle ────────────────────────────────────
    from db.duckdb import close_db

    close_db()

    # ── Run the 8 baselines sequentially ────────────────────────────────
    results: list[BaselineResult] = list(prior_results)
    total = len(BASELINE_VARIANTS)
    run_stamp = time.strftime("%Y%m%d-%H%M%S")
    progress_dir = PROJECT_ROOT / ".tmp_logs" / "baselines" / f"{args.end}-{run_stamp}"
    progress_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nRunning {total} baselines ({len(skip_labels)} skipped)...\n")

    for idx, variant in enumerate(BASELINE_VARIANTS, 1):
        label = variant["label"]

        if label in skip_labels:
            print(f"  [{idx}/{total}] {label} — SKIPPED (resume)")
            continue

        progress_file = progress_dir / f"{idx:02d}-{_slugify_name(label)}.jsonl"
        stdout_path = progress_dir / f"{idx:02d}-{_slugify_name(label)}.stdout.log"
        stderr_path = progress_dir / f"{idx:02d}-{_slugify_name(label)}.stderr.log"
        cmd = _build_backtest_args(
            args.start,
            args.end,
            variant,
            progress_file=progress_file,
        )
        print(
            f"  [{idx}/{total}] {label} ...",
            flush=True,
        )
        print(
            f"      progress: {progress_file}",
            flush=True,
        )
        print(
            f"      stdout:   {stdout_path}",
            flush=True,
        )
        print(
            f"      stderr:   {stderr_path}",
            flush=True,
        )
        t0 = time.time()

        try:
            completed = _run_backtest_with_logs(
                cmd,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                cwd=str(PROJECT_ROOT),
            )
        except subprocess.TimeoutExpired:
            print("      TIMEOUT")
            results.append(
                BaselineResult(
                    label=label, run_id="(timeout)", exit_code=-1, elapsed_sec=3600, params_dict={}
                )
            )
            _save_progress(progress_path, results, total)
            continue

        elapsed = round(time.time() - t0, 1)

        # Parse run_id from stdout
        run_id = _extract_run_id(stdout_path, progress_file, completed.stdout)

        status = "OK" if completed.returncode == 0 else f"FAIL({completed.returncode})"
        print(f"      {run_id} [{status}] ({elapsed}s)")

        results.append(
            BaselineResult(
                label=label,
                run_id=run_id,
                exit_code=completed.returncode,
                elapsed_sec=elapsed,
                params_dict=variant,
            )
        )

        _save_progress(progress_path, results, total)

        if completed.returncode != 0:
            try:
                stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                stderr_text = ""
            for line in stderr_text.strip().splitlines()[-3:]:
                logger.warning("    stderr: %s", line)

    _save_progress(progress_path, results, total)

    # ── Summary ─────────────────────────────────────────────────────────
    ok = sum(1 for r in results if r.exit_code == 0)
    fail = total - ok
    print(f"\n{'=' * 60}")
    print(f"Results: {ok}/{total} OK, {fail} failed")
    print(f"Progress saved: {progress_path}")

    # ── Delta report ────────────────────────────────────────────────────
    if ok > 0:
        _print_delta_report(results, previous)

    # ── Sync replica ────────────────────────────────────────────────────
    from db.backtest_db import close_backtest_db, get_backtest_db

    db = get_backtest_db()
    sync = db._sync
    assert sync is not None
    sync.mark_dirty()
    sync.force_sync(db.con)
    close_backtest_db()
    print("\nReplica synced.")

    # ── Notification ────────────────────────────────────────────────────
    if args.notify:
        _send_notification(results, previous, args.end)


if __name__ == "__main__":
    main()
