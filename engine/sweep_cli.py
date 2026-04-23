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
    parser.add_argument(
        "--resume",
        type=str,
        default=None,
        metavar="MANIFEST",
        help="Path to a previous sweep manifest JSON to resume from",
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send Telegram/email notification on sweep completion",
    )
    return parser


def _send_sweep_notification(sweep_name: str, results: list, ranked: list) -> None:
    """Best-effort Telegram notification for sweep completion."""
    try:
        from config.settings import get_settings

        settings = get_settings()
        token = settings.telegram_bot_token
        chat_ids = settings.telegram_chat_ids
        if not token or not chat_ids:
            logger.info("No Telegram config — skipping sweep notification.")
            return

        completed = sum(1 for r in results if r.exit_code == 0)
        failed = sum(1 for r in results if r.exit_code != 0)
        lines = [
            f"<b>Sweep Complete: {sweep_name}</b>",
            f"Runs: {completed} OK, {failed} failed",
        ]
        if ranked:
            lines.append("")
            for i, s in enumerate(ranked[:5], 1):
                lines.append(
                    f"{i}. {s.label}: Calmar={s.calmar:.2f} WR={s.win_rate:.1f}% "
                    f"P/L=₹{s.total_pnl:,.0f} Trades={s.trade_count}"
                )

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
        logger.info("Sweep notification sent.")
    except Exception as exc:
        logger.warning("Sweep notification failed: %s", exc)


def _sql_quote(value: str) -> str:
    """Return a SQL string literal for a trusted run_id."""
    return "'" + value.replace("'", "''") + "'"


def _build_baseline_delta(results: list, compare_against: dict) -> str:
    """Build a delta comparison table between new runs and baseline run IDs."""
    from db.duckdb import get_db

    new_by_label = {r.label: r.run_id for r in results if r.exit_code == 0}
    baseline_ids = {v for v in compare_against.values() if isinstance(v, str)}
    if not baseline_ids or not new_by_label:
        return ""

    db = get_db()
    ids_sql = ", ".join(
        _sql_quote(rid) for rid in sorted(baseline_ids | set(new_by_label.values()))
    )
    rows = db.con.execute(
        f"""
        SELECT run_id, trade_count, win_rate, total_pnl, calmar,
               annual_return_pct, max_dd_pct
        FROM run_metrics
        WHERE run_id IN ({ids_sql})
        """,
    ).pl()

    baseline_map: dict[str, dict] = {}
    for row in rows.to_dicts():
        baseline_map[row["run_id"]] = row

    lines = []
    lines.append(f"\n{'=' * 70}")
    lines.append("Baseline Delta (new vs compare_against)")
    lines.append(f"{'=' * 70}")

    for label, baseline_id in compare_against.items():
        baseline = baseline_map.get(baseline_id, {})
        if not baseline:
            lines.append(f"  {label}: baseline {baseline_id} not found in DB")
            continue

        new_id = new_by_label.get(label)
        if not new_id:
            lines.append(f"  {label}: no matching new run found")
            continue

        new_run = baseline_map.get(new_id, {})
        if not new_run:
            lines.append(f"  {label}: new run {new_id} not found in DB")
            continue

        pnl_delta = new_run.get("total_pnl", 0) - baseline.get("total_pnl", 0)
        wr_delta = new_run.get("win_rate", 0) - baseline.get("win_rate", 0)
        trades_delta = new_run.get("trade_count", 0) - baseline.get("trade_count", 0)

        lines.append(
            f"  {label}:"
            f"  P/L ₹{new_run.get('total_pnl', 0):,.0f} (Δ{pnl_delta:+,.0f})"
            f"  WR {new_run.get('win_rate', 0):.1f}% (Δ{wr_delta:+.1f}%)"
            f"  Trades {int(new_run.get('trade_count', 0) or 0)} (Δ{int(trades_delta):+d})"
        )

    return "\n".join(lines)


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

    resume_path = Path(args.resume) if args.resume else None
    results = run_sweep(config, dry_run=args.dry_run, resume_from=resume_path)

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

    # Baseline delta comparison
    if not args.dry_run and hasattr(config, "compare_against") and config.compare_against:
        delta_text = _build_baseline_delta(results, config.compare_against)
        if delta_text:
            print(delta_text)

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

    # Send notification if requested
    if args.notify and not args.dry_run:
        _send_sweep_notification(config.name, results, ranked)


if __name__ == "__main__":
    main()
