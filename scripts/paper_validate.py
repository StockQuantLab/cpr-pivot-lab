"""Run the canonical paper replay validation bundle for one trade date."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from db.paper_db import get_paper_db
from engine.cli_setup import configure_windows_stdio
from scripts.paper_prepare import resolve_trade_date

configure_windows_stdio(line_buffering=True, write_through=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str], *, dry_run: bool) -> dict[str, Any]:
    print("$ " + " ".join(cmd), flush=True)
    if dry_run:
        return {"command": cmd, "returncode": 0, "dry_run": True}
    completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), text=True)
    return {"command": cmd, "returncode": int(completed.returncode), "dry_run": False}


def _session_summary(trade_date: str) -> dict[str, Any]:
    db = get_paper_db()
    rows = db.con.execute(
        """
        SELECT session_id, status, strategy, direction, execution_mode, mode,
               total_pnl, created_at, updated_at
        FROM paper_sessions
        WHERE trade_date = ?
        ORDER BY created_at DESC
        """,
        [trade_date],
    ).fetchall()
    sessions = []
    for row in rows:
        session_id = str(row[0])
        pos = db.con.execute(
            """
            SELECT
                COUNT(*) AS trades,
                SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_positions,
                COALESCE(SUM(pnl), 0) AS pnl
            FROM paper_positions
            WHERE session_id = ?
            """,
            [session_id],
        ).fetchone()
        sessions.append(
            {
                "session_id": session_id,
                "status": row[1],
                "strategy": row[2],
                "direction": row[3],
                "execution_mode": row[4],
                "mode": row[5],
                "total_pnl": row[6],
                "created_at": row[7],
                "updated_at": row[8],
                "trades": int(pos[0] or 0) if pos else 0,
                "open_positions": int(pos[1] or 0) if pos else 0,
                "position_pnl": float(pos[2] or 0) if pos else 0.0,
            }
        )
    return {
        "sessions": sessions,
        "session_count": len(sessions),
        "open_position_count": sum(int(s["open_positions"]) for s in sessions),
    }


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run canonical paper replay validation.")
    parser.add_argument("--trade-date", required=True, help="Trading date to validate.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only.")
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Do not run paper cleanup before replay validation.",
    )
    parser.add_argument(
        "--alerts",
        action="store_true",
        help="Allow alerts during replay. Default suppresses alerts.",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Report path. Default: .tmp_logs/paper_validate_<date>.json",
    )
    args = parser.parse_args()
    trade_date = resolve_trade_date(args.trade_date)
    report_path = (
        Path(args.report)
        if args.report
        else PROJECT_ROOT / ".tmp_logs" / f"paper_validate_{trade_date.replace('-', '')}.json"
    )

    commands: list[list[str]] = []
    if not args.no_cleanup:
        commands.append(
            [
                sys.executable,
                "-m",
                "scripts.paper_trading",
                "cleanup",
                "--trade-date",
                trade_date,
                "--apply",
            ]
        )
    commands.extend(
        [
            [
                sys.executable,
                "-m",
                "scripts.paper_trading",
                "daily-prepare",
                "--trade-date",
                trade_date,
                "--all-symbols",
            ],
            [
                sys.executable,
                "-m",
                "scripts.paper_trading",
                "daily-replay",
                "--multi",
                "--strategy",
                "CPR_LEVELS",
                "--trade-date",
                trade_date,
                *(["--no-alerts"] if not args.alerts else []),
            ],
            [
                sys.executable,
                "-m",
                "scripts.paper_trading",
                "feed-audit",
                "--trade-date",
                trade_date,
                "--feed-source",
                "replay",
            ],
        ]
    )

    started_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    results = []
    ok = True
    for cmd in commands:
        result = _run(cmd, dry_run=bool(args.dry_run))
        results.append(result)
        if int(result["returncode"]) != 0:
            ok = False
            break

    summary = {} if args.dry_run else _session_summary(trade_date)
    if summary.get("open_position_count", 0):
        ok = False
    payload = {
        "ok": ok,
        "trade_date": trade_date,
        "started_at": started_at,
        "finished_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "commands": results,
        "summary": summary,
    }
    _write_report(report_path, payload)
    print(f"Report: {report_path}")
    print(json.dumps({"ok": ok, "trade_date": trade_date, "report": str(report_path)}, indent=2))
    if not ok:
        raise SystemExit(1)


if __name__ in {"__main__", "__mp_main__"}:
    main()
