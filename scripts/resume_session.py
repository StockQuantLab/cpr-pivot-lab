"""One-shot session resume helper.

Calls run_live_session() directly with the existing session_id, bypassing
paper_trading.py's _ensure_daily_session() dedup logic. Used when a session
goes STALE mid-day and we want to resume under the same session_id.

Usage:
    doppler run -- uv run python scripts/resume_session.py \
        --session-id CPR_LEVELS_SHORT-2026-04-13 \
        --symbols SUKHJITS

The session must already exist in paper.duckdb. Open positions are seeded
from DB automatically (no re-entry for already-closed symbols).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resume a stale/failed live session by ID")
    parser.add_argument("--session-id", required=True, help="Existing session_id to resume")
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Symbols to monitor (only open positions matter; closed ones are seeded from DB)",
    )
    parser.add_argument("--poll-interval-sec", type=float, default=1.0)
    parser.add_argument("--candle-interval-minutes", type=int, default=5)
    parser.add_argument("--no-alerts", action="store_true", help="Suppress Telegram/email alerts")
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()

    if args.no_alerts:
        from engine.paper_runtime import set_alerts_suppressed
        set_alerts_suppressed(True)

    from scripts.paper_live import run_live_session

    print(f"Resuming session {args.session_id!r} with symbols={args.symbols}", flush=True)
    payload = await run_live_session(
        session_id=args.session_id,
        symbols=args.symbols,
        poll_interval_sec=args.poll_interval_sec,
        candle_interval_minutes=args.candle_interval_minutes,
    )
    print(json.dumps(payload, default=str, indent=2))


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_main())
