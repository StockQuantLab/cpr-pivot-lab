"""CPR Pivot Lab — NiceGUI Dashboard Entry Point.

Usage:
    doppler run -- uv run pivot-dashboard
    doppler run -- uv run python web/run_nicedash.py
"""

from __future__ import annotations

import asyncio
import sys

# Windows: must set event loop policy BEFORE any asyncio/uvicorn import
if sys.platform == "win32":
    windows_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if windows_policy is not None:
        asyncio.set_event_loop_policy(windows_policy())

import web.main  # Registers all @ui.page routes


def main() -> None:
    try:
        web.main.main()
    except KeyboardInterrupt:
        # Normal interactive shutdown on Ctrl+C.
        return


if __name__ in {"__main__", "__mp_main__"}:
    main()
