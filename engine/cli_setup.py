"""Shared CLI stdio setup helpers.

Centralizes Windows UTF-8 wrapping and optional line-buffering setup so CLI
entry points use one implementation.
"""

from __future__ import annotations

import asyncio
import io
import selectors
import sys
from typing import TextIO


def _wrap_utf8(stream: TextIO) -> TextIO:
    """Return UTF-8 wrapped stream when possible, otherwise original stream."""
    if not hasattr(stream, "buffer"):
        return stream
    try:
        return io.TextIOWrapper(stream.buffer, encoding="utf-8", errors="replace")
    except Exception as _:
        return stream


def configure_windows_stdio(*, line_buffering: bool = False, write_through: bool = False) -> None:
    """Configure stdio consistently across CLI entry points.

    On Windows, stdout/stderr are wrapped in UTF-8 text wrappers.
    Optional line buffering/write-through is applied when supported.
    """
    if "pytest" in sys.modules:
        return
    if sys.platform == "win32":
        sys.stdout = _wrap_utf8(sys.stdout)
        sys.stderr = _wrap_utf8(sys.stderr)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=line_buffering, write_through=write_through)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=line_buffering, write_through=write_through)


def configure_windows_asyncio() -> None:
    """Install the selector event loop policy on Windows when available.

    Python 3.14 + psycopg async usage can fail on Windows under the default
    proactor policy, so async CLI entry points that touch PostgreSQL should
    call this before asyncio.run(...).
    """
    if sys.platform.startswith("win"):
        windows_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
        if windows_policy is not None:
            asyncio.set_event_loop_policy(windows_policy())


def _windows_selector_loop_factory() -> asyncio.AbstractEventLoop:
    """Build a selector-backed loop for Windows async PostgreSQL clients."""

    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def run_asyncio(coro):
    """Run a coroutine with a Windows selector loop when needed."""

    if sys.platform.startswith("win"):
        with asyncio.Runner(loop_factory=_windows_selector_loop_factory) as runner:
            return runner.run(coro)
    return asyncio.run(coro)
