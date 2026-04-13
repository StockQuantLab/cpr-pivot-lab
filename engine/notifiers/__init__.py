"""Notifier protocol — implemented by TelegramNotifier and EmailNotifier."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Notifier(Protocol):
    """Minimal interface every notifier must satisfy."""

    enabled: bool

    async def send(self, subject: str, body: str) -> None: ...
