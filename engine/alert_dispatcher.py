"""Alert dispatcher — best-effort async alert delivery via Telegram and email.

Guarantees:
- On clean shutdown: all queued alerts are sent (drain + flush, up to 120 s timeout)
- On crash: alerts in the async queue may be lost (in-memory)
- Persistent audit: all sent/failed alerts logged to paper.duckdb alert_log

Does NOT guarantee zero alert loss on unclean crash.
Shutdown uses stop flag + wait (not cancel) to avoid losing in-flight events.
Uses asyncio.wait() with polling instead of wait_for() to avoid cancelling
the consumer task on timeout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import StrEnum

from db.paper_db import PaperDB
from engine.notifiers.email import EmailNotifier
from engine.notifiers.telegram import TelegramNotifier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class AlertType(StrEnum):
    TRADE_OPENED = "TRADE_OPENED"
    TRADE_CLOSED = "TRADE_CLOSED"
    SL_HIT = "SL_HIT"
    TARGET_HIT = "TARGET_HIT"
    TRAIL_STOP = "TRAIL_STOP"
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_COMPLETED = "SESSION_COMPLETED"
    SESSION_ERROR = "SESSION_ERROR"
    FEED_STALE = "FEED_STALE"
    FEED_RECOVERED = "FEED_RECOVERED"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    DRAWDOWN_LIMIT = "DRAWDOWN_LIMIT"
    FLATTEN_EOD = "FLATTEN_EOD"
    DAILY_PNL_SUMMARY = "DAILY_PNL_SUMMARY"


@dataclass
class AlertConfig:
    telegram_bot_token: str | None = None
    telegram_chat_ids: list[str] = field(default_factory=list)
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    alert_to_email: str | None = None


@dataclass
class AlertEvent:
    alert_type: AlertType
    subject: str
    body: str
    retries: int = 0


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class AlertDispatcher:
    """Best-effort async alert dispatcher.

    Guarantees:
    - On clean shutdown: all queued alerts are sent (drain + flush, up to 120s timeout)
    - On crash: alerts in the async queue may be lost (in-memory)
    - Persistent audit: all sent/failed alerts logged to paper.duckdb alert_log

    Does NOT guarantee zero alert loss on unclean crash.
    Shutdown uses stop flag + wait (not cancel) to avoid losing in-flight events.
    Uses asyncio.wait() with polling instead of wait_for() to avoid cancelling
    the consumer task on timeout.
    """

    MAX_QUEUE_SIZE = 100
    MAX_RETRIES = 3
    RETRY_BACKOFF = (1.0, 2.0, 4.0)

    def __init__(self, paper_db: PaperDB, config: AlertConfig) -> None:
        self.paper_db = paper_db
        self.telegram = TelegramNotifier(config.telegram_bot_token, config.telegram_chat_ids)
        self.email = EmailNotifier(
            config.smtp_host,
            config.smtp_port,
            config.smtp_user,
            config.smtp_password,
            config.alert_to_email,
        )
        self._queue: asyncio.Queue[AlertEvent] = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self._consumer_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Start the background consumer task."""
        if self._running:
            return
        self._running = True
        self._consumer_task = asyncio.create_task(self._consumer_loop())

    async def dispatch(self, alert_type: AlertType, subject: str, body: str) -> None:
        """Enqueue an alert for delivery. Non-blocking; drops if queue is full."""
        if not self._running:
            await self.start()
        try:
            event = AlertEvent(
                alert_type=alert_type,
                subject=subject,
                body=body,
                retries=0,
            )
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.error("Alert queue full, dropping: %s", subject)
            self.paper_db.log_alert(
                alert_type,
                subject,
                body,
                channel="LOG",
                status="failed",
                error_msg="queue_full",
            )

    async def shutdown(self) -> None:
        """Graceful shutdown: stop consumer, drain queue, send remaining items.

        Best-effort: sends all queued alerts within 120s. Uses asyncio.wait()
        instead of wait_for() to avoid cancelling the consumer task on timeout.
        """
        self._running = False
        if self._consumer_task:
            # Use polling instead of wait_for() to avoid cancelling
            # the consumer task (wait_for cancels on timeout, losing in-flight events)
            deadline = time.monotonic() + 120.0
            while not self._consumer_task.done() and time.monotonic() < deadline:
                await asyncio.sleep(0.5)
            if not self._consumer_task.done():
                logger.warning("Alert consumer did not stop within 120s (still running)")
        # Drain remaining items
        remaining: list[AlertEvent] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        for event in remaining:
            try:
                await self._send_with_retry(event)
            except Exception as exc:
                logger.error("Shutdown flush failed: %s — %s", event.subject, exc)

    async def _consumer_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            await self._send_with_retry(event)

    async def _send_with_retry(self, event: AlertEvent) -> None:
        last_error: Exception | None = None
        telegram_ok = False
        email_ok = False

        for attempt in range(self.MAX_RETRIES):
            try:
                # Telegram (primary)
                if self.telegram.enabled and not telegram_ok:
                    await self.telegram.send(event.subject, event.body)
                    telegram_ok = True
                # Email (backup)
                if self.email.enabled and not email_ok:
                    await asyncio.wait_for(
                        self.email.send(event.subject, event.body),
                        timeout=10.0,
                    )
                    email_ok = True
                # Both succeeded (or neither enabled) — stop retrying
                break
            except Exception as exc:
                last_error = exc
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt])

        # Determine overall channel status
        if telegram_ok and email_ok:
            channel = "BOTH"
            status = "sent"
        elif telegram_ok or email_ok:
            channel = "TELEGRAM" if telegram_ok else "EMAIL"
            status = "sent"
        else:
            channel = "BOTH"
            status = "failed"

        self.paper_db.log_alert(
            str(event.alert_type.value),
            event.subject,
            event.body,
            channel=channel,
            status=status,
            error_msg=str(last_error) if status == "failed" else None,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_alert_config() -> AlertConfig:
    """Build AlertConfig from Doppler-backed settings."""
    from config.settings import get_settings

    s = get_settings()
    chat_ids = [
        c.strip() for c in (getattr(s, "telegram_chat_ids", None) or "").split(",") if c.strip()
    ]
    return AlertConfig(
        telegram_bot_token=getattr(s, "telegram_bot_token", None),
        telegram_chat_ids=chat_ids,
        smtp_host=s.smtp_host,
        smtp_port=s.smtp_port,
        smtp_user=s.smtp_user,
        smtp_password=s.smtp_password,
        alert_to_email=s.alert_to_email,
    )
