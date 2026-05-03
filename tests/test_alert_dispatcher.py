from __future__ import annotations

import time

import httpx
import pytest

from engine.alert_dispatcher import AlertConfig, AlertDispatcher, AlertEvent, AlertType
from engine.notifiers.email import EmailNotifier


class FakePaperDB:
    def __init__(self) -> None:
        self.alerts: list[dict[str, object]] = []

    def log_alert(
        self,
        alert_type: str,
        subject: str,
        body: str = "",
        *,
        channel: str = "BOTH",
        status: str = "queued",
        error_msg: str | None = None,
        **_kwargs,
    ) -> None:
        self.alerts.append(
            {
                "alert_type": alert_type,
                "subject": subject,
                "body": body,
                "channel": channel,
                "status": status,
                "error_msg": error_msg,
            }
        )


@pytest.mark.asyncio
async def test_alert_dispatcher_logs_when_no_channels_enabled() -> None:
    paper_db = FakePaperDB()
    dispatcher = AlertDispatcher(paper_db, AlertConfig())

    await dispatcher._send_with_retry(AlertEvent(AlertType.SESSION_ERROR, "subject", "body"))

    assert paper_db.alerts == [
        {
            "alert_type": "SESSION_ERROR",
            "subject": "subject",
            "body": "body",
            "channel": "NONE",
            "status": "failed",
            "error_msg": "no_channels_enabled",
        }
    ]


@pytest.mark.asyncio
async def test_alert_dispatcher_retries_then_logs_telegram_success(monkeypatch) -> None:
    paper_db = FakePaperDB()
    dispatcher = AlertDispatcher(
        paper_db,
        AlertConfig(telegram_bot_token="token", telegram_chat_ids=["1"]),
    )
    dispatcher.RETRY_BACKOFF = (0.0, 0.0, 0.0)
    sends = 0

    async def fake_send(_subject: str, _body: str) -> None:
        nonlocal sends
        sends += 1
        if sends == 1:
            raise httpx.ConnectError("temporary")

    monkeypatch.setattr(dispatcher.telegram, "send", fake_send)

    await dispatcher._send_with_retry(AlertEvent(AlertType.FEED_STALE, "stale", "body"))

    assert sends == 2
    assert paper_db.alerts[-1]["status"] == "sent"
    assert paper_db.alerts[-1]["channel"] == "TELEGRAM"


@pytest.mark.asyncio
async def test_alert_dispatcher_discards_stale_network_retry(monkeypatch) -> None:
    paper_db = FakePaperDB()
    dispatcher = AlertDispatcher(
        paper_db,
        AlertConfig(telegram_bot_token="token", telegram_chat_ids=["1"]),
    )
    dispatcher.MAX_RETRIES = 1
    dispatcher.MAX_ALERT_AGE_SEC = 1
    dispatcher.NETWORK_RETRY_BACKOFF = (30.0,)

    async def fake_send(_subject: str, _body: str) -> None:
        raise OSError("network down")

    monkeypatch.setattr(dispatcher.telegram, "send", fake_send)
    event = AlertEvent(
        AlertType.FEED_STALE,
        "old stale",
        "body",
        created_at=time.monotonic() - 10.0,
    )
    await dispatcher._send_with_retry(event)

    assert paper_db.alerts[-1]["status"] == "failed"
    assert "network down" in str(paper_db.alerts[-1]["error_msg"])


@pytest.mark.asyncio
async def test_email_notifier_reraises_send_failure(monkeypatch) -> None:
    notifier = EmailNotifier(
        "smtp.example.com",
        587,
        "sender@example.com",
        "password",
        "ops@example.com",
    )

    async def fake_send(*_args, **_kwargs) -> None:
        raise OSError("smtp down")

    monkeypatch.setattr("engine.notifiers.email.aiosmtplib.send", fake_send)

    with pytest.raises(OSError, match="smtp down"):
        await notifier.send("subject", "body")


@pytest.mark.asyncio
async def test_alert_dispatcher_queue_full_critical_falls_back_sync(monkeypatch) -> None:
    paper_db = FakePaperDB()
    dispatcher = AlertDispatcher(paper_db, AlertConfig())
    dispatcher._running = True
    fallback: list[str] = []

    for idx in range(dispatcher.MAX_QUEUE_SIZE):
        dispatcher._queue.put_nowait(AlertEvent(AlertType.TRADE_OPENED, f"s{idx}", "body"))

    async def fake_send_with_retry(event: AlertEvent) -> None:
        fallback.append(event.subject)

    monkeypatch.setattr(dispatcher, "_send_with_retry", fake_send_with_retry)

    await dispatcher.dispatch(AlertType.SESSION_ERROR, "critical", "body")

    assert fallback == ["critical"]
    assert paper_db.alerts == []


def test_alert_dispatcher_retry_after_reads_telegram_json() -> None:
    request = httpx.Request("POST", "https://api.telegram.org/bot***/sendMessage")
    response = httpx.Response(
        429,
        json={"parameters": {"retry_after": 12}},
        request=request,
    )
    exc = httpx.HTTPStatusError("rate limited", request=request, response=response)

    assert AlertDispatcher._telegram_retry_after(exc) == 12.0


def test_alert_dispatcher_uses_instance_semaphores() -> None:
    first = AlertDispatcher(FakePaperDB(), AlertConfig())
    second = AlertDispatcher(FakePaperDB(), AlertConfig())

    assert first._get_semaphore() is not second._get_semaphore()
