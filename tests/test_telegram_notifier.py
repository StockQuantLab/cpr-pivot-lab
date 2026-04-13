from __future__ import annotations

import pytest

import engine.notifiers.telegram as telegram_mod
from engine.notifiers.telegram import TelegramNotifier


class _FakeResponse:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300


class _FakeClient:
    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout
        self.calls: list[dict[str, object]] = []

    async def post(self, url: str, json: dict[str, object]) -> _FakeResponse:
        self.calls.append({"url": url, "json": json})
        return _FakeResponse()

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_telegram_notifier_escapes_html_and_uses_html_parse_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[_FakeClient] = []

    def fake_async_client(*, timeout: float) -> _FakeClient:
        client = _FakeClient(timeout=timeout)
        clients.append(client)
        return client

    monkeypatch.setattr(telegram_mod.httpx, "AsyncClient", fake_async_client)

    notifier = TelegramNotifier("token-123", ["chat-1", "chat-2"])
    await notifier.send(
        "CPR <LEVELS> LONG", "Strategy: CPR_LEVELS & replay\nSession: paper-cprlevels"
    )

    assert len(clients) == 1
    assert len(clients[0].calls) == 2
    first_call = clients[0].calls[0]["json"]
    assert first_call["chat_id"] == "chat-1"
    assert first_call["parse_mode"] == "HTML"
    assert first_call["disable_web_page_preview"] is True
    assert "Strategy: CPR_LEVELS" in first_call["text"]
    assert "paper-cprlevels" in first_call["text"]


@pytest.mark.asyncio
async def test_telegram_notifier_escapes_problematic_markdown_chars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient(timeout=10.0)

    def fake_async_client(*, timeout: float) -> _FakeClient:
        client.timeout = timeout
        return client

    monkeypatch.setattr(telegram_mod.httpx, "AsyncClient", fake_async_client)

    notifier = TelegramNotifier("token-123", ["chat-1"])
    await notifier.send(
        "[WIN] GANESHCP LONG TARGET", "Strategy: CPR_LEVELS | Session: paper_cprlevels"
    )

    payload = client.calls[0]["json"]
    assert payload["chat_id"] == "chat-1"
    assert payload["parse_mode"] == "HTML"
    assert payload["disable_web_page_preview"] is True
    # Body is no longer html.escape()'d — literal & is preserved
    assert "Strategy: CPR_LEVELS |" in payload["text"]
