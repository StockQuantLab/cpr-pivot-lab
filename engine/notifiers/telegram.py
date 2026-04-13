"""Telegram notifier — sends messages via the Bot API using httpx (async).

Uses HTML parse_mode. The body is expected to be pre-formatted HTML from the
alert format functions in paper_runtime.py. An inline keyboard button linking
to TradingView is auto-attached when a symbol is detected in the subject.
Link previews are disabled to save screen space.
"""

from __future__ import annotations

import logging
import re
from html import escape

import httpx

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10.0

# Match symbol after "OPENED: SYMBOL" or after "[WIN/LOSS] SYMBOL DIRECTION"
_SYMBOL_RE = re.compile(r"(?:OPENED:\s*|\]\s*)([A-Z]{3,})")


class TelegramNotifier:
    """Send Telegram messages to one or more chat IDs via the Bot API.

    Uses httpx.AsyncClient with a 10 s timeout. Failures are logged but never
    raised — alerting is best-effort and must not crash the trading loop.
    """

    def __init__(self, bot_token: str | None, chat_ids: list[str]) -> None:
        self._bot_token = bot_token
        self._chat_ids = chat_ids
        self._client: httpx.AsyncClient | None = None

    @property
    def enabled(self) -> bool:
        return bool(self._bot_token and self._chat_ids)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, subject: str, body: str) -> None:
        if not self.enabled:
            return
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=_TIMEOUT)
        url = _TELEGRAM_API.format(token=self._bot_token)
        # Subject is escaped (contains user-visible text/emojis, no HTML).
        # Body is pre-formatted HTML from _format_*_alert — do NOT escape.
        text = f"<b>{escape(subject)}</b>\n\n{body}"
        # Auto-attach TradingView chart button when symbol is found
        reply_markup = self._chart_button(subject)
        for chat_id in self._chat_ids:
            payload: dict = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = await self._client.post(url, json=payload)
            if not resp.is_success:
                safe_url = url.replace(self._bot_token or "", "***")
                logger.error(
                    "Telegram send failed for chat_id=%s url=%s: HTTP %s",
                    chat_id,
                    safe_url,
                    resp.status_code,
                )
                resp.raise_for_status()  # Let dispatcher retry on 429 / transient errors

    @staticmethod
    def _chart_button(subject: str) -> dict | None:
        """Build inline keyboard with TradingView link if a symbol is detected."""
        m = _SYMBOL_RE.search(subject)
        if not m:
            return None
        symbol = m.group(1)
        return {
            "inline_keyboard": [
                [
                    {
                        "text": "📊 View Chart",
                        "url": f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}",
                    }
                ]
            ]
        }
