"""Email notifier — sends plain-text alerts via STARTTLS using aiosmtplib."""

from __future__ import annotations

import logging
import re
from email.mime.text import MIMEText

import aiosmtplib

logger = logging.getLogger(__name__)


class EmailNotifier:
    """Send plain-text email alerts via STARTTLS.

    Failures are logged but never raised — alerting is best-effort and must not
    crash the trading loop.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str | None,
        password: str | None,
        to_addr: str | None,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._to_addr = to_addr

    @property
    def enabled(self) -> bool:
        # Requires three Doppler secrets: SMTP_USER (sender address + SMTP login),
        # SMTP_PASSWORD, and ALERT_TO_EMAIL (recipient address).
        return bool(self._user and self._password and self._to_addr)

    async def send(self, subject: str, body: str) -> None:
        if not self.enabled:
            return
        # Body is HTML-formatted for Telegram — strip tags for plain-text email
        plain_body = re.sub(r"<[^>]+>", "", body)
        msg = MIMEText(plain_body, "plain")
        msg["Subject"] = subject
        msg["From"] = self._user or ""
        msg["To"] = self._to_addr or ""
        try:
            await aiosmtplib.send(
                msg,
                hostname=self._host,
                port=self._port,
                username=self._user,
                password=self._password,
                start_tls=True,
            )
        except Exception as exc:
            logger.error(
                "Email send failed to=%s host=%s:%s: %s",
                self._to_addr,
                self._host,
                self._port,
                exc,
            )
