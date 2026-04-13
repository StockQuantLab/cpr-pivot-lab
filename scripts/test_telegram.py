"""Test Telegram bot connectivity — sends sample alerts in the new format.

Usage:
    doppler run -- uv run python scripts/test_telegram.py
"""

from __future__ import annotations

import asyncio

import httpx

from config.settings import get_settings

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT = 10.0


async def test_telegram() -> None:
    settings = get_settings()
    token = settings.telegram_bot_token
    chat_ids_raw = settings.telegram_chat_ids

    if not token or not chat_ids_raw:
        print("ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS not configured")
        print(
            "  Set them via Doppler: doppler secrets set TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_IDS=..."
        )
        return

    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    if not chat_ids:
        print("ERROR: TELEGRAM_CHAT_IDS is set but empty after parsing")
        return

    print("Telegram bot token: [SET]")
    print(f"Chat IDs to test: {chat_ids}")
    print()

    url = _TELEGRAM_API.format(token=token)
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        msgs = [
            (
                "<b>🟢 LONG OPENED: GANESHCP</b>\n\n"
                "📥 Entry: <code>₹171.43</code>\n"
                "🛡️ SL: <code>₹168.61</code> | 🎯 Target: <code>₹177.60</code>\n"
                "📏 Qty: <code>353</code> | 💰 Risk: ₹997 (2.2R)\n"
                "🕒 09:40 01-Apr\n"
                "<i>CPR_LEVELS · CPR_LEVELS_LONG</i>",
                "OPENED (LONG)",
            ),
            (
                "<b>🔴 SHORT OPENED: ASTERDM</b>\n\n"
                "📥 Entry: <code>₹673.40</code>\n"
                "🛡️ SL: <code>₹677.62</code> | 🎯 Target: <code>₹644.97</code>\n"
                "📏 Qty: <code>148</code> | 💰 Risk: ₹625 (6.7R)\n"
                "🕒 09:35 01-Apr\n"
                "<i>CPR_LEVELS · CPR_LEVELS_SHORT</i>",
                "OPENED (SHORT)",
            ),
            (
                "<b>✅ [WIN] GANESHCP LONG</b>\n\n"
                "💰 P&L: <code>+₹2,107</code> (+3.60%)\n"
                "🏁 Reason: TARGET\n"
                "📈 Exit: <code>171.43</code> → <code>177.60</code>\n"
                "🕒 13:15 01-Apr\n"
                "<i>CPR_LEVELS · CPR_LEVELS_LONG</i>",
                "WIN (TARGET)",
            ),
            (
                "<b>❌ [LOSS] ASTERDM SHORT</b>\n\n"
                "💰 P&L: <code>-₹709</code> (-0.63%)\n"
                "🏁 Reason: INITIAL_SL\n"
                "📉 Exit: <code>673.40</code> → <code>677.62</code>\n"
                "🕒 09:50 01-Apr\n"
                "<i>CPR_LEVELS · CPR_LEVELS_SHORT</i>",
                "LOSS (SL)",
            ),
            (
                "<b>📊 Risk Management Update</b>\n\n"
                "Session: <code>CPR_LEVELS_SHORT</code>\n"
                "Status: 🕒 flatten_time:15:15:00\n"
                "Net P&L: <code>-708.76</code> 📉\n"
                "Trades closed: 3",
                "RISK (flatten EOD)",
            ),
            (
                "<b>📊 Risk Management Update</b>\n\n"
                "Session: <code>CPR_LEVELS_LONG</code>\n"
                "Status: 🕒 daily_loss_limit\n"
                "Net P&L: <code>-3,500.00</code> 📉\n"
                "Trades closed: 5",
                "RISK (loss limit)",
            ),
        ]

        for chat_id in chat_ids:
            print(f"--- Sending to chat_id={chat_id} ---")
            for text, label in msgs:
                try:
                    resp = await client.post(
                        url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
                    )
                    if resp.status_code == 200:
                        print(f"  OK   {label}")
                    else:
                        print(f"  FAIL {label} — HTTP {resp.status_code}: {resp.text[:200]}")
                except Exception as exc:
                    print(f"  FAIL {label} — {exc}")
            print()


if __name__ == "__main__":
    asyncio.run(test_telegram())
