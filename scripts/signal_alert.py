"""Signal Alert — Check market conditions and send email notifications.

Usage:
    doppler run -- uv run pivot-signal-alert --symbols RELIANCE,TCS --condition narrow-cpr
    doppler run -- uv run pivot-signal-alert --universe gold_51 --condition virgin-cpr
    doppler run -- uv run pivot-signal-alert --symbols NIFTY50 --condition orb-fail

Conditions:
    narrow-cpr    : Alert when CPR width is in bottom percentile (high-probability setup)
    virgin-cpr     : Alert when CPR zone is untouched (breakthrough signal)
    orb-fail       : Alert when Opening Range fails (FBR entry signal)
    gap-up         : Alert on gap up above threshold
    gap-down       : Alert on gap down below threshold
"""

from __future__ import annotations

import argparse
from datetime import date

from db.duckdb import _validate_universe_name, get_db
from engine.cli_setup import configure_windows_stdio
from engine.constants import normalize_symbol
from engine.signal_generation import (
    AlertSignal,
    check_gap_signals,
    check_narrow_cpr,
    check_orb_fail,
    check_virgin_cpr,
)

configure_windows_stdio(line_buffering=True, write_through=True)


def send_email_alerts(signals: list[AlertSignal], recipients: str) -> bool:
    """Send email alerts for signals."""
    if not signals:
        print("No signals to alert.")
        return False

    if not recipients:
        print("ERROR: No recipients configured. Set --recipients or ALERT_TO_EMAIL env var.")
        return False

    try:
        import asyncio
        from email.message import EmailMessage

        import aiosmtplib

        from config.settings import get_settings

        settings = get_settings()

        if not settings.smtp_user or not settings.smtp_password:
            print(
                "ERROR: SMTP credentials not configured. Set SMTP_USER and SMTP_PASSWORD env vars."
            )
            return False

        async def _send():
            msg = EmailMessage()
            msg["From"] = settings.smtp_user
            msg["To"] = recipients
            msg["Subject"] = f"[CPR Alert] {len(signals)} signal(s) for {signals[0].trade_date}"

            # Build email body
            body_lines = [
                "<h2>CPR Pivot Lab — Signal Alert</h2>",
                f"<p><strong>Date:</strong> {signals[0].trade_date}</p>",
                f"<p><strong>Total Signals:</strong> {len(signals)}</p>",
                "<table border='1' cellpadding='8' style='border-collapse: collapse;'>",
                "<tr style='background: #f0f0f0;'>",
                "<th>Symbol</th><th>Condition</th><th>Details</th><th>Pivot</th><th>TC</th><th>BC</th>",
                "</tr>",
            ]

            for s in signals:
                row_color = (
                    "#e6fffa"
                    if "fail" in s.condition
                    else "#fff5f5"
                    if "gap-down" in s.condition
                    else "#fffff0"
                )
                body_lines.extend(
                    [
                        f"<tr style='background: {row_color};'>",
                        f"<td><strong>{s.symbol}</strong></td>",
                        f"<td>{s.condition}</td>",
                        f"<td>{s.details}</td>",
                        f"<td>{s.pivot:.2f if s.pivot else 'N/A'}</td>",
                        f"<td>{s.tc:.2f if s.tc else 'N/A'}</td>",
                        f"<td>{s.bc:.2f if s.bc else 'N/A'}</td>",
                        "</tr>",
                    ]
                )

            body_lines.extend(
                [
                    "</table>",
                    "<p><em>Sent by CPR Pivot Lab signal-alert</em></p>",
                ]
            )

            msg.set_content("\n".join(body_lines), subtype="html")

            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: aiosmtplib.send_message(
                    settings.smtp_host,
                    settings.smtp_port,
                    msg,
                    username=settings.smtp_user,
                    password=settings.smtp_password,
                    start_tls=True,
                ),
            )

        asyncio.run(_send())
        print(f"✓ Email sent to {recipients}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to send email: {e}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Check market conditions and send email alerts")
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated symbols to check (e.g., RELIANCE,TCS,SBIN)",
    )
    parser.add_argument(
        "--universe",
        type=str,
        help="Named universe from backtest_universe table (e.g., gold_51)",
    )
    parser.add_argument(
        "--condition",
        type=str,
        required=True,
        choices=[
            "narrow-cpr",
            "virgin-cpr",
            "orb-fail",
            "gap-up",
            "gap-down",
            "gap-both",
        ],
        help="Condition type to check",
    )
    parser.add_argument(
        "--recipients",
        type=str,
        help="Comma-separated email addresses (or set ALERT_TO_EMAIL env var)",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Trade date to check (YYYY-MM-DD, default: today)",
    )
    parser.add_argument(
        "--gap-threshold",
        type=float,
        default=1.5,
        help="Gap threshold in %% (default: 1.5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check conditions but don't send email",
    )

    args = parser.parse_args()

    # Get symbols
    symbols = []
    if args.universe:
        universe_name = _validate_universe_name(args.universe.strip())
        con = get_db().con
        universe_symbols = con.execute(
            "SELECT DISTINCT symbol FROM backtest_universe WHERE universe_name = ?",
            [universe_name],
        ).pl()
        if not universe_symbols.is_empty():
            symbols = universe_symbols["symbol"].to_list()
    elif args.symbols:
        symbols = [normalize_symbol(s) for s in args.symbols.split(",")]
    else:
        parser.error("Either --symbols or --universe is required")

    if not symbols:
        parser.error("No symbols found")

    # Parse date
    trade_date = None
    if args.date:
        trade_date = date.fromisoformat(args.date)

    # Check condition
    print(f"Checking {args.condition} for {len(symbols)} symbols...")

    signals = []
    if args.condition == "narrow-cpr":
        signals = check_narrow_cpr(symbols, trade_date)
    elif args.condition == "virgin-cpr":
        signals = check_virgin_cpr(symbols, trade_date)
    elif args.condition == "orb-fail":
        signals = check_orb_fail(symbols, trade_date)
    elif args.condition == "gap-up":
        signals = check_gap_signals(symbols, trade_date, args.gap_threshold, "up")
    elif args.condition == "gap-down":
        signals = check_gap_signals(symbols, trade_date, args.gap_threshold, "down")
    elif args.condition == "gap-both":
        signals = check_gap_signals(symbols, trade_date, args.gap_threshold, "both")

    if not signals:
        print("No signals found.")
        return

    # Print results
    print(f"\nFound {len(signals)} signal(s):")
    for s in signals:
        print(f"  • {s.symbol}: {s.condition} — {s.details}")

    # Send email
    if not args.dry_run:
        recipients = args.recipients
        if not recipients:
            from config.settings import get_settings

            settings = get_settings()
            recipients = settings.alert_to_email

        if recipients:
            send_email_alerts(signals, recipients)
        else:
            print(
                "WARNING: No recipients configured. Use --recipients or set ALERT_TO_EMAIL env var."
            )
    else:
        print("\n[Dry run — email not sent]")


if __name__ in {"__main__", "__mp_main__"}:
    main()
