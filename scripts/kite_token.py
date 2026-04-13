from __future__ import annotations

import argparse
import json

from engine.cli_setup import configure_windows_stdio
from engine.kite_token import (
    KiteTokenWorkflowError,
    build_doppler_secret_command,
    exchange_kite_request_token,
    get_kite_client_from_env,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exchange a Kite request_token and optionally persist KITE_ACCESS_TOKEN to Doppler"
    )
    parser.add_argument(
        "--request-token",
        default=None,
        help="Raw Kite request_token or the full callback URL containing request_token=...",
    )
    parser.add_argument(
        "--apply-doppler",
        action="store_true",
        help="Write the new access token to Doppler automatically",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary at the end",
    )
    return parser


def _prompt_for_request_token(login_url: str) -> str:
    print("Open this Kite login URL in your browser:\n")
    print(login_url)
    print("\nAfter login, paste either the full redirected callback URL or just the request_token.")
    return input("Callback URL or request_token: ").strip()


def main() -> int:
    configure_windows_stdio(line_buffering=True, write_through=True)
    parser = build_parser()
    args = parser.parse_args()

    try:
        client = get_kite_client_from_env()
        login_url = client.login_url()
        request_token_input = args.request_token or _prompt_for_request_token(login_url)
        result = exchange_kite_request_token(
            request_token_input,
            apply_doppler=args.apply_doppler,
        )
    except KiteTokenWorkflowError as exc:
        raise SystemExit(str(exc)) from exc

    output = {
        "login_url": result.login_url,
        "user_id": result.user_id,
        "public_token": result.public_token,
        "doppler_updated": result.doppler_updated,
        "manual_doppler_command": build_doppler_secret_command(result.access_token),
    }

    if args.json:
        print(json.dumps(output, indent=2))
        return 0

    print("\nKite session exchange succeeded.")
    if result.user_id:
        print(f"User ID: {result.user_id}")

    if result.doppler_updated:
        print("Doppler updated: KITE_ACCESS_TOKEN")
    else:
        print("Run this to persist the token in Doppler:")
        print(output["manual_doppler_command"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
