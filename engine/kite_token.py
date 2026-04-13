from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from kiteconnect import KiteConnect


class KiteTokenWorkflowError(RuntimeError):
    """Raised when the local Kite token workflow cannot complete."""


@dataclass(slots=True)
class KiteTokenWorkflowResult:
    login_url: str
    request_token: str
    access_token: str
    public_token: str | None
    user_id: str | None
    doppler_updated: bool
    session_payload: dict[str, Any]


def extract_request_token(value: str) -> str:
    """Accept either a raw request_token or the full callback URL."""
    raw = value.strip()
    if not raw:
        raise KiteTokenWorkflowError("Request token input is empty")

    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        request_tokens = parse_qs(parsed.query).get("request_token", [])
        if not request_tokens or not request_tokens[0].strip():
            raise KiteTokenWorkflowError(
                "Callback URL does not contain request_token. Paste the full redirected URL."
            )
        return request_tokens[0].strip()

    return raw


def build_doppler_secret_command(access_token: str) -> str:
    """Render the Doppler command the user can run manually if needed."""
    token = access_token.replace("'", "''")
    return f"doppler secrets set KITE_ACCESS_TOKEN '{token}'"


def _decode_subprocess_output(raw: bytes | None) -> str:
    if not raw:
        return ""
    for encoding in ("utf-8", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise KiteTokenWorkflowError(
            f"Missing required environment variable: {name}. Run via Doppler with Kite secrets."
        )
    return value


def get_kite_client_from_env() -> KiteConnect:
    api_key = _require_env("KITE_API_KEY")
    api_secret = _require_env("KITE_API_SECRET")
    client = KiteConnect(api_key=api_key)
    client._cpr_kite_api_secret = api_secret
    return client


def persist_access_token_to_doppler(access_token: str) -> None:
    if shutil.which("doppler") is None:
        raise KiteTokenWorkflowError("Doppler CLI is not installed or not on PATH")

    try:
        subprocess.run(
            ["doppler", "secrets", "set", "KITE_ACCESS_TOKEN", access_token],
            check=True,
            capture_output=True,
            text=False,
        )
    except subprocess.CalledProcessError as exc:
        stderr = _decode_subprocess_output(exc.stderr).strip()
        stdout = _decode_subprocess_output(exc.stdout).strip()
        detail = stderr or stdout or str(exc)
        raise KiteTokenWorkflowError(f"Failed to update Doppler: {detail}") from exc


def exchange_kite_request_token(
    request_token_input: str,
    *,
    apply_doppler: bool = False,
) -> KiteTokenWorkflowResult:
    request_token = extract_request_token(request_token_input)
    client = get_kite_client_from_env()
    login_url = client.login_url()
    api_secret = getattr(client, "_cpr_kite_api_secret", None)
    if not api_secret:
        raise KiteTokenWorkflowError("KITE_API_SECRET is required to generate a Kite session")

    try:
        payload = client.generate_session(request_token, api_secret=api_secret)
    except Exception as exc:
        raise KiteTokenWorkflowError(str(exc)) from exc

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise KiteTokenWorkflowError("Kite did not return an access_token")

    if apply_doppler:
        persist_access_token_to_doppler(access_token)

    return KiteTokenWorkflowResult(
        login_url=login_url,
        request_token=request_token,
        access_token=access_token,
        public_token=str(payload.get("public_token") or "").strip() or None,
        user_id=str(payload.get("user_id") or "").strip() or None,
        doppler_updated=apply_doppler,
        session_payload=payload,
    )
