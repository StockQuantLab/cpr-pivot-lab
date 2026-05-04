from __future__ import annotations

import pytest

from engine.kite_token import (
    KiteTokenWorkflowError,
    build_doppler_secret_command,
    extract_request_token,
)


def test_extract_request_token_accepts_raw_value() -> None:
    assert extract_request_token("abc123") == "abc123"


def test_extract_request_token_accepts_callback_url() -> None:
    callback_url = (
        "http://127.0.0.1:8004/auth/kite/callback?status=success&request_token=req-123&action=login"
    )
    assert extract_request_token(callback_url) == "req-123"


def test_extract_request_token_rejects_missing_token() -> None:
    with pytest.raises(KiteTokenWorkflowError):
        extract_request_token("http://127.0.0.1:8004/auth/kite/callback?status=success")


def test_extract_request_token_rejects_kite_finish_url_without_request_token() -> None:
    callback_url = "https://kite.zerodha.com/connect/finish?api_key=abc&sess_id=session-123"

    with pytest.raises(KiteTokenWorkflowError, match="finished without a request_token"):
        extract_request_token(callback_url)


def test_extract_request_token_rejects_kite_user_not_enabled_error() -> None:
    error_payload = (
        '{"status":"error","message":"The user is not enabled for the app.",'
        '"data":null,"error_type":"InputException"}'
    )

    with pytest.raises(KiteTokenWorkflowError, match="Zerodha rejected"):
        extract_request_token(error_payload)


def test_build_doppler_secret_command_escapes_single_quotes() -> None:
    command = build_doppler_secret_command("ab'cd")
    assert command == "doppler secrets set KITE_ACCESS_TOKEN 'ab''cd'"
