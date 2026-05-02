"""Admin command queue helpers for paper/live sessions."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from engine.constants import normalize_symbol

_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-:.]+$")
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ADMIN_ACTIONS = {
    "close_positions",
    "close_all",
    "set_risk_budget",
    "pause_entries",
    "resume_entries",
    "cancel_pending_intents",
}
_ADMIN_TEXT_MAX_LEN = 80
_ADMIN_MAX_PORTFOLIO_VALUE = 10_000_000.0
_ADMIN_MAX_POSITIONS = 50
_ADMIN_MAX_POSITION_PCT = 1.0


def write_admin_command(
    session_id: str,
    action: str,
    *,
    symbols: list[str] | None = None,
    portfolio_value: float | None = None,
    max_positions: int | None = None,
    max_position_pct: float | None = None,
    reason: str = "manual",
    requester: str = "unknown",
) -> str:
    """Write a validated command to the session's admin queue."""

    if not _SESSION_ID_PATTERN.fullmatch(str(session_id)):
        raise ValueError(
            "session_id contains unsupported characters; allowed: letters, numbers, _, -, :, ."
        )
    action = str(action or "").strip()
    if action not in _ADMIN_ACTIONS:
        raise ValueError(f"action must be one of: {', '.join(sorted(_ADMIN_ACTIONS))}")

    clean_symbols = [normalize_symbol(str(s)) for s in (symbols or []) if str(s).strip()]
    if action == "close_positions" and not clean_symbols:
        raise ValueError("close_positions requires at least one symbol")
    if action != "close_positions" and clean_symbols:
        raise ValueError("symbols are only valid for close_positions")

    if portfolio_value is not None:
        portfolio_value = float(portfolio_value)
        if not 0 < portfolio_value <= _ADMIN_MAX_PORTFOLIO_VALUE:
            raise ValueError(f"portfolio_value must be > 0 and <= {_ADMIN_MAX_PORTFOLIO_VALUE:g}")
    if max_positions is not None:
        max_positions = int(max_positions)
        if not 1 <= max_positions <= _ADMIN_MAX_POSITIONS:
            raise ValueError(f"max_positions must be between 1 and {_ADMIN_MAX_POSITIONS}")
    if max_position_pct is not None:
        max_position_pct = float(max_position_pct)
        if not 0 < max_position_pct <= _ADMIN_MAX_POSITION_PCT:
            raise ValueError(f"max_position_pct must be > 0 and <= {_ADMIN_MAX_POSITION_PCT:g}")
    if action == "set_risk_budget" and all(
        value is None for value in (portfolio_value, max_positions, max_position_pct)
    ):
        raise ValueError("set_risk_budget requires at least one budget field")

    cmd_dir = Path(".tmp_logs") / f"cmd_{session_id}"
    cmd_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    cmd_file = cmd_dir / f"{ts}_{action}.json"
    cmd: dict[str, Any] = {
        "action": action,
        "reason": _clean_admin_text(reason),
        "requester": _clean_admin_text(requester),
    }
    if clean_symbols:
        cmd["symbols"] = clean_symbols
    if portfolio_value is not None:
        cmd["portfolio_value"] = float(portfolio_value)
    if max_positions is not None:
        cmd["max_positions"] = int(max_positions)
    if max_position_pct is not None:
        cmd["max_position_pct"] = float(max_position_pct)
    cmd_file.write_text(json.dumps(cmd))
    return str(cmd_file)


def _clean_admin_text(value: object) -> str:
    text = _CONTROL_CHARS.sub("", str(value or "")).strip()
    return text[:_ADMIN_TEXT_MAX_LEN] or "unknown"


__all__ = ["write_admin_command"]
