"""Paper session risk-limit helpers."""

from __future__ import annotations

from datetime import datetime
from datetime import time as dt_time
from typing import Any

from db.paper_db import PaperSession
from engine.paper_params import build_backtest_params


def _current_session_time(as_of: datetime) -> dt_time:
    return dt_time(as_of.hour, as_of.minute, as_of.second)


def _risk_limit_reasons(
    session: PaperSession | Any,
    as_of: datetime,
    net_pnl: float,
) -> list[str]:
    reasons: list[str] = []
    flatten_time = getattr(session, "flatten_time", None)
    if flatten_time is not None:
        if isinstance(flatten_time, str):
            parts = flatten_time.split(":")
            if len(parts) >= 2:
                try:
                    h, m = int(parts[0]), int(parts[1])
                    s = int(parts[2]) if len(parts) > 2 else 0
                    flatten_time = dt_time(h, m, s)
                except TypeError, ValueError:
                    flatten_time = None
            else:
                flatten_time = None
        if flatten_time is not None and _current_session_time(as_of) >= flatten_time:
            reasons.append(f"flatten_time:{flatten_time.isoformat()}")

    portfolio_value = float(build_backtest_params(session).portfolio_value)

    max_daily_loss_pct = float(getattr(session, "max_daily_loss_pct", 0.0) or 0.0)
    if max_daily_loss_pct > 0:
        loss_limit_amount = portfolio_value * max_daily_loss_pct
        if net_pnl <= (-loss_limit_amount):
            reasons.append(f"daily_loss_limit:{loss_limit_amount:.2f}")

    max_drawdown_pct = float(getattr(session, "max_drawdown_pct", 0.0) or 0.0)
    if max_drawdown_pct > 0:
        drawdown_limit_amount = portfolio_value * max_drawdown_pct
        if net_pnl <= (-drawdown_limit_amount):
            reasons.append(f"max_drawdown:{drawdown_limit_amount:.2f}")

    return reasons


__all__ = ["_current_session_time", "_risk_limit_reasons"]
