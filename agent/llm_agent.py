#!/usr/bin/env python3
"""
CPR Pivot Lab — AI Agent  (Phidata + Ollama)

Understands natural language queries about CPR-ATR strategy backtests,
stock performance, parameter optimization, and data availability.

Usage:
    doppler run -- uv run pivot-agent
    doppler run -- uv run pivot-agent -q "Run RELIANCE backtest for 2023"
    doppler run -- uv run pivot-agent -q "Which stocks had best win rate 2022-2024?"
"""

from __future__ import annotations

import argparse
import json
from functools import wraps
from typing import Any

from engine.cli_setup import configure_windows_stdio

try:
    from phi.agent.agent import Agent
except ImportError:
    from phi.agent import Agent

from phi.storage.agent.postgres import PgAgentStorage

from agent.llm.ollama_provider import create_ollama_model
from agent.tools.backtest_tools import (
    get_available_symbols,
    get_backtest_summary,
    get_cpr_for_date,
    get_data_status,
    get_paper_ledger,
    get_paper_positions,
    get_paper_session_summary,
    list_paper_sessions,
    paper_send_command,
    rebuild_indicators,
    run_backtest,
    run_multi_stock_backtest,
)
from config.settings import get_settings

configure_windows_stdio()


def _json_tool(func):
    """Wrap tool to return JSON string — Phidata requires string returns."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return (
            json.dumps(result, default=str, indent=2) if isinstance(result, dict) else str(result)
        )

    return wrapper


SYSTEM_PROMPT = """You are the CPR Pivot Lab AI analyst — expert in CPR-ATR trading strategy for NSE stocks.

Architecture:
- Market data: Parquet files (2100+ NSE symbols, ~10 years, 5-min OHLCV) queried via DuckDB
- PostgreSQL: agent chat sessions and signals only (paper trading state is in DuckDB)
- DuckDB: historical backtests, archived paper ledgers, and live paper trading state

You can:
1. Run backtests: "Run SBIN backtest 2022-2024" or "Test all stocks 2023"
2. Compare performance: "Which 5 stocks had highest win rate in 2023?"
3. Inspect indicators: "Show CPR levels for TCS on 2023-06-15"
4. Check data: "What symbols are available? What date ranges?"
5. Rebuild tables: "Rebuild CPR/ATR tables" (after new data import)
6. Inspect paper sessions: "List paper sessions" or "Show live paper summary for session X"

CPR-ATR Strategy:
- CPR = Central Pivot Range from previous day OHLC
- Entry after 9:15-9:20 AM observation if CPR is narrow (< P50 historical width)
- LONG if 9:20 close > TC; SHORT if 9:20 close < BC
- Entry on breakout of Opening Range with RVOL > 1.0
- TrailingStop: PROTECT → BREAKEVEN → TRAIL (4 phases)
- R:R = 1:2 default

Be concise. Lead with key metrics (win rate, P/L). Use ₹ for rupees."""


def create_agent(session_id: str | None = None) -> Agent:
    settings = get_settings()

    tools = [
        _json_tool(run_backtest),
        _json_tool(run_multi_stock_backtest),
        _json_tool(get_backtest_summary),
        _json_tool(get_available_symbols),
        _json_tool(get_cpr_for_date),
        _json_tool(get_data_status),
        _json_tool(rebuild_indicators),
        _json_tool(list_paper_sessions),
        _json_tool(get_paper_session_summary),
        _json_tool(get_paper_positions),
        _json_tool(get_paper_ledger),
        _json_tool(paper_send_command),
    ]

    storage = PgAgentStorage(
        table_name="agent_sessions",
        schema="cpr_pivot",
        db_url=settings.get_pg_sync_url(mask_password=False),  # Phidata needs real sync DSN
    )

    agent_kwargs: dict[str, Any] = {
        "model": create_ollama_model(),
        "tools": tools,
        "storage": storage,
        "session_id": session_id,
        "description": SYSTEM_PROMPT,
        "show_tool_calls": True,
        "markdown": True,
    }
    return Agent(**agent_kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description="CPR Pivot Lab AI Agent")
    parser.add_argument("-q", "--query", help="Single query (non-interactive)")
    parser.add_argument("--session", help="Resume session by ID")
    args = parser.parse_args()

    agent = create_agent(session_id=args.session)

    if args.query:
        agent.print_response(args.query)
    else:
        print("CPR Pivot Lab Agent — type your query (Ctrl+C to exit)\n")
        try:
            agent.cli_app(markdown=True)
        except KeyboardInterrupt:
            print("\nBye!")


if __name__ in {"__main__", "__mp_main__"}:
    main()
