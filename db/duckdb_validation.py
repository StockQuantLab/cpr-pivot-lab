"""Shared validation helpers for DuckDB-backed market data code."""

from __future__ import annotations

import re

from engine.constants import parse_iso_date

# Symbol name validation: NSE symbols are uppercase letters, digits, spaces, &, ., and -.
_SYMBOL_RE = re.compile(r"^[A-Z0-9& .-]{1,32}$")
_UNIVERSE_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_symbols(symbols: list[str]) -> list[str]:
    """Validate symbol names against a strict regex to prevent SQL injection."""
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise ValueError(f"Invalid symbol name type: {type(symbol)!r}")
        if not _SYMBOL_RE.match(symbol):
            raise ValueError(f"Invalid symbol name: '{symbol}'. Must match {_SYMBOL_RE.pattern}")
        if any(token in symbol for token in ("'", '"', "\\", ";", "--", "/*", "*/")):
            raise ValueError(f"Invalid symbol name: '{symbol}'. Contains forbidden characters")
    return symbols


def validate_universe_name(name: str) -> str:
    """Validate saved universe names used in metadata table."""
    if not _UNIVERSE_RE.match(name):
        raise ValueError(f"Invalid universe name: '{name}'. Must match {_UNIVERSE_RE.pattern}")
    return name


def validate_table_identifier(table: str) -> str:
    """Validate internal table names before interpolating them into SQL."""
    if not _SQL_IDENTIFIER_RE.fullmatch(table):
        raise ValueError(f"Invalid SQL table identifier: {table!r}")
    return table


def date_window_clause(
    column: str,
    since_date: str | None = None,
    until_date: str | None = None,
) -> str:
    """Return an AND clause for an inclusive date window."""
    clauses: list[str] = []
    if since_date:
        clauses.append(f"{column} >= '{since_date}'::DATE")
    if until_date:
        clauses.append(f"{column} <= '{until_date}'::DATE")
    return f"AND {' AND '.join(clauses)}" if clauses else ""


def prepare_date_window(
    since_date: str | None,
    until_date: str | None,
) -> tuple[str | None, str | None]:
    """Normalize and return (since_iso, until_iso) for incremental builds."""
    return (
        parse_iso_date(since_date) if since_date else None,
        parse_iso_date(until_date) if until_date else None,
    )
