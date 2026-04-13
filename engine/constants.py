"""Shared constants for CPR Pivot Lab.

Defines all stringly-typed values as Literals for type safety and DRY.
Use these instead of raw strings throughout the codebase.
"""

from __future__ import annotations

import re
from typing import Literal, TypeVar, cast

# ---------------------------------------------------------------------------
# Time constants
# ---------------------------------------------------------------------------
MARKET_OPEN = "09:15"
TIME_EXIT_DEFAULT = "15:15"
VCPR_SCAN_END_DEFAULT = "12:30"
ENTRY_WINDOW_END_DEFAULT = "10:15"


def _time_to_minutes(time_str: str) -> int:
    """Convert HH:MM into minutes since midnight with strict validation."""
    parts = str(time_str).split(":")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError(f"Invalid time format: {time_str!r}. Expected HH:MM")
    hours, minutes = int(parts[0]), int(parts[1])
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        raise ValueError(f"Invalid time value: {time_str!r}")
    return hours * 60 + minutes


# Time constants in minutes since midnight
MARKET_OPEN_MINUTES = _time_to_minutes(MARKET_OPEN)
TIME_EXIT_MINUTES = _time_to_minutes(TIME_EXIT_DEFAULT)

# ---------------------------------------------------------------------------
# Period constants
# ---------------------------------------------------------------------------
ATR_PERIODS_DEFAULT = 12  # Last 12 × 5-min candles of prev day = 1 ATR hour
CPR_ROLLING_WINDOW = 252  # Trading days in a year (for CPR width percentile)
VCPR_CONFIRM_CANDLES_DEFAULT = 2  # Confirmation candles for Virgin CPR
FBR_FAILURE_WINDOW_DEFAULT = 8  # Candles to detect FBR failure

# ---------------------------------------------------------------------------
# Calculation constants
# ---------------------------------------------------------------------------
BUFFER_PCT_DEFAULT = 0.0005  # 0.05% buffer above TC (LONG) or below BC (SHORT)
RVOL_THRESHOLD_DEFAULT = 1.2  # Minimum relative volume on entry candle (above-average)
RR_RATIO_DEFAULT = 2.0  # Risk-reward ratio for FBR
MIN_SL_ATR_RATIO_DEFAULT = 0.5  # Minimum SL distance as ATR multiple
MAX_SL_ATR_RATIO_DEFAULT = 2.0  # Maximum SL distance as ATR multiple
CPR_MAX_WIDTH_PCT_DEFAULT = 2.0  # Hard cap: skip days wider than this
VCPR_MIN_OPEN_DIST_ATR_DEFAULT = 0.2  # Min open-to-CPR distance for VCPR
FBR_MIN_OR_ATR_DEFAULT = 0.3  # Minimum OR/ATR for FBR

# ---------------------------------------------------------------------------
# Strategy types
# ---------------------------------------------------------------------------
Strategy = Literal["CPR_LEVELS", "FBR", "VIRGIN_CPR"]
# Public CLI surfaces intentionally exclude research-only VIRGIN_CPR.
PUBLIC_STRATEGIES: tuple[Strategy, ...] = ("CPR_LEVELS", "FBR")
STRATEGIES: tuple[Strategy, ...] = ("CPR_LEVELS", "FBR", "VIRGIN_CPR")

# Strategy display names
STRATEGY_LABELS: dict[Strategy, str] = {
    "CPR_LEVELS": "CPR",
    "FBR": "FBR",
    "VIRGIN_CPR": "VCPR",
}

# ---------------------------------------------------------------------------
# Direction filters
# ---------------------------------------------------------------------------
Direction = Literal["LONG", "SHORT", "BOTH"]
DIRECTIONS: tuple[Direction, ...] = ("LONG", "SHORT", "BOTH")

# ---------------------------------------------------------------------------
# CPR shift filters
# ---------------------------------------------------------------------------
CPRShift = Literal["ALL", "HIGHER", "LOWER", "OVERLAP"]
CPR_SHIFTS: tuple[CPRShift, ...] = ("ALL", "HIGHER", "LOWER", "OVERLAP")

# ---------------------------------------------------------------------------
# Exit reasons
# ---------------------------------------------------------------------------
ExitReason = Literal[
    "TARGET",
    "INITIAL_SL",
    "BREAKEVEN_SL",
    "TRAILING_SL",
    "TIME",
    "REVERSAL",
    "CANDLE_EXIT",
]
EXIT_REASONS: tuple[ExitReason, ...] = (
    "TARGET",
    "INITIAL_SL",
    "BREAKEVEN_SL",
    "TRAILING_SL",
    "TIME",
    "REVERSAL",
    "CANDLE_EXIT",
)

# Exit reason display labels (short form)
EXIT_LABELS: dict[ExitReason, str] = {
    "TARGET": "Target",
    "INITIAL_SL": "SL",
    "BREAKEVEN_SL": "BE-SL",
    "TRAILING_SL": "T-SL",
    "TIME": "Time",
    "REVERSAL": "Reversal",
    "CANDLE_EXIT": "Candle",
}

# ---------------------------------------------------------------------------
# SL phases (for TrailingStop)
# ---------------------------------------------------------------------------
SLPhase = Literal["PROTECT", "BREAKEVEN", "TRAIL", "DONE"]
SL_PHASE_TO_EXIT_REASON: dict[SLPhase, ExitReason] = {
    "PROTECT": "INITIAL_SL",
    "BREAKEVEN": "BREAKEVEN_SL",
    "TRAIL": "TRAILING_SL",
}

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


_LiteralStr = TypeVar("_LiteralStr", bound=str)


def _validate_literal(value: str, allowed: tuple[_LiteralStr, ...], type_name: str) -> _LiteralStr:  # noqa: UP047
    """
    Generic validator for literal types.

    Args:
        value: Value to validate
        allowed: Tuple of allowed values
        type_name: Human-readable type name for error message

    Returns:
        The validated value (as a Literal type)

    Raises:
        ValueError: If value is not in allowed tuple
    """
    if value not in allowed:
        raise ValueError(f"Invalid {type_name}: {value!r}. Must be one of {allowed}")
    return cast(_LiteralStr, value)


def validate_strategy(value: str) -> Strategy:
    """Validate and return a strategy literal."""
    return _validate_literal(value, STRATEGIES, "strategy")


def validate_direction(value: str) -> Direction:
    """Validate and return a direction literal."""
    return _validate_literal(value, DIRECTIONS, "direction")


def validate_cpr_shift(value: str) -> CPRShift:
    """Validate and return a CPR shift literal."""
    return _validate_literal(value, CPR_SHIFTS, "CPR shift")


def validate_exit_reason(value: str) -> ExitReason:
    """Validate and return an exit reason literal."""
    return _validate_literal(value, EXIT_REASONS, "exit reason")


# ---------------------------------------------------------------------------
# Utility: normalize symbol to uppercase
# ---------------------------------------------------------------------------
def normalize_symbol(symbol: str) -> str:
    """Normalize symbol name to uppercase with validation."""
    symbol = symbol.strip().upper()
    if not re.match(r"^[A-Z0-9& .-]{1,32}$", symbol):
        raise ValueError(f"Invalid symbol name: {symbol!r}")
    if any(token in symbol for token in ("'", '"', "\\", ";", "--", "/*", "*/")):
        raise ValueError(f"Invalid symbol name: {symbol!r}")
    return symbol


# ---------------------------------------------------------------------------
# Utility: format list preview string
# ---------------------------------------------------------------------------
def preview_list(items: list[str], limit: int = 10) -> str:
    """Return a compact comma-separated preview of a list.

    Args:
        items: List of strings to format
        limit: Maximum items to show before truncating with "..."

    Returns:
        Formatted string like "item1, item2, item3, ..." or "-" for empty lists
    """
    if not items:
        return "-"
    preview = ", ".join(items[:limit])
    if len(items) > limit:
        return f"{preview}, ..."
    return preview


# ---------------------------------------------------------------------------
# Utility: parse and validate ISO date (YYYY-MM-DD)
# ---------------------------------------------------------------------------
def parse_iso_date(date_str: str) -> str:
    """Validate YYYY-MM-DD and return normalized ISO date string.

    This is the shared date parsing utility for the entire codebase.
    All date parsing should use this function for consistency.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Normalized ISO date string

    Raises:
        ValueError: If date_str is not a valid YYYY-MM-DD date
    """
    from datetime import date

    try:
        return date.fromisoformat(str(date_str).strip()).isoformat()
    except ValueError as e:
        raise ValueError(f"Invalid date '{date_str}'. Expected YYYY-MM-DD.") from e


# ---------------------------------------------------------------------------
# Utility: format symbol list for SQL IN clause
# ---------------------------------------------------------------------------
def sql_symbol_list(symbols: list[str], validate: bool = True) -> str:
    """Format a list of symbols for SQL IN clause.

    Args:
        symbols: List of symbol names
        validate: If True, validate each symbol matches the expected pattern

    Returns:
        SQL-formatted string like "'SBIN','TCS','RELIANCE'"

    Raises:
        ValueError: If symbols are invalid and validate=True
    """
    if not symbols:
        return ""

    if validate:
        pattern = r"^[A-Z0-9& .-]{1,32}$"

        safe = [
            s
            for s in symbols
            if re.match(pattern, s)
            and not any(token in s for token in ("'", '"', "\\", ";", "--", "/*", "*/"))
        ]
        if len(safe) != len(symbols):
            invalid = [s for s in symbols if s not in safe]
            raise ValueError(f"Invalid symbol name(s): {invalid}. Must match {pattern}")
        symbols = safe

    return ", ".join(f"'{s}'" for s in symbols)


def sql_symbol_params(symbols: list[str], validate: bool = True) -> tuple[str, list[str]]:
    """Build parameter placeholders and params for a SQL IN clause."""
    if not symbols:
        return "", []
    if validate:
        symbols = [normalize_symbol(s) for s in symbols]
    return ", ".join("?" for _ in symbols), list(symbols)
