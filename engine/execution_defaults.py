"""Shared execution-sizing defaults for backtest, replay, and live."""

DEFAULT_PORTFOLIO_VALUE = 1_000_000.0
DEFAULT_POSITION_CAPITAL = 200_000.0
DEFAULT_RISK_PCT = 0.01
DEFAULT_MAX_POSITIONS = 5
DEFAULT_MAX_POSITION_PCT = 0.20

DEFAULT_EXECUTION_SIZING = {
    "capital": DEFAULT_POSITION_CAPITAL,
    "risk_pct": DEFAULT_RISK_PCT,
    "portfolio_value": DEFAULT_PORTFOLIO_VALUE,
    "max_positions": DEFAULT_MAX_POSITIONS,
    "max_position_pct": DEFAULT_MAX_POSITION_PCT,
}
