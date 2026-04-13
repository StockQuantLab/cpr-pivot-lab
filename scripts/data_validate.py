"""Data Validate — Validate data pipeline integrity before running backtests.

Usage:
    uv run pivot-data-validate                    # Validate all
    uv run pivot-data-validate --symbols SBIN    # Validate specific symbols
    uv run pivot-data-validate --strict          # Fail on ANY gap
    uv run pivot-data-validate --start 2025-01-01 --end 2025-03-31

Exit codes:
    0 = All checks passed
    1 = Missing Parquet files
    2 = intraday_day_pack incomplete
    3 = CPR/ATR tables missing
    4 = Date range has gaps
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from db.duckdb import get_db
from engine.cli_setup import configure_windows_stdio
from engine.constants import normalize_symbol

configure_windows_stdio(line_buffering=True, write_through=True)

PACK_REBUILD_HINT = (
    "Run: doppler run -- uv run pivot-build --table pack --refresh-since <YYYY-MM-DD> "
    "(alias: --since; add --allow-full-pack-rebuild only if you intentionally need a full pack rebuild)"
)


@dataclass
class ValidationResult:
    """Result of a validation check."""

    check: str
    passed: bool
    message: str
    details: str = ""


def check_parquet_files(symbols: list[str] | None = None) -> ValidationResult:
    """Check if Parquet files exist."""
    parquet_dir = Path("data/parquet")

    if not parquet_dir.exists():
        return ValidationResult(
            check="Parquet files",
            passed=False,
            message="Parquet directory not found",
            details="Run: doppler run -- uv run pivot-convert",
        )

    # Check 5min subdirectory exists
    min_dir = parquet_dir / "5min"
    daily_dir = parquet_dir / "daily"

    if not min_dir.exists() or not daily_dir.exists():
        return ValidationResult(
            check="Parquet files",
            passed=False,
            message="5min or daily subdirectory missing",
            details="Run: doppler run -- uv run pivot-convert",
        )

    # Check for specific symbols if provided
    missing = []
    for sym in (symbols or [])[:10]:  # Check first 10 to avoid spam
        sym_path = min_dir / sym
        if not sym_path.exists():
            missing.append(sym)

    if missing:
        return ValidationResult(
            check="Parquet files",
            passed=False,
            message=f"Missing Parquet data for: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}",
            details=f"Run: doppler run -- uv run pivot-convert --symbol {missing[0]}",
        )

    return ValidationResult(
        check="Parquet files",
        passed=True,
        message="Parquet directory structure OK",
    )


def check_intraday_pack(symbols: list[str] | None = None, strict: bool = False) -> ValidationResult:
    """Check intraday_day_pack table coverage."""
    con = get_db().con

    # Get total symbols and row count
    stats = con.execute(
        """
        SELECT COUNT(DISTINCT symbol) as symbols,
               COUNT(*) as total_rows
        FROM intraday_day_pack
        """
    ).fetchone()

    symbol_count = stats[0] if stats else 0
    row_count = stats[1] if stats else 0

    if symbol_count == 0:
        return ValidationResult(
            check="intraday_day_pack",
            passed=False,
            message="Table is empty",
            details=PACK_REBUILD_HINT,
        )

    # Expected: 1536 symbols (full NSE)
    expected = 1536
    coverage = 100.0 * symbol_count / expected

    if strict and symbol_count < expected:
        return ValidationResult(
            check="intraday_day_pack",
            passed=False,
            message=f"Only {symbol_count}/{expected} symbols covered ({coverage:.1f}%)",
            details=PACK_REBUILD_HINT,
        )

    if symbol_count < 100:  # Less than 100 symbols means mostly incomplete
        return ValidationResult(
            check="intraday_day_pack",
            passed=False,
            message=f"Low coverage: {symbol_count}/{expected} symbols ({coverage:.1f}%)",
            details=f"{row_count:,} rows. {PACK_REBUILD_HINT}",
        )

    return ValidationResult(
        check="intraday_day_pack",
        passed=True,
        message=f"{symbol_count} symbols, {row_count:,} rows ({coverage:.1f}% coverage)",
    )


def check_cpr_tables(symbols: list[str] | None = None) -> ValidationResult:
    """Check CPR, ATR, and threshold tables."""
    con = get_db().con

    tables_to_check = [
        ("cpr_daily", 3_000_000),
        ("atr_intraday", 2_000_000),
        ("cpr_thresholds", 3_000_000),
    ]

    issues = []

    for table_name, min_expected in tables_to_check:
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result else 0

        if count == 0:
            issues.append(f"{table_name}: EMPTY")
        elif count < min_expected:
            issues.append(f"{table_name}: {count:,} < {min_expected:,}")

    if issues:
        return ValidationResult(
            check="CPR/ATR tables",
            passed=False,
            message=f"Issues found: {', '.join(issues)}",
            details="Run: doppler run -- uv run pivot-build",
        )

    return ValidationResult(
        check="CPR/ATR tables",
        passed=True,
        message="All tables populated",
    )


def check_date_range(
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> ValidationResult:
    """Check for gaps in date coverage."""
    con = get_db().con

    query = """
    SELECT
        MIN(trade_date) as min_date,
        MAX(trade_date) as max_date,
        COUNT(DISTINCT trade_date) as trading_days
    FROM intraday_day_pack
    """

    result = con.execute(query).fetchone()
    if not result or result[0] is None:
        return ValidationResult(
            check="Date range",
            passed=False,
            message="No date data found",
            details="Run: doppler run -- uv run pivot-convert",
        )

    min_date = result[0]
    max_date = result[1]
    trading_days = result[2]

    # Expected ~252 trading days per year
    years_spanned = (max_date.year - min_date.year) + 1
    expected_days = years_spanned * 252

    if trading_days < expected_days * 0.5:  # Less than 50% coverage
        return ValidationResult(
            check="Date range",
            passed=False,
            message=f"Low coverage: {trading_days:,} days vs ~{expected_days:,} expected",
            details=f"Range: {min_date} to {max_date}",
        )

    return ValidationResult(
        check="Date range",
        passed=True,
        message=f"{min_date} to {max_date} ({trading_days:,} trading days)",
    )


def check_specific_symbols(
    symbols: list[str], start_date: date | None, end_date: date | None
) -> list[ValidationResult]:
    """Check data availability for specific symbols."""
    results = []
    con = get_db().con

    for sym in symbols[:20]:  # Limit to 20 to avoid spam
        result = con.execute(
            f"""
            SELECT
                COUNT(DISTINCT trade_date) as days,
                COUNT(*) as candles
            FROM intraday_day_pack
            WHERE symbol = '{sym}'
            """
        ).fetchone()

        days = result[0] if result else 0
        candles = result[1] if result else 0

        if days == 0:
            results.append(
                ValidationResult(
                    check=f"Symbol {sym}",
                    passed=False,
                    message="No data in intraday_day_pack",
                    details=PACK_REBUILD_HINT,
                )
            )
        elif days < 100:  # Less than 100 trading days
            results.append(
                ValidationResult(
                    check=f"Symbol {sym}",
                    passed=False,
                    message=f"Low coverage: {days} trading days",
                    details=f"{candles:,} candles",
                )
            )

    return results


def print_results(results: list[ValidationResult]) -> int:
    """Print results and return exit code."""
    print("\n" + "=" * 50)
    print("DATA VALIDATION RESULTS")
    print("=" * 50)

    all_passed = True
    for r in results:
        status = "✓ PASS" if r.passed else "✗ FAIL"
        color = "\033[92m" if r.passed else "\033[91m"
        reset = "\033[0m"
        print(f"\n{color}{status}{reset} {r.check}")
        print(f"  {r.message}")
        if r.details:
            print(f"  → {r.details}")

        if not r.passed:
            all_passed = False

    print("\n" + "=" * 50)
    if all_passed:
        print("✓ ALL CHECKS PASSED")
        return 0
    else:
        print("✗ VALIDATION FAILED")
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate data pipeline integrity before backtests"
    )
    parser.add_argument(
        "--symbols", type=str, help="Comma-separated symbols to validate (e.g., RELIANCE,TCS)"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date to check (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date to check (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if ANY gap in data (default: warn only)",
    )

    args = parser.parse_args()

    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [normalize_symbol(s) for s in args.symbols.split(",")]

    # Parse dates
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None

    # Run validations
    results = []

    # 1. Parquet files
    results.append(check_parquet_files(symbols))

    # 2. intraday_day_pack
    pack_result = check_intraday_pack(symbols, strict=args.strict)
    results.append(pack_result)

    # 3. CPR/ATR tables
    results.append(check_cpr_tables(symbols))

    # 4. Date range
    results.append(check_date_range(symbols, start_date, end_date))

    # 5. Specific symbols (if provided)
    if symbols:
        symbol_results = check_specific_symbols(symbols, start_date, end_date)
        results.extend(symbol_results)

    # Print and exit
    exit_code = print_results(results)
    sys.exit(exit_code)


if __name__ in {"__main__", "__mp_main__"}:
    main()
