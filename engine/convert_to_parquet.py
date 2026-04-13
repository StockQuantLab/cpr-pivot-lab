"""
CSV → Parquet Converter for Zerodha OHLCV data.

Run ONCE after placing Zerodha CSV files in data/raw/.

Input layout expected:
    data/raw/5min/Part 1/0001_RELIANCE.csv   (Parts 1-4, each ~490 stocks)
    data/raw/5min/Part 2/0001_RELIANCE.csv   (same symbols, different date ranges)
    data/raw/daily/0001_RELIANCE.csv         (full 10-year daily data)

Output layout:
    data/parquet/5min/RELIANCE/2015.parquet
    data/parquet/5min/RELIANCE/2016.parquet  ... (one file per year per symbol)
    data/parquet/daily/RELIANCE/all.parquet  (single file for daily data)

Usage:
    uv run pivot-convert                             # convert everything
    uv run pivot-convert --symbol RELIANCE           # single stock
    uv run pivot-convert --overwrite                 # overwrite existing Parquet
    uv run pivot-convert --daily-only                # only rebuild daily Parquet
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

from engine.constants import normalize_symbol

# ---------------------------------------------------------------------------
# Column mapping — handle Zerodha header variants
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "date": "candle_time",
    "Date": "candle_time",
    "datetime": "candle_time",
    "Datetime": "candle_time",
    "open": "open",
    "Open": "open",
    "high": "high",
    "High": "high",
    "low": "low",
    "Low": "low",
    "close": "close",
    "Close": "close",
    "volume": "volume",
    "Volume": "volume",
}

REQUIRED_COLS = {"candle_time", "open", "high", "low", "close", "volume"}


def detect_symbol(csv_path: Path) -> str:
    """
    Extract clean symbol name from Zerodha file naming conventions.

    Examples:
        0001_RELIANCE.csv  → RELIANCE   (index-prefixed format)
        SBIN.csv           → SBIN       (plain format)
        SBIN_part1.csv     → SBIN       (part-suffixed format)
    """
    stem = csv_path.stem.upper()
    parts = stem.split("_", 1)  # split on FIRST underscore only

    if len(parts) == 2 and parts[0].isdigit():
        # "0001_RELIANCE" → take everything after the number prefix
        return parts[1]

    # "SBIN" or "SBIN_part1" → take part before first underscore
    return parts[0]


def read_zerodha_csv(csv_path: Path) -> pl.DataFrame:
    """
    Parse a Zerodha OHLCV CSV file into a clean Polars DataFrame.

    Zerodha 5-min format:
        Date,Open,High,Low,Close,Volume
        2015-04-01T09:15:00+0530,194.5,194.5,193.05,194.2,474840

    Zerodha daily format:
        Date,Open,High,Low,Close,Volume
        2015-04-01T00:00:00+0530,196.6,199.9,193.75,199.2,13993680
    """
    try:
        df = pl.read_csv(csv_path, infer_schema_length=1000)
    except Exception as e:
        print(f"  ERROR reading {csv_path.name}: {e}")
        return pl.DataFrame()

    if df.is_empty():
        return df

    # Normalize column names
    rename = {c: COLUMN_MAP[c] for c in df.columns if c in COLUMN_MAP}
    df = df.rename(rename)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        print(f"  WARNING: {csv_path.name} missing columns: {missing} — skipping")
        return pl.DataFrame()

    # Parse datetime as IST (timezone-naive).
    # Zerodha CSVs use +0530 offset. Since all NSE data is IST and India has no DST,
    # we strip the timezone and store as naive datetime (IST implied). This avoids
    # UTC conversion bugs where midnight IST dates shift back one calendar day.
    df = df.with_columns(
        pl.col("candle_time")
        .str.slice(0, 19)
        .str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M:%S", strict=False)
        .alias("candle_time")
    )

    df = df.with_columns(
        [
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64, strict=False),
        ]
    )

    df = df.filter(
        pl.col("candle_time").is_not_null()
        & (pl.col("close") > 0)
        & pl.col("volume").is_not_null()
        & (pl.col("volume") < 1e15)
    )
    return df


# ---------------------------------------------------------------------------
# 5-minute conversion
# ---------------------------------------------------------------------------


def find_5min_files(base_dir: Path, symbol_filter: str | None = None) -> dict[str, list[Path]]:
    """
    Recursively find all 5-min CSVs and group by symbol.

    Handles Part 1 / Part 2 / Part 3 / Part 4 subdirectory structure.
    Returns: {"RELIANCE": [path_part1, path_part2, ...], ...}
    """
    groups: dict[str, list[Path]] = defaultdict(list)

    five_min_dir = base_dir / "5min"
    if not five_min_dir.exists():
        print(f"WARNING: {five_min_dir} not found")
        return {}

    symbol_filter_norm = normalize_symbol(symbol_filter) if symbol_filter else None
    for csv_file in sorted(five_min_dir.rglob("*.csv")):
        symbol = detect_symbol(csv_file)
        if symbol_filter_norm and symbol != symbol_filter_norm:
            continue
        groups[symbol].append(csv_file)

    return dict(groups)


def convert_5min_symbol(
    symbol: str, csv_files: list[Path], output_dir: Path, overwrite: bool = False
) -> dict:
    """Convert all CSV parts for one symbol → one Parquet file per year."""
    print(f"\n[{symbol}] Reading {len(csv_files)} CSV file(s)...")

    frames = []
    for path in sorted(csv_files):
        df = read_zerodha_csv(path)
        if not df.is_empty():
            frames.append(df)
            print(f"  {path.parent.name}/{path.name}: {len(df):,} rows")

    if not frames:
        print("  SKIP: no valid data")
        return {"symbol": symbol, "rows": 0, "files": 0}

    # Merge, deduplicate, sort
    combined = pl.concat(frames, how="diagonal_relaxed")
    combined = combined.unique(subset=["candle_time"], keep="first").sort("candle_time")

    # Compute true_range BEFORE splitting by year so that cross-year prev_close is correct.
    # shift(1) on the globally sorted DataFrame gives the last close of Dec 31 as the
    # prev_close for the first candle of Jan 1 the following year — impossible to do
    # correctly per-year after splitting.
    #
    # True Range = max(H-L, |H-prev_close|, |L-prev_close|)
    # First candle ever (null prev_close) → TR = H - L (fallback via max_horizontal null-skip)
    combined = (
        combined.with_columns(pl.col("close").shift(1).alias("_prev_close"))
        .with_columns(
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("_prev_close")).abs(),
                (pl.col("low") - pl.col("_prev_close")).abs(),
            )
            .cast(pl.Float64)
            .alias("true_range")
        )
        .drop("_prev_close")
    )

    # Add derived columns for DuckDB partitioning
    combined = combined.with_columns(
        [
            pl.col("candle_time").dt.date().alias("date"),
            pl.col("candle_time").dt.year().alias("year"),
            pl.lit(symbol).alias("symbol"),
        ]
    )

    years = sorted(combined["year"].unique().to_list())
    symbol_dir = output_dir / "5min" / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0
    for year in years:
        out = symbol_dir / f"{year}.parquet"
        if out.exists() and not overwrite:
            print(f"  SKIP {year}.parquet (exists — use --overwrite to replace)")
            continue
        year_df = combined.filter(pl.col("year") == year).drop("year")
        year_df.write_parquet(out, compression="snappy")
        files_written += 1
        size_kb = out.stat().st_size / 1024
        print(f"  -> {year}.parquet: {len(year_df):,} rows  ({size_kb:.0f} KB)")

    return {"symbol": symbol, "rows": len(combined), "years": len(years), "files": files_written}


# ---------------------------------------------------------------------------
# Daily conversion
# ---------------------------------------------------------------------------


def convert_daily(
    base_dir: Path, output_dir: Path, symbol_filter: str | None = None, overwrite: bool = False
) -> int:
    """
    Convert daily CSVs from data/raw/daily/ → data/parquet/daily/SYMBOL/all.parquet

    Daily CSVs are used directly for CPR calculations (more reliable than
    aggregating from 5-min due to potential intraday data gaps).
    """
    daily_dir = base_dir / "daily"
    if not daily_dir.exists():
        print("No data/raw/daily/ found — skipping daily conversion")
        return 0

    csv_files = sorted(daily_dir.glob("*.csv"))
    symbol_filter_norm = normalize_symbol(symbol_filter) if symbol_filter else None
    if symbol_filter_norm:
        csv_files = [f for f in csv_files if detect_symbol(f) == symbol_filter_norm]

    if not csv_files:
        print("No daily CSV files found")
        return 0

    print(f"\n--- Converting {len(csv_files)} daily CSV files ---")
    converted = 0

    for csv_path in csv_files:
        symbol = detect_symbol(csv_path)
        out_dir = output_dir / "daily" / symbol
        out_path = out_dir / "all.parquet"

        if out_path.exists() and not overwrite:
            print(f"  SKIP {symbol} daily (exists)")
            continue

        df = read_zerodha_csv(csv_path)
        if df.is_empty():
            continue

        # For daily data: extract just the date (ignore intraday time).
        # candle_time is now naive IST, so .dt.date() gives the correct trade date.
        df = (
            df.with_columns(
                [
                    pl.col("candle_time").dt.date().alias("date"),
                    pl.lit(symbol).alias("symbol"),
                ]
            )
            .drop("candle_time")
            .sort("date")
        )

        out_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_path, compression="snappy")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {symbol}/all.parquet: {len(df):,} days  ({size_kb:.0f} KB)")
        converted += 1

    return converted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Zerodha CSVs to partitioned Parquet files for DuckDB"
    )
    parser.add_argument("--input", default="data/raw", help="Raw data root (default: data/raw)")
    parser.add_argument(
        "--output", default="data/parquet", help="Parquet output root (default: data/parquet)"
    )
    parser.add_argument("--symbol", default=None, help="Single symbol only (e.g. RELIANCE)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Parquet files")
    parser.add_argument("--daily-only", action="store_true", help="Only convert daily CSVs")
    parser.add_argument(
        "--5min-only", dest="five_min_only", action="store_true", help="Only convert 5-min CSVs"
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        print(f"ERROR: {input_dir} not found.")
        print("Place Zerodha CSVs in data/raw/5min/Part 1/ ... Part 4/ and data/raw/daily/")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0

    # --- Convert 5-min ---
    if not args.daily_only:
        groups = find_5min_files(input_dir, symbol_filter=args.symbol)
        if not groups:
            print("No 5-min CSV files found in data/raw/5min/")
        else:
            print(f"Found {len(groups)} symbol(s) in 5-min data")
            for symbol, files in sorted(groups.items()):
                stats = convert_5min_symbol(symbol, files, output_dir, overwrite=args.overwrite)
                total_rows += stats.get("rows", 0)

    # --- Convert daily ---
    if not args.five_min_only:
        convert_daily(input_dir, output_dir, symbol_filter=args.symbol, overwrite=args.overwrite)

    print(f"\n{'=' * 55}")
    print(f"DONE — {total_rows:,} total 5-min candles converted")
    print(f"Output: {output_dir.resolve()}")
    print(
        "\nNext: uv run pivot-build --force && uv run pivot-backtest --symbol RELIANCE --start 2020-01-01 --end 2024-12-31"
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
