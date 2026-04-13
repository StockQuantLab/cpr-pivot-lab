"""
Copy 5-min parquet from cpr-pivot-lab → nse-momentum-lab for symbols with bad timestamps.

CAUTION: This writes to the NSE project. CPR is the source of truth here — its 5-min data
         comes from Zerodha CSV via pivot-convert, which always stored IST-naive timestamps
         correctly. NSE's ingest pipeline had a window where Kite API timestamps were stripped
         as UTC instead of IST, producing candle_time=03:45 instead of 09:15.

Input : scripts/nse_bad_timestamps.csv  (symbol,bad_years — years pipe-separated)
Output: NSE_BASE/5min/{symbol}/{year}.parquet  (overwrite in-place, atomic)

Transform applied:
  - Read CPR parquet  (candle_time: Datetime(us), includes true_range)
  - Sanity-check first candle hour ≥ 9  (guard against copying bad CPR data)
  - Drop true_range  (NSE schema does not have this column)
  - Cast candle_time  us → ns  (NSE stores Datetime(ns))
  - Reorder to NSE column order: [symbol, date, candle_time, open, high, low, close, volume]

Usage:
    uv run python scripts/copy_to_nse.py [--dry-run] [--symbols SYM1,SYM2]
"""

import argparse
import csv
import os
from pathlib import Path

import polars as pl

NSE_BASE = Path(r"C:\Users\kanna\github\nse-momentum-lab\data\parquet")
CPR_BASE = Path(r"C:\Users\kanna\github\cpr-pivot-lab\data\parquet")

NSE_5MIN = NSE_BASE / "5min"
CPR_5MIN = CPR_BASE / "5min"

INPUT_CSV = Path(__file__).parent / "nse_bad_timestamps.csv"

NSE_COLS = ["symbol", "date", "candle_time", "open", "high", "low", "close", "volume"]


def _read_input(symbols_filter: list[str] | None) -> dict[str, list[str]]:
    """Return {symbol: [year, ...]} from the input CSV, optionally filtered by symbol list."""
    result: dict[str, list[str]] = {}
    with open(INPUT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip()
            if symbols_filter and sym not in symbols_filter:
                continue
            years = [y.strip() for y in row["bad_years"].split("|") if y.strip()]
            if years:
                result[sym] = years
    return result


def _copy_symbol_year(symbol: str, year: str, dry_run: bool) -> str:
    """Copy one (symbol, year) pair from CPR → NSE. Returns a status string."""
    cpr_path = CPR_5MIN / symbol / f"{year}.parquet"
    nse_path = NSE_5MIN / symbol / f"{year}.parquet"

    if not cpr_path.exists():
        return f"    {year}: SKIP — CPR file not found"
    if not nse_path.exists():
        return f"    {year}: SKIP — NSE file not found (nothing to replace)"

    df = pl.read_parquet(cpr_path)
    if len(df) == 0:
        return f"    {year}: SKIP — CPR file is empty"

    # Sanity check: CPR candle_time must be IST and stay within the NSE session.
    first_ts = df.sort("candle_time")["candle_time"][0]
    first_hour = first_ts.hour  # type: ignore[union-attr]
    first_day = first_ts.date()  # type: ignore[union-attr]
    if first_day.year != int(year) or first_hour < 9 or first_hour >= 16:
        return (
            f"    {year}: ERROR — CPR timestamps look wrong "
            f"(first candle={first_ts}, expected same-year IST session time)"
        )

    if dry_run:
        return f"    {year}: would copy {len(df):,} rows (first: {first_ts})"

    # Drop true_range — NSE schema does not include it
    if "true_range" in df.columns:
        df = df.drop("true_range")

    # Cast candle_time Datetime(us) → Datetime(ns)
    df = df.with_columns(pl.col("candle_time").cast(pl.Datetime("ns")))

    # Enforce NSE column order
    df = df.select(NSE_COLS)

    # Atomic write — write .tmp then os.replace to avoid partial files on crash
    tmp = str(nse_path) + ".tmp"
    df.write_parquet(tmp, compression="snappy")
    os.replace(tmp, str(nse_path))

    return f"    {year}: {len(df):,} rows -> OK"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Copy 5-min parquet from CPR (IST-correct) → NSE (bad timestamps) "
            "for symbols listed in nse_bad_timestamps.csv"
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview actions, no writes")
    parser.add_argument(
        "--symbols",
        metavar="SYM1,SYM2",
        help="Comma-separated symbols to process (default: all rows in input CSV)",
    )
    args = parser.parse_args()

    symbols_filter = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    todo = _read_input(symbols_filter)

    if not todo:
        print("No matching symbols found in input CSV.")
        return

    total_years = sum(len(v) for v in todo.values())
    mode = "DRY RUN — " if args.dry_run else ""
    print(f"{mode}Copy CPR -> NSE: {len(todo)} symbols, {total_years} year-files")
    print(f"CPR source : {CPR_5MIN}  (READ ONLY)")
    print(f"NSE target : {NSE_5MIN}  (WRITE)")
    print(f"Input CSV  : {INPUT_CSV}")
    print()

    ok = skip = err = 0

    for i, (sym, years) in enumerate(sorted(todo.items()), 1):
        print(f"  [{i}/{len(todo)}] {sym}  bad years: {', '.join(years)}")
        for year in years:
            try:
                result = _copy_symbol_year(sym, year, args.dry_run)
            except Exception as exc:
                result = f"    {year}: ERROR — {exc}"
                err += 1
                print(result)
                continue

            print(result)
            if "OK" in result or "would copy" in result:
                ok += 1
            elif "SKIP" in result:
                skip += 1
            else:
                err += 1

    print(f"\nDone: {ok} written, {skip} skipped, {err} errors  (of {total_years} year-files)")


if __name__ == "__main__":
    main()
