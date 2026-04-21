"""
Copy 5-min and daily parquet from nse-momentum-lab to cpr-pivot-lab.

- Reads NSE parquet (READ ONLY — never writes/deletes from NSE)
- Strips tz=+05:30 from candle_time → naive IST (same wall-clock times)
- Adds true_range column to 5-min data
- Writes only 2025 + 2026 5-min files (2015-2024 already correct from CSV)
- Extends CPR baseline daily parquet where NSE has more recent data than CPR

Usage:
    uv run python scripts/copy_from_nse.py [--dry-run]
    uv run python scripts/copy_from_nse.py --missing [--dry-run]   # full backfill for new symbols only
    uv run python scripts/copy_from_nse.py --missing --only-5min   # 5-min backfill only
"""

import argparse
import os
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

NSE_BASE = Path(r"C:\Users\kanna\github\nse-momentum-lab\data\parquet")
CPR_BASE = Path(r"C:\Users\kanna\github\cpr-pivot-lab\data\parquet")

NSE_5MIN = NSE_BASE / "5min"
NSE_DAILY = NSE_BASE / "daily"
CPR_5MIN = CPR_BASE / "5min"
CPR_DAILY = CPR_BASE / "daily"

COPY_YEARS = ["2025", "2026"]


def strip_tz_to_naive_ist(arrow_col: pa.ChunkedArray) -> pa.Array:
    """
    Convert timestamp[ns/us, tz=*] -> timestamp[us] naive IST.
    Wall-clock times are preserved (09:15 IST stays 09:15 naive).
    """
    ts_type = arrow_col.type
    if not pa.types.is_timestamp(ts_type):
        raise TypeError(f"Expected timestamp column, got {ts_type}")
    if ts_type.tz is None:
        return pc.cast(arrow_col, pa.timestamp("us"))
    return pc.cast(pc.local_timestamp(arrow_col), pa.timestamp("us"))


def compute_true_range(df: pl.DataFrame, first_prev_close: float | None) -> pl.DataFrame:
    """Add true_range column. prev_close = previous candle's close within the file."""
    df = df.sort(["symbol", "candle_time"])
    df = df.with_columns(pl.col("close").shift(1).over("symbol").alias("_prev_close"))
    # Fill first candle's prev_close from end of prior year file
    fill = float(first_prev_close) if first_prev_close is not None else None
    if fill is not None:
        df = df.with_columns(
            pl.when(pl.col("_prev_close").is_null())
            .then(pl.lit(fill))
            .otherwise(pl.col("_prev_close"))
            .alias("_prev_close")
        )
    else:
        # Fallback: use own open if no prior close
        df = df.with_columns(
            pl.when(pl.col("_prev_close").is_null())
            .then(pl.col("open"))
            .otherwise(pl.col("_prev_close"))
            .alias("_prev_close")
        )
    df = df.with_columns(
        pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("_prev_close")).abs(),
            (pl.col("low") - pl.col("_prev_close")).abs(),
        ).alias("true_range")
    ).drop("_prev_close")
    return df


def get_last_close(parquet_path: Path, symbol: str) -> float | None:
    """Return last close value from a parquet file for the given symbol."""
    if not parquet_path.exists():
        return None
    try:
        df = pl.read_parquet(parquet_path, columns=["symbol", "candle_time", "close"])
        rows = df.filter(pl.col("symbol") == symbol).sort("candle_time")
        if len(rows) == 0:
            return None
        return float(rows["close"][-1])
    except Exception:
        return None


def copy_5min_symbol(symbol: str, dry_run: bool) -> str:
    nse_dir = NSE_5MIN / symbol
    cpr_dir = CPR_5MIN / symbol

    if not nse_dir.exists():
        return f"  SKIP {symbol}: not in NSE"

    results = []
    for year in COPY_YEARS:
        nse_path = nse_dir / f"{year}.parquet"
        cpr_path = cpr_dir / f"{year}.parquet"

        if not nse_path.exists():
            results.append(f"    {year}: no NSE file")
            continue

        if dry_run:
            results.append(f"    {year}: would copy")
            continue

        # Read NSE (READ ONLY)
        table = pq.read_table(str(nse_path))
        row_count = len(table)

        # Convert candle_time tz → naive IST
        ct_idx = table.schema.get_field_index("candle_time")
        new_ct = strip_tz_to_naive_ist(table.column("candle_time"))
        table = table.set_column(ct_idx, pa.field("candle_time", pa.timestamp("us")), new_ct)

        # To Polars for true_range computation
        df = cast(pl.DataFrame, pl.from_arrow(table))

        # Ensure date column is naive IST date (not tz-shifted)
        if "date" in df.columns:
            df = df.drop("date")
        df = df.with_columns(pl.col("candle_time").dt.date().alias("date"))

        # Get prev year's last close for accurate true_range on first candle
        prev_year_path = cpr_dir / f"{int(year) - 1}.parquet"
        first_prev_close = get_last_close(prev_year_path, symbol)

        # Add / recompute true_range
        if "true_range" in df.columns:
            df = df.drop("true_range")
        df = compute_true_range(df, first_prev_close)

        # Reorder columns to match CPR convention
        desired_cols = [
            "candle_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "true_range",
            "date",
            "symbol",
        ]
        actual_cols = [c for c in desired_cols if c in df.columns]
        df = df.select(actual_cols)

        # Atomic write
        cpr_dir.mkdir(parents=True, exist_ok=True)
        tmp = str(cpr_path) + ".tmp"
        df.write_parquet(tmp, compression="snappy")
        os.replace(tmp, str(cpr_path))

        results.append(f"    {year}: {row_count:,} rows -> OK")

    return f"  {symbol}\n" + "\n".join(results)


def copy_daily_symbol(symbol: str, dry_run: bool) -> str:
    nse_path = NSE_DAILY / symbol / "all.parquet"
    cpr_path = CPR_DAILY / symbol / "all.parquet"

    if not nse_path.exists():
        return f"  SKIP {symbol} daily: not in NSE"

    # Read NSE daily (READ ONLY)
    nse_table = pq.read_table(str(nse_path))
    nse_dates = sorted(nse_table.column("date").to_pylist())
    nse_last = nse_dates[-1] if nse_dates else None

    # Read CPR daily files to find the current visible end date across all daily parquet
    cpr_last = None
    cpr_daily_dir = CPR_DAILY / symbol
    cpr_daily_files = sorted(cpr_daily_dir.glob("*.parquet")) if cpr_daily_dir.exists() else []
    for daily_file in cpr_daily_files:
        cpr_table = pq.read_table(str(daily_file), columns=["date"])
        cpr_dates = sorted(cpr_table.column("date").to_pylist())
        last_date = cpr_dates[-1] if cpr_dates else None
        if last_date is not None and (cpr_last is None or last_date > cpr_last):
            cpr_last = last_date

    if nse_last is None or (cpr_last is not None and nse_last <= cpr_last):
        return f"  SKIP {symbol} daily: NSE ({nse_last}) <= CPR ({cpr_last})"

    if dry_run:
        return f"  {symbol} daily: would extend {cpr_last} -> {nse_last} (+{(nse_last - (cpr_last or date(2015, 1, 1))).days} days)"

    # Extract only new rows (after CPR's current last date)
    if cpr_last is not None:
        mask = pc.greater(nse_table.column("date"), pa.scalar(cpr_last, type=pa.date32()))
        new_rows = nse_table.filter(mask)
    else:
        new_rows = nse_table

    if len(new_rows) == 0:
        return f"  SKIP {symbol} daily: no new rows"

    # Normalize schema to match CPR: symbol as large_string, same column order
    new_rows = new_rows.cast(
        new_rows.schema.set(
            new_rows.schema.get_field_index("symbol"), pa.field("symbol", pa.large_utf8())
        )
    )

    # Append to the CPR baseline file. v_daily dedupes overlapping rows against kite.parquet.
    if cpr_path.exists():
        cpr_existing = pq.read_table(str(cpr_path))
        combined = pa.concat_tables([cpr_existing, new_rows], promote_options="default")
    else:
        combined = new_rows

    tmp = str(cpr_path) + ".tmp"
    pq.write_table(combined, tmp, compression="snappy")
    os.replace(tmp, str(cpr_path))

    return f"  {symbol} daily: +{len(new_rows)} rows -> {nse_last}"


def _get_missing_symbols() -> list[str]:
    """Return symbols present in NSE but absent from CPR (both daily and 5-min checked)."""
    nse_syms = {
        d.name
        for d in NSE_5MIN.iterdir()
        if d.is_dir() and next(d.glob("*.parquet"), None) is not None
    }
    cpr_syms = {
        d.name
        for d in CPR_5MIN.iterdir()
        if d.is_dir() and next(d.glob("*.parquet"), None) is not None
    }
    return sorted(nse_syms - cpr_syms)


def copy_missing_5min_symbol(symbol: str, dry_run: bool) -> str:
    """Copy ALL year files for a symbol that has no CPR 5-min data at all.

    Computes true_range seeded across year boundaries (last close of year N
    seeds the first candle of year N+1).
    """
    nse_dir = NSE_5MIN / symbol
    cpr_dir = CPR_5MIN / symbol

    if not nse_dir.exists():
        return f"  SKIP {symbol}: not in NSE 5min"

    year_files = sorted(nse_dir.glob("*.parquet"), key=lambda f: f.stem)
    if not year_files:
        return f"  SKIP {symbol}: no parquet files in NSE"

    if dry_run:
        years = [f.stem for f in year_files]
        return f"  {symbol}: would copy {len(year_files)} years ({years[0]}-{years[-1]})"

    cpr_dir.mkdir(parents=True, exist_ok=True)
    prev_close_seed: float | None = None
    files_written = 0

    for src in year_files:
        table = pq.read_table(str(src))

        # Convert candle_time tz → naive IST us
        ct_idx = table.schema.get_field_index("candle_time")
        new_ct = strip_tz_to_naive_ist(table.column("candle_time"))
        table = table.set_column(ct_idx, pa.field("candle_time", pa.timestamp("us")), new_ct)

        df = cast(pl.DataFrame, pl.from_arrow(table))

        # Recompute date from candle_time to ensure consistency
        if "date" in df.columns:
            df = df.drop("date")
        df = df.with_columns(pl.col("candle_time").dt.date().alias("date"))

        # Compute true_range seeded from previous year's last close
        if "true_range" in df.columns:
            df = df.drop("true_range")
        df = compute_true_range(df, prev_close_seed)

        if len(df) > 0:
            prev_close_seed = float(df.sort("candle_time")["close"][-1])

        # Enforce CPR column order
        desired_cols = [
            "candle_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "true_range",
            "date",
            "symbol",
        ]
        df = df.select([c for c in desired_cols if c in df.columns])

        dst = cpr_dir / src.name
        tmp = str(dst) + ".tmp"
        df.write_parquet(tmp, compression="snappy")
        os.replace(tmp, str(dst))
        files_written += 1

    years = [f.stem for f in year_files]
    return f"  {symbol}: {files_written} files ({years[0]}-{years[-1]}) OK"


def copy_missing_daily_symbol(symbol: str, dry_run: bool) -> str:
    """Copy daily all.parquet for a symbol that has no CPR daily data at all."""
    nse_path = NSE_DAILY / symbol / "kite.parquet"
    if not nse_path.exists():
        nse_path = NSE_DAILY / symbol / "all.parquet"
    if not nse_path.exists():
        return f"  SKIP {symbol} daily: not in NSE"

    cpr_path = CPR_DAILY / symbol / "all.parquet"
    if cpr_path.exists():
        return f"  SKIP {symbol} daily: already exists in CPR"

    if dry_run:
        table = pq.read_table(str(nse_path), columns=["date"])
        dates = sorted(table.column("date").to_pylist())
        return f"  {symbol} daily: would copy {len(table)} rows ({dates[0]}-{dates[-1]})"

    nse_table = pq.read_table(str(nse_path))
    cpr_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(cpr_path) + ".tmp"
    pq.write_table(nse_table, tmp, compression="snappy")
    os.replace(tmp, str(cpr_path))
    return f"  {symbol} daily: {len(nse_table)} rows OK"


def _run_missing_backfill(args: argparse.Namespace) -> None:
    """Full backfill mode: copy ALL years for symbols absent from CPR."""
    missing = _get_missing_symbols()
    mode_str = "DRY RUN - " if args.dry_run else ""
    print(f"{mode_str}Missing backfill: {len(missing)} symbols not in CPR")
    print(f"NSE base : {NSE_BASE}  (READ ONLY)")
    print(f"CPR base : {CPR_BASE}  (WRITE)")
    print()

    # --- 5-min backfill ---
    print("=== 5-MIN (all years) ===")
    ok = skip = err = 0
    for i, sym in enumerate(missing, 1):
        try:
            result = copy_missing_5min_symbol(sym, args.dry_run)
        except Exception as exc:
            result = f"  ERROR {sym}: {exc}"
            err += 1
        if "OK" in result or "would copy" in result:
            ok += 1
        elif "SKIP" in result:
            skip += 1
        else:
            err += 1
        if i % 100 == 0 or i == len(missing):
            print(f"  [{i}/{len(missing)}] {result.strip()}")

    print(f"\n5-min done: {ok} copied, {skip} skipped, {err} errors\n")

    if args.only_5min:
        print("Daily skipped by --only-5min")
        print("\nNext step: doppler run -- uv run pivot-build --missing")
        return

    # --- daily backfill ---
    print("=== DAILY (all.parquet) ===")
    dok = dskip = derr = 0
    for i, sym in enumerate(missing, 1):
        try:
            result = copy_missing_daily_symbol(sym, args.dry_run)
        except Exception as exc:
            result = f"  ERROR {sym} daily: {exc}"
            derr += 1
        if "OK" in result or "would copy" in result:
            dok += 1
        elif "SKIP" in result:
            dskip += 1
        else:
            derr += 1
        if i % 100 == 0 or i == len(missing):
            print(f"  [{i}/{len(missing)}] {result.strip()}")

    print(f"\nDaily done: {dok} copied, {dskip} skipped, {derr} errors")
    print("\nNext step: doppler run -- uv run pivot-build --missing")


def main():
    parser = argparse.ArgumentParser(
        description="Copy parquet from nse-momentum-lab to cpr-pivot-lab"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done, no writes")
    parser.add_argument("--only-5min", action="store_true", help="Copy only 5-min parquet")
    parser.add_argument(
        "--missing",
        action="store_true",
        help=(
            "Full backfill mode: copy ALL years for symbols absent from CPR. "
            "Default mode only updates 2025/2026 for existing symbols."
        ),
    )
    args = parser.parse_args()

    if args.missing:
        _run_missing_backfill(args)
        return

    # Copy from the full NSE parquet universe, not just symbols already present locally.
    source_symbols = sorted(d.name for d in NSE_5MIN.iterdir() if d.is_dir())

    mode_str = "DRY RUN - " if args.dry_run else ""
    print(f"{mode_str}Copying NSE -> CPR for {len(source_symbols)} symbols")
    print(f"NSE base : {NSE_BASE}  (READ ONLY)")
    print(f"CPR base : {CPR_BASE}  (WRITE)")
    print()

    # --- 5-min copy ---
    print("=== 5-MIN (2025 + 2026) ===")
    ok = skip = err = 0
    for i, sym in enumerate(source_symbols, 1):
        try:
            result = copy_5min_symbol(sym, args.dry_run)
        except Exception as exc:
            result = f"  ERROR {sym}: {exc}"
        if "-> OK" in result or "would copy" in result:
            ok += 1
        elif "SKIP" in result:
            skip += 1
        else:
            err += 1
        # Print progress every 100 symbols or on error
        if i % 100 == 0 or i == len(source_symbols):
            print(f"  [{i}/{len(source_symbols)}] {sym}: {result.strip().split(chr(10))[-1]}")

    print(f"\n5-min done: {ok} written, {skip} skipped, {err} errors\n")

    if args.only_5min:
        print("Daily skipped by --only-5min")
        print("\nAll done. Next step: pivot-build --refresh-since 2025-01-01")
        return

    # --- daily copy ---
    print("=== DAILY (extending all.parquet) ===")
    dok = dskip = derr = 0
    for i, sym in enumerate(source_symbols, 1):
        try:
            result = copy_daily_symbol(sym, args.dry_run)
        except Exception as exc:
            result = f"  ERROR {sym} daily: {exc}"
        if "SKIP" in result:
            dskip += 1
        elif "would extend" in result or "new rows" in result or "+" in result:
            dok += 1
        else:
            derr += 1
        if i % 100 == 0 or (i == len(source_symbols)):
            print(f"  [{i}/{len(source_symbols)}] last: {result.strip()}")

    print(f"\nDaily done: {dok} extended, {dskip} skipped, {derr} errors")
    print("\nAll done. Next step: pivot-build --refresh-since 2025-04-01")


if __name__ == "__main__":
    main()
