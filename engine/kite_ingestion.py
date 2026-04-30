from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import random
import subprocess
import threading
import time as time_module
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
from kiteconnect import KiteConnect

from config.settings import get_settings
from engine.constants import normalize_symbol, preview_list

# Allow reading legacy parquet files that have a fixed +05:30 tz annotation
os.environ.setdefault("POLARS_IGNORE_TIMEZONE_PARSE_ERROR", "1")

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
CHECKPOINT_FLUSH_EVERY = 25
PROGRESS_LOG_EVERY = 10
DEFAULT_5MIN_CHUNK_DAYS = 60
# Kite returns HTTP 400 for daily requests that span more than ~2000 trading days.
# 2000 calendar days ≈ 1370 NSE trading days, safely under that limit.
DEFAULT_DAILY_CHUNK_DAYS = 2000
HISTORICAL_REQUESTS_PER_SECOND = 2.85
HISTORICAL_RATE_LIMIT_BURST = 3.0

# Retry configuration
_RETRY_ATTEMPTS = 5
_RETRY_BASE_DELAY = 1.0  # seconds
_RETRY_JITTER = 0.5  # random jitter up to this many seconds
_RETRY_MAX_DELAY = 30.0  # cap
_RATE_LIMIT_BASE_DELAY = 5.0  # longer base for HTTP 429

_INGEST_CONFLICT_TOKENS = (
    "pivot-dashboard",
    "pivot-paper-trading",
    "pivot-backtest",
    "pivot-agent",
    "pivot-build",
)
_BUILD_CONFLICT_TOKENS = (
    "pivot-dashboard",
    "pivot-paper-trading",
    "pivot-backtest",
    "pivot-agent",
    "pivot-kite-ingest",
)

# Centralized parquet schemas — enforced on every write
PARQUET_DAILY_SCHEMA: dict[str, pl.DataType] = {
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "volume": pl.Int64(),
    "date": pl.Date(),
    "symbol": pl.String(),
}

PARQUET_5MIN_SCHEMA: dict[str, pl.DataType] = {
    "candle_time": pl.Datetime("us"),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "volume": pl.Int64(),
    "true_range": pl.Float64(),
    "date": pl.Date(),
    "symbol": pl.String(),
}

IngestionMode = Literal["daily", "5min"]
UniverseMode = Literal["local-first", "current-master"]

MAJOR_NSE_INDEX_SYMBOLS: tuple[str, ...] = ("NIFTY 50", "NIFTY 100", "NIFTY 500")


class KiteIngestionError(RuntimeError):
    """Raised when a Kite ingestion operation cannot complete."""


@dataclass(slots=True)
class RuntimeProcessConflict:
    pid: int
    name: str
    command_line: str


@dataclass(slots=True)
class KitePaths:
    parquet_root: Path
    raw_root: Path
    instrument_dir: Path
    daily_raw_dir: Path
    five_min_raw_dir: Path
    checkpoint_dir: Path


@dataclass(slots=True)
class KiteIngestionRequest:
    mode: IngestionMode
    start_date: date
    end_date: date
    exchange: str
    symbols: list[str]
    save_raw: bool = False
    resume: bool = False
    skip_existing: bool = False
    checkpoint_file: Path | None = None
    five_min_chunk_days: int = DEFAULT_5MIN_CHUNK_DAYS
    daily_chunk_days: int = DEFAULT_DAILY_CHUNK_DAYS
    universe: UniverseMode = "local-first"


@dataclass(slots=True)
class KiteIngestionResult:
    mode: IngestionMode
    start_date: str
    end_date: str
    exchange: str
    requested_symbols: list[str]
    completed_symbols: list[str]
    skipped_symbols: list[str]
    missing_instruments: list[str]
    errors: dict[str, str]
    rows_written: int
    raw_snapshot_count: int
    checkpoint_path: str | None
    checkpoint_cleared: bool


class TokenBucketRateLimiter:
    """Thread-safe token bucket limiter for Kite historical requests."""

    def __init__(self, rate_per_second: float, burst_capacity: float | None = None) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self.rate_per_second = float(rate_per_second)
        self.burst_capacity = float(burst_capacity or rate_per_second)
        if self.burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")
        self._tokens = self.burst_capacity
        self._last_refill = time_module.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        if tokens <= 0:
            raise ValueError("tokens must be positive")

        waited = 0.0
        while True:
            with self._lock:
                now = time_module.monotonic()
                elapsed = max(0.0, now - self._last_refill)
                self._last_refill = now
                self._tokens = min(
                    self.burst_capacity,
                    self._tokens + elapsed * self.rate_per_second,
                )
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                deficit = tokens - self._tokens
                wait_for = deficit / self.rate_per_second

            time_module.sleep(wait_for)
            waited += wait_for


_HISTORICAL_RATE_LIMITER = TokenBucketRateLimiter(
    rate_per_second=HISTORICAL_REQUESTS_PER_SECOND,
    burst_capacity=HISTORICAL_RATE_LIMIT_BURST,
)


@dataclass(slots=True)
class DailyOverlayCompactionResult:
    requested_symbols: list[str]
    compacted_symbols: list[str]
    skipped_symbols: list[str]
    rows_written: int


def get_kite_paths() -> KitePaths:
    settings = get_settings()
    raw_root = Path(settings.raw_data_dir)
    kite_raw_root = raw_root / "kite"
    return KitePaths(
        parquet_root=Path(settings.parquet_dir),
        raw_root=raw_root,
        instrument_dir=kite_raw_root / "instruments",
        daily_raw_dir=kite_raw_root / "daily",
        five_min_raw_dir=kite_raw_root / "5min",
        checkpoint_dir=kite_raw_root / "checkpoints",
    )


def detect_repo_process_conflicts(
    command: Literal["ingest", "build"],
) -> list[RuntimeProcessConflict]:
    """Return long-lived repo processes that should not overlap with ingest/build.

    Windows readers can block parquet file replacement even when they open files
    in read-only mode. We proactively block known repo commands that commonly
    hold DuckDB/parquet handles open.
    """
    if os.name != "nt" or "PYTEST_CURRENT_TEST" in os.environ:
        return []

    repo_root = str(Path(__file__).resolve().parents[1]).replace("'", "''")
    tokens = _INGEST_CONFLICT_TOKENS if command == "ingest" else _BUILD_CONFLICT_TOKENS
    quoted_tokens = [token.replace("'", "''") for token in tokens]
    token_filters = " -or ".join(f"$_.CommandLine -like '*{token}*'" for token in quoted_tokens)
    ps_command = (
        "$ErrorActionPreference='Stop';"
        "Get-CimInstance Win32_Process | "
        f"Where-Object {{ $_.CommandLine -like '*{repo_root}*' -and ({token_filters}) }} | "
        "Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress"
    )
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-Command",
                ps_command,
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except Exception as exc:
        logger.debug("process preflight skipped: %s", exc)
        return []

    raw = (completed.stdout or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.debug("process preflight returned non-JSON output: %r", raw)
        return []

    rows = payload if isinstance(payload, list) else [payload]
    conflicts: list[RuntimeProcessConflict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = int(row.get("ProcessId") or 0)
        command_line = str(row.get("CommandLine") or "")
        if pid == os.getpid():
            continue
        if (
            "Get-CimInstance Win32_Process" in command_line
            and "ConvertTo-Json -Compress" in command_line
        ):
            continue
        conflicts.append(
            RuntimeProcessConflict(
                pid=pid,
                name=str(row.get("Name") or ""),
                command_line=command_line,
            )
        )
    return conflicts


def ensure_repo_process_preflight(command: Literal["ingest", "build"]) -> None:
    conflicts = detect_repo_process_conflicts(command)
    if not conflicts:
        return
    lines = [
        f"Active repo processes would conflict with {command} on Windows.",
        "Stop these before retrying:",
    ]
    for conflict in conflicts:
        lines.append(f"- PID {conflict.pid} {conflict.name}: {conflict.command_line}")
    if command == "ingest":
        lines.append(
            "Reason: parquet readers can block atomic replace of daily files during Kite ingest."
        )
    else:
        lines.append(
            "Reason: runtime rebuilds should not overlap with live paper, dashboard, or ingest processes."
        )
    raise KiteIngestionError("\n".join(lines))


def get_kite_client() -> KiteConnect:
    settings = get_settings()
    api_key = (settings.kite_api_key or "").strip()
    access_token = (settings.kite_access_token or "").strip()
    if not api_key or not access_token:
        raise KiteIngestionError(
            "Kite API key and access token are required. Run via Doppler with KITE_API_KEY and KITE_ACCESS_TOKEN."
        )

    client = KiteConnect(api_key=api_key)
    client.set_access_token(access_token)
    return client


def parse_symbols_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [normalize_symbol(part) for part in value.split(",") if part.strip()]


def parse_symbols_file(path: str | Path) -> list[str]:
    """Read a text file with one symbol per line and return a normalized list.

    Blank lines and lines starting with '#' are ignored.
    """
    p = Path(path).expanduser()
    if not p.exists():
        raise KiteIngestionError(f"Symbols file not found: {p}")
    symbols = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        sym = normalize_symbol(s)
        if sym:
            symbols.append(sym)
    return symbols


def resolve_missing_ingest_symbols(exchange: str = "NSE", mode: str = "daily") -> list[str]:
    """Return tradeable symbols that have no local parquet data for the given mode.

    Used by --missing to auto-detect backfill candidates:
      missing = tradeable_set - parquet_set

    Args:
        mode: "daily" checks data/parquet/daily; "5min" checks data/parquet/5min.
              Pass the same mode as the planned ingestion run so that
              --5min --missing does not silently skip symbols that have daily
              parquet but no 5-minute parquet.
    """
    tradeable = tradeable_symbols(exchange)
    if not tradeable:
        raise KiteIngestionError(
            "Instrument master not found or empty. Run --refresh-instruments first."
        )
    paths = get_kite_paths()
    parquet_subdir = "5min" if mode == "5min" else "daily"
    parquet_root = paths.parquet_root / parquet_subdir
    existing: set[str] = set()
    if parquet_root.exists():
        existing = {
            normalize_symbol(entry.name)
            for entry in parquet_root.iterdir()
            if entry.is_dir() and any(entry.glob("*.parquet"))
        }
    missing = sorted(tradeable - existing)
    logger.info(
        "Missing ingest symbols (%s): %d of %d tradeable have no local parquet",
        parquet_subdir,
        len(missing),
        len(tradeable),
    )
    return missing


def parse_iso_date(value: str) -> date:
    """Parse ISO date string to date object with domain-specific error.

    This is a domain-specific wrapper that returns a date object (vs string)
    and uses KiteIngestionError for error handling.
    """
    from engine.constants import parse_iso_date as _validate

    try:
        return date.fromisoformat(_validate(value))
    except ValueError as exc:
        raise KiteIngestionError(f"Invalid date {value!r}. Expected YYYY-MM-DD.") from exc


def resolve_date_window(
    *,
    today: bool,
    one_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[date, date]:
    selected = int(bool(today)) + int(bool(one_date)) + int(bool(start_date or end_date))
    if selected != 1:
        raise KiteIngestionError(
            "Choose exactly one date mode: --today, --date YYYY-MM-DD, or --from YYYY-MM-DD --to YYYY-MM-DD."
        )

    if today:
        resolved = datetime.now(IST).date()
        return resolved, resolved

    if one_date:
        resolved = parse_iso_date(one_date)
        return resolved, resolved

    if not start_date or not end_date:
        raise KiteIngestionError("--from and --to must be provided together.")

    start = parse_iso_date(start_date)
    end = parse_iso_date(end_date)
    if start > end:
        raise KiteIngestionError("--from must be <= --to.")
    return start, end


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_text(path: Path, content: str) -> None:
    _ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def _write_parquet_atomically(
    df: pl.DataFrame, path: Path, schema: dict[str, pl.DataType] | None = None
) -> None:
    _ensure_parent(path)
    if schema:
        df = df.cast(schema, strict=False)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.write_parquet(tmp_path, compression="zstd")
    tmp_path.replace(path)


def _serialize_candle_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None:
        return ""
    return str(value)


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            raise KiteIngestionError("Encountered empty Kite candle timestamp")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        if len(raw) >= 5 and raw[-5] in {"+", "-"} and raw[-3] != ":":
            raw = raw[:-5] + raw[-5:-2] + ":" + raw[-2:]
        dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt


def _to_ist_naive(value: Any) -> datetime:
    """Convert any timestamp to naive IST datetime (timezone stripped)."""
    return _to_datetime(value).astimezone(IST).replace(tzinfo=None)


def _to_local_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return _to_datetime(value).astimezone(IST).date()


def refresh_instrument_master(*, exchange: str = "NSE") -> Path:
    client = get_kite_client()
    try:
        rows = client.instruments(exchange=exchange)
    except Exception as exc:
        raise KiteIngestionError(
            f"Failed to refresh instrument master for {exchange}: {exc}"
        ) from exc

    if not rows:
        raise KiteIngestionError(f"Kite returned no instruments for exchange {exchange}")

    paths = get_kite_paths()
    out_path = paths.instrument_dir / f"{exchange.upper()}.csv"
    fieldnames = sorted({key for row in rows for key in row.keys()})
    _ensure_parent(out_path)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(out_path)
    return out_path


def load_instrument_master(*, exchange: str = "NSE") -> pl.DataFrame:
    paths = get_kite_paths()
    instrument_path = paths.instrument_dir / f"{exchange.upper()}.csv"
    if not instrument_path.exists():
        raise KiteIngestionError(
            f"Instrument master cache missing: {instrument_path}. Run refresh-instruments first."
        )

    df = pl.read_csv(instrument_path, infer_schema_length=1000, ignore_errors=True)
    required = {"instrument_token", "tradingsymbol"}
    missing = required - set(df.columns)
    if missing:
        raise KiteIngestionError(
            f"Instrument master {instrument_path} is missing required columns: {sorted(missing)}"
        )

    if "exchange" in df.columns:
        df = df.filter(pl.col("exchange") == exchange.upper())

    return df.filter(
        pl.col("tradingsymbol").is_not_null() & pl.col("instrument_token").is_not_null()
    )


def _load_nse_equity_allowlist() -> set[str] | None:
    """Load the NSE equity allowlist from data/NSE_EQUITY_SYMBOLS.csv (SERIES=EQ).

    This file is downloaded from NSE's official equity listing and contains only
    true equity shares (SERIES=EQ), excluding ETFs, REITs, InvITs, bonds, etc.
    Returns None if the file is missing (graceful degradation — callers widen to segment filter).
    """
    settings = get_settings()
    allowlist_path = Path(settings.raw_data_dir).parent / "NSE_EQUITY_SYMBOLS.csv"
    if not allowlist_path.exists():
        logger.debug(
            "NSE equity allowlist not found at %s — using segment filter only", allowlist_path
        )
        return None

    try:
        with allowlist_path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            symbols: set[str] = set()
            for raw_row in reader:
                row = {str(k).strip(): str(v).strip() for k, v in raw_row.items()}
                if row.get("SERIES", "").upper() != "EQ":
                    continue
                symbol = row.get("SYMBOL", "").strip().upper()
                if symbol:
                    symbols.add(symbol)
        logger.debug("Loaded %d NSE EQ allowlist symbols", len(symbols))
        return symbols
    except Exception:
        logger.exception("Failed to read NSE equity allowlist")
        return None


def tradeable_symbols(exchange: str = "NSE") -> set[str] | None:
    """Return the set of currently tradeable equity symbols from the Kite instrument master.

    For NSE, cross-references with data/NSE_EQUITY_SYMBOLS.csv (SERIES=EQ) to exclude
    ETFs, REITs, bonds, and other non-equity instruments. Falls back to segment=NSE filter
    when the allowlist is absent.
    Returns None if the instrument master CSV is missing (graceful degradation).
    """
    try:
        instrument_df = load_instrument_master(exchange=exchange)
    except KiteIngestionError:
        return None
    filtered = instrument_df.filter(pl.col("segment") == exchange.upper())
    symbols = {
        normalize_symbol(s) for s in filtered.get_column("tradingsymbol").drop_nulls().to_list()
    }
    if exchange.upper() == "NSE":
        allowlist = _load_nse_equity_allowlist()
        if allowlist:
            symbols &= allowlist
    return symbols


def _resolve_from_instrument_master(exchange: str) -> list[str]:
    """Return all tradeable equity symbols from the Kite instrument master CSV.

    For NSE, cross-references with data/NSE_EQUITY_SYMBOLS.csv (SERIES=EQ) so that
    only true equity stocks are returned (not ETFs, REITs, bonds, etc.).
    """
    instrument_df = load_instrument_master(exchange=exchange)
    filtered = instrument_df.filter(pl.col("segment") == exchange.upper())
    symbols = {
        normalize_symbol(s) for s in filtered.get_column("tradingsymbol").drop_nulls().to_list()
    }
    if exchange.upper() == "NSE":
        allowlist = _load_nse_equity_allowlist()
        if allowlist:
            symbols &= allowlist
    return sorted(symbols)


def resolve_target_symbols(
    *,
    explicit_symbols: list[str] | None = None,
    exchange: str = "NSE",
    tradeable_only: bool = True,
    universe: UniverseMode = "local-first",
) -> list[str]:
    if explicit_symbols:
        return sorted(set(explicit_symbols))

    if universe == "current-master":
        symbols = _resolve_from_instrument_master(exchange)
        logger.info(
            "Universe=current-master: resolved %d symbols from instrument master", len(symbols)
        )
        return symbols

    # local-first: parquet dirs, optionally filtered to tradeable
    paths = get_kite_paths()
    daily_root = paths.parquet_root / "daily"
    if daily_root.exists():
        parquet_symbols = sorted(
            normalize_symbol(entry.name)
            for entry in daily_root.iterdir()
            if entry.is_dir() and any(entry.glob("*.parquet"))
        )
        if parquet_symbols:
            if tradeable_only:
                tradeable = tradeable_symbols(exchange) or set()
                filtered = [s for s in parquet_symbols if s in tradeable]
                skipped = len(parquet_symbols) - len(filtered)
                if skipped:
                    logger.info(
                        "Filtered %d non-tradeable symbols from %d parquet symbols",
                        skipped,
                        len(parquet_symbols),
                    )
                return filtered
            return parquet_symbols

    instrument_df = load_instrument_master(exchange=exchange)
    return sorted(
        {
            normalize_symbol(symbol)
            for symbol in instrument_df.get_column("tradingsymbol").drop_nulls().to_list()
        }
    )


def resolve_major_index_symbols(exchange: str = "NSE") -> list[str]:
    """Return the major NSE index symbols available in the current instrument master.

    These are intentionally separate from the tradeable equity universe. They are used as
    market-regime inputs, not tradable symbols.
    """
    instrument_df = load_instrument_master(exchange=exchange)
    wanted = {normalize_symbol(symbol) for symbol in MAJOR_NSE_INDEX_SYMBOLS}
    resolved = {
        normalize_symbol(symbol)
        for symbol in instrument_df.get_column("tradingsymbol").drop_nulls().to_list()
        if normalize_symbol(symbol) in wanted
    }
    return [symbol for symbol in MAJOR_NSE_INDEX_SYMBOLS if symbol in resolved]


def filter_already_ingested(
    symbols: list[str],
    *,
    mode: IngestionMode,
    end_date: date,
) -> tuple[list[str], list[str]]:
    """Return (need_fetch, already_done) by checking parquet max dates."""
    paths = get_kite_paths()
    need_fetch: list[str] = []
    already_done: list[str] = []
    for symbol in symbols:
        if mode == "daily":
            symbol_dir = paths.parquet_root / "daily" / symbol
            parquet_path = symbol_dir / "*.parquet"
            if not symbol_dir.exists() or not any(symbol_dir.glob("*.parquet")):
                need_fetch.append(symbol)
                continue
        else:
            parquet_path = paths.parquet_root / "5min" / symbol / f"{end_date.year}.parquet"
            if not parquet_path.exists():
                need_fetch.append(symbol)
                continue
        con = duckdb.connect(":memory:")
        try:
            row = con.execute(
                "SELECT MAX(date) FROM read_parquet(?)",
                [str(parquet_path)],
            ).fetchone()
        except Exception:
            need_fetch.append(symbol)
            continue
        finally:
            con.close()
        if row and row[0] is not None:
            max_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            if max_date >= end_date:
                already_done.append(symbol)
                continue
        need_fetch.append(symbol)
    return need_fetch, already_done


def resolve_instrument_tokens(
    symbols: list[str], *, exchange: str = "NSE"
) -> tuple[dict[str, int], list[str]]:
    instrument_df = load_instrument_master(exchange=exchange)
    filtered = instrument_df.filter(pl.col("tradingsymbol").is_in(symbols))
    mapping: dict[str, int] = {}
    for row in filtered.iter_rows(named=True):
        symbol = normalize_symbol(str(row["tradingsymbol"]))
        if symbol in mapping:
            continue
        mapping[symbol] = int(row["instrument_token"])
    missing = [symbol for symbol in symbols if symbol not in mapping]
    return mapping, missing


def _daily_kite_parquet_path(symbol: str) -> Path:
    return get_kite_paths().parquet_root / "daily" / symbol / "kite.parquet"


def _daily_baseline_parquet_path(symbol: str) -> Path:
    return get_kite_paths().parquet_root / "daily" / symbol / "all.parquet"


def _five_min_year_path(symbol: str, year: int) -> Path:
    return get_kite_paths().parquet_root / "5min" / symbol / f"{year}.parquet"


def _existing_prev_close(symbol: str, before_ts: datetime) -> float | None:
    symbol_dir = get_kite_paths().parquet_root / "5min" / symbol
    if not symbol_dir.exists():
        return None

    candidates: list[tuple[int, Path]] = []
    for path in symbol_dir.glob("*.parquet"):
        try:
            year = int(path.stem)
        except ValueError:
            continue
        if year <= before_ts.year:
            candidates.append((year, path))

    for _year, path in sorted(candidates, reverse=True):
        con = duckdb.connect(":memory:")
        try:
            row = con.execute(
                """
                SELECT close
                FROM read_parquet(?)
                WHERE candle_time < ?
                ORDER BY candle_time DESC
                LIMIT 1
                """,
                [str(path), before_ts],
            ).fetchone()
        finally:
            con.close()
        if row and row[0] is not None:
            return float(row[0])
    return None


def _compute_true_range(df: pl.DataFrame, *, prev_close_seed: float | None) -> pl.DataFrame:
    if df.is_empty():
        return df

    return (
        df.sort("candle_time")
        .with_row_index("_row_nr")
        .with_columns(
            pl.when(pl.col("_row_nr") == 0)
            .then(pl.lit(prev_close_seed, dtype=pl.Float64))
            .otherwise(pl.col("close").shift(1))
            .alias("_prev_close")
        )
        .with_columns(
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("_prev_close")).abs(),
                (pl.col("low") - pl.col("_prev_close")).abs(),
            )
            .cast(pl.Float64)
            .alias("true_range")
        )
        .drop("_row_nr", "_prev_close")
    )


def _normalize_daily_candles(symbol: str, candles: list[dict[str, Any]]) -> pl.DataFrame:
    if not candles:
        return pl.DataFrame(schema=PARQUET_DAILY_SCHEMA)

    rows = []
    for candle in candles:
        rows.append(
            {
                "symbol": symbol,
                "date": _to_local_date(candle["date"]),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": int(candle.get("volume") or 0),
            }
        )
    return (
        pl.DataFrame(rows)
        .cast(PARQUET_DAILY_SCHEMA, strict=False)
        .sort("date")
        .unique(subset=["symbol", "date"], keep="last")
    )


def _normalize_5min_candles(
    symbol: str,
    candles: list[dict[str, Any]],
    *,
    prev_close_seed: float | None,
) -> pl.DataFrame:
    if not candles:
        return pl.DataFrame(schema=PARQUET_5MIN_SCHEMA)

    rows = []
    for candle in candles:
        candle_ts = _to_ist_naive(candle["date"])
        rows.append(
            {
                "candle_time": candle_ts,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": int(candle.get("volume") or 0),
            }
        )

    df = _compute_true_range(pl.DataFrame(rows), prev_close_seed=prev_close_seed)
    return (
        df.with_columns(
            [
                pl.col("candle_time").dt.date().alias("date"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .cast(PARQUET_5MIN_SCHEMA, strict=False)
        .sort("candle_time")
        .unique(subset=["symbol", "candle_time"], keep="last")
    )


def _merge_daily_symbol(symbol: str, df: pl.DataFrame) -> int:
    out_path = _daily_kite_parquet_path(symbol)
    existing = pl.read_parquet(out_path) if out_path.exists() else None
    combined = df if existing is None else pl.concat([existing, df], how="diagonal_relaxed")
    combined = combined.sort("date").unique(subset=["symbol", "date"], keep="last").sort("date")
    _write_parquet_atomically(combined, out_path, schema=PARQUET_DAILY_SCHEMA)
    return df.height


def compact_daily_overlays(symbols: list[str]) -> DailyOverlayCompactionResult:
    """Merge daily kite overlays into baseline all.parquet and delete kite.parquet."""
    ensure_repo_process_preflight("ingest")
    compacted: list[str] = []
    skipped: list[str] = []
    rows_written = 0

    for symbol in symbols:
        overlay_path = _daily_kite_parquet_path(symbol)
        if not overlay_path.exists():
            skipped.append(symbol)
            continue

        baseline_path = _daily_baseline_parquet_path(symbol)
        overlay_df = pl.read_parquet(overlay_path).cast(PARQUET_DAILY_SCHEMA, strict=False)
        if baseline_path.exists():
            baseline_df = pl.read_parquet(baseline_path).cast(PARQUET_DAILY_SCHEMA, strict=False)
            merged = pl.concat([baseline_df, overlay_df], how="diagonal_relaxed")
        else:
            merged = overlay_df
        merged = merged.sort("date").unique(subset=["symbol", "date"], keep="last").sort("date")

        _write_parquet_atomically(merged, baseline_path, schema=PARQUET_DAILY_SCHEMA)
        overlay_path.unlink()
        compacted.append(symbol)
        rows_written += merged.height

    return DailyOverlayCompactionResult(
        requested_symbols=list(symbols),
        compacted_symbols=compacted,
        skipped_symbols=skipped,
        rows_written=rows_written,
    )


def _read_existing_5min_parquet(path: Path) -> pl.DataFrame:
    con = duckdb.connect(":memory:")
    try:
        arrow_table = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).arrow()
    finally:
        con.close()
    df = cast(pl.DataFrame, pl.from_arrow(arrow_table))
    candle_dtype = df.schema.get("candle_time")
    time_zone = getattr(candle_dtype, "time_zone", None)
    if time_zone:
        # Old Parquet may have UTC or other tz — convert to IST then strip timezone
        df = cast(
            pl.DataFrame,
            df.with_columns(
                [
                    pl.col("candle_time")
                    .dt.convert_time_zone("Asia/Kolkata")
                    .dt.replace_time_zone(None)
                    .alias("candle_time"),
                ]
            ),
        )
        if "date" in df.columns:
            df = df.with_columns(pl.col("candle_time").dt.date().alias("date"))
    return df


def _merge_5min_symbol(symbol: str, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    total_rows = 0
    years = sorted(set(df["candle_time"].dt.year().to_list()))
    for year in years:
        out_path = _five_min_year_path(symbol, int(year))
        year_df = df.filter(pl.col("candle_time").dt.year() == int(year))
        existing = _read_existing_5min_parquet(out_path) if out_path.exists() else None
        combined = (
            year_df if existing is None else pl.concat([existing, year_df], how="diagonal_relaxed")
        )
        combined = (
            combined.sort("candle_time")
            .unique(subset=["symbol", "candle_time"], keep="last")
            .sort("candle_time")
        )
        _write_parquet_atomically(combined, out_path, schema=PARQUET_5MIN_SCHEMA)
        total_rows += year_df.height
    return total_rows


def _save_raw_snapshot(path: Path, candles: list[dict[str, Any]]) -> None:
    rows = [
        {key: _serialize_candle_value(value) for key, value in candle.items()} for candle in candles
    ]
    fieldnames = sorted({key for row in rows for key in row.keys()})
    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _iter_date_chunks(start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    size = max(1, int(chunk_days))
    chunks: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(end_date, cursor + timedelta(days=size - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _history_from_ts(trading_day: date) -> str:
    return datetime.combine(trading_day, time(0, 0)).strftime("%Y-%m-%d %H:%M:%S")


def _history_to_ts(trading_day: date) -> str:
    return datetime.combine(trading_day, time(23, 59, 59)).strftime("%Y-%m-%d %H:%M:%S")


def _is_non_retryable(exc: Exception) -> bool:
    """Return True for errors that should NOT be retried (auth, validation, bad input)."""
    msg = str(exc).lower()
    # kiteconnect raises these as named exception classes; also guard by message keywords
    non_retryable_keywords = ("token", "permission", "invalid", "input", "bad request")
    try:
        from kiteconnect import exceptions as kite_exc  # type: ignore[import]

        if isinstance(
            exc,
            kite_exc.TokenException | kite_exc.PermissionException | kite_exc.InputException,
        ):
            return True
    except Exception:
        pass
    return any(kw in msg for kw in non_retryable_keywords)


def _is_rate_limit(exc: Exception) -> bool:
    msg = str(exc).lower()
    try:
        from kiteconnect import exceptions as kite_exc  # type: ignore[import]

        if isinstance(exc, kite_exc.NetworkException) and ("429" in msg or "rate" in msg):
            return True
    except Exception:
        pass
    return "429" in msg or "rate limit" in msg or "too many" in msg


def _historical_data_with_retry(
    client: KiteConnect,
    instrument_token: int,
    interval: str,
    from_ts: str,
    to_ts: str,
    *,
    attempts: int = _RETRY_ATTEMPTS,
    rate_limiter: TokenBucketRateLimiter | None = None,
) -> list[dict[str, Any]]:
    last_exc: Exception | None = None
    limiter = rate_limiter or _HISTORICAL_RATE_LIMITER
    for attempt in range(1, attempts + 1):
        try:
            waited = limiter.acquire()
            if waited > 0 and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Kite historical limiter waited %.3fs for token=%s interval=%s",
                    waited,
                    instrument_token,
                    interval,
                )
            return client.historical_data(
                instrument_token,
                from_ts,
                to_ts,
                interval,
                continuous=False,
                oi=False,
            )
        except Exception as exc:
            last_exc = exc
            if _is_non_retryable(exc):
                raise KiteIngestionError(
                    f"Kite fetch non-retryable error (token/input/permission): {exc}"
                ) from exc
            if attempt >= attempts:
                break
            base = _RATE_LIMIT_BASE_DELAY if _is_rate_limit(exc) else _RETRY_BASE_DELAY
            delay = min(
                _RETRY_MAX_DELAY, base * (2 ** (attempt - 1)) + random.uniform(0, _RETRY_JITTER)
            )
            logger.warning(
                "Kite fetch attempt %d/%d failed (token=%s, %s-%s): %s — retrying in %.1fs",
                attempt,
                attempts,
                instrument_token,
                from_ts,
                to_ts,
                exc,
                delay,
            )
            time_module.sleep(delay)
    raise KiteIngestionError(
        f"Kite historical fetch failed after {attempts} attempts: {last_exc}"
    ) from last_exc


def _default_checkpoint_path(request: KiteIngestionRequest) -> Path:
    digest = hashlib.sha1(",".join(request.symbols).encode("utf-8")).hexdigest()[:8]
    # Namespace by universe so current-master and local-first checkpoints never collide
    universe_prefix = f"{request.universe}_" if request.universe != "local-first" else ""
    filename = (
        f"{universe_prefix}{request.mode}_{request.exchange.lower()}_"
        f"{request.start_date.isoformat()}_{request.end_date.isoformat()}_"
        f"{len(request.symbols)}_{digest}.json"
    )
    return get_kite_paths().checkpoint_dir / filename


def _checkpoint_baseline(request: KiteIngestionRequest) -> dict[str, Any]:
    return {
        "mode": request.mode,
        "exchange": request.exchange,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "symbols": request.symbols,
        "completed_symbols": [],
        "errors": {},
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _load_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_checkpoint(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat()
    _atomic_write_text(path, json.dumps(state, indent=2, sort_keys=True))


def _load_or_init_checkpoint(
    request: KiteIngestionRequest,
) -> tuple[Path, dict[str, Any], list[str]]:
    checkpoint_path = request.checkpoint_file or _default_checkpoint_path(request)
    if not request.resume:
        return checkpoint_path, _checkpoint_baseline(request), []
    existing = _load_checkpoint(checkpoint_path)
    if existing is None:
        state = _checkpoint_baseline(request)
        return checkpoint_path, state, []

    expected = _checkpoint_baseline(request)
    for key in ("mode", "exchange", "start_date", "end_date", "symbols"):
        if existing.get(key) != expected.get(key):
            raise KiteIngestionError(
                f"Checkpoint mismatch for {checkpoint_path}. Use --checkpoint-file for explicit override or delete the old checkpoint."
            )

    completed = [
        normalize_symbol(symbol)
        for symbol in existing.get("completed_symbols", [])
        if str(symbol).strip()
    ]
    return checkpoint_path, existing, sorted(set(completed))


def _fetch_daily_symbol(
    *,
    client: KiteConnect,
    symbol: str,
    instrument_token: int,
    start_date: date,
    end_date: date,
    chunk_days: int,
    save_raw: bool,
) -> tuple[int, int]:
    frames: list[pl.DataFrame] = []
    raw_saved = 0

    for chunk_start, chunk_end in _iter_date_chunks(start_date, end_date, chunk_days):
        candles = _historical_data_with_retry(
            client,
            instrument_token,
            "day",
            _history_from_ts(chunk_start),
            _history_to_ts(chunk_end),
        )
        if save_raw:
            raw_path = (
                get_kite_paths().daily_raw_dir
                / f"{chunk_start.isoformat()}_{chunk_end.isoformat()}"
                / f"{symbol}.csv"
            )
            _save_raw_snapshot(raw_path, candles)
            raw_saved += 1

        normalized = _normalize_daily_candles(symbol, candles)
        if normalized.is_empty():
            continue
        frames.append(normalized)

    if not frames:
        return 0, raw_saved

    combined = pl.concat(frames, how="diagonal_relaxed")
    combined = combined.sort("date").unique(subset=["symbol", "date"], keep="last")
    return _merge_daily_symbol(symbol, combined), raw_saved


def _fetch_5min_symbol(
    *,
    client: KiteConnect,
    symbol: str,
    instrument_token: int,
    start_date: date,
    end_date: date,
    chunk_days: int,
    save_raw: bool,
) -> tuple[int, int]:
    prev_close_seed = _existing_prev_close(
        symbol,
        before_ts=datetime.combine(start_date, time(0, 0), tzinfo=IST).astimezone(UTC),
    )
    frames: list[pl.DataFrame] = []
    raw_saved = 0

    for chunk_start, chunk_end in _iter_date_chunks(start_date, end_date, chunk_days):
        candles = _historical_data_with_retry(
            client,
            instrument_token,
            "5minute",
            _history_from_ts(chunk_start),
            _history_to_ts(chunk_end),
        )
        if save_raw:
            raw_path = (
                get_kite_paths().five_min_raw_dir
                / symbol
                / f"{chunk_start.isoformat()}_{chunk_end.isoformat()}.csv"
            )
            _save_raw_snapshot(raw_path, candles)
            raw_saved += 1

        normalized = _normalize_5min_candles(
            symbol,
            candles,
            prev_close_seed=prev_close_seed,
        )
        if normalized.is_empty():
            continue
        frames.append(normalized)
        prev_close_seed = float(normalized["close"][-1])

    if not frames:
        return 0, raw_saved

    combined = pl.concat(frames, how="diagonal_relaxed")
    combined = (
        combined.sort("candle_time")
        .unique(subset=["symbol", "candle_time"], keep="last")
        .sort("candle_time")
    )
    return _merge_5min_symbol(symbol, combined), raw_saved


def run_ingestion(
    request: KiteIngestionRequest,
    *,
    progress_hook: Callable[[dict[str, Any]], None] | None = None,
) -> KiteIngestionResult:
    ensure_repo_process_preflight("ingest")
    checkpoint_path, state, completed_from_checkpoint = _load_or_init_checkpoint(request)
    target_symbols = request.symbols
    skipped = completed_from_checkpoint if request.resume else []

    instrument_tokens, missing_instruments = resolve_instrument_tokens(
        target_symbols,
        exchange=request.exchange,
    )
    all_processable_symbols = [
        symbol for symbol in target_symbols if symbol not in missing_instruments
    ]
    processable_symbols = list(all_processable_symbols)

    if request.resume and completed_from_checkpoint:
        processable_symbols = [s for s in processable_symbols if s not in completed_from_checkpoint]

    if request.skip_existing:
        need_fetch, already_ingested = filter_already_ingested(
            processable_symbols,
            mode=request.mode,
            end_date=request.end_date,
        )
        if already_ingested:
            logger.info(
                "Skipping %d symbols already ingested for %s (mode=%s)",
                len(already_ingested),
                request.end_date,
                request.mode,
            )
            skipped = sorted(set(skipped) | set(already_ingested))
            processable_symbols = need_fetch

    errors: dict[str, str] = dict(state.get("errors", {}))
    completed_symbols = list(completed_from_checkpoint)
    rows_written = 0
    raw_snapshot_count = 0
    completed_since_flush = 0
    started_at = time_module.perf_counter()
    total_processable = len(processable_symbols)

    logger.info(
        "Kite %s ingestion starting: %d symbols, %s to %s (exchange=%s, resume=%s)",
        request.mode,
        total_processable,
        request.start_date,
        request.end_date,
        request.exchange,
        request.resume,
    )

    def _emit_progress(
        *,
        symbol: str | None,
        status: str,
        processed_count: int,
        rows_for_symbol: int = 0,
        raw_for_symbol: int = 0,
        error: str | None = None,
    ) -> None:
        if progress_hook is None:
            return
        progress_hook(
            {
                "mode": request.mode,
                "symbol": symbol,
                "status": status,
                "processed_count": processed_count,
                "total_processable": total_processable,
                "requested_count": len(target_symbols),
                "completed_count": len(completed_symbols),
                "rows_written": rows_written,
                "rows_for_symbol": rows_for_symbol,
                "raw_snapshot_count": raw_snapshot_count,
                "raw_for_symbol": raw_for_symbol,
                "missing_instruments_count": len(missing_instruments),
                "errors_count": len(errors),
                "elapsed_sec": round(time_module.perf_counter() - started_at, 2),
                "checkpoint_path": str(checkpoint_path),
                "error": error,
            }
        )

    _emit_progress(symbol=None, status="start", processed_count=0)

    if not processable_symbols:
        state["completed_symbols"] = sorted(set(completed_symbols))
        state["errors"] = errors
        _write_checkpoint(checkpoint_path, state)
        checkpoint_path.unlink(missing_ok=True)
        logger.info(
            "Kite %s ingestion skipped: all %d processable symbols already covered for %s",
            request.mode,
            len(all_processable_symbols),
            request.end_date,
        )
        _emit_progress(symbol=None, status="skipped_existing", processed_count=0)
        _emit_progress(symbol=None, status="finished", processed_count=0)
        return KiteIngestionResult(
            mode=request.mode,
            start_date=request.start_date.isoformat(),
            end_date=request.end_date.isoformat(),
            exchange=request.exchange,
            requested_symbols=target_symbols,
            completed_symbols=sorted(set(completed_symbols)),
            skipped_symbols=skipped,
            missing_instruments=missing_instruments,
            errors=errors,
            rows_written=0,
            raw_snapshot_count=0,
            checkpoint_path=None,
            checkpoint_cleared=True,
        )

    client = get_kite_client()

    for index, symbol in enumerate(processable_symbols, start=1):
        token = instrument_tokens[symbol]
        try:
            if request.mode == "daily":
                symbol_rows, raw_saved = _fetch_daily_symbol(
                    client=client,
                    symbol=symbol,
                    instrument_token=token,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    chunk_days=request.daily_chunk_days,
                    save_raw=request.save_raw,
                )
            else:
                symbol_rows, raw_saved = _fetch_5min_symbol(
                    client=client,
                    symbol=symbol,
                    instrument_token=token,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    chunk_days=request.five_min_chunk_days,
                    save_raw=request.save_raw,
                )
            rows_written += symbol_rows
            raw_snapshot_count += raw_saved
            completed_symbols.append(symbol)
            completed_since_flush += 1
            errors.pop(symbol, None)
            _emit_progress(
                symbol=symbol,
                status="completed",
                processed_count=index,
                rows_for_symbol=symbol_rows,
                raw_for_symbol=raw_saved,
            )
            if index == 1 or index % PROGRESS_LOG_EVERY == 0 or index == total_processable:
                elapsed = time_module.perf_counter() - started_at
                rate = index / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Kite %s progress %d/%d rows=%d errors=%d elapsed=%.1fs rate=%.2f sym/s",
                    request.mode,
                    index,
                    total_processable,
                    rows_written,
                    len(errors),
                    elapsed,
                    rate,
                )
        except Exception as exc:
            errors[symbol] = str(exc)
            logger.error("Kite %s failed for %s: %s", request.mode, symbol, exc)
            _emit_progress(
                symbol=symbol,
                status="error",
                processed_count=index,
                error=str(exc),
            )

        if completed_since_flush >= CHECKPOINT_FLUSH_EVERY:
            state["completed_symbols"] = sorted(set(completed_symbols))
            state["errors"] = errors
            _write_checkpoint(checkpoint_path, state)
            completed_since_flush = 0
            logger.info(
                "Persisted Kite %s checkpoint after %d processed symbols: %s",
                request.mode,
                index,
                checkpoint_path,
            )
            _emit_progress(
                symbol=None,
                status="checkpoint_flushed",
                processed_count=index,
            )

    state["completed_symbols"] = sorted(set(completed_symbols))
    state["errors"] = errors
    _write_checkpoint(checkpoint_path, state)

    expected_completed = sorted(set(all_processable_symbols))
    checkpoint_cleared = False
    if (
        not errors
        and not missing_instruments
        and sorted(set(completed_symbols)) == sorted(set(expected_completed))
    ):
        checkpoint_path.unlink(missing_ok=True)
        checkpoint_cleared = True

    if checkpoint_cleared:
        logger.info(
            "Kite %s ingestion complete: %d rows, checkpoint cleared", request.mode, rows_written
        )
    else:
        logger.warning(
            "Kite %s ingestion finished with issues: %d errors, %d missing instruments",
            request.mode,
            len(errors),
            len(missing_instruments),
        )

    _emit_progress(
        symbol=None,
        status="finished",
        processed_count=total_processable,
    )

    return KiteIngestionResult(
        mode=request.mode,
        start_date=request.start_date.isoformat(),
        end_date=request.end_date.isoformat(),
        exchange=request.exchange,
        requested_symbols=target_symbols,
        completed_symbols=sorted(set(completed_symbols)),
        skipped_symbols=skipped,
        missing_instruments=missing_instruments,
        errors=errors,
        rows_written=rows_written,
        raw_snapshot_count=raw_snapshot_count,
        checkpoint_path=None if checkpoint_cleared else str(checkpoint_path),
        checkpoint_cleared=checkpoint_cleared,
    )


def refresh_runtime_tables(force: bool = True, symbols: list[str] | None = None) -> None:
    """Rebuild local DuckDB runtime tables after parquet ingestion.

    When symbols are provided, only that symbol subset is rebuilt. This keeps
    incremental ingest refreshes fast and lets index backfills rebuild just the
    affected symbols.
    """
    from db.duckdb import close_db, get_db

    close_db()
    db = get_db()
    db.build_all(force=force, symbols=sorted(set(symbols)) if symbols else None)


def summarize_result(result: KiteIngestionResult) -> str:
    lines = [
        f"mode={result.mode}",
        f"window={result.start_date}..{result.end_date}",
        f"exchange={result.exchange}",
        f"requested_symbols={len(result.requested_symbols)}",
        f"completed_symbols={len(result.completed_symbols)}",
        f"rows_written={result.rows_written}",
        f"raw_snapshots={result.raw_snapshot_count}",
        f"checkpoint_cleared={result.checkpoint_cleared}",
    ]
    if result.skipped_symbols:
        lines.append(f"skipped={preview_list(result.skipped_symbols)}")
    if result.missing_instruments:
        lines.append(f"missing_instruments={preview_list(result.missing_instruments)}")
    if result.errors:
        lines.append(f"errors={len(result.errors)}")
    if result.checkpoint_path:
        lines.append(f"checkpoint={result.checkpoint_path}")
    return "\n".join(lines)
