from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

import engine.kite_ingestion as kite_ingestion
from engine.kite_ingestion import (
    KiteIngestionRequest,
    KitePaths,
    _compute_true_range,
    _default_checkpoint_path,
    _historical_data_with_retry,
    _merge_5min_symbol,
    _merge_daily_symbol,
    compact_daily_overlays,
    detect_repo_process_conflicts,
    filter_already_ingested,
    resolve_date_window,
    resolve_missing_ingest_symbols,
    resolve_target_symbols,
    run_ingestion,
)


def _tmp_kite_paths(tmp_path: Path) -> KitePaths:
    return KitePaths(
        parquet_root=tmp_path / "parquet",
        raw_root=tmp_path / "raw",
        instrument_dir=tmp_path / "raw" / "kite" / "instruments",
        daily_raw_dir=tmp_path / "raw" / "kite" / "daily",
        five_min_raw_dir=tmp_path / "raw" / "kite" / "5min",
        checkpoint_dir=tmp_path / "raw" / "kite" / "checkpoints",
    )


def test_resolve_date_window_requires_single_mode() -> None:
    with pytest.raises(kite_ingestion.KiteIngestionError):
        resolve_date_window(
            today=False,
            one_date=None,
            start_date="2026-03-10",
            end_date=None,
        )


def test_compute_true_range_uses_prev_close_seed() -> None:
    df = pl.DataFrame(
        {
            "candle_time": [
                datetime(2026, 3, 10, 9, 15),
                datetime(2026, 3, 10, 9, 20),
            ],
            "open": [100.0, 104.0],
            "high": [105.0, 108.0],
            "low": [99.0, 103.0],
            "close": [104.0, 107.0],
            "volume": [1000, 900],
        }
    )

    result = _compute_true_range(df, prev_close_seed=98.0)

    assert result["true_range"].to_list() == [7.0, 5.0]


def test_merge_daily_symbol_deduplicates_by_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: _tmp_kite_paths(tmp_path))

    initial = pl.DataFrame(
        {
            "symbol": ["SBIN"],
            "date": [date(2026, 3, 9)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    _merge_daily_symbol("SBIN", initial)

    update = pl.DataFrame(
        {
            "symbol": ["SBIN", "SBIN"],
            "date": [date(2026, 3, 9), date(2026, 3, 10)],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [98.5, 100.0],
            "close": [101.25, 102.5],
            "volume": [1500, 2000],
        }
    )
    written_rows = _merge_daily_symbol("SBIN", update)

    assert written_rows == 2
    out_path = tmp_path / "parquet" / "daily" / "SBIN" / "kite.parquet"
    merged = pl.read_parquet(out_path).sort("date")
    assert merged.height == 2
    assert merged["close"].to_list() == [101.25, 102.5]


def test_filter_already_ingested_daily_checks_all_daily_parquet_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: _tmp_kite_paths(tmp_path))
    daily_dir = tmp_path / "parquet" / "daily" / "SBIN"
    daily_dir.mkdir(parents=True, exist_ok=True)

    baseline = pl.DataFrame(
        {
            "symbol": ["SBIN"],
            "date": [date(2026, 3, 29)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    overlay = pl.DataFrame(
        {
            "symbol": ["SBIN"],
            "date": [date(2026, 3, 30)],
            "open": [101.0],
            "high": [102.0],
            "low": [100.0],
            "close": [101.5],
            "volume": [1200],
        }
    )
    baseline.write_parquet(daily_dir / "all.parquet")
    overlay.write_parquet(daily_dir / "kite.parquet")

    need_fetch, already_done = filter_already_ingested(
        ["SBIN"],
        mode="daily",
        end_date=date(2026, 3, 30),
    )

    assert need_fetch == []
    assert already_done == ["SBIN"]


def test_compact_daily_overlays_merges_into_all_and_removes_overlay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: _tmp_kite_paths(tmp_path))
    monkeypatch.setattr(kite_ingestion, "detect_repo_process_conflicts", lambda command: [])

    daily_dir = tmp_path / "parquet" / "daily" / "SBIN"
    daily_dir.mkdir(parents=True, exist_ok=True)
    baseline = pl.DataFrame(
        {
            "symbol": ["SBIN"],
            "date": [date(2026, 3, 29)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    overlay = pl.DataFrame(
        {
            "symbol": ["SBIN"],
            "date": [date(2026, 3, 30)],
            "open": [101.0],
            "high": [102.0],
            "low": [100.0],
            "close": [101.5],
            "volume": [1200],
        }
    )
    baseline.write_parquet(daily_dir / "all.parquet")
    overlay.write_parquet(daily_dir / "kite.parquet")

    result = compact_daily_overlays(["SBIN"])

    assert result.compacted_symbols == ["SBIN"]
    assert result.skipped_symbols == []
    assert result.rows_written == 2
    assert (daily_dir / "kite.parquet").exists() is False
    merged = pl.read_parquet(daily_dir / "all.parquet").sort("date")
    assert merged["close"].to_list() == [100.5, 101.5]


def test_merge_5min_symbol_writes_year_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: _tmp_kite_paths(tmp_path))

    df = pl.DataFrame(
        {
            "candle_time": [
                datetime(2026, 3, 10, 9, 15),
                datetime(2026, 3, 10, 9, 20),
            ],
            "open": [100.0, 101.0],
            "high": [105.0, 106.0],
            "low": [99.0, 100.0],
            "close": [104.0, 105.0],
            "volume": [1000, 1100],
            "true_range": [6.0, 6.0],
            "date": [date(2026, 3, 10), date(2026, 3, 10)],
            "symbol": ["SBIN", "SBIN"],
        }
    )

    written_rows = _merge_5min_symbol("SBIN", df)

    assert written_rows == 2
    out_path = tmp_path / "parquet" / "5min" / "SBIN" / "2026.parquet"
    merged = pl.read_parquet(out_path).sort("candle_time")
    assert merged.height == 2
    assert merged.columns == [
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


def test_run_ingestion_resume_skips_completed_symbols(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _tmp_kite_paths(tmp_path)
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)
    checkpoint_path = paths.checkpoint_dir / "resume.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        (
            "{\n"
            '  "mode": "daily",\n'
            '  "exchange": "NSE",\n'
            '  "start_date": "2026-03-10",\n'
            '  "end_date": "2026-03-10",\n'
            '  "symbols": ["RELIANCE", "SBIN"],\n'
            '  "completed_symbols": ["RELIANCE"],\n'
            '  "errors": {}\n'
            "}"
        ),
        encoding="utf-8",
    )

    seen: list[str] = []

    monkeypatch.setattr(
        kite_ingestion,
        "resolve_instrument_tokens",
        lambda symbols, exchange="NSE": (
            {symbol: index + 1 for index, symbol in enumerate(symbols)},
            [],
        ),
    )
    monkeypatch.setattr(kite_ingestion, "get_kite_client", lambda: object())

    def _fake_fetch_daily_symbol(**kwargs):
        seen.append(kwargs["symbol"])
        return 1, 0

    monkeypatch.setattr(kite_ingestion, "_fetch_daily_symbol", _fake_fetch_daily_symbol)

    request = KiteIngestionRequest(
        mode="daily",
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 10),
        exchange="NSE",
        symbols=["RELIANCE", "SBIN"],
        resume=True,
        checkpoint_file=checkpoint_path,
    )

    result = run_ingestion(request)

    assert seen == ["SBIN"]
    assert result.skipped_symbols == ["RELIANCE"]
    assert result.completed_symbols == ["RELIANCE", "SBIN"]
    assert result.checkpoint_cleared is True
    assert checkpoint_path.exists() is False


def _make_instrument_csv(path: Path, symbols: list[str], segment: str = "NSE") -> None:
    header = (
        "exchange,exchange_token,expiry,instrument_token,instrument_type,"
        "last_price,lot_size,name,segment,strike,tick_size,tradingsymbol"
    )
    lines = [header]
    for i, sym in enumerate(symbols):
        lines.append(f"NSE,{i},,{100000 + i},EQ,100,1,{sym},{segment},0,0.05,{sym}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


class TestResolveTargetSymbolsCurrentMaster:
    def test_current_master_uses_instrument_master_not_parquet(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        paths = _tmp_kite_paths(tmp_path)
        monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)
        # Bypass the NSE equity allowlist so synthetic test symbols pass through
        monkeypatch.setattr(kite_ingestion, "_load_nse_equity_allowlist", lambda: None)

        # Instrument master has ALPHA and BETA
        _make_instrument_csv(paths.instrument_dir / "NSE.csv", ["ALPHA", "BETA"])

        # Parquet dirs only have GAMMA (not in instrument master)
        (paths.parquet_root / "daily" / "GAMMA").mkdir(parents=True)
        (paths.parquet_root / "daily" / "GAMMA" / "all.parquet").write_bytes(b"fake")

        result = resolve_target_symbols(universe="current-master", exchange="NSE")

        assert set(result) == {"ALPHA", "BETA"}
        assert "GAMMA" not in result

    def test_local_first_uses_parquet_dirs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        paths = _tmp_kite_paths(tmp_path)
        monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)

        _make_instrument_csv(paths.instrument_dir / "NSE.csv", ["ALPHA", "BETA"])
        (paths.parquet_root / "daily" / "ALPHA").mkdir(parents=True)
        (paths.parquet_root / "daily" / "ALPHA" / "all.parquet").write_bytes(b"fake")

        result = resolve_target_symbols(
            universe="local-first", exchange="NSE", tradeable_only=False
        )

        assert result == ["ALPHA"]
        assert "BETA" not in result


class TestCheckpointNamespacing:
    def test_current_master_checkpoint_differs_from_local_first(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        paths = _tmp_kite_paths(tmp_path)
        monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)

        base = {
            "mode": "daily",
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 3, 31),
            "exchange": "NSE",
            "symbols": ["RELIANCE", "SBIN"],
        }
        req_local = KiteIngestionRequest(**base, universe="local-first")
        req_master = KiteIngestionRequest(**base, universe="current-master")

        path_local = _default_checkpoint_path(req_local)
        path_master = _default_checkpoint_path(req_master)

        assert path_local != path_master
        assert "current-master" in path_master.name
        assert "current-master" not in path_local.name


class TestResolveMissingIngestSymbols:
    """resolve_missing_ingest_symbols must check the correct parquet sub-directory per mode."""

    def _setup(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> KitePaths:
        paths = _tmp_kite_paths(tmp_path)
        monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)
        monkeypatch.setattr(kite_ingestion, "_load_nse_equity_allowlist", lambda: None)
        return paths

    @staticmethod
    def _make_parquet_dir(root: Path, symbol: str) -> None:
        d = root / symbol
        d.mkdir(parents=True, exist_ok=True)
        (d / "all.parquet").write_bytes(b"fake")

    def test_daily_mode_checks_daily_subdir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        paths = self._setup(tmp_path, monkeypatch)
        _make_instrument_csv(paths.instrument_dir / "NSE.csv", ["SBIN", "TCS", "INFY"])

        # Only SBIN has daily parquet; TCS and INFY are missing
        self._make_parquet_dir(paths.parquet_root / "daily", "SBIN")

        result = resolve_missing_ingest_symbols(exchange="NSE", mode="daily")

        assert set(result) == {"TCS", "INFY"}
        assert "SBIN" not in result

    def test_5min_mode_checks_5min_subdir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        paths = self._setup(tmp_path, monkeypatch)
        _make_instrument_csv(paths.instrument_dir / "NSE.csv", ["SBIN", "TCS"])

        # Both symbols have daily parquet
        self._make_parquet_dir(paths.parquet_root / "daily", "SBIN")
        self._make_parquet_dir(paths.parquet_root / "daily", "TCS")
        # Only TCS has 5-min parquet; SBIN does not
        self._make_parquet_dir(paths.parquet_root / "5min", "TCS")

        result = resolve_missing_ingest_symbols(exchange="NSE", mode="5min")

        # SBIN is present in daily but absent from 5min → must appear as missing
        assert result == ["SBIN"]
        assert "TCS" not in result


def test_run_ingestion_emits_progress_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _tmp_kite_paths(tmp_path)
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)
    monkeypatch.setattr(
        kite_ingestion,
        "resolve_instrument_tokens",
        lambda symbols, exchange="NSE": (
            {symbol: index + 1 for index, symbol in enumerate(symbols)},
            [],
        ),
    )
    monkeypatch.setattr(kite_ingestion, "get_kite_client", lambda: object())
    monkeypatch.setattr(kite_ingestion, "_fetch_daily_symbol", lambda **kwargs: (2, 0))

    events: list[dict[str, object]] = []
    request = KiteIngestionRequest(
        mode="daily",
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 10),
        exchange="NSE",
        symbols=["RELIANCE", "SBIN"],
    )

    result = run_ingestion(request, progress_hook=events.append)

    assert result.checkpoint_cleared is True
    assert [event["status"] for event in events] == ["start", "completed", "completed", "finished"]
    assert events[1]["symbol"] == "RELIANCE"
    assert events[2]["processed_count"] == 2


def test_run_ingestion_blocks_on_conflicting_repo_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _tmp_kite_paths(tmp_path)
    monkeypatch.setattr(kite_ingestion, "get_kite_paths", lambda: paths)
    monkeypatch.setattr(
        kite_ingestion,
        "detect_repo_process_conflicts",
        lambda command: [
            kite_ingestion.RuntimeProcessConflict(
                pid=1234,
                name="pivot-paper-trading.exe",
                command_line="pivot-paper-trading daily-live --all-symbols",
            )
        ],
    )

    request = KiteIngestionRequest(
        mode="daily",
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 10),
        exchange="NSE",
        symbols=["SBIN"],
    )

    with pytest.raises(kite_ingestion.KiteIngestionError) as exc:
        run_ingestion(request)

    assert "Active repo processes would conflict with ingest on Windows." in str(exc.value)
    assert "pivot-paper-trading daily-live --all-symbols" in str(exc.value)


def test_historical_data_with_retry_uses_rate_limiter() -> None:
    class _Limiter:
        def __init__(self) -> None:
            self.calls = 0

        def acquire(self, tokens: float = 1.0) -> float:
            self.calls += 1
            return 0.0

    class _Client:
        def historical_data(self, *args, **kwargs):
            return [
                {
                    "date": "2026-03-30T09:15:00+05:30",
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100.5,
                    "volume": 1000,
                }
            ]

    limiter = _Limiter()

    rows = _historical_data_with_retry(
        _Client(),
        12345,
        "day",
        "2026-03-30 00:00:00",
        "2026-03-30 23:59:59",
        rate_limiter=limiter,
    )

    assert limiter.calls == 1
    assert len(rows) == 1


def test_detect_repo_process_conflicts_ignores_its_own_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(
        [
            {
                "ProcessId": 111,
                "Name": "powershell.exe",
                "CommandLine": (
                    "powershell.exe -NoProfile -Command "
                    "\"$ErrorActionPreference='Stop';Get-CimInstance Win32_Process | "
                    'Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress"'
                ),
            },
            {
                "ProcessId": 222,
                "Name": "pivot-paper-trading.exe",
                "CommandLine": "pivot-paper-trading daily-live --all-symbols",
            },
        ]
    )

    class _Completed:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    monkeypatch.setattr(kite_ingestion.os, "name", "nt")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(
        kite_ingestion.subprocess,
        "run",
        lambda *args, **kwargs: _Completed(payload),
    )

    conflicts = detect_repo_process_conflicts("ingest")

    assert len(conflicts) == 1
    assert conflicts[0].pid == 222
