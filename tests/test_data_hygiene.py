"""Tests for the data hygiene pipeline."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

import scripts.data_hygiene as hygiene


def _make_instrument_csv(path: Path, symbols: list[str], *, segment: str = "NSE") -> None:
    """Helper: write a minimal instrument master CSV."""
    header = (
        "exchange,exchange_token,expiry,instrument_token,instrument_type,"
        "last_price,lot_size,name,segment,strike,tick_size,tradingsymbol"
    )
    lines = [header]
    for i, sym in enumerate(symbols):
        lines.append(f"NSE,{i},,{100000 + i},EQ,100,1,{sym},{segment},0,0.05,{sym}")
    path.write_text("\n".join(lines) + "\n")


def _make_parquet_dirs(root: Path, symbols: list[str]) -> None:
    """Helper: create empty parquet symbol directories."""
    for mode in ("5min", "daily"):
        for sym in symbols:
            (root / mode / sym).mkdir(parents=True, exist_ok=True)


def _patch_kite_paths(monkeypatch, inst_dir: Path, parquet_root: Path) -> None:
    """Monkeypatch get_kite_paths for both kite_ingestion and data_hygiene modules.

    Also bypasses the NSE equity allowlist so synthetic test symbols pass through
    without requiring data/NSE_EQUITY_SYMBOLS.csv to contain them.
    """
    fake = type("P", (), {"instrument_dir": inst_dir, "parquet_root": parquet_root})()
    monkeypatch.setattr("engine.kite_ingestion.get_kite_paths", lambda: fake)
    monkeypatch.setattr("scripts.data_hygiene.get_kite_paths", lambda: fake)
    monkeypatch.setattr("engine.kite_ingestion._load_nse_equity_allowlist", lambda: None)


class TestDetectDeadSymbols:
    def test_finds_dead_symbols(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        live = [f"SYM{i}" for i in range(1100)]
        _make_instrument_csv(inst_dir / "NSE.csv", live)

        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, [*live, "DEADCO", "GONEINC"])
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        dead = hygiene.detect_dead_symbols()
        assert dead == {"DEADCO", "GONEINC"}

    def test_returns_empty_when_all_tradeable(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        live = [f"SYM{i}" for i in range(1100)]
        _make_instrument_csv(inst_dir / "NSE.csv", live)

        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, live[:50])
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        dead = hygiene.detect_dead_symbols()
        assert dead == set()

    def test_rejects_small_instrument_master(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        _make_instrument_csv(inst_dir / "NSE.csv", [f"SYM{i}" for i in range(50)])

        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, [f"SYM{i}" for i in range(60)])
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        with pytest.raises(SystemExit, match="too few"):
            hygiene.detect_dead_symbols()

    def test_rejects_missing_instrument_master(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        # No CSV created
        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, ["SBIN"])
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        with pytest.raises(SystemExit, match="Instrument master CSV not found"):
            hygiene.detect_dead_symbols()


class TestDryRun:
    def test_dry_run_does_not_delete(self, tmp_path, monkeypatch, capsys):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        live = [f"SYM{i}" for i in range(1100)]
        _make_instrument_csv(inst_dir / "NSE.csv", live)

        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, [*live, "DEAD1"])
        (parquet_root / "5min" / "DEAD1" / "2024.parquet").write_bytes(b"x" * 1000)
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        # Mock DuckDB to avoid real DB access
        class FakeResult:
            def fetchone(self):
                return (42,)

        class FakeCon:
            def execute(self, *a, **kw):
                return FakeResult()

        class FakeDB:
            con = FakeCon()

        monkeypatch.setattr("db.duckdb.get_db", lambda: FakeDB())

        dead = hygiene.detect_dead_symbols()
        hygiene.dry_run(dead)

        # Parquet dirs must still exist
        assert (parquet_root / "5min" / "DEAD1").exists()
        assert (parquet_root / "daily" / "DEAD1").exists()

        output = capsys.readouterr().out
        assert "DEAD1" in output
        assert "would be purged" in output


class TestPurge:
    def test_purge_deletes_parquet_and_calls_duckdb(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        live = [f"SYM{i}" for i in range(1100)]
        _make_instrument_csv(inst_dir / "NSE.csv", live)

        parquet_root = tmp_path / "parquet"
        _make_parquet_dirs(parquet_root, [*live[:10], "DEAD1", "DEAD2"])
        for sym in ["DEAD1", "DEAD2"]:
            (parquet_root / "5min" / sym / "2024.parquet").write_bytes(b"fake")
            (parquet_root / "daily" / sym / "all.parquet").write_bytes(b"fake")

        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        executed_sql: list[str] = []

        class FakeResult:
            def fetchone(self):
                return (10,)

        class FakeCon:
            def execute(self, sql, *a, **kw):
                executed_sql.append(sql)
                return FakeResult()

        meta_rebuilt = []

        class FakeDB:
            con = FakeCon()

            def _build_dataset_meta(self):
                meta_rebuilt.append(True)

        monkeypatch.setattr("db.duckdb.get_db", lambda: FakeDB())
        monkeypatch.chdir(tmp_path)

        dead = hygiene.detect_dead_symbols()
        assert dead == {"DEAD1", "DEAD2"}
        hygiene.purge(dead)

        # Parquet dirs should be gone
        assert not (parquet_root / "5min" / "DEAD1").exists()
        assert not (parquet_root / "5min" / "DEAD2").exists()
        assert not (parquet_root / "daily" / "DEAD1").exists()
        assert not (parquet_root / "daily" / "DEAD2").exists()
        # Live symbols untouched
        assert (parquet_root / "5min" / "SYM0").exists()
        assert (parquet_root / "daily" / "SYM0").exists()
        # DuckDB was called with BEGIN/DELETE/COMMIT
        assert any("BEGIN" in s for s in executed_sql)
        assert any("DELETE" in s for s in executed_sql)
        assert any("COMMIT" in s for s in executed_sql)
        # dataset_meta was rebuilt
        assert meta_rebuilt == [True]

    def test_purge_no_dead_symbols_is_noop(self, capsys):
        hygiene.purge(set())
        output = capsys.readouterr().out
        assert "No dead symbols to purge" in output


class TestCLI:
    def test_purge_without_confirm_exits(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["pivot-hygiene", "--purge"])
        with pytest.raises(SystemExit):
            hygiene.main()


class TestCheckStale:
    def test_check_stale_refreshes_and_deactivates_old_issues(self, monkeypatch, capsys):
        calls: list[tuple[str, str, tuple[str, ...]]] = []

        class FakeDB:
            def ensure_data_quality_table(self):
                calls.append(("ensure", "", ()))

            def upsert_data_quality_issues(self, symbols, issue_code, details):
                calls.append(("upsert", issue_code, tuple(symbols)))
                return len(symbols)

            def deactivate_data_quality_issue(self, issue_code, keep_symbols):
                calls.append(("deactivate", issue_code, tuple(keep_symbols)))
                return len(keep_symbols)

        monkeypatch.setattr(hygiene, "detect_short_history", lambda: {"SBIN", "TCS"})
        monkeypatch.setattr(hygiene, "detect_illiquid", lambda: {"RELIANCE"})
        monkeypatch.setattr("db.duckdb.get_db", lambda: FakeDB())

        hygiene.check_stale()

        out = capsys.readouterr().out
        assert "SHORT_HISTORY: 2 active, 2 upserted" in out
        assert "ILLIQUID: 1 active, 1 upserted" in out
        assert ("deactivate", "SHORT_HISTORY", ("SBIN", "TCS")) in calls
        assert ("deactivate", "ILLIQUID", ("RELIANCE",)) in calls


class TestRepairInvalid5MinSessionRows:
    def test_dry_run_reports_invalid_rows_without_writing(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        parquet_root = tmp_path / "parquet"
        path = parquet_root / "5min" / "SBIN" / "2025.parquet"
        path.parent.mkdir(parents=True)
        df = pl.DataFrame(
            {
                "candle_time": [
                    datetime(2025, 4, 1, 9, 15),
                    datetime(2025, 4, 1, 15, 35),
                ],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                "volume": [10, 11],
                "true_range": [2.0, 2.0],
                "date": [date(2025, 4, 1), date(2025, 4, 1)],
                "symbol": ["SBIN", "SBIN"],
            }
        )
        df.write_parquet(path)
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)

        results = hygiene.repair_invalid_5min_session_rows(
            symbols=["SBIN"],
            start_date=date(2025, 4, 1),
            end_date=date(2025, 4, 1),
            apply=False,
        )

        assert len(results) == 1
        assert results[0].invalid_rows == 1
        assert pl.read_parquet(path).height == 2

    def test_apply_removes_invalid_rows_and_writes_backup(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        parquet_root = tmp_path / "parquet"
        path = parquet_root / "5min" / "SBIN" / "2025.parquet"
        path.parent.mkdir(parents=True)
        df = pl.DataFrame(
            {
                "candle_time": [
                    datetime(2025, 4, 1, 9, 15),
                    datetime(2025, 4, 1, 20, 55),
                ],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                "volume": [10, 11],
                "true_range": [2.0, 2.0],
                "date": [date(2025, 4, 1), date(2025, 4, 1)],
                "symbol": ["SBIN", "SBIN"],
            }
        )
        df.write_parquet(path)
        _patch_kite_paths(monkeypatch, inst_dir, parquet_root)
        monkeypatch.chdir(tmp_path)

        results = hygiene.repair_invalid_5min_session_rows(
            symbols=["SBIN"],
            start_date=date(2025, 4, 1),
            end_date=date(2025, 4, 1),
            apply=True,
        )

        assert len(results) == 1
        repaired = pl.read_parquet(path)
        assert repaired.height == 1
        assert repaired["candle_time"].to_list() == [datetime(2025, 4, 1, 9, 15)]
        assert results[0].backup_path is not None
        assert results[0].backup_path.exists()
        assert pl.read_parquet(results[0].backup_path).height == 2


class TestTradeableSymbols:
    def test_filters_nse_segment(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        csv_content = (
            "exchange,exchange_token,expiry,instrument_token,instrument_type,"
            "last_price,lot_size,name,segment,strike,tick_size,tradingsymbol\n"
            "NSE,1,,100001,EQ,100,1,Reliance,NSE,0,0.05,RELIANCE\n"
            "NSE,2,,100002,EQ,200,1,TCS,NSE,0,0.05,TCS\n"
            "NSE,3,,100003,INDEX,0,1,Nifty 50,INDICES,0,0.05,NIFTY 50\n"
        )
        (inst_dir / "NSE.csv").write_text(csv_content)
        monkeypatch.setattr(
            "engine.kite_ingestion.get_kite_paths",
            lambda: type("P", (), {"instrument_dir": inst_dir, "parquet_root": tmp_path})(),
        )
        from engine.kite_ingestion import tradeable_symbols

        result = tradeable_symbols()
        assert result == {"RELIANCE", "TCS"}
        assert "NIFTY 50" not in result

    def test_returns_none_when_csv_missing(self, tmp_path, monkeypatch):
        inst_dir = tmp_path / "instruments"
        inst_dir.mkdir()
        monkeypatch.setattr(
            "engine.kite_ingestion.get_kite_paths",
            lambda: type("P", (), {"instrument_dir": inst_dir, "parquet_root": tmp_path})(),
        )
        from engine.kite_ingestion import tradeable_symbols

        result = tradeable_symbols()
        assert result is None
