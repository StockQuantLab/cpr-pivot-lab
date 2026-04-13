from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

import db.duckdb as duckdb_mod
from db.duckdb import MarketDB


def _write_daily(path: Path, close_value: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["SBIN", "SBIN"],
            "date": [date(2026, 3, 29), date(2026, 3, 30)],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, close_value],
            "volume": [1000, 1200],
        }
    ).write_parquet(path)


def test_v_daily_prefers_kite_overlay_over_all_parquet(tmp_path: Path, monkeypatch) -> None:
    parquet_root = tmp_path / "parquet"
    _write_daily(parquet_root / "daily" / "SBIN" / "all.parquet", 101.5)
    _write_daily(parquet_root / "daily" / "SBIN" / "kite.parquet", 103.25)

    monkeypatch.setattr(duckdb_mod, "PARQUET_DIR", parquet_root)

    seed = MarketDB(db_path=tmp_path / "overlay.duckdb")
    seed.close()
    db = MarketDB(db_path=tmp_path / "overlay.duckdb", read_only=True)
    row = db.con.execute(
        "SELECT close FROM v_daily WHERE symbol = 'SBIN' AND date = '2026-03-30'"
    ).fetchone()

    assert row is not None
    assert float(row[0]) == 103.25
    db.close()
