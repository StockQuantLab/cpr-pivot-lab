from __future__ import annotations

from datetime import date, datetime

import polars as pl

import db.duckdb as duckdb_mod
from db.duckdb import MarketDB


def test_incremental_pack_uses_prior_days_for_rvol_baseline(tmp_path, monkeypatch) -> None:
    parquet_root = tmp_path / "parquet"
    path = parquet_root / "5min" / "SBIN" / "2026.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "candle_time": [
                datetime(2026, 5, 5, 9, 15),
                datetime(2026, 5, 6, 9, 15),
            ],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [100.0, 250.0],
            "true_range": [2.0, 2.0],
            "date": [date(2026, 5, 5), date(2026, 5, 6)],
            "symbol": ["SBIN", "SBIN"],
        }
    ).write_parquet(path)
    monkeypatch.setattr(duckdb_mod, "PARQUET_DIR", parquet_root)

    with MarketDB(db_path=tmp_path / "market.duckdb") as db:
        db.build_intraday_day_pack(
            symbols=["SBIN"],
            rvol_lookback_days=1,
            batch_size=1,
            since_date="2026-05-06",
            until_date="2026-05-06",
        )
        rows = db.con.execute(
            """
            SELECT trade_date, rvol_baseline_arr
            FROM intraday_day_pack
            ORDER BY trade_date
            """
        ).fetchall()

    assert len(rows) == 1
    assert str(rows[0][0]) == "2026-05-06"
    assert rows[0][1] == [100.0]
