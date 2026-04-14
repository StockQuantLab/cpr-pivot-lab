from __future__ import annotations

from pathlib import Path

import polars as pl

from db.paper_db import PaperDB
from scripts.paper_feed_audit import compare_feed_audit


class _FakeRelation:
    def __init__(self, frame: pl.DataFrame):
        self._frame = frame

    def pl(self) -> pl.DataFrame:
        return self._frame


class _FakeCon:
    def __init__(self, frame: pl.DataFrame):
        self._frame = frame

    def execute(self, query: str, params=None):
        del query, params
        return _FakeRelation(self._frame)


class _FakeMarketDB:
    def __init__(self, frame: pl.DataFrame):
        self.con = _FakeCon(frame)

    def _table_has_column(self, table: str, column: str) -> bool:
        del table
        return column == "minute_arr"


def _make_pack_frame(*, close: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["MANOMAY"],
            "trade_date": ["2026-04-13"],
            "pack_time_arr": [[565]],
            "open_arr": [[221.49]],
            "high_arr": [[222.0]],
            "low_arr": [[220.5]],
            "close_arr": [[close]],
            "volume_arr": [[12345.0]],
        }
    )


def test_compare_feed_audit_passes_on_matching_pack(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        db.create_session(
            session_id="paper-audit",
            status="COMPLETED",
            trade_date="2026-04-13",
        )
        db.upsert_feed_audit_rows(
            [
                {
                    "session_id": "paper-audit",
                    "trade_date": "2026-04-13",
                    "feed_source": "kite",
                    "transport": "websocket",
                    "symbol": "MANOMAY",
                    "bar_start": "2026-04-13 09:20:00",
                    "bar_end": "2026-04-13 09:25:00",
                    "open": 221.49,
                    "high": 222.0,
                    "low": 220.5,
                    "close": 221.49,
                    "volume": 12345.0,
                    "first_snapshot_ts": "2026-04-13 09:20:01",
                    "last_snapshot_ts": "2026-04-13 09:24:59",
                }
            ]
        )

        result = compare_feed_audit(
            trade_date="2026-04-13",
            feed_source="kite",
            session_id="paper-audit",
            paper_db=db,
            market_db=_FakeMarketDB(_make_pack_frame(close=221.49)),
        )

        assert result["ok"] is True
        assert result["matched_rows"] == 1
        assert result["mismatched_rows"] == 0
        assert result["missing_pack_rows"] == 0
    finally:
        db.close()


def test_compare_feed_audit_flags_value_mismatch(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        db.create_session(
            session_id="paper-audit",
            status="COMPLETED",
            trade_date="2026-04-13",
        )
        db.upsert_feed_audit_rows(
            [
                {
                    "session_id": "paper-audit",
                    "trade_date": "2026-04-13",
                    "feed_source": "kite",
                    "transport": "websocket",
                    "symbol": "MANOMAY",
                    "bar_start": "2026-04-13 09:20:00",
                    "bar_end": "2026-04-13 09:25:00",
                    "open": 221.49,
                    "high": 222.0,
                    "low": 220.5,
                    "close": 221.49,
                    "volume": 12345.0,
                    "first_snapshot_ts": "2026-04-13 09:20:01",
                    "last_snapshot_ts": "2026-04-13 09:24:59",
                }
            ]
        )

        result = compare_feed_audit(
            trade_date="2026-04-13",
            feed_source="kite",
            session_id="paper-audit",
            paper_db=db,
            market_db=_FakeMarketDB(_make_pack_frame(close=220.90)),
        )

        assert result["ok"] is False
        assert result["matched_rows"] == 0
        assert result["mismatched_rows"] == 1
        assert result["samples"][0]["mismatches"]["close"]["delta"] > 0.5
    finally:
        db.close()
