from __future__ import annotations

from pathlib import Path

import polars as pl

from db.paper_db import PaperDB
from scripts.paper_feed_audit import (
    compare_feed_audit,
    compare_signal_audit,
    record_signal_decisions,
)


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


def _make_pack_frame(*, close: float, minute: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["MANOMAY"],
            "trade_date": ["2026-04-13"],
            "pack_time_arr": [[minute]],
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
            market_db=_FakeMarketDB(_make_pack_frame(close=221.49, minute=560)),
        )

        assert result["ok"] is True
        assert result["matched_rows"] == 1
        assert result["mismatched_rows"] == 0
        assert result["missing_pack_rows"] == 0
        assert result["price_exact_rows"] == 1
        assert result["volume_exact_rows"] == 1
    finally:
        db.close()


def test_compare_feed_audit_replay_local_uses_bar_end(tmp_path: Path) -> None:
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
                    "feed_source": "replay",
                    "transport": "replay",
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
            feed_source="replay",
            session_id="paper-audit",
            paper_db=db,
            market_db=_FakeMarketDB(_make_pack_frame(close=221.49, minute=565)),
        )

        assert result["ok"] is True
        assert result["matched_rows"] == 1
        assert result["mismatched_rows"] == 0
        assert result["missing_pack_rows"] == 0
        assert result["price_exact_rows"] == 1
        assert result["volume_exact_rows"] == 1
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
            market_db=_FakeMarketDB(_make_pack_frame(close=220.90, minute=560)),
        )

        assert result["ok"] is False
        assert result["matched_rows"] == 0
        assert result["mismatched_rows"] == 1
        assert result["price_exact_rows"] == 0
        assert result["volume_exact_rows"] == 1
        assert result["samples"][0]["mismatches"]["close"]["delta"] > 0.5
    finally:
        db.close()


def test_record_and_compare_signal_audit(tmp_path: Path) -> None:
    db = PaperDB(db_path=tmp_path / "paper.duckdb")
    try:
        for session_id, symbol in (("live-1", "SBIN"), ("replay-1", "TCS")):
            db.create_session(
                session_id=session_id,
                status="COMPLETED",
                trade_date="2026-05-05",
            )
            record_signal_decisions(
                rows=[
                    {
                        "session_id": session_id,
                        "trade_date": "2026-05-05",
                        "feed_source": "kite",
                        "transport": "websocket",
                        "bar_end": "2026-05-05 09:20:00",
                        "bar_time": "09:20",
                        "strategy": "CPR_LEVELS",
                        "direction_filter": "LONG",
                        "symbol": symbol,
                        "stage": "ENTRY_EXECUTED",
                        "action": "OPEN",
                        "selected_rank": 1,
                        "candidate_payload": {"symbol": symbol, "rr_ratio": 2.5},
                        "setup_payload": {"direction": "LONG"},
                        "execution_payload": {"action": "OPEN"},
                    }
                ],
                paper_db=db,
            )

        summary = compare_signal_audit(session_id="live-1", paper_db=db)
        assert summary["rows"] == 1
        assert summary["stage_counts"]["ENTRY_EXECUTED"] == 1

        comparison = compare_signal_audit(
            session_id="live-1",
            compare_session_id="replay-1",
            paper_db=db,
        )
        assert comparison["ok"] is False
        assert comparison["mismatch_count"] == 2
        assert comparison["missing_in_compare"] == [
            {"bar_end": "2026-05-05 09:20:00+05:30", "symbol": "SBIN"}
        ]
        assert comparison["extra_in_compare"] == [
            {"bar_end": "2026-05-05 09:20:00+05:30", "symbol": "TCS"}
        ]
    finally:
        db.close()
