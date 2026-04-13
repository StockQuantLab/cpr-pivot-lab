from __future__ import annotations

from pathlib import Path

import pytest

from db.duckdb import MarketDB


class _ResumeReachedBatchError(Exception):
    pass


class _FakeResult:
    def __init__(self, *, fetchone=None, fetchall=None, rowcount=0):
        self._fetchone = fetchone
        self._fetchall = fetchall or []
        self.rowcount = rowcount

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class _FakeCon:
    def execute(self, query: str, params=None):
        sql = " ".join(query.split())
        if "SELECT DISTINCT symbol FROM intraday_day_pack" in sql:
            return _FakeResult(fetchall=[("AAA",), ("BBB",)])
        if "SELECT COUNT(*) FROM intraday_day_pack" in sql:
            return _FakeResult(fetchone=(200,))
        if sql.startswith("BEGIN TRANSACTION"):
            raise _ResumeReachedBatchError()
        return _FakeResult()


def test_pack_resume_bypasses_existing_table_short_circuit(tmp_path: Path, monkeypatch) -> None:
    db = MarketDB.__new__(MarketDB)
    db.db_path = tmp_path / "runtime.duckdb"
    db.con = _FakeCon()

    monkeypatch.setattr(db, "_require_data", lambda name: None)
    monkeypatch.setattr(db, "_resolve_pack_symbols", lambda symbols: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(db, "_split_symbols_with_5min_data", lambda symbols: (symbols, []))
    monkeypatch.setattr(db, "_table_exists", lambda name: True)
    monkeypatch.setattr(db, "_table_has_column", lambda table, column: True)
    monkeypatch.setattr(db, "_iter_symbol_batches", lambda symbols, batch_size: [symbols])
    monkeypatch.setattr(db, "_build_parquet_source_sql", lambda batch: "source_sql")

    with pytest.raises(_ResumeReachedBatchError):
        db.build_intraday_day_pack(resume=True)
