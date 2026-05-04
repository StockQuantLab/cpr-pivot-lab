from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from db import duckdb as duckdb_module
from db.duckdb import MarketDB


def test_marketdb_write_lock_blocks_second_writer(tmp_path) -> None:
    db_path = tmp_path / "lock.duckdb"
    first = MarketDB(db_path=db_path)
    try:
        with pytest.raises(SystemExit, match="Another DuckDB write process is running"):
            MarketDB(db_path=db_path)
    finally:
        first.close()

    second = MarketDB(db_path=db_path)
    second.close()


def test_marketdb_write_lock_recovers_from_stale_lock(tmp_path) -> None:
    db_path = tmp_path / "stale.duckdb"
    lock_path = db_path.parent / f"{db_path.name}.writelock"
    lock_path.write_text(json.dumps({"pid": 99999999}), encoding="utf-8")

    db = MarketDB(db_path=db_path)
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        assert payload["pid"] == os.getpid()
    finally:
        db.close()

    assert not lock_path.exists()


def test_live_market_db_reuses_existing_source_connection(monkeypatch) -> None:
    existing = SimpleNamespace(db_path="source.duckdb")
    monkeypatch.setattr(duckdb_module, "_db", existing)
    monkeypatch.setattr(duckdb_module, "_live_market_db", None)

    assert duckdb_module.get_live_market_db() is existing
