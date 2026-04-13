from __future__ import annotations

import scripts.build_tables as build_tables
from scripts.build_tables import _detect_missing_symbols


class TestDetectMissingSymbols:
    """_detect_missing_symbols must route to the correct reference table per --table value."""

    def _make_fake_db(self, parquet_symbols: list[str], present_in: dict[str, list[str]]):
        """Return a fake db whose .con.execute() returns appropriate symbols.

        parquet_symbols: symbols returned by v_daily query
        present_in: mapping of table_name → symbols already in that table
        """

        class FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeCon:
            def __init__(self, parquet, present):
                self._parquet = parquet
                self._present = present

            def execute(self, sql: str, params=None):
                # detect which table is being queried as the "already-built" reference
                for tbl, syms in self._present.items():
                    if tbl in sql:
                        already = set(syms)
                        missing = [(s,) for s in self._parquet if s not in already]
                        return FakeCursor(missing)
                # fallback: v_daily without a reference table filter → all symbols
                return FakeCursor([(s,) for s in self._parquet])

        class FakeDB:
            def __init__(self, parquet, present):
                self.con = FakeCon(parquet, present)

        return FakeDB(parquet_symbols, present_in)

    def test_no_table_defaults_to_market_day_state(self):
        db = self._make_fake_db(
            parquet_symbols=["SBIN", "TCS", "NEWCO"],
            present_in={"market_day_state": ["SBIN", "TCS"]},
        )
        result = _detect_missing_symbols(db)
        assert result == ["NEWCO"]

    def test_table_pack_routes_to_intraday_day_pack(self):
        # SBIN is in market_day_state but NOT in intraday_day_pack
        db = self._make_fake_db(
            parquet_symbols=["SBIN", "TCS"],
            present_in={
                "market_day_state": ["SBIN", "TCS"],
                "intraday_day_pack": ["TCS"],
            },
        )
        result = _detect_missing_symbols(db, table="pack")
        assert result == ["SBIN"]

    def test_table_strategy_routes_to_strategy_day_state(self):
        db = self._make_fake_db(
            parquet_symbols=["SBIN", "TCS", "INFY"],
            present_in={"strategy_day_state": ["SBIN"]},
        )
        result = _detect_missing_symbols(db, table="strategy")
        assert set(result) == {"TCS", "INFY"}

    def test_table_state_routes_to_market_day_state(self):
        db = self._make_fake_db(
            parquet_symbols=["SBIN", "TCS"],
            present_in={"market_day_state": ["SBIN"]},
        )
        result = _detect_missing_symbols(db, table="state")
        assert result == ["TCS"]


def test_staged_full_rebuild_resume_from_pack_uses_pack_resume(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_build_single(db, table, **kwargs):
        calls.append((table, dict(kwargs)))

    monkeypatch.setattr(build_tables, "_build_single", fake_build_single)

    build_tables._run_staged_full_rebuild(
        object(),
        force=True,
        batch_size=64,
        pack_lookback=10,
        since_date=None,
        resume_from="pack",
        resume_pack=True,
    )

    assert [table for table, _ in calls] == ["pack", "virgin", "meta"]
    assert calls[0][1]["force"] is False
    assert calls[0][1]["resume"] is True
    assert calls[1][1]["force"] is True
    assert calls[1][1]["resume"] is False
