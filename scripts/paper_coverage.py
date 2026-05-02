"""Runtime coverage checks for paper trading workflows."""

from __future__ import annotations

from typing import Any


def _count_duckdb_rows_for_run_ids(run_ids: list[str]) -> dict[str, int]:
    ids = sorted({str(run_id).strip() for run_id in run_ids if str(run_id).strip()})
    if not ids:
        return {
            "backtest_results": 0,
            "run_daily_pnl": 0,
            "run_metrics": 0,
            "run_metadata": 0,
        }
    placeholders = ", ".join("?" for _ in ids)
    from db.backtest_db import get_backtest_db
    from db.duckdb import get_db

    counts: dict[str, int] = dict.fromkeys(
        ("backtest_results", "run_daily_pnl", "run_metrics", "run_metadata"),
        0,
    )
    for db in (get_db(), get_backtest_db()):
        for table in counts:
            row = db.con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id IN ({placeholders})",
                ids,
            ).fetchone()
            counts[table] += int(row[0] or 0) if row else 0
    return counts


def _build_runtime_coverage_fix_lines(missing_counts: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if int(missing_counts.get("cpr_daily") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table cpr --refresh-date <trade-date>")
        lines.append(
            "doppler run -- uv run pivot-build --table thresholds --refresh-date <trade-date>"
        )
    if int(missing_counts.get("market_day_state") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table state --refresh-date <trade-date>")
    if int(missing_counts.get("strategy_day_state") or 0) > 0:
        lines.append(
            "doppler run -- uv run pivot-build --table strategy --refresh-date <trade-date>"
        )
    if int(missing_counts.get("intraday_day_pack") or 0) > 0:
        lines.append("doppler run -- uv run pivot-build --table pack --refresh-since <trade-date>")
    return lines or ["doppler run -- uv run pivot-data-quality --date <trade-date>"]


def _handle_coverage_gaps(preparation: dict[str, Any], *, trade_date: str, mode: str) -> None:
    """Handle runtime coverage gaps for paper sessions.

    Strategy:
    - live/current-day checks use previous completed-day `v_daily`, `v_5min`,
      `atr_intraday`, and `cpr_thresholds`; they must not require future-date state rows.
    - `intraday_day_pack` gaps are warnings only because missing symbols simply did not
      trade that day and will produce no positions.
    - `cpr_daily` / `market_day_state` / `strategy_day_state` gaps are hard errors because
      CPR/ATR setup data is absent.
    """
    inner_coverage = preparation.get("coverage") or {}
    if mode == "live" and "missing_by_symbol" in inner_coverage:
        missing_by_symbol = dict(inner_coverage.get("missing_by_symbol") or {})
        sparse_missing_by_table = dict(inner_coverage.get("sparse_missing_by_table") or {})
        if sparse_missing_by_table:
            summary = ", ".join(
                f"{table}={len(symbols)}"
                for table, symbols in sorted(sparse_missing_by_table.items())
            )
            print(
                f"[coverage] WARNING: sparse previous-day live prerequisite gaps for "
                f"{trade_date}: {summary}. Missing symbols will be skipped.",
                flush=True,
            )
        if not missing_by_symbol:
            return

        missing_by_table: dict[str, list[str]] = {}
        for symbol, missing_items in missing_by_symbol.items():
            for item in missing_items:
                missing_by_table.setdefault(str(item), []).append(str(symbol))
        missing_detail_lines = [
            f"  {table}: {len(symbols)} missing"
            + (f" — {', '.join(sorted(symbols))}" if len(symbols) <= 20 else "")
            for table, symbols in sorted(missing_by_table.items())
        ]
        raise SystemExit(
            f"Live prerequisites incomplete for {trade_date}.\n"
            + "\n".join(missing_detail_lines)
            + "\n\nLive uses the previous completed trading day; do not build future-date "
            "market_day_state rows.\n" + "Fix:\n  doppler run -- uv run pivot-refresh --eod-ingest "
            "--date <prev_trading_date> --trade-date <trade_date>"
        )

    raw_coverage = inner_coverage.get("coverage") or inner_coverage
    missing_symbols: dict[str, list[str]] = {}
    missing_counts: dict[str, int] = inner_coverage.get("missing_counts") or {}
    if not missing_counts:
        for table, values in raw_coverage.items():
            if isinstance(values, list):
                missing_counts[str(table)] = len(values)
                missing_symbols[str(table)] = list(values)
    else:
        for table, values in raw_coverage.items():
            if isinstance(values, list):
                missing_symbols[str(table)] = list(values)

    pack_missing = int(missing_counts.get("intraday_day_pack") or 0)
    cpr_missing = int(missing_counts.get("cpr_daily") or 0)
    mds_missing = int(missing_counts.get("market_day_state") or 0)
    sds_missing = int(missing_counts.get("strategy_day_state") or 0)

    if pack_missing:
        syms = missing_symbols.get("intraday_day_pack") or []
        sym_str = f" — {', '.join(sorted(syms))}" if syms and len(syms) <= 20 else ""
        print(
            f"[coverage] WARNING: {pack_missing} symbol(s) missing from intraday_day_pack"
            f" for {trade_date}{sym_str}. They will be skipped (no trades).",
            flush=True,
        )

    if cpr_missing == 0 and mds_missing == 0 and sds_missing == 0:
        return

    detail_lines: list[str] = []
    blocking: dict[str, int] = {}
    for table in ("cpr_daily", "market_day_state", "strategy_day_state"):
        count = int(missing_counts.get(table) or 0)
        if count:
            blocking[table] = count
            syms = missing_symbols.get(table) or []
            if syms and len(syms) <= 20:
                detail_lines.append(f"  {table}: {count} missing — {', '.join(sorted(syms))}")
            else:
                detail_lines.append(f"  {table}: {count} missing")

    fix_source = dict(blocking)
    if pack_missing:
        fix_source["intraday_day_pack"] = pack_missing
    fix_lines = _build_runtime_coverage_fix_lines(fix_source)
    pre_market_hint = (
        "\n\nFor daily-live, run this pre-market:\n"
        f"  doppler run -- uv run pivot-refresh --since <prev_trading_date>\n"
        f"  doppler run -- uv run pivot-paper-trading daily-prepare --trade-date {trade_date} --all-symbols"
        if mode == "live"
        else ""
    )
    raise SystemExit(
        f"Runtime coverage incomplete for {trade_date} (mode={mode}).\n"
        + "\n".join(detail_lines)
        + "\n\nFix:\n  "
        + "\n  ".join(fix_lines)
        + pre_market_hint
    )
