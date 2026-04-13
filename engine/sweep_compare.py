"""Backtest sweep comparison — rank and format results from run_metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SweepSummary:
    """Summarized metrics for a single sweep run."""

    run_id: str
    label: str
    trade_count: int
    win_rate: float
    total_pnl: float
    profit_factor: float
    max_dd_pct: float
    annual_return_pct: float
    calmar: float


VALID_RANK_METRICS = frozenset(
    {
        "calmar",
        "win_rate",
        "total_pnl",
        "profit_factor",
        "annual_return_pct",
        "max_dd_pct",
        "trade_count",
    }
)


def fetch_summaries(db: Any, run_ids: list[str]) -> list[SweepSummary]:
    """Fetch run_metrics rows for a list of run_ids from DuckDB."""
    if not run_ids:
        return []
    ids = ", ".join(f"'{rid}'" for rid in run_ids)
    rows = db.con.execute(
        f"""
        SELECT
            run_id, trade_count, win_rate, total_pnl,
            profit_factor, max_dd_pct, annual_return_pct, calmar
        FROM run_metrics
        WHERE run_id IN ({ids})
        ORDER BY run_id
        """,
    ).pl()
    results = []
    for row in rows.to_dicts():
        results.append(
            SweepSummary(
                run_id=row["run_id"],
                label=row["run_id"],
                trade_count=row["trade_count"],
                win_rate=row["win_rate"],
                total_pnl=row["total_pnl"],
                profit_factor=row["profit_factor"],
                max_dd_pct=row["max_dd_pct"],
                annual_return_pct=row["annual_return_pct"],
                calmar=row["calmar"],
            )
        )
    return results


def rank_sweeps(
    summaries: list[SweepSummary],
    metric: str = "calmar",
    sort: str = "desc",
    top_n: int = 5,
) -> list[SweepSummary]:
    """Rank sweep results by metric and return top_n."""
    reverse = sort == "desc"
    sorted_summaries = sorted(summaries, key=lambda s: getattr(s, metric, 0), reverse=reverse)
    return sorted_summaries[:top_n]


def format_comparison_table(summaries: list[SweepSummary]) -> str:
    """Format ranked results as a readable table."""
    if not summaries:
        return "No results to display."
    header = (
        f"{'#':<4} {'Label':<14s} {'Calmar':>8} {'Win%':>7} "
        f"{'AnnRet':>8} {'MaxDD':>8} {'Trades':>7} {'PF':>7}"
    )
    separator = "-" * len(header)
    lines = [header, separator]
    for i, s in enumerate(summaries, 1):
        lines.append(
            f"{i:<4} {s.label:<14s} {s.calmar:>8.2f} {s.win_rate:>6.1f}% "
            f"{s.annual_return_pct:>7.1f}% {s.max_dd_pct:>7.1f}% "
            f"{s.trade_count:>7} {s.profit_factor:>7.2f}"
        )
    return "\n".join(lines)
