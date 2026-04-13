"""
Compare two saved backtest runs for parity.

Usage:
    doppler run -- uv run pivot-parity-check --expected-run-id fb9d879547e9 --actual-run-id d22692c878e5
    doppler run -- uv run pivot-parity-check --expected-run-id X --actual-run-id Y --fail-on-drift
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from db.duckdb import get_db
from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)


@dataclass(frozen=True)
class Metrics:
    run_id: str
    strategy: str
    start_date: str
    end_date: str
    symbol_count: int
    trade_count: int
    win_rate: float
    total_pnl: float
    profit_factor: float
    max_dd_pct: float
    annual_return_pct: float
    calmar: float


def _fetch_metrics(run_id: str) -> Metrics:
    db = get_db()
    row = db.con.execute(
        """
        SELECT
            run_id,
            strategy,
            start_date::VARCHAR,
            end_date::VARCHAR,
            symbol_count,
            trade_count,
            win_rate,
            total_pnl,
            profit_factor,
            max_dd_pct,
            annual_return_pct,
            calmar
        FROM run_metrics
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Run '{run_id}' not found in run_metrics")
    return Metrics(
        run_id=str(row[0]),
        strategy=str(row[1]),
        start_date=str(row[2]),
        end_date=str(row[3]),
        symbol_count=int(row[4]),
        trade_count=int(row[5]),
        win_rate=float(row[6]),
        total_pnl=float(row[7]),
        profit_factor=float(row[8]),
        max_dd_pct=float(row[9]),
        annual_return_pct=float(row[10]),
        calmar=float(row[11]),
    )


def _trade_key_stats(
    expected_run_id: str, actual_run_id: str, pl_epsilon: float
) -> dict[str, float]:
    db = get_db()
    row = db.con.execute(
        """
        WITH expected AS (
            SELECT symbol, trade_date, direction, entry_time, exit_time, profit_loss
            FROM backtest_results
            WHERE run_id = ?
        ),
        actual AS (
            SELECT symbol, trade_date, direction, entry_time, exit_time, profit_loss
            FROM backtest_results
            WHERE run_id = ?
        )
        SELECT
            (SELECT COUNT(*) FROM expected) AS expected_rows,
            (SELECT COUNT(*) FROM actual) AS actual_rows,
            (SELECT COUNT(*) FROM expected e JOIN actual a USING(symbol, trade_date, direction, entry_time, exit_time)) AS matched_keys,
            (SELECT COUNT(*) FROM expected e JOIN actual a USING(symbol, trade_date, direction, entry_time, exit_time)
             WHERE abs(e.profit_loss - a.profit_loss) <= ?) AS matched_keys_within_pl_epsilon,
            (SELECT COUNT(*) FROM expected e LEFT JOIN actual a USING(symbol, trade_date, direction, entry_time, exit_time)
             WHERE a.symbol IS NULL) AS expected_only,
            (SELECT COUNT(*) FROM actual a LEFT JOIN expected e USING(symbol, trade_date, direction, entry_time, exit_time)
             WHERE e.symbol IS NULL) AS actual_only,
            (SELECT COALESCE(SUM(abs(e.profit_loss - a.profit_loss)), 0.0)
             FROM expected e JOIN actual a USING(symbol, trade_date, direction, entry_time, exit_time)) AS total_abs_pl_delta_on_matched
        """,
        [expected_run_id, actual_run_id, pl_epsilon],
    ).fetchone()
    if row is None:
        return {}
    return {
        "expected_rows": float(row[0]),
        "actual_rows": float(row[1]),
        "matched_keys": float(row[2]),
        "matched_keys_within_pl_epsilon": float(row[3]),
        "expected_only": float(row[4]),
        "actual_only": float(row[5]),
        "total_abs_pl_delta_on_matched": float(row[6]),
    }


def _print_metrics(label: str, m: Metrics) -> None:
    print(f"{label}: {m.run_id}")
    print(
        f"  {m.strategy} | {m.start_date}->{m.end_date} | symbols={m.symbol_count} "
        f"trades={m.trade_count} wr={m.win_rate:.2f}% pnl=₹{m.total_pnl:,.2f} "
        f"pf={m.profit_factor:.3f} dd={m.max_dd_pct:.3f}% ann={m.annual_return_pct:.2f}% calmar={m.calmar:.3f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two run_ids for backtest parity.")
    parser.add_argument("--expected-run-id", required=True, help="Reference run_id")
    parser.add_argument("--actual-run-id", required=True, help="Candidate run_id to compare")
    parser.add_argument(
        "--pl-epsilon",
        type=float,
        default=0.01,
        help="Absolute P/L tolerance for matched trade keys (default 0.01)",
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit non-zero if any parity drift is detected.",
    )
    args = parser.parse_args()

    expected = _fetch_metrics(args.expected_run_id)
    actual = _fetch_metrics(args.actual_run_id)
    key_stats = _trade_key_stats(args.expected_run_id, args.actual_run_id, args.pl_epsilon)

    print("=" * 72)
    print("Run Metrics")
    print("=" * 72)
    _print_metrics("Expected", expected)
    _print_metrics("Actual  ", actual)
    print("-" * 72)
    print(f"Δ trades : {actual.trade_count - expected.trade_count:+d}")
    print(f"Δ total P/L: ₹{actual.total_pnl - expected.total_pnl:+,.2f}")
    print(f"Δ win rate: {actual.win_rate - expected.win_rate:+.2f}%")
    print(f"Δ calmar : {actual.calmar - expected.calmar:+.3f}")

    print("\n" + "=" * 72)
    print("Trade-Key Parity")
    print("=" * 72)
    print(f"expected_rows                 : {int(key_stats['expected_rows'])}")
    print(f"actual_rows                   : {int(key_stats['actual_rows'])}")
    print(f"matched_keys                  : {int(key_stats['matched_keys'])}")
    print(f"matched_keys_within_pl_epsilon: {int(key_stats['matched_keys_within_pl_epsilon'])}")
    print(f"expected_only                 : {int(key_stats['expected_only'])}")
    print(f"actual_only                   : {int(key_stats['actual_only'])}")
    print(f"total_abs_pl_delta_on_matched : ₹{key_stats['total_abs_pl_delta_on_matched']:,.2f}")

    has_drift = (
        expected.trade_count != actual.trade_count
        or abs(expected.total_pnl - actual.total_pnl) > args.pl_epsilon
        or int(key_stats["expected_only"]) > 0
        or int(key_stats["actual_only"]) > 0
        or int(key_stats["matched_keys"]) != int(key_stats["matched_keys_within_pl_epsilon"])
    )
    print("\nParity status:", "DRIFT DETECTED" if has_drift else "OK (exact/within tolerance)")
    if has_drift and args.fail_on_drift:
        raise SystemExit(1)


if __name__ in {"__main__", "__mp_main__"}:
    main()
