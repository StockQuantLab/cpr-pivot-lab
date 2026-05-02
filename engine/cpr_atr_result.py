"""Backtest result analytics and persistence."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict
from datetime import date
from typing import ClassVar

import numpy as np
import polars as pl

from db.backtest_db import BacktestDB, get_backtest_db
from engine.cpr_atr_models import BacktestParams, FunnelCounts, TradeResult

logger = logging.getLogger(__name__)


def _int_from_mapping(
    values: dict[str, object] | None,
    key: str,
    default: int,
) -> int:
    if values is None:
        return default
    value = values.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    try:
        return int(str(value))
    except TypeError, ValueError:
        return default


class BacktestResult:
    """Holds trades and computes performance metrics."""

    default_db_factory: ClassVar[Callable[[], BacktestDB] | None] = None

    def __init__(
        self,
        run_id: str,
        trades: list[TradeResult] | None = None,
        params: BacktestParams | None = None,
        _loaded_df: pl.DataFrame | None = None,
        cache_info: dict[str, object] | None = None,
        run_context: dict[str, object] | None = None,
        funnel: FunnelCounts | None = None,
    ):
        self.run_id = run_id
        self.trades = trades or []
        self.params = params
        self.cache_info = cache_info or {}
        self.run_context = run_context or {}
        self.funnel = funnel
        # _loaded_df: pre-loaded from DuckDB cache — merged with in-memory trades in .df
        self._loaded_df = _loaded_df
        self._df: pl.DataFrame | None = None

    @property
    def df(self) -> pl.DataFrame:
        if self._df is not None:
            return self._df

        frames = []
        if self.trades:
            frames.append(pl.from_dicts([t.__dict__ for t in self.trades]))
        if self._loaded_df is not None and not self._loaded_df.is_empty():
            # Select only TradeResult columns so schemas match
            common_cols = [
                "run_id",
                "symbol",
                "trade_date",
                "direction",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "sl_price",
                "target_price",
                "profit_loss",
                "profit_loss_pct",
                "exit_reason",
                "sl_phase",
                "atr",
                "cpr_width_pct",
                "cpr_threshold",
                "rvol",
                "position_size",
                "position_value",
                "strategy_version",
                "mfe_r",
                "mae_r",
                "or_atr_ratio",
                "gap_pct",
                "gross_pnl",
                "total_costs",
                "reached_1r",
                "reached_2r",
                "max_r",
            ]
            available = [c for c in common_cols if c in self._loaded_df.columns]
            frames.append(self._loaded_df.select(available))

        if not frames:
            self._df = pl.DataFrame()
        elif len(frames) == 1:
            self._df = frames[0]
        else:
            self._df = pl.concat(frames, how="diagonal_relaxed")

        return self._df

    def validate(self) -> None:
        """Validate summary fields before persisting or publishing results."""
        df = self.df
        if df.is_empty() or "profit_loss" not in df.columns:
            return

        total_trades = int(df.height)
        total_pnl = float(df["profit_loss"].sum())
        if "run_id" in df.columns:
            run_ids = {str(value) for value in df["run_id"].drop_nulls().unique().to_list()}
            if run_ids and run_ids != {self.run_id}:
                raise ValueError(
                    f"Mixed run_id values in backtest result for run_id={self.run_id}: "
                    f"{sorted(run_ids)}"
                )
        if total_trades == 0 and total_pnl != 0:
            raise ValueError(f"PnL without trades: {total_pnl}")
        if abs(total_pnl) > 1e12:
            logger.warning("Suspiciously large PnL for run_id=%s: %s", self.run_id, total_pnl)

    def _time_of_day_breakdown(self) -> list[tuple[str, int, float, float]]:
        """Return hourly entry-time buckets with trade count, PnL, and win rate."""
        df = self.df
        if df.is_empty() or "entry_time" not in df.columns or "profit_loss" not in df.columns:
            return []

        bucketed = df.with_columns(
            pl.col("entry_time").cast(pl.Utf8).str.slice(0, 2).alias("_entry_hour")
        ).filter(pl.col("_entry_hour").str.contains(r"^\d{2}$"))
        if bucketed.is_empty():
            return []

        grouped = (
            bucketed.group_by("_entry_hour")
            .agg(
                pl.len().alias("trades"),
                pl.sum("profit_loss").alias("pnl"),
                (pl.col("profit_loss") > 0).cast(pl.Float64).mean().mul(100.0).alias("win_rate"),
            )
            .sort("_entry_hour")
        )
        return [
            (
                str(row[0]),
                int(row[1] or 0),
                float(row[2] or 0.0),
                float(row[3] or 0.0),
            )
            for row in grouped.iter_rows()
        ]

    def summary(self) -> str:
        """Print-friendly performance summary."""
        df = self.df
        if df.is_empty():
            return "No trades found."

        df = self.df
        total = len(df)
        wins = int((df["profit_loss"] > 0).sum())
        losses = total - wins
        win_rate = wins / total * 100 if total else 0
        total_pnl = float(df["profit_loss"].sum())
        avg_pnl = float(df["profit_loss"].mean())
        best = float(df["profit_loss"].max())
        worst = float(df["profit_loss"].min())

        # Cost breakdown
        has_costs = "gross_pnl" in df.columns and "total_costs" in df.columns
        if has_costs:
            gross_pnl_total = float(df["gross_pnl"].sum())
            total_costs_sum = float(df["total_costs"].sum())
        else:
            gross_pnl_total = total_pnl
            total_costs_sum = 0.0

        # Exit breakdown — granular SL types for phase analysis
        sl_initial = int((df["exit_reason"] == "INITIAL_SL").sum())
        sl_breakeven = int((df["exit_reason"] == "BREAKEVEN_SL").sum())
        sl_trailing = int((df["exit_reason"] == "TRAILING_SL").sum())
        sl_exits = sl_initial + sl_breakeven + sl_trailing
        tgt_exits = int((df["exit_reason"] == "TARGET").sum())
        time_exits = int((df["exit_reason"] == "TIME").sum())

        # MFE/MAE stats — only if columns are present (older cached results may lack them)
        has_excursion = "mfe_r" in df.columns and "mae_r" in df.columns
        if has_excursion:
            avg_mfe = float(df["mfe_r"].mean())
            avg_mae = float(df["mae_r"].mean())
            pct_mfe_1r = float((df["mfe_r"] >= 1.0).sum()) / total * 100 if total else 0.0
            target_r = self.params.rr_ratio if self.params else 2.0
            pct_mfe_target = float((df["mfe_r"] >= target_r).sum()) / total * 100 if total else 0.0

        symbols = df["symbol"].unique().to_list()

        lines = [
            f"{'=' * 55}",
            f" CPR-ATR Backtest -- Run {self.run_id}",
            f"{'=' * 55}",
            f" Symbols:       {', '.join(sorted(symbols))}",
            f" Date range:    {str(df['trade_date'].min())} to {str(df['trade_date'].max())}",
            f"{'-' * 55}",
            f" Total trades:  {total}",
            f" Wins / Losses: {wins} / {losses}  ({win_rate:.1f}% win rate)",
            f" Total P/L:     Rs.{total_pnl:+,.2f} (net)",
            f" Avg P/L/trade: Rs.{avg_pnl:+,.2f}",
            f" Best trade:    Rs.{best:+,.2f}",
            f" Worst trade:   Rs.{worst:+,.2f}",
            f"{'-' * 55}",
            f" SL exits:      {sl_exits} (Initial:{sl_initial} BE:{sl_breakeven} Trail:{sl_trailing})",
            f" Target exits:  {tgt_exits}",
            f" Time exits:    {time_exits}",
        ]
        if has_costs and total_costs_sum > 0:
            lines += [
                f"{'-' * 55}",
                f" Gross P/L:     Rs.{gross_pnl_total:+,.2f}",
                f" Total Costs:   Rs.{total_costs_sum:,.2f}",
                f" Net P/L:       Rs.{total_pnl:+,.2f}",
                f" Cost/Trade:    Rs.{total_costs_sum / total:,.2f}",
            ]
        candidate_trades = _int_from_mapping(self.cache_info, "candidate_trade_count", total)
        skipped_portfolio = _int_from_mapping(self.cache_info, "not_executed_portfolio", 0)
        if candidate_trades > total or skipped_portfolio > 0:
            lines += [
                f"{'-' * 55}",
                f" Candidate trades: {candidate_trades}",
                f" Skipped (cash/slots): {skipped_portfolio}",
            ]
        if has_excursion:
            lines += [
                f"{'-' * 55}",
                f" MFE (avg):    {avg_mfe:+.2f}R  (how close trades get to target)",
                f" MAE (avg):    {avg_mae:+.2f}R  (how far trades go against us)",
                f" MFE > 1.0R:   {pct_mfe_1r:.1f}%  (trades that reached 1:1 R)",
                f" MFE > target: {pct_mfe_target:.1f}%  (trades that hit {target_r:.1f}R target)",
            ]

        # Exit diagnostics (§0.3)
        has_diagnostics = "reached_1r" in df.columns and "max_r" in df.columns
        if has_diagnostics and total > 0:
            reached_1r_count = int(df["reached_1r"].sum())
            reached_2r_count = int(df["reached_2r"].sum()) if "reached_2r" in df.columns else 0
            zero_pnl_count = int(((df["profit_loss"] >= -0.01) & (df["profit_loss"] <= 0.01)).sum())
            be_exits = sl_breakeven
            # Trades that reached >=1R but did NOT hit target
            reached_1r_no_target = int((df["reached_1r"] & (df["exit_reason"] != "TARGET")).sum())
            avg_max_r = float(df["max_r"].mean())
            lines += [
                f"{'-' * 55}",
                " EXIT DIAGNOSTICS",
                f" Zero-PnL exits:    {zero_pnl_count} ({zero_pnl_count / total * 100:.1f}%)",
                f" Breakeven SL:      {be_exits} ({be_exits / total * 100:.1f}%)",
                f" Reached >=1R:      {reached_1r_count} ({reached_1r_count / total * 100:.1f}%)",
                f" Reached >=2R:      {reached_2r_count} ({reached_2r_count / total * 100:.1f}%)",
                f" >=1R but no target: {reached_1r_no_target} ({reached_1r_no_target / total * 100:.1f}%)",
                f" Avg Max R:         {avg_max_r:.2f}R",
            ]

        tod_rows = self._time_of_day_breakdown()
        if tod_rows:
            lines += [
                f"{'-' * 55}",
                " TIME OF DAY PNL",
                " Hour  Trades  WinRate   P/L",
            ]
            for hour, trades, pnl, win_rate_pct in tod_rows:
                lines.append(f" {hour}:00  {trades:>6,}  {win_rate_pct:>7.1f}%  Rs.{pnl:+,.2f}")

        # Setup selection funnel
        if self.funnel and self.funnel.universe_count > 0:
            f = self.funnel
            lines += [
                f"{'-' * 55}",
                " SETUP FUNNEL",
                f" Universe:        {f.universe_count:>8,} symbol-days",
            ]

            def _funnel_line(label: str, count: int, prev: int) -> str:
                pct = count / prev * 100 if prev > 0 else 0.0
                return f" {label:<17} {count:>8,} ({pct:5.1f}% pass)"

            lines.append(_funnel_line("CPR width:", f.after_cpr_width, f.universe_count))
            lines.append(_funnel_line("Direction:", f.after_direction, f.after_cpr_width))
            setup_label = (
                f"Setup ({self.params.fbr_setup_filter}):"
                if self.params and self.params.strategy == "FBR"
                else "Dir filter:"
            )
            lines.append(_funnel_line(setup_label, f.after_dir_filter, f.after_direction))
            lines.append(_funnel_line("Min price:", f.after_min_price, f.after_dir_filter))
            lines.append(_funnel_line("Gap filter:", f.after_gap, f.after_min_price))
            lines.append(_funnel_line("OR/ATR:", f.after_or_atr, f.after_gap))
            lines.append(_funnel_line("Narrowing:", f.after_narrowing, f.after_or_atr))
            if f.after_shift != f.after_narrowing:
                lines.append(_funnel_line("CPR shift:", f.after_shift, f.after_narrowing))
            # Final stage: entry_triggered vs last filter output
            last_filter = f.after_shift if f.after_shift else f.after_narrowing
            lines.append(_funnel_line("Entry triggered:", f.entry_triggered, last_filter))

        # Advanced risk metrics
        adv = self._advanced_metrics()
        if adv:
            pf = adv["profit_factor"]
            pf_str = f"{pf:.2f}" if pf < 1e9 else "∞"
            calmar = adv["calmar"]
            calmar_str = f"{calmar:.2f}" if calmar < 1e9 else "∞"
            lines += [
                f"{'-' * 55}",
                f" Profit Factor:  {pf_str}",
                f" Max Drawdown:   -{adv['max_dd_pct']:.1f}%  (Rs.{adv['max_dd_abs']:,.0f} abs)",
                f" Annual Return:  {adv['annual_return_pct']:+.1f}%",
                f" Calmar Ratio:   {calmar_str}",
                f" Sharpe Ratio:   {adv['sharpe']:.2f}",
            ]
        lines.append(f"{'=' * 55}")

        # Per-symbol breakdown
        if len(symbols) > 1:
            lines.append(" Per-symbol:")
            for sym in sorted(symbols):
                sym_df = df.filter(pl.col("symbol") == sym)
                sym_pnl = float(sym_df["profit_loss"].sum())
                sym_trades = len(sym_df)
                sym_wins = int((sym_df["profit_loss"] > 0).sum())
                lines.append(
                    f"   {sym:<12} {sym_trades:3d} trades  "
                    f"{sym_wins}/{sym_trades} wins  ₹{sym_pnl:+,.2f}"
                )
            lines.append(f"{'=' * 55}")

        return "\n".join(lines)

    def _advanced_metrics(self) -> dict:
        """
        Compute advanced risk metrics from trade-level data.

        Returns a dict with keys: profit_factor, max_dd_abs, max_dd_pct,
        annual_return_pct, calmar, sharpe. Returns {} on any failure so
        older cached results (missing columns) degrade gracefully.
        """
        df = self.df
        if df.is_empty() or "profit_loss" not in df.columns:
            return {}
        try:
            capital_per_symbol = (self.params.capital if self.params else 100_000.0) or 100_000.0
            symbol_count = max(1, int(df["symbol"].n_unique())) if "symbol" in df.columns else 1
            allocated_capital = float(capital_per_symbol) * float(symbol_count)
            initial_equity = (
                float(self.params.portfolio_value)
                if self.params and float(self.params.portfolio_value or 0.0) > 0
                else allocated_capital
            )

            # Profit Factor
            gross_profit = float(df.filter(pl.col("profit_loss") > 0)["profit_loss"].sum() or 0.0)
            gross_loss = abs(
                float(df.filter(pl.col("profit_loss") < 0)["profit_loss"].sum() or 0.0)
            )
            profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else 1e10

            # Max Drawdown via end-of-day portfolio equity curve
            daily = df.group_by("trade_date").agg(pl.col("profit_loss").sum()).sort("trade_date")
            daily_pnl = daily["profit_loss"].to_numpy()
            equity_curve = initial_equity + np.cumsum(daily_pnl)
            running_peak = np.maximum.accumulate(np.maximum(equity_curve, initial_equity))
            dd = equity_curve - running_peak
            max_dd_abs = float(dd.min()) if len(dd) else 0.0
            max_dd_pct = round(abs(max_dd_abs) / max(initial_equity, 1.0) * 100, 2)

            # Annualized return: use requested run window when available, not first/last trade date.
            ctx_start = str(self.run_context.get("start_date", "") or "")
            ctx_end = str(self.run_context.get("end_date", "") or "")
            if ctx_start and ctx_end:
                min_d = date.fromisoformat(ctx_start[:10])
                max_d = date.fromisoformat(ctx_end[:10])
            else:
                min_d = date.fromisoformat(str(df["trade_date"].min())[:10])
                max_d = date.fromisoformat(str(df["trade_date"].max())[:10])
            days = (max_d - min_d).days + 1
            total_pnl = float(df["profit_loss"].sum())
            ending_equity = initial_equity + total_pnl
            if ending_equity <= 0:
                annual_return_pct = -100.0
            else:
                annual_return_pct = round(
                    ((ending_equity / max(initial_equity, 1.0)) ** (365.25 / max(days, 1)) - 1.0)
                    * 100.0,
                    2,
                )

            # Calmar Ratio = annual_return / max_dd_%
            calmar = round(annual_return_pct / max_dd_pct, 3) if max_dd_pct > 0 else 1e10

            # Sharpe Ratio — daily P/L normalized by portfolio equity base.
            daily_arr = daily["profit_loss"].to_numpy() / max(initial_equity, 1.0)
            std_d = float(np.std(daily_arr))
            sharpe = (
                round(float(np.mean(daily_arr)) / std_d * np.sqrt(252), 3) if std_d > 0 else 0.0
            )

            return {
                "profit_factor": profit_factor,
                "max_dd_abs": max_dd_abs,
                "max_dd_pct": max_dd_pct,
                "annual_return_pct": annual_return_pct,
                "calmar": calmar,
                "sharpe": sharpe,
            }
        except Exception as e:
            logger.debug("Advanced metrics computation failed for run_id=%s: %s", self.run_id, e)
            return {}

    def save_to_db(
        self,
        db: BacktestDB | None = None,
        *,
        execution_mode: str = "BACKTEST",
    ) -> int:
        """Persist results to DuckDB backtest_results + run_metadata tables."""
        factory = type(self).default_db_factory
        target_db = db or (factory() if factory else get_backtest_db())
        strategy = self.params.strategy if self.params else "UNKNOWN"
        run_context = self.run_context
        params_dict = asdict(self.params) if self.params else None
        param_signature = run_context.get("param_signature")
        self.validate()
        row_count = 0
        replica_sync = getattr(target_db, "_sync", None)
        begin_batch = getattr(target_db, "_begin_replica_batch", None)
        end_batch = getattr(target_db, "_end_replica_batch", None)
        if callable(begin_batch):
            begin_batch()
        target_db.con.execute("BEGIN TRANSACTION")
        try:
            mode_label = (
                "compound" if self.params and self.params.compound_equity else "daily-reset"
            )
            sizing_label = "risk" if self.params and self.params.risk_based_sizing else "standard"
            target_db.store_run_metadata(
                run_id=self.run_id,
                strategy=strategy,
                label=(
                    f"{strategy}"
                    f" {mode_label}-{sizing_label}"
                    f" | {run_context.get('start_date', '')}"
                    f" to {run_context.get('end_date', '')}"
                ),
                symbols=run_context.get("symbols"),
                start_date=run_context.get("start_date"),
                end_date=run_context.get("end_date"),
                params=params_dict,
                param_signature=param_signature,
                execution_mode=execution_mode,
            )
            row_count = target_db.store_backtest_results(
                self.df,
                execution_mode=execution_mode,
                transactional=False,
            )
            target_db.con.execute("COMMIT")
        except Exception:
            try:
                target_db.con.execute("ROLLBACK")
            except Exception as rollback_err:
                logger.debug("Rollback failed after save_to_db failure: %s", rollback_err)
            raise
        finally:
            if callable(end_batch):
                end_batch()
        if replica_sync is not None:
            try:
                replica_sync.force_sync(target_db.con)
            except Exception as exc:
                logger.warning("Replica sync failed after save_to_db: %s", exc)
        try:
            from web.state import invalidate_run_cache

            invalidate_run_cache(self.run_id)
        except Exception as exc:
            logger.debug(
                "Skipping dashboard cache invalidation for run_id=%s: %s", self.run_id, exc
            )
        return row_count
