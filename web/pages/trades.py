"""Trade Analytics page — cross-run aggregated exit reasons, monthly performance, symbol breakdown."""

from __future__ import annotations

import logging

import plotly.graph_objects as go
import polars as pl
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    apply_chart_theme,
    divider,
    empty_state,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
    safe_timer,
)
from web.state import aget_cross_run_trades, aget_runs, build_run_options


async def trade_analytics_page() -> None:
    """Render the Trade Analytics page — aggregates across ALL saved backtest runs."""
    runs = await aget_runs(force=True)
    bt_runs = [r for r in runs if str(r.get("execution_mode") or "BACKTEST").upper() != "PAPER"]

    with page_layout("Trade Analytics", "analytics"):
        theme = THEME
        colors = COLORS

        page_header(
            "Trade Analytics",
            "Aggregated across all saved backtest runs — exit patterns, monthly trends, symbol breakdown",
        )

        if not bt_runs:
            empty_state(
                "No backtest runs found",
                "Run a backtest with --save to see trade analytics.",
                icon="analytics",
            )
            return

        container = ui.column().classes("w-full")
        options = build_run_options(bt_runs)
        all_labels = list(options.keys())
        # Multi-run selector inside collapsible panel
        with (
            ui.expansion("Select Runs", icon="filter_list", value=False)
            .classes("w-full mb-2")
            .style(
                f"background: {theme['surface']}; border: 1px solid {theme['surface_border']}; "
                "border-radius: 4px;"
            )
        ):
            run_select = ui.select(
                all_labels,
                label="Runs to aggregate",
                multiple=True,
                value=all_labels,
            ).classes("w-full")

        async def _load() -> None:
            selected_labels = run_select.value
            selected_rids = {options[lbl] for lbl in selected_labels if lbl in options}
            filtered = [r for r in bt_runs if str(r.get("run_id") or "") in selected_rids]
            n_selected = len(filtered)
            try:
                df = await aget_cross_run_trades(filtered)
            except Exception as exc:
                logging.getLogger(__name__).exception("Failed to load cross-run trades: %s", exc)
                container.clear()
                with container:
                    empty_state(
                        "Failed to load trades",
                        "Could not aggregate trade data. Please refresh the page.",
                        icon="error",
                    )
                return
            container.clear()
            with container:
                ui.label(
                    f"Aggregating {n_selected} of {len(bt_runs)} run"
                    f"{'s' if len(bt_runs) != 1 else ''}"
                ).classes("text-xs mb-4").style(f"color: {theme['text_muted']};")
                _render_analytics_content(df, colors, theme)

        def _on_selection_change(e: object) -> None:
            if run_select.value:
                safe_timer(0.05, _load)

        run_select.on("update:model-value", _on_selection_change)

        await _load()


def _render_analytics_content(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    if df.is_empty():
        empty_state("No trades", "No saved trade data across runs.", icon="receipt_long")
        return

    n_trades = len(df)
    wins = df.filter(pl.col("profit_loss") > 0)
    win_rate = len(wins) / n_trades * 100 if n_trades else 0.0
    total_pnl = float(df["profit_loss"].sum())

    # Avg R
    avg_r = 0.0
    if "entry_price" in df.columns and "sl_price" in df.columns:
        risk = (pl.col("entry_price") - pl.col("sl_price")).abs()
        r_df = df.with_columns(
            pl.when(risk > 1e-9)
            .then(
                pl.when(pl.col("direction") == "LONG")
                .then(
                    (pl.col("exit_price") - pl.col("entry_price"))
                    / (pl.col("entry_price") - pl.col("sl_price"))
                )
                .otherwise(
                    (pl.col("entry_price") - pl.col("exit_price"))
                    / (pl.col("sl_price") - pl.col("entry_price"))
                )
            )
            .otherwise(None)
            .alias("r_val")
        ).filter(pl.col("r_val").is_finite())
        if not r_df.is_empty():
            avg_r = float(r_df["r_val"].mean() or 0.0)

    kpi_grid(
        [
            dict(
                title="Total Trades", value=f"{n_trades:,}", icon="swap_horiz", color=colors["info"]
            ),
            dict(
                title="Win Rate",
                value=f"{win_rate:.1f}%",
                icon="target",
                color=colors["success"] if win_rate >= 40 else colors["error"],
            ),
            dict(
                title="Avg R",
                value=f"{avg_r:.2f}R",
                icon="bar_chart",
                color=colors["success"] if avg_r >= 0 else colors["error"],
            ),
            dict(
                title="Total P/L",
                value=f"₹{total_pnl:,.0f}",
                icon="monetization_on",
                color=colors["success"] if total_pnl >= 0 else colors["error"],
            ),
        ],
        columns=4,
    )

    divider()

    with ui.tabs().classes("w-full") as tabs:
        ui.tab("exit", label="Exit Reasons")
        ui.tab("monthly", label="Monthly Performance")
        ui.tab("symbols", label="Symbol Breakdown")

    with ui.tab_panels(tabs, value="exit").classes("w-full bg-transparent pt-4"):
        with ui.tab_panel("exit"):
            _section_exit_reasons(df, colors, theme)
        with ui.tab_panel("monthly"):
            _section_monthly(df, colors, theme)
        with ui.tab_panel("symbols"):
            _section_symbols(df, colors, theme)


# ---------------------------------------------------------------------------
def _section_exit_reasons(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    exit_df = (
        df.with_columns((pl.col("profit_loss") > 0).alias("is_win"))
        .group_by("exit_reason")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("profit_loss").mean().round(1).alias("avg_pnl"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
            ]
        )
        .sort("count", descending=True)
    )

    _exit_colors = {
        "TARGET": colors["success"],
        "INITIAL_SL": colors["error"],
        "BREAKEVEN_SL": colors["warning"],
        "TRAILING_SL": "#f97316",
        "TIME": colors["gray"],
        "REVERSAL": "#8b5cf6",
        "CANDLE_EXIT": colors["gray"],
    }

    reasons = exit_df["exit_reason"].to_list()
    counts = exit_df["count"].to_list()
    pie_colors = [_exit_colors.get(r, colors["gray"]) for r in reasons]

    with ui.row().classes("w-full gap-6 responsive-row"):
        with ui.column().classes("flex-1"):
            fig = go.Figure(
                go.Pie(
                    labels=reasons,
                    values=counts,
                    hole=0.35,
                    marker_colors=pie_colors,
                    textinfo="label+percent",
                )
            )
            fig.update_layout(title="Exit Distribution (All Runs)")
            apply_chart_theme(fig)
            ui.plotly(fig).classes("w-full h-72")

        with ui.column().classes("flex-1"):
            avgs = exit_df["avg_pnl"].to_list()
            fig2 = go.Figure(
                go.Bar(
                    x=reasons,
                    y=avgs,
                    marker_color=[colors["success"] if v >= 0 else colors["error"] for v in avgs],
                    text=[f"₹{v:,.0f}" for v in avgs],
                    textposition="outside",
                )
            )
            fig2.update_layout(title="Avg P/L by Exit Reason", yaxis_title="₹")
            apply_chart_theme(fig2)
            ui.plotly(fig2).classes("w-full h-72")

    rows = [
        {
            "reason": r["exit_reason"],
            "count": int(r["count"]),
            "avg_pnl": round(float(r["avg_pnl"]), 0),
            "win_rate": round(float(r["win_rate"]), 1),
            "total_pnl": round(float(r["total_pnl"]), 0),
        }
        for r in exit_df.iter_rows(named=True)
    ]
    paginated_table(
        columns=[
            {"name": "reason", "label": "Exit Reason", "field": "reason", "align": "left"},
            {
                "name": "count",
                "label": "Count",
                "field": "count",
                "align": "right",
                "format": "int",
            },
            {
                "name": "avg_pnl",
                "label": "Avg P/L",
                "field": "avg_pnl",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "win_rate",
                "label": "Win %",
                "field": "win_rate",
                "align": "right",
                "format": "pct",
            },
            {
                "name": "total_pnl",
                "label": "Total P/L",
                "field": "total_pnl",
                "align": "right",
                "format": "currency",
            },
        ],
        rows=rows,
        row_key="reason",
        page_size=10,
    )


# ---------------------------------------------------------------------------
def _section_monthly(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    monthly = (
        df.with_columns(
            pl.col("trade_date").dt.strftime("%Y-%m").alias("month"),
            (pl.col("profit_loss") > 0).alias("is_win"),
        )
        .group_by("month")
        .agg(
            [
                pl.len().alias("trades"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
                pl.col("profit_loss").mean().round(0).alias("avg_pnl"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
            ]
        )
        .sort("month")
    )

    if monthly.is_empty():
        return

    months = monthly["month"].to_list()
    pnls = monthly["total_pnl"].to_list()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=months,
            y=pnls,
            mode="lines+markers",
            line=dict(color=colors["primary"], width=2),
            marker=dict(
                color=[colors["success"] if p >= 0 else colors["error"] for p in pnls], size=6
            ),
            fill="tozeroy",
            fillcolor=f"rgba({_hex2rgb(colors['primary'])},0.08)",
            hovertemplate="%{x}<br>P/L: ₹%{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(title="Monthly P/L (All Runs)", xaxis_title="Month", yaxis_title="₹")
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-72 mb-4")

    rows = [
        {
            "month": r["month"],
            "trades": int(r["trades"]),
            "total_pnl": round(float(r["total_pnl"]), 0),
            "avg_pnl": round(float(r["avg_pnl"]), 0),
            "win_rate": round(float(r["win_rate"]), 1),
        }
        for r in monthly.iter_rows(named=True)
    ]
    paginated_table(
        columns=[
            {"name": "month", "label": "Month", "field": "month", "align": "left"},
            {
                "name": "trades",
                "label": "Trades",
                "field": "trades",
                "align": "right",
                "format": "int",
            },
            {
                "name": "total_pnl",
                "label": "Total P/L",
                "field": "total_pnl",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "avg_pnl",
                "label": "Avg P/L",
                "field": "avg_pnl",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "win_rate",
                "label": "Win %",
                "field": "win_rate",
                "align": "right",
                "format": "pct",
            },
        ],
        rows=rows,
        row_key="month",
        page_size=24,
    )


# ---------------------------------------------------------------------------
def _section_symbols(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    sym_df = (
        df.with_columns((pl.col("profit_loss") > 0).alias("is_win"))
        .group_by("symbol")
        .agg(
            [
                pl.len().alias("trades"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
                pl.col("profit_loss").mean().round(0).alias("avg_pnl"),
                pl.col("profit_loss").max().round(0).alias("best"),
                pl.col("profit_loss").min().round(0).alias("worst"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
            ]
        )
        .sort("total_pnl", descending=True)
    )

    rows = [
        {
            "symbol": r["symbol"],
            "trades": int(r["trades"]),
            "total_pnl": round(float(r["total_pnl"]), 0),
            "avg_pnl": round(float(r["avg_pnl"]), 0),
            "best": round(float(r["best"]), 0),
            "worst": round(float(r["worst"]), 0),
            "win_rate": round(float(r["win_rate"]), 1),
        }
        for r in sym_df.iter_rows(named=True)
    ]
    paginated_table(
        columns=[
            {
                "name": "symbol",
                "label": "Symbol",
                "field": "symbol",
                "align": "left",
                "sortable": True,
            },
            {
                "name": "trades",
                "label": "Trades",
                "field": "trades",
                "align": "right",
                "format": "int",
            },
            {
                "name": "total_pnl",
                "label": "Total P/L",
                "field": "total_pnl",
                "align": "right",
                "sortable": True,
                "format": "currency",
            },
            {
                "name": "avg_pnl",
                "label": "Avg P/L",
                "field": "avg_pnl",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "best",
                "label": "Best",
                "field": "best",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "worst",
                "label": "Worst",
                "field": "worst",
                "align": "right",
                "format": "currency",
            },
            {
                "name": "win_rate",
                "label": "Win %",
                "field": "win_rate",
                "align": "right",
                "format": "pct",
            },
        ],
        rows=rows,
        row_key="symbol",
        page_size=30,
    )


def _hex2rgb(hex_c: str) -> str:
    h = hex_c.lstrip("#")
    return f"{int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)}"
