"""Symbol Performance page — per-symbol P/L across all saved runs."""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    apply_chart_theme,
    divider,
    empty_state,
    page_header,
    page_layout,
    paginated_table,
    safe_timer,
)
from web.state import aget_runs, aget_trades, build_run_options


async def symbols_page() -> None:
    """Per-symbol performance across all runs."""
    runs = await aget_runs(force=True)

    with page_layout("Symbols", "show_chart"):
        theme = THEME
        colors = COLORS

        page_header(
            "Symbol Performance",
            "Per-symbol win rates, P/L and trade counts — select a run to explore",
        )

        if not runs:
            empty_state("No runs found", "Run a backtest with --save first.", icon="show_chart")
            return

        options = build_run_options(runs)
        labels = list(options.keys())

        @ui.refreshable
        def render_symbols(label: str) -> None:
            exp_id = options.get(label, "")
            if not exp_id:
                return
            container = ui.column().classes("w-full")

            async def _load() -> None:
                df = await aget_trades(exp_id)
                container.clear()
                with container:
                    _render_symbols_content(df, colors, theme)

            safe_timer(0.1, _load)

        ui.select(
            labels,
            value=labels[0],
            label="Select Run",
            on_change=lambda e: render_symbols.refresh(e.value),
        ).props("outlined dense use-input options-dense input-debounce=0").classes(
            "w-full max-w-2xl mb-4"
        )

        divider()
        render_symbols(labels[0])


def _render_symbols_content(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    if df.is_empty():
        empty_state("No trade data", "This run has no saved trades.", icon="show_chart")
        return

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

    n_positive = int(sym_df.filter(pl.col("total_pnl") > 0).height)
    n_total = sym_df.height

    with ui.row().classes("gap-6 mb-4 flex-wrap"):
        _kpi_chip("Symbols Tracked", str(n_total), colors["info"])
        _kpi_chip("Profitable Symbols", str(n_positive), colors["success"])
        _kpi_chip(
            "Win Rate (symbols)",
            f"{n_positive / n_total * 100:.0f}%" if n_total else "—",
            colors["primary"],
        )
        _kpi_chip(
            "Best Symbol", sym_df["symbol"].to_list()[0] if n_total else "—", colors["success"]
        )

    divider()

    # Top 30 bar chart
    top = sym_df.head(30)
    symbols = top["symbol"].to_list()
    pnls = top["total_pnl"].to_list()

    fig = go.Figure(
        go.Bar(
            x=pnls,
            y=symbols,
            orientation="h",
            marker_color=[colors["success"] if p >= 0 else colors["error"] for p in pnls],
            hovertemplate="%{y}: ₹%{x:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Top 30 Symbols by Total P/L",
        xaxis_title="₹",
        height=max(350, len(symbols) * 22 + 80),
        yaxis=dict(autorange="reversed"),
    )
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full mb-4")

    # Full sortable table
    rows = [
        {
            "symbol": r["symbol"],
            "trades": str(r["trades"]),
            "total_pnl": f"₹{int(r['total_pnl']):,}",
            "avg_pnl": f"₹{int(r['avg_pnl']):,}",
            "best": f"₹{int(r['best']):,}",
            "worst": f"₹{int(r['worst']):,}",
            "win_rate": f"{r['win_rate']:.1f}%",
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
                "sortable": True,
            },
            {
                "name": "total_pnl",
                "label": "Total P/L",
                "field": "total_pnl",
                "align": "right",
                "sortable": True,
            },
            {"name": "avg_pnl", "label": "Avg P/L", "field": "avg_pnl", "align": "right"},
            {"name": "best", "label": "Best", "field": "best", "align": "right"},
            {"name": "worst", "label": "Worst", "field": "worst", "align": "right"},
            {"name": "win_rate", "label": "Win %", "field": "win_rate", "align": "right"},
        ],
        rows=rows,
        row_key="symbol",
        page_size=50,
    )


def _kpi_chip(label: str, value: str, color: str) -> None:
    theme = THEME
    with ui.column().classes("kpi-card gap-0 p-3"):
        ui.label(label).classes("text-xs uppercase").style(
            f"color: {theme['text_muted']}; letter-spacing: 0.08em;"
        )
        ui.label(value).classes("text-lg font-bold mono-font").style(f"color: {color};")
