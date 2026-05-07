"""Symbol Performance page — cross-run aggregated per-symbol P/L."""

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
)
from web.state import aget_cross_run_trades, aget_runs


async def symbols_page() -> None:
    """Per-symbol performance aggregated across ALL backtest runs."""
    runs = await aget_runs(force=False)
    bt_runs = [r for r in runs if str(r.get("execution_mode") or "BACKTEST").upper() != "PAPER"]

    with page_layout("Symbols", "show_chart"):
        theme = THEME
        colors = COLORS

        page_header(
            "Symbol Performance",
            f"Aggregated across {len(bt_runs)} backtest run{'s' if len(bt_runs) != 1 else ''} "
            "— per-symbol win rates, P/L, and trade counts",
        )

        if not bt_runs:
            empty_state(
                "No backtest runs found", "Run a backtest with --save first.", icon="show_chart"
            )
            return

        container = ui.column().classes("w-full")

        # Collapsible list of included runs
        with (
            ui.expansion(f"Included runs ({len(bt_runs)})", icon="list", value=False)
            .classes("w-full mb-2")
            .style(
                f"background: {theme['surface']}; border: 1px solid {theme['surface_border']}; "
                "border-radius: 4px;"
            )
        ):
            for r in bt_runs:
                rid = str(r.get("run_id") or "")[:12]
                direction = str(r.get("direction_filter") or "BOTH").upper()
                sizing = "Risk" if r.get("risk_based_sizing") else "Slot"
                compound = "Compound" if r.get("compound_equity") else "Daily"
                trades = int(r.get("trade_count") or 0)
                ui.label(f"{rid} · {direction} {sizing} {compound} · {trades:,} trades").classes(
                    "text-xs mono-font"
                ).style(f"color: {theme['text_secondary']};")

        async def _load() -> None:
            df = await aget_cross_run_trades(bt_runs)
            container.clear()
            with container:
                _render_symbols_content(df, colors, theme)

        await _load()


def _render_symbols_content(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    if df.is_empty():
        empty_state("No trade data", "No saved trades across runs.", icon="show_chart")
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
            "Best Symbol",
            (sym_df["symbol"].to_list() or ["—"])[0] if n_total else "—",
            colors["success"],
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
        title="Top 30 Symbols by Total P/L (All Runs)",
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
            "trades": int(r["trades"]),
            "total_pnl": int(r["total_pnl"]),
            "avg_pnl": int(r["avg_pnl"]),
            "best": int(r["best"]),
            "worst": int(r["worst"]),
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
                "sortable": True,
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
        page_size=50,
    )


def _kpi_chip(label: str, value: str, color: str) -> None:
    theme = THEME
    with ui.column().classes("kpi-card gap-0 p-3"):
        ui.label(label).classes("text-xs uppercase").style(
            f"color: {theme['text_muted']}; letter-spacing: 0.08em;"
        )
        ui.label(value).classes("text-lg font-bold mono-font").style(f"color: {color};")
