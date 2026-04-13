"""Run Results page — run selector, KPI cards, and trade analytics tabs."""

from __future__ import annotations

import asyncio
import json
import logging
import re

import numpy as np
import plotly.graph_objects as go
import polars as pl
from nicegui import ui

from web.components import (  # shared helpers
    COLORS,
    THEME,
    _as_bool,
    apply_chart_theme,
    apply_trade_filters,
    divider,
    empty_state,
    exit_badge,
    export_button,
    format_drawdown_pct,
    info_box,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
    param_detail_card,
    param_header_strip,
    strat_badge,
    trade_filter_bar,
)
from web.state import (
    aget_run_metadata,
    aget_runs,
    aget_trade_inspection,
    aget_trades,
    build_run_options,
)

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
_RUN_ID_RE = re.compile(r"^[a-f0-9]{12}$")
logger = logging.getLogger(__name__)


def _mini_card(title: str, value: str, color: str) -> None:
    """Compact metric card for secondary metrics — styled with border and accent."""
    theme = THEME
    with (
        ui.column()
        .classes("items-center px-5 py-3")
        .style(
            f"background:{theme['surface']};"
            f"border:1px solid {theme['surface_border']};"
            f"border-top:3px solid {color};"
            f"border-radius:6px;min-width:100px;"
        )
    ):
        ui.label(value).classes("text-lg font-bold mono-font tabular-nums").style(
            f"color: {color};"
        )
        ui.label(title).classes("text-xs uppercase tracking-wide mt-1").style(
            f"color: {theme['text_secondary']};"
        )


def _extract_run_params(meta: dict, run_meta: dict) -> dict:
    """Prefer the dedicated metadata fetch, but fall back to the run-list payload.

    Some dashboard rows can still render even when the async metadata fetch is stale.
    The run list already carries `params_json`, so use that as a secondary source.
    """
    if isinstance(run_meta, dict):
        params = run_meta.get("params")
        if isinstance(params, dict) and params:
            return params

    params_json = meta.get("params_json")
    if isinstance(params_json, str) and params_json.strip():
        try:
            parsed = json.loads(params_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _effective_run_params(params: dict) -> dict:
    """Return a display copy of params with skipped-RVOL runs rendered as OFF.

    The stored JSON remains unchanged; this only affects dashboard presentation.
    """
    display = dict(params or {})
    skip_rvol_check = _as_bool(display.get("skip_rvol_check") or display.get("skip_rvol"))
    if skip_rvol_check:
        display["rvol_threshold"] = "OFF"
    return display


async def backtest_page() -> None:
    """Render the Run Results page."""
    runs = await aget_runs(force=True)

    with page_layout("Run Results", "bar_chart"):
        theme = THEME
        colors = COLORS

        if not runs:
            page_header("Run Results")
            empty_state(
                "No backtest runs found",
                "Run a backtest first to see results here.",
                action_label="Go Home",
                action_callback=lambda: ui.navigate.to("/"),
                icon="science",
            )
            return

        page_header(
            "Run Results",
            "Primary metrics first, supporting metrics below. Pick a run, then read the summary.",
        )

        options = build_run_options(runs)
        labels = list(options.keys())

        # Restore last-viewed run from sessionStorage
        saved_id = await ui.run_javascript(
            "sessionStorage.getItem('cpr_run_id') || ''", timeout=2.0
        )
        id_to_label = {v: k for k, v in options.items()}
        initial_label = id_to_label.get(saved_id or "", labels[0]) if labels else ""

        # Also accept ?run_id= query param
        try:
            from nicegui import context as _ctx

            qs = getattr(_ctx.client.page, "query_string", "") or ""
            for part in qs.split("&"):
                if part.startswith("run_id="):
                    qid = part.split("=", 1)[1]
                    if _validate_run_id(qid) and qid in id_to_label:
                        initial_label = id_to_label[qid]
        except Exception as _:
            initial_label = initial_label or (labels[0] if labels else "")

        @ui.refreshable
        def render_run(exp_label: str) -> None:
            exp_id = options.get(exp_label, "")
            if not exp_id:
                return

            # Save to sessionStorage so theme toggle restores it
            ui.run_javascript(f"sessionStorage.setItem('cpr_run_id', '{exp_id}')")

            # Find run meta
            meta = next((r for r in runs if r.get("run_id") == exp_id), {})
            strategy = (meta.get("strategy") or "").split("|")[0].strip()

            # Load trades async (show spinner meanwhile)
            trades_container = ui.column().classes("w-full")

            async def _load_trades() -> None:
                try:
                    df, run_meta = await _load_run_payload(exp_id)
                except Exception as e:
                    logger.exception("Failed loading run payload for run_id=%s: %s", exp_id, e)
                    trades_container.clear()
                    with trades_container:
                        empty_state(
                            "Failed to load run payload",
                            f"Could not load run details for run_id `{exp_id}`. Please retry.",
                            icon="error",
                        )
                    return
                trades_container.clear()
                with trades_container:
                    _render_content(meta, strategy, df, run_meta, colors, theme)

            ui.timer(0.1, _load_trades, once=True)

        # Selector
        with ui.row().classes("w-full items-center gap-4 mb-4"):
            sel = (
                ui.select(
                    labels,
                    value=initial_label,
                    label="Select Run",
                    on_change=lambda e: render_run.refresh(e.value),
                )
                .props("outlined dense use-input options-dense input-debounce=0")
                .classes("flex-1")
            )
            ui.button(
                "Refresh",
                icon="refresh",
                on_click=lambda: render_run.refresh(sel.value),
            ).props("flat dense").style(f"color:{colors['primary']};")

        divider()
        render_run(initial_label)


async def _load_run_payload(run_id: str) -> tuple[pl.DataFrame, dict]:
    """Load run trades and metadata concurrently."""
    trades, run_meta = await asyncio.gather(aget_trades(run_id), aget_run_metadata(run_id))
    return trades, run_meta


def _validate_run_id(run_id: str) -> bool:
    """Validate run_id format (12-char lowercase hex)."""
    return bool(_RUN_ID_RE.fullmatch(str(run_id or "").strip()))


def _render_content(
    meta: dict,
    strategy: str,
    df: pl.DataFrame,
    run_meta: dict,
    colors: dict,
    theme: dict,
) -> None:
    """Render KPIs + tabs for the loaded run."""
    run_id = str(meta.get("run_id") or "")
    params = _extract_run_params(meta, run_meta)
    display_params = _effective_run_params(params)

    # ── Header: strategy badge + period ──────────────────────────────────────
    with ui.row().classes("w-full justify-between items-center mb-3"):
        ui.html(strat_badge(strategy))
        with ui.row().classes("items-center gap-3"):
            start = str(meta.get("start_date") or "")[:10]
            end = str(meta.get("end_date") or "")[:10]
            syms = meta.get("symbol_count", 0)
            ui.label(f"{start} → {end}  ·  {syms} symbols").classes("text-sm mono-font").style(
                f"color: {theme['text_secondary']};"
            )
            if run_id:
                ui.label(f"run_id: {run_id}").classes("text-xs mono-font").style(
                    f"color: {theme['text_muted']};"
                )

                async def _copy_run_id(_run_id: str = run_id) -> None:
                    await ui.run_javascript(f"navigator.clipboard.writeText({_run_id!a})")
                    ui.notify("Run ID copied", type="positive", timeout=900)

                (
                    ui.button(icon="content_copy", on_click=_copy_run_id)
                    .props("flat dense round size=sm")
                    .tooltip("Copy run ID")
                )

    # ── Parameter header strip ──────────────────────────────────────────────
    if display_params:
        param_header_strip(display_params)

    # ── Parameter detail card ────────────────────────────────────────────────
    param_detail_card(display_params)

    # ── Primary KPIs (compact — supporting metrics inline) ──────────────────
    n_trades = int(meta.get("trade_count") or (len(df) if not df.is_empty() else 0))
    n_syms = int(meta.get("symbol_count") or 0)
    allocated_capital = float(meta.get("allocated_capital") or 0.0)
    win_rate = float(meta.get("win_rate") or 0.0)
    total_pnl = float(meta.get("total_pnl") or 0.0)
    max_dd = abs(float(meta.get("max_dd_pct") or 0.0))
    calmar = float(meta.get("calmar") or 0.0)
    total_return = float(meta.get("total_return_pct") or 0.0)
    profit_factor = float(meta.get("profit_factor") or 0.0)
    cagr = float(meta.get("annual_return_pct") or 0.0)

    # Primary row: Win Rate, Total P/L, Calmar, Max Drawdown
    kpi_grid(
        [
            dict(
                title="Win Rate",
                value=f"{win_rate:.1f}%",
                subtitle="Winning trades / all trades",
                icon="target",
                color=colors["success"] if win_rate >= 40 else colors["error"],
            ),
            dict(
                title="Total P/L",
                value=f"₹{total_pnl:,.0f}",
                subtitle="Net rupees across saved trades",
                icon="monetization_on",
                color=colors["success"] if total_pnl >= 0 else colors["error"],
            ),
            dict(
                title="Calmar",
                value=f"{calmar:.2f}",
                subtitle="Return vs drawdown",
                icon="speed",
                color=colors["success"] if calmar >= 2.0 else colors["warning"],
            ),
            dict(
                title="Max Drawdown",
                value=f"-{format_drawdown_pct(max_dd)}",
                subtitle="Worst peak-to-trough loss",
                icon="trending_down",
                color=colors["error"],
            ),
        ],
        columns=4,
    )

    # Secondary metrics — styled mini cards
    with ui.row().classes("w-full gap-3 mb-4 flex-wrap"):
        _mini_card("Trades", f"{n_trades:,}", colors["info"])
        _mini_card("Symbols", str(n_syms), colors["info"])
        _mini_card(
            "Return",
            f"{total_return:.1f}%",
            colors["success"] if total_return >= 0 else colors["error"],
        )
        _mini_card(
            "PF",
            f"{profit_factor:.2f}",
            colors["success"] if profit_factor >= 1.5 else colors["warning"],
        )
        _mini_card("CAGR", f"{cagr:.1f}%", colors["success"] if cagr >= 0 else colors["error"])
        _mini_card("Capital", f"₹{allocated_capital:,.0f}", colors["info"])

    divider()

    if df.is_empty():
        expected_trades = int(meta.get("trade_count") or 0)
        msg = (
            "This run has no saved trades."
            if expected_trades <= 0
            else (
                f"No rows found in `backtest_results` for run_id `{run_id}` "
                f"(run_metrics says {expected_trades:,} trades). "
                "This usually means stale run_metrics after pruning; refresh/rebuild metrics."
            )
        )
        empty_state("No trade data", msg, icon="receipt_long")
        return

    with ui.dialog() as inspector_dialog:
        with ui.card().classes("w-full max-w-6xl mx-auto"):
            inspector_body = ui.column().classes("w-full gap-4")

    async def _open_trade_inspector(row: dict) -> None:
        payload = row
        nested_row = payload.get("row") if isinstance(payload, dict) else None
        if isinstance(nested_row, dict):
            payload = nested_row

        symbol = str(payload.get("symbol") or "")
        trade_date = str(payload.get("date") or payload.get("trade_date") or "")[:10]
        entry_time = str(payload.get("entry_time") or "")[:5]
        exit_time = str(payload.get("exit_time") or "")[:5]

        inspector_body.clear()
        with inspector_body:
            ui.label("Loading trade inspection...").classes("text-sm")
        inspector_dialog.open()
        details = await aget_trade_inspection(
            run_id=run_id,
            symbol=symbol,
            trade_date=trade_date,
            entry_time=entry_time,
            exit_time=exit_time,
        )
        inspector_body.clear()
        with inspector_body:
            if not details:
                empty_state(
                    "Trade inspection unavailable",
                    "Could not load daily CPR context and key candles for this trade.",
                    icon="search_off",
                )
            else:
                _render_trade_inspector(details, strategy, colors, theme)

    # Export button
    with ui.row().classes("mb-4"):
        export_button(
            df, filename=f"trades_{meta.get('run_id', 'run')[:8]}.csv", label="Export Trades CSV"
        )

    divider()

    # ── Analytics Tabs (5 tabs) ─────────────────────────────────────────────
    with ui.tabs().classes("w-full") as tabs:
        ui.tab("trades", label="Trades")
        ui.tab("top_trades", label="Top Trades")
        ui.tab("charts", label="Charts")
        ui.tab("analysis", label="Analysis")
        ui.tab("audit", label="Audit")

    with ui.tab_panels(tabs, value="trades").classes("w-full bg-transparent pt-4"):
        with ui.tab_panel("trades"):
            with ui.expansion("Trades", icon="table_chart").classes("w-full"):
                _tab_trades(df, colors, theme, _open_trade_inspector)
            with ui.expansion("Daily Summary", icon="calendar_view_day").classes("w-full"):
                _tab_daily_summary(df, colors, theme)
        with ui.tab_panel("top_trades"):
            _tab_winners_losers(df, colors, theme, _open_trade_inspector)
        with ui.tab_panel("charts"):
            _tab_equity(df, colors)
            divider()
            _tab_monthly(df, colors, theme)
            divider()
            _tab_daily_heatmap(df, colors, theme)
        with ui.tab_panel("analysis"):
            _tab_exits(df, colors, theme)
            divider()
            _tab_r_multiple(df, colors, theme)
            divider()
            _tab_per_symbol(df, colors, theme)
            divider()
            _tab_yearly(df, colors, theme)
        with ui.tab_panel("audit"):
            _tab_execution_audit(df, run_meta, colors, theme)


# ---------------------------------------------------------------------------
# Tab: Equity Curve
# ---------------------------------------------------------------------------
def _tab_equity(df: pl.DataFrame, colors: dict) -> None:
    sorted_df = df.sort("trade_date")
    pnl = sorted_df["profit_loss"].to_numpy()
    dates = sorted_df["trade_date"].cast(pl.Utf8).to_list()

    cumsum = np.cumsum(pnl)
    running_max = np.maximum.accumulate(cumsum)
    drawdown = (cumsum - running_max).tolist()
    cumsum_list = cumsum.tolist()

    def _rgba(hex_c: str, a: float = 0.18) -> str:
        h = hex_c.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{a})"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=drawdown,
            fill="tozeroy",
            fillcolor=_rgba(colors["error"], 0.15),
            line=dict(color=colors["error"], width=1),
            name="Drawdown (₹)",
            hovertemplate="%{x}<br>DD: ₹%{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=cumsum_list,
            mode="lines",
            line=dict(color=colors["primary"], width=2.5),
            name="Equity (₹)",
            fill="tozeroy",
            fillcolor=_rgba(colors["primary"], 0.07),
            hovertemplate="%{x}<br>Equity: ₹%{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(title="Equity Curve with Drawdown", xaxis_title="Date", yaxis_title="₹")
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-80")


# ---------------------------------------------------------------------------
# Tab: Yearly
# ---------------------------------------------------------------------------
def _tab_yearly(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    yearly = (
        df.with_columns(
            pl.col("trade_date").dt.year().cast(pl.Utf8).alias("year"),
            (pl.col("profit_loss") > 0).alias("is_win"),
        )
        .group_by("year")
        .agg(
            [
                pl.len().alias("trades"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
            ]
        )
        .sort("year")
    )

    if yearly.is_empty():
        return

    years = yearly["year"].to_list()
    pnls = yearly["total_pnl"].to_list()
    bar_colors = [colors["success"] if p >= 0 else colors["error"] for p in pnls]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=years,
            y=pnls,
            marker_color=bar_colors,
            text=[f"₹{int(p):,}" for p in pnls],
            textposition="outside",
            name="P/L",
        )
    )
    fig.update_layout(title="Annual P/L", xaxis_title="Year", yaxis_title="₹", showlegend=False)
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-72 mb-4")

    rows = [
        {
            "year": r["year"],
            "trades": int(r["trades"]),
            "win_rate": round(float(r["win_rate"]), 1),
            "total_pnl": round(float(r["total_pnl"]), 0),
        }
        for r in yearly.iter_rows(named=True)
    ]
    paginated_table(
        columns=[
            {"name": "year", "label": "Year", "field": "year", "align": "left"},
            {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
            {"name": "win_rate", "label": "Win Rate %", "field": "win_rate", "align": "right"},
            {"name": "total_pnl", "label": "Total P/L", "field": "total_pnl", "align": "right"},
        ],
        rows=rows,
        row_key="year",
        page_size=15,
    )


# ---------------------------------------------------------------------------
# Tab: Monthly Heatmap
# ---------------------------------------------------------------------------
def _tab_monthly(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    monthly = (
        df.with_columns(
            pl.col("trade_date").dt.year().alias("year"),
            pl.col("trade_date").dt.month().alias("month"),
        )
        .group_by(["year", "month"])
        .agg(pl.col("profit_loss").sum().round(0).alias("pnl"))
    )

    if monthly.is_empty():
        return

    years = sorted(monthly["year"].unique().to_list())
    year_index = {y: i for i, y in enumerate(years)}

    z = [[None] * 12 for _ in years]
    for row in monthly.iter_rows(named=True):
        z[year_index[row["year"]]][row["month"] - 1] = row["pnl"]

    pnl_max: float = max(abs(v) for row in z for v in row if v is not None) or 1

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=_MONTHS,
            y=[str(y) for y in years],
            colorscale=[
                [0, colors["error"]],
                [0.5, theme["surface_border"]],
                [1, colors["success"]],
            ],
            zmid=0,
            zmin=-pnl_max,
            zmax=pnl_max,
            text=[[f"₹{v:,.0f}" if v is not None else "" for v in row] for row in z],
            texttemplate="%{text}",
            textfont={"size": 9},
            hovertemplate="Month: %{x}<br>Year: %{y}<br>P/L: ₹%{z:,.0f}<extra></extra>",
            colorbar=dict(title="₹"),
        )
    )
    fig.update_layout(
        title="Monthly P/L Heatmap",
        height=max(300, len(years) * 38 + 100),
    )
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full")


# ---------------------------------------------------------------------------
# Tab: Daily Summary (per-date P/L aggregation)
# ---------------------------------------------------------------------------
def _tab_daily_summary(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    """Per-date trade summary: count, longs, shorts, wins, win_rate, day P/L."""
    if df.is_empty():
        return

    daily = (
        df.with_columns((pl.col("profit_loss") > 0).alias("_is_win"))
        .group_by("trade_date")
        .agg(
            pl.len().alias("trades"),
            pl.col("profit_loss").sum().round(2).alias("day_pnl"),
            pl.col("_is_win").sum().alias("wins"),
            (pl.col("_is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
            (pl.col("direction") == "LONG").cast(pl.Int8).sum().alias("longs"),
            (pl.col("direction") == "SHORT").cast(pl.Int8).sum().alias("shorts"),
        )
        .sort("trade_date", descending=True)
    )

    ui.label("Daily Summary").classes("text-base font-semibold mb-2")
    rows = [
        {
            "date": str(row["trade_date"] or "")[:10],
            "trades": str(int(row["trades"])),
            "longs": str(int(row["longs"])),
            "shorts": str(int(row["shorts"])),
            "wins": str(int(row["wins"])),
            "win_rate": f"{float(row['win_rate']):.1f}%",
            "day_pnl": round(float(row["day_pnl"]), 2),
        }
        for row in daily.iter_rows(named=True)
    ]
    paginated_table(
        columns=[
            {"name": "date", "label": "Date", "field": "date", "align": "left"},
            {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
            {"name": "longs", "label": "Long", "field": "longs", "align": "right"},
            {"name": "shorts", "label": "Short", "field": "shorts", "align": "right"},
            {"name": "wins", "label": "Wins", "field": "wins", "align": "right"},
            {"name": "win_rate", "label": "Win %", "field": "win_rate", "align": "right"},
            {"name": "day_pnl", "label": "Day P/L", "field": "day_pnl", "align": "right"},
        ],
        rows=rows,
        row_key="date",
        page_size=25,
        sort_by="date",
        descending=True,
    )


# ---------------------------------------------------------------------------
# Tab: Daily P/L Heatmap
# ---------------------------------------------------------------------------
def _tab_daily_heatmap(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    """Calendar-style daily P/L heatmap — each cell is one trading day."""
    daily = (
        df.group_by("trade_date")
        .agg(pl.col("profit_loss").sum().round(0).alias("pnl"))
        .sort("trade_date")
    )

    if daily.is_empty():
        return

    # Build calendar grid: rows = year-week, columns = weekday
    daily_with_cal = daily.with_columns(
        pl.col("trade_date").dt.year().alias("year"),
        pl.col("trade_date").dt.month().alias("month"),
        pl.col("trade_date").dt.day().alias("day"),
    )

    # Group by year-month for a simple grid
    months = (
        daily_with_cal.group_by(["year", "month"])
        .agg(
            [
                pl.col("day").alias("days"),
                pl.col("pnl").alias("pnls"),
            ]
        )
        .sort(["year", "month"])
    )

    # Build a flat heatmap: x = day-of-month, y = year-month
    y_labels = []
    z_data = []
    text_data = []
    for row in months.iter_rows(named=True):
        label = f"{row['year']}-{row['month']:02d}"
        y_labels.append(label)
        day_pnl = {int(d): float(p) for d, p in zip(row["days"], row["pnls"], strict=True)}
        z_row: list[float | None] = [None] * 31
        t_row = [""] * 31
        for d in range(1, 32):
            if d in day_pnl:
                z_row[d - 1] = day_pnl[d]
                t_row[d - 1] = f"₹{day_pnl[d]:,.0f}"
        z_data.append(z_row)
        text_data.append(t_row)

    pnl_max: float = max(abs(v) for row in z_data for v in row if v is not None) or 1

    fig = go.Figure(
        data=go.Heatmap(
            z=z_data,
            x=list(range(1, 32)),
            y=y_labels,
            colorscale=[
                [0, colors["error"]],
                [0.5, theme["surface_border"]],
                [1, colors["success"]],
            ],
            zmid=0,
            zmin=-pnl_max,
            zmax=pnl_max,
            text=text_data,
            texttemplate="%{text}",
            textfont={"size": 8},
            hovertemplate="Day %{x}<br>%{y}<br>P/L: ₹%{z:,.0f}<extra></extra>",
            colorbar=dict(title="₹", tickfont={"size": 10}),
            xgap=2,
            ygap=2,
        )
    )
    fig.update_layout(
        title="Daily P/L Heatmap",
        xaxis_title="Day of Month",
        xaxis=dict(dtick=1, tickfont={"size": 10}),
        height=max(350, len(y_labels) * 28 + 120),
    )
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full")


# ---------------------------------------------------------------------------
# Tab: Per Symbol
# ---------------------------------------------------------------------------
def _tab_per_symbol(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    sym_df = (
        df.with_columns((pl.col("profit_loss") > 0).alias("is_win"))
        .group_by("symbol")
        .agg(
            [
                pl.len().alias("trades"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
                pl.col("profit_loss").max().round(0).alias("best"),
                pl.col("profit_loss").min().round(0).alias("worst"),
            ]
        )
        .sort("total_pnl", descending=True)
    )

    symbols = sym_df["symbol"].to_list()
    pnls = sym_df["total_pnl"].to_list()
    bar_colors = [colors["success"] if p >= 0 else colors["error"] for p in pnls]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=pnls,
            y=symbols,
            orientation="h",
            marker_color=bar_colors,
            hovertemplate="%{y}: ₹%{x:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="P/L by Symbol",
        xaxis_title="₹",
        height=max(350, len(symbols) * 20 + 80),
        yaxis=dict(autorange="reversed"),
    )
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full mb-4")

    rows = [
        {
            "symbol": r["symbol"],
            "trades": int(r["trades"]),
            "win_rate": round(float(r["win_rate"]), 1),
            "total_pnl": round(float(r["total_pnl"]), 0),
            "best": round(float(r["best"]), 0),
            "worst": round(float(r["worst"]), 0),
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
            },
            {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
            {"name": "win_rate", "label": "Win %", "field": "win_rate", "align": "right"},
            {
                "name": "total_pnl",
                "label": "Total P/L",
                "field": "total_pnl",
                "align": "right",
            },
            {"name": "best", "label": "Best", "field": "best", "align": "right"},
            {"name": "worst", "label": "Worst", "field": "worst", "align": "right"},
        ],
        rows=rows,
        row_key="symbol",
        page_size=25,
    )


# ---------------------------------------------------------------------------
# Tab: R-Multiple
# ---------------------------------------------------------------------------
def _tab_r_multiple(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    if "entry_price" not in df.columns or "sl_price" not in df.columns:
        ui.label("R-multiple data not available (missing price columns).").style(
            f"color: {theme['text_muted']};"
        )
        return

    pnl_r_raw = df.with_columns(
        pl.when(pl.col("direction") == "LONG")
        .then(
            (pl.col("exit_price") - pl.col("entry_price"))
            / (pl.col("entry_price") - pl.col("sl_price"))
        )
        .otherwise(
            (pl.col("entry_price") - pl.col("exit_price"))
            / (pl.col("sl_price") - pl.col("entry_price"))
        )
        .alias("pnl_r")
    )["pnl_r"].to_numpy()

    pnl_r_raw = pnl_r_raw[np.isfinite(pnl_r_raw)]
    if len(pnl_r_raw) == 0:
        return

    @ui.refreshable
    def _render_r_chart(clip: bool) -> None:
        pnl_r = np.clip(pnl_r_raw, -5.0, 8.0) if clip else pnl_r_raw
        mean_r = float(pnl_r.mean())
        wins = pnl_r[pnl_r > 0]
        losses = pnl_r[pnl_r <= 0]

        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=pnl_r,
                nbinsx=52,
                marker_color=[colors["success"] if v >= 0 else colors["error"] for v in pnl_r],
                opacity=0.8,
                name="R-Multiple",
            )
        )
        fig.add_vline(
            x=0, line_dash="dash", line_color=theme["text_muted"], annotation_text="Break-even"
        )
        fig.add_vline(
            x=mean_r,
            line_dash="dash",
            line_color=colors["primary"],
            annotation_text=f"Mean {mean_r:.2f}R",
        )
        clip_note = " (clipped to -5R..8R)" if clip else " (full range)"
        fig.update_layout(
            title=f"R-Multiple Distribution{clip_note}",
            xaxis_title="R",
            yaxis_title="Count",
            bargap=0.05,
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-72")

        with ui.row().classes("gap-8 mt-4 flex-wrap"):
            _stat_chip("Mean R", f"{mean_r:.2f}R", colors["primary"])
            _stat_chip("Avg Win", f"{wins.mean():.2f}R" if len(wins) else "—", colors["success"])
            _stat_chip("Avg Loss", f"{losses.mean():.2f}R" if len(losses) else "—", colors["error"])
            _stat_chip("Expectancy", f"{mean_r:.3f}R", colors["info"])

    clip_state = {"clip": True}
    ui.checkbox(
        "Clip outliers (-5R to 8R)",
        value=True,
        on_change=lambda e: (clip_state.update(clip=e.value), _render_r_chart.refresh(e.value)),
    ).classes("mb-2")
    _render_r_chart(True)


# ---------------------------------------------------------------------------
# Tab: Exit Reasons
# ---------------------------------------------------------------------------
def _tab_exits(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    exit_df = (
        df.with_columns((pl.col("profit_loss") > 0).alias("is_win"))
        .group_by("exit_reason")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("profit_loss").sum().round(0).alias("total_pnl"),
                pl.col("profit_loss").mean().round(1).alias("avg_pnl"),
                (pl.col("is_win").sum() * 100.0 / pl.len()).round(1).alias("win_rate"),
            ]
        )
        .sort("count", descending=True)
    )

    if exit_df.is_empty():
        return

    reasons = exit_df["exit_reason"].to_list()
    counts = exit_df["count"].to_list()

    _exit_palette = {
        "TARGET": colors["success"],
        "INITIAL_SL": colors["error"],
        "BREAKEVEN_SL": colors["warning"],
        "TRAILING_SL": "#f97316",
        "TIME": colors["gray"],
        "REVERSAL": "#8b5cf6",
        "CANDLE_EXIT": colors["gray"],
    }
    pie_colors = [_exit_palette.get(r, colors["gray"]) for r in reasons]

    with ui.row().classes("w-full gap-4"):
        with ui.column().classes("flex-1"):
            fig_pie = go.Figure(
                data=go.Pie(
                    labels=reasons,
                    values=counts,
                    hole=0.35,
                    marker_colors=pie_colors,
                    textinfo="label+percent",
                )
            )
            fig_pie.update_layout(title="Exit Reason Distribution", showlegend=True)
            apply_chart_theme(fig_pie)
            ui.plotly(fig_pie).classes("w-full h-72")

        with ui.column().classes("flex-1"):
            rows = [
                {
                    "reason": r["exit_reason"],
                    "count": int(r["count"]),
                    "avg_pnl": round(float(r["avg_pnl"]), 1),
                    "win_rate": round(float(r["win_rate"]), 1),
                    "total_pnl": round(float(r["total_pnl"]), 0),
                }
                for r in exit_df.iter_rows(named=True)
            ]
            paginated_table(
                columns=[
                    {"name": "reason", "label": "Reason", "field": "reason", "align": "left"},
                    {"name": "count", "label": "Count", "field": "count", "align": "right"},
                    {"name": "avg_pnl", "label": "Avg P/L", "field": "avg_pnl", "align": "right"},
                    {"name": "win_rate", "label": "Win %", "field": "win_rate", "align": "right"},
                    {
                        "name": "total_pnl",
                        "label": "Total P/L",
                        "field": "total_pnl",
                        "align": "right",
                    },
                ],
                rows=rows,
                row_key="reason",
                page_size=10,
            )


# ---------------------------------------------------------------------------
# Tab: Execution Audit
# ---------------------------------------------------------------------------
def _tab_execution_audit(df: pl.DataFrame, run_meta: dict, colors: dict, theme: dict) -> None:
    params = run_meta.get("params") if isinstance(run_meta, dict) else {}
    params = params if isinstance(params, dict) else {}

    info_box(
        "Execution Audit verifies how the run actually traded: side split, timing distribution, "
        "exit mix, and per-day trade outcomes. This run uses Daily CPR context (previous-day 1D CPR).",
        color="blue",
    )

    enriched = df.with_columns(
        pl.col("trade_date").cast(pl.Date, strict=False).alias("_trade_date"),
        pl.col("entry_time").cast(pl.Utf8).str.slice(0, 5).alias("_entry_hhmm"),
        pl.col("exit_time").cast(pl.Utf8).str.slice(0, 5).alias("_exit_hhmm"),
        (pl.col("profit_loss") > 0).alias("_is_win"),
    )

    long_trades = int(enriched.filter(pl.col("direction") == "LONG").height)
    short_trades = int(enriched.filter(pl.col("direction") == "SHORT").height)
    total_days = int(enriched["_trade_date"].n_unique()) if "_trade_date" in enriched.columns else 0

    def _mean_or_zero(series: pl.Series | None) -> float:
        if series is None:
            return 0.0
        value = series.drop_nulls().mean()
        return float(value) if value is not None else 0.0

    avg_pos_value = _mean_or_zero(
        enriched.get_column("position_value") if "position_value" in enriched.columns else None
    )
    avg_or_atr = _mean_or_zero(
        enriched.get_column("or_atr_ratio") if "or_atr_ratio" in enriched.columns else None
    )
    avg_abs_gap = _mean_or_zero(
        enriched.get_column("gap_pct").abs() if "gap_pct" in enriched.columns else None
    )

    kpi_grid(
        [
            dict(
                title="Long Trades",
                value=f"{long_trades:,}",
                icon="north_east",
                color=colors["success"],
            ),
            dict(
                title="Short Trades",
                value=f"{short_trades:,}",
                icon="south_east",
                color=colors["error"],
            ),
            dict(
                title="Trading Days",
                value=f"{total_days:,}",
                icon="calendar_month",
                color=colors["info"],
            ),
            dict(
                title="Avg Position ₹",
                value=f"₹{avg_pos_value:,.0f}",
                icon="account_balance_wallet",
                color=colors["primary"],
            ),
            dict(
                title="Avg OR/ATR",
                value=f"{avg_or_atr:.3f}",
                icon="bar_chart",
                color=colors["warning"],
            ),
            dict(
                title="Avg |Gap| %",
                value=f"{avg_abs_gap:.3f}%",
                icon="difference",
                color=colors["info"],
            ),
        ],
        columns=6,
    )

    # Entry/Exit timing shape
    entry_bins = (
        enriched.group_by("_entry_hhmm")
        .agg(pl.len().alias("trades"), pl.col("profit_loss").sum().round(0).alias("pnl"))
        .sort("_entry_hhmm")
    )
    exit_mix = (
        enriched.group_by(["direction", "exit_reason"])
        .agg(pl.len().alias("count"))
        .sort(["exit_reason", "direction"])
    )

    with ui.row().classes("w-full gap-4"):
        with ui.column().classes("flex-1"):
            if not entry_bins.is_empty():
                fig_entry = go.Figure(
                    data=[
                        go.Bar(
                            x=entry_bins["_entry_hhmm"].to_list(),
                            y=entry_bins["trades"].to_list(),
                            marker_color=colors["primary"],
                            hovertemplate="Entry %{x}<br>Trades %{y}<extra></extra>",
                        )
                    ]
                )
                fig_entry.update_layout(
                    title="Entry-Time Distribution",
                    xaxis_title="Entry Time (HH:MM)",
                    yaxis_title="Trades",
                )
                apply_chart_theme(fig_entry)
                ui.plotly(fig_entry).classes("w-full h-72")

        with ui.column().classes("flex-1"):
            if not exit_mix.is_empty():
                reasons = sorted({str(v) for v in exit_mix["exit_reason"].to_list()})
                fig_exit = go.Figure()
                for side, color in (("LONG", colors["success"]), ("SHORT", colors["error"])):
                    side_counts = {
                        str(r["exit_reason"]): int(r["count"])
                        for r in exit_mix.filter(pl.col("direction") == side).iter_rows(named=True)
                    }
                    fig_exit.add_trace(
                        go.Bar(
                            x=reasons,
                            y=[side_counts.get(reason, 0) for reason in reasons],
                            name=side,
                            marker_color=color,
                        )
                    )
                fig_exit.update_layout(
                    title="Exit Reasons by Direction",
                    xaxis_title="Exit Reason",
                    yaxis_title="Trades",
                    barmode="group",
                )
                apply_chart_theme(fig_exit)
                ui.plotly(fig_exit).classes("w-full h-72")


# ---------------------------------------------------------------------------
# Tab: Trade List
# ---------------------------------------------------------------------------
def _tab_trades(df: pl.DataFrame, colors: dict, theme: dict, on_trade_click) -> None:
    # Pre-sort: date descending, then entry_time descending within each date
    sorted_df = df.sort(["trade_date", "entry_time"], descending=[True, True])

    rows = [
        {
            "idx": i,
            "symbol": r["symbol"],
            "date": str(r.get("trade_date") or "")[:10],
            "entry_time": str(r.get("entry_time") or "")[:5],
            "exit_time": str(r.get("exit_time") or "")[:5],
            "dir": r.get("direction", ""),
            "entry": round(float(r.get("entry_price") or 0.0), 2),
            "exit": round(float(r.get("exit_price") or 0.0), 2),
            "qty": int(r.get("position_size") or 0),
            "position_value": round(float(r.get("position_value") or 0.0), 2),
            "pnl": round(float(r.get("profit_loss") or 0.0), 2),
            "pnl_pct": round(float(r.get("profit_loss_pct") or 0.0), 4),
            "sl": round(float(r.get("sl_price") or 0.0), 2),
            "target": round(float(r.get("target_price") or 0.0), 2),
            "atr": round(float(r.get("atr") or 0.0), 4),
            "or_atr": round(float(r.get("or_atr_ratio") or 0.0), 4),
            "cpr_width_pct": round(float(r.get("cpr_width_pct") or 0.0), 4),
            "gap_pct": round(float(r.get("gap_pct") or 0.0), 4),
            "exit_reason": r.get("exit_reason", ""),
        }
        for i, r in enumerate(sorted_df.iter_rows(named=True))
    ]

    # Build date options from trade data
    date_options = ["ALL", *sorted({r["date"] for r in rows if r.get("date")})]
    exit_reasons = [
        "ALL",
        *sorted({r.get("exit_reason", "") for r in rows if r.get("exit_reason")}),
    ]

    filters: dict = {"date": "", "symbol": "", "direction": "ALL", "exit_reason": "ALL"}

    info_box(
        "Click any trade row to inspect the Daily CPR source, previous-day OHLC, "
        "09:15 signal candle, entry candle, exit candle, and TradingView checklist.",
        color="blue",
    )
    ui.label(
        f"Loaded trades: {len(rows):,}  |  LONG: {sum(1 for r in rows if r['dir'] == 'LONG'):,}"
        f"  |  SHORT: {sum(1 for r in rows if r['dir'] == 'SHORT'):,}"
    ).classes("text-sm mono-font mb-2").style(f"color: {theme['text_secondary']};")

    @ui.refreshable
    def _filtered() -> None:
        filtered = apply_trade_filters(rows, filters)
        paginated_table(
            columns=[
                {"name": "date", "label": "Date", "field": "date", "align": "left"},
                {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                {"name": "entry_time", "label": "In", "field": "entry_time", "align": "center"},
                {"name": "exit_time", "label": "Out", "field": "exit_time", "align": "center"},
                {"name": "dir", "label": "Dir", "field": "dir", "align": "center"},
                {"name": "entry", "label": "Entry", "field": "entry", "align": "right"},
                {"name": "exit", "label": "Exit", "field": "exit", "align": "right"},
                {"name": "qty", "label": "Qty", "field": "qty", "align": "right"},
                {
                    "name": "position_value",
                    "label": "Pos ₹",
                    "field": "position_value",
                    "align": "right",
                },
                {"name": "pnl", "label": "P/L", "field": "pnl", "align": "right"},
                {"name": "pnl_pct", "label": "P/L %", "field": "pnl_pct", "align": "right"},
                {"name": "exit_reason", "label": "Exit", "field": "exit_reason", "align": "left"},
                {"name": "sl", "label": "SL", "field": "sl", "align": "right"},
                {"name": "target", "label": "Target", "field": "target", "align": "right"},
                {"name": "atr", "label": "ATR", "field": "atr", "align": "right"},
                {"name": "or_atr", "label": "OR/ATR", "field": "or_atr", "align": "right"},
                {
                    "name": "cpr_width_pct",
                    "label": "CPR W%",
                    "field": "cpr_width_pct",
                    "align": "right",
                },
                {"name": "gap_pct", "label": "Gap %", "field": "gap_pct", "align": "right"},
            ],
            rows=filtered,
            row_key="idx",
            page_size=50,
            on_row_click=on_trade_click,
            sort_by="date",
            descending=True,
        )

    trade_filter_bar(
        filters,
        _filtered.refresh,
        show_date=True,
        date_options=date_options,
        show_symbol=True,
        show_direction=True,
        show_exit=True,
        exit_options=exit_reasons,
    )

    _filtered()


def _tab_winners_losers(df: pl.DataFrame, colors: dict, theme: dict, on_trade_click) -> None:
    winners = (
        df.sort("profit_loss_pct", descending=True).head(min(25, len(df))).with_row_index("idx")
    )
    losers = df.sort("profit_loss_pct").head(min(25, len(df))).with_row_index("idx")

    def _rows(frame: pl.DataFrame) -> list[dict]:
        return [
            {
                "idx": int(r["idx"]),
                "symbol": r["symbol"],
                "date": str(r.get("trade_date") or "")[:10],
                "entry_time": str(r.get("entry_time") or "")[:5],
                "exit_time": str(r.get("exit_time") or "")[:5],
                "dir": r.get("direction", ""),
                "entry": round(float(r.get("entry_price") or 0.0), 2),
                "exit": round(float(r.get("exit_price") or 0.0), 2),
                "pnl": round(float(r.get("profit_loss") or 0.0), 2),
                "pnl_pct": round(float(r.get("profit_loss_pct") or 0.0), 4),
                "exit_reason": r.get("exit_reason", ""),
            }
            for r in frame.iter_rows(named=True)
        ]

    columns = [
        {"name": "symbol", "label": "Symbol", "field": "symbol"},
        {"name": "date", "label": "Date", "field": "date"},
        {"name": "entry_time", "label": "In", "field": "entry_time"},
        {"name": "exit_time", "label": "Out", "field": "exit_time"},
        {"name": "dir", "label": "Dir", "field": "dir"},
        {"name": "entry", "label": "Entry", "field": "entry", "align": "right"},
        {"name": "exit", "label": "Exit", "field": "exit", "align": "right"},
        {"name": "pnl", "label": "P/L", "field": "pnl", "align": "right"},
        {"name": "pnl_pct", "label": "P/L %", "field": "pnl_pct", "align": "right"},
        {"name": "exit_reason", "label": "Exit", "field": "exit_reason"},
    ]

    info_box(
        "Click any winner or loser to open the Daily CPR inspection view. "
        "Direction is shown explicitly here so longs and shorts are not ambiguous.",
        color="blue",
    )

    with ui.row().classes("w-full gap-4"):
        with ui.column().classes("flex-1"):
            ui.label("Top Winners").classes("text-lg font-semibold mb-2").style(
                f"color: {COLORS['success']};"
            )
            paginated_table(
                columns=columns,
                rows=_rows(winners),
                row_key="idx",
                page_size=25,
                on_row_click=on_trade_click,
            )
        with ui.column().classes("flex-1"):
            ui.label("Top Losers").classes("text-lg font-semibold mb-2").style(
                f"color: {COLORS['error']};"
            )
            paginated_table(
                columns=columns,
                rows=_rows(losers),
                row_key="idx",
                page_size=25,
                on_row_click=on_trade_click,
            )


def _tab_calendar_heatmap(df: pl.DataFrame, colors: dict, theme: dict) -> None:
    """Render a calendar heatmap of daily P/L by date (GitHub-style contribution graph)."""
    if df.is_empty():
        ui.label("No trade data for calendar.").classes("text-sm").style(
            f"color: {theme['text_muted']};"
        )
        return

    trade_date_col = df.schema.get("trade_date")
    if trade_date_col == pl.Date:
        df_dates = df.with_columns(pl.col("trade_date").alias("dt"))
    else:
        df_dates = df.with_columns(pl.col("trade_date").str.to_date().alias("dt")).filter(
            pl.col("dt").is_not_null()
        )

    if df_dates.is_empty():
        ui.label("No valid trade dates for calendar.").classes("text-sm").style(
            f"color: {theme['text_muted']};"
        )
        return

    daily_pnl = df_dates.group_by("dt").agg(pl.col("profit_loss").sum().alias("day_pnl")).sort("dt")

    if daily_pnl.is_empty():
        ui.label("Insufficient data for calendar.").classes("text-sm").style(
            f"color: {theme['text_muted']};"
        )
        return

    pnl_by_date = {row["dt"]: row["day_pnl"] for row in daily_pnl.iter_rows(named=True)}

    years = sorted({dt.year for dt in pnl_by_date.keys()})
    if not years:
        ui.label("No valid years for calendar.").classes("text-sm").style(
            f"color: {theme['text_muted']};"
        )
        return

    all_pnls = list(pnl_by_date.values())
    max_abs = max(abs(p) for p in all_pnls) if all_pnls else 1

    for year in years:
        ui.label(f"{year}").classes("text-sm font-semibold mt-4 mb-2")

        months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        week_labels = ["", "Mon", "", "Wed", "", "Fri", ""]

        grid = [[None] * 12 for _ in range(7)]

        for dt, pnl in pnl_by_date.items():
            if dt.year == year:
                month_idx = dt.month - 1
                week_idx = dt.weekday()
                if 0 <= month_idx < 12 and 0 <= week_idx < 7:
                    grid[week_idx][month_idx] = pnl

        colorscale = [
            [0.0, colors["error"]],
            [0.5, theme["surface_border"]],
            [1.0, colors["success"]],
        ]

        fig = go.Figure(
            data=go.Heatmap(
                z=grid,
                x=months,
                y=week_labels,
                colorscale=colorscale,
                zmid=0,
                zmin=-max_abs,
                zmax=max_abs,
                showscale=False,
                hovertemplate="Month: %{x}<br>Day: %{y}<br>P/L: ₹%{z:,.0f}<extra></extra>",
            )
        )

        fig.update_layout(
            xaxis=dict(
                tickfont=dict(color=theme["text_secondary"], size=10),
                gridcolor=theme["surface_border"],
            ),
            yaxis=dict(
                tickfont=dict(color=theme["text_secondary"], size=10),
                gridcolor=theme["surface_border"],
            ),
            paper_bgcolor=theme["surface"],
            plot_bgcolor=theme["surface"],
            margin=dict(l=40, r=10, t=10, b=30),
            height=180,
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _render_trade_inspector(details: dict, strategy: str, colors: dict, theme: dict) -> None:
    # Defensive check: if client disconnected, don't render anything
    # This prevents "Client has been deleted" errors when user navigates away
    try:
        if not ui.context.client.has_socket_connection:
            return
    except (AttributeError, RuntimeError):
        return

    trade = details.get("trade", {})
    daily = details.get("daily_cpr", {})
    derived = details.get("derived", {})
    candles = details.get("candles", {})

    symbol = str(trade.get("symbol") or "")
    trade_date = str(trade.get("trade_date") or "")
    direction = str(trade.get("direction") or "")
    direction_color = colors["success"] if direction == "LONG" else colors["error"]
    prev_date = str(daily.get("prev_date") or "")
    signal_time = str(derived.get("signal_time") or "09:15")
    entry_scan_start = str(derived.get("entry_scan_start") or "09:20")
    tc = float(daily.get("tc") or 0.0)
    bc = float(daily.get("bc") or 0.0)
    cpr_lower = min(tc, bc)
    cpr_upper = max(tc, bc)

    page_header(
        f"{symbol} · {trade_date}",
        subtitle=(
            f"{strategy} trade inspection. Daily CPR is computed from the previous 1D candle "
            f"({prev_date or 'previous trading day'}), then applied on the current 5-minute session."
        ),
    )

    with ui.row().classes("w-full items-center gap-3 flex-wrap"):
        ui.label(direction).classes("text-sm font-bold mono-font").style(
            f"color:{direction_color};"
        )
        ui.html(exit_badge(str(trade.get("exit_reason") or "")))
        ui.label(
            f"Entry {float(trade.get('entry_price') or 0.0):.2f} · "
            f"Exit {float(trade.get('exit_price') or 0.0):.2f} · "
            f"P/L ₹{float(trade.get('profit_loss') or 0.0):,.2f}"
        ).classes("text-sm mono-font").style(f"color:{theme['text_secondary']};")

    info_box(
        "TradingView setup: use a previous-day Daily CPR / Floor Pivot indicator from the prior "
        "session only. Interpret it as a band: upper = max(TC, BC), lower = min(TC, BC). "
        "If the indicator is weekly, monthly, Camarilla, Woodie, or mixes multiple sessions, "
        "it is the wrong one for this strategy.",
        color="yellow",
    )

    with ui.grid(columns=4).classes("w-full gap-3"):
        _inspection_metric("Prev Day", prev_date or "-", colors["info"])
        _inspection_metric("Signal Candle", signal_time, colors["info"])
        _inspection_metric("Entry Time", str(trade.get("entry_time") or "-"), direction_color)
        _inspection_metric("Exit Time", str(trade.get("exit_time") or "-"), colors["warning"])

    with ui.row().classes("w-full gap-4 items-start flex-wrap"):
        with ui.column().classes("flex-1 gap-2").style("min-width:320px;"):
            ui.label("Daily CPR Source").classes("text-base font-semibold")
            _inspection_table(
                [
                    ("Source timeframe", "Previous trading day 1D candle"),
                    ("Previous day", prev_date or "-"),
                    ("Prev High", _fmt_num(daily.get("prev_high"))),
                    ("Prev Low", _fmt_num(daily.get("prev_low"))),
                    ("Prev Close", _fmt_num(daily.get("prev_close"))),
                    ("Pivot", _fmt_num(daily.get("pivot"))),
                    ("BC", _fmt_num(daily.get("bc"))),
                    ("TC", _fmt_num(daily.get("tc"))),
                    ("Lower CPR", _fmt_num(cpr_lower)),
                    ("Upper CPR", _fmt_num(cpr_upper)),
                    ("R1", _fmt_num(daily.get("r1"))),
                    ("S1", _fmt_num(daily.get("s1"))),
                    ("ATR", _fmt_num(daily.get("atr"), 4)),
                    ("CPR Width %", _fmt_num(daily.get("cpr_width_pct"), 4)),
                    ("CPR Shift", str(daily.get("cpr_shift") or "-")),
                    ("Is Narrowing", "Yes" if int(daily.get("is_narrowing") or 0) else "No"),
                ],
                theme,
            )
        with ui.column().classes("flex-1 gap-2").style("min-width:320px;"):
            ui.label("Why Picked").classes("text-base font-semibold")
            _inspection_table(
                [
                    ("Direction rule", str(derived.get("setup_rule") or "-")),
                    ("Open Side", str(daily.get("open_side") or "-")),
                    ("5m Direction", str(daily.get("direction_5") or "-")),
                    ("09:15 Close", _fmt_num(daily.get("or_close_5"))),
                    ("Entry scan start", entry_scan_start),
                    ("Trigger", _fmt_num(derived.get("trigger_price"), 4)),
                    ("Min qualifying close", _fmt_num(derived.get("min_signal_close"), 4)),
                    (
                        "Target level",
                        f"{derived.get('target_label') or '-'} @ {_fmt_num(trade.get('target_price'))}",
                    ),
                    ("Normalized stop", _fmt_num(trade.get("sl_price"))),
                    ("Gap % at open", _fmt_num(daily.get("gap_pct_open"), 4) + "%"),
                    ("OR/ATR (5m)", _fmt_num(daily.get("or_atr_5"), 4)),
                ],
                theme,
            )

    ui.label("Key Candles").classes("text-base font-semibold")
    key_rows = []
    for tm, label in [
        (signal_time, "Direction-defining 09:15 candle"),
        (str(trade.get("entry_time") or ""), "Entry candle"),
        (str(trade.get("exit_time") or ""), "Exit candle"),
    ]:
        candle = candles.get(tm)
        if not candle:
            continue
        key_rows.append(
            {
                "label": label,
                "time": tm,
                "open": round(float(candle.get("open") or 0.0), 2),
                "high": round(float(candle.get("high") or 0.0), 2),
                "low": round(float(candle.get("low") or 0.0), 2),
                "close": round(float(candle.get("close") or 0.0), 2),
            }
        )
    paginated_table(
        columns=[
            {"name": "label", "label": "Role", "field": "label", "align": "left"},
            {"name": "time", "label": "Time", "field": "time", "align": "center"},
            {"name": "open", "label": "Open", "field": "open", "align": "right"},
            {"name": "high", "label": "High", "field": "high", "align": "right"},
            {"name": "low", "label": "Low", "field": "low", "align": "right"},
            {"name": "close", "label": "Close", "field": "close", "align": "right"},
        ],
        rows=key_rows,
        row_key="label",
        page_size=10,
    )

    _render_trade_explanation(trade, daily, derived, candles, theme)


def _render_trade_explanation(
    trade: dict,
    daily: dict,
    derived: dict,
    candles: dict,
    theme: dict,
) -> None:
    signal = candles.get(str(derived.get("signal_time") or "09:15"), {})
    entry = candles.get(str(trade.get("entry_time") or ""), {})
    direction = str(trade.get("direction") or "")
    tc = float(daily.get("tc") or 0.0)
    bc = float(daily.get("bc") or 0.0)
    cpr_lower = min(tc, bc)
    cpr_upper = max(tc, bc)
    trigger = float(derived.get("trigger_price") or 0.0)
    threshold = float(derived.get("min_signal_close") or 0.0)
    signal_close = float(signal.get("close") or 0.0)
    target_label = str(derived.get("target_label") or "-")
    exit_reason = str(trade.get("exit_reason") or "")

    if direction == "LONG":
        direction_text = (
            f"09:15 close {signal_close:.2f} finished above upper CPR boundary {cpr_upper:.2f}"
        )
    else:
        direction_text = (
            f"09:15 close {signal_close:.2f} finished below lower CPR boundary {cpr_lower:.2f}"
        )

    exit_text = f"Exited via {exit_reason} at {float(trade.get('exit_price') or 0.0):.2f}."
    if exit_reason == "TARGET":
        exit_text = (
            f"Exited at {target_label} target {float(trade.get('target_price') or 0.0):.2f} "
            "once the exit candle touched that level."
        )
    elif exit_reason in {"INITIAL_SL", "BREAKEVEN_SL", "TRAILING_SL"}:
        exit_text = (
            f"Exited when the stop level {float(trade.get('exit_price') or 0.0):.2f} was touched. "
            "The engine checks stop before hard target on each candle."
        )

    with ui.column().classes("gap-1"):
        ui.label("Plain-English Explanation").classes("text-base font-semibold")
        ui.label(
            f"This is a {direction} trade. {direction_text}, so the engine started scanning from "
            f"{derived.get('entry_scan_start') or '09:20'}. The qualifying threshold was "
            f"{threshold:.4f}, with raw trigger {trigger:.4f}. "
            f"The entry candle opened at {float(entry.get('open') or 0.0):.2f}, so the simulated fill was "
            f"{float(trade.get('entry_price') or 0.0):.2f}. {exit_text}"
        ).classes("text-sm").style(f"color:{theme['text_secondary']};")

    with ui.column().classes("gap-1"):
        ui.label("How To Check In TradingView").classes("text-base font-semibold")
        ui.markdown(
            "\n".join(
                [
                    "1. Open the symbol on the **1D** chart and note the **previous trading day's** High, Low, Close.",
                    "2. Compute or plot **Daily CPR** from that prior day only: Pivot, BC, TC, R1, S1, then treat CPR as a band with upper = max(TC, BC) and lower = min(TC, BC).",
                    "3. Switch to the **5-minute** chart for the trade date.",
                    "4. Check the **09:15** candle close: above the upper CPR boundary means long bias, below the lower CPR boundary means short bias.",
                    f"5. From **{derived.get('entry_scan_start') or '09:20'}** onward, compare candle closes to the qualifying threshold `{threshold:.4f}`.",
                    "6. Track whether later candles hit the normalized stop first, the hard target, or a trailing stop.",
                ]
            )
        ).classes("text-sm")


def _inspection_metric(label: str, value: str, color: str) -> None:
    with ui.card().classes("p-3"):
        ui.label(label).classes("text-xs uppercase").style("letter-spacing:0.08em;")
        ui.label(value).classes("text-lg font-bold mono-font").style(f"color:{color};")


def _inspection_table(rows: list[tuple[str, str]], theme: dict) -> None:
    with ui.column().classes("w-full gap-1"):
        for label, value in rows:
            with ui.row().classes("w-full justify-between gap-4"):
                ui.label(label).classes("text-sm").style(f"color:{theme['text_secondary']};")
                ui.label(value).classes("text-sm mono-font text-right").style(
                    f"color:{theme['text_primary']};"
                )


def _fmt_num(value: object, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _stat_chip(label: str, value: str, color: str) -> None:
    theme = THEME
    with ui.column().classes("items-center gap-0"):
        ui.label(label).classes("text-xs uppercase").style(
            f"color: {theme['text_muted']}; letter-spacing: 0.08em;"
        )
        ui.label(value).classes("text-lg font-bold mono-font").style(f"color: {color};")


def _compact_metric(label: str, value: str, color: str) -> None:
    """Compact inline metric for secondary KPI row."""
    theme = THEME
    with ui.column().classes("gap-0"):
        ui.label(value).classes("text-sm font-bold mono-font tabular-nums").style(
            f"color: {color};"
        )
        ui.label(label).classes("text-xs").style(f"color: {theme['text_muted']};")
