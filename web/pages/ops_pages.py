"""Operational dashboard pages: scans, pipeline, paper ledger, market monitor."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

import numpy as np
import plotly.graph_objects as go
import polars as pl
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    apply_chart_theme,
    divider,
    empty_state,
    format_drawdown_pct,
    info_box,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
    safe_timer,
    set_table_mobile_labels,
)
from web.state import (
    aget_market_breadth_snapshot,
    aget_paper_active_sessions,
    aget_paper_archived_runs,
    aget_paper_daily_summary,
    aget_run_ledger,
    aget_run_metadata,
    aget_runtime_coverage,
    aget_scan_snapshot,
    aget_status,
    aget_trade_inspection,
    build_paper_session_options,
)

logger = logging.getLogger(__name__)


async def scans_page() -> None:
    """Render setup-bias scans from strategy_day_state snapshots."""
    snapshot = await aget_scan_snapshot(limit_days=180)

    with page_layout("Scans", "radar"):
        colors = COLORS
        page_header(
            "Scans",
            "Daily setup landscape from runtime state tables (LONG/SHORT bias, narrowing CPR, gap context)",
        )

        if snapshot.is_empty():
            empty_state(
                "No scan data available",
                "Build runtime tables first: doppler run -- uv run pivot-build --table runtime --force",
                icon="radar",
            )
            return

        latest = snapshot.sort("trade_date", descending=True).row(0, named=True)
        long_bias = int(latest.get("long_bias") or 0)
        short_bias = int(latest.get("short_bias") or 0)
        neutral_bias = int(latest.get("neutral_bias") or 0)
        narrowing = int(latest.get("narrowing_symbols") or 0)

        kpi_grid(
            [
                dict(
                    title="Latest Session",
                    value=str(latest.get("trade_date") or "-"),
                    icon="today",
                    color=colors["info"],
                ),
                dict(
                    title="Long Bias",
                    value=f"{long_bias:,}",
                    icon="north_east",
                    color=colors["success"],
                ),
                dict(
                    title="Short Bias",
                    value=f"{short_bias:,}",
                    icon="south_east",
                    color=colors["error"],
                ),
                dict(
                    title="Neutral",
                    value=f"{neutral_bias:,}",
                    icon="pause_circle",
                    color=colors["warning"],
                ),
                dict(
                    title="Narrowing CPR",
                    value=f"{narrowing:,}",
                    icon="filter_alt",
                    color=colors["primary"],
                ),
            ],
            columns=4,
        )
        info_box(
            "Scans are not entry decisions; they summarize the intraday setup context precomputed in runtime tables.",
            color="blue",
        )

        line_df = snapshot.sort("trade_date")
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=line_df["trade_date"].to_list(),
                y=line_df["long_bias"].to_list(),
                mode="lines",
                name="Long Bias",
                line=dict(color=colors["success"], width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=line_df["trade_date"].to_list(),
                y=line_df["short_bias"].to_list(),
                mode="lines",
                name="Short Bias",
                line=dict(color=colors["error"], width=2),
            )
        )
        fig.update_layout(
            title="Daily Bias Trend",
            xaxis_title="Trade Date",
            yaxis_title="Symbol Count",
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-72 mb-4")

        paginated_table(
            columns=[
                {"name": "trade_date", "label": "Date", "field": "trade_date", "align": "left"},
                {"name": "symbols", "label": "Symbols", "field": "symbols", "align": "right"},
                {"name": "long_bias", "label": "Long", "field": "long_bias", "align": "right"},
                {"name": "short_bias", "label": "Short", "field": "short_bias", "align": "right"},
                {
                    "name": "neutral_bias",
                    "label": "Neutral",
                    "field": "neutral_bias",
                    "align": "right",
                },
                {
                    "name": "narrowing_symbols",
                    "label": "Narrowing",
                    "field": "narrowing_symbols",
                    "align": "right",
                },
                {
                    "name": "avg_or_atr_5",
                    "label": "Avg OR/ATR",
                    "field": "avg_or_atr_5",
                    "align": "right",
                },
                {
                    "name": "avg_abs_gap_pct",
                    "label": "Avg |Gap| %",
                    "field": "avg_abs_gap_pct",
                    "align": "right",
                },
                {
                    "name": "avg_cpr_width_pct",
                    "label": "Avg CPR %",
                    "field": "avg_cpr_width_pct",
                    "align": "right",
                },
            ],
            rows=snapshot.to_dicts(),
            row_key="trade_date",
            page_size=30,
            sort_by="trade_date",
            descending=True,
        )


async def pipeline_page() -> None:
    """Render runtime pipeline and coverage status."""
    status, coverage = await asyncio.gather(aget_status(lite=True), aget_runtime_coverage())

    with page_layout("Pipeline", "engineering"):
        colors = COLORS
        theme = THEME
        page_header("Pipeline", "Runtime build health, table coverage, and operational checklist")

        tables = status.get("tables", {})
        kpi_grid(
            [
                dict(
                    title="market_day_state",
                    value=f"{int(tables.get('market_day_state') or 0):,}",
                    icon="table_rows",
                    color=colors["info"],
                ),
                dict(
                    title="strategy_day_state",
                    value=f"{int(tables.get('strategy_day_state') or 0):,}",
                    icon="table_view",
                    color=colors["primary"],
                ),
                dict(
                    title="intraday_day_pack",
                    value=f"{int(tables.get('intraday_day_pack') or 0):,}",
                    icon="inventory_2",
                    color=colors["success"],
                ),
                dict(
                    title="run_metrics",
                    value=f"{int(tables.get('run_metrics') or 0):,}",
                    icon="query_stats",
                    color=colors["warning"],
                ),
            ],
            columns=4,
        )

        if not coverage.is_empty():
            paginated_table(
                columns=[
                    {"name": "table", "label": "Table", "field": "table", "align": "left"},
                    {"name": "rows", "label": "Rows", "field": "rows", "align": "right"},
                    {"name": "symbols", "label": "Symbols", "field": "symbols", "align": "right"},
                    {"name": "min_date", "label": "Min Date", "field": "min_date", "align": "left"},
                    {"name": "max_date", "label": "Max Date", "field": "max_date", "align": "left"},
                ],
                rows=coverage.to_dicts(),
                row_key="table",
                page_size=10,
            )

        divider()
        ui.label("Operational Commands").classes("text-base font-semibold mb-2").style(
            f"color: {theme['text_primary']};"
        )
        for cmd in [
            "doppler run -- uv run pivot-build --status",
            "doppler run -- uv run pivot-build --table runtime --force",
            "doppler run -- uv run pivot-campaign --full-universe --start 2015-01-01 --end 2026-03-09",
            "doppler run -- uv run pivot-clean --dry-run",
        ]:
            ui.code(cmd, language="bash").classes("w-full text-xs")


def _render_live_paper_sessions(active_sessions: list[dict], colors: dict) -> None:
    if not active_sessions:
        empty_state(
            "No active paper sessions",
            "Create one with pivot-paper-trading start --activate or replay historical candles.",
            icon="receipt_long",
        )
        return

    def _format_session_ts(value: object) -> str:
        if not value:
            return "—"
        if isinstance(value, datetime):
            return value.strftime("%H:%M %d-%b-%Y")
        return str(value)

    lookup: dict[str, dict] = {}
    labels: list[str] = []
    for payload in active_sessions:
        session = payload.get("session")
        summary = payload.get("summary") or {}
        session_id = str(getattr(session, "session_id", ""))
        name = getattr(session, "name", None) or session_id[:8]
        session_mode = str(getattr(session, "mode", "") or "replay").upper()
        strategy_params = getattr(session, "strategy_params", {}) or {}
        if session_mode == "REPLAY":
            feed_source = "HISTORICAL"
        else:
            feed_source = str(strategy_params.get("feed_source") or "kite").upper()
        # Extract direction from session_id for sessions that pre-date the name fix
        # Session IDs follow patterns: CPR_LEVELS_LONG-2026-04-01-xxx, paper-cpr_levels-short-...
        sid_upper = session_id.upper()
        direction = ""
        if "_LONG" in sid_upper or "-LONG-" in sid_upper or sid_upper.endswith("-LONG"):
            direction = "LONG"
        elif "_SHORT" in sid_upper or "-SHORT-" in sid_upper or sid_upper.endswith("-SHORT"):
            direction = "SHORT"
        # Show direction if not already in the name
        if direction and direction not in name.upper():
            name = f"{name} [{direction}]"
        label = (
            f"{name} · "
            f"{getattr(session, 'strategy', '')} · "
            f"{session_mode} · "
            f"{feed_source} · "
            f"{summary.get('status') or getattr(session, 'status', '')}"
        )
        labels.append(label)
        lookup[label] = payload

    with ui.card().classes("w-full mb-2"):

        @ui.refreshable
        def _render(label: str) -> None:
            payload = lookup.get(label)
            if not payload:
                return
            session = payload["session"]
            summary = payload["summary"] or {}
            positions = payload.get("positions") or []
            orders = payload.get("orders") or []
            strategy_params = getattr(session, "strategy_params", {}) or {}
            session_mode = str(getattr(session, "mode", "") or "replay").upper()
            if session_mode == "REPLAY":
                feed_source = "HISTORICAL"
            else:
                feed_source = str(strategy_params.get("feed_source") or "kite").upper()

            # Structured session metadata — replaces markdown block
            with ui.row().classes("w-full gap-3 mb-4 flex-wrap items-center"):
                _session_meta_chip(
                    "Session", getattr(session, "session_id", "")[:16], colors["info"]
                )
                _session_meta_chip("Strategy", getattr(session, "strategy", ""), colors["primary"])
                _session_meta_chip("Mode", session_mode, colors["info"])
                _session_meta_chip("Feed", feed_source, colors["success"])
                _session_meta_chip(
                    "Latest Candle",
                    str(summary.get("latest_candle_ts") or "—"),
                    colors["info"],
                )

            kpi_grid(
                [
                    dict(
                        title="Status",
                        value=str(getattr(session, "status", "")),
                        icon="play_circle",
                        color=colors["info"],
                    ),
                    dict(
                        title="Feed",
                        value=str(summary.get("feed_status") or "UNKNOWN"),
                        icon="sensors",
                        color=colors["success"]
                        if str(summary.get("feed_status") or "").upper() == "OK"
                        else colors["warning"],
                    ),
                    dict(
                        title="Open Pos",
                        value=f"{int(summary.get('open_positions') or 0):,}",
                        icon="trending_up",
                        color=colors["primary"],
                    ),
                    dict(
                        title="Closed Pos",
                        value=f"{int(summary.get('closed_positions') or 0):,}",
                        icon="check_circle",
                        color=colors["success"],
                    ),
                    dict(
                        title="Realized P/L",
                        value=f"₹{float(summary.get('realized_pnl') or 0.0):,.0f}",
                        icon="paid",
                        color=colors["success"]
                        if float(summary.get("realized_pnl") or 0.0) >= 0
                        else colors["error"],
                    ),
                    dict(
                        title="Unrealized P/L",
                        value=f"₹{float(summary.get('unrealized_pnl') or 0.0):,.0f}",
                        icon="show_chart",
                        color=colors["warning"],
                    ),
                    dict(
                        title="Net P/L",
                        value=f"₹{float(summary.get('net_pnl') or 0.0):,.0f}",
                        icon="account_balance_wallet",
                        color=colors["success"]
                        if float(summary.get("net_pnl") or 0.0) >= 0
                        else colors["error"],
                    ),
                    dict(
                        title="Orders",
                        value=f"{int(summary.get('orders') or len(orders)):,}",
                        icon="receipt_long",
                        color=colors["info"],
                    ),
                ],
                columns=4,
            )

            if summary.get("feed_reason"):
                info_box(f"Feed state: {summary.get('feed_reason')}", color="yellow")

            position_rows = [
                {
                    "position_id": getattr(position, "position_id", None),
                    "symbol": getattr(position, "symbol", ""),
                    "direction": _direction_label(getattr(position, "direction", "")),
                    "direction_color": (
                        colors["success"]
                        if str(getattr(position, "direction", "")) == "LONG"
                        else colors["error"]
                        if str(getattr(position, "direction", "")) == "SHORT"
                        else colors["info"]
                    ),
                    "status": getattr(position, "status", ""),
                    "opened_at": _format_session_ts(getattr(position, "opened_at", None)),
                    "closed_at": _format_session_ts(getattr(position, "closed_at", None)),
                    "exit_reason": str(
                        getattr(position, "exit_reason", None)
                        or getattr(position, "closed_by", None)
                        or ""
                    ),
                    "qty": float(getattr(position, "quantity", 0.0) or 0.0),
                    "entry_price": float(getattr(position, "entry_price", 0.0) or 0.0),
                    "last_price": float(getattr(position, "last_price", 0.0) or 0.0),
                    "close_price": float(getattr(position, "close_price", 0.0) or 0.0),
                    "realized_pnl": float(getattr(position, "realized_pnl", 0.0) or 0.0),
                    "stop_loss": float(getattr(position, "stop_loss", 0.0) or 0.0),
                    "target_price": float(getattr(position, "target_price", 0.0) or 0.0),
                    "phase": (getattr(position, "trail_state", None) or {}).get("phase") or "",
                }
                for position in positions
            ]
            _colored_direction_table(position_rows, colors)

            order_rows = [
                {
                    "order_id": getattr(order, "order_id", None),
                    "symbol": getattr(order, "symbol", ""),
                    "side": getattr(order, "side", ""),
                    "status": getattr(order, "status", ""),
                    "order_type": getattr(order, "order_type", ""),
                    "requested_qty": float(getattr(order, "requested_qty", 0.0) or 0.0),
                    "fill_qty": float(getattr(order, "fill_qty", 0.0) or 0.0),
                    "fill_price": float(getattr(order, "fill_price", 0.0) or 0.0),
                    "requested_at": _format_session_ts(getattr(order, "requested_at", None)),
                }
                for order in orders
            ]
            if order_rows:
                ui.label("Recent Orders").classes("text-base font-semibold mt-4 mb-2")
                paginated_table(
                    columns=[
                        {"name": "order_id", "label": "ID", "field": "order_id", "align": "right"},
                        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                        {"name": "side", "label": "Side", "field": "side", "align": "center"},
                        {"name": "status", "label": "Status", "field": "status", "align": "left"},
                        {
                            "name": "order_type",
                            "label": "Type",
                            "field": "order_type",
                            "align": "left",
                        },
                        {
                            "name": "requested_qty",
                            "label": "Req Qty",
                            "field": "requested_qty",
                            "align": "right",
                            "format": "int",
                        },
                        {
                            "name": "fill_qty",
                            "label": "Fill Qty",
                            "field": "fill_qty",
                            "align": "right",
                            "format": "int",
                        },
                        {
                            "name": "fill_price",
                            "label": "Fill Px",
                            "field": "fill_price",
                            "align": "right",
                            "format": "decimal:2",
                        },
                        {
                            "name": "requested_at",
                            "label": "Requested",
                            "field": "requested_at",
                            "align": "left",
                        },
                    ],
                    rows=order_rows,
                    row_key="order_id",
                    page_size=10,
                )

        ui.select(
            labels,
            value=labels[0],
            label="Select Active Session",
            on_change=lambda e: _render.refresh(e.value),
        ).props("outlined dense use-input options-dense input-debounce=0").classes(
            "w-full max-w-4xl mb-4"
        )
        _render(labels[0])


async def paper_ledger_page() -> None:
    """Render live paper sessions and archived paper ledgers."""
    with page_layout("Paper Sessions", "receipt_long"):
        colors = COLORS
        page_header(
            "Paper Sessions",
            "Live paper sessions and archived ledgers are read from DuckDB.",
        )
        with ui.row().classes("mb-4 items-center gap-3"):
            ui.button("Refresh", icon="refresh", on_click=lambda: ui.navigate.reload()).props(
                "outline aria-label='Refresh page'"
            )
            auto_refresh_state = {"on": False, "timer": None}
            ui.checkbox(
                "Auto-refresh 60s",
                value=False,
                on_change=lambda e: (
                    _start_auto_refresh(auto_refresh_state)
                    if e.value
                    else _stop_auto_refresh(auto_refresh_state)
                ),
            ).classes("text-sm")

        content = ui.column().classes("w-full")
        with content:
            ui.spinner("dots").classes("mt-8 self-center").props(
                'role="status" aria-live="polite" aria-label="Loading paper sessions..."'
            )

        async def _load() -> None:
            # Fetch active + archived concurrently (both use BacktestDB)
            # then daily summary sequentially to avoid DuckDB connection
            # race — concurrent reads on a single DuckDB connection can
            # silently return empty result sets.
            active_sessions, archived_runs = await asyncio.gather(
                aget_paper_active_sessions(),
                aget_paper_archived_runs(),
            )
            daily_summary = await aget_paper_daily_summary()

            content.clear()
            with content:
                # ── Active Sessions (expanded by default, at top) ──
                active_count = len(active_sessions)
                with (
                    ui.expansion(
                        f"Active Paper Sessions ({active_count})",
                        icon="sensors",
                        value=True,
                    )
                    .classes("w-full mb-2")
                    .style(f"border-top:3px solid {colors['primary']};")
                ):
                    _render_live_paper_sessions(active_sessions, colors)

                # ── Daily Summary (collapsible) ──
                _render_daily_summary(daily_summary, colors)

                # ── Archived Sessions (collapsible, below active) ──
                options = build_paper_session_options(archived_runs)
                archive_count = len(options)
                with (
                    ui.expansion(
                        f"Archived Paper Sessions ({archive_count})",
                        icon="archive",
                        value=active_count == 0,
                    )
                    .classes("w-full")
                    .style(f"border-top:3px solid {THEME['text_muted']};")
                ):
                    if not options:
                        empty_state(
                            "No archived paper sessions",
                            "Completed paper sessions (replay, live-local, live-kite) appear here.",
                            icon="receipt_long",
                        )
                    else:
                        labels = list(options.keys())

                        @ui.refreshable
                        def _render(label: str) -> None:
                            run_id = options.get(label, "")
                            if not run_id:
                                return

                            container = ui.column().classes("w-full")

                            async def _load_ledger() -> None:
                                try:
                                    ledger_df, run_meta = await asyncio.gather(
                                        aget_run_ledger(run_id, execution_mode="PAPER"),
                                        aget_run_metadata(run_id),
                                    )
                                except Exception as ledger_exc:
                                    container.clear()
                                    with container:
                                        empty_state(
                                            "Archived ledger unavailable",
                                            f"Could not load archived paper trades: {ledger_exc}",
                                            icon="error",
                                        )
                                    return
                                container.clear()
                                with container:
                                    _render_ledger_content(
                                        run_id, archived_runs, ledger_df, run_meta, colors
                                    )

                            safe_timer(0.1, _load_ledger)

                        ui.select(
                            labels,
                            value=labels[0],
                            label="Select Archived Session",
                            on_change=lambda e: _render.refresh(e.value),
                        ).props("outlined dense use-input options-dense input-debounce=0").classes(
                            "w-full max-w-4xl mb-4"
                        )
                        _render(labels[0])

        safe_timer(0.1, _load)


def _render_daily_summary(daily_rows: list[dict], colors: dict) -> None:
    """Collapsible daily P/L summary across all paper sessions."""
    with (
        ui.expansion(
            f"Daily Summary ({len(daily_rows)} days)",
            icon="calendar_view_day",
            value=False,
        )
        .classes("w-full mb-2")
        .style(f"border-top:3px solid {colors['info']};")
    ):
        if not daily_rows:
            empty_state(
                "No daily summary data",
                "Paper trading results will appear here once sessions are archived.",
                icon="calendar_view_day",
            )
            return

        # Aggregate totals for KPI cards
        total_wins = sum(r["total_wins"] for r in daily_rows)
        total_trades = sum(r["total_trades"] for r in daily_rows)
        total_pnl = sum(r["total_pnl"] for r in daily_rows)
        long_pnl = sum(r["long_pnl"] for r in daily_rows)
        short_pnl = sum(r["short_pnl"] for r in daily_rows)
        wr = (total_wins / total_trades * 100) if total_trades else 0.0

        kpi_grid(
            [
                dict(
                    title="Total Days",
                    value=f"{len(daily_rows)}",
                    icon="calendar_month",
                    color=colors["info"],
                ),
                dict(
                    title="Total Trades",
                    value=f"{total_trades:,}",
                    icon="swap_vert",
                    color=colors["primary"],
                ),
                dict(
                    title="Win Rate",
                    value=f"{wr:.1f}%",
                    icon="percent",
                    color=colors["success"] if wr >= 35 else colors["warning"],
                ),
                dict(
                    title="Net P/L",
                    value=f"₹{total_pnl:,.0f}",
                    icon="account_balance_wallet",
                    color=colors["success"] if total_pnl >= 0 else colors["error"],
                ),
                dict(
                    title="LONG P/L",
                    value=f"₹{long_pnl:,.0f}",
                    icon="trending_up",
                    color=colors["success"] if long_pnl >= 0 else colors["error"],
                ),
                dict(
                    title="SHORT P/L",
                    value=f"₹{short_pnl:,.0f}",
                    icon="trending_down",
                    color=colors["success"] if short_pnl >= 0 else colors["error"],
                ),
            ],
            columns=3,
        )

        # Per-day table
        table_rows = []
        cumulative = 0.0
        for r in daily_rows:
            cumulative += r["total_pnl"]
            table_rows.append(
                {
                    "trade_date": str(r["trade_date"])[:10],
                    "long_trades": int(r["long_trades"]),
                    "long_wins": int(r["long_wins"]),
                    "long_pnl": float(r["long_pnl"]),
                    "short_trades": int(r["short_trades"]),
                    "short_wins": int(r["short_wins"]),
                    "short_pnl": float(r["short_pnl"]),
                    "total_trades": int(r["total_trades"]),
                    "total_wins": int(r["total_wins"]),
                    "total_pnl": float(r["total_pnl"]),
                    "cumulative_pnl": round(cumulative, 2),
                }
            )
        paginated_table(
            rows=table_rows,
            columns=[
                {
                    "name": "trade_date",
                    "label": "Date",
                    "field": "trade_date",
                    "align": "left",
                },
                {
                    "name": "long_trades",
                    "label": "L Trades",
                    "field": "long_trades",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "long_wins",
                    "label": "L Wins",
                    "field": "long_wins",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "long_pnl",
                    "label": "L P/L",
                    "field": "long_pnl",
                    "align": "right",
                    "format": "currency",
                },
                {
                    "name": "short_trades",
                    "label": "S Trades",
                    "field": "short_trades",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "short_wins",
                    "label": "S Wins",
                    "field": "short_wins",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "short_pnl",
                    "label": "S P/L",
                    "field": "short_pnl",
                    "align": "right",
                    "format": "currency",
                },
                {
                    "name": "total_trades",
                    "label": "Total",
                    "field": "total_trades",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "total_wins",
                    "label": "Wins",
                    "field": "total_wins",
                    "align": "right",
                    "format": "int",
                },
                {
                    "name": "total_pnl",
                    "label": "Day P/L",
                    "field": "total_pnl",
                    "align": "right",
                    "format": "currency",
                },
                {
                    "name": "cumulative_pnl",
                    "label": "Cumulative",
                    "field": "cumulative_pnl",
                    "align": "right",
                    "format": "currency",
                },
            ],
            page_size=15,
            sort_by="trade_date",
            descending=True,
        )


def _start_auto_refresh(state: dict) -> None:
    """Start a 60-second auto-refresh timer."""
    _stop_auto_refresh(state)
    state["timer"] = safe_timer(60.0, lambda: ui.navigate.reload(), once=False)


def _stop_auto_refresh(state: dict) -> None:
    """Stop the auto-refresh timer if active."""
    timer = state.get("timer")
    if timer is not None:
        timer.active = False
        state["timer"] = None


def _render_ledger_content(
    run_id: str,
    runs: list[dict],
    ledger_df: pl.DataFrame,
    run_meta: dict,
    colors: dict,
) -> None:
    run_row = next((r for r in runs if r.get("run_id") == run_id), {})

    def _setup_count() -> int:
        if isinstance(run_meta, dict):
            symbols = run_meta.get("symbols") or []
            if isinstance(symbols, list) and symbols:
                return len(symbols)
        symbols_json = str(run_row.get("symbols_json") or "")
        if symbols_json:
            try:
                parsed = json.loads(symbols_json)
                if isinstance(parsed, list):
                    return len(parsed)
            except (TypeError, ValueError):
                pass
        try:
            return int(run_row.get("symbol_count") or 0)
        except (TypeError, ValueError):
            return 0

    if ledger_df.is_empty():
        setup_count = _setup_count()
        if setup_count > 0:
            empty_state(
                "0 trades",
                f"{setup_count} setups scanned — no entry conditions triggered.",
                icon="receipt_long",
            )
        else:
            empty_state(
                "0 trades",
                "No qualifying setups — pre-filter returned 0 candidates for this session.",
                icon="receipt_long",
            )
        return

    setup_count = _setup_count()
    trade_count = len(ledger_df)

    params = run_meta.get("params") if isinstance(run_meta, dict) else {}
    params = params if isinstance(params, dict) else {}
    portfolio_base = float(params.get("portfolio_value") or run_row.get("allocated_capital") or 0.0)

    ledger_df = ledger_df.with_columns(
        pl.col("profit_loss").cum_sum().alias("cum_pnl"),
        (pl.lit(portfolio_base) + pl.col("profit_loss").cum_sum()).alias("equity"),
    )
    total_pnl = float(ledger_df["profit_loss"].sum())
    final_equity = float(ledger_df["equity"][-1])

    scanned_value = str(setup_count) if setup_count > 0 else "—"

    # Derived risk metrics — parity with Run Results page
    wins = ledger_df.filter(pl.col("profit_loss") > 0)
    losses = ledger_df.filter(pl.col("profit_loss") < 0)
    win_rate = (len(wins) / trade_count * 100.0) if trade_count > 0 else 0.0
    gross_wins = float(wins["profit_loss"].sum()) if len(wins) else 0.0
    gross_losses = abs(float(losses["profit_loss"].sum())) if len(losses) else 0.0
    profit_factor = (
        gross_wins / gross_losses if gross_losses > 0 else float("inf") if gross_wins > 0 else 0.0
    )

    equity_arr = ledger_df["equity"].to_numpy()
    running_max = np.maximum.accumulate(equity_arr)
    drawdown_arr = equity_arr - running_max
    max_dd_pct = float(np.min(drawdown_arr) / portfolio_base * 100.0) if portfolio_base > 0 else 0.0

    # Annualized return and Calmar
    dates = ledger_df["trade_date"].to_list()
    n_days = max(1, len(set(dates)))
    total_return_pct = (
        (final_equity - portfolio_base) / portfolio_base * 100.0 if portfolio_base > 0 else 0.0
    )
    annual_return = total_return_pct / max(1, n_days) * 252
    calmar = (
        annual_return / abs(max_dd_pct)
        if abs(max_dd_pct) > 0.01
        else float("inf")
        if annual_return > 0
        else 0.0
    )

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
                title="Profit Factor",
                value=f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞",
                subtitle="Gross wins / gross losses",
                icon="trending_up",
                color=colors["success"] if profit_factor >= 1.5 else colors["warning"],
            ),
            dict(
                title="Max Drawdown",
                value=f"-{format_drawdown_pct(abs(max_dd_pct))}",
                subtitle="Worst peak-to-trough loss",
                icon="trending_down",
                color=colors["error"],
            ),
            dict(
                title="Calmar",
                value=f"{calmar:.2f}" if calmar != float("inf") else "∞",
                subtitle="Annual return vs drawdown",
                icon="speed",
                color=colors["success"] if calmar >= 2.0 else colors["warning"],
            ),
            dict(title="Trades", value=f"{trade_count:,}", icon="swap_horiz", color=colors["info"]),
            dict(
                title="Setups Scanned",
                value=scanned_value,
                icon="search",
                color=colors["primary"],
            ),
            dict(
                title="Portfolio Base",
                value=f"₹{portfolio_base:,.0f}",
                icon="account_balance",
                color=colors["primary"],
            ),
            dict(
                title="Final Equity",
                value=f"₹{final_equity:,.0f}",
                icon="trending_up",
                color=colors["success"] if final_equity >= portfolio_base else colors["error"],
            ),
        ],
        columns=4,
    )

    rows = []
    for i, row in enumerate(ledger_df.iter_rows(named=True)):
        rows.append(
            {
                "idx": i,
                "trade_date": str(row["trade_date"]),
                "symbol": str(row["symbol"] or ""),
                "direction": str(row["direction"] or ""),
                "entry_time": str(row["entry_time"] or "")[:5],
                "exit_time": str(row["exit_time"] or "")[:5],
                "position_value": round(float(row["position_value"] or 0.0), 2),
                "profit_loss": round(float(row["profit_loss"] or 0.0), 2),
                "cum_pnl": round(float(row["cum_pnl"] or 0.0), 2),
                "equity": round(float(row["equity"] or 0.0), 2),
                "exit_reason": str(row["exit_reason"] or ""),
            }
        )

    # Trade inspector dialog — reuses run_detail's inspection rendering
    with ui.dialog() as inspector_dialog:
        with ui.card().classes("w-full mx-4").style("max-width:min(1152px, 95vw);"):
            inspector_body = ui.column().classes("w-full gap-4")

    async def _open_paper_inspector(row: dict) -> None:
        from web.pages.run_detail import _render_trade_inspector

        payload = row
        nested_row = payload.get("row") if isinstance(payload, dict) else None
        if isinstance(nested_row, dict):
            payload = nested_row

        symbol = str(payload.get("symbol") or "")
        trade_date = str(payload.get("trade_date") or "")[:10]
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
                    "Could not load daily CPR context for this paper trade.",
                    icon="search_off",
                )
            else:
                strategy = str(details.get("trade", {}).get("strategy", "") or "CPR_LEVELS")
                _render_trade_inspector(details, strategy, colors, THEME)

    info_box(
        "Click any trade row to inspect Daily CPR context, key candles, and TradingView checklist.",
        color="blue",
    )
    paginated_table(
        columns=[
            {"name": "trade_date", "label": "Date", "field": "trade_date", "align": "left"},
            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
            {"name": "direction", "label": "Dir", "field": "direction", "align": "center"},
            {"name": "entry_time", "label": "In", "field": "entry_time", "align": "center"},
            {"name": "exit_time", "label": "Out", "field": "exit_time", "align": "center"},
            {
                "name": "position_value",
                "label": "Position ₹",
                "field": "position_value",
                "align": "right",
            },
            {"name": "profit_loss", "label": "P/L ₹", "field": "profit_loss", "align": "right"},
            {
                "name": "cum_pnl",
                "label": "Cum P/L ₹",
                "field": "cum_pnl",
                "align": "right",
            },
            {
                "name": "equity",
                "label": "Equity ₹",
                "field": "equity",
                "align": "right",
            },
            {"name": "exit_reason", "label": "Exit", "field": "exit_reason", "align": "left"},
        ],
        rows=rows,
        row_key="idx",
        page_size=50,
        on_row_click=_open_paper_inspector,
        sort_by="trade_date",
        descending=True,
    )


def _direction_label(direction: str) -> str:
    """Return a plain-text direction label with arrow prefix for visual distinction."""
    d = str(direction).upper().strip()
    if d == "LONG":
        return "↑ LONG"
    if d == "SHORT":
        return "↓ SHORT"
    return direction


def _colored_direction_table(rows: list[dict], colors: dict) -> None:
    """Render a position table with per-row direction coloring via NiceGUI slots."""
    columns = [
        {
            "name": "position_id",
            "label": "ID",
            "field": "position_id",
            "align": "right",
            "classes": "hide-mobile",
        },
        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
        {"name": "direction", "label": "Dir", "field": "direction", "align": "center"},
        {"name": "status", "label": "Status", "field": "status", "align": "left"},
        {"name": "opened_at", "label": "Opened", "field": "opened_at", "align": "left"},
        {"name": "closed_at", "label": "Closed", "field": "closed_at", "align": "left"},
        {
            "name": "qty",
            "label": "Qty",
            "field": "qty",
            "align": "right",
            "classes": "hide-mobile",
            "format": "int",
        },
        {
            "name": "entry_price",
            "label": "Entry",
            "field": "entry_price",
            "align": "right",
            "classes": "hide-mobile",
            "format": "decimal:2",
        },
        {
            "name": "last_price",
            "label": "Last",
            "field": "last_price",
            "align": "right",
            "classes": "hide-mobile",
            "format": "decimal:2",
        },
        {
            "name": "close_price",
            "label": "Close",
            "field": "close_price",
            "align": "right",
            "classes": "hide-mobile",
            "format": "decimal:2",
        },
        {
            "name": "realized_pnl",
            "label": "Realized",
            "field": "realized_pnl",
            "align": "right",
            "format": "currency",
        },
        {
            "name": "stop_loss",
            "label": "SL",
            "field": "stop_loss",
            "align": "right",
            "classes": "hide-mobile",
            "format": "decimal:2",
        },
        {
            "name": "target_price",
            "label": "Target",
            "field": "target_price",
            "align": "right",
            "classes": "hide-mobile",
            "format": "decimal:2",
        },
        {
            "name": "phase",
            "label": "Phase",
            "field": "phase",
            "align": "left",
            "classes": "hide-mobile",
        },
        {"name": "exit_reason", "label": "Reason", "field": "exit_reason", "align": "left"},
    ]
    resolved = []
    for c in columns:
        col = {**c, "sortable": c.get("sortable", True)}
        fmt = col.pop("format", None)
        if fmt is not None:
            col["display_format"] = fmt
        resolved.append(col)
    tbl = ui.table(
        columns=resolved,
        rows=rows,
        row_key="position_id",
        pagination={"rowsPerPage": 10},
    ).classes("w-full")
    tbl.props('flat bordered separator=horizontal role="table"')
    set_table_mobile_labels(tbl, resolved)

    # Slot: color the direction cell by checking the text content
    green = colors["success"]
    red = colors["error"]
    tbl.add_slot(
        "body-cell-direction",
        f"""
        <td data-label="Dir" class="text-center">
            <span :style="props.row.direction.includes('LONG')
                ? 'color:{green};font-weight:700'
                : props.row.direction.includes('SHORT')
                ? 'color:{red};font-weight:700'
                : ''">{{{{ props.row.direction }}}}</span>
        </td>
        """,
    )


def _session_meta_chip(label: str, value: str, color: str) -> None:
    """Render a compact metadata chip (label + value) for session context."""
    with ui.row().classes("items-center gap-1"):
        ui.label(f"{label}:").classes("text-xs uppercase").style(
            f"color: {THEME['text_muted']}; letter-spacing: 0.05em;"
        )
        ui.label(str(value)).classes("text-sm mono-font").style(f"color: {color};")


def _format_ratio_value(value: float | None, fallback: str = "—") -> str:
    if value is None:
        return fallback
    return f"{float(value):.2f}"


def _format_pct_value(value: float | None, fallback: str = "—") -> str:
    if value is None:
        return fallback
    return f"{float(value):.2f}%"


async def daily_summary_page() -> None:
    """Render the Market Monitor with daily breadth KPIs and tables."""
    snapshot = await aget_market_breadth_snapshot(limit_days=180)

    with page_layout("Market Monitor", "today"):
        colors = COLORS
        page_header(
            "Market Monitor",
            "Full NSE eligible universe breadth using existing daily runtime tables",
        )

        if snapshot.is_empty():
            empty_state(
                "No market snapshot found",
                "Build runtime tables first: doppler run -- uv run pivot-build --table runtime --force",
                icon="today",
            )
            return

        df = snapshot.sort("trade_date", descending=True)
        latest = df.row(0, named=True)
        latest_date = str(latest.get("trade_date") or "")
        long_bias = int(latest.get("long_bias") or 0)
        short_bias = int(latest.get("short_bias") or 0)
        neutral_bias = int(latest.get("neutral_bias") or 0)
        narrowing = int(latest.get("narrowing_symbols") or 0)
        ratio_5d = latest.get("ratio_5d")
        pct_above_40_dma = latest.get("pct_above_40_dma")
        pct_above_ma20 = latest.get("pct_above_ma20")

        kpi_grid(
            [
                dict(
                    title="Latest Trade Date",
                    value=latest_date,
                    icon="event",
                    color=colors["info"],
                ),
                dict(
                    title="Long Bias",
                    value=f"{long_bias:,}",
                    icon="north_east",
                    color=colors["success"],
                ),
                dict(
                    title="Short Bias",
                    value=f"{short_bias:,}",
                    icon="south_east",
                    color=colors["error"],
                ),
                dict(
                    title="Neutral",
                    value=f"{neutral_bias:,}",
                    icon="pause_circle",
                    color=colors["warning"],
                ),
                dict(
                    title="Narrowing",
                    value=f"{narrowing:,}",
                    icon="filter_alt",
                    color=colors["primary"],
                ),
                dict(
                    title="Ratio 5D",
                    value=_format_ratio_value(
                        None if ratio_5d is None else float(ratio_5d),
                        fallback="No shorts",
                    ),
                    icon="compare_arrows",
                    color=colors["success"] if float(ratio_5d or 0.0) >= 1.0 else colors["warning"],
                ),
                dict(
                    title="% Above 40DMA",
                    value=_format_pct_value(
                        None if pct_above_40_dma is None else float(pct_above_40_dma)
                    ),
                    icon="trending_up",
                    color=colors["success"]
                    if (pct_above_40_dma or 0.0) >= 50.0
                    else colors["warning"],
                ),
                dict(
                    title="% Above MA20",
                    value=_format_pct_value(
                        None if pct_above_ma20 is None else float(pct_above_ma20)
                    ),
                    icon="trending_flat",
                    color=colors["success"]
                    if (pct_above_ma20 or 0.0) >= 50.0
                    else colors["warning"],
                ),
            ],
            columns=4,
        )

        info_box(
            "40DMA below approximates T2108-style trend context. MA20 is included as a secondary breadth gauge.",
            color="blue",
        )

        trend_df = df.sort("trade_date")
        trend_fig = go.Figure()
        trend_fig.add_trace(
            go.Scatter(
                x=trend_df["trade_date"].to_list(),
                y=trend_df["long_bias"].to_list(),
                mode="lines",
                name="Long Bias",
                line=dict(color=colors["success"], width=2),
            )
        )
        trend_fig.add_trace(
            go.Scatter(
                x=trend_df["trade_date"].to_list(),
                y=trend_df["short_bias"].to_list(),
                mode="lines",
                name="Short Bias",
                line=dict(color=colors["error"], width=2),
            )
        )
        trend_fig.add_trace(
            go.Scatter(
                x=trend_df["trade_date"].to_list(),
                y=trend_df["neutral_bias"].to_list(),
                mode="lines",
                name="Neutral Bias",
                line=dict(color=colors["warning"], width=2),
            )
        )
        trend_fig.update_layout(
            title="Market Breadth Trend",
            xaxis_title="Trade Date",
            yaxis_title="Symbol Count",
            legend=dict(orientation="h"),
        )
        apply_chart_theme(trend_fig)
        ui.plotly(trend_fig).classes("w-full h-80 mb-4")

        with (
            ui.card()
            .classes("w-full mb-4")
            .style(
                f"background:{THEME['surface']};border:1px solid {THEME['surface_border']};"
                f"border-top:2px solid {colors['primary']}"
            )
        ):
            ui.label("Primary Breadth Table (full universe)").classes(
                "text-sm font-semibold mb-2"
            ).style(f"color: {THEME['text_primary']};")
            ui.label("Symbols, direction mix and market breadth trend %").classes(
                "text-xs mb-2"
            ).style(f"color: {THEME['text_secondary']};")
            paginated_table(
                columns=[
                    {"name": "trade_date", "label": "Date", "field": "trade_date", "align": "left"},
                    {
                        "name": "symbols",
                        "label": "Universe Symbols",
                        "field": "symbols",
                        "align": "right",
                    },
                    {"name": "long_bias", "label": "Long", "field": "long_bias", "align": "right"},
                    {
                        "name": "short_bias",
                        "label": "Short",
                        "field": "short_bias",
                        "align": "right",
                    },
                    {
                        "name": "neutral_bias",
                        "label": "Neutral",
                        "field": "neutral_bias",
                        "align": "right",
                    },
                    {
                        "name": "ratio_5d",
                        "label": "Ratio 5D",
                        "field": "ratio_5d",
                        "align": "right",
                        "format": "decimal:2",
                    },
                    {
                        "name": "pct_above_40_dma",
                        "label": "% Above 40DMA",
                        "field": "pct_above_40_dma",
                        "align": "right",
                        "format": "pct:2",
                    },
                    {
                        "name": "pct_above_ma20",
                        "label": "% Above MA20",
                        "field": "pct_above_ma20",
                        "align": "right",
                        "format": "pct:2",
                    },
                ],
                rows=[
                    {
                        "trade_date": str(row["trade_date"] or ""),
                        "symbols": int(row["symbols"] or 0),
                        "long_bias": int(row["long_bias"] or 0),
                        "short_bias": int(row["short_bias"] or 0),
                        "neutral_bias": int(row["neutral_bias"] or 0),
                        "ratio_5d": (
                            None if row.get("ratio_5d") is None else float(row["ratio_5d"])
                        ),
                        "pct_above_40_dma": (
                            None
                            if row.get("pct_above_40_dma") is None
                            else float(row["pct_above_40_dma"])
                        ),
                        "pct_above_ma20": (
                            None
                            if row.get("pct_above_ma20") is None
                            else float(row["pct_above_ma20"])
                        ),
                    }
                    for row in df.iter_rows(named=True)
                ],
                row_key="trade_date",
                page_size=14,
                sort_by="trade_date",
                descending=True,
            )

        with (
            ui.card()
            .classes("w-full")
            .style(
                f"background:{THEME['surface']};border:1px solid {THEME['surface_border']};"
                f"border-top:2px solid {colors['warning']}"
            )
        ):
            ui.label("Secondary Breadth Table (setup quality)").classes(
                "text-sm font-semibold mb-2"
            ).style(f"color: {THEME['text_primary']};")
            ui.label("Averaged setup metrics used by strategy context").classes(
                "text-xs mb-2"
            ).style(f"color: {THEME['text_secondary']};")
            paginated_table(
                columns=[
                    {"name": "trade_date", "label": "Date", "field": "trade_date", "align": "left"},
                    {
                        "name": "narrowing_symbols",
                        "label": "Narrowing",
                        "field": "narrowing_symbols",
                        "align": "right",
                    },
                    {
                        "name": "avg_or_atr_5",
                        "label": "Avg OR/ATR",
                        "field": "avg_or_atr_5",
                        "align": "right",
                        "format": "decimal:4",
                    },
                    {
                        "name": "avg_abs_gap_pct",
                        "label": "Avg |Gap| %",
                        "field": "avg_abs_gap_pct",
                        "align": "right",
                        "format": "pct:2",
                    },
                    {
                        "name": "avg_cpr_width_pct",
                        "label": "Avg CPR %",
                        "field": "avg_cpr_width_pct",
                        "align": "right",
                        "format": "pct:2",
                    },
                ],
                rows=[
                    {
                        "trade_date": str(row["trade_date"] or ""),
                        "narrowing_symbols": int(row["narrowing_symbols"] or 0),
                        "avg_or_atr_5": round(float(row["avg_or_atr_5"] or 0.0), 4),
                        "avg_abs_gap_pct": (
                            None
                            if row.get("avg_abs_gap_pct") is None
                            else float(row["avg_abs_gap_pct"])
                        ),
                        "avg_cpr_width_pct": (
                            None
                            if row.get("avg_cpr_width_pct") is None
                            else float(row["avg_cpr_width_pct"])
                        ),
                    }
                    for row in df.iter_rows(named=True)
                ],
                row_key="trade_date",
                page_size=14,
                sort_by="trade_date",
                descending=True,
            )
