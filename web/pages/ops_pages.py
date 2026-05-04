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

from engine.execution_defaults import (
    DEFAULT_MAX_POSITION_PCT,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_PORTFOLIO_VALUE,
)
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
    aflatten_both_paper_sessions,
    aget_live_readiness,
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
    aqueue_paper_admin_command,
    areconcile_paper_session,
    build_paper_session_options,
)

logger = logging.getLogger(__name__)


def _extract_tab_value(event: object) -> str | None:
    """Return the selected tab value from a NiceGUI tab change event."""
    args = getattr(event, "args", None)
    if isinstance(args, str):
        return args
    if isinstance(args, dict):
        if (value := args.get("value")) is not None:
            return str(value)
        if (value := args.get("modelValue")) is not None:
            return str(value)
    value = getattr(event, "value", None)
    if value is not None:
        return str(value)
    return None


def _format_rupees(value: float | int | None) -> str:
    return f"₹{float(value or 0.0):,.0f}"


def _strategy_param_value(params: dict, key: str) -> object | None:
    if key in params and params.get(key) not in (None, ""):
        return params.get(key)
    resolved = params.get("_resolved_strategy_config")
    if isinstance(resolved, dict) and resolved.get(key) not in (None, ""):
        return resolved.get(key)
    return None


def _build_session_risk_cards(session: object, colors: dict) -> list[dict]:
    params = getattr(session, "strategy_params", {}) or {}
    if not isinstance(params, dict):
        params = {}
    portfolio_value = float(
        getattr(session, "portfolio_value", 0.0)
        or _strategy_param_value(params, "portfolio_value")
        or DEFAULT_PORTFOLIO_VALUE
    )
    max_positions = int(
        getattr(session, "max_positions", 0)
        or _strategy_param_value(params, "max_positions")
        or DEFAULT_MAX_POSITIONS
    )
    max_position_pct = float(
        getattr(session, "max_position_pct", 0.0)
        or _strategy_param_value(params, "max_position_pct")
        or DEFAULT_MAX_POSITION_PCT
    )
    slot_cap = portfolio_value * max_position_pct if portfolio_value > 0 else 0.0
    pct_label = f"{max_position_pct * 100:.0f}%" if max_position_pct > 0 else "—"
    sizing_value = (
        f"{max_positions:,} x {_format_rupees(slot_cap)}"
        if max_positions > 0 and slot_cap > 0
        else "—"
    )
    return [
        dict(
            title="Capital",
            value=_format_rupees(portfolio_value),
            subtitle="paper portfolio",
            icon="account_balance_wallet",
            color=colors["info"],
        ),
        dict(
            title="Max Pos",
            value=f"{max_positions:,}" if max_positions > 0 else "—",
            subtitle="concurrent limit",
            icon="format_list_numbered",
            color=colors["primary"],
        ),
        dict(
            title="Slot Cap",
            value=_format_rupees(slot_cap) if slot_cap > 0 else "—",
            subtitle=f"{pct_label} per position",
            icon="pie_chart",
            color=colors["warning"],
        ),
        dict(
            title="Sizing",
            value=sizing_value,
            subtitle="max positions x slot cap",
            icon="grid_view",
            color=colors["success"],
        ),
    ]


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


def _render_live_paper_sessions(
    active_sessions: list[dict],
    colors: dict,
    page_state: dict | None = None,
) -> None:
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
    label_by_session_id: dict[str, str] = {}
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
        orders = payload.get("orders") or []
        broker_modes = {
            str(getattr(order, "broker_mode", "") or "").upper()
            for order in orders
            if str(getattr(order, "broker_mode", "") or "").strip()
        }
        session_notes = str(getattr(session, "notes", "") or "").upper()
        broker_execution = (
            "ZERODHA LIVE"
            if "LIVE" in broker_modes or "ZERODHA_LIVE_REAL_ORDERS" in session_notes
            else "PAPER LIVE"
        )
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
            f"{broker_execution} · "
            f"{summary.get('status') or getattr(session, 'status', '')}"
        )
        labels.append(label)
        lookup[label] = payload
        label_by_session_id[session_id] = label

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
            broker_modes = {
                str(getattr(order, "broker_mode", "") or "").upper()
                for order in orders
                if str(getattr(order, "broker_mode", "") or "").strip()
            }
            session_notes = str(getattr(session, "notes", "") or "").upper()
            broker_execution = (
                "ZERODHA LIVE"
                if "LIVE" in broker_modes or "ZERODHA_LIVE_REAL_ORDERS" in session_notes
                else "PAPER LIVE"
            )

            # Structured session metadata — replaces markdown block
            with ui.row().classes("w-full gap-3 mb-4 flex-wrap items-center"):
                _session_meta_chip(
                    "Session", getattr(session, "session_id", "")[:16], colors["info"]
                )
                _session_meta_chip("Strategy", getattr(session, "strategy", ""), colors["primary"])
                _session_meta_chip("Mode", session_mode, colors["info"])
                _session_meta_chip("Feed", feed_source, colors["success"])
                _session_meta_chip(
                    "Broker",
                    broker_execution,
                    colors["error"] if broker_execution == "ZERODHA LIVE" else colors["info"],
                )
                _session_meta_chip(
                    "Latest Candle",
                    str(summary.get("latest_candle_ts") or "—"),
                    colors["info"],
                )

            kpi_grid(_build_session_risk_cards(session, colors), columns=4)

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

            _render_paper_operator_controls(
                session_id=str(getattr(session, "session_id", "")),
                positions=positions,
                colors=colors,
            )

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
                    "broker_mode": str(getattr(order, "broker_mode", "") or "PAPER"),
                    "exchange_order_id": str(getattr(order, "exchange_order_id", "") or ""),
                    "broker_status_message": str(getattr(order, "broker_status_message", "") or ""),
                    "broker_latency_ms": (
                        round(float(getattr(order, "broker_latency_ms", 0.0) or 0.0), 1)
                        if getattr(order, "broker_latency_ms", None) is not None
                        else None
                    ),
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
                        {
                            "name": "broker_mode",
                            "label": "Broker",
                            "field": "broker_mode",
                            "align": "left",
                        },
                        {
                            "name": "exchange_order_id",
                            "label": "Broker Order ID",
                            "field": "exchange_order_id",
                            "align": "left",
                        },
                        {
                            "name": "broker_status_message",
                            "label": "Broker Message",
                            "field": "broker_status_message",
                            "align": "left",
                        },
                        {
                            "name": "broker_latency_ms",
                            "label": "API ms",
                            "field": "broker_latency_ms",
                            "align": "right",
                        },
                    ],
                    rows=order_rows,
                    row_key="order_id",
                    page_size=10,
                )

        selected_session_id = str((page_state or {}).get("selected_active_session_id") or "")
        selected_label = label_by_session_id.get(selected_session_id, labels[0])

        def _select_active_session(e) -> None:
            payload = lookup.get(e.value)
            if page_state is not None and payload:
                page_state["selected_active_session_id"] = str(
                    getattr(payload.get("session"), "session_id", "") or ""
                )
            _render.refresh(e.value)

        ui.select(
            labels,
            value=selected_label,
            label="Select Active Session",
            on_change=_select_active_session,
        ).props("outlined dense use-input options-dense input-debounce=0").classes(
            "w-full max-w-4xl mb-4"
        )
        if page_state is not None:
            selected_payload = lookup.get(selected_label)
            if selected_payload:
                page_state["selected_active_session_id"] = str(
                    getattr(selected_payload.get("session"), "session_id", "") or ""
                )
        _render(selected_label)


def _render_live_readiness(readiness: dict, colors: dict) -> None:
    trade_date = str(readiness.get("trade_date") or "")
    ready = bool(readiness.get("ready"))
    requested_count = int(readiness.get("requested_count") or 0)
    setup_mode = "SETUP-ONLY" if readiness.get("setup_only_mode") else "FULL-DAY"
    blocking_counts = readiness.get("blocking_missing_counts") or {}
    missing_counts = readiness.get("missing_counts") or {}
    sparse_total = int(
        sum(
            int(count or 0)
            for table, count in missing_counts.items()
            if table not in blocking_counts
        )
    )

    ui.label("Live Readiness").classes("text-lg font-semibold mt-2 mb-2")
    kpi_grid(
        [
            dict(
                title="Ready",
                value="YES" if ready else "NO",
                icon="verified" if ready else "warning",
                color=colors["success"] if ready else colors["error"],
            ),
            dict(title="Trade Date", value=trade_date or "—", icon="event", color=colors["info"]),
            dict(title="Mode", value=setup_mode, icon="manage_search", color=colors["primary"]),
            dict(
                title="Universe",
                value=f"{requested_count:,}",
                icon="groups",
                color=colors["info"],
            ),
            dict(
                title="MDS Rows",
                value=f"{int(readiness.get('next_day_mds_count') or 0):,}",
                icon="table_chart",
                color=colors["success"]
                if int(readiness.get("next_day_mds_count") or 0) > 0
                else colors["warning"],
            ),
            dict(
                title="CPR Rows",
                value=f"{int(readiness.get('next_day_cpr_count') or 0):,}",
                icon="table_rows",
                color=colors["success"]
                if int(readiness.get("next_day_cpr_count") or 0) > 0
                else colors["warning"],
            ),
        ],
        columns=3,
    )

    if readiness.get("error"):
        info_box(f"Readiness check failed: {readiness['error']}", color="red")
        return

    if ready:
        info_box(
            f"{trade_date} live setup is ready from "
            f"{readiness.get('symbol_source') or 'runtime data'}.",
            color="green",
        )
    else:
        reasons: list[str] = []
        if readiness.get("freshness_blocking"):
            reasons.append("runtime freshness is blocking")
        if readiness.get("coverage_blocking"):
            reasons.append("coverage gaps are blocking")
        if readiness.get("next_day_rows_missing"):
            reasons.append("next-day market/CPR setup rows are missing")
        if readiness.get("setup_query_failed"):
            reasons.append("09:15 setup-candle query failed")
        reason_text = "; ".join(reasons) if reasons else "readiness gate returned NO"
        info_box(f"{trade_date} is not live-ready: {reason_text}.", color="red")

    setup_rows = list(readiness.get("setup_table_status_rows") or [])
    if setup_rows:
        ui.label("Exact Trade-Date Setup Tables").classes("text-sm font-semibold mt-3 mb-1")
        paginated_table(
            columns=[
                {"name": "table", "label": "Table", "field": "table", "align": "left"},
                {"name": "value", "label": "Rows", "field": "value", "align": "right"},
                {"name": "status", "label": "Status", "field": "status", "align": "left"},
                {"name": "detail", "label": "Detail", "field": "detail", "align": "left"},
            ],
            rows=setup_rows,
            row_key="table",
            page_size=8,
        )

    freshness_status_rows = list(readiness.get("freshness_status_rows") or [])
    if freshness_status_rows:
        ui.label("Runtime Freshness").classes("text-sm font-semibold mt-3 mb-1")
        paginated_table(
            columns=[
                {"name": "table", "label": "Table", "field": "table", "align": "left"},
                {
                    "name": "value",
                    "label": "Max Trade Date",
                    "field": "value",
                    "align": "left",
                },
                {"name": "status", "label": "Status", "field": "status", "align": "left"},
                {"name": "detail", "label": "Detail", "field": "detail", "align": "left"},
            ],
            rows=freshness_status_rows,
            row_key="table",
            page_size=8,
        )

    coverage_status_rows = list(readiness.get("coverage_status_rows") or [])
    if coverage_status_rows:
        ui.label("Symbol Coverage").classes("text-sm font-semibold mt-3 mb-1")
        paginated_table(
            columns=[
                {"name": "table", "label": "Check", "field": "table", "align": "left"},
                {"name": "value", "label": "Missing", "field": "value", "align": "right"},
                {"name": "status", "label": "Status", "field": "status", "align": "left"},
                {"name": "detail", "label": "Detail", "field": "detail", "align": "left"},
            ],
            rows=coverage_status_rows,
            row_key="table",
            page_size=8,
        )

    if blocking_counts:
        rows = [
            {"table": str(table), "missing": int(count)}
            for table, count in sorted(blocking_counts.items())
        ]
        paginated_table(
            columns=[
                {"name": "table", "label": "Blocking Table", "field": "table", "align": "left"},
                {
                    "name": "missing",
                    "label": "Missing Symbols",
                    "field": "missing",
                    "align": "right",
                    "format": "int",
                },
            ],
            rows=rows,
            row_key="table",
            page_size=6,
        )
    elif sparse_total:
        info_box(
            f"Sparse symbol/day warnings exist ({sparse_total:,} table-symbol gaps), "
            "but they are below the blocking threshold.",
            color="yellow",
        )


def _parse_symbols_csv(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip().upper() for part in text.split(",") if part.strip()]


def _parse_float_input(value: object) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    return float(text)


def _parse_int_input(value: object) -> int | None:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    return int(float(text))


def _render_paper_operator_controls(
    *,
    session_id: str,
    positions: list[object],
    colors: dict,
) -> None:
    open_symbols = sorted(
        {
            str(getattr(position, "symbol", "") or "").upper()
            for position in positions
            if str(getattr(position, "status", "") or "").upper() == "OPEN"
        }
    )

    with (
        ui.expansion(
            "Operator Controls",
            icon="settings",
            value=False,
        )
        .classes("w-full my-4")
        .style(f"border-left:4px solid {colors['warning']};")
    ):
        ui.label(
            "Commands are queued for the running live loop; positions are closed by the runtime, then reconciled."
        ).classes("text-xs mb-2").style(f"color:{THEME['text_secondary']};")

        status_label = (
            ui.label("").classes("text-xs mono-font").style(f"color:{THEME['text_secondary']};")
        )

        with ui.row().classes("w-full gap-3 items-end flex-wrap"):
            symbols_input = (
                ui.input(
                    "Symbols to close",
                    value=",".join(open_symbols[:3]),
                    placeholder="SBIN,RELIANCE",
                )
                .props("outlined dense")
                .classes("min-w-64")
            )
            reason_input = (
                ui.input("Reason", value="dashboard_operator")
                .props("outlined dense")
                .classes("min-w-56")
            )

            async def _close_symbols() -> None:
                symbols = _parse_symbols_csv(symbols_input.value)
                if not symbols:
                    ui.notify("Enter at least one symbol to close.", type="warning")
                    return
                try:
                    result = await aqueue_paper_admin_command(
                        session_id=session_id,
                        action="close_positions",
                        symbols=symbols,
                        reason=str(reason_input.value or "dashboard_operator"),
                        requester="dashboard",
                    )
                    status_label.text = f"Queued close_positions: {result['command_file']}"
                    ui.notify(f"Queued close for {', '.join(symbols)}", type="positive")
                except Exception as exc:
                    logger.exception("Dashboard close_symbols failed")
                    ui.notify(f"Close symbols failed: {exc}", type="negative")

            async def _flatten_session() -> None:
                try:
                    result = await aqueue_paper_admin_command(
                        session_id=session_id,
                        action="close_all",
                        reason=str(reason_input.value or "dashboard_flatten_session"),
                        requester="dashboard",
                    )
                    status_label.text = f"Queued close_all: {result['command_file']}"
                    ui.notify("Queued session flatten", type="positive")
                except Exception as exc:
                    logger.exception("Dashboard flatten_session failed")
                    ui.notify(f"Flatten failed: {exc}", type="negative")

            ui.button("Close Symbols", icon="logout", on_click=_close_symbols).props("outline")
            ui.button("Flatten Session", icon="dangerous", on_click=_flatten_session).props(
                "color=negative"
            )

        with ui.row().classes("w-full gap-3 items-end flex-wrap mt-2"):

            async def _queue_control(action: str, label: str) -> None:
                try:
                    result = await aqueue_paper_admin_command(
                        session_id=session_id,
                        action=action,
                        reason=str(reason_input.value or f"dashboard_{action}"),
                        requester="dashboard",
                    )
                    status_label.text = f"Queued {action}: {result['command_file']}"
                    ui.notify(label, type="positive")
                except Exception as exc:
                    logger.exception("Dashboard %s failed", action)
                    ui.notify(f"{label} failed: {exc}", type="negative")

            async def _pause_entries() -> None:
                await _queue_control("pause_entries", "Queued entry pause")

            async def _resume_entries() -> None:
                await _queue_control("resume_entries", "Queued entry resume")

            async def _cancel_pending_intents() -> None:
                await _queue_control("cancel_pending_intents", "Queued pending-intent cancel")

            ui.button(
                "Pause Entries",
                icon="pause_circle",
                on_click=_pause_entries,
            ).props("outline")
            ui.button(
                "Resume Entries",
                icon="play_circle",
                on_click=_resume_entries,
            ).props("outline")
            ui.button(
                "Cancel Pending Intents",
                icon="cancel",
                on_click=_cancel_pending_intents,
            ).props("outline color=warning")

        with ui.row().classes("w-full gap-3 items-end flex-wrap mt-2"):
            portfolio_input = (
                ui.input("Capital ₹", placeholder="500000").props("outlined dense").classes("w-36")
            )
            max_positions_input = (
                ui.input("Max Positions", placeholder="5").props("outlined dense").classes("w-32")
            )
            max_pct_input = (
                ui.input("Max Pos %", placeholder="0.10").props("outlined dense").classes("w-32")
            )

            async def _set_budget() -> None:
                try:
                    portfolio_value = _parse_float_input(portfolio_input.value)
                    max_positions = _parse_int_input(max_positions_input.value)
                    max_position_pct = _parse_float_input(max_pct_input.value)
                    result = await aqueue_paper_admin_command(
                        session_id=session_id,
                        action="set_risk_budget",
                        portfolio_value=portfolio_value,
                        max_positions=max_positions,
                        max_position_pct=max_position_pct,
                        reason=str(reason_input.value or "dashboard_risk_budget"),
                        requester="dashboard",
                    )
                    status_label.text = f"Queued set_risk_budget: {result['command_file']}"
                    ui.notify("Queued risk budget update", type="positive")
                except Exception as exc:
                    logger.exception("Dashboard set_budget failed")
                    ui.notify(f"Risk budget update failed: {exc}", type="negative")

            async def _reconcile() -> None:
                try:
                    result = await areconcile_paper_session(session_id)
                    findings = result.get("findings") or []
                    severity = "positive" if result.get("ok") else "warning"
                    status_label.text = (
                        f"Reconcile ok={result.get('ok')} findings={len(findings)} "
                        f"summary={json.dumps(result.get('summary') or {}, default=str)}"
                    )
                    ui.notify(f"Reconcile findings: {len(findings)}", type=severity)
                except Exception as exc:
                    logger.exception("Dashboard reconcile failed")
                    ui.notify(f"Reconcile failed: {exc}", type="negative")

            ui.button("Set Budget", icon="tune", on_click=_set_budget).props("outline")
            ui.button("Reconcile", icon="fact_check", on_click=_reconcile).props("outline")


async def paper_ledger_page() -> None:
    """Render live paper sessions and archived paper ledgers."""
    with page_layout("Paper Sessions", "receipt_long"):
        colors = COLORS
        page_header(
            "Paper Sessions",
            "Live paper sessions and archived ledgers are read from the paper DuckDB replica.",
        )
        page_state: dict[str, object] = {
            "auto_refresh": True,
            "selected_tab": "active",
            "timer": None,
            "selected_active_session_id": "",
        }
        status_line = (
            ui.label("Loading paper sessions...")
            .classes("text-sm mono-font mb-2")
            .style(f"color:{THEME['text_muted']};")
        )

        tabs = ui.tabs().classes("w-full")
        with tabs:
            ui.tab("active", label="Active Sessions", icon="sensors")
            ui.tab("readiness", label="Live Readiness", icon="verified")
            ui.tab("archived", label="Archived Sessions", icon="archive")
            ui.tab("daily", label="Daily Summary", icon="calendar_view_day")

        with ui.tab_panels(tabs, value="active", keep_alive=True).classes("w-full"):
            with ui.tab_panel("readiness"):
                with ui.row().classes("mb-4 items-center gap-3 flex-wrap"):
                    readiness_date_input = (
                        ui.input("Readiness Date", value="")
                        .props("outlined dense placeholder='auto prepared date'")
                        .classes("w-44")
                    )
                    ui.button(
                        "Refresh Readiness",
                        icon="refresh",
                        on_click=lambda: safe_timer(0.01, _load_readiness),
                    ).props("outline aria-label='Refresh live readiness now'")
                readiness_content = ui.column().classes("w-full")
            with ui.tab_panel("active"):
                # Active-session controls — only visible on this tab
                with ui.row().classes("mb-4 items-center gap-3 flex-wrap"):
                    ui.button(
                        "Refresh Now",
                        icon="refresh",
                        on_click=lambda: safe_timer(0.01, _load_active),
                    ).props("outline aria-label='Refresh paper sessions now'")
                    ui.checkbox(
                        "Near-real-time refresh 3s",
                        value=True,
                        on_change=lambda e: page_state.update({"auto_refresh": bool(e.value)}),
                    ).classes("text-sm")

                    trade_date_input = (
                        ui.input("Trade Date", value=datetime.now().date().isoformat())
                        .props("outlined dense")
                        .classes("w-36")
                    )
                    reason_input = (
                        ui.input("Reason", value="dashboard_flatten_both")
                        .props("outlined dense")
                        .classes("w-56")
                    )

                    async def _flatten_both() -> None:
                        try:
                            result = await aflatten_both_paper_sessions(
                                trade_date=str(trade_date_input.value or "").strip() or None,
                                reason=str(reason_input.value or "dashboard_flatten_both"),
                                requester="dashboard",
                            )
                            commands = result.get("commands") or []
                            if not commands:
                                ui.notify(
                                    "No active LONG/SHORT sessions found for that date.",
                                    type="warning",
                                )
                                return
                            ui.notify(
                                f"Queued flatten for {len(commands)} sessions", type="positive"
                            )
                            status_line.text = f"Queued flatten-both commands={len(commands)}"
                        except Exception as exc:
                            logger.exception("Dashboard flatten_both failed")
                            ui.notify(f"Flatten both failed: {exc}", type="negative")

                    ui.button("Flatten LONG+SHORT", icon="dangerous", on_click=_flatten_both).props(
                        "color=negative"
                    )

                active_content = ui.column().classes("w-full")
            with ui.tab_panel("archived"):
                archived_content = ui.column().classes("w-full")
            with ui.tab_panel("daily"):
                daily_content = ui.column().classes("w-full")

        def _loading(container: ui.column, label: str) -> None:
            container.clear()
            with container:
                with ui.row().classes("justify-center items-center p-10 w-full"):
                    ui.spinner("dots").props('role="status" aria-live="polite"')
                    ui.label(label).classes("text-sm").style(f"color:{THEME['text_muted']};")

        async def _load_readiness() -> None:
            if page_state.get("loading_readiness"):
                return
            page_state["loading_readiness"] = True
            _loading(readiness_content, "Checking live readiness...")
            trade_date = str(readiness_date_input.value or "").strip() or None
            try:
                readiness = await aget_live_readiness(trade_date)
                resolved_date = str(readiness.get("trade_date") or "")
                if not trade_date and resolved_date:
                    readiness_date_input.value = resolved_date
                status_line.text = (
                    f"Readiness refreshed {datetime.now().strftime('%H:%M:%S')} | "
                    f"trade_date={resolved_date or '-'} | "
                    f"readiness={'YES' if readiness.get('ready') else 'NO'}"
                )
                readiness_content.clear()
                with readiness_content:
                    _render_live_readiness(readiness, colors)
            finally:
                page_state["loading_readiness"] = False

        async def _load_active() -> None:
            if page_state.get("loading_active"):
                return
            page_state["loading_active"] = True
            try:
                active_sessions = await aget_paper_active_sessions()
                active_count = len(active_sessions)
                status_line.text = (
                    f"Active refreshed {datetime.now().strftime('%H:%M:%S')} | "
                    f"active={active_count}"
                )
                active_content.clear()
                with active_content:
                    _render_live_paper_sessions(active_sessions, colors, page_state)
            finally:
                page_state["loading_active"] = False

        async def _load_archived() -> None:
            if page_state.get("loading_archived"):
                return
            page_state["loading_archived"] = True
            _loading(archived_content, "Loading archived sessions...")
            try:
                archived_runs = await aget_paper_archived_runs()
                options = build_paper_session_options(archived_runs)
                archive_count = len(options)
                archived_content.clear()
                with archived_content:
                    with ui.row().classes("w-full items-center justify-between mb-3"):
                        ui.label(f"Archived Paper Sessions ({archive_count})").classes(
                            "text-lg font-semibold"
                        )
                        ui.button(
                            "Refresh Archived",
                            icon="refresh",
                            on_click=lambda: safe_timer(0.01, _load_archived),
                        ).props("outline dense")
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
                                    ledger_df = await aget_run_ledger(
                                        run_id, execution_mode="PAPER"
                                    )
                                    run_meta = await aget_run_metadata(run_id)
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
            finally:
                page_state["loading_archived"] = False

        async def _load_daily() -> None:
            if page_state.get("loading_daily"):
                return
            page_state["loading_daily"] = True
            _loading(daily_content, "Loading daily summary...")
            try:
                daily_summary = await aget_paper_daily_summary()
                daily_content.clear()
                with daily_content:
                    with ui.row().classes("w-full justify-end mb-2"):
                        ui.button(
                            "Refresh Daily",
                            icon="refresh",
                            on_click=lambda: safe_timer(0.01, _load_daily),
                        ).props("outline dense")
                    _render_daily_summary(daily_summary, colors, expanded=True)
            finally:
                page_state["loading_daily"] = False

        async def _load() -> None:
            selected = str(page_state.get("selected_tab") or "active")
            if selected == "readiness":
                await _load_readiness()
            elif selected == "active":
                await _load_active()
            if page_state.get("archived_loaded"):
                await _load_archived()
            if page_state.get("daily_loaded"):
                await _load_daily()

        def _on_tab_change(e) -> None:
            selected = _extract_tab_value(e)
            page_state["selected_tab"] = selected
            if selected == "readiness" and not page_state.get("readiness_loaded"):
                page_state["readiness_loaded"] = True
                safe_timer(0.01, _load_readiness)
            elif selected == "active" and not page_state.get("active_loaded"):
                page_state["active_loaded"] = True
                safe_timer(0.01, _load_active)
            elif selected == "archived" and not page_state.get("archived_loaded"):
                page_state["archived_loaded"] = True
                safe_timer(0.01, _load_archived)
            elif selected == "daily" and not page_state.get("daily_loaded"):
                page_state["daily_loaded"] = True
                safe_timer(0.01, _load_daily)

        tabs.on("update:model-value", _on_tab_change)
        _loading(active_content, "Loading active paper sessions...")
        page_state["active_loaded"] = True
        safe_timer(0.1, _load_active)
        safe_timer(
            3.0,
            lambda: (
                safe_timer(0.01, _load_active)
                if page_state.get("auto_refresh") and page_state.get("selected_tab") == "active"
                else None
            ),
            once=False,
        )


def _render_daily_summary(daily_rows: list[dict], colors: dict, *, expanded: bool = False) -> None:
    """Daily P/L summary across all paper sessions."""
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
            except TypeError, ValueError:
                pass
        try:
            return int(run_row.get("symbol_count") or 0)
        except TypeError, ValueError:
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

    # Compute cumulative PnL from deterministic chronological order for on-screen visibility.
    # DB reads are schema-tolerant, so sort by available columns only.
    sort_columns = [
        c for c in ["trade_date", "entry_time", "exit_time", "symbol"] if c in ledger_df.columns
    ]
    if not sort_columns:
        sort_columns = ["profit_loss"]
    ledger_df = ledger_df.sort(
        by=sort_columns,
        descending=False,
    )
    ledger_df = ledger_df.with_columns(
        pl.col("profit_loss").cum_sum().alias("cum_pnl"),
    )
    total_pnl = float(ledger_df["profit_loss"].sum())
    final_equity = portfolio_base + float(ledger_df["cum_pnl"][-1])

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

    equity_arr = np.asarray(
        [portfolio_base + float(x or 0.0) for x in ledger_df["cum_pnl"].to_list()]
    )
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
