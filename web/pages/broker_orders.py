"""Broker order audit dashboard page."""

from __future__ import annotations

import logging
from datetime import datetime

from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    empty_state,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
    safe_timer,
)
from web.state import aget_broker_order_audit

logger = logging.getLogger(__name__)


async def broker_orders_page() -> None:
    """Render local real-order intents with optional live Kite orderbook overlay."""
    with page_layout("Broker Orders", "receipt_long"):
        colors = COLORS
        page_header(
            "Broker Orders",
            "Real-order audit view. Local order intents are read from the paper replica; Kite status is fetched only on demand.",
        )
        status_line = (
            ui.label("Loading local broker order rows...")
            .classes("text-sm mono-font mb-2")
            .style(f"color:{THEME['text_muted']};")
        )
        content = ui.column().classes("w-full")

        async def _load(*, include_kite: bool = False) -> None:
            content.clear()
            with content:
                with ui.row().classes("justify-center items-center p-10 w-full"):
                    ui.spinner("dots").props('role="status" aria-live="polite"')
                    ui.label("Fetching broker order audit...").classes("text-sm").style(
                        f"color:{THEME['text_muted']};"
                    )
            try:
                audit = await aget_broker_order_audit(include_kite=include_kite, limit=150)
            except Exception as exc:
                logger.exception("Broker order audit load failed")
                content.clear()
                with content:
                    empty_state("Broker order audit unavailable", str(exc), icon="error")
                return

            rows = list(audit.get("rows") or [])
            kite_error = str(audit.get("kite_error") or "")
            status_line.text = (
                f"Refreshed {datetime.now().strftime('%H:%M:%S')} | "
                f"local={int(audit.get('local_count') or 0)} | "
                f"kite={int(audit.get('kite_count') or 0)} | "
                f"kite_latency={audit.get('kite_latency_ms') or '-'}ms"
            )
            content.clear()
            with content:
                with ui.row().classes("mb-4 items-center gap-3 flex-wrap"):
                    ui.button(
                        "Refresh Local",
                        icon="refresh",
                        on_click=lambda: safe_timer(0.01, page_state["load_local"]),
                    ).props("outline")
                    ui.button(
                        "Fetch Kite Orderbook",
                        icon="cloud_sync",
                        on_click=lambda: safe_timer(0.01, page_state["load_kite"]),
                    ).props("color=primary")
                if kite_error:
                    ui.label(f"Kite orderbook fetch failed: {kite_error}").classes(
                        "text-sm text-negative mb-3"
                    )
                _render_summary(rows, colors, fetched_kite=bool(audit.get("fetched_kite")))
                _render_orders(rows)

        async def _load_local() -> None:
            await _load(include_kite=False)

        async def _load_kite() -> None:
            await _load(include_kite=True)

        page_state = {"load_local": _load_local, "load_kite": _load_kite}
        safe_timer(0.1, _load_local)


def _render_summary(rows: list[dict], colors: dict, *, fetched_kite: bool) -> None:
    live_rows = [
        row
        for row in rows
        if str(row.get("source") or "") != "KITE"
        and str(row.get("local_status") or "").upper() != ""
    ]
    rejected = sum(1 for row in rows if str(row.get("kite_status") or "").upper() == "REJECTED")
    filled = sum(1 for row in rows if str(row.get("kite_status") or "").upper() == "FILLED")
    pending = sum(
        1
        for row in rows
        if str(row.get("kite_status") or "").upper() in {"OPEN", "TRIGGER PENDING", "NOT_FETCHED"}
    )
    kpi_grid(
        [
            {
                "title": "Local Rows",
                "value": f"{len(live_rows):,}",
                "icon": "storage",
                "color": colors["info"],
            },
            {
                "title": "Kite Overlay",
                "value": "YES" if fetched_kite else "NO",
                "icon": "cloud_sync",
                "color": colors["success"] if fetched_kite else colors["warning"],
            },
            {
                "title": "Filled",
                "value": f"{filled:,}",
                "icon": "check_circle",
                "color": colors["success"],
            },
            {
                "title": "Rejected",
                "value": f"{rejected:,}",
                "icon": "error",
                "color": colors["error"] if rejected else colors["info"],
            },
            {
                "title": "Open/Pending",
                "value": f"{pending:,}",
                "icon": "pending",
                "color": colors["warning"],
            },
        ],
        columns=5,
    )


def _render_orders(rows: list[dict]) -> None:
    if not rows:
        empty_state(
            "No broker order rows",
            "Real-order and real-dry-run rows will appear here after an order intent is recorded.",
            icon="receipt_long",
        )
        return
    paginated_table(
        columns=[
            {"name": "source", "label": "Source", "field": "source", "align": "left"},
            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
            {"name": "side", "label": "Side", "field": "side", "align": "center"},
            {"name": "product", "label": "Product", "field": "product", "align": "center"},
            {"name": "order_type", "label": "Type", "field": "order_type", "align": "center"},
            {"name": "qty", "label": "Qty", "field": "qty", "align": "right", "format": "int"},
            {
                "name": "request_price",
                "label": "Limit Px",
                "field": "request_price",
                "align": "right",
                "format": "decimal:2",
            },
            {"name": "local_status", "label": "Local", "field": "local_status", "align": "left"},
            {"name": "kite_status", "label": "Kite", "field": "kite_status", "align": "left"},
            {
                "name": "avg_price",
                "label": "Avg Px",
                "field": "avg_price",
                "align": "right",
                "format": "decimal:2",
            },
            {"name": "filled_qty", "label": "Filled", "field": "filled_qty", "align": "right"},
            {
                "name": "local_requested_at",
                "label": "Local Time",
                "field": "local_requested_at",
                "align": "left",
            },
            {
                "name": "kite_order_timestamp",
                "label": "Kite Time",
                "field": "kite_order_timestamp",
                "align": "left",
            },
            {
                "name": "latency_ms",
                "label": "API ms",
                "field": "latency_ms",
                "align": "right",
            },
            {
                "name": "broker_order_id",
                "label": "Kite Order ID",
                "field": "broker_order_id",
                "align": "left",
            },
            {
                "name": "exchange_order_id",
                "label": "Exchange ID",
                "field": "exchange_order_id",
                "align": "left",
            },
            {
                "name": "status_message",
                "label": "Message",
                "field": "status_message",
                "align": "left",
            },
            {"name": "session_id", "label": "Session", "field": "session_id", "align": "left"},
        ],
        rows=rows,
        row_key="broker_order_id",
        page_size=25,
        sort_by="kite_order_timestamp",
        descending=True,
        mobile_hidden_cols={
            "session_id",
            "exchange_order_id",
            "status_message",
            "local_requested_at",
            "latency_ms",
        },
        max_client_rows=0,
    )
