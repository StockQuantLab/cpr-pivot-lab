"""Home page — run-centric dashboard with Recent Runs as hero content."""

from __future__ import annotations

import asyncio
from datetime import date

from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    copyable_code,
    divider,
    empty_state,
    extract_row_payload,
    nav_card,
    page_layout,
    set_table_mobile_labels,
)
from web.state import _universe_size_from_json, aget_runs, aget_status, awarm_home_cache

_MIN_TRADES_FOR_BEST = 10
_MIN_SPAN_DAYS_FOR_BEST = 365
_MIN_MAX_DD_PCT_FOR_BEST = 0.10


async def home_page() -> None:
    """Render the CPR Pivot Lab home page."""
    await awarm_home_cache(force=False)
    status, runs = await asyncio.gather(
        aget_status(lite=True),
        aget_runs(force=False),
    )

    with page_layout("Home", "home"):
        theme = THEME
        colors = COLORS

        # ── Title + Status Bar (compact) ────────────────────────────────────
        with ui.row().classes("w-full items-center justify-between gap-4 mb-2"):
            with ui.column().classes("gap-0"):
                ui.label("CPR Pivot Lab").classes("text-2xl font-bold").style(
                    f"color: {theme['text_primary']};"
                )
                ui.label("Backtesting & paper trading analysis").classes("text-xs").style(
                    f"color: {theme['text_muted']};"
                )
            with ui.row().classes("gap-3 items-center"):
                tables = status.get("tables", {})
                symbol_count = status.get("symbols", tables.get("intraday_day_pack", 0))
                run_count = len(runs)
                candidate_runs = _select_best_calmar_runs(runs)
                best_calmar = max(
                    (float(r.get("calmar") or 0.0) for r in candidate_runs), default=0.0
                )
                _stat_chip(f"{symbol_count:,}", "symbols", colors["info"])
                _stat_chip(str(run_count), "runs", colors["primary"])
                _stat_chip(
                    f"{best_calmar:.1f}",
                    "best calmar",
                    colors["success"] if best_calmar >= 2.0 else colors["warning"],
                )

        # ── Primary Actions (2 prominent cards) ─────────────────────────────
        with ui.grid(columns=2).classes("w-full gap-4 mb-6 nav-grid-2"):
            nav_card(
                "Run Results",
                "Analyze backtest performance — trades, charts, equity curves, drill-down inspector",
                "bar_chart",
                "/backtest",
                colors["primary"],
            )
            nav_card(
                "Compare Runs",
                "Side-by-side comparison with metric deltas and parameter diffs",
                "compare_arrows",
                "/compare",
                colors["info"],
            )

        # ── Secondary Actions (single row, compact) ─────────────────────────
        with ui.row().classes("w-full gap-3 mb-8 flex-wrap responsive-row"):
            _compact_nav_card(
                "Trades", "Exit patterns & monthly", "analytics", "/trades", colors["success"]
            )
            _compact_nav_card("Symbols", "Per-symbol P/L", "show_chart", "/symbols", colors["info"])
            _compact_nav_card(
                "Paper", "Live sessions", "receipt_long", "/paper_ledger", colors["error"]
            )
            _compact_nav_card(
                "Strategy", "Per-strategy KPIs", "tune", "/strategy", colors["warning"]
            )

        # ── Recent Runs Table (HERO content) ────────────────────────────────
        with ui.row().classes("w-full items-center justify-between mb-4"):
            ui.label("Recent Runs").classes("text-lg font-semibold").style(
                f"color: {theme['text_primary']};"
            )
            with ui.row().classes("gap-2"):
                # Quick-start toggle (collapsed by default — dismissible)
                with (
                    ui.expansion("CLI Quick Start", icon="terminal", value=False)
                    .classes("text-xs")
                    .style(
                        f"background: {theme['surface']}; border: 1px solid {theme['surface_border']}; "
                        "border-radius: 4px;"
                    )
                ):
                    for desc, cmd in _quick_start_commands():
                        with ui.column().classes("mb-3 gap-1"):
                            ui.label(desc).classes("text-xs").style(
                                f"color: {theme['text_secondary']};"
                            )
                            copyable_code(cmd)

        if not runs:
            empty_state(
                "No runs yet",
                "Run a backtest: doppler run -- uv run pivot-backtest --help",
                icon="science",
            )
        else:
            run_rows = [
                {
                    "_run_id_full": r.get("run_id", ""),
                    "run_id": (r.get("run_id") or "")[:12],
                    "strategy": (r.get("strategy") or "").split("|")[0].strip(),
                    "period": (
                        f"{str(r.get('start_date', ''))[:7]} → {str(r.get('end_date', ''))[:7]}"
                    ),
                    "symbols": str(
                        _universe_size_from_json(r.get("symbols_json"))
                        or int(r.get("symbol_count") or 0)
                    ),
                    "trades": f"{int(r.get('trade_count') or 0):,}",
                    "win_rate": f"{float(r.get('win_rate') or 0):.1f}%",
                    "total_pnl": f"₹{float(r.get('total_pnl') or 0):,.0f}",
                    "calmar": f"{float(r.get('calmar') or 0):.2f}",
                }
                for r in runs[:25]
            ]

            cols = [
                {"name": "run_id", "label": "Run ID", "field": "run_id", "align": "left"},
                {
                    "name": "copy",
                    "label": "Copy",
                    "field": "copy",
                    "align": "center",
                    "classes": "hide-mobile",
                },
                {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                {
                    "name": "period",
                    "label": "Period",
                    "field": "period",
                    "align": "left",
                    "classes": "hide-mobile",
                },
                {
                    "name": "symbols",
                    "label": "Symbols",
                    "field": "symbols",
                    "align": "right",
                    "classes": "hide-mobile",
                },
                {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
                {"name": "win_rate", "label": "Win %", "field": "win_rate", "align": "right"},
                {"name": "total_pnl", "label": "Total P/L", "field": "total_pnl", "align": "right"},
                {"name": "calmar", "label": "Calmar", "field": "calmar", "align": "right"},
            ]

            tbl = ui.table(columns=cols, rows=run_rows, row_key="run_id").classes("w-full")
            set_table_mobile_labels(tbl, cols)
            tbl.add_slot(
                "body-cell-copy",
                """
                <td data-label="Copy" class="text-center hide-mobile" style="width:44px">
                    <q-btn
                        dense
                        flat
                        round
                        icon="content_copy"
                        size="sm"
                        aria-label="Copy run ID"
                        @click.stop="navigator.clipboard.writeText(props.row._run_id_full || props.row.run_id || '');
                                     $q.notify({type:'positive', message:'Run ID copied', timeout:900})"
                    />
                </td>
                """,
            )

            def _navigate_run_detail(event) -> None:
                row = extract_row_payload(event)
                run_id = str(row.get("_run_id_full") or row.get("run_id") or "")
                if run_id:
                    ui.navigate.to(f"/backtest?run_id={run_id}")

            tbl.on("row-click", _navigate_run_detail)

        # ── Ops links (footer) ──────────────────────────────────────────────
        divider()
        with ui.row().classes("gap-4 flex-wrap text-xs").style(f"color: {theme['text_muted']};"):
            ui.label("Ops:")
            for label, path in [
                ("Scans", "/scans"),
                ("Pipeline", "/pipeline"),
                ("Data Quality", "/data_quality"),
            ]:
                ui.link(label, path).classes("underline").style(f"color: {colors['primary']};")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _quick_start_commands() -> list[tuple[str, str]]:
    return [
        (
            "Check table row counts",
            "doppler run -- uv run pivot-build --status",
        ),
        (
            "CPR_LEVELS backtest — gold_51",
            "doppler run -- uv run pivot-backtest --universe-name gold_51 "
            "--start 2015-01-01 --end 2024-12-31 --skip-rvol --save",
        ),
        (
            "Build intraday_day_pack (required before full run)",
            "doppler run -- uv run pivot-build --table pack --force",
        ),
        (
            "Daily live paper session",
            "doppler run -- uv run pivot-paper-trading daily-live "
            "--multi --strategy CPR_LEVELS --trade-date today --all-symbols",
        ),
    ]


def _stat_chip(value: str, label: str, color: str) -> None:
    """Compact stat for header — value first, label below."""
    theme = THEME
    with ui.column().classes("items-center gap-0"):
        ui.label(value).classes("text-base font-bold tabular-nums").style(f"color: {color};")
        ui.label(label).classes("text-xs uppercase tracking-wide").style(
            f"color: {theme['text_muted']};"
        )


def _compact_nav_card(title: str, desc: str, icon: str, target: str, color: str) -> None:
    """Compact nav card for secondary actions row — keyboard accessible."""
    theme = THEME

    def _nav():
        ui.navigate.to(target)

    def _handle_key(e: dict) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.card()
        .classes("cursor-pointer flex-1 min-w-[120px]")
        .props('tabindex="0" role="button" aria-label="Navigate to ' + title + '"')
        .style(
            f"background:{theme['surface']};border:1px solid {theme['surface_border']};"
            f"border-radius:6px;transition:all 0.15s;"
        )
        .on("click", _nav)
        .on("keydown", _handle_key)
        .style(f"hover:border-color:{color};hover:background:{theme['surface_hover']};")
    ):
        with ui.column().classes("gap-1 p-3 items-center text-center"):
            ui.icon(icon).classes("text-lg").style(f"color: {color};")
            ui.label(title).classes("text-xs font-semibold").style(
                f"color: {theme['text_primary']};"
            )
            ui.label(desc).classes("text-xs").style(
                f"color: {theme['text_muted']}; max-width:100px;"
            )


def _run_span_days(run: dict) -> int:
    precomputed = int(run.get("run_span_days") or 0)
    if precomputed > 0:
        return precomputed
    try:
        start = date.fromisoformat(str(run.get("start_date") or "")[:10])
        end = date.fromisoformat(str(run.get("end_date") or "")[:10])
    except ValueError:
        return 0
    return max((end - start).days + 1, 0)


def _select_best_calmar_runs(runs: list[dict]) -> list[dict]:
    trade_filtered = [r for r in runs if int(r.get("trade_count") or 0) >= _MIN_TRADES_FOR_BEST]
    long_window = [r for r in trade_filtered if _run_span_days(r) >= _MIN_SPAN_DAYS_FOR_BEST]
    dd_filtered = [
        r for r in long_window if abs(float(r.get("max_dd_pct") or 0.0)) >= _MIN_MAX_DD_PCT_FOR_BEST
    ]
    if dd_filtered:
        return dd_filtered
    return long_window if long_window else trade_filtered
