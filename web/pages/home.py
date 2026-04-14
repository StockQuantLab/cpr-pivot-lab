"""Home page — dashboard overview with hero section, compact status, varied nav layouts."""

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
    strat_badge,
)
from web.state import aget_runs, aget_status, awarm_home_cache

_MIN_TRADES_FOR_BEST = 10
_MIN_SPAN_DAYS_FOR_BEST = 365
_MIN_MAX_DD_PCT_FOR_BEST = 0.10


async def home_page() -> None:
    """Render the CPR Pivot Lab home page."""
    await awarm_home_cache(force=True)
    # aget_status and aget_runs are independent after cache warm — run in parallel
    status, runs = await asyncio.gather(
        aget_status(lite=True),
        aget_runs(force=True),
    )

    with page_layout("Home", "home"):
        theme = THEME
        colors = COLORS

        # ── Title + Compact Status Bar ────────────────────────────────────────
        with ui.column().classes("mb-8 gap-3"):
            with ui.row().classes("items-center justify-between gap-4"):
                with ui.column().classes("gap-1"):
                    ui.label("CPR Pivot Lab").classes("text-3xl font-bold").style(
                        f"color: {theme['text_primary']};"
                    )
                    ui.label("CPR/FBR backtesting with paper trading.").classes("text-sm").style(
                        f"color: {theme['text_secondary']};"
                    )
                # Compact status indicators (replaces large KPI cards)
                tables = status.get("tables", {})
                symbol_count = status.get("symbols", tables.get("intraday_day_pack", 0))
                run_count = len(runs)
                candidate_runs = _select_best_calmar_runs(runs)
                best_calmar = max(
                    (float(r.get("calmar") or 0.0) for r in candidate_runs), default=0.0
                )
                with ui.row().classes("gap-4 flex-wrap"):
                    _compact_stat("Symbols", f"{symbol_count:,}", colors["info"])
                    _compact_stat("Runs", str(run_count), colors["primary"])
                    _compact_stat(
                        "Best Calmar",
                        f"{best_calmar:.1f}",
                        colors["success"] if best_calmar >= 2.0 else colors["warning"],
                    )
            date_range = status.get("date_range", "—")
            if date_range and date_range != "—":
                ui.label(f"Dataset: {date_range}").classes("text-xs").style(
                    f"color: {theme['text_muted']};"
                )

        # ── Quick Start (collapsible) ─────────────────────────────────────────
        with (
            ui.expansion("Quick Start Guide", icon="rocket_launch", value=False)
            .classes("w-full mb-6")
            .style(
                f"background:{theme['surface']};border:1px solid {colors['primary']};"
                f"border-radius:8px;"
            )
        ):
            ui.label("Follow these steps to get started with backtesting:").classes(
                "text-sm mb-4 block"
            ).style(f"color: {theme['text_secondary']};")

            # Steps stacked vertically to avoid overflow — commands wrap naturally
            with ui.column().classes("w-full gap-3"):
                _step_card(
                    "1. Build Tables",
                    "Required before backtests or paper runs.",
                    "doppler run -- uv run pivot-build --table pack --force",
                    colors["primary"],
                )
                _step_card(
                    "2. Run Backtest",
                    "Start with gold_51 or one symbol.",
                    "doppler run -- uv run pivot-backtest --universe-name gold_51 --start 2015-01-01 --end 2024-12-31 --save",
                    colors["info"],
                )
                _step_card(
                    "3. View Results",
                    "Open Run Results to analyze performance.",
                    "/backtest",
                    colors["success"],
                    is_link=True,
                )

        divider()

        # ── Best Run Highlight (if exists) ────────────────────────────────────
        if candidate_runs:
            best = max(candidate_runs, key=lambda r: float(r.get("calmar") or 0.0))
            strategy = (best.get("strategy") or "").split("|")[0].strip()
            with (
                ui.card()
                .classes("w-full mb-8")
                .style(
                    f"background:{theme['surface']};border:1px solid {theme['surface_border']};"
                    f"border-left:4px solid {colors['success']};border-radius:6px;"
                )
            ):
                with ui.row().classes("items-center justify-between w-full mb-4"):
                    with ui.row().classes("items-center gap-3"):
                        ui.icon("emoji_events").classes("text-xl").style(
                            f"color: {colors['success']};"
                        )
                        ui.label("Best Run by Calmar").classes("text-lg font-semibold").style(
                            f"color: {theme['text_primary']};"
                        )
                    ui.html(strat_badge(strategy))
                with ui.row().classes("gap-6"):
                    _compact_metric(
                        "Calmar", f"{float(best.get('calmar') or 0):.2f}", colors["success"]
                    )
                    _compact_metric(
                        "Win Rate", f"{float(best.get('win_rate') or 0):.1f}%", colors["info"]
                    )
                    _compact_metric(
                        "Total P/L", f"₹{float(best.get('total_pnl') or 0):,.0f}", colors["primary"]
                    )
                ui.button(
                    "View Full Analysis →",
                    on_click=lambda b=best: ui.navigate.to(f"/backtest?run_id={b['run_id']}"),
                ).props("flat aria-label='View full analysis of best run'").classes(
                    "text-sm mt-3"
                ).style(f"color: {colors['primary']}")

        divider()

        # ── Primary Analysis (3 cards, prominent) ─────────────────────────────
        _section_label("Analysis", theme)
        with ui.grid(columns=3).classes("w-full gap-5 mb-10 nav-grid-3"):
            nav_card(
                "Run Results",
                "View backtest results — profit, loss, and trade-by-trade details",
                "bar_chart",
                "/backtest",
                colors["primary"],
            )
            nav_card(
                "Trade Analytics",
                "Why trades ended, monthly trends, and per-symbol breakdown",
                "analytics",
                "/trades",
                colors["info"],
            )
            nav_card(
                "Compare Runs",
                "Side-by-side comparison of two backtest runs",
                "compare_arrows",
                "/compare",
                colors["warning"],
            )

        # ── Research Tools (2x2 grid — varied layout) ─────────────────────────
        _section_label("Research", theme)
        with ui.grid(columns=2).classes("w-full gap-5 mb-10 nav-grid-2"):
            nav_card(
                "Strategy Analysis",
                "Performance of each strategy across all runs",
                "tune",
                "/strategy",
                colors["success"],
            )
            nav_card(
                "Symbol Performance",
                "Which stocks made or lost money",
                "show_chart",
                "/symbols",
                colors["info"],
            )
            nav_card(
                "Strategy Guide",
                "Learn how CPR_LEVELS and FBR strategies work",
                "school",
                "/strategy-guide",
                colors["primary"],
            )

        # ── Operations (compact horizontal row) ───────────────────────────────
        _section_label("Operations", theme)
        with ui.row().classes("w-full gap-3 mb-10 flex-wrap responsive-row"):
            _compact_nav_card("Scans", "Daily market bias", "radar", "/scans", colors["info"])
            _compact_nav_card(
                "Pipeline", "Table status & build", "engineering", "/pipeline", colors["primary"]
            )
            _compact_nav_card(
                "Paper Sessions",
                "Live & historical sessions",
                "receipt_long",
                "/paper_ledger",
                colors["warning"],
            )
            _compact_nav_card(
                "Market Monitor",
                "Market health overview",
                "today",
                "/daily_summary",
                colors["success"],
            )
            _compact_nav_card(
                "Data Quality", "Check data tables", "verified", "/data_quality", colors["gray"]
            )

        divider()

        # ── Recent runs table ────────────────────────────────────────────────
        ui.label("Recent Backtest Runs").classes("text-xl font-semibold mb-4").style(
            f"color: {theme['text_primary']};"
        )

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
                    "symbols": str(int(r.get("symbol_count") or 0)),
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
                    "label": "Syms",
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

            tbl.on(
                "row-click",
                _navigate_run_detail,
            )

        divider()

        # ── Quick start commands ─────────────────────────────────────────────
        with (
            ui.expansion("Quick Start Commands", icon="terminal")
            .classes("w-full")
            .style(
                f"background: {theme['surface']}; border: 1px solid {theme['surface_border']}; "
                "border-radius: 4px;"
            )
        ):
            for desc, cmd in [
                (
                    "Check table row counts",
                    "doppler run -- uv run pivot-build --status",
                ),
                (
                    "CPR_LEVELS backtest — gold_51, 10 years",
                    "doppler run -- uv run pivot-backtest --universe-name gold_51 "
                    "--start 2015-01-01 --end 2024-12-31 --skip-rvol --save",
                ),
                (
                    "FBR backtest — best settings (--failure-window 10)",
                    "doppler run -- uv run pivot-backtest --universe-name gold_51 "
                    "--start 2015-01-01 --end 2024-12-31 --strategy FBR "
                    "--failure-window 10 --skip-rvol --save",
                ),
                (
                    "Build intraday_day_pack (required before full run)",
                    "doppler run -- uv run pivot-build --table pack --force",
                ),
                (
                    "Daily live paper session (real-time paper trading)",
                    "doppler run -- uv run pivot-paper-trading daily-live "
                    "--trade-date 2026-03-21 --symbols SBIN,RELIANCE "
                    "--strategy CPR_LEVELS",
                ),
            ]:
                with ui.column().classes("mb-4 gap-1"):
                    ui.label(desc).classes("text-sm").style(f"color: {theme['text_secondary']};")
                    copyable_code(cmd)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _section_label(text: str, theme: dict) -> None:
    ui.html(
        f"<h2 class='text-base font-semibold mb-4' "
        f"style='color:{theme['text_secondary']};text-transform:uppercase;"
        f"letter-spacing:0.1em;margin:0'>{text}</h2>"
    )


def _compact_stat(label: str, value: str, color: str) -> None:
    """Compact inline stat for header status bar."""
    theme = THEME
    with ui.row().classes("items-center gap-1"):
        ui.label(value).classes("text-lg font-bold tabular-nums").style(f"color: {color};")
        ui.label(label).classes("text-xs uppercase tracking-wide").style(
            f"color: {theme['text_muted']};"
        )


def _compact_metric(label: str, value: str, color: str) -> None:
    """Compact metric for Best Run card."""
    theme = THEME
    with ui.column().classes("gap-0"):
        ui.label(label).classes("text-xs uppercase tracking-wide").style(
            f"color: {theme['text_muted']};"
        )
        ui.label(value).classes("text-base font-bold tabular-nums").style(f"color: {color};")


def _step_card(title: str, desc: str, content: str, color: str, is_link: bool = False) -> None:
    """Hero section step card — keyboard accessible for link variant."""
    theme = THEME

    def _handle_key(e: dict) -> None:
        if is_link and e.get("key") in ("Enter", " "):
            ui.navigate.to(content)

    with (
        ui.card()
        .classes("w-full min-w-0")
        .props('tabindex="0" role="button" aria-label="' + title + '"' if is_link else "")
        .style(
            f"background:{theme['surface']};border:1px solid {theme['surface_border']};"
            f"border-left:3px solid {color};border-radius:4px;"
        )
        .on("keydown", _handle_key, args=["key"])
    ):
        with ui.column().classes("gap-2 p-4 w-full min-w-0"):
            with ui.row().classes("items-center gap-2"):
                ui.label(title).classes("text-base font-semibold").style(
                    f"color: {theme['text_primary']};"
                )
            ui.label(desc).classes("text-sm").style(f"color: {theme['text_secondary']};")
            if is_link:
                # Capture content in default arg to avoid late binding
                ui.button("Open →", on_click=lambda: ui.navigate.to(content)).props(
                    "flat dense"
                ).classes("text-sm self-start px-3").style(f"color: {color};")
            else:
                copyable_code(content)


def _compact_nav_card(title: str, desc: str, icon: str, target: str, color: str) -> None:
    """Compact nav card for operations row — keyboard accessible."""
    theme = THEME

    # Capture target in closure to avoid late binding
    def _nav():
        ui.navigate.to(target)

    def _handle_key(e: dict) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.card()
        .classes("cursor-pointer flex-1 min-w-[140px]")
        .props('tabindex="0" role="button" aria-label="Navigate to ' + title + '"')
        .style(
            f"background:{theme['surface']};border:1px solid {theme['surface_border']};"
            f"border-radius:4px;transition:all 0.15s;"
        )
        .on("click", _nav)
        .on("keydown", _handle_key)
        .style(f"hover:border-color:{color};hover:background:{theme['surface_hover']};")
    ):
        with ui.column().classes("gap-1 p-3"):
            with ui.row().classes("items-center gap-2"):
                ui.icon(icon).classes("text-lg").style(f"color: {color};")
                ui.label(title).classes("text-sm font-semibold").style(
                    f"color: {theme['text_primary']};"
                )
            ui.label(desc).classes("text-xs").style(f"color: {theme['text_muted']};")


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
