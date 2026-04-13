"""Strategy Analysis page — parameter sensitivity across CPR_LEVELS and FBR."""

from __future__ import annotations

import plotly.graph_objects as go
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    apply_chart_theme,
    divider,
    empty_state,
    extract_row_payload,
    info_box,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
)
from web.state import aget_runs

_STRAT_NOTES = {
    "CPR_LEVELS": (
        "SL at CPR boundary, target at R1/S1. Production defaults: "
        "0.35 ATR close + min-price 50 + narrowing filter."
    ),
    "FBR": (
        "Trade failed OR breakouts as reversals. Baseline defaults: "
        "failure-window 10, rr-ratio 2.0."
    ),
}


async def strategy_page() -> None:
    """Render the Strategy Analysis page."""
    runs = await aget_runs(force=True)

    with page_layout("Strategy", "tune"):
        theme = THEME
        colors = COLORS

        page_header(
            "Strategy Analysis",
            "Compare CPR_LEVELS and FBR across saved runs",
        )

        if not runs:
            empty_state(
                "No runs found",
                "Run backtests with --save first to populate this page.",
                icon="tune",
            )
            return

        # Group runs by strategy
        by_strategy: dict[str, list[dict]] = {}
        for r in runs:
            strat = (r.get("strategy") or "").split("|")[0].strip()
            by_strategy.setdefault(strat, []).append(r)

        # Filter out tiny test runs (<10 trades) for meaningful metrics
        valid_runs = [r for r in runs if int(r.get("trade_count") or 0) >= 10]

        # ── Overview KPIs ────────────────────────────────────────────────────
        strat_names = list(by_strategy.keys())
        kpi_grid(
            [
                dict(
                    title="Strategies",
                    value=str(len(strat_names)),
                    icon="tune",
                    color=colors["info"],
                ),
                dict(
                    title="Total Runs",
                    value=str(len(runs)),
                    icon="bar_chart",
                    color=colors["primary"],
                ),
                dict(
                    title="Best Calmar",
                    value=f"{max((float(r.get('calmar') or 0) for r in valid_runs), default=0):.2f}",
                    icon="speed",
                    color=colors["success"],
                ),
                dict(
                    title="Best Strategy",
                    value=_best_strategy(valid_runs),
                    icon="emoji_events",
                    color=colors["success"],
                ),
            ],
            columns=4,
        )

        info_box(
            "Run multiple backtests with different parameters and --save to compare them here. "
            "Runs with <10 trades are excluded from Best Calmar/Strategy metrics.",
            color="blue",
        )

        divider()

        # ── Cross-strategy Calmar comparison chart ───────────────────────────
        all_run_ids = [(r.get("run_id") or "")[:8] for r in valid_runs]
        all_calmars = [float(r.get("calmar") or 0) for r in valid_runs]
        all_strats = [(r.get("strategy") or "").split("|")[0].strip() for r in valid_runs]

        # Use centralized theme colors
        def _strat_color(s: str) -> str:
            key = {"CPR_LEVELS": "strat_cpr_levels", "FBR": "strat_fbr"}.get(s, "strat_default")
            return colors.get(key, "#64748b")

        bar_colors = [_strat_color(s) for s in all_strats]

        fig = go.Figure(
            go.Bar(
                x=all_run_ids,
                y=all_calmars,
                marker_color=bar_colors,
                text=[f"{c:.2f}" for c in all_calmars],
                textposition="outside",
                customdata=all_strats,
                hovertemplate="%{customdata}<br>Run: %{x}<br>Calmar: %{y:.2f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="Calmar by Run (colour = strategy)",
            xaxis_title="Run ID",
            yaxis_title="Calmar",
            showlegend=False,
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-64 mb-4")

        divider()

        # ── Per-strategy sections ────────────────────────────────────────────
        for strat in ["CPR_LEVELS", "FBR"]:
            strat_runs = by_strategy.get(strat, [])
            key = {"CPR_LEVELS": "strat_cpr_levels", "FBR": "strat_fbr"}.get(strat, "strat_default")
            color = colors.get(key, "#64748b")
            note = _STRAT_NOTES.get(strat, "")

            with (
                ui.expansion(
                    f"{strat}  ({len(strat_runs)} runs)",
                    icon="bar_chart",
                )
                .classes("w-full mb-3")
                .style(
                    f"background: {theme['surface']}; "
                    f"border: 1px solid {theme['surface_border']}; "
                    f"border-left: 3px solid {color};"
                )
            ):
                if not strat_runs:
                    ui.label(f"No {strat} runs saved yet.").style(f"color: {theme['text_muted']};")
                    continue

                # Note
                if note:
                    ui.label(note).classes("text-xs mb-3").style(
                        f"color: {theme['text_secondary']};"
                    )

                # Summary KPIs for this strategy
                best = max(strat_runs, key=lambda r: float(r.get("calmar") or 0))
                calmars = [float(r.get("calmar") or 0) for r in strat_runs]
                median_calmar = sorted(calmars)[len(calmars) // 2]

                kpi_grid(
                    [
                        dict(
                            title="Runs", value=str(len(strat_runs)), icon="bar_chart", color=color
                        ),
                        dict(
                            title="Best Calmar",
                            value=f"{float(best.get('calmar') or 0):.2f}",
                            icon="speed",
                            color=colors["success"],
                        ),
                        dict(
                            title="Median Calmar",
                            value=f"{median_calmar:.2f}",
                            icon="equalizer",
                            color=colors["info"],
                        ),
                        dict(
                            title="Best Win Rate",
                            value=f"{max(float(r.get('win_rate') or 0) for r in strat_runs):.1f}%",
                            icon="target",
                            color=colors["success"],
                        ),
                    ],
                    columns=4,
                )

                # Runs table
                rows = [
                    {
                        "run_id": (r.get("run_id") or "")[:12],
                        "period": f"{str(r.get('start_date', ''))[:7]} → {str(r.get('end_date', ''))[:7]}",
                        "symbols": str(int(r.get("symbol_count") or 0)),
                        "trades": f"{int(r.get('trade_count') or 0):,}",
                        "win_rate": f"{float(r.get('win_rate') or 0):.1f}%",
                        "total_pnl": f"₹{float(r.get('total_pnl') or 0):,.0f}",
                        "calmar": f"{float(r.get('calmar') or 0):.2f}",
                        "_rid_full": r.get("run_id", ""),
                    }
                    for r in sorted(
                        strat_runs, key=lambda r: float(r.get("calmar") or 0), reverse=True
                    )
                ]
                tbl = ui.table(
                    columns=[
                        {"name": "run_id", "label": "Run ID", "field": "run_id", "align": "left"},
                        {"name": "period", "label": "Period", "field": "period", "align": "left"},
                        {"name": "symbols", "label": "Syms", "field": "symbols", "align": "right"},
                        {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
                        {
                            "name": "win_rate",
                            "label": "Win %",
                            "field": "win_rate",
                            "align": "right",
                        },
                        {
                            "name": "total_pnl",
                            "label": "Total P/L",
                            "field": "total_pnl",
                            "align": "right",
                        },
                        {"name": "calmar", "label": "Calmar", "field": "calmar", "align": "right"},
                    ],
                    rows=rows,
                    row_key="run_id",
                ).classes("w-full")

                def _navigate_run_detail(event) -> None:
                    row = extract_row_payload(event)
                    run_id = str(row.get("_rid_full") or row.get("run_id") or "")
                    if run_id:
                        ui.navigate.to(f"/backtest?run_id={run_id}")

                tbl.on(
                    "row-click",
                    _navigate_run_detail,
                )

        # Any other strategies not in the known list
        for strat, strat_runs in by_strategy.items():
            if strat in ("CPR_LEVELS", "FBR"):
                continue
            color = colors["gray"]
            with (
                ui.expansion(f"{strat}  ({len(strat_runs)} runs)", icon="bar_chart")
                .classes("w-full mb-3")
                .style(
                    f"background: {theme['surface']}; border: 1px solid {theme['surface_border']};"
                )
            ):
                paginated_table(
                    columns=[
                        {"name": "run_id", "label": "Run ID", "field": "run_id", "align": "left"},
                        {"name": "period", "label": "Period", "field": "period", "align": "left"},
                        {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
                        {"name": "calmar", "label": "Calmar", "field": "calmar", "align": "right"},
                    ],
                    rows=[
                        {
                            "run_id": (r.get("run_id") or "")[:12],
                            "period": f"{str(r.get('start_date', ''))[:7]} → {str(r.get('end_date', ''))[:7]}",
                            "trades": f"{int(r.get('trade_count') or 0):,}",
                            "calmar": f"{float(r.get('calmar') or 0):.2f}",
                        }
                        for r in strat_runs
                    ],
                    row_key="run_id",
                    page_size=15,
                )


def _best_strategy(runs: list[dict]) -> str:
    best = max(runs, key=lambda r: float(r.get("calmar") or 0), default=None)
    if not best:
        return "—"
    return (best.get("strategy") or "").split("|")[0].strip() or "—"
