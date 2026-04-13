"""Compare Runs page — side-by-side performance comparison with Plotly."""

from __future__ import annotations

import json
from typing import Any

import plotly.graph_objects as go
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    _flatten_params,
    apply_chart_theme,
    divider,
    empty_state,
    format_drawdown_pct,
    kpi_grid,
    page_header,
    page_layout,
)
from web.state import aget_runs, build_run_options


def _format_param_value(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _effective_run_params(params: dict) -> dict:
    """Return a display copy with skipped-RVOL runs rendered as OFF."""
    display = dict(params or {})
    skip_rvol_check = bool(display.get("skip_rvol_check") or display.get("skip_rvol") or False)
    if skip_rvol_check:
        display["rvol_threshold"] = "OFF"
    return display


def _build_param_diff_rows(params_a: dict, params_b: dict) -> tuple[list[dict], int]:
    flat_a = dict(_flatten_params(_effective_run_params(params_a)))
    flat_b = dict(_flatten_params(_effective_run_params(params_b)))
    all_keys = sorted(set(flat_a) | set(flat_b))
    rows: list[dict] = []
    diff_count = 0
    for key in all_keys:
        value_a = flat_a.get(key)
        value_b = flat_b.get(key)
        same = value_a == value_b
        if not same:
            diff_count += 1
        rows.append(
            {
                "parameter": key,
                "run_a": _format_param_value(value_a),
                "run_b": _format_param_value(value_b),
                "status": "same" if same else "diff",
            }
        )
    return rows, diff_count


def _param_group(key: str) -> str:
    root = key.split(".", 1)[0]
    if root == "cpr_levels_config":
        return "CPR Levels Config"
    if root == "fbr_config":
        return "FBR Config"
    if root == "virgin_cpr_config":
        return "Virgin CPR Config"
    return "General"


def _build_param_sections(params_a: dict, params_b: dict) -> list[dict]:
    flat_a = dict(_flatten_params(_effective_run_params(params_a)))
    flat_b = dict(_flatten_params(_effective_run_params(params_b)))
    all_keys = sorted(set(flat_a) | set(flat_b))
    sections: dict[str, dict] = {}
    for key in all_keys:
        group = _param_group(key)
        section = sections.setdefault(
            group,
            {"name": group, "rows": [], "diff_count": 0, "total_count": 0},
        )
        value_a = flat_a.get(key)
        value_b = flat_b.get(key)
        same = value_a == value_b
        section["total_count"] += 1
        if not same:
            section["diff_count"] += 1
        section["rows"].append(
            {
                "parameter": key,
                "run_a": value_a,
                "run_b": value_b,
                "same": same,
            }
        )
    order = ["General", "CPR Levels Config", "FBR Config", "Virgin CPR Config"]
    return [sections[name] for name in order if name in sections]


def _render_param_value(value: object, *, is_diff: bool, theme: dict) -> None:
    style = (
        f"font-weight: 700; color: {theme['text_primary']};"
        if is_diff
        else f"color: {theme['text_secondary']};"
    )
    ui.label(_format_param_value(value)).classes("text-xs w-full").style(
        "word-break: break-word; white-space: normal; " + style
    )


def _render_param_section(
    section: dict, *, label_a: str, label_b: str, theme: dict, colors: dict
) -> None:
    diff_count = int(section.get("diff_count") or 0)
    total_count = int(section.get("total_count") or 0)
    rows = section.get("rows") or []
    with ui.expansion(
        f"{section['name']} ({diff_count} / {total_count} different)",
        value=diff_count > 0,
    ).classes("w-full mb-3"):
        with (
            ui.grid(columns=4)
            .classes("w-full gap-3 px-2 pb-2 items-center")
            .style(
                f"color: {theme['text_muted']}; border-bottom: 1px solid {theme['surface_border']};"
            )
        ):
            ui.label("Parameter").classes("text-xs font-semibold min-w-0")
            ui.label(label_a).classes("text-xs font-semibold min-w-0")
            ui.label(label_b).classes("text-xs font-semibold min-w-0")
            ui.label("Status").classes("text-xs font-semibold text-right min-w-0")

        for row in rows:
            same = bool(row.get("same"))
            row_style = (
                f"border-bottom: 1px solid {theme['surface_border']};"
                if same
                else f"border-bottom: 1px solid {theme['surface_border']}; background: {theme['surface']};"
            )
            with ui.grid(columns=4).classes("w-full gap-3 px-2 py-2 items-start").style(row_style):
                ui.label(str(row.get("parameter") or "")).classes("text-xs min-w-0").style(
                    "word-break: break-word; white-space: normal; "
                    + (
                        f"font-weight: 700; color: {theme['text_primary']};"
                        if not same
                        else f"color: {theme['text_secondary']};"
                    )
                )
                with ui.column().classes("gap-0 min-w-0"):
                    _render_param_value(row.get("run_a"), is_diff=not same, theme=theme)
                with ui.column().classes("gap-0 min-w-0"):
                    _render_param_value(row.get("run_b"), is_diff=not same, theme=theme)
                ui.label("different" if not same else "same").classes(
                    "text-right text-xs font-semibold min-w-0"
                ).style(
                    f"color: {colors['error'] if not same else theme['text_muted']};"
                    + (" font-weight: 700;" if not same else "")
                )


async def compare_page() -> None:
    """Side-by-side comparison of two backtest runs."""
    runs = await aget_runs(force=True)

    with page_layout("Compare", "compare_arrows"):
        theme = THEME
        colors = COLORS

        page_header(
            "Compare Runs",
            "Select two runs to compare their performance side-by-side",
        )

        if len(runs) < 2:
            empty_state(
                "Need at least 2 runs",
                "Run more backtests to enable comparison.",
                icon="compare_arrows",
            )
            return

        options = build_run_options(runs)
        labels = list(options.keys())

        # Restore from sessionStorage
        saved1 = await ui.run_javascript(
            "sessionStorage.getItem('cpr_compare_run1') || ''", timeout=2.0
        )
        saved2 = await ui.run_javascript(
            "sessionStorage.getItem('cpr_compare_run2') || ''", timeout=2.0
        )
        id_to_label = {v: k for k, v in options.items()}
        init1 = id_to_label.get(saved1 or "", labels[0])
        init2 = id_to_label.get(saved2 or "", labels[1] if len(labels) > 1 else labels[0])

        @ui.refreshable
        def render_comparison(label1: str, label2: str) -> None:
            id1 = options.get(label1, "")
            id2 = options.get(label2, "")

            ui.run_javascript(
                f"sessionStorage.setItem('cpr_compare_run1','{id1}');"
                f"sessionStorage.setItem('cpr_compare_run2','{id2}');"
            )

            if id1 == id2:
                ui.label("⚠ Please select two different runs.").style(
                    f"color: {colors['warning']};"
                )
                return

            meta1 = next((r for r in runs if r.get("run_id") == id1), {})
            meta2 = next((r for r in runs if r.get("run_id") == id2), {})

            if not meta1 or not meta2:
                return

            def _strat(m: dict) -> str:
                return (m.get("strategy") or "").split("|")[0].strip()

            def _period(m: dict) -> str:
                s = str(m.get("start_date") or "")[:7]
                e = str(m.get("end_date") or "")[:7]
                return f"{s} → {e}"

            def _direction(m: dict) -> str:
                return str(m.get("direction_filter") or "BOTH").upper()

            # ── Side-by-side KPIs ────────────────────────────────────────────
            with ui.grid(columns=2).classes("w-full gap-6"):
                for meta, label in [(meta1, "Run A"), (meta2, "Run B")]:
                    with ui.column().classes("w-full"):
                        strategy = _strat(meta)
                        period = _period(meta)
                        direction = _direction(meta)
                        # Use centralized theme colors instead of hard-coded values
                        strat_color_key = {
                            "CPR_LEVELS": "strat_cpr_levels",
                            "FBR": "strat_fbr",
                        }.get(strategy, "strat_default")
                        color_tag = colors.get(strat_color_key, "#64748b")
                        direction_color = (
                            colors["success"]
                            if direction == "LONG"
                            else colors["error"]
                            if direction == "SHORT"
                            else colors["info"]
                        )
                        with ui.row().classes("items-center gap-3 mb-3"):
                            ui.label(f"**{label}**").classes("text-base font-bold").style(
                                f"color: {theme['text_primary']};"
                            )
                            ui.html(
                                f'<span style="background:{color_tag};color:#fff;padding:2px 8px;'
                                f"border-radius:3px;font-size:0.75rem;font-weight:600;"
                                f'font-family:monospace">{strategy}</span>'
                            )
                            ui.label(direction).classes("text-xs font-bold mono-font").style(
                                f"color: {direction_color};"
                            )
                            ui.label(period).classes("text-xs").style(
                                f"color: {theme['text_muted']};"
                            )

                        kpi_grid(
                            [
                                dict(
                                    title="Trades",
                                    value=f"{int(meta.get('trade_count') or 0):,}",
                                    icon="swap_horiz",
                                    color=colors["info"],
                                ),
                                dict(
                                    title="Win Rate",
                                    value=f"{float(meta.get('win_rate') or 0):.1f}%",
                                    icon="target",
                                    color=colors["success"],
                                ),
                                dict(
                                    title="Calmar",
                                    value=f"{float(meta.get('calmar') or 0):.2f}",
                                    icon="speed",
                                    color=colors["primary"],
                                ),
                                dict(
                                    title="Total P/L",
                                    value=f"₹{float(meta.get('total_pnl') or 0):,.0f}",
                                    icon="monetization_on",
                                    color=colors["success"]
                                    if float(meta.get("total_pnl") or 0) >= 0
                                    else colors["error"],
                                ),
                            ],
                            columns=2,
                        )

            divider()

            # ── Visual comparison chart ─────────────────────────────────────
            s1 = _strat(meta1) or id1[:8]
            s2 = _strat(meta2) or id2[:8]

            metrics = ["Win Rate %", "Profit Factor", "CAGR %", "Calmar", "Max DD % (daily)"]
            v1 = [
                float(meta1.get("win_rate") or 0),
                float(meta1.get("profit_factor") or 0),
                float(meta1.get("annual_return_pct") or 0),
                float(meta1.get("calmar") or 0),
                abs(float(meta1.get("max_dd_pct") or 0)),
            ]
            v2 = [
                float(meta2.get("win_rate") or 0),
                float(meta2.get("profit_factor") or 0),
                float(meta2.get("annual_return_pct") or 0),
                float(meta2.get("calmar") or 0),
                abs(float(meta2.get("max_dd_pct") or 0)),
            ]

            fig = go.Figure()
            fig.add_trace(go.Bar(name=s1, x=metrics, y=v1, marker_color=colors["primary"]))
            fig.add_trace(go.Bar(name=s2, x=metrics, y=v2, marker_color=colors["info"]))
            fig.update_layout(
                title="Performance Comparison",
                barmode="group",
                xaxis_title="Metric",
                showlegend=True,
            )
            apply_chart_theme(fig)
            ui.plotly(fig).classes("w-full h-72")

            divider()

            # ── Detailed comparison table ────────────────────────────────────
            ui.label("Detailed Metrics").classes("text-base font-semibold mb-3").style(
                f"color: {theme['text_primary']};"
            )
            metric_rows = [
                ("Strategy", _strat(meta1), _strat(meta2)),
                ("Direction", _direction(meta1), _direction(meta2)),
                ("Period", _period(meta1), _period(meta2)),
                (
                    "Symbols",
                    str(int(meta1.get("symbol_count") or 0)),
                    str(int(meta2.get("symbol_count") or 0)),
                ),
                (
                    "Trades",
                    f"{int(meta1.get('trade_count') or 0):,}",
                    f"{int(meta2.get('trade_count') or 0):,}",
                ),
                (
                    "Win Rate",
                    f"{float(meta1.get('win_rate') or 0):.1f}%",
                    f"{float(meta2.get('win_rate') or 0):.1f}%",
                ),
                (
                    "Total P/L",
                    f"₹{float(meta1.get('total_pnl') or 0):,.0f}",
                    f"₹{float(meta2.get('total_pnl') or 0):,.0f}",
                ),
                (
                    "Profit Factor",
                    f"{float(meta1.get('profit_factor') or 0):.2f}",
                    f"{float(meta2.get('profit_factor') or 0):.2f}",
                ),
                (
                    "Max Drawdown",
                    format_drawdown_pct(float(meta1.get("max_dd_pct") or 0)),
                    format_drawdown_pct(float(meta2.get("max_dd_pct") or 0)),
                ),
                (
                    "Annualized Return (CAGR)",
                    f"{float(meta1.get('annual_return_pct') or 0):.1f}%",
                    f"{float(meta2.get('annual_return_pct') or 0):.1f}%",
                ),
                (
                    "Calmar",
                    f"{float(meta1.get('calmar') or 0):.2f}",
                    f"{float(meta2.get('calmar') or 0):.2f}",
                ),
            ]
            ui.table(
                columns=[
                    {"name": "metric", "label": "Metric", "field": "metric", "align": "left"},
                    {"name": "run_a", "label": f"Run A ({s1})", "field": "run_a", "align": "right"},
                    {"name": "run_b", "label": f"Run B ({s2})", "field": "run_b", "align": "right"},
                ],
                rows=[{"metric": m, "run_a": a, "run_b": b} for m, a, b in metric_rows],
                row_key="metric",
            ).classes("w-full")

            params1 = {}
            params2 = {}
            try:
                params1 = json.loads(str(meta1.get("params_json") or "{}"))
            except (TypeError, ValueError):
                params1 = {}
            try:
                params2 = json.loads(str(meta2.get("params_json") or "{}"))
            except (TypeError, ValueError):
                params2 = {}
            if not isinstance(params1, dict):
                params1 = {}
            if not isinstance(params2, dict):
                params2 = {}

            param_sections = _build_param_sections(params1, params2)
            divider()

            ui.label("Parameter Comparison").classes("text-base font-semibold mb-2").style(
                f"color: {theme['text_primary']};"
            )
            ui.label(
                "All parameters are shown below, grouped by parameter family. "
                "Differences are bolded."
            ).classes("text-sm mb-3").style(f"color: {theme['text_secondary']};")

            if param_sections:
                for section in param_sections:
                    _render_param_section(
                        section,
                        label_a=f"Run A ({s1})",
                        label_b=f"Run B ({s2})",
                        theme=theme,
                        colors=colors,
                    )

        # ── Selectors ────────────────────────────────────────────────────────
        with ui.row().classes("w-full gap-4 items-end mb-4"):
            sel_a: Any | None = None
            sel_b: Any | None = None
            sel_a = (
                ui.select(
                    labels,
                    value=init1,
                    label="Run A",
                    on_change=lambda e: render_comparison.refresh(
                        e.value,
                        sel_b.value if sel_b is not None else init2,
                    ),
                )
                .props("outlined dense use-input options-dense input-debounce=0")
                .classes("flex-1")
            )
            sel_b = (
                ui.select(
                    labels,
                    value=init2,
                    label="Run B",
                    on_change=lambda e: render_comparison.refresh(
                        sel_a.value if sel_a is not None else init1,
                        e.value,
                    ),
                )
                .props("outlined dense use-input options-dense input-debounce=0")
                .classes("flex-1")
            )

        divider()
        render_comparison(init1, init2)
