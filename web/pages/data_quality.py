"""Data Quality page — tabbed analytics dashboard for data health."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import plotly.graph_objects as go
import polars as pl
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    apply_chart_theme,
    copyable_code,
    divider,
    empty_state,
    export_button,
    info_box,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
    safe_timer,
    set_table_mobile_labels,
)
from web.state import (
    aget_data_quality_detail,
    aget_date_coverage,
    aget_dq_issues_detail,
    aget_freshness_buckets,
    aget_status,
    aget_symbol_coverage,
    aget_symbol_gaps,
    aget_symbol_profile,
    aget_symbols,
    aget_top_gaps,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Debounce helper
# ---------------------------------------------------------------------------
_debounce_timers: dict[str, Any] = {}


def _client_is_alive() -> bool:
    try:
        client = ui.context.client
    except (AttributeError, RuntimeError):
        return False
    return bool(getattr(client, "has_socket_connection", False))


def _debounce(key: str, fn, delay: float = 0.3) -> None:
    """Cancel any pending timer for *key* and schedule *fn* after *delay*."""
    if key in _debounce_timers:
        try:
            _debounce_timers[key].deactivate()
        except Exception:
            pass
    _debounce_timers[key] = safe_timer(delay, fn)


async def _render_section_guarded(title: str, render_fn) -> None:
    """Render a section and keep the page alive if the section fails."""
    try:
        await render_fn()
    except Exception as exc:
        logger.exception("Data Quality section %s failed: %s", title, exc)
        if _client_is_alive():
            empty_state(
                f"{title} unavailable",
                "The dashboard hit an internal error while loading this section. "
                "Check the server logs for the stack trace.",
                icon="error",
            )


# ---------------------------------------------------------------------------
# Overview tab
# ---------------------------------------------------------------------------


def _render_overview_tab(detail: dict, status: dict) -> None:
    """Reorganize current flat-page content into collapsible sections."""
    theme = THEME
    colors = COLORS

    tables = status.get("tables", {})

    parquet_count = int(detail.get("parquet_symbol_count") or 0)
    tradeable_count = int(detail.get("tradeable_symbol_count") or 0)
    tradeable_covered = int(detail.get("tradeable_covered_count") or 0)
    short_history_count = int(detail.get("short_history_count") or 0)
    freshness_list: list[dict] = detail.get("freshness") or []
    history_dist: dict = detail.get("history_dist") or {}
    short_history_syms: list[dict] = detail.get("short_history_symbols") or []
    dq_summary: dict = detail.get("dq_summary") or {}

    # Most recent update across all tables
    valid = [f for f in freshness_list if f.get("days_since", -1) >= 0]
    if valid:
        freshest = min(valid, key=lambda f: f["days_since"])
        freshness_label = freshest["max_date"]
        freshness_days = freshest["days_since"]
    else:
        freshness_label = "—"
        freshness_days = -1

    freshness_color = (
        colors["success"]
        if freshness_days == 0
        else colors["warning"]
        if freshness_days <= 5
        else colors["error"]
    )

    # --- Symbol Coverage section ---
    with ui.expansion("Symbol Coverage", icon="verified").classes("w-full").props("default-opened"):
        kpi_grid(
            [
                dict(
                    title="Parquet Symbols",
                    value=f"{parquet_count:,}",
                    icon="show_chart",
                    color=colors["success"] if parquet_count >= 1400 else colors["warning"],
                ),
                dict(
                    title="Tradeable (NSE)",
                    value=f"{tradeable_count:,}" if tradeable_count else "—",
                    icon="verified",
                    color=colors["info"],
                ),
                dict(
                    title="Last Updated",
                    value=freshness_label,
                    icon="update",
                    color=freshness_color,
                ),
                dict(
                    title="Short History",
                    value=f"{short_history_count:,}",
                    icon="history_toggle_off",
                    color=colors["warning"] if short_history_count > 0 else colors["success"],
                ),
            ],
            columns=4,
        )

        if tradeable_count and parquet_count:
            gap = tradeable_count - tradeable_covered
            if gap > 0:
                info_box(
                    f"{gap} tradeable NSE symbols are not yet in parquet. "
                    "Run: doppler run -- uv run pivot-kite-ingest",
                    color="yellow",
                )
            else:
                info_box(
                    f"All {tradeable_count:,} tradeable symbols are covered in parquet.",
                    color="green",
                )
        elif parquet_count:
            info_box(
                f"{parquet_count:,} symbols in parquet. Instrument master CSV not found — "
                "run pivot-kite-ingest --refresh-instruments to enable tradeable-only filtering.",
                color="yellow",
            )

        if history_dist:
            hist_kpis = []
            for label, count in [
                ("5yr+", history_dist.get("5yr+", 0)),
                ("2-5yr", history_dist.get("2-5yr", 0)),
                ("1-2yr", history_dist.get("1-2yr", 0)),
                ("<1yr", history_dist.get("<1yr", 0)),
            ]:
                hist_kpis.append(
                    dict(
                        title=label,
                        value=f"{count:,}",
                        icon="bar_chart",
                        color=(
                            colors["success"]
                            if label == "5yr+"
                            else colors["info"]
                            if label in ("2-5yr", "1-2yr")
                            else colors["warning"]
                        ),
                    )
                )
            kpi_grid(hist_kpis, columns=4)

        if short_history_syms:
            with ui.expansion(
                f"Symbols with <1yr history ({len(short_history_syms)} shown)",
                icon="history_toggle_off",
            ).classes("w-full mt-2"):
                info_box(
                    "These symbols have < 252 trading days. "
                    "Run pivot-kite-ingest with --universe current-master for backfill.",
                    color="yellow",
                )
                paginated_table(
                    rows=short_history_syms,
                    columns=[
                        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                        {"name": "days", "label": "Days", "field": "days", "align": "right"},
                        {
                            "name": "first_date",
                            "label": "First Date",
                            "field": "first_date",
                            "align": "center",
                        },
                    ],
                    row_key="symbol",
                    page_size=25,
                )

    # --- Runtime Tables section ---
    with ui.expansion("Runtime Tables", icon="storage").classes("w-full"):
        _table_descriptions = {
            "cpr_daily": ("CPR Daily", "Daily CPR + pivot levels"),
            "atr_intraday": ("ATR Intraday", "ATR values from 5-min candles"),
            "cpr_thresholds": ("CPR Thresholds", "Rolling P50 CPR width per symbol"),
            "market_day_state": ("Market Day State", "Per-day gap, CPR, ATR per symbol"),
            "strategy_day_state": ("Strategy Day State", "Per-day strategy inputs"),
            "intraday_day_pack": (
                "Intraday Day Pack",
                "Full intraday candle arrays — required for backtesting",
            ),
            "or_daily": ("OR Daily (legacy)", "Opening Range data (legacy)"),
            "backtest_results": ("Backtest Results", "Trade-level backtest output"),
            "run_metrics": ("Run Metrics", "Aggregated performance per run"),
        }

        base = max(parquet_count, 1)
        _min_expected = {
            "cpr_daily": base * 1700,
            "atr_intraday": base * 900,
            "cpr_thresholds": base * 1700,
            "intraday_day_pack": base * 900,
            "market_day_state": base * 900,
            "strategy_day_state": base * 900,
        }

        freshness_by_table: dict[str, str] = {f["table"]: f["max_date"] for f in freshness_list}

        table_rows = []
        for key, (name, desc) in _table_descriptions.items():
            rows_n = int(tables.get(key) or 0)
            expected = _min_expected.get(key, 0)
            if rows_n == 0:
                status_text = "MISSING"
            elif expected and rows_n < expected:
                status_text = "PARTIAL"
            else:
                status_text = "READY"
            max_date = freshness_by_table.get(key, "—")
            table_rows.append(
                {
                    "status": status_text,
                    "table": name,
                    "rows": f"{rows_n:,}",
                    "last_date": max_date,
                    "desc": desc,
                }
            )

        overview_columns = [
            {"name": "status", "label": "Status", "field": "status", "align": "center"},
            {"name": "table", "label": "Table", "field": "table", "align": "left"},
            {"name": "rows", "label": "Rows", "field": "rows", "align": "right"},
            {"name": "last_date", "label": "Last Date", "field": "last_date", "align": "center"},
            {
                "name": "desc",
                "label": "Description",
                "field": "desc",
                "align": "left",
            },
        ]
        overview_tbl = ui.table(
            columns=overview_columns,
            rows=table_rows,
            row_key="table",
        ).classes("w-full")
        set_table_mobile_labels(overview_tbl, overview_columns)

    # --- Data Issues section ---
    with ui.expansion("Data Issues", icon="report_problem").classes("w-full"):
        dq_total = int(dq_summary.get("total_affected", 0))
        dq_critical = int(dq_summary.get("critical_count", 0))
        dq_by_issue: list[dict] = dq_summary.get("by_issue") or []

        if dq_total == 0 and not dq_by_issue:
            info_box(
                "No data quality issues found. Run: doppler run -- uv run pivot-data-quality "
                "--refresh --full",
                color="yellow",
            )
        elif dq_total == 0:
            info_box("All data quality checks passed. No active issues.", color="green")
        else:
            sev_color = colors["error"] if dq_critical > 0 else colors["warning"]
            kpi_grid(
                [
                    dict(
                        title="Affected Symbols",
                        value=f"{dq_total:,}",
                        icon="bug_report",
                        color=sev_color,
                    ),
                    dict(
                        title="Critical Issues",
                        value=f"{dq_critical:,}",
                        icon="error",
                        color=colors["error"] if dq_critical > 0 else colors["success"],
                    ),
                    dict(
                        title="Check Types",
                        value=str(len(dq_by_issue)),
                        icon="checklist",
                        color=colors["info"],
                    ),
                ],
                columns=3,
            )

            issue_table_rows = [
                {
                    "severity": r.get("severity", "WARNING"),
                    "code": str(r.get("code", "")),
                    "symbol_count": int(r.get("symbol_count", 0)),
                }
                for r in dq_by_issue
            ]
            issue_columns = [
                {"name": "severity", "label": "Severity", "field": "severity", "align": "center"},
                {"name": "code", "label": "Issue Code", "field": "code", "align": "left"},
                {
                    "name": "symbol_count",
                    "label": "Symbols Affected",
                    "field": "symbol_count",
                    "align": "right",
                },
            ]
            issue_tbl = ui.table(
                columns=issue_columns,
                rows=issue_table_rows,
                row_key="code",
            ).classes("w-full")
            set_table_mobile_labels(issue_tbl, issue_columns)

            info_box(
                "To see details: doppler run -- uv run pivot-data-quality --issue-code "
                "OHLC_VIOLATION\n"
                "To re-scan: doppler run -- uv run pivot-data-quality --refresh --full",
                color="yellow",
            )

    # --- Fix Commands section ---
    with ui.expansion("Fix Commands", icon="build").classes("w-full"):
        for desc, cmd in [
            (
                "Fast DQ refresh (parquet presence)",
                "doppler run -- uv run pivot-data-quality --refresh",
            ),
            (
                "Full DQ scan (OHLC, timestamps, gaps, extremes — 1-5 min)",
                "doppler run -- uv run pivot-data-quality --refresh --full",
            ),
            ("Show all active DQ issues", "doppler run -- uv run pivot-data-quality"),
            (
                "Filter by issue code",
                "doppler run -- uv run pivot-data-quality --issue-code OHLC_VIOLATION",
            ),
            ("Check table status", "doppler run -- uv run pivot-build --status"),
            (
                "Rebuild intraday_day_pack (20-60 min)",
                "doppler run -- uv run pivot-build --table pack --force --batch-size 16",
            ),
            ("Rebuild all tables", "doppler run -- uv run pivot-build --force"),
            (
                "Preview dead symbols (no deletion)",
                "doppler run -- uv run pivot-hygiene --dry-run",
            ),
            (
                "Purge dead symbols (requires --confirm)",
                "doppler run -- uv run pivot-hygiene --purge --confirm",
            ),
            (
                "Flag short-history and illiquid symbols",
                "doppler run -- uv run pivot-hygiene --check-stale",
            ),
            (
                "Convert Zerodha CSV → Parquet (first time only)",
                "doppler run -- uv run pivot-convert",
            ),
        ]:
            with ui.column().classes("mb-3 gap-1"):
                ui.label(desc).classes("text-sm").style(f"color: {theme['text_secondary']};")
                copyable_code(cmd)


# ---------------------------------------------------------------------------
# Coverage tab
# ---------------------------------------------------------------------------


def _render_coverage_tab(coverage_data: list[dict], date_cov: list[dict]) -> None:
    """Per-symbol coverage % with search, filter, chart, and CSV export."""
    colors = COLORS

    if not coverage_data:
        ui.label("No data available.").style(f"color: {THEME['text_muted']};")
        return

    avg_cov = round(sum(r["coverage_pct"] for r in coverage_data) / len(coverage_data), 1)
    below_90 = sum(1 for r in coverage_data if r["coverage_pct"] < 90)
    below_50 = sum(1 for r in coverage_data if r["coverage_pct"] < 50)

    kpi_grid(
        [
            dict(
                title="Total Symbols",
                value=f"{len(coverage_data):,}",
                icon="analytics",
                color=colors["info"],
            ),
            dict(
                title="Avg Coverage",
                value=f"{avg_cov}%",
                icon="pie_chart",
                color=colors["success"] if avg_cov >= 95 else colors["warning"],
            ),
            dict(
                title="Below 90%",
                value=f"{below_90:,}",
                icon="warning",
                color=colors["warning"] if below_90 > 0 else colors["gray"],
            ),
            dict(
                title="Below 50%",
                value=f"{below_50:,}",
                icon="error",
                color=colors["error"] if below_50 > 0 else colors["gray"],
            ),
        ],
        columns=4,
    )

    # Search + bucket filter
    search_input = (
        ui.input("Search symbol", placeholder="e.g. RELIANCE")
        .props("dense outlined clearable")
        .classes("w-48 min-w-[140px]")
    )
    bucket_select = (
        ui.select(
            {"all": "All", "gt95": "> 95%", "90_95": "90-95%", "lt90": "< 90%", "lt50": "< 50%"},
            value="all",
            label="Coverage Filter",
        )
        .props("dense outlined")
        .classes("w-40 min-w-[140px]")
    )

    @ui.refreshable
    def _coverage_table() -> None:
        filtered = coverage_data
        search = (search_input.value or "").strip().upper()
        if search:
            filtered = [r for r in filtered if search in r["symbol"]]
        bkt = bucket_select.value
        if bkt == "gt95":
            filtered = [r for r in filtered if r["coverage_pct"] > 95]
        elif bkt == "90_95":
            filtered = [r for r in filtered if 90 <= r["coverage_pct"] <= 95]
        elif bkt == "lt90":
            filtered = [r for r in filtered if r["coverage_pct"] < 90]
        elif bkt == "lt50":
            filtered = [r for r in filtered if r["coverage_pct"] < 50]
        filtered = sorted(filtered, key=lambda r: r["coverage_pct"])

        rows = [
            {
                "symbol": r["symbol"],
                "first_date": r["first_date"],
                "last_date": r["last_date"],
                "distinct_days": r["distinct_days"],
                "coverage_pct": round(float(r["coverage_pct"]), 1),
                "gap_estimate": r["gap_estimate"],
            }
            for r in filtered
        ]
        columns = [
            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
            {"name": "first_date", "label": "First Date", "field": "first_date", "align": "left"},
            {"name": "last_date", "label": "Last Date", "field": "last_date", "align": "left"},
            {
                "name": "distinct_days",
                "label": "Days",
                "field": "distinct_days",
                "align": "right",
            },
            {
                "name": "coverage_pct",
                "label": "Coverage %",
                "field": "coverage_pct",
                "align": "right",
                "format": "decimal:1",
            },
            {
                "name": "gap_estimate",
                "label": "Est. Gaps",
                "field": "gap_estimate",
                "align": "right",
            },
        ]
        paginated_table(rows=rows, columns=columns, row_key="symbol", page_size=25)
        if filtered:
            export_button(
                pl.DataFrame(filtered), filename="symbol_coverage.csv", label="Export CSV"
            )

    _coverage_table()
    search_input.on("update:model-value", lambda: _debounce("coverage", _coverage_table.refresh))
    bucket_select.on("update:model-value", lambda: _debounce("coverage", _coverage_table.refresh))

    divider()

    # Date-level coverage line chart
    if date_cov:
        dates = [r["trading_date"] for r in date_cov]
        counts = [r["symbol_count"] for r in date_cov]
        median_count = sorted(counts)[len(counts) // 2] if counts else 0

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=counts,
                mode="lines",
                name="Symbols",
                line=dict(color=colors["primary"], width=1),
            )
        )
        fig.add_hline(
            y=median_count,
            line_dash="dash",
            annotation_text=f"Median: {median_count:,}",
            line_color=colors["gray"],
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Symbol Count",
            title="Daily Symbol Coverage Over Time",
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-80")


# ---------------------------------------------------------------------------
# Gaps tab
# ---------------------------------------------------------------------------


async def _render_gaps_tab(gaps: list[dict]) -> None:
    """Top gaps > 5 calendar days + symbol drill-down."""
    colors = COLORS

    if not gaps:
        ui.label("No gaps > 5 calendar days detected.").style(f"color: {THEME['text_muted']};")
        return

    unique_syms = len({g["symbol"] for g in gaps})
    largest = gaps[0]["gap_days"] if gaps else 0

    kpi_grid(
        [
            dict(
                title="Total Gaps",
                value=f"{len(gaps):,}",
                icon="broken_image",
                color=colors["warning"],
            ),
            dict(
                title="Largest Gap",
                value=f"{largest} days",
                icon="event_busy",
                color=colors["error"],
            ),
            dict(
                title="Affected Symbols",
                value=f"{unique_syms:,}",
                icon="people",
                color=colors["info"],
            ),
        ],
        columns=3,
    )

    ui.label("Top Gaps (> 5 calendar days)").classes("text-lg font-semibold mb-4").style(
        f"color: {THEME['text_primary']};"
    )

    columns = [
        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
        {"name": "gap_start", "label": "Gap Start", "field": "gap_start", "align": "left"},
        {"name": "gap_end", "label": "Gap End", "field": "gap_end", "align": "left"},
        {"name": "gap_days", "label": "Calendar Days", "field": "gap_days", "align": "right"},
    ]
    paginated_table(rows=gaps, columns=columns, row_key=None, page_size=25)

    divider()

    # Symbol drill-down
    ui.label("Symbol Gap Drill-Down").classes("text-lg font-semibold mb-4").style(
        f"color: {THEME['text_primary']};"
    )
    sym_input = (
        ui.input("Enter symbol", placeholder="e.g. RELIANCE")
        .props("dense outlined clearable")
        .classes("w-48")
    )

    @ui.refreshable
    async def _symbol_gap_detail() -> None:
        sym = (sym_input.value or "").strip().upper()
        if not sym:
            ui.label("Enter a symbol above to see its gaps.").classes("text-sm").style(
                f"color: {THEME['text_muted']};"
            )
            return
        sym_gaps = await aget_symbol_gaps(sym)
        if not sym_gaps:
            ui.label(f"No gaps > 3 days found for {sym}.").classes("text-sm").style(
                f"color: {THEME['text_muted']};"
            )
            return
        ui.label(f"{len(sym_gaps)} gap(s) for {sym}:").classes("text-sm font-semibold")
        cols = [
            {"name": "gap_start", "label": "Gap Start", "field": "gap_start", "align": "left"},
            {"name": "gap_end", "label": "Gap End", "field": "gap_end", "align": "left"},
            {"name": "gap_days", "label": "Calendar Days", "field": "gap_days", "align": "right"},
        ]
        paginated_table(rows=sym_gaps, columns=cols, row_key="gap_start", page_size=25)

    await _symbol_gap_detail()
    sym_input.on("update:model-value", lambda: _debounce("gap_drill", _symbol_gap_detail.refresh))


# ---------------------------------------------------------------------------
# Freshness tab
# ---------------------------------------------------------------------------


def _render_freshness_tab(buckets: list[dict]) -> None:
    """Freshness bucket distribution with pie chart and expandable symbol lists."""
    colors = COLORS

    if not buckets:
        ui.label("No data available.").style(f"color: {THEME['text_muted']};")
        return

    total = sum(b["count"] for b in buckets)
    colors_map = {
        "Fresh (<7d)": colors["success"],
        "Recent (7-30d)": colors["info"],
        "Stale (30-90d)": colors["warning"],
        "Very Stale (>90d)": colors["error"],
    }

    kpi_grid(
        [
            dict(
                title=b["bucket"],
                value=f"{b['count']:,}",
                subtitle=f"{round(b['count'] / max(total, 1) * 100, 1)}%",
                icon="schedule",
                color=colors_map.get(b["bucket"], colors["gray"]),
            )
            for b in buckets
        ]
    )

    fig = go.Figure(
        data=[
            go.Pie(
                labels=[b["bucket"] for b in buckets],
                values=[b["count"] for b in buckets],
                marker=dict(colors=[colors_map.get(b["bucket"], colors["gray"]) for b in buckets]),
                textinfo="label+value+percent",
                hole=0.4,
            )
        ]
    )
    fig.update_layout(title="Data Freshness Distribution")
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-80")

    for b in buckets:
        symbols = b.get("symbols", [])
        with ui.expansion(f"{b['bucket']} — {b['count']:,} symbols", icon="list").classes("w-full"):
            if symbols:
                ui.label(", ".join(symbols[:50])).classes("text-xs font-mono").style(
                    f"color: {THEME['text_secondary']};"
                )
                if b["count"] > 50:
                    ui.label(f"... and {b['count'] - 50} more").classes("text-xs").style(
                        f"color: {THEME['text_muted']};"
                    )
            else:
                ui.label("No symbols in this bucket.").style(f"color: {THEME['text_muted']};")


# ---------------------------------------------------------------------------
# Anomalies tab
# ---------------------------------------------------------------------------


def _render_anomalies_tab(anomalies: list[dict]) -> None:
    """Active DQ issues from pre-computed data_quality_issues table."""
    colors = COLORS

    if not anomalies:
        ui.label("No data quality issues detected.").style(f"color: {THEME['text_muted']};")
        return

    # Count by issue type
    by_type: dict[str, int] = {}
    for a in anomalies:
        by_type[a["issue"]] = by_type.get(a["issue"], 0) + 1

    severity_colors = {
        "CRITICAL": colors["error"],
        "WARNING": colors["warning"],
        "INFO": colors["info"],
    }

    kpi_grid(
        [
            dict(
                title=issue,
                value=f"{count:,}",
                icon="report_problem",
                color=severity_colors.get(
                    next(
                        (a["severity"] for a in anomalies if a["issue"] == issue),
                        "WARNING",
                    ),
                    colors["gray"],
                ),
            )
            for issue, count in sorted(by_type.items(), key=lambda x: -x[1])
        ]
    )

    # Filter controls
    issue_options = {"all": "All"} | {k: k for k in sorted(by_type.keys())}
    issue_filter = (
        ui.select(issue_options, value="all", label="Issue Type")
        .props("dense outlined")
        .classes("w-56")
    )
    sym_filter = (
        ui.input("Filter symbol", placeholder="e.g. TCS")
        .props("dense outlined clearable")
        .classes("w-48")
    )

    @ui.refreshable
    def _anomaly_table() -> None:
        filtered = anomalies
        if issue_filter.value and issue_filter.value != "all":
            filtered = [a for a in filtered if a["issue"] == issue_filter.value]
        search = (sym_filter.value or "").strip().upper()
        if search:
            filtered = [a for a in filtered if search in a["symbol"]]

        columns = [
            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
            {"name": "issue", "label": "Issue", "field": "issue", "align": "left"},
            {"name": "severity", "label": "Severity", "field": "severity", "align": "center"},
            {"name": "detail", "label": "Detail", "field": "detail", "align": "left"},
            {"name": "last_seen", "label": "Last Seen", "field": "last_seen", "align": "left"},
        ]
        paginated_table(rows=filtered, columns=columns, row_key=None, page_size=25)
        if filtered:
            export_button(pl.DataFrame(filtered), filename="anomalies.csv", label="Export CSV")

    _anomaly_table()
    issue_filter.on("update:model-value", lambda: _debounce("anomaly", _anomaly_table.refresh))
    sym_filter.on("update:model-value", lambda: _debounce("anomaly", _anomaly_table.refresh))


# ---------------------------------------------------------------------------
# Symbol Lookup tab
# ---------------------------------------------------------------------------


async def _render_symbol_lookup_tab(symbols: list[str]) -> None:
    """Per-symbol profile with daily + 5-min + market_day_state stats + gaps."""
    colors = COLORS

    if not symbols:
        empty_state(
            "No symbols available",
            "Run the Kite ingest and runtime build steps to populate the symbol list.",
            icon="search_off",
        )
        return

    sym_select = (
        ui.select(
            options=symbols,
            with_input=True,
            label="Search Symbol",
            value=None,
        )
        .props("dense outlined clearable")
        .classes("w-64")
    )

    @ui.refreshable
    async def _profile_card() -> None:
        sym = sym_select.value
        if not sym:
            ui.label("Select a symbol to view its data profile.").classes("text-sm").style(
                f"color: {THEME['text_muted']};"
            )
            return

        profile = await aget_symbol_profile(sym)
        if profile is None:
            ui.label(f"No data found for {sym}.").style(f"color: {THEME['text_muted']};")
            return

        kpi_grid(
            [
                dict(
                    title="Daily Range",
                    value=f"{profile['daily_first']} to {profile['daily_last']}",
                    icon="date_range",
                    color=colors["info"],
                ),
                dict(
                    title="Daily Rows",
                    value=f"{profile['daily_rows']:,}",
                    icon="storage",
                    color=colors["info"],
                ),
                dict(
                    title="Coverage",
                    value=f"{profile['daily_coverage_pct']}%",
                    icon="pie_chart",
                    color=(
                        colors["success"]
                        if profile["daily_coverage_pct"] >= 95
                        else colors["warning"]
                    ),
                ),
                dict(
                    title="Gaps (>3d)",
                    value=f"{len(profile['gaps'])}",
                    icon="broken_image",
                    color=(colors["warning"] if profile["gaps"] else colors["success"]),
                ),
            ]
        )

        if profile.get("fivemin_rows", 0) > 0:
            kpi_grid(
                [
                    dict(
                        title="5-Min Range",
                        value=f"{profile['fivemin_first']} to {profile['fivemin_last']}",
                        icon="access_time",
                        color=colors["warning"],
                    ),
                    dict(
                        title="5-Min Rows",
                        value=f"{profile['fivemin_rows']:,}",
                        icon="storage",
                        color=colors["warning"],
                    ),
                    dict(
                        title="5-Min Days",
                        value=f"{profile['fivemin_days']:,}",
                        icon="calendar_month",
                        color=colors["warning"],
                    ),
                    dict(
                        title="Day Pack Rows",
                        value=f"{profile['mds_rows']:,}",
                        icon="database",
                        color=colors["info"],
                    ),
                ]
            )

        gaps = profile.get("gaps", [])
        if gaps:
            ui.label("Gaps (> 3 calendar days)").classes("text-lg font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )
            cols = [
                {"name": "gap_start", "label": "Start", "field": "gap_start", "align": "left"},
                {"name": "gap_end", "label": "End", "field": "gap_end", "align": "left"},
                {"name": "gap_days", "label": "Days", "field": "gap_days", "align": "right"},
            ]
            paginated_table(rows=gaps, columns=cols, row_key="gap_start", page_size=25)
        else:
            ui.label("No significant gaps detected.").classes("text-sm").style(
                f"color: {colors['success']};"
            )

    await _profile_card()
    sym_select.on("update:model-value", lambda: _debounce("profile", _profile_card.refresh))


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------


async def data_quality_page() -> None:
    """Render the Data Quality page with tabbed analytics.

    Strategy: fetch only the lightweight overview data before rendering (fast),
    then load all heavy per-tab data in parallel in the background after the page
    is served.  Each tab shows a spinner until its data arrives.
    """
    # Lightweight pre-fetch — these two queries are fast (~100ms each).
    try:
        status, detail = await asyncio.gather(
            aget_status(lite=True),
            aget_data_quality_detail(),
        )
    except Exception as exc:
        logger.exception("Data Quality page: failed to load overview data: %s", exc)
        status, detail = {}, {}

    # Mutable state filled in by the background loader after the page renders.
    tab_state: dict[str, Any] = {
        "coverage_data": None,
        "date_cov": None,
        "gaps": None,
        "buckets": None,
        "anomalies": None,
        "symbols": None,
    }

    with page_layout("Data Quality", "verified"):
        page_header(
            "Data Quality",
            "Runtime tables status, data freshness, and symbol history coverage",
        )

        tabs = ui.tabs().classes("w-full")
        with tabs:
            tab_overview = ui.tab("Overview", icon="dashboard")
            tab_coverage = ui.tab("Coverage", icon="analytics")
            tab_gaps = ui.tab("Gaps", icon="broken_image")
            tab_freshness = ui.tab("Freshness", icon="schedule")
            tab_anomalies = ui.tab("Anomalies", icon="report_problem")
            tab_lookup = ui.tab("Symbol Lookup", icon="search")

        # ---------------------------------------------------------------------------
        # Spinner helper and per-tab refreshable panels
        # ---------------------------------------------------------------------------

        def _tab_spinner(msg: str = "Loading…") -> None:
            with ui.row().classes("justify-center items-center p-12 w-full"):
                ui.spinner("dots", size="lg").style(f"color: {THEME['primary']};")
                ui.label(msg).classes("ml-4 text-sm").style(f"color: {THEME['text_muted']};")

        @ui.refreshable
        def _coverage_panel() -> None:
            if tab_state["coverage_data"] is None:
                _tab_spinner("Loading coverage data…")
            else:
                _render_coverage_tab(tab_state["coverage_data"], tab_state["date_cov"])

        @ui.refreshable
        async def _gaps_panel() -> None:
            if tab_state["gaps"] is None:
                _tab_spinner("Loading gap data…")
            else:
                await _render_gaps_tab(tab_state["gaps"])

        @ui.refreshable
        def _freshness_panel() -> None:
            if tab_state["buckets"] is None:
                _tab_spinner("Loading freshness data…")
            else:
                _render_freshness_tab(tab_state["buckets"])

        @ui.refreshable
        def _anomalies_panel() -> None:
            if tab_state["anomalies"] is None:
                _tab_spinner("Loading anomaly data…")
            else:
                _render_anomalies_tab(tab_state["anomalies"])

        @ui.refreshable
        async def _lookup_panel() -> None:
            if tab_state["symbols"] is None:
                _tab_spinner("Loading symbol list…")
            else:
                await _render_symbol_lookup_tab(tab_state["symbols"])

        # ---------------------------------------------------------------------------
        # Tab panels — Overview renders immediately; others show spinners
        # ---------------------------------------------------------------------------

        with ui.tab_panels(tabs, value=tab_overview).classes("w-full"):
            with ui.tab_panel(tab_overview):
                _render_overview_tab(detail, status)

            with ui.tab_panel(tab_coverage):
                _coverage_panel()

            with ui.tab_panel(tab_gaps):
                await _gaps_panel()

            with ui.tab_panel(tab_freshness):
                _freshness_panel()

            with ui.tab_panel(tab_anomalies):
                _anomalies_panel()

            with ui.tab_panel(tab_lookup):
                await _lookup_panel()

        # ---------------------------------------------------------------------------
        # Background loader — fires after the page is served to the browser
        # ---------------------------------------------------------------------------

        async def _load_heavy_tabs() -> None:
            """Fetch all heavy tab data in parallel, then refresh each panel."""
            try:
                (
                    coverage_data,
                    date_cov,
                    gaps,
                    buckets,
                    anomalies,
                    symbols,
                ) = await asyncio.gather(
                    aget_symbol_coverage(),
                    aget_date_coverage(),
                    aget_top_gaps(),
                    aget_freshness_buckets(),
                    aget_dq_issues_detail(),
                    aget_symbols(),
                )
            except Exception as exc:
                logger.exception("DQ tabs background load failed: %s", exc)
                coverage_data, date_cov, gaps, buckets, anomalies, symbols = (
                    [],
                    [],
                    [],
                    [],
                    [],
                    [],
                )

            tab_state.update(
                {
                    "coverage_data": coverage_data,
                    "date_cov": date_cov,
                    "gaps": gaps,
                    "buckets": buckets,
                    "anomalies": anomalies,
                    "symbols": symbols,
                }
            )

            if not _client_is_alive():
                return
            _coverage_panel.refresh()
            _gaps_panel.refresh()
            _freshness_panel.refresh()
            _anomalies_panel.refresh()
            _lookup_panel.refresh()

        safe_timer(0.1, _load_heavy_tabs)

        # Background refresh for the status badge in the Overview tab
        async def _refresh_status() -> None:
            if not _client_is_alive():
                return
            await aget_status(lite=False)

        safe_timer(0.5, _refresh_status)
