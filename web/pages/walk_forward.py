"""Walk-Forward dashboard page — fast validation gate and fold history."""

from __future__ import annotations

import asyncio
import json
from typing import cast

import polars as pl
from nicegui import ui

from web.components import (
    COLORS,
    THEME,
    divider,
    empty_state,
    info_box,
    kpi_grid,
    page_header,
    page_layout,
    paginated_table,
)
from web.state import (
    aget_run_ledger,
    aget_walk_forward_folds,
    aget_walk_forward_runs,
    build_walk_forward_run_options,
)


async def walk_forward_page() -> None:
    """Render the walk-forward validation dashboard."""
    runs = await aget_walk_forward_runs(limit=50)

    with page_layout("Walk Forward", "view_week"):
        theme = THEME
        colors = COLORS

        page_header(
            "Walk-Forward",
            "Fast promotion gate with fold lineage and parity checks. Replay/live remain separate.",
        )
        info_box(
            "Use this page to inspect fast-validator runs from local DuckDB/parquet. "
            "Archived paper-session execution remains on /paper_ledger.",
            color="blue",
        )

        if not runs:
            empty_state(
                "No walk-forward runs yet",
                "Run: doppler run -- uv run pivot-paper-trading walk-forward --help",
                icon="view_week",
            )
            return

        passes = [
            run
            for run in runs
            if str(run.get("decision") or run.get("status") or "").upper() == "PASS"
        ]
        fails = [
            run
            for run in runs
            if str(run.get("decision") or run.get("status") or "").upper() == "FAIL"
        ]
        latest = runs[0]
        latest_pass = passes[0] if passes else None

        kpi_grid(
            [
                dict(
                    title="Recent Runs",
                    value=f"{len(runs):,}",
                    subtitle="Fast validator outputs",
                    icon="query_stats",
                    color=colors["info"],
                ),
                dict(
                    title="Passing Gates",
                    value=f"{len(passes):,}",
                    subtitle="Promotion approved",
                    icon="check_circle",
                    color=colors["success"],
                ),
                dict(
                    title="Failing Gates",
                    value=f"{len(fails):,}",
                    subtitle="Needs review",
                    icon="cancel",
                    color=colors["error"],
                ),
                dict(
                    title="Latest Decision",
                    value=str(latest.get("decision") or latest.get("status") or "-"),
                    subtitle=str(latest.get("strategy") or "-"),
                    icon="flag",
                    color=colors["warning"]
                    if str(latest.get("decision") or "").upper() == "FAIL"
                    else colors["success"],
                ),
            ],
            columns=4,
        )

        if latest_pass:
            info_box(
                "Latest passing gate: "
                f"{latest_pass.get('strategy')} "
                f"{str(latest_pass.get('direction_filter') or 'BOTH').upper()} "
                f"{latest_pass.get('start_date')}→{latest_pass.get('end_date')} "
                f"({str(latest_pass.get('gate_key') or '')[:12]})",
                color="green",
            )

        options = build_walk_forward_run_options(runs)
        labels = list(options.keys())
        if not labels:
            empty_state(
                "No walk-forward labels",
                "The validation run list is empty or malformed.",
                icon="view_week",
            )
            return

        current_label = {"value": labels[0]}

        def _select_run(label: str) -> None:
            current_label["value"] = label
            _render.refresh(label)

        controls = ui.row().classes("items-center gap-3 mb-4 w-full")
        with controls:
            ui.select(
                labels,
                value=labels[0],
                label="Validation Gate",
                on_change=lambda e: _select_run(str(e.value)),
            ).props("outlined dense use-input options-dense input-debounce=0").classes(
                "w-full max-w-5xl"
            )
            ui.button(
                "Refresh", icon="refresh", on_click=lambda: _render.refresh(current_label["value"])
            ).props("outline")

        @ui.refreshable
        def _render(label: str) -> None:
            run_id = options.get(label, "")
            if not run_id:
                return

            run = next((row for row in runs if str(row.get("wf_run_id") or "") == run_id), None)
            if run is None:
                return

            summary_obj = run.get("summary_json")
            summary = cast(dict[str, object], summary_obj) if isinstance(summary_obj, dict) else {}
            lineage_obj = run.get("lineage_json")
            lineage = cast(dict[str, object], lineage_obj) if isinstance(lineage_obj, dict) else {}
            strategy_params_obj = lineage.get("strategy_params")
            strategy_params = (
                cast(dict[str, object], strategy_params_obj)
                if isinstance(strategy_params_obj, dict)
                else {}
            )

            run_container = ui.column().classes("w-full")

            async def _load() -> None:
                try:
                    folds = await aget_walk_forward_folds(run_id)
                    ref_run_ids = [str(fold.get("reference_run_id") or "") for fold in folds]
                    non_empty_ref_run_ids = [ref_run_id for ref_run_id in ref_run_ids if ref_run_id]
                    ledgers = await asyncio.gather(
                        *[
                            aget_run_ledger(ref_run_id, execution_mode="BACKTEST")
                            for ref_run_id in non_empty_ref_run_ids
                        ]
                    )
                except Exception as exc:
                    run_container.clear()
                    with run_container:
                        empty_state(
                            "Walk-forward details unavailable",
                            f"Could not load validation folds for {run_id}: {exc}",
                            icon="error",
                        )
                    return

                run_container.clear()
                fold_ledgers: dict[str, pl.DataFrame] = {}
                for ref_run_id, ledger_df in zip(non_empty_ref_run_ids, ledgers, strict=False):
                    fold_ledgers[ref_run_id] = ledger_df

                with run_container:
                    kpi_grid(
                        [
                            dict(
                                title="Result",
                                value=str(run.get("decision") or run.get("status") or "-"),
                                subtitle=str(run.get("validation_engine") or "-"),
                                icon="flag",
                                color=colors["success"]
                                if str(run.get("decision") or "").upper() == "PASS"
                                else colors["error"],
                            ),
                            dict(
                                title="Window",
                                value=f"{(run.get('start_date') or '')!s} → {(run.get('end_date') or '')!s}",
                                subtitle=f"{int(str(run.get('days_requested') or 0))} requested",
                                icon="calendar_month",
                                color=colors["info"],
                            ),
                            dict(
                                title="Trades",
                                value=f"{_safe_int(summary.get('total_trades')):,}",
                                subtitle=f"P/L ₹{_safe_float(summary.get('total_pnl')):,.2f}",
                                icon="receipt_long",
                                color=colors["primary"],
                            ),
                            dict(
                                title="Return",
                                value=_fmt_pct(summary.get("avg_daily_return_pct")),
                                subtitle=(
                                    f"Profitable {_safe_int(summary.get('profitable_days')):,}/"
                                    f"{_safe_int(summary.get('replayed_days')):,} days"
                                ),
                                icon="trending_up",
                                color=colors["success"],
                            ),
                        ],
                        columns=4,
                    )

                    divider()
                    ui.label("Gate Metadata").classes("text-lg font-semibold mb-2").style(
                        f"color: {theme['text_primary']};"
                    )
                    with ui.row().classes("gap-4 flex-wrap mb-3"):
                        _meta_chip("Gate Key", str(run.get("gate_key") or "-"))
                        _meta_chip("Scope", str(run.get("scope_key") or "-"))
                        _meta_chip("Direction", str(run.get("direction_filter") or "BOTH"))
                        _meta_chip("Engine", str(run.get("validation_engine") or "-"))

                    with ui.expansion("Lineage JSON", icon="code").classes("w-full mb-3"):
                        ui.code(
                            json.dumps(lineage or {}, indent=2, sort_keys=True, default=str),
                            language="json",
                        ).classes("w-full")

                    with ui.expansion("Strategy Params", icon="tune").classes("w-full mb-3"):
                        ui.code(
                            json.dumps(
                                strategy_params or {}, indent=2, sort_keys=True, default=str
                            ),
                            language="json",
                        ).classes("w-full")

                    ui.label("Fold History").classes("text-lg font-semibold mb-2").style(
                        f"color: {theme['text_primary']};"
                    )
                    if not folds:
                        empty_state(
                            "No fold rows",
                            "This fast validator run has no persisted fold history.",
                            icon="view_week",
                        )
                    else:
                        paginated_table(
                            rows=[
                                {
                                    "fold_index": fold.get("fold_index"),
                                    "trade_date": fold.get("trade_date"),
                                    "status": fold.get("status"),
                                    "total_trades": int(str(fold.get("total_trades") or 0)),
                                    "total_pnl": f"₹{float(fold.get('total_pnl') or 0.0):,.2f}",
                                    "total_return_pct": _fmt_pct(fold.get("total_return_pct")),
                                    "reference_run_id": str(fold.get("reference_run_id") or "-")[
                                        :12
                                    ],
                                    "paper_session_id": str(fold.get("paper_session_id") or "-")[
                                        :12
                                    ],
                                    "parity_status": str(fold.get("parity_status") or "-"),
                                }
                                for fold in folds
                            ],
                            columns=[
                                {
                                    "name": "fold_index",
                                    "label": "Fold",
                                    "field": "fold_index",
                                    "align": "right",
                                },
                                {
                                    "name": "trade_date",
                                    "label": "Trade Date",
                                    "field": "trade_date",
                                },
                                {"name": "status", "label": "Status", "field": "status"},
                                {
                                    "name": "total_trades",
                                    "label": "Trades",
                                    "field": "total_trades",
                                    "align": "right",
                                },
                                {
                                    "name": "total_pnl",
                                    "label": "PnL",
                                    "field": "total_pnl",
                                    "align": "right",
                                },
                                {
                                    "name": "total_return_pct",
                                    "label": "Return %",
                                    "field": "total_return_pct",
                                    "align": "right",
                                },
                                {
                                    "name": "reference_run_id",
                                    "label": "Ref Run",
                                    "field": "reference_run_id",
                                },
                                {
                                    "name": "paper_session_id",
                                    "label": "Paper Session",
                                    "field": "paper_session_id",
                                },
                                {
                                    "name": "parity_status",
                                    "label": "Parity",
                                    "field": "parity_status",
                                },
                            ],
                            row_key="fold_index",
                            page_size=10,
                        )

                    divider()
                    ui.label("Fold Trade Ledgers").classes("text-lg font-semibold mb-2").style(
                        f"color: {theme['text_primary']};"
                    )
                    for fold in folds:
                        ref_run_id = str(fold.get("reference_run_id") or "")
                        fold_title = (
                            f"Fold {int(str(fold.get('fold_index') or 0)):02d} · "
                            f"{(fold.get('trade_date') or '')!s} · "
                            f"{(fold.get('status') or '')!s} · "
                            f"{ref_run_id[:12] or 'no-run'}"
                        )
                        with ui.expansion(fold_title, icon="receipt_long").classes("w-full mb-2"):
                            with ui.row().classes("gap-4 flex-wrap mb-3"):
                                _meta_chip("Trades", str(int(str(fold.get("total_trades") or 0))))
                                _meta_chip("PnL", f"₹{float(fold.get('total_pnl') or 0.0):,.2f}")
                                _meta_chip("Return", _fmt_pct(fold.get("total_return_pct")))
                                _meta_chip("Parity", str(fold.get("parity_status") or "-"))

                            ui.button(
                                "Open Backtest Run",
                                icon="open_in_new",
                                on_click=lambda rid=ref_run_id: ui.navigate.to(
                                    f"/backtest?run_id={rid}"
                                ),
                            ).props("outline dense").classes("mb-3")

                            fold_ledger_df: pl.DataFrame | None = fold_ledgers.get(ref_run_id)
                            if (
                                fold_ledger_df is None
                                or getattr(fold_ledger_df, "is_empty", lambda: True)()
                            ):
                                empty_state(
                                    "No trade rows",
                                    "This fold did not persist a trade ledger.",
                                    icon="receipt_long",
                                )
                                continue

                            trade_rows = [
                                {
                                    "idx": idx,
                                    "date": str(row.get("trade_date") or "")[:10],
                                    "symbol": str(row.get("symbol") or ""),
                                    "direction": str(row.get("direction") or ""),
                                    "entry_time": str(row.get("entry_time") or "")[:5],
                                    "exit_time": str(row.get("exit_time") or "")[:5],
                                    "entry_price": f"₹{float(row.get('entry_price') or 0.0):,.2f}",
                                    "exit_price": f"₹{float(row.get('exit_price') or 0.0):,.2f}",
                                    "qty": int(
                                        row.get("quantity") or row.get("position_size") or 0
                                    ),
                                    "position_value": f"₹{float(row.get('position_value') or 0.0):,.2f}",
                                    "profit_loss": f"₹{float(row.get('profit_loss') or 0.0):,.2f}",
                                    "profit_loss_pct": f"{float(row.get('profit_loss_pct') or 0.0):.4f}%",
                                    "exit_reason": str(row.get("exit_reason") or ""),
                                }
                                for idx, row in enumerate(fold_ledger_df.iter_rows(named=True))
                            ]
                            paginated_table(
                                rows=trade_rows,
                                columns=[
                                    {"name": "date", "label": "Date", "field": "date"},
                                    {"name": "symbol", "label": "Symbol", "field": "symbol"},
                                    {
                                        "name": "direction",
                                        "label": "Dir",
                                        "field": "direction",
                                        "align": "center",
                                    },
                                    {
                                        "name": "entry_time",
                                        "label": "In",
                                        "field": "entry_time",
                                        "align": "center",
                                    },
                                    {
                                        "name": "exit_time",
                                        "label": "Out",
                                        "field": "exit_time",
                                        "align": "center",
                                    },
                                    {
                                        "name": "entry_price",
                                        "label": "Entry",
                                        "field": "entry_price",
                                        "align": "right",
                                    },
                                    {
                                        "name": "exit_price",
                                        "label": "Exit",
                                        "field": "exit_price",
                                        "align": "right",
                                    },
                                    {
                                        "name": "qty",
                                        "label": "Qty",
                                        "field": "qty",
                                        "align": "right",
                                    },
                                    {
                                        "name": "position_value",
                                        "label": "Position ₹",
                                        "field": "position_value",
                                        "align": "right",
                                    },
                                    {
                                        "name": "profit_loss",
                                        "label": "P/L ₹",
                                        "field": "profit_loss",
                                        "align": "right",
                                    },
                                    {
                                        "name": "profit_loss_pct",
                                        "label": "P/L %",
                                        "field": "profit_loss_pct",
                                        "align": "right",
                                    },
                                    {
                                        "name": "exit_reason",
                                        "label": "Exit",
                                        "field": "exit_reason",
                                    },
                                ],
                                row_key="idx",
                                page_size=8,
                                sort_by="date",
                                descending=True,
                            )

                    if run.get("notes"):
                        divider()
                        info_box(str(run.get("notes")), color="blue")

            ui.timer(0.1, _load, once=True)

        _render(labels[0])


def _fmt_pct(value: object) -> str:
    try:
        if value is None:
            return "-"
        return f"{float(value):.4f}%"
    except (TypeError, ValueError):
        return "-"


def _safe_int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _meta_chip(label: str, value: str) -> None:
    with ui.column().classes("gap-0"):
        ui.label(label).classes("text-xs uppercase").style("color: var(--theme-text-muted);")
        ui.code(value or "-", language="text").classes("text-xs")
