"""NiceGUI dashboard entry point — CPR Pivot Lab."""

from __future__ import annotations

import asyncio
import logging
import re
import sys

import nicegui.run as nicegui_run
from nicegui import app, ui

from web.pages.compare import compare_page
from web.pages.data_quality import data_quality_page
from web.pages.home import home_page
from web.pages.ops_pages import daily_summary_page, paper_ledger_page, pipeline_page, scans_page
from web.pages.run_detail import backtest_page
from web.pages.strategy_analysis import strategy_page
from web.pages.strategy_guide import strategy_guide_page
from web.pages.symbols import symbols_page
from web.pages.trades import trade_analytics_page
from web.state import shutdown_state

_RUN_ID_RE = re.compile(r"^[a-f0-9]{12}$")


def _install_nicegui_startup_fallback() -> None:
    """Keep the dashboard runnable on Windows environments that block process pools."""
    original_setup = nicegui_run.setup

    def _safe_setup() -> None:
        try:
            original_setup()
        except (NotImplementedError, PermissionError, OSError) as exc:
            logging.getLogger(__name__).warning(
                "NiceGUI process pool unavailable, continuing without it: %s", exc
            )
            nicegui_run.process_pool = None

    nicegui_run.setup = _safe_setup


@ui.page("/")
async def _home() -> None:
    await home_page()


@ui.page("/backtest")
async def _backtest() -> None:
    await backtest_page()


# Legacy route — redirect to /backtest preserving run_id
@ui.page("/run/{run_id}")
async def _run_detail(run_id: str) -> None:
    clean_run_id = str(run_id or "").strip().lower()
    if _RUN_ID_RE.fullmatch(clean_run_id):
        ui.navigate.to(f"/backtest?run_id={clean_run_id}")
    else:
        ui.navigate.to("/backtest")


@ui.page("/trades")
async def _trades() -> None:
    await trade_analytics_page()


@ui.page("/trade_analytics")
async def _trade_analytics_alias() -> None:
    await trade_analytics_page()


@ui.page("/compare")
async def _compare() -> None:
    await compare_page()


@ui.page("/strategy")
async def _strategy() -> None:
    await strategy_page()


@ui.page("/strategy-guide")
async def _strategy_guide() -> None:
    await strategy_guide_page()


@ui.page("/symbols")
async def _symbols() -> None:
    await symbols_page()


@ui.page("/data_quality")
async def _data_quality() -> None:
    await data_quality_page()


@ui.page("/scans")
async def _scans() -> None:
    await scans_page()


@ui.page("/pipeline")
async def _pipeline() -> None:
    await pipeline_page()


@ui.page("/paper_ledger")
async def _paper_ledger() -> None:
    await paper_ledger_page()


@ui.page("/daily_summary")
async def _daily_summary() -> None:
    await daily_summary_page()


app.on_shutdown(shutdown_state)


def main() -> None:
    _install_nicegui_startup_fallback()

    if sys.platform.startswith("win") and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Suppress Windows asyncio ConnectionResetError noise during client disconnects.
    class _ConnectionResetFilter(logging.Filter):
        def filter(self, record):
            return "ConnectionResetError" not in record.getMessage()

    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.addFilter(_ConnectionResetFilter())
    asyncio_logger.setLevel(logging.CRITICAL + 1)

    ui.run(
        port=9999,
        reload=False,
        show=False,
        dark=False,
        title="CPR Pivot Lab",
        root_path="/cpr-pivot-lab",
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
