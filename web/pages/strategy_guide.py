"""Strategy Guide page — human-readable strategy reference."""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from web.components import divider, page_header, page_layout


def _load_markdown() -> str:
    docs_root = Path(__file__).resolve().parents[2] / "docs"
    guide_file = docs_root / "strategy-guide.md"
    if not guide_file.exists():
        return "### Strategy guide not found\n\nNo strategy guide document was found at docs/strategy-guide.md."
    return guide_file.read_text(encoding="utf-8")


async def strategy_guide_page() -> None:
    """Render a static strategy reference page for quick onboarding."""
    with page_layout("Strategy Guide", "school"):
        page_header("Strategy Guide", "CPR_LEVELS vs FBR comparison and operator workflow")
        divider()

        ui.markdown(
            """
            The content below is loaded from `docs/strategy-guide.md`.
            It is the easiest high-level starting point for strategy questions.
            """
        ).classes("text-sm")

        ui.separator().classes("my-4")
        ui.markdown(_load_markdown()).classes("w-full")
