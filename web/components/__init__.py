"""Reusable UI components for CPR Pivot Lab NiceGUI dashboard.

Provides:
- THEME / COLORS dicts for consistent styling (Terminal + Clean modes)
- page_layout() context manager — collapsible sidebar + top bar + content
- kpi_card / kpi_grid — aligned metric cards
- nav_card — home-page navigation tiles
- apply_chart_theme — Plotly theme integration
- strat_badge / exit_badge — coloured HTML badges
- paginated_table — pagination via @ui.refreshable
- info_box, divider, empty_state, page_header, export_button, export_menu
- save_session_state / restore_session_state — cross-reload state persistence
- accessible_heading — semantic heading with ARIA attributes
- Keyboard shortcuts (Alt+key navigation, ? for help)
"""

from __future__ import annotations

import html
import inspect
import json
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from nicegui import ui

# ---------------------------------------------------------------------------
# Theme definitions — Terminal (Dark) and Clean (Light)
# ---------------------------------------------------------------------------

# Terminal theme — Dark, brutalist trading terminal with neon green
THEME_TERMINAL = {
    "page_bg": "#0a0e14",  # Deeper near-black for better contrast
    "surface": "#161b22",
    "surface_border": "#30363d",
    "surface_hover": "#21262d",
    "text_primary": "#f0f6fc",
    "text_secondary": "#8b949e",
    # WCAG AA compliant (7:1 on surface) — from nse-momentum-lab
    "text_muted": "#b4b9c1",
    "primary": "#00ff88",  # Classic terminal phosphor green
    "primary_dark": "#00cc6a",
    "divider": "#30363d",
}

COLORS_TERMINAL = {
    "success": "#00ff88",  # Neon green
    "error": "#ff6b6b",
    "warning": "#ffd93d",
    "info": "#6bcfff",
    "primary": "#00ff88",
    "gray": "#9ca3af",  # Updated to match text_muted
    # Strategy colors — centralized for consistency
    "strat_cpr_levels": "#2563eb",
    "strat_fbr": "#10b981",
    "strat_virgin_cpr": "#8b5cf6",
    "strat_default": "#64748b",
}

# Clean theme — Light, warm professional dashboard with teal primary
THEME_CLEAN = {
    "page_bg": "#f7f6f3",  # Warm off-white — paper-like, easy on eyes
    "surface": "#ffffff",
    "surface_border": "#e0ddd8",  # Warm gray border
    "surface_hover": "#eeede9",
    "text_primary": "#1a1a1a",
    "text_secondary": "#525252",
    "text_muted": "#7a7a7a",
    "primary": "#0d9488",  # Teal — distinctive, not generic indigo
    "primary_dark": "#0f766e",
    "divider": "#e0ddd8",
}

COLORS_CLEAN = {
    "success": "#16a34a",
    "error": "#dc2626",
    "warning": "#d97706",
    "info": "#0d9488",  # Match primary
    "primary": "#0d9488",
    "gray": "#6b7280",
    # Strategy colors — centralized for consistency
    "strat_cpr_levels": "#0d9488",
    "strat_fbr": "#d97706",
    "strat_virgin_cpr": "#7c3aed",
    "strat_default": "#6b7280",
}


# ---------------------------------------------------------------------------
# THEME COLOR CONSTANTS - convenience accessors for current theme
# ---------------------------------------------------------------------------
def theme_text_primary() -> str:
    return THEME["text_primary"]


def theme_text_secondary() -> str:
    return THEME["text_secondary"]


def theme_text_muted() -> str:
    return THEME["text_muted"]


def theme_page_bg() -> str:
    return THEME["page_bg"]


def theme_surface() -> str:
    return THEME["surface"]


def theme_surface_border() -> str:
    return THEME["surface_border"]


def theme_surface_hover() -> str:
    return THEME["surface_hover"]


def theme_primary() -> str:
    return THEME["primary"]


def color_success() -> str:
    return COLORS["success"]


def color_error() -> str:
    return COLORS["error"]


def color_warning() -> str:
    return COLORS["warning"]


def color_info() -> str:
    return COLORS["info"]


def color_primary() -> str:
    return COLORS["primary"]


def color_gray() -> str:
    return COLORS["gray"]


# ---------------------------------------------------------------------------
# SPACING SYSTEM - 4px base scale for consistent rhythm (A11Y-006)
# ---------------------------------------------------------------------------
# Tailwind classes: spacing-1=4px, spacing-2=8px, spacing-3=12px, spacing-4=16px,
#                   spacing-6=24px, spacing-8=32px, spacing-12=48px, spacing-16=64px
SPACING_XS = "1"  # 4px  - tight grouping, related items
SPACING_SM = "2"  # 8px  - sibling spacing
SPACING_MD = "3"  # 12px - component internal spacing
SPACING_LG = "4"  # 16px - default gap between related elements
SPACING_XL = "6"  # 24px - section spacing
SPACING_2XL = "8"  # 32px - major section separation
SPACING_3XL = "12"  # 48px - page-level spacing
SPACING_4XL = "16"  # 64px - hero section spacing

# Semantic spacing presets for common patterns
SPACE_CARD_INNER = "gap-3 p-4"  # Inside cards
SPACE_SECTION = "mb-8"  # Between sections
SPACE_SUBSECTION = "mb-6"  # Between subsections
SPACE_RELATED = "gap-2"  # Related items in a row
SPACE_GROUP_TIGHT = "gap-1"  # Tightly grouped items
SPACE_GRID_DEFAULT = "gap-4"  # Grid gaps
SPACE_FORM_ROW = "gap-3 mb-4"  # Form rows
SPACE_LG = "mb-4"  # Large spacing
SPACE_MD = "mb-3"  # Medium spacing
SPACE_SM = "gap-2"  # Small spacing
SPACE_XL = "mb-6"  # Extra large spacing
SPACE_XS = "mb-1"  # Extra small spacing

# ---------------------------------------------------------------------------
# TYPOGRAPHY SCALE - Modular type scale (1.25 ratio) with semantic tokens
# ---------------------------------------------------------------------------
# Based on 16px base: 12→14→16→20→24→32→40→48
TYPE_DISPLAY = "text-5xl font-bold leading-tight tracking-tight"  # 48px
TYPE_HERO = "text-4xl font-bold leading-tight tracking-tight"  # 36px
TYPE_H1 = "text-3xl font-bold leading-tight"  # 30px
TYPE_H2 = "text-2xl font-semibold leading-snug"  # 24px
TYPE_H3 = "text-xl font-semibold leading-snug"  # 20px
TYPE_H4 = "text-lg font-medium leading-relaxed"  # 18px
TYPE_BODY = "text-base leading-relaxed"  # 16px
TYPE_BODY_LG = "text-lg leading-relaxed"  # 18px
TYPE_LABEL = "text-sm font-medium leading-relaxed"  # 14px (form labels, KPI labels)
TYPE_CAPTION = "text-xs leading-relaxed"  # 12px (metadata, timestamps)
TYPE_MONO = "text-sm font-mono leading-relaxed"  # 14px monospace (code, IDs)
TYPE_NUMBER = "tabular-nums"  # Apply to any text class for aligned numbers
TYPE_NUMBER_LG = "text-2xl font-bold tabular-nums leading-tight"  # Large metrics
TYPE_NUMBER_MD = "text-xl font-semibold tabular-nums leading-tight"  # Medium metrics

# Combined presets for common patterns
TYPE_PRESET_PAGE_HEADER = "text-4xl font-bold leading-tight tracking-tight"
TYPE_PRESET_SECTION_HEADER = "text-xl font-semibold leading-snug"
TYPE_PRESET_CARD_TITLE = "text-lg font-medium leading-relaxed"
TYPE_PRESET_KPI_LABEL = "text-xs uppercase tracking-wide font-medium"
TYPE_PRESET_KPI_VALUE = "text-2xl font-bold tabular-nums leading-tight"
TYPE_PRESET_TABLE_HEADER = "text-xs uppercase tracking-wide font-semibold"
TYPE_PRESET_TABLE_CELL = "text-sm font-mono leading-relaxed tabular-nums"
TYPE_PRESET_NAV_LABEL = "text-sm font-medium leading-relaxed"
TYPE_PRESET_BUTTON = "text-sm font-medium leading-relaxed"

# Live read-only theme/color views (avoid mutable global dict state)


class _LivePalette(Mapping[str, str]):
    """Dynamic mapping proxy that always resolves against current theme mode."""

    def __init__(self, getter: Callable[[], dict[str, str]]):
        self._getter = getter

    def __getitem__(self, key: str) -> str:
        return self._getter()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._getter())

    def __len__(self) -> int:
        return len(self._getter())

    def as_dict(self) -> dict[str, str]:
        return dict(self._getter())


# ---------------------------------------------------------------------------
# Navigation definition — grouped sections
# ---------------------------------------------------------------------------
NAV_SECTIONS = [
    {
        "label": "Analysis",
        "items": [
            {"label": "Run Results", "icon": "bar_chart", "path": "/backtest"},
            {"label": "Trade Analytics", "icon": "analytics", "path": "/trades"},
            {"label": "Compare", "icon": "compare_arrows", "path": "/compare"},
            {"label": "Strategy", "icon": "tune", "path": "/strategy"},
            {"label": "Symbols", "icon": "show_chart", "path": "/symbols"},
        ],
    },
    {
        "label": "Operations",
        "items": [
            {"label": "Scans", "icon": "radar", "path": "/scans"},
            {"label": "Pipeline", "icon": "engineering", "path": "/pipeline"},
            {"label": "Paper Sessions", "icon": "receipt_long", "path": "/paper_ledger"},
            {"label": "Market Monitor", "icon": "today", "path": "/daily_summary"},
        ],
    },
    {
        "label": "Reference",
        "items": [
            {"label": "Strategy Guide", "icon": "school", "path": "/strategy-guide"},
            {"label": "Data Quality", "icon": "verified", "path": "/data_quality"},
        ],
    },
]

# Flat list for backward compat (keyboard shortcuts, etc.)
NAV_ITEMS = [
    {"label": "Home", "icon": "home", "path": "/"},
    *[item for section in NAV_SECTIONS for item in section["items"]],
]

# ---------------------------------------------------------------------------
# Financial glossary — plain-English definitions for KPI tooltips
# ---------------------------------------------------------------------------
METRIC_GLOSSARY: dict[str, str] = {
    "Calmar": "Annual return divided by worst drawdown — higher is better. Above 2.0 is strong.",
    "Win Rate": "Percentage of trades that made money. Low win rate can still be profitable if winners are much larger than losers.",
    "Profit Factor": "Total money won divided by total money lost. Above 1.5 is good, above 2.0 is strong.",
    "Max Drawdown": "Largest peak-to-trough drop in portfolio value. Shows worst-case scenario during the test period.",
    "CAGR": "Compound Annual Growth Rate — smoothed yearly return as if growth were steady.",
    "Total P/L": "Net profit or loss in rupees across all trades.",
    "Total Return": "Percentage gain or loss on the starting capital.",
    "R-Multiple": "Trade result measured in risk units. 1R = you gained what you risked. -1R = you lost what you risked.",
    "Portfolio Base": "Starting capital the backtest assumes. All position sizes are computed from this.",
    "Trades": "Total number of completed trades (entry + exit).",
    "Traded Symbols": "Number of different stocks that had at least one trade.",
    "Symbols": "Universe size — total stocks in the backtest. Shows traded / total when they differ.",
    "Return": "Total percentage gain or loss on starting capital for this run.",
    "Capital": "Starting capital (portfolio base) used for this backtest run.",
}

EXIT_GLOSSARY: dict[str, str] = {
    "TARGET": "Price reached the profit target (R1 for LONG, S1 for SHORT).",
    "INITIAL_SL": "Price hit the initial stop loss — trade lost the risked amount.",
    "BREAKEVEN_SL": "Stop was moved to entry price after partial profit — exited flat.",
    "TRAILING_SL": "Trailing stop locked in profit as price moved favorably.",
    "TIME": "Position closed at 15:15 (market close) — neither target nor stop was hit.",
    "REVERSAL": "FBR-specific: the breakout failure pattern reversed.",
    "CANDLE_EXIT": "Exited after a fixed number of candles.",
}


# ---------------------------------------------------------------------------
# Theme state (encapsulated to avoid global mutation issues)
# ---------------------------------------------------------------------------
class _ThemeState:
    """Simple singleton for theme mode management.

    Note: For single-user NiceGUI dashboard, this module-level state is fine.
    For multi-user scenarios, NiceGUI's app.storage.user should be used instead.
    """

    def __init__(self) -> None:
        self._terminal = False

    @property
    def is_terminal(self) -> bool:
        return self._terminal

    def toggle(self) -> bool:
        """Toggle theme and return new mode (True=terminal, False=clean)."""
        self._terminal = not self._terminal
        return self._terminal


_theme_state = _ThemeState()


def get_current_theme() -> dict:
    return THEME_TERMINAL if _theme_state.is_terminal else THEME_CLEAN


def get_current_colors() -> dict:
    return COLORS_TERMINAL if _theme_state.is_terminal else COLORS_CLEAN


THEME: Mapping[str, str] = _LivePalette(get_current_theme)
COLORS: Mapping[str, str] = _LivePalette(get_current_colors)


# ---------------------------------------------------------------------------
# Fonts — Terminal (IBM Plex Sans + Fira Code) and Clean (Bitter + JetBrains Mono)
# ---------------------------------------------------------------------------
# Terminal fonts — IBM Plex Sans for body, Fira Code for monospace/terminal aesthetic
_FONT_HEAD_TERMINAL = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" as="style" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap" media="print" onload="this.media='all'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap"></noscript>
"""

# Clean theme fonts — Manrope (geometric grotesque) + JetBrains Mono for data
_FONT_HEAD_CLEAN = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" as="style" href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" media="print" onload="this.media='all'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"></noscript>
"""


def _get_font_html() -> str:
    return _FONT_HEAD_TERMINAL if _theme_state.is_terminal else _FONT_HEAD_CLEAN


# ---------------------------------------------------------------------------
# CSS base (uses CSS variables — works for both themes)
# ---------------------------------------------------------------------------
_PAGE_CSS_BASE = """
/* ============================================================================
   ACCESSIBILITY: Skip Navigation Link (A11Y-008)
   ============================================================================ */
.skip-link {
    position: absolute;
    top: -40px;
    left: 0;
    background: var(--theme-primary);
    color: var(--theme-page-bg);
    padding: 8px 16px;
    text-decoration: none;
    z-index: 10000;
    font-weight: 600;
    font-family: var(--font-mono);
    transition: top 0.2s;
}
.skip-link:focus {
    top: 0;
}

/* ============================================================================
   ACCESSIBILITY: Focus Indicators (A11Y-001)
   Visible focus state for keyboard navigation
   ============================================================================ */
/* Focus visible - only show for keyboard navigation, not mouse clicks */
*:focus-visible {
    outline: 2px solid var(--theme-primary) !important;
    outline-offset: 2px !important;
    border-radius: 2px;
}
/* Buttons need stronger focus */
.q-btn:focus-visible,
.q-item:focus-visible,
.nav-item:focus-visible {
    outline: 3px solid var(--theme-primary) !important;
    outline-offset: 2px !important;
    box-shadow: 0 0 0 4px var(--theme-primary-alpha) !important;
}
/* Table cells focus */
.q-table td:focus-visible,
.q-table th:focus-visible {
    outline: 2px solid var(--theme-primary) !important;
    background: var(--theme-surface-hover) !important;
}

/* ============================================================================
   RESPONSIVE: Touch Targets (RESP-002)
   Minimum 44x44px for all interactive elements (WCAG AAA)
   ============================================================================ */
.q-btn {
    min-height: 44px !important;
    min-width: 44px !important;
}
.q-btn--dense {
    min-height: 44px !important;
    min-width: 44px !important;
    padding: 0 12px !important;
}
.nav-item {
    min-height: 44px !important;
    display: flex !important;
    align-items: center !important;
}
.nav-tile {
    min-height: 44px !important;
    padding: 20px !important;
}
.q-item {
    min-height: 44px !important;
}
.q-pagination .q-btn {
    min-height: 44px !important;
    min-width: 44px !important;
}
/* Icon-only buttons need explicit touch targets */
.q-btn .q-icon {
    font-size: 20px;
}

/* ============================================================================
   ACCESSIBILITY: Color-Only Status Alternatives (A11Y-007)
   Visual indicators for color-blind users.

   NOTE: For accessibility, use the value_label() helper function instead of
   applying these classes directly. The helper includes proper ARIA labels.
   ============================================================================ */
.value-positive::before {
    content: "↑ ";
    color: var(--theme-color-success);
    font-weight: 700;
}
.value-negative::before {
    content: "↓ ";
    color: var(--theme-color-error);
    font-weight: 700;
}
.value-neutral::before {
    content: "-";
    color: var(--theme-color-gray);
    font-weight: 700;
}

/* ============================================================================
   TYPOGRAPHY: Type Scale & Readability (TYPE-001)
   Consistent type hierarchy, tabular numbers, improved readability
   ============================================================================ */
/* Base typography */
body {
    font-family: var(--font-body);
    font-size: 16px;
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* Tabular numbers for data alignment */
.tabular-nums {
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum";
    letter-spacing: 0.02em; /* Slightly open for numbers */
}

/* Improved line heights for dark mode */
body[class*="terminal"] {
    line-height: 1.65;
}
body[class*="terminal"] .kpi-card {
    line-height: 1.5;
}

/* Uppercase styling with intentional letter-spacing */
.text-uppercase {
    letter-spacing: 0.08em; /* More open for small caps/uppercase */
}
.tracking-wide {
    letter-spacing: 0.05em;
}
.tracking-tight {
    letter-spacing: -0.02em; /* Tighter for large display text */
}

/* Readable text width for long content */
.readable-width {
    max-width: 65ch;
}

/* Typography scale - ensure minimum sizes */
.text-xs { font-size: 0.75rem; }    /* 12px */
.text-sm { font-size: 0.875rem; }   /* 14px */
.text-base { font-size: 1rem; }     /* 16px */
.text-lg { font-size: 1.125rem; }    /* 18px */
.text-xl { font-size: 1.25rem; }     /* 20px */
.text-2xl { font-size: 1.5rem; }     /* 24px */
.text-3xl { font-size: 1.875rem; }   /* 30px */
.text-4xl { font-size: 2.25rem; }    /* 36px */
.text-5xl { font-size: 3rem; }       /* 48px */

/* Leading utilities */
.leading-tight { line-height: 1.25; }
.leading-snug { line-height: 1.375; }
.leading-normal { line-height: 1.5; }
.leading-relaxed { line-height: 1.625; }
.leading-loose { line-height: 2; }

/* Kerning */
.font-kerning {
    font-kerning: normal;
    text-rendering: optimizeLegibility;
}

/* Base styles */
body, .q-app {
    font-family: var(--font-body, 'Manrope', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif) !important;
}
.mono-font, .kpi-value, .q-table, .q-input, .q-select {
    font-family: var(--font-mono, 'JetBrains Mono', 'SF Mono', 'Consolas', 'Courier New', monospace) !important;
}
.q-table, .q-input, .q-select { letter-spacing: 0.02em; }
/* Prevent KPI values from overflowing narrow card cells */
.kpi-value {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
    display: block;
}

h1, .text-4xl { font-size: 2.25rem; font-weight: 700; letter-spacing: -0.02em; }
h2, .text-3xl { font-size: 1.75rem; font-weight: 600; letter-spacing: -0.01em; }
h3, .text-2xl { font-size: 1.5rem;  font-weight: 600; }
h4, .text-xl  { font-size: 1.25rem; font-weight: 600; }
.text-lg { font-size: 1.1rem; font-weight: 500; }
.text-sm { font-size: 0.875rem; }
.text-xs { font-size: 0.75rem; }

/* Terminal scanline — only in terminal mode, subtle overlay */
body.terminal-mode::after {
    content: "";
    position: fixed; top: 0; left: 0;
    width: 100vw; height: 100vh;
    background: repeating-linear-gradient(
        0deg, transparent, transparent 2px,
        rgba(255,255,255,0.01) 2px, rgba(255,255,255,0.01) 4px
    );
    pointer-events: none;
    z-index: 9999; opacity: 0.15;
}

/* KPI cards */
.kpi-card {
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    border-radius: var(--card-radius, 4px);
    padding: 20px 24px;
    transition: all 0.15s ease;
    box-shadow: var(--card-shadow, 0 2px 8px rgba(0,0,0,0.4));
    position: relative;
    min-width: 0;        /* allow grid cell to shrink below content size */
    overflow: hidden;    /* clip long values rather than blowing out the card */
}
/* Responsive KPI grid — auto-fit collapses empty tracks so cards fill full row width */
.kpi-grid {
    display: grid !important;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)) !important;
    gap: 16px;
}
@media (max-width: 480px) {
    .kpi-grid {
        grid-template-columns: repeat(2, 1fr) !important;
        gap: 8px;
    }
    .kpi-card { padding: 12px 14px; }
}
/* Responsive side-by-side layouts — stack on mobile */
.responsive-row {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
}
.responsive-row > * {
    flex: 1 1 320px;
    min-width: 0;
}
@media (max-width: 768px) {
    .responsive-row > * {
        flex: 1 1 100%;
    }
}
/* Responsive metric grids */
.responsive-grid-4 {
    display: grid !important;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)) !important;
    gap: 12px;
}
@media (max-width: 768px) {
    .responsive-grid-4 {
        grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    }
}
@media (max-width: 480px) {
    .responsive-grid-4 {
        grid-template-columns: 1fr !important;
    }
}
/* Responsive nav grid on home page */
.nav-grid-3 {
    display: grid !important;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)) !important;
}
.nav-grid-2 {
    display: grid !important;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)) !important;
}
/* Responsive step cards row */
.step-cards-row {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
}
.step-cards-row > * {
    flex: 1 1 280px;
    min-width: 0;
}
/* Responsive mini-card row */
.mini-card-row {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
}
.mini-card-row > * {
    min-width: 100px;
}
.kpi-card::before {
    content: "";
    position: absolute; top: 0; left: 0;
    width: 2px; height: 100%;
    background: var(--theme-primary);
    opacity: 0; transition: opacity 0.15s;
}
.kpi-card:hover {
    border-color: var(--theme-primary);
    box-shadow: var(--card-hover-shadow, 0 0 20px rgba(0,255,136,0.15));
    transform: translateX(2px);
}
.kpi-card:hover::before { opacity: 1; }

@keyframes fade-in-terminal {
    from { opacity: 0; }
    to   { opacity: 1; }
}
.kpi-card { animation: fade-in-terminal 0.15s ease-out backwards; }

/* Nav tiles */
.nav-tile {
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    border-radius: var(--tile-radius, 2px);
    padding: 20px; cursor: pointer;
    transition: all 0.1s;
    box-shadow: var(--tile-shadow, 0 2px 4px rgba(0,0,0,0.3));
    position: relative;
}
.nav-tile::after {
    content: ">>";
    position: absolute; right: 16px; top: 50%; transform: translateY(-50%);
    color: var(--theme-primary); opacity: 0; transition: opacity 0.15s;
    font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;
}
.nav-tile:hover {
    border-color: var(--theme-primary);
    transform: translateX(4px);
    box-shadow: 0 0 16px var(--theme-primary-alpha);
}
.nav-tile:hover::after { opacity: 1; }

/* Sidebar nav */
.nav-item {
    border-radius: var(--nav-radius, 0);
    padding: 8px 16px; margin: 0;
    transition: all 0.1s; cursor: pointer;
    color: var(--theme-text-secondary);
    position: relative;
}
.nav-item::before {
    content: ">";
    position: absolute; left: 4px;
    opacity: 0; color: var(--theme-primary);
    font-family: 'JetBrains Mono', monospace;
    transition: opacity 0.1s;
}
.nav-item:hover {
    background: var(--theme-surface-hover);
    color: var(--theme-text-primary);
    padding-left: 20px;
}
.nav-item:hover::before { opacity: 1; }
.nav-item-active {
    background: var(--theme-primary-alpha);
    color: var(--theme-primary) !important;
    font-weight: 500;
}
.nav-item-active::before { content: ">"; opacity: 1; }

/* Quasar table overrides */
.q-table {
    background: var(--theme-surface) !important;
    color: var(--theme-text-primary) !important;
    border: 1px solid var(--theme-surface-border);
}
.q-table thead th {
    color: var(--theme-primary) !important;
    font-weight: 600; text-transform: uppercase;
    font-size: 0.75rem; letter-spacing: 0.1em;
    border-bottom: 2px solid var(--theme-surface-border) !important;
    font-family: var(--font-mono); padding: 12px 16px !important;
}
.q-table tbody td {
    border-bottom: 1px solid var(--theme-divider) !important;
    color: var(--theme-text-primary) !important;
    font-family: var(--font-mono); padding: 12px 16px !important;
}
.q-table tbody tr:hover td { background: var(--theme-surface-hover) !important; }
.q-table tbody tr { cursor: pointer; }
/* Keyboard focus indicator for clickable table rows (A11Y-012) */
.q-table tbody tr:focus-visible td {
    outline: 2px solid var(--theme-primary) !important;
    outline-offset: -2px;
}

/* Quasar tabs */
.q-tab {
    color: var(--theme-text-secondary) !important;
    font-family: var(--font-mono);
    text-transform: uppercase; font-size: 0.75rem; letter-spacing: 0.05em;
}
.q-tab--active { color: var(--theme-primary) !important; font-weight: 600; }
.q-tabs__content { border-bottom: 1px solid var(--theme-surface-border); }

/* Quasar expansion */
.q-expansion-item {
    background: var(--theme-surface) !important;
    border: 1px solid var(--theme-surface-border);
    border-radius: 2px !important;
}

/* Quasar inputs */
.q-field__native, .q-field__input {
    color: var(--theme-text-primary) !important;
    font-family: var(--font-mono);
}
.q-field__label { color: var(--theme-text-secondary) !important; }
.q-field--outlined .q-field__control:before {
    border-color: var(--theme-surface-border) !important;
}
.q-field--outlined.q-field--focused .q-field__control:before {
    border-color: var(--theme-primary) !important;
}

/* Info box */
.info-box {
    background: var(--info-box-bg);
    border: 1px solid var(--theme-primary);
    border-radius: 2px; padding: 12px 16px;
}

/* Code blocks */
.code-block {
    background: var(--code-bg, #000);
    border: 1px solid var(--theme-surface-border);
    border-radius: 2px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    color: var(--theme-primary);
}

/* Mini sidebar */
.q-drawer--mini .sidebar-logo { display: none !important; }
.q-drawer--mini .nav-row {
    justify-content: center !important;
    padding: 10px 0 !important; margin: 2px 4px !important; gap: 0 !important;
}
.q-drawer--mini .nav-label { display: none !important; }
.q-drawer--mini .nav-icon  { font-size: 1.3rem !important; }
.q-drawer { transition: width 0.2s ease !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: var(--theme-page-bg); }
::-webkit-scrollbar-thumb { background: var(--theme-surface-border); border-radius: 0; }
::-webkit-scrollbar-thumb:hover { background: var(--theme-text-muted); }

/* Pagination */
.q-table .q-pagination { color: var(--theme-text-secondary) !important; }
.q-table .q-pagination .q-btn {
    color: var(--theme-text-secondary) !important;
    background: var(--theme-surface) !important;
    border: 1px solid var(--theme-surface-border) !important;
}
.q-table .q-pagination .q-btn:hover {
    background: var(--theme-surface-hover) !important;
    border-color: var(--theme-primary) !important;
    color: var(--theme-primary) !important;
}

/* Value colours */
.value-negative { color: var(--theme-color-error) !important; font-weight: 600; }
.value-positive  { color: var(--theme-color-success) !important; }
.value-neutral   { color: var(--theme-text-muted) !important; }

/* Table container */
.scrollable-table {
    max-height: 500px; overflow-y: auto; overflow-x: auto; width: 100%;
}
.q-table__card { overflow-x: auto !important; background: transparent !important; }

/* ============================================================================
   RESPONSIVE: Mobile Table Optimizations (RESP-003)
   Card-based view for small screens
   ============================================================================ */
@media (max-width: 768px) {
    .q-table {
        font-size: 0.8rem !important;
    }
    .q-table thead th {
        padding: 8px 12px !important;
        font-size: 0.65rem !important;
    }
    .q-table tbody td {
        padding: 10px 12px !important;
        font-size: 0.75rem !important;
    }
    /* Hide less important columns on mobile */
    .q-table .hide-mobile {
        display: none !important;
    }
    .q-table .hide-detail {
        display: none;
    }
    /* Tab bar scroll on mobile */
    .q-tabs__content {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch;
    }
    .q-tab {
        min-width: 64px !important;
        padding: 0 8px !important;
    }
    /* Filter bar wrapping */
    .filter-bar {
        flex-wrap: wrap !important;
    }
    .filter-bar .q-field {
        min-width: 120px !important;
        flex: 1 1 120px;
    }
}
/* Small screens: compact padding, horizontal scroll (no card-layout transform) */
@media (max-width: 480px) {
    .q-table__card {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch;
    }
    .q-table tbody td {
        padding: 6px 10px !important;
        font-size: 0.7rem !important;
        white-space: nowrap;
    }
    .q-table thead th {
        padding: 6px 10px !important;
        font-size: 0.65rem !important;
        white-space: nowrap;
    }
}

/* ============================================================================
   RESPONSIVE: Fluid Chart Heights (RESP-004)
   Charts adapt to viewport size
   ============================================================================ */
.plotly-graph-wrapper,
.plotly {
    min-height: 250px !important;
    max-height: 80vh !important;
    width: 100% !important;
}
@media (max-width: 768px) {
    .plotly-graph-wrapper,
    .plotly {
        min-height: 200px !important;
        max-height: 60vh !important;
    }
}

/* ============================================================================
   RESPONSIVE: Mobile Bottom Navigation (RESP-005)
   Bottom tab bar for mobile devices
   ============================================================================ */
.mobile-bottom-nav {
    display: none !important;
}
@media (max-width: 768px) {
    .mobile-bottom-nav {
        display: flex !important;
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        height: 56px;
        background: var(--theme-surface);
        border-top: 1px solid var(--theme-surface-border);
        justify-content: space-around;
        align-items: center;
        z-index: 1000;
        padding: 0 8px;
    }
    .mobile-bottom-nav-item {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 8px 12px;
        color: var(--theme-text-secondary);
        text-decoration: none;
        font-size: 0.65rem;
        min-width: 48px;
    }
    .mobile-bottom-nav-item.active {
        color: var(--theme-primary);
    }
    .mobile-bottom-nav-item .q-icon,
    .mobile-bottom-nav-item i.q-icon {
        font-size: 1.4rem !important;
        margin-bottom: 2px;
        display: block;
    }
    /* Adjust main content for bottom nav */
    .q-page-container, main {
        padding-bottom: 68px !important;
    }
}

/* ============================================================================
   ACCESSIBILITY: Reduced Motion Support (THEME-003)
   Respect prefers-reduced-motion for users with vestibular disorders
   ============================================================================ */
@media (prefers-reduced-motion: reduce) {
    /* Disable scanline effect for motion-sensitive users */
    body.terminal-mode::after {
        display: none !important;
    }
    /* Reduce or disable animations */
    *,
    *::before,
    *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
    }
    .kpi-card {
        animation: none !important;
    }
    /* Keep essential transitions but make them instant */
    .nav-item:hover,
    .nav-tile:hover,
    .kpi-card:hover {
        transition: none !important;
    }
}

@keyframes terminal-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.7; } }
"""


def _get_themed_css() -> str:
    """Generate complete CSS with :root variables for the current theme."""
    theme = get_current_theme()
    colors = get_current_colors()

    css_vars = f"""
:root {{
    --theme-page-bg: {theme["page_bg"]};
    --theme-surface: {theme["surface"]};
    --theme-surface-border: {theme["surface_border"]};
    --theme-surface-hover: {theme["surface_hover"]};
    --theme-text-primary: {theme["text_primary"]};
    --theme-text-secondary: {theme["text_secondary"]};
    --theme-text-muted: {theme["text_muted"]};
    --theme-primary: {theme["primary"]};
    --theme-primary-dark: {theme["primary_dark"]};
    --theme-divider: {theme["divider"]};
    --theme-color-success: {colors["success"]};
    --theme-color-error: {colors["error"]};
    --theme-color-warning: {colors["warning"]};
    --theme-color-info: {colors["info"]};
    --theme-color-gray: {colors["gray"]};
"""

    if _theme_state.is_terminal:
        css_vars += """
    --font-body: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    --font-mono: 'Fira Code', 'SF Mono', 'Consolas', 'Courier New', monospace;
    --card-radius: 4px;
    --card-shadow: 0 2px 8px rgba(0,0,0,0.4);
    --card-hover-shadow: 0 0 20px rgba(0,255,136,0.2), 0 2px 12px rgba(0,0,0,0.5);
    --tile-radius: 2px;
    --tile-shadow: 0 2px 4px rgba(0,0,0,0.3);
    --nav-radius: 0;
    --theme-primary-alpha: rgba(0,255,136,0.15);
    --info-box-bg: rgba(0,255,136,0.05);
    --code-bg: #000;
}}
body {
    --font-body: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    --font-mono: 'Fira Code', 'SF Mono', 'Consolas', 'Courier New', monospace !important;
}
"""
    else:
        css_vars += """
    --font-body: 'Manrope', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    --font-mono: 'JetBrains Mono', 'SF Mono', 'Consolas', 'Courier New', monospace;
    --card-radius: 8px;
    --card-shadow: 0 1px 3px rgba(0,0,0,0.1);
    --card-hover-shadow: 0 4px 12px rgba(0,0,0,0.15);
    --tile-radius: 8px;
    --tile-shadow: 0 1px 3px rgba(0,0,0,0.1);
    --nav-radius: 6px;
    --theme-primary-alpha: rgba(99,102,241,0.1);
    --info-box-bg: rgba(99,102,241,0.05);
    --code-bg: #1e293b;
}}
body::after { display: none; }
"""

    return css_vars + _PAGE_CSS_BASE


# ---------------------------------------------------------------------------
# Keyboard shortcuts HTML injection
# ---------------------------------------------------------------------------
_KEYBINDINGS_HTML = """
<script>
document.addEventListener('keydown', function(e) {
    if (e.altKey) {
        const nav = {
            'g': '/', 'b': '/backtest', 't': '/trades',
            'c': '/compare', 's': '/strategy', 'i': '/strategy-guide',
            'r': '/scans', 'p': '/pipeline',
            'l': '/paper_ledger', 'y': '/daily_summary',
            'u': '/symbols', 'd': '/data_quality',
        };
        const path = nav[e.key.toLowerCase()];
        if (path) { e.preventDefault(); window.location.href = path; }
    }
    if (e.key === '?' && !e.ctrlKey && !e.altKey) {
        const el = document.getElementById('shortcuts-help-btn');
        if (el) el.click();
    }
});
</script>
"""

_PLOTLY_RESIZE_GUARD_HTML = """
<script>
(() => {
    if (window.__cprPlotlyResizeGuardInstalled) return;
    window.__cprPlotlyResizeGuardInstalled = true;

    const RESIZE_ERR = 'Resize must be passed a displayed plot div element';

    const isDisplayed = (el) => {
        if (!el || !el.isConnected) return false;
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') return false;
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    };

    const wrapResize = (obj, key) => {
        if (!obj || typeof obj[key] !== 'function') return;
        const original = obj[key];
        if (original.__cprWrappedResize) return;

        const wrapped = function(gd, ...args) {
            if (!isDisplayed(gd)) {
                return Promise.resolve(gd);
            }
            try {
                const result = original.call(this, gd, ...args);
                if (result && typeof result.catch === 'function') {
                    return result.catch((err) => {
                        const msg = String(err && err.message ? err.message : err || '');
                        if (msg.includes(RESIZE_ERR)) return gd;
                        throw err;
                    });
                }
                return result;
            } catch (err) {
                const msg = String(err && err.message ? err.message : err || '');
                if (msg.includes(RESIZE_ERR)) return Promise.resolve(gd);
                throw err;
            }
        };

        wrapped.__cprWrappedResize = true;
        obj[key] = wrapped;
    };

    const install = () => {
        const p = window.Plotly;
        if (!p) return false;
        wrapResize(p, 'resize');
        if (p.Plots) wrapResize(p.Plots, 'resize');
        return true;
    };

    if (install()) return;

    const intervalId = window.setInterval(() => {
        if (install()) {
            window.clearInterval(intervalId);
        }
    }, 250);
    window.setTimeout(() => window.clearInterval(intervalId), 10000);
})();
</script>
"""


# ---------------------------------------------------------------------------
# Theme toggle
# ---------------------------------------------------------------------------
def toggle_theme_mode() -> None:
    """Switch between Terminal and Clean modes and reload the page."""
    _theme_state.toggle()
    announce_live_region("Theme mode updated.")
    ui.navigate.reload()


def announce_live_region(message: str) -> None:
    """Send a polite screen-reader announcement to the shared live region."""
    message_js = json.dumps(str(message))
    ui.run_javascript(
        "(() => {"
        " const el = document.querySelector('.live-region');"
        " if (!el) return;"
        " el.textContent = '';"
        " window.setTimeout(() => { el.textContent = "
        f"{message_js};"
        " }, 50);"
        "})();"
    )


def safe_timer(delay: float, callback: Callable, once: bool = True) -> ui.timer:
    """Create a ui.timer whose callback is guarded against deleted-client errors.

    NiceGUI timers that fire after the browser tab closes or the user navigates
    away raise ``RuntimeError('The client this element belongs to has been
    deleted.')``.  Wrapping the callback here prevents that error from
    propagating to the NiceGUI background-task exception handler.

    Both sync and async callbacks are supported.
    """
    if inspect.iscoroutinefunction(callback):

        async def _guarded_async() -> None:
            try:
                await callback()
            except RuntimeError:
                pass

        return ui.timer(delay, _guarded_async, once=once)
    else:

        def _guarded() -> None:
            try:
                callback()
            except RuntimeError:
                pass

        return ui.timer(delay, _guarded, once=once)


@contextmanager
def page_layout(title: str, icon: str = "bar_chart"):
    """Context manager: sidebar nav + header + content area.

    Includes accessibility features:
    - A11Y-010: Live region for dynamic updates
    - A11Y-011: Page title updates via JavaScript
    """
    theme = get_current_theme()
    is_terminal = _theme_state.is_terminal

    ui.dark_mode(is_terminal)
    ui.colors(primary=theme["primary"])
    ui.query("body").style(f"background-color: {theme['page_bg']}; color: {theme['text_primary']};")
    ui.add_head_html(_get_font_html())
    ui.add_css(_get_themed_css())
    ui.add_head_html(_KEYBINDINGS_HTML)
    ui.add_head_html(_PLOTLY_RESIZE_GUARD_HTML)

    # Update page title for accessibility (A11Y-011)
    ui.run_javascript(f'document.title = "CPR Pivot Lab — {title}"')

    # Make clickable table rows keyboard-focusable (P1-7 / WCAG 2.1.1)
    # Runs once and installs a MutationObserver so paginated rows stay focusable.
    ui.run_javascript(
        "(function(){"
        "const apply=()=>{"
        "document.querySelectorAll('.q-table tbody tr:not([tabindex])').forEach(tr=>{"
        "tr.setAttribute('tabindex','0');"
        "tr.setAttribute('role','row');"
        "tr.addEventListener('keydown',e=>{"
        "if(e.key==='Enter'||e.key===' '){e.preventDefault();tr.click();}"
        "});"
        "});"
        "};"
        "apply();"
        "if(!window.__cprRowFocusObserver){"
        "window.__cprRowFocusObserver=new MutationObserver(apply);"
        "window.__cprRowFocusObserver.observe(document.body,{childList:true,subtree:true});"
        "}"
        "})();"
    )

    # Skip navigation link (A11Y-008) — lets keyboard users bypass the sidebar
    ui.html('<a href="#main-content" class="skip-link">Skip to main content</a>')

    # Add live region for dynamic updates (A11Y-010)
    ui.element("div").props('aria-live="polite" aria-atomic="true"').style(
        "position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; "
        "overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;"
    ).classes("live-region")

    # sidebar cycle state
    _state = {"v": "expanded"}

    def _cycle_sidebar():
        if _state["v"] == "expanded":
            _state["v"] = "mini"
            drawer.props(add="mini")
        elif _state["v"] == "mini":
            _state["v"] = "hidden"
            drawer.hide()
        else:
            _state["v"] = "expanded"
            drawer.show()
            drawer.props(remove="mini")

    # -- header bar ----------------------------------------------------------
    with (
        ui.header()
        .classes("items-center px-4 py-0")
        .style(
            f"background: {theme['surface']}; "
            f"border-bottom: 1px solid {theme['surface_border']}; "
            "height: 48px;"
        )
    ):
        ui.button(icon="menu", on_click=_cycle_sidebar).props(
            'flat round dense aria-label="Toggle sidebar navigation"'
        ).classes("min-w-[44px] min-h-[44px]").style(f"color: {theme['text_secondary']};")

        with ui.row().classes("items-center gap-2 ml-2"):
            ui.icon("circle").classes("text-xs").style(
                f"color: {theme['primary']}; animation: terminal-pulse 2s infinite;"
            )
            ui.label("CPR_PIVOT_LAB").classes("text-sm font-semibold mono-font").style(
                f"color: {theme['text_primary']}; letter-spacing: 0.1em;"
            )
            ui.label("v1.0").classes("text-xs").style(
                f"color: {theme['text_muted']}; font-family: 'JetBrains Mono', monospace;"
            )

        ui.label(f"// {title.upper()}").classes("text-sm ml-4 mono-font").style(
            f"color: {theme['text_secondary']}; letter-spacing: 0.05em;"
        )

        ui.space()

        toggle_label = "TERMINAL" if not is_terminal else "CLEAN"
        ui.button(toggle_label, on_click=toggle_theme_mode).props(
            'flat dense aria-label="Switch theme mode"'
        ).classes("text-xs mono-font px-3 min-w-[44px] min-h-[44px]").style(
            f"color: {theme['text_secondary']}; "
            f"border: 1px solid {theme['surface_border']}; "
            "border-radius: 2px; padding: 4px 12px;"
        )

        # shortcuts button — has id so keyboard handler can click it
        dlg = _shortcuts_dialog()
        ui.button("?", on_click=dlg.open).props(
            'flat dense id=shortcuts-help-btn aria-label="Keyboard shortcuts help"'
        ).classes("mono-font text-xs px-2 min-w-[44px] min-h-[44px]").style(
            f"color: {theme['primary']}; "
            f"border: 1px solid {theme['surface_border']}; border-radius: 2px;"
        )

        ui.icon(icon).classes("text-lg").style(f"color: {theme['primary']};")

    # -- sidebar drawer ------------------------------------------------------
    with (
        ui.left_drawer(value=True, bordered=False)
        .props("width=240 mini-width=56 breakpoint=768")
        .classes("p-0")
        .style(
            f"background: {theme['surface']}; "
            f"border-right: 1px solid {theme['surface_border']}; "
            "transition: width 0.2s ease;"
        ) as drawer
    ):
        with ui.column().classes("px-4 py-3 gap-1 sidebar-logo"):
            with ui.row().classes("items-center gap-2"):
                ui.icon("candlestick_chart").classes("text-sm").style(f"color: {theme['primary']};")
                ui.label("CPR_LAB").classes("text-sm font-bold mono-font").style(
                    f"color: {theme['text_primary']}; letter-spacing: 0.1em;"
                )
            with ui.row().classes("items-center gap-2"):
                ui.label("BACKTEST").classes("text-xs mono-font").style(
                    f"color: {theme['text_muted']};"
                )
                ui.icon("circle").classes("text-xs").style(
                    f"color: {theme['primary']}; animation: terminal-pulse 2s infinite;"
                )

        ui.separator().classes("sidebar-logo").style(f"background: {theme['surface_border']};")

        # Detect active path
        try:
            from nicegui import context as _ctx

            _client_path = str(getattr(getattr(_ctx, "client", None), "path", "/"))
        except Exception as _:
            _client_path = "/"

        with ui.column().classes("py-2 gap-0 w-full"):
            # Home link (always visible, not in sections)
            is_home = _client_path == "/"
            home_cls = " nav-item-active" if is_home else ""
            with (
                ui.row()
                .classes(f"nav-item{home_cls} items-center gap-3 w-full nav-row mb-2")
                .props('aria-label="Navigate to Home" role="button" tabindex="0"')
                .on("click", lambda: ui.navigate.to("/"))
            ):
                ui.icon("home").classes("text-lg nav-icon").props('aria-label="Home"').style(
                    f"color: {theme['primary'] if is_home else theme['text_secondary']};"
                )
                ui.label("Home").classes("text-sm nav-label").props("role='presentation'")

            # Grouped navigation sections
            for section in NAV_SECTIONS:
                ui.label(section["label"]).classes(
                    "text-xs uppercase tracking-wide font-semibold px-4 pt-3 pb-1"
                ).style(f"color: {theme['text_muted']};").classes("sidebar-logo")
                for item in section["items"]:
                    is_active = (
                        _client_path == item["path"]
                        or (_client_path.startswith("/backtest") and item["path"] == "/backtest")
                        or (_client_path == "/trade_analytics" and item["path"] == "/trades")
                    )
                    active_cls = " nav-item-active" if is_active else ""
                    item_path = item["path"]
                    with (
                        ui.row()
                        .classes(f"nav-item{active_cls} items-center gap-3 w-full nav-row")
                        .props(
                            f'aria-label="Navigate to {item["label"]}" role="button" tabindex="0"'
                        )
                        .on("click", lambda p=item_path: ui.navigate.to(p))
                    ):
                        ui.icon(item["icon"]).classes("text-lg nav-icon").props(
                            f'aria-label="{item["label"]}"'
                        ).style(
                            f"color: {theme['primary'] if is_active else theme['text_secondary']};"
                        )
                        ui.label(item["label"]).classes("text-sm nav-label").props(
                            "role='presentation'"
                        )

    # -- main content --------------------------------------------------------
    with ui.element("main").props('id="main-content" role="main"').classes("w-full px-6 py-6"):
        yield

    # -- mobile bottom navigation (hidden on desktop via CSS) ----------------
    _mobile_nav_items = [
        {"label": "Home", "icon": "home", "path": "/"},
        {"label": "Backtest", "icon": "bar_chart", "path": "/backtest"},
        {"label": "Trades", "icon": "analytics", "path": "/trades"},
        {"label": "Paper", "icon": "receipt_long", "path": "/paper_ledger"},
        {"label": "Data", "icon": "verified", "path": "/data_quality"},
    ]
    with ui.element("nav").classes("mobile-bottom-nav").props('aria-label="Mobile navigation"'):
        for _nav in _mobile_nav_items:
            _is_active = _client_path == _nav["path"] or (
                _nav["path"] == "/backtest" and _client_path.startswith("/backtest")
            )
            _cls = "mobile-bottom-nav-item active" if _is_active else "mobile-bottom-nav-item"
            _path = _nav["path"]
            with (
                ui.element("a").classes(_cls).props(f'href="{_path}" aria-label="{_nav["label"]}"')
            ):
                ui.icon(_nav["icon"])
                ui.label(_nav["label"]).style("font-size: 0.6rem; line-height: 1;")


def kpi_card(
    title: str,
    value: str | float | int,
    subtitle: str | None = None,
    icon: str = "info",
    color: str | None = None,
) -> None:
    theme = get_current_theme()
    colors = get_current_colors()
    card_color = color or colors["primary"]
    tooltip_text = METRIC_GLOSSARY.get(title)
    with ui.column().classes("kpi-card gap-1"):
        with ui.row().classes("items-center gap-3"):
            ui.icon(icon).classes("text-2xl").style(f"color: {card_color};")
            title_label = (
                ui.label(title)
                .classes("text-xs uppercase tracking-wide font-medium")
                .style(f"color: {theme['text_secondary']};")
            )
            if tooltip_text:
                with title_label:
                    ui.tooltip(tooltip_text).classes("text-sm").style(
                        f"background: {theme['surface']}; color: {theme['text_primary']}; "
                        f"border: 1px solid {theme['surface_border']}; "
                        "max-width: 300px; padding: 8px 12px;"
                    )
        ui.label(str(value)).classes("text-2xl font-bold mt-1 kpi-value").style(
            f"color: {card_color};"
        )
        if subtitle:
            ui.label(subtitle).classes("text-xs").style(f"color: {theme['text_muted']};")


def kpi_grid(cards: list[dict[str, Any]], columns: int = 4) -> None:
    """Render a row of KPI cards. Each dict: title, value, subtitle, icon, color.

    Uses CSS auto-fill grid for responsive column collapse on mobile.
    The `columns` parameter is kept for API compatibility but layout is now
    driven by CSS `grid-template-columns: repeat(auto-fill, minmax(160px, 1fr))`.
    """
    with ui.grid(columns=columns).classes("w-full gap-4 mb-6 kpi-grid"):
        for card in cards:
            kpi_card(**card)


def kpi_section(title: str, cards: list[dict[str, Any]], columns: int = 4) -> None:
    """Render a titled section of KPI cards.

    Args:
        title: Section heading
        cards: List of card dictionaries for :func:`kpi_card`
        columns: Grid column count
    """
    theme = get_current_theme()
    ui.label(title).classes("text-xl font-semibold mb-4").style(f"color: {theme['text_primary']};")
    kpi_grid(cards, columns=columns)


# ---------------------------------------------------------------------------
# Primary action card (prominent CTA for home page)
# ---------------------------------------------------------------------------
def primary_action_card(
    title: str,
    description: str,
    icon: str,
    target: str,
    subtitle: str | None = None,
) -> None:
    """Render a prominent call-to-action card for the home page.

    Larger and more visually prominent than nav_card, used for primary actions.

    Includes keyboard handler for Enter/Space keys (A11Y-013).
    """
    theme = get_current_theme()

    # Keyboard handler for Enter/Space keys (A11Y-013)
    def _handle_key(e: dict[str, Any]) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.column()
        .classes("primary-action-card cursor-pointer")
        .props('tabindex="0" role="button"')
        .on("click", lambda: ui.navigate.to(target))
        .on("keydown", _handle_key)
    ):
        with ui.row().classes("items-center gap-4 mb-3"):
            ui.icon(icon).classes("text-4xl").style(f"color: {THEME['primary']};")
            ui.label(title).classes("text-2xl font-bold").style(f"color: {theme['text_primary']};")

        ui.label(description).classes("text-base leading-relaxed mb-3").style(
            f"color: {theme['text_secondary']};"
        )

        if subtitle:
            ui.label(subtitle).classes(
                "text-xs uppercase tracking-wide font-semibold px-3 py-1 rounded"
            ).style(
                f"background: {THEME['primary']}; color: {THEME['page_bg']}; display: inline-block;"
            )


# ---------------------------------------------------------------------------
# Navigation card (home page)
# ---------------------------------------------------------------------------
def nav_card(
    title: str,
    description: str,
    icon: str,
    target: str,
    color: str | None = None,
) -> None:
    """Render a navigation tile for the home page grid.

    Includes keyboard handler for Enter/Space keys (A11Y-013).
    """
    theme = get_current_theme()
    colors = get_current_colors()
    c = color or colors["info"]

    # Keyboard handler for Enter/Space keys (A11Y-013)
    def _handle_key(e: dict[str, Any]) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.column()
        .classes("nav-tile")
        .props('tabindex="0" role="button"')
        .on("click", lambda: ui.navigate.to(target))
        .on("keydown", _handle_key)
    ):
        with ui.row().classes("items-center gap-3 mb-2"):
            ui.icon(icon).classes("text-2xl").style(f"color: {c};")
            ui.label(title).classes("text-base font-semibold").style(
                f"color: {theme['text_primary']};"
            )
        ui.label(description).classes("text-sm leading-relaxed").style(
            f"color: {theme['text_secondary']};"
        )


# ---------------------------------------------------------------------------
# Plotly chart theme
# ---------------------------------------------------------------------------
def apply_chart_theme(fig: Any) -> None:
    """Apply Terminal/Clean theme to a Plotly figure (mutates in-place)."""
    is_terminal = _theme_state.is_terminal
    theme = get_current_theme()
    mono_font = "Fira Code, monospace" if is_terminal else "JetBrains Mono, monospace"
    body_font = "IBM Plex Sans, sans-serif" if is_terminal else "Manrope, sans-serif"

    axis_style = dict(
        gridcolor=theme["surface_border"],
        zerolinecolor=theme["surface_border"],
        linecolor=theme["surface_border"],
        tickfont=dict(color=theme["text_secondary"], family=mono_font, size=10),
    )

    fig.update_layout(
        paper_bgcolor=theme["surface"],
        plot_bgcolor=theme["page_bg"],
        font_color=theme["text_primary"],
        font_family=mono_font,
        title_font=dict(color=theme["text_primary"], size=14, family=body_font),
        margin=dict(l=40, r=20, t=40, b=40),
        xaxis=axis_style,
        yaxis=axis_style,
        legend=dict(
            bgcolor=theme["surface"],
            font_color=theme["text_secondary"],
            bordercolor=theme["surface_border"],
            borderwidth=1,
        ),
        hoverlabel=dict(
            bgcolor=theme["surface_border"],
            font_color=theme["text_primary"],
            font_size=11,
        ),
        hovermode="x unified",
        transition_duration=0,
    )


# ---------------------------------------------------------------------------
# Utility widgets
# ---------------------------------------------------------------------------
def divider() -> None:
    theme = get_current_theme()
    ui.separator().classes("my-6").style(f"background: {theme['surface_border']};")


def info_box(text: str, color: str = "blue") -> None:
    colors = get_current_colors()
    palette = {
        "blue": (colors["info"], "rgba(99,102,241,0.08)"),
        "green": (colors["success"], "rgba(34,197,94,0.08)"),
        "yellow": (colors["warning"], "rgba(245,158,11,0.08)"),
        "red": (colors["error"], "rgba(239,68,68,0.08)"),
    }
    accent, bg = palette.get(color, palette["blue"])
    with (
        ui.row()
        .classes("info-box items-center gap-3 mb-4")
        .style(f"background: {bg}; border-color: {accent}66;")
    ):
        ui.icon("lightbulb").classes("text-lg").style(f"color: {accent};")
        ui.label(text).classes("text-sm").style(f"color: {accent};")


def empty_state(
    title: str,
    message: str,
    action_label: str | None = None,
    action_callback: Any = None,
    icon: str = "inbox",
) -> None:
    theme = get_current_theme()
    with ui.column().classes("items-center justify-center py-16 gap-4 w-full"):
        ui.icon(icon).classes("text-6xl opacity-50").style(f"color: {theme['text_muted']};")
        ui.label(title).classes("text-xl font-semibold").style(f"color: {theme['text_primary']};")
        ui.label(message).classes("text-center max-w-md").style(
            f"color: {theme['text_secondary']};"
        )
        if action_label and action_callback:
            ui.button(action_label, on_click=action_callback).props("push color=primary").classes(
                "mt-4"
            ).props(f'aria-label="{action_label}"')


def copyable_code(text: str, language: str = "bash") -> None:
    """Code block with copy button for convenience.

    Args:
        text: Code content to display
        language: Syntax highlighting language (default: bash)
    """
    theme = get_current_theme()
    colors = get_current_colors()

    with ui.row().classes("w-full items-stretch gap-2 mb-3"):
        with (
            ui.card()
            .classes("flex-1 code-block relative overflow-hidden")
            .style(
                f"background: {theme['surface']}; "
                f"border: 1px solid {theme['surface_border']}; "
                f"border-radius: 4px;"
            )
        ):
            ui.code(text, language=language).classes("w-full text-xs")

        # Copy button with icon
        async def _copy():
            await ui.run_javascript(f"navigator.clipboard.writeText({text!a})")
            ui.notify("Copied!", type="positive", position="top", timeout=1000)

        btn = (
            ui.button(on_click=_copy)
            .props("flat dense round aria-label='Copy to clipboard'")
            .classes("self-start")
            .tooltip("Copy to clipboard")
        )
        with btn:
            ui.icon("content_copy").classes("text-sm").style(f"color: {colors['info']}")


def value_label(value: str, is_positive: bool | None = None) -> None:
    """Display a value with accessibility support.

    Adds visual indicators (↑/↓ via CSS) and proper ARIA labels for screen readers.
    Use this instead of raw ui.label() for financial values.

    Args:
        value: Formatted value string (e.g., "₹12,000" or "2.5%")
        is_positive: True for gain, False for loss, None for neutral

    Example:
        value_label("₹12,000", is_positive=True)  # Shows "↑ ₹12,000" with ARIA "Positive: ₹12,000"
        value_label("₹8,500", is_positive=False)  # Shows "↓ ₹8,500" with ARIA "Negative: ₹8,500"
    """
    if is_positive is True:
        css_class = "value-positive"
        aria_label = f"Positive: {value}"
    elif is_positive is False:
        css_class = "value-negative"
        aria_label = f"Negative: {value}"
    else:
        css_class = "value-neutral"
        aria_label = value
    ui.label(value).classes(f"tabular-nums {css_class}").props(f'aria-label="{aria_label}"')


def page_header(
    title: str,
    subtitle: str | None = None,
    kpi_row: list[dict] | None = None,
    level: int = 1,
) -> None:
    """Consistent page header with optional KPIs.

    Uses semantic HTML heading elements for accessibility (A11Y-012).

    Args:
        title: Page title
        subtitle: Optional subtitle
        kpi_row: Optional KPI cards
        level: Heading level (1 or 2)
    """
    theme = get_current_theme()
    with ui.column().classes("mb-8 w-full"):
        with ui.column().classes("gap-1 mb-6"):
            # Use semantic heading elements for accessibility (A11Y-012)
            heading_tag = f"h{level}"
            ui.html(
                f"<{heading_tag} class='text-2xl font-bold' style='color: {theme['text_primary']}; margin: 0;'>{title}</{heading_tag}>"
            )
            if subtitle:
                ui.html(
                    f"<p class='text-sm' style='color: {theme['text_secondary']}; margin: 4px 0 0;'>{subtitle}</p>"
                )

        if kpi_row:
            kpi_grid(kpi_row, columns=len(kpi_row))


def export_button(
    data: Any,
    filename: str = "export.csv",
    label: str = "Download CSV",
) -> None:
    theme = get_current_theme()
    if data is None:
        return
    if hasattr(data, "is_empty") and data.is_empty():
        return
    if hasattr(data, "write_csv"):
        csv_content = data.write_csv()
    elif hasattr(data, "to_csv"):
        csv_content = data.to_csv(index=False)
    else:
        return

    def _download():
        ui.download(csv_content.encode("utf-8"), filename=filename)

    ui.button(label, icon="download", on_click=_download).props(
        "flat aria-label='Download CSV'"
    ).classes("text-sm").style(
        f"color: {theme['text_secondary']}; "
        f"border: 1px solid {theme['surface_border']}; "
        "border-radius: 4px; padding: 6px 14px;"
    )


def export_menu(data: Any, filename_base: str, label: str = "Export") -> None:
    if data is None:
        return
    if hasattr(data, "is_empty") and data.is_empty():
        return
    if hasattr(data, "write_csv"):
        csv_content = data.write_csv()
    elif hasattr(data, "to_csv"):
        csv_content = data.to_csv(index=False)
    else:
        return

    with ui.button(label, icon="download").props("flat aria-label='Export data'"):
        with ui.menu().props("anchor=top-end"):
            ui.menu_item(
                "Download CSV",
                lambda: ui.download(csv_content.encode("utf-8"), filename=f"{filename_base}.csv"),
            )


@contextmanager
def loading_spinner():
    """Show loading spinner during async operations.

    Includes aria-live region for screen readers (A11Y-010).
    """
    announce_live_region("Loading...")
    spinner = (
        ui.spinner("dots")
        .classes("mt-8")
        .props('role="status" aria-live="polite" aria-label="Loading..."')
    )
    try:
        yield
    finally:
        announce_live_region("Loading complete.")
        spinner.delete()


# ---------------------------------------------------------------------------
# Paginated table with native Quasar sorting/pagination
# ---------------------------------------------------------------------------
def paginated_table(
    rows: list,
    columns: list,
    page_size: int = 20,
    row_key: str | None = None,
    on_row_click: Any = None,
    *,
    sort_by: str | None = None,
    descending: bool = False,
    mobile_hidden_cols: set[str] | None = None,
) -> None:
    """Render a paginated Quasar QTable with responsive mobile support.

    Args:
        sort_by: Column name to sort by initially. Defaults to first column.
        descending: If True, initial sort is descending (newest first for dates).
        mobile_hidden_cols: Set of column field names to hide on mobile (<768px).
    """
    theme = get_current_theme()
    if not rows:
        ui.label("No data to display.").style(f"color: {theme['text_muted']};")
        return

    hidden = mobile_hidden_cols or set()
    resolved_columns = []
    for col in columns:
        c = {**col, "sortable": col.get("sortable", True)}
        display_format = c.pop("format", None)
        if display_format is not None:
            c["display_format"] = display_format
        if col.get("name") in hidden:
            c["classes"] = (col.get("classes", "") + " hide-mobile").strip()
        resolved_columns.append(c)

    initial_sort = sort_by or (resolved_columns[0]["name"] if resolved_columns else None)
    tbl = ui.table(
        columns=resolved_columns,
        rows=rows,
        row_key=row_key or "id",
        pagination={
            "rowsPerPage": page_size,
            "sortBy": initial_sort,
            "descending": descending,
        },
    ).classes("w-full")
    tbl.props('flat bordered separator=horizontal role="table"')
    set_table_mobile_labels(tbl, resolved_columns)

    if on_row_click:

        async def _handle_row_click(e) -> None:
            row_payload = extract_row_payload(e)
            if not row_payload:
                return
            # Check if client is still connected before proceeding
            # Prevents "Client has been deleted" errors on navigation/timeout
            try:
                current_client = ui.context.client
            except (AttributeError, RuntimeError):
                # No client context or context already gone
                return
            if not current_client.has_socket_connection:
                return
            result = on_row_click(row_payload)
            if inspect.isawaitable(result):
                # Check connection again before awaiting long-running handler
                if not ui.context.client.has_socket_connection:
                    return
                await result

        tbl.on("row-click", _handle_row_click)


def _vue_display_expr(fmt: str | None) -> str:
    """Return a Vue JS expression for formatting ``props.value`` by format type.

    Format types:
      ``"currency"``  — ₹X,XXX  (0 decimals, rupee prefix, comma grouping)
      ``"pct"``       — XX.X%   (1 decimal + percent sign)
      ``"pct:N"``     — XX.XX%  (N decimals + percent sign)
      ``"int"``       — X,XXX   (0 decimals, comma grouping)
      ``"decimal:N"`` — XX.XX   (N decimals)
    """
    if not fmt:
        return "props.value"
    if fmt == "currency":
        return (
            "props.value != null "
            "? '₹' + Number(props.value).toLocaleString('en-IN', "
            "{maximumFractionDigits:0,useGrouping:true}) "
            ": ''"
        )
    if fmt == "pct":
        return "props.value != null ? Number(props.value).toFixed(1) + '%' : ''"
    if fmt.startswith("pct:"):
        n = fmt.split(":", 1)[1]
        return "props.value != null ? Number(props.value).toFixed(" + n + ") + '%' : ''"
    if fmt == "int":
        return "props.value != null ? Number(props.value).toLocaleString('en-IN') : ''"
    if fmt.startswith("decimal:"):
        n = fmt.split(":", 1)[1]
        return (
            "props.value != null "
            "? Number(props.value).toLocaleString('en-IN', "
            "{minimumFractionDigits:" + n + ",maximumFractionDigits:" + n + "}) "
            ": ''"
        )
    return "props.value"


def set_table_mobile_labels(tbl: Any, columns: list[dict[str, Any]]) -> None:
    """Attach mobile-friendly table cell templates with ``data-label`` values.

    Uses NiceGUI's ``add_slot("body-cell-{name}", ...)`` API with Quasar slot props.
    When a column definition includes a ``"display_format"`` key (or legacy
    ``"format"`` key), the slot renders the raw
    numeric value with locale-aware formatting (currency, percent, integer, etc.).
    This keeps Quasar's native sort comparing raw numbers instead of formatted strings.

    Custom slots added after this call (e.g. direction coloring) override as expected.
    """
    for col in columns:
        if "format" in col and "display_format" not in col:
            col["display_format"] = col.pop("format")
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        label = html.escape(str(col.get("label") or name))
        extra_class = str(col.get("classes") or "").strip()
        if extra_class:
            class_val = "'text-' + props.col.align + ' " + extra_class + "'"
        else:
            class_val = "'text-' + props.col.align"
        display_expr = _vue_display_expr(col.get("display_format") or col.get("format"))
        tbl.add_slot(
            "body-cell-" + name,
            '<td data-label="' + label + '" :class="' + class_val + '">'
            '<span v-html="' + display_expr + '"></span></td>',
        )


def extract_row_payload(event: Any) -> dict[str, Any]:
    """Extract row payload from NiceGUI/Quasar table row-click events across versions."""
    args = getattr(event, "args", None)

    def _is_probable_row(payload: dict[str, Any]) -> bool:
        if not payload:
            return False
        row_markers = {
            "run_id",
            "strategy",
            "symbol",
            "date",
            "trade_date",
            "entry_time",
            "exit_time",
            "idx",
        }
        if row_markers.intersection(payload.keys()):
            return True
        pointer_event_keys = {
            "altKey",
            "ctrlKey",
            "metaKey",
            "shiftKey",
            "button",
            "buttons",
            "clientX",
            "clientY",
            "offsetX",
            "offsetY",
            "pageX",
            "pageY",
            "screenX",
            "screenY",
            "type",
            "isTrusted",
        }
        return not set(payload.keys()).issubset(pointer_event_keys)

    if isinstance(args, dict):
        for key in ("row", "record", "item", "data"):
            row = args.get(key)
            if isinstance(row, dict):
                return row
        for val in args.values():
            if isinstance(val, dict) and _is_probable_row(val):
                return val
        return args if _is_probable_row(args) else {}

    if isinstance(args, list | tuple):
        # Common shape: [mouse_event_dict, row_dict]
        if len(args) >= 2 and isinstance(args[1], dict) and _is_probable_row(args[1]):
            return args[1]
        for item in args:
            if isinstance(item, dict) and _is_probable_row(item):
                return item
        for item in reversed(args):
            if isinstance(item, dict):
                return item
        return {}

    return {}


# ---------------------------------------------------------------------------
# Trade filter bar — consistent filter UI across tabs
# ---------------------------------------------------------------------------
def trade_filter_bar(
    filters: dict,
    on_change: Callable,
    *,
    show_date: bool = True,
    date_options: list[str] | None = None,
    show_symbol: bool = True,
    show_direction: bool = True,
    show_exit: bool = True,
    exit_options: list[str] | None = None,
) -> None:
    """Reusable trade filter bar with date, symbol, direction, and exit reason.

    Args:
        filters: Mutable dict holding current filter state. Keys: date, symbol, direction, exit_reason.
        on_change: Callable invoked after any filter changes (triggers parent refresh).
        show_date: Show date dropdown filter.
        date_options: List of date strings for the dropdown. Pass None to auto-build from rows.
        show_symbol: Show symbol text input.
        show_direction: Show direction (LONG/SHORT/ALL) dropdown.
        show_exit: Show exit reason dropdown.
        exit_options: List of exit reason strings. Pass None to skip.
    """
    colors = get_current_colors()

    def _apply(key: str, value: str) -> None:
        filters[key] = value
        on_change()

    def _clear() -> None:
        filters.update(date="", symbol="", direction="ALL", exit_reason="ALL")
        on_change()

    with ui.row().classes("w-full gap-3 mb-3 items-end flex-wrap filter-bar"):
        if show_date:
            opts = date_options if date_options is not None else ["ALL"]
            ui.select(
                opts,
                value=filters.get("date") or "ALL",
                label="Date",
                on_change=lambda e: _apply(
                    "date", "" if str(e.value or "ALL") == "ALL" else str(e.value or "")
                ),
            ).props("dense outlined").classes("w-44")
        if show_symbol:
            ui.input(
                "Symbol",
                value=filters.get("symbol", ""),
                on_change=lambda e: _apply("symbol", str(e.value or "")),
            ).props("dense outlined clearable").classes("w-32")
        if show_direction:
            ui.select(
                ["ALL", "LONG", "SHORT"],
                value=filters.get("direction", "ALL"),
                label="Direction",
                on_change=lambda e: _apply("direction", str(e.value or "ALL")),
            ).props("dense outlined").classes("w-32")
        if show_exit and exit_options is not None:
            ui.select(
                exit_options,
                value=filters.get("exit_reason", "ALL"),
                label="Exit",
                on_change=lambda e: _apply("exit_reason", str(e.value or "ALL")),
            ).props("dense outlined").classes("w-40")
        ui.button(
            "Clear",
            icon="clear",
            on_click=_clear,
        ).props("flat dense aria-label='Clear all filters'").style(f"color: {colors['primary']};")


def apply_trade_filters(rows: list[dict], filters: dict) -> list[dict]:
    """Filter a list of trade row dicts using the standard filter dict."""
    filtered = rows
    if filters.get("date"):
        filtered = [r for r in filtered if r.get("date") == filters["date"]]
    if filters.get("symbol"):
        s = filters["symbol"].upper()
        filtered = [r for r in filtered if s in str(r.get("symbol", "")).upper()]
    if filters.get("direction", "ALL") != "ALL":
        filtered = [r for r in filtered if r.get("dir") == filters["direction"]]
    if filters.get("exit_reason", "ALL") != "ALL":
        filtered = [r for r in filtered if r.get("exit_reason") == filters["exit_reason"]]
    return filtered


# ---------------------------------------------------------------------------
# Parameter display — elegant grouped card + header strip
# ---------------------------------------------------------------------------
def param_header_strip(params: dict) -> None:
    """Render key strategy parameters as compact inline badges.

    Shows the most important 6-8 params as colored pills in the run header
    for instant recognition without expanding the full card.
    """
    theme = get_current_theme()
    colors = get_current_colors()

    direction = str(params.get("direction_filter") or "BOTH")
    direction_color = (
        colors["success"]
        if direction == "LONG"
        else colors["error"]
        if direction == "SHORT"
        else colors["info"]
    )

    pills: list[tuple[str, str, str]] = [
        (
            "Strategy",
            str(params.get("strategy", "CPR_LEVELS")).split("|")[0].strip(),
            colors["primary"],
        ),
        ("Dir", direction, direction_color),
    ]

    min_price = float(params.get("min_price") or 0.0)
    if min_price > 0:
        pills.append(("Min Price", f"₹{min_price:g}", theme["text_secondary"]))

    cpr_raw = params.get("cpr_levels_config") or params.get("cpr_levels")
    cpr_cfg = cpr_raw if isinstance(cpr_raw, dict) else {}
    if isinstance(cpr_cfg, dict):
        atr_val = float(cpr_cfg.get("cpr_min_close_atr") or 0.0)
        if atr_val > 0:
            pills.append(("ATR Gate", f"{atr_val:g}", colors["info"]))
        if cpr_cfg.get("narrowing_filter"):
            pills.append(("Narrow", "ON", colors["success"]))

    skip_rvol = _as_bool(params.get("skip_rvol_check") or params.get("skip_rvol"))
    if skip_rvol:
        pills.append(("RVOL", "OFF", theme["text_muted"]))
    else:
        rvol = float(params.get("rvol_threshold") or params.get("rvol") or 1.0)
        pills.append(("RVOL", f"{rvol:g}", colors["warning"]))

    risk_based = _as_bool(params.get("risk_based_sizing") or params.get("legacy_sizing"))
    if risk_based:
        pills.append(("Sizing", "Risk", colors["info"]))

    with ui.row().classes("w-full gap-2 flex-wrap items-center"):
        for label, value, color in pills:
            ui.html(
                f'<span style="display:inline-flex;align-items:center;gap:4px;'
                f"background:{theme['surface_hover']};border:1px solid {theme['surface_border']};"
                f"border-radius:4px;padding:3px 10px;font-size:0.7rem;"
                f'font-family:var(--font-mono);letter-spacing:0.03em;">'
                f'<span style="color:{theme["text_muted"]};font-weight:500;">{label}</span>'
                f'<span style="color:{color};font-weight:600;">{value}</span>'
                f"</span>"
            )


def param_detail_card(params: dict) -> None:
    """Render an elegant expandable parameter card grouped by category.

    Groups params into: Strategy Config, Entry Rules, Risk Management, Filters.
    Shows human-readable labels instead of raw JSON. Advanced params collapsed.
    """
    theme = get_current_theme()
    colors = get_current_colors()

    cpr_raw = params.get("cpr_levels_config") or params.get("cpr_levels") or {}
    cpr_cfg = cpr_raw if isinstance(cpr_raw, dict) else {}
    fbr_raw = params.get("fbr_config") or {}
    fbr_cfg = fbr_raw if isinstance(fbr_raw, dict) else {}
    vcpr_raw = params.get("virgin_cpr_config") or {}
    vcpr_cfg = vcpr_raw if isinstance(vcpr_raw, dict) else {}

    groups: list[tuple[str, list[tuple[str, str]]]] = []

    # Strategy
    groups.append(
        (
            "Strategy Config",
            [
                ("Strategy", str(params.get("strategy", "—")).split("|")[0].strip()),
                ("Direction", str(params.get("direction_filter") or "BOTH")),
                ("Execution Mode", str(params.get("execution_mode") or "BACKTEST")),
                ("Commission Model", str(params.get("commission_model") or "zerodha")),
            ],
        )
    )

    # Entry Rules
    entry_rows = [
        ("CPR Percentile", f"{float(params.get('cpr_percentile') or 33):g}"),
        ("CPR Min Close ATR", f"{float(cpr_cfg.get('cpr_min_close_atr') or 0):g}"),
        ("Narrowing Filter", "ON" if cpr_cfg.get("narrowing_filter") else "OFF"),
        ("Buffer Pct", f"{float(cpr_cfg.get('buffer_pct') or 0):g}%"),
        (
            "Failure Window",
            f"{int(fbr_cfg.get('failure_window') or params.get('failure_window') or 8)}",
        ),
        (
            "FBR Direction",
            str(fbr_cfg.get("fbr_setup_filter") or params.get("fbr_setup_filter") or "—"),
        ),
        ("VCPR RR Ratio", f"{float(vcpr_cfg.get('rr_ratio') or 0):g}"),
    ]
    groups.append(("Entry Rules", entry_rows))

    # Risk Management
    groups.append(
        (
            "Risk Management",
            [
                ("RR Ratio", f"{float(params.get('rr_ratio') or 1.0):g}"),
                ("Min Effective RR", f"{float(params.get('min_effective_rr') or 2.0):g}"),
                ("Max SL ATR Ratio", f"{float(params.get('max_sl_atr_ratio') or 2.0):g}"),
                ("Breakeven R", f"{float(params.get('breakeven_r') or 1.0):g}"),
                (
                    "Risk-Based Sizing",
                    "ON"
                    if _as_bool(params.get("risk_based_sizing") or params.get("legacy_sizing"))
                    else "OFF",
                ),
                ("Max Positions", str(int(params.get("max_positions") or 10))),
            ],
        )
    )

    # Filters
    groups.append(
        (
            "Filters",
            [
                ("Min Price", f"₹{float(params.get('min_price') or 0):g}"),
                (
                    "RVOL Threshold",
                    "OFF"
                    if _as_bool(params.get("skip_rvol_check") or params.get("skip_rvol"))
                    else f"{float(params.get('rvol_threshold') or params.get('rvol') or 1.0):g}",
                ),
                ("Max Gap Pct", f"{float(params.get('max_gap_pct') or 1.5):g}%"),
                ("OR ATR Min", f"{float(params.get('or_atr_min') or 0.3):g}"),
                ("OR ATR Max", f"{float(params.get('or_atr_max') or 2.5):g}"),
                ("Time Exit", str(params.get("time_exit") or "15:15")),
            ],
        )
    )

    # Advanced params — anything not shown in groups
    shown_keys = {
        "strategy",
        "direction_filter",
        "execution_mode",
        "commission_model",
        "cpr_percentile",
        "cpr_levels_config",
        "cpr_levels",
        "fbr_config",
        "failure_window",
        "fbr_setup_filter",
        "virgin_cpr_config",
        "rr_ratio",
        "min_effective_rr",
        "max_sl_atr_ratio",
        "breakeven_r",
        "risk_based_sizing",
        "legacy_sizing",
        "max_positions",
        "min_price",
        "skip_rvol_check",
        "skip_rvol",
        "rvol_threshold",
        "rvol",
        "max_gap_pct",
        "or_atr_min",
        "or_atr_max",
        "time_exit",
    }
    advanced_rows: list[tuple[str, str]] = []
    for key, value in _flatten_params(params):
        if any(
            key.startswith(prefix)
            for prefix in ("cpr_levels_config.", "cpr_levels.", "fbr_config.", "virgin_cpr_config.")
        ):
            continue
        if key not in shown_keys:
            display_val = (
                json.dumps(value, default=str) if isinstance(value, dict | list) else str(value)
            )
            advanced_rows.append((key, display_val))

    with ui.expansion("Run Parameters", icon="settings").classes("w-full mb-4"):
        if not params:
            ui.label("No parameters stored for this run.").classes("text-sm").style(
                f"color: {theme['text_muted']};"
            )
            ui.label("Re-run with --save to capture parameters.").classes("text-xs").style(
                f"color: {theme['text_muted']};"
            )
            return

        for group_title, group_rows in groups:
            with ui.column().classes("mb-4"):
                ui.label(group_title).classes(
                    "text-xs uppercase tracking-wide font-semibold mb-2"
                ).style(f"color: {colors['primary']};")
                with (
                    ui.row()
                    .classes("w-full gap-x-6 gap-y-2 flex-wrap")
                    .style(
                        f"border-bottom: 1px solid {theme['surface_border']};padding-bottom: 8px;"
                    )
                ):
                    for label, value in group_rows:
                        val_color = (
                            colors["success"]
                            if value == "ON"
                            else theme["text_muted"]
                            if value == "OFF"
                            else theme["text_primary"]
                        )
                        with ui.row().classes("items-center gap-2"):
                            ui.label(label).classes("text-xs").style(
                                f"color: {theme['text_secondary']};"
                            )
                            ui.label(value).classes("text-xs font-semibold mono-font").style(
                                f"color: {val_color};"
                            )

        if advanced_rows:
            with ui.expansion(f"Advanced ({len(advanced_rows)} params)", icon="tune").classes(
                "w-full mt-2"
            ):
                for label, value in advanced_rows:
                    with ui.row().classes("items-center gap-2 mb-1"):
                        ui.label(label).classes("text-xs").style(f"color: {theme['text_muted']};")
                        ui.label(value).classes("text-xs mono-font").style(
                            f"color: {theme['text_secondary']};"
                        )


# ---------------------------------------------------------------------------
# Strategy + exit badges
# ---------------------------------------------------------------------------
def _get_strat_color(strategy: str) -> str:
    """Get strategy color from current theme (centralized for consistency)."""
    colors = get_current_colors()
    strat_colors = {
        "CPR_LEVELS": colors.get("strat_cpr_levels", "#2563eb"),
        "FBR": colors.get("strat_fbr", "#10b981"),
        "VIRGIN_CPR": colors.get("strat_virgin_cpr", "#8b5cf6"),
    }
    return strat_colors.get(strategy, colors.get("strat_default", "#64748b"))


_EXIT_COLORS: dict[str, str] = {
    "TARGET": "#10b981",
    "INITIAL_SL": "#ef4444",
    "BREAKEVEN_SL": "#f59e0b",
    "TRAILING_SL": "#f97316",
    "TIME": "#64748b",
    "REVERSAL": "#8b5cf6",
    "CANDLE_EXIT": "#64748b",
}

_STRAT_LABELS: dict[str, str] = {
    "CPR_LEVELS": "CPR",
    "FBR": "FBR",
}

_EXIT_LABELS: dict[str, str] = {
    "TARGET": "Target",
    "INITIAL_SL": "Init SL",
    "BREAKEVEN_SL": "BE SL",
    "TRAILING_SL": "Trail SL",
    "TIME": "Time",
    "REVERSAL": "Reversal",
    "CANDLE_EXIT": "Candle",
}


def strat_badge(strategy: str) -> str:
    color = _get_strat_color(strategy)
    label = _STRAT_LABELS.get(strategy, strategy)
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f"border-radius:3px;font-size:0.75rem;font-weight:600;"
        f'font-family:monospace;letter-spacing:0.05em">{label}</span>'
    )


def exit_badge(reason: str) -> str:
    color = _EXIT_COLORS.get(reason, "#64748b")
    label = _EXIT_LABELS.get(reason, reason)
    tooltip = EXIT_GLOSSARY.get(reason, "")
    # Include both title (for visual tooltip) and aria-label (for screen readers)
    title_attr = f' title="{tooltip}"' if tooltip else ""
    aria_label = f' aria-label="{label}: {tooltip}"' if tooltip else f' aria-label="{label}"'
    return (
        f'<span{title_attr}{aria_label} style="background:{color};color:#fff;padding:2px 8px;'
        f"border-radius:3px;font-size:0.7rem;font-weight:600;cursor:help;"
        f'font-family:monospace">{label}</span>'
    )


def format_drawdown_pct(pct: float) -> str:
    """Format drawdown percentage with precision suitable for small values.

    Args:
        pct: Drawdown percentage (typically negative)

    Returns:
        Formatted string like "5.2%" or "0.15%" with appropriate precision
    """
    val = abs(float(pct))
    if val >= 1.0:
        return f"{val:.1f}%"
    if val >= 0.1:
        return f"{val:.2f}%"
    return f"{val:.4f}%"


# ---------------------------------------------------------------------------
# Session state utilities — for theme toggle persistence
# ---------------------------------------------------------------------------
async def save_session_state(key: str, value: str) -> None:
    """Save a value to browser sessionStorage for cross-reload persistence."""
    key_js = json.dumps(str(key))
    value_js = json.dumps(str(value))
    await ui.run_javascript(f"sessionStorage.setItem({key_js}, {value_js});")


async def restore_session_state(key: str, default: str = "") -> str:
    """Restore a value from browser sessionStorage."""
    result = await ui.run_javascript(f"sessionStorage.getItem('{key}') || '{default}'", timeout=2.0)
    return str(result or default)


# ---------------------------------------------------------------------------
# Shared utility functions
# ---------------------------------------------------------------------------


def _flatten_params(params: dict, prefix: str = "") -> list[tuple[str, object]]:
    """Flatten a nested dict into (key, value) pairs for display."""
    flat: list[tuple[str, object]] = []
    for key, value in params.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.extend(_flatten_params(value, full_key))
        else:
            flat.append((full_key, value))
    return flat


def _as_bool(value: object) -> bool:
    """Convert various truthy representations to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


# ---------------------------------------------------------------------------
# Accessibility helpers
# ---------------------------------------------------------------------------
def accessible_heading(text: str, level: int = 1) -> None:
    """Render a semantically accessible heading with proper ARIA attributes.

    Args:
        text: Heading text content
        level: Heading level (1-6), defaults to 1
    """
    theme = get_current_theme()
    size_classes = {
        1: "text-4xl font-bold",
        2: "text-3xl font-bold",
        3: "text-2xl font-bold",
        4: "text-xl font-semibold",
        5: "text-lg font-semibold",
        6: "text-base font-semibold",
    }
    cls = size_classes.get(level, "text-base font-semibold")
    ui.label(text).classes(cls).props(f'role="heading" aria-level="{level}"').style(
        f"color: {theme['text_primary']};"
    )


# ---------------------------------------------------------------------------
# P/L cell helper — green/red coloring for table cells
# ---------------------------------------------------------------------------
def pnl_cell(value: float | int, prefix: str = "₹", suffix: str = "") -> str:
    """Format a P/L value as an HTML span with green/red coloring and ARIA label.

    Returns an HTML string for use in table rows where the column
    is configured with ``"html": True``.
    """
    colors = get_current_colors()
    color = colors["success"] if value >= 0 else colors["error"]
    sign = "+" if value > 0 else ""
    formatted = (
        f"{prefix}{sign}{value:,.0f}{suffix}" if prefix == "₹" else f"{sign}{value:.4f}{suffix}"
    )
    aria_label = f"{'Gain' if value >= 0 else 'Loss'}: {formatted}"
    arrow = "↑" if value > 0 else ("↓" if value < 0 else "")
    return (
        f'<span aria-label="{aria_label}" '
        f'style="color:{color};font-weight:600;font-family:var(--font-mono)">'
        f"{arrow} {formatted}</span>"
    )


def pnl_pct_cell(value: float) -> str:
    """Format a P/L % value as an HTML span with green/red coloring and ARIA label."""
    colors = get_current_colors()
    color = colors["success"] if value >= 0 else colors["error"]
    sign = "+" if value > 0 else ""
    aria_label = f"{'Gain' if value >= 0 else 'Loss'}: {sign}{value:.2f}%"
    arrow = "↑" if value > 0 else ("↓" if value < 0 else "")
    return (
        f'<span aria-label="{aria_label}" '
        f'style="color:{color};font-weight:600;font-family:var(--font-mono)">'
        f"{arrow} {sign}{value:.2f}%</span>"
    )


# ---------------------------------------------------------------------------
# Keyboard shortcuts dialog
# ---------------------------------------------------------------------------
def _shortcuts_dialog() -> Any:
    theme = get_current_theme()
    shortcuts = [
        ("Alt+G", "Home"),
        ("Alt+B", "Run Results"),
        ("Alt+T", "Trade Analytics"),
        ("Alt+C", "Compare"),
        ("Alt+S", "Strategy"),
        ("Alt+I", "Strategy Guide"),
        ("Alt+R", "Scans"),
        ("Alt+P", "Pipeline"),
        ("Alt+L", "Paper Ledger"),
        ("Alt+Y", "Market Monitor"),
        ("Alt+U", "Symbols"),
        ("Alt+D", "Data Quality"),
        ("?", "This help dialog"),
    ]
    with (
        ui.dialog() as dlg,
        ui.card().style(
            f"background:{theme['surface']};border:1px solid {theme['surface_border']};"
            "min-width:320px;"
        ),
    ):
        with ui.row().classes("w-full justify-between items-center mb-4"):
            ui.label("Keyboard Shortcuts").classes("text-base font-semibold").style(
                f"color:{theme['text_primary']};"
            )
            ui.button(icon="close", on_click=dlg.close).props(
                "flat round dense aria-label='Close keyboard shortcuts'"
            ).style(f"color:{theme['text_muted']};")
        for key, desc in shortcuts:
            with ui.row().classes("justify-between w-full py-1"):
                ui.label(key).classes("text-sm mono-font").style(
                    f"color:{theme['primary']};background:{theme['surface_border']};"
                    "padding:2px 8px;border-radius:3px;"
                )
                ui.label(desc).classes("text-sm").style(f"color:{theme['text_secondary']};")
    return dlg
