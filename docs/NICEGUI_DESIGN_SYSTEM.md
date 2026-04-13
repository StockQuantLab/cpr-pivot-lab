# NiceGUI Dashboard — Design System & Component Library

This document describes the design system, component architecture, and styling patterns for the CPR Pivot Lab NiceGUI dashboard. Use as a reference when adding pages or adapting patterns.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Theme System](#theme-system)
3. [Component Library](#component-library)
4. [Styling Patterns](#styling-patterns)
5. [Page Layout](#page-layout)
6. [Performance Patterns](#performance-patterns)
7. [Adding New Pages](#adding-new-pages)

---

## Architecture Overview

### File Structure

```
web/
├── main.py                 # App entry point, route definitions
├── run_nicedash.py         # CLI entry point (Windows event loop fix)
├── state.py                # DB connection, TTL caching, async wrappers
├── components/
│   └── __init__.py         # Theme system, components, CSS, keybindings
└── pages/
    ├── home.py             # Dashboard home — status KPIs + nav cards
    ├── run_detail.py       # Run Results — 8 KPIs + 7 analytics tabs
    ├── trades.py           # Trade Analytics — exit reasons, monthly, symbols
    ├── compare.py          # Compare Runs — side-by-side performance
    ├── strategy_analysis.py # Strategy Analysis — per-strategy breakdown
    ├── symbols.py          # Symbol Performance — per-symbol P/L
    └── data_quality.py     # Data Quality — table status + fix commands
```

### Key Principles

1. **Async DB** — All DB calls use `ThreadPoolExecutor` (DuckDB not async-native)
2. **CSS variables** — Theme switching via `:root` CSS variable injection, no page reload
3. **Reusable components** — All UI primitives are composable functions in `components/__init__.py`
4. **TTL caching** — In-memory + disk cache (30s runs, 120s symbols, 300s status)
5. **Polars throughout** — No pandas. All data processing in Polars (`pl.DataFrame`).

---

## Theme System

### Dual Theme Architecture

| Aspect | Terminal Mode | Clean Mode |
|--------|--------------|-------------|
| **Purpose** | Dark, brutalist trading terminal | Light, modern SaaS dashboard |
| **Background** | `#0d1117` (GitHub deep dark) | `#f8fafc` (off-white) |
| **Primary Color** | `#00ff88` (neon phosphor green) | `#6366f1` (indigo) |
| **Body Font** | IBM Plex Sans | DM Sans |
| **Mono Font** | Fira Code | JetBrains Mono |
| **Border Radius** | 2-4px (sharp edges) | 6-8px (rounded) |
| **Shadows** | Heavy, green glow | Subtle, gray |
| **Special Effects** | Scanline overlay via CSS | None |

### Theme Tokens

```python
# In web/components/__init__.py

THEME_TERMINAL = {
    "page_bg": "#0d1117",
    "surface": "#161b22",
    "surface_border": "#30363d",
    "surface_hover": "#21262d",
    "text_primary": "#f0f6fc",
    "text_secondary": "#8b949e",
    "text_muted": "#6e7681",
    "primary": "#00ff88",
    "primary_dark": "#00cc6a",
    "divider": "#30363d",
}

THEME_CLEAN = {
    "page_bg": "#f8fafc",
    "surface": "#ffffff",
    "surface_border": "#e2e8f0",
    "surface_hover": "#f1f5f9",
    "text_primary": "#0f172a",
    "text_secondary": "#475569",
    "text_muted": "#64748b",
    "primary": "#6366f1",
    "primary_dark": "#4f46e5",
    "divider": "#e2e8f0",
}
```

### CSS Variables

All theme values are injected as CSS variables on page load, so components can reference them without Python knowing which theme is active:

```css
:root {
    --theme-page-bg: #0d1117;
    --theme-surface: #161b22;
    --theme-surface-border: #30363d;
    --theme-text-primary: #f0f6fc;
    --theme-primary: #00ff88;
    --theme-color-success: #00ff88;
    --theme-color-error: #ff6b6b;
    --theme-color-warning: #ffd93d;
    --theme-color-info: #6bcfff;
    /* ... */
}
```

### Theme Toggle

The toggle button is in the header bar on every page. It:
1. Flips `_theme_mode["terminal"]`
2. Updates `THEME` and `COLORS` dicts in-place (via `.update()`)
3. Calls `ui.navigate.reload()` — full page reload with new CSS vars

State is preserved via `sessionStorage` before reload (see `run_detail.py` and `compare.py` for the pattern).

---

## Component Library

### `page_layout(title, icon)` — Every Page Wraps This

```python
from web.components import page_layout

def my_page() -> None:
    with page_layout("My Page", "icon_name"):
        # Page content here
        ui.label("Hello World")
```

Provides:
- Header bar (brand, title, theme toggle, `?` shortcuts, page icon)
- Collapsible sidebar (expanded → mini → hidden) with active route highlighting
- Full-width content area with 24px padding
- Keyboard shortcut injection (`Alt+G/B/T/C/S/Y/D`, `?` for help dialog)

### `kpi_card` / `kpi_grid`

```python
from web.components import kpi_grid, COLORS

kpi_grid([
    dict(title="Total Trades",  value="2,691",  icon="swap_horiz",    color=COLORS["info"]),
    dict(title="Win Rate",      value="22.7%",  icon="target",        color=COLORS["success"]),
    dict(title="Calmar",        value="5.46",   icon="speed",         color=COLORS["success"]),
    dict(title="Total P/L",     value="₹12.0L", icon="monetization_on", color=COLORS["primary"]),
], columns=4)
```

Each card has a left-accent glow on hover and staggered fade-in animation.

### `nav_card` — Home Page Navigation

```python
from web.components import nav_card, COLORS

nav_card(
    title="Run Results",
    description="Browse backtests — equity curves, KPIs, exit analysis",
    icon="bar_chart",
    target="/backtest",
    color=COLORS["primary"],
)
```

### `page_header`

```python
from web.components import page_header, COLORS

page_header(
    "Strategy Analysis",
    "Compare CPR_LEVELS and FBR across all saved runs",
    kpi_row=[
        dict(title="Best Calmar", value="5.46", icon="speed", color=COLORS["success"]),
    ]
)
```

### `paginated_table`

Use for any table with more than ~20 rows. Renders only the current page slice.

```python
from web.components import paginated_table

paginated_table(
    rows=row_list,  # list of dicts
    columns=[
        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
        {"name": "pnl",    "label": "P/L",    "field": "pnl",    "align": "right"},
    ],
    page_size=20,
    row_key="symbol",
    on_row_click=lambda row: ui.navigate.to(f"/backtest?run_id={row['run_id']}"),
)
```

**Important:** Use `rowsPerPage` (Quasar API), NOT `rows_per_page`.

### `strat_badge` / `exit_badge` — HTML Badges

```python
from web.components import strat_badge, exit_badge

ui.html(strat_badge("CPR_LEVELS"))  # Blue pill
ui.html(exit_badge("TARGET"))       # Green pill
```

Color map:
- `CPR_LEVELS` → blue `#2563eb`
- `FBR` → green `#10b981`
- `TARGET` → green, `INITIAL_SL` → red, `TRAILING_SL` → orange, `TIME` → gray

### `divider`

```python
from web.components import divider
divider()  # themed horizontal separator
```

### `empty_state`

```python
from web.components import empty_state

empty_state(
    "No runs found",
    "Run a backtest first: doppler run -- uv run pivot-backtest --help",
    icon="science",
    action_label="Go Home",
    action_callback=lambda: ui.navigate.to("/"),
)
```

### `info_box`

```python
from web.components import info_box
info_box("intraday_day_pack incomplete — run pivot-build --table pack", color="yellow")
```

Colors: `"blue"`, `"green"`, `"yellow"`, `"red"`.

### `export_button`

```python
from web.components import export_button
export_button(df, filename="trades_run001.csv", label="Export Trades CSV")
```

Takes a Polars `DataFrame`. Creates a `ui.download()` trigger.

### `apply_chart_theme(fig)` — Plotly

```python
from web.components import apply_chart_theme
import plotly.graph_objects as go

fig = go.Figure()
fig.add_trace(go.Scatter(x=x, y=y))
apply_chart_theme(fig)  # Sets paper_bgcolor, fonts, axis colors
ui.plotly(fig).classes("w-full h-80")
```

---

## Styling Patterns

### Using Theme Colors

Always use `THEME` and `COLORS` from `web.components` — never hardcode hex values in pages.

```python
from web.components import THEME, COLORS, get_current_theme, get_current_colors

# Inside a page function:
theme = get_current_theme()   # Returns current THEME_TERMINAL or THEME_CLEAN dict
colors = get_current_colors() # Returns current COLORS_TERMINAL or COLORS_CLEAN dict

ui.column().style(f"background: {theme['surface']}; border: 1px solid {theme['surface_border']};")
ui.label("Value").style(f"color: {colors['success']};")
```

Or use module-level globals (same dict, mutated in-place on toggle):
```python
from web.components import THEME, COLORS
# These are always current because toggle_theme_mode() calls THEME.update(...)
```

### Type Scale

```python
ui.label("Page Title").classes("text-2xl font-bold")       # 1.5rem, 600
ui.label("Section Head").classes("text-xl font-semibold")  # 1.25rem, 600
ui.label("Subsection").classes("text-lg")                  # 1.1rem, 500
ui.label("Body").classes("text-sm")                        # 0.875rem
ui.label("Muted").classes("text-xs")                       # 0.75rem
```

### Value Color Coding

```python
pnl = float(meta.get("total_pnl", 0))
color = colors["success"] if pnl >= 0 else colors["error"]
ui.label(f"₹{pnl:,.0f}").style(f"color: {color};")
```

---

## Page Layout

### Standard Page Structure

```python
async def my_page() -> None:
    data = await aget_some_data()  # async fetch before layout

    with page_layout("My Page", "icon"):
        theme = get_current_theme()
        colors = get_current_colors()

        page_header("My Page", "Optional subtitle")
        divider()

        # KPIs
        kpi_grid([dict(title="Metric", value="123", icon="bar_chart", color=colors["info"])])
        divider()

        # Content sections
        ui.label("Section").classes("text-xl font-semibold mb-4").style(
            f"color: {theme['text_primary']};"
        )
        # ... section content
```

### Refreshable Sections (Run Selector Pattern)

Used by `run_detail.py`, `trades.py`, `symbols.py` — select a run and reload data without full page reload:

```python
@ui.refreshable
def render_section(label: str) -> None:
    run_id = options.get(label, "")
    container = ui.column().classes("w-full")

    async def _load() -> None:
        df = await aget_trades(run_id)
        container.clear()
        with container:
            _render_content(df)

    ui.timer(0.1, _load, once=True)  # Async load after initial render

sel = ui.select(labels, value=labels[0], on_change=lambda e: render_section.refresh(e.value))
render_section(labels[0])
```

### Session State Preservation (Theme Toggle)

When a user toggles theme, the page reloads. Preserve selections using `sessionStorage`:

```python
# Save before toggle (called in on_change handler)
ui.run_javascript(f"sessionStorage.setItem('cpr_run_id', '{run_id}');")

# Restore on page load
saved_id = await ui.run_javascript("sessionStorage.getItem('cpr_run_id') || ''", timeout=2.0)
```

---

## Performance Patterns

### 1. Read-Only DB for Dashboard

```python
# In state.py — one connection, read-only, never blocks backtest writer
db = get_dashboard_db()
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-worker")
```

### 2. Async Wrappers for All DB Calls

```python
async def aget_runs(force: bool = False) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, lambda: _fetch_runs_sync(force))
```

### 3. TTL + Disk Cache

| Cache | TTL | Notes |
|-------|-----|-------|
| Runs list | 30s | In-memory, refreshed on each backtest save |
| Symbol list | 120s | In-memory |
| DB status | 300s | In-memory + disk (`~/.cache/cpr_dashboard_status.json`) |

Disk cache enables instant first render without a DB query.

### 4. Lite Mode for Initial Load

```python
# In home.py and data_quality.py
status = await aget_status(lite=True)  # Uses disk cache if available

# Background warm of full status
async def _warm():
    await aget_status(lite=False)

ui.timer(0.5, _warm, once=True)
```

---

## Adding New Pages

### Step 1: Create page file

```python
# web/pages/my_page.py
from __future__ import annotations
from nicegui import ui
from web.components import page_layout, divider, COLORS, get_current_theme
from web.state import aget_runs

async def my_page() -> None:
    data = await aget_runs()

    with page_layout("My Page", "icon_name"):
        theme = get_current_theme()
        divider()
        ui.label("Content").style(f"color: {theme['text_primary']};")
```

### Step 2: Register route in `web/main.py`

```python
from web.pages.my_page import my_page

@ui.page("/my_page")
async def _my_page() -> None:
    await my_page()
```

### Step 3: Add to sidebar nav

```python
# In web/components/__init__.py — NAV_ITEMS list
NAV_ITEMS.append({"label": "My Page", "icon": "star", "path": "/my_page"})
```

### Step 4: Add keyboard shortcut

```python
# In _KEYBINDINGS_HTML script block
'x': '/my_page',
```

---

## Quasar Components Reference

NiceGUI wraps Quasar (Vue 3). Key gotchas:

| Issue | Correct | Wrong |
|-------|---------|-------|
| Pagination prop | `rowsPerPage` | `rows_per_page` |
| Table scroll | Wrap with `overflow-x: auto` div | Add `w-full` to table |
| Dark mode | `ui.dark_mode(True)` in page_layout | Setting CSS directly |

---

## Navigation

| Page | Route | Keyboard |
|------|-------|---------|
| Home | `/` | `Alt+G` |
| Run Results | `/backtest` | `Alt+B` |
| Trade Analytics | `/trades` | `Alt+T` |
| Compare Runs | `/compare` | `Alt+C` |
| Strategy Analysis | `/strategy` | `Alt+S` |
| Symbol Performance | `/symbols` | `Alt+Y` |
| Data Quality | `/data_quality` | `Alt+D` |
| Shortcuts help | _(any page)_ | `?` |

---

## Fonts

### Terminal Mode
- **Body:** IBM Plex Sans (400, 500, 600)
- **Mono:** Fira Code (400, 500, 600, 700)

### Clean Mode
- **Body:** DM Sans (400, 500, 600, 700)
- **Mono:** JetBrains Mono (400, 500, 600)

Fonts are loaded via `<link>` with `media="print" onload="this.media='all'"` — non-blocking.

---

## Windows-Specific

1. `asyncio.WindowsSelectorEventLoopPolicy()` set in `run_nicedash.py` — **must be first import**
2. DuckDB paths must use forward slashes (`.replace("\\", "/")`)
3. Kill dashboard before running backtest: `taskkill //IM python.exe //F`

---

## DO / DON'T

### DO ✅
- Use `get_current_theme()` / `get_current_colors()` inside async page functions
- Use `paginated_table()` for any list >20 rows
- Use `ui.timer(0.1, load_fn, once=True)` to load data after the container renders
- Use `sessionStorage` to preserve selections across theme toggle
- Use Polars throughout — never convert to pandas

### DON'T ❌
- Hardcode hex colors — use `THEME["primary"]` or `COLORS["success"]`
- Call blocking DB functions directly in page scope — always `await aget_*()`
- Use `rows_per_page` — it's `rowsPerPage` (Quasar API)
- Use `ui.link(target_path=...)` — it's `ui.link(target=...)`
- Import `pandas` — use Polars instead

---

*Generated for CPR Pivot Lab v1.0 — 2026-03-08*
