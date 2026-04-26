"""Strategy Guide page — sticky sidebar TOC with scroll-spy and section-based layout."""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

from nicegui import ui

from web.components import divider, page_header, page_layout


class _Section(NamedTuple):
    title: str
    slug: str
    level: int
    body_md: str


def _load_markdown() -> str:
    docs_root = Path(__file__).resolve().parents[2] / "docs"
    guide_file = docs_root / "strategy-guide.md"
    if not guide_file.exists():
        return "### Strategy guide not found\n\nNo strategy guide document was found at docs/strategy-guide.md."
    return guide_file.read_text(encoding="utf-8")


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return text.strip("-") or "section"


def _extract_h2_headings(md: str) -> list[tuple[str, str]]:
    """Extract only ## headings (not ### or ####) for TOC."""
    headings: list[tuple[str, str]] = []
    for match in re.finditer(r"^##(?!#)\s+(.+)$", md, re.MULTILINE):
        text = match.group(1).strip()
        clean = re.sub(r"[*_`~]", "", text)
        slug = _slugify(clean)
        headings.append((clean, slug))
    return headings


def _parse_sections(md: str) -> list[_Section]:
    """Split markdown at ## boundaries only. ### and #### remain in body markdown."""
    parts = re.split(r"^##(?!#)\s+(.+)$", md, flags=re.MULTILINE)
    sections: list[_Section] = []
    if parts[0].strip():
        sections.append(_Section(title="", slug="intro", level=0, body_md=parts[0].strip()))
    i = 1
    while i + 1 < len(parts):
        title = parts[i].strip()
        body = parts[i + 1]
        slug = _slugify(re.sub(r"[*_`~]", "", title))
        sections.append(_Section(title=title, slug=slug, level=2, body_md=body))
        i += 2
    return sections


def _extract_rejected_variants(body_md: str) -> tuple[str, str]:
    """Extract rejected variants block; return (clean_body, variants_md)."""
    pat = re.compile(
        r"<!--\s*REJECTED_VARIANTS\s*-->\s*\n(.*?)\n\s*<!--\s*/REJECTED_VARIANTS\s*-->",
        re.DOTALL,
    )
    m = pat.search(body_md)
    if not m:
        return body_md, ""
    variants = m.group(1).strip()
    clean = body_md[: m.start()] + body_md[m.end() :]
    return clean.strip(), variants


def _section_accent(section: _Section) -> str | None:
    """Return a strategy key if this section is strategy-specific."""
    t = section.title.lower()
    if "cpr_levels" in t or "cpr levels" in t:
        return "CPR_LEVELS"
    if t == "fbr" or "failed breakout" in t:
        return "FBR"
    return None


# ---------------------------------------------------------------------------
# Scoped CSS — two-column layout, sticky TOC, scroll-spy, theme variables
# ---------------------------------------------------------------------------
_GUIDE_CSS = """
/* ── Two-column layout ─────────────────────────────────────────────── */
.strategy-guide-layout {
    display: grid;
    grid-template-columns: 220px 1fr;
    gap: 2rem;
    align-items: start;
}

/* ── Sticky TOC sidebar (desktop) ──────────────────────────────────── */
.strategy-toc-sidebar {
    position: sticky;
    top: 60px;
    max-height: calc(100vh - 80px);
    overflow-y: auto;
    scrollbar-width: thin;
    padding: 16px 0;
}
.strategy-toc-sidebar::-webkit-scrollbar {
    width: 3px;
}
.strategy-toc-sidebar::-webkit-scrollbar-thumb {
    background: var(--theme-surface-border);
    border-radius: 2px;
}
.strategy-toc-title {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--theme-text-secondary);
    margin-bottom: 12px;
    padding-left: 12px;
}
.strategy-toc-list {
    list-style: none;
    margin: 0;
    padding: 0;
}
.strategy-toc-list li {
    margin: 0;
    padding: 0;
}
.strategy-toc-list a {
    display: block;
    padding: 7px 12px;
    text-decoration: none;
    color: var(--theme-text-secondary);
    font-family: var(--font-mono);
    font-size: 0.75rem;
    line-height: 1.4;
    border-left: 2px solid transparent;
    transition: color 0.15s, border-color 0.15s, background 0.15s;
    border-radius: 0 3px 3px 0;
}
.strategy-toc-list a:hover {
    color: var(--theme-text-primary);
    background: var(--theme-surface-hover);
}
.strategy-toc-list a:focus-visible {
    outline: 2px solid var(--theme-primary);
    outline-offset: -2px;
    border-radius: 0 3px 3px 0;
}
.strategy-toc-list a.active {
    color: var(--theme-primary);
    border-left-color: var(--theme-primary);
    background: var(--theme-surface-hover);
    font-weight: 600;
}
.strategy-toc-list a .toc-num {
    color: var(--theme-primary);
    font-weight: 700;
    font-size: 0.65rem;
    margin-right: 6px;
    opacity: 0.7;
}
.strategy-toc-list a.active .toc-num {
    opacity: 1;
}

/* ── Mobile TOC (inline horizontal, hidden on desktop) ─────────────── */
.strategy-toc-mobile {
    display: none;
    margin-bottom: 1.5rem;
    padding: 12px 16px;
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    border-radius: 4px;
}
.strategy-toc-mobile-title {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--theme-text-secondary);
    margin-bottom: 8px;
}
.strategy-toc-mobile-links {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}
.strategy-toc-mobile-links a {
    padding: 6px 10px;
    text-decoration: none;
    color: var(--theme-text-secondary);
    font-family: var(--font-mono);
    font-size: 0.72rem;
    border-radius: 3px;
    transition: color 0.15s, background 0.15s;
    white-space: nowrap;
}
.strategy-toc-mobile-links a:hover,
.strategy-toc-mobile-links a.active {
    color: var(--theme-primary);
    background: var(--theme-surface-hover);
}
.strategy-toc-mobile-links a:focus-visible {
    outline: 2px solid var(--theme-primary);
    outline-offset: 1px;
}

/* ── Content column ────────────────────────────────────────────────── */
.strategy-guide-content {
    max-width: 82ch;
    min-width: 0;
}

/* ── Section cards (## only) ───────────────────────────────────────── */
.guide-section {
    margin-bottom: 2rem;
    padding: 20px 24px;
    border-radius: var(--card-radius, 4px);
    border: 1px solid var(--theme-surface-border);
    background: var(--theme-surface);
}
.guide-section.is-intro {
    border-left: 3px solid var(--theme-primary);
    background: var(--info-box-bg, var(--theme-surface));
}

/* ── Section header ────────────────────────────────────────────────── */
.guide-section-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 14px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--theme-surface-border);
}
.guide-section-header h2 {
    margin: 0;
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--theme-text-primary);
}

/* Strategy badges via CSS class (theme-aware, no inline styles) */
.strat-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 3px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    color: #fff;
    font-family: var(--font-mono);
}
.strat-badge.cpr-levels {
    background: var(--strat-cpr, #2563eb);
}
.strat-badge.fbr {
    background: var(--strat-fbr, #10b981);
}

/* ── Markdown inside sections ──────────────────────────────────────── */
/* Hide h2 (rendered as card header) — show h3/h4 as sub-sections */
.guide-section .nicegui-markdown h2 {
    display: none;
}
.guide-section .nicegui-markdown h3 {
    font-size: 1rem;
    font-weight: 600;
    color: var(--theme-text-primary);
    margin: 1.5rem 0 0.75rem;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--theme-surface-border);
}
.guide-section .nicegui-markdown h4 {
    font-size: 0.92rem;
    font-weight: 600;
    color: var(--theme-text-secondary);
    margin: 1.25rem 0 0.5rem;
}
.guide-section .nicegui-markdown p:first-child {
    margin-top: 0;
}
.guide-section .nicegui-markdown p:last-child {
    margin-bottom: 0;
}

/* ── Section nav (prev/next) ───────────────────────────────────────── */
.section-nav {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--theme-surface-border);
}
.section-nav a {
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--theme-text-secondary);
    text-decoration: none;
    padding: 4px 8px;
    border-radius: 3px;
    transition: color 0.15s, background 0.15s;
}
.section-nav a:hover {
    color: var(--theme-primary);
    background: var(--theme-surface-hover);
}
.section-nav a:focus-visible {
    outline: 2px solid var(--theme-primary);
    outline-offset: 1px;
}

/* ── Tables ────────────────────────────────────────────────────────── */
.guide-section .nicegui-markdown table {
    width: 100%;
    border-collapse: collapse;
    margin: 1rem 0;
    display: block;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}
.guide-section .nicegui-markdown th {
    background: var(--theme-surface-hover);
    color: var(--theme-primary);
    font-weight: 600;
    text-align: left;
    padding: 8px 12px;
    border: 1px solid var(--theme-surface-border);
    font-family: var(--font-mono);
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
}
.guide-section .nicegui-markdown td {
    padding: 8px 12px;
    border: 1px solid var(--theme-surface-border);
    color: var(--theme-text-primary);
    font-family: var(--font-mono);
    font-size: 0.8rem;
}
.guide-section .nicegui-markdown tr:nth-child(even) td {
    background: var(--theme-surface-hover);
}

/* ── Code blocks ───────────────────────────────────────────────────── */
.guide-section .nicegui-markdown pre {
    background: var(--code-bg, #000);
    color: #e2e8f0 !important;
    border: 1px solid var(--theme-surface-border);
    border-radius: 4px;
    padding: 12px 16px;
    overflow-x: auto;
    max-width: 100%;
}
.guide-section .nicegui-markdown pre code {
    color: inherit;
}
.guide-section .nicegui-markdown code {
    font-family: var(--font-mono);
    font-size: 0.85rem;
}
.guide-section .nicegui-markdown p code,
.guide-section .nicegui-markdown li code {
    background: var(--theme-surface-hover);
    padding: 2px 6px;
    border-radius: 3px;
    border: 1px solid var(--theme-surface-border);
    color: var(--theme-primary);
}

/* ── Blockquotes ───────────────────────────────────────────────────── */
.guide-section .nicegui-markdown blockquote {
    border-left: 3px solid var(--theme-primary);
    margin: 1rem 0;
    padding: 0.5rem 1rem;
    background: var(--info-box-bg);
}

/* ── Horizontal rules ─────────────────────────────────────────────── */
.guide-section .nicegui-markdown hr {
    border: none;
    border-top: 1px solid var(--theme-surface-border);
    margin: 2rem 0;
}

/* ── Rejected variants (flat expansion, no nested card) ────────────── */
.rejected-variants {
    margin-top: 12px;
    border-top: 1px dashed var(--theme-surface-border);
    padding-top: 8px;
}
.rejected-variants .nicegui-markdown li {
    font-size: 0.85rem;
    color: var(--theme-text-secondary);
    padding: 4px 0;
}

/* ── Mobile responsive ─────────────────────────────────────────────── */
@media (max-width: 1024px) {
    .strategy-guide-layout {
        grid-template-columns: 1fr;
    }
    .strategy-toc-sidebar {
        display: none;
    }
    .strategy-toc-mobile {
        display: block;
    }
    .guide-section {
        padding: 16px;
    }
}
@media (max-width: 640px) {
    .guide-section {
        padding: 12px;
        border-radius: 0;
        margin-left: -12px;
        margin-right: -12px;
        margin-bottom: 1rem;
    }
    .guide-section-header h2 {
        font-size: 1rem;
    }
}

/* ── Print ──────────────────────────────────────────────────────────── */
@media print {
    .strategy-toc-sidebar,
    .strategy-toc-mobile,
    .section-nav {
        display: none !important;
    }
    .strategy-guide-layout {
        display: block;
    }
    .strategy-guide-content {
        max-width: 100% !important;
    }
    .guide-section {
        border: none;
        background: none;
        padding: 0;
        page-break-inside: avoid;
    }
}
"""


async def strategy_guide_page() -> None:
    """Render a themed strategy reference page with sticky sidebar TOC and scroll-spy."""
    md = _load_markdown()
    sections = _parse_sections(md)
    h2_headings = _extract_h2_headings(md)

    with page_layout("Strategy Guide", "school"):
        ui.add_css(_GUIDE_CSS)

        page_header(
            "Strategy Guide",
            "CPR_LEVELS vs FBR — entry rules, trailing stops, parameter reference, and audit feed",
        )
        divider()

        # ── Two-column: sticky TOC + content ────────────────────────
        with ui.element("div").classes("strategy-guide-layout"):
            # ── Left: Sticky TOC sidebar (desktop only) ─────────────
            if h2_headings:
                with ui.element("aside").classes("strategy-toc-sidebar"):
                    ui.html('<div class="strategy-toc-title">Contents</div>')
                    with ui.element("ul").classes("strategy-toc-list"):
                        for i, (text, slug) in enumerate(h2_headings, 1):
                            with ui.element("li"):
                                with ui.element("a").props(f'href="#{slug}" data-slug="{slug}"'):
                                    ui.html(f'<span class="toc-num">{i:02d}</span>{text}')

            # ── Right: Scrollable content ────────────────────────────
            with ui.element("div").classes("strategy-guide-content"):
                # ── Mobile TOC (inline, hidden on desktop) ───────────
                if h2_headings:
                    with ui.element("div").classes("strategy-toc-mobile"):
                        ui.html('<div class="strategy-toc-mobile-title">Contents</div>')
                        with ui.element("div").classes("strategy-toc-mobile-links"):
                            for i, (text, slug) in enumerate(h2_headings, 1):
                                with ui.element("a").props(f'href="#{slug}" data-slug="{slug}"'):
                                    ui.html(f'<span class="toc-num">{i:02d}</span> {text}')

                # ── Section cards ─────────────────────────────────────
                section_slugs = [(s.slug, s.title) for s in sections if s.title]

                for sec in sections:
                    accent = _section_accent(sec)
                    extra_classes = ""
                    if sec.slug == "intro":
                        extra_classes = " is-intro"

                    with (
                        ui.element("div")
                        .classes(f"guide-section{extra_classes}")
                        .props(f'id="{sec.slug}"')
                    ):
                        # Section header
                        if sec.title:
                            with ui.element("div").classes("guide-section-header"):
                                ui.html(f"<h2>{sec.title}</h2>")
                                if accent:
                                    badge_color = "#2563eb" if accent == "CPR_LEVELS" else "#10b981"
                                    label = "CPR" if accent == "CPR_LEVELS" else "FBR"
                                    ui.badge(label, color=badge_color).classes("q-ma-xs")

                        # CPR_LEVELS: extract rejected variants
                        if accent == "CPR_LEVELS":
                            body_clean, variants_md = _extract_rejected_variants(sec.body_md)
                            if body_clean.strip():
                                ui.markdown(body_clean).classes("w-full")
                            if variants_md:
                                with ui.expansion("Rejected Variants").classes(
                                    "w-full rejected-variants"
                                ):
                                    ui.markdown(variants_md).classes("w-full")
                        else:
                            body = sec.body_md.strip()
                            if body:
                                ui.markdown(body).classes("w-full")

                        # Prev/next navigation between major sections
                        if sec.title:
                            idx = next(
                                (j for j, (s, _) in enumerate(section_slugs) if s == sec.slug),
                                None,
                            )
                            if idx is not None:
                                with ui.element("div").classes("section-nav"):
                                    if idx > 0:
                                        prev_slug, prev_title = section_slugs[idx - 1]
                                        ui.html(f'<a href="#{prev_slug}">&larr; {prev_title}</a>')
                                    else:
                                        ui.html("<span></span>")
                                    if idx < len(section_slugs) - 1:
                                        next_slug, next_title = section_slugs[idx + 1]
                                        ui.html(f'<a href="#{next_slug}">{next_title} &rarr;</a>')
                                    else:
                                        ui.html("<span></span>")

        # ── JavaScript: smooth-scroll + scroll-spy ────────────────────
        ui.run_javascript("""
        (() => {
            // Smooth-scroll for all anchor links
            document.querySelectorAll('a[href^="#"]').forEach(a => {
                a.addEventListener('click', e => {
                    e.preventDefault();
                    const id = a.getAttribute('href').slice(1);
                    const target = document.getElementById(id);
                    if (target) {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        history.replaceState(null, '', '#' + id);
                    }
                });
            });

            // Scroll-spy via Intersection Observer
            const sections = document.querySelectorAll('.guide-section[id]');
            if (!sections.length) return;

            const tocLinks = document.querySelectorAll(
                '.strategy-toc-list a, .strategy-toc-mobile-links a'
            );

            const observer = new IntersectionObserver(entries => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        const id = entry.target.id;
                        tocLinks.forEach(link => {
                            const slug = link.getAttribute('data-slug')
                                || (link.getAttribute('href') || '').slice(1);
                            link.classList.toggle('active', slug === id);
                        });
                    }
                });
            }, {
                rootMargin: '-80px 0px -70% 0px',
                threshold: 0,
            });

            sections.forEach(s => observer.observe(s));
        })();
        """)
