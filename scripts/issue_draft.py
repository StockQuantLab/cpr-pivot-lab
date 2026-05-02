"""Draft docs/ISSUES.md entries from local change and test context."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ISSUES_PATH = PROJECT_ROOT / "docs" / "ISSUES.md"


def _git_lines(args: list[str]) -> list[str]:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def _build_entry(title: str, *, notes: str | None) -> str:
    changed = sorted(
        set(_git_lines(["diff", "--name-only"]) + _git_lines(["diff", "--cached", "--name-only"]))
    )
    files = "\n".join(f"- `{path}`" for path in changed[:20]) or "- None detected"
    more = f"\n- ... {len(changed) - 20} more files" if len(changed) > 20 else ""
    note_block = notes.strip() if notes else "TBD"
    return f"""### {title}

- Date: TBD
- Type: bug/fix/incident
- Summary: {note_block}
- Impact: TBD
- Root cause: TBD
- Fix: TBD
- Validation: TBD
- Changed files:
{files}{more}

"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Draft a docs/ISSUES.md entry.")
    parser.add_argument("--title", default="TBD issue", help="Issue entry heading.")
    parser.add_argument("--notes", default=None, help="Known context to seed the draft.")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append the draft to docs/ISSUES.md. Default prints to stdout.",
    )
    args = parser.parse_args()
    entry = _build_entry(args.title, notes=args.notes)
    if args.append:
        ISSUES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ISSUES_PATH.open("a", encoding="utf-8") as handle:
            handle.write("\n" + entry)
        print(f"Appended draft to {ISSUES_PATH}")
    else:
        print(entry)


if __name__ in {"__main__", "__mp_main__"}:
    main()
