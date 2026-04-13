"""Initialize PostgreSQL operational schema."""

from __future__ import annotations

from db.postgres import initialize_schema
from engine.cli_setup import configure_windows_asyncio, configure_windows_stdio, run_asyncio

configure_windows_stdio(line_buffering=True, write_through=True)


async def _main() -> None:
    await initialize_schema()
    print("PostgreSQL schema initialized.")


def main() -> None:
    configure_windows_asyncio()
    run_asyncio(_main())


if __name__ in {"__main__", "__mp_main__"}:
    main()
