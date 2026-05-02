"""HTTP smoke test for the NiceGUI dashboard."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from typing import Any

from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

DEFAULT_PATHS = ("/", "/paper_ledger", "/data_quality")


def _fetch(url: str, timeout: float) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read(4096)
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        return {
            "url": url,
            "ok": False,
            "status": int(exc.code),
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "url": url,
            "ok": False,
            "status": None,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": str(exc),
        }
    text = body.decode("utf-8", errors="replace").lower()
    return {
        "url": url,
        "ok": 200 <= status < 500 and "traceback" not in text,
        "status": status,
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
        "contains_traceback": "traceback" in text,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-test dashboard HTTP pages.")
    parser.add_argument("--base-url", default="http://127.0.0.1:9999", help="Dashboard base URL.")
    parser.add_argument(
        "--paths",
        default=",".join(DEFAULT_PATHS),
        help="Comma-separated URL paths to check.",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout seconds.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    results = [
        _fetch(f"{base_url}{path if path.startswith('/') else '/' + path}", args.timeout)
        for path in paths
    ]
    ok = all(bool(row["ok"]) for row in results)
    payload = {"ok": ok, "base_url": base_url, "results": results}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        for row in results:
            marker = "OK" if row["ok"] else "FAIL"
            print(f"{marker:<5} {row['status']} {row['elapsed_ms']:>8} ms {row['url']}")
            if row.get("error"):
                print(f"      {row['error']}")
    if not ok:
        raise SystemExit(1)


if __name__ in {"__main__", "__mp_main__"}:
    main()
