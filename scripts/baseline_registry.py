"""Validate the machine-readable CPR baseline registry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from db.backtest_db import get_backtest_db
from engine.cli_setup import configure_windows_stdio

configure_windows_stdio(line_buffering=True, write_through=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REGISTRY = PROJECT_ROOT / "config" / "baselines" / "cpr_current.yaml"


def load_registry(path: Path = DEFAULT_REGISTRY) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Registry {path} must contain a YAML mapping.")
    return payload


def _variant_errors(registry: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    variants = registry.get("variants")
    if not isinstance(variants, list):
        return ["variants must be a list"]
    if len(variants) != 8:
        errors.append(f"expected 8 CPR variants, found {len(variants)}")
    labels: set[str] = set()
    run_ids: set[str] = set()
    for index, variant in enumerate(variants, start=1):
        if not isinstance(variant, dict):
            errors.append(f"variant {index} is not a mapping")
            continue
        label = str(variant.get("label") or "")
        run_id = str(variant.get("run_id") or "")
        if not label:
            errors.append(f"variant {index} missing label")
        if label in labels:
            errors.append(f"duplicate label {label}")
        labels.add(label)
        if not run_id:
            errors.append(f"variant {label or index} missing run_id")
        if run_id in run_ids:
            errors.append(f"duplicate run_id {run_id}")
        run_ids.add(run_id)
        if variant.get("direction") not in {"LONG", "SHORT"}:
            errors.append(f"variant {label or index} has invalid direction")
        if not str(variant.get("preset") or "").startswith("CPR_LEVELS_"):
            errors.append(f"variant {label or index} has invalid preset")
    return errors


def validate_registry(path: Path = DEFAULT_REGISTRY, *, check_db: bool = False) -> dict[str, Any]:
    registry = load_registry(path)
    errors = _variant_errors(registry)
    warnings: list[str] = []
    missing_run_ids: list[str] = []
    if check_db and not errors:
        variants = registry.get("variants") or []
        run_ids = [str(v["run_id"]) for v in variants]
        placeholders = ", ".join("?" for _ in run_ids)
        db = get_backtest_db()
        rows = db.con.execute(
            f"SELECT run_id FROM run_metadata WHERE run_id IN ({placeholders})",
            run_ids,
        ).fetchall()
        present = {str(row[0]) for row in rows}
        missing_run_ids = [run_id for run_id in run_ids if run_id not in present]
        if missing_run_ids:
            errors.append(f"{len(missing_run_ids)} registry run_id(s) missing from run_metadata")
    if registry.get("universe") and not str(registry["universe"]).startswith("full_"):
        warnings.append("registry universe does not use the full_YYYY_MM_DD convention")
    return {
        "ok": not errors,
        "registry_path": str(path),
        "name": registry.get("name"),
        "universe": registry.get("universe"),
        "variant_count": len(registry.get("variants") or []),
        "run_ids": [str(v.get("run_id")) for v in registry.get("variants") or []],
        "errors": errors,
        "warnings": warnings,
        "missing_run_ids": missing_run_ids,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate CPR baseline registry.")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY), help="Registry YAML path.")
    parser.add_argument("--check-db", action="store_true", help="Verify run IDs exist in DB.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    args = parser.parse_args()
    payload = validate_registry(Path(args.registry), check_db=bool(args.check_db))
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Registry: {payload['registry_path']}")
        print(f"Name: {payload['name']}")
        print(f"Universe: {payload['universe']}")
        print(f"Variants: {payload['variant_count']}")
        print(f"Status: {'OK' if payload['ok'] else 'FAIL'}")
        for warning in payload["warnings"]:
            print(f"Warning: {warning}")
        for error in payload["errors"]:
            print(f"Error: {error}")
        if payload["missing_run_ids"]:
            print("Missing run IDs:")
            for run_id in payload["missing_run_ids"]:
                print(f"  {run_id}")
    if not payload["ok"]:
        raise SystemExit(1)


if __name__ in {"__main__", "__mp_main__"}:
    main()
