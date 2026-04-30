from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
AIRLINES_FILE = REPO_ROOT / "config" / "airlines.json"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.atomic_write import atomic_write_json, atomic_write_text
from core.source_switches import DEFAULT_SOURCE_SWITCHES_FILE, load_source_switches, source_switch_status

DIRECT_MODULES = {
    "airasia": "Direct airline website",
    "airarabia": "Direct airline website",
    "biman": "Direct airline website",
    "novoair": "Direct airline website",
    "indigo": "Direct airline website",
    "maldivian": "Direct airline website",
    "salamair": "Direct airline website",
}

HYBRID_MODULES = {
    "bs": "Hybrid OTA wrapper",
    "airastra": "Hybrid OTA wrapper",
}

OTA_MODULES = {
    "sharetrip": "OTA wrapper",
}


def _classify_module(module_name: str) -> tuple[str, str]:
    module_key = str(module_name or "").strip().lower()
    if module_key in DIRECT_MODULES:
        return "direct_first", DIRECT_MODULES[module_key]
    if module_key in HYBRID_MODULES:
        return "hybrid_ota", HYBRID_MODULES[module_key]
    if module_key in OTA_MODULES:
        return "ota_only", OTA_MODULES[module_key]
    return "unknown", "Unknown source family"


def _recommended_next_step(classification: str, fallback_modules: List[str]) -> str:
    if classification == "direct_first":
        if fallback_modules:
            return f"Keep direct source primary; fall back to {', '.join(fallback_modules)} when direct rows are empty."
        return "Keep direct source primary."
    if classification == "hybrid_ota":
        return "No true direct website connector yet; keep best OTA chain active and build direct connector later."
    if classification == "ota_only":
        return "Build a direct website connector for this airline if direct-first coverage is required."
    return "Inspect this airline module manually."


def _status_note(status: dict[str, Any]) -> str:
    reasons = [str(x).strip() for x in (status.get("reasons") or []) if str(x).strip()]
    return "; ".join(reasons)


def build_source_plan(
    items: List[Dict[str, Any]],
    source_switches_file: str | Path | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    switches = load_source_switches(source_switches_file)
    for item in items:
        code = str(item.get("code") or "").upper().strip()
        module_name = str(item.get("module") or "").strip()
        fallback_modules = []
        disabled_fallback_modules = []
        for raw_fallback in item.get("fallback_modules") or []:
            fallback_module = str(raw_fallback).strip()
            if not fallback_module:
                continue
            fallback_status = source_switch_status(fallback_module, switches=switches)
            if fallback_status.get("enabled"):
                fallback_modules.append(fallback_module)
            else:
                disabled_fallback_modules.append(f"{fallback_module} ({_status_note(fallback_status)})")
        classification, source_family = _classify_module(module_name)
        runtime_enabled = bool(item.get("enabled", False))
        runtime_note = ""
        primary_status = source_switch_status(module_name, switches=switches)
        if not primary_status.get("enabled"):
            runtime_enabled = False
            runtime_note = _status_note(primary_status) or "disabled in source switches"
        rows.append(
            {
                "code": code,
                "enabled": bool(item.get("enabled", False)),
                "runtime_enabled": runtime_enabled,
                "module": module_name,
                "classification": classification,
                "source_family": source_family,
                "fallback_modules": fallback_modules,
                "disabled_fallback_modules": disabled_fallback_modules,
                "runtime_note": runtime_note,
                "recommended_next_step": _recommended_next_step(classification, fallback_modules),
            }
        )
    return rows


def find_empty_effective_chains(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return rows where the airline is enabled but no source can serve it.

    An airline has an empty effective chain when it is enabled in
    ``airlines.json``, its primary module is disabled in ``source_switches.json``,
    and every fallback module is also disabled. Such airlines guarantee zero
    rows for the cycle and indicate a misconfiguration.
    """
    bad: List[Dict[str, Any]] = []
    for row in rows:
        if not row.get("enabled"):
            continue
        if row.get("runtime_enabled"):
            continue
        if row.get("fallback_modules"):
            continue
        bad.append(row)
    return bad


def render_markdown(rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Airline Source Plan",
        "",
        "| Airline | Enabled | Primary Module | Classification | Fallback Modules | Next Step |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        fallback = ", ".join(row["fallback_modules"]) if row["fallback_modules"] else "--"
        enabled = "yes" if row["enabled"] else "no"
        if row.get("enabled") and not row.get("runtime_enabled"):
            enabled = "runtime-no"
        note = f" {row['runtime_note']}" if row.get("runtime_note") else ""
        lines.append(
            f"| {row['code']} | {enabled} | {row['module']} | {row['classification']} | {fallback} | {row['recommended_next_step']}{note} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit current direct-vs-OTA airline source plan.")
    parser.add_argument("--airlines-file", default=str(AIRLINES_FILE))
    parser.add_argument("--source-switches-file", default=str(DEFAULT_SOURCE_SWITCHES_FILE))
    parser.add_argument("--json-out", default=str(REPO_ROOT / "output" / "reports" / "airline_source_plan_latest.json"))
    parser.add_argument("--md-out", default=str(REPO_ROOT / "output" / "reports" / "airline_source_plan_latest.md"))
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Exit non-zero (3) if any enabled airline has an empty effective source chain "
            "(primary disabled and no enabled fallbacks). Use to gate scheduled runs against "
            "broken source_switches configurations."
        ),
    )
    args = parser.parse_args()

    airlines_path = Path(args.airlines_file)
    items = json.loads(airlines_path.read_text(encoding="utf-8"))
    rows = build_source_plan(items, args.source_switches_file)

    atomic_write_json(Path(args.json_out), rows)
    atomic_write_text(Path(args.md_out), render_markdown(rows))

    print(f"Wrote JSON: {args.json_out}")
    print(f"Wrote Markdown: {args.md_out}")
    print(f"Total airlines audited: {len(rows)}")

    empty_chains = find_empty_effective_chains(rows)
    if empty_chains:
        codes = ",".join(row["code"] for row in empty_chains)
        print(f"WARNING: {len(empty_chains)} airline(s) have empty effective source chains: {codes}", file=sys.stderr)
        if args.strict:
            print("--strict: refusing to proceed with broken source plan.", file=sys.stderr)
            return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
