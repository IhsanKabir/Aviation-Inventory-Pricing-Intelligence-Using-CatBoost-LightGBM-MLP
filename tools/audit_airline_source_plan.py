from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
AIRLINES_FILE = REPO_ROOT / "config" / "airlines.json"

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


def build_source_plan(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in items:
        code = str(item.get("code") or "").upper().strip()
        module_name = str(item.get("module") or "").strip()
        fallback_modules = [str(m).strip() for m in (item.get("fallback_modules") or []) if str(m).strip()]
        classification, source_family = _classify_module(module_name)
        rows.append(
            {
                "code": code,
                "enabled": bool(item.get("enabled", False)),
                "module": module_name,
                "classification": classification,
                "source_family": source_family,
                "fallback_modules": fallback_modules,
                "recommended_next_step": _recommended_next_step(classification, fallback_modules),
            }
        )
    return rows


def render_markdown(rows: List[Dict[str, Any]]) -> str:
    lines = [
        "# Airline Source Plan",
        "",
        "| Airline | Enabled | Primary Module | Classification | Fallback Modules | Next Step |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        fallback = ", ".join(row["fallback_modules"]) if row["fallback_modules"] else "--"
        lines.append(
            f"| {row['code']} | {'yes' if row['enabled'] else 'no'} | {row['module']} | {row['classification']} | {fallback} | {row['recommended_next_step']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit current direct-vs-OTA airline source plan.")
    parser.add_argument("--airlines-file", default=str(AIRLINES_FILE))
    parser.add_argument("--json-out", default=str(REPO_ROOT / "output" / "reports" / "airline_source_plan_latest.json"))
    parser.add_argument("--md-out", default=str(REPO_ROOT / "output" / "reports" / "airline_source_plan_latest.md"))
    args = parser.parse_args()

    airlines_path = Path(args.airlines_file)
    items = json.loads(airlines_path.read_text(encoding="utf-8"))
    rows = build_source_plan(items)

    json_out = Path(args.json_out)
    md_out = Path(args.md_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)

    json_out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    md_out.write_text(render_markdown(rows), encoding="utf-8")

    print(f"Wrote JSON: {json_out}")
    print(f"Wrote Markdown: {md_out}")
    print(f"Total airlines audited: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
