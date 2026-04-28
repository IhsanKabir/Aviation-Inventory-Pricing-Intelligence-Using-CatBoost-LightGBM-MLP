from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AIRLINES_CONFIG = REPO_ROOT / "config" / "airlines.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "reports"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.source_switches import DEFAULT_SOURCE_SWITCHES_FILE, load_source_switches, source_switch_status


def _load_enabled_airlines(path: Path, source_switches_file: str | Path | None = None) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    rows: list[dict[str, Any]] = []
    switches = load_source_switches(source_switches_file)
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict) or not item.get("enabled"):
            continue
        code = str(item.get("code") or "").upper().strip()
        module = str(item.get("module") or "").strip().lower()
        if not source_switch_status(module, switches=switches).get("enabled"):
            continue
        if code and module:
            rows.append({"code": code, "module": module})
    return rows


def _fallback_health(module_name: str) -> dict[str, Any]:
    return {
        "source": module_name,
        "ok": True,
        "status": "warn",
        "blocking": False,
        "message": "module has no check_source_health/check_session contract yet; import succeeded",
    }


def _module_health(module_name: str, *, dry_run: bool) -> dict[str, Any]:
    try:
        mod = importlib.import_module(f"modules.{module_name}")
    except Exception as exc:
        return {
            "source": module_name,
            "ok": False,
            "status": "fail",
            "blocking": True,
            "message": f"module import failed: {exc}",
        }

    checker = getattr(mod, "check_source_health", None) or getattr(mod, "check_session", None)
    if not callable(checker):
        return _fallback_health(module_name)
    try:
        result = checker(dry_run=dry_run)
    except Exception as exc:
        return {
            "source": module_name,
            "ok": False,
            "status": "fail",
            "blocking": True,
            "message": f"health check raised: {exc}",
        }
    if not isinstance(result, dict):
        return {
            "source": module_name,
            "ok": False,
            "status": "fail",
            "blocking": True,
            "message": f"health check returned {type(result).__name__}, expected dict",
        }
    result.setdefault("source", module_name)
    result.setdefault("ok", bool(result.get("status") != "fail"))
    result.setdefault("blocking", not bool(result.get("ok")))
    result.setdefault("status", "ok" if result.get("ok") else "fail")
    return result


def _write_md(path: Path, report: dict[str, Any]) -> None:
    lines = [
        "# Preflight Session Check",
        "",
        f"- status: `{report.get('status')}`",
        f"- generated_at_utc: `{report.get('generated_at_utc')}`",
        f"- airlines: `{report.get('airline_count')}`",
        f"- blocking_count: `{report.get('blocking_count')}`",
        f"- warning_count: `{report.get('warning_count')}`",
        "",
        "## Airlines",
        "",
    ]
    for row in report.get("results") or []:
        health = row.get("health") or {}
        lines.append(
            f"- `{row.get('airline')}` module=`{row.get('module')}` status=`{health.get('status')}` "
            f"blocking=`{health.get('blocking')}` message={health.get('message') or '-'}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_preflight(
    *,
    airlines_config: Path,
    output_dir: Path,
    dry_run: bool,
    strict: bool,
    source_switches_file: Path | None = None,
) -> tuple[int, dict[str, Any]]:
    airlines = _load_enabled_airlines(airlines_config, source_switches_file)
    module_cache: dict[str, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []
    for row in airlines:
        module = row["module"]
        if module not in module_cache:
            module_cache[module] = _module_health(module, dry_run=dry_run)
        results.append(
            {
                "airline": row["code"],
                "module": module,
                "health": module_cache[module],
            }
        )

    status_counts = Counter(str((row.get("health") or {}).get("status") or "unknown") for row in results)
    blocking = [row for row in results if bool((row.get("health") or {}).get("blocking"))]
    warnings = [row for row in results if str((row.get("health") or {}).get("status") or "") == "warn"]
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FAIL" if blocking else ("WARN" if warnings else "PASS"),
        "dry_run": bool(dry_run),
        "strict": bool(strict),
        "airlines_config": str(airlines_config),
        "source_switches_file": str(source_switches_file or DEFAULT_SOURCE_SWITCHES_FILE),
        "airline_count": len(airlines),
        "module_count": len(module_cache),
        "blocking_count": len(blocking),
        "warning_count": len(warnings),
        "status_counts": dict(sorted(status_counts.items())),
        "results": results,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_json = output_dir / "preflight_session_check_latest.json"
    latest_md = output_dir / "preflight_session_check_latest.md"
    latest_json.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _write_md(latest_md, report)

    rc = 1 if strict and blocking else 0
    return rc, report


def main() -> int:
    parser = argparse.ArgumentParser(description="Check extractor session/capture/source readiness before accumulation.")
    parser.add_argument("--airlines-config", default=str(DEFAULT_AIRLINES_CONFIG))
    parser.add_argument("--source-switches-file", default=str(DEFAULT_SOURCE_SWITCHES_FILE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--dry-run", action="store_true", help="Do only local/import/cached-state checks.")
    parser.add_argument("--strict", action="store_true", help="Return non-zero if any blocking check fails.")
    args = parser.parse_args()

    rc, report = run_preflight(
        airlines_config=Path(args.airlines_config),
        output_dir=Path(args.output_dir),
        dry_run=bool(args.dry_run),
        strict=bool(args.strict),
        source_switches_file=Path(args.source_switches_file),
    )
    print(
        "preflight_session_check "
        f"status={report['status']} airlines={report['airline_count']} "
        f"blocking={report['blocking_count']} warnings={report['warning_count']} "
        f"strict={bool(args.strict)}"
    )
    print(f"report={Path(args.output_dir) / 'preflight_session_check_latest.json'}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
