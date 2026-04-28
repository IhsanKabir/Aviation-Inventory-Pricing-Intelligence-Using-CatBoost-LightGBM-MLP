from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.source_switches import DEFAULT_SOURCE_SWITCHES_FILE
from core.scheduler_timing import load_scheduler_timing_plan, timing_entry_to_dict


def _render_markdown(plan: dict) -> str:
    lines = [
        "# Scheduler Timing Plan",
        "",
        f"- enabled: `{plan.get('enabled')}`",
        f"- timezone: `{plan.get('timezone')}`",
        "",
        "| Scope | ID | Enabled | Start | Repeat Min | Filters |",
        "| --- | --- | --- | --- | ---: | --- |",
    ]
    for entry in plan.get("entries") or []:
        row = timing_entry_to_dict(entry)
        filters = " ".join(row["pipeline_filter_args"]) if row["pipeline_filter_args"] else "--"
        lines.append(
            f"| {row['scope_type']} | {row['scope_id']} | {row['enabled']} | "
            f"{row['start_time']} | {row['repeat_minutes']} | `{filters}` |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and render scheduler timing settings.")
    parser.add_argument("--schedule-file", default=str(REPO_ROOT / "config" / "schedule.json"))
    parser.add_argument("--airlines-file", default=str(REPO_ROOT / "config" / "airlines.json"))
    parser.add_argument("--source-switches-file", default=str(DEFAULT_SOURCE_SWITCHES_FILE))
    parser.add_argument("--json-out", default=str(REPO_ROOT / "output" / "reports" / "scheduler_timing_plan_latest.json"))
    parser.add_argument("--md-out", default=str(REPO_ROOT / "output" / "reports" / "scheduler_timing_plan_latest.md"))
    args = parser.parse_args()

    plan = load_scheduler_timing_plan(
        schedule_file=Path(args.schedule_file),
        airlines_file=Path(args.airlines_file),
        source_switches_file=args.source_switches_file,
    )
    payload = {
        "enabled": plan.get("enabled"),
        "timezone": plan.get("timezone"),
        "entries": [timing_entry_to_dict(entry) for entry in plan.get("entries") or []],
    }

    json_out = Path(args.json_out)
    md_out = Path(args.md_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_out.write_text(_render_markdown(plan), encoding="utf-8")

    enabled_count = sum(1 for entry in plan.get("entries") or [] if entry.enabled)
    print(f"scheduler_timing_plan entries={len(plan.get('entries') or [])} enabled={enabled_count}")
    print(f"json={json_out}")
    print(f"md={md_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
