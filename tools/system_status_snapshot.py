"""
Generate unified system status snapshot (JSON + Markdown).
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


OPS_STATUS_RE = re.compile(r"^- Status:\s+\*\*(?P<status>[A-Z]+)\*\*")
OPS_RANGE_RE = re.compile(r"^- Time range:\s+(?P<range>.+)$")


def parse_args():
    p = argparse.ArgumentParser(description="Write current system status snapshot")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def stamp(ts: datetime):
    return ts.strftime("%Y%m%d_%H%M%S")


def latest_match(root: Path, pattern: str, kind: str = "any"):
    paths = list(root.glob(pattern))
    if kind == "file":
        paths = [p for p in paths if p.is_file()]
    elif kind == "dir":
        paths = [p for p in paths if p.is_dir()]
    if not paths:
        return None
    return sorted(paths, key=lambda p: p.stat().st_mtime)[-1]


def file_meta(path: Path | None):
    if not path or not path.exists():
        return None
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
    }


def parse_ops(path: Path | None):
    out = {"status": None, "time_range": None}
    if not path or not path.exists():
        return out
    text = path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        m = OPS_STATUS_RE.match(line.strip())
        if m:
            out["status"] = m.group("status")
            continue
        m = OPS_RANGE_RE.match(line.strip())
        if m:
            out["time_range"] = m.group("range")
            continue
    return out


def main():
    args = parse_args()
    ts = now(args.timestamp_tz)
    st = stamp(ts)

    reports_dir = Path(args.reports_dir)
    logs_dir = Path(args.logs_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    ops_latest = reports_dir / "ops_health_latest.md"
    always_on_state = reports_dir / "always_on_maintenance_state.json"
    status_latest_json = reports_dir / "system_status_latest.json"
    status_latest_md = reports_dir / "system_status_latest.md"

    ops_archives = [p for p in reports_dir.glob("ops_health_*.md") if p.name != "ops_health_latest.md"]
    latest_ops_archive = sorted(ops_archives, key=lambda p: p.stat().st_mtime)[-1] if ops_archives else None
    latest_pack_zip = latest_match(reports_dir, "thesis_pack_*.zip", kind="file")
    latest_pack_dir = latest_match(reports_dir, "thesis_pack_*", kind="dir")
    latest_retention = latest_match(reports_dir, "retention_cleanup_*.json")

    latest_daily_log = logs_dir / "maintenance_daily_ops.log"
    latest_weekly_log = logs_dir / "maintenance_weekly_pack.log"
    latest_pulse_log = logs_dir / "maintenance_pulse.log"
    daemon_log = logs_dir / "always_on_maintenance.log"

    ops_parsed = parse_ops(ops_latest)

    state_payload = None
    if always_on_state.exists():
        try:
            state_payload = json.loads(always_on_state.read_text(encoding="utf-8"))
        except Exception:
            state_payload = {"parse_error": True}

    payload = {
        "generated_at": ts.isoformat(),
        "ops_health": {
            "latest": file_meta(ops_latest),
            "latest_archive": file_meta(latest_ops_archive),
            "status": ops_parsed.get("status"),
            "time_range": ops_parsed.get("time_range"),
        },
        "thesis_pack": {
            "latest_zip": file_meta(latest_pack_zip),
            "latest_dir": file_meta(latest_pack_dir),
        },
        "maintenance_logs": {
            "daemon": file_meta(daemon_log),
            "daily_ops": file_meta(latest_daily_log),
            "weekly_pack": file_meta(latest_weekly_log),
            "pulse": file_meta(latest_pulse_log),
        },
        "always_on_state": state_payload,
        "retention": file_meta(latest_retention),
    }

    status_latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    status_run_json = reports_dir / f"system_status_{st}.json"
    status_run_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = []
    lines.append("# System Status Snapshot")
    lines.append("")
    lines.append(f"- Generated at: {payload['generated_at']}")
    lines.append(f"- Ops status: {payload['ops_health'].get('status')}")
    lines.append(f"- Ops range: {payload['ops_health'].get('time_range')}")
    lines.append(f"- Latest ops archive: {(payload['ops_health'].get('latest_archive') or {}).get('path')}")
    lines.append(f"- Latest thesis zip: {(payload['thesis_pack'].get('latest_zip') or {}).get('path')}")
    lines.append(f"- Daily log: {(payload['maintenance_logs'].get('daily_ops') or {}).get('modified_at')}")
    lines.append(f"- Weekly log: {(payload['maintenance_logs'].get('weekly_pack') or {}).get('modified_at')}")
    lines.append(f"- Pulse log: {(payload['maintenance_logs'].get('pulse') or {}).get('modified_at')}")
    if isinstance(state_payload, dict):
        lines.append(f"- Last cycle at: {state_payload.get('last_cycle_at')}")
        lines.append(f"- Last daily ok at: {state_payload.get('last_daily_ok_at')}")
        lines.append(f"- Last weekly ok at: {state_payload.get('last_weekly_ok_at')}")

    status_latest_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    status_run_md = reports_dir / f"system_status_{st}.md"
    status_run_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"latest_json={status_latest_json}")
    print(f"latest_md={status_latest_md}")
    print(f"run_json={status_run_json}")
    print(f"run_md={status_run_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
