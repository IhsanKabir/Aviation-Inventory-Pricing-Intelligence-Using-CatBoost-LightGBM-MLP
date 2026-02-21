"""
Build a minimal unified operator dashboard (Markdown + HTML).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Build operator dashboard")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--backups-dir", default="output/backups")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _status_badge(status: str):
    s = (status or "UNKNOWN").upper()
    if s == "PASS":
        return "PASS"
    if s == "WARN":
        return "WARN"
    return "FAIL"


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    reports_dir = Path(args.reports_dir)
    backups_dir = Path(args.backups_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    smoke = _read_json(reports_dir / "smoke_check_latest.json") or {}
    sla = _read_json(reports_dir / "data_sla_latest.json") or {}
    drift = _read_json(reports_dir / "model_drift_latest.json") or {}
    status = _read_json(reports_dir / "system_status_latest.json") or {}
    backup = _read_json(backups_dir / "db_backup_latest.json") or {}
    restore = _read_json(backups_dir / "db_restore_test_latest.json") or {}
    drill = _read_json(backups_dir / "db_restore_drill_latest.json") or {}

    smoke_status = _status_badge(smoke.get("status", "UNKNOWN"))
    sla_status = _status_badge(sla.get("status", "UNKNOWN"))
    drift_status = _status_badge(drift.get("overall_status", "UNKNOWN"))
    backup_status = "PASS" if backup.get("ok") else "WARN"
    restore_status = "PASS" if restore.get("ok") else "WARN"
    drill_status = "PASS" if drill.get("ok") else ("WARN" if drill else "UNKNOWN")

    latest_ops = (((status.get("ops_health") or {}).get("latest_archive") or {}).get("path")) or ""
    latest_pack = (((status.get("thesis_pack") or {}).get("latest_zip") or {}).get("path")) or ""

    md = [
        "# Operator Dashboard",
        "",
        f"- Generated at: `{now.isoformat()}`",
        "",
        "## Status Summary",
        "",
        f"- Smoke Check: **{smoke_status}** (`output/reports/smoke_check_latest.md`)",
        f"- Data SLA: **{sla_status}** (`output/reports/data_sla_latest.md`)",
        f"- Model Drift: **{drift_status}** (`output/reports/model_drift_latest.md`)",
        f"- DB Backup: **{backup_status}** (`output/backups/db_backup_latest.json`)",
        f"- DB Restore Test: **{restore_status}** (`output/backups/db_restore_test_latest.json`)",
        f"- DB Restore Drill: **{drill_status}** (`output/backups/db_restore_drill_latest.json`)",
        "",
        "## Pointers",
        "",
        f"- Ops Health (latest archive): `{latest_ops}`",
        f"- Thesis Pack (latest zip): `{latest_pack}`",
        f"- System Status: `output/reports/system_status_latest.md`",
        "",
    ]

    md_text = "\n".join(md)
    latest_md = reports_dir / "operator_dashboard_latest.md"
    run_md = reports_dir / f"operator_dashboard_{ts}.md"
    latest_md.write_text(md_text, encoding="utf-8")
    run_md.write_text(md_text, encoding="utf-8")

    json_payload = {
        "generated_at": now.isoformat(),
        "statuses": {
            "smoke": smoke_status,
            "sla": sla_status,
            "drift": drift_status,
            "backup": backup_status,
            "restore": restore_status,
            "restore_drill": drill_status,
        },
        "pointers": {
            "ops_archive": latest_ops,
            "thesis_pack": latest_pack,
            "smoke_md": "output/reports/smoke_check_latest.md",
            "sla_md": "output/reports/data_sla_latest.md",
            "drift_md": "output/reports/model_drift_latest.md",
        },
    }
    latest_json = reports_dir / "operator_dashboard_latest.json"
    run_json = reports_dir / f"operator_dashboard_{ts}.json"
    latest_json.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
    run_json.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")

    html = f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>Operator Dashboard</title></head>
<body>
<pre>{md_text}</pre>
</body>
</html>
"""
    latest_html = reports_dir / "operator_dashboard_latest.html"
    run_html = reports_dir / f"operator_dashboard_{ts}.html"
    latest_html.write_text(html, encoding="utf-8")
    run_html.write_text(html, encoding="utf-8")

    print(f"latest_md={latest_md}")
    print(f"latest_html={latest_html}")
    print(f"latest_json={latest_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
