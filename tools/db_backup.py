"""
Create Postgres backup artifact using pg_dump.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url
from sqlalchemy import create_engine, text


def parse_args():
    p = argparse.ArgumentParser(description="Create DB backup")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--output-dir", default="output/backups")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _to_pg_uri(db_url: str) -> str:
    # sqlalchemy style: postgresql+psycopg2://...
    return re.sub(r"^postgresql\+[^:]+://", "postgresql://", db_url)


def _find_pg_tool(tool_name: str) -> str | None:
    direct = shutil.which(tool_name)
    if direct:
        return direct

    pg_bin_dir = os.getenv("PG_BIN_DIR", "").strip()
    if pg_bin_dir:
        candidate = Path(pg_bin_dir) / f"{tool_name}.exe"
        if candidate.exists():
            return str(candidate)

    search_roots = [
        Path("C:/Program Files/PostgreSQL"),
        Path("C:/Program Files (x86)/PostgreSQL"),
    ]
    candidates: list[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        candidates.extend(root.glob(f"*/bin/{tool_name}.exe"))
    if not candidates:
        return None

    # Prefer newest installed major version by lexical descending path.
    candidates = sorted(candidates, key=lambda p: p.as_posix(), reverse=True)
    return str(candidates[0])


def _capture_table_metrics(db_url: str, sample_limit: int = 3000):
    tables = ["public.flight_offers", "public.flight_offer_raw_meta", "airline_intel.column_change_events"]
    metrics = {}
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        for t in tables:
            row = conn.execute(
                text(
                    f"""
                    WITH src AS (
                        SELECT id
                        FROM {t}
                        ORDER BY id
                        LIMIT :sample_limit
                    )
                    SELECT
                        (SELECT COUNT(*) FROM {t})::bigint AS row_count,
                        (SELECT COUNT(*) FROM src)::bigint AS sample_count,
                        COALESCE((SELECT md5(string_agg(id::text, ',' ORDER BY id)) FROM src), md5('')) AS sample_checksum,
                        (SELECT MIN(id) FROM {t})::bigint AS min_id,
                        (SELECT MAX(id) FROM {t})::bigint AS max_id
                    """
                ),
                {"sample_limit": int(sample_limit)},
            ).mappings().first()
            metrics[t] = {
                "row_count": int(row["row_count"] or 0),
                "sample_count": int(row["sample_count"] or 0),
                "sample_checksum": row["sample_checksum"],
                "min_id": int(row["min_id"] or 0) if row["min_id"] is not None else None,
                "max_id": int(row["max_id"] or 0) if row["max_id"] is not None else None,
            }
    return metrics


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pg_dump = _find_pg_tool("pg_dump")
    latest_meta = out_dir / "db_backup_latest.json"
    run_meta = out_dir / f"db_backup_{ts}.json"

    result = {
        "generated_at": now.isoformat(),
        "ok": False,
        "backup_file": None,
        "pg_dump_found": bool(pg_dump),
        "pg_dump_path": pg_dump,
        "command": None,
        "table_metrics": {},
        "detail": "",
    }

    if not pg_dump:
        result["detail"] = "pg_dump_not_found_on_path"
        latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("pg_dump not found on PATH")
        return 1 if args.strict else 0

    backup_file = out_dir / f"db_backup_{ts}.dump"
    pg_uri = _to_pg_uri(args.db_url)
    cmd = [
        pg_dump,
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(backup_file),
        pg_uri,
    ]
    result["command"] = subprocess.list2cmdline(cmd)

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0 and backup_file.exists():
        result["ok"] = True
        result["backup_file"] = str(backup_file)
        try:
            result["table_metrics"] = _capture_table_metrics(args.db_url)
            result["detail"] = f"size_bytes={backup_file.stat().st_size}"
        except Exception as exc:  # pragma: no cover
            result["detail"] = f"size_bytes={backup_file.stat().st_size}; metric_capture_warn={exc}"
        print(f"backup_created={backup_file}")
    else:
        stderr = (proc.stderr or "").strip().replace("\n", " | ")
        stdout = (proc.stdout or "").strip().replace("\n", " | ")
        result["detail"] = f"rc={proc.returncode}; stderr={stderr[:400]}; stdout={stdout[:200]}"
        print(f"backup_failed rc={proc.returncode}")

    latest_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
    run_meta.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"latest_meta={latest_meta}")
    print(f"run_meta={run_meta}")

    if not result["ok"] and args.strict:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
