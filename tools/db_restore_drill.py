"""
End-to-end backup restore drill:
1) create temporary database
2) restore latest dump
3) compare row counts + sample checksums against source DB
4) drop temporary database (unless --keep-temp-db)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url


def parse_args():
    p = argparse.ArgumentParser(description="Run DB restore drill")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--backup-meta", default="output/backups/db_backup_latest.json")
    p.add_argument("--output-dir", default="output/backups")
    p.add_argument("--tables", default="public.flight_offers,public.flight_offer_raw_meta,airline_intel.column_change_events")
    p.add_argument("--sample-limit", type=int, default=3000)
    p.add_argument("--max-row-count-drift-pct", type=float, default=1.0, help="Allowed source-vs-restored row count growth percentage")
    p.add_argument("--keep-temp-db", action="store_true")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _find_pg_tool(tool_name: str) -> str | None:
    direct = shutil.which(tool_name)
    if direct:
        return direct
    pg_bin_dir = os.getenv("PG_BIN_DIR", "").strip()
    if pg_bin_dir:
        candidate = Path(pg_bin_dir) / f"{tool_name}.exe"
        if candidate.exists():
            return str(candidate)
    for root in (Path("C:/Program Files/PostgreSQL"), Path("C:/Program Files (x86)/PostgreSQL")):
        if not root.exists():
            continue
        matches = sorted(root.glob(f"*/bin/{tool_name}.exe"), reverse=True)
        if matches:
            return str(matches[0])
    return None


def _load_backup(meta_path: Path) -> dict | None:
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    backup = meta.get("backup_file")
    if not backup or not Path(backup).exists():
        return None
    return meta


def _run(cmd: list[str], env: dict[str, str] | None = None):
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return proc.returncode, (proc.stdout or ""), (proc.stderr or "")


def _sample_stats(engine, table: str, sample_limit: int):
    q = text(
        f"""
        WITH src AS (
            SELECT id
            FROM {table}
            ORDER BY id
            LIMIT :sample_limit
        )
        SELECT
            (SELECT COUNT(*) FROM {table})::bigint AS row_count,
            (SELECT COUNT(*) FROM src)::bigint AS sample_count,
            COALESCE((SELECT md5(string_agg(id::text, ',' ORDER BY id)) FROM src), md5('')) AS sample_checksum,
            (SELECT MIN(id) FROM {table})::bigint AS min_id,
            (SELECT MAX(id) FROM {table})::bigint AS max_id
        """
    )
    with engine.connect() as conn:
        row = conn.execute(q, {"sample_limit": int(sample_limit)}).mappings().first()
    return {
        "row_count": int(row["row_count"] or 0),
        "sample_count": int(row["sample_count"] or 0),
        "sample_checksum": row["sample_checksum"],
        "min_id": int(row["min_id"] or 0) if row["min_id"] is not None else None,
        "max_id": int(row["max_id"] or 0) if row["max_id"] is not None else None,
    }


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    latest = out_dir / "db_restore_drill_latest.json"
    run = out_dir / f"db_restore_drill_{ts}.json"

    url = make_url(args.db_url)
    source_db = url.database
    temp_db = f"{source_db}_restore_drill_{ts}"
    backup_meta = _load_backup(Path(args.backup_meta))
    backup_file = backup_meta.get("backup_file") if backup_meta else None
    createdb = _find_pg_tool("createdb")
    dropdb = _find_pg_tool("dropdb")
    pg_restore = _find_pg_tool("pg_restore")

    result: dict[str, Any] = {
        "generated_at": now.isoformat(),
        "ok": False,
        "source_db": source_db,
        "temp_db": temp_db,
        "backup_file": backup_file,
        "tool_paths": {
            "createdb": createdb,
            "dropdb": dropdb,
            "pg_restore": pg_restore,
        },
        "tables": {},
        "detail": "",
    }

    if not all([backup_file, createdb, dropdb, pg_restore]):
        result["detail"] = "missing_backup_or_pg_tools"
        latest.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("restore drill prerequisites missing")
        return 1 if args.strict else 0

    env = dict(os.environ)
    if url.password:
        env["PGPASSWORD"] = url.password

    host = str(url.host or "localhost")
    port = str(url.port or 5432)
    user = str(url.username or "postgres")

    rc, _, err = _run([createdb, "--host", host, "--port", port, "--username", user, temp_db], env=env)
    if rc != 0:
        result["detail"] = f"createdb_failed:{err.strip()[:300]}"
        latest.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("createdb failed")
        return 1 if args.strict else 0

    temp_url = url.set(database=temp_db)
    restore_target = temp_url.render_as_string(hide_password=False).replace("postgresql+psycopg2://", "postgresql://")
    rc, _, err = _run([pg_restore, "--no-owner", "--no-privileges", "--dbname", restore_target, backup_file], env=env)
    if rc != 0:
        result["detail"] = f"pg_restore_failed:{err.strip()[:300]}"
        if not args.keep_temp_db:
            _run([dropdb, "--if-exists", "--host", host, "--port", port, "--username", user, temp_db], env=env)
        latest.write_text(json.dumps(result, indent=2), encoding="utf-8")
        run.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("pg_restore failed")
        return 1 if args.strict else 0

    restore_engine = create_engine(temp_url.render_as_string(hide_password=False), pool_pre_ping=True, future=True)
    expected_metrics = (backup_meta or {}).get("table_metrics") or {}
    failed = []
    for t in [x.strip() for x in args.tables.split(",") if x.strip()]:
        if t in expected_metrics:
            src = expected_metrics[t]
        else:
            # fallback if old backup metadata doesn't include table metrics
            source_engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
            src = _sample_stats(source_engine, t, args.sample_limit)
        rst = _sample_stats(restore_engine, t, args.sample_limit)
        same_count = src["row_count"] == rst["row_count"]
        same_checksum = src["sample_checksum"] == rst["sample_checksum"]
        src_count = int(src["row_count"] or 0)
        rst_count = int(rst["row_count"] or 0)
        count_growth_pct = ((src_count - rst_count) / src_count * 100.0) if src_count > 0 else 0.0
        acceptable_count_drift = (rst_count <= src_count) and (count_growth_pct <= args.max_row_count_drift_pct)
        ok = same_checksum and acceptable_count_drift
        if not ok:
            failed.append(t)
        result["tables"][t] = {
            "ok": ok,
            "source": src,
            "restored": rst,
            "same_row_count": same_count,
            "same_sample_checksum": same_checksum,
            "count_growth_pct": round(count_growth_pct, 6),
            "acceptable_count_drift": acceptable_count_drift,
        }

    if not args.keep_temp_db:
        _run([dropdb, "--if-exists", "--host", host, "--port", port, "--username", user, temp_db], env=env)

    result["ok"] = len(failed) == 0
    result["detail"] = "restore_drill_passed" if result["ok"] else f"mismatch_tables:{','.join(failed)}"
    latest.write_text(json.dumps(result, indent=2), encoding="utf-8")
    run.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"restore_drill_ok={result['ok']} failed_tables={failed}")
    print(f"latest={latest}")
    print(f"run={run}")
    return 0 if result["ok"] or not args.strict else 1


if __name__ == "__main__":
    raise SystemExit(main())
