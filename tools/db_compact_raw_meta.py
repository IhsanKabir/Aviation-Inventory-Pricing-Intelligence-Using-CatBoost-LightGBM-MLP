"""
Maintenance compaction tool for public.flight_offer_raw_meta.

Supports VACUUM / VACUUM FULL / REINDEX on a maintenance window.
Writes metadata JSON so ops can track what ran.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import psycopg2
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.runtime_config import get_database_url


def parse_args():
    p = argparse.ArgumentParser(description="Compact flight_offer_raw_meta table")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--table", default="public.flight_offer_raw_meta")
    p.add_argument(
        "--mode",
        choices=["analyze", "vacuum", "vacuum_full", "reindex", "vacuum_full_reindex"],
        default="vacuum",
    )
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--lock-timeout-ms", type=int, default=5000)
    p.add_argument("--statement-timeout-ms", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def _pg_uri_to_params(db_url: str):
    # Strip sqlalchemy driver prefix if present.
    db_url = re.sub(r"^postgresql\+[^:]+://", "postgresql://", db_url)
    m = re.match(
        r"^postgresql://(?P<user>[^:/@]+)(?::(?P<pwd>[^@]*))?@(?P<host>[^:/?#]+)(?::(?P<port>\d+))?/(?P<db>[^?]+)",
        db_url,
    )
    if not m:
        raise ValueError("Unsupported DB URL format")
    gd = m.groupdict()
    return {
        "host": gd["host"],
        "port": int(gd["port"] or 5432),
        "dbname": unquote(gd["db"]),
        "user": unquote(gd["user"]),
        "password": unquote(gd["pwd"] or ""),
    }


def _sizes(db_url: str, table: str) -> dict:
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    with eng.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    pg_total_relation_size('{table}'::regclass) AS total_bytes,
                    pg_relation_size('{table}'::regclass) AS heap_bytes,
                    pg_indexes_size('{table}'::regclass) AS index_bytes,
                    (SELECT COUNT(*) FROM {table}) AS row_count
                """
            )
        ).mappings().first()
    return {
        "total_bytes": int(row["total_bytes"] or 0),
        "heap_bytes": int(row["heap_bytes"] or 0),
        "index_bytes": int(row["index_bytes"] or 0),
        "row_count": int(row["row_count"] or 0),
    }


def _commands(mode: str, table: str) -> list[str]:
    if mode == "analyze":
        return [f"ANALYZE {table}"]
    if mode == "vacuum":
        return [f"VACUUM (VERBOSE, ANALYZE) {table}"]
    if mode == "vacuum_full":
        return [f"VACUUM (VERBOSE, FULL, ANALYZE) {table}"]
    if mode == "reindex":
        return [f"REINDEX TABLE {table}", f"ANALYZE {table}"]
    if mode == "vacuum_full_reindex":
        return [f"VACUUM (VERBOSE, FULL, ANALYZE) {table}", f"REINDEX TABLE {table}", f"ANALYZE {table}"]
    raise ValueError(f"unsupported mode={mode}")


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_path = out_dir / "db_compact_raw_meta_latest.json"
    run_path = out_dir / f"db_compact_raw_meta_{ts}.json"

    result = {
        "generated_at": now.isoformat(),
        "ok": False,
        "mode": args.mode,
        "table": args.table,
        "dry_run": bool(args.dry_run),
        "before": {},
        "after": {},
        "commands": [],
        "durations_sec": [],
        "detail": "",
    }
    try:
        result["before"] = _sizes(args.db_url, args.table)
        cmds = _commands(args.mode, args.table)
        result["commands"] = cmds
        if not args.dry_run:
            conn = psycopg2.connect(**_pg_uri_to_params(args.db_url))
            conn.autocommit = True
            cur = conn.cursor()
            try:
                if args.lock_timeout_ms:
                    cur.execute(f"SET lock_timeout = '{int(args.lock_timeout_ms)}ms'")
                if args.statement_timeout_ms:
                    cur.execute(f"SET statement_timeout = '{int(args.statement_timeout_ms)}ms'")
                for sql in cmds:
                    t0 = time.time()
                    cur.execute(sql)
                    result["durations_sec"].append(round(time.time() - t0, 3))
                cur.close()
                conn.close()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
        result["after"] = _sizes(args.db_url, args.table)
        result["ok"] = True
        saved = (result["before"].get("total_bytes", 0) - result["after"].get("total_bytes", 0))
        result["detail"] = f"bytes_saved={saved}"
        print(f"ok=True bytes_saved={saved}")
    except Exception as exc:
        result["detail"] = f"{type(exc).__name__}: {exc}"
        print(f"ok=False error={exc}")

    latest_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    run_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"latest={latest_path}")
    print(f"run={run_path}")
    if args.strict and not result["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
