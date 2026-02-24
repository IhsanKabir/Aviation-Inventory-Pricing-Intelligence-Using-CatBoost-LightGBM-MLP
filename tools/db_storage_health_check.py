"""
Read-only database storage health monitor.

Outputs JSON + Markdown summaries with:
- DB size / top tables
- disk free space (DB drive + reports dir drive)
- raw-meta growth runway estimate
- simple bloat heuristic for flight_offer_raw_meta
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from urllib.parse import unquote

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.runtime_config import get_database_url


def parse_args():
    p = argparse.ArgumentParser(description="Read-only DB storage health check")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true", help="Return non-zero on WARN/FAIL")
    p.add_argument("--warn-db-gb", type=float, default=10.0)
    p.add_argument("--warn-free-gb", type=float, default=10.0)
    p.add_argument("--fail-free-gb", type=float, default=5.0)
    p.add_argument("--warn-runway-days", type=float, default=60.0)
    p.add_argument("--fail-runway-days", type=float, default=21.0)
    p.add_argument("--growth-lookback-days", type=int, default=30)
    return p.parse_args()


def _now(tz_mode: str) -> datetime:
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime) -> str:
    return now.strftime("%Y%m%d_%H%M%S")


def _human_bytes(n: int | None) -> str:
    if n is None:
        return "n/a"
    if n < 1024:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        x /= 1024.0
        if x < 1024 or u == units[-1]:
            return f"{x:.2f} {u}"
    return f"{n} B"


def _safe_disk_usage_for_path(path_str: str | None):
    if not path_str:
        return None
    try:
        p = Path(path_str)
        target = p.drive + "\\" if p.drive else str(p)
        usage = shutil.disk_usage(target)
        return {
            "path": target,
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
        }
    except Exception:
        return None


def _extract_password_safe_url(url: str) -> str:
    # For display only, hide password if present.
    try:
        head, tail = url.split("://", 1)
        if "@" not in tail or ":" not in tail.split("@", 1)[0]:
            return url
        creds, rest = tail.split("@", 1)
        user = creds.split(":", 1)[0]
        return f"{head}://{user}:***@{rest}"
    except Exception:
        return url


def collect_metrics(db_url: str, lookback_days: int):
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    metrics: dict = {}
    with eng.connect() as conn:
        db_row = conn.execute(
            text("SELECT current_database() AS name, pg_database_size(current_database()) AS bytes")
        ).mappings().first()
        metrics["database"] = {
            "name": db_row["name"],
            "bytes": int(db_row["bytes"] or 0),
        }

        data_dir = conn.execute(text("SHOW data_directory")).scalar()
        wal_size = None
        try:
            wal_size = conn.execute(text("SELECT COALESCE(sum(size),0) FROM pg_ls_waldir()")).scalar()
            wal_size = int(wal_size or 0)
        except Exception:
            wal_size = None
        metrics["postgres"] = {"data_directory": data_dir, "wal_dir_bytes": wal_size}

        top_tables = conn.execute(
            text(
                """
                SELECT n.nspname AS schema_name,
                       c.relname AS table_name,
                       pg_total_relation_size(c.oid) AS total_bytes,
                       pg_relation_size(c.oid) AS heap_bytes,
                       pg_indexes_size(c.oid) AS index_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind='r'
                  AND n.nspname NOT IN ('pg_catalog','information_schema')
                ORDER BY pg_total_relation_size(c.oid) DESC
                LIMIT 20
                """
            )
        ).mappings().all()
        metrics["top_tables"] = [
            {
                "schema": r["schema_name"],
                "table": r["table_name"],
                "total_bytes": int(r["total_bytes"] or 0),
                "heap_bytes": int(r["heap_bytes"] or 0),
                "index_bytes": int(r["index_bytes"] or 0),
            }
            for r in top_tables
        ]

        raw_tbl = conn.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM public.flight_offer_raw_meta) AS n_rows,
                    pg_total_relation_size('public.flight_offer_raw_meta'::regclass) AS total_bytes,
                    pg_relation_size('public.flight_offer_raw_meta'::regclass) AS heap_bytes,
                    pg_indexes_size('public.flight_offer_raw_meta'::regclass) AS index_bytes,
                    COALESCE((
                        SELECT pg_total_relation_size(c.reltoastrelid)
                        FROM pg_class c
                        WHERE c.oid = 'public.flight_offer_raw_meta'::regclass
                    ),0) AS toast_bytes
                """
            )
        ).mappings().first()
        raw_daily = conn.execute(
            text(
                f"""
                SELECT date_trunc('day', scraped_at)::date AS d, COUNT(*) AS c
                FROM public.flight_offer_raw_meta
                WHERE scraped_at >= NOW() - (:lookback_days || ' days')::interval
                GROUP BY 1
                ORDER BY 1 DESC
                """
            ),
            {"lookback_days": int(lookback_days)},
        ).mappings().all()
        row_size = conn.execute(
            text(
                """
                SELECT avg(pg_column_size(t)) AS avg_row_bytes,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY pg_column_size(t)) AS p50_row_bytes,
                       percentile_cont(0.9) WITHIN GROUP (ORDER BY pg_column_size(t)) AS p90_row_bytes
                FROM (
                    SELECT *
                    FROM public.flight_offer_raw_meta
                    ORDER BY id DESC
                    LIMIT 20000
                ) t
                """
            )
        ).mappings().first()

        metrics["flight_offer_raw_meta"] = {
            "row_count": int(raw_tbl["n_rows"] or 0),
            "total_bytes": int(raw_tbl["total_bytes"] or 0),
            "heap_bytes": int(raw_tbl["heap_bytes"] or 0),
            "index_bytes": int(raw_tbl["index_bytes"] or 0),
            "toast_bytes": int(raw_tbl["toast_bytes"] or 0),
            "daily_rows": [{"date": str(r["d"]), "count": int(r["c"])} for r in raw_daily],
            "sample_row_size": {
                "avg_bytes": float(row_size["avg_row_bytes"] or 0),
                "p50_bytes": float(row_size["p50_row_bytes"] or 0),
                "p90_bytes": float(row_size["p90_row_bytes"] or 0),
            },
        }

        payload_store_exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name='raw_offer_payload_store'
                )
                """
            )
        ).scalar()
        metrics["raw_payload_store"] = {"exists": bool(payload_store_exists)}
        if payload_store_exists:
            ps = conn.execute(
                text(
                    """
                    SELECT COUNT(*) AS n_rows,
                           COALESCE(SUM(seen_count),0) AS seen_total,
                           pg_total_relation_size('public.raw_offer_payload_store'::regclass) AS total_bytes
                    FROM public.raw_offer_payload_store
                    """
                )
            ).mappings().first()
            metrics["raw_payload_store"].update(
                {
                    "row_count": int(ps["n_rows"] or 0),
                    "seen_total": int(ps["seen_total"] or 0),
                    "total_bytes": int(ps["total_bytes"] or 0),
                }
            )

    return metrics


def build_assessment(metrics: dict, db_drive: dict | None, report_drive: dict | None, args) -> dict:
    db_bytes = metrics["database"]["bytes"]
    raw = metrics["flight_offer_raw_meta"]
    daily_counts = [x["count"] for x in raw.get("daily_rows", [])]
    today = daily_counts[0] if daily_counts else 0
    hist = daily_counts[1:] if len(daily_counts) > 1 else []
    avg7 = sum(hist[:7]) / min(len(hist[:7]), 7) if hist else 0.0
    avg14 = sum(hist[:14]) / min(len(hist[:14]), 14) if hist else 0.0
    med = float(median(hist)) if hist else 0.0

    row_count = raw.get("row_count") or 0
    total_bytes = raw.get("total_bytes") or 0
    bytes_per_row_disk = (total_bytes / row_count) if row_count else 0.0
    sample_avg_row = raw.get("sample_row_size", {}).get("avg_bytes") or 0.0
    bloat_ratio = (bytes_per_row_disk / sample_avg_row) if sample_avg_row else None

    free_bytes = (db_drive or {}).get("free_bytes")
    free_gb = (free_bytes / (1024**3)) if free_bytes is not None else None

    scenarios = {}
    for name, rows_per_day in {
        "today_rate": float(today),
        "avg7_excl_today": float(avg7),
        "avg14_excl_today": float(avg14),
        "median_excl_today": float(med),
    }.items():
        gb_per_day = (rows_per_day * bytes_per_row_disk) / (1024**3) if bytes_per_row_disk else 0.0
        runway_days = (free_gb / gb_per_day) if (free_gb and gb_per_day > 0) else None
        scenarios[name] = {
            "rows_per_day": round(rows_per_day, 2),
            "gb_per_day_est": round(gb_per_day, 3),
            "runway_days_est": round(runway_days, 1) if runway_days is not None else None,
        }

    severity = "PASS"
    reasons: list[str] = []
    if free_gb is not None:
        if free_gb <= args.fail_free_gb:
            severity = "FAIL"
            reasons.append(f"db_drive_free_gb={free_gb:.2f} <= fail_free_gb={args.fail_free_gb}")
        elif free_gb <= args.warn_free_gb and severity != "FAIL":
            severity = "WARN"
            reasons.append(f"db_drive_free_gb={free_gb:.2f} <= warn_free_gb={args.warn_free_gb}")
    if (db_bytes / (1024**3)) >= args.warn_db_gb and severity == "PASS":
        severity = "WARN"
        reasons.append(f"db_size_gb={db_bytes/(1024**3):.2f} >= warn_db_gb={args.warn_db_gb}")
    runway_ref = scenarios.get("avg14_excl_today", {}).get("runway_days_est")
    if runway_ref is not None:
        if runway_ref <= args.fail_runway_days:
            severity = "FAIL"
            reasons.append(f"runway_days_est(avg14)={runway_ref} <= fail_runway_days={args.fail_runway_days}")
        elif runway_ref <= args.warn_runway_days and severity != "FAIL":
            severity = "WARN"
            reasons.append(f"runway_days_est(avg14)={runway_ref} <= warn_runway_days={args.warn_runway_days}")
    if bloat_ratio is not None and bloat_ratio >= 2.0 and severity == "PASS":
        severity = "WARN"
        reasons.append(f"raw_meta_bloat_heuristic_ratio={bloat_ratio:.2f} (disk bytes/row vs sample row size)")

    return {
        "status": severity,
        "reasons": reasons,
        "bytes_per_row_disk_est": round(bytes_per_row_disk, 2),
        "raw_meta_bloat_heuristic_ratio": round(bloat_ratio, 2) if bloat_ratio is not None else None,
        "runway_scenarios": scenarios,
        "drives": {
            "db_drive": db_drive,
            "reports_drive": report_drive,
        },
    }


def write_outputs(out_dir: Path, ts: str, payload: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_json = out_dir / "db_storage_health_latest.json"
    ts_json = out_dir / f"db_storage_health_{ts}.json"
    latest_md = out_dir / "db_storage_health_latest.md"
    ts_md = out_dir / f"db_storage_health_{ts}.md"

    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    ts_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    db = payload["metrics"]["database"]
    raw = payload["metrics"]["flight_offer_raw_meta"]
    assess = payload["assessment"]
    top = payload["metrics"]["top_tables"][:5]
    lines = [
        f"# DB Storage Health ({payload['assessment']['status']})",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- DB: `{db['name']}` = `{_human_bytes(db['bytes'])}`",
        f"- DB drive free: `{_human_bytes((assess['drives']['db_drive'] or {}).get('free_bytes'))}`",
        f"- Raw meta table: `{_human_bytes(raw['total_bytes'])}` rows=`{raw['row_count']}`",
        f"- Raw-meta bloat heuristic ratio: `{assess.get('raw_meta_bloat_heuristic_ratio')}`",
        "",
        "## Runway Estimates",
        "",
    ]
    for k, v in assess["runway_scenarios"].items():
        lines.append(f"- `{k}`: rows/day=`{v['rows_per_day']}`, GB/day~`{v['gb_per_day_est']}`, runway~`{v['runway_days_est']}` days")
    if assess["reasons"]:
        lines.extend(["", "## Reasons", ""] + [f"- {r}" for r in assess["reasons"]])
    lines.extend(["", "## Top Tables", ""])
    for t in top:
        lines.append(f"- `{t['schema']}.{t['table']}` total=`{_human_bytes(t['total_bytes'])}` heap=`{_human_bytes(t['heap_bytes'])}` idx=`{_human_bytes(t['index_bytes'])}`")
    latest_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    ts_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return latest_json, ts_json, latest_md, ts_md


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    output_dir = Path(args.output_dir)

    masked_url = _extract_password_safe_url(args.db_url)
    metrics = collect_metrics(args.db_url, args.growth_lookback_days)
    db_drive = _safe_disk_usage_for_path(metrics.get("postgres", {}).get("data_directory"))
    report_drive = _safe_disk_usage_for_path(str(output_dir))
    assessment = build_assessment(metrics, db_drive, report_drive, args)

    payload = {
        "generated_at": now.isoformat(),
        "db_url": masked_url,
        "metrics": metrics,
        "assessment": assessment,
    }
    latest_json, ts_json, latest_md, ts_md = write_outputs(output_dir, ts, payload)
    print(f"status={assessment['status']}")
    print(f"latest_json={latest_json}")
    print(f"latest_md={latest_md}")
    print(f"run_json={ts_json}")
    print(f"run_md={ts_md}")

    if args.strict and assessment["status"] in {"WARN", "FAIL"}:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
