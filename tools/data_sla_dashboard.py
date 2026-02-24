"""
Data SLA dashboard:
- route coverage
- mandatory null rate
- accumulation success rate
- freshness lag
Includes threshold evaluation and optional webhook alerting.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url

LOG_TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[")
PIPELINE_RC_RE = re.compile(r"finished with rc=(?P<rc>\d+)")


def parse_args():
    p = argparse.ArgumentParser(description="Generate data SLA dashboard")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--routes-config", default="config/routes.json")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--lookback-hours", type=float, default=24.0)
    p.add_argument("--min-route-coverage-pct", type=float, default=80.0)
    p.add_argument("--max-null-rate-pct", type=float, default=10.0)
    p.add_argument(
        "--min-scrape-success-pct",
        "--min-accumulation-success-pct",
        dest="min_scrape_success_pct",
        type=float,
        default=85.0,
        help="Minimum successful accumulation run percentage (legacy alias: --min-scrape-success-pct)",
    )
    p.add_argument("--max-freshness-lag-hours", type=float, default=8.0)
    p.add_argument("--webhook-url", default=os.getenv("AIRLINE_OPS_WEBHOOK_URL", ""))
    p.add_argument("--channel", default="ops-alerts")
    p.add_argument("--strict", action="store_true")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _ratio(num: float, den: float):
    if not den:
        return 0.0
    return (float(num) / float(den)) * 100.0


def _load_expected_routes(routes_path: Path):
    if not routes_path.exists():
        return set()
    try:
        routes = json.loads(routes_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()
    return {(str(r.get("airline", "")).upper(), str(r.get("origin", "")).upper(), str(r.get("destination", "")).upper()) for r in routes}


def _scrape_success_pct(logs_dir: Path, cutoff: datetime):
    total = 0
    success = 0
    considered_files = []
    for name in ["scheduler_bg_live.err.log", "scheduler_vq_live.err.log", "always_on_maintenance.log"]:
        p = logs_dir / name
        if not p.exists():
            continue
        considered_files.append(str(p))
        with p.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                ts_match = LOG_TS_RE.match(line)
                if not ts_match:
                    continue
                try:
                    ts = datetime.strptime(ts_match.group("ts"), "%Y-%m-%d %H:%M:%S")
                    ts = ts.replace(tzinfo=cutoff.tzinfo)
                except ValueError:
                    continue
                if ts < cutoff:
                    continue
                rc_match = PIPELINE_RC_RE.search(line)
                if not rc_match:
                    continue
                total += 1
                if int(rc_match.group("rc")) == 0:
                    success += 1
    return {
        "total_runs": total,
        "success_runs": success,
        "success_pct": round(_ratio(success, total), 2) if total else 100.0,
        "files": considered_files,
    }


def _db_metrics(db_url: str, cutoff: datetime):
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    result = {
        "observed_routes": set(),
        "freshness_lag_hours": None,
        "null_rate_pct": None,
        "total_rows": 0,
        "null_rows": 0,
    }
    with eng.connect() as conn:
        obs = conn.execute(
            text(
                """
                SELECT DISTINCT airline, origin, destination
                FROM flight_offers
                WHERE scraped_at >= :cutoff
                """
            ),
            {"cutoff": cutoff.replace(tzinfo=None)},
        ).mappings().all()
        result["observed_routes"] = {(str(r["airline"]).upper(), str(r["origin"]).upper(), str(r["destination"]).upper()) for r in obs}

        max_scraped = conn.execute(text("SELECT MAX(scraped_at) AS m FROM flight_offers")).mappings().first()
        max_val = max_scraped["m"] if max_scraped else None
        if max_val is not None:
            lag = (datetime.now() - max_val).total_seconds() / 3600.0
            result["freshness_lag_hours"] = round(lag, 2)

        q = text(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(
                    CASE WHEN rm.adt_count IS NULL
                           OR rm.chd_count IS NULL
                           OR rm.inf_count IS NULL
                           OR rm.inventory_confidence IS NULL
                           OR rm.source_endpoint IS NULL
                           OR rm.departure_utc IS NULL
                         THEN 1 ELSE 0 END
                ) AS null_rows
            FROM flight_offers fo
            LEFT JOIN LATERAL (
                SELECT r.*
                FROM flight_offer_raw_meta r
                WHERE r.flight_offer_id = fo.id
                ORDER BY r.id DESC
                LIMIT 1
            ) rm ON TRUE
            WHERE fo.scraped_at >= :cutoff
            """
        )
        row = conn.execute(q, {"cutoff": cutoff.replace(tzinfo=None)}).mappings().first()
        total_rows = int(row["total_rows"] or 0)
        null_rows = int(row["null_rows"] or 0)
        result["total_rows"] = total_rows
        result["null_rows"] = null_rows
        result["null_rate_pct"] = round(_ratio(null_rows, total_rows), 4) if total_rows else 0.0
    return result


def _evaluate(metrics: dict, args):
    checks = []

    checks.append(
        {
            "name": "route_coverage_pct",
            "value": metrics["route_coverage_pct"],
            "threshold": f">= {args.min_route_coverage_pct}",
            "ok": metrics["route_coverage_pct"] >= args.min_route_coverage_pct,
        }
    )
    checks.append(
        {
            "name": "mandatory_null_rate_pct",
            "value": metrics["null_rate_pct"],
            "threshold": f"<= {args.max_null_rate_pct}",
            "ok": metrics["null_rate_pct"] <= args.max_null_rate_pct,
        }
    )
    checks.append(
        {
            "name": "accumulation_success_pct",
            "legacy_name": "scrape_success_pct",
            "value": metrics["accumulation_success_pct"],
            "threshold": f">= {args.min_scrape_success_pct}",
            "ok": metrics["accumulation_success_pct"] >= args.min_scrape_success_pct,
        }
    )
    lag = metrics["freshness_lag_hours"] if metrics["freshness_lag_hours"] is not None else 9999.0
    checks.append(
        {
            "name": "freshness_lag_hours",
            "value": lag,
            "threshold": f"<= {args.max_freshness_lag_hours}",
            "ok": lag <= args.max_freshness_lag_hours,
        }
    )

    failed = [c["name"] for c in checks if not c["ok"]]
    status = "PASS" if not failed else ("FAIL" if len(failed) >= 2 else "WARN")
    return status, checks, failed


def _notify(webhook_url: str, channel: str, payload: dict):
    if not webhook_url:
        return False, "no_webhook_configured"
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:  # nosec B310
        return 200 <= int(resp.getcode()) < 300, f"http_{int(resp.getcode())}"


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    cutoff = now - timedelta(hours=max(1.0, args.lookback_hours))
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    expected = _load_expected_routes(Path(args.routes_config))
    dbm = _db_metrics(args.db_url, cutoff)
    lm = _scrape_success_pct(Path(args.logs_dir), cutoff)

    observed = dbm["observed_routes"]
    route_cov = round(_ratio(len(observed), len(expected)), 2) if expected else 100.0

    metrics = {
        "expected_route_count": len(expected),
        "observed_route_count": len(observed),
        "route_coverage_pct": route_cov,
        "total_rows": dbm["total_rows"],
        "null_rows": dbm["null_rows"],
        "null_rate_pct": dbm["null_rate_pct"],
        "freshness_lag_hours": dbm["freshness_lag_hours"],
        "accumulation_runs_total": lm["total_runs"],
        "accumulation_runs_success": lm["success_runs"],
        "accumulation_success_pct": lm["success_pct"],
    }
    # Compatibility aliases (legacy terminology)
    metrics["scrape_runs_total"] = metrics["accumulation_runs_total"]
    metrics["scrape_runs_success"] = metrics["accumulation_runs_success"]
    metrics["scrape_success_pct"] = metrics["accumulation_success_pct"]

    status, checks, failed = _evaluate(metrics, args)
    missing_routes = sorted(expected - observed)

    alert_sent = None
    alert_detail = ""
    if status in {"WARN", "FAIL"}:
        ok, detail = _notify(
            args.webhook_url,
            args.channel,
            {
                "channel": args.channel,
                "title": f"[{status}] Data SLA Dashboard",
                "status": status,
                "failed_checks": failed,
                "metrics": metrics,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        alert_sent = ok
        alert_detail = detail

    payload = {
        "generated_at": now.isoformat(),
        "lookback_hours": args.lookback_hours,
        "status": status,
        "failed_checks": failed,
        "checks": checks,
        "metrics": metrics,
        "missing_routes": [f"{a}:{o}-{d}" for a, o, d in missing_routes],
        "log_files": lm["files"],
        "alert_sent": alert_sent,
        "alert_detail": alert_detail,
    }

    latest_json = reports_dir / "data_sla_latest.json"
    run_json = reports_dir / f"data_sla_{ts}.json"
    latest_md = reports_dir / "data_sla_latest.md"
    run_md = reports_dir / f"data_sla_{ts}.md"
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Data SLA Dashboard",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Lookback hours: `{args.lookback_hours}`",
        f"- Status: **{status}**",
        f"- Failed checks: `{', '.join(failed) if failed else 'none'}`",
        "",
        "## Metrics",
        "",
    ]
    for k, v in metrics.items():
        lines.append(f"- `{k}`: `{v}`")
    lines.extend(["", "## Checks", ""])
    for c in checks:
        lines.append(f"- `{c['name']}`: `value={c['value']}` threshold `{c['threshold']}` -> **{'PASS' if c['ok'] else 'FAIL'}**")
    lines.extend(["", "## Missing Routes", ""])
    if missing_routes:
        lines.extend([f"- `{a}:{o}-{d}`" for a, o, d in missing_routes[:100]])
    else:
        lines.append("- none")
    md_text = "\n".join(lines) + "\n"
    latest_md.write_text(md_text, encoding="utf-8")
    run_md.write_text(md_text, encoding="utf-8")

    print(f"sla_status={status} failed={failed}")
    print(f"latest_json={latest_json}")
    print(f"latest_md={latest_md}")
    if args.strict and status != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
