import argparse
import csv
import datetime as dt
import json
import logging
import os
import signal
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL


LOG = logging.getLogger("run_pipeline")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
SCHEDULE_FILE = Path("config/schedule.json")
RUN_ALL_DEFAULT_DATES_FILE = Path("config/dates.json")
REPO_ROOT = Path(__file__).resolve().parent
REPORTS_ROOT = REPO_ROOT / "output" / "reports"
RECOVERY_STATUS_FILE = REPORTS_ROOT / "accumulation_recovery_latest.json"
CYCLE_STATE_FILE = REPORTS_ROOT / "accumulation_cycle_latest.json"
HEARTBEAT_STATUS_FILE = REPORTS_ROOT / "run_all_accumulation_status_latest.json"
PARALLEL_STATUS_FILE = REPORTS_ROOT / "scrape_parallel_latest.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Run accumulation + reports as one pipeline")
    parser.add_argument("--python-exe", default=sys.executable, help="Python executable to run child scripts")
    parser.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL), help="Postgres URL")
    parser.add_argument(
        "--trip-plan-mode",
        choices=["operational", "training", "deep"],
        default=os.getenv("RUN_ALL_TRIP_PLAN_MODE", "operational"),
        help="Trip planning mode. 'operational' uses comparison-safe active route profiles; 'training' adds daily core enrichment; 'deep' enables the broadest weekly enrichment set.",
    )

    # accumulation filters
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--date-start", help="Inclusive range start (YYYY-MM-DD)")
    parser.add_argument("--date-end", help="Inclusive range end (YYYY-MM-DD)")
    parser.add_argument("--dates", help="Comma-separated YYYY-MM-DD values for dynamic search windows")
    parser.add_argument("--date-offsets", help="Comma-separated day offsets from today")
    parser.add_argument("--dates-file", help="Optional dates config file path")
    parser.add_argument("--schedule-file", default=str(SCHEDULE_FILE), help="Optional scheduler config file for auto-run date defaults")
    parser.add_argument("--cabin")
    parser.add_argument("--adt", type=int, default=1, help="Adult passenger count for accumulation requests (default: 1)")
    parser.add_argument("--chd", type=int, default=0, help="Child passenger count for accumulation requests (default: 0)")
    parser.add_argument("--inf", type=int, default=0, help="Infant passenger count for accumulation requests (default: 0)")
    parser.add_argument("--probe-group-id", help="Optional identifier linking multi-passenger probe runs")
    parser.add_argument("--route-scope", choices=["all", "domestic", "international"], default="all")
    parser.add_argument("--market-country", default="BD", help="Domestic market country (e.g., BD, IN)")
    parser.add_argument("--strict-route-audit", action="store_true", help="Fail fast if route config audit finds blocking issues")
    parser.add_argument("--limit-routes", type=int)
    parser.add_argument("--limit-dates", type=int)
    parser.add_argument("--parallel-airlines", type=int, default=1, help="Run one run_all process per airline in parallel when airline filter is not set")
    parser.add_argument(
        "--query-timeout-seconds",
        type=float,
        default=120.0,
        help="Soft timeout per search query passed to run_all (default: 120s).",
    )
    parser.add_argument(
        "--cycle-id",
        help="Optional shared cycle UUID. Reuse this to resume an interrupted accumulation cycle.",
    )
    parser.add_argument("--profile-runtime", action="store_true", help="Enable runtime profiling output from run_all")
    parser.add_argument(
        "--skip-scrape",
        "--skip-accumulation",
        dest="skip_scrape",
        action="store_true",
        help="Skip accumulation step (legacy alias: --skip-scrape)",
    )

    # reports
    parser.add_argument("--skip-reports", action="store_true")
    parser.add_argument("--report-start-date", help="YYYY-MM-DD")
    parser.add_argument("--report-end-date", help="YYYY-MM-DD")
    parser.add_argument("--report-format", choices=["csv", "xlsx", "both"], default="both")
    parser.add_argument("--report-output-dir", default="output/reports")
    parser.add_argument("--report-timestamp-tz", choices=["local", "utc"], default="local")
    parser.add_argument("--route-monitor", action="store_true", help="Also generate route_flight_fare_monitor workbook")
    parser.add_argument(
        "--route-monitor-macro-xlsm",
        action="store_true",
        help="When --route-monitor is enabled, also export macro-enabled .xlsm workbook.",
    )
    parser.add_argument(
        "--route-monitor-macro-xlsm-path",
        help="Optional explicit output path for route monitor macro workbook.",
    )

    # prediction
    parser.add_argument(
        "--run-prediction",
        dest="run_prediction",
        action="store_true",
        help="Run the ML+DL prediction step (default: enabled).",
    )
    parser.add_argument(
        "--skip-prediction",
        dest="run_prediction",
        action="store_false",
        help="Skip the ML+DL prediction step.",
    )
    parser.set_defaults(run_prediction=True)
    parser.add_argument(
        "--prediction-target",
        choices=[
            "total_change_events",
            "price_events",
            "availability_events",
            "min_price_bdt",
            "avg_seat_available",
            "offers_count",
            "soldout_rate",
        ],
        default="total_change_events",
    )
    parser.add_argument(
        "--prediction-series-mode",
        choices=["event_daily", "search_dynamic"],
        default="event_daily",
    )
    parser.add_argument("--prediction-departure-start-date", help="YYYY-MM-DD departure lower bound for search_dynamic")
    parser.add_argument("--prediction-departure-end-date", help="YYYY-MM-DD departure upper bound for search_dynamic")
    parser.add_argument("--prediction-disable-backtest", action="store_true")
    parser.add_argument(
        "--prediction-ml-models",
        default="none",
        help="Comma-separated ML models for prediction: catboost,lightgbm (default: none)",
    )
    parser.add_argument("--prediction-ml-quantiles", default="0.1,0.5,0.9")
    parser.add_argument("--prediction-ml-min-history", type=int, default=14)
    parser.add_argument("--prediction-ml-random-seed", type=int, default=42)
    parser.add_argument(
        "--prediction-dl-models",
        default="mlp",
        help="Comma-separated DL models for prediction: mlp (default: mlp)",
    )
    parser.add_argument("--prediction-dl-quantiles", default="0.1,0.5,0.9")
    parser.add_argument("--prediction-dl-min-history", type=int, default=8)
    parser.add_argument("--prediction-dl-random-seed", type=int, default=42)
    parser.add_argument("--prediction-backtest-selection-metric", choices=["mae", "rmse"], default="mae")
    parser.add_argument("--prediction-backtest-model-min-coverage-ratio", type=float, default=0.8)

    # unified intelligence hub
    parser.add_argument("--run-intelligence-hub", action="store_true")
    parser.add_argument("--intel-lookback-days", type=int, default=14)
    parser.add_argument(
        "--intel-forecast-target",
        choices=["min_price_bdt", "avg_seat_available", "offers_count", "soldout_rate"],
        default="min_price_bdt",
    )

    # alert quality
    parser.add_argument("--run-alert-eval", action="store_true")
    parser.add_argument("--alert-lookback-days", type=int, default=7)
    parser.add_argument("--alert-spike-threshold", type=float, default=250.0)
    parser.add_argument("--alert-sellout-threshold", type=float, default=1.0)
    parser.add_argument("--alert-spike-false-alarm-cost", type=float, default=1.0)
    parser.add_argument("--alert-spike-missed-cost", type=float, default=3.0)
    parser.add_argument("--alert-sellout-false-alarm-cost", type=float, default=2.0)
    parser.add_argument("--alert-sellout-missed-cost", type=float, default=8.0)

    # warehouse sync
    parser.add_argument(
        "--skip-bigquery-sync",
        action="store_true",
        help="Skip automatic BigQuery warehouse sync after a successful pipeline run.",
    )
    parser.add_argument(
        "--bigquery-sync-lookback-days",
        type=int,
        default=max(1, int(os.getenv("BIGQUERY_SYNC_LOOKBACK_DAYS", "7"))),
        help="Rolling UTC capture-date window exported to BigQuery after a successful run (default: 7 days).",
    )
    parser.add_argument(
        "--bigquery-sync-output-dir",
        default=os.getenv("BIGQUERY_SYNC_OUTPUT_DIR", "output/warehouse/bigquery"),
        help="Base output directory for staged BigQuery parquet exports.",
    )
    parser.add_argument(
        "--bigquery-project-id",
        default=os.getenv("BIGQUERY_PROJECT_ID", "").strip() or None,
        help="BigQuery project id for automatic warehouse sync.",
    )
    parser.add_argument(
        "--bigquery-dataset",
        default=os.getenv("BIGQUERY_DATASET", "").strip() or None,
        help="BigQuery dataset for automatic warehouse sync.",
    )
    parser.add_argument(
        "--fail-on-bigquery-sync-error",
        action="store_true",
        help="Fail the overall pipeline if the automatic BigQuery sync step fails.",
    )

    parser.add_argument("--fail-fast", action="store_true", help="Stop immediately on first step failure")
    return parser.parse_args()


def _has_explicit_scrape_date_selection(args) -> bool:
    return bool(
        args.date
        or args.dates
        or args.date_start
        or args.date_end
        or args.date_offsets
        or args.dates_file
    )


def _parse_iso_date_list(values) -> list[str]:
    out = []
    seen = set()
    for raw in values or []:
        s = str(raw or "").strip()
        if not s:
            continue
        try:
            d = dt.date.fromisoformat(s)
        except Exception:
            continue
        iso = d.isoformat()
        if iso not in seen:
            seen.add(iso)
            out.append(iso)
    return out


def _expand_date_range(start_raw, end_raw) -> list[str]:
    try:
        start = dt.date.fromisoformat(str(start_raw).strip())
        end = dt.date.fromisoformat(str(end_raw).strip())
    except Exception:
        return []
    if end < start:
        start, end = end, start
    return [(start + dt.timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def _parse_offsets_csv(raw: str) -> list[int]:
    vals = []
    seen = set()
    for part in str(raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            n = int(s)
        except Exception:
            continue
        if n not in seen:
            seen.add(n)
            vals.append(n)
    return vals


def _drop_past_iso_dates(values: list[str], *, today: dt.date) -> list[str]:
    kept: list[str] = []
    for raw in values or []:
        s = str(raw or "").strip()
        if not s:
            continue
        try:
            parsed = dt.date.fromisoformat(s)
        except Exception:
            continue
        iso = parsed.isoformat()
        if parsed < today:
            continue
        if iso not in kept:
            kept.append(iso)
    return kept


def _has_future_iso_date(values: list[str], *, today: dt.date) -> bool:
    for raw in values or []:
        s = str(raw or "").strip()
        if not s:
            continue
        try:
            parsed = dt.date.fromisoformat(s)
        except Exception:
            continue
        if parsed > today:
            return True
    return False


def _ensure_at_least_one_future_iso_date(values: list[str], *, today: dt.date) -> list[str]:
    normalized = _drop_past_iso_dates(values, today=today)
    if _has_future_iso_date(normalized, today=today):
        return normalized
    fallback = (today + dt.timedelta(days=1)).isoformat()
    if fallback not in normalized:
        normalized.append(fallback)
    return normalized


def _ensure_weekday_coverage(values: list[str], *, today: dt.date) -> list[str]:
    normalized = _drop_past_iso_dates(values, today=today)
    present_weekdays: set[int] = set()
    anchor_date = today
    for value in normalized:
        try:
            parsed = dt.date.fromisoformat(value)
        except Exception:
            continue
        present_weekdays.add(parsed.weekday())
        if parsed > anchor_date:
            anchor_date = parsed

    additions: list[str] = []
    anchor_weekday = anchor_date.weekday()
    for weekday in range(7):
        if weekday in present_weekdays:
            continue
        delta = (weekday - anchor_weekday) % 7
        if delta == 0:
            delta = 7
        candidate = (anchor_date + dt.timedelta(days=delta)).isoformat()
        if candidate not in normalized and candidate not in additions:
            additions.append(candidate)

    additions.sort()
    return normalized + additions


def _finalize_outbound_dates(values: list[str], *, today: dt.date, limit_dates: int | None = None) -> list[str]:
    dates = _drop_past_iso_dates(values, today=today)
    if not dates:
        dates = [today.isoformat()]
    if limit_dates and limit_dates > 0:
        dates = dates[:limit_dates]
    dates = _ensure_at_least_one_future_iso_date(dates, today=today)
    dates = _ensure_weekday_coverage(dates, today=today)
    return dates


def _load_dates_from_file_pipeline(path: Path, today: dt.date) -> list[str]:
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(obj, list):
        return _parse_iso_date_list(obj)
    if isinstance(obj, dict):
        if isinstance(obj.get("dates"), list):
            parsed = _parse_iso_date_list(obj["dates"])
            if parsed:
                return parsed
        if obj.get("date_start") and obj.get("date_end"):
            parsed = _expand_date_range(obj.get("date_start"), obj.get("date_end"))
            if parsed:
                return parsed
        if obj.get("start_date") and obj.get("end_date"):
            parsed = _expand_date_range(obj.get("start_date"), obj.get("end_date"))
            if parsed:
                return parsed
        if isinstance(obj.get("date_range"), dict):
            parsed = _expand_date_range(
                obj["date_range"].get("start") or obj["date_range"].get("date_start"),
                obj["date_range"].get("end") or obj["date_range"].get("date_end"),
            )
            if parsed:
                return parsed
        if isinstance(obj.get("date_ranges"), list):
            merged = []
            for item in obj["date_ranges"]:
                if not isinstance(item, dict):
                    continue
                parsed = _expand_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                )
                for d in parsed:
                    if d not in merged:
                        merged.append(d)
            if merged:
                return merged
        if isinstance(obj.get("day_offsets"), list):
            offs = []
            for v in obj["day_offsets"]:
                try:
                    offs.append(int(v))
                except Exception:
                    continue
            return [(today + dt.timedelta(days=o)).isoformat() for o in dict.fromkeys(offs)]
    return []


def _load_schedule_date_defaults(path: Path) -> dict:
    obj = _load_schedule_file_obj(path)
    if not obj:
        return {}
    root = obj.get("auto_run_date_ranges")
    return root if isinstance(root, dict) else {}


def _load_schedule_file_obj(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed to parse schedule file %s: %s", path, exc)
        return {}
    return obj if isinstance(obj, dict) else {}


def _apply_schedule_date_defaults_pipeline(args) -> None:
    schedule_obj = _load_schedule_file_obj(Path(args.schedule_file))
    root = schedule_obj.get("auto_run_date_ranges") if isinstance(schedule_obj.get("auto_run_date_ranges"), dict) else {}
    if not root:
        return

    default_section = root.get("default") if isinstance(root.get("default"), dict) else {}
    pipeline_section = root.get("run_pipeline") if isinstance(root.get("run_pipeline"), dict) else {}
    # Backward-compatible section names:
    # - legacy: "scrape"
    # - preferred: "accumulation"
    scrape_section = root.get("scrape") if isinstance(root.get("scrape"), dict) else {}
    accumulation_section = root.get("accumulation") if isinstance(root.get("accumulation"), dict) else {}
    report_section = root.get("report") if isinstance(root.get("report"), dict) else {}
    prediction_section = root.get("prediction") if isinstance(root.get("prediction"), dict) else {}

    # merged precedence inside schedule file: default < run_pipeline < legacy-scrape < accumulation
    merged_scrape = {}
    for s in (default_section, pipeline_section, scrape_section, accumulation_section):
        merged_scrape.update(s)

    applied = []
    if not _has_explicit_scrape_date_selection(args):
        if bool(merged_scrape.get("combine")):
            today = datetime.now(timezone.utc).date()
            combined: list[str] = []

            def _add_many(values: list[str]):
                for v in values:
                    if v and v not in combined:
                        combined.append(v)

            if merged_scrape.get("date"):
                _add_many(_parse_iso_date_list([merged_scrape.get("date")]))
            dates_val = merged_scrape.get("dates")
            if dates_val:
                if isinstance(dates_val, list):
                    _add_many(_parse_iso_date_list(dates_val))
                else:
                    _add_many(_parse_iso_date_list(str(dates_val).split(",")))
            ds = merged_scrape.get("date_start")
            de = merged_scrape.get("date_end")
            if ds and de:
                _add_many(_expand_date_range(ds, de))
            elif ds or de:
                _add_many(_parse_iso_date_list([ds or de]))
            date_ranges = merged_scrape.get("date_ranges")
            if isinstance(date_ranges, list):
                for item in date_ranges:
                    if not isinstance(item, dict):
                        continue
                    _add_many(
                        _expand_date_range(
                            item.get("start") or item.get("date_start"),
                            item.get("end") or item.get("date_end"),
                        )
                    )
            offs = merged_scrape.get("date_offsets")
            if isinstance(offs, list):
                parsed_offs = []
                for v in offs:
                    try:
                        parsed_offs.append(int(v))
                    except Exception:
                        continue
                _add_many([(today + dt.timedelta(days=o)).isoformat() for o in parsed_offs])
            elif isinstance(offs, str) and offs.strip():
                _add_many([(today + dt.timedelta(days=o)).isoformat() for o in _parse_offsets_csv(offs)])
            if merged_scrape.get("dates_file"):
                _add_many(_load_dates_from_file_pipeline(Path(str(merged_scrape.get("dates_file"))), today=today))

            if combined:
                combined = _finalize_outbound_dates(combined, today=today, limit_dates=args.limit_dates)
                args.dates = ",".join(combined)
                applied.append(f"combine=true dates={args.dates}")

        if not args.dates:
            for attr in ("date", "date_start", "date_end", "dates", "dates_file"):
                if getattr(args, attr, None):
                    continue
                val = merged_scrape.get(attr)
                if val in (None, "", []):
                    continue
                if attr == "dates" and isinstance(val, list):
                    val = ",".join(str(v) for v in val)
                setattr(args, attr, str(val))
                applied.append(f"{attr}={getattr(args, attr)}")

        if not getattr(args, "date_offsets", None) and not args.dates:
            offs = merged_scrape.get("date_offsets")
            if isinstance(offs, list) and offs:
                args.date_offsets = ",".join(str(int(x)) for x in offs)
                applied.append(f"date_offsets={args.date_offsets}")
            elif isinstance(offs, str) and offs.strip():
                args.date_offsets = offs.strip()
                applied.append(f"date_offsets={args.date_offsets}")

        resolved_schedule_dates = _resolve_scrape_dates_for_log(args)
        if resolved_schedule_dates:
            args.dates = ",".join(resolved_schedule_dates)
            args.date = None
            args.date_start = None
            args.date_end = None
            args.date_offsets = None
            args.dates_file = None
            applied.append(f"resolved_dates={args.dates}")

    # Optional report date window defaults (applies only if not explicitly set)
    report_start = report_section.get("start_date") or report_section.get("report_start_date")
    report_end = report_section.get("end_date") or report_section.get("report_end_date")
    if not args.report_start_date and report_start:
        args.report_start_date = str(report_start)
        applied.append(f"report_start_date={args.report_start_date}")
    if not args.report_end_date and report_end:
        args.report_end_date = str(report_end)
        applied.append(f"report_end_date={args.report_end_date}")

    # Optional prediction departure date defaults
    pred_dep_start = prediction_section.get("departure_start_date")
    pred_dep_end = prediction_section.get("departure_end_date")
    if not args.prediction_departure_start_date and pred_dep_start:
        args.prediction_departure_start_date = str(pred_dep_start)
        applied.append(f"prediction_departure_start_date={args.prediction_departure_start_date}")
    if not args.prediction_departure_end_date and pred_dep_end:
        args.prediction_departure_end_date = str(pred_dep_end)
        applied.append(f"prediction_departure_end_date={args.prediction_departure_end_date}")

    # Optional default parallelism from schedule root-level concurrency.
    # Applies only when caller did not pin a specific airline and did not request custom worker count (>1).
    try:
        schedule_concurrency = int(schedule_obj.get("concurrency") or 0)
    except Exception:
        schedule_concurrency = 0
    if not args.airline and int(args.parallel_airlines or 1) <= 1 and schedule_concurrency > 1:
        # Keep scheduled launches aggressive, but cap them below the unstable max fan-out.
        args.parallel_airlines = max(1, min(schedule_concurrency, 4))
        applied.append(f"parallel_airlines={args.parallel_airlines} (from schedule.concurrency={schedule_concurrency})")

    if applied:
        LOG.info("Applied auto-run date defaults from %s: %s", args.schedule_file, ", ".join(applied))


def _load_execution_plan(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed to parse schedule file %s for execution plan: %s", path, exc)
        return {}
    if not isinstance(obj, dict):
        return {}
    plan = obj.get("execution_plan")
    return plan if isinstance(plan, dict) else {}


def _collect_expected_airlines_from_routes(routes_file: Path) -> list[str]:
    if not routes_file.exists():
        return []
    try:
        obj = json.loads(routes_file.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed to parse routes file %s: %s", routes_file, exc)
        return []
    if not isinstance(obj, list):
        return []
    seen = set()
    out = []
    for row in obj:
        if not isinstance(row, dict):
            continue
        airline = str(row.get("airline") or "").upper().strip()
        if not airline or airline in seen:
            continue
        seen.add(airline)
        out.append(airline)
    return sorted(out)


def _collect_observed_airline_row_counts(combined_csv: Path) -> dict:
    counts = {}
    if not combined_csv.exists():
        return counts
    try:
        with combined_csv.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                airline = str(row.get("airline") or "").upper().strip()
                if not airline:
                    continue
                counts[airline] = counts.get(airline, 0) + 1
    except Exception as exc:
        LOG.warning("Failed to compute observed airline counts from %s: %s", combined_csv, exc)
    return counts


def _read_latest_cycle_id(status_path: Path = Path("output/reports/run_all_status_latest.json")) -> str | None:
    if not status_path.exists():
        return None
    try:
        obj = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    sid = str(obj.get("cycle_id") or obj.get("scrape_id") or "").strip()
    return sid or None


def _collect_observed_airline_row_counts_db(*, db_url: str, cycle_id: str | None) -> dict:
    if not cycle_id:
        return {}
    try:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        sql = text(
            """
            SELECT UPPER(COALESCE(airline, '')) AS airline, COUNT(*) AS row_count
            FROM flight_offers
            WHERE scrape_id::text = :cycle_id
            GROUP BY UPPER(COALESCE(airline, ''))
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"cycle_id": str(cycle_id)}).fetchall()
        out = {}
        for airline, row_count in rows:
            a = str(airline or "").strip().upper()
            if not a:
                continue
            out[a] = int(row_count or 0)
        return out
    except Exception as exc:
        LOG.warning("Failed to compute observed airline counts from DB cycle_id=%s: %s", cycle_id, exc)
        return {}


def _compute_all_airline_coverage(plan: dict, *, db_url: str) -> dict:
    gate = plan.get("coverage_gate") if isinstance(plan.get("coverage_gate"), dict) else {}
    enabled = bool(gate.get("enabled"))

    routes_file_raw = gate.get("routes_file") or "config/routes.json"
    routes_file = Path(str(routes_file_raw))
    if not routes_file.is_absolute():
        routes_file = REPO_ROOT / routes_file

    try:
        min_rows = int(gate.get("minimum_rows_per_airline") or 1)
    except Exception:
        min_rows = 1
    min_rows = max(1, min_rows)

    expected = _collect_expected_airlines_from_routes(routes_file)
    cycle_id = _read_latest_cycle_id()
    observed_counts = _collect_observed_airline_row_counts_db(db_url=db_url, cycle_id=cycle_id)
    observed_source = "db_cycle"
    if not observed_counts:
        observed_counts = _collect_observed_airline_row_counts(Path("output/latest/combined_results.csv"))
        observed_source = "combined_csv"

    covered = []
    missing = []
    for airline in expected:
        if int(observed_counts.get(airline, 0)) >= min_rows:
            covered.append(airline)
        else:
            missing.append(airline)

    coverage_pct = 0.0
    if expected:
        coverage_pct = round((100.0 * len(covered)) / len(expected), 2)

    return {
        "enabled": enabled,
        "routes_file": str(routes_file),
        "minimum_rows_per_airline": min_rows,
        "expected_airlines": expected,
        "observed_row_counts": observed_counts,
        "observed_source": observed_source,
        "cycle_id": cycle_id,
        "covered_airlines": covered,
        "missing_airlines": missing,
        "coverage_pct": coverage_pct,
        "coverage_gate_passed": bool(expected) and not missing,
    }


def _write_execution_plan_status(report_output_dir: str, plan: dict, coverage: dict, pipeline_rc: int) -> Path | None:
    if not plan:
        return None
    out_dir = Path(report_output_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        LOG.warning("Failed to create report output dir %s for execution plan status: %s", out_dir, exc)
        return None

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    gate_passed = bool(coverage.get("coverage_gate_passed"))

    recommended_next_phase = plan.get("current_phase")
    if gate_passed:
        recommended_next_phase = "ota_discount_markup_calculation"

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ultimate_priority_goal": plan.get("ultimate_priority_goal"),
        "current_phase": plan.get("current_phase"),
        "phase_sequence": plan.get("phase_sequence"),
        "coverage_summary": coverage,
        "pipeline_rc": int(pipeline_rc or 0),
        "recommended_next_phase": recommended_next_phase,
    }

    latest_json = out_dir / "pipeline_execution_plan_latest.json"
    run_json = out_dir / f"pipeline_execution_plan_{stamp}.json"
    try:
        text_payload = json.dumps(payload, indent=2, ensure_ascii=False)
        latest_json.write_text(text_payload + "\n", encoding="utf-8")
        run_json.write_text(text_payload + "\n", encoding="utf-8")
    except Exception as exc:
        LOG.warning("Failed writing execution plan status artifacts: %s", exc)
        return None

    return latest_json


def _count_column_events(db_url: str):
    try:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            return conn.execute(text("SELECT count(*) FROM airline_intel.column_change_events")).scalar()
    except Exception as exc:
        LOG.warning("Could not read column_change_events count: %s", exc)
        return None


def _add_arg(cmd: list[str], flag: str, value):
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _parse_cycle_state_ts(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _cycle_scope_score(payload: dict) -> tuple[int, int, int]:
    try:
        airline_count = int(payload.get("aggregate_airline_count") or 0)
    except (TypeError, ValueError):
        airline_count = 0
    try:
        query_total = int(payload.get("overall_query_total") or 0)
    except (TypeError, ValueError):
        query_total = 0
    try:
        total_rows = int(payload.get("total_rows_accumulated") or 0)
    except (TypeError, ValueError):
        total_rows = 0
    return (airline_count, query_total, total_rows)


def _is_global_cycle_candidate(payload: dict) -> bool:
    airline_count, query_total, _ = _cycle_scope_score(payload)
    return airline_count >= 5 and query_total >= 20


def _should_replace_cycle_state(existing: dict, candidate: dict) -> bool:
    if not existing:
        return True
    existing_cycle = str(existing.get("cycle_id") or "").strip()
    candidate_cycle = str(candidate.get("cycle_id") or "").strip()
    if existing_cycle and candidate_cycle and existing_cycle == candidate_cycle:
        return True

    existing_state = str(existing.get("state") or "").strip().lower()
    candidate_state = str(candidate.get("state") or "").strip().lower()
    existing_ts = (
        _parse_cycle_state_ts(existing.get("started_at_utc"))
        or _parse_cycle_state_ts(existing.get("checked_at_utc"))
        or _parse_cycle_state_ts(existing.get("completed_at_utc"))
    )
    candidate_ts = (
        _parse_cycle_state_ts(candidate.get("started_at_utc"))
        or _parse_cycle_state_ts(candidate.get("checked_at_utc"))
        or _parse_cycle_state_ts(candidate.get("completed_at_utc"))
    )

    if (
        existing_state in {"starting", "running"}
        and candidate_state in {"starting", "running", "completed"}
        and existing_ts is not None
        and candidate_ts is not None
        and existing_ts > candidate_ts
    ):
        return False
    if (
        existing_state == "completed"
        and candidate_state == "completed"
        and existing_cycle != candidate_cycle
        and _is_global_cycle_candidate(existing)
        and not _is_global_cycle_candidate(candidate)
    ):
        return False
    return True


def _read_json(path: Path) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _db_health_check_payload(db_url: str) -> dict:
    try:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "ok": True,
            "reason": "db_reachable",
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": "db_unreachable",
            "detail": str(exc),
        }


def _read_cycle_heartbeat(cycle_id: str | None) -> dict:
    payload = _read_json(HEARTBEAT_STATUS_FILE)
    if not cycle_id:
        return payload
    payload_cycle = str(payload.get("cycle_id") or payload.get("scrape_id") or "").strip()
    return payload if payload_cycle == str(cycle_id).strip() else {}


def _read_cycle_parallel_status(cycle_id: str | None) -> dict:
    payload = _read_json(PARALLEL_STATUS_FILE)
    if not cycle_id:
        return payload
    payload_cycle = str(payload.get("cycle_id") or "").strip()
    return payload if payload_cycle == str(cycle_id).strip() else {}


def _sync_accumulation_artifacts(
    *,
    args,
    cycle_id: str | None,
    lifecycle_state: str,
    action: str,
    reason: str,
    command: list[str],
    started_at_utc: str | None,
    return_code: int | None = None,
) -> None:
    checked_at = datetime.now(timezone.utc).isoformat()
    heartbeat = _read_cycle_heartbeat(cycle_id)
    parallel_status = _read_cycle_parallel_status(cycle_id)
    db_check = _db_health_check_payload(args.db_url)

    recovery_payload = {
        "mode": "run_pipeline",
        "root": str(REPO_ROOT),
        "reports_dir": str(REPORTS_ROOT),
        "status_file": str(HEARTBEAT_STATUS_FILE),
        "state_file": str(RECOVERY_STATUS_FILE),
        "active_pipeline_process_count": None,
        "active_pipeline_processes": [],
        "heartbeat_state": heartbeat.get("state") if heartbeat else None,
        "heartbeat_accumulation_run_id": cycle_id,
        "heartbeat_age_minutes": None,
        "lock_file": str(REPORTS_ROOT / "accumulation_wrapper_lock.json"),
        "lock_present": (REPORTS_ROOT / "accumulation_wrapper_lock.json").exists(),
        "lock_age_minutes": None,
        "lock_created_at_utc": None,
        "db_check": db_check,
        "action": action,
        "wrapper_event": f"run_pipeline_{action}",
        "reason": reason,
        "launched": lifecycle_state in {"starting", "running"},
        "checked_at_utc": checked_at,
        "cycle_id": cycle_id,
        "command": command,
        "return_code": return_code,
    }
    _write_json(RECOVERY_STATUS_FILE, recovery_payload)

    cycle_payload = {
        "state": lifecycle_state,
        "status_source": "run_pipeline",
        "mode": "run_pipeline",
        "action": action,
        "wrapper_event": f"run_pipeline_{action}",
        "reason": reason,
        "checked_at_utc": checked_at,
        "cycle_id": cycle_id,
        "accumulation_run_id": cycle_id,
        "started_at_utc": parallel_status.get("started_at_utc")
        or heartbeat.get("started_at_utc")
        or heartbeat.get("accumulation_started_at_utc")
        or started_at_utc,
        "completed_at_utc": checked_at if lifecycle_state == "completed" else None,
        "phase": heartbeat.get("phase"),
        "selected_dates": heartbeat.get("selected_dates"),
        "overall_query_total": heartbeat.get("overall_query_total"),
        "overall_query_completed": heartbeat.get("overall_query_completed"),
        "total_rows_accumulated": heartbeat.get("total_rows_accumulated"),
        "aggregate_airline_count": parallel_status.get("airline_count"),
        "aggregate_failed_count": parallel_status.get("failed_count"),
        "duration_sec": parallel_status.get("duration_sec"),
        "worker_status_path": str(HEARTBEAT_STATUS_FILE),
        "parallel_status_path": str(PARALLEL_STATUS_FILE),
        "db_check": db_check,
        "command": command,
        "return_code": return_code,
    }
    if lifecycle_state == "completed" and parallel_status.get("completed_at_utc"):
        cycle_payload["completed_at_utc"] = parallel_status.get("completed_at_utc")
        cycle_payload["state"] = "completed"
        cycle_payload["action"] = "completed"
        cycle_payload["reason"] = "parallel_scrape_done"
        cycle_payload["aggregate_airline_count"] = parallel_status.get("airline_count")
        cycle_payload["aggregate_failed_count"] = parallel_status.get("failed_count")
        cycle_payload["duration_sec"] = parallel_status.get("duration_sec")
    existing_cycle_payload = _read_json(CYCLE_STATE_FILE)
    if _should_replace_cycle_state(existing_cycle_payload, cycle_payload):
        _write_json(CYCLE_STATE_FILE, cycle_payload)


def _run_step(name: str, cmd: list[str]):
    LOG.info("%s command: %s", name, subprocess.list2cmdline(cmd))
    started = datetime.now(timezone.utc)
    proc = None
    try:
        proc = subprocess.Popen(cmd, cwd=str(REPO_ROOT))
        rc = proc.wait()
    except KeyboardInterrupt:
        LOG.warning("%s interrupted by user; terminating child process", name)
        if proc is not None and proc.poll() is None:
            try:
                if os.name == "nt":
                    proc.terminate()
                else:
                    proc.send_signal(signal.SIGTERM)
            except Exception as exc:
                LOG.warning("%s child terminate failed: %s", name, exc)
            try:
                proc.wait(timeout=10)
            except Exception:
                LOG.warning("%s child still running after terminate; killing", name)
                try:
                    proc.kill()
                except Exception:
                    LOG.debug("%s child kill failed", name, exc_info=True)
        raise
    except Exception:
        LOG.exception("%s failed before completion", name)
        raise
    ended = datetime.now(timezone.utc)
    duration_sec = (ended - started).total_seconds()
    LOG.info("%s finished with rc=%s in %.1fs", name, rc, duration_sec)
    return rc


def _resolve_route_audit_report_path(report_output_dir: str) -> Path:
    # Preferred: same report output dir; fallback: canonical run_all route-audit location.
    candidates = [
        Path(report_output_dir) / "route_audit_report_latest.json",
        Path("output/reports/route_audit_report_latest.json"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def _log_per_airline_row_counts(report_output_dir: str, *, db_url: str | None = None) -> None:
    cycle_id = _read_latest_cycle_id()
    if db_url and cycle_id:
        db_counts = _collect_observed_airline_row_counts_db(db_url=db_url, cycle_id=cycle_id)
        if db_counts:
            LOG.info(
                "accumulation_rows_by_airline source=db_cycle cycle_id=%s %s",
                cycle_id,
                ", ".join(f"{k}={v}" for k, v in sorted(db_counts.items())),
            )
            return

    # 1) Accumulation-level rows from output/latest/combined_results.csv
    combined_csv = Path("output/latest/combined_results.csv")
    if combined_csv.exists():
        counts = {}
        try:
            with combined_csv.open("r", newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    airline = str(row.get("airline") or "").upper().strip() or "UNKNOWN"
                    counts[airline] = counts.get(airline, 0) + 1
            if counts:
                LOG.info("accumulation_rows_by_airline %s", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        except Exception as exc:
            LOG.warning("Failed to compute accumulation per-airline counts from %s: %s", combined_csv, exc)

    # 2) Report-level summary rows from latest run directory
    out_dir = Path(report_output_dir)
    try:
        run_dirs = [p for p in out_dir.glob("run_*") if p.is_dir()]
        if not run_dirs:
            return
        latest_run = max(run_dirs, key=lambda p: p.stat().st_mtime)
        summary_csvs = sorted(latest_run.glob("route_airline_summary_*.csv"))
        if not summary_csvs:
            return
        summary_csv = summary_csvs[-1]
        counts = {}
        with summary_csv.open("r", newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                airline = str(row.get("airline") or "").upper().strip() or "UNKNOWN"
                counts[airline] = counts.get(airline, 0) + 1
        if counts:
            LOG.info(
                "report_route_airline_summary_rows_by_airline run_dir=%s file=%s %s",
                latest_run,
                summary_csv.name,
                ", ".join(f"{k}={v}" for k, v in sorted(counts.items())),
            )
    except Exception as exc:
        LOG.warning("Failed to compute report per-airline summary counts: %s", exc)


def _log_prediction_health(report_output_dir: str) -> None:
    out_dir = Path(report_output_dir)
    try:
        history_files = [p for p in out_dir.glob("prediction_history_*_*.csv") if p.is_file()]
        if not history_files:
            LOG.warning("prediction_health: prediction output file not found in %s", out_dir)
            return
        latest_history = max(history_files, key=lambda p: p.stat().st_mtime)
        m = None
        try:
            m = latest_history.stem.rsplit("_", 2)
        except Exception:
            m = None
        if not m or len(m) < 2:
            LOG.info("prediction_health: latest_history=%s (timestamp parse unavailable)", latest_history.name)
            return
        stamp = f"{m[-2]}_{m[-1]}"
        meta_candidates = list(out_dir.glob(f"prediction_backtest_meta_*_{stamp}.json"))
        if not meta_candidates:
            LOG.info(
                "prediction_health: latest_history=%s stamp=%s metadata_missing (likely backtest disabled)",
                latest_history.name,
                stamp,
            )
            return

        latest_meta = max(meta_candidates, key=lambda p: p.stat().st_mtime)
        meta = json.loads(latest_meta.read_text(encoding="utf-8"))
        if not isinstance(meta, dict):
            LOG.warning("prediction_health: invalid metadata format in %s", latest_meta)
            return

        ml_requested = meta.get("ml_requested_models") or []
        ml_active = meta.get("ml_active_models") or []
        dl_requested = meta.get("dl_requested_models") or []
        dl_active = meta.get("dl_active_models") or []
        backtest = meta.get("backtest") if isinstance(meta.get("backtest"), dict) else {}
        backtest_status = backtest.get("status")
        LOG.info(
            "prediction_health: meta=%s ml_requested=%s ml_active=%s dl_requested=%s dl_active=%s backtest_status=%s",
            latest_meta.name,
            ml_requested,
            ml_active,
            dl_requested,
            dl_active,
            backtest_status,
        )
        if ml_requested and not ml_active:
            LOG.warning("prediction_health: ML models were requested but none were activated")
        if dl_requested and not dl_active:
            LOG.warning("prediction_health: DL models were requested but none were activated")
    except Exception as exc:
        LOG.warning("prediction_health: failed to inspect prediction metadata: %s", exc)


def _resolve_scrape_dates_for_log(args) -> list[str]:
    """
    Reconstruct the accumulation date list as run_all will resolve it, for logging only.
    """
    today = datetime.now(timezone.utc).date()
    dates: list[str] = []

    if args.date:
        dates = _parse_iso_date_list([args.date])
    elif args.dates:
        dates = _parse_iso_date_list(str(args.dates).split(","))
    elif args.date_start and args.date_end:
        dates = _expand_date_range(args.date_start, args.date_end)
    elif args.date_start or args.date_end:
        dates = _parse_iso_date_list([args.date_start or args.date_end])
    elif args.date_offsets:
        offsets = _parse_offsets_csv(args.date_offsets)
        dates = [(today + dt.timedelta(days=o)).isoformat() for o in offsets]
    else:
        # Match run_all default behavior: if dates_file omitted at pipeline level, run_all defaults to config/dates.json.
        file_path = Path(args.dates_file) if args.dates_file else RUN_ALL_DEFAULT_DATES_FILE
        file_dates = _load_dates_from_file_pipeline(file_path, today=today)
        if file_dates:
            dates = file_dates
        else:
            day_offsets = [0] if args.quick else [0, 3, 5, 7, 15, 30]
            dates = [(today + dt.timedelta(days=d)).isoformat() for d in day_offsets]

    return _finalize_outbound_dates(dates, today=today, limit_dates=args.limit_dates)


def build_scrape_cmd(args):
    if (args.parallel_airlines or 1) > 1 and not args.airline:
        cmd = [
            args.python_exe,
            str(REPO_ROOT / "tools/parallel_airline_runner.py"),
            "--python-exe",
            args.python_exe,
            "--max-workers",
            str(args.parallel_airlines),
            "--output-dir",
            args.report_output_dir,
        ]
        if args.quick:
            cmd.append("--quick")
        _add_arg(cmd, "--origin", args.origin)
        _add_arg(cmd, "--destination", args.destination)
        _add_arg(cmd, "--date", args.date)
        _add_arg(cmd, "--date-start", args.date_start)
        _add_arg(cmd, "--date-end", args.date_end)
        _add_arg(cmd, "--dates", args.dates)
        _add_arg(cmd, "--date-offsets", args.date_offsets)
        _add_arg(cmd, "--dates-file", args.dates_file)
        _add_arg(cmd, "--schedule-file", args.schedule_file)
        _add_arg(cmd, "--trip-plan-mode", args.trip_plan_mode)
        _add_arg(cmd, "--cycle-id", args.cycle_id)
        _add_arg(cmd, "--cabin", args.cabin)
        _add_arg(cmd, "--adt", args.adt)
        _add_arg(cmd, "--chd", args.chd)
        _add_arg(cmd, "--inf", args.inf)
        _add_arg(cmd, "--probe-group-id", args.probe_group_id)
        _add_arg(cmd, "--route-scope", args.route_scope)
        _add_arg(cmd, "--market-country", args.market_country)
        if args.strict_route_audit:
            cmd.append("--strict-route-audit")
        _add_arg(cmd, "--limit-routes", args.limit_routes)
        _add_arg(cmd, "--limit-dates", args.limit_dates)
        _add_arg(cmd, "--query-timeout-seconds", args.query_timeout_seconds)
        return cmd

    cmd = [args.python_exe, str(REPO_ROOT / "run_all.py")]
    if args.quick:
        cmd.append("--quick")
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--date", args.date)
    _add_arg(cmd, "--date-start", args.date_start)
    _add_arg(cmd, "--date-end", args.date_end)
    _add_arg(cmd, "--dates", args.dates)
    _add_arg(cmd, "--date-offsets", args.date_offsets)
    _add_arg(cmd, "--dates-file", args.dates_file)
    _add_arg(cmd, "--schedule-file", args.schedule_file)
    _add_arg(cmd, "--trip-plan-mode", args.trip_plan_mode)
    _add_arg(cmd, "--cycle-id", args.cycle_id)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--adt", args.adt)
    _add_arg(cmd, "--chd", args.chd)
    _add_arg(cmd, "--inf", args.inf)
    _add_arg(cmd, "--probe-group-id", args.probe_group_id)
    _add_arg(cmd, "--route-scope", args.route_scope)
    _add_arg(cmd, "--market-country", args.market_country)
    if args.strict_route_audit:
        cmd.append("--strict-route-audit")
    _add_arg(cmd, "--limit-routes", args.limit_routes)
    _add_arg(cmd, "--limit-dates", args.limit_dates)
    _add_arg(cmd, "--query-timeout-seconds", args.query_timeout_seconds)
    if args.profile_runtime:
        cmd.append("--profile-runtime")
        _add_arg(cmd, "--profile-output-dir", args.report_output_dir)
    return cmd


def build_report_cmd(args):
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "generate_reports.py"),
        "--format",
        args.report_format,
        "--output-dir",
        args.report_output_dir,
        "--timestamp-tz",
        args.report_timestamp_tz,
    ]

    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--route-scope", args.route_scope)
    _add_arg(cmd, "--market-country", args.market_country)
    if args.route_monitor:
        cmd.append("--route-monitor")
    if args.route_monitor_macro_xlsm:
        cmd.append("--route-monitor-macro-xlsm")
    _add_arg(cmd, "--route-monitor-macro-xlsm-path", args.route_monitor_macro_xlsm_path)
    return cmd


def build_prediction_cmd(args):
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "predict_next_day.py"),
        "--series-mode",
        args.prediction_series_mode,
        "--target-column",
        args.prediction_target,
        "--output-dir",
        args.report_output_dir,
    ]
    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--departure-start-date", args.prediction_departure_start_date)
    _add_arg(cmd, "--departure-end-date", args.prediction_departure_end_date)
    _add_arg(cmd, "--ml-models", args.prediction_ml_models)
    _add_arg(cmd, "--ml-quantiles", args.prediction_ml_quantiles)
    _add_arg(cmd, "--ml-min-history", args.prediction_ml_min_history)
    _add_arg(cmd, "--ml-random-seed", args.prediction_ml_random_seed)
    _add_arg(cmd, "--dl-models", args.prediction_dl_models)
    _add_arg(cmd, "--dl-quantiles", args.prediction_dl_quantiles)
    _add_arg(cmd, "--dl-min-history", args.prediction_dl_min_history)
    _add_arg(cmd, "--dl-random-seed", args.prediction_dl_random_seed)
    _add_arg(cmd, "--backtest-selection-metric", args.prediction_backtest_selection_metric)
    _add_arg(cmd, "--backtest-model-min-coverage-ratio", args.prediction_backtest_model_min_coverage_ratio)
    if args.prediction_disable_backtest:
        cmd.append("--disable-backtest")
    return cmd


def build_alert_eval_cmd(args):
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools/evaluate_alert_quality.py"),
        "--output-dir",
        args.report_output_dir,
        "--timestamp-tz",
        args.report_timestamp_tz,
        "--lookback-days",
        str(args.alert_lookback_days),
        "--spike-threshold",
        str(args.alert_spike_threshold),
        "--sellout-threshold",
        str(args.alert_sellout_threshold),
        "--spike-false-alarm-cost",
        str(args.alert_spike_false_alarm_cost),
        "--spike-missed-cost",
        str(args.alert_spike_missed_cost),
        "--sellout-false-alarm-cost",
        str(args.alert_sellout_false_alarm_cost),
        "--sellout-missed-cost",
        str(args.alert_sellout_missed_cost),
    ]
    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    return cmd


def build_intelligence_hub_cmd(args):
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools/build_intelligence_hub.py"),
        "--output-dir",
        args.report_output_dir,
        "--lookback-days",
        str(args.intel_lookback_days),
        "--forecast-target",
        args.intel_forecast_target,
        "--timestamp-tz",
        args.report_timestamp_tz,
    ]
    return cmd


def _bigquery_sync_is_configured(args) -> bool:
    return bool((args.bigquery_project_id or "").strip() and (args.bigquery_dataset or "").strip())


def _bigquery_sync_window(args) -> tuple[str, str]:
    end_date = dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=max(1, int(args.bigquery_sync_lookback_days or 1)))
    return start_date.isoformat(), end_date.isoformat()


def build_bigquery_sync_cmd(args) -> list[str]:
    start_date, end_date = _bigquery_sync_window(args)
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools" / "export_bigquery_stage.py"),
        "--output-dir",
        args.bigquery_sync_output_dir,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--load-bigquery",
        "--project-id",
        str(args.bigquery_project_id),
        "--dataset",
        str(args.bigquery_dataset),
    ]
    return cmd


def main():
    args = parse_args()
    args.adt = max(1, int(args.adt or 1))
    args.chd = max(0, int(args.chd or 0))
    args.inf = max(0, int(args.inf or 0))
    _apply_schedule_date_defaults_pipeline(args)
    execution_plan = _load_execution_plan(Path(args.schedule_file))
    if execution_plan:
        LOG.info(
            "Execution plan loaded: current_phase=%s ultimate_priority_goal=%s",
            execution_plan.get("current_phase"),
            execution_plan.get("ultimate_priority_goal"),
        )
    before_count = _count_column_events(args.db_url)

    pipeline_rc = 0

    if not args.skip_scrape:
        if not args.cycle_id:
            args.cycle_id = str(uuid.uuid4())
        resolved_scrape_dates = _resolve_scrape_dates_for_log(args)
        LOG.info("Resolved accumulation dates (%d): %s", len(resolved_scrape_dates), resolved_scrape_dates)
        LOG.info("Accumulation passenger mix: ADT=%d CHD=%d INF=%d", args.adt, args.chd, args.inf)
        if args.probe_group_id:
            LOG.info("Probe group id: %s", args.probe_group_id)
        scrape_cmd = build_scrape_cmd(args)
        _sync_accumulation_artifacts(
            args=args,
            cycle_id=args.cycle_id,
            lifecycle_state="starting",
            action="launch",
            reason="run_pipeline_accumulation_starting",
            command=scrape_cmd,
            started_at_utc=datetime.now(timezone.utc).isoformat(),
            return_code=None,
        )
        rc = _run_step("accumulation", scrape_cmd)
        _sync_accumulation_artifacts(
            args=args,
            cycle_id=args.cycle_id,
            lifecycle_state="completed" if rc == 0 else "failed",
            action="completed" if rc == 0 else "failed",
            reason="run_pipeline_accumulation_finished" if rc == 0 else "run_pipeline_accumulation_failed",
            command=scrape_cmd,
            started_at_utc=None,
            return_code=rc,
        )
        if rc != 0:
            pipeline_rc = rc
            if args.fail_fast:
                return pipeline_rc
    else:
        LOG.info("Skipping accumulation step.")

    if not args.skip_reports:
        rc = _run_step("reports", build_report_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc
    else:
        LOG.info("Skipping reports step.")

    if args.run_prediction:
        rc = _run_step("prediction", build_prediction_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc
        else:
            _log_prediction_health(args.report_output_dir)

    if args.run_intelligence_hub:
        rc = _run_step("intelligence_hub", build_intelligence_hub_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc

    if args.run_alert_eval:
        rc = _run_step("alert_eval", build_alert_eval_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc

    if pipeline_rc == 0 and not args.skip_bigquery_sync:
        if _bigquery_sync_is_configured(args):
            start_date, end_date = _bigquery_sync_window(args)
            LOG.info(
                "Automatic BigQuery sync enabled: project=%s dataset=%s window=%s..%s output_dir=%s",
                args.bigquery_project_id,
                args.bigquery_dataset,
                start_date,
                end_date,
                args.bigquery_sync_output_dir,
            )
            rc = _run_step("bigquery_sync", build_bigquery_sync_cmd(args))
            if rc != 0:
                if args.fail_on_bigquery_sync_error:
                    pipeline_rc = rc or pipeline_rc
                    if args.fail_fast:
                        return pipeline_rc
                else:
                    LOG.warning(
                        "Automatic BigQuery sync failed rc=%s but pipeline result is preserved. "
                        "Use --fail-on-bigquery-sync-error to make this blocking.",
                        rc,
                    )
        else:
            LOG.info(
                "Skipping automatic BigQuery sync because BIGQUERY project/dataset are not configured."
            )
    elif args.skip_bigquery_sync:
        LOG.info("Skipping automatic BigQuery sync by request.")

    after_count = _count_column_events(args.db_url)
    if before_count is not None and after_count is not None:
        LOG.info("column_change_events before=%s after=%s delta=%s", before_count, after_count, after_count - before_count)

    route_audit_path = _resolve_route_audit_report_path(args.report_output_dir)
    _log_per_airline_row_counts(args.report_output_dir, db_url=args.db_url)
    coverage = _compute_all_airline_coverage(execution_plan, db_url=args.db_url)
    if execution_plan and coverage.get("enabled"):
        LOG.info(
            "all_airline_coverage expected=%d covered=%d missing=%d pct=%.2f pass=%s",
            len(coverage.get("expected_airlines") or []),
            len(coverage.get("covered_airlines") or []),
            len(coverage.get("missing_airlines") or []),
            float(coverage.get("coverage_pct") or 0.0),
            bool(coverage.get("coverage_gate_passed")),
        )
        if coverage.get("missing_airlines"):
            LOG.info("all_airline_coverage_missing=%s", ",".join(coverage.get("missing_airlines")))
    execution_plan_status_path = _write_execution_plan_status(
        args.report_output_dir,
        execution_plan,
        coverage,
        pipeline_rc,
    )
    if execution_plan_status_path:
        LOG.info("execution_plan_status_artifact=%s", execution_plan_status_path)
    LOG.info(
        "pipeline_summary rc=%s route_audit_report=%s exists=%s report_output_dir=%s",
        pipeline_rc,
        route_audit_path,
        route_audit_path.exists(),
        args.report_output_dir,
    )

    return pipeline_rc


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOG.warning("Pipeline interrupted by user. Child process termination was requested.")
        raise SystemExit(130)
