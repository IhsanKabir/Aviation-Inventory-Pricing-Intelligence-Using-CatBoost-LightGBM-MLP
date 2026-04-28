from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


RETRYABLE_ERROR_CLASSES = {"rate_limit", "expired_session", "source_exception", "timeout"}
MANUAL_ERROR_CLASSES = {"manual_required", "stale_capture", "waf_blocked", "missing_capture"}
FAIL_ERROR_CLASSES = RETRYABLE_ERROR_CLASSES | MANUAL_ERROR_CLASSES | {"source_error"}
NO_INVENTORY_CLASSES = {"no_inventory"}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _json_default(value: Any) -> str:
    return str(value)


def _compact_text(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    text_value = str(value).replace("\r", " ").replace("\n", " ").strip()
    return text_value[:limit]


def _collect_strings(node: Any, *, max_items: int = 80) -> list[str]:
    out: list[str] = []

    def walk(value: Any) -> None:
        if len(out) >= max_items:
            return
        if isinstance(value, dict):
            for key, nested in value.items():
                if str(key).lower() in {
                    "error",
                    "message",
                    "hint",
                    "detail",
                    "status",
                    "initialize_status",
                    "initialize_response_preview",
                    "response_preview",
                    "live_error",
                    "live_exception",
                    "source",
                }:
                    out.append(_compact_text(nested))
                walk(nested)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, (str, int)):
            s = _compact_text(value)
            if s:
                out.append(s)

    walk(node)
    return [s for s in out if s]


def source_family(module_name: str | None, final_source: str | None = None) -> str:
    key = str(module_name or final_source or "").strip().lower()
    source = str(final_source or "").strip().lower()
    haystack = f"{key} {source}"
    if "sharetrip" in haystack:
        return "sharetrip"
    if key in {"bs", "airastra"} or "ttinteractive" in haystack:
        return "wrapper"
    if "gozayaan" in haystack:
        return "gozayaan"
    if key in {"airarabia", "salamair", "maldivian", "airasia"} or "capture" in haystack:
        return "capture"
    if key in {"indigo"}:
        return "protected_direct"
    return "direct"


def classify_attempt(resp: dict[str, Any] | None, *, row_count: int | None = None) -> dict[str, Any]:
    response = resp if isinstance(resp, dict) else {}
    rows = response.get("rows") if isinstance(response.get("rows"), list) else []
    count = _safe_int(row_count if row_count is not None else len(rows))
    raw = response.get("raw") if isinstance(response.get("raw"), dict) else {}
    ok = bool(response.get("ok"))
    source_attempts = raw.get("source_attempts") if isinstance(raw.get("source_attempts"), list) else []

    if count > 0:
        return {
            "error_class": "success",
            "manual_action_required": False,
            "retry_recommended": False,
            "no_rows_reason": "",
        }

    haystack = " | ".join(_collect_strings({"raw": raw, "source_attempts": source_attempts})).lower()
    raw_error = str(raw.get("error") or "").strip().lower()

    def result(error_class: str, reason: str = "") -> dict[str, Any]:
        return {
            "error_class": error_class,
            "manual_action_required": error_class in MANUAL_ERROR_CLASSES,
            "retry_recommended": error_class in RETRYABLE_ERROR_CLASSES,
            "no_rows_reason": _compact_text(reason or haystack, 500),
        }

    if raw_error == "stale_capture" or "stale_capture" in haystack or "stale capture" in haystack:
        return result("stale_capture")
    if raw_error in {"capture_not_found", "fare_capture_not_found", "flight_fares_capture_not_found"}:
        return result("missing_capture")
    if any(token in haystack for token in ("captcha", "datadome", "imperva", "waf", "bot_block", "bot block", "challenge")):
        return result("waf_blocked")
    if any(token in haystack for token in ("manual", "search_flow_not_implemented", "session_not_found", "capture_missing")):
        return result("manual_required")
    if any(token in haystack for token in ("429", "rate_limit", "rate limit", "too many", "e_rate_limit", "cooldown")):
        return result("rate_limit")
    if any(
        token in haystack
        for token in (
            "session expired",
            "expired session",
            "invalid session",
            "token expired",
            "unauthorized",
            "forbidden",
            "401",
            "403",
        )
    ):
        return result("expired_session")
    if any(token in haystack for token in ("timeout", "timed out")):
        return result("timeout")
    if any(token in haystack for token in ("exception", "request_failed", "request_exception", "initialize_failed")):
        return result("source_exception")

    if ok:
        return result("no_inventory", reason=haystack or "source returned ok=true with zero parsed rows")
    if raw_error:
        return result("source_error")
    return result("source_exception", reason=haystack or "source returned ok=false with zero parsed rows")


def _last_successful_source(source_attempts: list[dict[str, Any]]) -> str | None:
    for attempt in reversed(source_attempts or []):
        if not isinstance(attempt, dict):
            continue
        if bool(attempt.get("ok")) and _safe_int(attempt.get("rows")) > 0:
            return str(attempt.get("source") or "").strip() or None
    for attempt in reversed(source_attempts or []):
        if isinstance(attempt, dict) and attempt.get("source"):
            return str(attempt.get("source") or "").strip()
    return None


def _extract_state(raw: dict[str, Any], *, kind: str) -> dict[str, Any]:
    keys = {
        "capture": {
            "capture_file",
            "capture_summary_path",
            "capture_rows_path",
            "capture_payloads_path",
            "capture_generated_at",
            "captured_at_utc",
            "capture_rows_count",
            "capture_age_hours",
            "max_capture_age_hours",
            "capture_available",
        },
        "session": {
            "session_file",
            "session_summary_file",
            "cookies_path",
            "cookies_cache_file",
            "headers_cache_file",
            "has_cookie_session",
            "has_x_kong_segment_id",
            "x_kong_token_source",
            "x_kong_token_ttl_sec",
            "x_kong_token_expires_at_utc",
            "session_auto_refresh",
        },
    }.get(kind, set())
    state: dict[str, Any] = {}

    def pull(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in keys and key not in state:
                    state[key] = value
                if key in {"capture", "direct", "headers_hint", "capture_available", "browser_capture", "refresh"}:
                    pull(value)
            return
        if isinstance(node, list):
            for item in node:
                pull(item)

    pull(raw)
    return state


def build_attempt_row(
    *,
    scrape_id: str,
    cycle_id: str | None,
    airline: str,
    module_name: str,
    fetched: dict[str, Any],
    inserted_core_count: int = 0,
    inserted_raw_meta_count: int = 0,
    raw_meta_matched: int = 0,
    raw_meta_unmatched: int = 0,
    raw_meta_match_modes: dict[str, Any] | None = None,
    no_rows_reason: str | None = None,
) -> dict[str, Any]:
    resp = fetched.get("resp") if isinstance(fetched, dict) else {}
    if not isinstance(resp, dict):
        resp = {}
    raw = resp.get("raw") if isinstance(resp.get("raw"), dict) else {}
    rows = fetched.get("rows") if isinstance(fetched.get("rows"), list) else []
    row_count = len(rows)
    source_attempts = raw.get("source_attempts") if isinstance(raw.get("source_attempts"), list) else []
    final_source = str(raw.get("source") or "").strip() or _last_successful_source(source_attempts)
    classification = classify_attempt(resp, row_count=row_count)
    if no_rows_reason:
        classification["no_rows_reason"] = _compact_text(no_rows_reason, 500)

    trip_context = fetched.get("trip_context") if isinstance(fetched.get("trip_context"), dict) else {}
    return_date = fetched.get("return_date")
    trip_type = fetched.get("trip_type") or trip_context.get("search_trip_type")
    family = source_family(module_name, final_source)
    fallback_used = bool(len(source_attempts) > 1 or raw.get("fallback_source") or raw.get("fallback_ok"))
    first_row = rows[0] if rows and isinstance(rows[0], dict) else {}
    return {
        "scrape_id": str(scrape_id),
        "cycle_id": str(cycle_id or scrape_id),
        "query_key": fetched.get("checkpoint_key"),
        "airline": str(airline).upper(),
        "module_name": str(module_name or "").strip().lower(),
        "source_family": family,
        "final_source": final_source,
        "fallback_used": fallback_used,
        "origin": fetched.get("origin"),
        "destination": fetched.get("destination"),
        "departure_date": fetched.get("date"),
        "return_date": return_date or trip_context.get("requested_return_date"),
        "trip_type": trip_type,
        "cabin": fetched.get("cabin"),
        "adt_count": _safe_int(first_row.get("adt_count"), 1),
        "chd_count": _safe_int(first_row.get("chd_count"), 0),
        "inf_count": _safe_int(first_row.get("inf_count"), 0),
        "ok": bool(resp.get("ok")),
        "row_count": row_count,
        "inserted_core_count": _safe_int(inserted_core_count),
        "inserted_raw_meta_count": _safe_int(inserted_raw_meta_count),
        "raw_meta_matched": _safe_int(raw_meta_matched),
        "raw_meta_unmatched": _safe_int(raw_meta_unmatched),
        "raw_meta_match_modes": dict(raw_meta_match_modes or {}),
        "elapsed_sec": _safe_float(fetched.get("elapsed_sec")),
        "error_class": classification["error_class"],
        "no_rows_reason": classification["no_rows_reason"],
        "manual_action_required": bool(classification["manual_action_required"]),
        "retry_recommended": bool(classification["retry_recommended"]),
        "capture_state": _extract_state(raw, kind="capture"),
        "session_state": _extract_state(raw, kind="session"),
        "source_attempts": source_attempts,
        "meta": {
            "raw_error": raw.get("error"),
            "raw_message": raw.get("message"),
            "hint": raw.get("hint"),
        },
    }


def summarize_attempts(attempts: list[dict[str, Any]], *, expected_airlines: list[str] | None = None) -> dict[str, Any]:
    normalized = [dict(a) for a in attempts if isinstance(a, dict)]
    expected = sorted({str(a).upper().strip() for a in (expected_airlines or []) if str(a or "").strip()})
    observed = sorted({str(a.get("airline") or "").upper().strip() for a in normalized if str(a.get("airline") or "").strip()})
    missing = [a for a in expected if a not in observed]

    class_counts = Counter(str(a.get("error_class") or "unknown") for a in normalized)
    failures = [a for a in normalized if str(a.get("error_class") or "") in FAIL_ERROR_CLASSES]
    manual = [a for a in normalized if bool(a.get("manual_action_required"))]
    retry = [a for a in normalized if bool(a.get("retry_recommended"))]
    no_inventory = [a for a in normalized if str(a.get("error_class") or "") in NO_INVENTORY_CLASSES]

    by_airline: dict[str, dict[str, Any]] = {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for attempt in normalized:
        grouped[str(attempt.get("airline") or "").upper().strip() or "UNKNOWN"].append(attempt)
    for airline, rows in sorted(grouped.items()):
        c = Counter(str(row.get("error_class") or "unknown") for row in rows)
        by_airline[airline] = {
            "attempts": len(rows),
            "rows": sum(_safe_int(row.get("row_count")) for row in rows),
            "inserted_core_count": sum(_safe_int(row.get("inserted_core_count")) for row in rows),
            "classes": dict(sorted(c.items())),
            "manual_action_required": sum(1 for row in rows if bool(row.get("manual_action_required"))),
            "retry_recommended": sum(1 for row in rows if bool(row.get("retry_recommended"))),
        }

    status = "PASS"
    status_reasons: list[str] = []
    if not normalized:
        status = "FAIL"
        status_reasons.append("no extraction attempts recorded")
    if missing:
        status = "FAIL"
        status_reasons.append(f"missing expected airlines: {','.join(missing)}")
    if failures:
        status = "FAIL"
        status_reasons.append(f"failure attempts: {len(failures)}")
    if status == "PASS" and no_inventory:
        status = "WARN"
        status_reasons.append(f"clean zero-row attempts: {len(no_inventory)}")

    return {
        "generated_at_utc": _now_utc_iso(),
        "status": status,
        "status_reasons": status_reasons,
        "attempt_count": len(normalized),
        "expected_airlines": expected,
        "observed_airlines": observed,
        "missing_airlines": missing,
        "row_count": sum(_safe_int(a.get("row_count")) for a in normalized),
        "inserted_core_count": sum(_safe_int(a.get("inserted_core_count")) for a in normalized),
        "inserted_raw_meta_count": sum(_safe_int(a.get("inserted_raw_meta_count")) for a in normalized),
        "error_class_counts": dict(sorted(class_counts.items())),
        "failure_count": len(failures),
        "manual_action_required_count": len(manual),
        "retry_recommended_count": len(retry),
        "by_airline": by_airline,
        "manual_action_needed": [
            _attempt_summary_item(a) for a in manual[:100]
        ],
        "retry_recommended": [
            _attempt_summary_item(a) for a in retry[:100]
        ],
    }


def _attempt_summary_item(attempt: dict[str, Any]) -> dict[str, Any]:
    return {
        "airline": attempt.get("airline"),
        "route": f"{attempt.get('origin')}->{attempt.get('destination')}",
        "departure_date": attempt.get("departure_date"),
        "return_date": attempt.get("return_date"),
        "cabin": attempt.get("cabin"),
        "module_name": attempt.get("module_name"),
        "source_family": attempt.get("source_family"),
        "error_class": attempt.get("error_class"),
        "reason": attempt.get("no_rows_reason"),
    }


def _write_csv(path: Path, attempts: list[dict[str, Any]]) -> None:
    fields = [
        "scrape_id",
        "cycle_id",
        "airline",
        "module_name",
        "source_family",
        "final_source",
        "origin",
        "destination",
        "departure_date",
        "return_date",
        "trip_type",
        "cabin",
        "ok",
        "row_count",
        "inserted_core_count",
        "inserted_raw_meta_count",
        "raw_meta_matched",
        "raw_meta_unmatched",
        "elapsed_sec",
        "error_class",
        "manual_action_required",
        "retry_recommended",
        "no_rows_reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for attempt in attempts:
            writer.writerow({field: attempt.get(field, "") for field in fields})


def _write_markdown(path: Path, report: dict[str, Any]) -> None:
    lines = [
        "# Extraction Health",
        "",
        f"- status: `{report.get('status')}`",
        f"- generated_at_utc: `{report.get('generated_at_utc')}`",
        f"- attempts: `{report.get('attempt_count')}`",
        f"- observed_airlines: `{len(report.get('observed_airlines') or [])}`",
        f"- missing_airlines: `{','.join(report.get('missing_airlines') or []) or '-'}`",
        f"- rows: `{report.get('row_count')}`",
        f"- inserted_core_count: `{report.get('inserted_core_count')}`",
        f"- failures: `{report.get('failure_count')}`",
        f"- manual_action_required: `{report.get('manual_action_required_count')}`",
        f"- retry_recommended: `{report.get('retry_recommended_count')}`",
        "",
        "## Error Classes",
        "",
    ]
    for key, value in (report.get("error_class_counts") or {}).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## By Airline", ""])
    for airline, row in (report.get("by_airline") or {}).items():
        class_bits = ", ".join(f"{k}={v}" for k, v in (row.get("classes") or {}).items())
        lines.append(
            f"- `{airline}` attempts={row.get('attempts')} rows={row.get('rows')} inserted={row.get('inserted_core_count')} classes={class_bits or '-'}"
        )
    if report.get("manual_action_needed"):
        lines.extend(["", "## Manual Action Needed", ""])
        for item in report["manual_action_needed"]:
            lines.append(
                f"- `{item.get('airline')}` {item.get('route')} {item.get('departure_date')} {item.get('cabin')} "
                f"`{item.get('error_class')}`: {item.get('reason') or '-'}"
            )
    if report.get("retry_recommended"):
        lines.extend(["", "## Retry Recommended", ""])
        for item in report["retry_recommended"]:
            lines.append(
                f"- `{item.get('airline')}` {item.get('route')} {item.get('departure_date')} {item.get('cabin')} "
                f"`{item.get('error_class')}`: {item.get('reason') or '-'}"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_health_reports(
    attempts: list[dict[str, Any]],
    *,
    output_dir: str | Path = "output/reports",
    cycle_id: str | None = None,
    expected_airlines: list[str] | None = None,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report = summarize_attempts(attempts, expected_airlines=expected_airlines)
    report["cycle_id"] = cycle_id
    payload = {"summary": report, "attempts": attempts}
    latest_json = out_dir / "extraction_health_latest.json"
    latest_md = out_dir / "extraction_health_latest.md"
    latest_csv = out_dir / "extraction_health_latest.csv"
    latest_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8")
    _write_markdown(latest_md, report)
    _write_csv(latest_csv, attempts)

    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    suffix = f"{ts}_{cycle_id}" if cycle_id else ts
    run_json = out_dir / f"extraction_health_{suffix}.json"
    run_md = out_dir / f"extraction_health_{suffix}.md"
    run_csv = out_dir / f"extraction_health_{suffix}.csv"
    run_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8")
    _write_markdown(run_md, report)
    _write_csv(run_csv, attempts)
    report["artifacts"] = {
        "latest_json": str(latest_json),
        "latest_md": str(latest_md),
        "latest_csv": str(latest_csv),
        "run_json": str(run_json),
        "run_md": str(run_md),
        "run_csv": str(run_csv),
    }
    return report


def load_attempts_from_db(db_url: str, *, cycle_id: str | None = None, scrape_id: str | None = None) -> list[dict[str, Any]]:
    if not cycle_id and not scrape_id:
        raise ValueError("cycle_id or scrape_id is required")
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    where = "cycle_id = :run_id" if cycle_id else "scrape_id = :run_id"
    run_id = cycle_id or scrape_id
    sql = text(
        f"""
        SELECT
            scrape_id, cycle_id, query_key, airline, module_name, source_family, final_source,
            fallback_used, origin, destination, departure_date, return_date, trip_type, cabin,
            adt_count, chd_count, inf_count, ok, row_count, inserted_core_count,
            inserted_raw_meta_count, raw_meta_matched, raw_meta_unmatched,
            raw_meta_match_modes, elapsed_sec, error_class, no_rows_reason,
            manual_action_required, retry_recommended, capture_state, session_state,
            source_attempts, meta, created_at
        FROM extraction_attempts
        WHERE {where}
        ORDER BY airline, origin, destination, departure_date, cabin, id
        """
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(sql, {"run_id": str(run_id)})]
    return rows
