from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import gozayaan as gz


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def _search_results_url(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> str:
    params = {
        "adult": int(max(1, adt)),
        "child": int(max(0, chd)),
        "child_age": "",
        "infant": int(max(0, inf)),
        "cabin_class": str(cabin or "Economy"),
        "trips": f"{str(origin).upper().strip()},{str(destination).upper().strip()},{str(date).strip()}",
    }
    return f"https://gozayaan.com/flight/list?{urlencode(params)}"


def _latest_success(records: List[Dict[str, Any]], url: str) -> Optional[Dict[str, Any]]:
    for item in reversed(records):
        if item.get("url") != url:
            continue
        body = item.get("response_body")
        if item.get("status") == 200 and isinstance(body, dict) and body.get("status") is True:
            return item
    return None


def _wait_for_search_and_legs(
    *,
    page,
    records: List[Dict[str, Any]],
    timeout_sec: float,
) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    deadline = time.time() + max(5.0, float(timeout_sec))
    last_search: Optional[Dict[str, Any]] = None
    completed_legs: Optional[Dict[str, Any]] = None
    while time.time() < deadline:
        last_search = _latest_success(records, gz.SEARCH_URL)
        latest_legs = _latest_success(records, gz.LEGS_URL)
        if latest_legs:
            result = latest_legs.get("response_body", {}).get("result") or {}
            status_text = str(result.get("status") or "").upper()
            progress = gz._safe_int(result.get("progress"))
            expected = gz._safe_int(result.get("expected_progress"))
            if status_text == "COMPLETED" or (
                progress is not None and expected is not None and progress >= expected
            ):
                completed_legs = latest_legs
                break
        page.wait_for_timeout(250)
    return last_search, completed_legs


def _browser_fetch_json(page, *, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return page.evaluate(
        """async ({ url, payload }) => {
            try {
                const response = await fetch(url, {
                    method: "POST",
                    credentials: "include",
                    headers: {
                        "Accept": "application/json, text/plain, */*",
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                });
                const text = await response.text();
                let body;
                try {
                    body = JSON.parse(text);
                } catch (_) {
                    body = text;
                }
                return { ok: response.ok, status: response.status, body };
            } catch (err) {
                return { ok: false, status: null, error: String(err), body: null };
            }
        }""",
        {"url": url, "payload": payload},
    )


def _collect_fares_from_legs_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fares_by_key: Dict[str, Dict[str, Any]] = {}
    for item in records:
        if item.get("url") != gz.LEGS_URL:
            continue
        body = item.get("response_body")
        if not isinstance(body, dict) or not body.get("status"):
            continue
        result = body.get("result") or {}
        fares = result.get("fares") or []
        if not isinstance(fares, list):
            continue
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            key = str(fare.get("id") or "") or (
                f"{fare.get('hash')}::{fare.get('total_fare_amount')}::{fare.get('total_base_amount')}"
            )
            fares_by_key[key] = fare
    return list(fares_by_key.values())


def _candidate_leg_hashes_by_airline(
    fares: List[Dict[str, Any]],
    *,
    origin: str,
    destination: str,
) -> Dict[str, List[str]]:
    out: Dict[str, set[str]] = {}
    wanted_origin = str(origin or "").upper().strip()
    wanted_dest = str(destination or "").upper().strip()
    for fare in fares:
        if not isinstance(fare, dict):
            continue
        meta = gz._parse_hash_str(str(fare.get("hash_str") or ""))
        airline = str(meta.get("airline") or "").upper().strip()
        if not airline:
            continue
        if meta.get("origin") and str(meta.get("origin")).upper().strip() != wanted_origin:
            continue
        if meta.get("destination") and str(meta.get("destination")).upper().strip() != wanted_dest:
            continue
        bag = out.setdefault(airline, set())
        leg_hashes = fare.get("leg_hashes")
        if isinstance(leg_hashes, list):
            for h in leg_hashes:
                hs = str(h or "").strip()
                if hs:
                    bag.add(hs)
        hs = str(fare.get("hash") or "").strip()
        if hs:
            bag.add(hs)
    return {airline: sorted(values) for airline, values in sorted(out.items()) if values}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture live Gozayaan flight search data through the browser and store it as reusable manual-session artifacts.",
    )
    parser.add_argument(
        "--airline",
        default="",
        help="Optional airline code or comma-separated airline codes. When omitted, capture all airlines returned for the route/date.",
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="Departure date YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT))
    parser.add_argument("--wait-seconds", type=float, default=20.0)
    parser.add_argument(
        "--browser-channel",
        default="chromium",
        choices=["chromium", "chrome", "msedge"],
    )
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--user-data-dir", default="")
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(f"Playwright import failed: {exc}")

    origin = str(args.origin or "").upper().strip()
    destination = str(args.destination or "").upper().strip()
    date = str(args.date or "").strip()
    requested_airlines = sorted(
        {
            str(x or "").upper().strip()
            for x in str(args.airline or "").split(",")
            if str(x or "").strip()
        }
    )
    list_url = _search_results_url(
        origin=origin,
        destination=destination,
        date=date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
    )

    records: List[Dict[str, Any]] = []
    search_response: Optional[Dict[str, Any]] = None
    completed_legs: Optional[Dict[str, Any]] = None

    with sync_playwright() as p:
        launch_kwargs: Dict[str, Any] = {"headless": bool(args.headless)}
        channel = None if args.browser_channel == "chromium" else args.browser_channel
        if channel:
            launch_kwargs["channel"] = channel

        browser = None
        context = None
        try:
            if args.user_data_dir:
                profile_dir = str(Path(args.user_data_dir).resolve())
                Path(profile_dir).mkdir(parents=True, exist_ok=True)
                context = p.chromium.launch_persistent_context(profile_dir, **launch_kwargs)
            else:
                browser = p.chromium.launch(**launch_kwargs)
                context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(90000)

            def on_response(resp) -> None:
                url = str(resp.url or "")
                if url not in {gz.SEARCH_URL, gz.LEGS_URL}:
                    return
                try:
                    body: Any = resp.json()
                except Exception:
                    try:
                        body = resp.text()
                    except Exception:
                        body = None
                try:
                    request_post_data = resp.request.post_data
                except Exception:
                    request_post_data = None
                request_body = gz._safe_json_loads(request_post_data)
                records.append(
                    {
                        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                        "url": url,
                        "status": resp.status,
                        "request_headers": dict(resp.request.headers or {}),
                        "request_body": request_body,
                        "response_body": body,
                    }
                )

            page.on("response", on_response)
            page.goto(list_url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(1500)
            search_response, completed_legs = _wait_for_search_and_legs(
                page=page,
                records=records,
                timeout_sec=args.wait_seconds,
            )
            if not search_response or not completed_legs:
                raise SystemExit(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "search_or_legs_not_captured",
                            "list_url": list_url,
                            "search_captured": bool(search_response),
                            "legs_captured": bool(completed_legs),
                        },
                        indent=2,
                    )
                )

            search_body = search_response.get("response_body") or {}
            search_result = search_body.get("result") or {}
            search_id = str(search_result.get("search_id") or "").strip()
            if not search_id:
                raise SystemExit(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "search_id_missing",
                            "search_response": search_body,
                        },
                        indent=2,
                        ensure_ascii=False,
                    )
                )

            fares = _collect_fares_from_legs_records(records)
            leg_hashes_by_airline = _candidate_leg_hashes_by_airline(
                fares,
                origin=origin,
                destination=destination,
            )
            if not leg_hashes_by_airline:
                raise SystemExit(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "leg_hashes_not_found",
                            "search_id": search_id,
                            "fares_seen": len(fares),
                        },
                        indent=2,
                    )
                )
            if requested_airlines:
                missing = [x for x in requested_airlines if x not in leg_hashes_by_airline]
                if missing:
                    raise SystemExit(
                        json.dumps(
                            {
                                "ok": False,
                                "error": "requested_airlines_not_found",
                                "requested_airlines": requested_airlines,
                                "available_airlines": sorted(leg_hashes_by_airline),
                                "missing_airlines": missing,
                                "search_id": search_id,
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                    )
                target_airlines = requested_airlines
            else:
                target_airlines = sorted(leg_hashes_by_airline)
            leg_hashes = sorted(
                {
                    leg_hash
                    for airline in target_airlines
                    for leg_hash in leg_hashes_by_airline.get(airline, [])
                }
            )

            payload_items: List[Dict[str, Any]] = []
            leg_fares_responses: List[Dict[str, Any]] = []
            for leg_hash in leg_hashes:
                fetch_payload = {
                    "search_id": search_id,
                    "leg_type": "L1",
                    "leg_hash": leg_hash,
                }
                leg_fares = _browser_fetch_json(
                    page,
                    url=gz.LEG_FARES_URL,
                    payload=fetch_payload,
                )
                body = leg_fares.get("body")
                leg_fares_responses.append(
                    {
                        "leg_hash": leg_hash,
                        "status": leg_fares.get("status"),
                        "ok": leg_fares.get("ok"),
                        "error": leg_fares.get("error"),
                        "response_body": body,
                    }
                )
                if not isinstance(body, dict) or not body.get("status"):
                    continue
                payload_items.append(
                    {
                        "source_type": "playwright_live",
                        "source_path": list_url,
                        "entry_index": len(payload_items),
                        "request_url": gz.LEG_FARES_URL,
                        "request_body": fetch_payload,
                        "search_id": search_id,
                        "leg_hash": leg_hash,
                        "response_body": body,
                    }
                )
        finally:
            if context is not None:
                context.close()
            if browser is not None:
                browser.close()

    groups = gz.extract_capture_groups_from_payload_items(
        payload_items,
        requested_cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
    )
    filtered_groups: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for key, group in groups.items():
        airline_key, origin_key, destination_key, date_key = key
        if str(origin_key or "").upper().strip() != origin:
            continue
        if str(destination_key or "").upper().strip() != destination:
            continue
        if str(date_key or "")[:10] != date[:10]:
            continue
        if requested_airlines and str(airline_key or "").upper().strip() not in requested_airlines:
            continue
        filtered_groups[key] = group
    if not filtered_groups:
        raise SystemExit(
            json.dumps(
                {
                    "ok": False,
                    "error": "target_groups_not_found",
                    "requested_airlines": requested_airlines,
                    "groups_found": list(groups.keys()),
                    "leg_hashes": leg_hashes,
                },
                indent=2,
                ensure_ascii=False,
            )
        )

    session_root = Path(args.session_root)
    tag = _now_tag()
    route_leg_records = [x for x in records if x.get("url") == gz.LEGS_URL]
    results: List[Dict[str, Any]] = []
    for key, group in sorted(filtered_groups.items()):
        airline_key, origin_key, destination_key, date_key = key
        run_dir = session_root / "runs" / f"gozayaan_{airline_key}_{origin_key}_{destination_key}_{date_key}_{tag}"
        rows_path = run_dir / "gozayaan_rows.json"
        payloads_path = run_dir / "gozayaan_leg_fares_payloads.json"
        summary_path = run_dir / "gozayaan_capture_summary.json"
        search_path = run_dir / "gozayaan_search_response.json"
        legs_path = run_dir / "gozayaan_legs_responses.json"
        leg_fares_path = run_dir / "gozayaan_browser_leg_fares.json"

        rows = list(group.get("rows") or [])
        airline_payloads = list(group.get("payload_items") or [])
        airline_leg_hashes = list(leg_hashes_by_airline.get(airline_key, []))
        _json_dump(rows_path, rows)
        _json_dump(payloads_path, airline_payloads)
        _json_dump(search_path, search_response)
        _json_dump(legs_path, route_leg_records)
        _json_dump(leg_fares_path, leg_fares_responses)

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_type": "playwright_live",
            "list_url": list_url,
            "airline": airline_key,
            "origin": origin_key,
            "destination": destination_key,
            "date": date_key[:10],
            "cabin_hint": args.cabin,
            "adt": args.adt,
            "chd": args.chd,
            "inf": args.inf,
            "search_id": search_id,
            "rows_count": len(rows),
            "payload_count": len(airline_payloads),
            "leg_hashes": airline_leg_hashes,
            "all_airlines_found": sorted(leg_hashes_by_airline),
            "requested_airlines": requested_airlines,
            "rows_path": str(rows_path.resolve()),
            "payloads_path": str(payloads_path.resolve()),
            "search_response_path": str(search_path.resolve()),
            "legs_responses_path": str(legs_path.resolve()),
            "leg_fares_path": str(leg_fares_path.resolve()),
            "sample_rows": rows[:3],
        }
        _json_dump(summary_path, summary)
        results.append(
            {
                "airline": airline_key,
                "run_dir": str(run_dir.resolve()),
                "summary_path": str(summary_path.resolve()),
                "rows_count": len(rows),
                "leg_hashes": airline_leg_hashes,
            }
        )

    print(
        json.dumps(
            {
                "ok": True,
                "groups_created": len(results),
                "search_id": search_id,
                "requested_airlines": requested_airlines,
                "all_airlines_found": sorted(leg_hashes_by_airline),
                "results": results,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
