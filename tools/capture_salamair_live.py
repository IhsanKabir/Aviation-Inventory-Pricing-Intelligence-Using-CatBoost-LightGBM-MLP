from __future__ import annotations

import argparse
import json
import sys
from datetime import date as dt_date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import salamair as ov


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"
SEARCH_PAGE_URL = "https://booking.salamair.com/en/search"
FLIGHT_FARES_URL = "https://api.salamair.com/api/flights/flightFares"
CONFIRM_URL = "https://api.salamair.com/api/flights/confirm"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _parse_iso_dates(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            normalized = dt_date.fromisoformat(text).isoformat()
        except Exception:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _expand_date_range(start_raw: Optional[str], end_raw: Optional[str]) -> List[str]:
    if not start_raw and not end_raw:
        return []
    if not start_raw or not end_raw:
        return _parse_iso_dates([start_raw or end_raw or ""])
    start = dt_date.fromisoformat(str(start_raw))
    end = dt_date.fromisoformat(str(end_raw))
    if end < start:
        start, end = end, start
    current = start
    out: List[str] = []
    while current <= end:
        out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def _browser_fetch_json(page, *, url: str, method: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return page.evaluate(
        """async ({ url, method, payload }) => {
            try {
                const init = {
                    method,
                    credentials: "include",
                    headers: {
                        "Accept": "application/json, text/plain, */*",
                    },
                };
                if (method !== "GET") {
                    init.headers["Content-Type"] = "application/json";
                    init.body = JSON.stringify(payload || {});
                }
                const response = await fetch(url, init);
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
        {"url": url, "method": method, "payload": payload},
    )


def _choose_confirm_sell_key(payload: Dict[str, Any], preferred_brand: str) -> Optional[str]:
    preferred = str(preferred_brand or "").strip().lower()
    fallback: Optional[str] = None
    for flight in payload.get("flights") or []:
        if not isinstance(flight, dict):
            continue
        for fare in flight.get("fares") or []:
            if not isinstance(fare, dict):
                continue
            brand = str(fare.get("fareTypeName") or "").strip().lower()
            for fare_info in fare.get("fareInfos") or []:
                if not isinstance(fare_info, dict):
                    continue
                sell_key = str(fare_info.get("fareSellKey") or "").strip()
                if not sell_key:
                    continue
                if brand == preferred:
                    return sell_key
                if fallback is None:
                    fallback = sell_key
    return fallback


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture live SalamAir fare data through the browser and store reusable manual-session artifacts.",
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", help="Single departure date YYYY-MM-DD")
    parser.add_argument("--dates", help="Comma-separated departure dates YYYY-MM-DD")
    parser.add_argument("--date-start", help="Inclusive departure date range start YYYY-MM-DD")
    parser.add_argument("--date-end", help="Inclusive departure date range end YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT))
    parser.add_argument("--browser-channel", default="chromium", choices=["chromium", "chrome", "msedge"])
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--user-data-dir", default="")
    parser.add_argument("--confirm-brand", default="Flexi", help="Fare brand to use when attempting confirm capture")
    parser.add_argument("--skip-confirm", action="store_true", help="Do not call /api/flights/confirm")
    args = parser.parse_args()

    dates: List[str] = []
    if args.date:
        dates.extend(_parse_iso_dates([args.date]))
    if args.dates:
        dates.extend(_parse_iso_dates([piece.strip() for piece in str(args.dates).split(",")]))
    dates.extend(_expand_date_range(args.date_start, args.date_end))
    dates = _parse_iso_dates(dates)
    if not dates:
        raise SystemExit("Provide --date, --dates, or --date-start/--date-end")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(f"Playwright import failed: {exc}")

    origin = str(args.origin or "").upper().strip()
    destination = str(args.destination or "").upper().strip()
    session_root = Path(args.session_root)
    results: List[Dict[str, Any]] = []

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
            page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(1500)

            for dep_date in dates:
                request_payload = {
                    "departureDate": f"{dep_date}T00:00:00",
                    "origin": origin,
                    "destination": destination,
                }
                fares_result = _browser_fetch_json(page, url=FLIGHT_FARES_URL, method="POST", payload=request_payload)
                fares_body = fares_result.get("body")
                if not fares_result.get("ok") or not isinstance(fares_body, dict):
                    results.append(
                        {
                            "date": dep_date,
                            "ok": False,
                            "error": "flight_fares_request_failed",
                            "status": fares_result.get("status"),
                            "detail": fares_result.get("error") or fares_body,
                        }
                    )
                    continue

                confirm_body = None
                confirm_sell_key = None
                if not args.skip_confirm:
                    confirm_sell_key = _choose_confirm_sell_key(fares_body, args.confirm_brand)
                    if confirm_sell_key:
                        confirm_result = _browser_fetch_json(
                            page,
                            url=CONFIRM_URL,
                            method="POST",
                            payload={"sellKey": confirm_sell_key},
                        )
                        if confirm_result.get("ok") and isinstance(confirm_result.get("body"), dict):
                            confirm_body = confirm_result.get("body")

                rows = ov.parse_flight_fares_payload(
                    fares_body,
                    requested_cabin=args.cabin,
                    adt=args.adt,
                    chd=args.chd,
                    inf=args.inf,
                    confirm_payload=confirm_body if isinstance(confirm_body, dict) else None,
                )
                if not rows:
                    results.append({"date": dep_date, "ok": False, "error": "no_rows_parsed"})
                    continue

                run_dir = session_root / "runs" / f"ov_{origin}_{destination}_{dep_date}_{_now_tag()}"
                fares_path = run_dir / "salamair_flight_fares_response.json"
                confirm_path = run_dir / "salamair_confirm_response.json"
                summary_path = run_dir / "salamair_capture_summary.json"
                rows_path = run_dir / "salamair_rows.json"

                _json_dump(fares_path, fares_body)
                _json_dump(rows_path, rows)
                if isinstance(confirm_body, dict):
                    _json_dump(confirm_path, confirm_body)

                summary = {
                    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                    "carrier": "OV",
                    "ok": True,
                    "source_type": "playwright_live",
                    "origin": origin,
                    "destination": destination,
                    "date": dep_date,
                    "cabin": args.cabin,
                    "adt": args.adt,
                    "chd": args.chd,
                    "inf": args.inf,
                    "search_page_url": SEARCH_PAGE_URL,
                    "flight_fares_request_body": request_payload,
                    "flight_fares_response_body_path": str(fares_path.resolve()),
                    "flight_fares_response_body": fares_body,
                    "confirm_sell_key": confirm_sell_key,
                    "confirm_brand": args.confirm_brand,
                    "confirm_response_body_path": str(confirm_path.resolve()) if isinstance(confirm_body, dict) else None,
                    "confirm_response_body": confirm_body,
                    "rows_path": str(rows_path.resolve()),
                    "rows_count": len(rows),
                    "sample_rows": rows[:3],
                }
                _json_dump(summary_path, summary)
                results.append(
                    {
                        "date": dep_date,
                        "ok": True,
                        "run_dir": str(run_dir.resolve()),
                        "summary_path": str(summary_path.resolve()),
                        "rows_count": len(rows),
                        "brands": [row.get("brand") for row in rows],
                    }
                )
        finally:
            if context is not None:
                context.close()
            if browser is not None:
                browser.close()

    print(json.dumps({"ok": True, "origin": origin, "destination": destination, "dates": dates, "results": results}, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
