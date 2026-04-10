from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import salamair as ov


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"
SEARCH_PAGE_URL = "https://booking.salamair.com/en/search"
FLIGHT_FARES_URL_TOKEN = "/api/flights/flightFares"
CONFIRM_URL_TOKEN = "/api/flights/confirm"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _safe_json_or_none(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _dismiss_popups(page) -> None:
    labels = ["Accept", "Accept All", "Allow All", "I Agree", "Got it", "OK", "Close"]
    for label in labels:
        try:
            button = page.get_by_role("button", name=label).first
            if button and button.is_visible(timeout=350):
                button.click(timeout=1500)
                page.wait_for_timeout(120)
        except Exception:
            continue


def _latest_matching(records: List[Dict[str, Any]], token: str) -> Optional[Dict[str, Any]]:
    for item in reversed(records):
        if token in str(item.get("url") or ""):
            return item
    return None


def _wait_for_capture(page, records: List[Dict[str, Any]], wait_seconds: float) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    deadline = time.time() + max(1.0, float(wait_seconds))
    fare_entry = None
    confirm_entry = None
    while time.time() < deadline:
        fare_entry = _latest_matching(records, FLIGHT_FARES_URL_TOKEN)
        if fare_entry:
            confirm_entry = _latest_matching(records, CONFIRM_URL_TOKEN)
            return fare_entry, confirm_entry
        page.wait_for_timeout(250)
    return fare_entry, confirm_entry


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Open SalamAir in a real browser, capture native flightFares/confirm traffic, and store reusable manual-session artifacts.",
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="Expected departure date YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT))
    parser.add_argument("--browser-channel", default="chrome", choices=["chromium", "chrome", "msedge"])
    parser.add_argument("--user-data-dir", default=str(DEFAULT_SESSION_ROOT / "salamair_profile"))
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--timeout-ms", type=int, default=180000)
    parser.add_argument("--wait-seconds", type=float, default=240.0)
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--keep-open", action="store_true")
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(
            "Playwright is required. Install with:\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium\n"
            f"Import error: {exc}"
        )

    origin = str(args.origin or "").upper().strip()
    destination = str(args.destination or "").upper().strip()
    expected_date = str(args.date or "").strip()
    session_root = Path(args.session_root)
    records: List[Dict[str, Any]] = []

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
            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(int(args.timeout_ms))

            def on_response(resp) -> None:
                try:
                    url = str(resp.url or "")
                except Exception:
                    return
                if FLIGHT_FARES_URL_TOKEN not in url and CONFIRM_URL_TOKEN not in url:
                    return
                try:
                    body_text = resp.text()
                except Exception:
                    body_text = ""
                try:
                    request_post_data = resp.request.post_data or ""
                except Exception:
                    request_post_data = ""
                records.append(
                    {
                        "captured_at_utc": _now_utc_iso(),
                        "url": url,
                        "status": int(resp.status),
                        "ok": bool(resp.ok),
                        "request_method": str(getattr(resp.request, "method", "GET")),
                        "request_headers": dict(resp.request.headers or {}),
                        "request_body_text": request_post_data,
                        "request_body_json": _safe_json_or_none(request_post_data),
                        "response_headers": dict(resp.headers or {}),
                        "response_body_text": body_text,
                        "response_body_json": _safe_json_or_none(body_text),
                    }
                )

            context.on("response", on_response)
            page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=int(args.timeout_ms))
            page.wait_for_timeout(1500)
            _dismiss_popups(page)

            if not args.non_interactive:
                print("")
                print("Manual steps:")
                print(f"1. In the browser, search {origin} -> {destination} for {expected_date}")
                print("2. Wait for fares to appear.")
                print("3. If possible, click one fare so the site triggers its confirm/summary call too.")
                input("Press ENTER after the search results are visible... ")

            fare_entry, confirm_entry = _wait_for_capture(page, records, args.wait_seconds)
            if not fare_entry:
                raise SystemExit(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "flight_fares_not_captured",
                            "search_page_url": SEARCH_PAGE_URL,
                            "records_seen": len(records),
                        },
                        indent=2,
                        ensure_ascii=False,
                    )
                )

            fares_body = fare_entry.get("response_body_json")
            confirm_body = confirm_entry.get("response_body_json") if isinstance(confirm_entry, dict) else None
            rows = ov.parse_flight_fares_payload(
                fares_body,
                requested_cabin=args.cabin,
                adt=args.adt,
                chd=args.chd,
                inf=args.inf,
                confirm_payload=confirm_body if isinstance(confirm_body, dict) else None,
            )
            if not rows:
                raise SystemExit(
                    json.dumps(
                        {
                            "ok": False,
                            "error": "no_rows_parsed",
                            "fare_request": fare_entry.get("request_body_json"),
                            "fare_status": fare_entry.get("status"),
                        },
                        indent=2,
                        ensure_ascii=False,
                    )
                )

            run_date = str(rows[0].get("departure") or "")[:10] or expected_date
            run_dir = session_root / "runs" / f"ov_{origin}_{destination}_{run_date}_{_now_tag()}"
            fares_path = run_dir / "salamair_flight_fares_response.json"
            confirm_path = run_dir / "salamair_confirm_response.json"
            summary_path = run_dir / "salamair_capture_summary.json"
            rows_path = run_dir / "salamair_rows.json"
            network_path = run_dir / "salamair_captured_requests.json"

            _json_dump(fares_path, fares_body)
            _json_dump(rows_path, rows)
            _json_dump(network_path, records)
            if isinstance(confirm_body, dict):
                _json_dump(confirm_path, confirm_body)

            summary = {
                "captured_at_utc": _now_utc_iso(),
                "carrier": "OV",
                "ok": True,
                "source_type": "playwright_native_intercept",
                "origin": origin,
                "destination": destination,
                "date": run_date,
                "cabin": args.cabin,
                "adt": args.adt,
                "chd": args.chd,
                "inf": args.inf,
                "search_page_url": SEARCH_PAGE_URL,
                "flight_fares_request_body": fare_entry.get("request_body_json"),
                "flight_fares_response_body_path": str(fares_path.resolve()),
                "flight_fares_response_body": fares_body,
                "confirm_response_body_path": str(confirm_path.resolve()) if isinstance(confirm_body, dict) else None,
                "confirm_response_body": confirm_body,
                "rows_path": str(rows_path.resolve()),
                "network_capture_path": str(network_path.resolve()),
                "rows_count": len(rows),
                "sample_rows": rows[:3],
            }
            _json_dump(summary_path, summary)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "run_dir": str(run_dir.resolve()),
                        "summary_path": str(summary_path.resolve()),
                        "rows_count": len(rows),
                        "brands": [row.get("brand") for row in rows],
                    },
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )
            )

            if args.keep_open and not args.non_interactive:
                input("Press ENTER to close browser... ")
        finally:
            if context is not None:
                context.close()
            if browser is not None:
                browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
