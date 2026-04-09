from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MANUAL_ROOT = REPO_ROOT / "output" / "manual_sessions"
RUNS_ROOT = MANUAL_ROOT / "runs"
SEARCH_PATH_TOKEN = "/web/fp/search/flights/v5/aggregated-results"
REFRESH_PATH_TOKEN = "/sso/v2/authorization/by-refresh-token"
DEFAULT_SITE_ROOT = "https://www.airasia.com"
DEFAULT_SEARCH_URL = "https://flights.airasia.com/web/fp/search/flights/v5/aggregated-results"
DEFAULT_PROFILE_DIR = str(MANUAL_ROOT / "airasia_profile")


def parse_args():
    parser = argparse.ArgumentParser(description="Refresh AirAsia direct-session artifacts from browser traffic")
    parser.add_argument("--origin", default="DAC", help="Origin IATA")
    parser.add_argument("--destination", default="KUL", help="Destination IATA")
    parser.add_argument("--date", help="Search date YYYY-MM-DD (default: today+14)")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--currency", default="BDT")
    parser.add_argument("--locale", default="en-gb")
    parser.add_argument("--url", help="Optional explicit page URL to open")
    parser.add_argument("--search-url", default=DEFAULT_SEARCH_URL)
    parser.add_argument("--out", default=str(MANUAL_ROOT / "airasia_session_latest.json"))
    parser.add_argument("--cookies-out", default=str(MANUAL_ROOT / "airasia_cookies.json"))
    parser.add_argument("--headers-out", default=str(MANUAL_ROOT / "airasia_headers_latest.json"))
    parser.add_argument("--storage-state-out", default=str(MANUAL_ROOT / "airasia_storage_state.json"))
    parser.add_argument("--env-out", default=str(MANUAL_ROOT / "airasia_env_latest.ps1"))
    parser.add_argument("--timeout-ms", type=int, default=180000)
    parser.add_argument("--settle-ms", type=int, default=1800)
    parser.add_argument("--wait-seconds", type=float, default=180.0)
    parser.add_argument("--browser-channel", default="chrome", choices=["chromium", "chrome", "msedge"])
    parser.add_argument("--user-data-dir", default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--keep-open", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _default_search_date(raw: Optional[str]) -> str:
    s = str(raw or "").strip()
    if s:
        return s
    return (date.today() + timedelta(days=14)).isoformat()


def _format_search_date(iso_date: str) -> str:
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


def _normalize_cabin(cabin: str) -> str:
    value = str(cabin or "Economy").strip().lower()
    if value in {"business", "premium flatbed", "premium_flatbed", "flatbed"}:
        return "premiumFlatbed"
    if value in {"premium economy", "premium_economy"}:
        return "premiumEconomy"
    return "economy"


def _build_search_page_url(args) -> str:
    if args.url:
        return str(args.url)
    depart_date = _format_search_date(_default_search_date(args.date))
    return (
        f"{DEFAULT_SITE_ROOT}/flights/search/?origin={str(args.origin).upper().strip()}"
        f"&destination={str(args.destination).upper().strip()}"
        f"&departDate={depart_date}"
        "&tripType=O"
        f"&adult={max(1, int(args.adt or 1))}"
        f"&child={max(0, int(args.chd or 0))}"
        f"&infant={max(0, int(args.inf or 0))}"
        f"&locale={str(args.locale or 'en-gb').strip()}"
        f"&currency={str(args.currency or 'BDT').strip()}"
        "&ule=true"
        f"&cabinClass={_normalize_cabin(args.cabin)}"
        "&uce=true&ancillaryAbTest=false&isOC=true&isDC=true&promoCode=&type=paired"
        "&airlineProfile=k,d,g&upsellWidget=true&upsellPremiumFlatbedWidget=true&aid=search"
    )


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _cookies_map(cookies: list[dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for cookie in cookies or []:
        name = str(cookie.get("name") or "").strip()
        if name:
            out[name] = str(cookie.get("value") or "")
    return out


def _safe_json_or_none(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return None


def _dismiss_popups(page):
    labels = ["Accept", "Accept All", "Allow All", "I Agree", "Got it", "OK", "Close"]
    for label in labels:
        try:
            button = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.I)).first
            if button and button.is_visible(timeout=350):
                button.click(timeout=1500)
                page.wait_for_timeout(120)
        except Exception:
            continue


def _wait_for_capture(page, holder: Dict[str, Any], wait_seconds: float) -> bool:
    deadline = time.time() + max(1.0, float(wait_seconds))
    while time.time() < deadline:
        response = holder.get("search_response") or {}
        if response.get("status") == 200 and (response.get("response_body_json") is not None or response.get("response_body_text")):
            return True
        page.wait_for_timeout(250)
    return False


def _build_env_snippet(*, session_out: Path, cookies_out: Path) -> str:
    session_value = str(session_out.resolve()).replace("\\", "/")
    cookies_value = str(cookies_out.resolve()).replace("\\", "/")
    return "\n".join(
        [
            '$env:AIRASIA_SOURCE_MODE="direct"',
            f'$env:AIRASIA_SESSION_FILE="{session_value}"',
            f'$env:AIRASIA_COOKIES_PATH="{cookies_value}"',
            "",
        ]
    )


def main():
    args = parse_args()
    iso_date = _default_search_date(args.date)
    run_dir = RUNS_ROOT / f"ak_{str(args.origin).upper()}_{str(args.destination).upper()}_{iso_date}_{_now_tag()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_summary_path = run_dir / "airasia_capture_summary.json"

    out_path = Path(args.out)
    cookies_out_path = Path(args.cookies_out)
    headers_out_path = Path(args.headers_out)
    storage_state_out_path = Path(args.storage_state_out)
    env_out_path = Path(args.env_out)
    search_page_url = _build_search_page_url(args)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(
            "Playwright is required. Install with:\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium\n"
            f"Import error: {exc}"
        )

    holder: Dict[str, Any] = {}

    def _on_request(req):
        try:
            url = str(req.url or "")
        except Exception:
            return
        if SEARCH_PATH_TOKEN in url:
            post_data = req.post_data or ""
            holder["search_request"] = {
                "captured_at_utc": _now_utc_iso(),
                "request_url": url,
                "request_method": str(getattr(req, "method", "POST")),
                "request_headers": dict(req.headers or {}),
                "request_body_text": post_data,
                "request_body_json": _safe_json_or_none(post_data),
            }
        elif REFRESH_PATH_TOKEN in url:
            post_data = req.post_data or ""
            holder["refresh_request"] = {
                "captured_at_utc": _now_utc_iso(),
                "request_url": url,
                "request_method": str(getattr(req, "method", "POST")),
                "request_headers": dict(req.headers or {}),
                "request_body_text": post_data,
                "request_body_json": _safe_json_or_none(post_data),
            }

    def _on_response(resp):
        try:
            url = str(resp.url or "")
        except Exception:
            return
        if SEARCH_PATH_TOKEN in url or REFRESH_PATH_TOKEN in url:
            try:
                body_text = resp.text()
            except Exception:
                body_text = ""
            payload = {
                "captured_at_utc": _now_utc_iso(),
                "response_url": url,
                "status": int(resp.status),
                "ok": bool(resp.ok),
                "response_headers": dict(resp.headers or {}),
                "response_body_text": body_text,
                "response_body_json": _safe_json_or_none(body_text),
            }
            if SEARCH_PATH_TOKEN in url:
                holder["search_response"] = payload
            else:
                holder["refresh_response"] = payload

    with sync_playwright() as p:
        launch_kwargs: Dict[str, Any] = {"headless": bool(args.headless)}
        if args.browser_channel != "chromium":
            launch_kwargs["channel"] = args.browser_channel

        browser = None
        context = None
        if args.user_data_dir:
            profile_dir = str(Path(args.user_data_dir).resolve())
            Path(profile_dir).mkdir(parents=True, exist_ok=True)
            context = p.chromium.launch_persistent_context(profile_dir, **launch_kwargs)
        else:
            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context()

        context.on("request", _on_request)
        context.on("response", _on_response)
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(int(args.timeout_ms))

        if not args.quiet:
            print(f"Opening {search_page_url}")
        page.goto(search_page_url, wait_until="domcontentloaded")
        page.wait_for_timeout(max(0, int(args.settle_ms)))
        _dismiss_popups(page)
        _wait_for_capture(page, holder, min(8.0, float(args.wait_seconds)))

        if not (holder.get("search_response") or {}).get("response_body_json"):
            if not args.non_interactive:
                print("")
                print("Manual steps:")
                print("1. Clear any Cloudflare / security verification in the opened browser.")
                print("2. If results do not auto-load, run one AirAsia search for the requested route/date.")
                print("3. Wait until flight listings appear.")
                input("Press ENTER after results appear or the search request is in flight... ")
            _wait_for_capture(page, holder, args.wait_seconds)

        if not holder.get("search_request"):
            context.close()
            if browser:
                browser.close()
            raise SystemExit("Failed to capture AirAsia search request. Re-run in headed Chrome/Edge and complete one search.")

        cookies = context.cookies()
        cookies_map = _cookies_map(cookies)
        request_headers = dict((holder.get("search_request") or {}).get("request_headers") or {})
        context.storage_state(path=str(storage_state_out_path))

        _save_json(cookies_out_path, cookies_map)
        _save_json(headers_out_path, request_headers)

        parsed_rows_count = None
        parse_error = None
        response_payload = (holder.get("search_response") or {}).get("response_body_json")
        if isinstance(response_payload, dict):
            try:
                from modules.airasia import parse_aggregated_results

                parsed_rows_count = len(parse_aggregated_results(response_payload, requested_cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf))
            except Exception as exc:
                parse_error = str(exc)

        summary: Dict[str, Any] = {
            "captured_at_utc": _now_utc_iso(),
            "source": "playwright_request_intercept",
            "search_query": {
                "origin": str(args.origin).upper().strip(),
                "destination": str(args.destination).upper().strip(),
                "date": iso_date,
                "cabin": str(args.cabin),
                "adt": max(1, int(args.adt or 1)),
                "chd": max(0, int(args.chd or 0)),
                "inf": max(0, int(args.inf or 0)),
            },
            "search_page_url": search_page_url,
            "search_request": holder.get("search_request"),
            "search_response": holder.get("search_response"),
            "refresh_request": holder.get("refresh_request"),
            "refresh_response": holder.get("refresh_response"),
            "cookies_out": str(cookies_out_path),
            "cookies_count": len(cookies_map),
            "headers_out": str(headers_out_path),
            "headers_count": len(request_headers),
            "storage_state_out": str(storage_state_out_path),
            "parsed_rows_count": parsed_rows_count,
            "parse_error": parse_error,
        }
        _save_json(run_summary_path, summary)
        _save_json(out_path, summary)

        env_out_path.parent.mkdir(parents=True, exist_ok=True)
        env_out_path.write_text(_build_env_snippet(session_out=out_path, cookies_out=cookies_out_path), encoding="utf-8")

        if not args.quiet:
            print(f"Run summary -> {run_summary_path}")
            print(f"Latest summary -> {out_path}")
            print(f"cookies={len(cookies_map)} -> {cookies_out_path}")
            print(f"headers={len(request_headers)} -> {headers_out_path}")
            print(f"storage state -> {storage_state_out_path}")
            if parsed_rows_count is not None:
                print(f"parsed_rows_count={parsed_rows_count}")
            if parse_error:
                print(f"parse_error={parse_error}")

        if args.keep_open and not args.non_interactive:
            input("Press ENTER to close browser... ")

        context.close()
        if browser:
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
