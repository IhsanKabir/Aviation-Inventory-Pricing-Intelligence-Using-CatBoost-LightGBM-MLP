"""
Capture/refresh Indigo session artifacts from real browser traffic.

Purpose:
- Open Indigo in Playwright.
- Trigger (or manually perform) one flight search.
- Capture `/v1/flight/search` request headers + payload.
- Save cookies + headers for modules/indigo.py direct mode retries.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time
from typing import Any, Dict, Optional


SEARCH_PATH = "/v1/flight/search"
TOKEN_REFRESH_PATH = "/v1/token/refresh"
DEFAULT_SITE_URL = "https://www.goindigo.in/"
DEFAULT_SEARCH_API = "https://api-prod-flight-skyplus6e.goindigo.in/v1/flight/search"
DEFAULT_REFRESH_API = "https://api-prod-session-skyplus6e.goindigo.in/v1/token/refresh"


def parse_args():
    p = argparse.ArgumentParser(description="Refresh Indigo session cookies/headers from browser traffic")
    p.add_argument("--url", default=DEFAULT_SITE_URL, help="Page URL to open")
    p.add_argument("--origin", default="DAC", help="Search origin IATA")
    p.add_argument("--destination", default="CCU", help="Search destination IATA")
    p.add_argument("--date", help="Search date YYYY-MM-DD (default: today+14)")
    p.add_argument("--cabin", default="Economy", help="Cabin class")
    p.add_argument("--adt", type=int, default=1, help="Adult count")
    p.add_argument("--chd", type=int, default=0, help="Child count")
    p.add_argument("--inf", type=int, default=0, help="Infant count")
    p.add_argument("--search-url", default=DEFAULT_SEARCH_API, help="Search API URL")
    p.add_argument("--refresh-url", default=DEFAULT_REFRESH_API, help="Token refresh API URL")
    p.add_argument("--disable-auto-trigger", action="store_true", help="Disable JS fetch auto-trigger")
    p.add_argument("--out", default="output/manual_sessions/indigo_session_latest.json", help="Session summary JSON output")
    p.add_argument("--cookies-out", default="output/manual_sessions/indigo_cookies.json", help="Cookies JSON output")
    p.add_argument("--headers-out", default="output/manual_sessions/indigo_headers_latest.json", help="Captured headers JSON output")
    p.add_argument("--env-out", default="output/manual_sessions/indigo_env_latest.ps1", help="PowerShell env snippet output")
    p.add_argument("--timeout-ms", type=int, default=120000, help="Browser timeout in ms")
    p.add_argument("--settle-ms", type=int, default=1200, help="Initial settle delay in ms")
    p.add_argument("--wait-seconds", type=float, default=120.0, help="Capture wait budget")
    p.add_argument(
        "--browser-channel",
        default="chrome",
        choices=["chromium", "chrome", "msedge"],
        help="Browser channel. chrome/msedge often works better for real-session capture.",
    )
    p.add_argument("--user-data-dir", default="", help="Persistent profile dir (recommended)")
    p.add_argument("--headless", action="store_true", help="Run headless")
    p.add_argument("--non-interactive", action="store_true", help="Do not wait for manual ENTER")
    p.add_argument("--keep-open", action="store_true", help="Pause before closing browser in interactive mode")
    p.add_argument("--quiet", action="store_true", help="Reduce stdout logs")
    return p.parse_args()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_search_date(raw: Optional[str]) -> str:
    s = str(raw or "").strip()
    if s:
        return s
    return (date.today() + timedelta(days=14)).isoformat()


def _build_payload(args) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "codes": {
            "currency": "BDT",
            "promotionCode": "",
        },
        "criteria": [
            {
                "dates": {"beginDate": _default_search_date(args.date)},
                "flightFilters": {"type": "All"},
                "stations": {
                    "originStationCodes": [str(args.origin).upper().strip()],
                    "destinationStationCodes": [str(args.destination).upper().strip()],
                },
            }
        ],
        "passengers": {
            "residentCountry": "IN",
            "types": [{"count": max(1, int(args.adt or 1)), "discountCode": "", "type": "ADT"}],
        },
        "taxesAndFees": "TaxesAndFees",
        "tripCriteria": "oneWay",
        "isRedeemTransaction": False,
    }
    chd = max(0, int(args.chd or 0))
    inf = max(0, int(args.inf or 0))
    if chd > 0:
        payload["passengers"]["types"].append({"count": chd, "discountCode": "", "type": "CHD"})
    if inf > 0:
        payload["passengers"]["types"].append({"count": inf, "discountCode": "", "type": "INF"})
    return payload


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _cookies_map(cookies: list[dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for c in cookies or []:
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        out[name] = str(c.get("value") or "")
    return out


def _try_click_search(page) -> Optional[str]:
    script = """
    () => {
      const txt = (el) => ((el.innerText || el.value || '').trim().replace(/\\s+/g, ' '));
      const all = Array.from(document.querySelectorAll("button,[role='button'],input[type='submit']"));
      const patterns = [/^search$/i, /^search now$/i, /^search flights?$/i];
      for (const re of patterns) {
        for (const el of all) {
          const t = txt(el);
          if (t && re.test(t)) { el.click(); return t; }
        }
      }
      for (const el of all) {
        const t = txt(el);
        if (t && /search/i.test(t)) { el.click(); return t; }
      }
      return null;
    }
    """
    try:
        clicked = page.evaluate(script)
    except Exception:
        return None
    return str(clicked) if clicked else None


def _dismiss_popups(page):
    labels = ["Accept", "Accept All", "Allow All", "I Agree", "Got it", "OK", "Close", "No Thanks"]
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.I)).first
            if btn and btn.is_visible(timeout=300):
                btn.click(timeout=2000)
                page.wait_for_timeout(120)
        except Exception:
            continue
    # Generic close icons.
    for selector in ["button[aria-label='Close']", "button[title='Close']", ".close", ".btn-close"]:
        try:
            loc = page.locator(selector).first
            if loc and loc.is_visible(timeout=250):
                loc.click(timeout=1200)
                page.wait_for_timeout(100)
        except Exception:
            continue


def _auto_trigger_fetch(page, refresh_url: str, search_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return page.evaluate(
        """async ({ refreshUrl, searchUrl, payload }) => {
            const headers = {
              "Accept": "*/*",
              "Content-Type": "application/json"
            };
            const out = { refresh: null, search: null };
            try {
              const r1 = await fetch(refreshUrl, {
                method: "PUT",
                credentials: "include",
                headers,
                body: JSON.stringify({})
              });
              const t1 = await r1.text();
              out.refresh = { ok: r1.ok, status: r1.status, textSample: (t1 || "").slice(0, 180) };
            } catch (e) {
              out.refresh = { ok: false, status: null, error: String(e) };
            }
            try {
              const r2 = await fetch(searchUrl, {
                method: "POST",
                credentials: "include",
                headers,
                body: JSON.stringify(payload)
              });
              const t2 = await r2.text();
              out.search = { ok: r2.ok, status: r2.status, textSample: (t2 || "").slice(0, 180) };
            } catch (e) {
              out.search = { ok: false, status: null, error: String(e) };
            }
            return out;
        }""",
        {"refreshUrl": refresh_url, "searchUrl": search_url, "payload": payload},
    )


def _wait_for_capture(page, holder: Dict[str, Any], wait_seconds: float) -> bool:
    deadline = time.time() + max(1.0, float(wait_seconds))
    while time.time() < deadline:
        if holder.get("search_request"):
            return True
        page.wait_for_timeout(200)
    return bool(holder.get("search_request"))


def _build_env_snippet(*, cookies_out: Path, headers_out: Path) -> str:
    return "\n".join(
        [
            "$env:INDIGO_SOURCE_MODE=\"direct\"",
            f"$env:INDIGO_COOKIES_PATH=\"{str(cookies_out).replace(chr(92), '/')}\"",
            f"$env:INDIGO_HEADERS_FILE=\"{str(headers_out).replace(chr(92), '/')}\"",
            "",
        ]
    )


def main():
    args = parse_args()
    out_path = Path(args.out)
    cookies_out_path = Path(args.cookies_out)
    headers_out_path = Path(args.headers_out)
    env_out_path = Path(args.env_out)

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
        headers = req.headers or {}
        if SEARCH_PATH in url and not holder.get("search_request"):
            post_data = req.post_data
            parsed_payload = None
            if post_data:
                try:
                    parsed_payload = json.loads(post_data)
                except Exception:
                    parsed_payload = None
            holder["search_request"] = {
                "captured_at_utc": _now_utc_iso(),
                "request_url": url,
                "request_method": str(getattr(req, "method", "POST")),
                "request_headers": dict(headers),
                "request_body_text": post_data or "",
                "request_body_json": parsed_payload,
            }
        if TOKEN_REFRESH_PATH in url and not holder.get("refresh_request"):
            holder["refresh_request"] = {
                "captured_at_utc": _now_utc_iso(),
                "request_url": url,
                "request_method": str(getattr(req, "method", "PUT")),
                "request_headers": dict(headers),
            }

    with sync_playwright() as p:
        context = None
        browser = None
        launch_kwargs: Dict[str, Any] = {"headless": bool(args.headless)}
        if args.browser_channel != "chromium":
            launch_kwargs["channel"] = args.browser_channel
        if args.user_data_dir:
            context = p.chromium.launch_persistent_context(
                user_data_dir=args.user_data_dir,
                **launch_kwargs,
            )
        else:
            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context()

        context.on("request", _on_request)
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(int(args.timeout_ms))

        if not args.quiet:
            print(f"Opened: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_timeout(max(0, int(args.settle_ms)))
        _dismiss_popups(page)

        auto_result: Optional[Dict[str, Any]] = None
        if not args.disable_auto_trigger:
            payload = _build_payload(args)
            auto_result = _auto_trigger_fetch(page, args.refresh_url, args.search_url, payload)
            _dismiss_popups(page)
            clicked = _try_click_search(page)
            if clicked and not args.quiet:
                print(f"Auto-clicked button: {clicked}")

        if not holder.get("search_request"):
            if not args.non_interactive:
                print("\nManual steps:")
                print("1. Do one Indigo flight search in the opened browser.")
                print("2. Wait until results start loading.")
                input("Press ENTER after results start loading...\n")
            _wait_for_capture(page, holder, args.wait_seconds)

        if not holder.get("search_request"):
            context.close()
            if browser:
                browser.close()
            raise SystemExit(
                "Failed to capture Indigo search request. "
                "Run again with --browser-channel chrome and complete one manual search."
            )

        cookies = context.cookies()
        cookies_map = _cookies_map(cookies)
        request_headers = dict((holder.get("search_request") or {}).get("request_headers") or {})

        _save_json(cookies_out_path, cookies_map)
        _save_json(headers_out_path, request_headers)

        summary: Dict[str, Any] = {
            "captured_at_utc": _now_utc_iso(),
            "source": "playwright_request_intercept",
            "site_url": args.url,
            "auto_trigger_result": auto_result,
            "search_request": holder.get("search_request"),
            "refresh_request": holder.get("refresh_request"),
            "cookies_out": str(cookies_out_path),
            "cookies_count": len(cookies_map),
            "headers_out": str(headers_out_path),
            "headers_count": len(request_headers),
            "headers_hint": {
                "has_cookie": bool(cookies_map),
                "has_origin": bool(request_headers.get("origin") or request_headers.get("Origin")),
                "has_referer": bool(request_headers.get("referer") or request_headers.get("Referer")),
            },
        }
        _save_json(out_path, summary)

        env_text = _build_env_snippet(cookies_out=cookies_out_path, headers_out=headers_out_path)
        env_out_path.parent.mkdir(parents=True, exist_ok=True)
        env_out_path.write_text(env_text, encoding="utf-8")

        if not args.quiet:
            print(f"Captured session -> {out_path}")
            print(f"cookies={len(cookies_map)} -> {cookies_out_path}")
            print(f"headers={len(request_headers)} -> {headers_out_path}")
            print(f"env snippet -> {env_out_path}")

        if args.keep_open and not args.non_interactive:
            input("Press ENTER to close browser...")
        context.close()
        if browser:
            browser.close()


if __name__ == "__main__":
    main()
