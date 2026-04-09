"""
Capture/refresh Gozayaan x-kong-segment-id via browser traffic.

This opens Gozayaan in Playwright, tries to trigger a search request, and stores
the captured token to a JSON cache file for modules/gozayaan.py.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Dict, Optional
import uuid


SEARCH_URL_TOKEN = "/api/flight/v4.0/search/"
DEFAULT_SEARCH_URL = "https://production.gozayaan.com/api/flight/v4.0/search/"


def parse_args():
    p = argparse.ArgumentParser(description="Refresh Gozayaan x-kong token from browser traffic")
    p.add_argument("--out", default="output/manual_sessions/gozayaan_token_latest.json", help="Output token cache JSON")
    p.add_argument("--cookies-out", default=None, help="Optional cookie JSON output path (name->value)")
    p.add_argument("--headers-out", default=None, help="Optional request headers JSON output path")
    p.add_argument("--url", default="https://gozayaan.com/", help="Page URL to open")
    p.add_argument("--search-url", default=DEFAULT_SEARCH_URL, help="Gozayaan search API URL to trigger")
    p.add_argument("--origin", help="Target origin airport (e.g. DAC)")
    p.add_argument("--destination", help="Target destination airport (e.g. CXB)")
    p.add_argument("--date", help="Target departure date YYYY-MM-DD")
    p.add_argument("--cabin", default="Economy", help="Cabin class for targeted payload")
    p.add_argument("--adt", type=int, default=1, help="Adult count for targeted payload")
    p.add_argument("--chd", type=int, default=0, help="Child count for targeted payload")
    p.add_argument("--inf", type=int, default=0, help="Infant count for targeted payload")
    p.add_argument("--currency", default="BDT", help="Currency for targeted payload")
    p.add_argument("--region", default="BD", help="Region for targeted payload")
    p.add_argument("--timeout-ms", type=int, default=90000, help="Navigation/action timeout in ms")
    p.add_argument("--settle-ms", type=int, default=1200, help="Initial settle wait in ms")
    p.add_argument("--wait-seconds", type=float, default=45.0, help="Total time budget to capture token")
    p.add_argument("--disable-targeted-fetch", action="store_true", help="Do not auto-trigger a direct API fetch; wait for a real manual/browser search request instead")
    p.add_argument("--disable-auto-click-search", action="store_true", help="Do not auto-click search buttons; wait for a real manual/browser search request instead")
    p.add_argument("--allow-stale-fallback", action="store_true", help="If no fresh token is seen, keep the last stale token candidate instead of failing")
    p.add_argument("--proxy-server", help="Optional proxy server URL, e.g. http://127.0.0.1:8080")
    p.add_argument(
        "--browser-channel",
        default="chromium",
        choices=["chromium", "chrome", "msedge"],
        help="Browser channel. Use 'chrome' for a more realistic logged-in browser session.",
    )
    p.add_argument(
        "--user-data-dir",
        default="",
        help="Persistent browser profile dir. Recommended when reusing a logged-in GOzayaan browser session.",
    )
    p.add_argument("--headless", action="store_true", help="Run browser headless")
    p.add_argument("--non-interactive", action="store_true", help="Do not prompt for manual actions")
    p.add_argument("--keep-open", action="store_true", help="Pause before closing browser (interactive mode only)")
    p.add_argument("--quiet", action="store_true", help="Reduce stdout logging")
    return p.parse_args()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _decode_jwt_exp_utc(token: str) -> Optional[datetime]:
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
        obj = json.loads(decoded.decode("utf-8"))
    except Exception:
        return None
    exp = obj.get("exp")
    if isinstance(exp, (int, float)):
        try:
            return datetime.fromtimestamp(float(exp), tz=timezone.utc)
        except Exception:
            return None
    return None


def _seconds_until(dt_utc: Optional[datetime]) -> Optional[int]:
    if dt_utc is None:
        return None
    return int((dt_utc - datetime.now(timezone.utc)).total_seconds())


def _token_preview(token: str) -> str:
    s = str(token or "")
    if len(s) <= 18:
        return s
    return f"{s[:10]}...{s[-6:]}"


def _dismiss_popups(page, timeout_ms: int):
    labels = [
        "Accept",
        "Accept All",
        "Allow All",
        "I Agree",
        "Got it",
        "OK",
    ]
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.I)).first
            if btn and btn.is_visible(timeout=300):
                btn.click(timeout=min(timeout_ms, 2000))
                page.wait_for_timeout(150)
        except Exception:
            continue


def _try_click_search(page) -> Optional[str]:
    # JS-side click is more resilient across frequent UI selector changes.
    script = """
    () => {
      const textOf = (el) => {
        const t = (el.innerText || el.value || "").trim();
        return t.replace(/\\s+/g, " ");
      };
      const candidates = Array.from(document.querySelectorAll("button,[role='button'],input[type='submit']"));
      const strict = [/^search$/i, /^search flights?$/i, /^find flights?$/i];
      for (const re of strict) {
        for (const el of candidates) {
          const t = textOf(el);
          if (!t) continue;
          if (re.test(t)) { el.click(); return t; }
        }
      }
      for (const el of candidates) {
        const t = textOf(el);
        if (!t) continue;
        if (/search/i.test(t)) { el.click(); return t; }
      }
      return null;
    }
    """
    try:
        clicked = page.evaluate(script)
    except Exception:
        return None
    return str(clicked) if clicked else None


def _build_target_payload(args) -> Optional[Dict[str, Any]]:
    origin = str(args.origin or "").strip().upper()
    destination = str(args.destination or "").strip().upper()
    date = str(args.date or "").strip()
    if not (origin and destination and date):
        return None
    return {
        "adult": max(1, int(args.adt or 1)),
        "child": max(0, int(args.chd or 0)),
        "child_age": [],
        "infant": max(0, int(args.inf or 0)),
        "cabin_class": str(args.cabin or "Economy"),
        "trips": [
            {
                "origin": origin,
                "destination": destination,
                "preferred_time": date,
            }
        ],
        "currency": str(args.currency or "BDT"),
        "region": str(args.region or "BD"),
        "segment_id": str(uuid.uuid4()),
        "platform_type": "GZ_WEB",
        "trip_type": "One Way",
    }


def _trigger_targeted_fetch(page, search_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    js_headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }
    return page.evaluate(
        """async ({ url, payload, headers }) => {
            try {
                const response = await fetch(url, {
                    method: "POST",
                    credentials: "include",
                    headers,
                    body: JSON.stringify(payload),
                });
                const text = await response.text();
                return { ok: response.ok, status: response.status, textSample: (text || "").slice(0, 180) };
            } catch (err) {
                return { ok: false, status: null, error: String(err) };
            }
        }""",
        {"url": search_url, "payload": payload, "headers": js_headers},
    )


def _wait_for_capture(page, holder: Dict[str, Any], wait_seconds: float):
    deadline = time.time() + max(1.0, float(wait_seconds))
    while time.time() < deadline:
        if holder.get("token"):
            return
        page.wait_for_timeout(200)


def _save_output(path: Path, payload: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _save_cookies(path: Path, cookies: list[dict[str, Any]]) -> int:
    out: Dict[str, str] = {}
    for item in cookies:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        out[name] = str(item.get("value") or "")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    return len(out)


def _save_headers(path: Path, headers: Dict[str, Any]) -> int:
    clean: Dict[str, str] = {}
    for k, v in (headers or {}).items():
        name = str(k or "").strip()
        if not name:
            continue
        clean[name] = str(v or "")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(clean, indent=2, ensure_ascii=False), encoding="utf-8")
    return len(clean)


def main():
    args = parse_args()
    out_path = Path(args.out)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(
            "Playwright is required for token refresh. Install with:\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium\n"
            f"Import error: {exc}"
        )

    holder: Dict[str, Any] = {"capture_enabled": False}

    def _arm_capture():
        holder["capture_enabled"] = True
        holder.pop("token", None)
        holder.pop("request_url", None)
        holder.pop("request_method", None)
        holder.pop("captured_at_utc", None)
        holder.pop("expires_at_utc", None)
        holder.pop("ttl_sec", None)
        holder.pop("stale_candidate", None)

    def _on_request(req):
        if not holder.get("capture_enabled"):
            return
        try:
            url = str(req.url or "")
        except Exception:
            return
        if SEARCH_URL_TOKEN not in url:
            return
        try:
            headers = req.headers or {}
        except Exception:
            headers = {}
        token = str(headers.get("x-kong-segment-id") or headers.get("X-Kong-Segment-Id") or "").strip()
        if not token or holder.get("token"):
            return
        exp_utc = _decode_jwt_exp_utc(token)
        ttl_sec = _seconds_until(exp_utc)
        candidate = {
            "token": token,
            "request_url": url,
            "request_method": str(getattr(req, "method", "POST")),
            "request_headers": dict(headers),
            "captured_at_utc": _now_utc_iso(),
            "expires_at_utc": exp_utc.isoformat() if exp_utc else None,
            "ttl_sec": ttl_sec,
        }
        if ttl_sec is None or ttl_sec > 0:
            holder.update(candidate)
            return
        if not holder.get("stale_candidate"):
            holder["stale_candidate"] = candidate

    with sync_playwright() as p:
        launch_kwargs: Dict[str, Any] = {"headless": bool(args.headless)}
        channel = None if args.browser_channel == "chromium" else args.browser_channel
        if channel:
            launch_kwargs["channel"] = channel
        if args.proxy_server:
            launch_kwargs["proxy"] = {"server": args.proxy_server}
        browser = None
        context = None
        try:
            if args.user_data_dir:
                profile_dir = str(Path(args.user_data_dir).resolve())
                os.makedirs(profile_dir, exist_ok=True)
                context = p.chromium.launch_persistent_context(profile_dir, **launch_kwargs)
            else:
                browser = p.chromium.launch(**launch_kwargs)
                context = browser.new_context()
        except Exception as exc:
            msg = str(exc)
            if "Executable doesn't exist" in msg:
                raise SystemExit(
                    "Playwright browser binary is missing. Run once:\n"
                    "  python -m playwright install chromium\n"
                    f"Launch error: {msg}"
                )
            raise SystemExit(f"Failed to launch browser: {msg}")
        context.on("request", _on_request)
        page = context.new_page()
        page.set_default_timeout(args.timeout_ms)

        if not args.quiet and args.user_data_dir:
            print(
                "Browser mode:",
                json.dumps(
                    {
                        "channel": args.browser_channel,
                        "persistent_profile": True,
                        "user_data_dir": str(Path(args.user_data_dir).resolve()),
                    }
                ),
            )

        if not args.quiet:
            print(f"Opening {args.url}")
        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_timeout(max(0, int(args.settle_ms)))
        _dismiss_popups(page, args.timeout_ms)

        target_payload = _build_target_payload(args)
        targeted_fetch_result = None
        if target_payload and not args.disable_targeted_fetch:
            _arm_capture()
            targeted_fetch_result = _trigger_targeted_fetch(page, args.search_url, target_payload)
            if not args.quiet:
                print(
                    "Triggered targeted search fetch:",
                    json.dumps(
                        {
                            "status": targeted_fetch_result.get("status"),
                            "ok": targeted_fetch_result.get("ok"),
                            "error": targeted_fetch_result.get("error"),
                        }
                    ),
                )
            _wait_for_capture(page, holder, args.wait_seconds)

        if not holder.get("token") and not args.disable_auto_click_search:
            _arm_capture()
            clicked_text = _try_click_search(page)
            if clicked_text and (not args.quiet):
                print(f"Auto-clicked button: {clicked_text}")
            _wait_for_capture(page, holder, args.wait_seconds)

        if not holder.get("token") and not args.non_interactive:
            print("")
            print("No token captured yet. Do one flight search in the opened browser window.")
            input("Press ENTER after search results start loading... ")
            _arm_capture()
            _wait_for_capture(page, holder, args.wait_seconds)

        if not holder.get("token") and holder.get("stale_candidate") and args.allow_stale_fallback:
            holder.update(holder["stale_candidate"])
            if not args.quiet:
                print("Only stale token candidate observed; using it as fallback.")

        if not holder.get("token"):
            context.close()
            if browser is not None:
                browser.close()
            raise SystemExit(
                "Failed to capture x-kong-segment-id from browser traffic. "
                "Try running without --headless and complete one manual search."
            )

        token = str(holder["token"])
        output = {
            "captured_at_utc": holder.get("captured_at_utc") or _now_utc_iso(),
            "x_kong_segment_id": token,
            "token_preview": _token_preview(token),
            "expires_at_utc": holder.get("expires_at_utc"),
            "ttl_sec": holder.get("ttl_sec"),
            "token_is_stale": bool(holder.get("ttl_sec") is not None and int(holder.get("ttl_sec")) <= 0),
            "request_url": holder.get("request_url"),
            "request_method": holder.get("request_method"),
            "request_headers": holder.get("request_headers"),
            "target_payload": target_payload,
            "targeted_fetch_result": targeted_fetch_result,
            "source": "playwright_request_intercept",
        }
        if args.cookies_out:
            cookie_path = Path(args.cookies_out)
            cookie_count = _save_cookies(cookie_path, context.cookies())
            output["cookies_out"] = str(cookie_path)
            output["cookies_count"] = cookie_count
        if args.headers_out and isinstance(holder.get("request_headers"), dict):
            headers_path = Path(args.headers_out)
            headers_count = _save_headers(headers_path, holder["request_headers"])
            output["headers_out"] = str(headers_path)
            output["headers_count"] = headers_count
        _save_output(out_path, output)

        print(f"Captured token -> {out_path}")
        print(f"token={output['token_preview']}; ttl_sec={output.get('ttl_sec')}")
        if output.get("cookies_out"):
            print(f"cookies={output['cookies_count']} -> {output['cookies_out']}")
        if output.get("headers_out"):
            print(f"headers={output['headers_count']} -> {output['headers_out']}")
        print("Use GOZAYAAN_TOKEN_CACHE_FILE to point modules/gozayaan.py to this file if needed.")

        if args.keep_open and not args.non_interactive:
            input("Press ENTER to close browser... ")
        context.close()
        if browser is not None:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
