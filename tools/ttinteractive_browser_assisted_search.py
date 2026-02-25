"""
Manual browser-assisted TTInteractive session/cookie capture + search probe.

Purpose:
- Support BS (US-Bangla) and 2A (Air Astra) while DataDome blocks direct
  automated requests in this environment.
- This script does not solve captchas automatically. A human operator completes
  any challenge in the browser window, then the script reuses that browser
  session to issue a same-origin SearchFlightsAction request.

Examples:
  python tools/ttinteractive_browser_assisted_search.py --carrier BS --capture-only
  python tools/ttinteractive_browser_assisted_search.py --carrier 2A --origin DAC --destination CGP --date 2026-03-10
  python tools/ttinteractive_browser_assisted_search.py --carrier BS --origin DAC --destination CXB --date 2026-03-10 --proxy-server http://127.0.0.1:8080
  python tools/ttinteractive_browser_assisted_search.py --carrier BS --cdp-url http://127.0.0.1:9222 --origin DAC --destination CGP --date 2026-03-10
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_carrier_module(carrier: str):
    code = (carrier or "").strip().upper()
    if code == "BS":
        from modules import bs as module

        return module, "US-Bangla"
    if code == "2A":
        from modules import airastra as module

        return module, "Air Astra"
    raise ValueError(f"Unsupported carrier: {carrier}")


def _mkdir_parent(path_str: str | None) -> Path | None:
    if not path_str:
        return None
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_json_file(path: str | Path | None) -> Any:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def _write_cookie_outputs(context, cookie_path: Path, cookies_full_path: Path | None) -> tuple[dict[str, str], list[dict[str, Any]]]:
    cookies_full = context.cookies()
    cookies_simple = _cookie_list_to_dict(cookies_full)
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text(json.dumps(cookies_simple, indent=2, ensure_ascii=False), encoding="utf-8")
    if cookies_full_path:
        cookies_full_path.parent.mkdir(parents=True, exist_ok=True)
        cookies_full_path.write_text(json.dumps(cookies_full, indent=2, ensure_ascii=False), encoding="utf-8")
    return cookies_simple, cookies_full


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_cookie_path(carrier: str) -> Path:
    return REPO_ROOT / "output" / "manual_sessions" / f"{carrier.lower()}_cookies.json"


def _cookie_list_to_dict(cookies: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in cookies:
        name = item.get("name")
        if not name:
            continue
        out[str(name)] = str(item.get("value") or "")
    return out


def _slugify_url(url: str) -> str:
    text = (url or "").strip()
    # Drop massive JSON payloads from query strings and keep a stable hash suffix.
    parsed = urlsplit(text)
    q_items = []
    try:
        for k, v in parse_qsl(parsed.query, keep_blank_values=True):
            key = str(k)
            if key.lower() == "json":
                q_items.append((key, "payload"))
            else:
                q_items.append((key, v))
    except Exception:
        q_items = []
    compact_query = urlencode(q_items, doseq=True)
    compact_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, compact_query, parsed.fragment))
    text = re.sub(r"^https?://", "", compact_url, flags=re.I)
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    digest = hashlib.sha1((url or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
    base = (text[:90].strip("_") or "response")
    return f"{base}_{digest}"


def _looks_like_json_text(text: str) -> bool:
    t = (text or "").lstrip()
    return t.startswith("{") or t.startswith("[")


def _looks_like_capture_worthy_tti_html(url: str, content_type: str) -> bool:
    url = (url or "").lower()
    ctype = (content_type or "").lower()
    if "ttinteractive.com" not in url:
        return False
    if "html" not in ctype:
        return False
    return any(
        marker in url
        for marker in (
            "/flexibleflightstaticajax/",
            "/bookingengine/searchresult",
            "/bookingengine/searchflights",
        )
    )


def _looks_like_tti_error_page(html_text: str) -> bool:
    t = (html_text or "").lower()
    return (
        "an error occurred while processing your request" in t
        or ("<title>error</title>" in t and "ttinteractive" in t)
    )


def _is_tti_results_flow_url(url: str) -> bool:
    u = (url or "").lower()
    return (
        "/bookingengine/searchresult" in u
        or "/bookingengine/flexibleflightliststatic" in u
    )


def _is_retryable_goto_error(exc: Exception) -> bool:
    msg = str(exc)
    retry_markers = (
        "ERR_NETWORK_CHANGED",
        "ERR_INTERNET_DISCONNECTED",
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_TUNNEL_CONNECTION_FAILED",
        "ERR_NAME_NOT_RESOLVED",
    )
    return any(marker in msg for marker in retry_markers)


def _goto_with_retries(page, url: str, timeout_ms: int, wait_until: str = "domcontentloaded", max_attempts: int = 4):
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            if attempt > 1:
                print(f"[retry] page.goto attempt {attempt}/{max_attempts}: {url}")
            return page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        except Exception as exc:
            last_exc = exc
            if not _is_retryable_goto_error(exc) or attempt >= max_attempts:
                raise
            sleep_sec = min(2 * attempt, 8)
            print(f"[retry] transient navigation error: {exc}")
            print(f"[retry] waiting {sleep_sec}s before retry...")
            time.sleep(sleep_sec)
    if last_exc:
        raise last_exc
    raise RuntimeError("page.goto failed without exception")


def _get_active_page(context, current_page):
    try:
        if current_page and not current_page.is_closed():
            return current_page
    except Exception:
        pass
    try:
        for candidate in reversed(context.pages):
            try:
                if not candidate.is_closed():
                    return candidate
            except Exception:
                continue
    except Exception:
        pass
    return current_page


def _pick_best_existing_page(context, preferred_domain: str = "ttinteractive.com"):
    try:
        pages = list(context.pages)
    except Exception:
        return None
    if not pages:
        return None
    # Prefer an already-open TTInteractive tab in this context.
    for candidate in reversed(pages):
        try:
            if candidate.is_closed():
                continue
            url = str(candidate.url or "").lower()
            if preferred_domain in url:
                return candidate
        except Exception:
            continue
    # Else return the last live page.
    for candidate in reversed(pages):
        try:
            if not candidate.is_closed():
                return candidate
        except Exception:
            continue
    return None


def _browser_fetch_search(page, search_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # Browsers block some headers (User-Agent/Origin/Referer) from JS fetch;
    # same-origin cookies + default browser headers are enough for this probe.
    js_headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    result = page.evaluate(
        """async ({ url, payload, headers }) => {
            const response = await fetch(url, {
                method: "POST",
                credentials: "include",
                headers,
                body: JSON.stringify(payload),
            });
            const text = await response.text();
            return {
                ok: response.ok,
                status: response.status,
                url: response.url,
                redirected: response.redirected,
                headers: Object.fromEntries(response.headers.entries()),
                text,
            };
        }""",
        {"url": search_url, "payload": payload, "headers": js_headers},
    )
    return dict(result or {})


def _build_search_navigation_url(search_url: str, payload: Dict[str, Any]) -> str:
    parts = urlsplit(search_url)
    query_items = parse_qsl(parts.query, keep_blank_values=True)
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    query_items.append(("json", payload_json))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_items), parts.fragment))


def _extract_frontoffice_session_segment(url: str) -> str | None:
    m = re.search(r"/Zenith/FrontOffice/\(S\([^)]+\)\)", url or "")
    return m.group(0) if m else None


def _replace_frontoffice_session_segment(url: str, session_segment: str) -> str:
    if not session_segment:
        return url
    return re.sub(
        r"/Zenith/FrontOffice/\(S\([^)]+\)\)",
        session_segment,
        url or "",
        count=1,
    )


def _browser_navigate_search(page, search_url: str, payload: Dict[str, Any], timeout_ms: int, settle_ms: int) -> Dict[str, Any]:
    nav_url = _build_search_navigation_url(search_url, payload)
    response = _goto_with_retries(page, nav_url, timeout_ms=timeout_ms)
    page.wait_for_timeout(settle_ms)

    status = None
    headers: Dict[str, Any] = {}
    if response is not None:
        try:
            status = response.status
        except Exception:
            status = None
        try:
            hdrs = response.headers
            headers = dict(hdrs) if isinstance(hdrs, dict) else hdrs
        except Exception:
            try:
                headers = dict(response.all_headers())
            except Exception:
                headers = {}

    final_url = str(page.url or "")
    return {
        "ok": bool(status is not None and 200 <= int(status) < 300),
        "status": status,
        "url": final_url,
        "submitted_url": nav_url,
        "transport": "navigate_get_json_query",
        "redirected": final_url != nav_url,
        "headers": headers,
        "text": page.content(),
    }


def _parse_probe_body(resp: Dict[str, Any]) -> Tuple[str, Any]:
    body_text = str(resp.get("text") or "")
    try:
        return body_text, json.loads(body_text)
    except Exception:
        return body_text, body_text


def _print_env_hint(module, cookies_path: Path | None, proxy_server: str | None) -> None:
    env_cookie = getattr(module, "ENV_COOKIES_PATH", None)
    env_proxy = getattr(module, "ENV_PROXY_URL", None)
    if cookies_path and env_cookie:
        print("")
        print("PowerShell session env for scraper tests:")
        print(f"  $env:{env_cookie} = '{cookies_path.resolve()}'")
        if proxy_server and env_proxy:
            print(f"  $env:{env_proxy} = '{proxy_server}'")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--carrier", required=True, choices=["BS", "2A"])
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--capture-only", action="store_true", help="Only open page and capture cookies")
    parser.add_argument("--proxy-server", help="Browser proxy server, e.g. http://host:port")
    parser.add_argument("--chrome-path", help="Optional real Chrome executable path")
    parser.add_argument("--cdp-url", help="Attach to an existing Chrome/Edge DevTools endpoint, e.g. http://127.0.0.1:9222")
    parser.add_argument("--user-data-dir", help="Persistent browser profile directory (recommended for DataDome stability)")
    parser.add_argument("--storage-state-in", help="Playwright storage state JSON to preload into a new context")
    parser.add_argument("--storage-state-out", help="Write Playwright storage state JSON after the run")
    parser.add_argument("--session-bundle-in", help="Load defaults (proxy/cookie/state paths) from a previous session bundle JSON")
    parser.add_argument("--session-bundle-out", help="Write a session bundle JSON for future reruns")
    parser.add_argument("--cookies-out", help="Output cookie JSON dict for modules.requester")
    parser.add_argument("--cookies-full-out", help="Output full Playwright cookie list JSON")
    parser.add_argument("--response-out", help="Write search response payload/body to file")
    parser.add_argument("--bootstrap-config-out", help="Write parsed TTInteractive data-config JSON to file")
    parser.add_argument("--result-page-out", help="Write final SearchResult HTML page to file if reached")
    parser.add_argument(
        "--network-json-dir",
        help="Directory to save captured TTInteractive JSON responses after loading SearchResult page",
    )
    parser.add_argument("--timeout-ms", type=int, default=120000)
    parser.add_argument("--settle-ms", type=int, default=3000)
    parser.add_argument("--max-search-attempts", type=int, default=3, help="Retries for automated same-browser search when challenge blocks")
    parser.add_argument("--keep-browser-open", action="store_true", help="Pause before closing browser at the end (manual inspection)")
    args = parser.parse_args()

    if args.session_bundle_in:
        bundle = _load_json_file(args.session_bundle_in) or {}
        if not isinstance(bundle, dict):
            raise SystemExit("--session-bundle-in must point to a JSON object")
        args.proxy_server = args.proxy_server or bundle.get("proxy_server")
        args.chrome_path = args.chrome_path or bundle.get("chrome_path")
        args.cdp_url = args.cdp_url or bundle.get("cdp_url")
        args.user_data_dir = args.user_data_dir or bundle.get("user_data_dir")
        args.cookies_out = args.cookies_out or bundle.get("cookies_path")
        args.cookies_full_out = args.cookies_full_out or bundle.get("cookies_full_path")
        args.storage_state_in = args.storage_state_in or bundle.get("storage_state_path")
        if not args.network_json_dir and bundle.get("network_capture_dir"):
            args.network_json_dir = bundle.get("network_capture_dir")

    if not args.capture_only and not (args.origin and args.destination and args.date):
        parser.error("--origin, --destination, and --date are required unless --capture-only is used")
    if args.cdp_url and args.chrome_path:
        print("[warn] --chrome-path is ignored when using --cdp-url (browser is already running)")
        args.chrome_path = None

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(f"Playwright is required for this tool: {exc}")

    module, carrier_label = _load_carrier_module(args.carrier)

    cookie_path = _mkdir_parent(args.cookies_out) or _default_cookie_path(args.carrier)
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookies_full_path = _mkdir_parent(args.cookies_full_out)
    storage_state_in_path = Path(args.storage_state_in) if args.storage_state_in else None
    storage_state_out_path = _mkdir_parent(args.storage_state_out)
    session_bundle_out_path = _mkdir_parent(args.session_bundle_out)
    response_path = _mkdir_parent(args.response_out)
    bootstrap_out_path = _mkdir_parent(args.bootstrap_config_out)
    result_page_out_path = _mkdir_parent(args.result_page_out)
    network_json_dir = Path(args.network_json_dir) if args.network_json_dir else (
        REPO_ROOT / "output" / "manual_sessions" / f"{args.carrier.lower()}_network_json"
    )

    launch_kwargs: Dict[str, Any] = {"headless": False}
    if args.proxy_server:
        launch_kwargs["proxy"] = {"server": args.proxy_server}
    if args.chrome_path:
        launch_kwargs["executable_path"] = args.chrome_path

    print(f"Opening {carrier_label} TTInteractive page in browser...")
    print(f"URL: {module.INDEX_URL}")
    if args.cdp_url:
        print(f"CDP attach: {args.cdp_url}")
    if args.proxy_server and not args.cdp_url:
        print(f"Proxy: {args.proxy_server}")
    elif args.proxy_server and args.cdp_url:
        print(f"Proxy (metadata/fallback only): {args.proxy_server}")
    if args.user_data_dir:
        print(f"User data dir (persistent profile): {args.user_data_dir}")
    elif storage_state_in_path:
        print(f"Preloading storage state: {storage_state_in_path}")

    with sync_playwright() as p:
        browser = None
        persistent_context = None
        cdp_attached = False
        if args.cdp_url:
            browser = p.chromium.connect_over_cdp(args.cdp_url)
            cdp_attached = True
            if browser.contexts:
                context = browser.contexts[0]
            else:
                context = browser.new_context()
            page = _pick_best_existing_page(context) or context.new_page()
            print(f"[cdp] Attached to browser. Contexts={len(browser.contexts)} pages={len(context.pages)}")
        elif args.user_data_dir:
            user_data_dir = str(Path(args.user_data_dir))
            Path(user_data_dir).mkdir(parents=True, exist_ok=True)
            persistent_context = p.chromium.launch_persistent_context(user_data_dir=user_data_dir, **launch_kwargs)
            context = persistent_context
            page = context.pages[0] if context.pages else context.new_page()
        else:
            browser = p.chromium.launch(**launch_kwargs)
            if storage_state_in_path and storage_state_in_path.exists():
                context = browser.new_context(storage_state=str(storage_state_in_path))
            else:
                context = browser.new_context()
            page = context.new_page()
        captured_responses: list[dict[str, Any]] = []

        def on_response(response) -> None:
            try:
                url = response.url or ""
                if "ttinteractive.com" not in url:
                    return
                req = response.request
                method = req.method
                ctype = (response.headers.get("content-type") or "").lower()
                should_capture = ("json" in ctype) or _looks_like_capture_worthy_tti_html(url, ctype)
                if not should_capture:
                    return
                body_text = response.text()
                captured_responses.append(
                    {
                        "url": url,
                        "status": response.status,
                        "method": method,
                        "content_type": ctype,
                        "body_text": body_text,
                    }
                )
                kind = "JSON" if "json" in ctype else "HTML"
                print(f"[capture] {kind} response {response.status} {method} {url}")
            except Exception as exc:
                if "Target page, context or browser has been closed" in str(exc):
                    return
                print(f"[capture] response hook error: {exc}")

        for existing_page in list(getattr(context, "pages", []) or []):
            try:
                if existing_page is page:
                    continue
                existing_page.on("response", on_response)
            except Exception:
                pass
        page.on("response", on_response)
        context.on("page", lambda p: p.on("response", on_response))

        page = _get_active_page(context, page)
        current_url_pre = str((page.url or "") if page else "")
        if cdp_attached and "ttinteractive.com" in current_url_pre.lower():
            print(f"[cdp] Reusing existing tab: {current_url_pre}")
            page.wait_for_timeout(args.settle_ms)
        else:
            _goto_with_retries(page, module.INDEX_URL, timeout_ms=args.timeout_ms)
            page.wait_for_timeout(args.settle_ms)

        print("")
        print("Manual step required:")
        print("1. Complete any anti-bot/captcha challenge in the opened browser window.")
        print("2. Wait until the TTInteractive booking page is loaded normally (idle booking page).")
        print("3. DO NOT type routes/date and DO NOT click the website Search button.")
        if not args.capture_only:
            print("4. Return here and press ENTER to let the script send the search itself.")
        else:
            print("4. Return here and press ENTER to save cookies.")
        input("Press ENTER when ready: ")

        cookies_simple, cookies_full = _write_cookie_outputs(context, cookie_path, cookies_full_path)
        print(f"Saved {len(cookies_simple)} cookies (simple dict) to {cookie_path}")
        if cookies_full_path:
            print(f"Saved full cookie list ({len(cookies_full)} entries) to {cookies_full_path}")
        if storage_state_out_path:
            context.storage_state(path=str(storage_state_out_path))
            print(f"Saved Playwright storage state to {storage_state_out_path}")

        if args.capture_only:
            if session_bundle_out_path:
                bundle = {
                    "carrier": args.carrier,
                    "captured_at_utc": _now_utc_iso(),
                    "proxy_server": args.proxy_server,
                    "chrome_path": args.chrome_path,
                    "cdp_url": args.cdp_url,
                    "user_data_dir": args.user_data_dir,
                    "cookies_path": str(cookie_path),
                    "cookies_full_path": str(cookies_full_path) if cookies_full_path else None,
                    "storage_state_path": str(storage_state_out_path or storage_state_in_path) if (storage_state_out_path or storage_state_in_path) else None,
                    "network_capture_dir": str(network_json_dir),
                    "capture_only": True,
                }
                session_bundle_out_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"Wrote session bundle to {session_bundle_out_path}")
            _print_env_hint(module, cookie_path, args.proxy_server)
            if args.keep_browser_open:
                print("")
                input("Inspection pause: press ENTER to close the browser and exit... ")
            if cdp_attached:
                print("[cdp] Leaving attached browser open.")
            elif persistent_context:
                persistent_context.close()
            elif browser:
                browser.close()
            return

        html_text = page.content()
        page_url = str(page.url or "")
        try:
            cfg = module._extract_data_config(html_text)
            cfg["_bootstrap_meta"] = {
                "index_url": page_url,
                "source": "browser_page",
            }
        except Exception as extract_exc:
            lower_html = html_text.lower()
            # Normal TTInteractive pages can include js.datadome.co tags even when usable.
            looks_datadome = (
                "captcha-delivery.com/captcha" in lower_html
                or "geo.captcha-delivery.com/captcha" in lower_html
                or "please enable js and disable any ad blocker" in lower_html
            )
            snapshot_path = REPO_ROOT / "output" / "manual_sessions" / f"{args.carrier.lower()}_bootstrap_page_snapshot.html"
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(html_text, encoding="utf-8")
            print("")
            print(f"[warn] Could not extract data-config from current browser page (url={page_url})")
            print(f"[warn] Saved page snapshot to {snapshot_path}")
            if looks_datadome:
                print("[warn] The current page still looks like a DataDome/captcha page.")
            print("[fallback] Trying module.bootstrap_config() using captured cookies/proxy...")
            try:
                cfg = module.bootstrap_config(
                    cookies_path=str(cookie_path),
                    proxy_url=args.proxy_server,
                )
                meta = dict(cfg.get("_bootstrap_meta") or {})
                meta["source"] = "requester_fallback"
                meta["browser_page_url"] = page_url
                cfg["_bootstrap_meta"] = meta
                print("[fallback] Loaded bootstrap config successfully via requests/cookies.")
            except Exception as fallback_exc:
                raise RuntimeError(
                    "Unable to obtain TTInteractive bootstrap config. "
                    f"page_url={page_url!r}; extract_error={extract_exc}; fallback_error={fallback_exc}"
                ) from fallback_exc
        if bootstrap_out_path:
            bootstrap_out_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Wrote bootstrap config to {bootstrap_out_path}")

        model = module._build_search_model(
            cfg,
            args.origin,
            args.destination,
            args.date,
            args.cabin,
            args.adt,
            args.chd,
            args.inf,
        )
        rel_url = cfg.get("sourceData", {}).get("Urls", {}).get("SearchFlightsAction")
        if not rel_url:
            raise RuntimeError("SearchFlightsAction URL missing from bootstrap config")
        search_url = urljoin(module.BASE_URL, rel_url)
        meta = dict(cfg.get("_bootstrap_meta") or {})
        bootstrap_source = str(meta.get("source") or "")
        browser_session_seg = _extract_frontoffice_session_segment(page_url)
        search_session_seg = _extract_frontoffice_session_segment(search_url)
        if browser_session_seg and search_session_seg and browser_session_seg != search_session_seg:
            if bootstrap_source == "requester_fallback":
                patched_url = _replace_frontoffice_session_segment(search_url, browser_session_seg)
                if patched_url != search_url:
                    print(
                        "[session] Replaced fallback TTInteractive session in search URL "
                        f"({search_session_seg} -> {browser_session_seg})"
                    )
                    search_url = patched_url
            else:
                print(
                    "[warn] SearchFlightsAction session does not match current browser page "
                    f"({search_session_seg} vs {browser_session_seg})"
                )

        print("")
        print(f"Triggering same-browser SearchFlightsAction navigation (GET with json query) to: {search_url}")
        max_search_attempts = max(1, int(args.max_search_attempts or 1))
        search_attempts = 0
        resp: Dict[str, Any]
        body_text: str
        parsed_body: Any
        while True:
            search_attempts += 1
            page = _get_active_page(context, page)
            resp = _browser_navigate_search(
                page,
                search_url=search_url,
                payload=model,
                timeout_ms=args.timeout_ms,
                settle_ms=args.settle_ms,
            )
            body_text, parsed_body = _parse_probe_body(resp)
            status_int = int(resp.get("status") or 0)
            is_blocked = bool(module._is_datadome_block(status_int, parsed_body))
            if not is_blocked or search_attempts >= max_search_attempts:
                break

            print("")
            print(
                f"[search-step] DataDome blocked search attempt {search_attempts}/{max_search_attempts} "
                f"(status={status_int})."
            )
            print("A captcha/challenge page should now be open in the same browser window.")
            print("Solve the challenge only. Do NOT type routes/date and DO NOT click the website Search button.")
            input("Press ENTER after the search-step challenge is solved to retry the same search: ")
            page = _get_active_page(context, page)
            page.wait_for_timeout(args.settle_ms)

            current_url = str(page.url or "")
            if _is_tti_results_flow_url(current_url):
                print(f"[search-step] Browser already reached TTInteractive results flow: {current_url}")
                resp = {
                    "ok": True,
                    "status": 200,
                    "url": current_url,
                    "submitted_url": resp.get("submitted_url"),
                    "transport": "challenge_followup_page_state",
                    "redirected": True,
                    "headers": resp.get("headers") or {},
                    "text": page.content(),
                }
                body_text, parsed_body = _parse_probe_body(resp)
                break

        final_url = str(resp.get("url") or "")
        reached_search_result = _is_tti_results_flow_url(final_url)
        final_status_int = int(resp.get("status") or 0)
        final_is_datadome = bool(module._is_datadome_block(final_status_int, parsed_body))
        final_is_server_error = final_status_int >= 500
        final_is_tti_error_page = isinstance(parsed_body, str) and _looks_like_tti_error_page(parsed_body)
        fallback_needed = (final_is_datadome or final_is_server_error or final_is_tti_error_page) and not reached_search_result
        if fallback_needed:
            print("")
            if final_is_datadome:
                print("[fallback] Automated same-browser search remained blocked by DataDome.")
            elif final_is_server_error or final_is_tti_error_page:
                print(
                    "[fallback] Automated same-browser search reached TTInteractive but returned an app error "
                    f"(status={final_status_int})."
                )
                print("This usually means the synthetic SearchFlights request is not matching the site's exact UI state.")
            else:
                print("[fallback] Automated same-browser search did not reach SearchResult.")

            # If the synthetic search leaves us on a TTInteractive error page, reset to the booking page
            # before asking the operator to perform the manual UI search flow.
            page = _get_active_page(context, page)
            current_before_manual = str(page.url or "")
            if "/BookingEngine/SearchFlights" in current_before_manual or final_is_tti_error_page:
                print("[fallback] Resetting browser to the booking page before manual UI search...")
                try:
                    _goto_with_retries(page, module.INDEX_URL, timeout_ms=args.timeout_ms)
                    page.wait_for_timeout(args.settle_ms)
                except Exception as exc:
                    print(f"[warn] Could not auto-reset to booking page: {exc}")
                    print("[warn] Please navigate back to the booking page manually before continuing.")

            print("Manual UI capture fallback:")
            print(f"  - In the browser, enter {args.origin} -> {args.destination} for {args.date}")
            print("  - Click the website Search button")
            print("  - Solve any captcha/challenge shown")
            print("  - Wait until results load (SearchResult / FlexibleFlightListStatic) if possible")
            input("Press ENTER after the manual UI search flow has settled (or press ENTER to skip): ")
            page = _get_active_page(context, page)
            page.wait_for_timeout(args.settle_ms)
            current_url = str(page.url or "")
            if _is_tti_results_flow_url(current_url):
                print(f"[fallback] Reached TTInteractive results flow via manual UI flow: {current_url}")
                resp = {
                    "ok": True,
                    "status": 200,
                    "url": current_url,
                    "submitted_url": resp.get("submitted_url"),
                    "transport": "manual_ui_search_page_state",
                    "redirected": True,
                    "headers": resp.get("headers") or {},
                    "text": page.content(),
                }
                body_text, parsed_body = _parse_probe_body(resp)
                final_url = current_url
                reached_search_result = True
            else:
                # Persist the current browser page HTML as the probe body for debugging if manual fallback also fails.
                resp = {
                    "ok": False,
                    "status": final_status_int or None,
                    "url": current_url,
                    "submitted_url": resp.get("submitted_url"),
                    "transport": "manual_ui_search_page_state",
                    "redirected": bool(current_url and current_url != str(resp.get("submitted_url") or "")),
                    "headers": resp.get("headers") or {},
                    "text": page.content(),
                }
                body_text, parsed_body = _parse_probe_body(resp)
                final_url = current_url

        if reached_search_result and not _looks_like_json_text(body_text):
            print("")
            print(f"SearchFlightsAction reached TTInteractive results flow URL: {final_url}")
            print("If a challenge appears on the results page, complete it. Otherwise just wait for the page to finish loading.")
            print("Do NOT click the website Search button here either.")
            input("Press ENTER after the results page has settled: ")
            page = _get_active_page(context, page)
            page.wait_for_timeout(args.settle_ms)
            result_html = page.content()
            out_html = result_page_out_path or (REPO_ROOT / "output" / "manual_sessions" / f"{args.carrier.lower()}_searchresult_page.html")
            out_html.parent.mkdir(parents=True, exist_ok=True)
            out_html.write_text(result_html, encoding="utf-8")
            print(f"Wrote SearchResult page HTML to {out_html}")

        # Refresh saved cookies/state after any challenge or SearchResult navigation.
        try:
            cookies_simple, cookies_full = _write_cookie_outputs(context, cookie_path, cookies_full_path)
            print(f"Refreshed cookies saved ({len(cookies_simple)} names)")
            if storage_state_out_path:
                context.storage_state(path=str(storage_state_out_path))
                print(f"Refreshed Playwright storage state saved to {storage_state_out_path}")
        except Exception as exc:
            print(f"[warn] Failed to refresh saved browser session artifacts: {exc}")

        summary = {
            "carrier": args.carrier,
            "origin": args.origin,
            "destination": args.destination,
            "date": args.date,
            "status": resp.get("status"),
            "ok": resp.get("ok"),
            "url": resp.get("url"),
            "submitted_url": resp.get("submitted_url"),
            "transport": resp.get("transport"),
            "redirected": resp.get("redirected"),
            "search_attempts": search_attempts,
            "reached_search_result_url": reached_search_result,
            "reached_flexibleflightliststatic_url": "/BookingEngine/FlexibleFlightListStatic" in final_url,
            "headers": resp.get("headers"),
            "body": parsed_body,
            "datadome_blocked": bool(module._is_datadome_block(int(resp.get("status") or 0), parsed_body)),
        }

        saved_network_files: list[str] = []
        saved_network_json_files: list[str] = []
        saved_network_html_files: list[str] = []
        if captured_responses:
            network_json_dir.mkdir(parents=True, exist_ok=True)
            for idx, item in enumerate(captured_responses, start=1):
                try:
                    body_text_i = str(item.get("body_text") or "")
                    ctype_i = str(item.get("content_type") or "").lower()
                    slug = _slugify_url(str(item.get("url") or ""))
                    if _looks_like_json_text(body_text_i):
                        out_path = network_json_dir / f"{idx:03d}_{slug}.json"
                        try:
                            obj = json.loads(body_text_i)
                            out_path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
                        except Exception:
                            out_path.write_text(body_text_i, encoding="utf-8")
                        saved_network_json_files.append(str(out_path))
                    elif _looks_like_capture_worthy_tti_html(str(item.get("url") or ""), ctype_i):
                        out_path = network_json_dir / f"{idx:03d}_{slug}.html"
                        out_path.write_text(body_text_i, encoding="utf-8")
                        saved_network_html_files.append(str(out_path))
                    else:
                        continue
                    saved_network_files.append(str(out_path))
                except Exception as exc:
                    print(f"[capture] failed to write captured network file: {exc}")
        if saved_network_files:
            summary["captured_network_files"] = saved_network_files
        if saved_network_json_files:
            summary["captured_network_json_files"] = saved_network_json_files
        if saved_network_html_files:
            summary["captured_network_html_files"] = saved_network_html_files

        # Try parsing captured FlexibleFlightSelectedDays HTML with the carrier module parser.
        parsed_selected_rows = []
        for item in captured_responses:
            url_i = str(item.get("url") or "")
            if "/FlexibleFlightStaticAjax/FlexibleFlightListLoadSelectedDays" not in url_i:
                continue
            try:
                body_i = str(item.get("body_text") or "")
                parsed_selected_rows = module._extract_rows_if_known(
                    body_i,
                    cfg=cfg,
                    cabin=args.cabin,
                    adt=args.adt,
                    chd=args.chd,
                    inf=args.inf,
                )
                if parsed_selected_rows:
                    summary["parsed_selected_days_rows_count"] = len(parsed_selected_rows)
                    summary["parsed_selected_days_sample_rows"] = parsed_selected_rows[:3]

                    parsed_routes = sorted(
                        {
                            (str(r.get("origin") or "").upper(), str(r.get("destination") or "").upper())
                            for r in parsed_selected_rows
                            if r.get("origin") and r.get("destination")
                        }
                    )
                    parsed_dates = sorted(
                        {
                            (str(r.get("search_date") or "")[:10]) or (str(r.get("departure") or "")[:10])
                            for r in parsed_selected_rows
                            if (r.get("search_date") or r.get("departure"))
                        }
                    )
                    expected_route = (str(args.origin or "").upper(), str(args.destination or "").upper())
                    expected_date = str(args.date or "")[:10]
                    route_match = expected_route in parsed_routes if all(expected_route) else True
                    date_match = expected_date in parsed_dates if expected_date else True
                    if not route_match or not date_match:
                        mismatch = {
                            "expected_route": list(expected_route),
                            "expected_date": expected_date,
                            "parsed_routes": [list(x) for x in parsed_routes[:10]],
                            "parsed_dates": parsed_dates[:10],
                            "route_match": route_match,
                            "date_match": date_match,
                        }
                        summary["parsed_selected_days_input_mismatch"] = mismatch
                        print("")
                        print("[warn] Parsed TTInteractive fare rows do not match the CLI inputs.")
                        print(json.dumps(mismatch, indent=2))
                    break
            except Exception as exc:
                summary["parser_error"] = str(exc)
                break

        if session_bundle_out_path:
            bundle = {
                "carrier": args.carrier,
                "captured_at_utc": _now_utc_iso(),
                "proxy_server": args.proxy_server,
                "chrome_path": args.chrome_path,
                "cdp_url": args.cdp_url,
                "user_data_dir": args.user_data_dir,
                "cookies_path": str(cookie_path),
                "cookies_full_path": str(cookies_full_path) if cookies_full_path else None,
                "storage_state_path": str(storage_state_out_path or storage_state_in_path) if (storage_state_out_path or storage_state_in_path) else None,
                "network_capture_dir": str(network_json_dir),
                "bootstrap_config_path": str(bootstrap_out_path) if bootstrap_out_path else None,
                "response_summary_path": str(response_path) if response_path else None,
                "last_probe": {
                    "status": summary.get("status"),
                    "ok": summary.get("ok"),
                    "datadome_blocked": summary.get("datadome_blocked"),
                    "reached_search_result_url": summary.get("reached_search_result_url"),
                    "reached_flexibleflightliststatic_url": summary.get("reached_flexibleflightliststatic_url"),
                    "parsed_selected_days_rows_count": summary.get("parsed_selected_days_rows_count"),
                },
            }
            session_bundle_out_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Wrote session bundle to {session_bundle_out_path}")

        if response_path:
            response_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Wrote response summary to {response_path}")

        print("")
        print("Search probe summary:")
        print(json.dumps(
            {
                "status": summary["status"],
                "ok": summary["ok"],
                "datadome_blocked": summary["datadome_blocked"],
                "redirected": summary["redirected"],
                "reached_search_result_url": summary["reached_search_result_url"],
                "reached_flexibleflightliststatic_url": summary.get("reached_flexibleflightliststatic_url"),
                "body_type": type(parsed_body).__name__,
                "captured_network_json_count": len(saved_network_json_files),
                "captured_network_html_count": len(saved_network_html_files),
                "parsed_selected_days_rows_count": summary.get("parsed_selected_days_rows_count"),
            },
            indent=2,
        ))

        _print_env_hint(module, cookie_path, args.proxy_server)
        if args.keep_browser_open:
            print("")
            input("Inspection pause: press ENTER to close the browser and exit... ")
        if cdp_attached:
            print("[cdp] Leaving attached browser open.")
        elif persistent_context:
            persistent_context.close()
        elif browser:
            browser.close()


if __name__ == "__main__":
    main()
