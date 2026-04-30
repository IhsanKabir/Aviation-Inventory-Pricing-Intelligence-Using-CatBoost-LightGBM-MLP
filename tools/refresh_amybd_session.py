"""
Capture/refresh AMYBD authenticated session from browser traffic.

Purpose:
- Open AMYBD in Playwright.
- You log in and run one flight search manually.
- Script captures the atapi.aspx search request/response and writes:
  - session summary JSON
  - cookies JSON (name->value)
  - headers JSON (captured request headers)
  - PowerShell env snippet for BS/2A AMYBD mode
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import sys
from pathlib import Path
import re
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl
import time

# Allow `from core.atomic_write import ...` regardless of where this script
# is launched from.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.atomic_write import atomic_write_json, atomic_write_text  # noqa: E402


SEARCH_PATH = "/atapi.aspx"
SEARCH_COMMANDS = {"_FLIGHTSEARCH_", "_FLIGHTSEARCHOPEN_"}


def parse_args():
    p = argparse.ArgumentParser(description="Refresh AMYBD auth session from browser traffic")
    p.add_argument("--url", default="https://www.amybd.com/flights", help="Page URL to open")
    p.add_argument("--origin", default="DAC", help="Target origin IATA for optional auto-trigger")
    p.add_argument("--destination", default="CGP", help="Target destination IATA for optional auto-trigger")
    p.add_argument("--date", help="Target departure date YYYY-MM-DD for optional auto-trigger (default: today+7)")
    p.add_argument("--cabin", default="Economy", help="Cabin for optional auto-trigger")
    p.add_argument("--adt", type=int, default=1, help="Adult count for optional auto-trigger")
    p.add_argument("--chd", type=int, default=0, help="Child count for optional auto-trigger")
    p.add_argument("--inf", type=int, default=0, help="Infant count for optional auto-trigger")
    p.add_argument("--search-cmnd", default="_FLIGHTSEARCHOPEN_", choices=["_FLIGHTSEARCH_", "_FLIGHTSEARCHOPEN_"], help="CMND value for optional auto-trigger")
    p.add_argument("--disable-auto-click-login", action="store_true", help="Disable automatic click on Sign In / Sign Up")
    p.add_argument("--disable-auto-click-google", action="store_true", help="Disable automatic click on Continue with Google")
    p.add_argument("--disable-auto-click-google-account", action="store_true", help="Disable automatic click on Google account chooser row")
    p.add_argument(
        "--google-email",
        default=os.environ.get("AMYBD_GOOGLE_EMAIL", ""),
        help="Google email for auto-fill on accounts.google.com (or set AMYBD_GOOGLE_EMAIL)",
    )
    p.add_argument("--disable-auto-fill-google-email", action="store_true", help="Disable automatic Google email entry")
    p.add_argument("--google-login-timeout-sec", type=float, default=20.0, help="Max seconds to wait for Google login page")
    p.add_argument(
        "--google-password",
        default=os.environ.get("AMYBD_GOOGLE_PASSWORD", ""),
        help="Google password for auto-fill on accounts.google.com (or set AMYBD_GOOGLE_PASSWORD)",
    )
    p.add_argument("--disable-auto-fill-google-password", action="store_true", help="Disable automatic Google password entry")
    p.add_argument("--google-password-timeout-sec", type=float, default=30.0, help="Max seconds to wait for Google password page")
    p.add_argument("--disable-auto-ui-search", action="store_true", help="Disable automatic AMYBD UI form fill + Search Now click")
    p.add_argument("--auto-ui-search-wait-sec", type=float, default=6.0, help="Seconds to wait after auto UI search for request capture")
    p.add_argument("--disable-auto-trigger", action="store_true", help="Disable automatic atapi search trigger after ENTER")
    p.add_argument("--out", default="output/manual_sessions/amybd_session_latest.json", help="Session summary output JSON")
    p.add_argument("--cookies-out", default="output/manual_sessions/amybd_cookies.json", help="Cookie JSON output path")
    p.add_argument("--headers-out", default="output/manual_sessions/amybd_headers_latest.json", help="Headers JSON output path")
    p.add_argument("--env-out", default="output/manual_sessions/amybd_env_latest.ps1", help="PowerShell env snippet output path")
    p.add_argument("--timeout-ms", type=int, default=120000, help="Browser timeout in ms")
    p.add_argument("--wait-seconds", type=float, default=180.0, help="Time budget to capture a successful search request")
    p.add_argument("--proxy-server", help="Optional proxy server URL, e.g. http://127.0.0.1:8080")
    p.add_argument(
        "--browser-channel",
        default="chromium",
        choices=["chromium", "chrome", "msedge"],
        help="Browser channel. Use 'chrome' to avoid Google OAuth rejection in Chrome-for-Testing.",
    )
    p.add_argument(
        "--user-data-dir",
        default="",
        help="Persistent browser profile dir (recommended with --browser-channel chrome).",
    )
    p.add_argument(
        "--disable-stealth-arg",
        action="store_true",
        help="Disable --disable-blink-features=AutomationControlled launch arg.",
    )
    p.add_argument(
        "--disable-auto-dismiss-amybd-popup",
        action="store_true",
        help="Disable automatic dismissal of AMYBD in-page popups (e.g., Ok modal).",
    )
    p.add_argument("--headless", action="store_true", help="Run headless (not recommended for login)")
    p.add_argument("--non-interactive", action="store_true", help="Do not prompt for manual confirmation")
    p.add_argument("--keep-open", action="store_true", help="Pause before closing browser (interactive only)")
    p.add_argument("--quiet", action="store_true", help="Reduce stdout logging")
    return p.parse_args()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    atomic_write_json(path, payload)


def _clean_headers(headers: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (headers or {}).items():
        key = str(k or "").strip()
        if not key:
            continue
        out[key] = str(v or "")
    return out


def _cookies_from_context(cookies: list[dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for c in cookies or []:
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        out[name] = str(c.get("value") or "")
    return out


def _cookies_from_header(cookie_header: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    raw = str(cookie_header or "").strip()
    if not raw:
        return out
    for part in raw.split(";"):
        p = part.strip()
        if not p or "=" not in p:
            continue
        k, v = p.split("=", 1)
        key = str(k or "").strip()
        if not key:
            continue
        out[key] = str(v or "").strip()
    return out


def _parse_payload(raw_body: Optional[str]) -> Optional[Dict[str, Any]]:
    raw = str(raw_body or "").strip()
    if not raw:
        return None

    # Case 1: body is JSON text directly.
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Case 2: form-urlencoded body.
    try:
        kv = dict(parse_qsl(raw, keep_blank_values=True))
    except Exception:
        kv = {}
    if not kv:
        return None

    # Some sites use json=<payload>.
    if "json" in kv:
        try:
            obj = json.loads(str(kv.get("json") or ""))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    # Fallback: if common keys present, treat kv as payload.
    if "CMND" in kv or "FROM" in kv or "DEST" in kv:
        return kv
    return None


def _is_search_candidate(url: str, method: str, payload: Optional[Dict[str, Any]]) -> bool:
    if SEARCH_PATH not in str(url or ""):
        return False
    if str(method or "").upper() != "POST":
        return False
    if not isinstance(payload, dict):
        return False
    cmnd = str(payload.get("CMND") or "").upper().strip()
    return cmnd in SEARCH_COMMANDS


def _date_or_default(date_raw: Optional[str]) -> str:
    raw = str(date_raw or "").strip()
    if raw:
        return raw
    return (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()


def _airport_label(iata: str) -> str:
    labels = {
        "DAC": "Dhaka - DAC - BANGLADESH",
        "CGP": "Chittagong - CGP - BANGLADESH",
        "CXB": "Cox's Bazar - CXB - BANGLADESH",
        "JSR": "Jessore - JSR - BANGLADESH",
        "RJH": "Rajshahi - RJH - BANGLADESH",
        "SPD": "Saidpur - SPD - BANGLADESH",
        "ZYL": "Sylhet - ZYL - BANGLADESH",
        "BZL": "Barisal - BZL - BANGLADESH",
    }
    code = str(iata or "").strip().upper()
    return labels.get(code, f"{code} - {code} - BANGLADESH")


def _to_amybd_date(date_iso: str) -> str:
    try:
        return datetime.fromisoformat(str(date_iso)).strftime("%d-%b-%Y")
    except Exception:
        return str(date_iso)


def _build_target_payload(args) -> Dict[str, Any]:
    date_iso = _date_or_default(args.date)
    today_text = datetime.now(timezone.utc).strftime("%d-%b-%Y")
    cabin_code = "C" if "business" in str(args.cabin or "").strip().lower() else "Y"
    return {
        "is_combo": 0,
        "CMND": str(args.search_cmnd or "_FLIGHTSEARCHOPEN_"),
        "TRIP": "OW",
        "FROM": _airport_label(args.origin),
        "DEST": _airport_label(args.destination),
        "JDT": _to_amybd_date(date_iso),
        "RDT": today_text,
        "ACLASS": cabin_code,
        "AD": max(1, int(args.adt or 1)),
        "CH": max(0, int(args.chd or 0)),
        "INF": max(0, int(args.inf or 0)),
        "Umrah": "0",
        "DOBC1": "01-Mar-2017",
        "DOBC2": "01-Mar-2017",
        "DOBC3": "01-Mar-2017",
        "DOBC4": "01-Mar-2017",
    }


def _trigger_targeted_fetch(page, payload: Dict[str, Any]) -> Dict[str, Any]:
    return page.evaluate(
        """async ({ payload }) => {
            const bodyJson = JSON.stringify(payload || {});
            const bodyFormJson = new URLSearchParams({ json: bodyJson }).toString();
            const bodyFormFlat = new URLSearchParams(
                Object.entries(payload || {}).map(([k, v]) => [k, typeof v === "object" ? JSON.stringify(v) : String(v ?? "")])
            ).toString();
            const variants = [
                { name: "raw_json_text", body: bodyJson },
                { name: "form_json_field", body: bodyFormJson },
                { name: "form_flattened", body: bodyFormFlat },
            ];
            try {
                let best = null;
                for (const variant of variants) {
                    const response = await fetch("/atapi.aspx", {
                        method: "POST",
                        credentials: "include",
                        headers: {
                            "Accept": "application/json, text/javascript, */*; q=0.01",
                            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        body: variant.body,
                    });
                    const text = await response.text();
                    let parsed = null;
                    try { parsed = JSON.parse(text || ""); } catch (_) {}
                    const candidate = {
                        variant: variant.name,
                        ok: response.ok,
                        status: response.status,
                        responseUrl: response.url,
                        message: parsed && typeof parsed === "object" ? parsed.message || null : null,
                        success: parsed && typeof parsed === "object" ? parsed.success : null,
                        textSample: (text || "").slice(0, 180),
                    };
                    if (!best) best = candidate;
                    if (candidate.ok || candidate.success === true) {
                        best = candidate;
                        break;
                    }
                }
                return best || { ok: false, status: null, error: "no_fetch_attempt" };
            } catch (err) {
                return { ok: false, status: null, error: String(err) };
            }
        }""",
        {"payload": payload},
    )


def _extract_iata(value: Optional[str], fallback: str) -> str:
    raw = str(value or "").upper()
    m = re.search(r"\b([A-Z]{3})\b", raw)
    if m:
        return m.group(1)
    return str(fallback or "").upper()


def _pick_first_visible(locator, max_probe: int = 10):
    try:
        count = int(locator.count())
    except Exception:
        return None
    for i in range(min(max_probe, count)):
        item = locator.nth(i)
        try:
            if item.is_visible():
                return item
        except Exception:
            continue
    return None


def _visible_text_inputs(page, max_probe: int = 30):
    out = []
    loc = page.locator("input")
    try:
        count = int(loc.count())
    except Exception:
        count = 0
    for i in range(min(max_probe, count)):
        item = loc.nth(i)
        try:
            if not item.is_visible():
                continue
            if not item.is_enabled():
                continue
            t = str(item.get_attribute("type") or "").strip().lower()
            if t in {"hidden", "radio", "checkbox", "button", "submit", "file"}:
                continue
            out.append(item)
        except Exception:
            continue
    return out


def _type_airport_value(page, locator, value: str) -> bool:
    v = str(value or "").strip()
    if not v:
        return False
    try:
        locator.click(timeout=2500, force=True)
        try:
            locator.press("Control+A")
        except Exception:
            pass
        locator.fill("", timeout=2500)
        # Type naturally; AMYBD field listeners often ignore direct JS assignment.
        locator.type(v, delay=70, timeout=8000)
        page.wait_for_timeout(450)
        # Select first suggestion if autocomplete appears.
        try:
            locator.press("ArrowDown")
            page.wait_for_timeout(120)
        except Exception:
            pass
        locator.press("Enter")
        page.wait_for_timeout(350)
        return True
    except Exception:
        return False


def _auto_submit_amybd_ui_search(page, payload: Dict[str, Any]) -> Dict[str, Any]:
    trips = payload.get("trips") if isinstance(payload, dict) else None
    t0 = trips[0] if isinstance(trips, list) and trips else {}
    from_value = _extract_iata(payload.get("FROM") or t0.get("origin"), "DAC")
    dest_value = _extract_iata(payload.get("DEST") or t0.get("destination"), "CGP")

    from_loc = _pick_first_visible(
        page.locator("xpath=(//*[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'journey from')])[1]/following::input[1]")
    )
    to_loc = _pick_first_visible(
        page.locator("xpath=(//*[contains(translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'journey to')])[1]/following::input[1]")
    )

    fallback_inputs = _visible_text_inputs(page)
    if from_loc is None and len(fallback_inputs) >= 1:
        from_loc = fallback_inputs[0]
    if to_loc is None and len(fallback_inputs) >= 2:
        to_loc = fallback_inputs[1]

    from_filled = bool(from_loc and _type_airport_value(page, from_loc, from_value))
    dest_filled = bool(to_loc and _type_airport_value(page, to_loc, dest_value))

    search_clicked = False
    search_text = None
    search_error = None
    for candidate in [
        _pick_first_visible(page.get_by_role("button", name=re.compile(r"search\s*now", re.I))),
        _pick_first_visible(page.get_by_role("button", name=re.compile(r"^\s*search\s*$", re.I))),
        _pick_first_visible(page.locator("text=/search\\s*now/i")),
    ]:
        if candidate is None:
            continue
        try:
            search_text = candidate.inner_text(timeout=800)
        except Exception:
            search_text = None
        try:
            candidate.click(timeout=3500, force=True)
            search_clicked = True
            page.wait_for_timeout(500)
            break
        except Exception as exc:
            search_error = str(exc)
            continue

    if not search_clicked:
        # Last resort: press Enter on destination field.
        try:
            if to_loc is not None:
                to_loc.press("Enter")
                search_clicked = True
        except Exception as exc:
            search_error = search_error or str(exc)

    return {
        "fromFilled": from_filled,
        "destFilled": dest_filled,
        "dateFilled": False,
        "searchClicked": search_clicked,
        "searchText": search_text,
        "fromValue": from_value,
        "destValue": dest_value,
        "searchError": search_error,
    }


def _auto_click_login_entry(page) -> Dict[str, Any]:
    patterns = [
        re.compile(r"sign\s*in\s*\|\s*sign\s*up", re.I),
        re.compile(r"sign\s*in\s*/\s*sign\s*up", re.I),
        re.compile(r"sign\s*in", re.I),
        re.compile(r"log\s*in", re.I),
    ]
    probes = [
        ("role:link", lambda pat: page.get_by_role("link", name=pat)),
        ("role:button", lambda pat: page.get_by_role("button", name=pat)),
        ("text:locator", lambda pat: page.locator(f"text=/{pat.pattern}/i")),
    ]
    for pat in patterns:
        for label, build in probes:
            try:
                loc = build(pat)
                if loc.count() < 1:
                    continue
                loc.first.click(timeout=4000, force=True)
                return {"clicked": True, "via": label, "pattern": pat.pattern}
            except Exception:
                continue

    # Fallback: direct DOM text scan
    try:
        result = page.evaluate(
            """() => {
                const nodes = Array.from(document.querySelectorAll("a,button,[role='button']"));
                const norm = (s) => String(s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                const matchers = [
                    "sign in | sign up",
                    "sign in / sign up",
                    "sign in",
                    "log in",
                    "login"
                ];
                for (const el of nodes) {
                    const txt = norm(el.innerText || el.textContent || el.getAttribute("aria-label"));
                    if (!txt) continue;
                    if (matchers.some((m) => txt.includes(m))) {
                        el.click();
                        return { clicked: true, via: "dom_scan", text: txt };
                    }
                }
                return { clicked: false, via: "dom_scan" };
            }"""
        )
        if isinstance(result, dict):
            return result
    except Exception as exc:
        return {"clicked": False, "via": "dom_scan", "error": str(exc)}
    return {"clicked": False, "via": "selector_scan"}


def _auto_click_continue_with_google(page) -> Dict[str, Any]:
    patterns = [
        re.compile(r"continue\s+with\s+google", re.I),
        re.compile(r"google", re.I),
    ]
    probes = [
        ("role:button", lambda pat: page.get_by_role("button", name=pat)),
        ("role:link", lambda pat: page.get_by_role("link", name=pat)),
        ("text:locator", lambda pat: page.locator(f"text=/{pat.pattern}/i")),
    ]
    for pat in patterns:
        for label, build in probes:
            try:
                loc = build(pat)
                if loc.count() < 1:
                    continue
                loc.first.click(timeout=5000, force=True)
                return {"clicked": True, "via": label, "pattern": pat.pattern}
            except Exception:
                continue

    try:
        result = page.evaluate(
            """() => {
                const nodes = Array.from(document.querySelectorAll("a,button,[role='button'],div"));
                const norm = (s) => String(s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                for (const el of nodes) {
                    const txt = norm(el.innerText || el.textContent || el.getAttribute("aria-label"));
                    if (!txt) continue;
                    if (txt.includes("continue with google")) {
                        el.click();
                        return { clicked: true, via: "dom_scan", text: txt };
                    }
                }
                return { clicked: false, via: "dom_scan" };
            }"""
        )
        if isinstance(result, dict):
            return result
    except Exception as exc:
        return {"clicked": False, "via": "dom_scan", "error": str(exc)}
    return {"clicked": False, "via": "selector_scan"}


def _mask_email(email: Optional[str]) -> Optional[str]:
    raw = str(email or "").strip()
    if not raw or "@" not in raw:
        return None
    user, domain = raw.split("@", 1)
    if len(user) <= 2:
        user_masked = user[0] + "*" * max(0, len(user) - 1)
    else:
        user_masked = user[:2] + "*" * (len(user) - 2)
    return f"{user_masked}@{domain}"


def _find_google_login_page(context, page, timeout_sec: float):
    deadline = time.time() + max(1.0, float(timeout_sec))
    while time.time() < deadline:
        pages = [page] + list(context.pages or [])
        for pg in pages:
            try:
                url = str(pg.url or "")
            except Exception:
                continue
            if "accounts.google.com" in url:
                return pg
        try:
            new_page = context.wait_for_event("page", timeout=500)
            try:
                new_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            try:
                if "accounts.google.com" in str(new_page.url or ""):
                    return new_page
            except Exception:
                pass
        except Exception:
            pass
        page.wait_for_timeout(200)
    return None


def _auto_fill_google_email(context, page, email: str, timeout_sec: float = 20.0) -> Dict[str, Any]:
    email_text = str(email or "").strip()
    if not email_text:
        return {"attempted": False, "filled": False, "reason": "email_missing"}

    gpage = _find_google_login_page(context, page, timeout_sec=timeout_sec)
    if not gpage:
        return {"attempted": True, "filled": False, "reason": "google_page_not_found", "email_masked": _mask_email(email_text)}

    try:
        gpage.bring_to_front()
    except Exception:
        pass

    selectors = [
        "input[type='email']",
        "input[name='identifier']",
        "input[autocomplete='username']",
        "#identifierId",
    ]
    filled = False
    used_selector = None
    for sel in selectors:
        try:
            loc = gpage.locator(sel)
            if loc.count() < 1:
                continue
            loc.first.click(timeout=4000, force=True)
            loc.first.fill(email_text, timeout=5000)
            used_selector = sel
            filled = True
            break
        except Exception:
            continue

    if not filled:
        return {"attempted": True, "filled": False, "reason": "email_input_not_found", "email_masked": _mask_email(email_text)}

    next_clicked = False
    next_via = None
    try:
        gpage.get_by_role("button", name=re.compile(r"^\s*next\s*$", re.I)).first.click(timeout=5000, force=True)
        next_clicked = True
        next_via = "role:button"
    except Exception:
        try:
            gpage.locator("#identifierNext button, div#identifierNext").first.click(timeout=5000, force=True)
            next_clicked = True
            next_via = "css:#identifierNext"
        except Exception:
            next_clicked = False

    return {
        "attempted": True,
        "filled": True,
        "email_masked": _mask_email(email_text),
        "selector": used_selector,
        "next_clicked": next_clicked,
        "next_via": next_via,
    }


def _auto_click_google_account_chooser(context, page, email: str, timeout_sec: float = 20.0) -> Dict[str, Any]:
    email_text = str(email or "").strip().lower()
    if not email_text:
        return {"attempted": False, "clicked": False, "reason": "email_missing"}

    gpage = _find_google_login_page(context, page, timeout_sec=timeout_sec)
    if not gpage:
        return {"attempted": True, "clicked": False, "reason": "google_page_not_found", "email_masked": _mask_email(email_text)}

    try:
        gpage.bring_to_front()
    except Exception:
        pass

    # Common account chooser rows expose email text directly.
    try:
        loc = gpage.get_by_text(re.compile(re.escape(email_text), re.I))
        if loc.count() > 0:
            loc.first.click(timeout=5000, force=True)
            return {"attempted": True, "clicked": True, "via": "text:email", "email_masked": _mask_email(email_text)}
    except Exception:
        pass

    # Known Google account chooser attributes/selectors.
    selectors = [
        f"[data-email='{email_text}']",
        f"[data-identifier='{email_text}']",
        f"[aria-label*='{email_text}']",
    ]
    for sel in selectors:
        try:
            loc = gpage.locator(sel)
            if loc.count() < 1:
                continue
            loc.first.click(timeout=5000, force=True)
            return {"attempted": True, "clicked": True, "via": f"css:{sel}", "email_masked": _mask_email(email_text)}
        except Exception:
            continue

    # DOM fallback: scan visible nodes and click nearest clickable ancestor.
    try:
        result = gpage.evaluate(
            """(email) => {
                const norm = (s) => String(s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                const target = norm(email);
                const nodes = Array.from(document.querySelectorAll("div,li,a,button,span,p"));
                for (const n of nodes) {
                    const txt = norm(n.innerText || n.textContent || "");
                    if (!txt || !txt.includes(target)) continue;
                    let el = n;
                    let hops = 0;
                    while (el && hops < 6) {
                        const tag = (el.tagName || "").toLowerCase();
                        const role = norm(el.getAttribute && el.getAttribute("role"));
                        const onclick = !!el.onclick;
                        const tabbable = (el.tabIndex || -1) >= 0;
                        if (onclick || tag === "a" || tag === "button" || role === "button" || role === "link" || tabbable) {
                            el.click();
                            return { clicked: true, via: "dom_scan", text: txt.slice(0, 120) };
                        }
                        el = el.parentElement;
                        hops += 1;
                    }
                }
                return { clicked: false, via: "dom_scan", reason: "row_not_found" };
            }""",
            email_text,
        )
        if isinstance(result, dict) and bool(result.get("clicked")):
            return {
                "attempted": True,
                "clicked": True,
                "via": result.get("via"),
                "email_masked": _mask_email(email_text),
                "text": result.get("text"),
            }
    except Exception:
        pass

    return {"attempted": True, "clicked": False, "reason": "row_not_found", "email_masked": _mask_email(email_text)}


def _auto_fill_google_password(context, page, password: str, timeout_sec: float = 30.0) -> Dict[str, Any]:
    password_text = str(password or "")
    if not password_text:
        return {"attempted": False, "filled": False, "reason": "password_missing"}

    deadline = time.time() + max(1.0, float(timeout_sec))
    selectors = [
        "input[type='password']",
        "input[name='Passwd']",
        "input[autocomplete='current-password']",
    ]
    while time.time() < deadline:
        pages = [page] + list(context.pages or [])
        for pg in pages:
            try:
                url = str(pg.url or "")
            except Exception:
                continue
            if "accounts.google.com" not in url:
                continue

            try:
                pg.bring_to_front()
            except Exception:
                pass

            for sel in selectors:
                try:
                    loc = pg.locator(sel)
                    if loc.count() < 1:
                        continue
                    loc.first.click(timeout=2000, force=True)
                    loc.first.fill(password_text, timeout=5000)
                except Exception:
                    continue

                next_clicked = False
                next_via = None
                try:
                    pg.get_by_role("button", name=re.compile(r"^\s*next\s*$", re.I)).first.click(timeout=5000, force=True)
                    next_clicked = True
                    next_via = "role:button"
                except Exception:
                    try:
                        pg.locator("#passwordNext button, div#passwordNext").first.click(timeout=5000, force=True)
                        next_clicked = True
                        next_via = "css:#passwordNext"
                    except Exception:
                        next_clicked = False

                return {
                    "attempted": True,
                    "filled": True,
                    "selector": sel,
                    "next_clicked": next_clicked,
                    "next_via": next_via,
                }

        page.wait_for_timeout(250)

    return {"attempted": True, "filled": False, "reason": "password_input_not_found"}


def _frame_click_ok_like(frame) -> Dict[str, Any]:
    try:
        result = frame.evaluate(
            """() => {
                const norm = (s) => String(s || "").replace(/\\s+/g, " ").trim().toLowerCase();
                const isVisible = (el) => {
                    if (!el || !el.getBoundingClientRect) return false;
                    const r = el.getBoundingClientRect();
                    if (r.width < 24 || r.height < 16) return false;
                    const cs = window.getComputedStyle(el);
                    if (!cs) return false;
                    return cs.display !== "none" && cs.visibility !== "hidden" && Number(cs.opacity || "1") > 0.02;
                };
                const clickable = (el) => {
                    const tag = String(el.tagName || "").toLowerCase();
                    const role = norm(el.getAttribute && el.getAttribute("role"));
                    return tag === "button" || tag === "a" || role === "button" || role === "link" || !!el.onclick || (el.tabIndex || -1) >= 0;
                };
                const buttons = Array.from(document.querySelectorAll("button,a,[role='button'],[role='link'],div,span"));
                const candidates = [];
                for (const el of buttons) {
                    if (!isVisible(el)) continue;
                    const txt = norm(el.innerText || el.textContent || el.getAttribute("aria-label"));
                    if (!txt) continue;
                    if (!(txt === "ok" || txt === "okay" || txt === "close" || txt === "confirm")) continue;
                    let target = el;
                    let hops = 0;
                    while (target && hops < 6 && !clickable(target)) {
                        target = target.parentElement;
                        hops += 1;
                    }
                    if (!target || !isVisible(target)) continue;
                    const z = Number(window.getComputedStyle(target).zIndex || "0") || 0;
                    candidates.push({ target, txt, z });
                }
                if (!candidates.length) return { clicked: false };
                candidates.sort((a, b) => b.z - a.z);
                const picked = candidates[0];
                picked.target.click();
                return { clicked: true, via: "dom_scan", text: picked.txt, z: picked.z };
            }"""
        )
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    return {"clicked": False}


def _visible_modal_like_count(page) -> int:
    try:
        counts = []
        for fr in list(page.frames):
            try:
                c = fr.evaluate(
                    """() => {
                        const sels = [
                          ".modal.show",
                          ".modal[style*='display: block']",
                          ".modal[aria-modal='true']",
                          ".swal2-container.swal2-shown",
                          ".swal2-popup",
                          "[role='dialog']",
                          "[aria-modal='true']",
                          ".ReactModal__Overlay",
                          ".MuiDialog-root"
                        ];
                        const isVisible = (el) => {
                            if (!el || !el.getBoundingClientRect) return false;
                            const r = el.getBoundingClientRect();
                            if (r.width < 30 || r.height < 30) return false;
                            const cs = window.getComputedStyle(el);
                            if (!cs) return false;
                            return cs.display !== "none" && cs.visibility !== "hidden" && Number(cs.opacity || "1") > 0.02;
                        };
                        let n = 0;
                        for (const sel of sels) {
                          for (const el of document.querySelectorAll(sel)) {
                            if (isVisible(el)) n += 1;
                          }
                        }
                        return n;
                    }"""
                )
                counts.append(int(c or 0))
            except Exception:
                continue
        return int(sum(counts))
    except Exception:
        return 0


def _try_dismiss_amybd_popup_once(page) -> Dict[str, Any]:
    patterns = [
        re.compile(r"^\s*ok\s*$", re.I),
        re.compile(r"^\s*okay\s*$", re.I),
        re.compile(r"^\s*close\s*$", re.I),
        re.compile(r"^\s*confirm\s*$", re.I),
    ]
    for pat in patterns:
        for label, getter in [
            ("role:button", lambda: page.get_by_role("button", name=pat)),
            ("role:link", lambda: page.get_by_role("link", name=pat)),
            ("text:locator", lambda: page.locator(f"text=/{pat.pattern}/i")),
        ]:
            try:
                loc = getter()
                if loc.count() < 1:
                    continue
                loc.last.click(timeout=1200, force=True)
                return {"clicked": True, "via": label, "pattern": pat.pattern}
            except Exception:
                continue

    # Common close icons/buttons on modal frameworks.
    for css in [
        ".modal.show [aria-label='Close']",
        ".modal.show .close",
        ".modal.show button.btn-primary",
        ".modal.show button.btn",
        ".swal2-popup .swal2-confirm",
        ".swal2-close",
    ]:
        try:
            loc = page.locator(css)
            if loc.count() < 1:
                continue
            loc.last.click(timeout=1200, force=True)
            return {"clicked": True, "via": f"css:{css}"}
        except Exception:
            continue

    # DOM/frame fallback for custom popups and nested iframes.
    for fr in list(page.frames):
        hit = _frame_click_ok_like(fr)
        if bool(hit.get("clicked")):
            return {"clicked": True, "via": hit.get("via"), "text": hit.get("text"), "z": hit.get("z")}
    return {"clicked": False}


def _auto_dismiss_amybd_popup(page, timeout_sec: float = 10.0) -> Dict[str, Any]:
    deadline = time.time() + max(1.0, float(timeout_sec))
    clicked_count = 0
    last_hit: Dict[str, Any] = {}
    last_click_at = 0.0

    while time.time() < deadline:
        hit = _try_dismiss_amybd_popup_once(page)
        if bool(hit.get("clicked")):
            clicked_count += 1
            last_hit = hit
            last_click_at = time.time()
            page.wait_for_timeout(300)
            continue

        # If we already clicked one or more popups and nothing new appears for a short settle window, stop.
        if clicked_count > 0 and (time.time() - last_click_at) >= 1.2:
            break
        page.wait_for_timeout(200)

    if clicked_count > 0:
        return {
            "attempted": True,
            "clicked": True,
            "clicked_count": clicked_count,
            "via": last_hit.get("via"),
            "pattern": last_hit.get("pattern"),
            "text": last_hit.get("text"),
        }
    return {"attempted": True, "clicked": False, "reason": "ok_popup_not_found", "clicked_count": 0}


def _wait_for_popups_to_clear(page, timeout_sec: float = 35.0) -> Dict[str, Any]:
    deadline = time.time() + max(2.0, float(timeout_sec))
    total_clicks = 0
    while time.time() < deadline:
        hit = _try_dismiss_amybd_popup_once(page)
        if bool(hit.get("clicked")):
            total_clicks += 1
            page.wait_for_timeout(250)
            continue
        active = _visible_modal_like_count(page)
        if active <= 0:
            return {"cleared": True, "active_modals": 0, "clicks": total_clicks}
        page.wait_for_timeout(250)
    return {"cleared": False, "active_modals": _visible_modal_like_count(page), "clicks": total_clicks}


def _build_env_script(
    *,
    cookies_path: Path,
    token: Optional[str],
    authid: Optional[str],
    chauth: Optional[str],
    origin: Optional[str],
    referer: Optional[str],
) -> str:
    lines = [
        "$env:BS_SOURCE_MODE=\"amybd\"",
        "$env:AIRASTRA_SOURCE_MODE=\"amybd\"",
        "$env:AMYBD_DISABLE_DEFAULT_TOKEN=\"1\"",
        f"$env:AMYBD_COOKIES_PATH=\"{str(cookies_path).replace('\\', '/')}\"",
    ]
    if token:
        lines.append(f"$env:AMYBD_TOKEN=\"{token}\"")
    if authid:
        lines.append(f"$env:AMYBD_AUTHID=\"{authid}\"")
    if chauth:
        lines.append(f"$env:AMYBD_CAUTH=\"{chauth}\"")
    if origin:
        lines.append(f"$env:AMYBD_ORIGIN=\"{origin}\"")
    if referer:
        lines.append(f"$env:AMYBD_REFERER=\"{referer}\"")
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(
            "Playwright is required. Install with:\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium\n"
            f"Import error: {exc}"
        )

    out_path = Path(args.out)
    cookies_out_path = Path(args.cookies_out)
    headers_out_path = Path(args.headers_out)
    env_out_path = Path(args.env_out)

    holder: Dict[str, Any] = {
        "best": None,   # best successful search capture
        "latest": None, # latest search capture (fallback)
    }
    req_meta: Dict[int, Dict[str, Any]] = {}

    def _on_request(req):
        try:
            url = str(req.url or "")
            method = str(req.method or "").upper()
            headers = _clean_headers(req.headers or {})
            payload = _parse_payload(req.post_data)
        except Exception:
            return
        if not _is_search_candidate(url, method, payload):
            return
        req_meta[id(req)] = {
            "captured_at_utc": _now_utc_iso(),
            "request_url": url,
            "request_method": method,
            "request_headers": headers,
            "payload": payload,
        }

    def _on_response(resp):
        try:
            req = resp.request
            info = req_meta.get(id(req))
            if not info:
                return
            status = int(resp.status)
            body_text = resp.text()
            try:
                body = json.loads(body_text)
            except Exception:
                body = body_text
        except Exception:
            return

        payload = info.get("payload") or {}
        cmnd = str(payload.get("CMND") or "").upper().strip()
        token = str(payload.get("TOKEN") or "").strip() or None
        headers = info.get("request_headers") or {}
        authid = str(headers.get("authid") or headers.get("Authid") or "").strip() or None
        chauth = str(headers.get("chauth") or headers.get("Chauth") or "").strip() or None
        cookie_header = str(headers.get("cookie") or headers.get("Cookie") or "").strip()
        search_success = bool(body.get("success")) if isinstance(body, dict) else None
        search_message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""

        captured = {
            **info,
            "search_status": status,
            "search_success": search_success,
            "search_message": search_message,
            "search_command": cmnd,
            "token": token,
            "authid": authid,
            "chauth": chauth,
            "cookie_header_present": bool(cookie_header),
            "response_preview": (body_text or "")[:300],
            "response_json": body if isinstance(body, dict) else None,
        }
        holder["latest"] = captured
        if search_success is True:
            holder["best"] = captured

    with sync_playwright() as p:
        launch_kwargs: Dict[str, Any] = {"headless": bool(args.headless)}
        if args.proxy_server:
            launch_kwargs["proxy"] = {"server": args.proxy_server}
        browser_args = [
            "--hide-crash-restore-bubble",
            "--disable-session-crashed-bubble",
        ]
        if not args.disable_stealth_arg:
            browser_args.append("--disable-blink-features=AutomationControlled")
        launch_kwargs["args"] = browser_args
        if str(args.browser_channel).strip().lower() in {"chrome", "msedge"}:
            launch_kwargs["channel"] = str(args.browser_channel).strip().lower()

        browser = None
        context = None
        try:
            if str(args.user_data_dir or "").strip():
                user_data_dir = str(Path(args.user_data_dir).resolve())
                context = p.chromium.launch_persistent_context(user_data_dir=user_data_dir, **launch_kwargs)
            else:
                browser = p.chromium.launch(**launch_kwargs)
                context = browser.new_context()
        except Exception as exc:
            msg = str(exc)
            if "Executable doesn't exist" in msg:
                raise SystemExit(
                    "Playwright browser missing. Run:\n"
                    "  python -m playwright install chromium\n"
                    f"Launch error: {msg}"
                )
            if "chromium distribution 'chrome'" in msg.lower() or "chrome" in msg.lower() and "not found" in msg.lower():
                raise SystemExit(
                    "Chrome channel launch failed. Install Google Chrome and re-run with:\n"
                    "  --browser-channel chrome --user-data-dir output/manual_sessions/chrome_amybd_profile\n"
                    f"Launch error: {msg}"
                )
            raise SystemExit(f"Failed to launch browser: {msg}")

        context.on("request", _on_request)
        context.on("response", _on_response)
        page = context.new_page()
        page.set_default_timeout(args.timeout_ms)
        page.goto(args.url, wait_until="domcontentloaded")

        auto_login_click: Optional[Dict[str, Any]] = None
        auto_google_click: Optional[Dict[str, Any]] = None
        auto_google_account_click: Optional[Dict[str, Any]] = None
        auto_google_email_fill: Optional[Dict[str, Any]] = None
        auto_google_password_fill: Optional[Dict[str, Any]] = None
        auto_amybd_popup_dismiss: Optional[Dict[str, Any]] = None
        popup_clear_status: Optional[Dict[str, Any]] = None
        auto_ui_search_result: Optional[Dict[str, Any]] = None
        if not args.disable_auto_click_login:
            page.wait_for_timeout(1200)
            auto_login_click = _auto_click_login_entry(page)
        if not args.disable_auto_click_google:
            page.wait_for_timeout(600)
            auto_google_click = _auto_click_continue_with_google(page)
        if not args.disable_auto_click_google_account:
            auto_google_account_click = _auto_click_google_account_chooser(
                context,
                page,
                args.google_email,
                timeout_sec=args.google_login_timeout_sec,
            )
        if not args.disable_auto_fill_google_email:
            auto_google_email_fill = _auto_fill_google_email(
                context,
                page,
                args.google_email,
                timeout_sec=args.google_login_timeout_sec,
            )
        if not args.disable_auto_fill_google_password:
            auto_google_password_fill = _auto_fill_google_password(
                context,
                page,
                args.google_password,
                timeout_sec=args.google_password_timeout_sec,
            )
        if not args.disable_auto_dismiss_amybd_popup:
            auto_amybd_popup_dismiss = _auto_dismiss_amybd_popup(page, timeout_sec=8.0)

        if not args.quiet:
            print(f"Opened: {args.url}")
            print(
                "Browser mode:",
                json.dumps(
                    {
                        "channel": str(args.browser_channel),
                        "persistent_profile": bool(str(args.user_data_dir or "").strip()),
                        "user_data_dir": str(Path(args.user_data_dir).resolve()) if str(args.user_data_dir or "").strip() else None,
                    }
                ),
            )
            if auto_login_click:
                print(
                    "Auto-click login result:",
                    json.dumps(
                        {
                            "clicked": bool(auto_login_click.get("clicked")),
                            "via": auto_login_click.get("via"),
                            "pattern": auto_login_click.get("pattern"),
                            "text": auto_login_click.get("text"),
                            "error": auto_login_click.get("error"),
                        }
                    ),
                )
            if auto_google_click:
                print(
                    "Auto-click Google result:",
                    json.dumps(
                        {
                            "clicked": bool(auto_google_click.get("clicked")),
                            "via": auto_google_click.get("via"),
                            "pattern": auto_google_click.get("pattern"),
                            "text": auto_google_click.get("text"),
                            "error": auto_google_click.get("error"),
                        }
                    ),
                )
            if auto_google_account_click:
                print(
                    "Auto-click Google account result:",
                    json.dumps(
                        {
                            "attempted": bool(auto_google_account_click.get("attempted")),
                            "clicked": bool(auto_google_account_click.get("clicked")),
                            "email_masked": auto_google_account_click.get("email_masked"),
                            "via": auto_google_account_click.get("via"),
                            "reason": auto_google_account_click.get("reason"),
                            "text": auto_google_account_click.get("text"),
                        }
                    ),
                )
            if auto_google_email_fill:
                print(
                    "Auto-fill Google email result:",
                    json.dumps(
                        {
                            "attempted": bool(auto_google_email_fill.get("attempted")),
                            "filled": bool(auto_google_email_fill.get("filled")),
                            "email_masked": auto_google_email_fill.get("email_masked"),
                            "selector": auto_google_email_fill.get("selector"),
                            "next_clicked": bool(auto_google_email_fill.get("next_clicked")),
                            "next_via": auto_google_email_fill.get("next_via"),
                            "reason": auto_google_email_fill.get("reason"),
                        }
                    ),
                )
            if auto_google_password_fill:
                print(
                    "Auto-fill Google password result:",
                    json.dumps(
                        {
                            "attempted": bool(auto_google_password_fill.get("attempted")),
                            "filled": bool(auto_google_password_fill.get("filled")),
                            "selector": auto_google_password_fill.get("selector"),
                            "next_clicked": bool(auto_google_password_fill.get("next_clicked")),
                            "next_via": auto_google_password_fill.get("next_via"),
                            "reason": auto_google_password_fill.get("reason"),
                        }
                    ),
                )
            if auto_amybd_popup_dismiss:
                print(
                    "Auto-dismiss AMYBD popup result:",
                    json.dumps(
                        {
                            "attempted": bool(auto_amybd_popup_dismiss.get("attempted")),
                            "clicked": bool(auto_amybd_popup_dismiss.get("clicked")),
                            "clicked_count": int(auto_amybd_popup_dismiss.get("clicked_count") or 0),
                            "via": auto_amybd_popup_dismiss.get("via"),
                            "pattern": auto_amybd_popup_dismiss.get("pattern"),
                            "reason": auto_amybd_popup_dismiss.get("reason"),
                            "text": auto_amybd_popup_dismiss.get("text"),
                        }
                    ),
                )
            if popup_clear_status:
                print(
                    "Popup clear status:",
                    json.dumps(
                        {
                            "cleared": bool(popup_clear_status.get("cleared")),
                            "active_modals": int(popup_clear_status.get("active_modals") or 0),
                            "clicks": int(popup_clear_status.get("clicks") or 0),
                        }
                    ),
                )
            print("")
            print("Manual steps:")
            print("1. Log in to AMYBD in the opened browser (2FA/captcha may still require manual action).")
            print("2. (Optional) Run one flight search and wait for results.")
            print("3. Return here after login/session is ready.")

        target_payload = _build_target_payload(args)
        targeted_fetch_result: Optional[Dict[str, Any]] = None
        if args.non_interactive:
            page.wait_for_timeout(int(max(1.0, float(args.wait_seconds)) * 1000))
        else:
            input("Press ENTER after login is complete (search optional)... ")
            if not args.disable_auto_dismiss_amybd_popup:
                auto_amybd_popup_dismiss = _auto_dismiss_amybd_popup(page, timeout_sec=18.0)
                popup_clear_status = _wait_for_popups_to_clear(page, timeout_sec=40.0)
                if not args.quiet:
                    print(
                        "Post-login popup clear:",
                        json.dumps(
                            {
                                "cleared": bool(popup_clear_status.get("cleared")) if popup_clear_status else None,
                                "active_modals": int(popup_clear_status.get("active_modals") or 0) if popup_clear_status else None,
                                "clicks": int(popup_clear_status.get("clicks") or 0) if popup_clear_status else None,
                            }
                        ),
                    )
            if not args.disable_auto_ui_search:
                auto_ui_search_result = _auto_submit_amybd_ui_search(page, target_payload)
                if not args.quiet:
                    print(
                        "Auto UI search result:",
                        json.dumps(
                            {
                                "fromFilled": bool(auto_ui_search_result.get("fromFilled")) if auto_ui_search_result else None,
                                "destFilled": bool(auto_ui_search_result.get("destFilled")) if auto_ui_search_result else None,
                                "dateFilled": bool(auto_ui_search_result.get("dateFilled")) if auto_ui_search_result else None,
                                "searchClicked": bool(auto_ui_search_result.get("searchClicked")) if auto_ui_search_result else None,
                                "searchText": auto_ui_search_result.get("searchText") if auto_ui_search_result else None,
                                "fromValue": auto_ui_search_result.get("fromValue") if auto_ui_search_result else None,
                                "destValue": auto_ui_search_result.get("destValue") if auto_ui_search_result else None,
                                "searchError": auto_ui_search_result.get("searchError") if auto_ui_search_result else None,
                            }
                        ),
                    )
                ui_deadline = time.time() + max(1.0, float(args.auto_ui_search_wait_sec))
                while time.time() < ui_deadline:
                    if holder.get("best") or holder.get("latest"):
                        break
                    if not args.disable_auto_dismiss_amybd_popup:
                        _try_dismiss_amybd_popup_once(page)
                    page.wait_for_timeout(250)
            if not args.disable_auto_trigger:
                targeted_fetch_result = _trigger_targeted_fetch(page, target_payload)
                if not args.quiet:
                    print(
                        "Auto-triggered AMYBD search:",
                        json.dumps(
                            {
                                "status": targeted_fetch_result.get("status"),
                                "ok": targeted_fetch_result.get("ok"),
                                "success": targeted_fetch_result.get("success"),
                                "message": targeted_fetch_result.get("message"),
                                "error": targeted_fetch_result.get("error"),
                            }
                        ),
                    )
            deadline = time.time() + max(1.0, float(args.wait_seconds))
            while time.time() < deadline:
                if not args.disable_auto_dismiss_amybd_popup:
                    _try_dismiss_amybd_popup_once(page)
                if holder.get("best"):
                    break
                if holder.get("latest"):
                    # Any captured search request is enough to write cookies/headers/env.
                    break
                page.wait_for_timeout(300)

        capture = holder.get("best") or holder.get("latest")
        if not capture:
            context.close()
            if browser is not None:
                browser.close()
            raise SystemExit(
                "Failed to capture AMYBD search request. "
                "Please run again, log in, and do one search in the opened browser."
            )

        request_headers = _clean_headers(capture.get("request_headers") or {})
        cookie_header = str(request_headers.get("cookie") or request_headers.get("Cookie") or "")
        cookies = _cookies_from_context(context.cookies())
        cookies_from_hdr = _cookies_from_header(cookie_header)
        if cookies_from_hdr:
            cookies.update(cookies_from_hdr)

        token = str(capture.get("token") or "").strip() or None
        authid = str(capture.get("authid") or "").strip() or None
        chauth = str(capture.get("chauth") or "").strip() or None
        origin = str(request_headers.get("origin") or request_headers.get("Origin") or "").strip() or None
        referer = str(request_headers.get("referer") or request_headers.get("Referer") or "").strip() or None

        session_summary = {
            "captured_at_utc": _now_utc_iso(),
            "source": "playwright_request_intercept",
            "request_url": capture.get("request_url"),
            "request_method": capture.get("request_method"),
            "search_status": capture.get("search_status"),
            "search_success": capture.get("search_success"),
            "search_message": capture.get("search_message"),
            "search_command": capture.get("search_command"),
            "token_present": bool(token),
            "token": token,
            "authid_present": bool(authid),
            "authid": authid,
            "chauth_present": bool(chauth),
            "chauth": chauth,
            "request_headers": request_headers,
            "request_payload": capture.get("payload"),
            "targeted_fetch_payload": target_payload,
            "targeted_fetch_result": targeted_fetch_result,
            "auto_login_click": auto_login_click,
            "auto_google_click": auto_google_click,
            "auto_google_account_click": auto_google_account_click,
            "auto_google_email_fill": auto_google_email_fill,
            "auto_google_password_fill": auto_google_password_fill,
            "auto_amybd_popup_dismiss": auto_amybd_popup_dismiss,
            "popup_clear_status": popup_clear_status,
            "auto_ui_search_result": auto_ui_search_result,
            "cookies_count": len(cookies),
            "cookies_out": str(cookies_out_path),
            "headers_out": str(headers_out_path),
            "env_out": str(env_out_path),
        }

        _save_json(out_path, session_summary)
        _save_json(cookies_out_path, cookies)
        _save_json(headers_out_path, request_headers)

        env_script = _build_env_script(
            cookies_path=cookies_out_path,
            token=token,
            authid=authid,
            chauth=chauth,
            origin=origin,
            referer=referer,
        )
        atomic_write_text(env_out_path, env_script)

        print(f"Captured AMYBD session -> {out_path}")
        print(f"cookies={len(cookies)} -> {cookies_out_path}")
        print(f"headers={len(request_headers)} -> {headers_out_path}")
        print(f"env snippet -> {env_out_path}")
        if capture.get("search_success") is not True:
            msg = str(capture.get("search_message") or "unknown")
            print(f"WARNING: captured search is not successful yet (message={msg}).")
            print("Login/session may still be invalid. Re-run after successful AMYBD login/search if module tests fail.")
        print("")
        print("Load env in current PowerShell session:")
        print(f"  . .\\{str(env_out_path).replace('/', '\\')}")

        if args.keep_open and not args.non_interactive:
            input("Press ENTER to close browser... ")
        context.close()
        if browser is not None:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
