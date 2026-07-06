"""Desktop bridge: everything the UI calls, with no UI imports at module level.

Local-first by design — run/export work fully offline; login and sync are layered,
best-effort steps (failed syncs land in the Outbox). The session token lives in the
OS keyring (Windows Credential Manager); a config-file fallback is used only when
no keyring backend exists, and the UI surfaces that as insecure.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from discount_engine import build_report
from discount_engine.grid import _parse_routes, auto_detect_hars, write_single_sheet_xlsx
from discount_engine.highlight import apply_highlights
from discount_engine.sanitize import sanitize_report_for_sync

from . import APP_ID, __version__
from .outbox import Outbox

DEFAULT_API_BASE = "https://aero-pulse-api-591603094460.asia-south1.run.app"
DEFAULT_WEB_BASE = "https://aviation-inventory-pricing-intellig.vercel.app"
KEYRING_SERVICE = "ota-discount-report"
KEYRING_ENTRY = "session-token"
RAM_HEADROOM_FACTOR = 2.5           # need ~2.5x the HAR size free to parse safely
REQUEST_TIMEOUT = 30

try:                                # optional: absent on exotic setups
    import keyring
    _HAS_KEYRING = True
except Exception:                   # noqa: BLE001
    keyring = None                  # type: ignore[assignment]
    _HAS_KEYRING = False

try:
    import psutil
    _HAS_PSUTIL = True
except Exception:                   # noqa: BLE001
    psutil = None                   # type: ignore[assignment]
    _HAS_PSUTIL = False


def config_dir() -> Path:
    base = os.environ.get("APPDATA") or str(Path.home() / ".config")
    return Path(base) / "OTADiscountReport"


def _sync_id_for(payload: dict[str, Any]) -> str:
    """Deterministic id for a report payload: same report (incl. generated_at) ->
    same id, so an outbox retry dedupes server-side and never double-bills a use.
    A genuinely new run has a fresh generated_at -> a new id."""
    import hashlib
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:32]


class DesktopApi:
    """Methods exposed to the webview UI via the pywebview JS bridge."""

    def __init__(self) -> None:
        self._config_path = config_dir() / "config.json"
        self._config = self._load_config()
        self._outbox = Outbox(config_dir() / "outbox")
        self._window = None          # attached by app.py
        self._report: Optional[dict[str, Any]] = None       # raw (for sync/export)
        self._prev_payload: Optional[dict[str, Any]] = None  # backend prev (red diff)
        self._busy = False
        self._status = ""
        # Every launch shows up on the /usage dashboard (no-op when signed out —
        # and signed-out users can't get past the sign-in wall anyway).
        self._log_usage("app_launch", target=f"v{__version__}")

    # ------------------------------------------------------------------ config
    def _load_config(self) -> dict[str, Any]:
        try:
            return json.loads(self._config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_config(self) -> None:
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        self._config_path.write_text(
            json.dumps(self._config, ensure_ascii=False, indent=2), encoding="utf-8")

    def attach_window(self, window: Any) -> None:
        self._window = window

    @property
    def api_base(self) -> str:
        return str(self._config.get("api_base") or DEFAULT_API_BASE).rstrip("/")

    # ------------------------------------------------------------------- token
    def _store_token(self, token: str) -> None:
        if _HAS_KEYRING:
            try:
                keyring.set_password(KEYRING_SERVICE, KEYRING_ENTRY, token)
                self._config.pop("token_fallback", None)
                self._save_config()
                return
            except Exception:       # noqa: BLE001 — fall through to file fallback
                pass
        self._config["token_fallback"] = token
        self._save_config()

    def _token(self) -> str:
        if _HAS_KEYRING:
            try:
                stored = keyring.get_password(KEYRING_SERVICE, KEYRING_ENTRY)
                if stored:
                    return stored
            except Exception:       # noqa: BLE001
                pass
        return str(self._config.get("token_fallback") or "")

    def _clear_token(self) -> None:
        if _HAS_KEYRING:
            try:
                keyring.delete_password(KEYRING_SERVICE, KEYRING_ENTRY)
            except Exception:       # noqa: BLE001
                pass
        self._config.pop("token_fallback", None)
        self._save_config()

    # ------------------------------------------------------------------- usage
    def _log_usage(self, action: str, count: int = 0, target: str | None = None) -> None:
        """Fire-and-forget usage ping to the /usage dashboard (never blocks or
        breaks the flow; silently skipped when signed out or offline)."""
        token = self._token()
        if not token:
            return
        import threading

        def _send() -> None:
            try:
                requests.post(
                    f"{self.api_base}/api/v1/lookups/log",
                    json={"app_id": APP_ID, "action": action,
                          "target": target, "count": int(count)},
                    headers={"X-User-Session": token}, timeout=10)
            except requests.RequestException:
                pass

        threading.Thread(target=_send, daemon=True).start()

    # ------------------------------------------------------------------- state
    def get_state(self) -> dict[str, Any]:
        return {
            "version": __version__,
            "api_base": self.api_base,
            "email": self._config.get("email") or "",
            "logged_in": bool(self._token()),
            "insecure_token_store": bool(self._config.get("token_fallback")),
            "har_dir": self._config.get("har_dir") or "",
            "routes": self._config.get("routes") or "DAC-CGP,DAC-DXB,DAC-SIN",
            "travel_date": self._config.get("travel_date") or "",
            "outbox_count": self._outbox.count(),
            "busy": self._busy,
            "status": self._status,
            "has_report": self._report is not None,
        }

    # Sentinel: set_config leaves a field untouched unless a value is passed. The
    # UI always sends the current textbox contents, so "" MEANS clear for routes /
    # travel_date (otherwise an expired date could never be removed and would keep
    # hard-failing run()).
    def set_config(self, api_base: str | None = None, routes: str | None = None,
                   travel_date: str | None = None, har_dir: str | None = None) -> dict[str, Any]:
        if api_base:
            self._config["api_base"] = api_base.strip().rstrip("/")
        if routes is not None:
            self._config["routes"] = routes.strip()
        if travel_date is not None:
            self._config["travel_date"] = travel_date.strip()
        if har_dir:
            self._config["har_dir"] = har_dir.strip()
        self._save_config()
        return self.get_state()

    # -------------------------------------------------------------------- auth
    def login(self, email: str, password: str) -> dict[str, Any]:
        try:
            response = requests.post(
                f"{self.api_base}/api/v1/user-auth/login",
                json={"email": email, "password": password},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            return {"ok": False, "error": f"API unreachable: {exc}"}
        if response.status_code != 200:
            detail = response.json().get("detail", response.reason) \
                if response.headers.get("content-type", "").startswith("application/json") \
                else response.reason
            return {"ok": False, "error": str(detail)}
        token = response.json().get("session_token") or ""
        if not token:
            return {"ok": False, "error": "Login succeeded but no session token returned."}
        self._store_token(token)
        self._config["email"] = email.strip().lower()
        self._save_config()
        self._log_usage("app_login", target=f"v{__version__}")
        flushed = self.flush_outbox()
        return {"ok": True, "email": self._config["email"],
                "outbox_flushed": flushed.get("sent", 0)}

    def open_account_page(self) -> dict[str, Any]:
        """Open the website's /account page — where Google-sign-in users create the
        password the desktop app signs in with."""
        import webbrowser
        web_base = str(self._config.get("web_base") or DEFAULT_WEB_BASE).rstrip("/")
        webbrowser.open(f"{web_base}/account")
        return {"ok": True}

    def change_password(self, new_password: str) -> dict[str, Any]:
        """Change the signed-in user's password (min 8 chars)."""
        token = self._token()
        if not token:
            return {"ok": False, "error": "Sign in first."}
        if len(new_password or "") < 8:
            return {"ok": False, "error": "Password must be at least 8 characters."}
        try:
            response = requests.post(
                f"{self.api_base}/api/v1/user-auth/set-password",
                json={"password": new_password},
                headers={"X-User-Session": token}, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            return {"ok": False, "error": f"API unreachable: {exc}"}
        if response.status_code == 401:
            return {"ok": False, "error": "Session expired — sign in again.",
                    "needs_login": True}
        if response.status_code != 200:
            try:
                return {"ok": False, "error": str(response.json().get("detail", response.reason))}
            except ValueError:
                return {"ok": False, "error": response.reason or str(response.status_code)}
        return {"ok": True}

    def check_update(self) -> dict[str, Any]:
        """Compare the running version with the release channel. /app/latest is
        PUBLIC — updates must be discoverable even when signed out (a broken old
        version could otherwise never learn about its fix)."""
        try:
            token = self._token()
            response = requests.get(
                f"{self.api_base}/api/v1/app/latest?app=discount-report",
                headers={"X-User-Session": token} if token else {}, timeout=15)
            if response.status_code != 200:
                return {"update_available": False,
                        "error": f"Update check failed ({response.status_code})."}
            latest = response.json()
        except (requests.RequestException, ValueError) as exc:
            return {"update_available": False,
                    "error": f"Update check failed: offline? ({exc})"}

        def _ver(v: str) -> tuple[int, ...]:
            # Per-segment parse so a stray prefix/suffix can't collapse the whole
            # version to (0,) and hide every update (the original .split() bug).
            parts = re.findall(r"\d+", str(v))
            return tuple(int(x) for x in parts) or (0,)

        newer = _ver(latest.get("version", "0")) > _ver(__version__)
        return {"update_available": newer, "version": latest.get("version"),
                "download_url": latest.get("download_url"),
                "notes": (latest.get("notes") or "")[:400]}

    def open_download(self, url: str) -> dict[str, Any]:
        """Open the (public) mirror download in the browser. Only our own hosts."""
        allowed = (self.api_base,
                   str(self._config.get("web_base") or DEFAULT_WEB_BASE).rstrip("/"))
        if not str(url).startswith(allowed):
            return {"ok": False, "error": "Refusing to open a non-app URL."}
        import webbrowser
        webbrowser.open(url)
        return {"ok": True}

    def check_access(self) -> dict[str, Any]:
        """The signed-in user's discount-report access status from the server.
        Returns {status, allowed, detail} or {status:'unknown'} when offline.
        A successful 'allowed' answer is cached (timestamp) so a signed-in,
        already-approved user keeps working through a short offline stretch."""
        token = self._token()
        if not token:
            return {"status": "signed_out", "allowed": False,
                    "detail": "Sign in to use the app."}
        try:
            r = requests.get(f"{self.api_base}/api/v1/discount-reports/access",
                             headers={"X-User-Session": token}, timeout=15)
            if r.status_code == 200:
                result = r.json()
                if result.get("allowed"):
                    self._config["last_access_ok_utc"] = datetime.now(timezone.utc).isoformat()
                    self._save_config()
                elif result.get("status") in ("rejected", "payment_required",
                                              "expired", "not_started", "none"):
                    self._config.pop("last_access_ok_utc", None)   # revoke offline grace
                    self._save_config()
                return result
            if r.status_code == 401:
                return {"status": "signed_out", "allowed": False,
                        "detail": "Session expired — sign in again."}
        except requests.RequestException:
            pass
        return {"status": "unknown", "allowed": False, "detail": ""}

    #: Signed-in users whose access was server-verified within this window keep
    #: working offline; beyond it the app requires a fresh online check.
    OFFLINE_GRACE_HOURS = 72

    def _require_access(self) -> Optional[dict[str, Any]]:
        """The sign-in wall: None when the user may use the app, else the error
        payload to return. No session -> blocked. Explicit server denial ->
        blocked (and the offline-grace cache is cleared). Offline -> allowed only
        within OFFLINE_GRACE_HOURS of the last verified approval."""
        if not self._token():
            return {"ok": False, "auth_required": True,
                    "error": "Sign in to use the app."}
        access = self.check_access()
        if access.get("allowed"):
            return None
        status = access.get("status", "unknown")
        if status == "signed_out":
            return {"ok": False, "auth_required": True,
                    "error": access.get("detail") or "Session expired — sign in again."}
        if status == "unknown":     # offline: honor the grace window
            last = self._config.get("last_access_ok_utc") or ""
            try:
                verified = datetime.fromisoformat(last)
                age_h = (datetime.now(timezone.utc) - verified).total_seconds() / 3600
                if age_h <= self.OFFLINE_GRACE_HOURS:
                    return None
            except ValueError:
                pass
            return {"ok": False, "auth_required": True,
                    "error": "Can't verify your access while offline — connect to "
                             "the internet once and try again."}
        if status == "pending":
            # A logged-in user awaiting approval may run locally (sync stays
            # gated server-side) — they are identified and tracked.
            return None
        return {"ok": False, "access_blocked": True,
                "error": access.get("detail")
                or "Your access to the discount report is not active."}

    def open_guide(self) -> dict[str, Any]:
        """Open the visual HAR-collection guide (flow diagrams + per-site steps) in
        the browser — same guide the web app serves."""
        import webbrowser
        web_base = str(self._config.get("web_base") or DEFAULT_WEB_BASE).rstrip("/")
        webbrowser.open(f"{web_base}/discount-comparison/guide")
        return {"ok": True}

    def run_diagnostics(self) -> dict[str, Any]:
        """Self-test: is everything the app needs working? Returns an ordered list
        of checks [{name, ok, detail}] so the user can see at a glance what's set up
        and what isn't — before a run/sync, or when something misbehaves."""
        checks: list[dict[str, Any]] = []

        def add(name: str, ok: bool, detail: str = "") -> None:
            checks.append({"name": name, "ok": ok, "detail": detail})

        # 1. Engine present (bundled) — importable means the exe is intact.
        try:
            import discount_engine  # noqa: F401
            add("Report engine", True, f"v{discount_engine.__version__} bundled")
        except Exception as exc:  # noqa: BLE001
            add("Report engine", False, f"engine import failed: {exc}")

        # 2. Secure token store.
        add("Secure sign-in store", not self._config.get("token_fallback"),
            "OS keyring" if _HAS_KEYRING and not self._config.get("token_fallback")
            else "config-file fallback (no keyring)")

        # 3. Backend reachable (public health endpoint — no auth needed).
        api_ok = False
        try:
            r = requests.get(f"{self.api_base}/health", timeout=15)
            api_ok = r.status_code == 200
            add("Backend reachable", api_ok,
                self.api_base if api_ok else f"HTTP {r.status_code} from {self.api_base}")
        except requests.RequestException as exc:
            add("Backend reachable", False, f"offline? {exc} — local runs still work")

        # 4. Signed in + session valid + access.
        token = self._token()
        if not token:
            add("Signed in", False, "not signed in — sign-in is required to use the app")
        else:
            access = self.check_access()
            status = access.get("status", "unknown")
            add("Signed in", status not in ("signed_out",),
                self._config.get("email") or "session present")
            add("Discount-report access", bool(access.get("allowed")),
                access.get("detail") or f"status: {status}")
            plan = access.get("plan") or {}
            if plan:
                left = plan.get("uses_remaining")
                add("Plan status", True,
                    f"valid to {plan.get('end_date') or 'no expiry'} · "
                    + (f"{left} syncs left" if left is not None else "unlimited syncs"))

        # 5. HAR folder + captures.
        har_dir = self._config.get("har_dir") or ""
        if not har_dir or not Path(har_dir).is_dir():
            add("HAR folder", False, "pick the folder with today's .har files")
        else:
            try:
                n = len(list(Path(har_dir).glob("*.har")))
            except OSError:
                n = 0
            add("HAR folder", n > 0, f"{n} .har file(s) in {har_dir}")

        # 6. Update check.
        upd = self.check_update()
        if upd.get("update_available"):
            add("App version", False, f"update v{upd.get('version')} available")
        else:
            add("App version", True, f"v{__version__} (latest)")

        ok_count = sum(1 for c in checks if c["ok"])
        return {"checks": checks, "ok_count": ok_count, "total": len(checks),
                "all_ok": ok_count == len(checks)}

    def logout(self) -> dict[str, Any]:
        token = self._token()
        if token:
            try:        # best-effort server-side revoke
                requests.post(f"{self.api_base}/api/v1/user-auth/logout",
                              headers={"X-User-Session": token}, timeout=10)
            except requests.RequestException:
                pass
        self._clear_token()
        self._config.pop("last_access_ok_utc", None)   # signing out ends offline grace
        self._save_config()
        return self.get_state()

    # -------------------------------------------------------------------- scan
    def pick_folder(self) -> dict[str, Any]:
        import webview  # local import: only available inside the app shell
        result = self._window.create_file_dialog(webview.FOLDER_DIALOG) if self._window else None
        if result:
            self._config["har_dir"] = result[0]
            self._save_config()
        return self.scan()

    def scan(self) -> dict[str, Any]:
        """Detect HARs in the folder + per-file RAM-safety verdicts (review P0)."""
        har_dir = self._config.get("har_dir") or ""
        if not har_dir or not Path(har_dir).is_dir():
            return {"ok": False, "error": "Pick the HAR capture folder first.", "files": []}
        available = psutil.virtual_memory().available if _HAS_PSUTIL else None
        files: list[dict[str, Any]] = []
        detected = auto_detect_hars(Path(har_dir))
        recognized = {p for paths in detected.values() for p in paths}
        for channel, paths in detected.items():
            for p in paths:
                size = Path(p).stat().st_size
                needed = int(size * RAM_HEADROOM_FACTOR)
                files.append({
                    "file": Path(p).name,
                    "path": p,
                    "channel": channel,
                    "size_mb": round(size / 1e6, 1),
                    "ram_ok": (available is None) or (available > needed),
                })
        # Unknown sites: show them so an ignored capture is VISIBLE, not silent.
        for p in sorted(Path(har_dir).glob("*.har")):
            if str(p) not in recognized:
                files.append({
                    "file": p.name, "path": str(p),
                    "channel": "unrecognized (site not supported yet)",
                    "size_mb": round(p.stat().st_size / 1e6, 1),
                    "ram_ok": False,
                })
        files.sort(key=lambda f: f["size_mb"])   # smallest-first: cheap wins land first
        return {"ok": True, "files": files,
                "available_ram_mb": round(available / 1e6) if available else None,
                "ram_gate_active": _HAS_PSUTIL}

    def archive_hars(self) -> dict[str, Any]:
        """Move every .har in the capture folder into archive/YYYY-MM-DD/ so the
        next day starts clean. Never deletes; same-name collisions get a suffix."""
        blocked = self._require_access()
        if blocked:
            return blocked
        har_dir = self._config.get("har_dir") or ""
        if not har_dir or not Path(har_dir).is_dir():
            return {"ok": False, "error": "Pick the HAR capture folder first."}
        hars = sorted(Path(har_dir).glob("*.har"))
        if not hars:
            return {"ok": True, "moved": 0, "dest": ""}
        dest = Path(har_dir) / "archive" / date.today().isoformat()
        dest.mkdir(parents=True, exist_ok=True)
        moved = 0
        for h in hars:
            target = dest / h.name
            n = 1
            while target.exists():
                target = dest / f"{h.stem}_{n}{h.suffix}"
                n += 1
            try:
                h.rename(target)
                moved += 1
            except OSError as exc:
                return {"ok": False, "moved": moved,
                        "error": f"Could not move {h.name}: {exc} (file open?)"}
        return {"ok": True, "moved": moved, "dest": str(dest)}

    # --------------------------------------------------------------------- run
    def _fetch_previous_payload(self, before: date) -> Optional[dict[str, Any]]:
        """Backend-stored report strictly before `before` — the red-diff source.
        Offline/unauthed is fine: the grid renders without 'changed' flags."""
        token = self._token()
        if not token:
            return None
        try:
            history = requests.get(
                f"{self.api_base}/api/v1/discount-reports/history?limit=30",
                headers={"X-User-Session": token}, timeout=REQUEST_TIMEOUT)
            if history.status_code != 200:
                return None
            prev_date = next(
                (i["report_date"] for i in history.json().get("items", [])
                 if i.get("report_date") and date.fromisoformat(i["report_date"]) < before),
                None)
            if not prev_date:
                return None
            stored = requests.get(
                f"{self.api_base}/api/v1/discount-reports/by-date?date={prev_date}",
                headers={"X-User-Session": token}, timeout=REQUEST_TIMEOUT)
            if stored.status_code != 200:
                return None
            return stored.json().get("report")
        except (requests.RequestException, ValueError):
            return None

    def run(self, skip_paths: Optional[list[str]] = None) -> dict[str, Any]:
        if self._busy:
            return {"ok": False, "error": "A run is already in progress."}
        state = self.get_state()
        har_dir = state["har_dir"]
        if not har_dir or not Path(har_dir).is_dir():
            return {"ok": False, "error": "Pick the HAR capture folder first."}
        travel_date = (state["travel_date"] or "").strip()
        if travel_date:
            try:
                parsed = datetime.strptime(travel_date, "%Y-%m-%d").date()
            except ValueError:
                return {"ok": False,
                        "error": f"Travel date must be YYYY-MM-DD (got {travel_date!r})."}
            if parsed <= date.today():
                # A stale date silently skips the live fetch rather than hard-failing
                # the whole run; clear it and warn.
                travel_date = ""
                self._config["travel_date"] = ""
                self._save_config()
        try:
            routes = _parse_routes(state["routes"])
        except SystemExit as exc:
            return {"ok": False, "error": str(exc)}

        # Sign-in wall: every run requires an identified user (usage-tracked);
        # explicit denials block, short offline stretches are covered by the
        # grace window in _require_access().
        blocked = self._require_access()
        if blocked:
            return blocked

        self._busy, self._status = True, "Parsing HAR captures…"
        # The engine reports per-channel problems via print(); a windowed exe has no
        # console, so capture the output and hand the log to the UI — a dead live
        # fetch must be VISIBLE, not silent.
        log_buffer = io.StringIO()
        try:
            skips = set(skip_paths or [])
            detected = auto_detect_hars(Path(har_dir))
            hars = {ch: [p for p in paths if p not in skips]
                    for ch, paths in detected.items()}
            with contextlib.redirect_stdout(log_buffer):
                report = build_report(
                    travel_date or None, routes,
                    gozayaan_hars=hars.get("gozayaan"), amy_hars=hars.get("amy"),
                    firsttrip_b2b_hars=hars.get("firsttrip_b2b"),
                    sharetrip_hars=hars.get("sharetrip"),
                    akij_hars=hars.get("akij"), bdfare_hars=hars.get("bdfare"),
                    firsttrip_b2c_hars=hars.get("firsttrip_b2c"),
                    use_true_base=True,
                )
            self._status = "Fetching previous report for change detection…"
            run_date = datetime.strptime(report["report_date"], "%d/%m/%Y").date()
            self._prev_payload = self._fetch_previous_payload(run_date)
            colored = apply_highlights(report, self._prev_payload)
            self._report = report
            self._status = "Done."
            self._log_usage("run_report",
                            count=sum(len(v) for v in hars.values()),
                            target=report.get("report_date"))

            warnings: list[str] = []
            if routes and not travel_date and not any(rd for _o, _d, rd in routes):
                warnings.append("No future travel date — the live FirstTrip B2C "
                                "fetch was SKIPPED (a past date is cleared "
                                "automatically). Enter a future date and re-run.")
            elif routes and report.get("channel_status", {}).get(
                    "Firsttrip-B2C") == "captured_but_empty":
                warnings.append("The live FirstTrip B2C fetch returned no data — "
                                "see the run log (network block, Cloudflare "
                                "challenge, or no fares for that date).")
            return {"ok": True, "report": colored,
                    "prev_available": self._prev_payload is not None,
                    "warnings": warnings,
                    "log": log_buffer.getvalue()[-8000:]}
        except MemoryError:
            self._status = "Out of memory."
            return {"ok": False, "log": log_buffer.getvalue()[-8000:],
                    "error": "Ran out of memory parsing a HAR. Re-scan and skip the "
                             "largest capture, or free RAM and retry."}
        except Exception as exc:    # noqa: BLE001 — surfaced in the UI, never crash
            self._status = "Failed."
            return {"ok": False, "error": f"Run failed: {exc}",
                    "log": log_buffer.getvalue()[-8000:]}
        finally:
            self._busy = False

    # ------------------------------------------------------------------ export
    def export_xlsx(self) -> dict[str, Any]:
        if not self._token():
            return {"ok": False, "auth_required": True, "error": "Sign in to use the app."}
        if not self._report:
            return {"ok": False, "error": "Run the report first."}
        import webview
        default_name = f"OTA_Discount_{datetime.now().strftime('%Y%m%d')}.xlsx"
        target = None
        if self._window:
            target = self._window.create_file_dialog(
                webview.SAVE_DIALOG, save_filename=default_name)
        # pywebview returns a tuple/list on some platforms and a plain string on
        # others; Path(str(tuple)) silently wrote to a garbage filename in the
        # exe's temp cwd — the "exported file isn't there" field bug.
        if isinstance(target, (tuple, list)):
            target = target[0] if target else None
        if not target:
            return {"ok": False, "error": "Export cancelled."}
        target_path = Path(str(target))
        if target_path.suffix.lower() != ".xlsx":
            target_path = target_path.with_suffix(".xlsx")
        path = write_single_sheet_xlsx(self._report, self._prev_payload, target_path)
        self._log_usage("export_xlsx", count=1)
        try:    # open Explorer with the file selected, so it's impossible to miss
            import subprocess
            subprocess.Popen(["explorer", "/select,", str(path)])
        except OSError:
            pass
        return {"ok": True, "path": str(path)}

    # -------------------------------------------------------------------- sync
    def _post_report(self, payload: dict[str, Any]) -> tuple[bool, int, str, dict[str, Any]]:
        token = self._token()
        if not token:
            return False, 401, "Sign in is required.", {}
        try:
            response = requests.post(
                f"{self.api_base}/api/v1/discount-reports",
                json={"report": payload, "client_version": __version__,
                      "sync_id": _sync_id_for(payload)},
                headers={"X-User-Session": token}, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            return False, 0, f"API unreachable: {exc}", {}
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 200:
            return True, 200, "", body
        detail = str(body.get("detail", response.reason or response.status_code))
        return False, response.status_code, detail, body

    def sync_now(self) -> dict[str, Any]:
        if not self._report:
            return {"ok": False, "error": "Run the report first."}
        # NOTE: the UI shows an explicit confirm before syncing an un-normalized
        # report (normalized=False) — by the time we're here, the user agreed.
        payload = sanitize_report_for_sync(self._report)
        report_date_iso = datetime.strptime(
            payload["report_date"], "%d/%m/%Y").date().isoformat()
        ok, status, error, body = self._post_report(payload)
        if ok:
            self._outbox.mark_done(report_date_iso)
            self._log_usage("sync_report", count=1, target=report_date_iso)
            return {"ok": True, "synced": report_date_iso,
                    "uses_remaining": body.get("uses_remaining"),
                    "outbox_count": self._outbox.count()}
        if status in (401, 403):    # auth problems don't belong in the outbox
            return {"ok": False, "error": error, "needs_login": status == 401}
        self._outbox.enqueue(report_date_iso, payload)
        return {"ok": False, "queued": True, "error": error,
                "outbox_count": self._outbox.count()}

    def flush_outbox(self) -> dict[str, Any]:
        sent = failed = rejected = 0
        last_error = ""
        for report_date_iso, payload in self._outbox.pending():
            ok, status, error, _body = self._post_report(payload)
            if ok:
                self._outbox.mark_done(report_date_iso)
                sent += 1
            elif status == 403:
                # Quota exhausted / plan expired / access revoked: this payload will
                # NEVER be accepted — move it aside so it stops zombie-retrying, and
                # surface why. (Idempotent sync_id means no risk if it was billed.)
                self._outbox.reject(report_date_iso)
                rejected += 1
                last_error = error
            else:
                failed += 1
                last_error = error
                if status in (0, 401):   # offline or expired session: stop retrying
                    break
        return {"sent": sent, "failed": failed, "rejected": rejected,
                "error": last_error, "outbox_count": self._outbox.count()}
