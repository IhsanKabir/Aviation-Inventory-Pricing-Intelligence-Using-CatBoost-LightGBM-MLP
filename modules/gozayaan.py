"""
Gozayaan OTA connector for normalized BS / 2A fare feeds.

Contract:
- fetch_flights_for_airline(...) returns:
  { raw, originalResponse, rows, ok }

Notes:
- This source currently provides fare + fare-rule policy data reliably.
- Seat inventory is usually not exposed in these endpoints, so inventory fields
  may remain unknown.
- Supports cached `x-kong-segment-id` loading and optional auto-refresh retry.
"""

from __future__ import annotations

import base64
import argparse
import datetime
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

from modules.penalties import parse_gozayaan_policies
from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

API_BASE = os.getenv("GOZAYAAN_API_BASE", "https://production.gozayaan.com/api")
SEARCH_URL = f"{API_BASE}/flight/v4.0/search/"
LEGS_URL = f"{API_BASE}/flight/v4.0/search/legs/"
LEG_FARES_URL = f"{API_BASE}/flight/v4.0/search/legs/fares/"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ENV_X_KONG_SEGMENT_ID = "GOZAYAAN_X_KONG_SEGMENT_ID"
ENV_COOKIES_PATH = "GOZAYAAN_COOKIES_PATH"
ENV_PROXY_URL = "GOZAYAAN_PROXY_URL"
ENV_ORIGIN = "GOZAYAAN_ORIGIN"
ENV_REFERER = "GOZAYAAN_REFERER"
ENV_MAX_LEG_POLLS = "GOZAYAAN_MAX_LEG_POLLS"
ENV_LEG_POLL_SLEEP_SEC = "GOZAYAAN_LEG_POLL_SLEEP_SEC"
ENV_TOKEN_CACHE_FILE = "GOZAYAAN_TOKEN_CACHE_FILE"
ENV_TOKEN_AUTO_REFRESH = "GOZAYAAN_TOKEN_AUTO_REFRESH"
ENV_TOKEN_REFRESH_CMD = "GOZAYAAN_TOKEN_REFRESH_CMD"
ENV_TOKEN_MIN_TTL_SEC = "GOZAYAAN_TOKEN_MIN_TTL_SEC"
ENV_TOKEN_REFRESH_TIMEOUT_SEC = "GOZAYAAN_TOKEN_REFRESH_TIMEOUT_SEC"
ENV_HEADERS_FILE = "GOZAYAAN_HEADERS_FILE"
ENV_RATE_LIMIT_STATE_FILE = "GOZAYAAN_RATE_LIMIT_STATE_FILE"
ENV_RATE_LIMIT_COOLDOWN_SEC = "GOZAYAAN_RATE_LIMIT_COOLDOWN_SEC"

DEFAULT_TOKEN_CACHE_FILE = "output/manual_sessions/gozayaan_token_latest.json"
DEFAULT_COOKIES_CACHE_FILE = "output/manual_sessions/gozayaan_cookies.json"
DEFAULT_HEADERS_CACHE_FILE = "output/manual_sessions/gozayaan_headers_latest.json"
DEFAULT_RATE_LIMIT_STATE_FILE = "output/manual_sessions/gozayaan_rate_limit_state.json"


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v in (None, ""):
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v in (None, ""):
            return None
        return int(v)
    except Exception:
        return None


def _bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    return bool(v)


def _parse_hash_str(hash_str: str) -> Dict[str, Optional[str]]:
    """
    Example hash_str:
    BS|DAC-CXB-2026-04-13-BS-157-AT7
    """
    out: Dict[str, Optional[str]] = {
        "airline": None,
        "origin": None,
        "destination": None,
        "departure_date": None,
        "flight_number_hint": None,
    }
    s = str(hash_str or "")
    parts = s.split("|")
    if parts:
        out["airline"] = (parts[0] or "").strip().upper() or None
    if len(parts) >= 2:
        block = parts[1].strip().upper()
        seg = block.split("-")
        if len(seg) >= 3:
            out["origin"] = seg[0]
            out["destination"] = seg[1]
            out["departure_date"] = seg[2]
    if len(parts) >= 3:
        out["flight_number_hint"] = (parts[2] or "").strip().upper() or None
    return out


def _first_adt_rule(leg_wise_fare_rules: Any, leg_hash_hint: Optional[str] = None) -> Dict[str, Any]:
    if not isinstance(leg_wise_fare_rules, dict) or not leg_wise_fare_rules:
        return {}
    if leg_hash_hint and leg_hash_hint in leg_wise_fare_rules:
        candidate = leg_wise_fare_rules.get(leg_hash_hint) or {}
        if isinstance(candidate, dict):
            adt = candidate.get("ADT")
            return adt if isinstance(adt, dict) else {}
    first_leg = next(iter(leg_wise_fare_rules.values()), None)
    if not isinstance(first_leg, dict):
        return {}
    adt = first_leg.get("ADT")
    return adt if isinstance(adt, dict) else {}


def _baggage_text(adt_rule: Dict[str, Any]) -> Optional[str]:
    bag = adt_rule.get("baggage_policy")
    if not isinstance(bag, dict):
        return None
    unit = str(bag.get("unit") or "").strip().upper()
    check_in = bag.get("check_in_quantity")
    if check_in not in (None, ""):
        if unit:
            return f"{check_in} {unit}"
        return str(check_in)
    pieces = bag.get("check_in_piece_count")
    if pieces not in (None, ""):
        return f"{pieces} PC"
    return None


def _seat_available_from_rule(adt_rule: Dict[str, Any]) -> Optional[int]:
    for k in (
        "available_seat",
        "available_seats",
        "seat_left",
        "seats_left",
        "seat_remaining",
        "seats_remaining",
        "remaining_seats",
    ):
        v = _safe_int(adt_rule.get(k))
        if v is not None:
            return v
    return None


def build_search_payload(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    return {
        "adult": max(1, int(adt or 1)),
        "child": max(0, int(chd or 0)),
        "child_age": [],
        "infant": max(0, int(inf or 0)),
        "cabin_class": str(cabin or "Economy"),
        "trips": [
            {
                "origin": str(origin).upper().strip(),
                "destination": str(destination).upper().strip(),
                "preferred_time": str(date).strip(),
            }
        ],
        "currency": "BDT",
        "region": "BD",
        "segment_id": str(uuid.uuid4()),
        "platform_type": "GZ_WEB",
        "trip_type": "One Way",
    }


def _default_headers() -> Dict[str, str]:
    origin = os.getenv(ENV_ORIGIN, "https://gozayaan.com").strip() or "https://gozayaan.com"
    referer = os.getenv(ENV_REFERER, "https://gozayaan.com/").strip() or "https://gozayaan.com/"
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
    }


def _load_extra_headers_from_cache() -> Dict[str, str]:
    path = Path(_headers_cache_file())
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    excluded = {
        "host",
        "content-length",
        "cookie",
        "connection",
        "accept-encoding",
    }
    out: Dict[str, str] = {}
    for k, v in raw.items():
        name = str(k or "").strip()
        if not name:
            continue
        if name.lower() in excluded:
            continue
        out[name] = str(v or "")
    return out


def _build_headers(token: Optional[str] = None) -> Dict[str, str]:
    out = _default_headers()
    out.update(_load_extra_headers_from_cache())
    x_kong = str(token or os.getenv(ENV_X_KONG_SEGMENT_ID, "")).strip()
    if x_kong:
        out["x-kong-segment-id"] = x_kong
    return out


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _parse_iso8601_utc(value: Any) -> Optional[datetime.datetime]:
    if value in (None, ""):
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    try:
        dt = datetime.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _decode_jwt_exp_utc(token: str) -> Optional[datetime.datetime]:
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
            return datetime.datetime.fromtimestamp(float(exp), tz=datetime.timezone.utc)
        except Exception:
            return None
    return None


def _seconds_until(dt_utc: Optional[datetime.datetime]) -> Optional[int]:
    if dt_utc is None:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    return int((dt_utc - now).total_seconds())


def _token_preview(token: Optional[str]) -> Optional[str]:
    s = str(token or "").strip()
    if not s:
        return None
    if len(s) <= 18:
        return s
    return f"{s[:10]}...{s[-6:]}"


def _token_cache_file() -> str:
    return str(os.getenv(ENV_TOKEN_CACHE_FILE, "").strip() or DEFAULT_TOKEN_CACHE_FILE)


def _cookies_cache_file() -> str:
    return str(os.getenv(ENV_COOKIES_PATH, "").strip() or DEFAULT_COOKIES_CACHE_FILE)


def _headers_cache_file() -> str:
    return str(os.getenv(ENV_HEADERS_FILE, "").strip() or DEFAULT_HEADERS_CACHE_FILE)


def _rate_limit_state_file() -> str:
    return str(os.getenv(ENV_RATE_LIMIT_STATE_FILE, "").strip() or DEFAULT_RATE_LIMIT_STATE_FILE)


def _rate_limit_cooldown_sec() -> int:
    raw = _safe_int(os.getenv(ENV_RATE_LIMIT_COOLDOWN_SEC))
    return int(raw if raw is not None else 900)


def _resolve_initial_cookies_path(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit
    env_path = str(os.getenv(ENV_COOKIES_PATH, "")).strip()
    if env_path:
        return env_path
    default_path = Path(DEFAULT_COOKIES_CACHE_FILE)
    if default_path.exists():
        return str(default_path)
    return None


def _load_cached_kong_token(min_ttl_sec: int = 0) -> Optional[Dict[str, Any]]:
    path = Path(_token_cache_file())
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    token = str(obj.get("x_kong_segment_id") or obj.get("token") or "").strip()
    if not token:
        return None
    exp = _parse_iso8601_utc(obj.get("expires_at_utc")) or _decode_jwt_exp_utc(token)
    ttl_sec = _seconds_until(exp)
    below_min_ttl = bool(ttl_sec is not None and min_ttl_sec > 0 and ttl_sec < min_ttl_sec)
    return {
        "token": token,
        "source": "cache",
        "cache_file": str(path),
        "expires_at_utc": exp.isoformat() if exp else None,
        "ttl_sec": ttl_sec,
        "below_min_ttl": below_min_ttl,
    }


def _resolve_active_kong_token(min_ttl_sec: int = 0) -> Optional[Dict[str, Any]]:
    env_token = str(os.getenv(ENV_X_KONG_SEGMENT_ID, "")).strip()
    env_ctx: Optional[Dict[str, Any]] = None
    if env_token:
        exp = _decode_jwt_exp_utc(env_token)
        ttl_sec = _seconds_until(exp)
        env_ctx = {
            "token": env_token,
            "source": "env",
            "cache_file": None,
            "expires_at_utc": exp.isoformat() if exp else None,
            "ttl_sec": ttl_sec,
        }
        if ttl_sec is None or ttl_sec > min_ttl_sec:
            return env_ctx
    cache_ctx = _load_cached_kong_token(min_ttl_sec=min_ttl_sec)
    if cache_ctx:
        return cache_ctx
    return env_ctx


def _load_rate_limit_state() -> Dict[str, Any]:
    path = Path(_rate_limit_state_file())
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_rate_limit_state(payload: Dict[str, Any]) -> None:
    path = Path(_rate_limit_state_file())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _record_rate_limit_hit(*, status_code: Optional[int], body: Any) -> Dict[str, Any]:
    now = datetime.datetime.now(datetime.timezone.utc)
    cooldown_sec = _rate_limit_cooldown_sec()
    until_utc = now + datetime.timedelta(seconds=max(60, cooldown_sec))
    payload = {
        "recorded_at_utc": now.isoformat(),
        "cooldown_until_utc": until_utc.isoformat(),
        "cooldown_sec": cooldown_sec,
        "status_code": status_code,
        "body_preview": str(body or "")[:300],
    }
    _save_rate_limit_state(payload)
    return payload


def _clear_rate_limit_state() -> None:
    path = Path(_rate_limit_state_file())
    if path.exists():
        try:
            path.unlink()
        except Exception:
            pass


def _active_rate_limit_state() -> Optional[Dict[str, Any]]:
    state = _load_rate_limit_state()
    if not state:
        return None
    until = _parse_iso8601_utc(state.get("cooldown_until_utc"))
    if until is None:
        return None
    ttl_sec = _seconds_until(until)
    if ttl_sec is None or ttl_sec <= 0:
        return None
    out = dict(state)
    out["remaining_cooldown_sec"] = ttl_sec
    return out


def _token_needs_refresh(token_ctx: Optional[Dict[str, Any]], min_ttl_sec: int) -> bool:
    if not isinstance(token_ctx, dict):
        return True
    token = str(token_ctx.get("token") or "").strip()
    if not token:
        return True
    ttl_sec = token_ctx.get("ttl_sec")
    if ttl_sec is None:
        return False
    try:
        return int(ttl_sec) < int(max(0, min_ttl_sec))
    except Exception:
        return False


def _refresh_command_context(search_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = search_payload if isinstance(search_payload, dict) else {}
    trips = payload.get("trips") if isinstance(payload.get("trips"), list) else []
    trip0 = trips[0] if trips and isinstance(trips[0], dict) else {}
    return {
        "origin": str(trip0.get("origin") or ""),
        "destination": str(trip0.get("destination") or ""),
        "date": str(trip0.get("preferred_time") or ""),
        "cabin": str(payload.get("cabin_class") or ""),
        "adt": int(_safe_int(payload.get("adult")) or 1),
        "chd": int(_safe_int(payload.get("child")) or 0),
        "inf": int(_safe_int(payload.get("infant")) or 0),
    }


def _default_refresh_command(
    cache_file: str,
    search_payload: Optional[Dict[str, Any]],
    cookies_file: str,
    headers_file: str,
) -> str:
    py = sys.executable or "python"
    ctx = _refresh_command_context(search_payload)
    return (
        f'"{py}" tools/refresh_gozayaan_token.py '
        f'--out "{cache_file}" --non-interactive '
        f'--cookies-out "{cookies_file}" '
        f'--headers-out "{headers_file}" '
        f'--origin "{ctx["origin"]}" --destination "{ctx["destination"]}" '
        f'--date "{ctx["date"]}" --cabin "{ctx["cabin"]}" '
        f'--adt {ctx["adt"]} --chd {ctx["chd"]} --inf {ctx["inf"]}'
    )


def _run_refresh_command(
    cache_file: str,
    search_payload: Optional[Dict[str, Any]] = None,
    cookies_file: Optional[str] = None,
    headers_file: Optional[str] = None,
) -> Dict[str, Any]:
    effective_cookies_file = str(cookies_file or _cookies_cache_file())
    effective_headers_file = str(headers_file or _headers_cache_file())
    command_template = str(os.getenv(ENV_TOKEN_REFRESH_CMD, "")).strip()
    fmt_ctx = {
        "python": sys.executable or "python",
        "cache_file": cache_file,
        "cookies_file": effective_cookies_file,
        "headers_file": effective_headers_file,
    }
    fmt_ctx.update(_refresh_command_context(search_payload))
    if command_template:
        try:
            command = command_template.format(**fmt_ctx)
        except Exception:
            command = command_template
    else:
        command = _default_refresh_command(
            cache_file,
            search_payload,
            effective_cookies_file,
            effective_headers_file,
        )

    timeout_sec = _safe_float(os.getenv(ENV_TOKEN_REFRESH_TIMEOUT_SEC))
    timeout_sec = float(timeout_sec if timeout_sec is not None else 120.0)
    timeout_sec = max(15.0, timeout_sec)
    LOG.info("Running Gozayaan token refresh command")
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "command": command,
            "returncode": proc.returncode,
            "context": fmt_ctx,
            "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-10:]),
            "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-10:]),
            "timeout_sec": timeout_sec,
        }
    except Exception as exc:
        return {
            "ok": False,
            "command": command,
            "context": fmt_ctx,
            "error": str(exc),
            "timeout_sec": timeout_sec,
        }


def _attempt_runtime_refresh(
    *,
    search_payload: Optional[Dict[str, Any]],
    cookie_cache_file: str,
    headers_cache_file: str,
    min_ttl_sec: int,
) -> Dict[str, Any]:
    refresh_meta = _run_refresh_command(
        _token_cache_file(),
        search_payload=search_payload,
        cookies_file=cookie_cache_file,
        headers_file=headers_cache_file,
    )
    refreshed_ctx = _resolve_active_kong_token(min_ttl_sec=min_ttl_sec)
    refreshed_token = str((refreshed_ctx or {}).get("token") or "").strip()
    return {
        "refresh_meta": refresh_meta,
        "token_ctx": refreshed_ctx,
        "token": refreshed_token,
        "cookies_path": cookie_cache_file if Path(cookie_cache_file).exists() else None,
        "headers": _build_headers(token=refreshed_token or None),
    }


def _post_json(req: Requester, url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> tuple[int, Any]:
    response = req.session.post(url, json=payload, headers=headers, timeout=req.timeout)
    try:
        body: Any = response.json()
    except Exception:
        body = response.text
    return response.status_code, body


def _is_rate_limited(body: Any, status_code: Optional[int] = None) -> bool:
    if status_code in {419, 420, 429}:
        return True
    if not isinstance(body, dict):
        return False
    err = body.get("error") or {}
    msg = str((err.get("message") if isinstance(err, dict) else "") or "").lower()
    code = str((err.get("code") if isinstance(err, dict) else "") or "").strip()
    return ("rate limit" in msg) or (code in {"419", "420", "429"})


def _is_auth_or_access_error(body: Any, status_code: Optional[int] = None) -> bool:
    if status_code in {401, 403}:
        return True
    text = ""
    if isinstance(body, dict):
        err = body.get("error") or {}
        if isinstance(err, dict):
            text = " ".join(
                str(err.get(k) or "")
                for k in ("message", "code", "detail", "title")
            ).lower()
        else:
            text = str(body).lower()
    else:
        text = str(body or "").lower()
    return any(
        needle in text
        for needle in (
            "unauthorized",
            "forbidden",
            "access denied",
            "invalid token",
            "token expired",
            "x-kong",
        )
    )


def _poll_legs(
    *,
    req: Requester,
    search_id: str,
    headers: Dict[str, str],
    max_polls: int,
    poll_sleep_sec: float,
) -> Dict[str, Any]:
    fares_by_key: Dict[str, Dict[str, Any]] = {}
    legs_by_hash: Dict[str, Dict[str, Any]] = {}
    segments_by_hash: Dict[str, Dict[str, Any]] = {}
    polls: List[Dict[str, Any]] = []
    last_body: Any = None

    for idx in range(max(1, max_polls)):
        status, body = _post_json(
            req,
            LEGS_URL,
            {"search_id": search_id, "leg_type": "L1"},
            headers,
        )
        last_body = body
        poll_info = {"attempt": idx + 1, "http_status": status}
        polls.append(poll_info)

        if status != 200 or not isinstance(body, dict):
            break

        result = body.get("result") or {}
        if not isinstance(result, dict):
            break

        fares = result.get("fares") or []
        legs = result.get("legs") or []
        segments = result.get("segments") or []

        if isinstance(fares, list):
            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                key = str(fare.get("id") or "") or (
                    f"{fare.get('hash')}::{fare.get('total_fare_amount')}::{fare.get('total_base_amount')}"
                )
                fares_by_key[key] = fare
        if isinstance(legs, list):
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                h = str(leg.get("hash") or "")
                if h:
                    legs_by_hash[h] = leg
        if isinstance(segments, list):
            for seg in segments:
                if not isinstance(seg, dict):
                    continue
                h = str(seg.get("hash") or "")
                if h:
                    segments_by_hash[h] = seg

        status_text = str(result.get("status") or "").upper()
        progress = _safe_int(result.get("progress"))
        expected = _safe_int(result.get("expected_progress"))
        poll_info["status"] = status_text
        poll_info["progress"] = progress
        poll_info["expected_progress"] = expected
        poll_info["fares_count"] = len(fares) if isinstance(fares, list) else 0

        if status_text in {"COMPLETED", "DONE", "FINISHED"}:
            break
        if progress is not None and expected is not None and progress >= expected:
            break

        if idx < max_polls - 1 and poll_sleep_sec > 0:
            time.sleep(poll_sleep_sec)

    return {
        "fares": list(fares_by_key.values()),
        "legs_by_hash": legs_by_hash,
        "segments_by_hash": segments_by_hash,
        "polls": polls,
        "last_response": last_body,
    }


def _candidate_leg_hashes_for_airline(
    fares: List[Dict[str, Any]],
    *,
    airline_code: str,
    origin: str,
    destination: str,
) -> List[str]:
    wanted_airline = str(airline_code or "").upper().strip()
    wanted_origin = str(origin or "").upper().strip()
    wanted_dest = str(destination or "").upper().strip()
    found: set[str] = set()
    for fare in fares:
        if not isinstance(fare, dict):
            continue
        meta = _parse_hash_str(str(fare.get("hash_str") or ""))
        if meta.get("airline") != wanted_airline:
            continue
        if meta.get("origin") and meta["origin"] != wanted_origin:
            continue
        if meta.get("destination") and meta["destination"] != wanted_dest:
            continue
        leg_hashes = fare.get("leg_hashes")
        if isinstance(leg_hashes, list):
            for h in leg_hashes:
                hs = str(h or "").strip()
                if hs:
                    found.add(hs)
        hs = str(fare.get("hash") or "").strip()
        if hs:
            found.add(hs)
    return sorted(found)


def _normalize_fare_row(
    *,
    airline_code: str,
    search_id: str,
    leg_hash: str,
    fare: Dict[str, Any],
    leg: Optional[Dict[str, Any]],
    segments: List[Dict[str, Any]],
    policies: List[Dict[str, Any]],
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    hash_meta = _parse_hash_str(str(fare.get("hash_str") or ""))
    adt_rule = _first_adt_rule(fare.get("leg_wise_fare_rules"), leg_hash_hint=leg_hash)

    seg_first = segments[0] if segments else {}
    seg_last = segments[-1] if segments else {}

    flight_number = (
        str(seg_first.get("flight_number") or "").strip()
        or hash_meta.get("flight_number_hint")
        or str(fare.get("id") or "")
    )
    departure = (
        seg_first.get("departure_date_time")
        or (leg or {}).get("departure_date_time")
        or (
            f"{hash_meta['departure_date']}T00:00:00"
            if hash_meta.get("departure_date")
            else None
        )
    )
    arrival = (
        seg_last.get("arrival_date_time")
        or (leg or {}).get("arrival_date_time")
        or departure
    )
    origin = (
        str(seg_first.get("origin") or "").upper().strip()
        or (hash_meta.get("origin") or "")
        or ""
    )
    destination = (
        str(seg_last.get("destination") or "").upper().strip()
        or (hash_meta.get("destination") or "")
        or ""
    )
    equipment = str(seg_first.get("equipment") or "").strip() or None
    duration_min = _safe_int((leg or {}).get("travel_time")) or _safe_int(seg_first.get("flight_time"))
    cabin = str(adt_rule.get("cabin_class") or requested_cabin or "Economy")
    fare_basis = str(adt_rule.get("fare_basis") or fare.get("id") or "")
    brand = str(adt_rule.get("fare_family") or fare.get("fare_type") or "OTA")

    currency = str(fare.get("currency") or "BDT")
    fare_amount = _safe_float(fare.get("total_base_amount"))
    tax_amount = _safe_float(fare.get("total_tax_amount"))
    total_amount = _safe_float(fare.get("total_fare_amount"))
    price_total_bdt = total_amount
    if total_amount is not None and currency.upper() != "BDT":
        # Keep output contract stable; FX conversion can be added later.
        price_total_bdt = total_amount

    seat_available = _seat_available_from_rule(adt_rule)
    inventory_confidence = "reported" if seat_available is not None else "unknown_ota"
    soldout = bool(seat_available == 0) if seat_available is not None else False

    penalty_fields = parse_gozayaan_policies(policies if isinstance(policies, list) else [])
    if penalty_fields.get("fare_changeable") is None and adt_rule.get("changeable") is not None:
        penalty_fields["fare_changeable"] = bool(adt_rule.get("changeable"))
    if penalty_fields.get("fare_refundable") is None and adt_rule.get("refundable") is not None:
        penalty_fields["fare_refundable"] = bool(adt_rule.get("refundable"))
    if not penalty_fields.get("penalty_currency") and adt_rule.get("currency"):
        penalty_fields["penalty_currency"] = str(adt_rule.get("currency"))
    via_airports = list(
        dict.fromkeys(
            str(seg.get("destination") or "").upper().strip()
            for seg in segments[:-1]
            if str(seg.get("destination") or "").upper().strip()
            and str(seg.get("destination") or "").upper().strip() not in {origin, destination}
        )
    )

    row: Dict[str, Any] = {
        "airline": str(airline_code).upper(),
        "operating_airline": str(seg_first.get("operating_carrier") or "").upper().strip() or None,
        "flight_number": flight_number,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "arrival": arrival,
        "cabin": cabin,
        "fare_basis": fare_basis,
        "brand": brand,
        "price_total_bdt": price_total_bdt,
        "fare_amount": fare_amount,
        "tax_amount": tax_amount,
        "currency": currency,
        "duration_min": duration_min,
        "stops": max(0, len(segments) - 1),
        "via_airports": "|".join(via_airports) if via_airports else None,
        "booking_class": adt_rule.get("booking_code"),
        "baggage": _baggage_text(adt_rule),
        "equipment_code": equipment,
        "aircraft": equipment,
        "seat_capacity": None,
        "seat_available": seat_available,
        "inventory_confidence": inventory_confidence,
        "estimated_load_factor_pct": None,
        "soldout": soldout,
        "adt_count": max(1, int(adt or 1)),
        "chd_count": max(0, int(chd or 0)),
        "inf_count": max(0, int(inf or 0)),
        "fare_ref_num": str(fare.get("id") or ""),
        "fare_search_reference": str(search_id),
        "source_endpoint": "api/flight/v4.0/search/legs/fares",
        "raw_offer": {
            "search_id": search_id,
            "leg_hash": leg_hash,
            "fare": fare,
            "leg": leg,
            "segments": segments,
            "policies": policies,
        },
    }
    row.update({k: v for k, v in penalty_fields.items() if v is not None})
    if row.get("fare_changeable") is None and adt_rule.get("changeable") is not None:
        row["fare_changeable"] = _bool_or_none(adt_rule.get("changeable"))
    if row.get("fare_refundable") is None and adt_rule.get("refundable") is not None:
        row["fare_refundable"] = _bool_or_none(adt_rule.get("refundable"))
    return row


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for r in rows:
        key = (
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            r.get("departure"),
            r.get("flight_number"),
            r.get("cabin"),
            r.get("fare_basis"),
            r.get("brand"),
            r.get("via_airports"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def fetch_flights_for_airline(
    *,
    airline_code: str,
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified run_all.py contract:
    { raw, originalResponse, rows, ok }
    """
    search_payload = build_search_payload(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    cookie_cache_file = _cookies_cache_file()
    headers_cache_file = _headers_cache_file()
    cookies = _resolve_initial_cookies_path(cookies_path)
    proxy = proxy_url or os.getenv(ENV_PROXY_URL) or None
    min_ttl_sec = _safe_int(os.getenv(ENV_TOKEN_MIN_TTL_SEC))
    min_ttl_sec = int(min_ttl_sec if min_ttl_sec is not None else 300)
    auto_refresh_token = _env_truthy(ENV_TOKEN_AUTO_REFRESH, default=True)
    token_ctx = _resolve_active_kong_token(min_ttl_sec=min_ttl_sec)
    token_value = str((token_ctx or {}).get("token") or "").strip()
    headers = _build_headers(token=token_value or None)

    out: Dict[str, Any] = {
        "raw": {
            "source": "gozayaan",
            "airline": str(airline_code).upper(),
            "search_payload": search_payload,
            "headers_hint": {
                "has_x_kong_segment_id": bool(headers.get("x-kong-segment-id")),
                "x_kong_segment_id_preview": _token_preview(token_value),
                "x_kong_token_source": (token_ctx or {}).get("source"),
                "x_kong_token_expires_at_utc": (token_ctx or {}).get("expires_at_utc"),
                "x_kong_token_ttl_sec": (token_ctx or {}).get("ttl_sec"),
                "token_cache_file": _token_cache_file(),
                "cookies_path": cookies,
                "cookies_cache_file": cookie_cache_file,
                "headers_cache_file": headers_cache_file,
                "extra_headers_loaded": bool(_load_extra_headers_from_cache()),
                "token_auto_refresh": auto_refresh_token,
                "origin": headers.get("Origin"),
                "referer": headers.get("Referer"),
            },
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }

    active_cooldown = _active_rate_limit_state()
    if active_cooldown:
        out["raw"]["error"] = "rate_limit_cooldown_active"
        out["raw"]["rate_limit_state"] = active_cooldown
        out["raw"]["hint"] = (
            "GOzayaan is in cooldown after a recent rate-limit response. "
            "Wait for the cooldown window to expire before retrying."
        )
        return out

    if auto_refresh_token and _token_needs_refresh(token_ctx, min_ttl_sec=min_ttl_sec):
        pre_refresh = _attempt_runtime_refresh(
            search_payload=search_payload,
            cookie_cache_file=str(cookies or cookie_cache_file),
            headers_cache_file=headers_cache_file,
            min_ttl_sec=min_ttl_sec,
        )
        out["raw"]["pre_search_token_refresh"] = pre_refresh["refresh_meta"]
        refreshed_token = str(pre_refresh.get("token") or "").strip()
        if refreshed_token:
            token_ctx = pre_refresh.get("token_ctx")
            token_value = refreshed_token
            headers = dict(pre_refresh.get("headers") or headers)
            cookies = str(pre_refresh.get("cookies_path") or cookies or cookie_cache_file)
    req = Requester(cookies_path=cookies, user_agent=USER_AGENT, proxy_url=proxy)

    status, search_body = _post_json(req, SEARCH_URL, search_payload, headers)
    out["raw"]["search_status"] = status
    out["raw"]["search_response"] = search_body

    search_ok = status == 200 and isinstance(search_body, dict) and bool(search_body.get("status"))
    should_retry_refresh = (
        not search_ok
        and auto_refresh_token
        and (
            _is_rate_limited(search_body, status_code=status)
            or _is_auth_or_access_error(search_body, status_code=status)
        )
    )
    if should_retry_refresh:
        refresh_cookies_file = str(cookies or cookie_cache_file)
        refresh_attempt = _attempt_runtime_refresh(
            search_payload=search_payload,
            cookie_cache_file=refresh_cookies_file,
            headers_cache_file=headers_cache_file,
            min_ttl_sec=min_ttl_sec,
        )
        out["raw"]["token_refresh"] = refresh_attempt["refresh_meta"]
        refreshed_token = str(refresh_attempt.get("token") or "").strip()
        if refreshed_token:
            token_ctx = refresh_attempt.get("token_ctx")
            retry_headers = dict(refresh_attempt.get("headers") or _build_headers(token=refreshed_token))
            refresh_cookies_path = str(refresh_attempt.get("cookies_path") or refresh_cookies_file)
            if refresh_cookies_path and Path(refresh_cookies_path).exists():
                cookies = refresh_cookies_path
                req = Requester(cookies_path=cookies, user_agent=USER_AGENT, proxy_url=proxy)
            retry_status, retry_body = _post_json(req, SEARCH_URL, search_payload, retry_headers)
            out["raw"]["search_retry_attempted"] = True
            out["raw"]["search_retry_status"] = retry_status
            out["raw"]["search_retry_response"] = retry_body
            out["raw"]["search_retry_headers_hint"] = {
                "has_x_kong_segment_id": bool(retry_headers.get("x-kong-segment-id")),
                "x_kong_segment_id_preview": _token_preview(refreshed_token),
                "x_kong_token_source": (token_ctx or {}).get("source"),
                "x_kong_token_expires_at_utc": (token_ctx or {}).get("expires_at_utc"),
                "x_kong_token_ttl_sec": (token_ctx or {}).get("ttl_sec"),
            }
            status, search_body = retry_status, retry_body
            headers = retry_headers
            search_ok = status == 200 and isinstance(search_body, dict) and bool(search_body.get("status"))
        else:
            out["raw"]["search_retry_attempted"] = False
            out["raw"]["search_retry_error"] = "token_refresh_did_not_produce_token"

    if status != 200 or not isinstance(search_body, dict):
        if _is_rate_limited(search_body, status_code=status):
            out["raw"]["rate_limit_state"] = _record_rate_limit_hit(status_code=status, body=search_body)
        out["raw"]["error"] = "search_failed"
        return out

    if not bool(search_body.get("status")):
        out["raw"]["error"] = "search_not_ok"
        if _is_rate_limited(search_body, status_code=status):
            out["raw"]["rate_limit_state"] = _record_rate_limit_hit(status_code=status, body=search_body)
            out["raw"]["hint"] = (
                "OTA rejected request as rate-limited/blocked. "
                "Use GOZAYAAN_TOKEN_AUTO_REFRESH=1 with a valid "
                "GOZAYAAN_TOKEN_REFRESH_CMD, or set GOZAYAAN_X_KONG_SEGMENT_ID. "
                "If non-interactive refresh keeps failing, run one interactive "
                "tools/refresh_gozayaan_token.py capture in a real browser session."
            )
        return out

    result = search_body.get("result") or {}
    if not isinstance(result, dict):
        out["raw"]["error"] = "search_result_missing"
        return out

    search_id = str(result.get("search_id") or "").strip()
    out["raw"]["search_id"] = search_id
    out["originalResponse"] = result
    if not search_id:
        out["raw"]["error"] = "search_id_missing"
        return out

    max_polls = _safe_int(os.getenv(ENV_MAX_LEG_POLLS)) or 4
    poll_sleep = _safe_float(os.getenv(ENV_LEG_POLL_SLEEP_SEC))
    if poll_sleep is None:
        poll_sleep = 0.8

    legs_state = _poll_legs(
        req=req,
        search_id=search_id,
        headers=headers,
        max_polls=max_polls,
        poll_sleep_sec=float(max(0.0, poll_sleep)),
    )
    out["raw"]["legs_polls"] = legs_state.get("polls")
    out["raw"]["legs_last_response"] = legs_state.get("last_response")

    basic_fares = legs_state.get("fares") or []
    legs_by_hash = dict(legs_state.get("legs_by_hash") or {})
    segments_by_hash = dict(legs_state.get("segments_by_hash") or {})

    target_leg_hashes = _candidate_leg_hashes_for_airline(
        basic_fares,
        airline_code=airline_code,
        origin=origin,
        destination=destination,
    )
    out["raw"]["target_leg_hashes"] = target_leg_hashes

    rows: List[Dict[str, Any]] = []
    leg_fares_calls: List[Dict[str, Any]] = []
    for leg_hash in target_leg_hashes:
        status_leg_fares, leg_fares_body = _post_json(
            req,
            LEG_FARES_URL,
            {"search_id": search_id, "leg_type": "L1", "leg_hash": leg_hash},
            headers,
        )
        leg_fares_calls.append(
            {"leg_hash": leg_hash, "http_status": status_leg_fares, "status_ok": bool(isinstance(leg_fares_body, dict) and leg_fares_body.get("status"))}
        )

        if status_leg_fares != 200 or not isinstance(leg_fares_body, dict):
            continue
        if not bool(leg_fares_body.get("status")):
            continue
        leg_result = leg_fares_body.get("result") or {}
        if not isinstance(leg_result, dict):
            continue

        fares = leg_result.get("fares") or []
        policies = leg_result.get("policies") or []
        if isinstance(leg_result.get("legs"), list):
            for leg in leg_result.get("legs"):
                if isinstance(leg, dict):
                    h = str(leg.get("hash") or "")
                    if h:
                        legs_by_hash[h] = leg
        if isinstance(leg_result.get("segments"), list):
            for seg in leg_result.get("segments"):
                if isinstance(seg, dict):
                    h = str(seg.get("hash") or "")
                    if h:
                        segments_by_hash[h] = seg

        if not isinstance(fares, list):
            continue

        for fare in fares:
            if not isinstance(fare, dict):
                continue
            meta = _parse_hash_str(str(fare.get("hash_str") or ""))
            if meta.get("airline") != str(airline_code).upper():
                continue

            fare_leg_hashes = fare.get("leg_hashes")
            primary_leg_hash = leg_hash
            if isinstance(fare_leg_hashes, list) and fare_leg_hashes:
                hs = str(fare_leg_hashes[0] or "").strip()
                if hs:
                    primary_leg_hash = hs

            leg_obj = legs_by_hash.get(primary_leg_hash)
            seg_objs: List[Dict[str, Any]] = []
            if isinstance(leg_obj, dict):
                segment_hashes = leg_obj.get("segment_hashes")
                if isinstance(segment_hashes, list):
                    for sh in segment_hashes:
                        seg = segments_by_hash.get(str(sh or ""))
                        if isinstance(seg, dict):
                            seg_objs.append(seg)

            row = _normalize_fare_row(
                airline_code=str(airline_code).upper(),
                search_id=search_id,
                leg_hash=primary_leg_hash,
                fare=fare,
                leg=leg_obj,
                segments=seg_objs,
                policies=policies if isinstance(policies, list) else [],
                requested_cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
            )
            rows.append(row)

    out["raw"]["leg_fares_calls"] = leg_fares_calls
    out["rows"] = _dedupe_rows(rows)
    out["ok"] = True
    _clear_rate_limit_state()
    return out


def cli_main():
    parser = argparse.ArgumentParser(description="Gozayaan OTA connector tester")
    parser.add_argument("--airline", required=True, choices=["BS", "2A"])
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--cookies-path", default=None)
    parser.add_argument("--proxy-url", default=None)
    args = parser.parse_args()

    output = fetch_flights_for_airline(
        airline_code=args.airline,
        origin=args.origin,
        destination=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        cookies_path=args.cookies_path,
        proxy_url=args.proxy_url,
    )
    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
