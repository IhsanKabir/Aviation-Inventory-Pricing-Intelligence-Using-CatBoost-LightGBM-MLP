"""
Dynamic fleet seat-capacity mapping.

Loads aircraft capacity maps from:
1) Cached local file (cache/fleet_capacity_cache.json)
2) Live airline fleet pages (if refresh window expired)
3) Built-in defaults as safe fallback
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests


LOG = logging.getLogger("fleet_mapping")
LOG.addHandler(logging.NullHandler())

CONFIG_PATH = Path("config/fleet_capacity_sources.json")
CACHE_PATH = Path("cache/fleet_capacity_cache.json")


DEFAULT_CAPACITY_MAP: Dict[str, Dict[str, int]] = {
    "VQ": {
        "ATR725": 72,
        "ATR72-500": 72,
        "ATR72": 72,
    },
    "BG": {
        "BOEING 787-8": 271,
        "BOEING 787-9": 298,
        "BOEING 777-300 ER": 419,
        "BOEING 777-300ER": 419,
        "BOEING 737-800": 162,
        "DASH 8-400": 74,
        "BOMBARDIER DASH-8-Q400": 74,
        "Q400": 74,
        "DH8": 74,
        "788": 271,
        "789": 298,
        "77W": 419,
        "773": 419,
        "737": 162,
        "738": 162,
    },
}

DEFAULT_FLEET_INVENTORY: Dict[str, list[dict]] = {
    "VQ": [
        {
            "aircraft_type": "ATR 72-500",
            "aircraft_count": 7,
            "seats_per_aircraft": 72,
        }
    ],
    "BG": [
        {
            "aircraft_type": "Boeing 787-8",
            "aircraft_count": 4,
            "seats_per_aircraft": 271,
        },
        {
            "aircraft_type": "Boeing 787-9",
            "aircraft_count": 2,
            "seats_per_aircraft": 298,
        },
        {
            "aircraft_type": "Boeing 777-300 ER",
            "aircraft_count": 4,
            "seats_per_aircraft": 419,
        },
        {
            "aircraft_type": "Boeing 737-800",
            "aircraft_count": 4,
            "seats_per_aircraft": 162,
        },
        {
            "aircraft_type": "Dash 8-400",
            "aircraft_count": 5,
            "seats_per_aircraft": 74,
        },
    ],
}


DEFAULT_REFRESH_HOURS = 24
DEFAULT_FAILURE_RETRY_MINUTES = 60


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {
            "refresh_hours": DEFAULT_REFRESH_HOURS,
            "failure_retry_minutes": DEFAULT_FAILURE_RETRY_MINUTES,
            "sources": {
                "VQ": "https://www.flynovoair.com/about/fleet",
                "BG": "https://www.biman-airlines.com/fleet",
            },
        }
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed to read fleet config, using defaults: %s", exc)
        return {
            "refresh_hours": DEFAULT_REFRESH_HOURS,
            "failure_retry_minutes": DEFAULT_FAILURE_RETRY_MINUTES,
            "sources": {
                "VQ": "https://www.flynovoair.com/about/fleet",
                "BG": "https://www.biman-airlines.com/fleet",
            },
        }


def _parse_vq_capacity(html: str) -> Dict[str, int]:
    # Example text: "ATR 72-500 ... 72 - seater turboprop ..."
    out: Dict[str, int] = {}
    m = re.search(r"ATR\s*72-?500[^0-9]{0,80}(\d{2,3})\s*-\s*seater", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        seats = int(m.group(1))
        out["ATR725"] = seats
        out["ATR72-500"] = seats
        out["ATR72"] = seats
    return out


def _parse_bg_capacity(html: str) -> Dict[str, int]:
    out: Dict[str, int] = {}

    # Parse table rows with aircraft type and total seats per aircraft.
    row_rx = re.compile(
        r"<tr>\s*<td>\s*(?P<aircraft>[^<]+?)\s*</td>\s*<td>\s*\d+\s*</td>\s*<td>.*?</td>\s*<td>\s*(?P<seats>\d{2,3})\s*</td>\s*</tr>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    for m in row_rx.finditer(html):
        aircraft = re.sub(r"\s+", " ", m.group("aircraft")).strip().upper()
        seats = int(m.group("seats"))
        out[aircraft] = seats

    # Add common code aliases when aircraft names are present.
    if "BOEING 787-8" in out:
        out["788"] = out["BOEING 787-8"]
    if "BOEING 787-9" in out:
        out["789"] = out["BOEING 787-9"]
    if "BOEING 777-300 ER" in out:
        out["77W"] = out["BOEING 777-300 ER"]
        out["773"] = out["BOEING 777-300 ER"]
        out["BOEING 777-300ER"] = out["BOEING 777-300 ER"]
    if "BOEING 737" in out:
        out["737"] = out["BOEING 737"]
        out["738"] = out["BOEING 737"]
        out["BOEING 737-800"] = out["BOEING 737"]
    if "BOEING 737-800" in out:
        out["737"] = out["BOEING 737-800"]
        out["738"] = out["BOEING 737-800"]
    if "DASH 8-400" in out:
        out["Q400"] = out["DASH 8-400"]
        out["DH8"] = out["DASH 8-400"]
        out["BOMBARDIER DASH-8-Q400"] = out["DASH 8-400"]

    return out


def _word_to_int(token: str) -> Optional[int]:
    token_l = str(token or "").strip().lower()
    words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
        "thirteen": 13,
        "fourteen": 14,
        "fifteen": 15,
        "sixteen": 16,
        "seventeen": 17,
        "eighteen": 18,
        "nineteen": 19,
        "twenty": 20,
    }
    if token_l.isdigit():
        return int(token_l)
    return words.get(token_l)


def _parse_vq_inventory(html: str) -> list[dict]:
    clean = re.sub(r"\s+", " ", html or "")
    count = None
    patterns = [
        r"with\s+([A-Za-z0-9]+)\s+ATR\s*72-?500\s+aircraft",
        r"ATR\s*72-?500[^.]{0,80}\b([A-Za-z0-9]+)\s*-\s*seater",
    ]
    for pat in patterns:
        m = re.search(pat, clean, flags=re.IGNORECASE)
        if m:
            count = _word_to_int(m.group(1))
            if count:
                break
    if count is None:
        count = 7
    return [
        {
            "aircraft_type": "ATR 72-500",
            "aircraft_count": int(count),
            "seats_per_aircraft": 72,
        }
    ]


def _parse_bg_inventory(html: str) -> list[dict]:
    rows = []
    row_rx = re.compile(
        r"<tr>\s*<td>\s*(?P<aircraft>[^<]+?)\s*</td>\s*<td>\s*(?P<count>\d{1,3})\s*</td>\s*<td>.*?</td>\s*<td>\s*(?P<seats>\d{2,3})\s*</td>\s*</tr>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for m in row_rx.finditer(html or ""):
        aircraft = re.sub(r"\s+", " ", m.group("aircraft")).strip()
        rows.append(
            {
                "aircraft_type": aircraft,
                "aircraft_count": int(m.group("count")),
                "seats_per_aircraft": int(m.group("seats")),
            }
        )
    return rows


def _download_html(url: str, timeout: int = 20) -> Optional[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            LOG.warning("Fleet page fetch failed status=%s url=%s", resp.status_code, url)
            return None
        return resp.text
    except Exception as exc:
        LOG.warning("Fleet page fetch exception url=%s err=%s", url, exc)
        return None


def _merge_maps(base: Dict[str, Dict[str, int]], update: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {k: dict(v) for k, v in base.items()}
    for airline, mapping in update.items():
        out.setdefault(airline, {})
        out[airline].update(mapping or {})
    return out


def _merge_inventory_maps(base: Dict[str, list[dict]], update: Dict[str, list[dict]]) -> Dict[str, list[dict]]:
    out: Dict[str, list[dict]] = {k: list(v) for k, v in base.items()}
    for airline, rows in (update or {}).items():
        if not rows:
            continue
        out[airline] = list(rows)
    return out


def _read_cache() -> Optional[dict]:
    if not CACHE_PATH.exists():
        return None
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed reading fleet cache: %s", exc)
        return None


def _write_cache(payload: dict) -> None:
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        LOG.warning("Failed writing fleet cache: %s", exc)


def _needs_refresh(cached_at_iso: Optional[str], refresh_hours: int) -> bool:
    if not cached_at_iso:
        return True
    try:
        cached_at = datetime.fromisoformat(cached_at_iso)
    except Exception:
        return True
    now = datetime.now(timezone.utc)
    return (now - cached_at) >= timedelta(hours=refresh_hours)


def _within_failure_cooldown(
    failed_at_iso: Optional[str],
    retry_minutes: int,
) -> bool:
    if not failed_at_iso:
        return False
    try:
        failed_at = datetime.fromisoformat(failed_at_iso)
    except Exception:
        return False
    now = datetime.now(timezone.utc)
    return (now - failed_at) < timedelta(minutes=retry_minutes)


def _normalize_airline_list(airlines: Optional[list[str]]) -> Optional[set[str]]:
    if not airlines:
        return None
    return {str(a).upper() for a in airlines if str(a).strip()}


def get_fleet_capacity_map(
    force_refresh: bool = False,
    airlines: Optional[list[str]] = None,
) -> Dict[str, Dict[str, int]]:
    cfg = _load_config()
    refresh_hours = int(cfg.get("refresh_hours") or DEFAULT_REFRESH_HOURS)
    failure_retry_minutes = int(cfg.get("failure_retry_minutes") or DEFAULT_FAILURE_RETRY_MINUTES)
    sources = cfg.get("sources") or {}
    target_airlines = _normalize_airline_list(airlines)

    cached = _read_cache() or {}
    cached_at = cached.get("cached_at")
    cached_at_by_airline = cached.get("cached_at_by_airline") or {}
    failed_at_by_airline = cached.get("failed_at_by_airline") or {}
    cached_map = cached.get("capacity_map") if isinstance(cached.get("capacity_map"), dict) else {}
    cached_inventory = (
        cached.get("fleet_inventory_map")
        if isinstance(cached.get("fleet_inventory_map"), dict)
        else {}
    )

    base_map = _merge_maps(DEFAULT_CAPACITY_MAP, cached_map or {})

    all_targets = target_airlines or {str(a).upper() for a in sources.keys()} | set(base_map.keys())

    stale_airlines = []
    for airline_u in all_targets:
        airline_cached_at = cached_at_by_airline.get(airline_u) or cached_at
        needs_refresh = force_refresh or _needs_refresh(airline_cached_at, refresh_hours)
        cooling_down = _within_failure_cooldown(
            failed_at_by_airline.get(airline_u),
            failure_retry_minutes,
        )
        if needs_refresh and not cooling_down:
            stale_airlines.append(airline_u)

    if not stale_airlines:
        return base_map

    live_updates: Dict[str, Dict[str, int]] = {}
    now_iso = datetime.now(timezone.utc).isoformat()
    for airline_u in stale_airlines:
        url = sources.get(airline_u) or sources.get(airline_u.lower()) or sources.get(airline_u.title())
        if not url:
            continue
        html = _download_html(str(url))
        if not html:
            failed_at_by_airline[airline_u] = now_iso
            continue
        if airline_u == "VQ":
            parsed = _parse_vq_capacity(html)
        elif airline_u == "BG":
            parsed = _parse_bg_capacity(html)
        else:
            parsed = {}
        if parsed:
            live_updates[airline_u] = parsed
            LOG.info("Fleet mapping refreshed airline=%s entries=%d", airline_u, len(parsed))
            cached_at_by_airline[airline_u] = now_iso
            if airline_u in failed_at_by_airline:
                failed_at_by_airline.pop(airline_u, None)
        else:
            failed_at_by_airline[airline_u] = now_iso

    merged = _merge_maps(base_map, live_updates)
    _write_cache(
        {
            "cached_at": now_iso,
            "cached_at_by_airline": cached_at_by_airline,
            "failed_at_by_airline": failed_at_by_airline,
            "capacity_map": merged,
            "fleet_inventory_map": cached_inventory,
            "sources": sources,
        }
    )
    return merged


def _lookup_capacity(mapping: Dict[str, int], aircraft: Optional[str], equipment_code: Optional[str]) -> Optional[int]:
    candidates = []
    if equipment_code:
        candidates.append(str(equipment_code).strip().upper())
    if aircraft:
        candidates.append(str(aircraft).strip().upper())

    for token in candidates:
        token_clean = re.sub(r"\s+", " ", token).strip()
        if token_clean in mapping:
            return mapping[token_clean]
        compact = token_clean.replace(" ", "")
        if compact in mapping:
            return mapping[compact]
        m = re.search(r"(\d{2,4}|77W|DH8|Q400)", compact)
        if m and m.group(1) in mapping:
            return mapping[m.group(1)]

        # partial contains match fallback for strings like "Boeing 787-8 Dreamliner"
        for key, val in mapping.items():
            if key and key in token_clean:
                return val

    return None


def resolve_seat_capacity(
    airline: str,
    aircraft: Optional[str] = None,
    equipment_code: Optional[str] = None,
    force_refresh: bool = False,
) -> Optional[int]:
    airline_u = str(airline or "").upper()
    capacity_map = get_fleet_capacity_map(
        force_refresh=force_refresh,
        airlines=[airline_u],
    )
    mapping = capacity_map.get(airline_u) or {}
    if not mapping:
        return None
    return _lookup_capacity(mapping, aircraft, equipment_code)


def get_fleet_inventory(
    force_refresh: bool = False,
    airlines: Optional[list[str]] = None,
) -> Dict[str, list[dict]]:
    cfg = _load_config()
    refresh_hours = int(cfg.get("refresh_hours") or DEFAULT_REFRESH_HOURS)
    failure_retry_minutes = int(cfg.get("failure_retry_minutes") or DEFAULT_FAILURE_RETRY_MINUTES)
    sources = cfg.get("sources") or {}
    target_airlines = _normalize_airline_list(airlines)

    cached = _read_cache() or {}
    cached_at = cached.get("cached_at")
    cached_at_by_airline = cached.get("cached_at_by_airline") or {}
    failed_at_by_airline = cached.get("failed_at_by_airline") or {}
    cached_inventory = cached.get("fleet_inventory_map") if isinstance(cached.get("fleet_inventory_map"), dict) else {}

    base_inventory = _merge_inventory_maps(DEFAULT_FLEET_INVENTORY, cached_inventory or {})
    all_targets = target_airlines or {str(a).upper() for a in sources.keys()} | set(base_inventory.keys())

    stale_airlines = []
    for airline_u in all_targets:
        airline_cached_at = cached_at_by_airline.get(airline_u) or cached_at
        needs_refresh = force_refresh or _needs_refresh(airline_cached_at, refresh_hours)
        cooling_down = _within_failure_cooldown(
            failed_at_by_airline.get(airline_u),
            failure_retry_minutes,
        )
        if needs_refresh and not cooling_down:
            stale_airlines.append(airline_u)

    if not stale_airlines:
        return base_inventory

    live_updates: Dict[str, list[dict]] = {}
    now_iso = datetime.now(timezone.utc).isoformat()
    for airline_u in stale_airlines:
        url = sources.get(airline_u) or sources.get(airline_u.lower()) or sources.get(airline_u.title())
        if not url:
            continue
        html = _download_html(str(url))
        if not html:
            failed_at_by_airline[airline_u] = now_iso
            continue
        if airline_u == "VQ":
            parsed = _parse_vq_inventory(html)
        elif airline_u == "BG":
            parsed = _parse_bg_inventory(html)
        else:
            parsed = []
        if parsed:
            live_updates[airline_u] = parsed
            cached_at_by_airline[airline_u] = now_iso
            failed_at_by_airline.pop(airline_u, None)
        else:
            failed_at_by_airline[airline_u] = now_iso

    merged_inventory = _merge_inventory_maps(base_inventory, live_updates)
    capacity_map = get_fleet_capacity_map(force_refresh=False, airlines=list(all_targets))
    _write_cache(
        {
            "cached_at": now_iso,
            "cached_at_by_airline": cached_at_by_airline,
            "failed_at_by_airline": failed_at_by_airline,
            "capacity_map": capacity_map,
            "fleet_inventory_map": merged_inventory,
            "sources": sources,
        }
    )
    return merged_inventory
