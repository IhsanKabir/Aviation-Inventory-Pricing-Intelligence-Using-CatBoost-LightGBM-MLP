from __future__ import annotations

import hashlib
import json
from datetime import date
from datetime import timedelta
from typing import Any


TRIP_TYPE_ONE_WAY = "OW"
TRIP_TYPE_ROUND_TRIP = "RT"


def normalize_trip_type(value: Any) -> str:
    normalized = str(value or TRIP_TYPE_ONE_WAY).strip().upper().replace("-", "_")
    if normalized in {"RT", "ROUNDTRIP", "ROUND_TRIP", "ROUNDTRIP"}:
        return TRIP_TYPE_ROUND_TRIP
    return TRIP_TYPE_ONE_WAY


def normalize_iso_date(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return date.fromisoformat(raw).isoformat()


def expand_iso_date_range(start_raw: Any, end_raw: Any) -> list[str]:
    start = normalize_iso_date(start_raw)
    end = normalize_iso_date(end_raw)
    if not start or not end:
        return []
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    span = (end_date - start_date).days
    return [(start_date + timedelta(days=offset)).isoformat() for offset in range(span + 1)]


def build_trip_search_windows(
    *,
    outbound_dates: list[str],
    trip_type: str = TRIP_TYPE_ONE_WAY,
    return_dates: list[str] | None = None,
    return_offsets: list[int] | None = None,
) -> list[dict[str, str | None]]:
    normalized_trip_type = normalize_trip_type(trip_type)
    normalized_outbounds: list[str] = []
    seen_outbounds: set[str] = set()
    for value in outbound_dates:
        normalized = normalize_iso_date(value)
        if not normalized or normalized in seen_outbounds:
            continue
        seen_outbounds.add(normalized)
        normalized_outbounds.append(normalized)

    if normalized_trip_type == TRIP_TYPE_ONE_WAY:
        return [{"departure_date": outbound, "return_date": None} for outbound in normalized_outbounds]

    normalized_returns: list[str] = []
    seen_returns: set[str] = set()
    for value in return_dates or []:
        normalized = normalize_iso_date(value)
        if not normalized or normalized in seen_returns:
            continue
        seen_returns.add(normalized)
        normalized_returns.append(normalized)

    normalized_offsets: list[int] = []
    seen_offsets: set[int] = set()
    for value in return_offsets or []:
        offset = int(value)
        if offset < 0:
            raise ValueError("return_date offsets cannot be negative")
        if offset in seen_offsets:
            continue
        seen_offsets.add(offset)
        normalized_offsets.append(offset)

    if not normalized_returns and not normalized_offsets:
        raise ValueError("return_date is required for round-trip searches")

    windows: list[dict[str, str | None]] = []
    seen_windows: set[tuple[str, str]] = set()
    for outbound in normalized_outbounds:
        outbound_date = date.fromisoformat(outbound)
        candidate_returns: list[str] = []

        for inbound in normalized_returns:
            if inbound >= outbound and inbound not in candidate_returns:
                candidate_returns.append(inbound)

        for offset in normalized_offsets:
            inbound = (outbound_date + timedelta(days=offset)).isoformat()
            if inbound not in candidate_returns:
                candidate_returns.append(inbound)

        for inbound in candidate_returns:
            key = (outbound, inbound)
            if key in seen_windows:
                continue
            seen_windows.add(key)
            windows.append({"departure_date": outbound, "return_date": inbound})

    if not windows:
        raise ValueError("No valid round-trip search windows resolved")

    return windows


def build_trip_context(
    *,
    origin: str,
    destination: str,
    departure_date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
    trip_type: str = TRIP_TYPE_ONE_WAY,
    return_date: str | None = None,
) -> dict[str, Any]:
    normalized_trip_type = normalize_trip_type(trip_type)
    outbound_date = normalize_iso_date(departure_date)
    inbound_date = normalize_iso_date(return_date)
    if not outbound_date:
        raise ValueError("departure_date is required")
    if normalized_trip_type == TRIP_TYPE_ROUND_TRIP and not inbound_date:
        raise ValueError("return_date is required for round-trip searches")
    if normalized_trip_type == TRIP_TYPE_ONE_WAY:
        inbound_date = None

    trip_duration_days = None
    if outbound_date and inbound_date:
        duration = (date.fromisoformat(inbound_date) - date.fromisoformat(outbound_date)).days
        if duration < 0:
            raise ValueError("return_date cannot be earlier than departure_date")
        trip_duration_days = duration

    fingerprint_payload = {
        "trip_type": normalized_trip_type,
        "origin": str(origin or "").strip().upper(),
        "destination": str(destination or "").strip().upper(),
        "departure_date": outbound_date,
        "return_date": inbound_date,
        "cabin": str(cabin or "").strip(),
        "adt": max(1, int(adt or 1)),
        "chd": max(0, int(chd or 0)),
        "inf": max(0, int(inf or 0)),
    }
    trip_request_id = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:24]

    return {
        "trip_request_id": trip_request_id,
        "search_trip_type": normalized_trip_type,
        "requested_outbound_date": outbound_date,
        "requested_return_date": inbound_date,
        "trip_duration_days": trip_duration_days,
        "trip_origin": fingerprint_payload["origin"],
        "trip_destination": fingerprint_payload["destination"],
    }


def apply_trip_context(row: dict[str, Any], trip_context: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    for key, value in trip_context.items():
        enriched.setdefault(key, value)

    direction = str(enriched.get("leg_direction") or "").strip().lower()
    if direction in {"return"}:
        direction = "inbound"
    if not direction:
        direction = "outbound"
    enriched["leg_direction"] = direction

    if enriched.get("leg_sequence") is None:
        if direction == "outbound":
            enriched["leg_sequence"] = 1
        elif direction == "inbound":
            enriched["leg_sequence"] = 2

    if enriched.get("itinerary_leg_count") is None:
        enriched["itinerary_leg_count"] = 2 if trip_context.get("search_trip_type") == TRIP_TYPE_ROUND_TRIP else 1

    return enriched
