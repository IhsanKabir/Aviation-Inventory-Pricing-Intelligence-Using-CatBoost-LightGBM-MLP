from __future__ import annotations

import json
import logging
import re
from datetime import date
from datetime import timedelta
from pathlib import Path
from typing import Any

from core.trip_context import (
    TRIP_TYPE_ONE_WAY,
    build_trip_search_windows,
    expand_iso_date_range,
    normalize_iso_date,
    normalize_trip_type,
)
from core.market_priors import load_market_priors


def _parse_iso_date_list(values: list[Any]) -> list[str]:
    parsed: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_iso_date(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parsed.append(normalized)
    return parsed


def _drop_past_iso_dates(values: list[str], *, today: date) -> list[str]:
    kept: list[str] = []
    for value in values or []:
        normalized = normalize_iso_date(value)
        if not normalized:
            continue
        try:
            parsed = date.fromisoformat(normalized)
        except Exception:
            continue
        if parsed < today:
            continue
        if normalized not in kept:
            kept.append(normalized)
    return kept


def _has_future_iso_date(values: list[str], *, today: date) -> bool:
    for value in values or []:
        normalized = normalize_iso_date(value)
        if not normalized:
            continue
        try:
            parsed = date.fromisoformat(normalized)
        except Exception:
            continue
        if parsed > today:
            return True
    return False


def _ensure_at_least_one_future_iso_date(values: list[str], *, today: date) -> list[str]:
    normalized = _drop_past_iso_dates(values, today=today)
    if _has_future_iso_date(normalized, today=today):
        return normalized
    fallback = (today + timedelta(days=1)).isoformat()
    if fallback not in normalized:
        normalized.append(fallback)
    return normalized


def _ensure_weekday_coverage(values: list[str], *, today: date) -> list[str]:
    normalized = _drop_past_iso_dates(values, today=today)
    present_weekdays: set[int] = set()
    anchor_date = today
    for value in normalized:
        try:
            parsed = date.fromisoformat(value)
        except Exception:
            continue
        present_weekdays.add(parsed.weekday())
        if parsed > anchor_date:
            anchor_date = parsed

    additions: list[str] = []
    anchor_weekday = anchor_date.weekday()
    for weekday in range(7):
        if weekday in present_weekdays:
            continue
        delta = (weekday - anchor_weekday) % 7
        if delta == 0:
            delta = 7
        candidate = (anchor_date + timedelta(days=delta)).isoformat()
        if candidate not in normalized and candidate not in additions:
            additions.append(candidate)

    additions.sort()
    return normalized + additions


def _parse_offset_csv(raw: Any) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for part in str(raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        if not re.fullmatch(r"[-+]?\d+", token):
            continue
        value = int(token)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def _expand_offset_range(start_raw: Any, end_raw: Any) -> list[int]:
    if start_raw is None and end_raw is None:
        return []
    if start_raw is None or end_raw is None:
        return [int(start_raw if start_raw is not None else end_raw)]
    start = int(start_raw)
    end = int(end_raw)
    if end < start:
        start, end = end, start
    return list(range(start, end + 1))


def _extract_dates_from_obj(obj: Any, today: date) -> list[str]:
    if isinstance(obj, list):
        return _drop_past_iso_dates(_parse_iso_date_list(obj), today=today)

    if not isinstance(obj, dict):
        return []

    if isinstance(obj.get("dates"), list):
        parsed = _drop_past_iso_dates(_parse_iso_date_list(obj["dates"]), today=today)
        if parsed:
            return parsed
    if isinstance(obj.get("dates"), str):
        parsed = _drop_past_iso_dates(_parse_iso_date_list(str(obj["dates"]).split(",")), today=today)
        if parsed:
            return parsed

    if obj.get("date_start") and obj.get("date_end"):
        parsed = _drop_past_iso_dates(expand_iso_date_range(obj.get("date_start"), obj.get("date_end")), today=today)
        if parsed:
            return parsed
    elif obj.get("date_start") or obj.get("date_end"):
        parsed = _drop_past_iso_dates(
            _parse_iso_date_list([obj.get("date_start") or obj.get("date_end")]),
            today=today,
        )
        if parsed:
            return parsed

    if isinstance(obj.get("date_range"), dict):
        parsed = _drop_past_iso_dates(
            expand_iso_date_range(
                obj["date_range"].get("start") or obj["date_range"].get("date_start"),
                obj["date_range"].get("end") or obj["date_range"].get("date_end"),
            ),
            today=today,
        )
        if parsed:
            return parsed

    if isinstance(obj.get("date_ranges"), list):
        merged: list[str] = []
        for item in obj["date_ranges"]:
            if not isinstance(item, dict):
                continue
            parsed = _drop_past_iso_dates(
                expand_iso_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                ),
                today=today,
            )
            for value in parsed:
                if value not in merged:
                    merged.append(value)
        if merged:
            return merged

    offsets = obj.get("day_offsets")
    if isinstance(offsets, list):
        values: list[int] = []
        seen: set[int] = set()
        for raw in offsets:
            try:
                offset = int(raw)
            except Exception:
                continue
            if offset in seen:
                continue
            seen.add(offset)
            values.append(offset)
        if values:
            return [(today + timedelta(days=offset)).isoformat() for offset in values]
    elif isinstance(offsets, str):
        values = _parse_offset_csv(offsets)
        if values:
            return [(today + timedelta(days=offset)).isoformat() for offset in values]

    if obj.get("day_offset_start") is not None or obj.get("day_offset_end") is not None:
        values = _expand_offset_range(obj.get("day_offset_start"), obj.get("day_offset_end"))
        if values:
            return [(today + timedelta(days=offset)).isoformat() for offset in values]

    if isinstance(obj.get("day_offset_range"), dict):
        values = _expand_offset_range(
            obj["day_offset_range"].get("start"),
            obj["day_offset_range"].get("end"),
        )
        if values:
            return [(today + timedelta(days=offset)).isoformat() for offset in values]

    if isinstance(obj.get("day_offset_ranges"), list):
        merged: list[str] = []
        for item in obj["day_offset_ranges"]:
            if not isinstance(item, dict):
                continue
            values = _expand_offset_range(item.get("start"), item.get("end"))
            for offset in values:
                resolved = (today + timedelta(days=offset)).isoformat()
                if resolved not in merged:
                    merged.append(resolved)
        if merged:
            return merged

    return []


def _extract_return_selectors_from_obj(
    obj: Any,
    *,
    today: date,
    source_label: str,
    logger: logging.Logger | None = None,
) -> tuple[list[str], list[int]]:
    if not isinstance(obj, dict):
        return [], []

    return_dates: list[str] = []
    return_offsets: list[int] = []

    def _add_dates(values: list[str]) -> None:
        for value in values:
            if value and value not in return_dates:
                normalized = _drop_past_iso_dates([value], today=today)
                if normalized and normalized[0] not in return_dates:
                    return_dates.append(normalized[0])

    def _add_offsets(values: list[int]) -> None:
        for value in values:
            if value < 0:
                if logger:
                    logger.warning("Ignoring invalid negative return-day offset in %s: %s", source_label, value)
                continue
            if value not in return_offsets:
                return_offsets.append(value)

    def _safe_int_list(values: list[Any]) -> list[int]:
        parsed: list[int] = []
        for value in values:
            try:
                parsed.append(int(value))
            except Exception:
                continue
        return parsed

    if obj.get("return_date"):
        _add_dates(_parse_iso_date_list([obj.get("return_date")]))
    if isinstance(obj.get("return_dates"), list):
        _add_dates(_parse_iso_date_list(obj["return_dates"]))
    elif isinstance(obj.get("return_dates"), str):
        _add_dates(_parse_iso_date_list(str(obj.get("return_dates")).split(",")))

    if obj.get("return_date_start") and obj.get("return_date_end"):
        _add_dates(expand_iso_date_range(obj.get("return_date_start"), obj.get("return_date_end")))
    elif obj.get("return_date_start") or obj.get("return_date_end"):
        _add_dates(_parse_iso_date_list([obj.get("return_date_start") or obj.get("return_date_end")]))

    if isinstance(obj.get("return_date_range"), dict):
        _add_dates(
            expand_iso_date_range(
                obj["return_date_range"].get("start") or obj["return_date_range"].get("date_start"),
                obj["return_date_range"].get("end") or obj["return_date_range"].get("date_end"),
            )
        )

    if isinstance(obj.get("return_date_ranges"), list):
        for item in obj["return_date_ranges"]:
            if not isinstance(item, dict):
                continue
            _add_dates(
                expand_iso_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                )
            )

    if isinstance(obj.get("return_date_offsets"), list):
        _add_offsets(_safe_int_list(obj["return_date_offsets"]))
    elif isinstance(obj.get("return_date_offsets"), str):
        _add_offsets(_parse_offset_csv(obj.get("return_date_offsets")))

    if obj.get("return_date_offset_start") is not None or obj.get("return_date_offset_end") is not None:
        _add_offsets(_expand_offset_range(obj.get("return_date_offset_start"), obj.get("return_date_offset_end")))

    if isinstance(obj.get("return_date_offset_range"), dict):
        _add_offsets(
            _expand_offset_range(
                obj["return_date_offset_range"].get("start"),
                obj["return_date_offset_range"].get("end"),
            )
        )

    if isinstance(obj.get("return_date_offset_ranges"), list):
        for item in obj["return_date_offset_ranges"]:
            if not isinstance(item, dict):
                continue
            _add_offsets(_expand_offset_range(item.get("start"), item.get("end")))

    if isinstance(obj.get("return_day_offsets"), list):
        _add_offsets(_safe_int_list(obj["return_day_offsets"]))

    return return_dates, return_offsets


def _parse_route_endpoint_pair(item: dict[str, Any]) -> tuple[str | None, str | None]:
    origin = str(item.get("origin") or "").strip().upper() or None
    destination = str(item.get("destination") or "").strip().upper() or None
    if origin and destination:
        return origin, destination

    route_value = str(item.get("route") or item.get("route_code") or "").strip().upper()
    if not route_value:
        return None, None

    if "->" in route_value:
        parts = [part.strip() for part in route_value.split("->", 1)]
    elif "-" in route_value:
        parts = [part.strip() for part in route_value.split("-", 1)]
    else:
        return None, None

    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None, None
    return parts[0], parts[1]


def _normalize_market_trip_profile_names(item: dict[str, Any]) -> list[str | None]:
    raw_list = item.get("market_trip_profiles")
    names: list[str | None] = []
    if isinstance(raw_list, list):
        for value in raw_list:
            normalized = str(value or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)
    elif isinstance(raw_list, str):
        for part in raw_list.split(","):
            normalized = str(part or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)

    single = str(item.get("market_trip_profile") or "").strip()
    if single and single not in names:
        names.append(single)

    return names or [None]


def _normalize_active_market_trip_profile_names(item: dict[str, Any]) -> list[str] | None:
    explicit = False
    names: list[str] = []
    raw_list = item.get("active_market_trip_profiles")
    if isinstance(raw_list, list):
        explicit = True
        for value in raw_list:
            normalized = str(value or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)
    elif isinstance(raw_list, str):
        explicit = True
        for part in raw_list.split(","):
            normalized = str(part or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)

    single = str(item.get("active_market_trip_profile") or "").strip()
    if single:
        explicit = True
        if single not in names:
            names.append(single)

    return names if explicit else None


def _normalize_training_market_trip_profile_names(item: dict[str, Any]) -> list[str]:
    names: list[str] = []
    raw_list = item.get("training_market_trip_profiles")
    if isinstance(raw_list, list):
        for value in raw_list:
            normalized = str(value or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)
    elif isinstance(raw_list, str):
        for part in raw_list.split(","):
            normalized = str(part or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)

    single = str(item.get("training_market_trip_profile") or "").strip()
    if single and single not in names:
        names.append(single)

    return names


def _normalize_deep_market_trip_profile_names(item: dict[str, Any]) -> list[str]:
    names: list[str] = []
    raw_list = item.get("deep_market_trip_profiles")
    if isinstance(raw_list, list):
        for value in raw_list:
            normalized = str(value or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)
    elif isinstance(raw_list, str):
        for part in raw_list.split(","):
            normalized = str(part or "").strip()
            if normalized and normalized not in names:
                names.append(normalized)

    single = str(item.get("deep_market_trip_profile") or "").strip()
    if single and single not in names:
        names.append(single)

    return names


def _flatten_grouped_airlines(
    payload: dict[str, Any],
    *,
    logger: logging.Logger | None = None,
    path: Path,
) -> list[dict[str, Any]]:
    airlines_payload = payload.get("airlines")
    if not isinstance(airlines_payload, dict):
        return []

    flattened: list[dict[str, Any]] = []
    for airline_code, airline_block in airlines_payload.items():
        if not isinstance(airline_block, dict):
            continue
        if airline_block.get("enabled") is False:
            continue

        airline_defaults: dict[str, Any] = {}
        if airline_block.get("default_profile"):
            airline_defaults["profile"] = airline_block.get("default_profile")
        if airline_block.get("market_trip_profile"):
            airline_defaults["market_trip_profile"] = airline_block.get("market_trip_profile")
        if "market_trip_profiles" in airline_block:
            airline_defaults["market_trip_profiles"] = airline_block.get("market_trip_profiles")
        if "active_market_trip_profile" in airline_block:
            airline_defaults["active_market_trip_profile"] = airline_block.get("active_market_trip_profile")
        if "active_market_trip_profiles" in airline_block:
            airline_defaults["active_market_trip_profiles"] = airline_block.get("active_market_trip_profiles")
        if "training_market_trip_profile" in airline_block:
            airline_defaults["training_market_trip_profile"] = airline_block.get("training_market_trip_profile")
        if "training_market_trip_profiles" in airline_block:
            airline_defaults["training_market_trip_profiles"] = airline_block.get("training_market_trip_profiles")
        if "deep_market_trip_profile" in airline_block:
            airline_defaults["deep_market_trip_profile"] = airline_block.get("deep_market_trip_profile")
        if "deep_market_trip_profiles" in airline_block:
            airline_defaults["deep_market_trip_profiles"] = airline_block.get("deep_market_trip_profiles")
        if airline_block.get("trip_type") is not None:
            airline_defaults["trip_type"] = airline_block.get("trip_type")
        for key in (
            "dates",
            "date_start",
            "date_end",
            "date_range",
            "date_ranges",
            "day_offsets",
            "day_offset_start",
            "day_offset_end",
            "day_offset_range",
            "day_offset_ranges",
            "return_date",
            "return_dates",
            "return_date_start",
            "return_date_end",
            "return_date_range",
            "return_date_ranges",
            "return_date_offsets",
            "return_date_offset_start",
            "return_date_offset_end",
            "return_date_offset_range",
            "return_date_offset_ranges",
            "return_day_offsets",
        ):
            if key in airline_block:
                airline_defaults[key] = airline_block.get(key)

        routes_block = airline_block.get("routes")
        if not isinstance(routes_block, dict):
            if logger:
                logger.warning("Ignoring airline block %s.%s: routes must be an object", path, airline_code)
            continue

        for route_code, route_config in routes_block.items():
            if route_config is None:
                route_config = {}
            if not isinstance(route_config, dict):
                if logger:
                    logger.warning(
                        "Ignoring route config %s.airlines.%s.routes.%s: route value must be an object",
                        path,
                        airline_code,
                        route_code,
                    )
                continue
            if route_config.get("enabled") is False:
                continue

            item = {
                "airline": str(airline_code).strip().upper(),
                "route": str(route_code).strip().upper(),
            }
            item.update(airline_defaults)
            item.update(route_config)
            flattened.append(item)
    return flattened


def load_route_trip_overrides(
    path: Path,
    *,
    today: date,
    trip_plan_mode: str = "operational",
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        payload = json.loads(text)
    except Exception as exc:
        if logger:
            logger.warning("Failed to parse route trip config %s: %s", path, exc)
        return []

    profiles: dict[str, dict[str, Any]] = {}
    if isinstance(payload, dict) and isinstance(payload.get("profiles"), dict):
        for name, profile in payload["profiles"].items():
            if not isinstance(profile, dict):
                continue
            profiles[str(name)] = dict(profile)

    market_trip_profiles: dict[str, dict[str, Any]] = {}
    try:
        priors = load_market_priors()
        raw_market_profiles = priors.get("trip_date_profiles")
        if isinstance(raw_market_profiles, dict):
            for name, profile in raw_market_profiles.items():
                if isinstance(profile, dict):
                    market_trip_profiles[str(name)] = dict(profile)
    except Exception:
        market_trip_profiles = {}

    def _apply_market_trip_profile(
        item: dict[str, Any],
        *,
        market_profile_name: str | None,
        source_label: str,
    ) -> dict[str, Any] | None:
        if not market_profile_name:
            return dict(item)
        market_profile = market_trip_profiles.get(market_profile_name)
        if not market_profile:
            if logger:
                logger.warning(
                    "Ignoring unknown market trip profile '%s' in %s",
                    market_profile_name,
                    source_label,
                )
            return None
        merged = dict(market_profile)
        merged.update(item)
        return merged

    grouped_routes = []
    if isinstance(payload, dict):
        grouped_routes = _flatten_grouped_airlines(payload, logger=logger, path=path)

    if isinstance(payload, list):
        raw_routes = payload
    elif grouped_routes:
        raw_routes = grouped_routes
    elif isinstance(payload, dict) and isinstance(payload.get("routes"), list):
        raw_routes = payload["routes"]
    else:
        if logger:
            logger.warning("Ignoring unsupported route trip config shape in %s", path)
        return []

    overrides: list[dict[str, Any]] = []
    for index, item in enumerate(raw_routes):
        if not isinstance(item, dict):
            continue
        if item.get("enabled") is False:
            continue

        profile_name = str(item.get("profile") or "").strip()
        base_item = dict(item)
        if profile_name:
            profile = profiles.get(profile_name)
            if not profile:
                if logger:
                    logger.warning(
                        "Ignoring unknown trip profile '%s' in %s[%d]",
                        profile_name,
                        path,
                        index,
                    )
                continue
            base_item = dict(profile)
            base_item.update(item)

        market_profile_names = _normalize_market_trip_profile_names(base_item)
        active_market_profile_names = _normalize_active_market_trip_profile_names(base_item)
        training_market_profile_names = _normalize_training_market_trip_profile_names(base_item)
        deep_market_profile_names = _normalize_deep_market_trip_profile_names(base_item)
        mode = str(trip_plan_mode or "operational").strip().lower()
        if mode == "operational":
            if active_market_profile_names is not None:
                market_profile_names = [
                    name for name in market_profile_names
                    if name is None or name in active_market_profile_names
                ]
                if not market_profile_names and logger:
                    logger.info(
                        "Skipping route trip override %s[%d]: no active market trip profiles enabled",
                        path,
                        index,
                    )
                    continue
        elif mode == "training":
            if active_market_profile_names is not None:
                market_profile_names = [
                    name for name in market_profile_names
                    if name is None or name in active_market_profile_names
                ]
                if not market_profile_names and logger:
                    logger.info(
                        "Skipping route trip override %s[%d]: no active market trip profiles enabled",
                        path,
                        index,
                    )
                    continue
            for name in training_market_profile_names:
                if name not in market_profile_names:
                    market_profile_names.append(name)
        elif mode == "deep":
            for name in training_market_profile_names:
                if name not in market_profile_names:
                    market_profile_names.append(name)
            for name in deep_market_profile_names:
                if name not in market_profile_names:
                    market_profile_names.append(name)
        else:
            if logger:
                logger.warning(
                    "Unknown trip_plan_mode '%s'; falling back to operational activation behavior",
                    trip_plan_mode,
                )
            if active_market_profile_names is not None:
                market_profile_names = [
                    name for name in market_profile_names
                    if name is None or name in active_market_profile_names
                ]
                if not market_profile_names and logger:
                    logger.info(
                        "Skipping route trip override %s[%d]: no active market trip profiles enabled",
                        path,
                        index,
                    )
                    continue
        for market_profile_name in market_profile_names:
            effective_item = _apply_market_trip_profile(
                dict(base_item),
                market_profile_name=market_profile_name,
                source_label=f"{path}[{index}]",
            )
            if effective_item is None:
                continue

            origin, destination = _parse_route_endpoint_pair(effective_item)
            if not origin or not destination:
                if logger:
                    logger.warning(
                        "Ignoring route trip override %s[%d]: missing origin/destination or route",
                        path,
                        index,
                    )
                continue

            airline = str(effective_item.get("airline") or "").strip().upper() or None
            trip_type_raw = effective_item.get("trip_type")
            trip_type = normalize_trip_type(trip_type_raw) if trip_type_raw else None
            outbound_dates = _extract_dates_from_obj(effective_item, today=today)
            return_dates, return_offsets = _extract_return_selectors_from_obj(
                effective_item,
                today=today,
                source_label=f"{path}[{index}]",
                logger=logger,
            )

            source_suffix = f"#{market_profile_name}" if market_profile_name else ""
            overrides.append(
                {
                    "airline": airline,
                    "origin": origin,
                    "destination": destination,
                    "trip_type": trip_type,
                    "outbound_dates": outbound_dates,
                    "return_dates": return_dates,
                    "return_offsets": return_offsets,
                    "source": f"{path}[{index}]{source_suffix}",
                }
            )
    return overrides


def match_route_trip_overrides(
    overrides: list[dict[str, Any]],
    *,
    airline: str,
    origin: str,
    destination: str,
) -> list[dict[str, Any]]:
    airline_code = str(airline or "").strip().upper()
    origin_code = str(origin or "").strip().upper()
    destination_code = str(destination or "").strip().upper()

    specific_matches: list[dict[str, Any]] = []
    wildcard_matches: list[dict[str, Any]] = []
    for item in overrides:
        if item.get("origin") != origin_code or item.get("destination") != destination_code:
            continue
        item_airline = item.get("airline")
        if item_airline == airline_code:
            specific_matches.append(item)
        elif not item_airline:
            wildcard_matches.append(item)
    return specific_matches or wildcard_matches


def match_route_trip_override(
    overrides: list[dict[str, Any]],
    *,
    airline: str,
    origin: str,
    destination: str,
) -> dict[str, Any] | None:
    matches = match_route_trip_overrides(
        overrides,
        airline=airline,
        origin=origin,
        destination=destination,
    )
    return matches[0] if matches else None


def resolve_route_trip_plan(
    *,
    today: date,
    base_outbound_dates: list[str],
    base_trip_type: str,
    base_return_dates: list[str],
    base_return_offsets: list[int],
    route_override: dict[str, Any] | None,
    limit_dates: int | None = None,
) -> dict[str, Any]:
    trip_type = base_trip_type
    outbound_dates = list(base_outbound_dates)
    return_dates = list(base_return_dates)
    return_offsets = list(base_return_offsets)
    source = "global"

    if route_override:
        if route_override.get("trip_type"):
            trip_type = normalize_trip_type(route_override["trip_type"])
            source = str(route_override.get("source") or "route_override")
        if route_override.get("outbound_dates"):
            outbound_dates = list(route_override["outbound_dates"])
            source = str(route_override.get("source") or "route_override")
        if route_override.get("return_dates") or route_override.get("return_offsets"):
            return_dates = list(route_override.get("return_dates") or [])
            return_offsets = list(route_override.get("return_offsets") or [])
            source = str(route_override.get("source") or "route_override")

    if limit_dates and limit_dates > 0:
        outbound_dates = outbound_dates[:limit_dates]

    outbound_dates = _ensure_at_least_one_future_iso_date(outbound_dates, today=today)
    outbound_dates = _ensure_weekday_coverage(outbound_dates, today=today)

    if trip_type == TRIP_TYPE_ONE_WAY:
        return_dates = []
        return_offsets = []

    search_windows = build_trip_search_windows(
        outbound_dates=outbound_dates,
        trip_type=trip_type,
        return_dates=return_dates,
        return_offsets=return_offsets,
    )
    search_windows = [
        {
            "departure_date": window["departure_date"],
            "return_date": window["return_date"],
            "trip_type": trip_type,
        }
        for window in search_windows
    ]

    return {
        "trip_type": trip_type,
        "outbound_dates": outbound_dates,
        "return_dates": return_dates,
        "return_offsets": return_offsets,
        "search_windows": search_windows,
        "source": source,
    }
