from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _route_key(origin: str, destination: str) -> str:
    return f"{origin.upper()}-{destination.upper()}"


def _build_route_universe(routes_payload: list[dict]) -> set[tuple[str, str]]:
    universe: set[tuple[str, str]] = set()
    for row in routes_payload:
        airline = str(row.get("airline", "")).upper().strip()
        origin = str(row.get("origin", "")).upper().strip()
        destination = str(row.get("destination", "")).upper().strip()
        if not airline or not origin or not destination:
            continue
        universe.add((airline, _route_key(origin, destination)))
    return universe


def _normalize_profile_names(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    names: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                names.append(item.strip())
    return names


def _validate_profile_list(
    *,
    owner: str,
    field_name: str,
    names: list[str],
    known_profiles: set[str],
    warnings: list[str],
    errors: list[str],
) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            warnings.append(f"{owner}: duplicate profile '{name}' in {field_name}")
            continue
        seen.add(name)
        if name not in known_profiles:
            errors.append(f"{owner}: unknown profile '{name}' referenced in {field_name}")


def _validate_active_profile_membership(
    *,
    owner: str,
    market_names: list[str],
    active_names: list[str],
    errors: list[str],
) -> None:
    if not market_names or not active_names:
        return
    missing = [name for name in active_names if name not in market_names]
    for name in missing:
        errors.append(
            f"{owner}: active profile '{name}' must also be present in market_trip_profiles "
            f"because active_market_trip_profiles only filters the candidate set"
        )


def validate_trip_config(
    *,
    route_trip_payload: dict,
    market_priors_payload: dict,
    routes_payload: list[dict],
) -> tuple[list[str], list[str], dict]:
    warnings: list[str] = []
    errors: list[str] = []

    route_universe = _build_route_universe(routes_payload)
    local_profiles = set(route_trip_payload.get("profiles", {}).keys())
    market_profiles = set(market_priors_payload.get("trip_date_profiles", {}).keys())
    known_profiles = local_profiles | market_profiles

    airlines_block = route_trip_payload.get("airlines", {})
    configured_route_count = 0
    missing_from_trip_config: set[tuple[str, str]] = set(route_universe)

    for airline_code, airline_payload in airlines_block.items():
        airline = str(airline_code).upper().strip()
        if not airline:
            errors.append("airlines block contains an empty airline key")
            continue

        owner_prefix = f"{airline}/*"
        default_profile = airline_payload.get("default_profile")
        if default_profile:
            if default_profile not in local_profiles:
                errors.append(f"{owner_prefix}: unknown local default_profile '{default_profile}'")

        for field_name in (
            "market_trip_profile",
            "market_trip_profiles",
            "active_market_trip_profile",
            "active_market_trip_profiles",
            "training_market_trip_profile",
            "training_market_trip_profiles",
            "deep_market_trip_profile",
            "deep_market_trip_profiles",
        ):
            names = _normalize_profile_names(airline_payload.get(field_name))
            _validate_profile_list(
                owner=owner_prefix,
                field_name=field_name,
                names=names,
                known_profiles=known_profiles,
                warnings=warnings,
                errors=errors,
            )
        _validate_active_profile_membership(
            owner=owner_prefix,
            market_names=_normalize_profile_names(
                airline_payload.get("market_trip_profiles") or airline_payload.get("market_trip_profile")
            ),
            active_names=_normalize_profile_names(
                airline_payload.get("active_market_trip_profiles") or airline_payload.get("active_market_trip_profile")
            ),
            errors=errors,
        )

        routes_map = airline_payload.get("routes", {})
        if not isinstance(routes_map, dict):
            errors.append(f"{owner_prefix}: routes must be an object")
            continue

        for route_key, route_payload in routes_map.items():
            route_key_norm = str(route_key).upper().strip()
            owner = f"{airline}:{route_key_norm}"
            configured_route_count += 1

            if (airline, route_key_norm) not in route_universe:
                errors.append(f"{owner}: route not found in config/routes.json")
            else:
                missing_from_trip_config.discard((airline, route_key_norm))

            profile = route_payload.get("profile")
            if profile and profile not in local_profiles:
                errors.append(f"{owner}: unknown local profile '{profile}'")

            for field_name in (
                "market_trip_profile",
                "market_trip_profiles",
                "active_market_trip_profile",
                "active_market_trip_profiles",
                "training_market_trip_profile",
                "training_market_trip_profiles",
                "deep_market_trip_profile",
                "deep_market_trip_profiles",
            ):
                names = _normalize_profile_names(route_payload.get(field_name))
                _validate_profile_list(
                    owner=owner,
                    field_name=field_name,
                    names=names,
                    known_profiles=known_profiles,
                    warnings=warnings,
                    errors=errors,
                )
            _validate_active_profile_membership(
                owner=owner,
                market_names=_normalize_profile_names(
                    route_payload.get("market_trip_profiles") or route_payload.get("market_trip_profile")
                ),
                active_names=_normalize_profile_names(
                    route_payload.get("active_market_trip_profiles") or route_payload.get("active_market_trip_profile")
                ),
                errors=errors,
            )

    for airline, route_key in sorted(missing_from_trip_config):
        warnings.append(
            f"{airline}:{route_key}: present in config/routes.json but missing from config/route_trip_windows.json"
        )

    summary = {
        "route_universe_count": len(route_universe),
        "configured_route_count": configured_route_count,
        "local_profile_count": len(local_profiles),
        "market_profile_count": len(market_profiles),
        "warning_count": len(warnings),
        "error_count": len(errors),
    }
    return warnings, errors, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate trip config/profile wiring.")
    parser.add_argument(
        "--route-trip-config",
        default="config/route_trip_windows.json",
        help="Path to route trip windows config",
    )
    parser.add_argument(
        "--market-priors",
        default="config/market_priors.json",
        help="Path to market priors config",
    )
    parser.add_argument(
        "--routes-config",
        default="config/routes.json",
        help="Path to route universe config",
    )
    args = parser.parse_args()

    route_trip_path = Path(args.route_trip_config)
    market_priors_path = Path(args.market_priors)
    routes_path = Path(args.routes_config)

    warnings, errors, summary = validate_trip_config(
        route_trip_payload=_load_json(route_trip_path),
        market_priors_payload=_load_json(market_priors_path),
        routes_payload=_load_json(routes_path),
    )

    print("Trip config validation summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    if warnings:
        print("\nWarnings:")
        for item in warnings:
            print(f"  - {item}")

    if errors:
        print("\nErrors:")
        for item in errors:
            print(f"  - {item}")
        return 1

    print("\nValidation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
