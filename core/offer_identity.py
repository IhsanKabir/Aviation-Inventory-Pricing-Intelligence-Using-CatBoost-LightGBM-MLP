from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


IdentityKey = tuple[str, str, str, str, str, str, str, str]


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def flight_offer_identity_key(
    *,
    airline: str | None,
    origin: str | None,
    destination: str | None,
    departure: Any,
    flight_number: Any,
    cabin: str | None,
    fare_basis: str | None,
    brand: str | None,
) -> IdentityKey:
    dt = _parse_iso_datetime(departure)
    if dt is not None and dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    departure_key = dt.isoformat(sep="T", timespec="seconds") if dt is not None else str(departure or "")
    return (
        str(airline or "").upper(),
        str(origin or "").upper(),
        str(destination or "").upper(),
        departure_key,
        str(flight_number or ""),
        str(cabin or ""),
        str(fare_basis or ""),
        str(brand or ""),
    )


def flight_offer_identity_key_no_brand(key: IdentityKey) -> tuple[str, str, str, str, str, str, str]:
    return key[:-1]


def flight_offer_identity_key_no_fare_basis(key: IdentityKey) -> tuple[str, str, str, str, str, str, str]:
    return key[:6] + key[7:]


def flight_offer_identity_key_core(key: IdentityKey) -> tuple[str, str, str, str, str, str]:
    return key[:6]


def build_offer_id_lookup_maps(rows: list[Any]) -> dict[str, dict[tuple[str, ...], int]]:
    keyed_exact: dict[tuple[str, ...], int] = {}
    keyed_no_brand: dict[tuple[str, ...], int] = {}
    keyed_no_fare_basis: dict[tuple[str, ...], int] = {}
    keyed_core: dict[tuple[str, ...], int] = {}

    for row in rows:
        identity = flight_offer_identity_key(
            airline=getattr(row, "airline", None),
            origin=getattr(row, "origin", None),
            destination=getattr(row, "destination", None),
            departure=getattr(row, "departure", None),
            flight_number=getattr(row, "flight_number", None),
            cabin=getattr(row, "cabin", None),
            fare_basis=getattr(row, "fare_basis", None),
            brand=getattr(row, "brand", None),
        )
        row_id = int(getattr(row, "id"))
        keyed_exact[identity] = row_id

        no_brand = flight_offer_identity_key_no_brand(identity)
        if no_brand not in keyed_no_brand:
            keyed_no_brand[no_brand] = row_id

        no_fare_basis = flight_offer_identity_key_no_fare_basis(identity)
        if no_fare_basis not in keyed_no_fare_basis:
            keyed_no_fare_basis[no_fare_basis] = row_id

        core = flight_offer_identity_key_core(identity)
        if core not in keyed_core:
            keyed_core[core] = row_id

    return {
        "exact": keyed_exact,
        "no_brand": keyed_no_brand,
        "no_fare_basis": keyed_no_fare_basis,
        "core": keyed_core,
    }


def resolve_offer_id(
    identity: IdentityKey,
    lookup_maps: dict[str, dict[tuple[str, ...], int]],
) -> tuple[int | None, str | None]:
    match_sequence = (
        ("exact", identity),
        ("no_brand", flight_offer_identity_key_no_brand(identity)),
        ("no_fare_basis", flight_offer_identity_key_no_fare_basis(identity)),
        ("core", flight_offer_identity_key_core(identity)),
    )
    for mode, key in match_sequence:
        row_id = lookup_maps.get(mode, {}).get(key)
        if row_id is not None:
            return row_id, mode
    return None, None
