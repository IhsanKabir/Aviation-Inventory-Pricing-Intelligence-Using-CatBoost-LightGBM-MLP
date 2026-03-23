from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from models import Base, FlightOfferORM, ChangeEventORM

from sqlalchemy.orm import Session
from datetime import datetime
import json
import os
from sqlalchemy.dialects.postgresql import insert
from models.flight_offer import FlightOfferORM
from models.flight_offer_raw_meta import FlightOfferRawMetaORM
from models.raw_offer_payload_store import RawOfferPayloadStoreORM
from core.runtime_config import get_database_url
from collections import Counter
import hashlib
import re



DATABASE_URL = get_database_url()

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_session():
    return SessionLocal()


def _ensure_schema_extensions():
    """
    Safe additive schema evolution for environments without formal migrations.
    """
    ddl = [
        # flight_offer_raw_meta extensions
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS inventory_confidence VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS adt_count INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS chd_count INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS inf_count INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS probe_group_id VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS search_trip_type VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS trip_request_id VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS requested_outbound_date VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS requested_return_date VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS trip_duration_days INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS trip_origin VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS trip_destination VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS leg_direction VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS leg_sequence INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS itinerary_leg_count INTEGER",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_local TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_utc TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_tz_offset VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS arrival_utc TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS arrival_tz_offset VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_ref_num VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_search_reference VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS source_endpoint VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS raw_offer_fingerprint VARCHAR(64)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS raw_offer_storage VARCHAR(32)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS penalty_source VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS penalty_currency VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS penalty_rule_text VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_change_fee_before_24h NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_change_fee_within_24h NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_change_fee_no_show NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_cancel_fee_before_24h NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_cancel_fee_within_24h NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_cancel_fee_no_show NUMERIC(10,2)",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_changeable BOOLEAN",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_refundable BOOLEAN",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS via_airports VARCHAR",
        "CREATE INDEX IF NOT EXISTS ix_flight_offers_scrape_route_cabin_departure ON flight_offers (scrape_id, origin, destination, cabin, departure)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offers_scrape_airline_cabin_route ON flight_offers (scrape_id, airline, cabin, origin, destination)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offers_route_departure ON flight_offers (origin, destination, departure)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_raw_offer_fingerprint ON flight_offer_raw_meta (raw_offer_fingerprint)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_probe_group_id ON flight_offer_raw_meta (probe_group_id)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_trip_request_id ON flight_offer_raw_meta (trip_request_id)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_offer_trip_filters ON flight_offer_raw_meta (flight_offer_id, search_trip_type, requested_return_date)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_trip_scope_lookup ON flight_offer_raw_meta (search_trip_type, trip_origin, trip_destination, requested_return_date, flight_offer_id)",
    ]
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))


def init_db(create_tables: bool = True):
    if create_tables:
        Base.metadata.create_all(bind=engine)
    _ensure_schema_extensions()

# def bulk_insert_offers(rows: list[dict]) -> int:
#     if not rows:
#         return 0
#
#     with SessionLocal() as session:
#         objs = [FlightOfferORM(**r) for r in rows]
#         session.bulk_save_objects(objs)
#         session.commit()
#         return len(objs)

import logging
LOG = logging.getLogger("db")

AIRPORT_CODE_RE = re.compile(r"^[A-Z]{3}$")


def _normalize_airport_code(value):
    code = str(value or "").strip().upper()
    return code if AIRPORT_CODE_RE.fullmatch(code) else None


def _iter_segment_like_nodes(node):
    if isinstance(node, dict):
        departure_candidates = [
            node.get("departureAirport"),
            node.get("origin"),
            node.get("from"),
            node.get("xFrom"),
            node.get("boardPoint"),
            node.get("departureAirportCode"),
            node.get("depAirport"),
        ]
        arrival_candidates = [
            node.get("arrivalAirport"),
            node.get("destination"),
            node.get("to"),
            node.get("xDest"),
            node.get("offPoint"),
            node.get("arrivalAirportCode"),
            node.get("arrAirport"),
        ]

        def _extract_code(candidate):
            if isinstance(candidate, dict):
                for key in ("code", "iata", "iataCode", "airportCode", "value"):
                    code = _normalize_airport_code(candidate.get(key))
                    if code:
                        return code
                return None
            return _normalize_airport_code(candidate)

        dep_code = next((code for code in (_extract_code(item) for item in departure_candidates) if code), None)
        arr_code = next((code for code in (_extract_code(item) for item in arrival_candidates) if code), None)
        if dep_code or arr_code:
            yield (dep_code, arr_code)

        for value in node.values():
            yield from _iter_segment_like_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_segment_like_nodes(item)


def _collect_named_via_codes(node, origin=None, destination=None):
    via_codes = []
    if isinstance(node, dict):
        for key in ("layoverAirports", "viaAirports", "transitAirports"):
            values = node.get(key)
            if isinstance(values, list):
                for item in values:
                    code = None
                    if isinstance(item, dict):
                        for nested_key in ("code", "iata", "iataCode", "airportCode", "value"):
                            code = _normalize_airport_code(item.get(nested_key))
                            if code:
                                break
                    else:
                        code = _normalize_airport_code(item)
                    if code and code != origin and code != destination:
                        via_codes.append(code)
        for value in node.values():
            via_codes.extend(_collect_named_via_codes(value, origin=origin, destination=destination))
    elif isinstance(node, list):
        for item in node:
            via_codes.extend(_collect_named_via_codes(item, origin=origin, destination=destination))
    return via_codes


def _infer_via_airports(row):
    explicit = row.get("via_airports")
    if explicit:
        tokens = [_normalize_airport_code(part) for part in str(explicit).replace(",", "|").split("|")]
        cleaned = [token for token in tokens if token]
        if cleaned:
            return "|".join(dict.fromkeys(cleaned))

    raw_offer = row.get("raw_offer")
    if not isinstance(raw_offer, (dict, list)):
        return None

    origin = _normalize_airport_code(row.get("origin"))
    destination = _normalize_airport_code(row.get("destination"))
    via_codes = []
    for dep_code, arr_code in _iter_segment_like_nodes(raw_offer):
        for code in (dep_code, arr_code):
            if not code or code == origin or code == destination:
                continue
            via_codes.append(code)

    via_codes.extend(_collect_named_via_codes(raw_offer, origin=origin, destination=destination))

    unique_codes = list(dict.fromkeys(via_codes))
    return "|".join(unique_codes) if unique_codes else None


def infer_via_airports(row):
    return _infer_via_airports(row)


def bulk_insert_offers(rows: list[dict]) -> int:
    if not rows:
        return 0

    stmt = insert(FlightOfferORM).values([
        {k: v for k, v in row.items() if k in FlightOfferORM.__table__.columns}
        for row in rows
    ])

    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_flight_offer_snapshot"
    )

    with SessionLocal() as session:
        result = session.execute(stmt)
        session.commit()
        return result.rowcount or 0


def save_change_events(events):
    if not events:
        return 0

    session = get_session()
    try:
        rows = []
        for e in events:
            rows.append(
                ChangeEventORM(
                    domain=getattr(e.domain, "value", e.domain),
                    change_type=getattr(e.change_type, "value", e.change_type),
                    direction=getattr(e.direction, "value", e.direction),
                    velocity=getattr(e.velocity, "value", e.velocity),

                    magnitude=e.magnitude,
                    percent_change=e.percent_change,

                    airline=e.airline,
                    flight_number=e.flight_number,
                    origin=e.origin,
                    destination=e.destination,
                    cabin=e.cabin,
                    departure=e.departure,

                    from_timestamp=e.from_timestamp,
                    to_timestamp=e.to_timestamp,

                    meta=e.metadata,
                )
            )
        session.bulk_save_objects(rows)
        session.commit()
        return len(rows)
    finally:
        session.close()


def save_column_change_events(events: list[dict]) -> int:
    if not events:
        return 0

    def _json_text(value):
        return json.dumps(value, ensure_ascii=False, default=str)

    insert_sql = text(
        """
        INSERT INTO airline_intel.column_change_events (
            scrape_id,
            previous_scrape_id,
            airline,
            departure_day,
            departure_time,
            origin,
            destination,
            flight_number,
            fare_basis,
            brand,
            cabin,
            domain,
            change_type,
            direction,
            field_name,
            old_value,
            new_value,
            magnitude,
            percent_change,
            event_meta
        )
        VALUES (
            :scrape_id,
            :previous_scrape_id,
            :airline,
            :departure_day,
            :departure_time,
            :origin,
            :destination,
            :flight_number,
            :fare_basis,
            :brand,
            :cabin,
            :domain,
            :change_type,
            :direction,
            :field_name,
            CAST(:old_value AS jsonb),
            CAST(:new_value AS jsonb),
            :magnitude,
            :percent_change,
            CAST(:event_meta AS jsonb)
        )
        """
    )

    payload = []
    for e in events:
        payload.append(
            {
                "scrape_id": e.get("scrape_id"),
                "previous_scrape_id": e.get("previous_scrape_id"),
                "airline": e.get("airline"),
                "departure_day": e.get("departure_day"),
                "departure_time": e.get("departure_time"),
                "origin": e.get("origin"),
                "destination": e.get("destination"),
                "flight_number": str(e.get("flight_number")) if e.get("flight_number") is not None else None,
                "fare_basis": e.get("fare_basis"),
                "brand": e.get("brand"),
                "cabin": e.get("cabin"),
                "domain": e.get("domain"),
                "change_type": e.get("change_type"),
                "direction": e.get("direction"),
                "field_name": e.get("field_name"),
                "old_value": _json_text(e.get("old_value")),
                "new_value": _json_text(e.get("new_value")),
                "magnitude": e.get("magnitude"),
                "percent_change": e.get("percent_change"),
                "event_meta": _json_text(e.get("event_meta", {})),
            }
        )

    session = get_session()
    try:
        session.execute(insert_sql, payload)
        session.commit()
        return len(payload)
    finally:
        session.close()


def bulk_insert_raw_meta(rows):
    if not rows:
        return 0

    dedupe_mode = os.getenv("RAW_META_PAYLOAD_DEDUPE_MODE", "externalize_duplicates").strip().lower()
    dedupe_enabled = dedupe_mode not in ("", "0", "false", "off", "disabled", "none")

    session = get_session()
    try:
        prepared_rows = [dict(r) for r in rows]
        payload_map: dict[str, dict] = {}
        payload_fingerprint_counts: Counter[str] = Counter()

        # Build canonical payload fingerprint map (payload only, not observation row fields),
        # so we can preserve observation history while deduplicating repeated raw payload JSON.
        for row in prepared_rows:
            raw_offer = row.get("raw_offer")
            if raw_offer is None:
                continue
            try:
                payload_text = json.dumps(raw_offer, sort_keys=True, ensure_ascii=False, default=str)
            except Exception:
                payload_text = json.dumps({"_raw_offer_repr": repr(raw_offer)}, sort_keys=True, ensure_ascii=False)
                raw_offer = {"_raw_offer_repr": repr(raw_offer)}
                row["raw_offer"] = raw_offer
            fp = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
            row["raw_offer_fingerprint"] = fp
            payload_fingerprint_counts[fp] += 1
            if fp not in payload_map:
                payload_map[fp] = {
                    "fingerprint": fp,
                    "payload_json": raw_offer,
                    "payload_size_bytes": len(payload_text.encode("utf-8")),
                    "seen_count": 0,
                }
        for fp, cnt in payload_fingerprint_counts.items():
            if fp in payload_map:
                payload_map[fp]["seen_count"] = int(cnt)

        existing_fingerprints: set[str] = set()
        if dedupe_enabled and payload_map:
            fps = list(payload_map.keys())
            for i in range(0, len(fps), 5000):
                batch = fps[i : i + 5000]
                q = text(
                    "SELECT fingerprint FROM raw_offer_payload_store WHERE fingerprint = ANY(CAST(:fps AS text[]))"
                )
                rows_existing = session.execute(q, {"fps": batch}).fetchall()
                existing_fingerprints.update(str(r[0]) for r in rows_existing if r and r[0])

            payload_rows = list(payload_map.values())
            stmt = insert(RawOfferPayloadStoreORM).values(payload_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["fingerprint"],
                set_={
                    "last_seen_at": text("NOW()"),
                    "seen_count": text("raw_offer_payload_store.seen_count + EXCLUDED.seen_count"),
                },
            )
            session.execute(stmt)

            for row in prepared_rows:
                fp = row.get("raw_offer_fingerprint")
                if not fp:
                    continue
                if dedupe_mode == "externalize_all":
                    row["raw_offer"] = None
                    row["raw_offer_storage"] = "external_ref"
                elif dedupe_mode == "externalize_duplicates":
                    if fp in existing_fingerprints or payload_fingerprint_counts.get(fp, 0) > 1:
                        row["raw_offer"] = None
                        row["raw_offer_storage"] = "external_ref"
                    else:
                        row["raw_offer_storage"] = "inline+ref"
                else:
                    row["raw_offer_storage"] = "inline_only"

        session.bulk_insert_mappings(
            FlightOfferRawMetaORM,
            prepared_rows,
        )
        session.commit()
        return len(prepared_rows)
    finally:
        session.close()

def save_raw_response_meta(
    session: Session,
    airline: str,
    origin: str,
    destination: str,
    flight_date: str,
    cabin: str,
    raw_payload: dict,
):
    """
    Persist raw response metadata for debugging, replay, and audits.
    Soft-fail by design.
    """
    try:
        session.execute(
            """
            INSERT INTO raw_response_meta (
                airline,
                origin,
                destination,
                flight_date,
                cabin,
                payload,
                captured_at
            )
            VALUES (
                :airline,
                :origin,
                :destination,
                :flight_date,
                :cabin,
                :payload,
                :captured_at
            )
            """,
            {
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "flight_date": flight_date,
                "cabin": cabin,
                "payload": json.dumps(raw_payload),
                "captured_at": datetime.utcnow(),
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        # deliberately silent (observability > reliability here)

# def upsert_flight_offers(rows: list[dict]) -> int:
#     """
#     Upserts flight offers into flight_offers table.
#     Returns number of affected rows.
#     """
#     if not rows:
#         return 0
#
#     session = get_session()
#     try:
#         stmt = insert(FlightOfferORM).values(rows)
#         flight_key = f"{airline}|{origin}|{destination}|{flight_date}|{cabin}"
#
#         FlightOfferORM(
#             flight_key=flight_key,
#             airline=airline,
#             origin=origin,
#             destination=destination,
#             ...
#         )
#
#         conflict_cols = [
#             "airline",
#             "origin",
#             "destination",
#             "departure_date",
#             "cabin",
#             "flight_number",
#         ]
#
#         update_cols = {
#             "price": stmt.excluded.price,
#             "seats_available": stmt.excluded.seats_available,
#             "currency": stmt.excluded.currency,
#             "scraped_at": stmt.excluded.scraped_at,
#         }
#
#         stmt = stmt.on_conflict_do_update(
#             index_elements=conflict_cols,
#             set_=update_cols,
#         )
#
#         result = session.execute(stmt)
#         session.commit()
#         return result.rowcount or 0
#
#     except Exception:
#         session.rollback()
#         raise
#
#     finally:
#         session.close()

def normalize_for_db(rows, scraped_at, scrape_id):
    """
    Normalize parsed rows into CORE flight_offers rows only.
    Raw-meta is handled separately by design.
    """
    if not rows:
        return []

    allowed_columns = {
        c.name for c in FlightOfferORM.__table__.columns
    }

    core_rows = []
    for r in rows:
        clean = {k: v for k, v in r.items() if k in allowed_columns}
        clean["scraped_at"] = scraped_at
        clean["scrape_id"] = scrape_id
        core_rows.append(clean)

    return core_rows
def normalize_raw_meta(rows, scraped_at):
    """
    Normalize parsed rows into RAW META rows.
    flight_offer_id is attached AFTER core insert.
    """
    if not rows:
        return []

    raw_meta_rows = []
    for r in rows:
        raw_meta_rows.append({
            "currency": r.get("currency"),
            "fare_amount": r.get("fare_amount"),
            "tax_amount": r.get("tax_amount"),
            "baggage": r.get("baggage"),
            "aircraft": r.get("aircraft"),
            "equipment_code": r.get("equipment_code"),
            "duration_min": r.get("duration_min"),
            "stops": r.get("stops"),
            "via_airports": _infer_via_airports(r),
            "arrival": r.get("arrival"),
            "estimated_load_factor_pct": r.get("estimated_load_factor_pct"),
            "inventory_confidence": r.get("inventory_confidence"),
            "booking_class": r.get("booking_class"),
            "soldout": r.get("soldout"),
            "adt_count": r.get("adt_count"),
            "chd_count": r.get("chd_count"),
            "inf_count": r.get("inf_count"),
            "probe_group_id": r.get("probe_group_id"),
            "search_trip_type": r.get("search_trip_type"),
            "trip_request_id": r.get("trip_request_id"),
            "requested_outbound_date": r.get("requested_outbound_date"),
            "requested_return_date": r.get("requested_return_date"),
            "trip_duration_days": r.get("trip_duration_days"),
            "trip_origin": r.get("trip_origin"),
            "trip_destination": r.get("trip_destination"),
            "leg_direction": r.get("leg_direction"),
            "leg_sequence": r.get("leg_sequence"),
            "itinerary_leg_count": r.get("itinerary_leg_count"),
            "departure_local": r.get("departure_local"),
            "departure_utc": r.get("departure_utc"),
            "departure_tz_offset": r.get("departure_tz_offset"),
            "arrival_utc": r.get("arrival_utc"),
            "arrival_tz_offset": r.get("arrival_tz_offset"),
            "fare_ref_num": r.get("fare_ref_num"),
            "fare_search_reference": r.get("fare_search_reference"),
            "source_endpoint": r.get("source_endpoint"),
            "penalty_source": r.get("penalty_source"),
            "penalty_currency": r.get("penalty_currency"),
            "penalty_rule_text": r.get("penalty_rule_text"),
            "fare_change_fee_before_24h": r.get("fare_change_fee_before_24h"),
            "fare_change_fee_within_24h": r.get("fare_change_fee_within_24h"),
            "fare_change_fee_no_show": r.get("fare_change_fee_no_show"),
            "fare_cancel_fee_before_24h": r.get("fare_cancel_fee_before_24h"),
            "fare_cancel_fee_within_24h": r.get("fare_cancel_fee_within_24h"),
            "fare_cancel_fee_no_show": r.get("fare_cancel_fee_no_show"),
            "fare_changeable": r.get("fare_changeable"),
            "fare_refundable": r.get("fare_refundable"),
            "raw_offer": r.get("raw_offer"),
            "scraped_at": scraped_at,
        })

    return raw_meta_rows
