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
from core.runtime_config import get_database_url



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
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_local TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_utc TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS departure_tz_offset VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS arrival_utc TIMESTAMP",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS arrival_tz_offset VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_ref_num VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS fare_search_reference VARCHAR",
        "ALTER TABLE IF EXISTS flight_offer_raw_meta ADD COLUMN IF NOT EXISTS source_endpoint VARCHAR",
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

    session = get_session()
    try:
        session.bulk_insert_mappings(
            FlightOfferRawMetaORM,
            rows,
        )
        session.commit()
        return len(rows)
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
            "arrival": r.get("arrival"),
            "estimated_load_factor_pct": r.get("estimated_load_factor_pct"),
            "inventory_confidence": r.get("inventory_confidence"),
            "booking_class": r.get("booking_class"),
            "soldout": r.get("soldout"),
            "adt_count": r.get("adt_count"),
            "chd_count": r.get("chd_count"),
            "inf_count": r.get("inf_count"),
            "departure_local": r.get("departure_local"),
            "departure_utc": r.get("departure_utc"),
            "departure_tz_offset": r.get("departure_tz_offset"),
            "arrival_utc": r.get("arrival_utc"),
            "arrival_tz_offset": r.get("arrival_tz_offset"),
            "fare_ref_num": r.get("fare_ref_num"),
            "fare_search_reference": r.get("fare_search_reference"),
            "source_endpoint": r.get("source_endpoint"),
            "raw_offer": r.get("raw_offer"),
            "scraped_at": scraped_at,
        })

    return raw_meta_rows
