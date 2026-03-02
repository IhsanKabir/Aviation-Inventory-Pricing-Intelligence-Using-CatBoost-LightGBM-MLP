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
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_raw_offer_fingerprint ON flight_offer_raw_meta (raw_offer_fingerprint)",
        "CREATE INDEX IF NOT EXISTS ix_flight_offer_raw_meta_probe_group_id ON flight_offer_raw_meta (probe_group_id)",
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
            "arrival": r.get("arrival"),
            "estimated_load_factor_pct": r.get("estimated_load_factor_pct"),
            "inventory_confidence": r.get("inventory_confidence"),
            "booking_class": r.get("booking_class"),
            "soldout": r.get("soldout"),
            "adt_count": r.get("adt_count"),
            "chd_count": r.get("chd_count"),
            "inf_count": r.get("inf_count"),
            "probe_group_id": r.get("probe_group_id"),
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
