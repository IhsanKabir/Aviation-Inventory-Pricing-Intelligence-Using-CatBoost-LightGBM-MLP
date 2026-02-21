# modules/db/db.py
import os
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from core.runtime_config import get_database_url

from .models import Base, FlightOffer, TaxComponent, SourceRawResponse

LOG = logging.getLogger("db")

# read database URL from env var, fallback to local dev (no embedded password)
DATABASE_URL = get_database_url()

# create engine (tune pool_size, max_overflow for production)
engine = create_engine(DATABASE_URL, echo=False, pool_size=10, max_overflow=20)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def safe_get(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default


def init_db(create_tables: bool = True):
    if create_tables:
        Base.metadata.create_all(bind=engine)
        LOG.info("DB tables created or verified.")


@contextmanager
def get_session():
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def _normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map parser rows to model fields. Keep robust to missing keys.
    """
    def safe(k, default=None):
        return r.get(k, default)

    return {
        "airline": safe("airline"),
        "operating_airline": safe("operating_airline"),
        "brand": safe("brand"),
        "flight_number": safe("flight_number"),
        "origin": safe("origin"),
        "destination": safe("destination"),
        "departure": safe("departure"),
        "arrival": safe("arrival"),
        "duration_min": safe("duration_min"),
        "stops": safe("stops"),
        "cabin": safe("cabin"),
        "booking_class": safe("booking_class"),
        "fare_basis": safe("fare_basis"),
        "fare_amount": safe("fare_amount"),
        "tax_amount": safe("tax_amount"),
        "total_amount": safe("total_amount"),
        "currency": safe("currency"),
        "seats_remaining": safe("seats_remaining"),
        "seat_capacity": safe("seat_capacity"),
        "estimated_load_factor_pct": safe("estimated_load_factor_pct"),
        "baggage": safe("baggage"),
        "raw_offer": safe("raw_offer") or {},
        "soldout": safe("soldout", False),
    }
def normalize_for_db(rows):
    normalized = []

    for r in rows:
        if not r:
            continue

        normalized.append({
            "airline": r.get("airline"),
            "origin": r.get("origin"),
            "destination": r.get("destination"),
            "departure": r.get("departure"),
            "cabin": r.get("cabin"),
            "price": r.get("price"),
            "currency": r.get("currency"),

            # SAFE nested access
            "brand": safe_get(r.get("brand"), "code"),
            "fare_family": safe_get(r.get("fare_family"), "name"),

            # always set
            "scraped_at": r.get("scraped_at"),
        })

    return normalized


def bulk_insert_offers(rows: List[Dict[str, Any]], save_raw_response: Optional[Dict[str, Any]] = None):
    """
    Bulk insert rows. Do simple insert (no upsert dedupe by default).
    If you need dedupe/upsert, implement logic by unique key.
    """
    if not rows:
        LOG.debug("bulk_insert_offers called with empty rows.")
        return 0

    normalized = [_normalize_row(r) for r in rows]

    with get_session() as s:
        try:
            s.bulk_insert_mappings(FlightOffer, normalized)
            # if tax components are included, insert them too
            # assume each row may have tax_components list
            tax_mappings = []
            for r, norm in zip(rows, normalized):
                tc = r.get("tax_components") or r.get("tax_components_raw") or []
                offer_id_placeholder = None  # we don't have id until commit; skip relational TC for now
                if tc:
                    for comp in tc:
                        tax_mappings.append({
                            "offer_id": None,  # left as None if you intend to post-link later
                            "amount": comp.get("amount"),
                            "currency": comp.get("currency"),
                            "description": comp.get("desc") or comp.get("description")
                        })
            if tax_mappings:
                s.bulk_insert_mappings(TaxComponent, tax_mappings)
            LOG.info("Inserted %d offer rows", len(normalized))
            return len(normalized)
        except SQLAlchemyError as e:
            LOG.exception("DB insert failed: %s", e)
            raise


def save_raw_response_meta(airline: str, origin: str, destination: str, departure_date: str, cabin: str,
                           request_payload: dict, response_json: dict, status_code: int = None, note: str = None):
    with get_session() as s:
        try:
            s.add(SourceRawResponse(
                airline=airline, origin=origin, destination=destination,
                departure_date=departure_date, cabin=cabin,
                request_payload=request_payload,
                response_json=response_json,
                status_code=status_code,
                note=note
            ))
        except Exception:
            LOG.exception("Failed to save source raw response meta.")


def upsert_offer_by_key(unique_keys: Dict[str, Any], payload: Dict[str, Any]):
    """
    Example upsert: use the postgres ON CONFLICT DO UPDATE.
    unique_keys = {"airline":.., "origin":.., "destination":.., "departure":.., "flight_number":.., "cabin":..}
    payload = normalized row
    """
    with get_session() as s:
        try:
            stmt = pg_insert(FlightOffer).values(**payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=["airline", "origin", "destination", "departure", "flight_number", "cabin"],
                set_={k: stmt.excluded[k] for k in payload.keys() if k not in ("id",)}
            )
            res = s.execute(stmt)
            LOG.debug("Upsert executed, rowcount=%s", res.rowcount)
            return res.rowcount
        except Exception:
            LOG.exception("Upsert failed.")
            raise
