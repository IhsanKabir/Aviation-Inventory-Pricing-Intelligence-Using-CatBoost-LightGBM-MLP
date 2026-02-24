# models/flight_offer_raw_meta.py

from models.base import Base
from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, ForeignKey, Boolean, JSON
)
from sqlalchemy.orm import relationship


class FlightOfferRawMetaORM(Base):
    __tablename__ = "flight_offer_raw_meta"

    id = Column(Integer, primary_key=True)

    flight_offer_id = Column(
        Integer,
        ForeignKey("flight_offers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Raw / volatile fields
    currency = Column(String)
    fare_amount = Column(Numeric(10, 2))
    tax_amount = Column(Numeric(10, 2))
    baggage = Column(String)

    aircraft = Column(String)
    equipment_code = Column(String)
    duration_min = Column(Integer)
    stops = Column(Integer)
    arrival = Column(DateTime)

    estimated_load_factor_pct = Column(Numeric(5, 2))
    inventory_confidence = Column(String)

    booking_class = Column(String)
    soldout = Column(Boolean)
    adt_count = Column(Integer)
    chd_count = Column(Integer)
    inf_count = Column(Integer)
    probe_group_id = Column(String, index=True)

    departure_local = Column(DateTime)
    departure_utc = Column(DateTime)
    departure_tz_offset = Column(String)
    arrival_utc = Column(DateTime)
    arrival_tz_offset = Column(String)

    fare_ref_num = Column(String)
    fare_search_reference = Column(String)
    source_endpoint = Column(String)
    raw_offer_fingerprint = Column(String(64), index=True)
    raw_offer_storage = Column(String(32))

    raw_offer = Column(JSON)

    scraped_at = Column(DateTime, nullable=False)
