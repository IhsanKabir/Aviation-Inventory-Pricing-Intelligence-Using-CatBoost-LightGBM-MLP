# models/flight_offer_raw_meta.py

from models.base import Base
from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, ForeignKey, Boolean, JSON, Index
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
    via_airports = Column(String)
    arrival = Column(DateTime)

    estimated_load_factor_pct = Column(Numeric(5, 2))
    inventory_confidence = Column(String)

    booking_class = Column(String)
    soldout = Column(Boolean)
    adt_count = Column(Integer)
    chd_count = Column(Integer)
    inf_count = Column(Integer)
    probe_group_id = Column(String, index=True)
    search_trip_type = Column(String)
    trip_request_id = Column(String, index=True)
    requested_outbound_date = Column(String)
    requested_return_date = Column(String)
    trip_duration_days = Column(Integer)
    trip_origin = Column(String)
    trip_destination = Column(String)
    leg_direction = Column(String)
    leg_sequence = Column(Integer)
    itinerary_leg_count = Column(Integer)

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
    penalty_source = Column(String)
    penalty_currency = Column(String)
    penalty_rule_text = Column(String)
    fare_change_fee_before_24h = Column(Numeric(10, 2))
    fare_change_fee_within_24h = Column(Numeric(10, 2))
    fare_change_fee_no_show = Column(Numeric(10, 2))
    fare_cancel_fee_before_24h = Column(Numeric(10, 2))
    fare_cancel_fee_within_24h = Column(Numeric(10, 2))
    fare_cancel_fee_no_show = Column(Numeric(10, 2))
    fare_changeable = Column(Boolean)
    fare_refundable = Column(Boolean)

    raw_offer = Column(JSON)

    scraped_at = Column(DateTime, nullable=False)

    __table_args__ = (
        Index(
            "ix_flight_offer_raw_meta_offer_trip_filters",
            "flight_offer_id",
            "search_trip_type",
            "requested_return_date",
        ),
        Index(
            "ix_flight_offer_raw_meta_trip_scope_lookup",
            "search_trip_type",
            "trip_origin",
            "trip_destination",
            "requested_return_date",
            "flight_offer_id",
        ),
    )
