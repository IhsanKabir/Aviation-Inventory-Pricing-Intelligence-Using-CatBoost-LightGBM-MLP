# modules/db/models.py
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Boolean, JSON, BigInteger,
    UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class FlightOffer(Base):
    """
    This stores each distinct offer row produced by modules/parser.
    Keep one row per scraped offer snapshot (use timestamped 'scraped_at').
    """
    __tablename__ = "flight_offers"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    scraped_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # canonical fields (flattened for easy analysis)
    airline = Column(String(8), index=True, nullable=False)
    operating_airline = Column(String(8), index=True)
    brand = Column(String(16), index=True)
    flight_number = Column(Integer, index=True)
    origin = Column(String(8), index=True, nullable=False)
    destination = Column(String(8), index=True, nullable=False)
    departure = Column(DateTime(timezone=True), index=True)
    arrival = Column(DateTime(timezone=True))
    duration_min = Column(Integer)
    stops = Column(Integer)
    cabin = Column(String(32), index=True)
    booking_class = Column(String(8), index=True)
    fare_basis = Column(String(32), index=True)

    fare_amount = Column(Integer)      # minor units or integer currency
    tax_amount = Column(Integer)
    total_amount = Column(Integer)
    currency = Column(String(6), index=True)

    seats_remaining = Column(Integer)
    seat_capacity = Column(Integer)
    estimated_load_factor_pct = Column(Float)

    baggage = Column(String(64))

    # raw JSON returned by airline module — keep for forensics and re-parsing.
    raw_offer = Column(JSONB)

    # helpful flags
    soldout = Column(Boolean, default=False)

    # unique constraint to avoid inserting the same exact offer snapshot repeatedly:
    __table_args__ = (
        Index("ix_offer_core", "airline", "origin", "destination", "departure", "flight_number", "cabin"),
    )


class TaxComponent(Base):
    """Separate table for tax breakdowns if you need details."""
    __tablename__ = "tax_components"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    offer_id = Column(BigInteger, index=True, nullable=False)
    amount = Column(Integer)
    currency = Column(String(6))
    description = Column(String(256))


class SourceRawResponse(Base):
    """Store raw responses per search (payload + response) for debugging / replay."""
    __tablename__ = "source_raw_response"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    airline = Column(String(8), index=True)
    origin = Column(String(8), index=True)
    destination = Column(String(8), index=True)
    departure_date = Column(Date, index=True)
    cabin = Column(String(32), index=True)
    scraped_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    request_payload = Column(JSONB)
    response_json = Column(JSONB)
    status_code = Column(Integer)
    note = Column(String(256))
