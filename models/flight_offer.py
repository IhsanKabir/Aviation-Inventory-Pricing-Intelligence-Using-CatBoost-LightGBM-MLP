from models.base import Base
from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Numeric, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID
import uuid


class FlightOfferORM(Base):
    __tablename__ = "flight_offers"

    id = Column(Integer, primary_key=True)

    # Scrape identity
    scrape_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    scraped_at = Column(DateTime, nullable=False, index=True)

    # Flight identity
    airline = Column(String, nullable=False, index=True)          # airline code (BG)
    flight_number = Column(String, nullable=False, index=True)
    origin = Column(String, nullable=False, index=True)
    destination = Column(String, nullable=False, index=True)
    departure = Column(DateTime, nullable=False, index=True)
    cabin = Column(String, nullable=True)
    brand = Column(String, nullable=True)

    # Pricing
    price_total_bdt = Column(Numeric(10, 2), nullable=False)
    fare_basis = Column(String, nullable=True)    # RBD / booking class

    # Capacity & inventory
    seat_capacity = Column(Integer, nullable=True)
    seat_available = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "scrape_id",
            "airline",
            "origin",
            "destination",
            "flight_number",
            "departure",
            "cabin",
            "fare_basis",
            name="uq_flight_offer_snapshot"
        ),
        Index(
            "ix_flight_offers_scrape_route_cabin_departure",
            "scrape_id",
            "origin",
            "destination",
            "cabin",
            "departure",
        ),
        Index(
            "ix_flight_offers_scrape_airline_cabin_route",
            "scrape_id",
            "airline",
            "cabin",
            "origin",
            "destination",
        ),
        Index(
            "ix_flight_offers_route_departure",
            "origin",
            "destination",
            "departure",
        ),
    )
