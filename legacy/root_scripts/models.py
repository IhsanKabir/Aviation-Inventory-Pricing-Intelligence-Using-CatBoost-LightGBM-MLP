from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, JSON, BigInteger
)
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class FlightOffer(Base):
    __tablename__ = "flight_offers"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    airline = Column(String, index=True)
    operating_airline = Column(String)
    flight_number = Column(String)
    operating_flight_number = Column(String)

    origin = Column(String, index=True)
    destination = Column(String, index=True)

    departure = Column(DateTime, index=True)
    arrival = Column(DateTime)

    cabin = Column(String)
    booking_class = Column(String)
    fare_basis = Column(String)

    brand = Column(String)
    soldout = Column(Boolean, default=False)

    equipment_code = Column(String)
    aircraft = Column(String)
    seat_capacity = Column(Integer)
    seats_remaining = Column(Integer)
    estimated_load_factor_pct = Column(Float)

    fare_amount = Column(Float)
    tax_amount = Column(Float)
    total_amount = Column(Float)
    currency = Column(String)

    tax_components = Column(JSON)
    baggage = Column(String)

    raw_offer = Column(JSON)

    scraped_at = Column(
        DateTime, default=datetime.utcnow, index=True
    )
