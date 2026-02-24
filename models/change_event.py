from sqlalchemy import Column, Integer, String, DateTime, Float, JSON
from models.base import Base

class ChangeEventORM(Base):
    __tablename__ = "change_events"

    id = Column(Integer, primary_key=True)
    domain = Column(String, index=True)
    change_type = Column(String)
    direction = Column(String)
    velocity = Column(String)
    magnitude = Column(Float)
    percent_change = Column(Float)

    airline = Column(String, index=True)
    flight_number = Column(String, nullable=True)
    origin = Column(String, index=True)
    destination = Column(String, index=True)
    cabin = Column(String)
    departure = Column(DateTime)

    from_timestamp = Column(DateTime)
    to_timestamp = Column(DateTime)

    meta = Column(JSON)
