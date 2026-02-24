from sqlalchemy import Column, Integer, String, DateTime, Float, JSON
from sqlalchemy.sql import func
from models.base import Base


class StrategySignal(Base):
    __tablename__ = "strategy_signals"

    id = Column(Integer, primary_key=True)

    airline = Column(String, index=True)
    flight_key = Column(String, index=True)

    signal_category = Column(String, index=True)
    signal_type = Column(String, index=True)

    confidence = Column(Float)          # 0.0 – 1.0
    severity = Column(Float)            # normalized magnitude

    detected_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    supporting_change_ids = Column(JSON)  # list of ChangeEvent IDs
    context = Column(JSON, nullable=True)
