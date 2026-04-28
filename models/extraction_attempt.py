from datetime import datetime

from models.base import Base
from sqlalchemy import Boolean, Column, DateTime, Float, Index, Integer, JSON, String


class ExtractionAttemptORM(Base):
    __tablename__ = "extraction_attempts"

    id = Column(Integer, primary_key=True)
    scrape_id = Column(String, index=True, nullable=False)
    cycle_id = Column(String, index=True)
    query_key = Column(String, index=True)

    airline = Column(String, index=True, nullable=False)
    module_name = Column(String, index=True)
    source_family = Column(String, index=True)
    final_source = Column(String)
    fallback_used = Column(Boolean, default=False)

    origin = Column(String, index=True)
    destination = Column(String, index=True)
    departure_date = Column(String, index=True)
    return_date = Column(String)
    trip_type = Column(String)
    cabin = Column(String)
    adt_count = Column(Integer)
    chd_count = Column(Integer)
    inf_count = Column(Integer)

    ok = Column(Boolean, default=False)
    row_count = Column(Integer, default=0)
    inserted_core_count = Column(Integer, default=0)
    inserted_raw_meta_count = Column(Integer, default=0)
    raw_meta_matched = Column(Integer, default=0)
    raw_meta_unmatched = Column(Integer, default=0)
    raw_meta_match_modes = Column(JSON)
    elapsed_sec = Column(Float)

    error_class = Column(String, index=True)
    no_rows_reason = Column(String)
    manual_action_required = Column(Boolean, index=True, default=False)
    retry_recommended = Column(Boolean, default=False)

    capture_state = Column(JSON)
    session_state = Column(JSON)
    source_attempts = Column(JSON)
    meta = Column(JSON)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_extraction_attempts_scrape_airline", "scrape_id", "airline"),
        Index(
            "ix_extraction_attempts_route_window",
            "airline",
            "origin",
            "destination",
            "departure_date",
            "cabin",
        ),
        Index("ix_extraction_attempts_gate", "scrape_id", "error_class", "manual_action_required"),
    )
