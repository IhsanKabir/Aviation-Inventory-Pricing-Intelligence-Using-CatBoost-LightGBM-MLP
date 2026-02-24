from models.base import Base
from sqlalchemy import Column, String, DateTime, Integer, JSON
from sqlalchemy.sql import func


class RawOfferPayloadStoreORM(Base):
    __tablename__ = "raw_offer_payload_store"

    # sha256 hex digest of canonical raw_offer payload JSON
    fingerprint = Column(String(64), primary_key=True)

    payload_json = Column(JSON, nullable=False)
    payload_size_bytes = Column(Integer)

    first_seen_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_seen_at = Column(DateTime, server_default=func.now(), nullable=False)
    seen_count = Column(Integer, nullable=False, server_default="1")
