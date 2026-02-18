# app/review_requests/models.py
import enum
from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Enum, Index
)
from sqlalchemy.sql import func

from app.db import Base


class ReviewRequestStatus(str, enum.Enum):
    scheduled = "scheduled"
    sent = "sent"
    cancelled = "cancelled"
    failed = "failed"


class ReviewRequest(Base):
    __tablename__ = "review_requests"

    id = Column(Integer, primary_key=True, index=True)

    job_id = Column(Integer, nullable=False, index=True)

    customer_name = Column(String(200), nullable=False)
    phone_e164 = Column(String(32), nullable=False, index=True)

    appointment_at = Column(DateTime(timezone=True), nullable=False)
    send_at = Column(DateTime(timezone=True), nullable=False, index=True)

    status = Column(Enum(ReviewRequestStatus), nullable=False, default=ReviewRequestStatus.scheduled)

    sent_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)

    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


Index("ix_review_requests_job_send_status", ReviewRequest.job_id, ReviewRequest.send_at, ReviewRequest.status)


class BusinessSettings(Base):
    __tablename__ = "business_settings"

    job_id = Column(Integer, primary_key=True)

    # ✅ NUEVO: Place ID para generar la URL automáticamente
    # Ejemplo: ChIJrYaFfMAgQg0RwUizaSyFE80
    google_place_id = Column(String(128), nullable=True)

    # Ejemplo: https://search.google.com/local/writereview?placeid=XXXX
    google_review_url = Column(Text, nullable=True)

    business_name = Column(String(200), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
