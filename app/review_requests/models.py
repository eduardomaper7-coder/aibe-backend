import enum
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Text,
    Enum,
    Index,
    Boolean,
    UniqueConstraint,
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
    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "phone_e164",
            "appointment_at",
            name="uq_review_request_job_phone_appointment",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, nullable=False, index=True)

    customer_name = Column(String(200), nullable=False)
    phone_e164 = Column(String(32), nullable=False, index=True)

    appointment_at = Column(DateTime(timezone=True), nullable=False)
    send_at = Column(DateTime(timezone=True), nullable=False, index=True)

    status = Column(
        Enum(ReviewRequestStatus),
        nullable=False,
        default=ReviewRequestStatus.scheduled,
    )

    sent_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


Index(
    "ix_review_requests_job_send_status",
    ReviewRequest.job_id,
    ReviewRequest.send_at,
    ReviewRequest.status,
)


class BusinessSettings(Base):
    __tablename__ = "business_settings"
    __table_args__ = {"extend_existing": True}

    job_id = Column(Integer, primary_key=True)

    google_place_id = Column(String(128), nullable=True)
    google_review_url = Column(Text, nullable=True)
    business_name = Column(String(200), nullable=True)

    prevent_duplicate_whatsapp = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    # 👇 NUEVO
    whatsapp_provider = Column(
        String(50),
        nullable=False,
        default="twilio",
        server_default="twilio",
    )

    whatsapp_personal_number = Column(String(32), nullable=True)

    whatsapp_personal_enabled = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    whatsapp_session_status = Column(String(50), nullable=True)

    whatsapp_last_error = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )