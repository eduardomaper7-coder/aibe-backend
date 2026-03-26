from __future__ import annotations

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    DateTime,
    Text,
    ForeignKey,
    Integer,
    Boolean,
    Date,
    Numeric,
    JSON,
)
from sqlalchemy.sql import func

from app.db import Base


class ReviewImportBatch(Base):
    __tablename__ = "review_import_batches"

    id = Column(BigInteger, primary_key=True, index=True)
    job_id = Column(BigInteger, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="processing")
    files_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    manual_review_required = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
    manual_review_reason = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

class ReviewImportFile(Base):
    __tablename__ = "review_import_files"

    id = Column(BigInteger, primary_key=True, index=True)
    batch_id = Column(BigInteger, ForeignKey("review_import_batches.id", ondelete="CASCADE"), nullable=False, index=True)

    original_filename = Column(String(255), nullable=False)
    mime_type = Column(String(120), nullable=True)
    file_hash = Column(String(128), nullable=False, index=True)

    storage_provider = Column(String(32), nullable=True)
    storage_bucket = Column(String(255), nullable=True)
    storage_key = Column(Text, nullable=True)
    storage_url = Column(Text, nullable=True)
    size_bytes = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class ReviewPatient(Base):
    __tablename__ = "review_patients"

    id = Column(BigInteger, primary_key=True, index=True)
    job_id = Column(BigInteger, nullable=False, index=True)

    display_name = Column(String(200), nullable=True)
    normalized_name = Column(String(200), nullable=True, index=True)
    phone_e164 = Column(String(32), nullable=True, index=True)

    last_seen_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ReviewPatientSource(Base):
    __tablename__ = "review_patient_sources"

    id = Column(BigInteger, primary_key=True, index=True)
    patient_id = Column(BigInteger, ForeignKey("review_patients.id", ondelete="CASCADE"), nullable=False, index=True)
    import_file_id = Column(BigInteger, ForeignKey("review_import_files.id", ondelete="CASCADE"), nullable=False)

    raw_name = Column(Text, nullable=True)
    raw_phone = Column(Text, nullable=True)
    confidence = Column(Numeric(4, 3), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ReviewAppointment(Base):
    __tablename__ = "review_appointments"

    id = Column(BigInteger, primary_key=True, index=True)
    job_id = Column(BigInteger, nullable=False, index=True)

    patient_id = Column(BigInteger, ForeignKey("review_patients.id", ondelete="SET NULL"), nullable=True, index=True)

    display_name = Column(String(200), nullable=True)
    normalized_name = Column(String(200), nullable=True, index=True)
    phone_e164 = Column(String(32), nullable=True, index=True)

    appointment_date = Column(Date, nullable=True)
    appointment_time = Column(String(5), nullable=True)
    appointment_at = Column(DateTime(timezone=True), nullable=True, index=True)
    timezone = Column(String(64), nullable=True)

    status = Column(String(32), nullable=False, default="incomplete")
    missing_fields_json = Column(JSON, nullable=False, default=list)
    issues_json = Column(JSON, nullable=False, default=list)

    merge_confidence = Column(Numeric(4, 3), nullable=True)

    is_duplicate = Column(Boolean, nullable=False, default=False)
    is_too_old = Column(Boolean, nullable=False, default=False)

    review_request_id = Column(BigInteger, ForeignKey("review_requests.id", ondelete="SET NULL"), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ReviewAppointmentSource(Base):
    __tablename__ = "review_appointment_sources"

    id = Column(BigInteger, primary_key=True, index=True)
    appointment_id = Column(BigInteger, ForeignKey("review_appointments.id", ondelete="CASCADE"), nullable=False, index=True)
    import_file_id = Column(BigInteger, ForeignKey("review_import_files.id", ondelete="CASCADE"), nullable=False)

    raw_name = Column(Text, nullable=True)
    raw_phone = Column(Text, nullable=True)
    raw_date = Column(Text, nullable=True)
    raw_time = Column(Text, nullable=True)
    confidence = Column(Numeric(4, 3), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)