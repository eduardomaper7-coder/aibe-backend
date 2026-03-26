from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from .import_models import (
    ReviewImportBatch,
    ReviewImportFile,
    ReviewPatient,
    ReviewPatientSource,
    ReviewAppointment,
    ReviewAppointmentSource,
)
from .utils import utcnow


def create_import_batch(db: Session, *, job_id: int, files_count: int) -> ReviewImportBatch:
    row = ReviewImportBatch(
        job_id=job_id,
        status="processing",
        files_count=files_count,
    )
    db.add(row)
    db.flush()
    return row


def mark_import_batch_completed(
    db: Session,
    *,
    batch_id: int,
    manual_review_required: bool = False,
    manual_review_reason: Optional[str] = None,
) -> None:
    row = db.get(ReviewImportBatch, batch_id)
    if not row:
        return
    row.status = "completed"
    row.manual_review_required = manual_review_required
    row.manual_review_reason = manual_review_reason[:4000] if manual_review_reason else None
    row.updated_at = utcnow()
    db.flush()


def mark_import_batch_failed(db: Session, *, batch_id: int, error_message: str) -> None:
    row = db.get(ReviewImportBatch, batch_id)
    if not row:
        return
    row.status = "failed"
    row.error_message = error_message[:4000]
    row.updated_at = utcnow()
    db.flush()


def create_import_file(
    db: Session,
    *,
    batch_id: int,
    original_filename: str,
    mime_type: Optional[str],
    file_hash: str,
    storage_provider: Optional[str] = None,
    storage_bucket: Optional[str] = None,
    storage_key: Optional[str] = None,
    storage_url: Optional[str] = None,
    size_bytes: Optional[int] = None,
) -> ReviewImportFile:
    row = ReviewImportFile(
        batch_id=batch_id,
        original_filename=original_filename,
        mime_type=mime_type,
        file_hash=file_hash,
        storage_provider=storage_provider,
        storage_bucket=storage_bucket,
        storage_key=storage_key,
        storage_url=storage_url,
        size_bytes=size_bytes,
    )
    db.add(row)
    db.flush()
    return row

def find_patient_by_phone(db: Session, *, job_id: int, phone_e164: str) -> Optional[ReviewPatient]:
    stmt = (
        select(ReviewPatient)
        .where(
            and_(
                ReviewPatient.job_id == job_id,
                ReviewPatient.phone_e164 == phone_e164,
            )
        )
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def find_patient_by_name(db: Session, *, job_id: int, normalized_name: str) -> Optional[ReviewPatient]:
    stmt = (
        select(ReviewPatient)
        .where(
            and_(
                ReviewPatient.job_id == job_id,
                ReviewPatient.normalized_name == normalized_name,
            )
        )
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def upsert_patient(
    db: Session,
    *,
    job_id: int,
    display_name: Optional[str],
    normalized_name: Optional[str],
    phone_e164: Optional[str],
) -> tuple[Optional[ReviewPatient], str]:
    patient = None

    if phone_e164:
        patient = find_patient_by_phone(db, job_id=job_id, phone_e164=phone_e164)

    if not patient and normalized_name:
        patient = find_patient_by_name(db, job_id=job_id, normalized_name=normalized_name)

    if not patient:
        patient = ReviewPatient(
            job_id=job_id,
            display_name=display_name,
            normalized_name=normalized_name,
            phone_e164=phone_e164,
            last_seen_at=utcnow(),
        )
        db.add(patient)
        db.flush()
        return patient, "created"

    changed = False

    if display_name and (not patient.display_name or len(display_name) > len(patient.display_name or "")):
        patient.display_name = display_name
        changed = True

    if normalized_name and not patient.normalized_name:
        patient.normalized_name = normalized_name
        changed = True

    if phone_e164 and not patient.phone_e164:
        patient.phone_e164 = phone_e164
        changed = True

    patient.last_seen_at = utcnow()
    changed = True

    if changed:
        db.flush()
        return patient, "updated"

    return patient, "unchanged"


def add_patient_source(
    db: Session,
    *,
    patient_id: int,
    import_file_id: int,
    raw_name: Optional[str],
    raw_phone: Optional[str],
    confidence: Optional[float],
) -> None:
    row = ReviewPatientSource(
        patient_id=patient_id,
        import_file_id=import_file_id,
        raw_name=raw_name,
        raw_phone=raw_phone,
        confidence=confidence,
    )
    db.add(row)


def find_matching_appointment(
    db: Session,
    *,
    job_id: int,
    normalized_name: Optional[str],
    phone_e164: Optional[str],
    appointment_date: Optional[date],
    appointment_time: Optional[str],
) -> Optional[ReviewAppointment]:
    if phone_e164 and appointment_date and appointment_time:
        stmt = (
            select(ReviewAppointment)
            .where(
                and_(
                    ReviewAppointment.job_id == job_id,
                    ReviewAppointment.phone_e164 == phone_e164,
                    ReviewAppointment.appointment_date == appointment_date,
                    ReviewAppointment.appointment_time == appointment_time,
                )
            )
            .limit(1)
        )
        row = db.execute(stmt).scalar_one_or_none()
        if row:
            return row

    if normalized_name and appointment_date and appointment_time:
        stmt = (
            select(ReviewAppointment)
            .where(
                and_(
                    ReviewAppointment.job_id == job_id,
                    ReviewAppointment.normalized_name == normalized_name,
                    ReviewAppointment.appointment_date == appointment_date,
                    ReviewAppointment.appointment_time == appointment_time,
                )
            )
            .limit(1)
        )
        row = db.execute(stmt).scalar_one_or_none()
        if row:
            return row

    if phone_e164:
        stmt = (
            select(ReviewAppointment)
            .where(
                and_(
                    ReviewAppointment.job_id == job_id,
                    ReviewAppointment.phone_e164 == phone_e164,
                    or_(
                        ReviewAppointment.appointment_date.is_(None),
                        ReviewAppointment.appointment_time.is_(None),
                    ),
                )
            )
            .order_by(ReviewAppointment.updated_at.desc())
            .limit(2)
        )
        rows = list(db.execute(stmt).scalars().all())
        if len(rows) == 1:
            return rows[0]

    if normalized_name:
        stmt = (
            select(ReviewAppointment)
            .where(
                and_(
                    ReviewAppointment.job_id == job_id,
                    ReviewAppointment.normalized_name == normalized_name,
                    or_(
                        ReviewAppointment.appointment_date.is_(None),
                        ReviewAppointment.appointment_time.is_(None),
                    ),
                )
            )
            .order_by(ReviewAppointment.updated_at.desc())
            .limit(2)
        )
        rows = list(db.execute(stmt).scalars().all())
        if len(rows) == 1:
            return rows[0]

    return None


def create_appointment(
    db: Session,
    *,
    job_id: int,
    patient_id: Optional[int],
    display_name: Optional[str],
    normalized_name: Optional[str],
    phone_e164: Optional[str],
    appointment_date: Optional[date],
    appointment_time: Optional[str],
    appointment_at,
    timezone: Optional[str],
    missing_fields: list[str],
    issues: list[str],
    merge_confidence: Optional[float],
    status: str,
    is_duplicate: bool,
    is_too_old: bool,
) -> ReviewAppointment:
    row = ReviewAppointment(
        job_id=job_id,
        patient_id=patient_id,
        display_name=display_name,
        normalized_name=normalized_name,
        phone_e164=phone_e164,
        appointment_date=appointment_date,
        appointment_time=appointment_time,
        appointment_at=appointment_at,
        timezone=timezone,
        missing_fields_json=missing_fields,
        issues_json=issues,
        merge_confidence=merge_confidence,
        status=status,
        is_duplicate=is_duplicate,
        is_too_old=is_too_old,
    )
    db.add(row)
    db.flush()
    return row


def update_appointment(
    db: Session,
    *,
    appointment: ReviewAppointment,
    patient_id: Optional[int],
    display_name: Optional[str],
    normalized_name: Optional[str],
    phone_e164: Optional[str],
    appointment_date: Optional[date],
    appointment_time: Optional[str],
    appointment_at,
    timezone: Optional[str],
    missing_fields: list[str],
    issues: list[str],
    merge_confidence: Optional[float],
    status: str,
    is_duplicate: bool,
    is_too_old: bool,
) -> ReviewAppointment:
    merged_issues = list(appointment.issues_json or [])
    for issue in issues:
        if issue not in merged_issues:
            merged_issues.append(issue)

    if patient_id and not appointment.patient_id:
        appointment.patient_id = patient_id

    if display_name and (not appointment.display_name or len(display_name) > len(appointment.display_name or "")):
        appointment.display_name = display_name

    if normalized_name and not appointment.normalized_name:
        appointment.normalized_name = normalized_name

    if phone_e164:
        if appointment.phone_e164 and appointment.phone_e164 != phone_e164:
            if "conflict_phone" not in merged_issues:
                merged_issues.append("conflict_phone")
            status = "conflict"
        elif not appointment.phone_e164:
            appointment.phone_e164 = phone_e164

    if appointment_date:
        if appointment.appointment_date and appointment.appointment_date != appointment_date:
            if "conflict_date" not in merged_issues:
                merged_issues.append("conflict_date")
            status = "conflict"
        elif not appointment.appointment_date:
            appointment.appointment_date = appointment_date

    if appointment_time:
        if appointment.appointment_time and appointment.appointment_time != appointment_time:
            if "conflict_time" not in merged_issues:
                merged_issues.append("conflict_time")
            status = "conflict"
        elif not appointment.appointment_time:
            appointment.appointment_time = appointment_time

    if appointment_at and not appointment.appointment_at:
        appointment.appointment_at = appointment_at

    if timezone and not appointment.timezone:
        appointment.timezone = timezone

    appointment.missing_fields_json = missing_fields
    appointment.issues_json = merged_issues
    appointment.merge_confidence = merge_confidence
    appointment.status = status
    appointment.is_duplicate = is_duplicate
    appointment.is_too_old = is_too_old
    appointment.updated_at = utcnow()

    db.flush()
    return appointment


def add_appointment_source(
    db: Session,
    *,
    appointment_id: int,
    import_file_id: int,
    raw_name: Optional[str],
    raw_phone: Optional[str],
    raw_date: Optional[str],
    raw_time: Optional[str],
    confidence: Optional[float],
) -> None:
    row = ReviewAppointmentSource(
        appointment_id=appointment_id,
        import_file_id=import_file_id,
        raw_name=raw_name,
        raw_phone=raw_phone,
        raw_date=raw_date,
        raw_time=raw_time,
        confidence=confidence,
    )
    db.add(row)


def attach_review_request_to_appointment(
    db: Session,
    *,
    appointment: ReviewAppointment,
    review_request_id: int,
) -> None:
    appointment.review_request_id = review_request_id
    appointment.status = "scheduled"
    appointment.updated_at = utcnow()
    db.flush()

def load_patients_for_job(db: Session, *, job_id: int) -> list[ReviewPatient]:
    stmt = select(ReviewPatient).where(ReviewPatient.job_id == job_id)
    return list(db.execute(stmt).scalars().all())


def load_appointments_for_job(db: Session, *, job_id: int) -> list[ReviewAppointment]:
    stmt = select(ReviewAppointment).where(ReviewAppointment.job_id == job_id)
    return list(db.execute(stmt).scalars().all())