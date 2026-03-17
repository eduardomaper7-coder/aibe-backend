from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from . import repo as review_repo
from .import_repo import (
    create_import_batch,
    create_import_file,
    mark_import_batch_completed,
    mark_import_batch_failed,
    upsert_patient,
    add_patient_source,
    create_appointment,
    update_appointment,
    add_appointment_source,
    attach_review_request_to_appointment,
    load_patients_for_job,
    load_appointments_for_job,
)
from .import_normalizers import (
    normalize_name,
    choose_display_name,
    normalize_phone,
    normalize_date_str,
    normalize_time_str,
    build_appointment_at,
    detect_missing_fields,
    is_older_than_24h,
)
from .utils import compute_send_at


def _to_date_obj(date_str: str | None):
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _decide_status(
    *,
    missing_fields: list[str],
    issues: list[str],
    duplicate_exists: bool,
    too_old: bool,
) -> str:
    if "conflict_phone" in issues or "conflict_date" in issues or "conflict_time" in issues:
        return "conflict"
    if duplicate_exists:
        return "duplicate"
    if too_old:
        return "too_old"
    if missing_fields:
        return "incomplete"
    return "ready"


def import_appointments_payloads(
    db: Session,
    *,
    job_id: int,
    files_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    batch = create_import_batch(db, job_id=job_id, files_count=len(files_payload))

    summary = {
        "files_received": len(files_payload),
        "rows_extracted": 0,
        "patients_created": 0,
        "patients_updated": 0,
        "appointments_created": 0,
        "appointments_updated": 0,
        "patient_only_saved": 0,
        "scheduled_now": 0,
        "incomplete": 0,
        "duplicates": 0,
        "too_old": 0,
        "conflicts": 0,
    }

    items: list[dict[str, Any]] = []

    try:
        # -------------------------
        # Precarga en memoria
        # -------------------------
        existing_patients = load_patients_for_job(db, job_id=job_id)
        existing_appointments = load_appointments_for_job(db, job_id=job_id)
        existing_rrs = review_repo.load_existing_review_requests_for_job(db, job_id=job_id)

        patients_by_phone: dict[str, Any] = {}
        patients_by_name: dict[str, Any] = {}

        for p in existing_patients:
            if p.phone_e164:
                patients_by_phone[p.phone_e164] = p
            if p.normalized_name:
                patients_by_name[p.normalized_name] = p

        appointments_by_phone_key: dict[tuple[str, str, str], Any] = {}
        appointments_by_name_key: dict[tuple[str, str, str], Any] = {}
        incomplete_appointments_by_phone: dict[str, list[Any]] = {}
        incomplete_appointments_by_name: dict[str, list[Any]] = {}

        for a in existing_appointments:
            if a.phone_e164 and a.appointment_date and a.appointment_time:
                appointments_by_phone_key[
                    (a.phone_e164, a.appointment_date.isoformat(), a.appointment_time)
                ] = a

            if a.normalized_name and a.appointment_date and a.appointment_time:
                appointments_by_name_key[
                    (a.normalized_name, a.appointment_date.isoformat(), a.appointment_time)
                ] = a

            if a.phone_e164 and (a.appointment_date is None or a.appointment_time is None):
                incomplete_appointments_by_phone.setdefault(a.phone_e164, []).append(a)

            if a.normalized_name and (a.appointment_date is None or a.appointment_time is None):
                incomplete_appointments_by_name.setdefault(a.normalized_name, []).append(a)

        existing_rr_keys: set[tuple[str, str]] = set()
        for rr in existing_rrs:
            if rr.phone_e164 and rr.appointment_at:
                existing_rr_keys.add((rr.phone_e164, rr.appointment_at.isoformat()))

        # -------------------------
        # Procesamiento
        # -------------------------
        for file_payload in files_payload:
            file_row = create_import_file(
                db,
                batch_id=batch.id,
                original_filename=file_payload["original_filename"],
                mime_type=file_payload.get("mime_type"),
                file_hash=file_payload["file_hash"],
            )

            appointments = file_payload.get("appointments") or []
            summary["rows_extracted"] += len(appointments)

            for idx, raw in enumerate(appointments, start=1):

                raw_name = raw.get("name")
                raw_phone = raw.get("phone")
                raw_date = raw.get("date")
                raw_time = raw.get("time")
                timezone_str = raw.get("timezone") or "Europe/Madrid"
                confidence = raw.get("confidence") or 0.0
                raw_issues = list(raw.get("issues") or [])

                display_name = (raw_name or "").strip() or None
                normalized_name = normalize_name(raw_name)
                phone_e164 = normalize_phone(raw_phone)
                date_str = normalize_date_str(raw_date)
                time_str = normalize_time_str(raw_time)
                appointment_at = build_appointment_at(date_str, time_str, timezone_str)

                patient = None
                patient_state = None

                # -------------------------
                # Paciente
                # -------------------------
                if phone_e164:
                    patient = patients_by_phone.get(phone_e164)
                if not patient and normalized_name:
                    patient = patients_by_name.get(normalized_name)

                if not patient and (normalized_name or phone_e164):
                    patient, patient_state = upsert_patient(
                        db,
                        job_id=job_id,
                        display_name=display_name,
                        normalized_name=normalized_name,
                        phone_e164=phone_e164,
                    )

                    if patient_state == "created":
                        summary["patients_created"] += 1
                    elif patient_state == "updated":
                        summary["patients_updated"] += 1

                    if patient:
                        if patient.phone_e164:
                            patients_by_phone[patient.phone_e164] = patient
                        if patient.normalized_name:
                            patients_by_name[patient.normalized_name] = patient

                if not date_str and not time_str:
                    summary["patient_only_saved"] += 1
                    continue

                final_phone = phone_e164 or (patient.phone_e164 if patient else None)
                final_missing = detect_missing_fields(normalized_name, final_phone, date_str, time_str)
                appointment_date_obj = _to_date_obj(date_str)

                # -------------------------
                # Buscar cita existente
                # -------------------------
                appointment = None

                if final_phone and appointment_date_obj and time_str:
                    appointment = appointments_by_phone_key.get(
                        (final_phone, appointment_date_obj.isoformat(), time_str)
                    )

                if not appointment and normalized_name and appointment_date_obj and time_str:
                    appointment = appointments_by_name_key.get(
                        (normalized_name, appointment_date_obj.isoformat(), time_str)
                    )

                if not appointment and final_phone:
                    candidates = incomplete_appointments_by_phone.get(final_phone, [])
                    if len(candidates) == 1:
                        appointment = candidates[0]

                # -------------------------
                # FIX MERGE POR NOMBRE
                # -------------------------
                if (
                    not appointment
                    and normalized_name
                    and not appointment_date_obj
                    and not time_str
                ):
                    candidates = incomplete_appointments_by_name.get(normalized_name, [])
                    if len(candidates) == 1:
                        appointment = candidates[0]

                final_display_name = choose_display_name(
                    appointment.display_name if appointment else None,
                    patient.display_name if patient else display_name,
                ) or display_name

                too_old = is_older_than_24h(appointment_at)

                duplicate_exists = False
                if not final_missing and final_phone and appointment_at:
                    duplicate_exists = (final_phone, appointment_at.isoformat()) in existing_rr_keys

                status = _decide_status(
                    missing_fields=final_missing,
                    issues=raw_issues,
                    duplicate_exists=duplicate_exists,
                    too_old=too_old,
                )

                if appointment is None:
                    appointment = create_appointment(
                        db,
                        job_id=job_id,
                        patient_id=patient.id if patient else None,
                        display_name=final_display_name,
                        normalized_name=normalized_name,
                        phone_e164=final_phone,
                        appointment_date=appointment_date_obj,
                        appointment_time=time_str,
                        appointment_at=appointment_at,
                        timezone=timezone_str,
                        missing_fields=final_missing,
                        issues=raw_issues,
                        merge_confidence=confidence,
                        status=status,
                        is_duplicate=duplicate_exists,
                        is_too_old=too_old,
                    )
                    summary["appointments_created"] += 1

                else:
                    appointment = update_appointment(
                        db,
                        appointment=appointment,
                        patient_id=patient.id if patient else None,
                        display_name=final_display_name,
                        normalized_name=normalized_name,
                        phone_e164=final_phone,
                        appointment_date=appointment_date_obj,
                        appointment_time=time_str,
                        appointment_at=appointment_at,
                        timezone=timezone_str,
                        missing_fields=final_missing,
                        issues=raw_issues,
                        merge_confidence=confidence,
                        status=status,
                        is_duplicate=duplicate_exists,
                        is_too_old=too_old,
                    )
                    summary["appointments_updated"] += 1

                if appointment.status == "too_old":
                    summary["too_old"] += 1
                elif appointment.status == "conflict":
                    summary["conflicts"] += 1
                elif appointment.status == "incomplete":
                    summary["incomplete"] += 1
                elif appointment.status == "duplicate":
                    summary["duplicates"] += 1

        mark_import_batch_completed(db, batch_id=batch.id)
        db.commit()

        return {
            "batch_id": batch.id,
            "summary": summary,
            "items": items,
        }

    except Exception as e:
        db.rollback()
        mark_import_batch_failed(db, batch_id=batch.id, error_message=str(e))
        db.commit()
        raise