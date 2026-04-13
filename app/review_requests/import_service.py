from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session
from .import_models import ReviewAppointment
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
MANUAL_REVIEW_USER_MESSAGE = (
    "Tu archivo se ha recibido correctamente. En menos de 24 horas, "
    "uno de nuestros especialistas configurará el flujo adecuado para tu negocio."
)

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

    ready_appointment_ids: list[int] = []
    item_index_by_appointment_id: dict[int, int] = {}

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

        pending_rr_keys = set(existing_rr_keys)

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
                storage_provider=file_payload.get("storage_provider"),
                storage_bucket=file_payload.get("storage_bucket"),
                storage_key=file_payload.get("storage_key"),
                storage_url=file_payload.get("storage_url"),
                size_bytes=file_payload.get("size_bytes"),
            )

            appointments = file_payload.get("appointments") or []
            summary["rows_extracted"] += len(appointments)

            for idx, raw in enumerate(appointments, start=1):
                if idx % 50 == 0:
                    print(f"⏳ procesadas {idx}/{len(appointments)} filas")

                if idx <= 20:
                    print("RAW IMPORT ROW:", raw)

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

                if idx <= 20:
                    print("NORMALIZED IMPORT ROW:", {
                        "display_name": display_name,
                        "normalized_name": normalized_name,
                        "raw_phone": raw_phone,
                        "phone_e164": phone_e164,
                        "raw_date": raw_date,
                        "date_str": date_str,
                        "raw_time": raw_time,
                        "time_str": time_str,
                        "timezone": timezone_str,
                        "appointment_at": appointment_at.isoformat() if appointment_at else None,
                        "issues": raw_issues,
                    })

                patient = None
                patient_state = None

                # -------------------------
                # Paciente en memoria
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

                elif patient:
                    patient, patient_state = upsert_patient(
                        db,
                        job_id=job_id,
                        display_name=display_name,
                        normalized_name=normalized_name,
                        phone_e164=phone_e164,
                    )
                    if patient_state == "updated":
                        summary["patients_updated"] += 1

                    if patient:
                        if patient.phone_e164:
                            patients_by_phone[patient.phone_e164] = patient
                        if patient.normalized_name:
                            patients_by_name[patient.normalized_name] = patient

                if patient:
                    add_patient_source(
                        db,
                        patient_id=patient.id,
                        import_file_id=file_row.id,
                        raw_name=raw_name,
                        raw_phone=raw_phone,
                        confidence=confidence,
                    )

                # -------------------------
                # Solo paciente
                # -------------------------
                if not date_str and not time_str:
                    summary["patient_only_saved"] += 1
                    items.append({
                        "kind": "patient",
                        "patient_id": patient.id if patient else None,
                        "appointment_id": None,
                        "review_request_id": None,
                        "customer_name": patient.display_name if patient else display_name,
                        "phone_e164": patient.phone_e164 if patient else phone_e164,
                        "appointment_date": None,
                        "appointment_time": None,
                        "status": "patient_saved",
                        "missing_fields": ["date", "time"],
                        "issues": raw_issues,
                    })
                    continue

                final_phone = phone_e164 or (patient.phone_e164 if patient else None)
                final_missing = detect_missing_fields(normalized_name, final_phone, date_str, time_str)
                appointment_date_obj = _to_date_obj(date_str)

                # -------------------------
                # Cita en memoria
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
                    rr_key = (final_phone, appointment_at.isoformat())
                    duplicate_exists = rr_key in pending_rr_keys

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

                if appointment.phone_e164 and appointment.appointment_date and appointment.appointment_time:
                    appointments_by_phone_key[
                        (appointment.phone_e164, appointment.appointment_date.isoformat(), appointment.appointment_time)
                    ] = appointment

                if appointment.normalized_name and appointment.appointment_date and appointment.appointment_time:
                    appointments_by_name_key[
                        (appointment.normalized_name, appointment.appointment_date.isoformat(), appointment.appointment_time)
                    ] = appointment

                if appointment.phone_e164 and (appointment.appointment_date is None or appointment.appointment_time is None):
                    lst = incomplete_appointments_by_phone.setdefault(appointment.phone_e164, [])
                    if appointment not in lst:
                        lst.append(appointment)

                if appointment.normalized_name and (appointment.appointment_date is None or appointment.appointment_time is None):
                    lst = incomplete_appointments_by_name.setdefault(appointment.normalized_name, [])
                    if appointment not in lst:
                        lst.append(appointment)

                add_appointment_source(
                    db,
                    appointment_id=appointment.id,
                    import_file_id=file_row.id,
                    raw_name=raw_name,
                    raw_phone=raw_phone,
                    raw_date=str(raw_date) if raw_date is not None else None,
                    raw_time=str(raw_time) if raw_time is not None else None,
                    confidence=confidence,
                )

                if (
                    appointment.status == "ready"
                    and appointment.review_request_id is None
                    and appointment.phone_e164
                    and appointment.appointment_at
                ):
                    key = (appointment.phone_e164, appointment.appointment_at.isoformat())
                    if key not in pending_rr_keys:
                        ready_appointment_ids.append(appointment.id)
                        pending_rr_keys.add(key)

                if appointment.status == "incomplete":
                    summary["incomplete"] += 1
                elif appointment.status == "duplicate":
                    summary["duplicates"] += 1
                elif appointment.status == "too_old":
                    summary["too_old"] += 1
                elif appointment.status == "conflict":
                    summary["conflicts"] += 1

                items.append({
                    "kind": "appointment",
                    "patient_id": appointment.patient_id,
                    "appointment_id": appointment.id,
                    "review_request_id": appointment.review_request_id,
                    "customer_name": appointment.display_name,
                    "phone_e164": appointment.phone_e164,
                    "appointment_date": appointment.appointment_date.isoformat() if appointment.appointment_date else None,
                    "appointment_time": appointment.appointment_time,
                    "status": appointment.status,
                    "missing_fields": list(appointment.missing_fields_json or []),
                    "issues": list(appointment.issues_json or []),
                })
                item_index_by_appointment_id[appointment.id] = len(items) - 1

                if idx % 200 == 0:
                    db.flush()

        manual_review_required = False
        manual_review_reason = None

        ready_candidates = len(ready_appointment_ids)

        if summary["rows_extracted"] == 0:
            manual_review_required = True
            manual_review_reason = "No se detectaron registros utilizables"
        elif ready_candidates == 0 and summary["rows_extracted"] > 0:
            manual_review_required = True
            manual_review_reason = "No se obtuvo ninguna cita programable"
        elif (
            summary["rows_extracted"] > 0
            and ready_candidates == 0
            and (summary["incomplete"] / summary["rows_extracted"]) >= 0.5
        ):
            manual_review_required = True
            manual_review_reason = "Más del 50% de los registros están incompletos y no hay ninguna cita programable"

        if not manual_review_required:
            for appointment_id in ready_appointment_ids:
                appointment = db.get(ReviewAppointment, appointment_id)
                if not appointment:
                    continue

                if (
                    appointment.status == "ready"
                    and appointment.review_request_id is None
                    and appointment.phone_e164
                    and appointment.appointment_at
                ):
                    rr = review_repo.create_review_request(
                        db,
                        job_id=job_id,
                        customer_name=appointment.display_name or "Paciente",
                        phone_e164=appointment.phone_e164,
                        appointment_at=appointment.appointment_at,
                        send_at=compute_send_at(appointment.appointment_at),
                    )
                    attach_review_request_to_appointment(
                        db,
                        appointment=appointment,
                        review_request_id=rr.id,
                    )
                    summary["scheduled_now"] += 1

                    idx = item_index_by_appointment_id.get(appointment.id)
                    if idx is not None:
                        items[idx]["status"] = "scheduled"
                        items[idx]["review_request_id"] = rr.id

        mark_import_batch_completed(
            db,
            batch_id=batch.id,
            manual_review_required=manual_review_required,
            manual_review_reason=manual_review_reason,
        )
        db.commit()
        
        print("SUMMARY:", summary)
        print("MANUAL REVIEW:", manual_review_required, manual_review_reason)
        print("ITEMS SAMPLE:", items[:5])

        return {
            "batch_id": batch.id,
            "summary": summary,
            "items": items,
            "manual_review_required": manual_review_required,
            "manual_review_reason": manual_review_reason,
            "user_message": MANUAL_REVIEW_USER_MESSAGE if manual_review_required else None,
        }

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass

        try:
            mark_import_batch_failed(db, batch_id=batch.id, error_message=str(e))
            db.commit()
        except Exception:
            pass

        raise