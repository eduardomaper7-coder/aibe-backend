from __future__ import annotations

from typing import Optional, Literal
from pydantic import BaseModel


ImportStatus = Literal[
    "patient_saved",
    "incomplete",
    "ready",
    "scheduled",
    "duplicate",
    "too_old",
    "conflict",
]


class ImportItemOut(BaseModel):
    kind: Literal["patient", "appointment"]

    patient_id: Optional[int] = None
    appointment_id: Optional[int] = None
    review_request_id: Optional[int] = None

    customer_name: Optional[str] = None
    phone_e164: Optional[str] = None
    appointment_date: Optional[str] = None
    appointment_time: Optional[str] = None

    status: ImportStatus
    missing_fields: list[str] = []
    issues: list[str] = []


class ImportSummaryOut(BaseModel):
    files_received: int
    rows_extracted: int
    patients_created: int
    patients_updated: int
    appointments_created: int
    appointments_updated: int
    patient_only_saved: int
    scheduled_now: int
    incomplete: int
    duplicates: int
    too_old: int
    conflicts: int


class ImportBatchOut(BaseModel):
    batch_id: int
    summary: ImportSummaryOut
    items: list[ImportItemOut]

    manual_review_required: bool = False
    manual_review_reason: Optional[str] = None
    user_message: Optional[str] = None