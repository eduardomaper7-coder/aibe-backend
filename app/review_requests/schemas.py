# app/review_requests/schemas.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from .models import ReviewRequestStatus  # ✅ IMPORTANTE: usa el Enum real


class ReviewRequestCreate(BaseModel):
    job_id: int
    customer_name: str = Field(min_length=1, max_length=200)
    phone_e164: str = Field(min_length=8, max_length=32)
    appointment_at: datetime

    @field_validator("phone_e164")
    @classmethod
    def validate_phone(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("+"):
            raise ValueError("El teléfono debe estar en formato E.164, por ejemplo +34699111222")
        digits = v[1:].replace(" ", "")
        if not digits.isdigit():
            raise ValueError("El teléfono E.164 solo debe contener dígitos tras el +")
        if len(digits) < 8 or len(digits) > 15:
            raise ValueError("Longitud E.164 inválida (8-15 dígitos)")
        return v


class ReviewRequestOut(BaseModel):
    id: int
    job_id: int
    customer_name: str
    phone_e164: str
    appointment_at: datetime
    send_at: datetime

    # ✅ FIX: antes era Literal[...] y fallaba al recibir Enum
    status: ReviewRequestStatus

    sent_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class ReviewRequestListOut(BaseModel):
    items: list[ReviewRequestOut]


class CancelOut(BaseModel):
    ok: bool
    status: str




class BusinessSettingsOut(BaseModel):
    job_id: int
    google_place_id: Optional[str] = None
    google_review_url: Optional[str] = None
    business_name: Optional[str] = None

    class Config:
        from_attributes = True


class BusinessSettingsUpsert(BaseModel):
    job_id: int
    google_place_id: Optional[str] = None
    google_review_url: Optional[str] = None
    business_name: Optional[str] = None

