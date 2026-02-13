# app/review_requests/utils.py
from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_send_at(appointment_at: datetime) -> datetime:
    if appointment_at.tzinfo is None:
        appointment_at = appointment_at.replace(tzinfo=timezone.utc)
    return appointment_at + timedelta(minutes=60)
