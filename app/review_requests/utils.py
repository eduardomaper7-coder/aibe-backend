# app/review_requests/utils.py
from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_send_at(appointment_at: datetime) -> datetime:
    """
    appointment_at: datetime con tz.
    Envía entre 15 y 30 min después.
    """
    if appointment_at.tzinfo is None:
        # asumimos UTC si viene naive
        appointment_at = appointment_at.replace(tzinfo=timezone.utc)
    delay_min = 15 + secrets.randbelow(16)  # 15..30
    return appointment_at + timedelta(minutes=delay_min)
