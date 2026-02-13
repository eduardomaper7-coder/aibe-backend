# app/review_requests/repo.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from .models import ReviewRequest, ReviewRequestStatus, BusinessSettings
from .utils import utcnow


def create_review_request(
    db: Session,
    *,
    job_id: int,
    customer_name: str,
    phone_e164: str,
    appointment_at: datetime,
    send_at: datetime,
) -> ReviewRequest:
    rr = ReviewRequest(
        job_id=job_id,
        customer_name=customer_name,
        phone_e164=phone_e164,
        appointment_at=appointment_at,
        send_at=send_at,
        status=ReviewRequestStatus.scheduled,
    )
    db.add(rr)
    db.commit()
    db.refresh(rr)
    return rr


def list_review_requests(db: Session, *, job_id: int, limit: int = 200) -> list[ReviewRequest]:
    stmt = (
        select(ReviewRequest)
        .where(ReviewRequest.job_id == job_id)
        .order_by(ReviewRequest.appointment_at.desc())
        .limit(limit)
    )
    return list(db.execute(stmt).scalars().all())


def cancel_review_request(db: Session, *, request_id: int) -> Optional[ReviewRequest]:
    rr = db.get(ReviewRequest, request_id)
    if not rr:
        return None
    if rr.status != ReviewRequestStatus.scheduled:
        return rr  # no cambia
    rr.status = ReviewRequestStatus.cancelled
    rr.cancelled_at = utcnow()
    db.commit()
    db.refresh(rr)
    return rr


def get_due_scheduled(db: Session, *, batch_size: int = 25) -> list[ReviewRequest]:
    now = utcnow()
    stmt = (
        select(ReviewRequest)
        .where(
            and_(
                ReviewRequest.status == ReviewRequestStatus.scheduled,
                ReviewRequest.send_at <= now,
            )
        )
        .order_by(ReviewRequest.send_at.asc())
        .limit(batch_size)
    )
    return list(db.execute(stmt).scalars().all())


def mark_sent(db: Session, *, rr: ReviewRequest) -> None:
    rr.status = ReviewRequestStatus.sent
    rr.sent_at = utcnow()
    rr.error_message = None
    db.commit()


def mark_failed(db: Session, *, rr: ReviewRequest, error_message: str) -> None:
    rr.status = ReviewRequestStatus.failed
    rr.error_message = error_message[:4000]
    db.commit()


def upsert_business_settings(
    db: Session,
    *,
    job_id: int,
    google_review_url: Optional[str],
    business_name: Optional[str],
) -> BusinessSettings:
    bs = db.get(BusinessSettings, job_id)
    if not bs:
        bs = BusinessSettings(job_id=job_id)
        db.add(bs)

    if google_review_url is not None:
        bs.google_review_url = google_review_url.strip() if google_review_url else None
    if business_name is not None:
        bs.business_name = business_name.strip() if business_name else None

    db.commit()
    db.refresh(bs)
    return bs


def get_business_settings(db: Session, *, job_id: int) -> Optional[BusinessSettings]:
    return db.get(BusinessSettings, job_id)
