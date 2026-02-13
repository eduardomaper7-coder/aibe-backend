from __future__ import annotations

from datetime import datetime
from typing import Optional
import re

from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func

from .models import ReviewRequest, ReviewRequestStatus, BusinessSettings
from .utils import utcnow

from app.models import ScrapeJob, Review


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


def get_stats(db: Session, *, job_id: int):
    sent_count = db.execute(
        select(func.count(ReviewRequest.id))
        .where(ReviewRequest.job_id == job_id)
        .where(ReviewRequest.status == ReviewRequestStatus.sent)
    ).scalar() or 0

    first_sent_at = db.execute(
        select(func.min(ReviewRequest.sent_at))
        .where(ReviewRequest.job_id == job_id)
        .where(ReviewRequest.status == ReviewRequestStatus.sent)
    ).scalar()

    reviews_gained = 0
    if first_sent_at:
        # published_at es string ISO en tu Review; comparamos por prefijo fecha si hace falta.
        # Mejor: si published_at viene tipo "2026-02-12..." hacemos comparación string.
        # Para hacerlo robusto, asumimos que published_at empieza con ISO.
        iso = first_sent_at.isoformat()
        reviews_gained = db.execute(
            select(func.count(Review.id))
            .where(Review.job_id == job_id)
            .where(Review.published_at >= iso)
        ).scalar() or 0

    conversion = (reviews_gained / sent_count) if sent_count > 0 else 0.0

    return {
        "messages_sent": int(sent_count),
        "reviews_gained": int(reviews_gained),
        "conversion_rate": float(conversion),
    }


PLACE_ID_RE = re.compile(r"place_id:([A-Za-z0-9_-]+)")

def ensure_review_url(db: Session, *, job_id: int) -> None:
    bs = db.get(BusinessSettings, job_id)
    if not bs:
        return
    if bs.google_review_url:
        return

    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        return

    url = getattr(job, "google_maps_url", None) or ""
    m = PLACE_ID_RE.search(url)
    if not m:
        return

    place_id = m.group(1)
    bs.google_review_url = f"https://search.google.com/local/writereview?placeid={place_id}"
    db.commit()
