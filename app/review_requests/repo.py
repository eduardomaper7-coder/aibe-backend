from __future__ import annotations

from datetime import datetime
from typing import Optional
import re
import os
import requests

from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func
from sqlalchemy.exc import IntegrityError

from .models import ReviewRequest, ReviewRequestStatus, BusinessSettings
from .utils import utcnow

from app.models import ScrapeJob, Review


def find_existing_review_request(
    db: Session,
    *,
    job_id: int,
    phone_e164: str,
    appointment_at: datetime,
) -> Optional[ReviewRequest]:
    stmt = (
        select(ReviewRequest)
        .where(
            and_(
                ReviewRequest.job_id == job_id,
                ReviewRequest.phone_e164 == phone_e164,
                ReviewRequest.appointment_at == appointment_at,
            )
        )
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def create_review_request(
    db: Session,
    *,
    job_id: int,
    customer_name: str,
    phone_e164: str,
    appointment_at: datetime,
    send_at: datetime,
) -> ReviewRequest:
    existing = find_existing_review_request(
        db,
        job_id=job_id,
        phone_e164=phone_e164,
        appointment_at=appointment_at,
    )
    if existing:
        return existing

    bs = db.get(BusinessSettings, job_id)
    prevent = bool(getattr(bs, "prevent_duplicate_whatsapp", False)) if bs else False

    if prevent and already_sent_to_phone(db, job_id=job_id, phone_e164=phone_e164):
        rr = ReviewRequest(
            job_id=job_id,
            customer_name=customer_name,
            phone_e164=phone_e164,
            appointment_at=appointment_at,
            send_at=send_at,
            status=ReviewRequestStatus.cancelled,
            cancelled_at=utcnow(),
            error_message="ALREADY_SENT",
        )
        db.add(rr)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            existing = find_existing_review_request(
                db,
                job_id=job_id,
                phone_e164=phone_e164,
                appointment_at=appointment_at,
            )
            if existing:
                return existing
            raise
        db.refresh(rr)
        return rr

    rr = ReviewRequest(
        job_id=job_id,
        customer_name=customer_name,
        phone_e164=phone_e164,
        appointment_at=appointment_at,
        send_at=send_at,
        status=ReviewRequestStatus.scheduled,
    )
    db.add(rr)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = find_existing_review_request(
            db,
            job_id=job_id,
            phone_e164=phone_e164,
            appointment_at=appointment_at,
        )
        if existing:
            return existing
        raise

    db.refresh(rr)
    return rr


def already_sent_to_phone(db: Session, *, job_id: int, phone_e164: str) -> bool:
    stmt = (
        select(ReviewRequest.id)
        .where(
            and_(
                ReviewRequest.job_id == job_id,
                ReviewRequest.phone_e164 == phone_e164,
                ReviewRequest.status == ReviewRequestStatus.sent,
            )
        )
        .limit(1)
    )
    return db.execute(stmt).scalar() is not None


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
        return rr
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

    if (bs.google_review_url or "").strip():
        return

    place_id = (bs.google_place_id or "").strip()
    if place_id:
        bs.google_review_url = f"https://search.google.com/local/writereview?placeid={place_id}"
        db.commit()
        return

    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        return

    url = (getattr(job, "google_maps_url", None) or "").strip()

    m = re.search(r"[?&]placeid=([A-Za-z0-9_-]+)", url)
    if not m:
        m = re.search(r"place_id:([A-Za-z0-9_-]+)", url)

    if not m:
        return

    place_id = m.group(1)

    bs.google_place_id = place_id
    bs.google_review_url = f"https://search.google.com/local/writereview?placeid={place_id}"
    db.commit()


def upsert_business_settings(
    db: Session,
    *,
    job_id: int,
    google_place_id: Optional[str] = None,
    google_review_url: Optional[str] = None,
    business_name: Optional[str] = None,
    prevent_duplicate_whatsapp: Optional[bool] = None,
    whatsapp_provider: Optional[str] = None,
    whatsapp_personal_number: Optional[str] = None,
    whatsapp_personal_enabled: Optional[bool] = None,
    whatsapp_session_status: Optional[str] = None,
    whatsapp_last_error: Optional[str] = None,
) -> BusinessSettings:
    bs = db.get(BusinessSettings, job_id)
    if not bs:
        bs = BusinessSettings(job_id=job_id)
        db.add(bs)

    if google_place_id is not None:
        bs.google_place_id = google_place_id.strip() if google_place_id else None

    if google_review_url is not None:
        bs.google_review_url = google_review_url.strip() if google_review_url else None

    if business_name is not None:
        bs.business_name = business_name.strip() if business_name else None

    if prevent_duplicate_whatsapp is not None:
        bs.prevent_duplicate_whatsapp = bool(prevent_duplicate_whatsapp)

    if whatsapp_provider is not None:
        bs.whatsapp_provider = whatsapp_provider.strip() if whatsapp_provider else "twilio"

    if whatsapp_personal_number is not None:
        bs.whatsapp_personal_number = (
            whatsapp_personal_number.strip() if whatsapp_personal_number else None
        )

    if whatsapp_personal_enabled is not None:
        bs.whatsapp_personal_enabled = bool(whatsapp_personal_enabled)

    if whatsapp_session_status is not None:
        bs.whatsapp_session_status = (
            whatsapp_session_status.strip() if whatsapp_session_status else None
        )

    if whatsapp_last_error is not None:
        bs.whatsapp_last_error = whatsapp_last_error.strip() if whatsapp_last_error else None

    db.commit()
    db.refresh(bs)
    return bs

def build_review_url_from_place_id(place_id: str) -> str:
    place_id = (place_id or "").strip()
    return f"https://search.google.com/local/writereview?placeid={place_id}"


def resolve_place_id_via_places_api(query: str) -> str | None:
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        return None

    q = (query or "").strip()
    if not q:
        return None

    r = requests.get(
        "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
        params={
            "input": q,
            "inputtype": "textquery",
            "fields": "place_id",
            "key": key,
            "language": "es",
        },
        timeout=15,
    )
    data = r.json() if r.content else {}
    if data.get("status") != "OK":
        return None

    candidates = data.get("candidates") or []
    if not candidates:
        return None

    return candidates[0].get("place_id") or None


def ensure_business_review_url(db: Session, *, job_id: int) -> str:
    bs = db.get(BusinessSettings, job_id)

    if not bs:
        bs = BusinessSettings(job_id=job_id)
        db.add(bs)
        db.commit()
        db.refresh(bs)

    url = (bs.google_review_url or "").strip()
    if url:
        return url

    place_id = (bs.google_place_id or "").strip()
    if place_id:
        url = build_review_url_from_place_id(place_id)
        bs.google_review_url = url
        db.commit()
        return url

    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    name = (getattr(job, "place_name", None) or "").strip() if job else ""

    if not name:
        name = (bs.business_name or "").strip()

    if not name:
        raise RuntimeError(
            "No puedo resolver place_id: falta ScrapeJob.place_name y business_settings.business_name"
        )

    resolved = resolve_place_id_via_places_api(name)

    if not resolved:
        raise RuntimeError(
            f"No pude obtener place_id desde Places API (query='{name}')"
        )

    bs.google_place_id = resolved
    bs.google_review_url = build_review_url_from_place_id(resolved)

    db.commit()

    return bs.google_review_url


def load_existing_review_requests_for_job(db: Session, *, job_id: int) -> list[ReviewRequest]:
    stmt = select(ReviewRequest).where(ReviewRequest.job_id == job_id)
    return list(db.execute(stmt).scalars().all())