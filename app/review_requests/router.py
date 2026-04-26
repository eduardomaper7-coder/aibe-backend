from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.billing_service import maybe_activate_subscription_after_25_reviews
from .sender import process_pending
from app.db import get_db
from app.reviews_service import check_and_store_latest_reviews
from sqlalchemy import text
from .schemas import (
    ReviewRequestCreate,
    ReviewRequestSendNow,
    ReviewRequestOut,
    ReviewRequestListOut,
    CancelOut,
    BusinessSettingsUpsert,
    BusinessSettingsOut,
)
from .utils import compute_send_at, utcnow
from . import repo

router = APIRouter(prefix="/api", tags=["review-requests"])


@router.post("/review-requests", response_model=ReviewRequestOut)
def create_review_request(payload: ReviewRequestCreate, db: Session = Depends(get_db)):
    send_at = compute_send_at(payload.appointment_at)

    rr = repo.create_review_request(
        db,
        job_id=payload.job_id,
        customer_name=payload.customer_name,
        phone_e164=payload.phone_e164,
        appointment_at=payload.appointment_at,
        send_at=send_at,
    )
    return rr


@router.post("/review-requests/send-now", response_model=ReviewRequestOut)
def send_review_request_now(payload: ReviewRequestSendNow, db: Session = Depends(get_db)):
    now = utcnow()

    rr = repo.create_review_request(
        db,
        job_id=payload.job_id,
        customer_name=payload.customer_name,
        phone_e164=payload.phone_e164,
        appointment_at=now,
        send_at=now,
    )

    process_pending(db)

    db.refresh(rr)
    return rr


@router.get("/review-requests", response_model=ReviewRequestListOut)
def list_requests(
    job_id: int = Query(...),
    limit: int = Query(200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    items = repo.list_review_requests(db, job_id=job_id, limit=limit)
    return {"items": items}


@router.patch("/review-requests/{request_id}/cancel", response_model=CancelOut)
def cancel_request(request_id: int, db: Session = Depends(get_db)):
    rr = repo.cancel_review_request(db, request_id=request_id)
    if not rr:
        raise HTTPException(status_code=404, detail="No existe")
    return {"ok": True, "status": rr.status.value}


@router.get("/business-settings", response_model=BusinessSettingsOut)
def get_settings(job_id: int = Query(...), db: Session = Depends(get_db)):
    bs = repo.get_business_settings(db, job_id=job_id)

    if not bs:
        return {
            "job_id": job_id,
            "google_place_id": None,
            "google_review_url": None,
            "business_name": None,
            "prevent_duplicate_whatsapp": False,
        }

    return bs


@router.patch("/business-settings", response_model=BusinessSettingsOut)
def upsert_settings(payload: BusinessSettingsUpsert, db: Session = Depends(get_db)):
    bs = repo.upsert_business_settings(
        db,
        job_id=payload.job_id,
        google_place_id=payload.google_place_id,
        google_review_url=payload.google_review_url,
        business_name=payload.business_name,
        prevent_duplicate_whatsapp=payload.prevent_duplicate_whatsapp,
    )

    if not bs:
        raise HTTPException(status_code=500, detail="No se pudo guardar configuración")

    return bs


@router.get("/review-requests/stats")
def stats(
    job_id: int = Query(...),
    from_date: str | None = Query(None, alias="from"),
    db: Session = Depends(get_db),
):
    return repo.get_stats(db, job_id=job_id, from_date=from_date)


@router.post("/review-requests/check-new-reviews")
def check_new_reviews(job_id: int = Query(...), db: Session = Depends(get_db)):
    try:
        result = check_and_store_latest_reviews(db, job_id=job_id, personal_data=True)

        stats_data = repo.get_stats(db, job_id=job_id)
        reviews_gained = int((stats_data or {}).get("reviews_gained") or 0)

        activation = maybe_activate_subscription_after_25_reviews(
            db=db,
            job_id=job_id,
            reviews_gained=reviews_gained,
        )

        result["subscription_activation"] = activation
        result["reviews_gained"] = reviews_gained
        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comprobando reseñas: {e}")


@router.post("/review-requests/send-due")
def send_due(db: Session = Depends(get_db)):
    return process_pending(db)

@router.post("/review-requests/refund-request")
def request_refund(job_id: int = Query(...), db: Session = Depends(get_db)):
    sub = db.execute(
        text("""
            select
              user_id,
              subscription_id,
              plan,
              credit_eur,
              included_reviews,
              trial_reviews,
              trial_credit_eur,
              coalesce(refund_requested, false) as refund_requested
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not sub:
        raise HTTPException(status_code=404, detail="No existe suscripción para este negocio")

    user_id, subscription_id, plan, credit_eur, included_reviews, trial_reviews, trial_credit_eur, refund_requested = sub

    if refund_requested:
        raise HTTPException(status_code=400, detail="Ya existe una solicitud de reembolso para esta suscripción")

    stats_data = repo.get_stats(db, job_id=job_id)
    reviews_gained = int((stats_data or {}).get("reviews_gained") or 0)

    paid_reviews_total = int(included_reviews or 0)
    free_reviews_total = int(trial_reviews or 25)

    paid_reviews_used = max(0, reviews_gained - free_reviews_total)
    paid_reviews_remaining = max(0, paid_reviews_total - paid_reviews_used)

    refund_amount = round(paid_reviews_remaining * 0.2, 2)

    db.execute(
        text("""
            insert into refund_requests (
              job_id,
              user_id,
              subscription_id,
              requested_amount_eur,
              paid_reviews_total,
              paid_reviews_used,
              paid_reviews_remaining,
              trial_reviews_snapshot,
              trial_credit_snapshot_eur,
              status,
              created_at
            )
            values (
              :job_id,
              :user_id,
              :subscription_id,
              :amount,
              :paid_reviews_total,
              :paid_reviews_used,
              :paid_reviews_remaining,
              :trial_reviews_snapshot,
              :trial_credit_snapshot_eur,
              'pending',
              now()
            )
        """),
        {
            "job_id": job_id,
            "user_id": user_id,
            "subscription_id": subscription_id,
            "amount": refund_amount,
            "paid_reviews_total": paid_reviews_total,
            "paid_reviews_used": paid_reviews_used,
            "paid_reviews_remaining": paid_reviews_remaining,
            "trial_reviews_snapshot": free_reviews_total,
            "trial_credit_snapshot_eur": float(trial_credit_eur or 5),
        },
    )

    db.execute(
        text("""
            update subscriptions
            set
              refund_requested = true,
              refund_requested_at = now(),
              refund_requested_amount_eur = :amount,
              updated_at = now()
            where subscription_id = :sid
        """),
        {
            "amount": refund_amount,
            "sid": subscription_id,
        },
    )

    db.commit()

    return {
        "ok": True,
        "amount_eur": refund_amount,
        "message": f"Reembolso confirmado, Se abonarán {refund_amount:.2f}€ a tu cuenta en un plazo inferior a 72 horas",
    }

@router.post("/admin/subscription/mark-prepaid")
def mark_subscription_prepaid(job_id: int = Query(...), db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            select
              subscription_id,
              credit_eur
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No existe suscripción para este negocio")

    subscription_id, credit_eur = row

    db.execute(
        text("""
            update subscriptions
            set
              billing_flow = 'prepaid',
              status = 'prepaid',
              prepaid_amount_eur = coalesce(prepaid_amount_eur, credit_eur, :credit),
              prepaid_at = coalesce(prepaid_at, now()),
              manual_paid_override = true,
              updated_at = now()
            where subscription_id = :sid
        """),
        {
            "sid": subscription_id,
            "credit": float(credit_eur or 0),
        },
    )

    db.execute(
        text("""
            update users
            set subscription_status = 'prepaid'
            where subscription_id = :sid
        """),
        {"sid": subscription_id},
    )

    db.commit()

    return {"ok": True, "subscription_id": subscription_id, "status": "prepaid"}

@router.post("/admin/subscription/mark-legacy-deferred")
def mark_subscription_legacy(job_id: int = Query(...), db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            select subscription_id
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No existe suscripción para este negocio")

    subscription_id = row[0]

    db.execute(
        text("""
            update subscriptions
            set
              billing_flow = 'legacy_deferred',
              status = 'trialing',
              updated_at = now()
            where subscription_id = :sid
        """),
        {"sid": subscription_id},
    )

    db.execute(
        text("""
            update users
            set subscription_status = 'trialing'
            where subscription_id = :sid
        """),
        {"sid": subscription_id},
    )

    db.commit()

    return {"ok": True, "subscription_id": subscription_id, "status": "trialing"}