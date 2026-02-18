# app/review_requests/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from .sender import process_pending
from app.db import get_db

from .schemas import (
    ReviewRequestCreate,
    ReviewRequestOut,
    ReviewRequestListOut,
    CancelOut,
    BusinessSettingsUpsert,
    BusinessSettingsOut,
)
from .utils import compute_send_at
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
        }

    return bs


@router.patch("/business-settings", response_model=BusinessSettingsOut)
def upsert_settings(payload: BusinessSettingsUpsert, db: Session = Depends(get_db)):
    bs = repo.upsert_business_settings(
        db,
        job_id=payload.job_id,
        google_place_id=payload.google_place_id,  # ✅ ok
        google_review_url=payload.google_review_url,
        business_name=payload.business_name,
    )

    if not bs:
        raise HTTPException(status_code=500, detail="No se pudo guardar configuración")

    return bs


@router.get("/review-requests/stats")
def stats(job_id: int = Query(...), db: Session = Depends(get_db)):
    return repo.get_stats(db, job_id=job_id)


@router.post("/review-requests/send-due")
def send_due(db: Session = Depends(get_db)):
    # ✅ aquí es donde se resuelve placeId/url y se envía (vía sender.process_pending)
    return process_pending(db)
