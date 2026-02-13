# app/review_requests/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from db import SessionLocal  # ajusta si tu sesión se llama diferente

from .schemas import (
    ReviewRequestCreate, ReviewRequestOut, ReviewRequestListOut,
    CancelOut, BusinessSettingsUpsert, BusinessSettingsOut
)
from .utils import compute_send_at
from . import repo


router = APIRouter(prefix="/api", tags=["review-requests"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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
        # devolvemos vacío en vez de 404 para que el front lo trate fácil
        return {"job_id": job_id, "google_review_url": None, "business_name": None}
    return bs


@router.patch("/business-settings", response_model=BusinessSettingsOut)
def upsert_settings(payload: BusinessSettingsUpsert, db: Session = Depends(get_db)):
    bs = repo.upsert_business_settings(
        db,
        job_id=payload.job_id,
        google_review_url=payload.google_review_url,
        business_name=payload.business_name,
    )
    return bs
