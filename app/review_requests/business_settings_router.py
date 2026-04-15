from typing import Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.review_requests.models import BusinessSettings
from app.review_requests.whatsapp_gateway_service import (
    start_job_whatsapp_session,
    get_job_whatsapp_session_status,
    WhatsAppGatewayError,
)

router = APIRouter(prefix="/api/business-settings", tags=["business-settings"])


class BusinessSettingsPatchIn(BaseModel):
    job_id: int
    business_name: Optional[str] = None
    google_review_url: Optional[str] = None
    prevent_duplicate_whatsapp: Optional[bool] = None

    whatsapp_provider: Optional[Literal["twilio", "personal_number"]] = None
    whatsapp_personal_number: Optional[str] = None
    whatsapp_personal_enabled: Optional[bool] = None
    whatsapp_session_status: Optional[str] = None
    whatsapp_last_error: Optional[str] = None


class WhatsAppConnectIn(BaseModel):
    job_id: int


@router.get("")
def get_business_settings(
    job_id: int = Query(...),
    db: Session = Depends(get_db),
):
    row = db.query(BusinessSettings).filter(BusinessSettings.job_id == job_id).first()

    if not row:
        row = BusinessSettings(job_id=job_id)
        db.add(row)
        db.commit()
        db.refresh(row)

    return {
        "job_id": row.job_id,
        "google_review_url": row.google_review_url,
        "business_name": row.business_name,
        "prevent_duplicate_whatsapp": row.prevent_duplicate_whatsapp,
        "whatsapp_provider": row.whatsapp_provider,
        "whatsapp_personal_number": row.whatsapp_personal_number,
        "whatsapp_personal_enabled": row.whatsapp_personal_enabled,
        "whatsapp_session_status": row.whatsapp_session_status,
        "whatsapp_last_error": row.whatsapp_last_error,
    }


@router.patch("")
def patch_business_settings(
    payload: BusinessSettingsPatchIn,
    db: Session = Depends(get_db),
):
    row = db.query(BusinessSettings).filter(BusinessSettings.job_id == payload.job_id).first()

    if not row:
        row = BusinessSettings(job_id=payload.job_id)
        db.add(row)
        db.flush()

    if payload.business_name is not None:
        row.business_name = payload.business_name

    if payload.google_review_url is not None:
        row.google_review_url = payload.google_review_url

    if payload.prevent_duplicate_whatsapp is not None:
        row.prevent_duplicate_whatsapp = payload.prevent_duplicate_whatsapp

    if payload.whatsapp_provider is not None:
        row.whatsapp_provider = payload.whatsapp_provider

    if payload.whatsapp_personal_number is not None:
        row.whatsapp_personal_number = payload.whatsapp_personal_number

    if payload.whatsapp_personal_enabled is not None:
        row.whatsapp_personal_enabled = payload.whatsapp_personal_enabled

    if payload.whatsapp_session_status is not None:
        row.whatsapp_session_status = payload.whatsapp_session_status

    if payload.whatsapp_last_error is not None:
        row.whatsapp_last_error = payload.whatsapp_last_error

    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "job_id": row.job_id,
        "google_review_url": row.google_review_url,
        "business_name": row.business_name,
        "prevent_duplicate_whatsapp": row.prevent_duplicate_whatsapp,
        "whatsapp_provider": row.whatsapp_provider,
        "whatsapp_personal_number": row.whatsapp_personal_number,
        "whatsapp_personal_enabled": row.whatsapp_personal_enabled,
        "whatsapp_session_status": row.whatsapp_session_status,
        "whatsapp_last_error": row.whatsapp_last_error,
    }


@router.post("/whatsapp/connect")
def connect_whatsapp(
    payload: WhatsAppConnectIn,
    db: Session = Depends(get_db),
):
    row = db.query(BusinessSettings).filter(BusinessSettings.job_id == payload.job_id).first()

    if not row:
        row = BusinessSettings(job_id=payload.job_id)
        db.add(row)
        db.commit()
        db.refresh(row)

    try:
        result = start_job_whatsapp_session(payload.job_id)

        row.whatsapp_session_status = result.get("status")
        row.whatsapp_last_error = result.get("last_error")
        db.add(row)
        db.commit()

        return result
    except WhatsAppGatewayError as e:
        row.whatsapp_session_status = "error"
        row.whatsapp_last_error = str(e)
        db.add(row)
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/whatsapp/status")
def whatsapp_status(
    job_id: int = Query(...),
    db: Session = Depends(get_db),
):
    row = db.query(BusinessSettings).filter(BusinessSettings.job_id == job_id).first()

    if not row:
        row = BusinessSettings(job_id=job_id)
        db.add(row)
        db.commit()
        db.refresh(row)

    try:
        result = get_job_whatsapp_session_status(job_id)

        row.whatsapp_session_status = result.get("status")
        row.whatsapp_last_error = result.get("last_error")
        db.add(row)
        db.commit()

        return result
    except WhatsAppGatewayError as e:
        row.whatsapp_session_status = "error"
        row.whatsapp_last_error = str(e)
        db.add(row)
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))