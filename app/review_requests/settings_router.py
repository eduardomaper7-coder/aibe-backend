from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from .models import BusinessSettings

router = APIRouter(prefix="/api/business-settings", tags=["business-settings"])


class BusinessSettingsSave(BaseModel):
    job_id: int = Field(..., ge=1)
    business_name: str | None = Field(default=None, max_length=200)
    google_place_id: str = Field(..., min_length=5, max_length=128)


def _build_review_url(place_id: str) -> str:
    place_id = (place_id or "").strip()
    return f"https://search.google.com/local/writereview?placeid={place_id}"


@router.post("", summary="Save business settings (placeId) and generate review URL")
def save_business_settings(
    payload: BusinessSettingsSave,
    db: Session = Depends(get_db),
):
    job_id = payload.job_id
    place_id = (payload.google_place_id or "").strip()
    business_name = (payload.business_name or "").strip() or None

    if not job_id or not place_id:
        raise HTTPException(status_code=400, detail="job_id y google_place_id son obligatorios")

    row = db.query(BusinessSettings).filter_by(job_id=job_id).first()

    if row:
        row.business_name = business_name
        row.google_place_id = place_id

        # ✅ Si no hay URL (o está vacía), la generamos desde el placeId
        if not (row.google_review_url or "").strip():
            row.google_review_url = _build_review_url(place_id)
    else:
        row = BusinessSettings(
            job_id=job_id,
            business_name=business_name,
            google_place_id=place_id,
            # ✅ Generamos y guardamos la URL automáticamente
            google_review_url=_build_review_url(place_id),
        )
        db.add(row)

    db.commit()
    db.refresh(row)

    return {
        "ok": True,
        "job_id": row.job_id,
        "business_name": row.business_name,
        "google_place_id": row.google_place_id,
        "google_review_url": row.google_review_url,
    }
