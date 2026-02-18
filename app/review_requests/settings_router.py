from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from .models import BusinessSettings

router = APIRouter(prefix="/api/business-settings", tags=["business-settings"])


@router.post("")
def save_business_settings(
    job_id: int,
    business_name: str,
    google_place_id: str,
    db: Session = Depends(get_db),
):
    if not job_id or not google_place_id:
        raise HTTPException(400, "job_id y google_place_id son obligatorios")

    row = db.query(BusinessSettings).filter_by(job_id=job_id).first()

    if row:
        row.business_name = business_name
        row.google_place_id = google_place_id
    else:
        row = BusinessSettings(
            job_id=job_id,
            business_name=business_name,
            google_place_id=google_place_id,
        )
        db.add(row)

    db.commit()
    db.refresh(row)

    return {"ok": True}
