import os
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db import get_db
from api.reviews_sync import sync_reviews_all, sync_reviews_for_job

router = APIRouter(prefix="/cron", tags=["cron"])

def _check_secret(secret: str):
    expected = os.getenv("CRON_SECRET", "")
    if not expected or secret != expected:
        raise HTTPException(401, "Unauthorized")

@router.post("/sync-reviews")
def cron_sync_reviews(
    secret: str = Query(...),
    job_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    _check_secret(secret)

    if job_id:
        return sync_reviews_for_job(db, job_id)

    return sync_reviews_all(db)