from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

router = APIRouter(tags=["jobs"])

@router.get("/jobs/{job_id}/entitlements")
def entitlements(job_id: int, db: Session = Depends(get_db)):

    row = db.execute(
        text("""
        select u.subscription_status
        from scrape_jobs j
        join users u on j.user_id = u.id
        where j.id=:id
        """),
        {"id": job_id},
    ).fetchone()

    if not row:
        return {"isPro": False}

    return {
        "isPro": row[0] in ("active", "trialing")
    }