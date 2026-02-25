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

@router.get("/jobs/my-latest/{user_id}")
def my_latest_job(user_id: str, db: Session = Depends(get_db)):

    row = db.execute(text("""
        select id
        from scrape_jobs
        where user_id = :uid
        order by created_at desc
        limit 1
    """), {"uid": user_id}).fetchone()

    if not row:
        return {"job_id": None}

    return {"job_id": row[0]}