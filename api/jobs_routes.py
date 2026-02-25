from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

router = APIRouter(tags=["jobs"])

@router.get("/jobs/{job_id}/entitlements")
def entitlements(job_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("select user_id from scrape_jobs where id=:id"),
        {"id": job_id},
    ).fetchone()

    if not row or not row[0]:
        return {"isPro": False}

    user_id = str(row[0])

    sub = db.execute(
        text("""
            select status
            from subscriptions
            where user_id=:uid
            order by updated_at desc
            limit 1
        """),
        {"uid": user_id},
    ).fetchone()

    is_pro = bool(sub and sub[0] in ("active", "trialing"))
    return {"isPro": is_pro}