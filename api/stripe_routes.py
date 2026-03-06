from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

router = APIRouter(prefix="/stripe", tags=["stripe"])


@router.post("/sync")
def stripe_sync(payload: dict, db: Session = Depends(get_db)):

    db.execute(
        text("""
        update users
        set
          stripe_customer_id=:cid,
          subscription_id=:sid,
          subscription_status=:st
        where id=:uid
        """),
        {
            "cid": payload["customer_id"],
            "sid": payload["subscription_id"],
            "st": payload["status"],
            "uid": payload["user_id"],
        },
    )

    db.commit()

    return {"ok": True}


@router.get("/subscription-by-job")
def subscription_by_job(job_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            select plan, credit_eur, status
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        return {"plan": None, "credit_eur": None, "status": None}

    return {
        "plan": row[0],
        "credit_eur": float(row[1]) if row[1] is not None else None,
        "status": row[2],
    }