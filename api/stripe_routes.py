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
            select
              plan,
              credit_eur,
              status,
              included_reviews,
              trial_reviews,
              trial_credit_eur
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        return {
            "plan": None,
            "credit_eur": None,
            "status": "none",
            "included_reviews": None,
            "trial_reviews": 25,
            "trial_credit_eur": 5,
        }

    return {
        "plan": row[0],
        "credit_eur": float(row[1]) if row[1] is not None else None,
        "status": row[2] or "none",
        "included_reviews": int(row[3]) if row[3] is not None else None,
        "trial_reviews": int(row[4]) if row[4] is not None else 25,
        "trial_credit_eur": float(row[5]) if row[5] is not None else 5,
    }