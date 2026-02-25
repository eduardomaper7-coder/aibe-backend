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