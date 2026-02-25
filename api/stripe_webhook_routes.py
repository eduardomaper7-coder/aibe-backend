from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

router = APIRouter(prefix="/stripe/webhook", tags=["stripe"])

class CheckoutCompletedIn(BaseModel):
    job_id: int
    user_id: str
    subscription_id: str

@router.post("/checkout-completed")
def checkout_completed(payload: CheckoutCompletedIn, db: Session = Depends(get_db)):
    db.execute(
        text("""
          insert into subscriptions (user_id, subscription_id, status)
          values (:uid, :sid, 'active')
          on conflict (subscription_id)
          do update set status='active', updated_at=now()
        """),
        {"uid": payload.user_id, "sid": payload.subscription_id},
    )
    db.commit()
    return {"ok": True}