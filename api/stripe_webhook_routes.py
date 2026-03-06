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
    plan: str | None = None
    credit_eur: float | None = None


@router.post("/checkout-completed")
def checkout_completed(payload: CheckoutCompletedIn, db: Session = Depends(get_db)):
    db.execute(
        text(
            """
            insert into subscriptions (
              user_id,
              job_id,
              subscription_id,
              status,
              plan,
              credit_eur,
              updated_at
            )
            values (
              :uid,
              :jid,
              :sid,
              'active',
              :plan,
              :credit,
              now()
            )
            on conflict (subscription_id)
            do update set
              status='active',
              job_id=excluded.job_id,
              plan=excluded.plan,
              credit_eur=excluded.credit_eur,
              updated_at=now()
            """
        ),
        {
            "uid": payload.user_id,
            "jid": payload.job_id,
            "sid": payload.subscription_id,
            "plan": payload.plan,
            "credit": payload.credit_eur,
        },
    )
    db.commit()
    return {"ok": True}