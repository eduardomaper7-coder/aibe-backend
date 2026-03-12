from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

router = APIRouter(prefix="/stripe/webhook", tags=["stripe"])

PLAN_CONFIG = {
    "starter": {"credit_eur": 9, "included_reviews": 45},
    "growth": {"credit_eur": 29, "included_reviews": 145},
    "pro": {"credit_eur": 79, "included_reviews": 395},
}


class CheckoutCompletedIn(BaseModel):
    job_id: int
    user_id: str
    subscription_id: str
    customer_id: str | None = None
    plan: str


@router.post("/checkout-completed")
def checkout_completed(payload: CheckoutCompletedIn, db: Session = Depends(get_db)):
    plan_data = PLAN_CONFIG.get(payload.plan)
    if not plan_data:
        raise HTTPException(status_code=400, detail="Plan inválido")

    db.execute(
        text("""
            insert into subscriptions (
              user_id,
              job_id,
              subscription_id,
              stripe_customer_id,
              status,
              plan,
              credit_eur,
              included_reviews,
              trial_reviews,
              trial_credit_eur,
              updated_at
            )
            values (
              :uid,
              :jid,
              :sid,
              :cid,
              'trialing',
              :plan,
              :credit,
              :included_reviews,
              25,
              5,
              now()
            )
            on conflict (subscription_id)
            do update set
              user_id=excluded.user_id,
              job_id=excluded.job_id,
              stripe_customer_id=excluded.stripe_customer_id,
              status='trialing',
              plan=excluded.plan,
              credit_eur=excluded.credit_eur,
              included_reviews=excluded.included_reviews,
              trial_reviews=25,
              trial_credit_eur=5,
              updated_at=now()
        """),
        {
            "uid": payload.user_id,
            "jid": payload.job_id,
            "sid": payload.subscription_id,
            "cid": payload.customer_id,
            "plan": payload.plan,
            "credit": float(plan_data["credit_eur"]),
            "included_reviews": int(plan_data["included_reviews"]),
        },
    )

    db.execute(
        text("""
            update users
            set
              subscription_id = :sid,
              subscription_status = 'trialing',
              stripe_customer_id = coalesce(:cid, stripe_customer_id)
            where id = :uid
        """),
        {
            "sid": payload.subscription_id,
            "cid": payload.customer_id,
            "uid": payload.user_id,
        },
    )

    db.commit()
    return {"ok": True}