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

DEFAULT_BILLING_FLOW = "prepaid"


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
              billing_flow,
              prepaid_amount_eur,
              prepaid_at,
              free_reviews_used,
              plan_credits_unlocked,
              refund_requested,
              refund_requested_amount_eur,
              manual_paid_override,
              updated_at
            )
            values (
              :uid,
              :jid,
              :sid,
              :cid,
              'prepaid',
              :plan,
              :credit,
              :included_reviews,
              25,
              5,
              :billing_flow,
              :credit,
              now(),
              0,
              false,
              false,
              0,
              false,
              now()
            )
            on conflict (subscription_id)
            do update set
              user_id = excluded.user_id,
              job_id = excluded.job_id,
              stripe_customer_id = excluded.stripe_customer_id,
              status = 'prepaid',
              plan = excluded.plan,
              credit_eur = excluded.credit_eur,
              included_reviews = excluded.included_reviews,
              trial_reviews = 25,
              trial_credit_eur = 5,
              billing_flow = excluded.billing_flow,
              prepaid_amount_eur = excluded.prepaid_amount_eur,
              prepaid_at = coalesce(subscriptions.prepaid_at, excluded.prepaid_at),
              updated_at = now()
        """),
        {
            "uid": payload.user_id,
            "jid": payload.job_id,
            "sid": payload.subscription_id,
            "cid": payload.customer_id,
            "plan": payload.plan,
            "credit": float(plan_data["credit_eur"]),
            "included_reviews": int(plan_data["included_reviews"]),
            "billing_flow": DEFAULT_BILLING_FLOW,
        },
    )

    db.execute(
        text("""
            update users
            set
              subscription_id = :sid,
              subscription_status = 'prepaid',
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


class InvoicePaidIn(BaseModel):
    subscription_id: str
    status: str | None = None


@router.post("/invoice-paid")
def invoice_paid(payload: InvoicePaidIn, db: Session = Depends(get_db)):
    sid = payload.subscription_id

    db.execute(
        text("""
            update subscriptions
            set
              status = case
                when coalesce(plan_credits_unlocked, false) = true then 'active'
                else 'prepaid'
              end,
              prepaid_at = coalesce(prepaid_at, now()),
              updated_at = now()
            where subscription_id = :sid
        """),
        {"sid": sid},
    )

    db.execute(
        text("""
            update users
            set subscription_status = (
              select case
                when coalesce(plan_credits_unlocked, false) = true then 'active'
                else 'prepaid'
              end
              from subscriptions
              where subscription_id = :sid
              limit 1
            )
            where subscription_id = :sid
        """),
        {"sid": sid},
    )

    db.commit()
    return {"ok": True}