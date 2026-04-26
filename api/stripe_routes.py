import os
from datetime import datetime, timezone

import stripe
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import get_db

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

router = APIRouter(prefix="/stripe", tags=["stripe"])


@router.post("/sync")
def stripe_sync(payload: dict, db: Session = Depends(get_db)):
    db.execute(
        text("""
        update users
        set
          stripe_customer_id = :cid,
          subscription_id = :sid,
          subscription_status = :st
        where id = :uid
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
              subscription_id,
              plan,
              credit_eur,
              status,
              included_reviews,
              trial_reviews,
              trial_credit_eur,
              billing_flow,
              prepaid_amount_eur,
              prepaid_at,
              free_reviews_used,
              plan_credits_unlocked,
              refund_requested,
              refund_requested_amount_eur
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    # 🔴 Si no hay suscripción
    if not row:
        return {
            "plan": None,
            "credit_eur": None,
            "status": "none",
            "included_reviews": None,
            "trial_reviews": 25,
            "trial_credit_eur": 5,
            "billing_flow": "prepaid",
            "prepaid_amount_eur": 0,
            "prepaid_at": None,
            "free_reviews_used": 0,
            "plan_credits_unlocked": False,
            "refund_requested": False,
            "refund_requested_amount_eur": 0,
            "renewal_at": None,
        }

    subscription_id = row[0]
    renewal_at = None

    # 🔵 Obtener fecha real desde Stripe
    if subscription_id:
        try:
            stripe_sub = stripe.Subscription.retrieve(subscription_id)

            current_period_end = getattr(
                stripe_sub,
                "current_period_end",
                None
            )

            if current_period_end:
                renewal_at = datetime.fromtimestamp(
                    current_period_end,
                    tz=timezone.utc
                ).isoformat()

        except Exception as e:
            print("ERROR STRIPE renewal_at:", repr(e))
            renewal_at = None

    # 🔵 Respuesta final
    return {
        "plan": row[1],
        "credit_eur": float(row[2]) if row[2] is not None else None,
        "status": row[3] or "none",
        "included_reviews": int(row[4]) if row[4] is not None else None,
        "trial_reviews": int(row[5]) if row[5] is not None else 25,
        "trial_credit_eur": float(row[6]) if row[6] is not None else 5,
        "billing_flow": row[7] or "prepaid",
        "prepaid_amount_eur": float(row[8]) if row[8] is not None else 0,
        "prepaid_at": row[9].isoformat() if row[9] is not None else None,
        "free_reviews_used": int(row[10]) if row[10] is not None else 0,
        "plan_credits_unlocked": bool(row[11]) if row[11] is not None else False,
        "refund_requested": bool(row[12]) if row[12] is not None else False,
        "refund_requested_amount_eur": float(row[13]) if row[13] is not None else 0,
        "renewal_at": renewal_at,
    }