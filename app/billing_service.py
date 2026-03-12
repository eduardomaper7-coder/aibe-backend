import os
import stripe
from sqlalchemy.orm import Session
from sqlalchemy import text

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

def maybe_activate_subscription_after_25_reviews(
    db: Session,
    job_id: int,
    reviews_gained: int,
) -> dict:
    row = db.execute(
        text("""
            select
              subscription_id,
              status,
              plan
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        return {"activated": False, "reason": "no_subscription"}

    subscription_id, status, plan = row

    if status != "trialing":
        return {"activated": False, "reason": "not_trialing"}

    if reviews_gained < 25:
        return {"activated": False, "reason": "threshold_not_reached"}

    if not subscription_id:
        return {"activated": False, "reason": "missing_subscription_id"}

    # Stripe permite terminar el trial con trial_end="now"
    stripe.Subscription.modify(
        subscription_id,
        trial_end="now",
        proration_behavior="none",
    )

    # OJO:
    # Aquí lo marcamos provisionalmente como 'pending_activation'
    # hasta que llegue el webhook real de Stripe confirmando el cobro.
    db.execute(
        text("""
            update subscriptions
            set
              status = 'pending_activation',
              updated_at = now()
            where subscription_id = :sid
        """),
        {"sid": subscription_id},
    )

    db.execute(
        text("""
            update users
            set subscription_status = 'pending_activation'
            where subscription_id = :sid
        """),
        {"sid": subscription_id},
    )

    db.commit()

    return {
        "activated": True,
        "plan": plan,
        "status": "pending_activation",
    }