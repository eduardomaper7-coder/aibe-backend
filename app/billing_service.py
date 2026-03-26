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
              plan,
              coalesce(billing_flow, 'prepaid') as billing_flow,
              coalesce(plan_credits_unlocked, false) as plan_credits_unlocked
            from subscriptions
            where job_id = :job_id
            order by updated_at desc
            limit 1
        """),
        {"job_id": job_id},
    ).fetchone()

    if not row:
        return {"activated": False, "reason": "no_subscription"}

    subscription_id, status, plan, billing_flow, plan_credits_unlocked = row

    if reviews_gained < 25:
        return {"activated": False, "reason": "threshold_not_reached"}

    if plan_credits_unlocked:
        return {"activated": False, "reason": "already_unlocked"}

    if billing_flow == "prepaid":
        db.execute(
            text("""
                update subscriptions
                set
                  status = 'active',
                  plan_credits_unlocked = true,
                  unlocked_at = now(),
                  free_reviews_used = 25,
                  updated_at = now()
                where subscription_id = :sid
            """),
            {"sid": subscription_id},
        )

        db.execute(
            text("""
                update users
                set subscription_status = 'active'
                where subscription_id = :sid
            """),
            {"sid": subscription_id},
        )

        db.commit()

        return {
            "activated": True,
            "plan": plan,
            "status": "active",
            "charged_now": False,
            "reason": "prepaid_plan_unlocked",
        }

    if billing_flow == "legacy_deferred":
        if status != "trialing":
            return {"activated": False, "reason": "not_trialing"}

        if not subscription_id:
            return {"activated": False, "reason": "missing_subscription_id"}

        stripe.Subscription.modify(
            subscription_id,
            trial_end="now",
            proration_behavior="none",
        )

        db.execute(
            text("""
                update subscriptions
                set
                  status = 'pending_activation',
                  free_reviews_used = 25,
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
            "charged_now": True,
            "reason": "legacy_deferred_charge",
        }

    return {"activated": False, "reason": "unknown_billing_flow"}