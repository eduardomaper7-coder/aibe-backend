from __future__ import annotations

import os
import time
import json

from app.db import SessionLocal, engine, Base
from . import repo
from .twilio_service import send_whatsapp_template
from .whatsapp_gateway_service import send_whatsapp_review_message


POLL_SECONDS = int(os.environ.get("REVIEW_SENDER_POLL_SECONDS", "30"))
BATCH_SIZE = int(os.environ.get("REVIEW_SENDER_BATCH_SIZE", "25"))


def main():
    Base.metadata.create_all(bind=engine)

    print("[review_sender] worker started. poll=", POLL_SECONDS)

    template_sid = (
        os.environ.get("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS")
        or os.environ.get("TWILIO_CONTENT_SID_REVIEWS")
    )

    print(
        "[review_sender] ENV TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS =",
        os.environ.get("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS"),
    )
    print(
        "[review_sender] ENV TWILIO_CONTENT_SID_REVIEWS =",
        os.environ.get("TWILIO_CONTENT_SID_REVIEWS"),
    )
    print("[review_sender] TEMPLATE_SID (resolved) =", template_sid)

    if not template_sid:
        print(
            "[review_sender] WARNING: falta TEMPLATE SID en env vars "
            "(TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS o TWILIO_CONTENT_SID_REVIEWS)"
        )

    while True:
        db = SessionLocal()
        try:
            due = repo.get_due_scheduled(db, batch_size=BATCH_SIZE)
            if not due:
                time.sleep(POLL_SECONDS)
                continue

            for rr in due:
                try:
                    review_url = repo.ensure_business_review_url(db, job_id=rr.job_id)
                    settings = repo.get_business_settings(db, job_id=rr.job_id)

                    name = (rr.customer_name or "").strip() or "😊"
                    provider = getattr(settings, "whatsapp_provider", None) or "twilio"
                    business_name = getattr(settings, "business_name", None) if settings else None
                    personal_enabled = bool(
                        getattr(settings, "whatsapp_personal_enabled", False)
                    )

                    if provider == "personal_number" and personal_enabled:
                        print(
                            "[review_sender] sending via personal_number to",
                            rr.phone_e164,
                        )

                        result = send_whatsapp_review_message(
                            job_id=rr.job_id,
                            phone_e164=rr.phone_e164,
                            customer_name=name,
                            business_name=business_name,
                            google_review_url=review_url,
                        )

                        repo.mark_sent(db, rr=rr)
                        print(
                            f"[review_sender] sent id={rr.id} personal_result={json.dumps(result, ensure_ascii=False)}"
                        )

                    else:
                        variables = {
                            "1": name,
                            "2": review_url,
                        }

                        print(
                            "[review_sender] sending via twilio to",
                            rr.phone_e164,
                            "content_variables=",
                            json.dumps(variables, ensure_ascii=False),
                        )

                        sid = send_whatsapp_template(
                            to_e164=rr.phone_e164,
                            template_sid=template_sid,
                            variables=variables,
                        )

                        repo.mark_sent(db, rr=rr)
                        print(f"[review_sender] sent id={rr.id} twilio_sid={sid}")

                except Exception as e:
                    repo.mark_failed(db, rr=rr, error_message=str(e))
                    print(f"[review_sender] failed id={rr.id} err={e}")

        finally:
            db.close()

        time.sleep(2)


if __name__ == "__main__":
    main()