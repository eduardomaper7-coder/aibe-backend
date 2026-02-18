from __future__ import annotations

import os
import time
import json

from app.db import SessionLocal, engine, Base
from . import repo
from .twilio_service import send_whatsapp_template


POLL_SECONDS = int(os.environ.get("REVIEW_SENDER_POLL_SECONDS", "30"))
BATCH_SIZE = int(os.environ.get("REVIEW_SENDER_BATCH_SIZE", "25"))


def build_review_url(bs) -> str:
    """
    Devuelve una URL válida para reseña.
    Prioridad:
    1) business_settings.google_review_url (si existe)
    2) generar desde business_settings.google_place_id
    """
    if not bs:
        return ""

    # 1) URL guardada
    url = (getattr(bs, "google_review_url", None) or "").strip()
    if url:
        return url

    # 2) generar desde place_id
    place_id = (getattr(bs, "google_place_id", None) or "").strip()
    if place_id:
        return f"https://search.google.com/local/writereview?placeid={place_id}"

    return ""


def main():
    Base.metadata.create_all(bind=engine)

    print("[review_sender] worker started. poll=", POLL_SECONDS)

    template_sid = (
        os.environ.get("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS")
        or os.environ.get("TWILIO_CONTENT_SID_REVIEWS")
    )

    print("[review_sender] ENV TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS =",
          os.environ.get("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS"))
    print("[review_sender] ENV TWILIO_CONTENT_SID_REVIEWS =",
          os.environ.get("TWILIO_CONTENT_SID_REVIEWS"))
    print("[review_sender] TEMPLATE_SID (resolved) =", template_sid)

    if not template_sid:
        print("[review_sender] WARNING: falta TEMPLATE SID en env vars "
              "(TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS o TWILIO_CONTENT_SID_REVIEWS)")

    while True:
        db = SessionLocal()
        try:
            due = repo.get_due_scheduled(db, batch_size=BATCH_SIZE)
            if not due:
                time.sleep(POLL_SECONDS)
                continue

            for rr in due:
                try:
                    bs = repo.get_business_settings(db, job_id=rr.job_id)

                    review_url = build_review_url(bs)
                    if not review_url:
                        raise RuntimeError(
                            "No hay google_review_url ni google_place_id en business_settings "
                            f"(job_id={rr.job_id}). No puedo enviar template ({{2}} requerido)."
                        )

                    name = (rr.customer_name or "").strip() or "😊"

                    variables = {
                        "1": name,
                        "2": review_url,
                    }

                    # DEBUG útil (puedes quitarlo luego)
                    print("[review_sender] sending to", rr.phone_e164,
                          "content_variables=", json.dumps(variables, ensure_ascii=False))

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
