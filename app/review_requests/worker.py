# app/review_requests/worker.py
from __future__ import annotations

import os
import time

from db import SessionLocal, engine, Base  # ajusta si Base/engine están en otro sitio

from . import repo
from .twilio_service import send_whatsapp_message


POLL_SECONDS = int(os.environ.get("REVIEW_SENDER_POLL_SECONDS", "30"))
BATCH_SIZE = int(os.environ.get("REVIEW_SENDER_BATCH_SIZE", "25"))


def build_message(customer_name: str, business_name: str | None, google_review_url: str | None) -> str:
    biz = business_name or "nuestro negocio"
    url = google_review_url or ""
    # Mensaje simple (sin plantilla). OJO: en WhatsApp real quizá necesites Template.
    if url:
        return (
            f"Hola {customer_name} 👋\n"
            f"Somos {biz}.\n"
            f"Gracias por tu visita.\n"
            f"¿Nos dejas una reseña en Google? ⭐\n"
            f"{url}"
        )
    return (
        f"Hola {customer_name} 👋\n"
        f"Somos {biz}.\n"
        f"Gracias por tu visita.\n"
        f"¿Nos dejas una reseña en Google? ⭐"
    )


def main():
    # Asegura tablas si no usas Alembic (en producción te conviene migraciones, pero esto arranca rápido)
    Base.metadata.create_all(bind=engine)

    print("[review_sender] worker started. poll=", POLL_SECONDS)

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
                    msg = build_message(
                        customer_name=rr.customer_name,
                        business_name=(bs.business_name if bs else None),
                        google_review_url=(bs.google_review_url if bs else None),
                    )
                    sid = send_whatsapp_message(to_e164=rr.phone_e164, body=msg)
                    repo.mark_sent(db, rr=rr)
                    print(f"[review_sender] sent id={rr.id} twilio_sid={sid}")
                except Exception as e:
                    repo.mark_failed(db, rr=rr, error_message=str(e))
                    print(f"[review_sender] failed id={rr.id} err={e}")

        finally:
            db.close()

        # Si hubo trabajo, no duermas mucho para vaciar rápido
        time.sleep(2)


if __name__ == "__main__":
    main()
