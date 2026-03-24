import os

from .twilio_service import send_whatsapp_template
from . import repo


def process_pending(db):
    """
    Envía WhatsApps pendientes (due scheduled) usando plantilla de WhatsApp.
    ✅ Antes de enviar, garantiza que exista una URL de reseña:
       - si hay google_review_url -> usa esa
       - si hay google_place_id -> genera URL
       - si no hay place_id -> lo resuelve vía Google Places API (GOOGLE_MAPS_API_KEY)
         usando ScrapeJob.place_name, y persiste en business_settings

    Requiere en .env:
      - TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS=HX...
        o TWILIO_CONTENT_SID_REVIEWS=HX...
      - TWILIO_WHATSAPP_FROM=whatsapp:+...
      - TWILIO_ACCOUNT_SID=...
      - TWILIO_AUTH_TOKEN=...
    """
    print("########## PROCESS_PENDING TEMPLATE ACTIVO ##########")

    pending = repo.get_due_scheduled(db)

    template_sid = (
        os.getenv("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS")
        or os.getenv("TWILIO_CONTENT_SID_REVIEWS")
    )
    if not template_sid:
        raise RuntimeError(
            "Missing WhatsApp template SID "
            "(TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS / TWILIO_CONTENT_SID_REVIEWS)"
        )

    sent = 0
    failed = 0

    print(f"[send_due] encontrados: {len(pending)}")
    print(f"[send_due] template_sid={template_sid}")

    for rr in pending:
        try:
            review_url = repo.ensure_business_review_url(db, job_id=rr.job_id)

            variables = {
                "1": (rr.customer_name or "").strip() or "😊",
                "2": review_url,
            }

            print(
                f"[send_due] enviando rr={rr.id} "
                f"job_id={rr.job_id} "
                f"to={rr.phone_e164} "
                f"send_at={rr.send_at} "
                f"vars={variables}"
            )

            sid = send_whatsapp_template(
                to_e164=rr.phone_e164,
                template_sid=template_sid,
                variables=variables,
            )

            print(f"[send_due] twilio sid={sid} rr={rr.id}")

            repo.mark_sent(db, rr=rr)
            sent += 1

        except Exception as e:
            try:
                repo.mark_failed(db, rr=rr, error_message=str(e))
            except Exception as inner:
                db.rollback()
                print(f"[send_due] error marcando failed rr={rr.id}: {repr(inner)}")

            failed += 1
            print(f"[send_due] error enviando rr={rr.id}: {repr(e)}")

    return {
        "processed": len(pending),
        "sent": sent,
        "failed": failed,
    }