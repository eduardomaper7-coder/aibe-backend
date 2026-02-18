import os
from twilio.rest import Client

from . import repo


def get_twilio_client() -> Client:
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")

    if not sid or not token:
        raise RuntimeError("Twilio credentials missing (TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN)")

    return Client(sid, token)


def send_whatsapp(to_number: str, message: str):
    client = get_twilio_client()

    from_whatsapp = os.getenv("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("TWILIO_WHATSAPP_FROM missing")

    return client.messages.create(
        from_=from_whatsapp,
        to=f"whatsapp:{to_number}",
        body=message,
    )


def process_pending(db):
    """
    Envía WhatsApps pendientes (due scheduled).
    ✅ Antes de enviar, garantiza que exista una URL de reseña:
       - si hay google_review_url -> usa esa
       - si hay google_place_id -> genera URL
       - si no hay place_id -> lo resuelve vía Google Places API (GOOGLE_MAPS_API_KEY)
         usando ScrapeJob.place_name, y persiste en business_settings
    """
    pending = repo.get_due_scheduled(db)

    client = get_twilio_client()
    from_whatsapp = os.getenv("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("TWILIO_WHATSAPP_FROM missing")

    sent = 0
    failed = 0

    for rr in pending:
        try:
            # ✅ Garantiza URL (y la guarda en business_settings)
            review_url = repo.ensure_business_review_url(db, job_id=rr.job_id)

            text = (
                f"Hola {rr.customer_name}, gracias por tu visita.\n\n"
                f"Déjanos tu reseña aquí:\n"
                f"{review_url}"
            )

            client.messages.create(
                from_=from_whatsapp,
                to=f"whatsapp:{rr.phone_e164}",
                body=text,
            )

            repo.mark_sent(db, rr)
            sent += 1

        except Exception as e:
            repo.mark_failed(db, rr, str(e))
            failed += 1

    return {
        "processed": len(pending),
        "sent": sent,
        "failed": failed,
    }
