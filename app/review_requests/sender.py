import os
from twilio.rest import Client

from . import repo


def get_twilio_client():
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")

    if not sid or not token:
        raise RuntimeError("Twilio credentials missing")

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
    pending = repo.get_due_scheduled(db)

    client = get_twilio_client()

    from_whatsapp = os.getenv("TWILIO_WHATSAPP_FROM")

    sent = 0
    failed = 0

    for rr in pending:
        try:
            text = (
                f"Hola {rr.customer_name}, gracias por tu visita.\n\n"
                f"Déjanos tu reseña aquí:\n"
                f"{rr.business_settings.google_review_url}"
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
