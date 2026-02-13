# app/review_requests/twilio_service.py
import os
from twilio.rest import Client


def get_twilio_client() -> Client:
    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    if not sid or not token:
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN en env vars")
    return Client(sid, token)


def send_whatsapp_message(*, to_e164: str, body: str) -> str:
    """
    to_e164: "+34..." (E.164)
    Devuelve message SID
    """
    from_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("Falta TWILIO_WHATSAPP_FROM en env vars (ej: whatsapp:+34...)")

    client = get_twilio_client()
    msg = client.messages.create(
        from_=from_whatsapp,            # "whatsapp:+34...." o sandbox "whatsapp:+1415..."
        to=f"whatsapp:{to_e164}",       # TWILIO exige prefijo whatsapp:
        body=body,
    )
    return msg.sid
