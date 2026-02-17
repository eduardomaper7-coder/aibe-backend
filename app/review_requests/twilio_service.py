import os
import json
from twilio.rest import Client


def get_twilio_client() -> Client:
    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    if not sid or not token:
        raise RuntimeError("Faltan TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN en env vars")
    return Client(sid, token)


def _format_whatsapp_to(to_e164: str) -> str:
    """
    Acepta "+34..." o "whatsapp:+34..."
    Devuelve siempre "whatsapp:+34..."
    """
    to_e164 = (to_e164 or "").strip()
    if not to_e164:
        raise RuntimeError("to_e164 vacío")
    return to_e164 if to_e164.startswith("whatsapp:") else f"whatsapp:{to_e164}"


def send_whatsapp_template(*, to_e164: str, template_sid: str, variables: dict) -> str:
    """
    Envía WhatsApp usando Content Template (obligatorio fuera de ventana 24h).
    template_sid: ContentSid "HX...."
    variables: dict con claves "1", "2", ... en string (ej: {"1":"Juan", "2":"https://..."})
    Devuelve message SID.
    """
    from_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("Falta TWILIO_WHATSAPP_FROM en env vars (ej: whatsapp:+34...)")

    if not template_sid:
        raise RuntimeError("Falta template_sid (ContentSid) para WhatsApp template")

    client = get_twilio_client()
    msg = client.messages.create(
        from_=from_whatsapp,  # "whatsapp:+34...." o sandbox "whatsapp:+1415..."
        to=_format_whatsapp_to(to_e164),
        content_sid=template_sid,
        content_variables=json.dumps(variables, ensure_ascii=False),
    )
    return msg.sid


# (Opcional) Mantengo la función freeform por si algún día la usas dentro de la ventana 24h.
def send_whatsapp_message(*, to_e164: str, body: str) -> str:
    """
    ENVÍO FREEFORM (puede fallar fuera de la ventana 24h con error 63016).
    """
    from_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("Falta TWILIO_WHATSAPP_FROM en env vars (ej: whatsapp:+34...)")

    client = get_twilio_client()
    msg = client.messages.create(
        from_=from_whatsapp,
        to=_format_whatsapp_to(to_e164),
        body=body,
    )
    return msg.sid
