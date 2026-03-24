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
    variables: dict con claves "1", "2", ... en string
               ejemplo: {"1": "Juan", "2": "https://..."}

    Devuelve el Message SID de Twilio.
    """
    print("########## SEND_WHATSAPP_TEMPLATE ACTIVO ##########")

    from_whatsapp = os.environ.get("TWILIO_WHATSAPP_FROM")
    if not from_whatsapp:
        raise RuntimeError("Falta TWILIO_WHATSAPP_FROM en env vars (ej: whatsapp:+34...)")

    if not template_sid:
        raise RuntimeError("Falta template_sid (ContentSid) para WhatsApp template")

    client = get_twilio_client()

    formatted_to = _format_whatsapp_to(to_e164)
    content_variables = json.dumps(variables, ensure_ascii=False)

    print(f"[twilio_template] from={from_whatsapp}")
    print(f"[twilio_template] to={formatted_to}")
    print(f"[twilio_template] content_sid={template_sid}")
    print(f"[twilio_template] content_variables={content_variables}")

    msg = client.messages.create(
        from_=from_whatsapp,
        to=formatted_to,
        content_sid=template_sid,
        content_variables=content_variables,
    )

    print(f"[twilio_template] message sid={msg.sid}")
    return msg.sid