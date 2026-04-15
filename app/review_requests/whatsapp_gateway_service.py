import os
import requests


WHATSAPP_GATEWAY_URL = os.getenv("WHATSAPP_GATEWAY_URL", "").rstrip("/")
WHATSAPP_GATEWAY_API_KEY = os.getenv("WHATSAPP_GATEWAY_API_KEY", "")


class WhatsAppGatewayError(Exception):
    pass


def _headers():
    return {
        "x-api-key": WHATSAPP_GATEWAY_API_KEY,
        "Content-Type": "application/json",
    }


def start_job_whatsapp_session(job_id: int) -> dict:
    response = requests.post(
        f"{WHATSAPP_GATEWAY_URL}/sessions/start",
        json={"job_id": job_id},
        headers=_headers(),
        timeout=45,
    )

    try:
        payload = response.json()
    except Exception:
        payload = {"error": response.text}

    if response.status_code >= 400:
        raise WhatsAppGatewayError(payload.get("error") or f"HTTP {response.status_code}")

    return payload


def get_job_whatsapp_session_status(job_id: int) -> dict:
    response = requests.get(
        f"{WHATSAPP_GATEWAY_URL}/sessions/{job_id}/status",
        headers=_headers(),
        timeout=45,
    )

    try:
        payload = response.json()
    except Exception:
        payload = {"error": response.text}

    if response.status_code >= 400:
        raise WhatsAppGatewayError(payload.get("error") or f"HTTP {response.status_code}")

    return payload


def send_whatsapp_review_message(
    *,
    job_id: int,
    phone_e164: str,
    customer_name: str,
    business_name: str | None,
    google_review_url: str | None,
) -> dict:
    message = (
        f"Hola {customer_name}, gracias por tu visita"
        f"{' a ' + business_name if business_name else ''}. "
        f"¿Nos dejas una reseña aquí? {google_review_url}"
    )

    response = requests.post(
        f"{WHATSAPP_GATEWAY_URL}/messages/send",
        json={
            "job_id": job_id,
            "to": phone_e164,
            "message": message,
        },
        headers=_headers(),
        timeout=45,
    )

    try:
        payload = response.json()
    except Exception:
        payload = {"error": response.text}

    if response.status_code >= 400:
        raise WhatsAppGatewayError(payload.get("error") or f"HTTP {response.status_code}")

    return payload