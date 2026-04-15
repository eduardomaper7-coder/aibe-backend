import os
from typing import Optional

import requests


WHATSAPP_GATEWAY_URL = os.getenv("WHATSAPP_GATEWAY_URL", "").rstrip("/")
WHATSAPP_GATEWAY_API_KEY = os.getenv("WHATSAPP_GATEWAY_API_KEY", "")


class WhatsAppGatewayError(Exception):
    pass


def send_whatsapp_review_message(
    *,
    phone_e164: str,
    customer_name: str,
    business_name: Optional[str],
    google_review_url: Optional[str],
) -> dict:
    if not WHATSAPP_GATEWAY_URL:
        raise WhatsAppGatewayError("WHATSAPP_GATEWAY_URL no configurada")

    response = requests.post(
        f"{WHATSAPP_GATEWAY_URL}/send",
        json={
            "phone_e164": phone_e164,
            "customer_name": customer_name,
            "business_name": business_name,
            "google_review_url": google_review_url,
        },
        headers={
            "x-api-key": WHATSAPP_GATEWAY_API_KEY,
            "Content-Type": "application/json",
        },
        timeout=45,
    )

    try:
        payload = response.json()
    except Exception:
        payload = {"error": response.text}

    if response.status_code >= 400:
        raise WhatsAppGatewayError(payload.get("error") or f"HTTP {response.status_code}")

    return payload