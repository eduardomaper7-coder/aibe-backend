# gbp_client.py
import time, requests
from dataclasses import dataclass
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

BUSINESS_API = "https://businessprofile.googleapis.com/v1"

@dataclass
class TokenBundle:
    access_token: str
    refresh_token: str
    expiry_epoch: int  # segundos UNIX

def ensure_valid_token(tb: TokenBundle, client_id: str, client_secret: str) -> TokenBundle:
    # refresca si quedan < 2 min
    if tb.expiry_epoch - int(time.time()) > 120:
        return tb
    creds = Credentials(
        token=tb.access_token,
        refresh_token=tb.refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/business.manage"]
    )
    creds.refresh(Request())
    # google-auth no da expiry_epoch directo, calculamos aprox 55 min
    return TokenBundle(
        access_token=creds.token,
        refresh_token=tb.refresh_token,
        expiry_epoch=int(time.time()) + 3300
    )

def gbp_get(path: str, access_token: str, params: dict | None = None):
    r = requests.get(f"{BUSINESS_API}/{path}", params=params, headers={
        "Authorization": f"Bearer {access_token}"
    })
    r.raise_for_status()
    return r.json()

