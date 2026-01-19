import json
from pathlib import Path
from typing import List, Dict
import httpx

from .config import get_settings

settings = get_settings()


async def get_access_token_from_refresh(refresh_token: str) -> str:
    data = {
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://oauth2.googleapis.com/token", data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


async def list_gbp_locations(access_token: str) -> List[Dict]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.google_api_base_url}/accounts",
            headers={"Authorization": f"Bearer {access_token}"}
        )
    resp.raise_for_status()
    # Esto es simplificado; en realidad hay que navegar cuentas → locations
    accounts = resp.json().get("accounts", [])
    locations = []
    for account in accounts:
        account_name = account["name"]
        async with httpx.AsyncClient() as client:
            loc_resp = await client.get(
                f"{settings.google_api_base_url}/{account_name}/locations",
                headers={"Authorization": f"Bearer {access_token}"}
            )
        loc_resp.raise_for_status()
        locations.extend(loc_resp.json().get("locations", []))
    return locations


async def list_gbp_reviews(access_token: str, location_name: str) -> List[Dict]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.google_api_base_url}/{location_name}/reviews",
            headers={"Authorization": f"Bearer {access_token}"}
        )
    resp.raise_for_status()
    return resp.json().get("reviews", [])


def load_mock_reviews() -> List[Dict]:
    path = Path(__file__).parent / "mock_reviews.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
