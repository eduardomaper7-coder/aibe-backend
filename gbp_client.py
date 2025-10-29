import time
import httpx
from typing import Dict, Any, Optional

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GBP_API = "https://mybusiness.googleapis.com/v4"

class GBPClient:
    """Cliente mínimo para Google Business Profile API usando tokens OAuth de usuario."""

    def __init__(self, client_id: str, client_secret: str, access_token: str, refresh_token: str, expires_at: float):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_at = expires_at  # timestamp en segundos

    async def _ensure_token(self) -> None:
        if time.time() < self.expires_at - 60:
            return
        async with httpx.AsyncClient(timeout=30) as client:
            data = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            }
            r = await client.post(GOOGLE_TOKEN_URL, data=data)
            r.raise_for_status()
            tok = r.json()
            self.access_token = tok["access_token"]
            self.expires_at = time.time() + tok.get("expires_in", 3600)

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{GBP_API}{path}", headers=headers, params=params)
            r.raise_for_status()
            return r.json()

    async def _post(self, path: str, json: Dict[str, Any]) -> Dict[str, Any]:
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"{GBP_API}{path}", headers=headers, json=json)
            r.raise_for_status()
            return r.json() if r.text else {}

    async def list_accounts(self) -> Dict[str, Any]:
        return await self._get("/accounts")

    async def list_locations(self, account_id: str, page_size: int = 50, page_token: Optional[str] = None) -> Dict[str, Any]:
        params = {"pageSize": page_size}
        if page_token:
            params["pageToken"] = page_token
        return await self._get(f"/accounts/{account_id}/locations", params=params)

    async def list_reviews(self, account_id: str, location_id: str, page_token: Optional[str] = None) -> Dict[str, Any]:
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token
        return await self._get(f"/accounts/{account_id}/locations/{location_id}/reviews", params=params)

    async def update_reply(self, account_id: str, location_id: str, review_id: str, reply_text: str) -> Dict[str, Any]:
        body = {"comment": reply_text}
        return await self._post(f"/accounts/{account_id}/locations/{location_id}/reviews/{review_id}:updateReply", json=body)
