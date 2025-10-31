# gbp_client.py
from typing import Any, Dict

class GBPClient:
    def __init__(self, client_id: str, client_secret: str, access_token: str, refresh_token: str, expires_at: float):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_at = expires_at

    async def list_accounts(self) -> Dict[str, Any]:
        return {"accounts": []}

    async def list_locations(self, account_id: str) -> Dict[str, Any]:
        return {"locations": []}

    async def list_reviews(self, account_id: str, location_id: str) -> Dict[str, Any]:
        return {"reviews": []}

    async def update_reply(self, account_id: str, location_id: str, review_id: str, reply_text: str) -> Dict[str, Any]:
        return {"ok": False, "detail": "Not implemented"}

