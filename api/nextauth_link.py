from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
import requests

from app.db import get_db
from app.models import GoogleOAuth

GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

router = APIRouter(prefix="/auth/nextauth", tags=["auth-nextauth"])


class LinkPayload(BaseModel):
    refresh_token: str
    access_token: str
    scope: str | None = None
    google_user_id: str | None = None


@router.post("/link")
def link_google_from_nextauth(payload: LinkPayload, db: Session = Depends(get_db)):
    refresh_token = (payload.refresh_token or "").strip()
    access_token = (payload.access_token or "").strip()

    if not refresh_token:
        raise HTTPException(status_code=400, detail="refresh_token requerido")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token requerido")

    # Verifica access_token y saca email
    r = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    if r.status_code != 200:
        raise HTTPException(
            status_code=400,
            detail={"msg": "Error userinfo", "data": r.text},
        )

    email = ((r.json() or {}).get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="No se pudo obtener email de userinfo")

    row = db.query(GoogleOAuth).filter_by(email=email).first()
    if row:
        row.refresh_token = refresh_token
        row.google_user_id = payload.google_user_id
        row.scope = payload.scope
        row.connected = True
    else:
        row = GoogleOAuth(
            email=email,
            refresh_token=refresh_token,
            google_user_id=payload.google_user_id,
            scope=payload.scope,
            connected=True,
        )
        db.add(row)

    db.commit()
    return {"ok": True, "email": email, "connected": True}
