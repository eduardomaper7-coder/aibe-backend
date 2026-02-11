from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
import httpx

from sqlalchemy.orm import Session

from app.db import get_db
from app.models import GoogleOAuth
from .config import get_settings


router = APIRouter()
settings = get_settings()


def get_google_auth_url(state: str) -> str:
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": settings.google_oauth_scopes,
        "access_type": "offline",
        "prompt": "consent",  # para conseguir refresh_token siempre
        "state": state,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)


@router.get("/login")
async def google_login(user_id: str):
    """
    El front llama a /auth/google/login?user_id=<uuid>
    """
    url = get_google_auth_url(state=user_id)
    return RedirectResponse(url)


@router.get("/callback")
async def google_callback(
    code: str,
    state: str,
    db: Session = Depends(get_db),
):
    """
    Google redirige aquí con ?code=...&state=<user_id>
    """

    # 1️⃣ Intercambiar code → tokens
    data = {
        "code": code,
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "redirect_uri": settings.google_redirect_uri,
        "grant_type": "authorization_code",
    }

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data=data,
            timeout=20
        )

    if token_resp.status_code != 200:
        raise HTTPException(400, "Error intercambiando código")

    tokens = token_resp.json()

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")

    if not refresh_token:
        raise HTTPException(400, "No se recibió refresh_token")

    # 2️⃣ Obtener userinfo
    async with httpx.AsyncClient() as client:
        userinfo_resp = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )

    if userinfo_resp.status_code != 200:
        raise HTTPException(400, "Error userinfo")

    userinfo = userinfo_resp.json()

    email = (userinfo.get("email") or "").lower().strip()
    google_id = userinfo.get("sub")

    if not email:
        raise HTTPException(400, "No email")

    # 3️⃣ Guardar en Postgres
    row = db.query(GoogleOAuth).filter_by(email=email).first()

    if row:
        row.refresh_token = refresh_token
        row.google_user_id = google_id
        row.scope = settings.google_oauth_scopes
        row.connected = True
    else:
        row = GoogleOAuth(
            email=email,
            refresh_token=refresh_token,
            google_user_id=google_id,
            scope=settings.google_oauth_scopes,
        )
        db.add(row)

    db.commit()

    # 4️⃣ Volver al frontend
    return RedirectResponse(settings.frontend_post_login_url)
