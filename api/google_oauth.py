from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
import httpx

from sqlalchemy.orm import Session

from app.db import get_db
from app.models import GoogleOAuth
from api.config import get_settings


router = APIRouter(prefix="/auth/google", tags=["auth-google"])
settings = get_settings()

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"


def _require(value: str, name: str) -> str:
    v = (value or "").strip()
    if not v:
        raise HTTPException(status_code=500, detail=f"Falta configuración: {name}")
    return v


def get_google_auth_url(state: str) -> str:
    client_id = _require(getattr(settings, "GOOGLE_CLIENT_ID", ""), "GOOGLE_CLIENT_ID")
    redirect_uri = _require(getattr(settings, "GOOGLE_REDIRECT_URI", ""), "GOOGLE_REDIRECT_URI")

    # scopes: si no existe en config, usa uno por defecto
    scopes = getattr(settings, "GOOGLE_OAUTH_SCOPES", "") or "openid email profile"

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scopes,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return GOOGLE_AUTH_URL + "?" + urlencode(params)


@router.get("/login")
async def google_login(user_id: str = Query(..., description="Identificador que meteremos en state")):
    url = get_google_auth_url(state=user_id)
    return RedirectResponse(url)


@router.get("/callback")
async def google_callback(
    code: str,
    state: str,
    db: Session = Depends(get_db),
):
    client_id = _require(getattr(settings, "GOOGLE_CLIENT_ID", ""), "GOOGLE_CLIENT_ID")
    client_secret = _require(getattr(settings, "GOOGLE_CLIENT_SECRET", ""), "GOOGLE_CLIENT_SECRET")
    redirect_uri = _require(getattr(settings, "GOOGLE_REDIRECT_URI", ""), "GOOGLE_REDIRECT_URI")

    scopes = getattr(settings, "GOOGLE_OAUTH_SCOPES", "") or "openid email profile"
    frontend_post_login_url = getattr(settings, "FRONTEND_POST_LOGIN_URL", "") or getattr(
        settings, "FRONTEND_POST_LOGIN_UR", ""  # por si lo tienes mal escrito en Railway
    )

    # 1) code -> tokens
    data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }

    async with httpx.AsyncClient(timeout=20) as client:
        token_resp = await client.post(GOOGLE_TOKEN_URL, data=data)

    if token_resp.status_code != 200:
        raise HTTPException(
            status_code=400,
            detail={"msg": "Error intercambiando code", "data": token_resp.text},
        )

    tokens = token_resp.json()
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="No se recibió access_token")

    # 2) userinfo
    async with httpx.AsyncClient(timeout=20) as client:
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if userinfo_resp.status_code != 200:
        raise HTTPException(
            status_code=400,
            detail={"msg": "Error userinfo", "data": userinfo_resp.text},
        )

    userinfo = userinfo_resp.json()
    email = (userinfo.get("email") or "").lower().strip()
    google_id = userinfo.get("sub")

    if not email:
        raise HTTPException(status_code=400, detail="No se pudo obtener email del userinfo")

    # 3) Guardar en Postgres (si no viene refresh_token, NO lo machacamos)
    row = db.query(GoogleOAuth).filter_by(email=email).first()

    if row:
        if refresh_token:
            row.refresh_token = refresh_token
        row.google_user_id = google_id
        row.scope = scopes
        row.connected = True
    else:
        if not refresh_token:
            # primer login y no tenemos refresh => no podemos hacer sync
            if frontend_post_login_url:
                return RedirectResponse(f"{frontend_post_login_url}?error=no_refresh_token")
            raise HTTPException(status_code=400, detail="No refresh_token en primer login")

        row = GoogleOAuth(
            email=email,
            refresh_token=refresh_token,
            google_user_id=google_id,
            scope=scopes,
            connected=True,
        )
        db.add(row)

    db.commit()

    # 4) Volver al frontend
    if not frontend_post_login_url:
        # si no hay front, no rompas: devuelve algo útil
        return {"ok": True, "email": email, "connected": True, "state": state}

    return RedirectResponse(frontend_post_login_url)
