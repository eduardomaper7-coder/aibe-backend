from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
import httpx
from .config import get_settings
from .supabase_client import supabase

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
    y nosotros guardamos ese user_id en el parámetro state.
    """
    url = get_google_auth_url(state=user_id)
    return RedirectResponse(url)


@router.get("/callback")
async def google_callback(code: str, state: str, response: Response):
    """
    Google redirige aquí con ?code=...&state=<user_id>
    """
    user_id = state  # en el MVP asumimos que state = uuid de Supabase

    # Intercambiamos code por tokens
    data = {
        "code": code,
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "redirect_uri": settings.google_redirect_uri,
        "grant_type": "authorization_code",
    }

    async with httpx.AsyncClient() as client:
        token_resp = await client.post("https://oauth2.googleapis.com/token", data=data)
    if token_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Error al intercambiar el código por tokens")

    tokens = token_resp.json()
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    id_token = tokens.get("id_token")

    if not refresh_token:
        # Google a veces no envía refresh_token si el usuario ya dio consentimiento.
        # En producción deberías gestionar esto (pedir prompt=consent, etc.)
        raise HTTPException(status_code=400, detail="No se recibió refresh_token")

    # Obtenemos email e id de Google con id_token
    async with httpx.AsyncClient() as client:
        userinfo_resp = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )

    if userinfo_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="No se pudieron obtener los datos del usuario")

    userinfo = userinfo_resp.json()
    email = userinfo.get("email")
    google_user_id = userinfo.get("sub")

    # Guardamos/actualizamos conexión en Supabase
    supabase.table("google_connections").upsert({
        "user_email": email,
        "google_user_id": google_user_id,
        "refresh_token": refresh_token,
        "scope": settings.google_oauth_scopes,
        "user_id": user_id,
    }, on_conflict="user_email").execute()

    # Puedes poner cookies/sesión aquí si quieres
    return RedirectResponse(settings.frontend_post_login_url)
