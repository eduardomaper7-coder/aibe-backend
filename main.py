# main.py
import os, json, time
from fastapi import FastAPI, Request, Query, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv
from itsdangerous import URLSafeSerializer
from authlib.integrations.starlette_client import OAuth
from authlib.integrations.base_client.errors import OAuthError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# === NUEVO: DB ===
from aibe-backend.db import Base, engine, get_session
from aibe-backend.models import User

load_dotenv()

# === ENV requeridas ===
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")
GOOGLE_SCOPES = os.getenv("GOOGLE_OAUTH_SCOPES", "openid email profile https://www.googleapis.com/auth/business.manage")
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")

STATE_SIGNER = URLSafeSerializer(SECRET_KEY, salt="google-oauth")

app = FastAPI(title="Minimal Google OAuth Flow")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://aibetech.es",
        "https://www.aibetech.es",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, same_site="lax")

oauth = OAuth()
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": GOOGLE_SCOPES},
)

# === Crear tablas al arrancar ===
@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/health")
async def health():
    return {"ok": True}

# ---- Endpoint: estado conexión por email ----
@app.get("/auth/google/status")
async def google_status(email: str = Query(..., min_length=3), session: AsyncSession = Depends(get_session)):
    q = await session.execute(select(User).where(User.email == email.lower()))
    user = q.scalar_one_or_none()
    return {"connected": bool(user and user.google_connected)}

# ---- Inicia OAuth ----
@app.get("/auth/google/login")
async def google_login(request: Request, email: str = Query("", min_length=0)):
    state_payload = {"email": (email or "").strip().lower()}
    state = STATE_SIGNER.dumps(state_payload)
    return await oauth.google.authorize_redirect(
        request,
        redirect_uri=GOOGLE_REDIRECT_URI,
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
        state=state,
    )

# ---- Callback: guarda en BD y cierra popup ----
@app.get("/auth/google/callback")
async def google_callback(request: Request, session: AsyncSession = Depends(get_session)):
    try:
        token = await oauth.google.authorize_access_token(request)
        # Leer state
        raw_state = request.query_params.get("state")
        try:
            state = STATE_SIGNER.loads(raw_state) if raw_state else {}
        except Exception:
            state = {}

        # Datos del usuario
        userinfo = token.get("userinfo") or {}
        email = (userinfo.get("email") or "").lower() if isinstance(userinfo, dict) else None
        sub = userinfo.get("sub") if isinstance(userinfo, dict) else None

        # Preferimos el email del state si existe y es válido
        email = (state.get("email") or email or "").lower()
        if not email:
            raise HTTPException(status_code=400, detail="No se obtuvo email del usuario")

        # Upsert usuario
        q = await session.execute(select(User).where(User.email == email))
        user = q.scalar_one_or_none()
        if not user:
            user = User(email=email)

        user.google_connected = True
        user.google_sub = sub
        user.google_email = email
        user.access_token = token.get("access_token")
        user.refresh_token = token.get("refresh_token") or user.refresh_token  # puede no venir en reconsent
        user.token_type = token.get("token_type")
        # authlib normalmente trae expires_at (epoch) o expires_in
        expires_at = token.get("expires_at")
        if not expires_at and token.get("expires_in"):
            expires_at = int(time.time()) + int(token["expires_in"])
        user.expires_at = expires_at

        session.add(user)
        await session.commit()

        # Popup: notifica al opener y cierra
        html_ok = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage(
      {{ type:'oauth-complete', ok:true, email:{json.dumps(email)} }},
      '{FRONTEND_ORIGIN}'
    );
  }} catch (e) {{}}
  window.close();
</script>
<p>Login completado. Puedes cerrar esta ventana.</p>
</body></html>"""
        return HTMLResponse(html_ok)
    except OAuthError as oe:
        err = json.dumps(str(oe))
        html_err = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage(
      {{ type:'oauth-complete', ok:false, error:{err} }},
      '{FRONTEND_ORIGIN}'
    );
  }} catch (e) {{}}
  window.close();
</script>
<p>Error OAuth: {str(oe)}</p>
</body></html>"""
        return HTMLResponse(html_err, status_code=400)
    except Exception as e:
        err = json.dumps(str(e))
        html_exc = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage(
      {{ type:'oauth-complete', ok:false, error:{err} }},
      '{FRONTEND_ORIGIN}'
    );
  }} catch (e) {{}}
  window.close();
</script>
<p>Error: {str(e)}</p>
</body></html>"""
        return HTMLResponse(html_exc, status_code=500)
