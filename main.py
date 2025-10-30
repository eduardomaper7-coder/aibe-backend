import os, time, json
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from authlib.integrations.starlette_client import OAuth
from authlib.integrations.base_client.errors import OAuthError
from starlette.responses import JSONResponse, HTMLResponse
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, Text
from starlette.middleware.sessions import SessionMiddleware
from itsdangerous import URLSafeSerializer
from supabase import create_client, Client

from gbp_client import GBPClient

# ---------------------- CONFIG ----------------------
load_dotenv()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "https://aibetech.es")  # para postMessage
GOOGLE_SCOPES = os.getenv(
    "GOOGLE_OAUTH_SCOPES",
    "https://www.googleapis.com/auth/business.manage openid email profile",
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./aibe.db")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET):
    raise RuntimeError("Faltan GOOGLE_CLIENT_ID o GOOGLE_CLIENT_SECRET en .env")
if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
    raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env")

# ---------------------- Supabase (server-side) ----------------------
sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
STATE_SIGNER = URLSafeSerializer(os.getenv("SECRET_KEY", "dev-secret"), salt="google-oauth")

# ---------------------- DATABASE (local, mínimo) ----------------------
class Base(DeclarativeBase):
    pass

class OAuthToken(Base):
    __tablename__ = "oauth_tokens"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(50))
    user_id: Mapped[str] = mapped_column(String(100), default="demo-user")
    access_token: Mapped[str] = mapped_column(Text)
    refresh_token: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[float] = mapped_column(Float)
    account_json: Mapped[str] = mapped_column(Text, default="{}")

engine = create_async_engine(DATABASE_URL, echo=False)
Session = async_sessionmaker(engine, expire_on_commit=False)

# ---------------------- APP ----------------------
app = FastAPI(title="AIBE Backend")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        FRONTEND_ORIGIN,  # dominio del front
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sesiones (necesarias para OAuth)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SECRET_KEY", "dev-secret"),
    same_site="lax",
)

oauth = OAuth()
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": GOOGLE_SCOPES},
)

# ---------------------- HELPERS ----------------------
async def get_token(session=Depends(Session)) -> OAuthToken:
    async with session as s:
        res = await s.get(OAuthToken, 1)
        if not res:
            raise HTTPException(status_code=401, detail="No hay token. Inicia sesión con /auth/google/login")
        return res

async def gbp_client(tok: OAuthToken = Depends(get_token)) -> GBPClient:
    return GBPClient(
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        access_token=tok.access_token,
        refresh_token=tok.refresh_token,
        expires_at=tok.expires_at,
    )

class ReplyBody(BaseModel):
    account_id: str
    location_id: str
    review_id: str
    reply_text: str

# ---------------------- EVENTS ----------------------
@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ---------------------- ROUTES: OAuth ----------------------
@app.get("/auth/google/login")
async def google_login(request: Request, email: str = Query(..., min_length=3)):
    """
    Abre Google OAuth. Firmamos el email en el 'state' para recuperarlo en el callback.
    """
    email = email.strip().lower()
    state = STATE_SIGNER.dumps({"email": email})
    return await oauth.google.authorize_redirect(
        request,
        redirect_uri=GOOGLE_REDIRECT_URI,
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
        state=state,
    )

@app.get("/auth/google/callback")
async def google_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        # 1) validar state y extraer email
        raw_state = request.query_params.get("state")
        data = STATE_SIGNER.loads(raw_state)  # lanza si el state está manipulado
        email = (data.get("email") or "").strip().lower()
        if not email:
            return HTMLResponse("<p>Missing email</p>", status_code=400)

        # 2) tokens
        access_token = token.get("access_token")
        refresh_token = token.get("refresh_token") or ""
        if not access_token:
            html = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:false, error:'sin_access_token'}}, '{FRONTEND_ORIGIN}');
  }} catch (e) {{}}
  window.close();
</script>
<p>Error: Google no devolvió access_token</p>
</body></html>"""
            return HTMLResponse(html, status_code=400)

        # 3) Guardar/actualizar conexión en Supabase por email (clave: user_email)
        sb.table("google_connections").upsert({
            "user_email": email,
            "refresh_token": refresh_token,                 # <- lo importante para no reconectar
            "scope": token.get("scope"),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, on_conflict="user_email").execute()

        # 4) Avisar al opener (frontend) y cerrar el popup
        html_ok = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:true}}, '{FRONTEND_ORIGIN}');
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
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:false, error:{err}}}, '{FRONTEND_ORIGIN}');
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
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:false, error:{err}}}, '{FRONTEND_ORIGIN}');
  }} catch (e) {{}}
  window.close();
</script>
<p>Error: {str(e)}</p>
</body></html>"""
        return HTMLResponse(html_exc, status_code=500)

@app.post("/integrations/google/status")
async def google_status(body: dict):
    """
    Devuelve si el usuario (por email) ya tiene conexión guardada.
    """
    email = (body.get("email") or "").strip().lower()
    if not email:
        return {"connected": False}
    q = sb.table("google_connections").select("user_email").eq("user_email", email).maybe_single().execute()
    return {"connected": q.data is not None}

# ---------------------- GBP ROUTES (ejemplo) ----------------------
@app.get("/accounts")
async def accounts(client: GBPClient = Depends(gbp_client)):
    return await client.list_accounts()

@app.get("/locations")
async def locations(account_id: str, client: GBPClient = Depends(gbp_client)):
    return await client.list_locations(account_id)

@app.get("/reviews")
async def reviews(account_id: str, location_id: str, client: GBPClient = Depends(gbp_client)):
    return await client.list_reviews(account_id, location_id)

@app.post("/reviews/reply")
async def reply(body: ReplyBody, client: GBPClient = Depends(gbp_client)):
    return await client.update_reply(body.account_id, body.location_id, body.review_id, body.reply_text)

@app.get("/health")
async def health():
    return {"ok": True}




