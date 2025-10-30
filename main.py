import os, time, json
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Request
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

from gbp_client import GBPClient

# ---------------------- CONFIG ----------------------
load_dotenv()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
FRONTEND_POST_LOGIN_URL = os.getenv("FRONTEND_POST_LOGIN_URL", "http://localhost:3000")
GOOGLE_SCOPES = os.getenv(
    "GOOGLE_OAUTH_SCOPES",
    "https://www.googleapis.com/auth/business.manage openid email profile",
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./aibe.db")

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET):
    raise RuntimeError("Faltan GOOGLE_CLIENT_ID o GOOGLE_CLIENT_SECRET en .env")

# ---------------------- DATABASE ----------------------
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
        "https://aibetech.es",  # dominio del front
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

# ---------------------- ROUTES ----------------------
@app.get("/auth/google/login")
async def google_login(request: Request):
    # Redirige a Google OAuth
    return await oauth.google.authorize_redirect(
        request,
        redirect_uri=GOOGLE_REDIRECT_URI,
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )

@app.get("/auth/google/callback")
async def google_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        print("Token keys:", list(token.keys()))

        access_token = token.get("access_token")
        refresh_token = token.get("refresh_token")
        expires_at = float(token.get("expires_at", time.time() + 3600))

        if not access_token:
            html = """
<!doctype html><html><body>
<script>
  try {
    window.opener && window.opener.postMessage({type:'oauth-complete', ok:false, error:'sin_access_token'}, 'https://aibetech.es');
  } catch (e) {}
  window.close();
</script>
<p>Error: Google no devolvió access_token</p>
</body></html>"""
            return HTMLResponse(html, status_code=400)

        # Guarda/actualiza token
        async with Session() as s:
            obj = await s.get(OAuthToken, 1)
            if not obj:
                obj = OAuthToken(
                    id=1,
                    provider="google",
                    access_token=access_token,
                    refresh_token=refresh_token or "",
                    expires_at=expires_at,
                )
                s.add(obj)
            else:
                obj.access_token = access_token
                obj.refresh_token = refresh_token or obj.refresh_token
                obj.expires_at = expires_at
            await s.commit()

        # ✅ ÉXITO: avisa al opener (frontend) y cierra el popup
        html_ok = """
<!doctype html><html><body>
<script>
  try {
    window.opener && window.opener.postMessage({type:'oauth-complete', ok:true}, 'https://aibetech.es');
  } catch (e) {}
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
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:false, error:{err}}}, 'https://aibetech.es');
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
    window.opener && window.opener.postMessage({{type:'oauth-complete', ok:false, error:{err}}}, 'https://aibetech.es');
  }} catch (e) {{}}
  window.close();
</script>
<p>Error: {str(e)}</p>
</body></html>"""
        return HTMLResponse(html_exc, status_code=500)

# ---------------------- GBP ROUTES ----------------------
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



