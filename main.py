# aibe-backend/main.py
import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, String, Boolean, DateTime, Text, select, UniqueConstraint
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

# =========================
# Config desde variables .env
# =========================
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
# Puede ser una lista separada por comas: "http://localhost:3000,https://aibetech.es"
FRONTEND_ORIGIN_RAW = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./aibe.db")

# Scopes (espacios separados). Ej: "openid email profile https://www.googleapis.com/auth/business.manage"
SCOPES_STR = os.getenv(
    "GOOGLE_OAUTH_SCOPES",
    "openid email profile https://www.googleapis.com/auth/business.manage",
)
SCOPES = " ".join(SCOPES_STR.split())  # sanea espacios repetidos

if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    print("⚠️  Faltan GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET en .env")

# =========================
# DB (SQLAlchemy async)
# =========================
Base = declarative_base()
engine = create_async_engine(DATABASE_URL, future=True, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

class GoogleOAuth(Base):
    __tablename__ = "google_oauth"
    __table_args__ = (UniqueConstraint("email", name="uq_google_oauth_email"),)

    email = Column(String(320), primary_key=True)  # email en minúsculas
    google_account_id = Column(String(64), nullable=True)  # "sub" del id_token
    connected = Column(Boolean, default=False, nullable=False)

    access_token = Column(Text, nullable=True)
    refresh_token = Column(Text, nullable=True)
    token_type = Column(String(32), nullable=True)
    scope = Column(Text, nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)  # UTC

    id_token = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc), nullable=False)

# =========================
# Utilidades
# =========================
def sign_state(email: str, ttl_sec: int = 600) -> str:
    exp = int(time.time()) + ttl_sec
    payload = json.dumps({"email": email, "exp": exp}).encode("utf-8")
    sig = hmac.new(SECRET_KEY.encode("utf-8"), payload, hashlib.sha256).digest()
    raw = base64.urlsafe_b64encode(payload + b"." + sig).decode("ascii")
    return raw

def verify_state(state: str) -> str:
    try:
        raw = base64.urlsafe_b64decode(state.encode("ascii"))
        payload, sig = raw.rsplit(b".", 1)
        exp_data = json.loads(payload.decode("utf-8"))
        expected = hmac.new(SECRET_KEY.encode("utf-8"), payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("invalid signature")
        if int(exp_data["exp"]) < int(time.time()):
            raise ValueError("expired")
        email = (exp_data["email"] or "").lower()
        if not email:
            raise ValueError("missing email")
        return email
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid state: {e}")

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def parse_origins(s: str) -> list[str]:
    """Convierte 'a,b,c' en lista de orígenes sin barras finales."""
    out = []
    for part in s.split(","):
        o = part.strip()
        if o.endswith("/"):
            o = o[:-1]
        if o:
            out.append(o)
    return out

# =========================
# App
# =========================
app = FastAPI(title="AIBE Backend", version="1.0.0")

# ---- CORS: múltiples orígenes (localhost + producción) ----
ALLOWED_ORIGINS = parse_origins(FRONTEND_ORIGIN_RAW)
if not ALLOWED_ORIGINS:
    ALLOWED_ORIGINS = ["http://localhost:3000"]

# Opción A: lista exacta (recomendada)
USE_REGEX = False
# Opción B: regex flexible (activar si quieres cubrir subdominios)
# USE_REGEX = True
# ORIGIN_REGEX = r"https?://(localhost(:\d+)?|([a-zA-Z0-9-]+\.)*aibetech\.es)$"

print(f"🔐 CORS allow_origins = {ALLOWED_ORIGINS}")

if USE_REGEX:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["GET","POST","OPTIONS"],
        allow_headers=["*"],
    )
else:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,   # No usar "*" con allow_credentials=True
        allow_credentials=True,
        allow_methods=["GET","POST","OPTIONS"],
        allow_headers=["*"],
    )

async def get_db() -> AsyncSession:
    async with async_session() as s:
        yield s

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ DB lista")

# =========================
# Rutas
# =========================

@app.get("/health")
async def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/auth/google/login")
async def google_login(email: str = Query(..., description="Email del usuario que inicia OAuth")):
    email = email.lower().strip()
    if not email:
        raise HTTPException(400, "email requerido")

    state = sign_state(email)
    scope_param = urllib.parse.quote(SCOPES, safe="")
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(GOOGLE_REDIRECT_URI, safe='')}"
        f"&response_type=code"
        f"&scope={scope_param}"
        f"&access_type=offline"
        f"&include_granted_scopes=true"
        f"&prompt=consent"
        f"&state={urllib.parse.quote(state, safe='')}"
    )
    return RedirectResponse(auth_url, status_code=302)

@app.get("/auth/google/callback")
async def google_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    if error:
        html = f"""
        <script>
          window.opener && window.opener.postMessage({{ type:'oauth-complete', ok:false, error: {json.dumps(error)} }}, '*');
          window.close();
        </script>
        """
        return HTMLResponse(html)

    if not code or not state:
        raise HTTPException(400, "Falta code/state")

    email = verify_state(state)

    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    if r.status_code != 200:
        raise HTTPException(400, f"Token exchange failed: {r.text}")

    tok = r.json()
    access_token = tok.get("access_token")
    refresh_token = tok.get("refresh_token")  # puede venir vacío si ya concedido antes
    token_type = tok.get("token_type")
    scope = tok.get("scope")
    expires_in = tok.get("expires_in", 3600)
    id_token = tok.get("id_token")

    google_account_id = None
    if id_token:
        try:
            payload = id_token.split(".")[1] + "=="
            payload_bytes = base64.urlsafe_b64decode(payload.encode("ascii"))
            sub = json.loads(payload_bytes.decode("utf-8")).get("sub")
            if sub:
                google_account_id = str(sub)
        except Exception:
            pass

    expires_at = utcnow() + timedelta(seconds=int(expires_in))
    existing = (await db.execute(select(GoogleOAuth).where(GoogleOAuth.email == email))).scalars().first()
    if existing:
        existing.connected = True
        existing.access_token = access_token or existing.access_token
        existing.refresh_token = refresh_token or existing.refresh_token
        existing.token_type = token_type
        existing.scope = scope
        existing.expires_at = expires_at
        existing.id_token = id_token or existing.id_token
        existing.google_account_id = google_account_id or existing.google_account_id
    else:
        rec = GoogleOAuth(
            email=email,
            connected=True,
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=token_type,
            scope=scope,
            expires_at=expires_at,
            id_token=id_token,
            google_account_id=google_account_id,
        )
        db.add(rec)
    await db.commit()

    html = """
    <html><body>
    <script>
      try {
        window.opener && window.opener.postMessage({ type:'oauth-complete', ok:true }, '*');
      } catch (e) {}
      window.close();
    </script>
    Conexión completada. Puedes cerrar esta ventana.
    </body></html>
    """
    return HTMLResponse(html)

@app.get("/auth/google/status")
async def google_status(email: str = Query(...), db: AsyncSession = Depends(get_db)):
    email = email.lower().strip()
    if not email:
        raise HTTPException(400, "email requerido")
    row = (await db.execute(select(GoogleOAuth).where(GoogleOAuth.email == email))).scalars().first()
    return {"connected": bool(row and row.connected)}

@app.get("/me/gbp/locations")
async def gbp_locations(email: str = Query(...), db: AsyncSession = Depends(get_db)):
    email = email.lower().strip()
    row = (await db.execute(select(GoogleOAuth).where(GoogleOAuth.email == email))).scalars().first()
    if not row or not row.connected:
        raise HTTPException(status_code=401, detail="Not connected")
    return {"locations": [], "connected": True}
