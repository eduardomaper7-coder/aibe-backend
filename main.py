import os, time, json
from typing import Optional, Dict, Any, List
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
import httpx
# --- NUEVO: imports para cache/locks ---
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

from gbp_client import GBPClient

# ---------------------- CONFIG ----------------------
load_dotenv()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "https://aibetech.es")  # para CORS y postMessage
GOOGLE_SCOPES = os.getenv(
    "GOOGLE_OAUTH_SCOPES",
    "https://www.googleapis.com/auth/business.manage openid email profile",
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./aibe.db")

# Supabase (REST)
SUPABASE_URL = os.getenv("SUPABASE_URL")  # https://xxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_REST_URL = (SUPABASE_URL.rstrip("/") + "/rest/v1") if SUPABASE_URL else None

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET):
    raise RuntimeError("Faltan GOOGLE_CLIENT_ID o GOOGLE_CLIENT_SECRET en .env")
if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
    raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env")

STATE_SIGNER = URLSafeSerializer(os.getenv("SECRET_KEY", "dev-secret"), salt="google-oauth")

BUSINESS_API = "https://mybusinessaccountmanagement.googleapis.com/v1"

GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

# --- NUEVO: cache y límites por usuario ---
LOC_TTL_SECONDS = 5 * 60         # 5 min datos frescos
STALE_GRACE_SECONDS = 10 * 60     # 10 min se permite servir stale si Google limita
COOLDOWN_DEFAULT = 30             # si no viene Retry-After, cooldown 30s

# email -> {"data": {...}, "fetched_at": datetime}
LOCATIONS_CACHE: dict[str, dict[str, Any]] = {}

# email -> timestamp (epoch) hasta cuándo hay cooldown
USER_COOLDOWN_UNTIL: dict[str, float] = {}

# locks por email para evitar martillazos concurrentes
USER_LOCKS: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


# ---------------------- Supabase REST helpers ----------------------
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

async def upsert_google_connection_by_email(email: str, refresh_token: str, scope: Optional[str]):
    """
    Guarda/actualiza la conexión Google por email en tu tabla 'google_connections'
    (asumimos que existe con columna única user_email).
    """
    url = f"{SUPABASE_REST_URL}/google_connections?on_conflict=user_email"
    payload = {
        "user_email": email,
        "refresh_token": refresh_token,
        "scope": scope,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    headers = sb_headers() | {"Prefer": "resolution=merge-duplicates"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()

async def is_connected_by_email(email: str) -> bool:
    url = f"{SUPABASE_REST_URL}/google_connections"
    params = {
        "user_email": f"eq.{email}",
        "refresh_token": "not.is.null",
        "select": "refresh_token",
        "limit": 1,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=sb_headers(), params=params)
        r.raise_for_status()
        data = r.json()
        if not data:
            return False
        # asegura que el refresh_token no esté vacío
        return bool((data[0].get("refresh_token") or "").strip())


async def get_connection_by_email(email: str) -> Optional[Dict[str, Any]]:
    url = f"{SUPABASE_REST_URL}/google_connections"
    params = {"user_email": f"eq.{email}", "select": "user_email,refresh_token,scope", "limit": 1}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=sb_headers(), params=params)
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None

# ---- Upserts de negocio (ubicaciones/reseñas) ----
async def upsert_locations(rows: List[Dict[str, Any]]):
    if not rows:
        return
    url = f"{SUPABASE_REST_URL}/gbp_locations?on_conflict=name"
    headers = sb_headers() | {"Prefer": "resolution=merge-duplicates"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, json=rows)
        r.raise_for_status()

async def upsert_reviews(rows: List[Dict[str, Any]]):
    if not rows:
        return
    url = f"{SUPABASE_REST_URL}/gbp_reviews?on_conflict=review_id"
    headers = sb_headers() | {"Prefer": "resolution=merge-duplicates"}
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers, json=rows)
        r.raise_for_status()

async def upsert_review_replies(rows: List[Dict[str, Any]]):
    if not rows:
        return
    url = f"{SUPABASE_REST_URL}/gbp_review_replies?on_conflict=review_id"
    headers = sb_headers() | {"Prefer": "resolution=merge-duplicates"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, json=rows)
        r.raise_for_status()

# ---------------------- Access token via refresh token ----------------------
async def exchange_refresh_token(refresh_token: str) -> Dict[str, Any]:
    """
    Intercambia el refresh_token por un access_token válido.
    """
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(GOOGLE_TOKEN_ENDPOINT, data=data)
        # Si Google devuelve 400, suele ser refresh_token caducado o inválido
        if r.status_code >= 400:
            raise HTTPException(status_code=401, detail=f"Error al refrescar token: {r.text}")
        return r.json()  # {access_token, expires_in, scope, token_type}

# ---------------------- Llamadas directas a GBP ----------------------
async def gbp_get(access_token: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{BUSINESS_API}/{path}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=headers, params=params)
        if r.status_code >= 400:
            # Propaga status, body y Retry-After (si lo hay)
            detail = {
                "status": r.status_code,
                "url": url,
                "body": None,
                "retry_after": r.headers.get("Retry-After"),  # <-- clave
            }
            try:
                detail["body"] = r.json()
            except Exception:
                detail["body"] = r.text
            raise HTTPException(r.status_code, detail)
        return r.json()

# ---------------------- DATABASE (mínima local) ----------------------
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
        "https://aibetech.es",
        "https://www.aibetech.es",   # <--- añadido
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

# ---------------------- HELPERS GBP (legacy deps) ----------------------
async def get_token(session=Depends(Session)) -> OAuthToken:
    # Mantengo este helper para no romper tus rutas existentes.
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
    Inicia OAuth con Google. Firmamos el email del usuario en 'state' para recuperarlo en el callback.
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
        # 0) Intercambiar el código por tokens de Google
        token = await oauth.google.authorize_access_token(request)
        print("Token keys:", list(token.keys()))

        # 1) validar state y extraer email
        raw_state = request.query_params.get("state")
        data = STATE_SIGNER.loads(raw_state)  # lanza si se manipuló
        email = (data.get("email") or "").strip().lower()
        if not email:
            return HTMLResponse("<p>Missing email</p>", status_code=400)

        # 2) tokens (refactor)
        access_token = token.get("access_token")
        refresh_token = token.get("refresh_token")  # puede venir None o faltar

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

        # 3) Guardar/actualizar conexión en Supabase por email
        #    Solo si realmente tenemos refresh_token NO vacío
        if refresh_token and str(refresh_token).strip():
            await upsert_google_connection_by_email(email, refresh_token, token.get("scope"))
        else:
            print(f"[WARN] Google no devolvió refresh_token para {email}; no se guardará conexión.")

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
    Devuelve si el usuario (por email) ya tiene conexión guardada en Supabase.
    """
    email = (body.get("email") or "").strip().lower()
    if not email:
        return {"connected": False}
    connected = await is_connected_by_email(email)
    return {"connected": connected}

# ---------------------- NUEVOS ENDPOINTS CLAVE ----------------------
@app.get("/me/gbp/locations")
async def list_my_locations(email: str = Query(..., min_length=3)):
    """
    Lista TODAS las ubicaciones GBP del usuario (por email),
    con cache por 5 min y cooldown si Google limita (429).
    """
    email = email.strip().lower()
    conn = await get_connection_by_email(email)
    if not conn or not conn.get("refresh_token"):
        raise HTTPException(status_code=401, detail="Usuario sin conexión Google válida.")

    now = datetime.utcnow()
    lock = USER_LOCKS[email]

    # Helper para responder con cache (fresco o stale)
    def respond_with_cache(status_code: int = 200, retry_after: Optional[int] = None, stale: bool = False):
        cached = LOCATIONS_CACHE.get(email)
        payload = {
            "items": cached["data"]["items"] if cached else [],
            "fromCache": True,
            "stale": stale,
            "fetchedAt": cached["fetched_at"].isoformat() if cached else None,
        }
        headers = {}
        if retry_after is not None:
            headers["Retry-After"] = str(retry_after)
        return JSONResponse(payload, status_code=status_code, headers=headers)

    # 1) Si hay cooldown activo y tenemos cache (aunque sea stale), servimos cache
    cooldown_until = USER_COOLDOWN_UNTIL.get(email)
    if cooldown_until and time.time() < cooldown_until:
        if email in LOCATIONS_CACHE:
            # tiempo restante aproximado
            retry_after = max(1, int(cooldown_until - time.time()))
            return respond_with_cache(status_code=200, retry_after=retry_after, stale=True)
        # sin cache: caemos a refresh, pero si Google devuelve 429 volveremos con 429

    # 2) Si tenemos cache fresca, devolvemos y listo
    cached = LOCATIONS_CACHE.get(email)
    if cached and (now - cached["fetched_at"]).total_seconds() < LOC_TTL_SECONDS:
        return JSONResponse({ "items": cached["data"]["items"], "fromCache": True, "stale": False, "fetchedAt": cached["fetched_at"].isoformat() }, status_code=200)

    # 3) Refresco protegido por lock por usuario
    async with lock:
        # re-chequea cache por si otro coroutine ya la refrescó
        cached = LOCATIONS_CACHE.get(email)
        if cached and (datetime.utcnow() - cached["fetched_at"]).total_seconds() < LOC_TTL_SECONDS:
            return JSONResponse({ "items": cached["data"]["items"], "fromCache": True, "stale": False, "fetchedAt": cached["fetched_at"].isoformat() }, status_code=200)

        # refrescar tokens
        try:
            token = await exchange_refresh_token(conn["refresh_token"])
            access_token = token["access_token"]
        except HTTPException as e:
            # refresh inválido → borra cache y retorna 401
            LOCATIONS_CACHE.pop(email, None)
            raise

        # 4) Llamadas a Google con control de errores/429
        try:
            items: List[Dict[str, Any]] = []
            accounts_resp = await gbp_get(access_token, "accounts")
            for acc in accounts_resp.get("accounts", []):
                acc_name = acc["name"]
                next_token = None
                while True:
                    params = {"pageSize": 100}
                    if next_token:
                        params["pageToken"] = next_token
                    locs_resp = await gbp_get(access_token, f"{acc_name}/locations", params=params)
                    locs = locs_resp.get("locations", [])
                    for l in locs:
                        items.append({
                            "account": acc_name,
                            "location": l["name"],
                            "title": l.get("title")
                        })
                    next_token = locs_resp.get("nextPageToken")
                    if not next_token:
                        break

            # guarda cache y upsert a Supabase
            LOCATIONS_CACHE[email] = {
                "data": {"items": items},
                "fetched_at": datetime.utcnow(),
            }
            # upsert asíncrono sin bloquear la respuesta
            asyncio.create_task(upsert_locations([{"name": it["location"], "title": it.get("title")} for it in items]))

            return JSONResponse({ "items": items, "fromCache": False, "stale": False, "fetchedAt": LOCATIONS_CACHE[email]["fetched_at"].isoformat() }, status_code=200)

        except HTTPException as e:
            # Si Google limita (429) → establece cooldown y sirve cache si hay
            if e.status_code == 429:
                # establece cooldown (usa Retry-After si viene en el texto, si no COOLDOWN_DEFAULT)
                retry_after = COOLDOWN_DEFAULT
                try:
                    # intenta extraer Retry-After del mensaje (si lo incluyes en e.detail)
                    retry_after = int(json.loads(str(e.detail)).get("retry_after", COOLDOWN_DEFAULT))  # best effort
                except Exception:
                    pass
                USER_COOLDOWN_UNTIL[email] = time.time() + retry_after

                if email in LOCATIONS_CACHE:
                    return respond_with_cache(status_code=200, retry_after=retry_after, stale=True)
                # sin cache: devuelve 429 con Retry-After
                return JSONResponse({"detail": "Upstream rate limited", "retry_after": retry_after}, status_code=429, headers={"Retry-After": str(retry_after)})

            # Otros errores de Google → sirve cache si existe (stale), si no propaga
            if email in LOCATIONS_CACHE:
                return respond_with_cache(status_code=200, stale=True)
            raise


# ---------------------- GBP ROUTES (ejemplo, legado) ----------------------
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






