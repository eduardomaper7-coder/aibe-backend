# main.py
import os, json
from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse
from dotenv import load_dotenv
from itsdangerous import URLSafeSerializer
from authlib.integrations.starlette_client import OAuth
from authlib.integrations.base_client.errors import OAuthError

load_dotenv()

# === ENV requeridas ===
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:8001/auth/google/callback")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")
GOOGLE_SCOPES = os.getenv(
    "GOOGLE_OAUTH_SCOPES",
    "openid email profile https://www.googleapis.com/auth/business.manage",
)
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")

# Firmar/validar el "state" (opcional pero recomendado)
STATE_SIGNER = URLSafeSerializer(SECRET_KEY, salt="google-oauth")

# --- App base ---
app = FastAPI(title="Minimal Google OAuth Flow")

# CORS (ajusta origins a tu frontend)
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

# Sesiones para Authlib
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    same_site="lax",
)

# Authlib OAuth client
oauth = OAuth()
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": GOOGLE_SCOPES},
)

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/auth/google/login")
async def google_login(request: Request, email: str = Query("", min_length=0)):
    """
    Inicia el flujo OAuth con Google.
    Si pasas ?email=..., lo incluimos dentro del 'state' firmado (opcional).
    """
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

@app.get("/auth/google/callback")
async def google_callback(request: Request):
    """
    Callback de Google. No guardamos nada: solo devolvemos una página
    que hace postMessage al opener y se cierra.
    """
    try:
        token = await oauth.google.authorize_access_token(request)
        # Si necesitas leer algo del state:
        raw_state = request.query_params.get("state")
        try:
            state = STATE_SIGNER.loads(raw_state) if raw_state else {}
        except Exception:
            state = {}

        # Éxito: devolvemos ok:true y (opcionalmente) algunos datos básicos
        id_token = token.get("userinfo") or {}
        email = (id_token.get("email") or "").lower() if isinstance(id_token, dict) else None

        html_ok = f"""
<!doctype html><html><body>
<script>
  try {{
    window.opener && window.opener.postMessage(
      {{
        type: 'oauth-complete',
        ok: true,
        email: {json.dumps(email)},
      }},
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







