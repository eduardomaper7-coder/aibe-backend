from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import requests
from sqlalchemy.orm import Session
from sqlalchemy import func
import os
import hashlib
from datetime import datetime, timezone


from app.db import get_db
from app.models import ScrapeJob, Review

from app.models import GoogleOAuth  # ✅ nuevo

router = APIRouter(prefix="/gbp", tags=["gbp"])


bearer_scheme = HTTPBearer(auto_error=True)


GOOGLE_ACCOUNTS_URL = "https://mybusinessaccountmanagement.googleapis.com/v1/accounts"
GOOGLE_LOCATIONS_URL = "https://mybusinessbusinessinformation.googleapis.com/v1/{account}/locations"


# ✅ Endpoint correcto (accounts.locations.reviews.list)
GOOGLE_REVIEWS_LIST_URL = "https://mybusiness.googleapis.com/v4/{parent}/reviews"




def get_access_token(creds: HTTPAuthorizationCredentials = Security(bearer_scheme)) -> str:
    # creds.scheme == "Bearer"
    return creds.credentials.strip()




def google_get(url: str, access_token: str, params=None):
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
        timeout=30,
    )


    if r.status_code != 200:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise HTTPException(status_code=r.status_code, detail=detail)


    return r.json()




def star_to_int(star: str | None) -> int:
    m = {"ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5}
    return m.get((star or "").upper(), 0)




def pick_best_location(locations_out: list[dict]) -> dict | None:
    return locations_out[0] if locations_out else None




def location_to_job_url(location_name: str) -> str:
    # ScrapeJob.google_maps_url es NOT NULL: guardamos identificador estable
    return f"gbp://{location_name}"




def location_to_place_key(location_name: str) -> str:
    # ScrapeJob.place_key es NOT NULL
    return f"gbp::{location_name}".lower()




@router.get("/locations")
def list_locations(
    db: Session = Depends(get_db),  # (no lo usamos aquí, pero lo dejo por consistencia si luego guardas)
    access_token: str = Depends(get_access_token),
):
    accounts = google_get(GOOGLE_ACCOUNTS_URL, access_token).get("accounts", []) or []


    locations_out = []
    for acc in accounts:
        acc_name = acc.get("name")  # "accounts/123"
        if not acc_name:
            continue


        locs = google_get(
            GOOGLE_LOCATIONS_URL.format(account=acc_name),
            access_token,
            params={"readMask": "name,title,storefrontAddress"},
        ).get("locations", []) or []


        for loc in locs:
            title = loc.get("title") or "Tu negocio"
            addr = ""


            sa = loc.get("storefrontAddress")
            if sa:
                address_lines = sa.get("addressLines") or []
                line1 = address_lines[0] if address_lines else ""
                addr = " ".join([line1, sa.get("locality", "") or "", sa.get("postalCode", "") or ""]).strip()


            loc_name = loc.get("name")  # normalmente "locations/456"
            if not loc_name:
                continue


            # ✅ devolver siempre accounts/.../locations/...
            full_location_name = f"{acc_name}/{loc_name}" if loc_name.startswith("locations/") else loc_name


            locations_out.append({"name": full_location_name, "title": title, "address": addr})


    return {"locations": locations_out}




@router.post("/auto-job")
def auto_job(
    db: Session = Depends(get_db),
    access_token: str = Depends(get_access_token),
):
    """
    Crea un job automáticamente (elige el mejor negocio) y descarga reseñas.
    ✅ Cambios:
    - Obtiene el email del usuario vía userinfo (OpenID)
    - Guarda el email dentro de place_key para poder reusar luego con /gbp/last-job
    """

    # 0) userinfo -> email (para vincular job al usuario)
    GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
    r_ui = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    if r_ui.status_code != 200:
        try:
            detail = r_ui.json()
        except Exception:
            detail = r_ui.text
        raise HTTPException(status_code=r_ui.status_code, detail=detail)

    ui = r_ui.json() or {}
    email = (ui.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="No se pudo obtener email del usuario (userinfo)")

    # 1) listar accounts
    accounts = google_get(GOOGLE_ACCOUNTS_URL, access_token).get("accounts", []) or []
    if not accounts:
        raise HTTPException(status_code=404, detail="No se encontraron cuentas GBP")

    # 2) listar locations
    locations_out = []
    for acc in accounts:
        acc_name = acc.get("name")
        if not acc_name:
            continue

        locs = google_get(
            GOOGLE_LOCATIONS_URL.format(account=acc_name),
            access_token,
            params={"readMask": "name,title,storefrontAddress"},
        ).get("locations", []) or []

        for loc in locs:
            title = loc.get("title") or "Tu negocio"
            loc_name = loc.get("name")
            if not loc_name:
                continue

            full_location_name = (
                f"{acc_name}/{loc_name}" if loc_name.startswith("locations/") else loc_name
            )
            locations_out.append({"name": full_location_name, "title": title})

    chosen = pick_best_location(locations_out)
    if not chosen:
        raise HTTPException(status_code=404, detail="No se encontraron negocios en esta cuenta")

    location_name = chosen["name"]
    place_title = chosen["title"]

    # 3) crear job (rellenar NOT NULL)
    job = ScrapeJob(
        google_maps_url=location_to_job_url(location_name),
        # ✅ vincula el job al usuario (para poder encontrarlo luego por email)
        place_key=f"user::{email}::{location_to_place_key(location_name)}",
        place_name=place_title,
        actor_id=1,  # MVP: fijo
        status="running",
        apify_run_id=None,
        error=None,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # 4) bajar reseñas (✅ pageSize máx 50)
    saved = 0
    page_token = None

    while True:
        params = {"pageSize": 50, "orderBy": "updateTime desc"}
        if page_token:
            params["pageToken"] = page_token

        data = google_get(
            GOOGLE_REVIEWS_LIST_URL.format(parent=location_name),
            access_token,
            params=params,
        )

        # 🔎 DEBUG DURO – confirma que Google responde algo real
        print("🔵 GOOGLE REVIEWS RAW RESPONSE:", data)
        print("🔵 GOOGLE REVIEWS RESPONSE KEYS:", list(data.keys()))
        print("🔵 GOOGLE REVIEWS COUNT:", len(data.get("reviews", []) or []))
        print("🔵 GOOGLE NEXT PAGE TOKEN:", data.get("nextPageToken"))

        print("📦 Google reviews:", {
            "count": len(data.get("reviews", [])),
            "has_next": bool(data.get("nextPageToken")),
            "location": location_name,
        })

        reviews = data.get("reviews", []) or []
        for r in reviews:
            rating = star_to_int(r.get("starRating"))
            text = (r.get("comment") or "").strip()
            published_at = r.get("createTime") or r.get("updateTime")
            author = ((r.get("reviewer") or {}).get("displayName")) or None

            db.add(
                Review(
                    job_id=job.id,
                    rating=rating,
                    text=text,
                    published_at=published_at,
                    author_name=author,
                )
            )
            saved += 1

        db.commit()

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    job.status = "done"
    db.add(job)
    db.commit()
    
    print("✅ TOTAL SAVED:", saved)

    return {
        "job_id": job.id,
        "status": job.status,
        "reviews_saved": saved,
        "location_name": location_name,
        "place_name": place_title,
        "email": email,  # (opcional, útil para debug)
    }


GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"

def google_get_userinfo(access_token: str) -> dict:
    r = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    if r.status_code != 200:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise HTTPException(status_code=r.status_code, detail=detail)
    return r.json()

@router.get("/last-job")
def last_job(
    db: Session = Depends(get_db),
    access_token: str = Depends(get_access_token),
):
    """
    Devuelve el último job creado por este usuario (email),
    o {job_id: null} si no hay.
    """
    ui = google_get_userinfo(access_token)
    email = (ui.get("email") or "").strip().lower()
    if not email:
        return {"job_id": None}

    # ✅ buscamos el job más reciente de ese email (lo guardaremos en actor_id como hash simple)
    # Si prefieres, crea columna owner_email en ScrapeJob; pero para mínimo cambio usamos place_key.
    key_prefix = f"user::{email}::"

    job = (
        db.query(ScrapeJob)
        .filter(ScrapeJob.place_key.like(key_prefix + "%"))
        .order_by(ScrapeJob.id.desc())
        .first()
    )

    return {"job_id": job.id if job else None}


@router.get("/job-stats/{job_id}")
def job_stats(job_id: int, db: Session = Depends(get_db)):
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    total_reviews = db.query(func.count(Review.id)).filter(Review.job_id == job_id).scalar() or 0

    return {
        "job_id": job.id,
        "status": job.status,
        "place_name": job.place_name,
        "location_name": getattr(job, "google_maps_url", None),
        "reviews_saved": total_reviews,
    }

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

def refresh_access_token(refresh_token: str) -> str:
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Faltan GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET")

    r = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=20,
    )
    data = r.json()
    if r.status_code != 200 or "access_token" not in data:
        raise HTTPException(status_code=400, detail={"msg": "No se pudo refrescar access token", "data": data})
    return data["access_token"]


def extract_location_name_from_job(job: ScrapeJob) -> str | None:
    """
    En auto_job guardas:
      job.google_maps_url = "gbp://{location_name}"
    donde location_name = "accounts/123/locations/456"
    """
    v = (job.google_maps_url or "").strip()
    if v.startswith("gbp://"):
        return v[len("gbp://"):]
    return None


def google_review_uid(r: dict) -> str:
    """
    Intenta sacar un ID estable desde GBP.
    Según la API, a veces viene:
      - r["reviewId"]
      - r["name"]  (resource name)
    Si no viene, cae a hash de contenido.
    """
    rid = (r.get("reviewId") or "").strip()
    if rid:
        return f"reviewId:{rid}"

    name = (r.get("name") or "").strip()
    if name:
        return f"name:{name}"

    # fallback: hash (no perfecto, pero evita duplicados obvios)
    rating = str(r.get("starRating") or "")
    comment = (r.get("comment") or "").strip()
    author = ((r.get("reviewer") or {}).get("displayName") or "").strip()
    t = f"{rating}|{author}|{comment}"
    return "hash:" + hashlib.sha1(t.encode("utf-8")).hexdigest()

def ensure_job_for_email(db: Session, email: str, access_token: str) -> ScrapeJob:
    key_prefix = f"user::{email}::"

    job = (
        db.query(ScrapeJob)
        .filter(ScrapeJob.place_key.like(key_prefix + "%"))
        .order_by(ScrapeJob.id.desc())
        .first()
    )
    if job:
        return job

    # No hay job -> creamos uno “auto” (igual que auto_job)
    accounts = google_get(GOOGLE_ACCOUNTS_URL, access_token).get("accounts", []) or []
    if not accounts:
        raise HTTPException(status_code=404, detail="No se encontraron cuentas GBP")

    locations_out = []
    for acc in accounts:
        acc_name = acc.get("name")
        if not acc_name:
            continue

        locs = google_get(
            GOOGLE_LOCATIONS_URL.format(account=acc_name),
            access_token,
            params={"readMask": "name,title,storefrontAddress"},
        ).get("locations", []) or []

        for loc in locs:
            title = loc.get("title") or "Tu negocio"
            loc_name = loc.get("name")
            if not loc_name:
                continue

            full_location_name = (
                f"{acc_name}/{loc_name}" if loc_name.startswith("locations/") else loc_name
            )
            locations_out.append({"name": full_location_name, "title": title})

    chosen = pick_best_location(locations_out)
    if not chosen:
        raise HTTPException(status_code=404, detail="No se encontraron negocios en esta cuenta")

    location_name = chosen["name"]
    place_title = chosen["title"]

    job = ScrapeJob(
        google_maps_url=location_to_job_url(location_name),  # gbp://accounts/.../locations/...
        place_key=f"user::{email}::{location_to_place_key(location_name)}",
        place_name=place_title,
        actor_id="gbp",
        status="created",
        apify_run_id=None,
        error=None,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job

@router.post("/sync")
def sync_gbp_reviews(
    email: str,
    db: Session = Depends(get_db),
):
    """
    Sincroniza reseñas usando refresh_token guardado en Postgres.
    Uso: POST /gbp/sync?email=usuario@dominio.com
    """

    # -----------------------
    # 0) Validar email
    # -----------------------
    email = (email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email requerido")

    # -----------------------
    # 1) Recuperar refresh_token
    # -----------------------
    oauth = (
        db.query(GoogleOAuth)
        .filter(
            GoogleOAuth.email == email,
            GoogleOAuth.connected == True
        )
        .first()
    )

    if not oauth:
        raise HTTPException(
            status_code=404,
            detail="Este usuario no tiene Google conectado"
        )

    # -----------------------
    # 2) Refrescar access token
    # -----------------------
    access_token = refresh_access_token(oauth.refresh_token)

    # -----------------------
    # 3) Asegurar job
    # -----------------------
    job = ensure_job_for_email(
        db=db,
        email=email,
        access_token=access_token,
    )

    location_name = extract_location_name_from_job(job)

    if not location_name:
        raise HTTPException(
            status_code=500,
            detail="Job sin location_name válido"
        )

    job.status = "running"
    db.add(job)
    db.commit()
    db.refresh(job)

    # -----------------------
    # 4) Cargar reviews existentes (dedupe)
    # -----------------------
    existing_rows = (
        db.query(Review.raw)
        .filter(Review.job_id == job.id)
        .order_by(Review.id.desc())
        .limit(5000)
        .all()
    )

    existing_uids: set[str] = set()

    for (raw,) in existing_rows:
        try:
            existing_uids.add(google_review_uid(raw or {}))
        except Exception:
            pass

    # -----------------------
    # 5) Descargar reseñas
    # -----------------------
    saved = 0
    skipped = 0
    page_token = None

    while True:
        params = {
            "pageSize": 50,
            "orderBy": "updateTime desc",
        }

        if page_token:
            params["pageToken"] = page_token

        data = google_get(
            GOOGLE_REVIEWS_LIST_URL.format(parent=location_name),
            access_token,
            params=params,
        )

        reviews = data.get("reviews", []) or []

        if not reviews:
            break

        for r in reviews:
            uid = google_review_uid(r)

            # Dedupe
            if uid in existing_uids:
                skipped += 1
                continue

            rating = star_to_int(r.get("starRating"))
            text = (r.get("comment") or "").strip()
            published_at = r.get("createTime") or r.get("updateTime")

            author = (
                (r.get("reviewer") or {}).get("displayName")
            ) or None

            review_url = r.get("reviewUrl")

            db.add(
                Review(
                    job_id=job.id,
                    rating=rating,
                    text=text,
                    published_at=published_at,
                    author_name=author,
                    review_url=review_url,
                    raw=r,  # ✅ obligatorio
                )
            )

            existing_uids.add(uid)
            saved += 1

        db.commit()

        page_token = data.get("nextPageToken")

        if not page_token:
            break

        # Seguridad anti-loop infinito
        if saved + skipped > 3000:
            break

    # -----------------------
    # 6) Finalizar job
    # -----------------------
    job.status = "done"
    db.add(job)
    db.commit()

    total_reviews = (
        db.query(func.count(Review.id))
        .filter(Review.job_id == job.id)
        .scalar()
        or 0
    )

    # -----------------------
    # 7) Respuesta
    # -----------------------
    return {
        "email": email,
        "job_id": job.id,
        "location_name": location_name,
        "saved_new_reviews": saved,
        "skipped_existing": skipped,
        "total_reviews_for_job": total_reviews,
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }
