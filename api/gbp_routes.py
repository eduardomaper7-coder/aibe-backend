from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import requests
from sqlalchemy.orm import Session
from sqlalchemy import func


from app.db import get_db
from app.models import ScrapeJob, Review


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
