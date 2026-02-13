
import asyncio
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # ✅ Railway friendly

from fastapi import FastAPI, Depends, HTTPException, Query, Request
import json
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import requests
from supabase_client import supabase
from app.db import Base, engine, get_db

from urllib.parse import urlparse, parse_qs
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI

from collections import defaultdict
from typing import Literal, Optional, List, Dict, Any
from pydantic import BaseModel, HttpUrl
from sqlalchemy.orm import Session
from api.google_oauth import router as google_oauth_router
from app.review_requests import models as _rr_models  # noqa: F401



from app.schemas import ScrapeRequest, ScrapeResponse, JobStatusResponse
from app.models import ScrapeJob, Review
from app.reviews_service import scrape_and_store
from app.models_analysis_cache import AnalysisCache
from app.models_ai_reply_cache import ReviewAIReply

from sqlalchemy import text
from api.gbp_routes import router as gbp_router

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Security, Header


print("DEBUG OPENAI_API_KEY:", "OK" if os.getenv("OPENAI_API_KEY") else "MISSING")




FRONTEND_ORIGIN_RAW = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")



USE_MOCK_GBP = os.getenv("USE_MOCK_GBP", "true").lower() == "true"



# =========================
# App
# =========================
app = FastAPI(title="AIBE Backend", version="1.0.0")
security = HTTPBearer()

@app.get("/")
def root():
    return {"ok": True}

# =========================
# Startup: OpenAI client
# =========================
@app.on_event("startup")
def startup_event():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️  OPENAI_API_KEY no configurada")
        app.state.openai_client = None
    else:
        app.state.openai_client = OpenAI(api_key=api_key)
        print("✅ OpenAI client inicializado")

    # ✅ crea tablas aquí (NO arriba)
    Base.metadata.create_all(bind=engine)
    print("✅ DB ready:", engine.url)

import re



# ✅ CORS SIEMPRE PRIMERO
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 👈 DEV ONLY
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(gbp_router)
app.include_router(google_oauth_router)

from app.review_requests.router import router as review_requests_router
app.include_router(review_requests_router)

# =========================
# Rutas
# =========================

@app.get("/health")
async def health():
    return {"ok": True, "ts": int(time.time())}

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
def resolve_long_google_maps_url_from_place_id(place_id: str, hl: str = "es") -> str:
    """
    Intenta convertir place_id -> URL canónica (normalmente más larga) siguiendo redirects.
    Si falla, devuelve el fallback place_id url.
    """
    fallback = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl={hl}"

    try:
        # Truco: Google suele redirigir a una URL canónica con /place/<slug>/data=!...
        # Usamos un User-Agent "normal" para que no nos dé HTML raro.
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": f"{hl}-{hl.upper()},{hl};q=0.9,en;q=0.7",
        }

        r = requests.get(fallback, headers=headers, timeout=15, allow_redirects=True)

        # r.url es la URL final tras redirects (normalmente la “buena”)
        final_url = (r.url or "").strip()
        if final_url.startswith("http"):
            return final_url

        return fallback
    except Exception:
        return fallback

@app.get("/places/search")
def places_search(q: str = Query(..., min_length=2, description="Nombre del negocio + zona")):
    if not GOOGLE_MAPS_API_KEY:
        raise HTTPException(500, "GOOGLE_MAPS_API_KEY no configurada")

    # 1) Find Place From Text
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
        params={
            "input": q,
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,rating,user_ratings_total",
            "key": GOOGLE_MAPS_API_KEY,
            "language": "es",
        },
        timeout=15,
    )

    data = resp.json()
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        raise HTTPException(400, f"Places error: {status} {data.get('error_message','')}")

    candidates = []
    for c in (data.get("candidates") or [])[:8]:
        place_id = c.get("place_id")
        if not place_id:
            continue

        # 2) Place Details -> obtener URL canónica (MUY importante para scraping completo)
        details = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "fields": "url",
                "key": GOOGLE_MAPS_API_KEY,
                "language": "es",
            },
            timeout=15,
        ).json()

        details_status = details.get("status")
        place_url = None
        if details_status == "OK":
            place_url = (details.get("result") or {}).get("url")

        # Fallback si por lo que sea no devuelve url
        if not place_url:
            place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=es"

        candidates.append({
            "place_id": place_id,
            "name": c.get("name"),
            "address": c.get("formatted_address"),
            "rating": c.get("rating"),
            "user_ratings_total": c.get("user_ratings_total"),
            "google_maps_url": resolve_long_google_maps_url_from_place_id(place_id, hl="es"),
        })

    return {"query": q, "candidates": candidates}




# ======================================
# IA: generar respuesta a reseñas
# ======================================

async def generate_reply(review: dict, openai_client: OpenAI) -> str:
    if openai_client is None:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")


    star = review.get("star_rating", 5)
    comment = review.get("comment", "")
    reviewer = review.get("reviewer_name", "el cliente")

    system_prompt = (
        "Eres un asistente experto en atención al cliente para pequeñas empresas. "
        "Respondes a reseñas de Google en español con un tono humano, cercano y profesional. "
        "Sé breve (3-5 frases), agradecido y, si la reseña es negativa, empático y orientado a solución. "
        "No inventes datos ni promociones agresivas."
    )

    user_prompt = f"""Reseña:
- Estrellas: {star}
- Cliente: {reviewer}
- Comentario: "{comment}"

Redacta la respuesta que pondrá el negocio en su perfil de Google.
"""

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.5,
    )

    return completion.choices[0].message.content.strip()


# ======================================
# Mock de reseñas para pruebas
# ======================================

def load_mock_reviews() -> List[Dict]:
    path = Path(__file__).parent / "mock_reviews.json"
    if not path.exists():
        # mock mínimo por si no has creado el archivo aún
        return [
            {
                "review_id": "mock-1",
                "reviewer_name": "Juan Pérez",
                "star_rating": 5,
                "comment": "Servicio excelente, muy recomendado.",
                "create_time": "2025-01-10T12:00:00Z",
                "update_time": "2025-01-10T12:00:00Z",
                "location_name": "accounts/123456789/locations/987654321",
            }
        ]
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ======================================
# Endpoints de flujo reseñas
# ======================================

@app.post("/reviews/run-once")
async def run_flow_for_user(
    request: Request,   # 👈 ESTO ES CLAVE
    email: str = Query(..., description="Email del dueño del negocio")
):

    """
    MVP:
    - Usamos Supabase.google_connections para encontrar el refresh_token.
    - Leemos reseñas de un mock (USE_MOCK_GBP=true).
    - Generamos respuestas con IA.
    - Guardamos en Supabase.gbp_reviews y Supabase.gbp_review_replies.
    """
    if supabase is None:
        raise HTTPException(500, "Supabase no configurado")

    email = email.lower().strip()
    if not email:
        raise HTTPException(400, "email requerido")

    try:
        # 1) Asegurar que el usuario tiene conexión Google guardada
        conn_resp = (
            supabase.table("google_connections")
            .select("*")
            .eq("user_email", email)
            .single()
            .execute()
        )

        if not conn_resp.data:
            raise HTTPException(
                400, "No hay conexión Google para este email en Supabase"
            )

        refresh_token = conn_resp.data.get("refresh_token")
        if not refresh_token:
            raise HTTPException(
                400, "No hay refresh_token almacenado para este usuario"
            )

        # 2) Obtener owner_id desde profiles (es lo que luego usa /reviews/latest)
        profile_resp = (
            supabase.table("profiles")
            .select("id")
            .eq("email", email)
            .single()
            .execute()
        )
        if not profile_resp.data:
            raise HTTPException(400, "No existe perfil para este email en profiles")

        owner_id = profile_resp.data["id"]

        # 3) Por ahora usamos mock de reseñas (filtradas a últimos 30 días)
        def parse_ts(v: str) -> datetime:
            # Soporta ISO con "Z" al final
            return datetime.fromisoformat(v.replace("Z", "+00:00"))

        all_reviews = load_mock_reviews()
        
        reviews = all_reviews

        # --- FILTRO: solo reseñas de los últimos 30 días ---
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        

        # ---------------------------------------------------

        created_replies: list[dict] = []

        for r in reviews:
            review_id = r["review_id"]
            location_name = r.get("location_name")

            # 3.a) Asegurar que existe la location en gbp_locations
            if location_name:
                (
                    supabase.table("gbp_locations")
                    .upsert(
                        {
                            "name": location_name,
                            "owner_id": owner_id,
                        },
                        on_conflict="name",
                    )
                    .execute()
                )

            # 3.b) Insertar/actualizar reseña en gbp_reviews
            review_payload = {
                "review_id": review_id,
                "reviewer_name": r.get("reviewer_name"),
                "star_rating": r.get("star_rating"),
                "comment": r.get("comment"),
                "create_time": r.get("create_time"),
                "update_time": r.get("update_time"),
                "location_name": location_name,
                "owner_id": owner_id,
                "raw_payload": r,
            }

            (
                supabase.table("gbp_reviews")
                .upsert(review_payload, on_conflict="review_id")
                .execute()
            )

            # 3.c) Generar respuesta IA
            openai_client = request.app.state.openai_client

            reply_text = await generate_reply(
                {
                   "reviewer_name": review_payload["reviewer_name"],
                   "star_rating": review_payload["star_rating"],
                   "comment": review_payload["comment"],
                },
                openai_client,
            )


            # 3.d) Guardar respuesta IA
            reply_row = {
                "review_id": review_id,
                "reply_text": reply_text,
                "model_used": "gpt-4.1-mini",
                "tone": "default",
                "status": "pending",  # aún no publicado en Google
                "owner_id": owner_id,
            }

            reply_insert = (
                supabase.table("gbp_review_replies")
                .upsert(reply_row, on_conflict="review_id")
                .execute()
            )
            if reply_insert.data:
                created_replies.append(reply_insert.data[0])

            # 3.e) Marcar reseña como respondida
            (
                supabase.table("gbp_reviews")
                .update({"has_reply": True})
                .eq("review_id", review_id)
                .execute()
            )

        return {
            "email": email,
            "processed_reviews": len(reviews),
            "created_replies": len(created_replies),
            "replies": created_replies,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error en /reviews/run-once:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/reviews/latest-replies")
def latest_replies(
    email: str = Query(...),
    limit: int = Query(20, ge=1, le=100),
):
    if supabase is None:
        raise HTTPException(500, "Supabase no configurado")

    email = (email or "").lower().strip()
    if not email:
        raise HTTPException(400, "email requerido")

    # 1) Buscar el owner_id en profiles
    profile_resp = (
        supabase.table("profiles")
        .select("id")
        .eq("email", email)
        .single()
        .execute()
    )

    if not profile_resp or not profile_resp.data:
        return []

    owner_id = profile_resp.data.get("id")
    if not owner_id:
        return []

    # 2) Obtener las últimas respuestas de ese owner
    resp = (
        supabase.table("gbp_review_replies")
        .select("review_id, reply_text, status, update_time")
        .eq("owner_id", owner_id)
        .order("update_time", desc=True)
        .limit(limit)
        .execute()
    )

    rows = resp.data or []

    # 3) created_at sintético (frontend)
    for r in rows:
        if not r.get("created_at"):
            r["created_at"] = r.get("update_time")

    return rows


from typing import Literal

from fastapi import Query, HTTPException, Depends
from typing import Optional, Literal
from sqlalchemy.orm import Session
from datetime import datetime, date
from collections import defaultdict

from app.models import Review
print("🧩 DB URL:", engine.url)


@app.get("/reviews/sentiment-summary")
async def sentiment_summary(
    job_id: int = Query(..., description="ID del job de scraping"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    bucket: Literal["day", "week", "month"] = Query("day"),
    db: Session = Depends(get_db),
):


    q = db.query(Review).filter(Review.job_id == job_id)
    rows = q.all()

    from datetime import datetime

    def parse_dt_safe(v: str | None):
        if not v:
            return None
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None

    filtered_rows = []
    for r in rows:
        dt = parse_dt_safe(r.published_at)
        if not dt:
            continue
        if date_from and dt.date() < datetime.fromisoformat(date_from).date():
            continue
        if date_to and dt.date() > datetime.fromisoformat(date_to).date():
            continue
        filtered_rows.append(r)

    rows = filtered_rows

    if not rows:
        return {
            "total_reviews": 0,
            "avg_rating": 0,
            "breakdown": [],
            "trend": [],
            "bucket_type": bucket,
        }

    # ---------------------------
    # 2) Métricas globales
    # ---------------------------
    ratings = [int(r.rating or 0) for r in rows]
    total_reviews = len(ratings)
    avg_rating = sum(ratings) / total_reviews if total_reviews else 0

    def label_from_star(s: int) -> str:
        if s >= 4:
            return "positive"
        elif s == 3:
            return "neutral"
        else:
            return "negative"

    breakdown_counts = {"positive": 0, "neutral": 0, "negative": 0}

    for s in ratings:
        breakdown_counts[label_from_star(s)] += 1

    breakdown = [
        {"label": k, "count": v}
        for k, v in breakdown_counts.items()
        if v > 0
    ]

    # ---------------------------
    # 3) Agregación por bucket
    # ---------------------------
    def parse_date(v: str) -> date:
        return datetime.fromisoformat(v[:10]).date()

    buckets: dict[str, list[int]] = defaultdict(list)

    for r in rows:
        if not r.published_at:
            continue

        d = parse_date(r.published_at)

        if bucket == "day":
            key = d.strftime("%Y-%m-%d")
        elif bucket == "week":
            y, w, _ = d.isocalendar()
            key = f"{y}-W{w:02d}"
        else:  # month
            key = d.strftime("%Y-%m")

        buckets[key].append(int(r.rating or 0))

    trend = [
        {
            "bucket": k,
            "avg_rating": sum(v) / len(v),
            "count": len(v),
        }
        for k, v in sorted(buckets.items())
    ]

    # ---------------------------
    # 4) Respuesta final
    # ---------------------------
    return {
        "total_reviews": total_reviews,
        "avg_rating": avg_rating,
        "breakdown": breakdown,
        "trend": trend,
        "bucket_type": bucket,
    }


from typing import Literal

from fastapi import Request, Query, HTTPException, Depends
from typing import Optional, Literal
from sqlalchemy.orm import Session
from datetime import datetime
from collections import defaultdict
import json

from app.db import get_db
from app.models import Review


from typing import Optional, Literal
from fastapi import Request, Query, HTTPException, Depends
from sqlalchemy.orm import Session
import json

from sqlalchemy import func
from datetime import datetime
import json
import hashlib

@app.get("/reviews/topics-summary")
async def topics_summary(
    request: Request,
    job_id: int = Query(..., description="ID del job de scraping"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    max_topics: int = Query(7, ge=1, le=15),
    db: Session = Depends(get_db),
):
    openai_client = request.app.state.openai_client
    if not openai_client:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")

    # ---------------------------
    # 1) Construye una clave estable por params (from/to/max_topics)
    # ---------------------------
    params_obj = {
        "from": date_from or "",
        "to": date_to or "",
        "max_topics": max_topics,
    }
    params_key = hashlib.sha1(
        json.dumps(params_obj, sort_keys=True).encode("utf-8")
    ).hexdigest()
    section = "topics"

    # ---------------------------
    # 2) Firma del dataset (rápido y fiable)
    # ---------------------------
    base_q = db.query(Review).filter(Review.job_id == job_id)

    if date_from:
        base_q = base_q.filter(Review.published_at >= date_from)
    if date_to:
        base_q = base_q.filter(Review.published_at <= date_to)

    # Solo reseñas con texto (igual que luego)
    sig_q = base_q.filter(Review.text.isnot(None)).filter(Review.text != "")
    source_count = sig_q.with_entities(func.count(Review.id)).scalar() or 0
    source_max_review_id = sig_q.with_entities(func.max(Review.id)).scalar() or 0

    if source_count == 0:
        return {
            "topics": [],
            "total_mentions": 0,
            "avg_sentiment": 0,
            "global_trend": "flat",
        }

    # ---------------------------
    # 3) Cache hit
    # ---------------------------
    cache_row = (
        db.query(AnalysisCache)
        .filter(
            AnalysisCache.job_id == job_id,
            AnalysisCache.section == section,
            AnalysisCache.params_key == params_key,
        )
        .first()
    )

    if (
        cache_row
        and cache_row.source_reviews_count == source_count
        and cache_row.source_max_review_id == source_max_review_id
    ):
        return json.loads(cache_row.payload_json)

    # ---------------------------
    # 4) Cache miss -> calcula como antes (IA)
    # ---------------------------
    rows = sig_q.all()

    reviews = [
        {
            "id": r.id,
            "created_at": r.published_at[:10] if r.published_at else None,
            "star_rating": int(r.rating or 0),
            "comment": r.text or "",
        }
        for r in rows
        if r.text
    ]

    reviews_for_ai = reviews[-10000:]

    system_prompt = (
        "Eres un analista experto en reseñas de negocios. "
        "Detectas TEMAS ESPECÍFICOS y DIFERENCIADOS. "
        "Evita categorías genéricas. "
        "Usa temas claros como Atención al cliente, Precio, Calidad, Limpieza, Ambiente."
    )

    user_prompt = f"""
Estas son las reseñas en JSON:

{json.dumps(reviews_for_ai, ensure_ascii=False)}

Agrúpalas en un máximo de {max_topics} temas.

Devuelve SOLO este JSON:

{{
  "topics": [
    {{
      "tema": "Nombre del tema",
      "menciones": 12,
      "sentimiento": 0.4,
      "tendencia": "up"
    }}
  ]
}}
"""

    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        parsed = json.loads(completion.choices[0].message.content or "{}")
        topics = parsed.get("topics", [])
    except Exception as e:
        print("⚠️ Error IA topics_summary:", e)
        avg_star = sum(r["star_rating"] for r in reviews) / len(reviews)
        sentiment = (avg_star - 3) / 2
        topics = [
            {
                "tema": "General",
                "menciones": len(reviews),
                "sentimiento": sentiment,
                "tendencia": "flat",
            }
        ]

    # ---------------------------
    # 5) Normalización + métricas
    # ---------------------------
    norm_topics = []
    total_mentions = 0

    for t in topics:
        tema = str(t.get("tema") or "Tema").strip()
        menciones = int(t.get("menciones") or 0)
        sentimiento = max(-1, min(1, float(t.get("sentimiento") or 0)))
        tendencia = t.get("tendencia", "flat")
        if tendencia not in ("up", "down", "flat"):
            tendencia = "flat"

        total_mentions += menciones
        norm_topics.append(
            {
                "tema": tema,
                "menciones": menciones,
                "sentimiento": sentimiento,
                "tendencia": tendencia,
            }
        )

    num = sum(t["sentimiento"] * t["menciones"] for t in norm_topics)
    den = sum(t["menciones"] for t in norm_topics) or 1
    avg_sentiment = num / den

    trend_score = sum(
        1 if t["tendencia"] == "up" else -1 if t["tendencia"] == "down" else 0
        for t in norm_topics
    )
    global_trend = "up" if trend_score > 0 else "down" if trend_score < 0 else "flat"

    payload = {
        "topics": norm_topics,
        "total_mentions": total_mentions,
        "avg_sentiment": avg_sentiment,
        "global_trend": global_trend,
    }

    # ---------------------------
    # 6) Guarda/actualiza caché (FIX: sin source_max_published_at/source_max_pub)
    # ---------------------------
    payload_json = json.dumps(payload, ensure_ascii=False)

    if cache_row:
        cache_row.source_reviews_count = source_count
        cache_row.source_max_review_id = source_max_review_id
        cache_row.payload_json = payload_json
        cache_row.computed_at = datetime.now(timezone.utc)
    else:
        cache_row = AnalysisCache(
            job_id=job_id,
            section=section,
            params_key=params_key,
            source_reviews_count=source_count,
            source_max_review_id=source_max_review_id,
            payload_json=payload_json,
            computed_at=datetime.now(timezone.utc),
        )
        db.add(cache_row)

    db.commit()
    return payload



from typing import Literal

from fastapi import Request

from fastapi import Request, Query, HTTPException
from typing import Optional
from datetime import datetime
import json

from fastapi import Request, Query, HTTPException, Depends
from typing import Optional
from sqlalchemy.orm import Session
from datetime import datetime
import json

from app.db import get_db
from app.models import Review


from sqlalchemy import func
import hashlib
import json
from datetime import datetime, timezone

@app.get("/reviews/action-plan")
async def action_plan(
    request: Request,
    job_id: int = Query(..., description="ID del job de scraping"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    max_categories: int = Query(3, ge=1, le=10),
    db: Session = Depends(get_db),
):
    openai_client = request.app.state.openai_client
    if not openai_client:
        raise HTTPException(500, "IA no configurada")

    # DEBUG: confirma que entra y con qué params
    print("🔥 HIT /reviews/action-plan", {
        "job_id": job_id, "from": date_from, "to": date_to, "max_categories": max_categories
    })

    # -------------------------
    # 1) Params key
    # -------------------------
    params_obj = {
        "from": date_from or "",
        "to": date_to or "",
        "max_categories": max_categories,
    }

    params_key = hashlib.sha1(
        json.dumps(params_obj, sort_keys=True).encode("utf-8")
    ).hexdigest()

    section = "action_plan"

    # -------------------------
    # 2) Firma dataset
    # -------------------------
    base_q = db.query(Review).filter(Review.job_id == job_id)

    if date_from:
        base_q = base_q.filter(Review.published_at >= date_from)

    if date_to:
        base_q = base_q.filter(Review.published_at <= date_to)

    sig_q = base_q.filter(Review.text.isnot(None)).filter(Review.text != "")

    source_count = sig_q.with_entities(func.count(Review.id)).scalar() or 0
    source_max_id = sig_q.with_entities(func.max(Review.id)).scalar() or 0

    if source_count == 0:
        print("⚠️ action_plan: 0 reviews con texto")
        return {"categorias": []}

    # -------------------------
    # 3) Buscar caché
    # -------------------------
    cache_row = (
        db.query(AnalysisCache)
        .filter(
            AnalysisCache.job_id == job_id,
            AnalysisCache.section == section,
            AnalysisCache.params_key == params_key,
        )
        .first()
    )

    if (
        cache_row
        and cache_row.source_reviews_count == source_count
        and cache_row.source_max_review_id == source_max_id
    ):
        print("✅ action_plan CACHE HIT", {
            "job_id": job_id, "params_key": params_key,
            "count": source_count, "max_id": source_max_id
        })
        return json.loads(cache_row.payload_json)

    print("🧠 action_plan CACHE MISS", {
        "job_id": job_id, "params_key": params_key,
        "count": source_count, "max_id": source_max_id
    })

    # -------------------------
    # 4) Cache miss → IA
    # -------------------------
    rows = sig_q.all()

    reviews = []
    for r in rows:
        comment = (r.text or "").strip()
        if not comment:
            continue
        reviews.append(
            {
                "id": r.id,
                "star_rating": int(r.rating or 0),
                "comment": comment,
                "reviewer_name": r.author_name or "Cliente",
            }
        )

    negative = [r for r in reviews if r["star_rating"] <= 3]
    base = negative if len(negative) >= 5 else reviews
    base = base[-10000:]

    system_prompt = (
        "Eres un consultor experto en experiencia de cliente para negocios locales. "
        "Analizas reseñas reales y propones oportunidades de mejora accionables."
    )

    user_prompt = f"""
Estas son reseñas reales:

{json.dumps(base, ensure_ascii=False)}

Devuelve SOLO este JSON:

{{
  "categorias": [
    {{
      "categoria": "Nombre",
      "dato": "Insight",
      "oportunidad": "Acción",
      "reseñas": [
        {{"autor": "Nombre", "texto": "Texto"}}
      ]
    }}
  ]
}}

Máximo {max_categories}.
"""

    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )

        parsed = json.loads(completion.choices[0].message.content or "{}")
        categorias = parsed.get("categorias") or []

    except Exception as e:
        print("⚠️ IA action_plan:", repr(e))
        categorias = []

    payload = {"categorias": categorias}
    payload_json = json.dumps(payload, ensure_ascii=False)

    # -------------------------
    # 5) Guardar caché
    # -------------------------
    if cache_row:
        cache_row.source_reviews_count = source_count
        cache_row.source_max_review_id = source_max_id
        cache_row.payload_json = payload_json
        cache_row.computed_at = datetime.now(timezone.utc)
    else:
        cache_row = AnalysisCache(
            job_id=job_id,
            section=section,
            params_key=params_key,
            source_reviews_count=source_count,
            source_max_review_id=source_max_id,
            payload_json=payload_json,
            computed_at=datetime.now(timezone.utc),
        )
        db.add(cache_row)

    db.commit()
    try:
        db.refresh(cache_row)
    except Exception:
        pass

    print("💾 action_plan cache guardado", {
        "id": getattr(cache_row, "id", None),
        "job_id": job_id, "section": section
    })

    return payload


def normalize_gmaps_url(url: str) -> str:
    """
    Normaliza URLs de Google Maps para scraping.

    Prioridad:
    1) Mantener URLs largas /maps/place/.../data=!...
    2) Convertir /maps/search?...query_place_id=XXXX -> place_id
    3) Asegurar hl=es en place_id
    4) Soportar ?cid=XXXX sin romper validación
    """

    try:
        raw = (url or "").strip()
        if not raw:
            return raw

        u = urlparse(raw)
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()
        qs = parse_qs(u.query)

        # -------------------------------------------------
        # 0) Si ya es una URL larga /maps/place/.../data=!...
        #    -> NO tocarla (es la mejor)
        # -------------------------------------------------
        if "/maps/place" in path and "/data=" in raw:
            return raw

        # -------------------------------------------------
        # 1) /maps/search?...query_place_id=XXXX
        #    -> place_id canonical
        # -------------------------------------------------
        if "/maps/search" in path and "query_place_id" in qs:
            pid = (qs["query_place_id"][0] or "").strip()
            if pid:
                return f"https://www.google.com/maps/place/?q=place_id:{pid}&hl=es"

        # -------------------------------------------------
        # 2) URL con place_id:
        #    -> asegurar hl=es
        # -------------------------------------------------
        if "place_id:" in raw:
            if "hl=" not in raw:
                sep = "&" if "?" in raw else "?"
                return raw + f"{sep}hl=es"
            return raw

        # -------------------------------------------------
        # 3) URL tipo CID
        #    https://maps.google.com/?cid=...
        # -------------------------------------------------
        if "cid" in qs and ("google.com" in host or "google" in host):
            cid = (qs["cid"][0] or "").strip()
            if cid:
                return f"https://www.google.com/maps?cid={cid}&hl=es"

        # -------------------------------------------------
        # 4) /maps/place sin data -> mantener (es aceptable)
        # -------------------------------------------------
        if "/maps/place" in path:
            return raw

        return raw

    except Exception:
        return url


@app.post("/scrape", response_model=ScrapeResponse)
def scrape(req: ScrapeRequest, db: Session = Depends(get_db)):
    # ✅ DEBUG: qué llega realmente
    print("📥 /scrape payload place_name:", req.place_name)
    print("📥 /scrape payload google_maps_url:", req.google_maps_url)

    def looks_like_place_id_or_url(v: str) -> bool:
        s = (v or "").strip()
        return (
            s.startswith("http")
            or s.startswith("place_id:")
            or ("place_id:" in s)
            or ("google.com/maps" in s)
        )

    # ✅ Normaliza y filtra nombre
    incoming_name = (req.place_name or "").strip()
    safe_name: Optional[str] = None
    if incoming_name and not looks_like_place_id_or_url(incoming_name):
        safe_name = incoming_name

    # ✅ Normaliza la URL SIEMPRE (evita /maps/search y fuerza hl=es cuando es place_id)
    normalized_url = normalize_gmaps_url(str(req.google_maps_url))
    print("🔁 normalized_url:", normalized_url)

    # Ejecuta el scraping y guarda en SQLite
    job, saved = scrape_and_store(
        db=db,
        google_maps_url=normalized_url,
        max_reviews=req.max_reviews,
        personal_data=req.personal_data,
        place_name=safe_name,  # ✅ solo pasa nombre si es válido
    )

    # ✅ Guardar el nombre SOLO si es válido
    if safe_name:
        job.place_name = safe_name
        db.add(job)
        db.commit()
        db.refresh(job)
        print("✅ Nombre guardado en job.place_name:", job.place_name)
    else:
        print("⚠️ No guardo place_name porque parece URL/place_id o viene vacío:", incoming_name)

    print("🧪 SCRAPE terminado. job_id =", job.id)

    # ⬇️ Lógica opcional de Supabase (NO afecta al nombre en el panel)
    try:
        if supabase is None:
            print("❌ Supabase es None en /scrape")
        else:
            res = (
                supabase
                .table("analyses")
                .upsert(
                    {
                        "id": job.id,
                        "place_name": job.place_name,  # ✅ lo que quedó guardado
                    },
                    on_conflict="id",
                )
                .execute()
            )
            print("🧪 Resultado upsert analyses:", res)

    except Exception as e:
        print("❌ Error guardando en analyses:", repr(e))

    # Respuesta al frontend
    return {
        "job_id": job.id,
        "status": job.status,
        "reviews_saved": saved,
    }

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    reviews_saved = db.query(Review).filter(Review.job_id == job_id).count()
    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        apify_run_id=job.apify_run_id,
        error=job.error,
        reviews_saved=reviews_saved,
    )

def build_place_key(info: dict) -> str:
    """
    Genera una clave estable para un local.
    """
    if info.get("lat") and info.get("lon"):
        return f"{info.get('query_text','').lower()}::{info['lat']}::{info['lon']}"
    return info.get("query_text","").lower()


@app.get("/reviews/ai-replies")
async def ai_replies(
    request: Request,
    job_id: int = Query(..., description="ID del job (local)"),
    db: Session = Depends(get_db),
):
    openai_client = request.app.state.openai_client
    if not openai_client:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    def parse_dt_safe(v):
        if not v:
            return None
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    # 1) Reviews del último mes
    rows = db.query(Review).filter(Review.job_id == job_id).all()

    reviews = []
    for r in rows:
        dt = parse_dt_safe(r.published_at) or parse_dt_safe(getattr(r, "created_at", None))
        if not dt or dt < cutoff:
            continue
        if not r.text:
            continue

        rating = int(r.rating or 0)
        text = r.text.strip()

        input_hash = hashlib.sha1(f"{rating}|{text}".encode("utf-8")).hexdigest()

        reviews.append(
            {
                "id": r.id,
                "author": r.author_name or "Cliente",
                "rating": rating,
                "text": text,
                "created_at": dt,
                "input_hash": input_hash,
            }
        )

    reviews.sort(key=lambda x: x["created_at"], reverse=True)
    if not reviews:
        print("⚠️ ai_replies: no hay reviews en últimos 30 días")
        return []

    # 2) Trae replies cacheadas
    ids = [r["id"] for r in reviews]
    cached_rows = db.query(ReviewAIReply).filter(ReviewAIReply.review_id.in_(ids)).all()
    cached_map = {c.review_id: c for c in cached_rows}

    # 3) Genera SOLO las que faltan o cambiaron
    to_upsert = []
    results = []

    for r in reviews:
        c = cached_map.get(r["id"])

        if c and c.input_hash == r["input_hash"]:
            reply_text = c.reply_text
        else:
            reply_text = await generate_reply(
                {
                    "reviewer_name": r["author"],
                    "star_rating": r["rating"],
                    "comment": r["text"],
                },
                openai_client,
            )

            if c:
                c.reply_text = reply_text
                c.input_hash = r["input_hash"]
                c.updated_at = datetime.now(timezone.utc)
                to_upsert.append(c)
            else:
                to_upsert.append(
                    ReviewAIReply(
                        review_id=r["id"],
                        job_id=job_id,
                        input_hash=r["input_hash"],
                        reply_text=reply_text,
                        model_used="gpt-4.1-mini",
                        tone="default",
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )
                )

        results.append(
            {
                "review_id": r["id"],
                "review_text": r["text"],
                "reply_text": reply_text,
                "rating": r["rating"],
                "created_at": r["created_at"].isoformat(),
            }
        )

    # ✅ DEBUG (fuera del loop, bien indentado)
    print("🧠 ai_replies reviews:", len(reviews))
    print("🧠 ai_replies cached_rows:", len(cached_rows))
    print("🧠 ai_replies to_upsert:", len(to_upsert))

    # 4) Guarda nuevas/actualizadas
    if to_upsert:
        for obj in to_upsert:
            db.add(obj)
        db.commit()
        print("💾 ai_replies guardadas:", len(to_upsert))

    return results



@app.get("/jobs/{job_id}/meta")
def get_job_meta(job_id: int, db: Session = Depends(get_db)):
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "place_name": job.place_name
    }

@app.get("/admin/debug/google-oauth")
def debug_google_oauth(
    x_admin_key: str = Header(None),
    db: Session = Depends(get_db),
):
    if x_admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(status_code=403, detail="forbidden")

    try:
        result = db.execute(text("""
            SELECT email, google_account_id, connected, expires_at
            FROM google_oauth
            ORDER BY email
        """))
        rows = result.fetchall()
        return [
            {"email": r[0], "google_account_id": r[1], "connected": r[2], "expires_at": r[3]}
            for r in rows
        ]
    except Exception as e:
        print("❌ debug_google_oauth error:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))

