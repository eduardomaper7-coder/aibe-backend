# aibe-backend/main.py (parte superior)
import asyncio
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Depends, HTTPException, Query, Request
import json
import os
from dotenv import load_dotenv
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from supabase_client import supabase


from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware


from pathlib import Path
from openai import OpenAI

from collections import defaultdict
from typing import Literal

from pydantic import BaseModel, HttpUrl
from typing import List

from fastapi import FastAPI, HTTPException

from typing import Optional, List, Dict, Any


from sqlalchemy.orm import Session

from app.db import Base, engine, get_db
from app.schemas import ScrapeRequest, ScrapeResponse, JobStatusResponse
from app.models import ScrapeJob, Review
from app.reviews_service import scrape_and_store
from app.models import ScrapeJob
Base.metadata.create_all(bind=engine)



# =========================
# Config desde variables .env
# =========================
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

print("DEBUG OPENAI_API_KEY:", "OK" if os.getenv("OPENAI_API_KEY") else "MISSING")




FRONTEND_ORIGIN_RAW = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")



USE_MOCK_GBP = os.getenv("USE_MOCK_GBP", "true").lower() == "true"



# =========================
# App
# =========================
app = FastAPI(title="AIBE Backend", version="1.0.0")

# =========================
# Startup: OpenAI client
# =========================
@app.on_event("startup")
def startup_event():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️  OPENAI_API_KEY no configurada")
        app.state.openai_client = None
        return

    app.state.openai_client = OpenAI(api_key=api_key)
    print("✅ OpenAI client inicializado")



import re
import urllib.parse



# ✅ CORS SIEMPRE PRIMERO
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 👈 DEV ONLY
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)





# =========================
# Rutas
# =========================

@app.get("/health")
async def health():
    return {"ok": True, "ts": int(time.time())}




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

        # --- FILTRO: solo reseñas de los últimos 30 días ---
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        for r in rows:
            dt = parse_dt_safe(r.published_at) or parse_dt_safe(getattr(r, "created_at", None))

            if not dt or dt < cutoff:
                continue

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





    """
    Endpoint para tu sección "Últimas respuestas enviadas".
    Devuelve solo las respuestas relacionadas con el owner_id del email.
    """
    if supabase is None:
        raise HTTPException(500, "Supabase no configurado")

    email = (email or "").lower().strip()
    if not email:
        raise HTTPException(400, "email requerido")

    try:
        # 1) Buscar el owner_id en profiles
        profile_resp = (
            supabase.table("profiles")
            .select("id")
            .eq("email", email)
            .single()
            .execute()
        )
    except Exception as e:
        print("❌ Error consultando profiles:", repr(e))
        raise HTTPException(500, f"Error consultando profiles: {e}")

    if not profile_resp or not profile_resp.data:
        # Si no hay perfil, no devolvemos nada
        print("⚠️ No se encontró profile para", email)
        return []

    owner_id = profile_resp.data.get("id")
    if not owner_id:
        print("⚠️ profile sin id para", email)
        return []

    try:
        # 2) Obtener las últimas respuestas de ese owner
        # OJO: la tabla no tiene created_at, solo update_time
        resp = (
            supabase.table("gbp_review_replies")
            .select("review_id, reply_text, status, update_time")
            .eq("owner_id", owner_id)
            .order("update_time", desc=True)
            .limit(limit)
            .execute()
        )
        rows = resp.data or []

        # Añadimos un campo created_at sintético para que el frontend pueda usarlo
        from datetime import datetime, timezone

        for r in rows:
            if not r.get("created_at"):
                r["created_at"] = r.get("update_time") or datetime.now(
                    timezone.utc
                ).isoformat()

    except Exception as e:
        print("❌ Error consultando gbp_review_replies:", repr(e))
        raise HTTPException(500, f"Error consultando replies: {e}")


    print(f"✅ latest_replies: email={email}, owner_id={owner_id}, rows={len(rows)}")
    return rows

from typing import Literal

from fastapi import Query, HTTPException, Depends
from typing import Optional, Literal
from sqlalchemy.orm import Session
from datetime import datetime, date
from collections import defaultdict

from app.db import get_db
from app.models import Review


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

@app.get("/reviews/topics-summary")
async def topics_summary(
    request: Request,
    job_id: int = Query(..., description="ID del job de scraping"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    max_topics: int = Query(7, ge=1, le=15),
    db: Session = Depends(get_db),
):
    # ---------------------------
    # OpenAI
    # ---------------------------
    openai_client = request.app.state.openai_client
    if not openai_client:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")

    # ---------------------------
    # Obtener reseñas desde SQLite
    # ---------------------------
    q = db.query(Review).filter(Review.job_id == job_id)

    if date_from:
        q = q.filter(Review.published_at >= date_from)
    if date_to:
        q = q.filter(Review.published_at <= date_to)

    rows = q.all()

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

    if not reviews:
        return {
            "topics": [],
            "total_mentions": 0,
            "avg_sentiment": 0,
            "global_trend": "flat",
        }

    reviews_for_ai = reviews[-200:]

    # ---------------------------
    # Prompt OpenAI
    # ---------------------------
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
    # Normalización y métricas
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

        norm_topics.append({
            "tema": tema,
            "menciones": menciones,
            "sentimiento": sentimiento,
            "tendencia": tendencia,
        })

    num = sum(t["sentimiento"] * t["menciones"] for t in norm_topics)
    den = sum(t["menciones"] for t in norm_topics) or 1
    avg_sentiment = num / den

    trend_score = sum(1 if t["tendencia"] == "up" else -1 if t["tendencia"] == "down" else 0 for t in norm_topics)

    global_trend: Literal["up", "down", "flat"] = (
        "up" if trend_score > 0 else "down" if trend_score < 0 else "flat"
    )

    return {
        "topics": norm_topics,
        "total_mentions": total_mentions,
        "avg_sentiment": avg_sentiment,
        "global_trend": global_trend,
    }


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


@app.get("/reviews/action-plan")
async def action_plan(
    request: Request,
    job_id: int = Query(..., description="ID del job de scraping"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    max_categories: int = Query(3, ge=1, le=10),
    db: Session = Depends(get_db),
):
    """
    Genera un plan de acción a partir de reseñas reales (SQLite),
    priorizando reseñas negativas o mixtas.
    """

    # ---------------------------
    # OpenAI
    # ---------------------------
    openai_client = request.app.state.openai_client
    if openai_client is None:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")

    # ---------------------------
    # Helpers
    # ---------------------------
    def parse_dt_safe(v: str | None):
        if not v:
            return None
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None

    # ---------------------------
    # 1) Obtener reseñas desde SQLite
    # ---------------------------
    q = db.query(Review).filter(Review.job_id == job_id)
    rows = q.all()

    reviews: list[dict] = []

    for r in rows:
        dt = parse_dt_safe(r.published_at)
        if not dt:
            continue

        if date_from and dt.date() < datetime.fromisoformat(date_from).date():
            continue
        if date_to and dt.date() > datetime.fromisoformat(date_to).date():
            continue

        comment = (r.text or "").strip()
        if not comment:
            continue

        reviews.append(
            {
                "id": r.id,
                "created_at": dt.date().isoformat(),
                "star_rating": int(r.rating or 0),
                "comment": comment,
                "reviewer_name": r.author_name or "Cliente",
            }
        )

    if not reviews:
        return {"categorias": []}

    # ---------------------------
    # 2) Base para IA (priorizar negativas)
    # ---------------------------
    negative = [r for r in reviews if r["star_rating"] <= 3]
    base = negative if len(negative) >= 5 else reviews

    # límite de seguridad para IA
    base = base[-200:]

    # ---------------------------
    # 3) Prompt OpenAI
    # ---------------------------
    system_prompt = (
        "Eres un consultor experto en experiencia de cliente para negocios locales. "
        "Analizas reseñas reales y propones oportunidades de mejora accionables, "
        "claras y realistas."
    )

    user_prompt = f"""
Estas son reseñas reales de clientes en formato JSON:

{json.dumps(base, ensure_ascii=False)}

Devuelve SOLO un JSON con esta estructura EXACTA:

{{
  "categorias": [
    {{
      "categoria": "Nombre claro de la categoría",
      "dato": "Insight concreto detectado en las reseñas",
      "oportunidad": "Acción práctica y específica para mejorar",
      "reseñas": [
        {{"autor": "Nombre", "texto": "Texto de la reseña"}}
      ]
    }}
  ]
}}

Máximo {max_categories} categorías.
"""

    # ---------------------------
    # 4) Llamada a OpenAI
    # ---------------------------
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )

        raw = completion.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        categorias = parsed.get("categorias") or []

    except Exception as e:
        print("⚠️ Error IA action_plan:", repr(e))
        categorias = [
            {
                "categoria": "Mejora de experiencia del cliente",
                "dato": "Las reseñas muestran oportunidades de mejora recurrentes.",
                "oportunidad": "Revisar procesos internos y reforzar la formación del equipo.",
                "reseñas": [
                    {
                        "autor": r.get("reviewer_name", "Cliente"),
                        "texto": r["comment"],
                    }
                    for r in base[:3]
                ],
            }
        ]

    return {"categorias": categorias}



@app.post("/scrape", response_model=ScrapeResponse)
def scrape(req: ScrapeRequest, db: Session = Depends(get_db)):
    # Ejecuta el scraping y guarda en SQLite
    job, saved = scrape_and_store(
        db=db,
        google_maps_url=str(req.google_maps_url),
        max_reviews=req.max_reviews,
        personal_data=req.personal_data,
    )

    print("🧪 SCRAPE terminado. job_id =", job.id)

    # ⬇️ Lógica opcional de Supabase (NO afecta al nombre en el panel)
    try:
        if supabase is None:
            print("❌ Supabase es None en /scrape")
        else:
            # ⚠️ UPSERT (no insert) usando el nombre REAL del job
            res = (
                supabase
                .table("analyses")
                .upsert(
                    {
                        "id": job.id,
                        "place_name": job.place_name,  # ✅ nombre real del negocio
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
    """
    Devuelve reseñas del último mes (30 días) con
    respuestas IA generadas en tiempo real.
    """

    # ---------------------------
    # OpenAI
    # ---------------------------
    openai_client = request.app.state.openai_client
    if not openai_client:
        raise HTTPException(500, "IA no configurada (OPENAI_API_KEY falta)")

    # cutoff UTC (aware)
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    # ---------------------------
    # Helpers
    # ---------------------------
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

    # ---------------------------
    # 1) Obtener reseñas del job
    # ---------------------------
    rows = (
        db.query(Review)
        .filter(Review.job_id == job_id)
        .all()
    )

    reviews = []

    for r in rows:
        # published_at -> fallback created_at
        dt = (
            parse_dt_safe(r.published_at)
            or parse_dt_safe(getattr(r, "created_at", None))
        )

        if not dt or dt < cutoff:
            continue

        if not r.text:
            continue

        reviews.append(
            {
                "id": r.id,
                "author": r.author_name or "Cliente",
                "rating": int(r.rating or 0),
                "text": r.text,
                "created_at": dt,
            }
        )

    # ordenar: más reciente primero
    reviews.sort(key=lambda r: r["created_at"], reverse=True)

    if not reviews:
        return []

    # ---------------------------
    # 2) Generar respuestas IA
    # ---------------------------
    results = []

    for r in reviews:
        reply_text = await generate_reply(
            {
                "reviewer_name": r["author"],
                "star_rating": r["rating"],
                "comment": r["text"],
            },
            openai_client,
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

    return results


@app.get("/jobs/{job_id}/meta")
def get_job_meta(job_id: int, db: Session = Depends(get_db)):
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "place_name": job.place_name
    }

