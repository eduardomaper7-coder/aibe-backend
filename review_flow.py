from fastapi import APIRouter, Depends, Header, HTTPException
from typing import Optional, List
from datetime import datetime
from .config import get_settings
from .supabase_client import supabase
from .google_gbp import (
    get_access_token_from_refresh,
    list_gbp_locations,
    list_gbp_reviews,
    load_mock_reviews,
)
from .ai_responder import generate_reply

router = APIRouter()
settings = get_settings()


async def get_current_user_id(x_user_id: Optional[str] = Header(None)) -> str:
    """
    MVP: el front envía X-User-Id con el uuid de Supabase.
    En producción, valida el JWT de Supabase aquí.
    """
    if not x_user_id:
        raise HTTPException(status_code=401, detail="Falta X-User-Id")
    return x_user_id


@router.post("/run-once")
async def run_flow_for_user(user_id: str = Depends(get_current_user_id)):
    """
    Ejecuta manualmente el flujo para un usuario:
    1. Busca su conexión en google_connections.
    2. Obtiene reseñas (mock o reales).
    3. Genera respuestas y las guarda en Supabase.
    """

    # 1) Obtenemos conexión Google del usuario
    conn_resp = supabase.table("google_connections") \
        .select("*") \
        .eq("user_id", user_id) \
        .single() \
        .execute()

    if not conn_resp.data:
        raise HTTPException(status_code=400, detail="Usuario sin conexión Google")

    refresh_token = conn_resp.data["refresh_token"]

    # 2) Decidir mock vs real
    if settings.use_mock_gbp:
        reviews = load_mock_reviews()
    else:
        access_token = await get_access_token_from_refresh(refresh_token)
        # Aquí podrías listar locations dinámicamente,
        # para el MVP puedes fijar una o varias
        locations = await list_gbp_locations(access_token)
        reviews = []
        for loc in locations:
            loc_name = loc["name"]
            loc_reviews = await list_gbp_reviews(access_token, loc_name)
            for r in loc_reviews:
                r["location_name"] = loc_name
            reviews.extend(loc_reviews)

    created_replies = []

    for r in reviews:
        review_id = r["review_id"]

        # 3) Comprobar si ya la tenemos en Supabase y si ya tiene reply
        existing = supabase.table("gbp_reviews") \
            .select("id, has_reply") \
            .eq("review_id", review_id) \
            .single() \
            .execute()

        if existing.data and existing.data.get("has_reply"):
            # Ya respondida → saltamos
            continue

        # 3.1) Insertar/actualizar reseña en gbp_reviews
        review_payload = {
            "review_id": review_id,
            "reviewer_name": r.get("reviewer_name") or r.get("reviewer", {}).get("displayName"),
            "star_rating": r.get("star_rating") or r.get("starRating"),
            "comment": r.get("comment") or r.get("comment", {}).get("text"),
            "create_time": r.get("create_time") or r.get("createTime"),
            "update_time": r.get("update_time") or r.get("updateTime"),
            "location_name": r.get("location_name"),
            "owner_id": user_id,
            "raw_payload": r,
        }

        supabase.table("gbp_reviews").upsert(review_payload, on_conflict="review_id").execute()

        # 4) Generar respuesta con IA
        reply_text = await generate_reply({
            "reviewer_name": review_payload["reviewer_name"],
            "star_rating": review_payload["star_rating"],
            "comment": review_payload["comment"],
        })

        # 5) Guardar en gbp_review_replies (status pending)
        reply_row = {
            "review_id": review_id,
            "reply_text": reply_text,
            "model_used": "gpt-4.1-mini",
            "tone": "default",
            "status": "pending",
            "owner_id": user_id,
        }

        reply_insert = supabase.table("gbp_review_replies").insert(reply_row).execute()
        created_replies.append(reply_insert.data[0])

        # 6) (Opcional ahora) Publicar en Google cuando tengas API OK
        # TODO:
        # if not settings.use_mock_gbp:
        #   await post_reply_to_google(access_token, r["name"], reply_text)
        #   actualizar status = 'posted', published_to_google_at = now()

        # 7) Marcar reseña como respondida
        supabase.table("gbp_reviews") \
            .update({"has_reply": True}) \
            .eq("review_id", review_id) \
            .execute()

    return {
        "processed_reviews": len(reviews),
        "created_replies": len(created_replies),
        "replies": created_replies,
    }


@router.get("/latest")
async def latest_replies(
    limit: int = 20,
    user_id: str = Depends(get_current_user_id)
):
    """
    Endpoint para la sección "Últimas respuestas enviadas" en tu frontend.
    Devuelve las últimas respuestas (ya sea solo pending o posted, como prefieras).
    """
    resp = supabase.table("gbp_review_replies") \
        .select("review_id, reply_text, created_at, status") \
        .eq("owner_id", user_id) \
        .order("created_at", desc=True) \
        .limit(limit) \
        .execute()

    return resp.data or []
