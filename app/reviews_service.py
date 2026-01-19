import json
import os
import csv
from datetime import datetime
from sqlalchemy.orm import Session

from app.apify_client import ApifyWrapper
from app.config import settings
from app.models import ScrapeJob, Review
from app.google_maps import (
    is_valid_google_maps_url,
    parse_google_maps_url,
    build_place_key,
)



def ensure_export_dir():
    os.makedirs(settings.EXPORT_DIR, exist_ok=True)

def normalize_review(item: dict) -> dict:
    def pick(*keys):
        for k in keys:
            if k in item and item[k]:
                return item[k]
        return None

    published_at = pick(
        "publishedAt",
        "publishedAtDate",
        "publishedAtDateTime",
        "createdAt",
        "reviewPublishedAt",
        "publishDate",
        "date",
    )

    return {
        "rating": pick("rating", "stars"),
        "text": pick("text", "reviewText", "comment"),
        "published_at": str(published_at) if published_at else None,
        "author_name": pick("name", "reviewerName", "authorName", "userName"),
        "review_url": pick("reviewUrl", "url"),
        "raw": item,
    }

def export_job_reviews(job_id: int, items: list[dict]) -> tuple[str, str]:
    """
    Export opcional a JSON y CSV en ./data/exports
    """
    ensure_export_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(settings.EXPORT_DIR, f"job_{job_id}_{ts}.json")
    csv_path = os.path.join(settings.EXPORT_DIR, f"job_{job_id}_{ts}.csv")

    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    # CSV (flatten mínimo)
    fieldnames = ["rating", "published_at", "author_name", "review_url", "text"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            n = normalize_review(it)
            w.writerow({k: n.get(k) for k in fieldnames})

    return json_path, csv_path

def scrape_and_store(
    db: Session,
    google_maps_url: str,
    max_reviews: int,
    personal_data: bool,
) -> tuple[ScrapeJob, int]:

    if not is_valid_google_maps_url(google_maps_url):
        raise ValueError(
            "URL no válida de Google Maps (debe ser /maps/place, /maps/reviews o /maps/search)."
        )

    # ---------------------------
    # 0) Identificar el local
    # ---------------------------
    info = parse_google_maps_url(google_maps_url)
    place_key = build_place_key(info)

    place_name = (
        info.get("place_name")
        or info.get("name")
        or info.get("query_text")
    )


    # ---------------------------
    # 1) ¿Ya existe este local?
    # ---------------------------
    existing_job = (
        db.query(ScrapeJob)
        .filter(ScrapeJob.place_key == place_key)
        .first()
    )

    if existing_job:
        saved = (
            db.query(Review)
            .filter(Review.job_id == existing_job.id)
            .count()
        )
        print("♻️ Local ya scrapeado. Reutilizando reseñas.")
        return existing_job, saved

    # ---------------------------
    # 2) Crear job nuevo
    # ---------------------------
    job = ScrapeJob(
        google_maps_url=google_maps_url,
        place_key=place_key,
        place_name=place_name,  # ✅ AQUÍ
        actor_id=settings.APIFY_ACTOR_ID,
        status="running",
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        # ---------------------------
        # 3) Ejecutar Apify
        # ---------------------------
        apify = ApifyWrapper()
        run, items = apify.run_reviews_actor(
            google_maps_url=google_maps_url,
            max_reviews=max_reviews,
            personal_data=personal_data,
        )

        job.apify_run_id = run.get("id")
        job.status = "succeeded"
        db.add(job)

        # ---------------------------
        # 4) Guardar reseñas
        # ---------------------------
        saved = 0
        for item in items:
            n = normalize_review(item)
            r = Review(
                job_id=job.id,
                rating=int(n["rating"]) if n["rating"] is not None else None,
                text=n["text"],
                published_at=str(n["published_at"]) if n["published_at"] else None,
                author_name=n["author_name"],
                review_url=n["review_url"],
                raw=n["raw"],
            )
            db.add(r)
            saved += 1

        db.commit()

        # ---------------------------
        # 5) Export opcional
        # ---------------------------
        export_job_reviews(job.id, items)

        return job, saved

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        db.add(job)
        db.commit()
        raise
