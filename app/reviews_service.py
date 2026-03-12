import json
import os
import csv
from datetime import datetime
from typing import Optional

import requests
from sqlalchemy.orm import Session

from app.apify_client import ApifyWrapper
from app.config import settings
from app.models import ScrapeJob, Review, ReviewCheckRun, ReviewCheckItem
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

    review_id = pick("reviewId", "review_id", "id")

    return {
        "review_id": review_id,
        "rating": pick("rating", "stars"),
        "text": pick("text", "reviewText", "comment"),
        "published_at": str(published_at) if published_at else None,
        "author_name": pick("name", "reviewerName", "authorName", "userName"),
        "review_url": pick("reviewUrl", "url"),
        "raw": item,
    }


def export_job_reviews(job_id: int, items: list[dict]) -> tuple[str, str]:
    ensure_export_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(settings.EXPORT_DIR, f"job_{job_id}_{ts}.json")
    csv_path = os.path.join(settings.EXPORT_DIR, f"job_{job_id}_{ts}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    fieldnames = ["rating", "published_at", "author_name", "review_url", "text"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            n = normalize_review(it)
            w.writerow({k: n.get(k) for k in fieldnames})

    return json_path, csv_path


def expand_google_maps_short_url(url: str) -> str:
    try:
        resp = requests.get(url, allow_redirects=True, timeout=10)
        return resp.url
    except Exception:
        raise ValueError("No se pudo resolver la URL corta de Google Maps")


def _normalize_text_for_compare(text: Optional[str]) -> str:
    if not text:
        return ""
    return " ".join(str(text).strip().lower().split())


def _review_exists_in_main_table(db: Session, job_id: int, n: dict) -> bool:
    review_id = (n.get("review_id") or "").strip()

    if review_id:
        exists = (
            db.query(Review)
            .filter(
                Review.job_id == job_id,
                Review.review_id == review_id,
            )
            .first()
        )
        if exists:
            return True

    review_url = (n.get("review_url") or "").strip()

    if review_url:
        exists = (
            db.query(Review)
            .filter(
                Review.job_id == job_id,
                Review.review_url == review_url,
            )
            .first()
        )
        if exists:
            return True

    return False


def check_and_store_latest_reviews(
    db: Session,
    job_id: int,
    personal_data: bool = True,
) -> dict:
    """
    Flujo NUEVO:
    - consulta solo las 10 últimas reseñas en Apify
    - guarda esas 10 en tabla auxiliar review_check_items
    - inserta en reviews solo las que no existan
    """
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise ValueError(f"No existe ScrapeJob con id={job_id}")

    google_maps_url = (job.google_maps_url or "").strip()
    if not google_maps_url:
        raise ValueError("El job no tiene google_maps_url")

    run_row = ReviewCheckRun(
        job_id=job_id,
        source="apify",
        status="running",
        fetched_count=0,
        new_count=0,
    )
    db.add(run_row)
    db.commit()
    db.refresh(run_row)

    try:
        apify = ApifyWrapper()
        run, items = apify.check_latest_reviews(
            google_maps_url=google_maps_url,
            personal_data=personal_data,
        )

        job.apify_run_id = run.get("id")
        db.add(job)

        fetched = 0
        inserted = 0

        for item in items[:10]:
            fetched += 1
            n = normalize_review(item)

            already_exists = _review_exists_in_main_table(db, job_id=job_id, n=n)

            check_item = ReviewCheckItem(
                run_id=run_row.id,
                job_id=job_id,
                rating=int(n["rating"]) if n["rating"] is not None else None,
                text=n["text"],
                published_at=str(n["published_at"]) if n["published_at"] else None,
                author_name=n["author_name"],
                review_url=n["review_url"],
                raw=n["raw"],
                exists_in_reviews=already_exists,
                inserted_into_reviews=False,
            )
            db.add(check_item)
            db.flush()

            if not already_exists:
                review = Review(
                    job_id=job_id,
                    review_id=n["review_id"],
                    rating=int(n["rating"]) if n["rating"] is not None else None,
                    text=n["text"],
                    published_at=str(n["published_at"]) if n["published_at"] else None,
                    author_name=n["author_name"],
                    review_url=n["review_url"],
                    raw=n["raw"],
                )
                db.add(review)
                check_item.inserted_into_reviews = True
                inserted += 1

        run_row.status = "succeeded"
        run_row.fetched_count = fetched
        run_row.new_count = inserted

        db.add(run_row)
        db.commit()

        return {
            "ok": True,
            "job_id": job_id,
            "checked_count": fetched,
            "new_reviews_inserted": inserted,
            "apify_run_id": run.get("id"),
        }

    except Exception as e:
        run_row.status = "failed"
        run_row.error = str(e)
        db.add(run_row)
        db.commit()
        raise


def scrape_and_store(
    db: Session,
    google_maps_url: str,
    max_reviews: int,
    personal_data: bool,
    place_name: Optional[str] = None,
) -> tuple[ScrapeJob, int]:

    google_maps_url = google_maps_url.strip()

    if "maps.app.goo.gl" in google_maps_url:
        google_maps_url = expand_google_maps_short_url(google_maps_url)

    print("🔗 Google Maps URL final:", google_maps_url)

    if not is_valid_google_maps_url(google_maps_url):
        raise ValueError(
            "URL no válida de Google Maps (debe ser /maps/place, /maps/reviews o /maps/search)."
        )

    info = parse_google_maps_url(google_maps_url)
    place_key = build_place_key(info)

    def looks_like_place_id_or_url(v: str) -> bool:
        s = (v or "").strip()
        return (
            s.startswith("http")
            or s.startswith("place_id:")
            or ("place_id:" in s)
            or ("google.com/maps" in s)
        )

    incoming = (place_name or "").strip()

    fallback_candidates = [
        info.get("place_name"),
        info.get("name"),
        info.get("query_text"),
    ]

    fallback = next(
        (x for x in fallback_candidates if x and not looks_like_place_id_or_url(str(x))),
        None
    )

    place_name = incoming if (incoming and not looks_like_place_id_or_url(incoming)) else fallback

    existing_job = (
        db.query(ScrapeJob)
        .filter(ScrapeJob.place_key == place_key)
        .first()
    )

    if existing_job:
        if place_name and existing_job.place_name != place_name:
            existing_job.place_name = place_name
            db.add(existing_job)
            db.commit()
            db.refresh(existing_job)

        saved = (
            db.query(Review)
            .filter(Review.job_id == existing_job.id)
            .count()
        )
        print("♻️ Local ya scrapeado. Reutilizando reseñas.")
        return existing_job, saved

    job = ScrapeJob(
        google_maps_url=google_maps_url,
        place_key=place_key,
        place_name=place_name,
        actor_id=settings.APIFY_ACTOR_ID,
        status="running",
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        apify = ApifyWrapper()
        run, items = apify.run_reviews_actor(
            google_maps_url=google_maps_url,
            max_reviews=max_reviews,
            personal_data=personal_data,
        )

        job.apify_run_id = run.get("id")
        job.status = "succeeded"
        db.add(job)

        saved = 0
        for item in items:
            n = normalize_review(item)
            r = Review(
                job_id=job.id,
                review_id=n["review_id"],
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
        export_job_reviews(job.id, items)

        return job, saved

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        db.add(job)
        db.commit()
        raise