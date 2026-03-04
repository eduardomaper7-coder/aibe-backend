import os
import time
import json
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID")


def _apify_run(input_payload: dict):
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY_TOKEN missing")
    if not APIFY_ACTOR_ID:
        raise RuntimeError("APIFY_ACTOR_ID missing")

    run_url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs?token={APIFY_TOKEN}"

    r = requests.post(run_url, json=input_payload, timeout=60)
    r.raise_for_status()

    j = r.json()
    if "data" not in j:
        raise RuntimeError(f"Apify run create error: {j}")

    run_id = j["data"]["id"]

    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"

    for _ in range(120):  # ~10 min
        s = requests.get(status_url, timeout=30).json()

        if "data" not in s:
            raise RuntimeError(f"Apify status error: {s}")

        status = s["data"]["status"]

        if status == "SUCCEEDED":
            dataset_id = s["data"].get("defaultDatasetId")
            if not dataset_id:
                return []

            items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&token={APIFY_TOKEN}"
            items = requests.get(items_url, timeout=60).json()
            return items if isinstance(items, list) else []

        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run failed: {status}")

        time.sleep(5)

    raise RuntimeError("Apify timeout")


def sync_reviews_for_job(db: Session, job_id: int) -> dict:
    """
    Revisa reseñas de un job_id vía Apify y guarda nuevas.
    Devuelve {job_id, fetched, inserted}.
    """
    row = db.execute(
        text("select google_maps_url from scrape_jobs where id=:jid"),
        {"jid": job_id},
    ).fetchone()

    if not row or not row[0]:
        return {"job_id": job_id, "fetched": 0, "inserted": 0, "note": "no google_maps_url"}

    google_maps_url = row[0]

    # Input para: compass~Google-Maps-Reviews-Scraper
    apify_input = {
        "startUrls": [{"url": google_maps_url}],
        "maxReviews": 10000,
        "reviewsSort": "newest",
    }

    items = _apify_run(apify_input)

    inserted = 0

    for it in items:
        # Campos típicos del actor (fallbacks por si cambia)
        rating = it.get("stars") or it.get("rating")
        text_content = it.get("text") or it.get("reviewText") or ""
        author_name = it.get("name") or it.get("authorName") or None
        published_at = it.get("publishedAtDate") or it.get("publishedAt") or it.get("date") or None
        review_url = it.get("reviewUrl") or it.get("url") or None

        # ✅ raw NOT NULL en tu tabla -> guardamos el JSON completo
        raw = json.dumps(it, ensure_ascii=False)

        res = db.execute(
            text("""
                insert into reviews (job_id, rating, text, author_name, published_at, review_url, raw)
                values (:job_id, :rating, :text, :author_name, :published_at, :review_url, :raw)
                on conflict do nothing
            """),
            {
                "job_id": job_id,
                "rating": rating,
                "text": text_content,
                "author_name": author_name,
                "published_at": published_at,
                "review_url": review_url,
                "raw": raw,
            },
        )

        if getattr(res, "rowcount", 0) == 1:
            inserted += 1

    db.commit()
    return {"job_id": job_id, "fetched": len(items), "inserted": inserted}


def sync_reviews_all(db: Session) -> dict:
    jobs = db.execute(text("select id from scrape_jobs")).fetchall()
    out = []
    for (jid,) in jobs:
        out.append(sync_reviews_for_job(db, int(jid)))
    return {"ok": True, "results": out}