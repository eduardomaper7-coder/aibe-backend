from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, HttpUrl
from sqlalchemy.orm import Session
from sqlalchemy import text
import os

from app.db import get_db
from app.review_requests import repo as review_repo

router = APIRouter(tags=["jobs"])

ADMIN_UNLOCK_KEY = os.getenv("ADMIN_UNLOCK_KEY")


class SetupBusinessIn(BaseModel):
    place_name: str
    city: str | None = None
    google_maps_url: HttpUrl
    google_place_id: str | None = None


@router.get("/jobs/{job_id}/entitlements")
def entitlements(
    job_id: int,
    admin_unlock: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    if ADMIN_UNLOCK_KEY and admin_unlock == ADMIN_UNLOCK_KEY:
        return {"isPro": True}

    row = db.execute(
        text("""
        select u.subscription_status
        from scrape_jobs j
        join users u on j.user_id = u.id
        where j.id=:id
        """),
        {"id": job_id},
    ).fetchone()

    if not row:
        return {"isPro": False}

    return {
        "isPro": row[0] in ("active", "trialing")
    }


@router.get("/jobs/my-latest/{user_id}")
def my_latest_job(user_id: str, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            select id
            from scrape_jobs
            where user_id = :uid
            order by created_at desc
            limit 1
        """),
        {"uid": user_id},
    ).fetchone()

    if not row:
        return {"job_id": None}

    return {"job_id": row[0]}


@router.get("/jobs/{job_id}/meta")
def job_meta(job_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            select
                j.id,
                j.place_name,
                j.city,
                j.google_maps_url,
                j.status,
                j.created_at,
                j.updated_at,
                (
                    select count(*)
                    from reviews r
                    where r.job_id = j.id
                ) as reviews_count
            from scrape_jobs j
            where j.id = :jid
        """),
        {"jid": job_id},
    ).fetchone()

    if not row:
        return {
            "job_id": job_id,
            "exists": False,
        }

    reviews_count = int(row[7] or 0)
    has_business = bool(row[1] and row[3])

    return {
        "job_id": row[0],
        "exists": True,
        "place_name": row[1],
        "city": row[2],
        "google_maps_url": row[3],
        "status": row[4],
        "created_at": row[5],
        "updated_at": row[6],
        "reviews_count": reviews_count,
        "needs_setup": not has_business,
        "needs_analysis": has_business and reviews_count == 0,
    }


@router.post("/jobs/{job_id}/setup-business")
def setup_business(
    job_id: int,
    payload: SetupBusinessIn,
    db: Session = Depends(get_db),
):
    row = db.execute(
        text("""
            select id
            from scrape_jobs
            where id = :jid
        """),
        {"jid": job_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job no encontrado")

    google_review_url = None
    if payload.google_place_id:
        google_review_url = review_repo.build_review_url_from_place_id(payload.google_place_id)

    db.execute(
        text("""
            update scrape_jobs
            set
                place_name = :place_name,
                city = :city,
                google_maps_url = :google_maps_url,
                place_key = coalesce(place_key, :place_key),
                status = case
                    when status = 'pending_setup' then 'pending_analysis'
                    else status
                end,
                updated_at = now()
            where id = :jid
        """),
        {
            "jid": job_id,
            "place_name": payload.place_name.strip(),
            "city": (payload.city or "").strip() or None,
            "google_maps_url": str(payload.google_maps_url).strip(),
            "place_key": f"setup:{job_id}:{payload.place_name.strip().lower()}",
        },
    )

    review_repo.upsert_business_settings(
        db,
        job_id=job_id,
        business_name=payload.place_name.strip(),
        google_place_id=(payload.google_place_id or "").strip() or None,
        google_review_url=google_review_url,
    )

    meta = db.execute(
        text("""
            select id, place_name, city, google_maps_url, status
            from scrape_jobs
            where id = :jid
        """),
        {"jid": job_id},
    ).fetchone()

    return {
        "ok": True,
        "job_id": meta[0],
        "place_name": meta[1],
        "city": meta[2],
        "google_maps_url": meta[3],
        "status": meta[4],
        "google_review_url": google_review_url,
    }