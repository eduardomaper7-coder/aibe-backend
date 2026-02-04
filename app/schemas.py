from pydantic import BaseModel, Field, HttpUrl

from typing import Optional

class ScrapeRequest(BaseModel):
    google_maps_url: HttpUrl
    place_name: str | None = None   # ✅ NUEVO
    max_reviews: int
    personal_data: bool = True

class ScrapeResponse(BaseModel):
    job_id: int
    status: str
    reviews_saved: int

class JobStatusResponse(BaseModel):
    job_id: int
    status: str
    apify_run_id: str | None = None
    error: str | None = None
    reviews_saved: int
