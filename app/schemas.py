from pydantic import BaseModel, Field, HttpUrl

class ScrapeRequest(BaseModel):
    google_maps_url: HttpUrl = Field(..., description="URL del lugar en Google Maps")
    max_reviews: int = Field(99999, ge=0, description="Para 'todas', usa 99999 (recomendado por el actor).")

    # Opcional: si quieres activar/desactivar datos personales (GDPR)
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
