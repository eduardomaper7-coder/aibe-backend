from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services.geogrid_service import run_geogrid

router = APIRouter()

class GeoGridRequest(BaseModel):
    business_name: str = Field(..., min_length=2)
    keyword: str = Field(..., min_length=2)
    lat: float
    lng: float
    grid_size: int = 5
    spacing_meters: int = 500

@router.post("/api/geogrid/run")
def geogrid_run(payload: GeoGridRequest):
    try:
        return run_geogrid(
            business_name=payload.business_name,
            keyword=payload.keyword,
            lat=payload.lat,
            lng=payload.lng,
            grid_size=payload.grid_size,
            spacing_meters=payload.spacing_meters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))