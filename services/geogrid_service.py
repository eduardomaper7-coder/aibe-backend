from services.grid_generator import generate_grid
from services.serp_provider import get_local_rank


def run_geogrid(
    business_name: str,
    keyword: str,
    lat: float,
    lng: float,
    grid_size: int = 5,
    spacing_meters: int = 500,
):
    points = generate_grid(
        center_lat=lat,
        center_lng=lng,
        grid_size=grid_size,
        spacing_meters=spacing_meters,
    )

    grid = []
    for point in points:
        rank = get_local_rank(
            business_name=business_name,
            keyword=keyword,
            lat=point["lat"],
            lng=point["lng"],
        )
        grid.append({
            "lat": point["lat"],
            "lng": point["lng"],
            "rank": rank,
            "label": point["label"],
        })

    return {
        "business_name": business_name,
        "keyword": keyword,
        "center": {
            "lat": lat,
            "lng": lng,
        },
        "grid": grid,
    }