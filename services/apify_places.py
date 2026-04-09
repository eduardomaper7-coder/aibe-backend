import os
from typing import Optional, Dict, Any

from apify_client import ApifyClient


APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()


class ApifyPlacesError(Exception):
    pass


def _extract_coords_from_item(item: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """
    Apify documenta que el actor devuelve coordenadas, pero la forma exacta del
    campo puede variar según configuración / versión. Por eso probamos varias.
    """
    candidates = [
        item.get("coordinates"),
        item.get("location"),
        item.get("gpsCoordinates"),
        item.get("placeCoordinates"),
    ]

    for c in candidates:
        if isinstance(c, dict):
            lat = c.get("lat") or c.get("latitude")
            lng = c.get("lng") or c.get("lon") or c.get("longitude")
            if lat is not None and lng is not None:
                return {"lat": float(lat), "lng": float(lng)}

    # Algunos actores devuelven lat/lng planos
    lat = item.get("lat") or item.get("latitude")
    lng = item.get("lng") or item.get("lon") or item.get("longitude")
    if lat is not None and lng is not None:
        return {"lat": float(lat), "lng": float(lng)}

    return None


def find_business_coordinates_apify(
    business_name: str,
    city: str = "",
) -> Optional[Dict[str, float]]:
    if not APIFY_TOKEN:
        raise ApifyPlacesError("Missing APIFY_TOKEN")

    client = ApifyClient(APIFY_TOKEN)

    query = f"{business_name} {city}".strip()

    run_input = {
        "searchStringsArray": [business_name],
        "locationQuery": city or "Spain",
        "maxCrawledPlacesPerSearch": 5,
        "language": "es",
        "maxReviews": 0,
        "maximumLeadsEnrichmentRecords": 0,
        "maxImages": 0,
        "includeWebResults": False,
    }

    run = client.actor("compass/crawler-google-places").call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]

    items = list(client.dataset(dataset_id).iterate_items())
    print("📦 Apify items:", len(items))

    if not items:
        return None

    target = business_name.strip().lower()

    # 1) intenta match por título
    for item in items:
        title = str(item.get("title") or "").strip().lower()
        if target and (title == target or target in title):
            coords = _extract_coords_from_item(item)
            print("🎯 Apify match por title:", item.get("title"), coords)
            if coords:
                return coords

    # 2) fallback: primer resultado con coords
    for item in items:
        coords = _extract_coords_from_item(item)
        if coords:
            print("🥇 Apify fallback primer item con coords:", item.get("title"), coords)
            return coords

    # 3) debug útil
    print("⚠️ Apify no devolvió coords en items. Primer item:", items[0] if items else None)
    return None