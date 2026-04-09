from apify_client import ApifyClient
from .config import settings


class ApifyWrapper:
    def __init__(self) -> None:
        self.client = ApifyClient(settings.APIFY_TOKEN)

    def run_reviews_actor(
        self,
        google_maps_url: str,
        max_reviews: int,
        personal_data: bool,
    ):
        actor_input = {
            "startUrls": [{"url": google_maps_url}],
            "maxItems": int(max_reviews),
            "maxReviews": int(max_reviews),
            "maxResults": int(max_reviews),
            "reviewsLimit": int(max_reviews),
            "maxReviewsPerPlace": int(max_reviews),
            "reviewsSort": "newest",
            "personalData": personal_data,
            "language": "es",
            "reviewsOrigin": "all",
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
            "maxConcurrency": 1,
        }

        print("🧪 REVIEWS ACTOR ID:", settings.APIFY_REVIEWS_ACTOR_ID)
        print("🧪 REVIEWS ACTOR INPUT:", actor_input)

        run = (
            self.client
            .actor(settings.APIFY_REVIEWS_ACTOR_ID)
            .call(run_input=actor_input)
        )

        print("🧪 APIFY REVIEWS RUN:", run)

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError("El run no devolvió defaultDatasetId")

        items = list(self.client.dataset(dataset_id).iterate_items())
        print("🧪 APIFY REVIEWS ITEMS RECIBIDOS:", len(items))

        return run, items

    def find_place_coordinates(
        self,
        clinic_name: str,
        city: str,
    ):
        actor_input = {
            "searchStringsArray": [clinic_name],
            "locationQuery": city,
            "maxCrawledPlacesPerSearch": 5,
            "language": "es",
            "includeWebResults": False,
            "maxReviews": 0,
            "maxImages": 0,
            "maximumLeadsEnrichmentRecords": 0,
        }

        print("🧪 PLACES ACTOR ID:", settings.APIFY_PLACES_ACTOR_ID)
        print("🧪 PLACES ACTOR INPUT:", actor_input)

        run = (
            self.client
            .actor(settings.APIFY_PLACES_ACTOR_ID)
            .call(run_input=actor_input)
        )

        print("🧪 APIFY PLACES RUN:", run)

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            return None

        items = list(self.client.dataset(dataset_id).iterate_items())
        print("🧪 APIFY PLACES ITEMS RECIBIDOS:", len(items))

        if not items:
            return None

        target = (clinic_name or "").strip().lower()

        def extract_coords(item: dict):
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

            lat = item.get("lat") or item.get("latitude")
            lng = item.get("lng") or item.get("lon") or item.get("longitude")
            if lat is not None and lng is not None:
                return {"lat": float(lat), "lng": float(lng)}

            return None

        for item in items:
            title = str(item.get("title") or item.get("name") or "").strip().lower()
            if target and (title == target or target in title):
                coords = extract_coords(item)
                print("🎯 MATCH APIFY PLACE:", item.get("title") or item.get("name"), coords)
                if coords:
                    return coords

        for item in items:
            coords = extract_coords(item)
            if coords:
                print("🥇 FALLBACK APIFY PLACE:", item.get("title") or item.get("name"), coords)
                return coords

        print("⚠️ APIFY no devolvió coords útiles. Primer item:", items[0] if items else None)
        return None

    def check_latest_reviews(
        self,
        google_maps_url: str,
        personal_data: bool = True,
    ):
        return self.run_reviews_actor(
            google_maps_url=google_maps_url,
            max_reviews=10,
            personal_data=personal_data,
        )