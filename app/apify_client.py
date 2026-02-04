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

            # ✅ Proxy (2 formatos por compatibilidad)
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],  # si no tienes, usa ["AUTO"]
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],  # o ["AUTO"]
            },

            # ✅ (opcional) si el actor lo soporta, reduce rate-limit
            "maxConcurrency": 1,
        }

        print("🧪 ACTOR ID:", settings.APIFY_ACTOR_ID)
        print("🧪 ACTOR INPUT:", actor_input)

        run = (
            self.client
            .actor(settings.APIFY_ACTOR_ID)
            .call(run_input=actor_input)
        )

        print("🧪 APIFY RUN:", run)

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError("El run no devolvió defaultDatasetId")

        items = list(self.client.dataset(dataset_id).iterate_items())
        print("🧪 APIFY ITEMS RECIBIDOS:", len(items))

        return run, items
