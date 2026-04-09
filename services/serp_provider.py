import os
from typing import Any, Dict, List, Optional

import requests


SERP_API_KEY = os.getenv("SERP_API_KEY", "").strip()


class SerpProviderError(Exception):
    pass


def normalize_text(value: str) -> str:
    return (value or "").strip().lower()


def build_ll(lat: float, lng: float, zoom: int = 14) -> str:
    return f"@{lat},{lng},{zoom}z"


def fetch_local_results(
    keyword: str,
    lat: float,
    lng: float,
    hl: str = "es",
) -> List[Dict[str, Any]]:
    if not SERP_API_KEY:
        raise SerpProviderError("Missing SERP_API_KEY")

    url = "https://serpapi.com/search"
    params = {
        "engine": "google_maps",
        "q": keyword,
        "type": "search",
        "ll": build_ll(lat, lng, 14),
        "hl": hl,
        "api_key": SERP_API_KEY,
        "no_cache": "true",
    }

    response = requests.get(url, params=params, timeout=45)

    if response.status_code >= 400:
        raise SerpProviderError(f"SerpAPI HTTP {response.status_code}: {response.text}")

    data = response.json()

    if "error" in data:
        raise SerpProviderError(f"SerpAPI error: {data['error']}")

    return data.get("local_results", []) or []


def score_title_match(result_title: str, business_name: str) -> int:
    result_norm = normalize_text(result_title)
    target_norm = normalize_text(business_name)

    if not result_norm or not target_norm:
        return 0

    if result_norm == target_norm:
        return 100

    if target_norm in result_norm:
        return 80

    result_tokens = set(result_norm.replace(",", " ").split())
    target_tokens = set(target_norm.replace(",", " ").split())

    if not target_tokens:
        return 0

    overlap = len(result_tokens & target_tokens)
    return int((overlap / max(len(target_tokens), 1)) * 60)


def extract_rank_from_results(
    results: List[Dict[str, Any]],
    business_name: str,
) -> Optional[int]:
    best_rank = None
    best_score = 0

    for idx, item in enumerate(results, start=1):
        title = item.get("title") or item.get("name") or ""
        score = score_title_match(title, business_name)

        if score > best_score:
            best_score = score
            best_rank = idx

    if best_score < 40:
        return None

    return best_rank


def get_local_rank(
    business_name: str,
    keyword: str,
    lat: float,
    lng: float,
) -> int:
    results = fetch_local_results(keyword=keyword, lat=lat, lng=lng)
    rank = extract_rank_from_results(results, business_name)
    return rank if rank is not None else 20


def find_business_coordinates(business_name: str) -> Optional[Dict[str, float]]:
    if not SERP_API_KEY:
        raise SerpProviderError("Missing SERP_API_KEY")

    url = "https://serpapi.com/search"
    params = {
        "engine": "google_maps",
        "q": business_name,
        "type": "search",
        "hl": "es",
        "api_key": SERP_API_KEY,
        "no_cache": "true",
    }

    print("🔎 SerpAPI coords query:", business_name)

    response = requests.get(url, params=params, timeout=45)

    if response.status_code >= 400:
        raise SerpProviderError(f"SerpAPI HTTP {response.status_code}: {response.text}")

    data = response.json()

    if "error" in data:
        raise SerpProviderError(f"SerpAPI error: {data['error']}")

    results = data.get("local_results", []) or []
    print("📦 local_results encontrados:", len(results))

    if not results:
        return None

    first = results[0]
    print("🥇 primer resultado:", first.get("title"))

    gps = first.get("gps_coordinates") or {}
    lat = gps.get("latitude")
    lng = gps.get("longitude")

    print("📍 gps:", gps)

    if lat is None or lng is None:
        return None

    return {
        "lat": float(lat),
        "lng": float(lng),
    }