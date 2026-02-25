from urllib.parse import urlparse, parse_qs
import re
import urllib.parse
from urllib.parse import urlparse, parse_qs

def is_valid_google_maps_url(url: str) -> bool:
    """
    Validación flexible:
    - dominios Google Maps / Google
    - acepta:
        /maps/place
        /maps/reviews
        /maps/search
        /maps?cid=XXXX
        /maps/place/?q=place_id:XXXX   ✅
        /maps?q=place_id:XXXX          ✅
    - NO acepta consent.google.com (eso lo debes "unwrappear" antes)
    """
    try:
        u = urlparse((url or "").strip())
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()
        qs = parse_qs(u.query)

        # ❌ Si es consent, no es una URL final válida
        if "consent.google.com" in host:
            return False

        # Dominio válido (google maps)
        if "google" not in host:
            return False

        # ✅ Caso: https://www.google.com/maps?cid=XXXX
        if path.startswith("/maps") and "cid" in qs:
            return True

        # ✅ Caso: place_id en query (aunque path sea /maps o /maps/place)
        q = (qs.get("q", [None])[0] or "").strip()
        if "place_id:" in q:
            return True

        # ✅ Rutas válidas típicas
        if (
            "/maps/place" in path
            or "/maps/reviews" in path
            or "/maps/search" in path
        ):
            return True

        return False
    except Exception:
        return False


def parse_google_maps_url(url: str) -> dict:
    """
    Extrae información básica desde un enlace público de Google Maps
    SIN usar APIs de Google.
    """
    parsed = urllib.parse.urlparse(url)

    info = {
        "raw": url,
        "query_text": None,
        "lat": None,
        "lon": None,
    }

    # Coordenadas si existen: @lat,lon
    m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
    if m:
        info["lat"] = float(m.group(1))
        info["lon"] = float(m.group(2))

    # /maps/place/<nombre>
    parts = [p for p in parsed.path.split("/") if p]
    if "place" in parts:
        idx = parts.index("place")
        if idx + 1 < len(parts):
            info["query_text"] = (
                urllib.parse.unquote_plus(parts[idx + 1])
                .replace("+", " ")
                .strip()
            )

    # ?q=...
    qs = urllib.parse.parse_qs(parsed.query)
    if not info["query_text"] and "q" in qs:
        info["query_text"] = qs["q"][0].strip()

    # ✅ Extra: si viene por cid, guarda algo estable
    if not info["query_text"] and "cid" in qs:
        info["query_text"] = f"cid:{qs['cid'][0]}"

    return info


def build_place_key(info: dict) -> str:
    """
    Genera una clave estable para un local.
    """
    if info.get("lat") and info.get("lon"):
        return f"{info.get('query_text','').lower()}::{info['lat']}::{info['lon']}"

    return (info.get("query_text") or "").lower()
