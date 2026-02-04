from fastapi import APIRouter, Header, HTTPException
import requests

router = APIRouter(prefix="/gbp", tags=["gbp"])

GOOGLE_ACCOUNTS_URL = "https://mybusinessaccountmanagement.googleapis.com/v1/accounts"
GOOGLE_LOCATIONS_URL = "https://mybusinessbusinessinformation.googleapis.com/v1/{account}/locations"
GOOGLE_REVIEWS_URL = "https://mybusiness.googleapis.com/v4/{locationName}/reviews"

def google_get(url: str, access_token: str, params=None):
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail=r.text)
    return r.json()

@router.get("/locations")
def list_locations(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    access_token = authorization.split(" ", 1)[1]

    # 1) listar accounts
    accounts = google_get(GOOGLE_ACCOUNTS_URL, access_token).get("accounts", [])

    locations_out = []
    for acc in accounts:
        acc_name = acc["name"]  # "accounts/123"
        # 2) listar locations por account
        locs = google_get(
            GOOGLE_LOCATIONS_URL.format(account=acc_name),
            access_token,
            params={"readMask": "name,title,storefrontAddress"},
        ).get("locations", [])

        for loc in locs:
            title = loc.get("title", "")
            addr = ""
            sa = loc.get("storefrontAddress")
            if sa:
                addr = " ".join([sa.get("addressLines", [""])[0], sa.get("locality", ""), sa.get("postalCode", "")]).strip()
            locations_out.append({"name": loc["name"], "title": title, "address": addr})

    return {"locations": locations_out}

@router.post("/reviews/sync")
def sync_reviews(payload: dict, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    access_token = authorization.split(" ", 1)[1]

    location_name = payload.get("locationName")
    if not location_name:
        raise HTTPException(status_code=400, detail="locationName required")

    saved = 0
    page_token = None

    while True:
        params = {"pageSize": 200}
        if page_token:
            params["pageToken"] = page_token

        data = google_get(GOOGLE_REVIEWS_URL.format(locationName=location_name), access_token, params=params)

        reviews = data.get("reviews", [])
        # TODO: aquí guardas en tu DB (aibe.db)
        saved += len(reviews)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return {"saved": saved}
