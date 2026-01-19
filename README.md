### Setup

1) Crea un .env (puedes copiar .env.example)
2) Instala deps:
   pip install -r requirements.txt

3) Ejecuta:
   uvicorn main:app --reload

### Uso

POST /scrape
{
  "google_maps_url": "https://www.google.com/maps/place/....",
  "max_reviews": 99999,
  "personal_data": true
}

GET /jobs/{id}
