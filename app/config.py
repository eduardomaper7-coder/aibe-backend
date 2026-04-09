from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # 🔑 Apify
    APIFY_TOKEN: str | None = None

    # 👉 Nombre correcto (el que deberías usar en el código)
    APIFY_REVIEWS_ACTOR_ID: str = "compass~Google-Maps-Reviews-Scraper"
    APIFY_PLACES_ACTOR_ID: str = "compass/crawler-google-places"

    # 👉 Alias para evitar errores antiguos (compatibilidad)
    APIFY_ACTOR_ID: str | None = None

    # 🗄️ Base de datos
    DATABASE_URL: str = "sqlite:///./data/app.db"
    EXPORT_DIR: str = "./data/exports"

    # ☁️ Supabase
    SUPABASE_URL: str | None = None
    SUPABASE_SERVICE_ROLE_KEY: str | None = None


settings = Settings()