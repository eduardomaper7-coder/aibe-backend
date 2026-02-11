from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APIFY_TOKEN: str | None = None
    APIFY_ACTOR_ID: str = "compass/google-maps-reviews-scraper"

    DATABASE_URL: str = "sqlite:///./data/app.db"
    EXPORT_DIR: str = "./data/exports"

    # Supabase (hazlos opcionales si quieres que el backend arranque sin Supabase)
    SUPABASE_URL: str | None = None
    SUPABASE_SERVICE_ROLE_KEY: str | None = None

settings = Settings()
