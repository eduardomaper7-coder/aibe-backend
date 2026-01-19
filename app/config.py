from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APIFY_TOKEN: str
    APIFY_ACTOR_ID: str = "compass/google-maps-reviews-scraper"

    DATABASE_URL: str = "sqlite:///./data/app.db"
    EXPORT_DIR: str = "./data/exports"

    # 🔥 AÑADIR ESTO
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: str

settings = Settings()
