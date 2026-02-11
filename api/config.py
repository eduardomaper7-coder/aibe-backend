# api/config.py
import os
from functools import lru_cache

try:
    # Pydantic v2
    from pydantic_settings import BaseSettings
except Exception:
    # Pydantic v1 fallback
    from pydantic import BaseSettings


class Settings(BaseSettings):
    # OAuth Google
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = ""  # ej: https://tu-backend.../google/callback

    # Tu app (si usas cookies o JWT)
    JWT_SECRET: str = os.getenv("JWT_SECRET", "dev-secret")

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
