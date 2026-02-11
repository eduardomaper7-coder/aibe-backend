from functools import lru_cache
from pydantic import Field

try:
    # Pydantic v2
    from pydantic_settings import BaseSettings, SettingsConfigDict
except Exception:
    # Fallback por si acaso
    from pydantic import BaseSettings

    class SettingsConfigDict(dict):
        pass


class Settings(BaseSettings):
    # Google OAuth (lee variables Railway)
    google_client_id: str = Field(default="", alias="GOOGLE_CLIENT_ID")
    google_client_secret: str = Field(default="", alias="GOOGLE_CLIENT_SECRET")
    google_redirect_uri: str = Field(default="", alias="GOOGLE_REDIRECT_URI")

    # Scopes (si no lo pones, usa uno por defecto)
    google_oauth_scopes: str = Field(
        default="openid email profile https://www.googleapis.com/auth/business.manage",
        alias="GOOGLE_OAUTH_SCOPES",
    )

    # Frontend redirect post-login
    frontend_post_login_url: str = Field(default="", alias="FRONTEND_POST_LOGIN_URL")

    # (opcional)
    jwt_secret: str = Field(default="dev-secret", alias="JWT_SECRET")

    # Pydantic v2 config
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
