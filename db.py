# aibe-backend/db.py
import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base


def _normalize_database_url(url: str) -> str:
    """
    Normaliza DATABASE_URL para SQLAlchemy async:
    - sqlite:///...                -> sqlite+aiosqlite:///...
    - postgresql://...             -> postgresql+asyncpg://...
    - postgres://... (heroku style)-> postgresql+asyncpg://...
    Si ya viene con +aiosqlite o +asyncpg, lo deja tal cual.
    """
    url = (url or "").strip()

    # Default razonable si no viene nada
    if not url:
        return "sqlite+aiosqlite:///./aibe.db"

    # SQLite sync -> async driver
    if url.startswith("sqlite:///") and not url.startswith("sqlite+aiosqlite:///"):
        return url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)

    # Heroku-style "postgres://" -> "postgresql+asyncpg://"
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)

    # Postgres sync -> async driver
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return url


DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL"))

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,  # ayuda en Railway/containers
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

Base = declarative_base()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
