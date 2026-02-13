# app/db.py  (SYNC ONLY - Railway safe)

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session


def _normalize_database_url(url: str) -> str:
    url = (url or "").strip()

    if not url:
        return "sqlite:///./data/app.db"

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)

    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    if url.startswith("sqlite+aiosqlite://"):
        url = url.replace("sqlite+aiosqlite://", "sqlite://", 1)

    return url


DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL", ""))

print("🧩 DB URL (app/db.py):", DATABASE_URL)

connect_args = {}
if DATABASE_URL.startswith("sqlite://"):
    connect_args = {"check_same_thread": False}

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    future=True,
)

Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
