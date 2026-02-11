# db.py (SYNC - estable para Railway)

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./data/app.db"
)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
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
