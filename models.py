# aibe-backend/models.py
from sqlalchemy import Column, Integer, String, Boolean, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)

    # Info de Google
    google_connected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    google_sub: Mapped[str | None] = mapped_column(String(128))
    google_email: Mapped[str | None] = mapped_column(String(255))

    access_token: Mapped[str | None] = mapped_column(String(2048))
    refresh_token: Mapped[str | None] = mapped_column(String(2048))
    token_type: Mapped[str | None] = mapped_column(String(32))
    expires_at: Mapped[int | None] = mapped_column(BigInteger)  # epoch seconds

    updated_at: Mapped[int | None] = mapped_column(BigInteger, server_default=None)
