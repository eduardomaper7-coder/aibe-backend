# aibe-backend/models.py
from sqlalchemy import Column, Integer, String, Boolean, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class User(Base):
    __tablename__ = "users"

    id = mapped_column(String(36), primary_key=True)
    email = mapped_column(String(255), unique=True, index=True, nullable=False)

    password_hash = mapped_column(String(255), nullable=True)

    stripe_customer_id = mapped_column(String(128), nullable=True)
    subscription_id = mapped_column(String(128), nullable=True)
    subscription_status = mapped_column(String(32), default="inactive")

    updated_at = mapped_column(BigInteger, nullable=True)