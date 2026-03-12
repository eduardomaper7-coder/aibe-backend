from datetime import datetime

from sqlalchemy import (
    String,
    Integer,
    DateTime,
    Text,
    JSON,
    ForeignKey,
    Boolean,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    google_maps_url: Mapped[str] = mapped_column(String, nullable=False)

    place_key: Mapped[str] = mapped_column(String, index=True, nullable=False)
    place_name: Mapped[str | None] = mapped_column(String, nullable=True)

    actor_id: Mapped[str] = mapped_column(String, nullable=False)
    apify_run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    status: Mapped[str] = mapped_column(String, nullable=False, default="created")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        server_default=func.now(),
    )

    reviews: Mapped[list["Review"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
    )


class Review(Base):
    __tablename__ = "reviews"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("scrape_jobs.id"), index=True)

    review_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[str | None] = mapped_column(String, nullable=True)

    author_name: Mapped[str | None] = mapped_column(String, nullable=True)
    review_url: Mapped[str | None] = mapped_column(String, nullable=True)

    raw: Mapped[dict] = mapped_column(JSON, nullable=False)

    job: Mapped["ScrapeJob"] = relationship(back_populates="reviews")


class ReviewCheckRun(Base):
    __tablename__ = "review_check_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    source: Mapped[str] = mapped_column(String(50), nullable=False, default="apify")
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="running")

    fetched_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        server_default=func.now(),
        nullable=False,
    )

    items: Mapped[list["ReviewCheckItem"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class ReviewCheckItem(Base):
    __tablename__ = "review_check_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("review_check_runs.id"),
        index=True,
        nullable=False,
    )
    job_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[str | None] = mapped_column(String, nullable=True)
    author_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    review_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    exists_in_reviews: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
    inserted_into_reviews: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    run: Mapped["ReviewCheckRun"] = relationship(back_populates="items")


class GoogleOAuth(Base):
    __tablename__ = "google_oauth"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    email: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    refresh_token: Mapped[str] = mapped_column(Text, nullable=False)

    google_user_id: Mapped[str | None] = mapped_column(String, nullable=True)
    google_account_id: Mapped[str | None] = mapped_column(String, nullable=True)
    scope: Mapped[str | None] = mapped_column(Text, nullable=True)

    connected: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
        server_default=func.now(),
    )