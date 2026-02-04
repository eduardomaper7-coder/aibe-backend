from sqlalchemy import Column, Integer, String, Text, DateTime, UniqueConstraint
from datetime import datetime, timezone
from app.db import Base

class ReviewAIReply(Base):
    __tablename__ = "review_ai_replies"

    id = Column(Integer, primary_key=True, index=True)

    review_id = Column(Integer, index=True, nullable=False)
    job_id = Column(Integer, index=True, nullable=False)

    input_hash = Column(String(64), nullable=False)   # hash del texto+rating
    reply_text = Column(Text, nullable=False)

    model_used = Column(String(64), nullable=False, default="gpt-4.1-mini")
    tone = Column(String(32), nullable=False, default="default")

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("review_id", name="uq_review_ai_replies_review_id"),
    )
