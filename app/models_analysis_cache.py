# app/models_analysis_cache.py
from sqlalchemy import Column, Integer, String, Text, DateTime, UniqueConstraint
from datetime import datetime, timezone
from app.db import Base

class AnalysisCache(Base):
    __tablename__ = "analysis_cache"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, index=True, nullable=False)

    section = Column(String(50), index=True, nullable=False)      # "topics"
    params_key = Column(String(255), index=True, nullable=False)  # hash(from/to/max_topics)

    # ✅ Invalidación rápida (más fiable que published_at)
    source_reviews_count = Column(Integer, nullable=False, default=0)
    source_max_review_id = Column(Integer, nullable=False, default=0)

    payload_json = Column(Text, nullable=False)

    computed_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("job_id", "section", "params_key", name="uq_cache_job_section_params"),
    )
