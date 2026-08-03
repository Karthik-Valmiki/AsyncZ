"""
models.py — SQLAlchemy ORM models matching the DB schema exactly.

Tables:
    jobs                — master job records
    job_execution_logs  — per-attempt audit trail
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class Job(Base):
    __tablename__ = "jobs"

    # Primary key — UUID generated application-side for predictability
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Lifecycle status: queued | processing | completed | dead
    status: Mapped[str] = mapped_column(String(20), nullable=False)

    # Arbitrary JSON payload sent by the client
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Retry bookkeeping
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Failure info
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Worker that owns this job right now
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Heartbeat + idempotency
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    idempotency_key: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True, unique=True
    )

    # Relationship
    logs: Mapped[list["JobExecutionLog"]] = relationship(
        "JobExecutionLog", back_populates="job", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} status={self.status}>"


class JobExecutionLog(Base):
    __tablename__ = "job_execution_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False
    )

    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)

    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Attempt status: started | completed | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Relationship
    job: Mapped["Job"] = relationship("Job", back_populates="logs")

    def __repr__(self) -> str:
        return (
            f"<JobExecutionLog job_id={self.job_id} "
            f"attempt={self.attempt_number} status={self.status}>"
        )
