import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# Request


class JobCreateRequest(BaseModel):
    """Body for POST /jobs."""

    payload: dict[str, Any] = Field(
        ...,
        description="Arbitrary JSON payload describing the work to be done.",
        examples=[{"task": "send_email", "to": "user@example.com"}],
    )
    idempotency_key: uuid.UUID | None = Field(
        default=None,
        description="Optional client-supplied UUID. Duplicate submissions with "
        "the same key are rejected with 409.",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=5,
        description="Maximum retry attempts (0–5). Kept ≤5 to preserve low latency.",
    )


# Responses
class JobCreateResponse(BaseModel):
    """Returned immediately after a job is accepted (HTTP 202)."""

    job_id: uuid.UUID
    status: str  # always "queued"


class JobStatusResponse(BaseModel):
    """Full job state returned by GET /jobs/{job_id}."""

    job_id: uuid.UUID
    status: str
    payload: dict[str, Any]
    retry_count: int
    max_retries: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None
    worker_id: str | None

    model_config = {"from_attributes": True}


class HealthResponse(BaseModel):
    status: str
    db: str
    redis: str
