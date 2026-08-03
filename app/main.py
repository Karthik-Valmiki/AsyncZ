"""
main.py — FastAPI application entry point.

Endpoints:
    POST /jobs          → Accept job, insert as QUEUED, push to ARQ, return 202
    GET  /jobs/{job_id} → Return current job status from PostgreSQL
    GET  /dlq           → Inspect Dead Letter Queue contents from Redis
    GET  /health        → Verify DB + Redis connectivity and DLQ depth
"""

import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import json
import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db, engine, Base
from app.models import Job
from app.redis_client import enqueue_job, ping_redis, init_arq_redis, close_arq_redis, arq_redis
from app.schemas import (
    JobCreateRequest,
    JobCreateResponse,
    JobStatusResponse,
    HealthResponse,
    DLQResponse,
    DLQJobEntry,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("asyncz.api")

DLQ_KEY = "asyncz:dlq"


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await init_arq_redis()
    logger.info("AsyncZ API started.")
    yield
    await close_arq_redis()
    logger.info("AsyncZ API shut down.")


app = FastAPI(
    title="AsyncZ — Asynchronous Job Queue",
    description="High-throughput async job scheduler with retries, DLQ, and zombie recovery.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post(
    "/jobs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobCreateResponse,
    summary="Submit a new job",
)
async def create_job(
    body: JobCreateRequest,
    db: AsyncSession = Depends(get_db),
) -> JobCreateResponse:
    """
    Writes the job to PostgreSQL as QUEUED, then pushes the job_id to the ARQ
    queue. Returns 202 immediately — the client does not wait for execution.

    If an idempotency_key is supplied and already exists, returns 409 with the
    original job_id so the client can poll that instead of resubmitting.
    """
    if body.idempotency_key is not None:
        existing = await db.scalar(
            select(Job).where(Job.idempotency_key == body.idempotency_key)
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "duplicate_idempotency_key",
                    "job_id": str(existing.id),
                    "status": existing.status,
                },
            )

    job_id = uuid.uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    job = Job(
        id=job_id,
        status="queued",
        payload=body.payload,
        retry_count=0,
        max_retries=body.max_retries,
        created_at=now,
        updated_at=now,
        idempotency_key=body.idempotency_key,
    )
    db.add(job)

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        # Race condition: two concurrent requests with the same key both passed
        # the pre-check. The unique constraint caught the second one.
        existing = await db.scalar(
            select(Job).where(Job.idempotency_key == body.idempotency_key)
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "duplicate_idempotency_key",
                "job_id": str(existing.id) if existing else None,
                "status": existing.status if existing else None,
            },
        )

    await enqueue_job(str(job_id))
    logger.info("Job %s queued.", job_id)
    return JobCreateResponse(job_id=job_id, status="queued")


@app.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    summary="Get job status",
)
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> JobStatusResponse:
    """
    Returns the current state of a job from PostgreSQL.
    Poll this endpoint to track job progress.

    Possible status values: queued | processing | completed | dead
    """
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )

    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        payload=job.payload,
        retry_count=job.retry_count,
        max_retries=job.max_retries,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        last_error=job.last_error,
        worker_id=job.worker_id,
        heartbeat_at=job.heartbeat_at,
    )


@app.get(
    "/dlq",
    response_model=DLQResponse,
    summary="Inspect the Dead Letter Queue",
)
async def get_dlq() -> DLQResponse:
    """
    Returns jobs that permanently failed after exhausting all retries.
    The DLQ is stored in Redis and capped at 1,000 entries.
    """
    raw_entries: list[bytes] = await arq_redis.lrange(DLQ_KEY, 0, -1)

    jobs: list[DLQJobEntry] = []
    for raw in raw_entries:
        try:
            data = json.loads(raw)
            jobs.append(
                DLQJobEntry(
                    job_id=data["job_id"],
                    payload=data["payload"],
                    retry_count=data["retry_count"],
                    last_error=data.get("last_error"),
                    failed_at=data["failed_at"],
                )
            )
        except Exception:
            continue

    return DLQResponse(count=len(jobs), jobs=jobs)


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    """
    Verifies PostgreSQL and Redis connectivity.
    Returns DLQ depth — if this is growing, job execution is broken.
    """
    try:
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as exc:
        logger.error("DB health check failed: %s", exc)
        db_status = "error"

    redis_ok = await ping_redis()
    redis_status = "ok" if redis_ok else "error"

    try:
        dlq_length = await arq_redis.llen(DLQ_KEY)
    except Exception:
        dlq_length = -1

    overall = "ok" if db_status == "ok" and redis_status == "ok" else "degraded"

    return HealthResponse(
        status=overall,
        db=db_status,
        redis=redis_status,
        dlq_length=dlq_length,
    )
