"""
main.py — FastAPI application for AsyncZ Phase 1.

Endpoints:
    POST /jobs          → Accept job, insert as QUEUED, push to Redis, 202
    GET  /jobs/{job_id} → Return current job status from DB
    GET  /health        → Verify DB + Redis connectivity

Designed to handle k6 virtual-user concurrency:
    - All I/O is async (asyncpg + redis.asyncio)
    - DB session pool: 20 base + 40 overflow  (see db.py)
    - Redis pool: 50 connections               (see redis_client.py)
"""

import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from asyncpg import UniqueViolationError
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import Job
from app.redis_client import enqueue_job, ping_redis, init_arq_redis, close_arq_redis
from app.schemas import (
    JobCreateRequest,
    JobCreateResponse,
    JobStatusResponse,
    HealthResponse,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("asyncz.api")


# ---------------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_arq_redis()
    logger.info("AsyncZ API starting up…")
    yield
    await close_arq_redis()
    logger.info("AsyncZ API shutting down…")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Asynchrounous Job scheduling",
    description="Version - 1",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------
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
    Accepts a job payload, writes it to PostgreSQL as QUEUED,
    then pushes the job_id onto the Redis queue so a worker can pick it up.

    Returns 202 immediately — the client does NOT wait for execution.
    """
    # --- Idempotency check ---
    # If the client supplied a key we've already seen, return the existing job.
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
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for PG

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
        # Commit first — DB row is permanent before Redis is touched.
        # If Redis fails after this, the row stays as QUEUED and can be
        # re-enqueued by a recovery poller (Phase 2). The old order
        # (flush → LPUSH → commit) risked a ghost Redis entry with no DB row.
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "duplicate_idempotency_key"},
        )

    await enqueue_job(str(job_id))

    logger.info("Job %s queued", job_id)

    return JobCreateResponse(job_id=job_id, status="queued")


# ---------------------------------------------------------------------------
# GET /jobs/{job_id}
# ---------------------------------------------------------------------------
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
    Returns the latest state of a job directly from PostgreSQL.
    Poll this endpoint to track job progress.
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
    )


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    """
    Verifies that both PostgreSQL and Redis are reachable.
    Used by k6 scripts and monitoring to gate load tests.
    """
    # DB check
    try:
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as exc:
        logger.error("DB health check failed: %s", exc)
        db_status = "error"

    # Redis check
    redis_ok = await ping_redis()
    redis_status = "ok" if redis_ok else "error"

    overall = "ok" if db_status == "ok" and redis_status == "ok" else "degraded"

    return HealthResponse(status=overall, db=db_status, redis=redis_status)
