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

import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import Job
from app.redis_client import enqueue_job, ping_redis
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
    logger.info("AsyncZ API starting up…")
    yield
    logger.info("AsyncZ API shutting down…")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AsyncZ",
    description="High-performance async job queue — Phase 1",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount frontend directory for static assets
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/", summary="Serve Frontend Dashboard")
async def serve_frontend():
    return FileResponse("frontend/index.html")


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
    await db.flush()  # write to DB inside the open transaction

    # Push to Redis AFTER the DB row is flushed (atomicity: if Redis fails,
    # the transaction rolls back and no orphan row is left).
    queue_len = await enqueue_job(str(job_id))

    await db.commit()

    logger.info("Job %s queued (Redis queue depth: %d)", job_id, queue_len)

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
# GET /api/jobs
# ---------------------------------------------------------------------------
@app.get(
    "/api/jobs",
    response_model=list[JobStatusResponse],
    summary="List recent jobs",
)
async def list_jobs(db: AsyncSession = Depends(get_db)):
    """
    Returns the 50 most recent jobs for the frontend dashboard.
    """
    result = await db.execute(
        select(Job).order_by(Job.created_at.desc()).limit(50)
    )
    jobs = result.scalars().all()
    
    return [
        JobStatusResponse(
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
        for job in jobs
    ]


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
