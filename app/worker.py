"""
Flow per job:
    1. ARQ calls process_job(ctx, job_id)
    2. Fetch the Job row from PostgreSQL
    3. Mark status = "processing", record started_at + worker_id
    4. Insert a JobExecutionLog row (attempt started)
    5. Execute the job (simulated work via asyncio.sleep)
    6. On success: status = "completed", completed_at = now()
    7. On failure: status = "failed",   last_error = traceback
    8. Update the JobExecutionLog row with final status + finished_at

Run the worker:
    python -m arq app.worker.WorkerSettings

Environment variables respected:
    REDIS_URL       — where ARQ connects for job dequeue
    DATABASE_URL    — asyncpg DSN for PostgreSQL
"""

import asyncio
import logging
import os
import random
import socket
import traceback
import uuid
from datetime import datetime, timezone

from arq.connections import RedisSettings
from dotenv import load_dotenv
from sqlalchemy import select, update

from app.db import AsyncSessionLocal
from app.models import Job, JobExecutionLog

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("asyncz.worker")

# Unique identifier for this worker process (useful for multi-worker Phase 2)
WORKER_ID: str = f"{socket.gethostname()}-{os.getpid()}"


# ---------------------------------------------------------------------------
# Redis settings — parsed from REDIS_URL env var
# ---------------------------------------------------------------------------
def _build_redis_settings() -> RedisSettings:
    url = os.getenv("REDIS_URL", "redis://localhost:6379")
    # RedisSettings can parse a plain redis:// URL
    return RedisSettings.from_dsn(url)


# ---------------------------------------------------------------------------
# Simulated job executor — replace with real logic in production
# ---------------------------------------------------------------------------
async def _execute_payload(payload: dict) -> None:
    """
    Simulates variable-length work.
    - Normal jobs: 0.5–2.5 s
    - Slow jobs (payload has "slow": true): 2.5–5 s
    - Error jobs (payload has "fail": true): raises immediately (for testing)
    """
    if payload.get("fail"):
        raise RuntimeError("Job intentionally failed (payload.fail=true)")

    duration = (
        random.uniform(2.5, 5.0) if payload.get("slow") else random.uniform(0.5, 2.5)
    )
    await asyncio.sleep(duration)


# ---------------------------------------------------------------------------
# Core task function — called by ARQ for every dequeued job
# ---------------------------------------------------------------------------
async def process_job(ctx: dict, job_id: str) -> dict:
    """
    ARQ task entry point.

    ctx is provided by ARQ and contains the Redis connection + any startup data.
    """
    job_uuid = uuid.UUID(job_id)
    now = lambda: datetime.now(timezone.utc).replace(tzinfo=None)

    logger.info("[%s] Picked up job %s", WORKER_ID, job_id)

    async with AsyncSessionLocal() as session:
        # ------------------------------------------------------------------
        # 1. Fetch the job
        # ------------------------------------------------------------------
        job: Job | None = await session.get(Job, job_uuid)

        if job is None:
            logger.error("Job %s not found in DB — skipping.", job_id)
            return {"status": "skipped", "reason": "not_found"}

        if job.status not in ("queued",):
            # Guard against duplicate delivery
            logger.warning(
                "Job %s has unexpected status '%s' — skipping.", job_id, job.status
            )
            return {"status": "skipped", "reason": f"unexpected_status:{job.status}"}

        # ------------------------------------------------------------------
        # 2. Mark PROCESSING
        # ------------------------------------------------------------------
        started = now()
        job.status = "processing"
        job.started_at = started
        job.updated_at = started
        job.worker_id = WORKER_ID

        # ------------------------------------------------------------------
        # 3. Insert execution log — attempt started
        # ------------------------------------------------------------------
        attempt_number = job.retry_count + 1
        log_entry = JobExecutionLog(
            job_id=job_uuid,
            attempt_number=attempt_number,
            worker_id=WORKER_ID,
            status="started",
            started_at=started,
        )
        session.add(log_entry)
        await session.commit()

        logger.info(
            "[%s] Job %s → PROCESSING (attempt %d)", WORKER_ID, job_id, attempt_number
        )

    # ------------------------------------------------------------------------
    # 4. Execute — outside the DB session to avoid holding a connection open
    # ------------------------------------------------------------------------
    error_message: str | None = None
    final_status: str

    try:
        await _execute_payload(job.payload)
        final_status = "completed"
        logger.info("[%s] Job %s → COMPLETED", WORKER_ID, job_id)
    except Exception:
        error_message = traceback.format_exc()
        final_status = "failed"
        logger.error("[%s] Job %s → FAILED:\n%s", WORKER_ID, job_id, error_message)

    # ------------------------------------------------------------------------
    # 5. Persist final state
    # ------------------------------------------------------------------------
    finished = now()

    async with AsyncSessionLocal() as session:
        # Update job row
        await session.execute(
            update(Job)
            .where(Job.id == job_uuid)
            .values(
                status=final_status,
                updated_at=finished,
                completed_at=finished if final_status == "completed" else None,
                last_error=error_message,
            )
        )

        # Update log row
        await session.execute(
            update(JobExecutionLog)
            .where(
                JobExecutionLog.job_id == job_uuid,
                JobExecutionLog.attempt_number == attempt_number,
            )
            .values(
                status=final_status,
                error_message=error_message,
                finished_at=finished,
            )
        )

        await session.commit()

    return {"status": final_status, "job_id": job_id}


# ---------------------------------------------------------------------------
# ARQ WorkerSettings — entry point for: python -m arq app.worker.WorkerSettings
# ---------------------------------------------------------------------------
class WorkerSettings:
    """
    ARQ reads this class to configure the worker.

    functions   — list of async callables ARQ can dispatch
    redis_settings — where to connect for job dequeue
    max_jobs    — concurrent job coroutines per worker process
    job_timeout — hard cap per job execution (seconds)
    """

    functions = [process_job]
    redis_settings = _build_redis_settings()

    # Allow up to 10 concurrent jobs per worker process.
    # Increase when deploying multiple workers in Phase 2.
    max_jobs: int = 10

    # Kill a single job if it runs longer than 60 s (safety net)
    job_timeout: int = 60

    @staticmethod
    async def on_startup(ctx: dict) -> None:
        logger.info("Worker %s started. Ready to process jobs.", WORKER_ID)

    @staticmethod
    async def on_shutdown(ctx: dict) -> None:
        logger.info("Worker %s shutting down.", WORKER_ID)
