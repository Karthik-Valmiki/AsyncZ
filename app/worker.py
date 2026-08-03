"""
worker.py — ARQ background worker.

Flow per job:
    1. ARQ calls process_job(ctx, job_id)
    2. Fetch job from PostgreSQL, guard against unexpected state
    3. Mark status = "processing", record started_at + worker_id
    4. Run a background heartbeat task (writes heartbeat_at every 10s)
    5. Execute the job payload
    6. On success  → status = "completed"
    7. On failure  → retry_count += 1
                     retries remain  → status = "queued", re-enqueue with backoff
                     retries exhausted → status = "dead", push to DLQ

Run the worker:
    python -m arq app.worker.WorkerSettings

Scale horizontally:
    docker-compose up --scale worker=N
"""

import asyncio
import json
import logging
import os
import random
import socket
import traceback
import uuid
from datetime import datetime, timedelta

from arq import cron
from arq.connections import RedisSettings
from dotenv import load_dotenv
from sqlalchemy import select, update

from app.db import AsyncSessionLocal
from app.models import Job, JobExecutionLog

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("asyncz.worker")

WORKER_ID: str = f"{socket.gethostname()}-{os.getpid()}"

DLQ_KEY = "asyncz:dlq"
DLQ_MAX_SIZE = 1000
HEARTBEAT_INTERVAL = 10   # seconds between heartbeat writes
ZOMBIE_THRESHOLD = 60     # seconds of silence before a job is declared zombie


def _build_redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(os.getenv("REDIS_URL", "redis://127.0.0.1:6379"))


async def _execute_payload(payload: dict) -> None:
    """
    Replace this function with your actual business logic (LLM calls, RAG pipelines, etc.).

    Test hooks:
        {"fail": true}  → raises immediately, triggers retry/DLQ flow
        {"slow": true}  → simulates a slow job (2.5–5 s)
        default         → simulates normal job (0.5–2.5 s)
    """
    if payload.get("fail"):
        raise RuntimeError("Job intentionally failed (payload.fail=true)")

    duration = random.uniform(2.5, 5.0) if payload.get("slow") else random.uniform(0.5, 2.5)
    await asyncio.sleep(duration)


async def _heartbeat_loop(job_uuid: uuid.UUID) -> None:
    """Writes heartbeat_at every HEARTBEAT_INTERVAL seconds while a job is running."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            async with AsyncSessionLocal() as s:
                await s.execute(
                    update(Job).where(Job.id == job_uuid).values(heartbeat_at=datetime.utcnow())
                )
                await s.commit()
        except Exception as exc:
            logger.warning("Heartbeat write failed for job %s: %s", job_uuid, exc)


async def process_job(ctx: dict, job_id: str) -> dict:
    """ARQ task entry point. Called once per dequeued job."""
    job_uuid = uuid.UUID(job_id)
    now = datetime.utcnow

    logger.info("[%s] Picked up job %s", WORKER_ID, job_id)

    async with AsyncSessionLocal() as session:
        job: Job | None = await session.get(Job, job_uuid)

        if job is None:
            logger.error("Job %s not found in DB — skipping.", job_id)
            return {"status": "skipped", "reason": "not_found"}

        if job.status != "queued":
            logger.warning("Job %s has status '%s' — skipping.", job_id, job.status)
            return {"status": "skipped", "reason": f"unexpected_status:{job.status}"}

        started = now()
        job.status = "processing"
        job.started_at = started
        job.updated_at = started
        job.worker_id = WORKER_ID
        job.heartbeat_at = started

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

        payload_snapshot = dict(job.payload)
        max_retries_snapshot = job.max_retries
        retry_count_snapshot = job.retry_count

    logger.info("[%s] Job %s → PROCESSING (attempt %d)", WORKER_ID, job_id, attempt_number)

    heartbeat_task = asyncio.create_task(_heartbeat_loop(job_uuid))

    error_message: str | None = None
    succeeded = False

    try:
        await _execute_payload(payload_snapshot)
        succeeded = True
        logger.info("[%s] Job %s → COMPLETED", WORKER_ID, job_id)
    except Exception:
        error_message = traceback.format_exc()
        logger.error("[%s] Job %s → FAILED (attempt %d):\n%s",
                     WORKER_ID, job_id, attempt_number, error_message)
    finally:
        heartbeat_task.cancel()

    finished = now()

    async with AsyncSessionLocal() as session:
        if succeeded:
            final_status = "completed"
            await session.execute(
                update(Job).where(Job.id == job_uuid).values(
                    status="completed",
                    completed_at=finished,
                    updated_at=finished,
                    last_error=None,
                )
            )
        else:
            new_retry_count = retry_count_snapshot + 1

            if new_retry_count <= max_retries_snapshot:
                final_status = "retrying"
                await session.execute(
                    update(Job).where(Job.id == job_uuid).values(
                        status="queued",
                        retry_count=new_retry_count,
                        updated_at=finished,
                        last_error=error_message,
                        heartbeat_at=None,
                    )
                )
                await session.commit()

                backoff = min(2 ** retry_count_snapshot, 30)  # exponential: 1s, 2s, 4s … max 30s
                await asyncio.sleep(backoff)
                await ctx["redis"].enqueue_job("process_job", job_id)
                logger.info("[%s] Job %s → RETRY %d/%d (backoff %ds)",
                            WORKER_ID, job_id, new_retry_count, max_retries_snapshot, backoff)

                await session.execute(
                    update(JobExecutionLog)
                    .where(
                        JobExecutionLog.job_id == job_uuid,
                        JobExecutionLog.attempt_number == attempt_number,
                    )
                    .values(status="failed", error_message=error_message, finished_at=finished)
                )
                await session.commit()
                return {"status": "retrying", "attempt": new_retry_count, "job_id": job_id}

            else:
                final_status = "dead"
                await session.execute(
                    update(Job).where(Job.id == job_uuid).values(
                        status="dead",
                        retry_count=new_retry_count,
                        updated_at=finished,
                        last_error=error_message,
                        heartbeat_at=None,
                    )
                )
                dlq_entry = json.dumps({
                    "job_id": job_id,
                    "payload": payload_snapshot,
                    "retry_count": new_retry_count,
                    "last_error": error_message,
                    "failed_at": finished.isoformat(),
                })
                await ctx["redis"].lpush(DLQ_KEY, dlq_entry)
                await ctx["redis"].ltrim(DLQ_KEY, 0, DLQ_MAX_SIZE - 1)
                logger.error("[%s] Job %s → DEAD (pushed to DLQ)", WORKER_ID, job_id)

        await session.execute(
            update(JobExecutionLog)
            .where(
                JobExecutionLog.job_id == job_uuid,
                JobExecutionLog.attempt_number == attempt_number,
            )
            .values(status=final_status, error_message=error_message, finished_at=finished)
        )
        await session.commit()

    return {"status": final_status, "job_id": job_id}


async def recover_zombie_jobs(ctx: dict) -> None:
    """
    Cron job: runs every 60 seconds.
    Finds jobs stuck in 'processing' with a stale heartbeat and re-enqueues them.
    A stale heartbeat means the worker that owned the job died unexpectedly.
    """
    stale_cutoff = datetime.utcnow() - timedelta(seconds=ZOMBIE_THRESHOLD)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Job).where(
                Job.status == "processing",
                Job.heartbeat_at < stale_cutoff,
            )
        )
        zombies: list[Job] = result.scalars().all()

    if not zombies:
        return

    logger.warning("[%s] Zombie recovery: found %d stuck jobs", WORKER_ID, len(zombies))

    for job in zombies:
        async with AsyncSessionLocal() as session:
            new_retry = job.retry_count + 1
            if new_retry <= job.max_retries:
                await session.execute(
                    update(Job).where(Job.id == job.id).values(
                        status="queued",
                        retry_count=new_retry,
                        updated_at=datetime.utcnow(),
                        heartbeat_at=None,
                    )
                )
                await session.commit()
                await ctx["redis"].enqueue_job("process_job", str(job.id))
                logger.info("Zombie job %s re-enqueued (retry %d)", job.id, new_retry)
            else:
                await session.execute(
                    update(Job).where(Job.id == job.id).values(
                        status="dead",
                        retry_count=new_retry,
                        updated_at=datetime.utcnow(),
                        heartbeat_at=None,
                    )
                )
                dlq_entry = json.dumps({
                    "job_id": str(job.id),
                    "payload": job.payload,
                    "retry_count": new_retry,
                    "last_error": "zombie: heartbeat timed out",
                    "failed_at": datetime.utcnow().isoformat(),
                })
                await ctx["redis"].lpush(DLQ_KEY, dlq_entry)
                await ctx["redis"].ltrim(DLQ_KEY, 0, DLQ_MAX_SIZE - 1)
                await session.commit()
                logger.error("Zombie job %s → DEAD (retries exhausted)", job.id)


class WorkerSettings:
    functions = [process_job]
    cron_jobs = [cron(recover_zombie_jobs, second=0)]
    redis_settings = _build_redis_settings()
    max_jobs: int = 10
    job_timeout: int = 60

    @staticmethod
    async def on_startup(ctx: dict) -> None:
        logger.info("Worker %s started.", WORKER_ID)

    @staticmethod
    async def on_shutdown(ctx: dict) -> None:
        logger.info("Worker %s shutting down.", WORKER_ID)
