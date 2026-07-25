"""
redis_client.py — Async Redis connection and queue helpers.

Queue name : asyncz:jobs
Protocol   : LPUSH (enqueue) / BRPOP (dequeue in worker)
Client     : redis.asyncio (ships with redis-py >= 4.2, no extra install)
"""

import os
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

# Single queue list name — shared between API (producer) and worker (consumer)
QUEUE_NAME: str = "asyncz:jobs"


# ---------------------------------------------------------------------------
# Singleton client — created once at import time, reused across requests.
# decode_responses=False keeps values as raw bytes (ARQ compatible).
# ---------------------------------------------------------------------------
redis_client: aioredis.Redis = aioredis.from_url(
    REDIS_URL,
    encoding="utf-8",
    decode_responses=True,
    max_connections=50,  # plenty headroom for k6 burst traffic
)


# Producer helper — called by POST /jobs
async def enqueue_job(job_id: str) -> int:
    """
    LPUSH the job_id string onto the queue.
    Returns the new length of the list (useful for monitoring).
    """
    return await redis_client.lpush(QUEUE_NAME, job_id)


# Health check helper
async def ping_redis() -> bool:
    """Returns True if Redis responds to PING."""
    try:
        return await redis_client.ping()
    except Exception:
        return False
