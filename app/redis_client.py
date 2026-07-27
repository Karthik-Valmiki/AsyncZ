"""
redis_client.py — ARQ-based Redis connection and queue helpers.

Uses arq.connections.ArqRedis so the API and worker share the same queue.
The API enqueues via arq_redis.enqueue_job(); the worker dequeues via ARQ internals.
"""

import os
from dotenv import load_dotenv
from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

load_dotenv()

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

_redis_settings = RedisSettings.from_dsn(REDIS_URL)

# Module-level ArqRedis instance — created once during app lifespan.
# ArqRedis is a connection pool internally, safe for concurrent async use.
arq_redis: ArqRedis | None = None


async def init_arq_redis() -> None:
    """Call once at app startup to create the shared ArqRedis pool."""
    global arq_redis
    arq_redis = await create_pool(_redis_settings)


async def close_arq_redis() -> None:
    """Call once at app shutdown."""
    if arq_redis:
        await arq_redis.aclose()


async def enqueue_job(job_id: str) -> None:
    """
    Enqueue a job into ARQ's queue.
    ARQ serialises the call as process_job(job_id) and pushes it onto
    its internal sorted-set queue — the same one the worker reads from.
    """
    await arq_redis.enqueue_job("process_job", job_id)


async def ping_redis() -> bool:
    """Returns True if Redis responds to PING."""
    try:
        return await arq_redis.ping()
    except Exception:
        return False
