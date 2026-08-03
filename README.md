# AsyncZ

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-ARQ%20Queue-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0%20async-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)
![k6](https://img.shields.io/badge/k6-Load%20Testing-7D64FF?style=for-the-badge&logo=k6&logoColor=white)

AsyncZ is a durable, horizontally scalable background job queue built on FastAPI, Redis, and PostgreSQL. It accepts job submissions over HTTP and processes them asynchronously — the client gets a 202 immediately and polls for results. It exists to solve the specific problem of tasks that are too slow to run in a request/response cycle (LLM calls, report generation, file processing) but must not be silently dropped if a process dies mid-execution.

---

## The Problem

When a web service needs to do work that takes more than a few hundred milliseconds — sending email, generating PDFs, calling an external API — the standard mistake is to do it synchronously in the HTTP handler. This blocks the thread, the user waits, and if the server restarts the work is lost. In-process queues (asyncio.Queue, a simple list) solve the latency problem but not the durability problem: the queue dies with the process.

The design requirement here was: a job submitted at time T must eventually complete even if every process crashes between T and T+1m. That requires the queue to live outside the application process (Redis), the job state to be persisted to disk before the worker touches it (PostgreSQL), and a mechanism to detect and recover workers that die mid-execution (heartbeats + zombie recovery cron).

---

## Architecture

```
Client
  │
  ▼
┌────────────────────────────────────┐
│  FastAPI (uvicorn, 4 workers)      │
│  POST /jobs                        │
│  1. INSERT job → PostgreSQL        │  ← durable write first
│  2. enqueue_job() → Redis (ARQ)    │  ← ARQ sorted-set queue
│  3. Return 202 + job_id            │
└───────────────┬────────────────────┘
                │
         Redis (ARQ queue)
                │
  ┌─────────────┼─────────────┐
  ▼             ▼             ▼
Worker-1     Worker-2   ... Worker-N    (scale with --scale worker=N)
  │
  ├─ status = processing + heartbeat every 10s
  ├─ execute payload (_execute_payload)
  ├─ SUCCESS → status = completed
  └─ FAILURE → retry with exponential backoff (1s, 2s, 4s … 30s max)
               → exhausted → status = dead, push to DLQ in Redis

  Cron (every 60s, runs on each worker):
  └─ Find jobs WHERE status=processing AND heartbeat_at < now()-60s
     └─ Re-enqueue (zombie recovery)
```

**Key design decisions:**

- **PostgreSQL as the source of truth, not Redis.** The job row is written to Postgres before it is pushed to Redis. If the API crashes between step 1 and 2, the job is in the DB with status `queued` and can be re-enqueued by ops. If Redis were the source of truth and the push failed, the job would be lost silently.

- **Redis (ARQ) over RabbitMQ/SQS for the queue.** The stack already requires Redis for ARQ's worker coordination. Adding a second broker (Rabbit, SQS) would introduce an unnecessary operational dependency at this scale. ARQ's sorted-set queue gives at-least-once delivery guarantees sufficient for the target use case.

- **Heartbeat + zombie cron instead of ARQ's built-in job timeout.** ARQ's `job_timeout` kills a job after N seconds regardless of progress. For long-running tasks (LLM calls that take 45s) this would cause spurious failures. The heartbeat model lets a job run as long as it keeps writing. A 60-second silence window is the actual timeout, and the worker that notices it re-enqueues rather than discarding.

- **ARQ's `enqueue_job` over raw `LPUSH/BRPOP`.** Both have sub-millisecond latency, so latency was not the deciding factor. ARQ's `enqueue_job` uses a Redis sorted set internally, which gives job scheduling, natural deduplication, and a shared re-enqueue interface that both the API and the worker use identically (`ctx["redis"].enqueue_job(...)` for retries). Building the equivalent on raw `LPUSH/BRPOP` would require reimplementing the sorted-set scheduling, a blocking pop loop, and retry re-enqueue separately — all code that ARQ already maintains. Staying within the ARQ ecosystem trades raw control for operational consistency.

- **Idempotency key enforced at the DB level, not application level.** A `UNIQUE` constraint on `idempotency_key` in PostgreSQL catches duplicate submissions even under concurrent load (two requests with the same key that both pass the pre-check simultaneously will have one fail the constraint; the handler catches `IntegrityError` and returns 409).

- **`JobExecutionLog` as a separate audit table.** Each attempt (original + retries) gets its own log row with `started_at`, `finished_at`, `error_message`, and `worker_id`. This means post-mortem debugging doesn't require reconstructing a timeline from application logs.

---

## Quick Start

No `.env` file needed — all credentials are baked into `docker-compose.yml` for local development. If you want to override them, copy `.env.example` and edit before running.

```bash
git clone <your-repo-url>
cd AsyncZ
docker compose up --build --scale worker=5
```

The API is now at `http://localhost:8000`. Submit a job:

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"payload": {"task": "email_send", "user_id": 42}}' | python -m json.tool
```

Expected response:
```json
{
    "job_id": "a1b2c3d4-...",
    "status": "queued"
}
```

Poll for completion:
```bash
curl -s http://localhost:8000/jobs/<job_id> | python -m json.tool
```

OpenAPI docs: `http://localhost:8000/docs`

---

## Proof It Works

### Load test

Run against the live stack (k6 runs inside Docker on the same network as the API):

```bash
docker compose run --rm k6 run /tests/load_test.js
```

Script: `tests/load_test.js` — `constant-arrival-rate` executor, 1,500 req/s target, 30s duration, up to 5,000 VUs.

**Results on a developer machine (Windows, Docker Desktop, WSL2):**

```
checks_succeeded:  100.00%   7364 out of 7364
http_req_failed:     0.00%      0 out of 7364
http_reqs:           7364      176 req/s sustained
http_req_duration:    p95=21.36s   (target threshold: p95<500ms — FAILED)
dropped_iterations:  37607      903/s
```

**Post-test database metrics** (run after workers drain the queue):

```sql
-- Connect: docker exec -it asyncz-db-1 psql -U postgres -d asyncz

SELECT status, COUNT(*) FROM jobs GROUP BY status;
-- completed | 7364

SELECT ROUND(100.0 * SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)/COUNT(*), 2)
FROM jobs;
-- 100.00%

SELECT
  ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - started_at))), 3) AS avg_sec,
  PERCENTILE_CONT(0.95) WITHIN GROUP
    (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))) AS p95_sec
FROM jobs WHERE status='completed' AND completed_at IS NOT NULL;
-- avg: 1.570s   p95: 2.487s   (worker processing time, once a job is picked up)

SELECT worker_id, COUNT(*) AS jobs_completed
FROM jobs WHERE status='completed' GROUP BY worker_id;
-- Near-uniform distribution across 5 workers: 1458–1487 jobs each
```

> **Honest read of these numbers:** the system achieved 100% completion with zero retries and near-uniform work distribution across workers — the queue, worker pool, and DB-write path are correct under this load. But throughput is capped at ~176 req/s against a 1,500 req/s target, and `http_req_duration` p95 is 21.36s — meaning `POST /jobs` is not returning "immediately" as designed once load rises. This is a real bottleneck, not a WSL2/network artifact: the latency spread (min 395ms, max 23.5s) is the signature of requests queueing for a limited resource, not a flat network tax. Root cause not yet isolated — leading suspects are DB connection pool size and the single (unscaled) API container. `docker stats` and `pg_stat_activity` checks are the next step before this number should be treated as a ceiling rather than a symptom.
>
> Also note: `_execute_payload` currently simulates work with `asyncio.sleep` rather than real task execution, so this test measures queue/DB/retry throughput, not real job-processing throughput under production workloads.

---

## Tech Stack

| Component | Choice | Why |
|---|---|---|
| **API** | ![FastAPI](https://img.shields.io/badge/-FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white) uvicorn (4 workers) | Async-native; 4 uvicorn processes match Docker Desktop's ~4 allocated vCPUs — confirmed 4.4× throughput gain vs. single process in load tests |
| **Queue** | ![Redis](https://img.shields.io/badge/-Redis-DC382D?style=flat-square&logo=redis&logoColor=white) via ARQ | At-least-once delivery using Redis sorted sets. Redis was already required for ARQ worker coordination; adding a second broker (Rabbit, SQS) was unjustified complexity at this scale |
| **Worker** | ARQ worker process | Handles worker lifecycle, Redis pub/sub, and concurrent job slots (`max_jobs=10`). Scales horizontally with `--scale worker=N` |
| **Persistence** | ![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white) 15 + asyncpg | Job state must survive process restarts. PostgreSQL's `UNIQUE` constraint enforces idempotency atomically under concurrent load — an application-layer check cannot guarantee this |
| **ORM** | ![SQLAlchemy](https://img.shields.io/badge/-SQLAlchemy-D71F00?style=flat-square&logo=sqlalchemy&logoColor=white) 2.0 async | `async_sessionmaker` + `asyncpg` driver allows non-blocking DB I/O on the same event loop as the HTTP server — no thread-pool overhead per request |
| **Schema** | SQLAlchemy `create_all` | Acceptable for a self-contained project. Alembic would be added before any schema change in a team environment where multiple engineers touch the DB |
| **Runtime** | ![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat-square&logo=docker&logoColor=white) Compose | Entire stack (API, workers, Redis, Postgres) runs identically on any machine with Docker installed — no local dependency setup required |

---

## Known Limitations

- **No connection pooler (PgBouncer).** PostgreSQL holds a server process per active connection. At high scale, adding PgBouncer in transaction-pooling mode would cut Postgres memory consumption significantly. Currently mitigated by setting `DB_POOL_SIZE` per container type via environment variables in `docker-compose.yml`.

- **Zombie recovery does a table scan.** `recover_zombie_jobs` filters on `(status, heartbeat_at)` without a composite index. This becomes the slowest query in the system at millions of rows. Fix: `CREATE INDEX ON jobs (status, heartbeat_at)`.

- **DLQ is not durable.** Dead Letter Queue entries live in a Redis list capped at 1,000 entries. A Redis flush drops this history entirely. For production, DLQ entries should also be written to a `dead_jobs` table in PostgreSQL.

- **`_execute_payload` is a stub.** The worker simulates work with `asyncio.sleep`. Integrating real business logic (HTTP calls to external APIs, file I/O) will expose failure modes — network timeouts, partial writes — that the current generic retry loop may need to handle per-task-type.

- **Single API container by default.** The `api` service is not scaled in `docker-compose.yml`. HTTP throughput is bounded by one API container. Load testing shows this caps sustained throughput well below the queue/worker layer's actual capacity. To scale the API horizontally, add `--scale api=N` with an nginx load balancer in front, or deploy to an orchestrator (Kubernetes, ECS) that handles routing.

---

## API Reference

Full interactive docs at `http://localhost:8000/docs` (Swagger UI).

### `POST /jobs`
Submit a new job. Returns immediately with 202.

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"task": "pdf_generate", "user_id": 99},
    "max_retries": 3,
    "idempotency_key": "550e8400-e29b-41d4-a716-446655440000"
  }'
```

Response `202`:
```json
{"job_id": "...", "status": "queued"}
```

Response `409` (duplicate idempotency key):
```json
{"detail": {"error": "duplicate_idempotency_key", "job_id": "...", "status": "completed"}}
```

### `GET /jobs/{job_id}`
Poll job status. Values: `queued | processing | completed | dead`.

### `GET /health`
Returns DB + Redis connectivity and current DLQ depth. A growing `dlq_length` means job execution is broken.

### `GET /dlq`
Returns up to 1,000 permanently failed jobs with payload, retry count, and last error message.
