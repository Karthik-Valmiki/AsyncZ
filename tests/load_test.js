/**
 * AsyncZ — k6 Load Test
 *
 * What this tests:
 *   - POST /jobs  → job submission throughput + latency
 *   - GET  /jobs/{id} → status polling latency
 *   - GET  /health → baseline connectivity
 *
 * Metrics tracked:
 *   - http_req_duration  (avg, P95, P99)
 *   - job_submit_success (custom rate — did the 202 come back?)
 *   - job_status_success (custom rate — did the poll return 200?)
 *
 * Thresholds (pass/fail gates):
 *   - 95% of POST /jobs requests complete under 500ms
 *   - 99% of GET  /jobs/{id} requests complete under 200ms
 *   - Job submission success rate ≥ 95%
 *
 * Run:
 *   k6 run tests/load_test.js
 *
 * Run with HTML report (if k6-reporter installed):
 *   k6 run --out json=tests/results.json tests/load_test.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

// ---------------------------------------------------------------------------
// Base URL — change if your API is on a different host/port
// ---------------------------------------------------------------------------
const BASE_URL = "http://localhost:8000";

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const jobSubmitSuccessRate = new Rate("job_submit_success");
const jobPollSuccessRate = new Rate("job_poll_success");
const submitLatency = new Trend("submit_latency_ms", true);
const pollLatency = new Trend("poll_latency_ms", true);

// ---------------------------------------------------------------------------
// Load profile — three stages
//
//   Stage 1 (ramp-up)   : 0 → 50 VU over 30s   — warm up the pool
//   Stage 2 (sustained) : 50 VU for 1 min       — steady-state load
//   Stage 3 (spike)     : 50 → 100 VU over 15s  — simulate burst traffic
//   Stage 4 (cool-down) : 100 → 0 VU over 15s   — graceful wind-down
// ---------------------------------------------------------------------------
export const options = {
  stages: [
    { duration: "30s", target: 50 },  // ramp up
    { duration: "60s", target: 50 },  // sustained
    { duration: "15s", target: 100 },  // spike
    { duration: "15s", target: 0 },  // cool down
  ],

  // ----- Thresholds — these cause k6 to exit with a non-zero code on failure
  thresholds: {
    // POST /jobs: 95% of requests must finish under 500 ms
    "http_req_duration{endpoint:submit}": ["p(95)<500"],

    // GET /jobs/{id}: 99% of polls must finish under 200 ms
    "http_req_duration{endpoint:poll}": ["p(99)<200"],

    // Success rates
    "job_submit_success": ["rate>0.95"],  // ≥ 95% of submits got 202
    "job_poll_success": ["rate>0.95"],  // ≥ 95% of polls got 200

    // Overall http error rate < 5%
    "http_req_failed": ["rate<0.05"],
  },
};

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

/** Randomly pick a job type to exercise different code paths */
function randomPayload() {
  const roll = Math.random();

  if (roll < 0.70) {
    // 70% — normal fast job
    return JSON.stringify({
      payload: { task: "process_data", user_id: Math.floor(Math.random() * 10000) },
    });
  } else if (roll < 0.90) {
    // 20% — slow job (2.5–5 s worker sleep)
    return JSON.stringify({
      payload: { task: "heavy_compute", slow: true },
    });
  } else {
    // 10% — intentional failure (tests error path + last_error column)
    return JSON.stringify({
      payload: { task: "broken_task", fail: true },
    });
  }
}

const JSON_HEADERS = { "Content-Type": "application/json" };

// ---------------------------------------------------------------------------
// Default function — runs once per VU per iteration
// ---------------------------------------------------------------------------
export default function () {

  // ── 1. Health check (every 10th iteration to avoid noise) ─────────────────
  if (__ITER % 10 === 0) {
    const healthRes = http.get(`${BASE_URL}/health`, {
      tags: { endpoint: "health" },
    });
    check(healthRes, {
      "health OK": (r) => r.status === 200,
    });
  }

  // ── 2. Submit a job ────────────────────────────────────────────────────────
  const submitStart = Date.now();
  const submitRes = http.post(
    `${BASE_URL}/jobs`,
    randomPayload(),
    {
      headers: JSON_HEADERS,
      tags: { endpoint: "submit" },
    }
  );
  submitLatency.add(Date.now() - submitStart);

  const submitOk = check(submitRes, {
    "submit: status 202": (r) => r.status === 202,
    "submit: has job_id": (r) => {
      try { return !!JSON.parse(r.body).job_id; }
      catch { return false; }
    },
    "submit: status is queued": (r) => {
      try { return JSON.parse(r.body).status === "queued"; }
      catch { return false; }
    },
  });

  jobSubmitSuccessRate.add(submitOk);

  if (!submitOk) {
    // Don't try to poll if submission failed
    sleep(0.5);
    return;
  }

  // ── 3. Poll job status once after a short delay ────────────────────────────
  // We don't block-until-complete (that would inflate VU duration artificially).
  // One poll gives us real round-trip latency for the GET endpoint.
  const jobId = JSON.parse(submitRes.body).job_id;

  sleep(0.3); // 300 ms — let the worker pick it up

  const pollStart = Date.now();
  const pollRes = http.get(`${BASE_URL}/jobs/${jobId}`, {
    tags: { endpoint: "poll" },
  });
  pollLatency.add(Date.now() - pollStart);

  const pollOk = check(pollRes, {
    "poll: status 200": (r) => r.status === 200,
    "poll: has status": (r) => {
      try { return ["queued", "processing", "completed", "failed"].includes(JSON.parse(r.body).status); }
      catch { return false; }
    },
  });

  jobPollSuccessRate.add(pollOk);

  // ── 4. Think time — mimics real user pacing, prevents tight loops ──────────
  sleep(Math.random() * 0.5 + 0.2); // 200–700 ms
}

// ---------------------------------------------------------------------------
// Setup — runs once before the test, verifies the service is up
// ---------------------------------------------------------------------------
export function setup() {
  const res = http.get(`${BASE_URL}/health`);
  if (res.status !== 200) {
    throw new Error(
      `AsyncZ API is not reachable at ${BASE_URL}. ` +
      `Start it with: uvicorn app.main:app --host 0.0.0.0 --port 8000`
    );
  }
  console.log("AsyncZ API is up. Starting load test…");
}
