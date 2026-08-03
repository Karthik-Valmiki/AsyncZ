import http from 'k6/http';
import { check, sleep } from 'k6';/// Run command (inside Docker):

//   docker-compose run --rm k6 run /tests/load_test.js

export const options = {
    scenarios: {
        submit_jobs: {
            executor: "constant-arrival-rate",
            rate: 1500,
            timeUnit: "1s",
            duration: "30s",
            preAllocatedVUs: 500,
            maxVUs: 5000,
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'], // less than 1% failure
        http_req_duration: ['p(95)<500'], // 95% of requests must complete within 500ms
    },
};

const JOB_TYPES = ['email_send', 'pdf_generate', 'image_resize', 'data_export', 'report_build'];

export default function () {
    const url = 'http://api:8000/jobs';

    const payload = JSON.stringify({
        payload: {
            task: JOB_TYPES[Math.floor(Math.random() * JOB_TYPES.length)],
            user_id: Math.floor(Math.random() * 100000),
            priority: Math.random() > 0.8 ? 'high' : 'normal',
        }
    });

    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'is status 202': (r) => r.status === 202,
    });
}
