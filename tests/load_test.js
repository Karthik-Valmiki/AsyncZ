import http from 'k6/http';
import { check, sleep } from 'k6';

// 1. Configuration — 5 jobs/sec per user (sleep 0.2s), 10 VUs = 50 jobs/sec total
// On Windows, keep VUs ≤ 10 to avoid the 512 socket limit crash.
// On Linux/WSL2 you can safely push to 100+ VUs.
//docker-compose run --rm k6 run /tests/load_test.js

export const options = {
    vus: 5000,          // 10 concurrent users
    duration: '20s',  // Run long enough to see queue backlog
};

// 2. The Loop
export default function () {
    // const url = 'http://localhost:8000/jobs'; # for windows
    // const url = 'http://host.docker.internal:8000/jobs';   // for docker
    const url = 'http://api:8000/jobs';


    // The exact format your schemas.py expects
    const payload = JSON.stringify({
        payload: {
            task: "mass_stress_test",
            user_id: Math.floor(Math.random() * 1000000)
        }
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    // 3. Send the request
    const res = http.post(url, payload, params);

    // 4. Validate the response
    check(res, {
        'is status 202': (r) => r.status === 202,
    });

    // sleep(0.2) = 5 jobs per second per user max
    // 10 VUs × 5 jobs/sec = 50 total jobs/sec hitting the API
    sleep(0.2);
}
