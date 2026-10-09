Automated test scripts have been written to cover and verify all 5 problems specified in [`docs/spec/problem-set.md`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/docs/spec/problem-set.md).

---

### **Overview of Test Scripts Created**

| File | Problem Set | Key Verification Focus |
|---|---|---|
| [`tests/problem_set_support.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/problem_set_support.py) | *Shared Helper* | Mock admin API triggers, Zitadel token management, RabbitMQ management API, and latency statistics. |
| [`tests/test_problem_1.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/test_problem_1.py) | **Problem 1**: Arsitektur & Interoperabilitas Data | Canonical `HazardEvent` mapping, dynamic schema mutation via `POST /admin/schema-version`, `confidence_level` in dynamic `attributes`, v1 & v2 record coexistence without service restart. |
| [`tests/test_problem_2.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/test_problem_2.py) | **Problem 2**: Concurrency, Availability, & Graceful Degradation | BMKG-only $p95 < 300\text{ ms}$ under slow PVMBG contention, 50-VU sustained load test (error rate $< 1\%$), PVMBG outage degradation (`POST /admin/outage`) with stale data serving (no 500s/timeouts) and zero-restart recovery. |
| [`tests/test_problem_3.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/test_problem_3.py) | **Problem 3**: Service-to-Service Auth & Trust | Cross-service credential rejection (401/403), Media scope restriction (explicit `view=full` rejected with 403), Field Team token 60s natural TTL expiry and refresh flow, and codebase secret audit. |
| [`tests/test_problem_4.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/test_problem_4.py) | **Problem 4**: Microservices & Flexible Storage | Container independence & isolated restart with zero downtime on independent endpoints, Canonical Store dynamic JSONB attributes verification, and database access boundary isolation. |
| [`tests/test_problem_5.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/test_problem_5.py) | **Problem 5**: Fan-out Event Distribution / Pub-Sub | RabbitMQ fanout publication via transactional outbox, multiple independent consumers (`event-live-feed`, `webhook-provider`), consumer offline queue buffering & catchup, and dynamic zero-touch consumer registration. |
| [`tests/run_problem_set.py`](file:///c:/Users/rizac/sigma/aat/Tugas-Besar-AAT-M1/tests/run_problem_set.py) | *Master Runner* | CLI runner executing all problem test suites with summary scorecards and Markdown verification report export. |

---

### **How to Run the Automated Tests**

#### 1. Start the System Stack
```powershell
python tools/setup_auth_env.py
python tools/setup_mock_env.py
docker compose --env-file .env --env-file .env.auth up --build -d --wait
```

#### 2. Run All 5 Problem Tests
```powershell
$env:BNPB_E2E = "1"
python tests/run_problem_set.py --report
```

#### 3. Run a Specific Problem Set
```powershell
python tests/run_problem_set.py --problem 1
python tests/run_problem_set.py --problem 2
python tests/run_problem_set.py --problem 3
python tests/run_problem_set.py --problem 4
python tests/run_problem_set.py --problem 5
```

---

### **Manual Actions & Evidence to Capture for Your Report**

While the test scripts verify all functional and architectural criteria programmatically, the milestone specification requires specific visual proof, log excerpts, and explanations in the PDF report:

| Problem | What Cannot Be Fully Automated (Needs Human/UI Demo) | What to Capture for Report Proof |
|---|---|---|
| **Problem 1** | Visual representation of raw JSON vs. mapped Canonical view in the frontend dashboard. | 1. **Terminal/Log Excerpt**: Aggregator log showing correlation ID when ingesting v1 vs v2 with `confidence_level`.<br>2. **API Response Diffs**: Output of `GET /api/v1/hazards?view=full` showing v1 record (`attributes` without `confidence_level`) and v2 record (`attributes` with `confidence_level`) side-by-side. |
| **Problem 2** | Visual Grafana dashboard showing latency spikes during PVMBG delay, throughput graphs during k6 sustained load, and alert status during outage. | 1. **Grafana/Tempo Screenshot**: `http://localhost:3000` showing the "BNPB Health & Traces" dashboard with request duration per service and source status changing to `STALE`.<br>2. **k6 / Script Output**: Table of throughput, $p50/p95/p99$, and 0% uncontrolled error rate.<br>3. **HTTP Log Snippet**: Showing HTTP 200 with stale header/timestamp during `POST /admin/outage`. |
| **Problem 3** | Interactive login via Hosted Zitadel UI in the browser, Cookie DevTools inspection. | 1. **DevTools Application Tab**: Screenshot showing `bnpb_access` and `bnpb_session` cookies with `HttpOnly` and `SameSite=Lax` flags.<br>2. **Network Tab**: Showing `POST /auth/refresh` succeeding after 60s without re-entering credentials.<br>3. **403 Forbidden Screenshot/Log**: Showing `svc-media` receiving 403 when requesting `?view=full`. |
| **Problem 4** | Demonstration of Docker container boundaries in terminal (`docker compose ps` / `docker compose restart`). | 1. **Terminal Capture**: Command `docker compose restart pvmbg-mock` (or `webhook-provider`) with timestamp, alongside a concurrent loop of `curl http://localhost:8080/api/v1/hazards?source=BMKG` maintaining 200 OK.<br>2. **PostgreSQL Schema DDL**: SQL query of `\d hazards` in `aggregator-db` showing the `attributes JSONB` column. |
| **Problem 5** | Live multi-consumer reaction in browser (Live Feed SSE counter incrementing, FCM push notification received, or Webhook history log). | 1. **RabbitMQ Management Dashboard**: Screenshot of `http://localhost:15672` showing exchange `bnpb.hazards.v1` and queues `bnpb.event-live-feed.v1`, `bnpb.webhook-provider.v1`, `bnpb.danger-notification.v1`.<br>2. **Terminal Excerpt**: Demonstrating a newly created queue binding receiving messages without restarting the Aggregator. |

Edited test_problem_2.py
Edited test_problem_3.py
Viewed test_problem_5.py:191-215