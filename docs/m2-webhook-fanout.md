# Bounded webhook fanout

The consumer previously hydrated every active subscription and serialized FULL
JSON again for each recipient. It now walks a fixed database generation in
batches of at most 64 rows and 256 KiB of URL/secret bytes. Metadata preflight
does not select descriptions or owners. A single larger boundary subscription
remains intact; the limit bounds batching rather than silently dropping input.
Readers close before dispatch/log writes, including SQLite's single connection.

PostgreSQL migration 00002 adds an identity sequence and an active-generation
index. SQLite assigns persistent generations with a transactional counter and
insert trigger, including a legacy-row backfill. Deleting the highest row does
not reuse its generation. Registration during a walk cannot extend its fixed
head. This is a finite insertion boundary, not snapshot isolation for concurrent
commits or deletes. Subscriptions have no update API. Deployment applies the
PostgreSQL DDL under the existing migration lock; allow startup time for the
table/index migration on an existing large subscription catalog.

Four workers share one immutable, lazily generated JSON byte slice per view and
event. FULL serialization uses one envelope marshal, preserving every attribute;
SUMMARY retains its allowlist. Each HTTP attempt has its own reader over the
shared bytes and signs those exact bytes. The queue has four slots, prefetch
remains one, and workers join before ACK/NACK or the next event. Delivery claims,
retry policy, replay suppression and shutdown requeue/cleanup are preserved.
Only known response bodies up to 64 KiB are drained for connection reuse;
unknown or larger replies close after status instead of pinning a payload while
reading an unbounded receiver stream. Positive configured HTTP timeouts still
bound slow headers/known-small reply reads. Production defaults remain ten
seconds/five attempts. The executable defaults Go's soft memory limit to
160 MiB for its 256 MiB pod, honoring an explicit GOMEMLIMIT override.

## Verification on 19 November 2026

The complete webhook Go suite passes, including a real PostgreSQL 17.10 upgrade
from migration 00001 with an existing subscription. New tests cover row/UTF-8
byte limits, oversized boundary preservation, management-field exclusion,
SQLite legacy upgrade/reopen, deleted-tail advancement, generation non-reuse,
late registration, parallel payload ownership, slow-receiver progress, HMAC,
second-page failure/replay, four-worker cancellation and claim/log cleanup.
Dispatcher tests cover known-small/huge/unknown response bodies and an actual
HTTP response that flushes status and never ends its stream.

Native `TestWebhookFanoutMemory` runs the production Consumer.Start and real
RabbitMQ, Redis claims and PostgreSQL store. Its HTTP receivers parse JSON as a
stream in the same capped process, independently compare every detection key,
check SUMMARY privacy and verify HMAC over the complete received bytes. The
fixture publishes one 5,000,142-byte protobuf twice, with 200,000 detection keys,
80 subscriptions (40 FULL/40 SUMMARY), one held receiver and one initial 503.
It verifies 81 HTTP attempts, four maximum workers, exactly 80 successful logs,
no repeat after broker replay and empty normal/dead-letter queues after drain.
All eight million FULL detection-key observations are compared exactly.

The native test passes in 4.11 seconds. Charged peak memory is 130,461,696 bytes
(124.4 MiB) under an enforced 268,435,456-byte cap; memory.events max/OOM/kill
and restart counts are zero. The test uses the same 160 MiB soft Go limit as
main. Separate caps: PostgreSQL 512 MiB, RabbitMQ 768 MiB, Redis 256 MiB.
Their observed peaks and counters are saved in
[the report](evidence/m2-webhook-fanout/report.json), alongside raw logs and
the complete Go-suite JSON events. Task runtime containers/data are removed
after capture; compiler caches remain ignored.

## Reproduction

Use Go 1.26, serial compilation and the existing workspace module caches.
Run `go test -count=1 -p 1 -parallel 1 ./...` from
`services/webhook-provider`. Set WEBHOOK_TEST_POSTGRES_DSN to a disposable
`webhook_m2_fixture` database to include the real migration/paging test; it
owns a unique schema and removes that schema afterward. Without the variable,
that provider integration test is explicitly skipped.

For the native fixture, cross-compile its test binary with GOOS=linux,
GOARCH=amd64, CGO_ENABLED=0:

```text
go test -c -o ../../.cache/m2-webhook-fanout/consumer.test ./internal/consumer
```

Start isolated networked containers named `m2-webhook-postgres` (image
postgres:17.10-alpine, database webhook_m2_fixture), `m2-webhook-rabbit`
(rabbitmq:4-management) and `m2-webhook-redis` (redis:8-alpine). Use only fresh
disposable data, fixture credentials and the above caps; give each two CPUs.
RabbitMQ uses `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2:2`. Redis uses a
160 MiB maxmemory/noeviction policy and no snapshot/append-only persistence.
Mount the binary directory read-only into alpine:3.21 at /fixture on the same
network and enforce `--memory 256m --memory-swap 256m --cpus 2` with:

```text
GOMAXPROCS=2
GOMEMLIMIT=160MiB
WEBHOOK_TEST_POSTGRES_DSN=postgres://fixture:fixture-only@m2-webhook-postgres/webhook_m2_fixture?sslmode=disable
WEBHOOK_TEST_AMQP_URL=amqp://fixture:fixture-only@m2-webhook-rabbit:5672/
WEBHOOK_TEST_REDIS_URL=redis://m2-webhook-redis:6379/0
/fixture/consumer.test -test.v -test.run ^TestWebhookFanoutMemory$ -test.timeout 110s
```

The memory test requires an empty active subscription catalog. It leaves its
data for inspection, so remove its disposable volumes/containers before rerun.
Keep a supervisor alive after the test to capture cgroup memory.peak/events
before container removal. Environment variables gate the optional fixture;
ordinary unit runs do not silently substitute an in-memory store for it.

This evidence closes the webhook fanout implementation subtask. It does not
cover the hosted webhook gRPC/OIDC/telemetry process, simultaneous notification
AMQP/ASP.NET/FCM/API load, proxy/SSE behavior, official reference sessions,
separate-kernel k3s or RF=3. Those remain in the infrastructure task ledger.
