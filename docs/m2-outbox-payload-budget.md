# Bounded outbox payload reads

Outbox claims now select metadata first on PostgreSQL and Cassandra, then load
only the returned events' complete payloads. The old Cassandra scan fetched
payloads in 200-row pages before deciding which events were due. PostgreSQL
returned every selected body in one result. A row limit could therefore retain
hundreds of megabytes of large satellite fires.

Claims have an 8MiB payload target and a maximum of 50 events. Coastal warnings
have priority; when both classes have due work, each receives reserved byte and
row capacity. Each class includes its complete boundary event even when that
event exceeds its target, so a large oldest event cannot block forever. The
aggregate bound is the target plus at most one complete boundary payload per
nonempty class. Payloads are never truncated. This bounds multiplication by a
backlog; the largest individual canonical event still has to fit the supported
transport and application limits.

Only returned events receive a lease, starting after hydration. PostgreSQL
holds metadata row locks with `FOR UPDATE SKIP LOCKED`, reads selected bodies
inside the transaction and commits their leases together. Concurrent publishers
still receive disjoint events. Statement, idle and transaction deadlines bound a
frozen client holding locks. Cassandra keeps its elected single-consumer
contract, checks terminal markers before fetching bodies, and conditionally
leases an existing pending row so a concurrent acknowledgment cannot recreate
a partial row.

## Reproduction

Run database suites serially against disposable stores. The shared storage
contracts include `Outbox/PayloadBudgetAndFairness` and
`Outbox/LargePriorityClasses`. They exercise real writes, intact 3MiB bodies,
small coastal priority, large work in both classes, bounded claims, and immediate
availability of unreturned candidates. The ordinary outbox contracts also cover
lease expiration, acknowledgment, retry, release and concurrent publishers
(Cassandra deliberately skips concurrent publishers).

The opt-in Cassandra test `TestOutboxLargeBacklogBudget` builds 32 distinct
200,000-key satellite revisions in both durable outbox copies. The pending
partition alone exceeds a 256MiB driver response frame. Run a Linux test binary
with a 512MiB hard memory limit, `GOMEMLIMIT=384MiB`, `GOMAXPROCS=2`, and:

```text
AGG_TEST_CASSANDRA_HOSTS=<disposable-host>:9042
AGG_TEST_OUTBOX_STRESS=1
AGG_TEST_LARGE_FIRE_KEYS=200000
cassandra.test -test.run=^TestOutboxLargeBacklogBudget$ -test.v -test.timeout=5m
```

Use the chart's rendered Cassandra configuration and JVM limits inside a 1GiB
server cap. The test retries uncertain or server write timeouts during setup
with identical event/time keys; it reports the retry count rather than changing
production deadlines. It then checks whole-payload hashes, oldest-first claims,
unreturned work and release/reclaim. It reports process high-water RSS and
charged container memory. Check server logs for Java heap/direct-buffer OOMs as
well as Docker's OOMKilled flag; an internal Java failure need not set that flag.

## Acceptance limits

The final 32-event backlog contained 307,213,440 pending payload bytes and
returned one intact 9,600,420-byte event per claim. It passed in 48.28s including
schema setup (14.13s application work), at 151,512KiB peak client RSS and
159,252,480 bytes peak charged memory under the 512MiB cap. Five stress runs
passed overall; the maximum client peak RSS was 151,852KiB. All passing setups
needed zero retries. Cassandra's lifetime memory reached its configured 1GiB
ceiling; its internal logs and cgroup counters show no OOM in the final fixture.
Raw accounting and the distinction from RSS are preserved in the evidence.

Both real Cassandra bucket layouts pass mixed-priority/budget cases (90.582s).
The full PostgreSQL integration suite passes (11.898s), including concurrent
publishers. Full untagged Aggregator tests and 122 infrastructure tests pass
(one SOPS/age availability skip). Fixtures were run serially and removed.

An early combined oldest-ID/hash assertion failed without recording which
comparison differed. Its cause was not established. The fixture now logs both
values and uses explicitly ordered past creation timestamps; later repeated
and final runs pass. The early failure is retained alongside the passing logs
and buffer/GC failures in [the evidence report](evidence/m2-outbox-budget/report.json).

These synthetic single-node RF=1 fixtures do not establish RF=3 throughput or
official-feed catch-up. The subsequent [List payload-budget work](m2-list-payload-budget.md)
bounds catalog pages and normal summary reads. Get(SUMMARY), global admission
for concurrent full/cold-cache RPC/HTTP work and the combined processor/storage/
broker/consumer memory path remain F15 work.
The full implementation checklist and intended-topology gates remain in the
sibling infrastructure repository's `docs/m2-completion.md`.
