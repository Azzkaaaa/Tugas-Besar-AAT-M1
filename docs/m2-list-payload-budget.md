# Bounded hazard List reads

Both stores select a bounded metadata page before loading full records.
Cassandra merges headers from its eight bucket shards; its driver pages no
longer contain full satellite attributes. PostgreSQL selects header fields
only, closes that result and reads selected bodies individually. The RPC view
is passed into storage, so summary requests skip normal payload hydration and
discard coordinates, source references and attributes at that boundary.

Full pages target 8MiB of protobuf wire data. One complete fire can exceed that
target and occupy a page by itself. A continuation is issued before every
unreturned candidate, including when all candidates originally fitted inside
the metadata page. Clients must follow the token, rather than infer completion
from the item count. Gateway/frontend already use that rule. No detection keys
are truncated. The exact complete RPC response, including freshness and cursor,
must fit the existing 16MiB gateway receive budget; a larger record/response is
reported as ResourceExhausted. Records that fit are not rejected by a guessed
metadata allowance.

The wire-size counter follows the generated protobuf schema and structpb
encoding without allocating a duplicate attribute tree. Tests compare it with
actual proto.Size for nested values, long strings, numeric lists, binary values
and unsupported attributes. JSON size alone would underestimate numeric lists.

PostgreSQL point reads can see a concurrent move or withdrawal after metadata
selection. Such a row is skipped at its former position; its metadata position
is consumed without emitting an out-of-order record. Paging remains a live
catalog view, not a transaction spanning all future page requests. Incomplete
storage ranges retain successful results and retry the incoming cursor.

## Cassandra upgrade

Migration 0017 adds list_active, list_revision and list_wire_bytes to the existing
sharded projection. New writers set them in the same row mutation as attributes
and revision, preserving the existing final projection completion marker and
authoritative fencing batches. The cache is trusted only for its matching row
revision. A rolling prior writer changes revision without updating these fields,
which invalidates the cache.

Missing/stale caches use one full fallback at a time across Get/List on the DB
connection pool (see the later read-admission follow-up). A bounded
best-effort conditional update fills derived metadata only if revision has not
changed. Cold old rows can therefore require a detail read even for SUMMARY;
new/valid cached rows do not. Cache writes never change canonical state,
authority, outbox, attributes or the projection completion revision. Actual
bucket locations are retained through the merge for subsequent point reads.

## Verification and limits

Final evidence is in [the captured report](evidence/m2-list-budget/report.json).
After the broader Cassandra suite, the capped RPC repetition returned 32
summaries in 2,547 bytes and all full records in 32 pages (largest complete
response 9,600,451 bytes). Every one of the 200,000 keys in each fire was checked.
Peak client RSS was 176,756KiB, charged memory 185,315,328 bytes; application work
took 11.69s (49.95s with schema). The same process also passed 200,000-key legacy
cache repair and the large outbox regression, peaking at 221,960KiB RSS overall.

The original server profile was cgroup-OOM-killed during a repeat after the
broader suite. The chart now shares Netty/JDK direct accounting and uses smaller,
bounded arenas. The same sequence passes, with no server restart, Java OOM/CRC
error or cgroup OOM/kill counter. Exact allocation attribution was not isolated;
the raw failure and passing evidence are retained. Server memory still reaches
its 1GiB ceiling. These are workload-specific results, not a general proof of
headroom under RF=3/full-feed traffic.

Both bucket layouts pass List contracts (118.218s), schedule-change contracts
pass (58.578s), and move/backfill/upgrade cases pass (122.209s). Full PostgreSQL
integration passes (11.522s), including nanosecond bounds against stored
timestamp precision. Full Aggregator tests and 122 infrastructure tests pass
(one SOPS/age availability skip). Fixtures were run serially and removed.

Shared real-database contracts cover 3MiB records across buckets, tied timestamps,
byte-limited continuation, whole attributes, summary privacy and boolean-only
withdrawal semantics. Cassandra also exercises schedule changes, legacy backfill
and missing/rolling-writer caches. The exact 16MiB response boundary and the
storage projection hint are tested at the RPC handler.

The opt-in TestListLargeSatelliteRPCBudget uses a 512MiB Linux process with
GOMEMLIMIT=384MiB and GOMAXPROCS=2, against the chart's Cassandra 5.0.9 memory
profile under a 1GiB cap. It seeds 32 complete 200,000-key read projections,
exactly four in every shard of one bucket. The real RPC handler, protobuf
serializer, TCP socket and client decode run in that capped process. A summary
request must return all 32 headers; full requests must page in global order and
preserve every detection key. Configure a disposable store with:

```text
AGG_TEST_CASSANDRA_HOSTS=<disposable-host>:9042
AGG_TEST_LARGE_FIRE_KEYS=200000
AGG_TEST_LIST_STRESS=1
cassandra.test -test.run=^TestListLargeSatelliteRPCBudget$ -test.v -test.timeout=5m
```

This fixture injects trusted test caller scopes and isolates read projections.
It does not exercise JWT verification, the public HTTP gateway, concurrent full
responses, live ingest/outbox, broker/consumers or the disk processor together.
The later [read-admission follow-up](m2-read-admission.md) adds metadata-only
Get(SUMMARY), shared cold fallback and unary full-response admission through
RPC/HTTP delivery. Full SSE/feed and the combined processing/consumer path plus
full-feed RF=3 p95/catch-up/reference acceptance remain F15 work.
