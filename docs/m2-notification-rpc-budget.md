# Notification FULL paging and delivery bounds

19 November 2026. FULL notification List previously materialized pageSize+1
complete snapshots. Service-method completion also did not bound encoded data
waiting on a slow HTTP/2 peer. This checkpoint bounds both paths.

## Storage and response budget

FULL Get/List preflights metadata and SQL blob lengths before reading a body.
List streams candidate headers instead of collecting the whole page, and uses
a separate context/connection to hydrate one selected body at a time. Oversized
body candidates do not transfer their hazard ID/area text. Candidate objects
drop raw bytes immediately after parsing. Lookahead never hydrates an extra body.

Content is limited to 16MiB minus 64KiB reserved for freshness/cursor data.
Preflight includes protobuf notification/projection/framing overhead and alert
text, followed by exact item measurement. A byte-boundary item is left behind
the continuation token; it is never skipped or truncated. A first item that
cannot fit is rejected before body transfer. Both RPC responses also check their
complete serialized size, including freshness, cursor and notification text,
against the explicit 16MiB send limit matching the gateway receive budget.

## Admission and transport ownership

One FULL read pipeline per replica shares its payload slot with legacy SUMMARY
repair. Scope checks precede admission; warm SUMMARY avoids that slot. Direct
store calls hold a hydration slot through their work. Production RPCs retain
one admission reference in outer request middleware, so serialization remains
inside the lifetime. A mutable scope holder carries the authorized credit back
from the async handler to the serializer.

A decorator wraps the actual Kestrel memory pool. Rented output owners retain
the credit until disposal, including data queued after application completion.
Large size hints use exact unpooled arrays rather than retaining multi-megabyte
pool buckets. Ordinary rentals retain the framework's evicting pool. The pool
advertises support for large hints so Pipe cannot bypass ownership wrapping.
Small uncredited stream/connection buffers are separately bounded: 32 connections,
16 HTTP/2 streams per connection and a 64KiB response-buffer limit. The service
and probe both use server GC and a 50% heap hard limit (128MiB under a 256MiB
container), leaving space for native/runtime/network allocations.

This design uses the supported .NET 10
[memory-pool factory](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel/memory-management?view=aspnetcore-10.0).
The pinned gRPC 2.64
[writer](https://github.com/grpc/grpc-dotnet/blob/v2.64.0/src/Grpc.AspNetCore.Server/Internal/PipeExtensions.cs)
and Kestrel
[HTTP/2 output producer](https://github.com/dotnet/aspnetcore/blob/v10.0.0/src/Servers/Kestrel/Core/src/Internal/Http2/Http2OutputProducer.cs)
explain why a handler-only semaphore is insufficient. The configured
[heap setting](https://learn.microsoft.com/en-us/dotnet/core/runtime-config/garbage-collector#heap-hard-limit-percent)
is in the deployed service project, not a test-only environment override.

## Evidence and limits

All 92 notification tests pass. New tests cover a large opaque lookahead,
byte-boundary continuation, oversized rejection before transfer, complete RPC
envelope overflow, authorization before capacity and pool-owner lifetime after
the request scope ends. Gateway backend tests pass; no production gateway change
was required.

The native fixture runs the actual Kestrel/gRPC service and PostgreSQL store,
with fixed test auth/freshness. Ten distinct snapshots contain 200,000 keys each.
The production Go backend client and raw HTTP/2 clients run separately. Each
application container is capped at 256MiB memory/swap, two CPUs; PostgreSQL uses
a separate 512MiB cap. Zero-window output retains an actual pool owner; 24 extra
FULL requests are shed without any body read. Media FULL is denied, warm SUMMARY
remains available, and three RST cycles recover admission. Ten byte-bounded pages
compare all two million detection keys. Exactly 13 bodies are read: three held
Get responses and ten emitted List items. There is no lookahead hydration.

The final Go client peaks at 113,737,728 charged bytes. The .NET process reports
250,860KiB RSS, a 134,217,728-byte GC budget, 88,532,328 live heap bytes and
269,856,768 peak cgroup bytes. It exits successfully with zero OOM/kill/restart,
but memory.events.max is 562. Before serving, file cache alone is 158,273,536
bytes (100,618,240 mapped), with a 5,406,464-byte live heap. These measurements
show substantial file-backed runtime/assembly cost and reclamation pressure;
they do not demonstrate spare memory for concurrent ingestion. Earlier pressured
runs are retained instead of discarded. PostgreSQL peaks at 150,589,440 bytes.

The kernel defines max events as boundary/reclaim attempts and allows temporary
overshoot; these are distinct from OOM events. See the
[cgroup memory controller](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory).
The measured pressure remains a combined-load concern. Do not report a clean
memory-event run or infer hosted AMQP/FCM, webhook, public proxy, official feed,
RF=3 or separate-kernel k3s acceptance from this fixture. F15-a/F15 stay open.
Raw logs, pressure runs, caps and image digests are in the
[evidence report](evidence/m2-notification-rpc/report.json).

## Reproduction

Publish the locked portable MemoryProbe project as in prior notification docs.
Use a fresh disposable PostgreSQL notification_m2_fixture database and fixture
credentials; the probe refuses other names/nonempty tables. Run with
NOTIFICATION_TEST_MODE=rpc, NOTIFICATION_TEST_POSTGRES, DOTNET_PROCESSOR_COUNT=2
and the 256MiB cap. The common production hosting extension configures the
actual Kestrel memory pool/limits; no Firebase or live identity provider is used.
Ports 19094 (h2 gRPC) and 19095 (fixture stats) stay in the private test namespace.

Cross-compile API Gateway backend tests to Linux/amd64 and run under a separate
256MiB cap, GOMAXPROCS=2, GOMEMLIMIT=160MiB, NOTIFICATION_RPC_TARGET=127.0.0.1:19094
and NOTIFICATION_RPC_STATS=http://127.0.0.1:19095/fixture/stats:

```text
backend.test -test.run=TestNotificationFullRPCMemory -test.v=true -test.timeout=75s
```

Close clients, touch /tmp/m2-notification-rpc-stop for graceful shutdown and
capture final state/counters. The fixture also stops after 120s. Remove only
verified task-labelled runtime containers/anonymous data; retain compiler caches.
