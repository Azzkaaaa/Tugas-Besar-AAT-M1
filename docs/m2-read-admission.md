# Bounded summary and unary full reads

Get(SUMMARY), including the unspecified/default view, now asks storage for an
active header. PostgreSQL selects only summary columns and checks the exact
boolean withdrawal value in SQL. Cassandra selects headers and revision-checked
activity metadata. It never falls back to an older active legacy row when the
authoritative row is withdrawn. Full Get keeps its existing complete record
semantics and now checks the exact 16MiB complete response budget, as List does.

Migration 0018 adds nullable summary_active/summary_revision to canonical_events
and hazards_by_id. New canonical inserts, fenced revision updates and imports
write these fields in the same mutation as the row's revision. Authoritative
conditional batches and their fencing checks remain intact. Prior writers
invalidate the cache when they change revision. Missing/stale metadata uses
current content under one shared Get/List fallback slot per DB connection pool,
then optionally fills metadata with a matching-revision CAS. Null legacy
revisions remain readable without trusting a false revision match.

The shared fallback slot covers decoding, withdrawal/filter checks and copying
the small header. It is returned only after the full body is no longer retained
by the caller. Waiters hold headers, obey deadlines and preserve partial/retry
behavior. Normal cached summary reads bypass the body slot.

## Delivery lifetime

The Aggregator allows one authorized unary FULL Get/List at a time per gRPC
server. JWT verification and the full scope check precede admission. Excess full
reads fail promptly with ResourceExhausted; summaries do not consume this credit.
The production Start method installs BoundedReadOptions after JWT interception.

An ordinary interceptor defer, or even stats.End, can run while HTTP/2 still
retains a large encoded buffer. The server therefore uses its pinned gRPC 1.85
CodecV2 and mem.Buffer reference lifecycle. The protobuf buffer is wrapped
without a large copy; a pool callback returns credit only after the final slice
and transport reference are freed. Tiny replies need a small callback-capable
buffer. Error/cancellation paths that skip encoding are cleaned up at RPC end.
Full output uses identity encoding, including when the request uses gzip, so
compression cannot release credit while a separate compressed buffer is queued.
Revalidate this integration when upgrading gRPC's experimental memory APIs.

The gateway shares one FULL unary credit across hazard and notification Get/List
routes. It covers upstream decode, mapping, generated JSON encoding and HTTP
writing. Existing general and reserved request permits also remain held until
the outer HTTP handler finishes, rather than just the generated business method.
Busy full requests return the documented 429 without contacting the backend.
Existing signature-verified Field Team/Internal Ops reserved capacity is preserved.

## Verification

Raw logs, resource settings and a compact report are in
[m2-read-admission evidence](evidence/m2-read-admission/report.json).
Tests cover real Cassandra cache upgrade/old-writer withdrawal changes, import
mutations, null legacy revisions, concurrent cold Get/List waiters, whole summary
fields and untouched full bodies. PostgreSQL runs the same Get summary contract.

A real TCP HTTP/2 peer advertises a zero response window. After the handler and
stats.End finish, twelve competing FULL RPCs are shed, SUMMARY succeeds and the
credit stays held until the stalled peer resets its stream. Complete gzip-request
responses, marshal failures, cancellations, tiny partial replies and final buffer
slices are checked. The real generated HTTP router checks blocked writes, write
errors/panics, shared notification admission and reserved priority capacity.

The opt-in TestFullHTTPLargeFireMemory runs a real gRPC server, the deployed
gateway client options, mapping and generated HTTP serialization in one native
256MiB process. It retains an immutable 200,000-key upstream fixture, blocks the
HTTP writer, sends 24 competing FULL requests and verifies every emitted key.
Use GOMAXPROCS=2, GOMEMLIMIT=160MiB and AGG_TEST_HTTP_STRESS=1. The Cassandra
large-summary/RPC fixtures use a separate 512MiB process, GOMEMLIMIT=384MiB,
AGG_TEST_LARGE_FIRE_KEYS=200000 and AGG_TEST_LIST_STRESS=1. Heavy jobs run serially.

These results cover unary read delivery and shared cold fallback. FULL SSE/feed
delivery and consumer notification projections still need combined-path memory
verification with the disk processor, live ingest/outbox and broker/consumers.
No intended-topology k3s/RF=3, official satellite feed, full catch-up/p95 or public
JWT deployment acceptance is inferred. F15 remains open. The specification's
session/SSE API is the satellite reference; no separate sample dataset exists.
