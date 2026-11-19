# Notification SUMMARY storage checkpoint

19 November 2026. Get/List(SUMMARY) previously selected and decoded each full
Hazard snapshot, including every wildfire detection key. Even the extra row used
to detect the next page hydrated a full blob. Push metadata was already fixed
in the preceding checkpoint; these are separate read paths.

## Implementation and upgrade

PostgreSQL migration 20261119115701_SummarySnapshot and SQLite migration
20261119115658_SummarySnapshot add a nullable Summary blob. New inserts store
an explicit protobuf allowlist alongside the immutable FULL snapshot in the
same transaction. It preserves timestamp seconds/nanoseconds. Projection copies
only known fields, including within Timestamp, so cached unknown protobuf fields
cannot carry private data through a SUMMARY response.

Warm SUMMARY Get/List selects metadata and Summary, excluding Hazard. Values
larger than 64KiB are ignored in SQL before transfer. This is cache eligibility,
not a canonical-content limit: an intact larger public summary can still use
the serialized fallback. Invalid protobuf, mismatched identity/metadata and bad
timestamps also use repair. FULL snapshots and public page cursors are unchanged.

All adapters share one NotificationReadBudget from DI. A legacy/corrupt cache
repair acquires its slot before reading any full body, rechecks the cache/current
row, reads one snapshot, copies an allowlisted header and persists it. Only the
small projection escapes the body-loading helper. The slot remains owned through
query cancellation/disposal and cache persistence. Waiting callers retain
headers; warm reads bypass the slot. List repairs rows sequentially.

Repair updates only current rows. If the row is superseded while repair is
pending, it returns no item and does not repopulate the old cache. No startup
full-table blob scan/backfill is required: old rows remain nullable and repair
on demand. Push claim/outcome SQL maps a NULL Summary placeholder, so adding
the EF property does not make those paths select or overwrite cached summaries.

## Verification

All 86 notification tests pass. Coverage added for exact nanoseconds, opaque
FULL snapshots behind warm headers, missing/corrupt/wrong-identity caches,
top-level and nested unknown-field privacy, current-row checks, cancellation
ownership and a real SQLite downgrade/old-row/upgrade/on-demand-repair lifecycle.
Whitespace formatting was applied to changed handwritten server/test files.
The available EF tool 10.0.9 generated the nullable-column migrations; runtime
10.0.12 builds, migration application and old-row upgrade tests pass.
Both provider snapshot checks report no pending model changes.

Native PostgreSQL verification uses the existing portable MemoryProbe with
NOTIFICATION_TEST_MODE=summary. Application cap is 256MiB memory/swap, two CPUs;
PostgreSQL 17.10 has a separate 512MiB cap. Ten snapshots each contain 200,000
keys. Twenty-four concurrent Get+List readers use a role denied SELECT on Hazard,
proving warm queries cannot hydrate it. Twenty-four cold readers share one body
read. A held command plus caller cancellation keeps twelve waiters from starting
another body read; release permits one recovery read. Oversized cache repair and
superseding a row immediately before cache UPDATE both pass. All original blob
lengths/MD5s remain unchanged; an authorized FULL read compares 200,000 exact keys.

The summary fixture peaks at 236,428KiB process RSS / 196,210,688 charged bytes.
The current-schema push regression also passes under 256MiB, including four
claimers, stale leases, ten blocked/completed pushes, redelivery, unchanged FULL
snapshots and preserved cache presence: 205,208KiB RSS / 159,227,904 charged bytes.
PostgreSQL peaks at 249,163,776 charged bytes across the serial cases. There are
no max/OOM/kill memory events or restarts. Process RSS and charged cgroup memory
count shared/file-backed pages differently; both raw measurements are retained.
See the [evidence report](evidence/m2-notification-summary/report.json) and logs.

The held-command interceptor tests ownership before the driver proceeds; it is
not a network-fault test. These direct storage probes do not prove full hosted
ASP.NET/AMQP/FCM memory, byte-bounded FULL notification pages/RPC output, webhook
fanout or combined/official/k3s/RF=3 acceptance. F15-a and F15 remain open.

## Reproduction

Build the locked MemoryProbe project as documented in
[the push checkpoint](m2-notification-outbox-memory.md), and mount its portable
output into the pinned ASP.NET 10 image. Set NOTIFICATION_TEST_POSTGRES to a new
disposable PostgreSQL database named notification_m2_fixture. The probe refuses
an unexpected name or nonempty table. Use disposable administrator credentials;
the probe creates restricted fixture roles. Never target an existing database.

Run the summary mode with NOTIFICATION_TEST_MODE=summary:

```text
dotnet /probe/BNPB.Notification.MemoryProbe.dll
```

Run without that variable on another empty fixture for the push regression.
Use 256MiB memory/memory+swap, two CPUs and DOTNET_PROCESSOR_COUNT=2, without a
test-only GC override. Use a separate 512MiB PostgreSQL cap, one DB stack at a
time. Capture terminal states/counters, then remove only verified task-labelled
containers and anonymous data. The minimal runtime's optional libgssapi warning
does not prevent password authentication or the passing assertions.
