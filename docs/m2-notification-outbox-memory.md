# Notification push metadata checkpoint

19 November 2026. The push outbox previously returned complete stored hazard
blobs for each claimed row and decoded them all. A default batch of ten large
wildfires could retain ten full detection lists even though FCM sends only the
notification ID. Upserting an outcome or redelivery also loaded the snapshot.

Claims now return metadata, lease and trace context. PostgreSQL SKIP LOCKED
returns explicit header/status columns and an empty bytea placeholder for EF's
required Hazard mapping. SQLite selects the same metadata projection. Claims
contain neither a full hazard nor a summary protobuf. FCM still receives its
existing ID/type/timestamp push. Current/failed/coastal priority, lease recovery,
trace continuation and retry behavior remain covered by the existing suite.

Existing-row upserts read and track metadata only. PostgreSQL retains xmin for
optimistic concurrency; SQLite retains its IMMEDIATE transaction. Outcome updates
never mark the immutable Hazard property modified. Inserts still write the whole
snapshot; no detection keys are truncated. The RabbitMQ adapter now parses its
borrowed delivery span directly, removing a redundant complete wire-buffer copy.

## Verification

All 79 notification tests pass on .NET SDK 10.0.301. The new SQLite regression
uses an opaque malformed snapshot: claims, outcome writes and redelivery must
work without parsing it, and must leave its exact bytes unchanged. Existing
tests cover leases, old/new revisions, failure backoff, coastal retry, paging,
scope and FULL/SUMMARY projections. The local telemetry test needs HTTP listener
access; it failed inside the Windows sandbox and passed with that access.

The independent native MemoryProbe references the production store and push
dispatcher. It runs under a 256MiB memory/swap cap with two CPUs, against a
separate 512MiB PostgreSQL 17.10 fixture. It migrates an empty dedicated database
and stores ten distinct 200,000-key snapshots, each over 9MB. Four concurrent
claimers obtain ten distinct rows. A database role can select metadata and xmin,
and update push state, but cannot select Hazard. The probe first confirms this
denial, then uses that role for claims, stale-lease rejection, releases, a blocked
ten-push batch, overlapping redelivery and final outcomes. Thus a hidden full
SELECT/RETURNING fails even if a parser or allocation measurement missed it.

All ten pushes complete without repeats. Before/after length+MD5 checks show
that status writes preserve every snapshot. One final authorized FULL read checks
all 200,000 detection keys. Peak process RSS was 207,360KiB; charged cgroup peak
was 264,437,760 bytes, including fixture seeding and final FULL verification.
No max/OOM/kill memory events or restarts occurred. PostgreSQL peaked at
150,691,840 charged bytes under its separate cap. See the
[evidence report](evidence/m2-notification-outbox/report.json) and raw logs.

This verifies the push/store subtask. It does not prove the complete hosted
ASP.NET/RabbitMQ/FCM process under concurrent production load. Notification
SUMMARY reads still select/parse full snapshots, and FULL pages/RPC responses
still need byte/admission bounds. Webhook subscription fanout/delivery and
combined-path memory remain open. F15-a and F15 remain unchecked.

## Reproduction

From services/danger-notification, restore the locked projects and run the tests
serially. SDK compilation is separate from the runtime cap. Build the portable
probe with:

```text
dotnet restore BNPB.Notification.MemoryProbe/BNPB.Notification.MemoryProbe.csproj --locked-mode
dotnet publish BNPB.Notification.MemoryProbe/BNPB.Notification.MemoryProbe.csproj --no-restore -m:1 -o ../../.cache/m2-notification-outbox/probe
```

Start a new PostgreSQL fixture with database name notification_m2_fixture and
disposable administrator credentials. Run the probe in the pinned ASP.NET 10
runtime image (digest in images.log), with a read-only mount of that output and
NOTIFICATION_TEST_POSTGRES pointing to that isolated database. The probe refuses
an unexpected database name or a nonempty notifications table. It needs role-
creation privileges to install its metadata-only test role; these are never
production application permissions. Fixed credentials in the probe are only for
this disposable role. Do not point it at an existing database.

```text
dotnet /probe/BNPB.Notification.MemoryProbe.dll
```

Use 256MiB memory and memory+swap, two CPUs and DOTNET_PROCESSOR_COUNT=2 for the
probe, and a separate 512MiB PostgreSQL cap. There is no test-only GC heap override.
The minimal runtime emitted a missing optional libgssapi warning; password
authentication and every assertion completed successfully. Inspect terminal
state before removing only task-labelled containers and their anonymous data.
