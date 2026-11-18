# Write authority for leader terms

Timer cancellation is not write authorization. A process suspended beyond its
lease can resume a worker before the renewal/cancellation goroutine runs.
Each lease acquisition now uses a random term suffix in addition to the pod
identity. Storage implementing `leader.FencingStore` supplies a monotonically
increasing epoch and an authority context before the job starts. A completed
renewal must refresh the storage authority too. Reacquiring on the same pod
cannot make an older job context valid again.

BMKG/PVMBG and M2 HTTP transports validate the bound authority immediately
before making an upstream request. This avoids requests from a known expired
term. It does not replace the atomic write check or claim exactly-once delivery
of a nontransactional HTTP request across arbitrary network/process pauses.
The actual upstream log/fault criterion remains a runtime acceptance gate.

## PostgreSQL

Migration 00006 preserves the lease epoch across expiry and release. A new
acquisition increments it, including reuse of the same holder after expiry.
Lease expiry and write checks use the database's `clock_timestamp()`, not an
Aggregator's wall clock. Leader writes set transaction-local authority settings;
triggers check lease name, holder, epoch and expiry at the actual INSERT/UPDATE
statement, including a hazard that has never existed before. The triggers cover
hazards, M2 input journal/checkpoints, fallback quarantine and source watermarks.
Outbox writes remain in the same transaction as the authorized hazard write.

The check takes a lease-row share lock until transaction end, so takeover is
serialized with the commit. Transactions have a 2s lock timeout, 4s statement
timeout and 5s idle-in-transaction timeout. A frozen application cannot keep an
uncommitted hazard/lease lock indefinitely; the server rolls the transaction
back before a successor proceeds. Transaction-local settings do not leak through
the connection pool. Administrative imports and unit fixtures outside a leader
context retain their existing semantics; production pollers use lease contexts.

Real PostgreSQL integration verifies an uncancelled expired context cannot
insert a new hazard, update an existing one, append a journal entry, commit a
checkpoint, park input or update source state. It also checks epoch reuse and
current-term success. A separate test pauses the caller after the hazard INSERT:
the server expires its transaction, takeover succeeds, and the old hazard remains
absent when the caller resumes. The frozen-transaction test passed in 5.45s;
the entire PostgreSQL integration suite and untagged Aggregator suite pass.

## Cassandra

Migration 0011 places canonical records in 16 `canonical_events` partitions.
Each partition contains static source-authority maps. The authority condition
and the actual insert/revision CAS run in the same single-partition conditional
batch, including an ID that has never existed. Inbox mutations use the same
pattern in their 32 partitions. The journal, checkpoint and fallback quarantine
share 16 `stream_inputs_by_shard` partitions, covering future journal blocks;
source-state mutations check authority in their own row. Chunk archives may be
staged before metadata acceptance, but only guarded Inbox metadata accepts them.

The durable lease stores a monotonically increasing epoch and absolute expiry.
Acquisition, renewal and expiry checks use database time and Paxos. Before a
source worker starts, four bounded workers install its epoch on every mutation
partition and then revalidate the lease. A successor does not start before that
barrier completes. Higher installed epochs reject delayed old activation.
Release installs an odd revocation version before releasing the global lease,
so a delayed activation of that same term cannot restore permission. Ordinary
writes use server timestamps too; client timestamps could otherwise override
conditional outbox writes when application and storage clocks differ.

Existing canonical data is adopted with `IF NOT EXISTS` during bounded
background backfill and on access. Reads fall back to legacy records and stream
reads merge old/new journal blocks; retried legacy input retains its original
bytes. Audit remains partial until backfill finishes. Canonical placement now
uses `token(shard)`. Do a controlled upgrade: stop all old source writers,
deploy the new schema/build, wait for backfill, and then enable the new writers.
An old binary writing the legacy table cannot participate in this protocol.
Keep legacy tables until the upgraded catalog has been independently audited.

`TestCassandraProductionWritesFenceExpiredAndReleasedTerms` exercises the real
production paths with an uncancelled expired context, new/revised hazards,
intake, an unseen journal block, checkpoint, source state and quarantine. A
worker paused before canonical CAS resumes after takeover and is rejected.
The test also checks current-term success and permanent release revocation.
Focused integration passes on pinned Cassandra 5.0.9 under a 1 GiB limit;
separate upgrade and repeated outbox retry/release regressions pass too.
The 607.35s full run passed those paths and both ordinary storage contracts;
three tests stopped during keyspace creation at the old five-second bootstrap
timeout. Keyspace bootstrap now has 30 seconds while application queries retain
five seconds. All three failed cases passed together on rerun (151.11s),
including the complete schedule-change contract. The full untagged Aggregator
suite also passes. Cassandra stayed below its 1 GiB cgroup cap without OOM;
observed usage reached about 1,011 MiB. The disposable fixture was removed.
These RF=1 fixtures do not prove multi-node clock/freeze acceptance, which
remains separately unchecked in the infrastructure completion checklist.

Reproduce from `services/aggregator` with `AGG_TEST_CASSANDRA_HOSTS` set to a
disposable Cassandra 5.0.9 endpoint and the memory-limited Go environment from
the infrastructure checklist:

```powershell
go test -p 1 -parallel 1 ./...
go test -p 1 -parallel 1 -tags integration ./internal/storage/cassandra -count=1 -timeout=18m -v
```

## Deployment enforcement

`AGGREGATOR_REQUIRE_WRITE_AUTHORITY=true` rejects single/Kubernetes election or
an elector different from the canonical backend. Configuring an M2 source also
enables this requirement automatically. The M2/P3 chart sets it explicitly and
the Cassandra client ConfigMap selects Cassandra election. Use database-backed
election and upgraded writers throughout the deployment.
