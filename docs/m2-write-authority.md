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

## Cassandra work still required

The existing `hazards_by_id` partition does not contain source authority.
A pre-write SELECT against `leader_leases` would leave a check/write race and is
not sufficient. Authority and mutation must share the same conditional Paxos
boundary. [Apache Cassandra's batch guidance](https://cassandra.apache.org/doc/stable/cassandra/developing/cql/batch/batch-good-example.html)
describes static-column conditions in single-partition batches. The integration
probe in `authority_partition_integration_test.go` tests this primitive on the
pinned image; production Cassandra canonical/intake/stream writes still need to
adopt it. F05 remains unchecked until that implementation and its fault tests
are complete. The legacy single/Kubernetes electors also do not supply this
storage authority and must not be used as evidence for a fenced M2 deployment.
