# Streaming satellite recovery caches

`storage.StreamingSnapshots` saves an immutable seekable input and opens a
verified temporary disk file. The production path never passes a whole state
through `[]byte`. Individual mutations/download pages are bounded to 256KiB
chunks (Cassandra uses two-row pages); hashing uses 64KiB buffers. The caller
must close a downloaded reader to delete its private file.

PostgreSQL migration 9 stores chunks and metadata in one transaction, serializes
publication/retention per stream and uses repeatable-read downloads. Server
timeouts bound frozen transactions. Cassandra migration 16 addresses chunks by
content digest, uploads before publishing, and uses `IF NOT EXISTS` so a second
codec cannot overwrite the first complete snapshot at the same sequence.
Both keep the latest two published sequences and verify count, order, size and
SHA-256 before returning a candidate. Corruption tries an older retained cache;
storage/network/disk errors are returned, never disguised as verified state.

Cassandra chunks and manifests expire after seven days, including incomplete
uploads left by killed/frozen writers. A long-idle stream may therefore need a
full journal replay. A retired uploader can briefly recreate orphan chunks;
expiration bounds their lifetime. The replicated journal/checkpoint is always
the recovery authority and does not inherit this cache expiration.

Legacy blob/chunk snapshot tables are left intact for rolling rollback. The new
streaming interface ignores those old caches; the first upgraded term replays
the journal and publishes the new codec. This avoids loading a potentially
large legacy blob just to discover its format is incompatible.

Verification on 19 November 2026 used disposable real PostgreSQL 17 and
Cassandra 5.0.9 containers, one at a time, each capped at 1GiB/2 CPUs. The common
contract streams a 12MiB+123 byte generated input, checks restored content,
immutable same-sequence and concurrent publication, retention, failed/partial
source reads and temporary-file cleanup. Backend tests also corrupt/delete
stored chunks and prove fallback/rejection. PostgreSQL passed in 1.403s and the
final Cassandra run passed in 35.223s (963.4MiB observed, no OOM). Untagged
storage tests pass. These single-node fixtures do not prove quorum performance
or full-feed recovery on the assessed separate-kernel cluster.

From `services/aggregator`, with the documented Go memory limits and disposable
`AGG_TEST_POSTGRES_DSN` / `AGG_TEST_CASSANDRA_HOSTS`, run serially:

```powershell
go test -tags integration -p 1 -parallel 1 ./internal/storage/postgres -run 'TestStreamingSnapshots|TestContract/Snapshots' -count=1 -v
go test -tags integration -p 1 -parallel 1 ./internal/storage/cassandra -run '^TestStreamingSnapshots$' -count=1 -v
```
