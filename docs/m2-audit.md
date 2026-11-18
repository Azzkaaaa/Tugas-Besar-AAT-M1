# Indexed Milestone 2 Audit API

The Aggregator exposes events, checksum and location on the internal audit
listener. The assistant's audit token is required; raw failed payloads still
require an Internal Ops JWT and `ingest:inspect`.

Events use `ingested_at ASC, hazard_id ASC`; `since` is inclusive ingestion time.
The cursor binds source and since. PostgreSQL has matching btree indexes;
Cassandra maintains 16 hashed metadata shards for each source and for all
sources. A bounded merge keeps 64 metadata rows per shard and at most four
initial shard queries in flight. Canonical payloads are fetched only for emitted
rows. Pages target an 8 MiB payload budget, allowing one larger record intact.
Checksum merges the reference index in byte order and streams SHA-256 with
newlines between references and no trailing newline. It does not load all
canonical payloads or sort the whole catalog in application memory.

Cassandra upgrades backfill the new indexes under a renewed distributed lease.
Reads remain explicitly partial until the backfill marker is complete. New
writes update the indexes with the existing projection batch. Logical revision
timestamps prevent stale backfill from reactivating a withdrawn event. Failed
shards are named in `unavailable_units`; the incoming cursor is returned on
partial pages, so clients retry the incomplete interval rather than skip it.
Deduplicate by hazard ID when retrying. Events/location have a five-second
request budget and checksum twenty seconds; per-shard queries are bounded to
250 ms. No complete snapshot across concurrent revisions is claimed.

Location uses Cassandra's server-computed partition token, actual keyspace
replication settings and driver ring metadata, including down natural replicas.
The first natural replica is reported as `primary`, subsequent copies as
`replica`. Set `CANONICAL_STORE_CASSANDRA_NODE_FROM_RACK=true` only for a deployment
that sets each Cassandra rack to its Kubernetes `spec.nodeName`. Without this
contract the response reports missing node identity explicitly. PostgreSQL reads
the custom setting `bnpb.node_name`; its chart sets this from the downward API.
An authoritative absent or withdrawn hazard returns JSON 404; unavailable
storage returns a partial response.

Verification: the untagged unit suites cover merge/refill order, a missing
shard, rack-aware replica selection, cursor binding/retry, checksum byte order
and HTTP errors. `TestAuditIndexedPaginationChecksumWithdrawalAndLocation`
(`-tags integration`, Cassandra) exercises 1,050 records, tied ingestion times,
withdrawal and stale backfill. `TestAuditIndexedPaginationChecksumAndLocation`
(PostgreSQL) exercises the SQL migration and node identity. Both run against
disposable single-node databases; they do not prove multi-node fault acceptance.
