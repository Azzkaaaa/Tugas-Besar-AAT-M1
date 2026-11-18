# Partial reads during storage faults

Public hazard reads distinguish authoritative absence from an unavailable store.
The gRPC read budget is 1.5 seconds (inside the gateway's 2-second deadline),
source freshness probes are bounded to 100ms each, and Cassandra bucket reads
are bounded to 250ms each. Dependency failure does not fail process liveness;
an instance unable to reach storage becomes unready until connectivity recovers.

`ListHazards` and `GET /api/v1/hazards` retain the rows successfully read and
return `partial: true` with `unavailable_units`. These names identify logical
bucket/index/state units; physical replica placement belongs to the Audit
location endpoint. Source staleness also sets partial, independently of storage
incompleteness.

A partial storage page must be retried before advancing. Cassandra returns the
incoming cursor (an opaque restart cursor on the first page), so a temporarily
unavailable newer bucket is not permanently skipped. Clients may see the same
available records while retrying; deduplicate by hazard identity. Once the
interval is complete, normal ordered pagination resumes. The frontend disables
next-page navigation while storage units are unavailable and offers refresh.

`GetHazard` returns a partial response with no `item` when the record's storage
cannot be read. This is not a 404. An authoritative successful read which finds
no record still returns 404. A healthy complete response is required to have an
item; the gateway rejects inconsistent backend responses.

Audit uses ingestion-time indexes and streams byte-sorted references. It retains
healthy shard results and the original cursor on an incomplete interval. See
`m2-audit.md` for the index/backfill and node-identity contracts. Full aggregation
and analytics remain tracked under F04.

Verification uses Cassandra range/index fault fixtures, bounded gRPC source
failure tests, real HTTP-to-gRPC response-contract tests, and a frontend adapter
test for an unavailable detail. All untagged Aggregator/API Gateway tests and
the 34 frontend tests/production build pass. Live multi-node partition testing
is still required by the completion checklist.
