-- Small, hash-sharded indexes: payloads remain in hazards_by_id. The same
-- logged projection batch maintains these rows and the public read tables.
CREATE TABLE IF NOT EXISTS audit_events_by_shard (
    scope text, shard int, ingested_at timestamp, hazard_id text,
    source_ref_id text, active boolean, revision bigint,
    PRIMARY KEY ((scope, shard), ingested_at, hazard_id)
) WITH CLUSTERING ORDER BY (ingested_at ASC, hazard_id ASC);
CREATE TABLE IF NOT EXISTS audit_refs_by_shard (
    scope text, shard int, source_ref_id text, hazard_id text,
    active boolean, revision bigint,
    PRIMARY KEY ((scope, shard), source_ref_id, hazard_id)
) WITH CLUSTERING ORDER BY (source_ref_id ASC, hazard_id ASC);
