-- Public List projection, split into projectionShards partitions per bucket so
-- a burst inside one time bucket spreads over the ring instead of landing on
-- the RF replicas of a single partition. Migration 15 (Go) backfills it from
-- hazards_by_bucket, which is no longer written afterwards (one-way).
CREATE TABLE IF NOT EXISTS hazards_by_bucket_shard (
    bucket timestamp,
    shard int,
    occurred_at timestamp,
    hazard_id text,
    source text,
    source_ref_id text,
    hazard_type text,
    severity text,
    area_name text,
    latitude double,
    longitude double,
    ingested_at timestamp,
    attributes text,
    revision int,
    PRIMARY KEY ((bucket, shard), occurred_at, hazard_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, hazard_id ASC);
