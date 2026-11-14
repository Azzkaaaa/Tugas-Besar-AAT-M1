-- Baseline schema of the Cassandra canonical store. Statements are separated by ';' at the end
-- of a line. Later migrations must be additive (expand/contract), like the Postgres ones.

-- One row per hazard: Get, and the current content ApplyRevision compares against.
-- attributes is the JSON object of source-specific fields (schemaless, as JSONB in Postgres).
CREATE TABLE IF NOT EXISTS hazards_by_id (
    hazard_id text PRIMARY KEY,
    source text,
    source_ref_id text,
    hazard_type text,
    severity text,
    area_name text,
    latitude double,
    longitude double,
    occurred_at timestamp,
    ingested_at timestamp,
    attributes text
);

-- Identity of a source record. Claimed with INSERT ... IF NOT EXISTS so a (source, ref) pair
-- maps to exactly one hazard_id.
CREATE TABLE IF NOT EXISTS hazard_by_source_ref (
    source text,
    source_ref_id text,
    hazard_id text,
    ingested_at timestamp,
    PRIMARY KEY ((source, source_ref_id))
);

-- List: hazards partitioned by occurred_at truncated to the keyspace's bucket period, newest
-- first inside a bucket.
CREATE TABLE IF NOT EXISTS hazards_by_bucket (
    bucket timestamp,
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
    PRIMARY KEY ((bucket), occurred_at, hazard_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, hazard_id ASC);

-- Buckets that hold data, per scope: a source name, or '*' for all sources. List walks these
-- instead of every period, and HasData reads one row. A bucket emptied by a revision that moved
-- its only hazard stays listed; List then reads an empty partition.
CREATE TABLE IF NOT EXISTS hazard_buckets (
    scope text,
    bucket timestamp,
    PRIMARY KEY ((scope), bucket)
) WITH CLUSTERING ORDER BY (bucket DESC);

CREATE TABLE IF NOT EXISTS source_state (
    source_key text PRIMARY KEY,
    watermark_time timestamp,
    last_success_at timestamp,
    last_attempt_at timestamp,
    status text,
    error_message text
);

-- Every outbox event by id. Rewritten with a TTL of the outbox retention once it is published or
-- dead-lettered, so pruning is Cassandra's job.
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id text PRIMARY KEY,
    hazard_id text,
    source text,
    payload blob,
    trace_context text,
    created_at timestamp,
    retry_count int,
    last_error text,
    published_at timestamp,
    failed_at timestamp
);

-- Events still to publish, oldest first, in a single partition (one consumer). Rows are deleted
-- as events are published, so tombstones are collected after an hour instead of ten days; a row
-- resurrected by a node that missed the delete is published again, which at-least-once allows.
CREATE TABLE IF NOT EXISTS outbox_pending (
    shard int,
    created_at timestamp,
    event_id text,
    hazard_id text,
    source text,
    payload blob,
    trace_context text,
    retry_count int,
    next_attempt_at timestamp,
    last_error text,
    PRIMARY KEY ((shard), created_at, event_id)
) WITH CLUSTERING ORDER BY (created_at ASC, event_id ASC)
  AND gc_grace_seconds = 3600;

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    event_id text PRIMARY KEY,
    hazard_id text,
    source text,
    payload blob,
    trace_context text,
    created_at timestamp,
    failed_at timestamp,
    last_error text
);
