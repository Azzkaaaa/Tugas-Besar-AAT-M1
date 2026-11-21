-- Small headers in fixed hash shards avoid serially visiting empty minute
-- buckets after fire merges. Full payloads remain in time-bucket partitions.
CREATE TABLE IF NOT EXISTS list_headers_by_shard (
    scope text,
    shard int,
    occurred_at timestamp,
    hazard_id text,
    source text,
    hazard_type text,
    severity text,
    area_name text,
    ingested_at timestamp,
    revision int,
    bucket timestamp,
    full_bytes bigint,
    active boolean,
    PRIMARY KEY ((scope, shard), occurred_at, hazard_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, hazard_id ASC);
