-- Current metadata by stable hazard identity; revision timestamps make replay,
-- withdrawal and concurrent upgrade backfill idempotent.
CREATE TABLE IF NOT EXISTS analytics_by_shard (
    scope text, shard int, hazard_id text,
    occurred_at timestamp, hazard_type text, severity text, area_name text,
    tsunami boolean, active boolean, revision bigint,
    PRIMARY KEY ((scope, shard), hazard_id)
);
