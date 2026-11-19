-- Cached only for the matching projection revision. Older rolling writers
-- leave a mismatched cache which readers safely resolve one body at a time.
ALTER TABLE hazards_by_bucket_shard ADD IF NOT EXISTS list_active boolean;
ALTER TABLE hazards_by_bucket_shard ADD IF NOT EXISTS list_revision int;
ALTER TABLE hazards_by_bucket_shard ADD IF NOT EXISTS list_wire_bytes bigint;
