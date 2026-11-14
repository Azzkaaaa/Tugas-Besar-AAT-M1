-- Leader election lease (storage/cassandra/lease.go): one row per lease, expired by its TTL.
CREATE TABLE IF NOT EXISTS leader_leases (
    name text PRIMARY KEY,
    holder text
);

-- Committed revision of each hazard (1 on insert, +1 per content change). Event IDs derive from
-- (hazard_id, revision). The identity row holds the committed revision and its content hash and
-- is only written with lightweight transactions; the other tables carry a copy. Null on rows from
-- before this migration: ApplyRevision backfills them on first touch.
ALTER TABLE hazard_by_source_ref ADD IF NOT EXISTS revision int;
ALTER TABLE hazard_by_source_ref ADD IF NOT EXISTS content_hash text;
ALTER TABLE hazards_by_id ADD IF NOT EXISTS revision int;
ALTER TABLE hazards_by_bucket ADD IF NOT EXISTS revision int;
