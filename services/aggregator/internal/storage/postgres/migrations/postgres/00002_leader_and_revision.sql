-- +goose Up
-- Leader election lease (see storage/postgres/lease.go). Expiry is measured on the server clock.
CREATE TABLE IF NOT EXISTS leader_leases (
    name TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

-- Committed revision of each hazard: 1 on insert, +1 per content change. Event IDs derive from
-- (hazard_id, revision), so the same revision always publishes the same event_id. Existing rows
-- start at 1.
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS revision INTEGER NOT NULL DEFAULT 1;

-- +goose Down
ALTER TABLE hazard_events DROP COLUMN IF EXISTS revision;
DROP TABLE IF EXISTS leader_leases;
