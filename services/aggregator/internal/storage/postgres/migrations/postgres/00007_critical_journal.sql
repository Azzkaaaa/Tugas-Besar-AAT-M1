-- +goose Up
CREATE TABLE critical_journal_clock (id integer PRIMARY KEY CHECK(id=1),version bigint NOT NULL);
INSERT INTO critical_journal_clock VALUES (1,0);
CREATE TABLE critical_event_journal (
    event_id text PRIMARY KEY,
    hazard_id text NOT NULL,
    source_ref_id text NOT NULL,
    payload bytea NOT NULL,
    payload_sha256 text NOT NULL,
    trace_context jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    receipt jsonb,
    received_at timestamptz
    ,generation bigint NOT NULL UNIQUE
);
CREATE INDEX critical_pending ON critical_event_journal (event_id) WHERE receipt IS NULL;
CREATE TRIGGER critical_append_authority BEFORE INSERT ON critical_event_journal
    FOR EACH ROW EXECUTE FUNCTION enforce_leader_authority();
-- +goose Down
-- Retain immutable events and receiver evidence during application rollback.
