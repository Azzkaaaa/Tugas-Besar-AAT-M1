-- Durable epochs outlive lease expiry/release. Expiry uses storage time.
ALTER TABLE leader_leases ADD IF NOT EXISTS epoch bigint;
ALTER TABLE leader_leases ADD IF NOT EXISTS expires_at timestamp;

-- Authority and canonical CAS share a physical partition, including an unseen ID.
CREATE TABLE IF NOT EXISTS canonical_events (
    shard int, hazard_id text,
    authority_epochs map<text,bigint> static,
    authority_tokens map<text,text> static,
    authority_until map<text,timestamp> static,
    fence_guard text static,
    source text, source_ref_id text, hazard_type text, severity text, area_name text,
    latitude double, longitude double, occurred_at timestamp, ingested_at timestamp,
    attributes text, revision int, bucket timestamp, content_hash text,
    prev_bucket timestamp, prev_occurred_at timestamp, ingest_sequence bigint,
    PRIMARY KEY ((shard), hazard_id)
);

ALTER TABLE ingest_inbox ADD IF NOT EXISTS authority_epochs map<text,bigint> static;
ALTER TABLE ingest_inbox ADD IF NOT EXISTS authority_tokens map<text,text> static;
ALTER TABLE ingest_inbox ADD IF NOT EXISTS authority_until map<text,timestamp> static;
ALTER TABLE ingest_inbox ADD IF NOT EXISTS fence_guard text static;

-- Fixed authority partitions cover future journal blocks and checkpoints.
CREATE TABLE IF NOT EXISTS stream_inputs_by_shard (
    shard int, family text, stream text, block bigint, sequence bigint, message_id text,
    authority_epochs map<text,bigint> static,
    authority_tokens map<text,text> static,
    authority_until map<text,timestamp> static,
    fence_guard text static,
    kind text, cursor text, payload blob, received_at timestamp,
    checkpoint_sequence bigint, reason text,
    PRIMARY KEY ((shard), family, stream, block, sequence, message_id)
);

ALTER TABLE source_state ADD IF NOT EXISTS authority_epochs map<text,bigint>;
ALTER TABLE source_state ADD IF NOT EXISTS authority_tokens map<text,text>;
ALTER TABLE source_state ADD IF NOT EXISTS authority_until map<text,timestamp>;
ALTER TABLE source_state ADD IF NOT EXISTS fence_guard text;
