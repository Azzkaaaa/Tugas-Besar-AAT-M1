CREATE TABLE IF NOT EXISTS field_reports (
    shard int,
    report_id text,
    actor text,
    observed_at text,
    area_name text,
    description text,
    received_at timestamp,
    correlation_id text,
    processed boolean,
    processed_at timestamp,
    processed_area_name text,
    processed_description text,
    PRIMARY KEY (shard, report_id)
);

CREATE TABLE IF NOT EXISTS ingest_inbox (
    shard int,
    message_id text,
    source text,
    stream text,
    source_ref_id text,
    received_at timestamp,
    schema_version text,
    intake_sequence bigint,
    correlation_id text,
    format text,
    state text,
    reason text,
    attempts int,
    next_attempt timestamp,
    raw blob,
    companion blob,
    archive_id uuid,
    archive_chunks int,
    PRIMARY KEY (shard, message_id)
);

ALTER TABLE hazards_by_id ADD IF NOT EXISTS ingest_sequence bigint;

CREATE TABLE IF NOT EXISTS ingest_archive_chunks (
    archive_id uuid,
    chunk int,
    raw blob,
    PRIMARY KEY (archive_id, chunk)
);
