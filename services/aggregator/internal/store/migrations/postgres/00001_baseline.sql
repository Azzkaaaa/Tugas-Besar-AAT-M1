-- +goose Up
-- Baseline: the schema the aggregator built inline before it used goose. Every statement is
-- idempotent, so this applies to a fresh database and is a no-op (apart from recording version 1)
-- on databases created by the inline schema. Later migrations are ordinary goose migrations and
-- follow expand/contract: an older build must keep working on a newer schema.
CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id VARCHAR(128) PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    source_ref_id VARCHAR(128) NOT NULL,
    hazard_type VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    area_name VARCHAR(255) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
);

CREATE INDEX IF NOT EXISTS idx_hazard_pagination ON hazard_events (occurred_at DESC, hazard_id ASC);
CREATE INDEX IF NOT EXISTS idx_hazard_source ON hazard_events (source);
CREATE INDEX IF NOT EXISTS idx_hazard_type ON hazard_events (hazard_type);
CREATE INDEX IF NOT EXISTS idx_hazard_severity ON hazard_events (severity);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id VARCHAR(128) PRIMARY KEY,
    hazard_id VARCHAR(128) NOT NULL,
    source VARCHAR(32) NOT NULL,
    payload BYTEA NOT NULL,
    published_at TIMESTAMPTZ NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    failed_at TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ NULL;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error TEXT NULL;

-- W3C trace context of the ingest poll that enqueued the event (see EnqueueOutbox).
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS trace_context JSONB NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (created_at ASC) WHERE published_at IS NULL AND failed_at IS NULL;

-- When a record is next due: its claim lease while a publisher holds it, its backoff after a
-- failed attempt. Defaults to NOW() so rows from older releases are due at once.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox_events (next_attempt_at, created_at) WHERE published_at IS NULL AND failed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox_events (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_failed ON outbox_events (failed_at) WHERE failed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    event_id VARCHAR(128) PRIMARY KEY,
    hazard_id VARCHAR(128) NOT NULL,
    source VARCHAR(32) NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT
);

ALTER TABLE outbox_dead_letters ADD COLUMN IF NOT EXISTS trace_context JSONB NULL;

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    source_key VARCHAR(64) PRIMARY KEY,
    watermark_time TIMESTAMPTZ NOT NULL,
    last_success_at TIMESTAMPTZ NULL,
    last_attempt_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'UNAVAILABLE',
    error_message TEXT NULL
);

ALTER TABLE ingest_watermarks ALTER COLUMN last_success_at DROP NOT NULL;
