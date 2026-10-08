-- 002_outbox_retry.sql
ALTER TABLE outbox_events
  ADD COLUMN IF NOT EXISTS retry_count  INTEGER      NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS failed_at    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_error   TEXT;

-- Fix: last_success_at must be nullable (failure path inserts NULL)
ALTER TABLE ingest_watermarks
  ALTER COLUMN last_success_at DROP NOT NULL;

-- DLQ dead letters table for max-retried outbox records
CREATE TABLE IF NOT EXISTS outbox_dead_letters (
  event_id    TEXT PRIMARY KEY,
  hazard_id   TEXT NOT NULL,
  source      TEXT NOT NULL,
  payload     BYTEA NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL,
  failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error  TEXT
);
