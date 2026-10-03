-- 001_init.sql: create tables for aggregator service
CREATE TABLE IF NOT EXISTS volcano_references (
    volcano_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id      TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    source_ref_id  TEXT NOT NULL,
    hazard_type    TEXT NOT NULL,
    severity       TEXT NOT NULL,
    area_name      TEXT NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL,
    attributes     JSONB NOT NULL,
    UNIQUE (source, source_ref_id)
);

CREATE INDEX IF NOT EXISTS idx_hazard_events_occured_desc_id_asc ON hazard_events (occurred_at DESC, hazard_id ASC);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id      TEXT PRIMARY KEY,
    hazard_id     TEXT NOT NULL REFERENCES hazard_events (hazard_id) ON DELETE CASCADE,
    source        TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    source_key          TEXT PRIMARY KEY,
    watermark_time      TIMESTAMPTZ NOT NULL,
    last_success_at     TIMESTAMPTZ NOT NULL,
    last_attempt_at     TIMESTAMPTZ NOT NULL,
    status              TEXT NOT NULL, -- FRESH, STALE, UNAVAILABLE
    error_message       TEXT
);

-- static volcano data
INSERT INTO volcano_references (volcano_id, name, latitude, longitude) VALUES
('V001', 'Merapi', -7.5407, 110.4460),
('V002', 'Sinabung', 2.1036, 98.2996),
('V003', 'Krakatau', -6.1020, 105.4230),
('V004', 'Semeru', -8.1080, 112.9220),
('V005', 'Kelud', -7.9289, 112.9233)
ON CONFLICT (volcano_id) DO NOTHING;
