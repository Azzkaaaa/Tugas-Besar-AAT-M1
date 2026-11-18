-- +goose Up
CREATE INDEX IF NOT EXISTS hazards_audit_ingestion ON hazard_events (ingested_at, hazard_id);
CREATE INDEX IF NOT EXISTS hazards_audit_source_ingestion ON hazard_events (source, ingested_at, hazard_id);
CREATE INDEX IF NOT EXISTS hazards_audit_refs ON hazard_events (source_ref_id COLLATE "C", hazard_id);
CREATE INDEX IF NOT EXISTS hazards_audit_source_refs ON hazard_events (source, source_ref_id COLLATE "C", hazard_id);

-- +goose Down
DROP INDEX IF EXISTS hazards_audit_source_refs;
DROP INDEX IF EXISTS hazards_audit_refs;
DROP INDEX IF EXISTS hazards_audit_source_ingestion;
DROP INDEX IF EXISTS hazards_audit_ingestion;
