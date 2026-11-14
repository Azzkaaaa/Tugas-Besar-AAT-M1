-- Hazard IDs are derived from (source, source_ref_id) (domain.HazardID), so hazards_by_id is
-- addressed directly and becomes the commit point of revisions, written only with lightweight
-- transactions. It now also holds the content hash of the committed revision and where the
-- previous revision's List row is, which writing the revision out removes when the record moved.
-- Migration 0005 (Go) re-keys existing hazards; 0006 drops hazard_by_source_ref.
ALTER TABLE hazards_by_id ADD IF NOT EXISTS content_hash text;
ALTER TABLE hazards_by_id ADD IF NOT EXISTS prev_bucket timestamp;
ALTER TABLE hazards_by_id ADD IF NOT EXISTS prev_occurred_at timestamp;
