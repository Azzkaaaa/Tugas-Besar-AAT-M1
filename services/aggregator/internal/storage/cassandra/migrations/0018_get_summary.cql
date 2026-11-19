-- Cache activity at the same revision as the canonical mutation. Old writers
-- invalidate this cache by changing revision; old rows use a bounded fallback.
ALTER TABLE canonical_events ADD IF NOT EXISTS summary_active boolean;
ALTER TABLE canonical_events ADD IF NOT EXISTS summary_revision int;
ALTER TABLE hazards_by_id ADD IF NOT EXISTS summary_active boolean;
ALTER TABLE hazards_by_id ADD IF NOT EXISTS summary_revision int;
