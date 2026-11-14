-- No longer needed: hazard IDs are derived from (source, source_ref_id), see 0004/0005.
-- One-way: builds from before derived IDs cannot run on this keyspace afterwards.
DROP TABLE IF EXISTS hazard_by_source_ref;
