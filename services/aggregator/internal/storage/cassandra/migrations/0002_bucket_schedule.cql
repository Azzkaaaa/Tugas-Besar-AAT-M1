-- The bucket period can change over time (see schedule.go). Each segment applies from
-- effective_from on; segments are only appended, starting at a future boundary.
CREATE TABLE IF NOT EXISTS bucket_schedule (
    scope text,
    effective_from timestamp,
    period_seconds int,
    PRIMARY KEY ((scope), effective_from)
) WITH CLUSTERING ORDER BY (effective_from ASC);

-- The bucket a hazard's List row was written to, so a revision that moves it deletes the right
-- row whatever the schedule. Null on rows written before this column (their bucket is computed).
ALTER TABLE hazards_by_id ADD IF NOT EXISTS bucket timestamp;
