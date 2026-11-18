-- Journal and pending index share the canonical CAS's physical partition.
-- Reserved clustering keys !critical:<event_id> and !pending:<event_id> never
-- represent public hazards. No TTL: a broker/consumer loss cannot erase work.
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_event_id text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_hazard_id text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_source_ref text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_payload blob;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_sha256 text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_trace text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_created_at timestamp;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_receipt text;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_received_at timestamp;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_generation bigint static;
ALTER TABLE canonical_events ADD IF NOT EXISTS critical_event_generation bigint;
