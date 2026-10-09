-- 003_outbox_trace_context.sql
-- W3C trace context (traceparent, tracestate, baggage) of the ingest poll that enqueued the
-- event. The outbox publisher continues that trace when it publishes to RabbitMQ.
ALTER TABLE outbox_events
  ADD COLUMN IF NOT EXISTS trace_context JSONB;

ALTER TABLE outbox_dead_letters
  ADD COLUMN IF NOT EXISTS trace_context JSONB;
