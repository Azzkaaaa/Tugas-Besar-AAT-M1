# Required structured logs and correlation

The notification host now explicitly emits JSON console logs with UTC timestamps,
included scopes and Activity TraceId/SpanId/ParentId. Its existing OpenTelemetry
logging/tracing/metrics exporters remain enabled by the shared OTLP configuration.
This makes kubectl logs useful when the collector is unavailable.

The Rust feed already emitted JSON and exported OTLP traces, with a trace_id on
AMQP consume spans. Its gRPC subscription and feed delivery spans now also record
trace_id; subscriptions include the incoming x-correlation-id. Existing AMQP and
persisted-carrier propagation remains intact.

Frontend nginx already loads ngx_otel_module and both assessed environments
enable OTLP tracing. Its request access log is now JSON with trace_id, span_id,
correlation_id, method, path, status, duration and bytes. It excludes query strings
and credentials. The server-level access_log overrides the inherited plain access
log. The Helm frontend configuration uses the same format. The OTel variables
are documented by [the nginx module](https://nginx.org/en/docs/ngx_otel_module.html).

Verification on 19 November 2026:

- .NET 10 notification host builds with zero errors/warnings.
- Rust feed regression: 36 pass, zero failures; two opt-in Redis/memory tests are
  ignored in the ordinary suite. Tests compile offline, serially, with a 1536MiB
  compiler container cap; this is not a feed runtime memory measurement.
- Actual nginx -t and requests pass under 256MiB. The services configuration and
  actual rendered Helm ConfigMap both preserve the supplied trace ID and emit
  valid JSON with the correlation header. The chart fixture uses UID/GID 101,
  read-only root and only the chart's writable runtime directories.
- Native logs and the chart's memory counters are saved in
  docs/evidence/m2-observability/. All test containers were removed; build cache
  remains. Exporter source/configuration is present; these checks do not prove
  deployed collection or the complete ingest-to-receiver trace. Those are F11-b.
