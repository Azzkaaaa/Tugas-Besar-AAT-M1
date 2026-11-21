# Whole-catalog hazard analytics

Open the frontend Dashboard. The **Analitik seluruh kejadian** panel queries
`GET /api/v1/analytics` through the authenticated gateway and Aggregator gRPC.
It is independent of the eight-row event list and the operational metrics
dashboard. Every viewer needs `hazards:read:summary`; no raw attributes,
coordinates or source references are returned.

Choose 7 days, 30 days, one year or all time, and hourly/daily/monthly/yearly
UTC buckets. The five lines are earthquakes (`SEISMIC`), predicted tsunami
warnings (`TSUNAMI`), volcanoes (`VOLCANIC`), wildfires (`WILDFIRE`) and coastal
observations (`COASTAL`). A seismic hazard with a nonempty `tsunami_warnings`
array counts once in TSUNAMI, irrespective of the number of warning areas.
It also counts once in SEISMIC. Thus category totals overlap; `total`, severity
and area totals count each non-withdrawn hazard once. This is the same rule to
apply when recounting Audit records. Missing time buckets represent zero;
the chart uses real time coordinates and retains distant historical events.

The API accepts optional `source`, inclusive `since`, exclusive `until`
(both over `occurred_at`), and `interval=hour|day|month|year` (default day).
Omit dates for the entire catalog. Responses contain `total`, `categories`,
`severity`, `areas`, `series`, `as_of`, `partial`, and `unavailable_units`.
The browser shows an explicit warning whenever the result is incomplete.
All returned areas remain searchable/pageable locally; the view is not a
silently truncated top-N chart.

Cassandra stores small current-metadata rows in 16 hash shards, maintained in
the same projection batch as Audit. Revision timestamps prevent stale replay
or upgrade backfill from overriding newer withdrawals. Migration 0010 and the
versioned background Audit backfill install this index. Reads remain partial
until backfill completes. PostgreSQL streams only the needed metadata fields.
Neither backend loads the full payload catalog into application memory.
The Aggregator scans at most eight Cassandra pages concurrently (128 rows per
page, 500 ms each) and admits one analytics scan per replica. Busy scans are
explicitly partial. Requests have a 10-second Aggregator budget within the
gateway's 12 seconds. At most 10,000 area keys and 4,096 nonempty time buckets
are retained; capacity overflow is explicit, and coarser intervals reduce bucket
cardinality. Ingestion and ordinary hazard-read budgets stay independent.

These are current projection reads, without an atomic cross-shard snapshot of
concurrent writes. Reconciliation should use a quiescent catalog or repeat after
ingestion/projection recovery. During an upgrade from a version that lacks the
metadata projection, complete the writer rollout before relying on the backfill
marker; rerun backfill if an older writer remained active. Multi-node fault and
large-catalog latency acceptance remain in the M2 completion checklist.

Verification: all untagged Aggregator/Gateway tests, the real HTTP-to-gRPC
OpenAPI contract test, and 37 frontend tests pass. Real PostgreSQL and Cassandra
integration compare totals, five categories, severity, area and time series
with Audit fixtures, including 1,050 Cassandra hazards and a withdrawal.
Databases ran serially with a 1 GiB limit; the Cassandra test passed in 37.10s
without OOM. Browser regression verifies whole-catalog counts, range/interval
controls and responsive layout. These local fixtures do not replace the
specification's separate-kernel fault/load evidence.
