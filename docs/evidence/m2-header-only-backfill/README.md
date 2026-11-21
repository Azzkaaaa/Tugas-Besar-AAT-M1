# Ordered List backfill and source-freshness fix

These are local regression results, not deployed problem-set acceptance.

The first live ordered List backfill on snapshot 10 failed after 123.089 seconds
with a Cassandra response timeout. Its implementation scanned complete canonical
payloads one at a time. The correction copies canonical header metadata in pages
of 64 with four bounded workers. Legacy rows missing current summary metadata use
the existing shared single-body admission slot. Revision ordering, inactive old
positions and the explicit post-writer-upgrade readiness marker are preserved.

Independent source-freshness probes now run concurrently, in stable response
order, with a shared caller deadline and a 500ms per-source budget. The previous
100ms budget returned UNAVAILABLE even for recently ingested WAN-backed sources.
This does not change the 1.5s public read deadline or storage consistency.

- `integration.txt`: Cassandra 5.0.9 regression passed in 56.03 seconds, using a
  disposable RF=1 fixture limited to 1GiB. Covers explicit cutover, legacy NULL
  summary metadata, empty historical buckets, summary/full pagination, moves,
  withdrawals, delayed older writes and repeat preparation.
- `unit.txt`: focused gRPC, Cassandra and storeadmin package checks passed.
  The freshness test verifies all source probes overlap and permits two simulated
  70ms storage reads per source without reporting healthy sources unavailable.
- The fixture was inspected (task label `m2-gap-fixes`, OOM=false), then removed
  with its disposable anonymous volume. Application data was untouched.

The corrected image has not been built or deployed. Retry the live backfill only
after deploying the correction and verifying every writer; retain the first
failed attempt as evidence. Public pagination/load and runtime freshness remain
open until measured on the intended four-node cluster.
