# Disk-backed satellite processing (F15)

Status on 19 November 2026: the production runner uses a private SQLite graph,
streams canonical revisions individually, and restores verified streaming
snapshots from PostgreSQL or Cassandra before replaying the committed tail.
F15 remains unchecked for dense/full-feed and assessed-topology measurements.

The working database uses one connection, an 8MiB main pager cache, a 4MiB
temp cache, on-disk temp tables, dirty-page spill and no mmap. The spatial/time
index and exact Haversine predicate preserve the reference graph semantics.
Traversal visits occupied 250m ECEF/three-hour cells. Every point pair inside
one such cell is within 434m and three hours, so a cell is an exact clique.
Between cells, adjacency still requires an actual point pair within 1km and
six hours. This avoids enumerating a dense cell's quadratic point pairs.
SQLite triggers maintain occupied cells through inserts, moves, confidence
changes and deletes; snapshots rebuild that derived index from point rows.
Traversal, affected sets, owners, published identities, deduplication and
latest records reside on disk. Counts update only affected components instead
of scanning the complete catalog per input. Minute-resolution satellite times
use Unix milliseconds rather than overflow-prone nanoseconds.

Input policy is shared with the in-memory reference engine. Capacity pressure
never shortens the declared correction horizon. Splits reserve untouched IDs;
corrections, retractions and canonical revisions retain deterministic lineage.
Only invalid external input is parked. Local SQL/disk or canonical failures end
the term and replay the authoritative journal; a failed checkpoint retries an
idempotent canonical save. Incoming event types cannot impersonate the trusted
already-archived no-op marker.

Committed-tail recovery reads at most eight journal entries per call. At the
default 2MiB frame ceiling, a 200-entry batch could otherwise allocate 400MiB
before graph/output work. A regression replays 100 committed inputs and checks
the bounded read contract. Legacy/current Cassandra reconciliation is bounded
by that same per-query limit.

Snapshots encode sorted SQL rows through gzip without rebuilding graph-sized
maps or byte buffers. Restore checks format, policy, sequence, ownership and
counts. See [m2-streaming-snapshots.md](m2-streaming-snapshots.md) for bounded
chunking, immutable publication, integrity, retention and cache upgrade rules.
Exactly one remote uploader may be pending per term. The runner first spools
consistent committed state to disk, then continues ingestion during upload;
busy uploaders do not accumulate a queue. Term cancellation joins the uploader
and removes its spool before deleting the private graph.

The Aggregator chart provides a dedicated 2Gi disk-backed emptyDir, separate
from the 128Mi archive spool, and sets SATELLITE_STATE_DIR / SQLITE_TMPDIR to
that volume. It retains the exact 256Mi/512Mi request/limit and gives Go a
384MiB soft memory limit. The derived volume is private and disposable; losing
it never discards canonical input. Native runs may set SATELLITE_STATE_DIR or
use OS temporary disk. Compose's M2 overlay uses writable container disk.

## Verification already implemented

- Every revision, counter and ID matches the corrected reference over several
  days of mixed input, coordinate/confidence changes, split/merge, historical
  ID collisions, time boundaries and capacity pressure.
- Cell traversal also matches an independent 400-point all-pairs oracle over
  dateline/polar coordinates, low confidence and times crossing Unix epoch.
  A 6,000-point single-cell traversal takes about 4ms; index lifecycle checks
  cover move/delete/confidence transitions. The full Aggregator suite passes
  after this change (M2 package 12.663s).
- Snapshots at multiple journal cut points resume identically and re-encode to
  identical bytes. Corrupt/truncated state, missing/duplicate rows, wrong
  policy/sequence and output failures are rejected.
- A blocked upload permits continued processing and no second spool. Cancelled
  uploads and failed restores leave no private files. SQL/local errors and
  canonical failures are distinct from poison input.
- Real PostgreSQL and Cassandra streaming snapshot tests cover 12MiB+123-byte
  state, competing writers, retention, partial uploads, corruption fallback and
  cleanup. Single-node fixtures ran serially with 1GiB/2CPU caps.
- The real PostgreSQL runner recovers both from zero and a replicated snapshot
  after canonical save but before checkpoint, preserving the final fire and
  exact revision count (1.473s). Cassandra's snapshot/tail crash recovery also
  passes (33.131s, observed 986.5MiB/1GiB, no OOM). Both fixtures were removed.
  The full untagged Aggregator suite passes (M2 package 9.994s). The full
  PostgreSQL storage suite passed all cases except its node-identity fixture;
  that case passed with the required integration-node startup setting.
- Helm tests verify disk/temp paths, writable group ownership, read-only root,
  exact memory budgets and absence of a memory-backed working volume.
- An opt-in Linux run with 25,000 independent detections, a 512MiB container
  cap and 384MiB Go soft limit passed snapshot/restore in 28.26s. Peak process
  RSS was 40,732KiB; SQLite used 27,029,504 bytes and the compressed snapshot
  1,400,949 bytes. A Windows bind mount failed SQLite truncation before input;
  the successful fixture used native Linux local disk, matching the working
  volume's intended filesystem.

## Remaining verification before checking F15

- The latest 200,000-point independent run passes snapshot/restore in 256.06s:
  41,096KiB peak process RSS, 286,191,616 bytes peak container charged memory,
  220,811,264-byte database and 11,103,418-byte compressed snapshot. A seeded
  200,000-point dense fire retracts one point in 8.30s with one correct revision,
  unchanged identity and 156,184KiB peak RSS. Charged file cache reaches the
  512MiB cap in that dense test, without OOM. Both are synthetic local tests;
  the dense test seeds its prefix and excludes remote canonical storage.
  Raw output, methodology and caveats are saved in
  [the profile evidence](evidence/m2-satellite-disk/README.md).
- Verify the largest required canonical record through real storage, outbox,
  publication and RPC under the intended pod budgets; the seeded local test
  does not exercise those downstream serialization/transport limits.
- Measure the entire available official/practice stream, largest component,
  peak RSS and snapshot overhead, plus quorum-storage p95 and 60x catch-up.
  Synthetic replay is useful regression evidence, not a substitute for these
  runtime measurements or the assistants' expected-result comparison.

The required detection-key list is still materialized for one canonical fire
record. Its transient cost is proportional to the largest fire and must be
measured. The working graph itself no longer lives in permanent Go maps.
