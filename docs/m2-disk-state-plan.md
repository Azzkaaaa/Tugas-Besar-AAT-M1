# Remaining disk-backed satellite processor work (F15)

Status on 19 November 2026: the working graph, transactional disk processor and
streaming logical snapshot codec are implemented and tested in
`internal/m2/working_graph.go`, `disk_fire.go` and `disk_state.go`. They are
**not yet wired to the production runner or replicated streaming snapshot
backends**. F15 remains unchecked until that integration and recovery are
verified. This document records the concrete continuation steps; it does not
replace the full milestone objective.

The primitive uses a private derived SQLite file, one connection, an 8MiB main
pager cache, a 4MiB temp cache, on-disk temp tables, dirty-page spill and no mmap.
Points and spatial/time coordinates are indexed on disk. A recursive UNION
builds exact connected membership in a disk-backed temporary B-tree, using the
same Haversine edge function as the reference engine. Tests cover membership,
bridge removal, low confidence, the dateline, the exact six-hour boundary,
private-file cleanup and a 5,000-point database larger than the pager cache.
Minute-resolution satellite times use Unix milliseconds rather than the
overflow-prone nanosecond range.

## Implemented processor and codec

The disk processor shares validation and correction policy with the reference
engine, stores graph membership/identity/deduplication on disk, emits changed
canonical records individually, and rolls back derived state on canonical write
failure. Operational state errors are distinct from invalid external input.
Snapshots stream sorted logical SQL rows through gzip without a graph-sized
map or byte buffer; restore checks format, policy, sequence, ownership and counts.

Regression tests compare every revision and counter across several days of
mixed input, coordinate/confidence changes, splits and historical-ID collisions.
Snapshots at multiple cut points resume identically and re-encode to identical
bytes. Corrupt, truncated, missing/duplicated rows, wrong policy/sequence and
output failures are rejected. Archived no-op journal entries preserve sequencing.
The full untagged Aggregator suite passes (M2 package 9.981s).

## Processor integration to finish

1. Completed: implement the disk processor's input transaction. Parse and validate through
   the existing helpers, read a prior detection for update patches, preserve
   lexical keys/dedup, lateness and the configured correction horizon. Store
   `seen`, `owners`, `components`, `published` and `latest` in the supplied SQL
   tables; do not reload the whole graph or keep all component keys in Go maps.
2. Completed: before changing a point, copy the touched component keys and prior owners to
   the temporary `affected`/`old_owners` tables. A pure insertion merges touched
   components directly. A removal/update can split them: compute exact
   connectivity through `buildGraphComponent`, selecting roots in byte order.
   Preserve the reference engine's published-ID priority and reserve identifiers
   belonging to untouched components (the split collision fixed in `9fab57a`).
3. Completed: aggregate each changed component in key order so floating sums and canonical
   output match the reference engine. Emit a withdrawn revision for an old fire
   that shrinks/disappears. Stream changed components to the runner rather than
   retaining all results. The required detection-key list remains part of each
   canonical record; measure its transient cost for the largest real component.
4. Completed: finalize components only at the declared retention cutoff. Remove the same
   point/seen/latest metadata deterministically as the reference engine and keep
   expired corrections explicit. Capacity pressure must never shorten retention.
5. Streaming SQL encoding/restoration is complete. Existing snapshot backends
   currently accept/return `[]byte`; extend them with a bounded streaming path
   for large state rather than moving the whole graph to a serialization buffer.
   Version the snapshot semantics and fall back safely on corrupt/incompatible
   state. The replicated journal/checkpoint remains the recovery authority.
6. Wire the disk processor into `Runner.satellite`, with a private working
   directory and cleanup per leadership term. Local disk/SQL errors must end the
   term and retry from canonical journal state, **not** be parked as invalid
   input or advance the checkpoint. A partial canonical save is replayed with
   deterministic revisions/stream sequence, as before.
7. Add a disk-backed emptyDir and explicit state/temp directory to the Aggregator
   chart; retain its exact 256Mi/512Mi memory budget. SQLite temp sort/CTE files
   must use that disk, not the existing small archive spool. Keep cache files
   derived and non-authoritative; no other service gets access to canonical
   storage or this working state.

## Verification before checking F15

- Compare every emitted revision/counter/ID against the corrected reference
  engine over mixed detection/update/retract, split/merge, confidence and time
  boundary cases, including a dense component and many independent components.
- Restore at multiple journal cut points and compare uninterrupted, crashed and
  reconnected runs. Test a failed local write and canonical write/checkpoint
  failure; acknowledged/committed inputs must not disappear or be duplicated.
- Verify real PostgreSQL and Cassandra streaming snapshot integrity/retention
  with state larger than the small in-memory cache. Run one fixture at a time.
- Measure full official/practice-feed state, largest component, peak RSS and
  snapshot overhead within 512Mi, plus quorum-storage p95/catch-up. Synthetic
  replay is useful regression evidence, not a substitute for those runtime
  measurements or the assistants' expected-result comparison.

The production in-memory engine already preserves the correction horizon
(`c886ef3`), avoids retained quadratic edges and protects split identities
(`9fab57a`). Keep these invariants while completing the disk processor.
