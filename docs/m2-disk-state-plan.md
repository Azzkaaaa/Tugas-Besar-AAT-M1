# Remaining disk-backed satellite processor work (F15)

Status on 19 November 2026: the working-graph primitives are implemented and
tested in `internal/m2/working_graph.go`. They are **not yet the production
runner**. F15 remains unchecked until the processor, snapshots and recovery are
wired and verified. This document records the concrete next implementation
steps; it does not replace the full milestone objective.

The primitive uses a private derived SQLite file, one connection, an 8MiB main
pager cache, a 4MiB temp cache, on-disk temp tables, dirty-page spill and no mmap.
Points and spatial/time coordinates are indexed on disk. A recursive UNION
builds exact connected membership in a disk-backed temporary B-tree, using the
same Haversine edge function as the reference engine. Tests cover membership,
bridge removal, low confidence, the dateline, the exact six-hour boundary,
private-file cleanup and a 5,000-point database larger than the pager cache.
Minute-resolution satellite times use Unix milliseconds rather than the
overflow-prone nanosecond range.

## Processor integration to finish

1. Implement the disk processor's input transaction. Parse and validate through
   the existing helpers, read a prior detection for update patches, preserve
   lexical keys/dedup, lateness and the configured correction horizon. Store
   `seen`, `owners`, `components`, `published` and `latest` in the supplied SQL
   tables; do not reload the whole graph or keep all component keys in Go maps.
2. Before changing a point, copy the touched component keys and prior owners to
   the temporary `affected`/`old_owners` tables. A pure insertion merges touched
   components directly. A removal/update can split them: compute exact
   connectivity through `buildGraphComponent`, selecting roots in byte order.
   Preserve the reference engine's published-ID priority and reserve identifiers
   belonging to untouched components (the split collision fixed in `9fab57a`).
3. Aggregate each changed component in key order so floating sums and canonical
   output match the reference engine. Emit a withdrawn revision for an old fire
   that shrinks/disappears. Stream changed components to the runner rather than
   retaining all results. The required detection-key list remains part of each
   canonical record; measure its transient cost for the largest real component.
4. Finalize components only at the declared retention cutoff. Remove the same
   point/seen/latest metadata deterministically as the reference engine and keep
   expired corrections explicit. Capacity pressure must never shorten retention.
5. Add streaming snapshot encoding/restoration that iterates SQL rows. Do not
   recreate a full `fireState` map for serialization. Existing snapshot backends
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
