# Satellite engine: bounded state and fast recovery (F15)

## Declared policy

| Setting | Default | Meaning |
|---|---|---|
| `SATELLITE_ALLOWED_LATENESS` | `24h` | A *new* detection older than `max_event_time - lateness` is late: parked with `late_beyond_allowed_lateness` and counted. |
| `SATELLITE_STATE_RETENTION` | `48h` | Correction horizon. A fire component whose newest detection is older than `max_event_time - retention` is **final** and leaves memory. Clamped to at least `lateness + 6h`. |
| `SATELLITE_MAX_DETECTIONS` | `200000` | **Soft** bound. Reaching it compacts early, at the smallest safe horizon (`lateness + 6h`); if the window is still that dense the input is accepted and `bnpb.satellite.over_capacity` increases. Processing never stops. |
| `SATELLITE_SNAPSHOT_EVERY` | `2000` | Committed inputs between durable engine snapshots (`0` disables). |

Why finalization cannot change results: graph edges need |Δt| ≤ 6 h, and every
accepted new detection is at most `lateness` older than the event-time
watermark. A component whose newest member is older than
`watermark - lateness - 6h` can never gain an edge again. Cutoffs advance in
one-hour steps derived only from the journal, so replay from sequence zero, or
from any snapshot, evicts identical state at identical inputs.

Corrections are accepted while their fire is in memory (within retention).
An update or retraction of an already finalized detection is **reported, not
dropped**: it is parked as `correction_beyond_state_retention` /
`retract_beyond_state_retention` and counted in
`bnpb.satellite.expired_corrections`. Before this change, a retraction of an
unknown key was silently ignored.

## Recovery

Each committed `SATELLITE_SNAPSHOT_EVERY` inputs, the leader stores the
engine's deterministic, gzip-compressed state at that journal sequence
(`m2_stream_snapshots` in PostgreSQL, chunked `m2_stream_snapshot_*` tables in
Cassandra, two retained, SHA-256 verified). A new term restores the newest
verified snapshot not past the committed checkpoint and replays only the
tail. A missing, corrupt or policy-incompatible snapshot falls back to a full
replay, which yields the same state. Snapshots are a pure function of the
committed journal prefix, so they need no write fencing.
`bnpb.satellite.recovery.duration` and the `satellite state recovered` log
record report the replay length and time.

Fires are compared as canonical JSON, so a restored record never causes a
spurious revision.

## Evidence (offline, synthetic)

`go test ./internal/m2` covers: 30-day feed with bounded state; restore at
every 1/37/250 inputs byte-identical to an uninterrupted run (all revisions
and counters); expired retraction reported; soft bound never stops
processing. Both PostgreSQL and Cassandra snapshot stores pass the storage
contract (`Snapshots/LatestAndRetention`, 700 KiB multi-chunk state).

Dense synthetic feed: 601,438 inputs over 60 days, 400 hotspots, 8 % updates,
2 % retractions, hourly heartbeats, replayed with `cmd/m2replay` under
`GOMEMLIMIT=480MiB`, 2 CPUs:

| Engine | Capacity rejections | Peak active detections | Max RSS | Time |
|---|---|---|---|---|
| Before (HEAD `fa0ab7b`) | 334,979 (`satellite capacity reached`; the live runner stops at the first) | 200,000 (cap) | 487 MiB | 22.5 s |
| After | 0 | 23,962 | 336 MiB | 55.5 s (p95 0.18 ms/input) |

The old engine is faster only because it rejected 56 % of the inputs at the
cap without processing them.

This is offline CPU replay of a synthetic feed: it is **not** the official
full feed, quorum-storage ingest-to-storage p95 or the 60x catch-up gate,
which remain to be measured on the assessed topology.
