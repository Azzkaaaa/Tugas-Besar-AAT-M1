# Full-journal replay using production disk state

Run from services/aggregator:

```text
go run ./cmd/m2replay -input captured-journal.jsonl -output replay-report.json -work-dir /local-disk/replay
```

The input is the captured storage.StreamEntry JSONL journal, starting at sequence
1 without gaps; payload is the JSON encoding of Go []byte (base64). Policy flags
remain -lateness, -retention and -max-detections. The last is a soft pressure
threshold, not permission to discard detections. -work-dir is a parent directory
on a disk with room for working graph, final snapshots and per-input statistics.
The tool creates/removes only its private temporary graph directory there.

The command now runs the same diskFireProcessor as the production runner. Exact
graph/correction state, historical final results, duration samples and distinct
rejection reasons stay in indexed SQLite tables. Exact p95 uses a duration-index
rank query rather than a growing array. Rejection reasons and final hazards are
streamed into one JSON report, one row/record at a time. Memory is bounded by SQL
caches, the input line and the largest complete output record; FULL detection
keys are preserved. A bad journal sequence or working-store failure aborts;
rejected source input is counted separately and processing continues. A file
output is first written to a temporary sibling and renamed only after success.

The report keeps items/parked_reasons and existing counter/p95 fields, adds
engine=production-disk and parked_events, and writes a scalar summary to stderr.
Its elapsed/p95 values cover the offline disk processor. They do not prove
ingest-to-canonical-storage, upstream catch-up or receiver notification latency.

Verification on 19 November: the full untagged Aggregator suite passes. The new
replay test compares every final record and exact rejection count against the
independent in-memory engine across a correction/retention feed, verifies counters
and temporary-state cleanup, and rejects a missing journal sequence. A real CLI
fixture has three input rows, one two-detection fire, one parked invalid row,
complete JSON and engine=production-disk. This closes F18-a's replay implementation;
official/reference and intended-cluster acceptance still need their actual runs.
