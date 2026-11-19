# Large satellite records: storage, outbox and RPC

The required detection-key list can make a single fire larger than ordinary
hazards. Synthetic tests on 19 November exposed three independent failures:

- A 2,000-key fire committed canonically, then failed its cross-partition
  projection/outbox batch with `Batch too large` (default threshold 50KiB).
- A 110,000-key full RPC response failed the deployed gateway's default 4MiB
  receive limit (5,280,088-byte response).
- A 200,000-key canonical mutation failed Cassandra's derived CQL message
  ceiling (9,400,508-byte request; 6,710,886-byte allowed maximum).

## Implementation

Small derived writes still share a logged batch. Larger ones execute in order
as single-partition mutations. Both durable outbox copies precede the List
revision used as the completion marker. An interruption leaves a committed
canonical revision whose derived effects are repaired on retry, at the same
revision/event ID. Authoritative conditional/fencing batches remain atomic and
do not use the splitting helper.

Outbox finish writes a dead letter first when needed, then the terminal event
marker, then removes pending work. Failure cannot delete pending work before
the durable terminal result. Real tests remove/recreate a table only inside a
disposable UUID-named test keyspace to prove this ordering.

The gateway's real backend connection now has a finite 16MiB receive budget.
Tests verify a complete 110,000-key response and rejection beyond that budget.
No detection keys are truncated or replaced by a summary.

The sibling infrastructure chart sets CQL message/per-IP/global request budgets
to 16MiB/16MiB/32MiB. The 256MiB heap and exact 512Mi/1Gi pod request/limit
remain unchanged. Cassandra 5.0.9 derives its defaults from the heap and checks
that the message allowance fits both in-flight budgets; see the
[pinned upstream configuration implementation](https://github.com/apache/cassandra/blob/cassandra-5.0.9/src/java/org/apache/cassandra/config/DatabaseDescriptor.java).
These finite settings admit the measured large record without enlarging the
cross-partition batch threshold. The rendered chart preparation was applied to
the real fixture, including a successful server restart, before the rerun.

Cassandra uses an OnDelete update strategy. After promoting the configuration,
recreate one Cassandra pod at a time so its init preparation runs again;
verify readiness/ring/repair before advancing. A container-only restart in the
same pod does not rerun its init container. This is an operator procedure for
the intended cluster; no cluster rollout was performed here.

## Evidence and limits

The 200,000-key real Cassandra lifecycle passed with a 512MiB client cap,
384MiB Go soft limit, two CPUs and a separate 1GiB/2CPU Cassandra 5.0.9 fixture.
It covers canonical insert/update/read, full protobuf outbox claim,
acknowledgment, retry/release, dead-letter outage/recovery and exact key counts.
The application portion took 10.67s (46.76s including schema setup). Peak client
RSS was 285,052KiB and peak charged container memory 301,047,808 bytes; no OOM.
The independent interrupted-outbox test also repairs the original revision.

Normal revision/outbox contracts passed across one-minute and 24-hour bucket
layouts (141.372s; expected single-consumer skips). Full untagged Aggregator and
Gateway suites pass. The infrastructure suite passes 121 tests with one
SOPS/age tool-availability skip.

Raw before/after logs and structured results are in
[evidence/m2-large-fire](evidence/m2-large-fire/report.json). Tests use synthetic
data and a single-node RF=1 database. They do not prove RF=3 quorum latency,
full-feed 60x catch-up, a combined graph/storage/RPC/broker memory envelope or
official reference equivalence. F15 and intended-topology gates remain open.

The subsequent [outbox payload-budget work](m2-outbox-payload-budget.md) replaces
bulk body scans with metadata selection and bounded hydration on both stores.
Its backlog fixture also exposed Cassandra heap/direct-buffer failures that a
single large mutation did not reveal; the chart's internal buffer/GC profile
was revised within the same pod limit. Bulk List reads and the combined path
still need verification under the actual pod budgets before closing F15.
