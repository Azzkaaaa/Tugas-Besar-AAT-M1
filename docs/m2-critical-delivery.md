# Critical warning delivery

## Required routing

The Aggregator now sends a mandatory, confirmed direct copy to
`AGGREGATOR_REQUIRED_NOTIFICATION_QUEUE` before the ordinary fanout publication.
M2 defaults this to `bnpb.danger-notification.v1`; Compose and Helm provide the
same default. When changing the notification consumer's queue name, change the
Aggregator's required queue too. The consumer still declares its own queue.

A missing queue returns an error even if unrelated subscribers exist. The
canonical outbox is not acknowledged and coastal events remain eligible for
indefinite retry. Recreating the queue permits the same deterministic event ID
to be delivered. Both publications require confirmation and reject mandatory
returns. This follows RabbitMQ's documented
[return-before-confirm ordering](https://www.rabbitmq.com/docs/confirms).

The notification consumer may receive direct and fanout copies of the same event.
Its existing event-ID deduplication and monotonic push-state merge handle these
as ordinary at-least-once redelivery. Consumers must retain that contract.
Publication has a three-second confirmation budget; connection establishment has
a three-second timeout and ten-second heartbeat. Unknown outcomes close the
connection within one second so delayed replies cannot authorize another event.
The existing canonical outbox makes retries safe. Connection logs omit AMQP URLs
because they can include credentials.

## Required routing verification

Real RabbitMQ integration verifies an unrelated bound queue cannot mask a missing
notification queue, retry after 100 prior coastal failures, recovery when the
required queue appears, subsequent queue deletion, and unrouted fanout rejection.
A TCP proxy holds broker replies after the broker persists the direct message:
the publisher returns a deadline error and closes within about 1.3 seconds,
retaining an unknown outcome for retry. All broker integration checks pass in
2.17 seconds against a disposable single-node RabbitMQ fixture capped at 1 GiB.
Its final observed memory was 133.7 MiB without OOM; the fixture was removed.
All 21 notification-store tests also pass, including
`Upsert_RedeliveredAfterDelivery_DoesNotRegress`, which verifies a duplicate
does not schedule a delivered warning again. Generate the repository's .NET
protobuf clients before running those tests, as required by its build targets.
The full untagged Aggregator suite and all 80 infrastructure tests pass (one
SOPS/age tool availability skip).

From `services/aggregator`, set `AGG_TEST_AMQP_URL` to an isolated broker and run:

```powershell
go test -p 1 -parallel 1 -tags integration ./internal/broker -count=1 -timeout=1m -v
```

## Canonical recovery and receiver proof

Every newly committed, valid coastal revision with boolean `critical=true`
appends immutable warning bytes and a pending entry in the canonical write
transaction. PostgreSQL uses the hazard/outbox transaction. Cassandra migration
0012 puts the journal, pending entry, monotonically numbered history entry and
source-authority check in the same physical canonical shard/Paxos batch.
Changing a later revision to noncritical does not erase an earlier warning.
The journal has no TTL and survives deletion of RabbitMQ and notification-store
data, subject to the canonical store's own replication and availability.

`AGGREGATOR_MODE=critical-delivery-worker` starts a stateless notifier before any
database configuration is loaded. It reads the authenticated internal API,
delivers exact journal bytes directly to the designated Field Team receiver,
verifies its Ed25519 signature and payload hash, then records that receipt in
canonical storage. Only then is the pending entry removed. The ordinary
RabbitMQ/FCM path remains available. Its provider acknowledgment is not receiver
proof and cannot complete this journal.

The Field Team client is `critical-receiver`. It commits the full warning and
signed first-receipt timestamp to SQLite WAL with `synchronous=FULL` before
returning success. Duplicate event IDs return that original receipt; changed
bytes under the same ID are rejected. Restart requires the same receiver
identity, signing key and durable database. Multiple notifiers may race safely.
The private signing key stays on the Field Team client; Aggregators and workers
receive only its public key. HMAC authenticates incoming warning requests.

The API uses a dedicated `CRITICAL_DELIVERY_TOKEN`, distinct from Audit and
service credentials. Its routes are `/internal/critical/pending`, `/events`,
`/version` and `/receipts`; the common prefix is `/internal/critical`.
Requests have a three-second budget; receiver calls have a 1.5-second budget.
Each worker processes up to four deliveries concurrently by default (maximum
eight), cycles bounded pages, and retries indefinitely with polling backoff
from 250 ms to two seconds. Partial storage responses retain a retry cursor.
Memory is bounded by page bytes and concurrency; journal and receiver history
are retained on disk. Receiver saturation returns 503 for retry.

## Running the receiver and notifier

Build the three tools and Aggregator from `services/aggregator`. The Aggregator
Docker image includes all three tools and supports the notifier mode. For a
native Windows client, for example:

```powershell
go build -p 1 -o ../../.cache/critical-receiver.exe ./cmd/critical-receiver
go build -p 1 -o ../../.cache/critical-export.exe ./cmd/critical-export
go build -p 1 -o ../../.cache/critical-reconcile.exe ./cmd/critical-reconcile
```

Generate keys into a **new private directory on the Field Team machine**:

```powershell
.\critical-receiver.exe -keygen-dir C:\private\field-team-run -receiver-id field-team
.\critical-receiver.exe -config C:\private\field-team-run\receiver.keys.json `
  -db C:\private\field-team-run\receipts.db -listen 0.0.0.0:8099
```

The file contains a signing key, public key, receiver identity and separate API
token/HMAC secret. Keep it out of Git and retain the database across restarts.
Expose `/notifications` at an endpoint reachable by the notifiers; use HTTPS
for traffic outside the trusted development network. The receiver is the actual
Field Team notification client and must run outside the BNPB worker's fault
domain. A receipt proves durable arrival at this designated client.

For Kubernetes follow sibling `infrastructure/m2/critical-delivery.md`. The chart
deploys two stateless notifier replicas with host spreading and 128Mi/256Mi
consumer budgets. It is opt-in until the external receiver has been provisioned.

Development Compose can add `-f compose.critical.yaml` after the base and M2
files. Set `CRITICAL_DELIVERY_TOKEN`, `CRITICAL_RECEIVER_ID`,
`CRITICAL_RECEIVER_PUBLIC_KEY`, `CRITICAL_RECEIVER_SECRET` and
`CRITICAL_RECEIVER_URL` in the local environment. For a receiver on the Docker
host use `http://host.docker.internal:8099/notifications`. The signing private
key is never an Aggregator/notifier environment variable. Compose is a local
development route, not evidence of the separate-kernel deployment criteria.

## Closed-window reconciliation

Use an isolated run/session and retain independent source evidence for **all**
observations in that run. Each expected JSONL record must explicitly state:

```json
{"observation_id":"obs-1","critical":true,"valid":true,"emitted_at":"2026-11-18T03:00:00Z"}
```

`emitted_at` is the actual wall-clock source emission time captured by trusted
source/test instrumentation. An accelerated `observed_at` or `virtual_now` is
not an emission timestamp. Do not infer the expected set from successful
deliveries or from canonical storage. Record invalid input with `valid=false`
and its real critical flag so exclusions remain visible. Whole-feed capture
and official game-day evidence remain F18 work.

Close the source window, allow at least five seconds after its last emission,
then record an explicit UTC cutoff. Export signed receipts from the receiver
database using its local `-export` mode. Export the canonical journal **after**
that cutoff through the internal API with the dedicated token in the environment:

```powershell
$env:CRITICAL_DELIVERY_TOKEN = $localKeys.delivery_token
.\critical-export.exe -api http://127.0.0.1:9092 -output .\evidence\journal.jsonl
# Run on the Field Team client; redirect stdout to the receipts evidence file.
.\critical-receiver.exe -config C:\private\field-team-run\receiver.keys.json `
  -db C:\private\field-team-run\receipts.db -export > .\receipts.jsonl
.\critical-reconcile.exe -expected .\expected.jsonl -receipts .\receipts.jsonl `
  -journal .\evidence\journal.jsonl -receiver-id field-team `
  -public-key $localKeys.public_key -cutoff $cutoff -output .\report.json
```

Here `$localKeys` is the locally loaded generated configuration and `$cutoff`
is the actual closed-window RFC3339 timestamp. Never print private values. Use
UTF-8 JSONL when transferring evidence between machines.

Canonical export captures a committed generation vector (16 Cassandra shards
or one PostgreSQL clock), then reads that immutable boundary while new writes
may continue. It writes a completion manifest only if the row count matches
the committed generation count. Reconciliation checks every generation,
canonical payload hash, source identity and signed receipt. Missing revisions
fail even when another revision of the same observation arrived. Partial,
empty, ambiguous or incomplete evidence cannot produce a passing report.

The declared target is zero missing critical observations/revisions, p95 at
most five seconds and at least 95% of expected observations timely. Latency is
source emission to the first verified receipt for the observation; every
canonical critical revision must independently have a matching receipt.
The loss budget is zero; timeliness budget is 5% of expected observations.
Missing and late observations consume it. Negative latency is a clock anomaly
and fails the result. The report includes excluded invalid inputs, late/missing
counts, p95, consumed budget, snapshot versions and window completeness. Exit
code 2 means the report was written but the target failed; code 1 means invalid
evidence or an operational error. A signed client timestamp still requires
measured clock synchronization for a real cross-machine latency claim.

## Cutover and verification limits

Deploy this version before starting an assessed source session. Stop old writers
during schema/application cutover, as in `m2-write-authority.md`; a mixed old/new
writer deployment cannot promise that old writers create journal entries.
Cassandra adopts current legacy critical rows during index backfill or source
replay. PostgreSQL journals new revisions; its pre-feature history is not
automatically reconstructed. Neither backend can recover critical revisions
already overwritten before this feature existed. For an existing data set,
retain/replay authoritative historical input into an isolated new run rather
than claim retroactive completeness from current hazards.

The complete Cassandra integration suite passed in 917.231s against Cassandra
5.0.9 with a 1GiB cap, 256MiB heap, bounded native caches and two CPUs. The suite
includes a real receiver subprocess killed after receipt persistence but before
canonical acknowledgment, outbox deletion, receiver/worker restart, immutable
revision recovery, import idempotence, fencing, Audit exclusion and live-write
snapshot stability. No broker or notification database runs in that recovery
test. The latest PostgreSQL integration suite passed in 12.129s. All 83
infrastructure tests pass (one SOPS/age availability skip); chart tests check
credentials, spreading and budgets. Unit tests verify forged receipts, partial
snapshots, missing/late revisions and budget arithmetic. Aggregator and all
three critical tools cross-compile for Linux amd64 with CGO disabled and serial
compiler jobs. The complete untagged Aggregator regression suite passes.

Saved public fixture evidence is in
[evidence/m2-critical-recovery](evidence/m2-critical-recovery/README.md). It has
one synthetic source observation, two critical revisions, zero missing
revisions and p95 0.6963903s. This RF=1 local fixture proves recovery mechanics,
not full-feed throughput, replicated disk-loss tolerance, the official receiver
window or any multi-node acceptance gate. Those gates remain unchecked in
sibling `infrastructure/docs/m2-completion.md`.
