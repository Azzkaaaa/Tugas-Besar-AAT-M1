# Local critical-delivery recovery evidence

Captured on 18 November 2026 by
`TestCriticalDeliveryRecoversAfterReceiverDeathAndDownstreamErasure`.
This is a synthetic local RF=1 fixture, not official full-feed or cluster
acceptance. It contains one valid source observation and two immutable critical
revisions. No private signing key, API token or HMAC secret is included.

The test deletes ordinary outbox data, delivers through a real Field Team
receiver subprocess, prevents the canonical receipt acknowledgment, forcibly
kills that receiver, and starts a new receiver process and stateless worker.
Both revisions recover from the canonical journal; signed first-receipt times
survive the receiver crash. No broker or notification database is running.

Files: `expected.jsonl` is independent synthetic source evidence;
`receipts.jsonl` contains signed receiver receipts; `journal.jsonl` contains the
immutable snapshot and complete generation manifest; `receiver-public-key.txt`
verifies the signatures; `report.json` records the reconciliation result.
Result: missing observations/revisions 0/0; payload mismatches 0; p95 0.6963903s;
timeliness budget consumed 0; target met for this fixture only.

The full Cassandra suite passed in 917.231s using image
`cassandra@sha256:acd754f07c73f3a166bb477354b56ce9a05690c3e5e2aaacc1aab2afddba6436`
(5.0.9), one server with a 1GiB memory cap, 256MiB heap and two CPUs. Last
observed container memory was 998.6MiB with OOMKilled=false. The fixture was
removed afterward. Full PostgreSQL integration passed in 12.129s on an isolated
1GiB-capped fixture (48.65MiB observed, no OOM), also removed afterward.

From `services/aggregator`, with the normal bounded Go environment and a fresh
isolated Cassandra fixture:

```powershell
$env:AGG_TEST_CASSANDRA_HOSTS='127.0.0.1:19042'
$env:AGG_TEST_CRITICAL_EVIDENCE_DIR='<private ignored output directory>'
go test -p 1 -parallel 1 -tags integration ./internal/storage/cassandra `
  -run '^TestCriticalDeliveryRecoversAfterReceiverDeathAndDownstreamErasure$' `
  -count=1 -timeout=3m -v
```

For the entire storage suite omit `-run` and use `-timeout=22m`. The receiver
subprocess test creates and deletes its own keyspace and private temporary
files. Integration fixtures are disposable and must never point at real data.
Use the cutoff recorded in `report.json` and the saved public key to rerun
`critical-reconcile`; see [the delivery guide](../../m2-critical-delivery.md).
