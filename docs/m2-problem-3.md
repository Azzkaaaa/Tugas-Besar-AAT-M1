# Milestone 2 Problem 3 — Surat dari Tetangga

## Design and guarantees

Schema evolution is handled at the boundary of the Aggregator. Additive fields are accepted and carried into `attributes`. Required fields must be present with the expected type. PVMBG versions 3, 4, and 5 respectively rename `alert_level` to `volcano_alert`, encode `eruption_count_24h` as text, and omit zero ash height on Normal reports. With `PVMBG_SCHEMA_ADAPTERS=true`, the adapter applies only those declared transformations and validates the result again.

The Cassandra inbox combines the durable intake journal and a dead-letter queue. Each item retains its raw bytes, source, receive time, schema version and correlation ID. Broken envelopes are stored as whole bodies. Large bodies are spooled in bounded memory and persisted in 64 KiB chunks. If the socket stops delivering, the recoverable bytes are recorded with an incomplete-response reason and the poll checkpoint is held. Structurally valid siblings and other source polls can proceed. Schema and poison messages park. Transient writes retry automatically with backoff up to 30 seconds.

The poller lease serializes ordinary intake for each source. All revisions for one source reference share a Cassandra partition, so a conditional batch commits the intake sequence and message together. The canonical CAS compares that sequence with the applied sequence, and retries repair the existing projection through the canonical outbox. A parked head holds later revisions for its key. Irrecoverably malformed payloads are retained for inspection, and messages whose source reference cannot be extracted have no per-key sequence. Identical byte payloads for the same stream, representation and schema version represent the same delivery.

The gateway calls an Aggregator RPC for field reports. A quorum-confirmed conditional insert commits the receipt before the gateway responds `202`. The key is scoped to the JWT subject, so a retry after a timeout or receiver failure can recover the same receipt. A separate worker reads pending rows and conditionally commits the normalized report and terminal state together. Workers can attempt processing at least once. The report row contains one committed result. Deployment durability depends on the team Cassandra keyspace maintaining quorum and keeping its replicas on separate failure domains.

The current Cassandra implementation pages through fixed shards to discover unresolved messages and pending reports. This keeps the durable rows authoritative for discovery after a crash. Record retention and indexing for longer-running or higher-volume deployments need separate sizing.

## Deployment and access

Use the team’s P1/P2 cluster and the P3 versions of Aggregator, API Gateway, Envoy routing, PVMBG mock, and Zitadel Claims. The `infrastructure` checkout provides an opt-in P3 chart integration. Use the team’s existing Cassandra settings and Secret, enable `problem3`, and reference the dedicated auditor Secret. Keep the field worker independently scalable, and add the exact `/field-reports` match to the existing protected Envoy HTTPRoute. The Cassandra network policy permits the Aggregator receiver and report worker to use storage. The Internal Ops and Audit routes use a private Aggregator Service on port 9092.

| Identity | Access |
| --- | --- |
| `FIELD_TEAM` with `field-reports:write` | `POST /field-reports`, with `Idempotency-Key` and JSON `observed_at`, `area_name`, `description` |
| `BNPB_INTERNAL` with `ingest:inspect` | `GET /internal/ops/ingest`, `GET /internal/ops/ingest/{id}`, `GET /internal/ops/ingest/{id}/payload` |
| `BNPB_INTERNAL` with `ingest:replay` | `POST /internal/ops/ingest/{id}/replay` |
| `BNPB_INTERNAL` with `field-reports:read` | `GET /internal/ops/field-reports/{id}` |
| `AUDIT_API_TOKEN` | `GET /internal/audit/unprocessed`, with `source`, `limit`, and `cursor` |

For development on a machine that can run the existing Compose stack, `docker compose --profile problem3 up --build` adds the worker. With the Cassandra backend, the internal HTTP listener is available at `http://127.0.0.1:9092`. Set `AUDIT_API_TOKEN` in the local `.env` for an audit check.

## Demonstration and evidence

Set `PVMBG_URL`, `EDGE_URL`, `AGG_URL`, `PVMBG_BEARER_TOKEN`, `AUDIT_API_TOKEN`, `OPS_TOKEN` and `FIELD_TOKEN` in the demo shell. `AGG_URL` can use a port-forward to `service/aggregator-internal-http`. Capture the output and HTTP correlation IDs in the report evidence.

1. Start the baseline validator with `PVMBG_SCHEMA_ADAPTERS=false`. Probe versions 1 and 2 and check the canonical results. For each of 3, 4, and 5, switch the mode, allow the poller to receive new data, then capture the Audit listing. Mode 5 only changes Normal reports, so wait for one to arrive.

   ```sh
   curl -fsS -X POST "$PVMBG_URL/admin/schema-version" \
     -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" -H 'Content-Type: application/json' \
     -d '{"version":3}'
   curl -fsS "$AGG_URL/internal/audit/unprocessed?source=PVMBG&limit=100" \
     -H "Authorization: Bearer $AUDIT_API_TOKEN"
   ```

2. Capture a parked item’s details and original bytes. Roll out the adapter with `problem3.adapterEnabled=true`, then request replay of the same IDs. Repeat the request and compare canonical revision and outbox IDs. Use a fixture that sends two revisions under a single reference to show distinct canonical revisions in intake order. Confirm that resolved messages disappear from Audit and that Media, Tim Lapangan and the Audit token receive `401` or `403` on raw download and replay.

   ```sh
   curl -fsS "$AGG_URL/internal/ops/ingest/$MESSAGE_ID" -H "Authorization: Bearer $OPS_TOKEN"
   curl -fsS "$AGG_URL/internal/ops/ingest/$MESSAGE_ID/payload" -H "Authorization: Bearer $OPS_TOKEN" -o raw.bin
   curl -fsS -X POST "$AGG_URL/internal/ops/ingest/$MESSAGE_ID/replay" -H "Authorization: Bearer $OPS_TOKEN"
   ```

3. Use a disposable upstream fixture on the test release to return a truncated JSON body and a batch with deeply nested, invalid UTF-8 and oversized items alongside a valid item. Compare the saved bytes with the fixture, check that healthy items and BMKG still progress, and record pod restart counts. Exercise the transient case separately by interrupting hazard writes while leaving the inbox writable. Restore hazard writes and observe `retrying` return to processing while incompatible messages stay parked. The focused `TestPipeline*` and `TestRejectPoisonItems` tests cover these behaviors without a live fixture or heavy stack.

4. Scale `field-report-worker` to zero and submit a small batch. The script records every `202`, correlation ID and latency. Use p95 ≤300 ms as the demo target with healthy intake storage. Delete one API gateway receiver pod and one Aggregator receiver pod only after the receipts are recorded, wait for service readiness, and retry selected requests. Scale up the worker, capture the processed ID set, then repeat verification to compare each stored `processed_at`. Separately, interrupt Cassandra quorum and show `503` for a fresh field report request.

   ```sh
   NS=sistem-informasi-kebencanaan-bnpb
   kubectl -n "$NS" scale deployment/field-report-worker --replicas=0
   python tools/problem3_receipts.py submit "$EDGE_URL" receipts.jsonl
   kubectl -n "$NS" get pods
   # Use the receiver pod names shown above on the test release.
   kubectl -n "$NS" delete pod "$GATEWAY_RECEIVER_POD" "$AGGREGATOR_RECEIVER_POD"
   python tools/problem3_receipts.py retry "$EDGE_URL" receipts.jsonl --count 5
   kubectl -n "$NS" scale deployment/field-report-worker --replicas=1
   kubectl -n "$NS" rollout status deployment/field-report-worker
   python tools/problem3_receipts.py verify "$AGG_URL" receipts.jsonl > processed.json
   ```

Run the development tests serially on memory-constrained machines. The tagged Cassandra cases need a team test instance and create isolated disposable keyspaces.

```sh
cd services/aggregator
go test -p 1 -parallel 1 ./...
go test -p 1 -parallel 1 -v -run 'TestPipeline|TestRejectPoisonItems|TestPVMBGSchemaAdapters' ./internal/ingest
go test -p 1 -parallel 1 -tags integration -run '^TestProblem3' ./internal/storage/cassandra
```

Local checks passed for the Aggregator and API Gateway Go suites, 10 PVMBG unit tests, 16 mock HTTP tests, and both legacy and P3 Helm rendering and linting. During the P4/P5 integration on 16 November 2026, the tagged P3 Cassandra tests also passed against Cassandra 5.0.9. See [P3–P5 integration evidence](m2-integration.md) for scope and reproduction commands.

Cluster evidence to collect for the report consists of the schema mode and Audit outputs, raw-byte comparisons, replay revision logs, restart counts, receipt p95 and acknowledged-versus-processed IDs. Those measurements depend on the P1/P2 cluster run.
