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

## Verified behavior and remaining durability work

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

This closes the routing gap only. Broker confirmation does not prove receipt by
a Field Team device. F08 remains open for replicated broker/consumer state or
durable end-to-end replay after disk loss. F09 remains open for authenticated
receiver receipts, expected-critical reconciliation, end-to-end latency and
error-budget reporting. No single-node routing test substitutes for those gates.
