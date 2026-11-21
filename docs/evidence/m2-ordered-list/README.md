# Ordered List regression

`integration.log` is the original successful real Cassandra 5.0.9 run on a
disposable RF=1 fixture capped at 1GiB, 256MiB JVM heap and two CPUs. It is not
four-node load/fault acceptance.

The test creates 18 records, clears only its disposable new index to emulate
an upgrade, and leaves 200 empty newer minute buckets. After explicit index
preparation it moves one event, withdraws another, replays lower-revision
headers, and verifies duplicate-free ordered pagination for summary/full views,
source/time bounds, and retryable preparation. Each page has the production
1.5-second read budget. The run passed in 51.28 seconds including schema setup.

Focused Cassandra and gRPC unit suites also passed. Deployed performance and
acceptance measurements are separate evidence in the infrastructure repository.
