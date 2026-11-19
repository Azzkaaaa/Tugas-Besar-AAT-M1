# Bounded event-feed replay

The feed no longer reads an entire replay window or follows batches of 512 full
wildfires. Live broadcasts carry sequence numbers. Each subscriber reads the
next record from the shared store, including after coalesced/lagged notices.
Missing, expired and reset positions still terminate with OUT_OF_RANGE; the
gateway's existing reset_required path tells clients to take another snapshot.

Both stores return at most one replay entry per read and initial subscription.
SUMMARY copies an allowlist of public header fields before retaining a result.
New Redis entries get a separate summary hash in the same append script.
An old writer or interrupted earlier cache fill can leave no summary hash:
readers first receive a metadata-only hint, then one shared fallback per store
connection pool loads current immutable content and repairs its header.
Repair checks epoch/window again before filling, so it cannot resurrect an
evicted record. Invalid cached protobufs are repaired from the original entry.

Legacy body repair runs in an owned task with a dedicated, bounded-time Redis
connection. Cancelling the subscriber does not return admission while redis-rs
is still receiving its outstanding body. Its final connection/driver drop occurs
before returning the credit. Only a small header can escape the protected task.
Normal warm summaries do not use this body slot.

The local replay ring targets 32MiB of retained allocations, counting nested
values, string/list capacities and collection/dedup overhead. Redis targets
64MiB for the full stream object, including administrative allocation overhead.
The append script uses MEMORY USAGE with SAMPLES 0 and bounded-count halving
steps. See the [Redis command contract](https://redis.io/docs/latest/commands/memory-usage/).
Summary/dedup keys belonging to evicted new entries are removed too. One intact
boundary record may exceed the target; individual detection lists are never
truncated. Retention and the configured entry cap remain additional bounds.
Under large fires, the replay window can therefore be shorter in event count or
time. This is explicitly reported as an expired cursor, rather than skipping data.
Canonical history remains the Aggregator's responsibility.

A failed INCR or XADD removes the new dedup marker, and XADD failure restores
the previous head. Redis scripts do not roll back earlier commands automatically;
without this cleanup, a broker retry could be acknowledged as a duplicate for an
entry that was never stored. Failure remains retryable; no consumer acknowledgement
is introduced before a successful append.

The gRPC forwarder reserves its one-item channel before reading/projecting.
A blocked subscriber cannot retain another large response in an awaited send.
This is a per-subscription bound. Global FULL stream admission, queued transport
buffers, gateway SSE and notification consumers remain combined-path work.

## Verification and reproduction

Generate the ignored bindings with buf in packages/bnpb-common-rust,
packages/bnpb-event-rust and packages/bnpb-feed-rust. Use Rust 1.96 (the service's
Dockerfile version), one compiler job and a Linux-native build/cache volume.
The host's Rust 1.85 cannot build these locked dependencies. Compilation used
a separate 2GiB/two-CPU builder; this is not the runtime memory profile.

From services/event-live-feed, run:

```sh
cargo fmt --check
cargo clippy --locked --all-targets -j 1 -- -D warnings
cargo build --locked -j 1
cargo test --locked -j 1 --no-run
```

Cargo prints the native test executable path. Run that executable inside a
256MiB/256MiB-memory+swap, two-CPU container. For the external-store fixture,
start a separate empty disposable Redis 8 with a 1GiB cap and the chart's
--maxmemory 256mb --maxmemory-policy noeviction --appendonly yes profile. Set
FEED_TEST_REDIS_URL to that fixture (never a real database) and
FEED_TEST_LARGE_KEYS=200000, then run:

```sh
feed.test --test-threads=1 --nocapture
feed.test bounded_large_replay_and_legacy_repair --ignored --test-threads=1 --nocapture
```

The ignored fixture refuses a nonempty database. It exercises separate publisher
and reader store instances sharing Redis, 32 fires with 200,000 exact detection
keys, one-record replay, explicit expiry after byte eviction, warm summaries
while the fallback slot is held, 24 concurrent cold repairs, corrupt-cache
repair, caller cancellation during Redis CLIENT PAUSE, epoch changes and real
INCR/XADD failures followed by successful retries. It checks Redis output bytes
so summary reads cannot silently hydrate full bodies, and checks that trimmed
metadata/dedup keys do not accumulate.

Raw logs, caps, image digests and final measurements are saved in
[the evidence report](evidence/m2-feed-replay/report.json). The native suite
contains 34 ordinary passing tests and one separately executed Redis fixture.
The slow-subscriber test proves no next full entry is retained while its output
channel is full. The local byte-eviction test uses weak references to prove old
summary snapshots and live notices do not retain an evicted full payload.
The final two-store-instance run took 7.16s, peaking at 91,916KiB RSS and
96,501,760 charged bytes. Its maximum Redis stream size was 48,001,549 bytes;
five recent fires remained. All retained full detection lists were compared.
Twenty-four warm summaries transferred 5,248 Redis output bytes; 24 concurrent
cold repairs transferred 9,606,223 bytes, approximately one full record plus
headers. Redis reached 178,257,920 charged bytes under its separate 1GiB cap,
with no memory-pressure/OOM/kill counters or restart.

This is synthetic replay/retention evidence. It does not prove a complete
RabbitMQ-to-public-SSE path, arbitrarily many FULL peers, notification consumer
memory, deployed JWT/session rotation or the official satellite p95/catch-up
criteria on separate-kernel k3s with RF=3 storage. F15 remains open.
