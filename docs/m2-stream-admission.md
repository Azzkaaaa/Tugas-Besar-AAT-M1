# FULL feed and SSE admission checkpoint

19 November 2026. The user requested a pause after this fix and its local
commit. Do not start another implementation task until explicitly resumed.

Each event-live-feed replica admits one authorized FULL subscription, acquired
before replay hydration. SUMMARY uses no FULL slot. Authorization and cursor
validation precede admission. Exhaustion returns gRPC RESOURCE_EXHAUSTED.
The slot is shared by the forwarding task, projection stream, HTTP body and
encoded DATA buffers. Zero-copy Bytes owners retain it through transport
queueing and slicing, even after the response/body is dropped. A FULL Redis
read uses a dedicated timed connection so cancellation drops its connection
instead of leaving a large response on the shared multiplexer.

Gateway streams now use bounded general/reserved capacity, independent of
unary requests. Existing verified priority controls reserved stream capacity.
One additional FULL stream slot covers all three feed aliases. Rejected streams
return documented HTTP 429; the separate FULL unary slot remains available.
These are per-replica limits, not cluster-wide single-subscriber limits.

The SSE receiver waits for the current frame to be consumed before decoding
another message. Body closure cancels the subscription, unblocks pipe writes
and joins the pump/receiver before admission is returned. The outer HTTP
lifetime also closes the owned body on paths bypassing normal visitor cleanup.
Feed opening is bounded by RequestTimeout (fallback two seconds); accepted
idle streams have no request deadline. Each socket write/flush has a five-second
deadline, cleared immediately afterward. Failed writes cancel the stream.

## Verification

- Rust 1.96: formatting, all-target Clippy with warnings denied, production
  build and 36 ordinary tests pass. Two external fixtures are ignored by default.
- Complete API Gateway Go suite passes, including no prefetch while output is
  blocked, idle cancellation, joined cleanup, opening timeout, aliases sharing
  admission, reserved priority, unary independence, slot reuse and actual TCP
  write timeout. Deadline tests verify idle streams retain no write deadline.
- Separate native Rust/Tonic feed and Go gateway test processes, each capped
  at 256MiB memory/swap with two CPUs: one blocked FULL output, 24 extra FULL
  requests rejected, direct feed overload/scope rejection, SUMMARY access and
  all 200,000 detection keys compared through the generated SSE visitor.
  Gateway peak charged memory: 154,828,800 bytes; feed: 110,723,072 bytes.
  Both exit successfully without OOM or restart. The gateway fixture uses
  GOMEMLIMIT=160MiB. Its client parses JSON incrementally, avoiding another
  retained full result. A deliberate closed test writer produces an expected
  response-error log; all acceptance assertions pass.
- Redis regression rerun after the FULL connection change: 32 records times
  200,000 keys, all retained keys compared, bounded retention, warm/cold summary
  repair and cancellation/error recovery. Native application cap 256MiB;
  separate Redis cap 1GiB with maxmemory 256mb, noeviction, AOF/everysec.
  Both pass without OOM/restart; Redis memory-event counters remain zero.

Logs, image digests, caps and measurements are in
[the evidence directory](evidence/m2-stream-admission/report.json).

The fixture uses fixed test scopes and an in-memory publisher. It does not
exercise deployed OIDC/session rotation, a real public proxy, zero HTTP/2 flow
window cancellation, simultaneous large FULL unary plus SSE, notification
consumers or the combined disk-ingest/storage/outbox/broker path. Those broader
memory cases and official/k3s/RF=3 runtime acceptance remain open under F15.

## Reproduction

Use the retained task-labelled Docker build cache `m2-feed-build-cache`, mounted
at /build in rust:1.96-alpine. Set CARGO_HOME=/build/cargo,
CARGO_TARGET_DIR=/build/target, CARGO_BUILD_JOBS=1 and CARGO_PROFILE_DEV_DEBUG=0.
Mount this repository at /workspace; work in /workspace/services/event-live-feed.
Generate the ignored common/event/feed Rust bindings first on a clean checkout.
Use one compiler job and a separate 2GiB builder. Run fmt, Clippy, production
build and test --no-run; run the resulting native executable under 256MiB.

For the transport fixture, start that executable in a disposable capped Linux
container with FEED_TEST_SERVE=1 and arguments:

```text
serve_stream_memory_fixture --ignored --test-threads=1 --nocapture
```

Compile the gateway server tests for Linux/amd64. Run in a separate 256MiB
container sharing the feed network namespace, GOMAXPROCS=2, GOMEMLIMIT=160MiB
and FEED_STREAM_TEST_TARGET=127.0.0.1:19093:

```text
gateway-server.test -test.run=TestFullSSELargeFireMemory -test.v=true -test.timeout=60s
```

Close clients, then touch /tmp/m2-stream-stop inside the feed container for
graceful shutdown. The fixture also stops after 120 seconds. Run the existing
bounded_large_replay_and_legacy_repair test against an empty disposable Redis
with FEED_TEST_REDIS_URL and FEED_TEST_LARGE_KEYS=200000. Never use a real DB.
Remove only task-labelled runtime containers/anonymous data after inspecting
their exit state. Retain the named build cache for future work.
