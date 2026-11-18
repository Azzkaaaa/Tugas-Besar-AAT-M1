# Failed external payload retention

Coastal station/observation failures and satellite stream failures share P3's
durable Inbox and its Internal Ops protected payload download. Complete oversized
HTTP bodies and SSE frames retain every received byte, including invalid UTF-8,
NUL bytes, CRLF and SSE delimiters. Transport interruption is explicitly marked
incomplete; it is never presented as a complete original message.

The parser memory limit remains `M2_MAX_BODY_BYTES` (default 2 MiB). Failed
bodies/frames spill to the Aggregator's writable temporary disk. After hashing,
64 KiB chunks are persisted, followed by the Inbox row. Identical bytes, source
position and transfer status share an archive, so repeated poison polls do not
create duplicate message/chunk sets. Temporary files are removed on completion
or error. HTTP request and SSE idle deadlines bound infinite/slow transfers;
received prefixes are preserved with `capture.complete=false` when interrupted.

`GET /internal/ops/ingest/{id}` adds `capture` for streamed external captures:
`complete`, byte count and `position` (endpoint, source cursor, frame ID/kind
where known). No credentials or token-bearing URLs enter this metadata. The
existing `/payload` endpoint streams original chunks and still requires Internal
Ops `ingest:inspect`. Audit lists expose metadata, never raw bytes. External
messages remain parked until their source adapter supports a deliberate repair;
P3's schema replay cannot treat arbitrary external bytes as a supported schema.

A complete oversized satellite frame with a valid contiguous numeric offset is
archived before a small `quarantined` journal entry advances the checkpoint.
This entry leaves fire state unchanged, including on recovery, and the next
valid frame continues normally. Archive failure or an incomplete frame cannot
advance. Unknown/gapped offsets still fail closed and reconnect from the last
committed position rather than inventing an upstream cursor.

Oversized coastal pages retry the original cursor with progressively smaller
page limits down to one. A valid response can therefore resume without skipping
observations when the envelope exceeded the parser budget. Malformed envelopes
or an oversized individual observation retain their bytes and retry under the
existing circuit breaker until the source recovers; no opaque cursor is guessed.
Other source jobs have independent clients, deadlines and leases.

Verification: HTTP fixtures compare a 300,000-byte binary response byte for byte
after archival, test interrupted transfers, metadata and repeat deduplication.
SSE fixtures verify full oversized CRLF frames, following healthy events,
incomplete EOF and archive failure without checkpoint advance. The existing
framing suite uses the same parser as production. All untagged Aggregator tests
pass with serial, memory-bounded Go execution.
