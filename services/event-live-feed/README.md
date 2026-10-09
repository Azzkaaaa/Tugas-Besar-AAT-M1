# Event Live Feed

Service Rust yang mengonsumsi queue RabbitMQ `bnpb.event-live-feed.v1` dan melayani
[EventFeedService](../../contracts/proto/bnpb/feed/v1/feed.proto) (`SubscribeEvents`,
server streaming) pada port gRPC **9093**. Gateway menerjemahkan stream menjadi SSE
(`/api/v1/feeds/events`, alias `/seismic` dan `/volcanic`). Health memakai
`grpc.health.v1.Health`; status `bnpb.feed.v1.EventFeedService` SERVING hanya saat
consumer terhubung ke broker.

Kode kontrak berasal dari package `packages/bnpb-{common,event,feed}-rust`
(`buf generate`, folder `codegen/` tidak di-commit).

## Struktur

Heksagonal, seperti danger-notification: core tipis, pemetaan/filter di adapter,
komposisi hanya di `main.rs`.

| Lokasi | Isi |
| --- | --- |
| `src/core` | `FeedService` (ingest, subscribe replay→live tanpa celah), model, port |
| `src/adapters/inbound/rabbitmq.rs` | Consumer lapin, validasi envelope, ack/nack ke DLQ, reconnect |
| `src/adapters/inbound/grpc.rs` | `SubscribeEvents`: autentikasi, view, filter, proyeksi, cursor |
| `src/adapters/inbound/session.rs` | Umur stream dan rotasi token aktor |
| `src/adapters/inbound/{cursor,hazard}.rs` | Cursor ber-HMAC; filter, scope, proyeksi summary |
| `src/adapters/outbound/memory_store.rs` | Buffer replay, deduplikasi, sequence |
| `src/adapters/outbound/oidc.rs` | Verifikasi JWT via JWKS Zitadel; token exchange RFC 8693 |

## Perilaku

- **Consumer.** Envelope `HazardEventPublished` divalidasi (`schema_version=1`,
  publisher `aggregator`, ID tidak kosong, `source` sama dengan `payload.source`).
  Envelope tidak valid di-nack tanpa requeue sehingga masuk DLQ. Ack dikirim sesudah
  entri tersimpan di buffer. Service mendeklarasikan exchange, DLQ, dan queue-nya
  sendiri lalu mengikat queue ke exchange; Aggregator hanya mendeklarasikan exchange.
- **Replay.** Event ID yang sama selama jendela retensi tidak ditambahkan dua kali.
  Sequence naik mulai 1 per epoch. Buffer dibatasi `FEED_REPLAY_RETENTION_SECONDS`
  dan `FEED_REPLAY_MAX_ENTRIES`. Buffer berada di memori: restart membuat epoch acak
  baru sehingga cursor lama ditolak `OUT_OF_RANGE`, sesuai kontrak.
- **Subscribe.** Urutan pemeriksaan: bearer JWT (tanda tangan JWKS, issuer, `exp`),
  resolusi view (UNSPECIFIED→SUMMARY), scope (`hazards:read:summary`; FULL juga
  `hazards:read:full`, tanpa downgrade), filter, lalu cursor. Replay dan live diambil
  atomik sehingga tidak ada celah. Subscriber yang tertinggal mengisi ulang dari buffer,
  atau mendapat `OUT_OF_RANGE` bila event yang dibutuhkan sudah terbuang. IdP tidak
  dapat dihubungi menghasilkan `UNAVAILABLE`, tidak pernah lolos.
- **Cursor.** `resume_cursor` terikat pada epoch, sequence, subject, view, dan filter
  (feed alias juga tercakup karena menambahkan `hazard_type`). Cursor rusak atau milik
  konteks lain menghasilkan `INVALID_ARGUMENT`, epoch lama `OUT_OF_RANGE`.
- **Metadata.** `x-correlation-id` dikembalikan di metadata response; `traceparent`
  dari gRPC dan header AMQP dipakai sebagai parent span. `x-service-token` belum
  diperiksa, sama seperti danger-notification.

## Rotasi token aktor

Envoy menukar token browser menjadi JWT Zitadel berumur pendek (default 60 detik).
Tanpa rotasi, stream berakhir `UNAUTHENTICATED` ketika JWT habis, termasuk saat
idle, dan client reconnect.

Bila client token exchange feed tersedia, `FEED_TOKEN_RENEW_BEFORE_SECONDS` sebelum
JWT habis service menukar JWT aktor saat ini (RFC 8693, `subject_token_type`
`access_token`, `requested_token_type` `jwt`) dengan client miliknya sendiri.
Zitadel memeriksa ulang token subjek pada setiap exchange, sehingga ini juga menjadi
pemeriksaan pencabutan berkala saat idle. JWT baru diverifikasi ulang. Subject harus
sama dan scope harus tetap mengizinkan view; jika tidak, stream ditutup
`UNAUTHENTICATED`/`PERMISSION_DENIED`. Exchange yang ditolak menutup stream
`UNAUTHENTICATED`. Kegagalan sementara dicoba ulang dengan backoff sampai JWT lama
habis, lalu stream ditutup (fail closed). Status penutupan didahulukan dari event
yang masih di-buffer.

Client `BNPB event feed token exchange` dibuat oleh
[auth-bootstrap](../zitadel/README.md) dan dibaca dari `OIDC_CONFIG_FILE`
(`feed_client_id`/`feed_client_secret`) saat start. Untuk volume Auth yang sudah ada,
jalankan ulang provisioning lalu restart feed:

```bash
docker compose run --rm auth-bootstrap
docker compose restart zitadel-claims feed-service
```

Batasan: belum diverifikasi apakah Zitadel menolak exchange ulang setelah logout di
bridge. Karena itu `FEED_MAX_STREAM_SECONDS` (default 3600) membatasi umur satu stream.
Setelah batas itu stream ditutup `UNAUTHENTICATED` dan client harus reconnect dengan
token baru dari sesinya.

## Konfigurasi

| Variabel | Default |
| --- | --- |
| `EVENT_FEED_GRPC_PORT` | `9093` |
| `EVENT_FEED_AMQP_URL` | wajib |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` |
| `EVENT_FEED_CONSUMER_QUEUE` / `EVENT_FEED_CONSUMER_DLQ` | `bnpb.event-live-feed.v1` / kosong (pesan ditolak dibuang) |
| `FEED_REPLAY_RETENTION_SECONDS` / `FEED_REPLAY_MAX_ENTRIES` | `600` / `10000` |
| `FEED_AUTH_AUTHORITY` | `http://auth.localhost:8080` |
| `OIDC_CONFIG_FILE` | kosong |
| `FEED_TOKEN_EXCHANGE_CLIENT_ID` / `_SECRET` | kosong; override nilai `oidc.json` |
| `FEED_TOKEN_RENEW_BEFORE_SECONDS` / `FEED_MAX_STREAM_SECONDS` | `15` / `3600` |
| `LOG_LEVEL`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | `info`, kosong (trace nonaktif), `event-live-feed` |

Nilai berisi `REPLACE_` ditolak saat start. Di Compose service bernama `feed-service`
(alias yang dipakai Envoy) dan menunggu `auth-bootstrap` selesai.

## Nx dan Docker

```bash
npx nx run-many -t generate-proto -p bnpb-common-rust,bnpb-event-rust,bnpb-feed-rust
npx nx run event-live-feed:test
npx nx run event-live-feed:lint     # cargo fmt --check + clippy -D warnings
npx nx run event-live-feed:docker   # docker build -f services/event-live-feed/Dockerfile .
```

Dockerfile dibangun dari root repository dan menjalankan `buf generate` sendiri.
Image distroless memakai `/event-live-feed --healthcheck` sebagai healthcheck gRPC.

## Belum termasuk

- SSE, `Last-Event-ID` → `resume_after`, `reset_required`, dan heartbeat ditangani
  [api-gateway](../api-gateway/README.md).
- Store replay Redis (port `ReplayStore` sudah memisahkannya).
- Pengujian end-to-end (Aggregator → RabbitMQ → feed → SSE dengan login Zitadel,
  termasuk stream yang bertahan melewati 60 detik) dilakukan di sesi terpisah.
