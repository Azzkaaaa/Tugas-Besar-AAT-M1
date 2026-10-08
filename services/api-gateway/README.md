# API Gateway

Service Go yang mengimplementasikan [kontrak REST](../../contracts/openapi/gateway.yaml)
di atas service gRPC (Aggregator, Danger Notification, Webhook Provider, Event Live Feed).
Envoy tetap menjadi edge: cookie → bearer, Origin/CSRF, dan token exchange RFC 8693. Setelah
itu seluruh `/api/` diteruskan ke gateway ini lewat HTTP/1.1 pada port **8080** (internal).

Gateway ini menggantikan `grpc_json_transcoder` dan filter Lua `sse_filter` di Envoy. ProtoJSON
tidak sesuai OpenAPI: prefiks enum, pembungkus `oneof`, `secret` webhook ikut terkirim,
`int64` menjadi string, kode 201/204/409, problem+json, dan framing SSE.

## Sinkron dengan kontrak

| Sumber | Kode hasil generate | Penjaga |
| --- | --- | --- |
| `contracts/openapi/gateway.yaml` | `packages/bnpb-gateway-go/gatewayapi` (oapi-codegen, chi strict server) | Handler mengimplementasikan `StrictServerInterface`; setiap status sukses punya tipe sendiri. Perubahan kontrak memecah build. |
| `contracts/proto/**` | `packages/bnpb-aggregator-go` (buf) | Client gRPC bertipe; `nx run bnpb-aggregator-go:check-proto` memastikan stub tidak tertinggal. |
| Spec yang di-embed (`gatewayapi.GetSwagger()`) | — | Validasi request saat runtime (`nethttp-middleware`); status error disesuaikan dengan yang didokumentasikan tiap operasi; test memvalidasi setiap respons. |

- **Request.** Divalidasi terhadap spec sebelum handler berjalan: label enum, rentang
  `page_size`, format waktu, pola `X-Correlation-ID`, dan body
  (`additionalProperties: false`). Pelanggaran menghasilkan 400.
- **Error.** Selalu `application/problem+json` dengan `correlation_id`. 401 membawa
  `WWW-Authenticate: Bearer`; 429/503 membawa `Retry-After`. Status yang tidak
  didokumentasikan untuk operasi tersebut diturunkan: 409→400, 429→503, lainnya →503/500.
  Pesan backend tidak pernah diteruskan ke client.
- **Proyeksi.** Varian `oneof` dari backend harus sesuai view yang diminta. FULL untuk
  permintaan SUMMARY dianggap 503, bukan dikirim, sehingga field FULL tidak bocor.
- **Metadata gRPC.** `authorization` aktor dan `x-correlation-id` diteruskan; ID dibuat
  (UUID) bila tidak ada. `traceparent` diteruskan lewat `otelgrpc`. `x-service-token`
  belum dipakai.
- **Feed (SSE).** `/feeds/events|seismic|volcanic` memanggil `SubscribeEvents`.
  - `Last-Event-ID` diteruskan sebagai `resume_after`.
  - Alias `seismic`/`volcanic` mengisi `hazard_type` dan menolak parameter filter.
  - Penolakan sebelum stream terbuka menjadi problem+json.
  - Setiap event dikirim sebagai `id: <resume_cursor>`, `event: hazard.upsert`, dan
    `data: HazardFeedEvent`.
  - Heartbeat dikirim sebagai komentar `: keepalive`.
  - `OUT_OF_RANGE`, sebelum maupun di tengah stream, mengirim `reset_required` lalu
    menutup stream.
  - Status lain di tengah stream (misalnya token habis) menutup stream.
- **Batas.** Request unary dibatasi `GATEWAY_MAX_IN_FLIGHT` dan diberi deadline
  `GATEWAY_REQUEST_TIMEOUT_MS`. Feed tidak dihitung dan tidak diberi deadline.

## Konfigurasi

| Variabel | Default |
| --- | --- |
| `GATEWAY_HTTP_PORT` | `8080` |
| `AGGREGATOR_GRPC_ADDRESS` / `NOTIFICATION_GRPC_ADDRESS` | `aggregator-service:9091` / `notification-service:9095` |
| `WEBHOOK_GRPC_ADDRESS` / `EVENT_FEED_GRPC_ADDRESS` | `webhook-provider:9094` / `feed-service:9093` |
| `GATEWAY_REQUEST_TIMEOUT_MS` / `GATEWAY_MAX_IN_FLIGHT` | `2000` / `100` |
| `GATEWAY_RETRY_AFTER_SECONDS` / `GATEWAY_SSE_HEARTBEAT_SECONDS` | `5` / `15` |
| `LOG_LEVEL`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | `info`, kosong (trace nonaktif), `api-gateway` |

Koneksi gRPC bersifat lazy. Backend yang tidak ada menghasilkan 503 hanya pada route
miliknya. `GET /healthz` dan `/api-gateway --healthcheck` dipakai healthcheck container.

## Nx dan Docker

```bash
npx nx run bnpb-gateway-go:generate-openapi   # gatewayapi/gateway.gen.go (tidak di-commit)
npx nx run api-gateway:test                   # suite kontrak (lihat di bawah)
npx nx run api-gateway:lint                   # gofmt + go vet
npx nx run api-gateway:docker                 # docker build -f services/api-gateway/Dockerfile .
```

Versi oapi-codegen dikunci sebagai Go `tool` di `packages/bnpb-gateway-go/go.mod`;
Dockerfile menjalankan generator sendiri dari kontrak.

`internal/server/contract_test.go` menjalankan **semua** operasi OpenAPI melalui router
asli terhadap fake gRPC (bufconn). Setiap request dan respons, termasuk setiap frame SSE,
divalidasi dengan kin-openapi terhadap spec yang sama. Test gagal bila ada operasi
kontrak yang belum diuji.
