# Gateway

Rencana API yang diakses client pada HTTP **8080**, sesuai
[OpenAPI Gateway](../../contracts/openapi/gateway.yaml). Implementasi dan bahasa
belum dipilih.

- **Input:** login/refresh/logout, baca daftar/detail hazard dan notifikasi,
  serta subscription SSE seismik/vulkanik. Health: `GET /health`.
- **Output:** REST/JSON dan SSE `hazard.upsert`, pemetaan error gRPC ke HTTP,
  proyeksi summary/full, cursor replay, dan metadata freshness.
- **Storage:** tidak memiliki database bisnis dan tidak menerima DSN service
  lain. Hazard dibaca melalui Aggregator, notifikasi melalui Danger Notification.
- **Tanggung jawab:** adapter REST/SSE ke gRPC, validasi input, scope, batas
  request serentak, deadline/cancellation, correlation ID, dan trace context.
  Tiap RPC downstream memakai `GATEWAY_SERVICE_TOKEN` dan bearer aktor asli.
  Jangan menukar token aktor dengan kredensial service atau sumber data.

TODO: runtime/Dockerfile, handler dan adapter, client gRPC, rate/capacity limits,
proyeksi sesuai kontrak, streaming/reconnect, health, telemetry, dan uji media
403, status degradasi, refresh/revoke, serta performa. Deadline unary 2 detik
default tidak diterapkan sebagai lifetime seluruh SSE. Lihat
[konfigurasi](../../docs/configuration.md) dan
[aturan transport](../../contracts/proto/README.md).
