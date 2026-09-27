# Gateway (Envoy Proxy)

Rencana API gateway yang diakses client pada HTTP **8080**, sesuai dengan
[OpenAPI Gateway](../../contracts/openapi/gateway.yaml). Implementasi kini menggunakan **Envoy Proxy** sebagai edge proxy dan gRPC-JSON transcoder, menggantikan layanan custom sebelumnya.

- **Input:** Request REST untuk baca daftar/detail hazard dan notifikasi,
  subscription SSE live feed (dengan filter opsional), serta manajemen webhook. Flow login/refresh kini ditangani langsung oleh Zitadel (IdP). Health: `GET /health`.
- **Output:** Transformasi payload REST/JSON ke gRPC dan sebaliknya (gRPC-JSON transcoding), streaming SSE untuk `hazard.upsert`, pemetaan error gRPC ke HTTP (`application/problem+json`), serta propagasi metadata HTTP.
- **Storage:** Stateless (tidak memiliki database bisnis). Envoy murni merutekan request ke upstream gRPC: Hazard ke Aggregator, notifikasi ke Danger Notification, feed ke Event Live Feed, dan webhook ke Webhook Provider.
- **Tanggung jawab:** Adapter REST/SSE ke gRPC, validasi JWT eksternal secara edge, integrasi *token exchange* (menukar token eksternal ke short-lived asymmetric JWT internal via filter ExtAuthz/Wasm), global rate limiting, penerapan deadline/circuit breaking, pembuatan *Correlation ID* (jika absen), dan propagasi *trace context*.

TODO: Konfigurasi `envoy.yaml` dan Dockerfile, kompilasi file descriptor `.pb` untuk gRPC-JSON transcoder, integrasi layanan ExtAuthz untuk validasi scope dan token exchange, konfigurasi *Rate Limit Service* (RLS), setup metrik/access log (telemetry), serta uji coba otorisasi 403, status degradasi upstream, dan performa throughput. Timeout unary (default 2 detik) harus dikonfigurasi terpisah agar tidak memutus koneksi streaming SSE yang berumur panjang. Lihat
[konfigurasi](../../docs/configuration.md) dan
[aturan transport](../../contracts/proto/README.md).
