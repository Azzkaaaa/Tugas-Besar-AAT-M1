# Konfigurasi

[`.env.example`](../.env.example) adalah inventaris konfigurasi. Saat ini hanya
`API_DOCS_PORT` digunakan oleh [Compose penampil kontrak](../compose.yaml).
Variabel aplikasi di bawah merupakan rencana, belum dibaca oleh implementasi.
Bahasa, runtime, dan teknologi database belum dipilih.

Salin contoh ke `.env` lokal, lalu ganti setiap placeholder `REPLACE_*` dengan
nilai independen. Service nantinya harus menolak placeholder sebagai secret.
Jangan menyuntikkan seluruh `.env` ke semua container; pilih variabel sesuai
penerima pada tabel. Semua durasi memakai satuan yang tertulis pada nama.

## Lingkungan, port, dan alamat

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `API_DOCS_PORT` | `8090` | Penampil Swagger UI; bind localhost |
| `APP_ENV` | `development` | Semua service aplikasi |
| `LOG_LEVEL` | `info` | Semua service aplikasi |
| `GATEWAY_HTTP_PORT` | `8080` | Gateway |
| `BMKG_HTTP_PORT` | `8081` | BMKG Mock |
| `PVMBG_HTTP_PORT` | `8082` | PVMBG Mock |
| `AGGREGATOR_GRPC_PORT` | `9091` | Aggregator |
| `AUTH_GRPC_PORT` | `9092` | Auth |
| `EVENT_FEED_GRPC_PORT` | `9093` | Event Live Feed |
| `WEBHOOK_GRPC_PORT` | `9094` | Webhook Provider |
| `NOTIFICATION_GRPC_PORT` | `9095` | Danger Notification |
| `BMKG_BASE_URL` | `http://bmkg-mock:8081` | Aggregator |
| `PVMBG_BASE_URL` | `http://pvmbg-mock:8082` | Aggregator |
| `AGGREGATOR_GRPC_ADDRESS` | `aggregator:9091` | Gateway, Danger Notification untuk freshness |
| `AUTH_GRPC_ADDRESS` | `auth:9092` | Gateway, Aggregator, Event Live Feed, Webhook Provider, Danger Notification |
| `EVENT_FEED_GRPC_ADDRESS` | `event-live-feed:9093` | Gateway |
| `WEBHOOK_GRPC_ADDRESS` | `webhook-provider:9094` | Gateway |
| `NOTIFICATION_GRPC_ADDRESS` | `danger-notification:9095` | Gateway |

Alamat menggunakan nama service pada jaringan deployment yang direncanakan.
Mengganti port listener juga memerlukan pembaruan alamat pemanggil, port mapping,
dan server URL OpenAPI untuk pengujian. Port HTTP dan gRPC tidak perlu seluruhnya
dipublikasikan ke host. Health service HTTP adalah `/health`; health service gRPC
memakai `grpc.health.v1.Health` standar pada port gRPC, tanpa REST `/health`
tambahan yang diasumsikan tersedia.

## Domain kredensial dan identitas client

| Variabel | Default contoh | Penerima yang memerlukan nilai |
| --- | --- | --- |
| `BMKG_API_KEY` | `REPLACE_BMKG_ONLY` | BMKG Mock sebagai validator, Aggregator sebagai pemanggil |
| `PVMBG_BEARER_TOKEN` | `REPLACE_PVMBG_ONLY` | PVMBG Mock sebagai validator, Aggregator sebagai pemanggil |
| `AUTH_SIGNING_KEY` | `REPLACE_BNPB_SIGNING_KEY` | Auth saja |
| `MEDIA_CLIENT_ID` | `media-demo` | Auth untuk registrasi; client Media saat login |
| `MEDIA_CLIENT_SECRET` | `REPLACE_MEDIA_SECRET` | Auth untuk registrasi; client Media saat login |
| `FIELD_TEAM_CLIENT_ID` | `field-team-demo` | Auth untuk registrasi; client Tim Lapangan saat login |
| `FIELD_TEAM_CLIENT_SECRET` | `REPLACE_FIELD_TEAM_SECRET` | Auth untuk registrasi; client Tim Lapangan saat login |
| `BNPB_INTERNAL_CLIENT_ID` | `bnpb-internal-demo` | Auth untuk registrasi; client BNPB Pusat saat login |
| `BNPB_INTERNAL_CLIENT_SECRET` | `REPLACE_INTERNAL_CLIENT_SECRET` | Auth untuk registrasi; client BNPB Pusat saat login |
| `ACCESS_TOKEN_TTL_SECONDS` | `60` | Auth |
| `REFRESH_TOKEN_TTL_SECONDS` | `86400` | Auth |
| `GATEWAY_SERVICE_TOKEN` | `REPLACE_GATEWAY_SERVICE_TOKEN` | Gateway sebagai pemanggil; Auth untuk allowlist |
| `AGGREGATOR_SERVICE_TOKEN` | `REPLACE_AGGREGATOR_SERVICE_TOKEN` | Aggregator sebagai pemanggil; Auth untuk allowlist |
| `EVENT_FEED_SERVICE_TOKEN` | `REPLACE_EVENT_FEED_SERVICE_TOKEN` | Event Live Feed sebagai pemanggil; Auth untuk allowlist |
| `WEBHOOK_SERVICE_TOKEN` | `REPLACE_WEBHOOK_SERVICE_TOKEN` | Webhook Provider sebagai pemanggil; Auth untuk allowlist |
| `NOTIFICATION_SERVICE_TOKEN` | `REPLACE_NOTIFICATION_SERVICE_TOKEN` | Danger Notification sebagai pemanggil; Auth untuk allowlist |

Ketiga domain BMKG, PVMBG, dan token pengguna BNPB harus independen. Ketiga
identitas client juga berbeda. Auth memetakan identitas ke role/scope dan
menyimpan verifier secret client; role bukan input login. Kredensial client
demo dimasukkan saat login, tidak dibundel sebagai secret tetap dalam SPA.

Setiap pemanggil internal mendapat **hanya secret service miliknya sendiri**.
Auth mengetahui allowlist seluruh identitas service yang boleh memanggilnya.
Penerima gRPC lain harus mendapat verifier/hash allowlist untuk identitas
pemanggil yang diizinkan, bukan secret outbound milik service lain. Provisioning
dan rotasi verifier ini masih pekerjaan implementasi/deployment.

Contoh alur: Gateway memanggil Aggregator dengan `GATEWAY_SERVICE_TOKEN` pada
`x-service-token`, plus access token pengguna asli pada `authorization`.
Aggregator memverifikasi identitas pemanggil, lalu memanggil Auth dengan
`AGGREGATOR_SERVICE_TOKEN` miliknya sendiri dan access token pengguna yang sama.
Pola serupa berlaku untuk Event Live Feed, Webhook Provider, dan Danger Notification. Actor token tidak
diganti menjadi identitas service. Login/refresh/logout memakai kredensial
request sesuai proto dan tetap memiliki identitas service pada metadata.

Secret tidak boleh masuk URL HTTP, log, trace, respons error, payload broker,
atau Git. URL AMQP mengandung secret sehingga juga harus disamarkan pada log.
Aturan scope, pencabutan sesi, dan metadata ada di
[panduan gRPC](../contracts/proto/README.md).

## Perilaku mock, polling, dan kapasitas

| Variabel | Default contoh | Pemakai dan batas |
| --- | --- | --- |
| `MOCK_SEED_COUNT` | `20` | Kedua mock; minimal 20 record per mock |
| `MOCK_EVENT_INTERVAL_MS` | `10000` | Kedua mock; positif dan paling lama 10.000 ms |
| `BMKG_RESPONSE_DELAY_MS` | `100` | BMKG Mock; delay tetap 50–150 ms |
| `PVMBG_RESPONSE_DELAY_MIN_MS` | `500` | PVMBG Mock; batas bawah delay acak |
| `PVMBG_RESPONSE_DELAY_MAX_MS` | `3000` | PVMBG Mock; batas atas delay acak |
| `PVMBG_INITIAL_SCHEMA_VERSION` | `1` | PVMBG Mock; baseline mulai versi 1 tanpa confidence |
| `PVMBG_INITIAL_OUTAGE` | `false` | PVMBG Mock; outage selanjutnya diatur operator lewat admin API |
| `BMKG_POLL_INTERVAL_MS` | `3000` | Aggregator; positif, rekomendasi 2.000–5.000 ms |
| `PVMBG_POLL_INTERVAL_MS` | `3000` | Aggregator; positif, rekomendasi 2.000–5.000 ms |
| `BMKG_REQUEST_TIMEOUT_MS` | `1000` | Aggregator; timeout satu panggilan HTTP BMKG |
| `PVMBG_REQUEST_TIMEOUT_MS` | `3500` | Aggregator; timeout satu panggilan HTTP PVMBG |
| `SOURCE_STALE_AFTER_MS` | `10000` | Aggregator; freshness window sejak polling sukses terakhir |
| `GATEWAY_REQUEST_TIMEOUT_MS` | `2000` | Gateway; deadline request unary, bukan lifetime seluruh SSE |
| `GATEWAY_MAX_IN_FLIGHT` | `100` | Gateway; batas kerja serentak, overload ditolak terkontrol |
| `FEED_REPLAY_RETENTION_SECONDS` | `600` | Kedua feed; buffer masing-masing, bukan retensi queue RabbitMQ |
| `PAGE_TOKEN_TTL_SECONDS` | `300` | Aggregator dan Danger Notification; snapshot cursor masing-masing |

Baseline delay PVMBG adalah rentang 500–3.000 ms; uji P2 memakai min=max=3.000 ms.
Validator konfigurasi harus memeriksa min ≤ max dan nilai positif. Polling kedua
sumber harus independen. Poll gagal dapat langsung menandai cache STALE meskipun
freshness window belum lewat; window bukan periode tunggu sebelum mengakui
outage. Pemulihan dilakukan lewat polling sukses, tanpa restart.

## Penyimpanan

Semua DSN contoh sengaja kosong karena teknologi belum dipilih. Nilai kosong
bukan database siap pakai; implementasi nanti harus memvalidasi konfigurasi.
Setiap store/cache hanya boleh diakses langsung oleh pemilik berikut.

| Variabel | Default contoh | Pemilik tunggal |
| --- | --- | --- |
| `BMKG_STORE_DSN` | kosong | BMKG Mock |
| `PVMBG_STORE_DSN` | kosong | PVMBG Mock |
| `CANONICAL_STORE_DSN` | kosong | Aggregator, termasuk state ingest/outbox |
| `AUTH_STORE_DSN` | kosong | Auth |
| `AUTH_CACHE_DSN` | kosong | Auth |
| `NOTIFICATION_STORE_DSN` | kosong | Danger Notification |
| `EVENT_FEED_CACHE_DSN` | kosong | Event Live Feed |
| `WEBHOOK_STORE_DSN` | kosong | Webhook Provider |

Jika satu server database/cache fisik menampung beberapa store, gunakan
database/namespace, akun, dan izin terpisah. Gateway tidak menerima DSN.
Komponen lain meminta data kanonik melalui API Aggregator.

## RabbitMQ

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `AGGREGATOR_AMQP_URL` | `amqp://aggregator:REPLACE_PUBLISHER_SECRET@rabbitmq:5672/bnpb` | Aggregator; publish |
| `EVENT_FEED_AMQP_URL` | `amqp://event-feed:REPLACE_FEED_BROKER_SECRET@rabbitmq:5672/bnpb` | Event Live Feed; consume queue sendiri |
| `WEBHOOK_AMQP_URL` | `amqp://webhook-provider:REPLACE_WEBHOOK_BROKER_SECRET@rabbitmq:5672/bnpb` | Webhook Provider; consume queue sendiri |
| `NOTIFICATION_AMQP_URL` | `amqp://notifier:REPLACE_NOTIFIER_BROKER_SECRET@rabbitmq:5672/bnpb` | Danger Notification; consume queue sendiri |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` | Publisher, ketiga consumer, provisioning broker |
| `RABBITMQ_EVENT_FEED_QUEUE` | `bnpb.event-live-feed.v1` | Event Live Feed, provisioning broker |
| `RABBITMQ_WEBHOOK_QUEUE` | `bnpb.webhook-provider.v1` | Webhook Provider, provisioning broker |
| `RABBITMQ_NOTIFICATION_QUEUE` | `bnpb.danger-notification.v1` | Danger Notification, provisioning broker |

Rencanakan exchange fan-out dan queue durable independen, pesan persistent,
publisher confirm, ack setelah pemrosesan, retry terbatas, serta dead-letter
queue. Tiap consumer menerima event lalu menyaring domain/level yang relevan.
Semantik delivery at-least-once memerlukan idempotensi. Queue baru tidak otomatis
memperoleh event historis sebelum queue dibuat. Retensi, batas queue, nama
dead-letter queue, dan akun operator broker masih perlu ditetapkan dalam
deployment; variabel di atas belum menjalankan broker.

## Observability

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | Semua service aplikasi |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Semua service aplikasi |
| `OTEL_SERVICE_NAME` | `REPLACE_WITH_SERVICE_NAME` | Override menjadi nama unik tiap service |

Collector, Prometheus, Jaeger, dan Grafana masih merupakan rencana deployment.
Service harus meneruskan correlation ID dan trace context, serta mencatat
latensi setiap panggilan keluar. Port exporter/scrape metrics dan instrumentasi
SDK ditentukan setelah runtime dipilih; kontrak ini tidak menjanjikan endpoint
HTTP `/metrics` pada setiap service gRPC.
