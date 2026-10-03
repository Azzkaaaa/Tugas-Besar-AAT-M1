# Konfigurasi

[`.env.example`](../.env.example) adalah inventaris konfigurasi. BMKG dan PVMBG
sudah menggunakan Python 3.12 dan SQLite. Jalankan kedua mock melalui
[`compose.mocks.yaml`](../compose.mocks.yaml); Compose utama juga memuat service
lain yang belum selesai. Variabel untuk Aggregator, Auth, Gateway, broker, dan
observability di bawah tetap merupakan target integrasi, bukan jaminan bahwa
semua service tersebut sudah menggunakannya.

Jalankan `python tools/setup_mock_env.py` dari root repo untuk membuat `.env`
lokal dan mengisi placeholder kredensial kedua mock dengan nilai acak independen.
Jika `.env` sudah ada, nilai kredensial yang sudah diisi dipertahankan. Script
tidak menyiapkan secret BNPB atau broker. Mock menolak secret kosong/placeholder.
Jangan menyuntikkan seluruh `.env` ke semua container; pilih variabel sesuai
penerima pada tabel. Semua durasi memakai satuan yang tertulis pada nama.

Untuk eksekusi lokal, `python tools/run_mock.py bmkg` dan
`python tools/run_mock.py pvmbg` membaca konfigurasi `.env` milik service yang
dipilih; environment shell memiliki prioritas. Detail penggunaan dan pengujian
ada di [panduan mock](mock-services.md).

## Lingkungan, port, dan alamat

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `API_DOCS_PORT` | `8090` | Penampil Swagger UI; bind localhost |
| `APP_ENV` | `development` | Rencana konfigurasi aplikasi BNPB; belum dibaca mock |
| `LOG_LEVEL` | `info` | Rencana konfigurasi aplikasi BNPB; mock mencatat log JSON tanpa override ini |
| `GATEWAY_HTTP_PORT` | `8080` | Gateway |
| `BMKG_HTTP_PORT` | `8081` | Listener BMKG lokal; port host saat memakai Compose |
| `PVMBG_HTTP_PORT` | `8082` | Listener PVMBG lokal; port host saat memakai Compose |
| `AGGREGATOR_GRPC_PORT` | `9091` | Aggregator |
| `AUTH_GRPC_PORT` | `9092` | Auth |
| `EVENT_FEED_GRPC_PORT` | `9093` | Event Live Feed |
| `WEBHOOK_GRPC_PORT` | `9094` | Webhook Provider |
| `NOTIFICATION_GRPC_PORT` | `9095` | Danger Notification |
| `BMKG_BASE_URL` | `http://bmkg-mock:8080` | Aggregator pada jaringan Compose |
| `PVMBG_BASE_URL` | `http://pvmbg-mock:8080` | Aggregator pada jaringan Compose |
| `AGGREGATOR_GRPC_ADDRESS` | `aggregator:9091` | Gateway, Danger Notification untuk freshness |
| `AUTH_GRPC_ADDRESS` | `auth:9092` | Gateway, Aggregator, Event Live Feed, Webhook Provider, Danger Notification |
| `EVENT_FEED_GRPC_ADDRESS` | `event-live-feed:9093` | Gateway |
| `WEBHOOK_GRPC_ADDRESS` | `webhook-provider:9094` | Gateway |
| `NOTIFICATION_GRPC_ADDRESS` | `danger-notification:9095` | Gateway |

Kedua container mock mendengarkan port internal **8080**; host secara default
memakai **8081** untuk BMKG dan **8082** untuk PVMBG. Nama service hanya berlaku
di jaringan Compose. Pemanggil lokal memakai `http://localhost:8081` dan
`http://localhost:8082`. Alamat service BNPB lain masih mengikuti rencana deployment.
Mengganti port listener juga memerlukan pembaruan alamat pemanggil, port mapping,
dan server URL OpenAPI untuk pengujian. Port HTTP dan gRPC tidak perlu seluruhnya
dipublikasikan ke host. Health service HTTP adalah `/health`; health service gRPC
memakai `grpc.health.v1.Health` standar pada port gRPC, tanpa REST `/health`
tambahan yang diasumsikan tersedia.

## Domain kredensial dan identitas client

### Status frontend SPA

Frontend saat ini menggunakan identitas mock yang dipilih di browser, tanpa
password, client secret, atau JWT. Identitas ini hanya untuk pratinjau dan tidak
dikirim ke backend. Build produksi menonaktifkan akses demo secara default.
Konfigurasi publik frontend dan langkah testing ada di
[README frontend](../services/frontend/README.md).

Rencana browser login memakai OIDC/OAuth 2.0 Authorization Code + PKCE dengan
public client SPA. Issuer, client ID, callback, scope, dan penyedia token menunggu
integrasi dengan penanggung jawab Auth. Daftar `*_CLIENT_SECRET` dan alur login
client di bawah berasal dari rancangan Auth sebelumnya; itu bukan konfigurasi
atau kolom input SPA. Jangan masukkan secret tersebut ke `VITE_*`. Kontrak Auth
lama perlu diselaraskan oleh implementasi backend sebelum integrasi.

### Rancangan kredensial backend

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
| `PVMBG_RESPONSE_DELAY_MIN_MS` | `500` | PVMBG Mock; batas bawah delay acak, minimal 0 |
| `PVMBG_RESPONSE_DELAY_MAX_MS` | `3000` | PVMBG Mock; batas atas delay acak, minimal batas bawah |
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
Validator mock memeriksa min ≤ max dan kedua nilai nonnegatif sesuai OpenAPI;
min=max=0 dapat digunakan dalam uji fungsional. Interval generator harus positif.
PVMBG selalu mulai pada skema **1** dan outage **false**. Keduanya diubah lewat
admin API dan kembali ke baseline saat proses restart; tidak ada variabel
`PVMBG_INITIAL_SCHEMA_VERSION` atau `PVMBG_INITIAL_OUTAGE` yang perlu diatur.
Generator terus menulis record selama injected outage sehingga polling dapat
menyusul record yang terlewat sesudah pemulihan.

Polling kedua
sumber harus independen. Poll gagal dapat langsung menandai cache STALE meskipun
freshness window belum lewat; window bukan periode tunggu sebelum mengakui
outage. Pemulihan dilakukan lewat polling sukses, tanpa restart.

## Penyimpanan

Kedua mock memakai file SQLite yang berbeda. DSN `sqlite:///data/...` mengarah
ke path relatif `data/...` terhadap direktori kerja: repo saat memakai launcher
lokal, dan `/app` dalam container. Compose memasang volume terpisah pada
`/app/data` masing-masing container. Store BNPB lain belum ditentukan; DSN kosong
di bagian tersebut bukan database siap pakai. Setiap store/cache hanya boleh
diakses langsung oleh pemilik berikut.

DSN mock harus berawalan `sqlite:///` dan menunjuk file persisten. Nilai kosong,
`:memory:`, dan URI `file:` ditolak. Path absolut dapat digunakan, misalnya
`sqlite:///D:/mock-data/bmkg.sqlite3` pada Windows atau
`sqlite:////var/lib/mock/bmkg.sqlite3` pada Linux; direktori tujuan harus writable.

| Variabel | Default contoh | Pemilik tunggal |
| --- | --- | --- |
| `BMKG_STORE_DSN` | `sqlite:///data/bmkg.sqlite3` | BMKG Mock; volume Compose `bmkg_data` |
| `PVMBG_STORE_DSN` | `sqlite:///data/pvmbg.sqlite3` | PVMBG Mock; volume Compose `pvmbg_data` |
| `CANONICAL_STORE_DSN` | kosong | Aggregator, termasuk state ingest/outbox |
| `AUTH_STORE_DSN` | kosong | Auth |
| `AUTH_CACHE_DSN` | kosong | Auth |
| `NOTIFICATION_STORE_DSN` | kosong | Danger Notification |
| `EVENT_FEED_CACHE_DSN` | kosong | Event Live Feed |
| `WEBHOOK_STORE_DSN` | kosong | Webhook Provider |

Record SQLite bertahan setelah restart proses/container selama file/volume
dipertahankan. Kontrol skema/outage PVMBG adalah state runtime terpisah dari data.
Jangan memasang volume SQLite BMKG ke PVMBG, atau memasang salah satunya ke
Aggregator. Saat lokal, tetap gunakan dua path file yang berbeda.

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

Mock sudah menulis log JSON per request dengan correlation ID, durasi, status,
dan trace context masuk yang valid. Tidak ada panggilan service keluar dari mock.
Variabel OTEL di atas belum mengaktifkan exporter pada kedua mock; integrasi
Collector, Prometheus, Jaeger, dan Grafana tetap pekerjaan berikutnya. Port
exporter/scrape metrics dan instrumentasi SDK belum ditetapkan; jangan menganggap
endpoint HTTP `/metrics` tersedia.
