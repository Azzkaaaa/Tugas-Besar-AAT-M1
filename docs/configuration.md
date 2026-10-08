# Konfigurasi

[`.env.example`](../.env.example) adalah inventaris konfigurasi. BMKG dan PVMBG
sudah menggunakan Python 3.12 dan SQLite. Jalankan kedua mock melalui
[`compose.mocks.yaml`](../compose.mocks.yaml); Compose utama juga memuat service
lain yang belum selesai. Konfigurasi Auth yang sudah dipisahkan berada di
[`.env.auth.example`](../.env.auth.example) dan
[`compose.auth.yaml`](../compose.auth.yaml). Variabel Aggregator, API bisnis,
broker, dan observability tetap target integrasi, bukan jaminan semua service
sudah menggunakannya.

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
| `EVENT_FEED_GRPC_PORT` | `9093` | Event Live Feed |
| `WEBHOOK_GRPC_PORT` | `9094` | Webhook Provider |
| `NOTIFICATION_GRPC_PORT` | `9095` | Danger Notification |
| `BMKG_BASE_URL` | `http://bmkg-mock:8080` | Aggregator pada jaringan Compose |
| `PVMBG_BASE_URL` | `http://pvmbg-mock:8080` | Aggregator pada jaringan Compose |
| `AGGREGATOR_GRPC_ADDRESS` | `aggregator:9091` | Gateway, Danger Notification untuk freshness |
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

### Auth, Zitadel, dan SPA

Jalankan dari root repo:

```powershell
python tools/setup_auth_env.py
docker compose --env-file .env.auth -f compose.auth.yaml up -d --build
```

Script membuat `.env.auth` lokal dengan nilai acak terpisah dan mempertahankan
nilai yang sudah terisi. Jangan commit file ini, token bootstrap, file SQLite,
atau isi volume. Teman satu tim menjalankan setup yang sama untuk memperoleh
konfigurasi lokalnya sendiri. Setup ini terpisah dari `setup_mock_env.py`.

SPA di `http://localhost:8080` mengarahkan login ke Zitadel pada
`http://auth.localhost:8080`. Client OIDC bersifat confidential dan berada di
Auth bridge, bukan di JavaScript SPA. Callback adalah
`http://localhost:8080/auth/callback`. Login menggunakan Authorization Code +
PKCE; cookie mengirim access token kepada bridge. Konfigurasi tidak memakai
`AUTH_SIGNING_KEY`, `AUTH_GRPC_*`, atau secret OAuth per role dari rancangan lama.

| Variabel | Nilai/default lokal | Pemakai |
| --- | --- | --- |
| `APP_PUBLIC_URL` | `http://localhost:8080` | Bootstrap dan bridge; callback, Origin, redirect |
| `OIDC_ISSUER` | `http://auth.localhost:8080` | Bootstrap dan bridge; harus cocok dengan issuer publik Zitadel |
| `OIDC_CONFIG_FILE` | `/bootstrap/oidc.json` | Bootstrap menulis; bridge membaca volume read-only |
| `PORT` | `3001` | Listener HTTP bridge internal; tidak dipublikasikan ke host |
| `AUTH_ALLOW_HTTP` | `true` di Compose lokal | Bridge; pengecualian development untuk issuer HTTP |
| `AUTH_COOKIE_SECURE` | `false` di Compose lokal | Bridge; wajib `true` ketika aplikasi menggunakan HTTPS |
| `AUTH_SESSION_KEY` | 64 karakter hex dari setup | Bridge saja; kunci enkripsi refresh token sesi |
| `AUTH_DATABASE_PATH` | `/data/sessions.sqlite` | Bridge; volume `auth_sessions` |
| `AUTH_SESSION_TTL_SECONDS` | `86400` | Batas sesi lokal dan konfigurasi refresh di provisioning |
| `ZITADEL_ACCESS_TOKEN_TTL_SECONDS` | `60` | Bootstrap; mengatur TTL access token di Zitadel sesuai M1 |
| `GATEWAY_UPSTREAM_URL` | kosong pada standalone Auth | Bridge; 503 bila API bisnis belum dikonfigurasi |
| `GATEWAY_SERVICE_TOKEN` | nilai acak dari setup | Bridge; identitas pemanggil pada `x-service-token` ke Gateway internal |
| `ZITADEL_MASTERKEY` | 32 karakter dari setup | Zitadel; pertahankan selama memakai database yang sama |
| `AUTH_POSTGRES_PASSWORD` | nilai acak dari setup | PostgreSQL dan Zitadel |
| `LOGIN_SESSION_COOKIE_SECRET` | nilai acak dari setup | Hosted Login UI Zitadel |
| `AUTH_ADMIN_PASSWORD` | nilai acak dari setup | Provisioning administrator lokal; bukan akun dashboard |
| `AUTH_MEDIA_PASSWORD` | nilai acak dari setup | Provisioning `media-demo`, role `MEDIA` |
| `AUTH_FIELD_TEAM_PASSWORD` | nilai acak dari setup | Provisioning `lapangan-demo`, role `FIELD_TEAM` |
| `AUTH_BNPB_PASSWORD` | nilai acak dari setup | Provisioning `bnpb-demo`, role `BNPB_INTERNAL` |
| `FRONTEND_DATA_MODE` | `mock` pada standalone Auth | Build Compose: `mock` memakai fixture, `gateway` memakai API |

Bootstrap menyimpan `issuer`, `client_id`, `client_secret`, `project_id`,
`organization_id`, `introspection_client_id`, dan `introspection_client_secret`
di volume `auth_client_config`. Nilai secret ini hanya untuk server. SPA tidak
menerima file tersebut dan tidak boleh memakai variabel `VITE_*` untuk secret.
Grant harus berasal dari proyek dan organisasi yang tercantum di konfigurasi;
bridge memetakannya ke scope aplikasi sesuai [kontrak gRPC](../contracts/proto/README.md).

Jalankan ulang provisioning setelah perubahan kebijakan TTL dengan
`docker compose --env-file .env.auth -f compose.auth.yaml run --rm auth-bootstrap`.
Untuk perubahan password dan pemulihan volume, ikuti
[panduan Zitadel](../services/zitadel/README.md); jangan mengganti master key
pada database yang sudah berisi data.

Build Auth memakai `VITE_APP_MODE=oidc` dan `VITE_ENABLE_DEMO=false`. Mode data
terpisah: `VITE_DATA_MODE=mock` berarti autentikasi nyata, tetapi event masih
fixture frontend. Untuk data nyata, gunakan `VITE_DATA_MODE=gateway` dan
`VITE_API_BASE_URL=/api/v1`, lalu hubungkan bridge ke Gateway yang siap.
Compose utama menetapkan upstream internal `http://gateway-transcoder:8080`.
Perubahan variabel `VITE_*` memerlukan build ulang karena dibaca saat build.

Cookie `bnpb_access` dan `bnpb_session` adalah host-only, `HttpOnly`, `Path=/`,
dan `SameSite=Lax`. POST refresh/logout dan mutasi API berbasis cookie juga
memerlukan Origin aplikasi dan `X-CSRF-Token` dari `/auth/session`. CSRF token
bukan access token. Refresh token disimpan terenkripsi di SQLite; akun/password
pengguna disimpan oleh Zitadel di PostgreSQL. Detail implementasi dan testing
ada di [Auth bridge](../services/auth-bridge/README.md) dan
[frontend](../services/frontend/README.md).

### Kredensial sumber dan antar-service

| Variabel | Penerima yang memerlukan nilai |
| --- | --- |
| `BMKG_API_KEY` | BMKG Mock sebagai validator, Aggregator sebagai pemanggil |
| `PVMBG_BEARER_TOKEN` | PVMBG Mock sebagai validator, Aggregator sebagai pemanggil |
| `GATEWAY_SERVICE_TOKEN` | Auth bridge sebagai pemanggil; downstream perlu validator identitas ini |
| `AGGREGATOR_SERVICE_TOKEN` | Identitas outbound Aggregator pada integrasi antar-service berikutnya |
| `EVENT_FEED_SERVICE_TOKEN` | Identitas outbound Event Live Feed pada integrasi berikutnya |
| `WEBHOOK_SERVICE_TOKEN` | Identitas outbound Webhook Provider pada integrasi berikutnya |
| `NOTIFICATION_SERVICE_TOKEN` | Identitas outbound Danger Notification pada integrasi berikutnya |

Domain kredensial BMKG, PVMBG, dan pengguna BNPB tetap independen. Access token
Zitadel tidak menggantikan API key BMKG ataupun bearer PVMBG. Gateway meneruskan
access token aktor asli dan identitas service pemanggil; tidak ada internal JWT
exchange atau Auth RPC. Implementasi validasi identitas service/aktor pada
downstream masih harus diselesaikan pemilik masing-masing service. Jangan
menganggap header role/scope kiriman client sebagai izin yang valid.

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
| `AUTH_DATABASE_PATH` | `/data/sessions.sqlite` | Auth bridge; volume `auth_sessions` |
| PostgreSQL Zitadel | database `zitadel` | Zitadel; volume `auth_postgres`, tidak dibaca langsung oleh bridge |
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
