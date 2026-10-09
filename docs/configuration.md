# Konfigurasi

[`.env.example`](../.env.example) adalah inventaris konfigurasi. BMKG dan PVMBG
sudah menggunakan Python 3.12 dan SQLite. Jalankan kedua mock melalui
[`compose.mocks.yaml`](../compose.mocks.yaml); Compose utama juga memuat service
lain yang belum selesai. Konfigurasi Auth yang sudah dipisahkan berada di
[`.env.auth.example`](../.env.auth.example) dan
[`compose.auth.yaml`](../compose.auth.yaml). Setiap variabel di `.env.example`
dibaca oleh Compose atau oleh service yang memuat `.env`; variabel yang tidak
dipakai tidak dicantumkan.

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
| `APP_ENV` | `development` | Aggregator dan Webhook Provider (atribut `deployment.environment` telemetry) |
| `LOG_LEVEL` | `info` | Aggregator, Webhook Provider, Event Live Feed; mock mencatat log JSON tanpa override ini |
| `BMKG_HTTP_PORT` | `8081` | Listener BMKG lokal; port host saat memakai Compose |
| `PVMBG_HTTP_PORT` | `8082` | Listener PVMBG lokal; port host saat memakai Compose |
| `AGGREGATOR_GRPC_PORT` | `9091` | Aggregator |
| `EVENT_FEED_GRPC_PORT` | `9093` | Event Live Feed |
| `WEBHOOK_GRPC_PORT` | `9094` | Webhook Provider |
| `NOTIFICATION_GRPC_PORT` | `9095` | Danger Notification (`Kestrel__Endpoints__Grpc__Url`) |
| `BMKG_BASE_URL` | `http://bmkg-mock:8080` | Aggregator pada jaringan Compose |
| `PVMBG_BASE_URL` | `http://pvmbg-mock:8080` | Aggregator pada jaringan Compose |
| `AGGREGATOR_GRPC_ADDRESS` | `aggregator-service:9091` | api-gateway; Danger Notification sebagai `http://<alamat>` (`Aggregator__Address`) |
| `EVENT_FEED_GRPC_ADDRESS` | `feed-service:9093` | api-gateway |
| `WEBHOOK_GRPC_ADDRESS` | `webhook-provider:9094` | api-gateway |
| `NOTIFICATION_GRPC_ADDRESS` | `notification-service:9095` | api-gateway |

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
`http://auth.localhost:8080`. Di dalam jaringan Docker nama itu adalah alias Zitadel
(split-horizon DNS, lihat `compose.auth.yaml`), sehingga bridge dan service backend
memanggil Zitadel langsung, bukan melalui Envoy. Client OIDC bersifat confidential dan berada di
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
| `ZITADEL_MASTERKEY` | 32 karakter dari setup | Zitadel; pertahankan selama memakai database yang sama |
| `AUTH_POSTGRES_PASSWORD` | nilai acak dari setup | PostgreSQL dan Zitadel |
| `LOGIN_SESSION_COOKIE_SECRET` | nilai acak dari setup | Hosted Login UI Zitadel |
| `AUTH_ADMIN_PASSWORD` | nilai acak dari setup | Provisioning administrator lokal; bukan akun dashboard |
| `AUTH_MEDIA_PASSWORD` | nilai acak dari setup | Provisioning `media-demo`, role `MEDIA` |
| `AUTH_FIELD_TEAM_PASSWORD` | nilai acak dari setup | Provisioning `lapangan-demo`, role `FIELD_TEAM` |
| `AUTH_BNPB_PASSWORD` | nilai acak dari setup | Provisioning `bnpb-demo`, role `BNPB_INTERNAL` |
| `FRONTEND_DATA_MODE` | `mock` pada standalone Auth | Build Compose: `mock` memakai fixture, `gateway` memakai API |
| `FRONTEND_API_BASE_URL` | `/api/v1` | Build SPA `compose.yaml` (`VITE_API_BASE_URL`); di `.env` |
| `FRONTEND_HTTP_PORT` | `5173` | Port host preview `compose.frontend.yaml`; di `.env` |

Bootstrap menyimpan `issuer`, `client_id`, `client_secret`, `project_id`,
`organization_id`, `introspection_client_id`, `introspection_client_secret`,
`gateway_client_id`, dan `gateway_client_secret` di `oidc.json` (volume
`auth_client_config`). Nilai secret ini hanya untuk server (bridge dan gateway
Envoy). `actions_target_id`, `actions_signing_key`, `client_ids`, `project_id`, dan
`organization_id` ditulis terpisah ke `actions.json` (volume `auth_actions_config`,
variabel `ACTIONS_CONFIG_FILE`) yang hanya dibaca `zitadel-claims`. SPA tidak
menerima file tersebut dan tidak boleh memakai variabel `VITE_*` untuk secret.
Grant harus berasal dari proyek dan organisasi yang tercantum di konfigurasi;
target Actions v2 [`zitadel-claims`](../services/zitadel-claims/README.md)
memetakannya ke klaim `scopes` sesuai [kontrak gRPC](../contracts/proto/README.md).
Variabel template gateway Envoy dijelaskan di [README Envoy](../services/envoy/README.md).

Jalankan ulang provisioning setelah perubahan kebijakan TTL dengan
`docker compose --env-file .env.auth -f compose.auth.yaml run --rm auth-bootstrap`.
Untuk perubahan password dan pemulihan volume, ikuti
[panduan Zitadel](../services/zitadel/README.md); jangan mengganti master key
pada database yang sudah berisi data.

Build Auth memakai `VITE_APP_MODE=oidc` dan `VITE_ENABLE_DEMO=false`. Mode data
terpisah: `VITE_DATA_MODE=mock` berarti autentikasi nyata, tetapi event masih
fixture frontend. Untuk data nyata, gunakan `VITE_DATA_MODE=gateway` dan
`VITE_API_BASE_URL=/api/v1`; gateway Envoy pada `compose.yaml` meneruskan API ke
service gRPC.
Perubahan variabel `VITE_*` memerlukan build ulang karena dibaca saat build.

Cookie `bnpb_access` dan `bnpb_session` adalah host-only, `HttpOnly`, `Path=/`,
dan `SameSite=Lax`. POST refresh/logout memerlukan Origin aplikasi dan
`X-CSRF-Token` dari `/auth/session`; mutasi API berbasis cookie diperiksa gateway
(Origin dan header `X-CSRF-Token`). CSRF token
bukan access token. Refresh token disimpan terenkripsi di SQLite; akun/password
pengguna disimpan oleh Zitadel di PostgreSQL. Detail implementasi dan testing
ada di [Auth bridge](../services/auth-bridge/README.md) dan
[frontend](../services/frontend/README.md).

### Autentikasi JWT Aggregator

Aggregator memverifikasi JWT access token dari Zitadel menggunakan JWKS yang
dipublikasikan di endpoint OIDC discovery (`<authority>/.well-known/openid-configuration`
→ `jwks_uri`). Key set di-cache dan di-refresh otomatis saat `kid` tidak dikenal
(rotasi kunci). Variabel lama `AUTH_PUBLIC_KEY` dan `AUTH_PUBLIC_KEY_PATH` sudah
dihapus; gunakan `AUTH_AUTHORITY` sebagai gantinya.

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `AGGREGATOR_AUTH_AUTHORITY` | `http://auth.localhost:8080` | Aggregator; URL issuer OIDC untuk JWKS discovery (Compose: `AUTH_AUTHORITY`) |
| `WEBHOOK_AUTH_AUTHORITY` | `http://auth.localhost:8080` | Webhook Provider; idem |
| `AUTH_AUDIENCE` | *(kosong)* | Aggregator dan Webhook Provider; aud claim yang diharapkan. Kosong = tidak divalidasi |
| `AUTH_INSECURE_SKIP_VERIFY` | `false` | Aggregator dan Webhook Provider; `true` melewatkan verifikasi signature JWT (**hanya development**) |

> ⚠️ `AUTH_INSECURE_SKIP_VERIFY=true` menampilkan peringatan saat startup dan
> tidak boleh digunakan di production. Token tetap harus memiliki `exp` yang
> valid.

### Kredensial sumber dan antar-service

| Variabel | Penerima yang memerlukan nilai |
| --- | --- |
| `BMKG_API_KEY` | BMKG Mock sebagai validator, Aggregator sebagai pemanggil |
| `PVMBG_BEARER_TOKEN` | PVMBG Mock sebagai validator, Aggregator sebagai pemanggil |
| `AGGREGATOR_SERVICE_TOKEN` | Identitas outbound Aggregator pada integrasi antar-service berikutnya |
| `WEBHOOK_SERVICE_TOKEN` | Identitas outbound Webhook Provider pada integrasi berikutnya |

Domain kredensial BMKG, PVMBG, dan pengguna BNPB tetap independen. Access token
Zitadel tidak menggantikan API key BMKG ataupun bearer PVMBG. Gateway meneruskan
access token aktor asli dan identitas service pemanggil; tidak ada internal JWT
exchange atau Auth RPC. Aggregator memvalidasi signature JWT menggunakan JWKS
dari Zitadel, bukan dengan kunci PEM statis. Service token dibandingkan
menggunakan constant-time comparison (`crypto/subtle`). Jangan menganggap header
role/scope kiriman client sebagai izin yang valid.

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
| `GATEWAY_MAX_IN_FLIGHT` | `100` | Gateway; batas request unary serentak (feed tidak dihitung), kelebihan → 429/503 |
| `GATEWAY_RETRY_AFTER_SECONDS` | `5` | Gateway; header `Retry-After` pada 429/503 |
| `GATEWAY_SSE_HEARTBEAT_SECONDS` | `15` | Gateway; interval komentar heartbeat SSE |
| `FEED_REPLAY_RETENTION_SECONDS` | `600` | Kedua feed; buffer masing-masing, bukan retensi queue RabbitMQ |
| `WEBHOOK_MAX_RETRIES` | `5` | Webhook Provider; batas percobaan pengiriman ke subscriber |
| `WEBHOOK_HTTP_TIMEOUT_MS` | `10000` | Webhook Provider; timeout satu panggilan HTTP ke subscriber |

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
`/app/data` masing-masing container. Setiap store hanya boleh diakses langsung
oleh pemilik berikut. Danger Notification dan Event Live Feed belum memiliki
store: notifikasi diambil dari Aggregator dan buffer replay feed berada di memori.

DSN mock harus berawalan `sqlite:///` dan menunjuk file persisten. Nilai kosong,
`:memory:`, dan URI `file:` ditolak. Path absolut dapat digunakan, misalnya
`sqlite:///D:/mock-data/bmkg.sqlite3` pada Windows atau
`sqlite:////var/lib/mock/bmkg.sqlite3` pada Linux; direktori tujuan harus writable.

| Variabel | Default contoh | Pemilik tunggal |
| --- | --- | --- |
| `BMKG_STORE_DSN` | `sqlite:///data/bmkg.sqlite3` | BMKG Mock; volume Compose `bmkg_data` |
| `PVMBG_STORE_DSN` | `sqlite:///data/pvmbg.sqlite3` | PVMBG Mock; volume Compose `pvmbg_data` |
| `CANONICAL_STORE_DSN` | `postgres://aggregator:postgres@aggregator-db.local:5432/bnpb_canonical?sslmode=disable` | Aggregator, termasuk state ingest/outbox; volume `aggregator_data` |
| `AUTH_DATABASE_PATH` | `/data/sessions.sqlite` | Auth bridge; volume `auth_sessions` |
| PostgreSQL Zitadel | database `zitadel` | Zitadel; volume `auth_postgres`, tidak dibaca langsung oleh bridge |
| `WEBHOOK_STORE_DSN` | `file:/data/webhook.db` | Webhook Provider; volume `webhook_data` |

Record SQLite bertahan setelah restart proses/container selama file/volume
dipertahankan. Kontrol skema/outage PVMBG adalah state runtime terpisah dari data.
Jangan memasang volume SQLite BMKG ke PVMBG, atau memasang salah satunya ke
Aggregator. Saat lokal, tetap gunakan dua path file yang berbeda.

Jika satu server database/cache fisik menampung beberapa store, gunakan
database/namespace, akun, dan izin terpisah. Gateway tidak menerima DSN.
Komponen lain meminta data kanonik melalui API Aggregator.

## Event Live Feed

| Variabel | Default | Arti |
| --- | --- | --- |
| `EVENT_FEED_AMQP_URL` | wajib | Koneksi consumer; Compose menyusunnya dari `RABBITMQ_DEFAULT_*` |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` | Exchange fanout tempat queue diikat |
| `EVENT_FEED_CONSUMER_QUEUE` / `EVENT_FEED_CONSUMER_DLQ` | `bnpb.event-live-feed.v1` / kosong | Queue dan DLQ; dideklarasikan dan diikat oleh service ini sendiri |
| `FEED_REPLAY_MAX_ENTRIES` | `10000` | Batas jumlah entri replay selain `FEED_REPLAY_RETENTION_SECONDS` |
| `FEED_AUTH_AUTHORITY` | `http://auth.localhost:8080` | Issuer JWT; discovery, JWKS, dan endpoint token exchange |
| `OIDC_CONFIG_FILE` | kosong | `oidc.json` dari auth-bootstrap; dibaca `feed_client_id`/`feed_client_secret` |
| `FEED_TOKEN_EXCHANGE_CLIENT_ID` / `_SECRET` | kosong | Override client token exchange feed; tanpa client, stream berakhir saat token habis |
| `FEED_TOKEN_RENEW_BEFORE_SECONDS` | `15` | Token aktor ditukar ulang sekian detik sebelum habis |
| `FEED_MAX_STREAM_SECONDS` | `3600` | Batas umur satu stream walaupun token terus diperbarui |

Rincian rotasi token dan batasannya ada di [README Event Live Feed](../services/event-live-feed/README.md).

## Danger Notification

Service .NET ini membaca `appsettings.json`. `compose.yaml` memetakan variabel
`.env` ke override environment ASP.NET (`Section__Key`); nilai di appsettings
hanya dipakai saat service dijalankan di luar Compose.

| Variabel `.env` | Default contoh | Key appsettings |
| --- | --- | --- |
| `NOTIFICATION_GRPC_PORT` | `9095` | `Kestrel:Endpoints:Grpc:Url` (`http://0.0.0.0:<port>`) |
| `AGGREGATOR_GRPC_ADDRESS` | `aggregator-service:9091` | `Aggregator:Address` (`http://<alamat>`) |
| `NOTIFICATION_AUTH_AUTHORITY` | kosong → `http://auth.localhost:8080` | `Auth:Authority` |
| `NOTIFICATION_AUTH_REQUIRE_HTTPS_METADATA` | `false` | `Auth:RequireHttpsMetadata`; Zitadel lokal hanya HTTP |
| `NOTIFICATION_AUTH_SCOPE_CLAIM` | `scopes` | `Auth:ScopeClaim`; klaim dari `zitadel-claims` |
| `NOTIFICATION_RABBITMQ_HOST` / `_PORT` | `rabbitmq` / `5672` | `RabbitMq:HostName` / `RabbitMq:Port` |
| `RABBITMQ_DEFAULT_USER` / `_PASS` / `_VHOST` | lihat RabbitMQ | `RabbitMq:UserName` / `Password` / `VirtualHost` |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` | `RabbitMq:Exchange` |
| `NOTIFICATION_CONSUMER_QUEUE` / `NOTIFICATION_CONSUMER_DLQ` | `bnpb.danger-notification.v1` / `.dlq.v1` | `RabbitMq:QueueName` / `RabbitMq:DeadLetterQueue` |
| `NOTIFICATION_FCM_TOPIC` | `hazard-alerts` | `Fcm:Topic` |
| `FIREBASE_CREDENTIALS_PATH` | `./.secrets/firebase-credentials.json` | Bukan appsettings; di-mount untuk `GOOGLE_APPLICATION_CREDENTIALS` |

Service ini tidak menerima URL AMQP karena klien .NET dikonfigurasi per bagian.

## RabbitMQ

| Variabel | Default contoh | Pemakai |
| --- | --- | --- |
| `RABBITMQ_DEFAULT_USER` / `_PASS` / `_VHOST` | `guest` / `guest` / `vhost` | Akun broker yang dibuat Compose; dipakai bersama semua service lokal. Compose menyusun `EVENT_FEED_AMQP_URL` dan setelan RabbitMQ Danger Notification darinya |
| `AGGREGATOR_AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/vhost` | Aggregator; publish |
| `WEBHOOK_AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/vhost` | Webhook Provider; consume queue sendiri |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` | Publisher dan ketiga consumer |
| `EVENT_FEED_CONSUMER_QUEUE` / `EVENT_FEED_CONSUMER_DLQ` | `bnpb.event-live-feed.v1` / `.dlq.v1` | Event Live Feed |
| `WEBHOOK_CONSUMER_QUEUE` / `WEBHOOK_CONSUMER_DLQ` | `bnpb.webhook-provider.v1` / `.dlq.v1` | Webhook Provider (lewat `env_file`) |
| `NOTIFICATION_CONSUMER_QUEUE` / `NOTIFICATION_CONSUMER_DLQ` | `bnpb.danger-notification.v1` / `.dlq.v1` | Danger Notification (`RabbitMq:QueueName` / `DeadLetterQueue`) |

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
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-lgtm:4318` | Service aplikasi, auth-bridge, zitadel-claims, Zitadel, Login UI |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | Idem |
| `OTEL_SERVICE_NAME` | `REPLACE_WITH_SERVICE_NAME` | Override menjadi nama unik tiap service |
| `OTEL_COLLECTOR_ADDRESS`, `OTEL_COLLECTOR_GRPC_PORT` | `otel-lgtm`, `4317` | Envoy (template; OTLP/gRPC) |
| `NGINX_OTEL_TRACE`, `NGINX_OTEL_ENDPOINT` | `off`, `localhost:4317` (image); `on`, `otel-lgtm:4317` (`compose.yaml`) | spa-server (`ngx_otel_module`, gRPC saja) |
| `ZITADEL_INSTRUMENTATION_*` | `auto` (`compose.yaml`) | Zitadel; `auto` membaca variabel `OTEL_*` standar |
| `ENVOY_HEALTH_PATH` | `/healthz` | Envoy; route health tanpa trace untuk probe |
| `TRACE_ESCAPE_HATCH_ENABLED` | `false` | Envoy; **hanya development**. Bila `true`, request dengan header `x-bnpb-trace-debug` menerima `x-trace-id` dan header trace upstream di respons |

`compose.yaml` menjalankan `grafana/otel-lgtm` (Grafana `:3000`, OTLP `:4317`/`:4318`,
Prometheus `:9090`). Sinyal yang dikirim:

| Service | Trace | Metrics | Log |
| --- | --- | --- | --- |
| aggregator, webhook-provider | ya | ya | tidak |
| danger-notification | ya | ya | ya |
| api-gateway | ya | ya (durasi klien gRPC dan server HTTP) | tidak |
| event-live-feed | ya | tidak | tidak |
| envoy | ya | ya (stats sink) | tidak |
| zitadel | ya | ya | ya |
| zitadel-login | ya | ya | — |
| auth-bridge | ya | ya (durasi panggilan ke Zitadel) | tidak |
| zitadel-claims | ya | tidak | tidak |
| spa-server | ya | tidak | tidak |
| aggregator-db, auth-db, rabbitmq | — | di-scrape collector | — |

Scraping Postgres dan RabbitMQ didefinisikan di `services/otel-lgtm/otelcol-extra.yaml`,
digabung dengan konfigurasi bawaan image melalui `OTELCOL_EXTRA_ARGS`. Saat merge,
list diganti (bukan ditambah), sehingga receiver bawaan `otlp` dan
`prometheus/collector` harus tetap ditulis. Validasi dengan `nx run otel-lgtm:validate`.

### Health check

`blackbox` (`prom/blackbox-exporter`, modul di `services/blackbox/blackbox.yml`)
memeriksa setiap service; collector otel-lgtm men-scrape `/probe` per target
(`prometheus/blackbox` di `otelcol-extra.yaml`) sehingga Prometheus menerima
`probe_success{service="..."}` (1 sehat, 0 gagal) dan `probe_duration_seconds`.

| Probe | Target |
| --- | --- |
| gRPC `grpc.health.v1` | aggregator-service, feed-service, webhook-provider, notification-service |
| HTTP | api-gateway `/healthz`, auth-bridge `/health`, zitadel-claims `/health`, zitadel `/debug/healthz`, zitadel-login `/ui/v2/login/healthy`, spa-server `/health`, envoy `/healthz` (Host `localhost`) |
| TCP | aggregator-db, auth-db, rabbitmq |

Mock BMKG/PVMBG tidak diprobe: keduanya mewakili API eksternal, dan kesehatannya
terlihat dari sisi aggregator (span klien, node service graph, alert latensi keluar).

Container `blackbox` memakai `dns: [127.0.0.1]`. grpc-go mencari record TXT
`_grpc_config.<host>` sebelum setiap dial; pada host dengan resolver upstream yang
lambat, pencarian itu menunggu 10 detik sehingga setiap probe gRPC melewati batas
waktu scrape. Nama service tetap di-resolve oleh DNS internal Docker.

Request health tidak di-trace (api-gateway, auth-bridge, zitadel-claims, health
gRPC pada aggregator dan webhook-provider, route health Envoy) agar trace dan
span metrics Tempo tidak tertutup probe yang berjalan setiap beberapa detik.

### Batas trace

- **Masuk di Envoy.** Header trace dari klien (`traceparent`, `tracestate`, `baggage`,
  B3, Jaeger, Datadog, X-Ray, `x-request-id`, dan sejenisnya) dibuang sebelum routing
  (`early_header_mutation_extensions`), sehingga setiap trace dimulai di Envoy dan klien
  tidak dapat menyambung atau memalsukan trace. `X-Correlation-ID` tetap diteruskan.
  Header bernama dengan underscore ditolak 400.
- **Keluar di Envoy.** `trace_headers.lua` menghapus header trace dan `x-envoy-*` dari
  setiap respons, termasuk respons lokal Envoy.
- **Escape hatch (development).** Dengan `TRACE_ESCAPE_HATCH_ENABLED=true`, request yang
  membawa `x-bnpb-trace-debug` (nilai apa pun) menerima `x-trace-id` (trace ID Envoy untuk
  dicari di Grafana) dan header trace dari upstream tidak dihapus. Header itu sendiri
  tidak pernah diteruskan ke upstream. Jangan aktifkan di luar stack lokal.
- **Pihak ketiga.** Panggilan ke BMKG/PVMBG (aggregator), URL subscriber webhook, serta
  FCM dan Google OAuth (danger-notification) tetap memiliki span klien tetapi tidak
  membawa trace context. Hop internal (gRPC, RabbitMQ, Zitadel) tetap mempropagasikan
  W3C trace context dan baggage.
- **Event asinkron.** Aggregator menyimpan trace context poll ingest di
  `outbox_events.trace_context`. Publisher melanjutkan trace itu (`rabbitmq.publish`,
  span producer) dan menyuntikkannya ke header AMQP. Ketiga consumer membuat span
  `rabbitmq.consume` (consumer) sebagai anak span itu, termasuk baggage. Header yang
  rusak hanya memulai trace baru; pesan tetap diproses. Event Live Feed menambahkan
  `feed.deliver` per subscriber yang menerima event, sebagai anak `rabbitmq.consume`
  dengan link ke span stream `SubscribeEvents`. Trace berhenti di sana: penulisan SSE
  di api-gateway tidak memiliki span per event.

### Latensi panggilan keluar

Span klien mencatat setiap panggilan keluar: Envoy ke upstream, api-gateway ke
backend gRPC, aggregator ke mock, RabbitMQ, dan Postgres (`otelpgx`, hanya di dalam
span pemanggil; polling outbox 200 ms tidak membuat trace), webhook-provider ke URL
subscriber, danger-notification ke HTTP luar, auth-bridge ke Zitadel, dan
event-live-feed ke Zitadel (discovery, JWKS, token exchange).

api-gateway menamai span server menurut route (`GET /api/v1/hazards/{hazard_id}`)
setelah chi mencocokkan request, sehingga latensi per endpoint terpisah. Klien gRPC-nya
memakai `grpc.WithDisableServiceConfig()`: tanpa itu, lookup TXT `_grpc_config` yang
lambat membuat semua request gagal 503 selama ~10 detik setelah gateway mulai.

Token exchange Envoy (modul BOE) memanggil Zitadel tanpa span dan tanpa
`traceparent`; panggilan itu muncul sebagai trace terpisah di Zitadel
(`/oauth/v2/token`), dan latensinya termasuk waktu upstream cluster `zitadel` Envoy.

Tempo memakai `services/otel-lgtm/tempo-config.yaml` (salinan konfigurasi image
`grafana/otel-lgtm:0.35.0`; sinkronkan ulang saat upgrade). Perubahannya:
`server.address` menjadi dimensi span metrics (`traces_spanmetrics_*{server_address}`)
dan peer attribute service graph, sehingga target tanpa span sendiri (mock, URL
webhook, FCM, Postgres) muncul sebagai node dengan latensi sisi klien
(`traces_service_graph_request_client_seconds`).

### Alert dan dashboard

`services/otel-lgtm/grafana/` di-mount ke provisioning Grafana:

| Alert | Kondisi |
| --- | --- |
| Service down | `probe_success` < 1 selama 1 menit |
| Health probes not running | scrape `health-*` gagal selama 2 menit (blackbox mati) |
| High error rate | > 5% span server error selama 5 menit |
| Slow outgoing calls | p95 panggilan antar-node service graph > 2 detik selama 5 menit (kecuali ke mock) |
| Slow upstream source | p95 polling aggregator ke mock > 5 detik (mock sengaja menunda hingga 3 detik) |

Belum ada contact point; alert yang aktif terlihat di Grafana > Alerting.
Dashboard **BNPB Health & Traces** (folder BNPB) menampilkan status probe, peta
service, rate/error/latensi per service, latensi keluar per target, dan trace terbaru.

Query string `/auth/callback` membawa authorization code sekali pakai, sehingga
route itu tidak pernah di-sampling di Envoy dan auth-bridge hanya mencatat path
tanpa query. Stack mandiri (`compose.auth.yaml`, `compose.frontend.yaml`) tidak
memiliki otel-lgtm; Envoy membuang ekspor dan tracing nginx tetap `off`.

Mock sudah menulis log JSON per request dengan correlation ID, durasi, status,
dan trace context masuk yang valid. Tidak ada panggilan service keluar dari mock.
Variabel OTEL di atas tidak mengaktifkan exporter pada kedua mock, dan mock berada
di jaringan terisolasi yang tidak menjangkau otel-lgtm; jangan menganggap endpoint
HTTP `/metrics` tersedia.
