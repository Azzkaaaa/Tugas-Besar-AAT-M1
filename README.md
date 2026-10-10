# IF4031 — Platform Koordinasi Kebencanaan, Milestone 1

Platform BNPB yang mengagregasi data bahaya dari mock **BMKG** (gempa/tsunami)
dan **PVMBG** (vulkanik), menyimpannya dalam bentuk kanonik, lalu
mendistribusikannya ke API REST, live feed SSE, webhook, dan push notification.
Login memakai **Zitadel** (OIDC) melalui Auth bridge; SPA **React + TypeScript**
menampilkan dashboard.

| Komponen | Teknologi | Peran |
| --- | --- | --- |
| `bmkg-mock`, `pvmbg-mock` | Python 3.12 + SQLite | API sumber eksternal tiruan, data dan kredensial terpisah |
| `aggregator-service` | Go + PostgreSQL | Polling kedua mock, store kanonik, outbox → RabbitMQ (gRPC `:9091`) |
| `feed-service` | Rust | Event Live Feed, consumer RabbitMQ, stream gRPC (`:9093`) |
| `webhook-provider` | Go + SQLite | Langganan webhook dan pengiriman bertanda tangan HMAC (`:9094`, opsional) |
| `notification-service` | .NET 10 + SQLite | Danger Notification, push FCM (`:9095`) |
| `api-gateway` | Go | REST `/api/v1` dari OpenAPI, memanggil service gRPC |
| `envoy` | Envoy | Satu-satunya pintu masuk publik `http://localhost:8080` |
| `token-exchange`, `zitadel-claims` | Rust | Token exchange `ext_authz` Envoy; klaim `role`/`scopes` Zitadel |
| `auth-bridge`, `zitadel`, `auth-db` | Node.js, Zitadel, PostgreSQL | OIDC, cookie HttpOnly, sesi |
| `spa-server` | React + nginx | Frontend |
| `rabbitmq` | RabbitMQ 4 | Exchange fan-out `bnpb.hazards.v1` |
| `otel-lgtm`, `blackbox` | Grafana LGTM | Trace, metrics, log, health probe |

## Prasyarat

**Untuk menjalankan sistem** (semua service dikompilasi di dalam Docker; satu
langkah generasi kode .NET berjalan di host, lihat langkah 2):

| Kebutuhan | Versi | Keterangan |
| --- | --- | --- |
| Docker Engine / Docker Desktop | terbaru | Dengan plugin **Docker Compose v2** (`docker compose`, mendukung `--wait`) |
| Python | 3.12+ | Helper setup dan seluruh tes; hanya standard library, tanpa `pip install` |
| Node.js + npm | 22.12+ | Opsional: menjalankan `buf` untuk generasi kode .NET (langkah 2); dapat diganti image Docker `bufbuild/buf` |
| Akses internet ke `buf.build` | — | Plugin `buf` untuk C# bersifat remote; juga diperlukan saat build image |
| Kredensial Firebase | — | File service account JSON di `.secrets/firebase-credentials.json`; `notification-service` gagal start tanpa file ini |
| k6 | 1.x | Hanya untuk uji beban ([instalasi](https://grafana.com/docs/k6/latest/set-up/install-k6/)) |

Port host yang dipakai: `8080` (Envoy), `8081`/`8082` (mock), `9091`, `9093`–`9095`
(gRPC), `5432` (PostgreSQL aggregator), `5672`/`15672` (RabbitMQ), `3000`,
`4317`, `4318`, `9090` (otel-lgtm). Pastikan port tersebut kosong.

**Untuk mengembangkan/membangun service di luar Docker** (opsional):

| Toolchain | Versi | Dipakai oleh |
| --- | --- | --- |
| Node.js + npm | 22.12+ | `npm ci` di root (Nx, buf, openapi-generator), frontend, auth-bridge |
| Go | 1.26 | aggregator, api-gateway, webhook-provider, `packages/bnpb-*-go` |
| Rust | 1.96 | event-live-feed, token-exchange, zitadel-claims |
| .NET SDK | 10.0 | danger-notification |
| buf | 1.73 (lewat `npm ci`) | Generasi stub gRPC dari `contracts/proto` |

## Membangun dan menjalankan dari awal

Semua perintah dijalankan dari root repository.

### 1. Siapkan konfigurasi

```bash
python tools/setup_mock_env.py   # membuat .env dari .env.example, mengisi BMKG_API_KEY & PVMBG_BEARER_TOKEN
python tools/setup_auth_env.py   # membuat .env.auth dari .env.auth.example, mengisi secret Auth
```

Kedua helper hanya mengganti nilai kosong/placeholder dan mempertahankan nilai
yang sudah Anda isi. Setelah itu **edit `.env` secara manual**:

- `AGGREGATOR_DB_PASSWORD`: isi password apa pun (database aggregator dibuat dengan
  password ini pada start pertama).
- `AGGREGATOR_SERVICE_TOKEN` dan `WEBHOOK_SERVICE_TOKEN`: ganti placeholder
  `REPLACE_*` dengan secret acak berbeda; aggregator menolak start dengan placeholder.
  Contoh nilai: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- `FIREBASE_WEB_*` (opsional): isi untuk mengaktifkan push di browser; kosongkan
  untuk build tanpa push.

Letakkan service account Firebase di `.secrets/firebase-credentials.json` (atau
arahkan `FIREBASE_CREDENTIALS_PATH` ke lokasi lain). `.env`, `.env.auth`,
`.secrets/`, dan `.cache/` tidak masuk Git.

### 2. Generasi kode gRPC .NET

Kode C# hasil generasi `contracts/proto` (`packages/bnpb-*-dotnet/*/codegen/`)
tidak masuk Git, dan Dockerfile `notification-service` menyalinnya dari host.
Tanpa langkah ini build `notification-service` gagal. Pilih salah satu:

```bash
# Dengan Node.js/npm
npm ci
npx nx run-many -t generate-proto -p 'bnpb-*-dotnet'
```

```bash
# Hanya Docker
for p in common event aggregator notification; do
  docker run --rm -v "$PWD:/workspace" -w /workspace/packages/bnpb-$p-dotnet bufbuild/buf:1.73.0 generate
done
```

Ulangi setelah mengubah file `.proto`. Kode lain tidak memerlukan langkah host:
stub Go di `packages/bnpb-aggregator-go` sudah di-commit, server REST
`packages/bnpb-gateway-go` dan stub Rust dibuat di dalam Dockerfile masing-masing.
Go, Rust, dan .NET SDK tidak perlu terpasang di host.

### 3. Build dan jalankan seluruh stack

```bash
docker compose --env-file .env --env-file .env.auth up --build -d --wait
```

Build pertama memerlukan beberapa menit karena semua image (Go, Rust, .NET,
Node) dibangun dari sumber. `auth-bootstrap` memprovisi Zitadel sekali jalan:
akun seed, client OIDC, service user untuk tes, dan menulis secret service user
ke `.cache/auth/`.

Webhook Provider **mati secara default** (profile `manual`). Untuk ikut
menjalankannya, yang diperlukan oleh tes webhook dan Problem 5:

```bash
docker compose --env-file .env --env-file .env.auth --profile manual up --build -d --wait
```

### 4. Akses

| URL | Isi |
| --- | --- |
| `http://localhost:8080` | SPA; **Masuk dengan Zitadel** memakai `media-demo`, `lapangan-demo`, atau `bnpb-demo` (password ada di `.env.auth`: `AUTH_MEDIA_PASSWORD`, `AUTH_FIELD_TEAM_PASSWORD`, `AUTH_BNPB_PASSWORD`) |
| `http://localhost:8080/api/v1` | REST API (kontrak: [`contracts/openapi/gateway.yaml`](contracts/openapi/gateway.yaml)) |
| `http://auth.localhost:8080` | Zitadel (console/login) |
| `http://localhost:8081`, `http://localhost:8082` | Mock BMKG (`X-BMKG-Key`) dan PVMBG (`Authorization: Bearer`) |
| `http://localhost:15672` | RabbitMQ management (`RABBITMQ_DEFAULT_USER`/`_PASS`) |
| `http://localhost:3000` | Grafana, dashboard **BNPB Health & Traces** |

### 5. Operasi sehari-hari

```bash
docker compose --env-file .env --env-file .env.auth ps
docker compose --env-file .env --env-file .env.auth logs -f aggregator-service
docker compose --env-file .env --env-file .env.auth restart pvmbg-mock
docker compose --env-file .env --env-file .env.auth down      # data tetap di volume
docker compose --env-file .env --env-file .env.auth down -v   # hapus semua data (reset total)
```

Jangan mengganti `ZITADEL_MASTERKEY` selama volume `auth_postgres` masih ada; untuk
reset Auth gunakan `down -v`. Provisioning ulang Zitadel:
`docker compose --env-file .env.auth -f compose.auth.yaml run --rm auth-bootstrap`.

### Mode parsial

| Tujuan | Perintah | Panduan |
| --- | --- | --- |
| Hanya dua mock | `docker compose -f compose.mocks.yaml up --build -d` | - |
| Mock tanpa Docker | `python tools/run_mock.py bmkg` dan `python tools/run_mock.py pvmbg` (dua terminal) | - |
| Auth + SPA dengan data fixture | `docker compose --env-file .env.auth -f compose.auth.yaml up --build -d --wait` | [Auth bridge](services/auth-bridge/README.md) |
| SPA demo tanpa backend | `npm --prefix services/frontend ci && npm --prefix services/frontend run dev` → `http://localhost:5173` | [frontend](services/frontend/README.md) |

### Build service di luar Docker

```bash
npm ci                                         # Nx, buf, openapi-generator
npx nx run-many -t build                       # semua project Nx (generate stub lalu build)
npx nx run api-gateway:build                   # atau per project
(cd services/aggregator && go build ./...)     # aggregator dan webhook-provider bukan project Nx
```

Generasi kode kontrak: `npx nx run bnpb-aggregator-go:check-proto` (stub gRPC
sesuai `contracts/proto`) dan `npx nx run bnpb-gateway-go:generate-openapi`
(server REST dari OpenAPI). Service di luar Docker tetap membutuhkan RabbitMQ,
PostgreSQL, dan Zitadel yang berjalan.

## Konfigurasi

Ada dua file environment; keduanya dibaca Compose dan hanya variabel yang
relevan disuntikkan ke tiap container.

- [`.env.example`](.env.example) → `.env`: service aplikasi, mock, broker, observability.
- [`.env.auth.example`](.env.auth.example) → `.env.auth`: Zitadel, Auth bridge, akun seed.

Variabel shell mengalahkan nilai dari `--env-file`. Variabel `VITE_*`/`FIREBASE_WEB_*`/
`FRONTEND_*` dibaca saat build SPA, sehingga perubahannya memerlukan `--build`.

### `.env` — umum, port, dan alamat

| Variabel | Default | Arti |
| --- | --- | --- |
| `APP_ENV` | `development` | Label environment pada telemetry |
| `LOG_LEVEL` | `info` | Level log aggregator, webhook-provider, event-live-feed |
| `BMKG_HTTP_PORT` / `PVMBG_HTTP_PORT` | `8081` / `8082` | Port host mock (di dalam container selalu `8080`) |
| `AGGREGATOR_GRPC_PORT`, `EVENT_FEED_GRPC_PORT`, `WEBHOOK_GRPC_PORT`, `NOTIFICATION_GRPC_PORT` | `9091`, `9093`, `9094`, `9095` | Port gRPC tiap service |
| `BMKG_BASE_URL` / `PVMBG_BASE_URL` | `http://bmkg-mock:8080` / `http://pvmbg-mock:8080` | URL mock yang di-poll aggregator |
| `*_GRPC_ADDRESS` | `aggregator-service:9091`, dst. | Alamat backend yang dipanggil api-gateway dan Danger Notification |

### `.env` — kredensial dan autentikasi

| Variabel | Default | Arti |
| --- | --- | --- |
| `BMKG_API_KEY` | placeholder → diisi helper | Kunci BMKG; dipakai mock (validator) dan aggregator (pemanggil) |
| `PVMBG_BEARER_TOKEN` | placeholder → diisi helper | Bearer PVMBG; harus berbeda dari kunci BMKG |
| `AGGREGATOR_SERVICE_TOKEN` / `WEBHOOK_SERVICE_TOKEN` | placeholder → **isi manual** | Token antar-service (`x-service-token`) |
| `AGGREGATOR_AUTH_AUTHORITY` / `WEBHOOK_AUTH_AUTHORITY` | `http://auth.localhost:8080` | Issuer OIDC untuk verifikasi JWT via JWKS |
| `AUTH_AUDIENCE` | kosong | Klaim `aud` yang diharapkan; kosong = tidak divalidasi |
| `AUTH_INSECURE_SKIP_VERIFY` | `false` | `true` melewatkan verifikasi signature JWT; **hanya development** |
| `FEED_AUTH_AUTHORITY` | `http://auth.localhost:8080` | Issuer untuk Event Live Feed |
| `FEED_TOKEN_EXCHANGE_CLIENT_ID` / `_SECRET` | kosong | Override client token exchange feed; default dari `oidc.json` bootstrap |
| `FEED_TOKEN_RENEW_BEFORE_SECONDS` | `15` | Token aktor ditukar ulang sekian detik sebelum kedaluwarsa |
| `FEED_MAX_STREAM_SECONDS` | `3600` | Umur maksimum satu stream SSE |

### `.env` — Danger Notification dan Firebase

| Variabel | Default | Arti |
| --- | --- | --- |
| `NOTIFICATION_AUTH_AUTHORITY` | kosong → Zitadel lokal | Issuer JWT (`Auth:Authority`) |
| `NOTIFICATION_AUTH_REQUIRE_HTTPS_METADATA` | `false` | Zitadel lokal hanya HTTP |
| `NOTIFICATION_AUTH_SCOPE_CLAIM` | `scopes` | Nama klaim scope dari `zitadel-claims` |
| `NOTIFICATION_RABBITMQ_HOST` / `_PORT` | `rabbitmq` / `5672` | Lokasi broker |
| `NOTIFICATION_FCM_TOPIC` | `hazard-alerts` | Topik FCM tujuan push |
| `NOTIFICATION_STORE_CONNECTION` | `Data Source=/data/notifications.db` | SQLite notifikasi + outbox push |
| `FIREBASE_CREDENTIALS_PATH` | `./.secrets/firebase-credentials.json` | Service account Firebase yang di-mount read-only (wajib ada) |
| `FIREBASE_WEB_API_KEY`, `_PROJECT_ID`, `_MESSAGING_SENDER_ID`, `_APP_ID`, `_VAPID_KEY` | kosong | Identitas publik web app Firebase untuk push browser; kosong = tanpa push |

### `.env` — frontend

| Variabel | Default | Arti |
| --- | --- | --- |
| `FRONTEND_API_BASE_URL` | `/api/v1` | Base URL API yang ditanam ke build SPA `compose.yaml` |
| `FRONTEND_HTTP_PORT` | `5173` | Port host preview `compose.frontend.yaml` |

### `.env` — perilaku mock, polling, dan gateway

| Variabel | Default | Arti |
| --- | --- | --- |
| `MOCK_SEED_COUNT` | `20` | Record historis awal per mock (minimal 20) |
| `MOCK_EVENT_INTERVAL_MS` | `10000` | Interval event baru per mock (maks. 10.000) |
| `BMKG_RESPONSE_DELAY_MS` | `100` | Delay tetap BMKG (50–150) |
| `PVMBG_RESPONSE_DELAY_MIN_MS` / `_MAX_MS` | `500` / `3000` | Rentang delay acak PVMBG |
| `BMKG_POLL_INTERVAL_MS` / `PVMBG_POLL_INTERVAL_MS` | `3000` | Interval polling aggregator per sumber |
| `BMKG_REQUEST_TIMEOUT_MS` / `PVMBG_REQUEST_TIMEOUT_MS` | `1000` / `3500` | Timeout satu panggilan polling |
| `SOURCE_STALE_AFTER_MS` | `10000` | Sumber ditandai STALE bila tak ada polling sukses selama ini |
| `GATEWAY_REQUEST_TIMEOUT_MS` | `2000` | Deadline request unary gateway |
| `GATEWAY_MAX_IN_FLIGHT` | `100` | Batas request unary serentak; kelebihan → 429/503 |
| `GATEWAY_RETRY_AFTER_SECONDS` | `5` | Header `Retry-After` pada 429/503 |
| `GATEWAY_SSE_HEARTBEAT_SECONDS` | `15` | Interval heartbeat SSE |
| `FEED_REPLAY_RETENTION_SECONDS` / `FEED_REPLAY_MAX_ENTRIES` | `600` / `10000` | Buffer replay feed di memori |
| `WEBHOOK_MAX_RETRIES` / `WEBHOOK_HTTP_TIMEOUT_MS` | `5` / `10000` | Retry dan timeout pengiriman webhook |
| `OUTBOX_MAX_RETRIES` | `5` | Retry publish outbox aggregator |

### `.env` — penyimpanan dan RabbitMQ

| Variabel | Default | Arti |
| --- | --- | --- |
| `BMKG_STORE_DSN` / `PVMBG_STORE_DSN` | `sqlite:///data/bmkg.sqlite3` / `…pvmbg.sqlite3` | File SQLite tiap mock (harus berbeda) |
| `AGGREGATOR_DB_PASSWORD` | kosong → **isi manual** | Password PostgreSQL store kanonik aggregator |
| `WEBHOOK_STORE_DSN` | `file:/data/webhook.db` | SQLite webhook-provider |
| `RABBITMQ_DEFAULT_USER` / `_PASS` / `_VHOST` | `guest` / `guest` / `vhost` | Akun broker yang dibuat Compose |
| `AGGREGATOR_AMQP_URL` / `WEBHOOK_AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/vhost` | Koneksi broker; sesuaikan bila akun di atas diubah |
| `RABBITMQ_EXCHANGE` | `bnpb.hazards.v1` | Exchange fan-out |
| `*_CONSUMER_QUEUE` / `*_CONSUMER_DLQ` | `bnpb.<consumer>.v1` / `.dlq.v1` | Queue dan dead-letter queue tiap consumer (feed, webhook, notification) |

### `.env` — observability

| Variabel | Default | Arti |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `_PROTOCOL` | `http://otel-lgtm:4318` / `http/protobuf` | Tujuan ekspor OTLP |
| `OTEL_SERVICE_NAME` | placeholder | Di-override Compose per service |
| `TRACE_ESCAPE_HATCH_ENABLED` | `false` | Bila `true`, request dengan header `x-bnpb-trace-debug` menerima `x-trace-id`; **hanya development** |

### `.env.auth` — Auth

| Variabel | Default | Arti |
| --- | --- | --- |
| `ZITADEL_MASTERKEY` | dari helper (32 karakter) | Kunci enkripsi Zitadel; jangan diganti selama database ada |
| `AUTH_POSTGRES_PASSWORD` | dari helper | Password PostgreSQL Zitadel |
| `LOGIN_SESSION_COOKIE_SECRET` | dari helper | Secret cookie Login UI Zitadel |
| `AUTH_SESSION_KEY` | dari helper (64 hex) | Kunci enkripsi refresh token di Auth bridge |
| `AUTH_ADMIN_PASSWORD` | dari helper | Administrator Zitadel lokal |
| `AUTH_MEDIA_PASSWORD`, `AUTH_FIELD_TEAM_PASSWORD`, `AUTH_BNPB_PASSWORD` | dari helper | Password `media-demo` (MEDIA), `lapangan-demo` (FIELD_TEAM), `bnpb-demo` (BNPB_INTERNAL) |
| `ZITADEL_ACCESS_TOKEN_TTL_SECONDS` | `60` | Umur access token (sesuai M1) |
| `AUTH_SESSION_TTL_SECONDS` | `86400` | Umur sesi/refresh |
| `FRONTEND_DATA_MODE` | `mock` | Hanya untuk `compose.auth.yaml` standalone; `compose.yaml` selalu `gateway` |
| `TRACE_ESCAPE_HATCH_ENABLED` | `false` | Sama seperti di `.env` |

## Pengujian

### Unit dan tes proses (tanpa Docker)

```bash
python -m unittest discover -s tests -v       # mock BMKG/PVMBG; tes e2e/problem otomatis dilewati
python -m unittest discover -s services/bmkg-mock -p test_storage.py -v
npx nx run-many -t test                       # tes per service (butuh toolchain pengembangan)
```

### End-to-end (stack berjalan)

Jalankan stack lengkap dengan profile `manual` (lihat langkah 3), lalu:

```bash
BNPB_E2E=1 python -m unittest discover -s tests -p 'test_e2e_*.py' -v
```

PowerShell: `$env:BNPB_E2E = "1"` sebelum perintah. Tes masuk lewat Envoy
memakai service user `svc-media`, `svc-lapangan`, `svc-bnpb` (secret dari
`.cache/auth/`) dan mencakup auth, hazard, feed SSE, webhook, dan notifikasi.
Pada stack yang baru dinyalakan satu putaran memerlukan beberapa menit.
Konfigurasi `E2E_*` dan catatan firewall ada di [panduan e2e](docs/e2e-testing.md).

> Docker rootless/firewall: bila webhook-provider tidak dapat menjangkau listener
> tes di host, set `E2E_CALLBACK_HOST` ke IP LAN host.

### Skenario Problem Set 1–5

Pastikan sistem sudah berjalan. Ikuti petunjuk untuk membangun dan menjalankan sistem dari awal.

#### Skenario Automated Testing (P1-5)

```bash
python tests/run_problem_set.py               # kelima problem
python tests/run_problem_set.py --problem 1,3 # problem tertentu
python tests/run_problem_set.py --report      # simpan laporan ke .cache/problem_set_report.md
```

Runner menyetel `BNPB_E2E=1` sendiri.

| Problem | Skenario yang diuji |
| --- | --- |
| P1 — Interoperabilitas data | Mapping `HazardEvent` kanonik; `POST /admin/schema-version` v2 menambah `confidence_level` di `attributes` tanpa restart |
| P2 — Concurrency & degradasi | p95 BMKG-only < 300 ms saat PVMBG lambat, beban 50 VU (error < 1%), outage PVMBG → data STALE tanpa 500, pulih tanpa restart |
| P3 — Auth antar-service | Kredensial silang ditolak, MEDIA `view=full` → 403, TTL token 60 detik dan refresh, audit secret |
| P4 — Microservice & storage | Restart satu container tanpa mengganggu endpoint lain, atribut JSONB, batas akses database |
| P5 — Fan-out pub/sub | Outbox → exchange fan-out, consumer independen, buffering saat consumer mati, consumer baru tanpa restart |

#### Skenario Manual Testing
Untuk membantu jalannya testing manual, Anda perlu menjalankan test_menu (sebuah helper interaktif CLI) dari root dengan perintah berikut.
```bash
python tests/test_menu.py
```
##### P1

1. Buka dashboard pada localhost:8080, Anda akan diminta masuk melalui Zitadel. Masukkan kredensial `media-demo`, `lapangan-demo`, atau `bnpb-demo` dengan password yang sudah di generate pada `.env.auth`.
2. Untuk kriteria penyelesaian 1, tekan tombol panah di bagian kanan tiap hazard event untuk melihat ringkasan atau detail penuh (tergantung akun). Event dari kedua sumber: BMKG dan PVMBG telah diformat sesuai format kanonikal. Perhatikan bahwa PVMBG tidak memiliki confidence_level.
3. Untuk kriteria penyelesaian 2, bump schema PVMBG dengan memilih menu pertama di CLI test_menu, masukkan versi 2.
4. Untuk kriteria penyelesaian 3, tunggu hingga event baru pada dashboard muncul dari PVMBG. Buka detailnya dan confidence_level seharusnya ada di attributes list.

##### P2

1. Agak susah dilakukan manual. Untuk kriteria penyelesaian 1 dan 2, gunakan script `tests/load/aggregator.js` dan `tests/load/aggregator-isolation.js` menggunakan k6. Petunjuk lebih lengkap ada di [docs/load-testing.md](docs/load-testing.md).
2. Untuk kriteria penyelesaian 3, simulasikan outage menggunakan menu kedua dari CLI test_menu.
3. Anda bisa tembak API request ke `/api/v1/hazards` via envoy dengan memilih menu 4 di CLI test_menu untuk melihat bahwa PVMBG dinyatakan stale, namun tetap menyajikan data terakhir.
4. Atau Anda juga bisa lihat melalui dashboard bahwa tidak ada data PVMBG baru yang disajikan, dan pada bagian atas, tertulis bahwa PVMBG "terlambat" dengan waktu terakhir sebelum outage tertulis.

##### P3

1. Untuk kriteria penyelesaian 1, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 1. Script akan menunjukkan bahwa kredensial hanya valid untuk masing-masing servis.
2. Untuk kriteria penyelesaian 2, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 2. Script akan menunjukkan bahwa client media tidak diperbolehkan meminta full_view dari event.
3. Untuk kriteria penyelesaian 3, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 3. Script akan menunjukkan token di-refresh setelah TTL, dan token lama tidak dapat digunakan lagi.
4. Alternatif untuk kriteria penyelesaian 3, buka tab Network pada Devtools browser, dan lihat bahwa setelah beberapa waktu, akan ada pemanggila refresh.
5. Untuk kriteria penyelesaian 4, bisa menjalankan perintah seperti `rg -i "(key|token|secret|dsn)=" services/` dari root untuk menemukan apakah ada hardcoding variabel atau tidak.

##### P4
1. Untuk kriteria penyelesaian 1, ketika melakukan docker compose up, terlihat bahwa sistem terdiri dari banyak servis yang terpisah. Anda juga bisa membuka `localhost:3000` untuk membuka dashboard Grafana. Pilih dashboard BNPB yang sudah disediakan, dan Anda dapat melihat availability dari masing-masing servis.
2. Anda bisa menjalankan `docker compose --env-file .env --env-file .env.auth stop notification-service` untuk menghentikan salah satu servis (misal notification-service).
3. Anda bisa menjalankan `docker compose --env-file .env --env-file .env.auth up --build --no-deps notification-service` untuk mem-build ulang notification-service. Selama notification-service, sistem dan dashboard masih tetap bekerja dan melayani permintaan.
4. Untuk kriteria penyelesaian 2, Anda bisa memverifikasi bahwa JSONB digunaka dengan masuk ke Canonical Store menggunakan `docker exec -it tugas-besar-aat-m1-aggregator-db-1 psql -U aggregator -d bnpb_canonical`, kemudian memasukkan `\d hazard_events`.
5. Gunakan menu ke-10 dari CLI test_menu untuk menguji keberdampingan skema v1 dan v2, serta pengujian akses Canonical Store.

##### P5
1. Untuk kriteria penyelesaian 1, buka dashboard Grafana. Kemudian cari salah satu trace ID dari aggregator, kemudian buka detailnya. Terlihat bahwa aggregator mengirim event ke rabbitmq terlebih dahulu, kemudian consumer mengambil dari rabbitmq.
2. Untuk kriteria penyelesaian 2, buka dashboard rabbitmq pada `localhost:15672`, dan buka exchange `bnpb.hazards.v1`. Terlihat bahwa exchange tersebut memiliki 2 queue atau consumers. Buka menu queue masing-masing untuk melihat statistik pengambilan queue masing-masing service yang independen.
3. Untuk kriteria penyelesaian 3, jalankan `docker compose --env-file .env --env-file .env.auth stop notification-service`, kemudian lihat di dashboard rabbitmq bahwa message menumpuk pada queue milik notification-service, sedangkan queue milik live-feed tetap berjalan.
4. Untuk kriteria penyelesaian 4, Anda bisa menambahkan queue dan binding exchange secara manual melalui dashboard rabbitmq tanpa mengganggu sistem.


### Demo interaktif

```bash
python tests/test_menu.py
```

Menu CLI untuk mengubah skema/outage PVMBG, mengambil token, memanggil API lewat
Envoy atau mock langsung, memeriksa RabbitMQ, status container, berlangganan SSE,
serta menjalankan bukti P3/P4. Admin mock juga dapat dipanggil langsung:

```bash
curl -X POST http://localhost:8082/admin/schema-version -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" -H "Content-Type: application/json" -d '{"version":2}'
curl -X POST http://localhost:8082/admin/outage -H "Authorization: Bearer $PVMBG_BEARER_TOKEN" -H "Content-Type: application/json" -d '{"enabled":true}'
```

### Uji beban (k6)

```bash
mkdir -p .cache/load
k6 run tests/load/aggregator.js               # 50 VU, 60 detik
```

Uji isolasi BMKG–PVMBG (P2) memerlukan delay PVMBG tetap 3 detik:

```bash
PVMBG_RESPONSE_DELAY_MIN_MS=3000 PVMBG_RESPONSE_DELAY_MAX_MS=3000 \
  docker compose --env-file .env --env-file .env.auth up -d --no-deps pvmbg-mock
k6 run tests/load/aggregator-isolation.js
```

Jalankan ulang perintah `docker compose` tanpa kedua variabel untuk mengembalikan
delay normal. Parameter `LOAD_*`/`ISO_*` ada di [panduan uji beban](docs/load-testing.md).

### Tes container mock dan Auth

```bash
python tools/test_mock_containers.py --restart-check   # mock yang berjalan, termasuk persistensi
```

Langkah manual mock ada di [panduan testing mock](docs/mock-testing.md); pengujian
login browser, expiry, refresh, dan logout ada di [panduan testing Auth](docs/auth-testing.md).

## Dokumentasi lain

- [BMKG](services/bmkg-mock/README.md), [PVMBG](services/pvmbg-mock/README.md)
- [Kontrak gRPC](contracts/proto/README.md), [OpenAPI gateway](contracts/openapi/gateway.yaml)
- [Envoy](services/envoy/README.md), [Event Live Feed](services/event-live-feed/README.md), [Zitadel](services/zitadel/README.md)
