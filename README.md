# IF4031 — Platform Koordinasi Kebencanaan, ARLOW

Platform BNPB yang mengagregasi data bahaya dari **BMKG** (gempa/tsunami),
**PVMBG** (vulkanik), **Feed Satelit** (stream SSE) dan **Sensor Pantai**,
menyimpannya dalam bentuk kanonik di Cassandra, lalu mendistribusikannya ke API
REST, live feed SSE, webhook, push notification, dan pengiriman critical warning
ke Tim Lapangan. Login memakai **Zitadel** (OIDC). Dashboard dibangun dengan SPA
**React + TypeScript**.

## Dua repository

| Repository | Isi |
| --- | --- |
| **services** (repository ini) | Kode sumber semua komponen, kontrak gRPC/OpenAPI, mock BMKG/PVMBG, `.env.example`, stack Docker Compose untuk pengembangan lokal, dan tes |
| [**infrastructure**](https://gitlab.com/aat-nasigoreng/infrastructure) | Deployment Milestone 2 di k3s (Flux, Helm, Calico), skrip setup node, deklarasi topologi, konfigurasi cluster (Secret/ConfigMap), cara memicu skenario pengujian, dan bukti pengujian |

**Deployment demo M2, deklarasi topologi, variabel konfigurasi cluster, dan
cara memicu setiap skenario pengujian hanya didokumentasikan di repository
infrastructure.** Repository ini tidak mengulanginya. Lihat:

| Kebutuhan | Dokumen di repository infrastructure |
| --- | --- |
| Menjalankan sistem dari awal (satu perintah deploy) | [README: Menjalankan dari awal](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#menjalankan-dari-awal) |
| Menyiapkan node k3s (per node, Tailscale atau LAN) | [m2/k3s/nodes/README.md](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/m2/k3s/nodes/README.md) |
| Deklarasi topologi (node, jenis, host fisik, CPU/RAM) | [README: Deklarasi topologi](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#deklarasi-topologi) |
| Variabel konfigurasi cluster (Secret/ConfigMap/values) | [README: Variabel konfigurasi M2](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#variabel-konfigurasi-m2) |
| Memicu skenario pengujian P1–P5 (Tester Agent, Audit API) | [README: Memicu skenario pengujian](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#memicu-skenario-pengujian) dan [prosedur uji](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/docs/m2-demo-runbook.md) |
| Hasil pengukuran dan batas pembuktian | [indeks bukti M2](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/docs/m2-report-evidence-2026-11-22.md) |

> **Age key development.** Secret deployment di repository infrastructure
> dienkripsi dengan SOPS/age. Age key development tidak disimpan di repository
> mana pun. **Hubungi kami (tim ARLOW) bila memerlukan age key development.**
> Stack Compose lokal di repository ini tidak memerlukannya.

## Komponen

| Komponen | Teknologi | Peran |
| --- | --- | --- |
| `aggregator-service` | Go + Cassandra 5 | Polling BMKG/PVMBG/Sensor Pantai dan stream Feed Satelit; store kanonik (Cassandra: 4 anggota, RF=3, Paxos v2 di cluster); lease leader per job dengan fencing epoch; outbox ke RabbitMQ; gRPC `:9091`; Audit/Ops HTTP internal `:9092` |
| `field-report-worker`, `critical-notifier` | Go (image aggregator, mode berbeda) | Worker laporan lapangan (P3); pengiriman critical warning ke Tim Lapangan beserta receipt bertanda tangan (P5) |
| `aggregator` tools | Go (`services/aggregator/cmd/*`) | `storeadmin` (migrasi/indeks), `storecopy`, `m2replay`, `critical-export`/`critical-reconcile`, `critical-receiver` (receiver Tim Lapangan) |
| `bmkg-mock`, `pvmbg-mock` | Python 3.12 + SQLite | API sumber eksternal tiruan; PVMBG mendukung skema v1–v5 (`/admin/schema-version`) dan outage |
| Feed Satelit, Sensor Pantai | Layanan/image resmi dari asisten | Feed Satelit: `https://satellite.naufarrel.dev`. Sensor Pantai: image `sustuna/sensor-pantai` (digest dipin di infrastructure) |
| `feed-service` | Rust + Redis | Event Live Feed: consumer RabbitMQ, stream gRPC (`:9093`), buffer replay di Redis |
| `webhook-provider` | Go + PostgreSQL + Redis | Langganan webhook dan pengiriman bertanda tangan HMAC (`:9094`) |
| `notification-service` | .NET 10 + PostgreSQL | Danger Notification, push FCM (`:9095`) |
| `api-gateway` | Go | REST `/api/v1` dari OpenAPI; admission dengan slot cadangan untuk Field Team/BNPB (Media ditolak terkendali dengan 429) |
| `envoy` | Envoy | Satu-satunya pintu masuk publik |
| `token-exchange`, `zitadel-claims` | Rust | Token exchange `ext_authz` Envoy; klaim `role`/`scopes` Zitadel |
| `auth-bridge`, `zitadel`, `auth-db`, `auth-redis` | Node.js, Zitadel, PostgreSQL, Redis | OIDC, cookie HttpOnly, sesi |
| `spa-server` | React + nginx | Frontend dan dashboard analitik |
| `rabbitmq` | RabbitMQ 4 | Exchange fan-out `bnpb.hazards.v1` |
| `otel-lgtm`, `blackbox` | Grafana LGTM | Trace, metrics, log, health probe |

Catatan:
- Store milik tiap service hanya dapat diakses service pemiliknya (NetworkPolicy
  di cluster): `notification-db`, `webhook-db`, `webhook-redis`, `feed-redis`
  dan `aggregator-db`.
- `aggregator-db` (PostgreSQL) adalah backend kanonik alternatif
  (`AGGREGATOR_STORE_BACKEND=postgres`). Deployment M2 memakai Cassandra.

## Menjalankan

- **Demo Milestone 2 (Kubernetes, k3s):** lihat [repository infrastructure](https://gitlab.com/aat-nasigoreng/infrastructure),
  bagian [Menjalankan dari awal](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#menjalankan-dari-awal). Image
  dibangun dari repository ini, lalu dipublikasikan sebagai snapshot GitOps.
- **Pengembangan M2 penuh di Kind** (cluster multi-node lokal; tidak sah untuk demo):
  [m2/kind/README.md](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/m2/kind/README.md) di repository infrastructure.
- **Pengembangan lokal dengan Docker Compose:** langkah di bawah. Stack ini
  memakai satu node Cassandra dan cocok untuk mengembangkan atau menguji satu
  service. Ia tidak menggantikan cluster demo.

## Pengembangan lokal (Docker Compose)

### Prasyarat

| Kebutuhan | Versi | Keterangan |
| --- | --- | --- |
| Docker Engine / Docker Desktop | terbaru | Dengan plugin **Docker Compose v2** (`docker compose`, mendukung `--wait`) |
| Python | 3.12+ | Helper setup dan tes; hanya standard library |
| Node.js + npm | 22.12+ | Opsional: `buf` untuk generasi kode .NET (langkah 2); dapat diganti image Docker `bufbuild/buf` |
| Akses internet ke `buf.build` | — | Plugin `buf` untuk C# bersifat remote; juga diperlukan saat build image |
| Kredensial Firebase | — | File service account JSON di `.secrets/firebase-credentials.json`; `notification-service` gagal start tanpa file ini |
| k6 | 1.x | Hanya untuk uji beban ([instalasi](https://grafana.com/docs/k6/latest/set-up/install-k6/)) |

Port host yang dipakai:
- `8080` (Envoy) dan `8081`/`8082` (mock);
- `9091`, `9093`–`9095` (gRPC);
- `5432` (PostgreSQL aggregator) dan `9042` (Cassandra);
- `5672`/`15672` (RabbitMQ);
- `3000`, `4317`, `4318`, `9090` (otel-lgtm).

Pastikan port tersebut kosong.

Toolchain untuk membangun di luar Docker (opsional): Go 1.26, Rust 1.96,
.NET SDK 10.0, dan Node.js 22.12+ (Nx, buf, openapi-generator lewat `npm ci`).

### 1. Siapkan konfigurasi

```bash
python tools/setup_mock_env.py   # membuat .env dari .env.example, mengisi BMKG_API_KEY & PVMBG_BEARER_TOKEN
python tools/setup_auth_env.py   # membuat .env.auth dari .env.auth.example, mengisi secret Auth
```

Kedua helper hanya mengganti nilai kosong atau placeholder. Setelah itu, **edit `.env` secara manual**:
- `AGGREGATOR_DB_PASSWORD`, `NOTIFICATION_DB_PASSWORD`, `WEBHOOK_DB_PASSWORD`:
  isi password apa pun.
- `AGGREGATOR_SERVICE_TOKEN` dan `WEBHOOK_SERVICE_TOKEN`: ganti placeholder
  `REPLACE_*` dengan secret acak berbeda, misalnya
  `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
- Opsional, untuk sumber M2: `SATELLITE_TOKEN`, `PANTAI_AUTH_TOKEN`, `AUDIT_API_TOKEN`.

Letakkan service account Firebase di `.secrets/firebase-credentials.json`.
`.env`, `.env.auth`, `.secrets/` dan `.cache/` tidak masuk Git.

### 2. Generasi kode gRPC .NET

`notification-service` menyalin kode C# hasil generasi dari host. Jalankan:

```bash
npm ci && npx nx run-many -t generate-proto -p 'bnpb-*-dotnet'
# atau tanpa Node.js:
for p in common event aggregator notification; do
  docker run --rm -v "$PWD:/workspace" -w /workspace/packages/bnpb-$p-dotnet bufbuild/buf:1.73.0 generate
done
```

### 3. Build dan jalankan

```bash
docker compose --env-file .env --env-file .env.auth up --build -d --wait
docker compose --env-file .env --env-file .env.auth --profile manual up --build -d --wait     # + webhook-provider
docker compose --env-file .env --env-file .env.auth --profile problem3 up --build -d --wait   # + field-report-worker (P3)
```

`auth-bootstrap` memprovisikan Zitadel sekali jalan: akun seed, client OIDC, dan
service user untuk tes (secret ditulis ke `.cache/auth/`).

### 4. Akses

| URL | Isi |
| --- | --- |
| `http://localhost:8080` | SPA. Masuk dengan `media-demo`, `lapangan-demo` atau `bnpb-demo`; password ada di `.env.auth` |
| `http://localhost:8080/api/v1` | REST API ([`contracts/openapi/gateway.yaml`](contracts/openapi/gateway.yaml)) |
| `http://auth.localhost:8080` | Zitadel |
| `http://localhost:8081`, `http://localhost:8082` | Mock BMKG (`X-BMKG-Key`) dan PVMBG (`Authorization: Bearer`) |
| `http://localhost:15672` | RabbitMQ management |
| `http://localhost:3000` | Grafana, dashboard **BNPB Health & Traces** |

Operasi:
- `docker compose --env-file .env --env-file .env.auth ps`, `logs -f <service>`
  dan `down` (`down -v` untuk reset total).
- Jangan mengganti `ZITADEL_MASTERKEY` selama volume `auth_postgres` masih ada.

Mode parsial:
- Hanya mock: `docker compose -f compose.mocks.yaml up --build -d`.
- Auth + SPA: `compose.auth.yaml`; lihat [Auth bridge](services/auth-bridge/README.md).
- SPA tanpa backend: [frontend](services/frontend/README.md).

## Konfigurasi (`.env`, pengembangan lokal)

Variabel konfigurasi **cluster** (Secret/ConfigMap/values Helm) didokumentasikan di
[repository infrastructure](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#variabel-konfigurasi-m2). Tabel di bawah
adalah variabel `.env` untuk Compose lokal, dengan nama yang sama seperti di container.

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
| `NOTIFICATION_DB_PASSWORD` | kosong → **isi manual** | Password PostgreSQL `notification-db` (notifikasi + outbox push); Compose menyusun connection string-nya |
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
| `GATEWAY_GRPC_RESOLVE_INTERVAL_SECONDS` | `30` | Interval resolve ulang nama backend untuk menemukan replika baru (minimal efektif 30 detik) |
| `FEED_REPLAY_RETENTION_SECONDS` / `FEED_REPLAY_MAX_ENTRIES` | `600` / `10000` | Buffer replay feed (Redis di Compose) |
| `FEED_REDIS_URL` | Compose: `redis://feed-redis.local:6379/0` | Kosong di luar Compose = buffer di memori, satu replika |
| `WEBHOOK_MAX_RETRIES` / `WEBHOOK_HTTP_TIMEOUT_MS` | `5` / `10000` | Retry dan timeout pengiriman webhook |
| `OUTBOX_MAX_RETRIES` | `5` | Retry publish outbox aggregator |
| `OUTBOX_RETENTION_HOURS` | `168` | Lama baris outbox aggregator yang sudah terbit/di-dead-letter disimpan |

### `.env` — penyimpanan dan RabbitMQ

| Variabel | Default | Arti |
| --- | --- | --- |
| `BMKG_STORE_DSN` / `PVMBG_STORE_DSN` | `sqlite:///data/bmkg.sqlite3` / `…pvmbg.sqlite3` | File SQLite tiap mock (harus berbeda) |
| `AGGREGATOR_DB_PASSWORD` | kosong → **isi manual** | Password PostgreSQL store kanonik aggregator |
| `AGGREGATOR_STORE_BACKEND` | `cassandra` | Backend store kanonik aggregator: `cassandra` atau `postgres` |
| `AGGREGATOR_CASSANDRA_BUCKET_PERIOD` | `1m` | Rentang waktu awal satu partisi hazard di Cassandra (keyspace baru); ubah saat berjalan dengan `storeadmin`, lihat README aggregator |
| `AGGREGATOR_CASSANDRA_CONSISTENCY` | `LOCAL_QUORUM` | Consistency baca/tulis Cassandra |
| `AGGREGATOR_CASSANDRA_REPLICATION_STRATEGY` | `SimpleStrategy` | `SimpleStrategy` atau `NetworkTopologyStrategy`; hanya saat keyspace dibuat |
| `AGGREGATOR_CASSANDRA_REPLICATION_FACTOR` | `1` | Replika untuk `SimpleStrategy`; compose (1 node) harus 1 |
| `AGGREGATOR_CASSANDRA_DATACENTERS` | kosong | Replika per datacenter untuk `NetworkTopologyStrategy`, mis. `dc1:3,dc2:3` |
| `AGGREGATOR_CASSANDRA_LOCAL_DC` | kosong | Datacenter lokal untuk consistency `LOCAL_*` |
| `AGGREGATOR_LEADER_ELECTION` | kosong (ikut backend store) | Pemilihan leader aggregator: `postgres`, `cassandra`, `kubernetes`, atau `single` |
| `WEBHOOK_DB_PASSWORD` | kosong → **isi manual** | Password PostgreSQL `webhook-db`; Compose menyusun `WEBHOOK_STORE_DSN` |
| `WEBHOOK_DEDUP_TTL_HOURS` | `168` | Lama penanda "sudah terkirim" di Redis untuk dedup webhook |
| `WEBHOOK_SHUTDOWN_GRACE_SECONDS` | `25` | Waktu event webhook yang sedang dikirim boleh selesai setelah SIGTERM; harus di bawah `terminationGracePeriodSeconds` pod |
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

### `.env` — Milestone 2 (sumber baru, audit, kepemimpinan, critical delivery)

| Variabel | Default | Arti |
| --- | --- | --- |
| `SATELLITE_BASE_URL`, `SATELLITE_TOKEN` | kosong | Feed Satelit; `SATELLITE_BASE_URL` kosong = job satelit nonaktif. `SATELLITE_TOKEN` adalah token kelompok |
| `SATELLITE_STREAM_ID` | `satellite-practice-v1` | ID stream checkpoint; ganti bila sesi upstream di-reset |
| `SATELLITE_ALLOWED_LATENESS`, `SATELLITE_STATE_RETENTION`, `SATELLITE_MAX_DETECTIONS`, `SATELLITE_SNAPSHOT_EVERY` | lihat `.env.example` | Batas keterlambatan, horizon koreksi, batas deteksi aktif, jarak snapshot |
| `SATELLITE_STATE_DIR` | kosong (temp OS) | State turunan mesin satelit di disk lokal yang dapat ditulis |
| `PANTAI_BASE_URL`, `PANTAI_AUTH_TOKEN` | kosong | Sensor Pantai |
| `PANTAI_POLL_INTERVAL`, `PANTAI_REQUEST_TIMEOUT`, `PANTAI_STALE_AFTER`, `PANTAI_BREAKER_FAILURES`, `PANTAI_BREAKER_OPEN_FOR` | lihat `.env.example` | Polling, kesegaran dan circuit breaker Sensor Pantai |
| `PVMBG_SCHEMA_ADAPTERS` | `false` | Aktifkan adapter skema PVMBG v4/v5 yang dideklarasikan (P3) |
| `AUDIT_API_TOKEN` (`AUDIT_TOKEN` alias lama) | kosong | Token Audit API (P3–P5) |
| `AGGREGATOR_M2_HTTP_ADDRESS` | kosong | Listener HTTP internal Audit/Ops (cluster: `:9092`) |
| `INGEST_MAX_RESPONSE_BYTES`, `M2_MAX_BODY_BYTES` | `16777216`, `2097152` | Batas ukuran respons upstream dan body HTTP M2 |
| `AGGREGATOR_REQUIRE_WRITE_AUTHORITY` | `false` | `true` mewajibkan fencing epoch lease di setiap tulis. Menolak election `kubernetes`/`single`. Wajib di cluster M2 |
| `AGGREGATOR_LEADER_YIELD_PER_HOUR`, `AGGREGATOR_LEADER_UNREACHABLE_AFTER` | `1`, `3` | Pelepasan lease kooperatif dan serah lease setelah upstream gagal berturut-turut (0 = mati) |
| `AGGREGATOR_REQUIRED_NOTIFICATION_QUEUE` | `bnpb.danger-notification.v1` | Queue notifikasi yang wajib terhubung langsung |
| `CANONICAL_STORE_CASSANDRA_NODE_FROM_RACK` | `false` | Ambil nama node dari rack Cassandra (lokasi replika di Audit) |
| `CRITICAL_DELIVERY_API_URL`, `CRITICAL_DELIVERY_TOKEN` | `http://aggregator-service:9092`, kosong | API internal pengiriman critical |
| `CRITICAL_RECEIVER_ID`, `_URL`, `_SECRET`, `_PUBLIC_KEY`, `_PRIVATE_KEY` | kosong | Identitas receiver Tim Lapangan; kunci dibuat dengan `cmd/critical-receiver`. Kunci privat tetap di klien Tim Lapangan |
| `GATEWAY_RESERVED_IN_FLIGHT` | `25` | Slot in-flight gateway yang dicadangkan untuk peran prioritas |

## Pengujian

### Unit dan tes proses (tanpa Docker)

```bash
python -m unittest discover -s tests -v       # mock BMKG/PVMBG; tes e2e/problem otomatis dilewati
python -m unittest discover -s services/bmkg-mock -p test_storage.py -v
python -m unittest discover -s services/pvmbg-mock -p test_storage.py -v
npx nx run-many -t test                       # tes per service (butuh toolchain pengembangan)
npx nx run-many -t lint                       # lint per service; mock Python butuh ruff (lihat ci/runner)
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

### Skenario Milestone 1 (regresi lokal)

Skenario P1–P5 Milestone 1 (runner otomatis `tests/run_problem_set.py`, menu CLI
`tests/test_menu.py`, dan langkah manual) ada di [docs/m1-problem-set.md](docs/m1-problem-set.md).
Skenario Milestone 2 dijalankan di cluster. Lihat
[repository infrastructure](https://gitlab.com/aat-nasigoreng/infrastructure/-/blob/main/README.md#memicu-skenario-pengujian).

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

## Dokumentasi

- Komponen:
  - mock [BMKG](services/bmkg-mock/README.md) dan [PVMBG](services/pvmbg-mock/README.md);
  - [Aggregator](services/aggregator/README.md);
  - [Envoy](services/envoy/README.md), [Event Live Feed](services/event-live-feed/README.md),
    [Zitadel](services/zitadel/README.md), [token exchange](services/token-exchange/README.md).
- Kontrak: [gRPC](contracts/proto/README.md) dan [OpenAPI gateway](contracts/openapi/gateway.yaml).
- Milestone 2:
  - [Problem 3: Surat dari Tetangga](docs/m2-problem-3.md);
  - [Problem 4 dan 5](docs/m2-problems-4-5.md);
  - [integrasi P3 dengan P4–P5](docs/m2-integration.md).
- Mekanisme M2:

  | Topik | Dokumen |
  | --- | --- |
  | Kepemimpinan dan fencing | [write authority](docs/m2-write-authority.md) |
  | Audit | [Audit API](docs/m2-audit.md), [halaman metadata Audit](docs/m2-audit-metadata-pages.md) |
  | Pembacaan | [partial reads](docs/m2-partial-reads.md), [List terbatas](docs/m2-list-payload-budget.md), [admission baca](docs/m2-read-admission.md), [analitik](docs/m2-analytics.md) |
  | Satelit | [state](docs/m2-satellite-state.md), [snapshot streaming](docs/m2-streaming-snapshots.md), [replay](docs/m2-replay.md), [rekaman besar](docs/m2-large-satellite-records.md) |
  | Pengiriman | [critical delivery](docs/m2-critical-delivery.md), [webhook fanout](docs/m2-webhook-fanout.md), [feed replay](docs/m2-feed-replay.md) |
  | Payload | [payload gagal](docs/m2-failed-payloads.md), [outbox](docs/m2-outbox-payload-budget.md) |
  | Notifikasi | [RPC](docs/m2-notification-rpc-budget.md), [outbox memori](docs/m2-notification-outbox-memory.md), [SUMMARY](docs/m2-notification-summary.md) |
  | Observabilitas | [log dan korelasi](docs/m2-observability.md) |

- Pengujian: [e2e](docs/e2e-testing.md), [uji beban](docs/load-testing.md),
  [mock](docs/mock-testing.md), [Auth](docs/auth-testing.md).
