# IF4031 — Platform Koordinasi Kebencanaan, Milestone 1

Kerangka monorepo dan kontrak API berdasarkan **IF4031 - Tugas Besar - M1.pdf**
serta diagram BMKG–PVMBG–BNPB. Bahasa, framework, dan database belum ditentukan.
Service aplikasi belum diimplementasikan; Compose saat ini hanya menjalankan
penampil kontrak. Ini belum merupakan implementasi P1–P5.

## Mulai dari sini

Gunakan Node.js 24 dan npm 10 atau lebih baru untuk tooling kontrak:

```sh
npm ci
npm run check
npm run bundle
```

`check` memvalidasi OpenAPI, melint dan memeriksa format proto, lalu mengompilasi
semua proto menggunakan Buf. `bundle` menghasilkan OpenAPI mandiri di
`dist/openapi/` (diabaikan Git). CI GitHub menjalankan pemeriksaan yang sama.
Node.js di repo ini hanya untuk tooling, bukan pilihan bahasa service.

Untuk membaca ketiga REST API melalui Swagger UI, jalankan Docker Desktop/Engine
dengan Compose v2, kemudian:

```sh
docker compose up -d
```

Buka <http://localhost:8090>, lalu pilih **BNPB Gateway**, **BMKG Mock**, atau
**PVMBG Mock**. Tidak perlu kredensial untuk membaca dokumentasi. *Try it out*
baru dapat mengakses API setelah service diimplementasikan. Untuk menghentikan
penampil: `docker compose down`.

Alternatif dokumen HTML tanpa Docker:

```sh
npm run docs:gateway
npm run docs:bmkg
npm run docs:pvmbg
```

Hasilnya di `dist/docs/`. Di Windows PowerShell, gunakan `npm.cmd` bila execution
policy memblokir `npm.ps1`.

## Peta repo

```text
contracts/
  openapi/                     # bmkg.yaml, pvmbg.yaml, gateway.yaml
  proto/bnpb/
    common/v1/                 # kanonik, proyeksi, filter, freshness
    aggregator/v1/             # baca Canonical Store melalui gRPC
    auth/v1/                   # login, refresh, logout, authorize
    feed/v1/                   # server stream seismik dan vulkanik
    notification/v1/           # baca notifikasi
    event/v1/                  # envelope RabbitMQ
services/
  bmkg-mock/                   # REST, storage milik BMKG
  pvmbg-mock/                  # REST, schema switch dan outage
  aggregator/                 # polling, normalisasi, store, publish
  auth/                       # identitas, token, scope, sesi
  gateway/                    # REST/SSE client; adapter gRPC
  seismic-live-feed/           # consumer, replay seismik
  volcanic-live-feed/          # consumer, replay vulkanik
  danger-notification/        # consumer, penyimpanan notifikasi
frontend/                     # lokasi SPA; framework belum dipilih
deploy/                       # rencana deployment dan observability
docs/
  architecture.md             # ownership dan keputusan desain
  data-mapping.md             # aturan transformasi wajib dari PDF
  configuration.md            # konfigurasi dan pembagian secrets
  milestone-plan.md           # checklist implementasi/demo P1–P5
.github/workflows/contracts.yml
compose.yaml                  # penampil kontrak saja
```

## Kontrak utama

| Boundary | Kontrak | Port rencana |
| --- | --- | --- |
| Aggregator → BMKG | [bmkg.yaml](contracts/openapi/bmkg.yaml) | HTTP 8081 |
| Aggregator → PVMBG | [pvmbg.yaml](contracts/openapi/pvmbg.yaml) | HTTP 8082 |
| Client / SPA → Gateway | [gateway.yaml](contracts/openapi/gateway.yaml) | HTTP 8080 |
| Gateway → service BNPB | [proto dan aturan transport](contracts/proto/README.md) | gRPC 9091–9095 |
| Aggregator → RabbitMQ → consumer | [event.proto](contracts/proto/bnpb/event/v1/event.proto) | AMQP 5672 |

Endpoint mock mengikuti PDF tanpa prefiks tambahan. Endpoint client memakai
`/api/v1`: `auth/login`, `auth/refresh`, `auth/logout`, daftar/detail `hazards`,
SSE `feeds/seismic` dan `feeds/volcanic`, serta daftar/detail `notifications`.
Health REST menggunakan `/health`; gRPC memakai `grpc.health.v1.Health` standar.

Client bersifat read-only. Default `view=summary` hanya berisi tujuh field
ringkasan. `view=full` memerlukan scope tambahan; permintaan Media terhadap full
ditolak 403. Refresh token dirotasi dan membatalkan access token sesi lama.
Data tersimpan dapat dilayani saat sumber outage dengan metadata freshness.

## Konfigurasi dan implementasi berikutnya

Salin `.env.example` ke `.env` dengan `cp .env.example .env` atau PowerShell
`Copy-Item .env.example .env`. Nilai `REPLACE_*` adalah placeholder, bukan
kredensial yang boleh diterima service. Daftar variabel dan pemiliknya tersedia
di [konfigurasi](docs/configuration.md). Port dapat diubah di deployment;
sesuaikan URL server OpenAPI saat menjalankan API di port lain.

Setiap folder service berisi batas tanggung jawab. Setelah bahasa dipilih,
tambahkan dependency manifest, Dockerfile, health check, dan test masing-masing,
lalu orkestrasi sistem lengkap. Rencana jaringan dan observability ada di
[deploy](deploy/README.md). Tidak ada database atau library business logic yang
dibagikan lintas service.

Ikuti [CONTRIBUTING.md](CONTRIBUTING.md) untuk perubahan kontrak dan
[checklist M1](docs/milestone-plan.md) untuk demo dan bukti yang masih perlu dibuat.
Validasi kontrak belum membuktikan performa, ketahanan, atau otorisasi runtime.

Tooling mengikuti dokumentasi resmi [Redocly](https://redocly.com/docs/cli/installation),
[Buf](https://buf.build/docs/cli/quickstart/), dan
[Swagger UI Docker](https://swagger.io/docs/open-source-tools/swagger-ui/usage/installation/).
