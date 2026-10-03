# IF4031 — Platform Koordinasi Kebencanaan, Milestone 1

Mock **BMKG dan PVMBG sudah diimplementasikan** menggunakan Python 3.12 dan
SQLite. Keduanya berjalan sebagai service REST terpisah dengan database,
kredensial, generator data, dan Dockerfile masing-masing. Komponen BNPB lainnya
masih berupa kerangka/kontrak. Frontend **SPA React + TypeScript** sudah memiliki
halaman login mock, dashboard kejadian, dan notifikasi dengan fixture Gateway.
Login OIDC dan koneksi ke backend BNPB belum terintegrasi.

## Menjalankan frontend

Gunakan Node.js 22.12+ dan jalankan dari root repository:

```powershell
npm --prefix services/frontend ci
npm --prefix services/frontend run dev
```

Buka `http://localhost:5173`, lalu pilih akun pratinjau Media, Tim Lapangan, atau
BNPB Pusat. Mode demo ini tidak membutuhkan service lain atau konfigurasi secret.
Alternatif memakai Docker:

```powershell
docker compose -f compose.frontend.yaml up --build -d --wait
```

Panduan implementasi, skenario pengujian, konfigurasi, dan integrasi Auth ada di
[README frontend](services/frontend/README.md). Compose utama membangun frontend
dengan demo nonaktif; Compose khusus frontend secara eksplisit mengaktifkan demo.

## Menjalankan dua mock

Prasyarat: Python 3.12+ untuk helper lokal; Docker Desktop/Engine dan Compose
untuk container. Runtime Python hanya menggunakan standard library, tanpa pip.
Jalankan dari root repository:

```bash
python tools/setup_mock_env.py
docker compose -f compose.mocks.yaml up --build -d
```

Helper membuat `.env` dari contoh dan mengganti **hanya** kredensial mock yang
kosong/placeholder dengan nilai acak berbeda. Secret yang sudah Anda isi tetap
dipertahankan dan tidak dicetak. `.env` tidak masuk Git. Alternatifnya salin
`.env.example` sendiri dan isi `BMKG_API_KEY` serta `PVMBG_BEARER_TOKEN` berbeda.

| Service | URL host | Autentikasi data/admin | Storage privat |
| --- | --- | --- | --- |
| BMKG | `http://localhost:8081` | `X-BMKG-Key: <BMKG_API_KEY>` | volume `bmkg_data` |
| PVMBG | `http://localhost:8082` | `Authorization: Bearer <PVMBG_BEARER_TOKEN>` | volume `pvmbg_data` |

`GET /health` publik pada keduanya. Port host dapat diubah melalui
`BMKG_HTTP_PORT`/`PVMBG_HTTP_PORT` di `.env`; port **di dalam container selalu
8080**. Aggregator pada jaringan `internet_mock_net` nantinya memakai
`http://bmkg-mock:8080` dan `http://pvmbg-mock:8080`, tanpa akses volume SQLite.
Compose hanya menyuntikkan kredensial milik service penerima.

```bash
docker compose -f compose.mocks.yaml logs -f
docker compose -f compose.mocks.yaml restart pvmbg-mock
docker compose -f compose.mocks.yaml stop pvmbg-mock
docker compose -f compose.mocks.yaml up -d pvmbg-mock
docker compose -f compose.mocks.yaml down
```

Data tetap ada setelah restart/down biasa. Jangan memakai `down -v` bila ingin
mempertahankan data. `compose.yaml` utama mewarisi definisi mock yang sama,
tetapi service BNPB placeholder di sana masih perlu implementasi/build image;
gunakan `compose.mocks.yaml` untuk menjalankan hasil pekerjaan ini.

Tanpa Docker, gunakan dua terminal terpisah setelah helper konfigurasi:

```bash
python tools/run_mock.py bmkg
```

```bash
python tools/run_mock.py pvmbg
```

Helper membaca konfigurasi milik masing-masing service dari `.env`; environment
shell mengambil prioritas. Database lokal ada di `data/bmkg.sqlite3` dan
`data/pvmbg.sqlite3`. Ctrl+C menghentikan service. Untuk menjalankan `app.py`
secara langsung, set environment sendiri; entrypoint tidak membaca `.env`.

## Perilaku dan demo

- BMKG menyediakan `GET /seismic-events?since=...` dan
  `GET /tsunami-warnings?since=...`, delay tetap default 100 ms.
- PVMBG menyediakan `GET /volcanic-reports?since=...`, delay acak default
  500–3000 ms. `POST /admin/schema-version` dengan `{"version":2}` membuat
  laporan baru memiliki `confidence_level` tanpa restart/migrasi SQLite.
- `POST /admin/outage` dengan `{"enabled":true}` membuat data/health PVMBG
  mengembalikan 503. `{"enabled":false}` memulihkan proses yang sama; admin
  tetap tersedia dan BMKG tetap berjalan.
- Masing-masing membuat ≥20 record historis dan satu event baru setiap 10
  detik secara default. Data tersimpan permanen; `since` inklusif dan tanpa
  parameter tersebut seluruh data dikembalikan. Tidak ada push ke BNPB.

Panduan request, konfigurasi, skenario demo, dan penjelasan desain ada di
[implementasi mock](docs/mock-services.md), [BMKG](services/bmkg-mock/README.md),
[PVMBG](services/pvmbg-mock/README.md), dan
[inventaris konfigurasi](docs/configuration.md).

## Pengujian

Untuk menguji container yang sudah berjalan, termasuk persistensi setelah restart:

```bash
python tools/test_mock_containers.py --restart-check
```

Langkah manual PowerShell beserta hasil yang diharapkan ada di
[panduan testing Docker](docs/mock-testing.md). Script mengembalikan PVMBG ke
skema v1 dan outage nonaktif; laporan tersimpan di
`.cache/mock-container-test-results.json`.

Pengujian proses Python tanpa Docker:

```bash
python -m unittest discover -s tests -v
python -m unittest discover -s services/bmkg-mock -p test_storage.py -v
```

Pengujian menjalankan proses Python terpisah lewat jaringan HTTP dengan port
sementara, SQLite sementara, dan secret acak. Skenario mencakup kontrak data,
autentikasi silang, filter waktu, generator, perubahan skema/outage saat berjalan,
persistensi setelah restart, serta isolasi request lambat. Tidak membutuhkan
Docker, library tambahan, atau database yang sedang Anda gunakan.

## Pengembangan kontrak BNPB

Bagian berikut dipakai ketika mengembangkan service BNPB; tidak diperlukan
untuk menjalankan mock. Prasyarat: buf.

```bash
buf build -o services/envoy/bnpb_descriptor.pb --exclude-imports=false
buf generate
```

Lihat [arsitektur](docs/architecture.md), [pemetaan data](docs/data-mapping.md),
dan [rencana M1](docs/milestone-plan.md). Pengujian mock belum membuktikan kinerja
atau seluruh kriteria P1–P5 sistem BNPB.
