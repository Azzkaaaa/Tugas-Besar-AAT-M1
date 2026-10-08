# Uji beban aggregator

Script k6 `tests/load/aggregator.js` membebani aggregator dari luar sistem,
persis seperti klien API: request melewati Envoy, token exchange Zitadel,
api-gateway REST, lalu RPC gRPC aggregator. Secara default 50 virtual user
(VU) berjalan paralel selama 60 detik setelah pemanasan 10 detik. Setiap VU
memakai satu koneksi keep-alive tanpa jeda antar-request.

Hasil yang dilaporkan hanya berasal dari fase beban (pemanasan tidak dihitung):
throughput, latensi p50/p95/p99, dan error rate, baik total maupun per endpoint.

## Menjalankan

Prasyarat: [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) v1.x dan
stack `compose.yaml` yang sudah berjalan dengan secret service user di
`.cache/auth/` (lihat [panduan e2e](e2e-testing.md)).

```sh
mkdir -p .cache/load
k6 run tests/load/aggregator.js
```

Jalankan dari root repo. k6 tidak membuat direktori, sehingga `.cache/load/`
harus ada sebelum ringkasan JSON ditulis. Uji singkat untuk memeriksa setup:

```sh
LOAD_VUS=2 LOAD_DURATION=10s k6 run tests/load/aggregator.js
```

Pada `setup()` script menunggu `GET /api/v1/hazards` mengembalikan data (paling
lama `E2E_TIMEOUT`). ID hazard dari respons itu dipakai untuk request detail.

Token client credentials Zitadel hanya berlaku sekitar 60 detik, sehingga setiap
VU mengambil tokennya sendiri dan memperbaruinya 10 detik sebelum kedaluwarsa
(sama seperti `token()` di `tests/e2e_support.py`). Request token ditandai
`kind:token` dan tidak masuk angka yang dilaporkan. Bila pengambilan token gagal,
iterasi dilewati dan dihitung di baris `token failures` (threshold: harus 0).

## Campuran request

| Tag `name` | Proporsi | Request |
| --- | --- | --- |
| `list` | 60 % | `GET /api/v1/hazards?page_size=20`, bergantian tanpa filter, `source=BMKG`, `source=PVMBG` |
| `get` | 30 % | `GET /api/v1/hazards/{hazard_id}` dengan ID acak dari setup |
| `statuses` | 10 % | `GET /api/v1/hazards/sources/statuses` |

Setiap request membawa `X-Correlation-ID` berbentuk
`load-<run>-<vu>-<iterasi>`. ID run dicetak saat setup.

## Konfigurasi

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `LOAD_VUS` | `50` | Jumlah VU paralel (koneksi) |
| `LOAD_DURATION` | `60s` | Lama fase beban yang diukur |
| `LOAD_RAMP` | `10s` | Lama pemanasan (naik bertahap ke `LOAD_VUS`), tidak diukur |
| `LOAD_USER` | `svc-bnpb` | Service user; `svc-media` dan `svc-lapangan` juga bisa |
| `LOAD_OUT` | `.cache/load/aggregator-<waktu>.json` | Ringkasan lengkap k6 (JSON) |
| `E2E_HOST` / `E2E_PORT` | `127.0.0.1` / `8080` | Alamat Envoy |
| `E2E_APP_HOST` | `localhost:8080` | Header `Host` aplikasi (API) |
| `E2E_AUTH_HOST` | `auth.localhost:8080` | Header `Host` Zitadel (token endpoint) |
| `E2E_SECRETS_DIR` | `.cache/auth` | Lokasi secret dan `project-id`; isi dengan path absolut bila diubah |
| `E2E_TIMEOUT` | `120` | Batas tunggu (detik) data awal saat setup |

## Membaca hasil

Contoh keluaran:

```text
  endpoint  requests  throughput  p50      p95      p99      max      error rate
  all       1422      177.8/s     53.2 ms  63.2 ms  66.4 ms  66.8 ms  1.34 %
  list      879       109.9/s     ...
  error responses: HTTP 429: 19
  http_req_failed: 1.34 % (status >= 400 or network error)
```

- **throughput**: jumlah request fase beban dibagi `LOAD_DURATION`.
- **p50/p95/p99**: `http_req_duration`, dari request dikirim sampai respons
  selesai diterima.
- **error rate**: request yang bukan 200 atau body-nya tidak sesuai bentuk
  (`items`, `item.hazard_id`, `freshness`). `http_req_failed` hanya menghitung
  status ≥ 400 dan error jaringan. `error responses` merinci per status;
  `network error` berarti tidak ada respons HTTP sama sekali.
- **thresholds**: p95 < 2 detik dan error < 1 %. Pelanggaran hanya membuat exit
  code k6 bukan 0; angka tetap dilaporkan.

## Catatan

- Setiap request melewati filter `bnpb.token_exchange` Envoy, yang memanggil
  Zitadel. Latensi end-to-end karena itu termasuk Zitadel dan api-gateway.
  Untuk porsi aggregator sendiri, bandingkan dengan metrik server
  `aggregator-service` (`rpc.server.duration`, di Prometheus
  `rpc_server_duration_milliseconds`) di Grafana pada jendela waktu
  yang sama, atau cari trace di Tempo dengan correlation ID run tersebut.
- Request `list` tanpa `page_token` membekukan snapshot baru di aggregator,
  sehingga campuran ini juga mengukur penulisan snapshot ke aggregator-db.
- Status 429 adalah respons yang terdokumentasi di kontrak, tetapi dihitung
  sebagai error.
- Pemanasan dihentikan seketika saat fase beban dimulai, sehingga log server
  bisa memuat beberapa koneksi terputus pada detik itu.

## Isolasi BMKG–PVMBG (P2)

Script `tests/load/aggregator-isolation.js` membuktikan kriteria P2: dengan delay
PVMBG 3 detik, permintaan BMKG-only yang berjalan bersamaan dengan permintaan
PVMBG tetap memiliki **p95 < 300 ms**. Aggregator melayani hazard dari
database-nya sendiri; delay PVMBG hanya memengaruhi polling di latar belakang.
Uji ini menunjukkan bahwa polling PVMBG yang lambat dan pembacaan PVMBG serentak
tidak memperlambat pembacaan BMKG.

Script dan [uji beban di atas](#menjalankan) memakai helper bersama
`tests/load/lib.js`, sehingga konfigurasi `E2E_*`, `LOAD_USER`, `LOAD_OUT`,
pembaruan token, dan cara menghitung error sama.

### Fase

Kedua fase memakai jumlah VU yang sama: grup BMKG yang diukur (`ISO_BMKG_VUS`)
ditambah grup kedua (`ISO_PVMBG_VUS`). Pada baseline grup kedua mengirim request
BMKG, pada fase kontensi request PVMBG. Satu-satunya perbedaan antar-fase adalah
lalu lintas PVMBG, sehingga selisih p95 BMKG tidak tercampur dengan kenaikan
total beban.

| Fase | Grup diukur | Grup kedua | Durasi default |
| --- | --- | --- | --- |
| Pemanasan | BMKG | BMKG | `ISO_RAMP` (5 s), tidak diukur |
| Baseline | BMKG | BMKG | `ISO_BASELINE` (30 s) |
| Kontensi | BMKG | PVMBG | `ISO_DURATION` (60 s) |

Request BMKG-only: `?source=BMKG` (40 %), `?hazard_type=SEISMIC` (30 %), dan
detail hazard BMKG (30 %). Sisi PVMBG memakai campuran yang sama dengan
`source=PVMBG`, `hazard_type=VOLCANIC`, dan hazard PVMBG. Kriteria lulus hanya
p95 grup BMKG yang diukur pada fase kontensi; baseline dilaporkan sebagai pembanding.

### Menjalankan

Buat ulang `pvmbg-mock` dengan delay tetap 3 detik. Variabel shell mengalahkan
nilai dari `--env-file`:

```sh
PVMBG_RESPONSE_DELAY_MIN_MS=3000 PVMBG_RESPONSE_DELAY_MAX_MS=3000 \
  docker compose --env-file .env --env-file .env.auth up -d --no-deps pvmbg-mock
mkdir -p .cache/load
k6 run tests/load/aggregator-isolation.js
```

Setelah selesai, jalankan perintah `docker compose` yang sama tanpa kedua
variabel untuk kembali ke delay 500–3.000 ms. `PVMBG_REQUEST_TIMEOUT_MS`
default 3.500 ms lebih besar dari 3 detik, sehingga polling PVMBG tetap berhasil.

Sebelum beban dimulai, `setup()` mengukur delay mock langsung lewat port
`PVMBG_HTTP_PORT` (default 8082) di `E2E_HOST` dengan `PVMBG_BEARER_TOKEN`.
Keduanya dibaca dari environment atau `.env` root repo dan tidak dicetak. Bila
median tiga request di bawah `ISO_MIN_PVMBG_DELAY_MS`, uji dibatalkan dengan
petunjuk membuat ulang mock. Hasil pengukuran ini dicetak sebagai bukti
konfigurasi.

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `ISO_BMKG_VUS` | `10` | VU grup BMKG yang diukur (kedua fase) |
| `ISO_PVMBG_VUS` | `10` | VU grup kedua: BMKG pada baseline, PVMBG pada kontensi |
| `ISO_RAMP` / `ISO_BASELINE` / `ISO_DURATION` | `5s` / `30s` / `60s` | Durasi pemanasan, baseline, kontensi |
| `ISO_P95_MS` | `300` | Target p95 BMKG-only pada fase kontensi |
| `ISO_MIN_PVMBG_DELAY_MS` | `2900` | Delay mock minimum yang diterima saat probe |
| `PVMBG_BEARER_TOKEN` / `PVMBG_HTTP_PORT` | dari `.env` / `8082` | Akses probe ke mock PVMBG |

### Membaca hasil

```text
  config: 10 measured BMKG VUs + 10 second-group VUs (BMKG in baseline, PVMBG in contention); warm-up 5s, baseline 30s, contention 60s
  PVMBG mock delay (probe, 3 requests): min … ms, median … ms, max … ms
  baseline window:   <mulai> – <selesai>
  contention window: <mulai> – <selesai>
  phase                    requests  throughput  p50  p95  p99  max  error rate
  baseline BMKG            ...
  baseline 2nd group BMKG  ...
  contention BMKG          ...
  contention PVMBG         ...
  PASS: BMKG-only p95 under PVMBG contention = … ms (target < 300 ms, … samples)
  BMKG p95 change vs baseline: +… ms
```

- **PASS/FAIL** membandingkan p95 BMKG fase kontensi dengan `ISO_P95_MS`;
  jumlah sampel adalah jumlah request BMKG pada fase itu. FAIL, atau error BMKG
  ≥ 1 %, membuat exit code k6 bukan 0.
- **Jendela waktu** dihitung dari akhir `setup()` (UTC). Gunakan rentang ini di
  Grafana/Tempo untuk melihat metrik server dan trace pada waktu yang sama.
- **BMKG p95 change vs baseline** menunjukkan pengaruh lalu lintas PVMBG pada
  latensi BMKG pada total beban yang sama. Baris `baseline 2nd group BMKG`
  menunjukkan beban grup kedua selama baseline. Throughput-nya tidak harus sama
  dengan `contention PVMBG`, karena VU tanpa jeda menghasilkan request sesuai
  latensi masing-masing.
- Latensi tetap mencakup token exchange Envoy dan api-gateway (lihat catatan di
  atas). Jumlah VU default sengaja sedang agar angka mencerminkan isolasi, bukan
  kejenuhan edge.
