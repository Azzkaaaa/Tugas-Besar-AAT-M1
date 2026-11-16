# Milestone 2 — Problem 4 dan 5

## Dependensi dan cakupan

P4/P5 dapat dikerjakan paralel dengan P3. Keduanya menggunakan penyimpanan
kanonik dan leader election P1/P2, kemudian outbox RabbitMQ dan Danger
Notification dari M1. P3 tidak diperlukan untuk mulai menulis adapter sumber.
Titik penggabungan dengan P3 adalah pesan gagal, autentikasi Internal Ops,
serta endpoint `GET /internal/audit/unprocessed`.

Kode ini menambahkan dua job di Aggregator: `satellite-stream` dan
`coastal-poller`. Masing-masing memiliki lease dan HTTP client sendiri.
Tidak ada service lain yang mengakses tabel milik Aggregator.

**Status:** implementasi dan pengujian lokal tersedia. Kelulusan seluruh kriteria
demo belum diklaim: uji node/kernel terpisah, referensi asisten, latensi sampai
perangkat penerima, dan game day gabungan memerlukan cluster serta bahan demo.
Infrastructure lokal yang diperiksa masih memakai desain M1 Kind/dev/prod.
Adanya kode Cassandra/lease di services tidak membuktikan seluruh P1/P2 sudah
selesai pada cluster.

## P4: event time, dynamic connected components, dan replay

- Kunci deteksi: `satellite|acq_date|acq_time|latitude|longitude`. Representasi
  angka asli dipertahankan, tanpa pembulatan atau zero-padding pada kunci.
- Waktu deteksi berasal dari tanggal/jam UTC VIIRS, bukan waktu penerimaan.
- Confidence `l/low` tidak masuk graf; jumlahnya dicatat. Deteksi berulang
  diabaikan; perubahan hanya diterapkan melalui event `update`.
- Graf menghubungkan titik dengan jarak Haversine ≤1 km dan selisih ≤6 jam.
  Indeks kubus ECEF 1 km × interval 6 jam membatasi pencarian tetangga.
  Komponen terhubung ≥2 titik menjadi kebakaran. Hubungan berantai tetap berlaku.
- Update dan retract menghitung ulang komponen yang terdampak. ID berasal dari
  hash deterministik dan lineage komponen. Fire yang tetap ada mempertahankan
  ID; merge memilih salah satu ID lama dan menarik yang lain; split memberi
  satu bagian ID lama dan bagian lain ID baru. Replay **urutan offset yang sama**
  menghasilkan ID yang sama. Ini tidak menjanjikan ID identik untuk urutan
  kedatangan arbitrer yang berbeda.
- Fire yang menyusut menjadi singleton/hilang mendapat revisi
  `attributes.withdrawn=true`, bukan event baru. List/Audit mengecualikannya;
  outbox tetap membawa revisi pembatalan. Get gRPC mengembalikan NotFound.
- Centroid dihitung pada bola; `area_name` memakai titik provinsi terdekat dari
  tabel statis versi kode di `internal/m2/provinces.go`. Tabel ini adalah
  keputusan PoC kelompok, bukan dataset referensi yang diklaim berasal dari asisten.
- Severity: AWAS untuk ≥20 titik atau FRP ≥100 MW, SIAGA untuk ≥6 atau ≥30 MW,
  selain itu WASPADA. Attributes memuat jumlah, satelit, confidence tertinggi,
  total FRP, dan daftar kunci terurut.

Batas keterlambatan awal **24 jam** terhadap maksimum event time yang telah
terlihat, termasuk `virtual_now` heartbeat yang tersimpan. Titik baru di luar
batas diparkir dengan alasan, tidak dibuang diam-diam. Koreksi atas titik yang
sudah dikenal tetap diterima. Fire dapat dianggap tertutup secara event time
setelah `max_detection_time + 6h` melewati watermark; kode tetap menyimpan graf
dan journal sehingga koreksi lama masih dapat diterapkan. Belum ada garbage
collection/snapshot graf: startup memutar ulang seluruh journal. Batas
`SATELLITE_MAX_DETECTIONS` menghentikan kemajuan dengan error eksplisit bila
kapasitas terlampaui; input tetap tersimpan. Pemakaian memori pada dataset penuh
harus diukur, terutama komponen sangat rapat dengan banyak edge.

SSE menggunakan `from` dan `Last-Event-ID`. Offset duplikat diabaikan, celah
offset menutup koneksi dan memicu reconnect, frame yang terpotong belum
di-commit. Jenis payload yang diverifikasi pada feed latihan resmi:
`detection: {offset,detection}`, `update: {offset,key,changes}`, dan
`retract: {offset,key,reason}`. Feed latihan kelompok saat diperiksa berjalan 60×.

Urutan penyimpanan:

1. Append input immutable ke `m2_stream_journal`.
2. Terapkan perubahan HazardEvent dan outbox melalui storage backend.
3. Commit checkpoint dengan compare-and-set.

Crash pada langkah 2/3 mengulang input yang sama. Hazard ID, revision, dan event
ID deterministik mencegah duplikasi logis. `stream_sequence` dicek di bawah
row lock/LWT agar proyeksi lama tidak menimpa record baru. Ini adalah
**at-least-once delivery dengan side effect idempoten**, bukan transaksi tunggal
lintas database, broker, dan penerima. Cassandra tetap memakai mekanisme
proyeksi/outbox P1 yang sudah ada; konsistensi proyeksi ketika dua writer overlap
perlu dibuktikan pada cluster, tidak disamakan dengan transaksi PostgreSQL.

Journal PostgreSQL memakai primary key `(stream,sequence)`; Cassandra memakai
partisi `(stream,block)` berisi 1000 input. Journal mengikuti replikasi backend
kanonik dan tidak disimpan di filesystem pod Aggregator.

Alternatif: polling berulang lebih sederhana tetapi tidak cocok dengan offset
SSE, burst, dan koreksi terurut. Framework stream seperti Flink menyediakan
checkpoint/watermark bawaan, dengan biaya deployment dan integrasi lebih besar.
Implementasi Go ini memakai store/lease/outbox yang sudah ada, dengan konsekuensi
perlu menguji recovery dan mengelola pertumbuhan journal sendiri.

## P5: isolasi, validasi, circuit breaker, dan prioritas

- Gunakan image resmi dengan digest dari PDF, tanpa modifikasi.
- Ambil `/v1/stations`, kemudian `/v1/observations?limit=200&cursor=...`.
  Cursor hanya maju sesudah item valid tersimpan dan item invalid diparkir.
  Data stasiun disertakan dalam journal page agar pemulihan tidak bergantung
  pada perubahan respons stasiun di masa depan.
- Validasi UTF-8, struktur JSON, nesting maksimum 32, ukuran respons default
  2 MiB, tipe field wajib, koordinat, timestamp, wave height nonnegatif,
  `quality=ok`, dan boolean `critical` yang benar. Field tambahan diabaikan.
  Respons melebihi batas disimpan sebagai prefix dengan alasan truncation;
  cursor tidak maju. Kontrak yang diperluas vendor perlu ditinjau eksplisit.
- Pemetaan: `source=SENSOR_PANTAI`, `hazard_type=COASTAL`,
  `source_ref_id=observation_id`, area/koordinat dari stasiun, waktu dari
  `observed_at`. Appendix C tidak memberikan threshold severity numerik:
  pilihan PoC adalah `critical=true → AWAS`, selainnya WASPADA. Nilai anomali,
  wave height, quality, station_id, dan critical disimpan sebagai attributes.
- Item kritis diproses lebih dulu dalam satu halaman. Data lama yang valid
  tetap disimpan/dikirim, tetapi status sumber ditandai stale.
- Poll interval **500 ms**, request timeout **1,5 s**, breaker membuka sesudah
  **3 kegagalan**, menunggu **10 s**, lalu satu probe. `Retry-After` dihormati
  hingga 1 menit. Tidak ada retry per client; client membaca store terakhir.
  `/healthz` tidak dipakai untuk menyimpulkan kesegaran data. Timestamp payload,
  pengamatan terbaru, dan kemajuan generated_at diperiksa dengan ambang **2 menit**.
- API gateway menyediakan **25 slot dari 100** untuk FIELD_TEAM/BNPB_INTERNAL
  ketika pool umum penuh. Role harus berasal dari JWT RS256 yang terverifikasi
  issuer, expiry, dan audience bila dikonfigurasi; header role palsu tidak dipercaya.
  JWK dimuat di background; permintaan tidak menunggu pengambilan key.
  SSE mengikuti pengecualian limiter lama dan belum memiliki kuota terpisah.
- Outbox sensor terus mencoba setelah gangguan broker. Cassandra memberi
  partisi outbox sensor tersendiri dengan separuh batch, agar backlog satelit
  tidak menutupi sensor dan sumber biasa tidak kelaparan. PostgreSQL memilih
  sumber sensor terlebih dahulu. Danger Notification memetakan tipe baru,
  menunda retry transient broker 1 detik, dan mempertahankan retry push sensor.
  Interval polling push **250 ms**. FCM tetap bukan acknowledgment bahwa
  perangkat Tim Lapangan sudah menerima pesan.

Istilah persoalan: contract validation/poison messages, bulkhead isolation,
circuit breaker, bounded retries, load shedding/admission control, durable
outbox, deduplication, dan SLO/error budget. Nilai timeout/breaker di atas adalah
konfigurasi awal yang perlu dievaluasi melalui pengukuran dan game day.

## Menjalankan

Kubernetes: ikuti `infrastructure/m2/README.md` dan `m2/deploy.ps1`. Tambahan ini
mengasumsikan cluster dasar P1/P2 sudah ada. Jangan memakai Kind sebagai bukti
syarat kernel terpisah. Perubahan chart menambahkan envFrom ConfigMap/Secret,
port audit, label role Aggregator, dan resources 256Mi/512Mi.

Development Docker Compose: siapkan konfigurasi M1 sesuai README, isi token
di `.env` lokal, kemudian:

```sh
docker compose --env-file .env --env-file .env.auth -f compose.yaml -f compose.m2.yaml up --build -d
```

Daftar konfigurasi lengkap ada pada bagian M2 di `.env.example`. Menjalankan
tanpa overlay/URL sumber mempertahankan job baru dalam keadaan nonaktif.
Gunakan `SATELLITE_STREAM_ID` berbeda bila sesi upstream di-reset atau beralih
ke dataset ujian; jangan mengganti ID selama restart/reconnect sesi yang sama.

Token kelompok berada di `.secrets/satellite-token` lokal dan tidak masuk Git.
Jangan menyalinnya ke laporan, screenshot, atau example file.

```powershell
# Capture read-only sesi latihan, tidak memulai ujian.
.\tools\m2_feed_probe.ps1 -Seconds 20
cd services/aggregator
go run ./cmd/m2replay -input ../../.cache/m2/feed.jsonl -output ../../.cache/m2/replay.json
go test ./...
```

Linux/macOS dapat memakai `python tools/m2_feed_probe.py`. TLS tetap diverifikasi;
bila Python tidak mengenali CA sistem Windows, gunakan helper PowerShell.

Uji crash setelah HazardEvent/outbox tertulis tetapi sebelum checkpoint:

```powershell
# Gunakan database test terpisah: tes ini melakukan TRUNCATE tabel Aggregator!
$env:AGG_TEST_POSTGRES_DSN='postgres://postgres:<password>@127.0.0.1:15432/postgres?sslmode=disable'
go test -tags integration ./internal/m2 -run TestPostgresCrashBeforeCheckpoint -count=1
```

Untuk smoke test adapter terhadap container Sensor Pantai resmi yang berjalan
di port lokal 18085 dengan token test `m2-local-test-only`, gunakan database test
yang sama dan jalankan:

```powershell
$env:PANTAI_TEST_URL='http://127.0.0.1:18085'
go test -tags integration ./internal/m2 -run 'TestPostgresCrashBeforeCheckpoint|TestOfficialCoastalImage' -count=1 -v
```

Tes kedua hanya membuktikan pembacaan, pemetaan, penyimpanan, dan checkpoint
adapter resmi. Sampel terakhir belum memuat critical pada halaman yang diperiksa;
ini tidak membuktikan SLO pengiriman critical ke penerima.

## Observabilitas dan audit

Dashboard Grafana `BNPB M2 — Satellite & Coastal` diprovisikan bersama dashboard
lama dalam Compose. OTel membawa trace input → write/outbox → publish → consumer.
Log baru memuat source/reason/correlation_id. Metrik:

| Metrik | Arti |
|---|---|
| `bnpb.satellite.offset` | Offset upstream yang sudah di-commit |
| `bnpb.satellite.offset.lag` | latest_offset sesi dikurangi checkpoint, diperiksa tiap 5 s |
| `bnpb.satellite.fires`, `.singletons`, `.late` | Hasil aktif dan input terlambat |
| `bnpb.coastal.breaker.open` | Sirkuit sensor sedang terbuka |
| `bnpb.m2.inputs`, `.parked` | Input selesai / input diparkir per sumber |
| `bnpb.m2.ingest.duration` | Waktu sejak input diterima hingga penyimpanan; bukan latensi penerima push |

Audit berada pada port `AGGREGATOR_M2_HTTP_ADDRESS`, opsional default nonaktif:
`/internal/audit/events`, `/checksum`, `/unprocessed`. Token audit dibandingkan
constant-time. Events berurutan ingested_at lalu hazard_id dan memakai keyset
cursor. Checksum mengikuti SHA-256 source_ref_id terurut, newline tanpa newline
akhir. Bila pembacaan gagal, hasil bertanda partial, bukan klaim lengkap.
`unavailable_units` saat ini konservatif pada level backend, bukan daftar shard
per node; gabungkan dengan implementasi Audit P1 untuk granularitas itu.

Raw input hanya di `/internal/ops/m2/unprocessed` untuk JWT BNPB_INTERNAL;
endpoint audit hanya memberikan metadata. Tabel `m2_parked_inputs` khusus
adapter ini tidak menggantikan DLQ/replay P3. Integrasikan response P3 dan
adapter ini sebelum demo bersama; jangan mendaftarkan dua handler dengan path
yang sama pada satu server.

## Bukti lokal dan yang masih harus diukur

| Pemeriksaan | Status |
|---|---|
| Seluruh unit test Aggregator, termasuk P1/P2 yang sudah ada | Lulus pada workspace ini |
| Parser, chain/merge/split, koreksi, dedup, low/late, SSE terpotong, breaker | Lulus |
| PostgreSQL: crash setelah outbox sebelum checkpoint, restart, CAS stale writer | Lulus; tidak ada tambahan event outbox saat replay |
| Adapter → image Sensor Pantai resmi → PostgreSQL | Lulus; halaman audit memuat 1.000 record COASTAL, checkpoint 8; input quality invalid diparkir |
| Feed latihan resmi 60×: sampel 1.358 event | 1.261 detection, 70 update, 27 retract; seluruhnya diproses tanpa parked error |
| Hasil sampel | 21 fire aktif, 138 singleton, 134 low-confidence aktif, 0 late |
| Waktu replay sampel | ±0,136 s; p95 komputasi ±1 ms, **bukan** latensi storage/FCM |
| Frontend | Build lulus; 33 tes lulus, termasuk penarikan wildfire pada SSE |
| Gateway | Seluruh tes lulus, termasuk verifikasi JWT dan slot Tim Lapangan ketika Media memenuhi pool umum |
| Danger Notification | 78 tes lulus |
| Infrastruktur | Render Kustomize dan sintaks script PowerShell lulus; belum rollout cluster |
| Referensi asisten seluruh aliran | Belum tersedia/dibandingkan |
| Node mati/partition/freeze, Cassandra multi-node, autoscaling | Belum dibuktikan oleh pekerjaan P4/P5 ini |
| Catch-up ≤60 s dan p95 sampai HazardEvent tersimpan pada cluster | Perlu pengukuran end-to-end |
| 0 critical hilang dan p95 penerimaan Tim Lapangan ≤5 s | Perlu receipt dari penerima, bukan hanya publish/FCM success |

Sampel dan output asli berada di `.cache/m2/` lokal. Angka ini bukan nilai hasil
ujian dan tidak boleh ditampilkan sebagai kelulusan seluruh kriteria.
Log verifikasi terakhir (15 November 2026): `go-tests.txt`,
`go-integration.txt`, dan `dotnet-test.txt` di direktori tersebut.

## Rencana game day dan bahan laporan

SLO jalur kritis: 100% observation valid bertanda critical pada window pengujian
memiliki receipt Tim Lapangan; ≥95% tiba dalam 5 s sejak input diterima BNPB.
Budget kehilangan = 0. Budget keterlambatan = 5% × jumlah critical yang diharapkan.
Laporkan jumlah missing, p95, jumlah lebih dari 5 s, dan budget terpakai. Catat
excluded-invalid secara terpisah, jangan menyembunyikannya dari denominator.

1. Ambil baseline throughput, p95/error rate BMKG/PVMBG, offset lag dan input
   critical/receipt; simpan jendela waktu serta konfigurasi resource.
2. Jalankan feed 60×, hentikan pod pemroses di tengah komponen fire, lalu putus
   koneksi feed. Verifikasi cursor, hasil akhir, singleton/late, dan checksum.
3. Jalankan profil latihan Sensor Pantai resmi. Simpan respons menyimpang,
   alasan parkir, status breaker, jumlah request upstream, dan receipt critical.
4. Bebani gateway dengan JWT Media dan Tim Lapangan bersamaan. Media menerima
   429 terkontrol, Tim Lapangan menggunakan slot cadangan. Jangan memakai token
   palsu atau header role sebagai bukti prioritas.
5. Jalankan Tester Agent pada worker non-control-plane sesuai spesifikasi,
   gabungkan dengan outage PVMBG dan beban. Selama window pemulihan jangan
   melakukan perbaikan manual. Bandingkan baseline sumber lain.
6. Isi post-mortem dari bukti aktual: waktu deteksi, sinyal dashboard/trace,
   dampak, mekanisme pemulihan otomatis, lama pemulihan, kehilangan/latensi,
   konsumsi budget, dan perbaikan berikutnya. Jangan mengisi hasil simulasi
   seolah-olah game day sudah dilaksanakan.

Lampiran laporan P4/P5 dapat memakai uraian desain di atas. Tambahkan screenshot
dan log milik kelompok, tabel kontribusi, formulir penggunaan AI, serta hasil
referensi dan game day sebelum ekspor laporan final.
