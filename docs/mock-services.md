# Implementasi mock BMKG dan PVMBG

Kedua service menyediakan data bencana sintetis melalui REST sesuai kontrak
[BMKG](../contracts/openapi/bmkg.yaml) dan [PVMBG](../contracts/openapi/pvmbg.yaml).
Masing-masing berjalan sebagai proses/container Python 3.12 dengan file SQLite
sendiri. Posisi keduanya mengikuti diagram: Aggregator melakukan polling ke
gateway sumber; database sumber hanya diakses oleh mock pemiliknya.

Lingkup implementasi ini adalah dua sumber upstream. Pemetaan `HazardEvent`,
cache BNPB, gRPC, autentikasi pengguna BNPB, RabbitMQ, dan distribusi notifikasi
tetap menjadi pekerjaan service BNPB. Data mock bukan laporan BMKG/PVMBG aktual.

## Menjalankan service

Runtime menggunakan standard library Python, sehingga tidak memerlukan
`pip install`. Jalankan dari root repository dengan Python 3.12:

```sh
python tools/setup_mock_env.py
```

Script membuat `.env` bila belum tersedia, lalu mengisi kredensial mock yang
belum diisi/masih placeholder dengan dua secret acak yang independen. Nilai
yang sudah diisi dipertahankan. `.env` adalah konfigurasi lokal, bukan berkas
untuk di-commit. Secret BNPB dan broker tidak disiapkan oleh script ini.

Untuk Docker:

```sh
docker compose -f compose.mocks.yaml up --build -d
docker compose -f compose.mocks.yaml ps
docker compose -f compose.mocks.yaml logs -f
```

Untuk menjalankan langsung, buka dua terminal di root repo:

```sh
python tools/run_mock.py bmkg
```

```sh
python tools/run_mock.py pvmbg
```

Launcher memuat hanya konfigurasi `.env` yang dimiliki service tujuan, dengan
environment shell sebagai override. Proses anak berjalan di root repo sehingga
path database relatif tetap konsisten. `Ctrl+C` menghentikan proses lokal.

| Akses | BMKG | PVMBG |
| --- | --- | --- |
| Dari host/lokal | `http://localhost:8081` | `http://localhost:8082` |
| Dari jaringan Compose | `http://bmkg-mock:8080` | `http://pvmbg-mock:8080` |
| Database lokal default | `data/bmkg.sqlite3` | `data/pvmbg.sqlite3` |
| Database dalam container | `/app/data/bmkg.sqlite3` | `/app/data/pvmbg.sqlite3` |
| Volume Compose | `bmkg_data` | `pvmbg_data` |

Port host dapat diubah lewat `BMKG_HTTP_PORT`/`PVMBG_HTTP_PORT`. Port internal
kedua container tetap 8080 pada konfigurasi Compose ini. Gunakan Compose khusus
mock untuk menjalankan lingkup ini; Compose utama juga memuat komponen BNPB
yang masih dalam pengerjaan.

```sh
docker compose -f compose.mocks.yaml restart pvmbg-mock
docker compose -f compose.mocks.yaml down
```

Restart dapat dilakukan per service. `down` mempertahankan named volume;
penghapusan volume atau file SQLite akan menghapus riwayat mock tersebut.

## Endpoint dan kredensial

| Service | Method dan endpoint | Autentikasi | Perilaku |
| --- | --- | --- | --- |
| BMKG | `GET /seismic-events?since=...` | `X-BMKG-Key` | Daftar gempa, filter `occurred_at` |
| BMKG | `GET /tsunami-warnings?since=...` | `X-BMKG-Key` | Warning terkait gempa, filter metadata `updated_at` internal |
| BMKG | `GET /health` | Publik | Kesehatan service tanpa delay buatan |
| PVMBG | `GET /volcanic-reports?since=...` | `Authorization: Bearer ...` | Report vulkanik, filter `reported_at` |
| PVMBG | `POST /admin/schema-version` | Bearer PVMBG | Body `{"version":1}` atau `{"version":2}` |
| PVMBG | `POST /admin/outage` | Bearer PVMBG | Body `{"enabled":true}` atau `{"enabled":false}` |
| PVMBG | `GET /health` | Publik | 503 selama injected outage, tanpa delay buatan |

`since` opsional, inklusif, dan harus berupa RFC 3339 dengan timezone, misalnya
`2026-09-29T01:00:00Z`. Offset timezone dibandingkan sebagai waktu yang sama,
bukan urutan teks. Untuk offset positif pada query URL, encode `+` menjadi `%2B`.
Respons sukses berbentuk `{"items":[...]}`. Semua record yang cocok dikembalikan
tanpa pagination/batas tersembunyi, terurut timestamp lalu ID. Tanpa `since`,
seluruh record tersimpan dikembalikan. Pemanggil perlu melakukan deduplikasi ID
karena batas waktu inklusif dapat mengembalikan record yang sama.

BMKG hanya memverifikasi secret `BMKG_API_KEY`. PVMBG hanya memverifikasi
`PVMBG_BEARER_TOKEN`, termasuk pada admin API. Secret kosong/placeholder ditolak
saat startup; keduanya harus berbeda dari token pengguna BNPB. Compose hanya
menyuntikkan konfigurasi milik masing-masing service dan tidak membagikan volume.

Kredensial salah menghasilkan 401; query/body/correlation ID tidak valid
menghasilkan 400. Gangguan sumber/storage menghasilkan 503 dengan `Retry-After`.
Error memakai `application/problem+json` dan correlation ID, tanpa menampilkan
secret, payload request, atau traceback. `X-Correlation-ID` diteruskan bila valid
atau dibuat bila tidak diberikan; panjang 1–128 dan karakter yang diterima
adalah huruf, angka, `.`, `_`, `:`, dan `-`.

## Struktur dan alur implementasi

Di masing-masing folder service, `config.py` membaca dan memvalidasi environment,
`storage.py` mengelola SQLite serta data sintetis, dan `app.py` menjalankan HTTP,
generator, dan kontrol lifecycle. Kedua service tidak mengimpor kode bisnis satu
sama lain dan dapat dibangun, dijalankan, atau dihentikan sendiri.

Saat mulai, service membuka database dan menyiapkan minimal 20 record historis.
Data yang sudah ada dipertahankan. Generator kemudian menambah event pada
interval `MOCK_EVENT_INTERVAL_MS`, default 10000 ms, dengan batas positif paling
lama 10000 ms. Pembuatan data berlangsung terpisah dari request sehingga polling
yang lambat tidak menjadi pemicu satu-satunya untuk data baru.

BMKG menyimpan gempa dan warning terkait. Warning hanya menunjuk gempa dengan
`potential_tsunami=true`. Payload mempertahankan field mentah dari spesifikasi;
`estimated_arrival` adalah prediksi waktu tiba tsunami. Waktu pembuatan/perubahan
warning disimpan sebagai metadata internal untuk polling, tidak ditambahkan
sebagai field wajib payload. Pembacaan data menunggu delay tetap
`BMKG_RESPONSE_DELAY_MS`, default 100 ms, yang harus berada pada 50–150 ms.

PVMBG menghasilkan report untuk lima `volcano_id` yang disepakati dalam
[referensi demo](data-mapping.md#vulkanik). Mock tidak melakukan pemetaan nama
gunung dan koordinat; itu tanggung jawab Aggregator. Delay pembacaan data
disampel merata antara `PVMBG_RESPONSE_DELAY_MIN_MS` dan
`PVMBG_RESPONSE_DELAY_MAX_MS`, default 500–3000 ms. Keduanya nonnegatif dan minimum
tidak boleh melebihi maksimum. Admin serta health tidak dikenai delay ini.

HTTP memakai thread per request melalui `ThreadingHTTPServer`, sehingga request
admin/health dapat diproses saat request data sedang menunggu delay. Proses BMKG
dan PVMBG terpisah, sehingga sleep atau outage PVMBG tidak memblokir proses BMKG.
Runtime ini ditujukan untuk mock lokal/demo dengan beban terukur. Pengujian beban
sistem penuh tetap diperlukan; modul HTTP standard library sendiri tidak
direkomendasikan sebagai server produksi oleh
[dokumentasi Python](https://docs.python.org/3.12/library/http.server.html).

## SQLite dan perubahan skema

| Data | Isi payload JSON | Metadata untuk query |
| --- | --- | --- |
| BMKG seismic event | Field mentah `SeismicEvent` | ID dan waktu `occurred_at` |
| BMKG tsunami warning | Field mentah `TsunamiWarning` | ID, relasi gempa, waktu perubahan internal |
| PVMBG volcanic report | Field mentah `VolcanicReport`, termasuk confidence bila ada | ID dan waktu `reported_at` |

ID dan timestamp berindeks mendukung filter serta urutan polling. Payload JSON
menyimpan field sumber yang dapat bertambah tanpa perubahan kolom tabel.
Pembacaan mengambil koleksi dari snapshot SQLite; generator menulis dalam
transaksi. Penyimpanan file memberi persistensi setelah restart tanpa server
database tambahan. Python menyediakan antarmukanya melalui
[`sqlite3`](https://docs.python.org/3.12/library/sqlite3.html).

PVMBG selalu mulai pada skema v1. Kontrol skema merupakan state proses, terpisah
dari record yang tersimpan:

1. Pada v1, semua respons menghilangkan `confidence_level`.
2. Setelah `{"version":2}`, report baru menyimpan confidence bernilai 0–1.
   Report v1 historis boleh tetap tanpa confidence; field yang tidak ada tidak
   diganti menjadi nol.
3. Kembali ke v1 hanya menyembunyikan field pada respons, tanpa menghapus nilai
   dalam SQLite. Kembali ke v2 menampilkan nilai tersimpan lagi.
4. Restart mengembalikan versi aktif ke v1; database dan confidence yang pernah
   tersimpan tetap ada. Header `X-PVMBG-Schema-Version` menunjukkan versi respons.

Karena field tambahan berada dalam JSON, siklus v1/v2 tidak membutuhkan migrasi
database ataupun restart. Perubahan tipe, penghapusan field wajib, atau perubahan
makna data tetap memerlukan penanganan kompatibilitas tersendiri.

| Pilihan | Keuntungan untuk mock | Konsekuensi |
| --- | --- | --- |
| Memory saja | Implementasi sederhana dan akses cepat | Riwayat hilang saat restart; tidak membuktikan persistensi |
| SQLite, dipilih | File durable, transaksi, indeks, tersedia melalui Python | Satu penulis aktif per database; kurang sesuai untuk banyak replica penulis |
| PostgreSQL | Cocok untuk service dengan banyak koneksi/penulis dan deployment terpusat | Perlu server, kredensial, dan operasi database tambahan |

JSON memberi fleksibilitas, tetapi validasi payload tetap tanggung jawab aplikasi.
SQLite cukup untuk laju generator mock ini; kebutuhan replikasi atau throughput
penulisan yang jauh lebih tinggi perlu dievaluasi kembali. Retensi otomatis
belum diterapkan, jadi database dan respons tanpa `since` akan tumbuh selama
generator berjalan.

## Contoh polling, skema, dan outage di PowerShell

Contoh berikut memakai `.env` yang dibuat oleh helper, dengan nilai secret satu
baris tanpa kutip. Nilai tersebut dimuat tanpa dicetak ke terminal:

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^(BMKG_API_KEY|PVMBG_BEARER_TOKEN)=(.*)$') {
        [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process')
    }
}
$bmkgHeaders = @{ 'X-BMKG-Key' = $env:BMKG_API_KEY; 'X-Correlation-ID' = 'demo-bmkg-1' }
$pvmbgHeaders = @{ Authorization = "Bearer $env:PVMBG_BEARER_TOKEN"; 'X-Correlation-ID' = 'demo-pvmbg-1' }

Invoke-RestMethod http://localhost:8081/seismic-events -Headers $bmkgHeaders
Invoke-RestMethod http://localhost:8081/tsunami-warnings -Headers $bmkgHeaders
Invoke-RestMethod http://localhost:8082/volcanic-reports -Headers $pvmbgHeaders

Invoke-RestMethod http://localhost:8082/admin/schema-version -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"version":2}'
Start-Sleep -Seconds 11
Invoke-RestMethod http://localhost:8082/volcanic-reports -Headers $pvmbgHeaders

Invoke-RestMethod http://localhost:8082/admin/outage -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"enabled":true}'
Invoke-RestMethod http://localhost:8081/health
Invoke-RestMethod http://localhost:8082/admin/outage -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"enabled":false}'
Invoke-RestMethod http://localhost:8082/health
```

Saat outage aktif, pembacaan volcanic report dengan token valid dan health PVMBG
mengembalikan 503. Admin tetap tersedia, sehingga `enabled=false` memulihkan
service tanpa restart. Generator tetap bekerja selama outage; polling setelah
pemulihan dapat mengambil record yang terbentuk selama gangguan. State skema
dipertahankan saat outage. Restart proses mereset outage ke nonaktif dan skema
ke v1, sementara record persisten tetap tersedia.

Untuk simulasi PVMBG tepat 3 detik, set kedua variabel delay menjadi `3000`
sebelum menjalankan proses, atau ubah `.env` lalu buat ulang container PVMBG
dengan `docker compose -f compose.mocks.yaml up -d --force-recreate pvmbg-mock`.
Perubahan delay lewat environment memerlukan proses baru; kontrol schema/outage
lewat admin berlaku langsung.

## Pengujian dan observability

Untuk pengujian container, ikuti [panduan testing Docker](mock-testing.md).
Panduan tersebut menyediakan satu perintah otomatis dan langkah manual untuk
health, autentikasi, generator, skema, outage, delay, serta persistensi.

Jalankan suite dari root repository:

```sh
python -m unittest discover -s tests -v
```

Suite menjalankan subprocess service dan request HTTP aktual dengan database
sementara. Cakupannya memeriksa kontrak mock, pemisahan kredensial, filtering,
persistensi, generation, evolusi skema, dan recovery outage. Hasil aktual adalah
keluaran test saat dijalankan; dokumen ini tidak mengklaim pengujian P1–P5 sistem
BNPB lengkap atau benchmark 50 koneksi selama 60 detik sudah selesai.

Setiap request menghasilkan log JSON berisi waktu, service, operation, status,
durasi, dan correlation ID. Trace context masuk yang valid dapat dikaitkan ke
log. Header secret, body mentah, dan query sensitif tidak dicatat. Kedua mock
tidak membuat panggilan RPC keluar. Integrasi exporter OpenTelemetry, Collector,
Prometheus, Jaeger, dan Grafana belum disediakan oleh implementasi mock ini.
