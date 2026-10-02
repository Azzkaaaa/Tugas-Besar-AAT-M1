# BMKG Mock

Gateway REST Python 3.12 mandiri untuk polling Aggregator, sesuai
[OpenAPI BMKG](../../contracts/openapi/bmkg.yaml). Runtime hanya memakai standard
library: `ThreadingHTTPServer`, `sqlite3`, dan generator dalam background thread.
Service ini memiliki database sendiri dan tidak mengimpor business logic service lain.

## Menjalankan

Gunakan helper provisioning dan startup yang dijelaskan pada [README root](../../README.md).
Jika environment sudah diisi, entrypoint juga bisa dijalankan dari root repository:

```powershell
python services/bmkg-mock/app.py
```

Entrypoint tidak membaca `.env` otomatis. Jika menjalankan `npm run dev` dari
direktori service, isi environment lebih dahulu; path database relatif mengikuti
working directory saat proses dimulai. Akses default `http://localhost:8081`.

| Environment | Default | Ketentuan |
| --- | --- | --- |
| `BMKG_API_KEY` | wajib | ASCII yang terlihat tanpa spasi, bukan placeholder `REPLACE_*`; gunakan kunci khusus BMKG. |
| `BMKG_STORE_DSN` | `sqlite:///data/bmkg.sqlite3` | SQLite berbasis file; direktori dibuat otomatis. |
| `BMKG_HTTP_PORT` | `8081` | Port 1–65535. |
| `MOCK_SEED_COUNT` | `20` | Minimal 20 gempa historis. |
| `MOCK_EVENT_INTERVAL_MS` | `10000` | Interval 1–10000 ms. |
| `BMKG_RESPONSE_DELAY_MS` | `100` | Delay tetap untuk pembacaan data, 50–150 ms. |

Contoh DSN absolut: `sqlite:////data/bmkg.sqlite3` pada Linux atau
`sqlite:///D:/mock-data/bmkg.sqlite3` pada Windows. Database in-memory dan DSN
non-SQLite ditolak. Container menggunakan port 8080 dan `/app/data/bmkg.sqlite3`;
mount volume `/app/data` agar data bertahan saat container diganti. Proses container
berjalan sebagai user 10001; direktori data disiapkan untuk user tersebut saat build.

## API dan persistensi

- `GET /seismic-events`: semua gempa, urut `occurred_at`, lalu `event_id`.
- `GET /tsunami-warnings`: semua peringatan, urut metadata internal `updated_at`,
  lalu `warning_id`. Metadata tidak muncul dalam payload.
- `GET /health`: publik, tanpa delay buatan; memeriksa database dan generator.

Kedua endpoint data mewajibkan header `X-BMKG-Key`. Service hanya membaca
`BMKG_API_KEY`; kunci PVMBG dan token client BNPB tidak digunakan. Header
`X-Correlation-ID` diteruskan atau dibuat jika tidak dikirim. Nilainya harus
1–128 karakter ASCII dari `A-Z a-z 0-9 . _ : -`.

Parameter opsional `since` harus RFC3339 lengkap dengan timezone. Batas bersifat
inklusif, dibandingkan sebagai integer mikrodetik UTC, dan tidak ada limit atau
paginasi. Offset positif dalam query perlu di-URL-encode (`+` menjadi `%2B`).
Parameter tidak dikenal, berulang, atau timestamp tidak valid menghasilkan 400.
Contoh PowerShell:

```powershell
$headers = @{ 'X-BMKG-Key' = $env:BMKG_API_KEY; 'X-Correlation-ID' = 'demo-bmkg-1' }
Invoke-RestMethod 'http://localhost:8081/seismic-events' -Headers $headers
Invoke-RestMethod 'http://localhost:8081/tsunami-warnings?since=2026-01-01T00%3A00%3A00Z' -Headers $headers
Invoke-RestMethod 'http://localhost:8081/health'
```

Seed memakai ID stabil dan `INSERT OR IGNORE`; restart mempertahankan data serta
timestamp lama. Seed mencakup gempa berpotensi/tidak berpotensi tsunami dan level
peringatan `Waspada`, `Siaga`, `Awas`. Generator menyimpan gempa baru setiap
interval, dengan peringatan pada setiap gempa keempat. Warning hanya boleh
mereferensikan gempa dengan `potential_tsunami=true`; foreign key menjaga relasi.
Setiap penyimpanan gempa dan warning terkait berada dalam satu transaksi.

`Store.upsert_warning()` merupakan API internal untuk perubahan peringatan. Hanya
perubahan material yang memajukan `updated_at`; update identik tidak mengubah
watermark. `estimated_arrival` tetap berarti perkiraan kedatangan tsunami.
Aggregator memakai waktu mulai polling sukses sebagai `since` berikutnya,
ditambah overlap untuk clock skew, dan melakukan deduplikasi berdasarkan ID serta
konten. Watermark baru disimpan setelah hasil polling tersimpan secara durable.

SQLite menggunakan WAL, busy timeout 5 detik, indeks waktu dan ID, serta koneksi
terpisah yang selalu ditutup. Satu query SELECT memberi snapshot konsisten untuk
seluruh koleksi. Kegagalan database mengembalikan 503 dengan `Retry-After: 1`;
kesalahan autentikasi mengembalikan 401 dengan `WWW-Authenticate`. Error memakai
`application/problem+json` sesuai RFC 9457, tanpa traceback atau kredensial.

Log JSON memuat waktu, service, level, correlation ID, operasi, durasi, dan status.
Trace context W3C versi 00 yang valid diteruskan ke log. Tidak ada request body,
query, atau API key dalam log. Generator melaporkan kegagalan dan mencoba kembali;
health menjadi 503 selama generator gagal. SIGINT/SIGTERM menghentikan generator,
menutup listener, dan menunggu request aktif selesai. Tidak ada push ke BNPB,
broker, maupun dependensi jaringan keluar.

Uji aturan penyimpanan internal dapat dijalankan dari root dengan:

```powershell
python -m unittest discover -s services/bmkg-mock -p test_storage.py -v
```
