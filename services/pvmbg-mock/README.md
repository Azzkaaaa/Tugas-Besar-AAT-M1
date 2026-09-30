# PVMBG Mock

Gateway REST mandiri dengan Python 3.12 standard library dan SQLite, sesuai
[kontrak PVMBG](../../contracts/openapi/pvmbg.yaml). Tidak memerlukan instalasi
package Python. Service hanya memiliki data dan bearer token PVMBG sendiri.
Aggregator BNPB mengambil laporan melalui REST; mock tidak mengirim push.

## Menjalankan

Dari root repository, gunakan PowerShell:

```powershell
python tools/setup_mock_env.py
python tools/run_mock.py pvmbg
```

Helper setup menyediakan token acak khusus PVMBG di `.env`; helper run hanya
memuat konfigurasi yang dimiliki service. Port lokal default **8082**. Entry point
langsung `python services/pvmbg-mock/app.py` membaca environment proses dan tidak
memuat `.env` otomatis; gunakan bila environment telah disiapkan sendiri.
Path relatif SQLite mengikuti direktori kerja proses. Token
kosong atau placeholder `REPLACE_*` ditolak. SQLite wajib berupa file persisten,
bukan `:memory:`. Hentikan proses dengan Ctrl+C; SIGTERM juga ditangani di Linux.

Di terminal PowerShell kedua pada root repository, baca token lokal tanpa
mencetaknya, lalu panggil endpoint:

```powershell
$tokenLine = Get-Content .env | Where-Object { $_.StartsWith('PVMBG_BEARER_TOKEN=') }
$token = $tokenLine.Substring('PVMBG_BEARER_TOKEN='.Length).Trim().Trim('"').Trim("'")
$headers = @{ Authorization = "Bearer $token"; 'X-Correlation-ID' = 'pvmbg-demo-1' }
Invoke-RestMethod http://localhost:8082/volcanic-reports -Headers $headers
Invoke-RestMethod http://localhost:8082/admin/schema-version -Method Post -Headers $headers -ContentType application/json -Body '{"version":2}'
Invoke-RestMethod http://localhost:8082/admin/outage -Method Post -Headers $headers -ContentType application/json -Body '{"enabled":true}'
Invoke-RestMethod http://localhost:8082/admin/outage -Method Post -Headers $headers -ContentType application/json -Body '{"enabled":false}'
```

## Endpoint dan perilaku

| Endpoint | Autentikasi | Perilaku |
| --- | --- | --- |
| `GET /volcanic-reports?since=...` | Bearer PVMBG | Seluruh laporan yang cocok; `since` inklusif pada `reported_at` |
| `POST /admin/schema-version` | Bearer PVMBG | Body tepat `{"version":1}` atau `{"version":2}` |
| `POST /admin/outage` | Bearer PVMBG | Body tepat `{"enabled":true}` atau `{"enabled":false}` |
| `GET /health` | Publik | Memeriksa outage, SQLite, dan generator |

`since` menerima RFC 3339 bertimezone. Offset dinormalisasi ke UTC, lalu hasil
diurutkan berdasarkan waktu dan `report_id`. Timestamp tersimpan dengan presisi
mikrodetik; batas yang lebih presisi dibulatkan ke atas agar perbandingan inklusif
tetap benar. Semua hasil dikembalikan tanpa batas tersembunyi. URL-encode tanda
`+` pada offset timezone (`%2B07:00`). Deduplication merupakan tugas Aggregator
karena batas `since` inklusif.

Setiap startup memastikan minimal 20 seed historis dengan ID stabil, mencakup
Normal, Waspada, Siaga, dan Awas. Gunung: `merapi`, `semeru`, `anak-krakatau`,
`marapi`, dan `lewotobi-laki-laki`. Generator menambahkan laporan secara periodik
dan tetap berjalan selama outage, sehingga Aggregator dapat mengambil data yang
tertinggal sesudah pemulihan.

Schema aktif dimulai dari v1. Setelah diubah ke v2, laporan baru menyimpan
`confidence_level` dalam rentang 0–1 di JSON SQLite; seed dan laporan v1 lama
boleh tidak memiliki field ini. Beralih ke v1 hanya menyembunyikan field dalam
respons. Beralih lagi ke v2 menampilkan nilai tersimpan, tanpa migrasi tabel atau
restart. Header `X-PVMBG-Schema-Version` konsisten dengan proyeksi respons.

Outage menyebabkan pembacaan data dan health mengembalikan 503 dengan
`Retry-After: 1`. Autentikasi data diperiksa sebelum outage. Admin melewati
outage dan delay sehingga service dapat dipulihkan langsung. Schema dan outage
direset menjadi v1/nonaktif saat restart; laporan SQLite tetap tersimpan.

## Konfigurasi

| Environment | Default | Batas |
| --- | --- | --- |
| `PVMBG_BEARER_TOKEN` | Wajib diisi | Token khusus PVMBG; bukan placeholder |
| `PVMBG_HTTP_PORT` | `8082` | 1–65535 |
| `PVMBG_STORE_DSN` | `sqlite:///data/pvmbg.sqlite3` | File SQLite persisten |
| `MOCK_SEED_COUNT` | `20` | Minimal 20 |
| `MOCK_EVENT_INTERVAL_MS` | `10000` | 1–10000 ms |
| `PVMBG_RESPONSE_DELAY_MIN_MS` | `500` | Nonnegatif, tidak melebihi maksimum |
| `PVMBG_RESPONSE_DELAY_MAX_MS` | `3000` | Minimal sebesar minimum |

Delay acak uniform hanya berlaku pada endpoint data. `ThreadingHTTPServer`
memisahkan request agar satu delay tidak menghentikan health atau admin. SQLite
memakai WAL, busy timeout, koneksi per operasi, dan transaksi tulis. Payload
tersimpan sebagai JSON agar field aditif dapat ditambahkan tanpa migrasi tabel;
kolom timestamp mikrodetik menyediakan indeks incremental polling.

Log JSON stdout menyertakan timestamp, level, service, correlation ID, operasi,
durasi, dan status. `X-Correlation-ID` divalidasi dan diteruskan atau dibuat bila
tidak ada; `traceparent` valid diteruskan ke log untuk diagnosis. Header rahasia,
query, dan body tidak dicatat. Error memakai `application/problem+json` dengan
bentuk RFC 9457. Implementasi ini belum mengekspor span OTLP atau metrik Prometheus.

Dockerfile menjalankan proses non-root pada port internal 8080 dan database
`/app/data/pvmbg.sqlite3`; mount volume tersendiri ke `/app/data` untuk persistensi.
