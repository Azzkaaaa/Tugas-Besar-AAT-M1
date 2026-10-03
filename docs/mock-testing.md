# Panduan testing Docker BMKG dan PVMBG

Jalankan semua perintah dari root repository. Contoh manual di bawah menggunakan
PowerShell pada Windows, Python 3.12, Docker Desktop dengan Linux containers,
dan Docker Compose v2. Docker Engine harus sudah berjalan. Tidak perlu memasang
dependency Python tambahan.

Gunakan `compose.mocks.yaml`; Compose utama juga memuat komponen BNPB yang masih
berupa rencana/placeholder. Panduan ini memeriksa dua sumber upstream melalui
HTTP dan persistensi SQLite, bukan seluruh alur sistem BNPB.

## 1. Siapkan dan jalankan container

```powershell
python tools/setup_mock_env.py
docker compose -f compose.mocks.yaml up --build -d --wait
docker compose -f compose.mocks.yaml ps
```

Helper membuat `.env` dan mengisi dua secret mock yang berbeda bila nilainya
belum tersedia/masih placeholder. Secret yang sudah diisi dipertahankan dan tidak
dicetak. Kedua container seharusnya berstatus `Up` dan `healthy`. Port default
BMKG adalah `8081`, PVMBG `8082`; keduanya dapat diubah di `.env`.

SQLite dan seed dibuat otomatis pada startup. Dengan konfigurasi default,
database baru berisi minimal 20 gempa BMKG, 5 warning tsunami terkait, dan 20
laporan PVMBG. Jumlah dapat lebih besar karena generator menambah data setiap
10 detik dan volume dari pengujian sebelumnya tetap digunakan.

## 2. Jalankan pengujian container otomatis

```powershell
python tools/test_mock_containers.py --restart-check
$LASTEXITCODE
```

Script mengakses container yang sudah berjalan melalui HTTP. Pemeriksaannya
mencakup health, autentikasi, payload, seed, filter `since`, generation, delay,
perubahan skema, outage dan recovery. Opsi `--restart-check` juga me-restart
service satu per satu untuk memastikan record lama tetap tersedia dan service
lain tetap dapat diakses. Script tidak melakukan build/start awal.

Hasil yang diharapkan adalah exit code `0`. Laporan aktual tersimpan di
`.cache/mock-container-test-results.json`; durasi dan jumlah data mengikuti
eksekusi saat itu. Perintah ini sementara mengaktifkan skema v2 dan outage PVMBG,
lalu mengembalikannya ke skema v1 dan outage nonaktif. Record baru selama test
tetap disimpan. Jika proses test dihentikan paksa, gunakan langkah recovery
manual pada bagian outage atau restart PVMBG.

Pemeriksaan konkurensi memakai ambang smoke test health/admin di bawah 1000 ms.
Ambang ini membantu mendeteksi request yang ikut tertahan delay data; ini bukan
batas latensi yang dijanjikan kontrak dan dapat terpengaruh host yang sangat sibuk.

Untuk mengulang tanpa restart container:

```powershell
python tools/test_mock_containers.py
```

## 3. Muat konfigurasi untuk pengujian manual

Jalankan satu kali dalam sesi PowerShell yang akan dipakai untuk langkah
berikutnya. Contoh membaca hanya secret dan port milik kedua mock tanpa
menampilkan secret. Format `.env` mengikuti hasil helper: satu `NAME=value` per
baris. Jika menggunakan override environment saat menjalankan Compose, gunakan
nilai override yang sama pada sesi pengujian.

```powershell
$mockConfig = @{}
Get-Content .env | ForEach-Object {
    if ($_ -match '^(BMKG_API_KEY|PVMBG_BEARER_TOKEN|BMKG_HTTP_PORT|PVMBG_HTTP_PORT)=(.*)$') {
        $mockConfig[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
    }
}
$bmkgPort = if ($mockConfig['BMKG_HTTP_PORT']) { $mockConfig['BMKG_HTTP_PORT'] } else { '8081' }
$pvmbgPort = if ($mockConfig['PVMBG_HTTP_PORT']) { $mockConfig['PVMBG_HTTP_PORT'] } else { '8082' }
$bmkgUrl = "http://127.0.0.1:$bmkgPort"
$pvmbgUrl = "http://127.0.0.1:$pvmbgPort"
$bmkgHeaders = @{ 'X-BMKG-Key' = $mockConfig['BMKG_API_KEY'] }
$pvmbgHeaders = @{ Authorization = "Bearer $($mockConfig['PVMBG_BEARER_TOKEN'])" }
```

### Health dan data awal

```powershell
Invoke-RestMethod "$bmkgUrl/health"
Invoke-RestMethod "$pvmbgUrl/health"

$gempa = Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders
$tsunami = Invoke-RestMethod "$bmkgUrl/tsunami-warnings" -Headers $bmkgHeaders
$vulkanik = Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders
[PSCustomObject]@{
    Gempa = @($gempa.items).Count
    WarningTsunami = @($tsunami.items).Count
    LaporanVulkanik = @($vulkanik.items).Count
}
$gempa.items | Select-Object -First 1
$tsunami.items | Select-Object -First 1
$vulkanik.items | Select-Object -First 1
```

Kedua health menghasilkan `status: ok`. Endpoint data menghasilkan objek JSON
berisi array `items`. Dengan konfigurasi default, jumlah gempa dan laporan
vulkanik minimal 20, warning minimal 5. `related_event_id` warning menunjuk
gempa dengan `potential_tsunami=true`.

### Autentikasi wajib dan kredensial terpisah

Gunakan `curl.exe` agar respons 401 dapat diperiksa tanpa perilaku exception
`Invoke-RestMethod` pada Windows PowerShell 5.1. Perintah hanya mencetak status
HTTP, tanpa body atau nilai secret.

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" "$bmkgUrl/seismic-events"
curl.exe -s -o NUL -w "%{http_code}`n" "$pvmbgUrl/volcanic-reports"
curl.exe -s -o NUL -w "%{http_code}`n" -H "X-BMKG-Key: $($mockConfig['PVMBG_BEARER_TOKEN'])" "$bmkgUrl/seismic-events"
curl.exe -s -o NUL -w "%{http_code}`n" -H "Authorization: Bearer $($mockConfig['BMKG_API_KEY'])" "$pvmbgUrl/volcanic-reports"
```

Keempat hasil harus `401`: dua tanpa kredensial, dua dengan secret milik service
lain. Health bersifat publik, sementara data dan admin PVMBG memerlukan secret.

### Filter waktu `since`

Gunakan timestamp record yang sudah ada agar batas inklusif bisa dibuktikan.
Timestamp harus RFC 3339 dengan timezone. Encoding berikut juga menangani `+`
pada offset timezone.

```powershell
$batasGempa = $gempa.items[-1]
$sinceGempa = [Uri]::EscapeDataString($batasGempa.occurred_at)
$hasilGempa = Invoke-RestMethod "$bmkgUrl/seismic-events?since=$sinceGempa" -Headers $bmkgHeaders
@($hasilGempa.items.event_id) -contains $batasGempa.event_id

$batasVulkanik = $vulkanik.items[-1]
$sinceVulkanik = [Uri]::EscapeDataString($batasVulkanik.reported_at)
$hasilVulkanik = Invoke-RestMethod "$pvmbgUrl/volcanic-reports?since=$sinceVulkanik" -Headers $pvmbgHeaders
@($hasilVulkanik.items.report_id) -contains $batasVulkanik.report_id
```

Kedua pemeriksaan menghasilkan `True`: record tepat pada waktu `since` ikut
dikembalikan, bersama record yang lebih baru. Untuk warning tsunami, `since`
memakai metadata perubahan internal `updated_at`, bukan `estimated_arrival`.
Metadata tersebut tidak ditampilkan dalam payload warning.

### Generator data baru

```powershell
$awalGempa = @((Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders).items).Count
$awalVulkanik = @((Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders).items).Count
Start-Sleep -Seconds 11
$akhirGempa = @((Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders).items).Count
$akhirVulkanik = @((Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders).items).Count
$akhirGempa -gt $awalGempa
$akhirVulkanik -gt $awalVulkanik
```

Keduanya diharapkan `True` karena interval default 10 detik. Generator berjalan
di latar belakang, tanpa perlu dipicu polling. Jumlah pertambahan tidak harus
tepat satu karena waktu request dan jadwal generator juga berjalan.

### Delay respons

```powershell
1..5 | ForEach-Object {
    $bmkgMs = (Measure-Command {
        Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders | Out-Null
    }).TotalMilliseconds
    $pvmbgMs = (Measure-Command {
        Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders | Out-Null
    }).TotalMilliseconds
    [PSCustomObject]@{
        Percobaan = $_
        BMKG_ms = [math]::Round($bmkgMs, 1)
        PVMBG_ms = [math]::Round($pvmbgMs, 1)
    }
}
```

Dengan konfigurasi default, delay buatan BMKG selalu 100 ms, sedangkan PVMBG
memilih nilai acak setiap request dalam rentang 500 sampai 3000 ms. Durasi yang
terukur juga mencakup jaringan, pemrosesan, dan decoding JSON; angka aktual
bisa melebihi batas delay buatan. Lima sampel memperlihatkan perilaku, bukan
bukti statistik distribusi. Health dan admin tidak dikenai delay buatan.

### Perubahan skema v1 ke v2 lalu kembali ke v1

```powershell
try {
    Invoke-RestMethod "$pvmbgUrl/admin/schema-version" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"version":2}'
    Start-Sleep -Seconds 11
    $v2 = Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders
    $denganConfidence = @($v2.items | Where-Object { $_.PSObject.Properties.Name -contains 'confidence_level' })
    $denganConfidence.Count -gt 0
    $denganConfidence | Select-Object -Last 3 report_id, reported_at, confidence_level
}
finally {
    Invoke-RestMethod "$pvmbgUrl/admin/schema-version" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"version":1}'
}
$v1 = Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders
@($v1.items | Where-Object { $_.PSObject.Properties.Name -contains 'confidence_level' }).Count
```

Admin mengembalikan `version: 2`, kemudian minimal satu report memiliki
`confidence_level` antara 0 dan 1. Record historis v1 boleh tidak memiliki field
tersebut. Sesudah kembali ke v1, hitungan terakhir harus `0`. Nilai confidence
tetap tersimpan di SQLite; v1 hanya menyembunyikannya dalam respons.

### Outage PVMBG dan pemulihan

```powershell
try {
    Invoke-RestMethod "$pvmbgUrl/admin/outage" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"enabled":true}'
    curl.exe -s -o NUL -w "%{http_code}`n" "$pvmbgUrl/health"
    curl.exe -s -o NUL -w "%{http_code}`n" -H "Authorization: Bearer $($mockConfig['PVMBG_BEARER_TOKEN'])" "$pvmbgUrl/volcanic-reports"
    curl.exe -s -o NUL -w "%{http_code}`n" -H "X-BMKG-Key: $($mockConfig['BMKG_API_KEY'])" "$bmkgUrl/seismic-events"
}
finally {
    Invoke-RestMethod "$pvmbgUrl/admin/outage" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"enabled":false}'
}
Invoke-RestMethod "$pvmbgUrl/health"
```

Urutan status curl harus `503`, `503`, `200`. Admin tetap dapat memulihkan
PVMBG tanpa restart; health terakhir kembali `status: ok`. Jika outage cukup
lama, Docker dapat menandai PVMBG `unhealthy`; status akan kembali `healthy`
setelah healthcheck berikutnya berhasil. Generator tetap menambah data selama
outage.

Untuk recovery langsung bila pengujian sebelumnya terputus:

```powershell
Invoke-RestMethod "$pvmbgUrl/admin/outage" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"enabled":false}'
Invoke-RestMethod "$pvmbgUrl/admin/schema-version" -Method Post -Headers $pvmbgHeaders -ContentType application/json -Body '{"version":1}'
```

### Persistensi SQLite setelah restart

```powershell
$idGempaSebelum = @((Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders).items.event_id)
$idVulkanikSebelum = @((Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders).items.report_id)
docker compose -f compose.mocks.yaml restart bmkg-mock pvmbg-mock
docker compose -f compose.mocks.yaml up -d --wait
$idGempaSesudah = @((Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders).items.event_id)
$idVulkanikSesudah = @((Invoke-RestMethod "$pvmbgUrl/volcanic-reports" -Headers $pvmbgHeaders).items.report_id)
@($idGempaSebelum | Where-Object { $_ -notin $idGempaSesudah }).Count
@($idVulkanikSebelum | Where-Object { $_ -notin $idVulkanikSesudah }).Count
```

Kedua hasil harus `0`: tidak ada ID lama yang hilang. Jumlah data boleh bertambah
selama pengujian. BMKG memakai volume `bmkg_data`, PVMBG memakai `pvmbg_data`
(nama Docker diberi prefix proyek Compose). Seed dengan ID tetap tidak
digandakan saat startup. Restart PVMBG mengembalikan skema ke v1 dan outage
nonaktif, tetapi record dan confidence yang tersimpan tetap bertahan.

### Hentikan satu service, periksa service lain

```powershell
try {
    docker compose -f compose.mocks.yaml stop pvmbg-mock
    Invoke-RestMethod "$bmkgUrl/health"
    @((Invoke-RestMethod "$bmkgUrl/seismic-events" -Headers $bmkgHeaders).items).Count
}
finally {
    docker compose -f compose.mocks.yaml up -d --wait pvmbg-mock
}
```

BMKG tetap menghasilkan `status: ok` dan data ketika container PVMBG berhenti.
Perintah pada `finally` menjalankan PVMBG kembali.

## 4. Pengujian Python tanpa Docker

Suite berikut berbeda dari test terhadap container yang sedang berjalan:

```powershell
python -m unittest discover -s tests -v
python -m unittest discover -s services/bmkg-mock -p test_storage.py -v
```

Suite pertama memuat 15 pengujian HTTP melalui proses Python dan database
sementara. Suite kedua memuat 7 pengujian storage BMKG. Keduanya diharapkan
berakhir `OK`; tidak menggunakan volume SQLite container.

## 5. Log dan menghentikan lingkungan

```powershell
docker compose -f compose.mocks.yaml logs --tail 100 bmkg-mock pvmbg-mock
docker compose -f compose.mocks.yaml ps
docker compose -f compose.mocks.yaml down
```

Log request mencantumkan status, durasi, dan correlation ID. `down` menghentikan
dan menghapus container/network tetapi mempertahankan named volume. Jalankan
`up --build -d --wait` kembali untuk memakai riwayat database yang sama. Jangan
tambahkan `-v` jika ingin mempertahankan data; opsi tersebut menghapus volume.

Hasil yang diharapkan saat semua pemeriksaan berhasil: script container keluar
dengan kode `0`, kedua suite Python berakhir `OK`, autentikasi yang salah `401`,
outage PVMBG `503` sementara BMKG tetap `200`, kedua health normal `200`, serta
jumlah ID lama yang hilang setelah restart `0`. Gunakan laporan eksekusi untuk
melihat jumlah record dan durasi aktual; angka tersebut berubah setiap run.

## Hasil verifikasi 30 September 2026

Build kedua image dan startup melalui `up --build -d --wait` berhasil pada
Docker Engine 28.5.1. Eksekusi terakhir script container pada
`2026-09-30T04:36:35Z` sampai `2026-09-30T04:37:38Z` menghasilkan **10 dari 10
kelompok pemeriksaan lulus**, termasuk restart masing-masing container.

| Pemeriksaan | Hasil aktual |
| --- | --- |
| Health publik, seed, relasi warning, dan payload | Lulus |
| Kredensial kosong/silang dan admin tanpa token | 7 request ditolak 401 |
| Validasi input, correlation ID, filter inklusif/timezone | Lulus |
| Generator dan skema v1 → v2 → v1 → v2 | Data baru muncul; confidence tersimpan |
| Outage sekitar 10,67 detik | PVMBG 503, BMKG tetap 200; satu report dibuat selama outage |
| BMKG, 6 sampel HTTP | 115,01–121,57 ms; median 118,27 ms |
| PVMBG, 6 sampel HTTP | 848,88–2646,27 ms; median 1948,10 ms |
| Empat pembacaan PVMBG bersamaan | Health 16,22 ms; admin 5,05 ms; data BMKG 115,76 ms |
| Restart dan persistensi | 80 record yang diambil sebelum restart tetap identik pada masing-masing service |
| Isolasi restart | Service lain tetap sehat selama 18/21 pemeriksaan HTTP |

Pemeriksaan tambahan di dalam container memastikan proses berjalan sebagai UID
10001, mode SQLite `wal`, serta `PRAGMA integrity_check` menghasilkan `ok` untuk
kedua database. Mount `/app/data` menggunakan dua named volume berbeda.

Kondisi akhir: kedua health kembali `ok`, PVMBG skema v1, outage nonaktif.
Container dibiarkan berjalan untuk pengujian lanjutan. Jumlah record akan terus
bertambah. Angka di atas adalah sampel pengujian fungsional lokal, bukan hasil
benchmark sustained 50 koneksi selama 60 detik.
