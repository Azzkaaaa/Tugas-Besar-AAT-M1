# Skenario Problem Set Milestone 1 (regresi lokal)

Dipindahkan dari README utama pada 22 November 2026 tanpa perubahan isi. Berlaku untuk
stack Docker Compose lokal (lihat README, "Pengembangan lokal"). Skenario Milestone 2
dijalankan di cluster k3s; lihat repository infrastructure.

Pastikan sistem sudah berjalan. Ikuti petunjuk untuk membangun dan menjalankan sistem dari awal.

## Skenario Automated Testing (P1-5)

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

## Skenario Manual Testing
Untuk membantu jalannya testing manual, Anda perlu menjalankan test_menu (sebuah helper interaktif CLI) dari root dengan perintah berikut.
```bash
python tests/test_menu.py
```
### P1

1. Buka dashboard pada localhost:8080, Anda akan diminta masuk melalui Zitadel. Masukkan kredensial `media-demo`, `lapangan-demo`, atau `bnpb-demo` dengan password yang sudah di generate pada `.env.auth`.
2. Untuk kriteria penyelesaian 1, tekan tombol panah di bagian kanan tiap hazard event untuk melihat ringkasan atau detail penuh (tergantung akun). Event dari kedua sumber: BMKG dan PVMBG telah diformat sesuai format kanonikal. Perhatikan bahwa PVMBG tidak memiliki confidence_level.
3. Untuk kriteria penyelesaian 2, bump schema PVMBG dengan memilih menu pertama di CLI test_menu, masukkan versi 2.
4. Untuk kriteria penyelesaian 3, tunggu hingga event baru pada dashboard muncul dari PVMBG. Buka detailnya dan confidence_level seharusnya ada di attributes list.

### P2

1. Agak susah dilakukan manual. Untuk kriteria penyelesaian 1 dan 2, gunakan script `tests/load/aggregator.js` dan `tests/load/aggregator-isolation.js` menggunakan k6. Petunjuk lebih lengkap ada di [docs/load-testing.md](docs/load-testing.md).
2. Untuk kriteria penyelesaian 3, simulasikan outage menggunakan menu kedua dari CLI test_menu.
3. Anda bisa tembak API request ke `/api/v1/hazards` via envoy dengan memilih menu 4 di CLI test_menu untuk melihat bahwa PVMBG dinyatakan stale, namun tetap menyajikan data terakhir.
4. Atau Anda juga bisa lihat melalui dashboard bahwa tidak ada data PVMBG baru yang disajikan, dan pada bagian atas, tertulis bahwa PVMBG "terlambat" dengan waktu terakhir sebelum outage tertulis.

### P3

1. Untuk kriteria penyelesaian 1, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 1. Script akan menunjukkan bahwa kredensial hanya valid untuk masing-masing servis.
2. Untuk kriteria penyelesaian 2, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 2. Script akan menunjukkan bahwa client media tidak diperbolehkan meminta full_view dari event.
3. Untuk kriteria penyelesaian 3, gunakan menu ke-9 di CLI test_menu, dan pilih submenu 3. Script akan menunjukkan token di-refresh setelah TTL, dan token lama tidak dapat digunakan lagi.
4. Alternatif untuk kriteria penyelesaian 3, buka tab Network pada Devtools browser, dan lihat bahwa setelah beberapa waktu, akan ada pemanggila refresh.
5. Untuk kriteria penyelesaian 4, bisa menjalankan perintah seperti `rg -i "(key|token|secret|dsn)=" services/` dari root untuk menemukan apakah ada hardcoding variabel atau tidak.

### P4
1. Untuk kriteria penyelesaian 1, ketika melakukan docker compose up, terlihat bahwa sistem terdiri dari banyak servis yang terpisah. Anda juga bisa membuka `localhost:3000` untuk membuka dashboard Grafana. Pilih dashboard BNPB yang sudah disediakan, dan Anda dapat melihat availability dari masing-masing servis.
2. Anda bisa menjalankan `docker compose --env-file .env --env-file .env.auth stop notification-service` untuk menghentikan salah satu servis (misal notification-service).
3. Anda bisa menjalankan `docker compose --env-file .env --env-file .env.auth up --build --no-deps notification-service` untuk mem-build ulang notification-service. Selama notification-service, sistem dan dashboard masih tetap bekerja dan melayani permintaan.
4. Untuk kriteria penyelesaian 2, Anda bisa memverifikasi bahwa JSONB digunaka dengan masuk ke Canonical Store menggunakan `docker exec -it tugas-besar-aat-m1-aggregator-db-1 psql -U aggregator -d bnpb_canonical`, kemudian memasukkan `\d hazard_events`.
   Bila aggregator memakai Cassandra (default compose, `AGGREGATOR_STORE_BACKEND=cassandra`), `attributes` disimpan sebagai teks JSON tanpa skema tetap: jalankan `docker exec -it tugas-besar-aat-m1-aggregator-cassandra-1 cqlsh -e "DESCRIBE TABLE bnpb_canonical.hazards_by_id; SELECT source, attributes FROM bnpb_canonical.hazards_by_id LIMIT 5;"`.
5. Gunakan menu ke-10 dari CLI test_menu untuk menguji keberdampingan skema v1 dan v2, serta pengujian akses Canonical Store.

### P5
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
