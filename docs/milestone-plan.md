# Rencana penerimaan Milestone 1

Dokumen ini menurunkan kriteria dari **IF4031 - Tugas Besar - M1.pdf** menjadi
pekerjaan implementasi dan demo berikutnya. Repo saat ini menyiapkan struktur
dan kontrak lintas bahasa. Checkbox di bawah adalah **target yang belum
dibuktikan**; keberadaan OpenAPI atau proto tidak membuktikan perilaku runtime,
kinerja, autentikasi, persistensi, maupun ketahanan sistem.

## Peta kontrak

| Problem | Kontrak yang menjadi acuan | Bukti yang perlu dibuat setelah implementasi |
| --- | --- | --- |
| P1 — Arsitektur dan interoperabilitas data | [BMKG](../contracts/openapi/bmkg.yaml), [PVMBG](../contracts/openapi/pvmbg.yaml), [common.proto](../contracts/proto/bnpb/common/v1/common.proto), [aggregator.proto](../contracts/proto/bnpb/aggregator/v1/aggregator.proto) | Data sumber dan hasil kanonik sebelum/sesudah perubahan skema, log satu alur ingest |
| P2 — Konkurensi, ketersediaan, degradasi terkendali | [Gateway](../contracts/openapi/gateway.yaml), [PVMBG](../contracts/openapi/pvmbg.yaml), [aggregator.proto](../contracts/proto/bnpb/aggregator/v1/aggregator.proto) | Hasil uji beban, latensi per sumber, respons selama outage dan setelah pemulihan |
| P3 — Autentikasi dan pemisahan kepercayaan | Ketiga OpenAPI, [auth.proto](../contracts/proto/bnpb/auth/v1/auth.proto), [common.proto](../contracts/proto/bnpb/common/v1/common.proto), [feed.proto](../contracts/proto/bnpb/feed/v1/feed.proto), [webhook.proto](../contracts/proto/bnpb/webhook/v1/webhook.proto) | Penolakan kredensial silang, proyeksi Media, token kedaluwarsa/refresh/revoke |
| P4 — Batas service dan penyimpanan fleksibel | [aggregator.proto](../contracts/proto/bnpb/aggregator/v1/aggregator.proto), [auth.proto](../contracts/proto/bnpb/auth/v1/auth.proto), [feed.proto](../contracts/proto/bnpb/feed/v1/feed.proto), [notification.proto](../contracts/proto/bnpb/notification/v1/notification.proto), [webhook.proto](../contracts/proto/bnpb/webhook/v1/webhook.proto) | Restart satu container, bukti kepemilikan storage, record dua versi skema yang tersimpan bersamaan |
| P5 — Distribusi event ke banyak consumer | [event.proto](../contracts/proto/bnpb/event/v1/event.proto), [feed.proto](../contracts/proto/bnpb/feed/v1/feed.proto), [notification.proto](../contracts/proto/bnpb/notification/v1/notification.proto), [webhook.proto](../contracts/proto/bnpb/webhook/v1/webhook.proto) | Event pada queue independen, penghentian/pemulihan consumer, penambahan subscriber |

Semantik metadata, proyeksi, pagination, replay, dan pemetaan error gRPC ke HTTP
ada di [panduan proto](../contracts/proto/README.md).

## Prasyarat demo bersama

- [ ] Pilih bahasa dan runtime tiap service, lalu implementasikan mock BMKG,
  mock PVMBG, Aggregator, Auth, Gateway, Event Live Feed, Webhook Provider,
  Danger Notification, dan SPA sesuai batas arsitektur.
- [ ] Sediakan Dockerfile terpisah untuk setiap service dan satu perintah
  orkestrasi untuk menjalankan seluruh sistem. Setiap service dapat dihentikan
  dan dijalankan sendiri. Langkah ini belum dikerjakan pada tahap kontrak.
- [ ] Semua komunikasi antar-service melewati REST, gRPC, atau RabbitMQ.
  Gateway membaca data kanonik melalui Aggregator; setiap storage hanya
  memiliki satu service pemilik. Tidak ada akses langsung lintas service ke
  database, Redis, atau Canonical Store.
- [ ] Setiap mock menyediakan minimal **20 record historis** saat mulai dan
  menghasilkan minimal **1 event baru per 10 detik per instansi**. Mock bersifat
  pasif; Aggregator melakukan polling dengan interval dapat dikonfigurasi,
  disarankan **2–5 detik**.
- [ ] Dokumentasikan port default **Gateway 8080, BMKG 8081, PVMBG 8082** dan
  override konfigurasi. BMKG memakai delay tetap dalam rentang **50–150 ms**;
  PVMBG memakai delay acak **500–3.000 ms** yang dapat dikonfigurasi.
- [ ] Isi konfigurasi runtime dari environment atau berkas lokal yang tidak
  di-commit. Lengkapi `.env.example` dengan placeholder, tanpa kredensial nyata.
  Sediakan tiga identitas dan kredensial berbeda untuk Media, Tim Lapangan,
  dan BNPB Pusat; semua client M1 hanya membaca data.
- [ ] Implementasikan health check tiap service, log terstruktur berisi
  correlation ID yang diteruskan lintas service, dan pengukuran latensi tiap
  panggilan keluar. Hubungkan OpenTelemetry, Prometheus, Jaeger, dan Grafana
  sesuai arsitektur serta simpan bukti yang tidak memuat token/secret.

## P1 — Arsitektur dan interoperabilitas data

- [ ] Mulai PVMBG pada versi skema 1 tanpa `confidence_level`. Buktikan kedua
  sumber menghasilkan `HazardEvent` sesuai aturan pemetaan.
- [ ] Korelasikan `TsunamiWarning.related_event_id` dengan
  `SeismicEvent.event_id`. Severity mengikuti warning terkait; bila tidak ada
  warning, gunakan NORMAL untuk magnitude < 5, WASPADA untuk 5 ≤ magnitude
  < 6,5, dan SIAGA untuk magnitude ≥ 6,5. Simpan atribut sumber dan warning.
- [ ] Petakan `volcano_id` ke nama dan koordinat dari tabel referensi statis
  milik BNPB. Petakan `alert_level` langsung ke severity kanonik.
- [ ] Panggil `POST /admin/schema-version` dengan `{"version":2}` saat sistem
  berjalan. Tunjukkan `confidence_level` hadir di `attributes`, data lama tetap
  terbaca, dan tidak ada restart, redeploy, migrasi, atau perubahan kode manual.
- [ ] Buktikan kebijakan field tambahan: field yang tidak memiliki padanan
  kanonik dipertahankan di `attributes`. Jelaskan bahwa penambahan field
  didukung; perubahan tipe, penghapusan field wajib, atau perubahan makna
  bukan perubahan kompatibel otomatis.

## P2 — Konkurensi, ketersediaan, dan degradasi terkendali

- [ ] Atur delay PVMBG menjadi **3 detik**. Kirim permintaan BMKG-only bersamaan
  dengan permintaan PVMBG. Buktikan **p95 BMKG-only < 300 ms**; tampilkan
  konfigurasi, waktu pengukuran, dan jumlah sampel.
- [ ] Jalankan beban sustained **minimal 50 koneksi paralel selama 60 detik**.
  Sistem tidak crash. Laporkan throughput, p50/p95/p99, jumlah permintaan,
  seluruh kelas status, dan error rate di luar penolakan terkontrol **< 1%**.
  Laporkan jumlah HTTP 429 secara terpisah; jangan menyembunyikannya dari
  jumlah permintaan keseluruhan. Catat rumus error rate yang dipakai.
- [ ] Panggil `POST /admin/outage` dengan `{"enabled":true}` dan pertahankan
  outage selama **10–20 menit**, dikendalikan operator. Buktikan BMKG tetap
  dilayani, sementara data PVMBG terakhir tersedia dengan status STALE dan
  `stale_since`; bila data yang diminta belum memiliki cache usable, respons
  ketidaktersediaan harus eksplisit. Jangan memakai HTTP 500 atau membiarkan
  permintaan menggantung sampai timeout client sebagai mekanisme degradasi.
- [ ] Matikan outage melalui `{"enabled":false}`. Buktikan polling pulih dan
  metadata kebaruan kembali FRESH tanpa restart. Sertakan kasus cold start
  tanpa cache serta polling berhasil dengan hasil kosong.
- [ ] Dokumentasikan model konkurensi, batas kapasitas, timeout, retry/backoff,
  dan isolasi polling per sumber. Simpan konfigurasi dan hasil alat uji beban;
  angka di atas adalah target, bukan hasil benchmark repo saat ini.

## P3 — Autentikasi dan pemisahan kepercayaan

- [ ] Buktikan kredensial valid BMKG (`X-BMKG-Key`) ditolak PVMBG, serta token
  PVMBG (`Authorization: Bearer ...`) ditolak BMKG, dengan **401/403**.
  Token downstream BNPB juga tidak dapat menggantikan kedua kredensial itu.
- [ ] Login sebagai masing-masing dari tiga identitas client. Server menentukan
  role dan scope berdasarkan registrasi identitas; client tidak boleh memilih
  atau menaikkan role melalui request.
- [ ] Buktikan Media hanya menerima tujuh field ringkasan: `hazard_id`,
  `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, dan
  `ingested_at`. `source_ref_id`, `latitude`, `longitude`, dan `attributes`
  harus tidak hadir. Periksa list/detail, SSE, dan data turunan notifikasi.
- [ ] Minta `view=full` sebagai Media dan buktikan server mengembalikan **403**,
  termasuk pada feed. Penolakan harus terjadi sebelum payload mentah dikirim.
- [ ] Tunggu token Tim Lapangan kedaluwarsa secara alami dengan TTL default
  **60 detik**. Buktikan akses dengan token kedaluwarsa ditolak, refresh
  menghasilkan token baru tanpa login manual, token lama tetap ditolak,
  dan refresh token lama tidak dapat digunakan lagi setelah rotasi.
- [ ] Buktikan logout mencabut sesi; token dicabut tidak diterima lagi oleh
  RPC baca atau stream aktif. Stream juga berhenti saat token kedaluwarsa,
  sekalipun sedang tidak ada event baru.
- [ ] Periksa tidak ada secret nyata di kode, konfigurasi ter-commit, log,
  atau trace. Jelaskan mekanisme pemisahan kredensial, verifikasi scope,
  expiry, rotasi, dan pencabutan sesi dalam laporan.

## P4 — Batas service dan penyimpanan fleksibel

- [ ] Hentikan, rebuild, dan jalankan ulang satu service tanpa rebuild/restart
  container lain. Tunjukkan alur yang tidak bergantung padanya tetap berjalan.
- [ ] Pilih storage kanonik yang menerima atribut dinamis tanpa migrasi untuk
  `confidence_level`. Simpan lalu baca record sebelum dan sesudah perubahan
  skema secara berdampingan.
- [ ] Buktikan hanya Aggregator memiliki akses langsung ke Canonical Store.
  Gateway dan komponen lain menggunakan API pemilik data. Berlakukan prinsip
  satu pemilik juga pada store Auth, Notification, dan replay feed.
- [ ] Jelaskan alasan batas service dan bandingkan minimal dua alternatif
  penyimpanan beserta konsekuensinya. Bahasa aplikasi dan pilihan storage
  masih menjadi keputusan implementasi berikutnya.

## P5 — Distribusi event ke banyak consumer

- [ ] Publikasikan setiap hazard baru melalui RabbitMQ dengan envelope
  `HazardEventPublished` setelah commit record kanonik. Terapkan outbox
  transaksional atau mekanisme durable setara agar kegagalan publikasi dapat
  dipulihkan tanpa kehilangan event.
- [ ] Demonstrasikan minimal **dua consumer independen pada stream yang sama**
  menerima salinan event lewat **queue durable yang berbeda**. Rancangan ini
  menyediakan queue sendiri bagi Event Live Feed, Webhook Provider, dan Danger Notification;
  queue tidak dibagi di antara ketiga jenis consumer itu.
- [ ] Hentikan satu consumer, biarkan event baru terbit, dan buktikan producer
  serta consumer lain tetap berjalan. Nyalakan kembali consumer; event selama
  berhenti harus tersusul dari queue sesuai kebijakan delivery at-least-once.
  Simpan event ID sebelum/selama/setelah gangguan sebagai bukti.
- [ ] Buktikan redelivery tidak menggandakan efek bisnis: consumer melakukan
  deduplikasi/idempotensi, ack setelah side effect tersimpan, dan retry
  terbatas sebelum dead-letter queue untuk event yang gagal permanen.
- [ ] Tambahkan consumer baru melalui binding/subscription broker tanpa
  mengubah kode producer. Bedakan backlog queue yang sudah ada dengan replay
  historis untuk subscriber baru; jangan menjanjikan riwayat sebelum queue
  dibuat tanpa mekanisme penyimpanan tambahan.
- [ ] Uji replay SSE melalui `Last-Event-ID` dalam retensi feed, serta
  `reset_required` bila cursor tidak dapat dilanjutkan. Replay feed merupakan
  perilaku terpisah dari redelivery queue broker.
- [ ] Jelaskan pilihan broker, pola fan-out, retensi, retry, delivery
  at-least-once, idempotensi, dan keterbatasan replay dalam laporan.

## Bukti dan laporan yang perlu dilengkapi

Simpan bukti hanya setelah skenario dijalankan: tanggal, commit, konfigurasi,
langkah, hasil aktual, correlation ID, log, dan grafik atau keluaran uji.
Jangan mencentang checklist berdasarkan validasi sintaks kontrak saja.

Kerangka laporan yang dapat dipakai:

1. Identitas kelompok dan anggota, tabel kontribusi, deskripsi lingkup M1.
2. Diagram arsitektur, protokol, batas container, kepemilikan storage, alur event,
   teknologi terpilih, alasan pilihan, dan asumsi.
3. P1–P5: rancangan solusi, alternatif/trade-off, batasan, jawaban butir laporan,
   dan bukti setiap kriteria dari checklist ini.
4. Ringkasan menjalankan sistem yang merujuk README dan konfigurasi pengujian.
5. Deklarasi penggunaan LLM yang diisi sesuai pekerjaan sebenarnya.

Contoh template deklarasi yang harus ditinjau kelompok:

> Kami menggunakan [alat/model dan tanggal] untuk [bagian yang dibantu].
> Hasil yang digunakan berada pada [berkas/commit]. Anggota [nama] meninjau
> [bagian] dan memvalidasinya melalui [langkah serta hasil]. Bagian yang belum
> diimplementasikan atau belum diverifikasi adalah [daftar].

Persyaratan pengumpulan pada PDF merupakan pekerjaan kelompok berikutnya.
Setup struktur dan kontrak ini tidak mengubah visibilitas GitHub, membuat tag
pengumpulan, atau mengirim formulir/laporan.
