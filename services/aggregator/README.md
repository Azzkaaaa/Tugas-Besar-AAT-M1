# Aggregator

Rencana service ingest dan pemilik data kanonik, gRPC pada port **9091**.
Kontrak: [AggregatorService](../../contracts/proto/bnpb/aggregator/v1/aggregator.proto),
[data bersama](../../contracts/proto/bnpb/common/v1/common.proto), dan
[envelope broker](../../contracts/proto/bnpb/event/v1/event.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** polling REST independen ke BMKG/PVMBG; RPC `ListHazards`,
  `GetHazard`, dan `GetSourceStatuses` dari pemanggil internal yang sah.
- **Output:** proyeksi hazard, freshness/partial, serta publikasi event kanonik
  melalui RabbitMQ. Tidak memanggil setiap consumer secara langsung.
- **Storage milik sendiri:** `CANONICAL_STORE_DSN`, termasuk checkpoint ingest,
  pemetaan referensi gunung api, snapshot pagination, dan outbox. Hanya
  Aggregator mengakses Canonical Store langsung.
- **Tanggung jawab:** korelasi tsunami, normalisasi, attributes dinamis,
  deduplikasi/upsert, isolasi polling, cache saat outage, serta publikasi durable
  setelah commit. Memverifikasi JWT access token melalui JWKS endpoint Zitadel
  (`AUTH_AUTHORITY`); key set di-cache dan di-refresh otomatis saat rotasi kunci.
  Service token antar-service dibandingkan dengan constant-time comparison.
- **Banyak replika:** setiap replika melayani gRPC dan mempublikasikan outbox.
  Publisher mengklaim baris dengan `FOR UPDATE SKIP LOCKED` dan lease
  (`next_attempt_at`), sehingga replika (termasuk dua pod saat rolling update)
  mengambil baris yang berbeda; baris milik publisher yang mati diambil lagi
  setelah lease 30 detik. Publikasi gagal diulang dengan backoff per baris
  (1 detik, dua kali lipat, maksimal 1 menit) tanpa menahan baris lain, lalu
  dipindah ke `outbox_dead_letters` setelah `OUTBOX_MAX_RETRIES`. Pengiriman tetap
  at-least-once; consumer melakukan dedup dengan `event_id`.
- **Leader ingest:** polling BMKG/PVMBG dan pembersihan outbox hanya berjalan di
  satu replika, yaitu pemegang advisory lock sesi Postgres. Bila leader mati,
  replika lain mengambil alih dalam ±5 detik. Dua poller sekaligus akan membuat
  revisi yang sama dengan `event_id` berbeda, yang tidak bisa di-dedup consumer.
- **Retensi outbox:** baris yang sudah terbit atau di-dead-letter dihapus setelah
  `OUTBOX_RETENTION_HOURS` (default 168); isi `outbox_dead_letters` tetap disimpan.
- **Migrasi:** goose, file SQL di `internal/store/migrations/postgres` yang
  di-embed ke binary dan dijalankan saat startup dengan session lock Postgres,
  sehingga replika yang start bersamaan aman. `00001_baseline.sql` adalah skema
  lama (sebelum goose) yang ditulis idempoten: database baru dibuat lengkap,
  database lama hanya dicatat di versi 1. Migrasi berikutnya menambah file
  `0000N_<nama>.sql` (`-- +goose Up`) dan mengikuti expand/contract. Build lama
  yang di-rollback melewati migrasi yang tidak dikenalnya, jadi skema yang lebih
  baru tidak ditimpa.

Health memakai `grpc.health.v1.Health` standar. TODO: runtime/Dockerfile,
storage fleksibel, worker polling, referensi gunung api, retry/deadline,
outbox/publisher, otorisasi dan proyeksi, telemetry, serta uji P1/P2/P4/P5.
Lihat [konfigurasi](../../docs/configuration.md) dan
[checklist demo](../../docs/milestone-plan.md).
