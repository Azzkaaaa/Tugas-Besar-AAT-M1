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
  setelah commit. Memverifikasi caller dan hak aktor; panggilan ke Auth memakai
  token service Aggregator sendiri plus bearer aktor asli.

Health memakai `grpc.health.v1.Health` standar. TODO: runtime/Dockerfile,
storage fleksibel, worker polling, referensi gunung api, retry/deadline,
outbox/publisher, otorisasi dan proyeksi, telemetry, serta uji P1/P2/P4/P5.
Lihat [konfigurasi](../../docs/configuration.md) dan
[checklist demo](../../docs/milestone-plan.md).
