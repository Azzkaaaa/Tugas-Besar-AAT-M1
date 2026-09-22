# Seismic Live Feed

Rencana consumer RabbitMQ dan server streaming gRPC pada port **9093** melalui
[SeismicFeedService](../../contracts/proto/bnpb/feed/v1/feed.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope `HazardEventPublished` dari queue durable
  `bnpb.seismic-feed.v1`; RPC `SubscribeSeismic` dengan view dan cursor replay.
- **Output:** stream hazard **BMKG/SEISMIC** untuk Gateway, yang menerjemahkannya
  menjadi SSE. Consumer memfilter salinan event dari exchange bersama.
- **Storage milik sendiri:** `SEISMIC_FEED_CACHE_DSN` untuk deduplikasi,
  sequence/epoch, dan replay terbatas; bukan Canonical Store kedua.
- **Tanggung jawab:** ack setelah pemrosesan tersimpan, idempotensi, replay tanpa
  celah ke live stream, proyeksi berdasarkan scope, dan penghentian stream saat
  token habis/dicabut termasuk saat idle. Validasi ke Auth memakai token service
  feed sendiri dan bearer aktor asli; cursor terikat client, feed, dan view.

Health memakai `grpc.health.v1.Health`. TODO: runtime/Dockerfile, consumer,
storage replay, deduplikasi, retry/dead-letter, otorisasi berkelanjutan, telemetry,
serta uji berhenti/pulih dan replay kedaluwarsa. Konfigurasi tersedia di
[inventaris](../../docs/configuration.md).
