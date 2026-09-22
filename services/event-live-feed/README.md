# Event Live Feed

Rencana consumer RabbitMQ dan server streaming gRPC pada port **9093** melalui
[EventFeedService](../../contracts/proto/bnpb/feed/v1/feed.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope `HazardEventPublished` dari queue durable
  `bnpb.event-live-feed.v1`; RPC `SubscribeEvents` dengan filter opsional (source,
  hazard_type, severity), view, dan cursor replay.
- **Output:** stream hazard kanonik untuk Gateway, yang menerjemahkannya menjadi
  SSE (`/api/v1/feeds/events`, serta rute alias `/api/v1/feeds/seismic` dan
  `/api/v1/feeds/volcanic`).
- **Storage milik sendiri:** `EVENT_FEED_CACHE_DSN` (Redis / in-memory cache) untuk
  deduplikasi, sequence/epoch, dan buffer replay terbatas (default 600 detik);
  bukan Canonical Store kedua.
- **Tanggung jawab:** ack setelah pemrosesan tersimpan, idempotensi, replay tanpa
  celah ke live stream, proyeksi berdasarkan scope aktor, dan penghentian stream saat
  token habis/dicabut termasuk saat idle. Validasi ke Auth memakai token service
  feed sendiri dan bearer aktor asli; cursor terikat client, filter, dan view.

Health memakai `grpc.health.v1.Health`. TODO: runtime/Dockerfile, consumer,
storage replay, deduplikasi, retry/dead-letter, otorisasi berkelanjutan, telemetry,
serta uji berhenti/pulih dan replay kedaluwarsa. Konfigurasi tersedia di
[inventaris](../../docs/configuration.md).
