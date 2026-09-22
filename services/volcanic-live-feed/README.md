# Volcanic Live Feed

Rencana consumer RabbitMQ dan server streaming gRPC pada port **9094** melalui
[VolcanicFeedService](../../contracts/proto/bnpb/feed/v1/feed.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope `HazardEventPublished` dari queue durable
  `bnpb.volcanic-feed.v1`; RPC `SubscribeVolcanic` dengan view dan cursor replay.
- **Output:** stream hazard **PVMBG/VOLCANIC** untuk Gateway/SSE. Consumer
  memfilter salinan event dari exchange bersama.
- **Storage milik sendiri:** `VOLCANIC_FEED_CACHE_DSN` untuk deduplikasi,
  sequence/epoch, dan replay terbatas; terpisah dari cache feed seismik.
- **Tanggung jawab:** ack setelah pemrosesan tersimpan, idempotensi, replay ke
  live tanpa celah, serta proyeksi dan validasi sesi termasuk saat idle. Token
  service feed sendiri dipakai saat menghubungi Auth dengan bearer aktor asli.
  Cursor feed seismik tidak dapat dipakai pada feed ini.

Health memakai `grpc.health.v1.Health`. TODO: runtime/Dockerfile, consumer,
storage replay, retry/dead-letter, otorisasi dan pencabutan stream, telemetry,
serta uji redelivery dan replay. Outage sumber tidak membuat service feed menjadi
pemilik polling PVMBG; polling tetap tugas Aggregator. Lihat
[konfigurasi](../../docs/configuration.md).
