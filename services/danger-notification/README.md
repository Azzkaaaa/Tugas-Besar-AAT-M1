# Danger Notification

Rencana consumer dan service baca notifikasi gRPC pada port **9095**, melalui
[DangerNotificationService](../../contracts/proto/bnpb/notification/v1/notification.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope dari queue durable `bnpb.danger-notification.v1`. Service
  mendeklarasikan exchange `RabbitMq:Exchange`, DLQ, dan queue-nya sendiri lalu
  mengikat queue ke exchange;
  RPC `ListNotifications` dan `GetNotification` dari Gateway.
- **Output:** notifikasi turunan hazard **SIAGA/AWAS**, lengkap dengan proyeksi
  yang diizinkan bagi aktor. Teks pesan hanya dibangun dari field ringkasan.
- **Storage:** belum ada; daftar dan detail notifikasi diambil dari Aggregator
  melalui gRPC. Tidak mengakses Canonical Store langsung.
- **Tanggung jawab:** deduplikasi event, persistensi sebelum ack, filter/order
  sesuai kontrak, dan scope notifikasi beserta scope hazard. Freshness sumber
  diminta lewat API Aggregator; metadata ini bukan jaminan consumer sudah
  mengolah event broker paling baru. Panggilan ke Auth/Aggregator memakai token
  service Notification sendiri dengan bearer aktor asli pada jalur baca.

Health memakai `grpc.health.v1.Health`. Trace, metrik, dan log dikirim lewat
OTLP bila `OTEL_EXPORTER_OTLP_ENDPOINT` diisi; trace melanjutkan `traceparent`
dari header AMQP aggregator. TODO: runtime/Dockerfile, consumer, storage,
retry/dead-letter, gRPC, otorisasi/proyeksi, dan uji recovery tanpa notifikasi
ganda. Pengiriman email/SMS/push eksternal belum menjadi
kontrak implementasi. Lihat [konfigurasi](../../docs/configuration.md).
