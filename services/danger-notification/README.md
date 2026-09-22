# Danger Notification

Rencana consumer dan service baca notifikasi gRPC pada port **9095**, melalui
[DangerNotificationService](../../contracts/proto/bnpb/notification/v1/notification.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope dari queue durable `bnpb.danger-notification.v1`;
  RPC `ListNotifications` dan `GetNotification` dari Gateway.
- **Output:** notifikasi turunan hazard **SIAGA/AWAS**, lengkap dengan proyeksi
  yang diizinkan bagi aktor. Teks pesan hanya dibangun dari field ringkasan.
- **Storage milik sendiri:** `NOTIFICATION_STORE_DSN`; notifikasi, state
  idempotensi, dan snapshot pagination. Tidak mengakses Canonical Store langsung.
- **Tanggung jawab:** deduplikasi event, persistensi sebelum ack, filter/order
  sesuai kontrak, dan scope notifikasi beserta scope hazard. Freshness sumber
  diminta lewat API Aggregator; metadata ini bukan jaminan consumer sudah
  mengolah event broker paling baru. Panggilan ke Auth/Aggregator memakai token
  service Notification sendiri dengan bearer aktor asli pada jalur baca.

Health memakai `grpc.health.v1.Health`. TODO: runtime/Dockerfile, consumer,
storage, retry/dead-letter, gRPC, otorisasi/proyeksi, telemetry, dan uji recovery
tanpa notifikasi ganda. Pengiriman email/SMS/push eksternal belum menjadi
kontrak implementasi. Lihat [konfigurasi](../../docs/configuration.md).
