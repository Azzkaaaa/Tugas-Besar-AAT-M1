# Danger Notification

Consumer dan service baca notifikasi gRPC (.NET) pada port **9095**, melalui
[DangerNotificationService](../../contracts/proto/bnpb/notification/v1/notification.proto).

- **Input:** envelope dari queue durable `bnpb.danger-notification.v1`. Service
  mendeklarasikan exchange `RabbitMq:Exchange`, DLQ, dan queue-nya sendiri lalu
  mengikat queue ke exchange;
  RPC `ListNotifications` dan `GetNotification` dari Gateway.
- **Output:** notifikasi turunan hazard **SIAGA/AWAS**, lengkap dengan proyeksi
  yang diizinkan bagi aktor. Teks pesan hanya dibangun dari field ringkasan.
- **Storage:** store SQLite milik sendiri (EF Core, `ConnectionStrings:Notifications`,
  volume `notification_data`). Satu notifikasi per `event_id` berisi snapshot
  hazard saat notifikasi dibuat. Revisi SIAGA/AWAS yang lebih baru untuk
  `hazard_id` yang sama membuat revisi lama *usang*: tidak dikembalikan oleh
  List/Get, tidak dikirim ke FCM, dan `push_status`-nya menjadi `UNSPECIFIED`
  kecuali sudah `DELIVERED`. Event NORMAL/WASPADA dibuang sebelum store.
  Daftar dan detail dibaca dari store ini; Aggregator hanya dipakai untuk
  freshness. Tidak mengakses Canonical Store langsung.
- **Outbox push:** store yang sama menjadi outbox lewat `push_status`. Consumer
  hanya menyimpan notifikasi `PENDING` lalu ack; worker outbox mengklaim baris
  dengan lease (token + kedaluwarsa), mengirim ke topic FCM, lalu mencatat
  `DELIVERED` atau `FAILED`. `FAILED` dicoba ulang dengan backoff eksponensial
  sampai `Outbox:MaxAttempts`. Port outbox (`IPushOutbox`) berbasis klaim/lease
  sehingga beberapa dispatcher aman dijalankan bila store diganti ke database
  jaringan; SQLite sendiri tetap berarti satu replika. Skema dibuat lewat migrasi
  EF Core saat startup (`dotnet tool restore && dotnet ef migrations add <Nama>
  --output-dir Adapters/Outbound/Persistence/Migrations` dari
  `BNPB.Notification.Server`).
- **Tanggung jawab:** deduplikasi event, persistensi sebelum ack, filter/order
  sesuai kontrak, dan scope notifikasi beserta scope hazard. Freshness sumber
  diminta lewat API Aggregator; metadata ini bukan jaminan consumer sudah
  mengolah event broker paling baru. Panggilan ke Auth/Aggregator memakai token
  service Notification sendiri dengan bearer aktor asli pada jalur baca.

Health memakai `grpc.health.v1.Health`. Trace, metrik, dan log dikirim lewat
OTLP bila `OTEL_EXPORTER_OTLP_ENDPOINT` diisi; trace melanjutkan `traceparent`
dari header AMQP aggregator; `fcm.send` dari outbox melanjutkan trace consume
yang disimpan bersama baris. TODO: uji recovery end-to-end tanpa notifikasi
ganda. Pengiriman email/SMS eksternal belum menjadi kontrak implementasi. Lihat [konfigurasi](../../docs/configuration.md).
