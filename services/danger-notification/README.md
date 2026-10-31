# Danger Notification

Consumer dan service baca notifikasi gRPC (.NET) pada port **9095**, melalui
[DangerNotificationService](../../contracts/proto/bnpb/notification/v1/notification.proto).

- **Input:** envelope dari queue durable `bnpb.danger-notification.v1`. Service
  mendeklarasikan exchange `RabbitMq:Exchange`, DLQ, dan queue-nya sendiri lalu
  mengikat queue ke exchange;
  RPC `ListNotifications` dan `GetNotification` dari Gateway.
- **Output:** notifikasi turunan hazard **SIAGA/AWAS**, lengkap dengan proyeksi
  yang diizinkan bagi aktor. Teks pesan hanya dibangun dari field ringkasan.
- **Storage:** store Postgres milik sendiri (EF Core, service Compose
  `notification-db`; `Persistence:Provider=postgres` dan
  `ConnectionStrings:Notifications`). Tanpa konfigurasi tersebut service memakai
  file SQLite lokal (satu replika saja; juga dipakai oleh test). Satu notifikasi per `event_id` berisi snapshot
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
  sampai `Outbox:MaxAttempts`.
- **Banyak replika (Postgres):** klaim outbox memakai satu `UPDATE … FOR UPDATE
  SKIP LOCKED`, sehingga replika yang polling bersamaan tidak pernah mengklaim
  baris yang sama. Revisi dari hazard yang sama diserialkan dengan advisory lock
  per `hazard_id`; partial unique index menjamin paling banyak satu revisi
  *current* per hazard, dan row version `xmin` mengubah tulisan yang bertabrakan
  menjadi konflik yang diulang. Di SQLite transaksi IMMEDIATE sudah menyerialkan
  penulis.
- **Migrasi:** dijalankan saat startup; EF memegang migration lock sehingga
  replika yang start bersamaan aman. Setiap provider punya context dan set
  migrasi sendiri; perubahan model butuh keduanya (`dotnet tool restore`, lalu
  dari `BNPB.Notification.Server`: `dotnet ef migrations add <Nama> --context
  PostgresNotificationDbContext --output-dir Adapters/Outbound/Persistence/Migrations/Postgres`
  dan sama untuk `SqliteNotificationDbContext` ke `Migrations/Sqlite`). Migrasi
  harus kompatibel dengan versi sebelumnya (expand/contract) agar rollback aman.
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
