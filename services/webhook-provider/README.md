# Webhook Provider

Rencana consumer RabbitMQ dan server manajemen gRPC pada port **9094**, melalui
[WebhookService](../../contracts/proto/bnpb/webhook/v1/webhook.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** envelope `HazardEventPublished` dari queue durable
  `bnpb.webhook-provider.v1`; RPC manajemen (`CreateWebhook`, `ListWebhooks`,
  `GetWebhook`, `DeleteWebhook`, `ListDeliveries`, `PingWebhook`) dari Gateway.
- **Output:** panggilan HTTP POST ke endpoint webhook eksternal terdaftar dengan
  payload JSON event bencana, signature HMAC (`X-BNPB-Signature`), serta respons
  manajemen webhook ke Gateway.
- **Storage milik sendiri:** `WEBHOOK_STORE_DSN` untuk menyimpan data pendaftaran
  webhook, secret penandatanganan, status langganan, dan riwayat upaya pengiriman
  (delivery log); bukan Canonical Store. DSN `postgres://` memakai Postgres
  (service Compose `webhook-db`, migrasi goose di `internal/store/migrations/postgres`
  dengan session lock sehingga replika yang start bersamaan aman); DSN lain
  dianggap SQLite (satu replika, juga dipakai oleh test).
- **Deduplikasi antar replika:** sebelum dispatch, setiap pasangan
  (`event_id`, webhook) diklaim di Redis (`WEBHOOK_REDIS_URL`, key
  `webhook:dedup:*`) dengan lease yang mencakup seluruh retry dispatcher. Hanya
  pemegang klaim yang mengirim; redelivery yang tiba di replika lain menunggu
  klaim selesai. Pengiriman sukses ditandai `delivered` selama
  `WEBHOOK_DEDUP_TTL_HOURS` (default 168); kegagalan melepas klaim. Redis berjalan
  tanpa persistence: bila key hilang, event yang di-redeliver bisa terkirim lagi
  (at-least-once), sehingga penerima tetap melakukan dedup dengan `event_id`.
  Tanpa `WEBHOOK_REDIS_URL` klaim disimpan di memori proses (satu replika).
- **Shutdown (SIGTERM):** consumer berhenti mengambil pesan baru, dan event yang sedang
  dikirim boleh selesai selama `WEBHOOK_SHUTDOWN_GRACE_SECONDS` (default 25). Bila waktu
  itu habis, pengiriman dihentikan, klaim Redis dilepas, percobaan itu tidak dicatat sebagai
  `FAILED`, dan pesan dikembalikan ke queue (requeue, juga untuk redelivery) sehingga replika
  lain langsung mengirimnya. Store dan klaim baru ditutup setelah pesan itu di-ack atau
  dikembalikan. Nilai ini harus di bawah `terminationGracePeriodSeconds` pod (default k8s 30
  detik); di Compose `stop_grace_period` diset 30 detik.
- **Tanggung jawab:**
  - Evaluasi filter langganan terhadap setiap event masuk dari broker.
  - Penegakan proyeksi data: webhook milik client Media hanya menerima payload
    ringkasan (SUMMARY); webhook milik Tim Lapangan / BNPB Internal dapat menerima
    payload lengkap (FULL).
  - Deduplikasi event berdasarkan `event_id` broker dan pengiriman idempotensial.
  - Mekanisme retry dengan exponential backoff dan dead-letter queue untuk endpoint
    yang gagal atau timeout.
  - Otorisasi pemanggil melalui Auth menggunakan token service `WEBHOOK_SERVICE_TOKEN`
    dan bearer token aktor asli.

Health memakai `grpc.health.v1.Health`. TODO: runtime/Dockerfile, consumer RabbitMQ,
HTTP dispatch client, storage dan migrasi skema webhook, retry/dead-letter, telemetry.
Lihat [konfigurasi](../../docs/configuration.md).
