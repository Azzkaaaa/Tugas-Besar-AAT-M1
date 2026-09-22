# Rencana deployment

[Compose di root](../compose.yaml) saat ini hanya menjalankan Swagger UI pada
`localhost:8090`. Belum ada container aplikasi, broker, database, atau layanan
observability. Bahasa aplikasi dan teknologi storage belum dipilih.

Setelah implementasi tersedia, orkestrasi perlu menjalankan tiap service dalam
container terpisah dengan Dockerfile sendiri: BMKG Mock, PVMBG Mock, Aggregator,
Auth, Gateway, kedua Live Feed, Danger Notification, dan SPA. Sediakan satu
perintah untuk menjalankan keseluruhan sistem; tiap komponen tetap dapat
dihentikan/rebuild tanpa mengulang deployment komponen lain.

| Area | Rencana |
| --- | --- |
| Akses client | Gateway HTTP 8080; SPA mengakses REST/SSE Gateway |
| Mock sumber | BMKG HTTP 8081 dan PVMBG HTTP 8082; hanya Aggregator memerlukan jalur polling |
| API internal | Aggregator 9091, Auth 9092, Seismic Feed 9093, Volcanic Feed 9094, Notification 9095; gRPC pada jaringan internal |
| Health | `/health` untuk service REST; `grpc.health.v1.Health` standar untuk service gRPC |
| Storage | Jaringan/akses terpisah per pemilik; Gateway tidak memiliki jalur/akun database |
| RabbitMQ | Exchange `bnpb.hazards.v1`, tiga queue durable independen, akun publish/consume terpisah |
| Telemetry | Service mengekspor OTLP ke OpenTelemetry Collector; metrics ke Prometheus, trace ke Jaeger, visualisasi melalui Grafana |

Pisahkan jaringan akses publik, API internal, broker, observability, dan storage
sesuai kebutuhan koneksi. Jangan memberikan semua service semua jaringan.
Satu pemilik store tetap berlaku bila beberapa store memakai server fisik yang
sama: akun, database/namespace, izin, dan DSN tetap berbeda. UI admin broker dan
observability cukup dipublikasikan ke localhost untuk demo.

Provisioning RabbitMQ perlu exchange/queue durable, pesan persistent, publisher
confirm, manual ack setelah persistensi, retry terbatas, dan dead-letter queue.
Feed seismik, feed vulkanik, dan notifier memperoleh salinan event dari queue
masing-masing; menambah consumer tidak mengubah producer. Siapkan akun baru
dengan izin sesuai queue ketika menambah subscriber. Retensi broker bukan
retensi replay SSE.

Inject hanya konfigurasi yang diperlukan tiap service dari
[inventaris konfigurasi](../docs/configuration.md). Secret BMKG, PVMBG, setiap
client, tiap caller gRPC, dan tiap akun broker harus berbeda. Auth mengetahui
allowlist service; masing-masing caller hanya mendapat secret outbound sendiri.
Penerima internal mendapat verifier caller yang diizinkan. Gateway meneruskan
actor token bersama identitas Gateway; service downstream memakai identitasnya
sendiri ketika meminta otorisasi ke Auth.

TODO deployment: pilih image/runtime/storage, tentukan volume dan izin,
provisioning verifier/secret, readiness dan healthcheck, batas resource,
restart policy, konfigurasi collector/scrape/dashboard, serta orchestration
lengkap. Pastikan konfigurasi startup menolak placeholder/secret kosong.
Uji gangguan harus dipicu pada container/mock yang berjalan dan dibuktikan
menggunakan [checklist M1](../docs/milestone-plan.md); dokumentasi ini belum
merupakan bukti availability atau performa.
