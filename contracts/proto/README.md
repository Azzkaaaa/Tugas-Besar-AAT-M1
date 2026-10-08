# Kontrak gRPC internal

Semua kontrak memakai Proto3 dan package berversi `bnpb.<domain>.v1`. Tidak ada
pilihan bahasa implementasi atau kode hasil generate yang diwajibkan.
Field number yang sudah dipakai tidak boleh dipakai ulang; saat menghapus field,
cadangkan nomor dan namanya dengan `reserved`. Perubahan yang merusak kompatibilitas
memerlukan package versi baru.

| File | Layanan / tipe |
| --- | --- |
| `bnpb/common/v1/common.proto` | Record kanonik, proyeksi, filter, pagination, freshness |
| `bnpb/aggregator/v1/aggregator.proto` | `AggregatorService`: `ListHazards`, `GetHazard`, `GetSourceStatuses` |
| `bnpb/feed/v1/feed.proto` | `EventFeedService.SubscribeEvents` |
| `bnpb/webhook/v1/webhook.proto` | `WebhookService`: `CreateWebhook`, `ListWebhooks`, `GetWebhook`, `DeleteWebhook`, `ListDeliveries`, `PingWebhook` |
| `bnpb/notification/v1/notification.proto` | `DangerNotificationService`: `ListNotifications`, `GetNotification` |
| `bnpb/event/v1/event.proto` | Envelope RabbitMQ `HazardEventPublished` |

Gateway mengubah REST/SSE ke gRPC. BMKG Mock dan PVMBG Mock hanya menyediakan REST
untuk polling Aggregator. Health gRPC menggunakan layanan standar
`grpc.health.v1.Health`; tidak ada RPC health khusus di kontrak bisnis.

Autentikasi memakai Zitadel dan Auth bridge HTTP, bukan `AuthService` gRPC.
Endpoint login, callback, sesi, refresh, dan logout ada di
[kontrak Auth](../openapi/auth.yaml). Browser mengirim cookie HttpOnly;
bridge memvalidasi token melalui introspeksi Zitadel lalu meneruskan access
token aktor asli sebagai bearer ke Gateway internal. Tidak ada token exchange
ke JWT internal. Kontrak bisnis di bawah tetap merupakan target integrasi
downstream; tersedianya login tidak membuktikan service gRPC sudah terlindungi.

Bridge Node.js menyimpan sesi browser di SQLite; pengguna dan grant dikelola
Zitadel dalam PostgreSQL. Komunikasi bridge dengan IdP memakai HTTP OIDC/OAuth.
Gateway REST→gRPC ([api-gateway](../../services/api-gateway/README.md)) berada pada
jaringan privat di belakang Envoy. Cookie tidak
dipakai sebagai metadata RPC dan tidak menggantikan pemeriksaan aktor di
service bisnis. Pengujian fixture frontend dengan login OIDC tidak menguji
proyeksi, expiry, atau revocation pada RPC downstream.

## Metadata dan batas waktu

Semua pemanggilan gRPC menyertakan:

- `x-service-token`: kredensial identitas layanan, disuplai melalui konfigurasi
  runtime. Jangan commit nilainya. Kredensial ini tidak menggantikan identitas
  pengguna dan tidak memberi akses FULL secara otomatis.
- `authorization: Bearer <access_token>`: access token Zitadel aktor asli,
  wajib pada RPC baca, subscription, dan mutasi yang memerlukan aktor.
  Cookie browser tidak diteruskan ke gRPC. Login/refresh/logout ditangani
  Auth bridge HTTP dan tidak menjadi RPC bisnis.
- `x-correlation-id`: ID request dari Gateway, diteruskan tanpa perubahan dan
  dikembalikan dalam metadata response. Gateway membuatnya bila belum ada.
- `traceparent` dan opsional `tracestate`: konteks penelusuran W3C dari pemanggil.

Gunakan TLS di luar lingkungan lokal. Token tidak boleh dimasukkan ke log, trace,
URL, atau payload broker. RPC unary wajib memiliki deadline; Gateway meneruskan
sisa deadline ke layanan berikutnya. Subscription memiliki lifetime terbatas
sesuai masa berlaku token; client harus reconnect dengan token baru. Pembatalan
request Gateway membatalkan RPC turunannya.

Layanan downstream tetap harus memvalidasi identitas pemanggil dan hak aktor.
Integrasi validasi Zitadel pada masing-masing service belum selesai; gunakan
introspeksi online dengan kredensial API service sendiri atau mekanisme validasi
terpusat yang kontraknya disepakati sebelum mengaktifkan jalur tersebut.
Jangan mempercayai header role/scope kiriman browser. Error/deadline validasi
tidak boleh membuat request diizinkan. Cache berdasarkan TTL saja tidak
membuktikan pencabutan sesi; feed harus memeriksa ulang saat idle sekalipun.

## Scope dan proyeksi

| Role | Scope |
| --- | --- |
| `MEDIA` | `hazards:read:summary`, `notifications:read`, `webhooks:read` |
| `FIELD_TEAM` | `hazards:read:summary`, `hazards:read:full`, `notifications:read`, `webhooks:read`, `webhooks:write` |
| `BNPB_INTERNAL` | `hazards:read:summary`, `hazards:read:full`, `notifications:read`, `webhooks:read`, `webhooks:write` |

Role berasal dari grant pengguna di proyek dan organisasi Zitadel yang
dikonfigurasi. Auth bridge memetakan role tersebut ke scope aplikasi pada tabel;
scope aplikasi ini bukan otomatis scope OAuth custom yang diterbitkan Zitadel.
Role dan scope tidak boleh ditentukan melalui input login. Tiga akun demonstrasi
merupakan pengguna berbeda dengan kredensial dan grant terpisah.
Seluruh client M1 hanya membaca record hazard; `webhooks:write` terbatas pada
pengelolaan langganan dan bukan hak membuat atau mengubah data kejadian.
Default `VIEW_UNSPECIFIED` adalah `VIEW_SUMMARY`. Semua hazard reads dan feed
memerlukan `hazards:read:summary`; view FULL juga memerlukan
`hazards:read:full`. Notification reads juga memerlukan `notifications:read`.
Webhook subscription reads memerlukan `webhooks:read`; pembuatan/penghapusan
memerlukan `webhooks:write`. Request FULL tanpa izin gagal `PERMISSION_DENIED`,
bukan diturunkan diam-diam.

`HazardProjection` memakai `oneof summary/full`. Summary hanya memiliki
`hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, dan
`ingested_at`. Koordinat, `source_ref_id`, serta `attributes` hanya ada di FULL.
Pesan notifikasi dan payload webhook Media juga harus disusun dari field summary agar tidak membocorkan
field FULL. Aggregator merupakan satu-satunya penulis record kanonik; feed
menyimpan replay sementara, Danger Notification menyimpan hasil turunannya, dan Webhook Provider
mengelola langganan webhook serta riwayat pengiriman.

Gateway memetakan enum Protobuf berprefiks ke label REST, misalnya
`SOURCE_BMKG` menjadi `BMKG`, `VIEW_SUMMARY` menjadi `summary`, dan
`SOURCE_STATUS_STALE` menjadi `STALE`. Gateway juga membuka pembungkus oneof
menjadi objek hazard sesuai OpenAPI, mengubah Timestamp menjadi RFC 3339 UTC,
dan `uint64 sequence` menjadi string desimal. REST **bukan** serialisasi
ProtoJSON langsung. Nilai enum tidak dikenal ditolak sebagai `INVALID_ARGUMENT`.

## Pagination, freshness, dan replay

Daftar hazard memakai snapshot stabil dengan urutan `occurred_at DESC,
hazard_id ASC`; daftar notifikasi memakai `created_at DESC, notification_id ASC`.
`since` inklusif dan `until` eksklusif berlaku pada waktu kejadian hazard,
termasuk untuk daftar notifikasi. Filter source atau hazard_type mempersempit
sumber yang relevan untuk freshness; tanpa keduanya, kedua sumber relevan.
Kombinasi BMKG/VOLCANIC dan PVMBG/SEISMIC ditolak sebagai INVALID_ARGUMENT.
`page_size=0` berarti 20; rentang eksplisit 1 sampai 100. Cursor terikat pada
filter, view yang sudah di-resolve, ukuran halaman, dan client terautentikasi.
Cursor tidak boleh memuat kredensial. Cursor salah format/konteks ditolak;
cursor kedaluwarsa tidak boleh mengulang dari halaman pertama diam-diam.

Polling berhasil dengan nol record tetap berarti sumber usable. Polling gagal
atau data melewati freshness window menghasilkan STALE bila cache tersedia.
Sumber tanpa cache usable menghasilkan UNAVAILABLE. `partial=true` bila ada
sumber relevan STALE atau UNAVAILABLE. List/Get tetap dapat mengirim cache;
jika seluruh sumber relevan tidak usable, RPC gagal UNAVAILABLE. Status sumber
diambil saat response dibuat, meskipun record daftar berada pada snapshot.

Setiap feed mempertahankan replay terbatas dan menerima `resume_after` dari
SSE `Last-Event-ID`. Cursor kosong berarti hanya kejadian live setelah subscribe.
Replay dimulai setelah cursor dan harus disambungkan ke live tanpa celah.
Cursor kedaluwarsa/kehilangan buffer menghasilkan `OUT_OF_RANGE`; Gateway
mengirim SSE `reset_required` lalu menutup stream agar client melakukan snapshot
ulang. Cursor salah tipe stream menghasilkan `INVALID_ARGUMENT`.

Feed mempertahankan `event_id` publikasi pada redelivery, memberi sequence naik
pada setiap kejadian baru dalam stream epoch, dan melakukan deduplikasi broker
sebelum menambahkan event ke replay. `FeedEvent.resume_cursor` menjadi SSE `id`
dan terikat pada client/feed/view; cursor bukan ID publikasi dan tidak dimasukkan
ke objek JSON SSE. Semua data hazard memakai nama event SSE `hazard.upsert`.
Restart harus mempertahankan replay/sequence atau
mengganti epoch dan menolak cursor lama. Gateway mengirim heartbeat SSE sebagai
komentar transport, bukan hazard palsu. Token yang habis atau dicabut menutup
stream dengan `UNAUTHENTICATED`, termasuk ketika tidak ada kejadian baru.

## Pemetaan error Gateway

| gRPC | HTTP | Arti |
| --- | --- | --- |
| `INVALID_ARGUMENT` | 400 | Input, enum, filter, atau konteks cursor tidak valid |
| `UNAUTHENTICATED` | 401 | Kredensial tidak valid, habis, atau dicabut |
| `PERMISSION_DENIED` | 403 | Scope/role tidak mencukupi |
| `NOT_FOUND` | 404 | ID tidak ditemukan dalam data yang usable |
| `OUT_OF_RANGE` | 409 | Cursor sudah tidak dapat dilanjutkan |
| `RESOURCE_EXHAUSTED` | 429 | Batas client atau kapasitas tercapai |
| `UNAVAILABLE` / `DEADLINE_EXCEEDED` | 503 | Dependency tidak dapat melayani |
| `INTERNAL` | 500 | Kegagalan tak terduga; detail internal tidak diekspos |

Sebelum header SSE dikirim, Gateway dapat mengembalikan HTTP error. Setelah
header terkirim, Gateway menutup stream pada error atau otorisasi berakhir;
khusus replay kedaluwarsa, kirim `reset_required` terlebih dahulu. Status HTTP
tidak dapat diubah. Kegagalan otorisasi tidak boleh membawa payload hazard.

## Envelope broker

`HazardEventPublished` diserialisasikan sebagai binary Protobuf dengan
content type `application/protobuf`, `schema_version=1`, dan publisher
`aggregator`. Payload selalu FULL untuk pemrosesan internal. `occurred_at` pada
envelope adalah waktu event publikasi; waktu kejadian bencana tetap di
`payload.occurred_at`. `source` envelope wajib sama dengan `payload.source`.

Publikasi dilakukan sesudah commit record kanonik menggunakan transactional
outbox atau mekanisme durable setara. Record baru maupun perubahan source yang
bermakna menghasilkan publikasi. Revisi mempertahankan `hazard_id` dan waktu
ingest pertama `ingested_at`, tetapi mendapat `event_id` publikasi baru. Retry
atas publikasi yang sama memakai `event_id` yang sama.
Tiga consumer memakai queue durable masing-masing agar setiap event dapat
diterima semuanya: `bnpb.danger-notification.v1`, `bnpb.event-live-feed.v1`, dan
`bnpb.webhook-provider.v1`. Feed menyalurkan stream ke client dengan filter opsional;
notification memilih SIAGA/AWAS; webhook provider memicu pengiriman HTTP POST ke
endpoint eksternal terdaftar. Consumer idempotent, ack setelah side effect tersimpan,
dan error permanen dialihkan ke dead-letter queue setelah retry terbatas. Skema envelope
tidak mengubah kebutuhan server untuk memvalidasi nilai dan scope pada runtime.

Referensi: [Proto3](https://protobuf.dev/programming-guides/proto3/),
[gRPC status codes](https://grpc.io/docs/guides/status-codes/).
