# Envoy: edge publik dan Gateway internal

Repo membedakan dua proses Envoy agar API publik selalu melalui pemeriksaan
Auth bridge. Tidak ada implementasi token exchange ke JWT internal.

| Proses | Konfigurasi | Fungsi |
| --- | --- | --- |
| `envoy` publik | [`services/zitadel/envoy.auth.yaml`](../zitadel/envoy.auth.yaml) | Host port 8080; SPA, `/auth/`, `/api/`, dan hosted login Zitadel |
| `gateway-transcoder` internal | [`envoy.yaml`](envoy.yaml), [`sse_filter.lua`](sse_filter.lua) | Rancangan REST/gRPC untuk layanan bisnis; hanya jaringan Compose |

Alur browser adalah `localhost:8080 → Auth bridge → gateway-transcoder → gRPC`.
Frontend dan Auth/API memakai origin yang sama. Host `auth.localhost:8080`
merutekan halaman Login UI v2 dan endpoint OIDC ke Zitadel. Host
`api.localhost:8080` juga melewati bridge; bukan jalan untuk melewati autentikasi.
Gunakan alamat `localhost` yang dikonfigurasi untuk login browser agar cookie,
callback, dan pemeriksaan Origin sesuai.

Bridge mengikat cookie access token ke sesi lokal, melakukan introspeksi
Zitadel, memetakan role proyek/organisasi ke scope, dan memeriksa Origin/CSRF
untuk mutasi berbasis cookie. Bridge meneruskan access token aktor **asli**
sebagai bearer serta `GATEWAY_SERVICE_TOKEN` sebagai `x-service-token` kepada
upstream privat. Client API dapat memakai bearer tanpa cookie. Login, callback,
sesi, refresh, dan logout merupakan HTTP API pada bridge, bukan RPC Auth.
Lihat [kontrak Auth](../../contracts/openapi/auth.yaml) dan
[implementasinya](../auth-bridge/README.md).

`compose.auth.yaml` cukup untuk menguji login nyata dengan fixture dashboard.
Tanpa `GATEWAY_UPSTREAM_URL`, request API bisnis yang lolos pemeriksaan Auth
mendapat 503. `compose.yaml` memakai upstream `http://gateway-transcoder:8080`
untuk integrasi layanan bisnis; port transcoder tidak dibuka ke host. Setup dan
nilai runtime dijelaskan di [konfigurasi](../../docs/configuration.md).

## Batas implementasi bisnis

`envoy.yaml` dan filter Lua adalah fondasi transcoding yang masih perlu
diselaraskan dengan [OpenAPI Gateway](../../contracts/openapi/gateway.yaml).
ProtoJSON langsung bukan bentuk respons REST yang dijanjikan: enum, pembungkus
`oneof`, pagination, error `application/problem+json`, serta framing/replay SSE
harus diuji pada service nyata. Aggregator, feed, webhook, dan integrasi
notifikasi belum dinyatakan selesai hanya karena edge dan login sudah tersedia.

Validasi identitas pemanggil dan proyeksi data tetap wajib pada tiap service
downstream. Deadline unary harus terpisah dari koneksi SSE; stream harus
berhenti pada expiry/revocation, termasuk saat idle. Rate limiting, correlation
ID lintas semua service, dan observability lengkap juga masih memerlukan bukti
runtime. Aturan metadata ada di [panduan proto](../../contracts/proto/README.md).
