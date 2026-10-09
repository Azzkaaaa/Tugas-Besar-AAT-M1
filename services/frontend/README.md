# Frontend BNPB

React + TypeScript SPA dengan dashboard sederhana: status BMKG/PVMBG, daftar,
filter, detail kejadian, notifikasi, dan live feed. Tidak menggunakan SSR.
Nginx menyajikan hasil build; logo dan slogan produk telah dihapus.

## Login Zitadel

Dari root repository, dengan Docker Engine aktif:

```powershell
python tools/setup_auth_env.py
docker compose --env-file .env.auth -f compose.auth.yaml up --build -d --wait
```

Buka http://localhost:8080 lalu **Masuk dengan Zitadel**. Form username/password
milik Zitadel. Akun seed: media-demo, lapangan-demo, bnpb-demo; password di .env.auth.
Data dashboard standalone masih fixture lokal berlabel **Data simulasi** karena
backend BNPB belum selesai. Frontend tidak menghubungi mock BMKG/PVMBG langsung.

## Pratinjau tanpa Auth

Gunakan Node.js 22.12+ yang memenuhi engine package:

```powershell
npm --prefix services/frontend ci
npm --prefix services/frontend run dev
```

Buka http://localhost:5173. Identitas pratinjau hanya simulasi tampilan, tanpa
akun atau sesi Zitadel. Alternatif Docker:

```powershell
docker compose -f compose.frontend.yaml up --build -d --wait
```

## Konfigurasi build

| Variabel | Pilihan | Pengaruh |
| --- | --- | --- |
| VITE_APP_MODE | oidc / demo | Identitas server atau simulasi; gateway adalah alias lama untuk oidc |
| VITE_DATA_MODE | gateway / mock | API sebenarnya atau fixture lokal |
| VITE_ENABLE_DEMO | true / false | Izin tambahan identitas demo pada build produksi |
| VITE_API_BASE_URL | /api/v1 | Path API pada origin aplikasi yang sama |
| VITE_FIREBASE_* | konfigurasi web Firebase | API_KEY, PROJECT_ID, MESSAGING_SENDER_ID, APP_ID, VAPID_KEY; satu saja kosong berarti push nonaktif |

Standalone Auth memakai oidc + mock + demo false. Compose utama memakai oidc +
gateway. Identitas demo tidak boleh memakai API Gateway. Semua VITE_* masuk bundle
publik; jangan mengisi password, client secret, atau token. Nilai ditanam saat
build sehingga perubahan memerlukan build ulang.

Image runtime `nginx:*-alpine-otel` memuat `ngx_otel_module`. Tracing dikendalikan
saat runtime (bukan build): `NGINX_OTEL_TRACE` (`off` default, `on` di
`compose.yaml`) dan `NGINX_OTEL_ENDPOINT` (OTLP/gRPC, mis. `otel-lgtm:4317`),
dirender dari `otel.conf.template`. `/health` tidak di-trace.

## Implementasi

| File dalam src/ | Tanggung jawab |
| --- | --- |
| App.tsx | Routing hash, pemulihan sesi, refresh sebelum expiry, logout |
| auth/session.ts | GET session, POST refresh/logout, CSRF, satu refresh untuk request bersamaan |
| pages/LoginPage.tsx | Redirect /auth/login; pilihan pratinjau hanya pada mode demo |
| pages/DashboardPage.tsx | Status sumber, filter, tabel, detail, notifikasi, error |
| api/http.ts | REST/SSE dengan cookie same-origin dan satu percobaan refresh setelah 401 |
| mocks/fixtures.ts, mocks/gateway.ts | 24 kejadian awal, delay 350 ms, event simulasi setiap 15 detik |
| live/merge.ts | Menggabungkan event live feed ke halaman pertama tabel |
| push/client.ts, push/lazy.ts | Izin, token FCM, register/unregister ke Gateway; Firebase dimuat hanya jika push dikonfigurasi |
| push/sw.ts, push/swAuth.ts | Service worker /firebase-messaging-sw.js: ambil isi notifikasi, refresh sesi, klik notifikasi |

JavaScript menerima profil, scope, expiry, dan CSRF token. Access token berada
dalam cookie HttpOnly; refresh token disimpan server. Tidak ada token di
localStorage/sessionStorage. Role pratinjau saja yang disimpan sessionStorage;
build produksi tidak mempercayainya.

Media melihat ringkasan; Tim Lapangan/BNPB dapat melihat detail. Bridge memeriksa
izin request API; service bisnis tetap perlu memeriksa proyeksi dan kepemilikan.
Statistik dan pencarian lokasi berlaku pada halaman yang dimuat. Event baru
menawarkan tombol muat ulang agar daftar yang sedang dibaca tetap stabil.

## Live feed

Event `hazard.upsert` langsung masuk ke halaman pertama tabel (baris baru disorot
sebentar). Event yang lebih lama dari baris yang tampil (`ingested_at`) diabaikan.
Halaman pertama boleh bertambah panjang sampai dimuat ulang agar cursor halaman
berikutnya tetap konsisten. Halaman kedua dan seterusnya tetap memakai tombol
muat ulang. Event Siaga/Awas memuat ulang daftar notifikasi; status sumber dimuat
ulang setelah event dan setiap 60 detik selama tab terlihat, karena feed tidak
membawa notifikasi maupun freshness.

## Notifikasi push

Push hanya aktif pada build `VITE_DATA_MODE=gateway` dengan semua `VITE_FIREBASE_*`
terisi (project Firebase yang sama dengan `FIREBASE_CREDENTIALS_PATH`), di secure
context (HTTPS atau `localhost`; alamat IP LAN tidak bisa). Pengguna mengaktifkan
push lewat tombol di topbar; izin browser hanya diminta dari klik tersebut.

Pesan FCM hanya berisi data (`messageId` = hazard_id = notification_id), jadi
service worker mengambil `GET /api/v1/notifications/{id}?view=summary` dengan cookie.
Jika access token kedaluwarsa, worker memakai `GET /auth/session` (memberi CSRF saat
`refresh_required`) lalu `POST /auth/refresh`, sama seperti halaman. Jika sesi Auth
sudah habis, worker menampilkan notifikasi umum dan melepas langganan push browser;
login berikutnya mendaftarkan token baru. Logout juga melepas token dari Gateway.
Worker dibangun terpisah (`vite.sw.config.ts`, IIFE) dan tidak tersedia di `npm run dev`.

## Pengujian

```powershell
npm --prefix services/frontend test
npm --prefix services/frontend run build
npm --prefix services/frontend run test:e2e
```

Playwright menjalankan preview demo port 5173 dan build OIDC port 4173. Pastikan
kedua port bebas. Windows memakai Microsoft Edge; Linux/macOS perlu Chromium
melalui npm --prefix services/frontend exec -- playwright install chromium.
Laporan/screenshot ada di .cache/frontend-e2e-*.

Tes browser Auth mengintersep respons HTTP untuk menguji kontrak frontend,
bukan login Zitadel sebenarnya. Langkah provider Docker, cookie, akun seed,
expiry 60 detik, dan logout ada di [panduan Auth](../../docs/auth-testing.md).
