# Pengujian frontend dan Auth

Jalankan perintah dari root repository. Login memakai Zitadel dan Auth bridge;
API melewati gateway Envoy (cookie → bearer → token exchange → gRPC).
Data dashboard standalone masih fixture lokal karena service BNPB belum lengkap.

## Status verifikasi

- **Gateway tunggal sudah diuji pada stack Docker/Zitadel nyata: 48 pemeriksaan
  `tools/test_auth_browser.mjs` lulus** untuk tiga akun melalui hosted UI: role dari
  klaim Actions v2, cookie HttpOnly, CSRF bridge dan gateway, token exchange
  (API terautentikasi → 503 tanpa backend), refresh otomatis, penolakan token lama,
  expiry alami 60 detik, restart bridge, dan logout. Token exchange manual dengan
  client gateway menghasilkan JWT berisi klaim `role`, `scopes`, dan audiens project.
  `GET /api/v1/hazards?view=full` saat ini 400 dari transcoder karena enum proto
  (`VIEW_FULL`) belum dipetakan ke nilai REST `full`; ini pekerjaan kontrak backend.
- 12 tes bridge lokal lulus: signature/nonce/state/PKCE, audience/organisasi/role,
  cookie, CSRF, refresh bersamaan, expiry alami, token lama, logout, SQLite.
- 7 tes Rust `zitadel-claims` lulus: signature Actions v2 (tampering, header rusak,
  toleransi waktu, beberapa `v1`), pemetaan dan prioritas klaim, endpoint 401/400.
- 18 unit test dan 14 skenario browser frontend lulus. Tes browser Auth memakai
  respons yang diintersep; build produksi juga berhasil.
- 3 tes helper env dan 6 tes bootstrap lulus dengan provider terkendali.
- Konfigurasi Compose standalone lulus pemeriksaan `config --quiet`.
- Arsitektur sebelumnya (API melalui bridge) lulus 45 pemeriksaan. Login/logout
  tiga akun melalui hosted UI, role, cookie HttpOnly, CSRF, refresh otomatis,
  penolakan token lama, expiry alami 60 detik, pemulihan tanpa password, dan
  persistensi sesi setelah restart bridge berhasil. Bootstrap ulang juga berhasil
  dengan mempertahankan akun dan secret. Hasil tersimpan di
  `.cache/auth-browser-test-results.json`.
- API bisnis BNPB belum tersambung. Respons 503 untuk akses yang diizinkan adalah
  hasil yang diharapkan pada stack Auth standalone, bukan bukti integrasi data.

## Menjalankan stack

Nyalakan Docker Desktop dan tunggu Engine siap. Lalu:

```powershell
python tools/setup_auth_env.py
docker compose --env-file .env.auth -f compose.auth.yaml config --quiet
docker compose --env-file .env.auth -f compose.auth.yaml up --build -d --wait
docker compose --env-file .env.auth -f compose.auth.yaml ps -a
```

Bootstrap seharusnya `Exited (0)`; service lain berjalan dan service yang memiliki
healthcheck menjadi healthy. Unduhan image pertama dapat memakan waktu.
Jika gagal, periksa log tanpa membagikan env atau token:

```powershell
docker compose --env-file .env.auth -f compose.auth.yaml logs --tail 80 auth-bootstrap auth-bridge
```

Buka **http://localhost:8080** secara konsisten. Jangan mengganti ke 127.0.0.1:
cookie, Origin, dan callback dikonfigurasi untuk localhost. Provider berada di
`http://auth.localhost:8080`; host ini harus mengarah ke loopback.

## Login tiga akun

Klik **Masuk dengan Zitadel**. Form username/password harus tampil pada host
`auth.localhost:8080`, lalu kembali ke dashboard setelah login.

| Username | Password pada `.env.auth` | Hasil dashboard |
| --- | --- | --- |
| `media-demo` | `AUTH_MEDIA_PASSWORD` | Ringkasan publik, tanpa koordinat/raw |
| `lapangan-demo` | `AUTH_FIELD_TEAM_PASSWORD` | Detail lengkap |
| `bnpb-demo` | `AUTH_BNPB_PASSWORD` | Detail lengkap |

Administrator `admin@bnpb.local` memakai `AUTH_ADMIN_PASSWORD` dan terpisah dari
akun aplikasi. Jangan mem-push `.env.auth` atau menyalin password ke laporan.
Gunakan logout atau browser profile terpisah untuk berganti akun.

Dashboard menampilkan **Data simulasi**. BMKG/PVMBG tidak perlu dinyalakan untuk
tes ini. Data simulasi tidak berarti login ikut disimulasikan.

## Cookie, otorisasi, dan CSRF

Di DevTools → Application → Cookies → localhost, periksa `bnpb_access` dan
`bnpb_session`, atribut HttpOnly dan SameSite=Lax. Secure dimatikan hanya untuk
HTTP lokal. Hindari menyalin/screenshot nilai cookie. JavaScript tidak bisa
membaca kedua cookie tersebut.

Network `GET /auth/session` harus 200 dengan `user`, `csrf_token`, `expires_at`.
Access/refresh token tidak muncul dalam respons atau localStorage.

Dari Console aplikasi, request berikut hanya menghasilkan status:

```javascript
await fetch('/api/v1/hazards?view=full').then(r => r.status)
await fetch('/auth/refresh', {method: 'POST'}).then(r => r.status)
```

Request pertama: semua role **503** pada standalone Auth karena gateway menerima
token hasil exchange tetapi service bisnis tidak ada. Anonim **401**. Pemeriksaan
scope (Media meminta `view=full` → 403) dilakukan service setelah integrasi
backend. Request kedua harus **403** karena tanpa CSRF. Mutasi API tanpa header
`X-CSRF-Token` juga ditolak gateway dengan **403**.

## Refresh dan expiry alami

Default `ZITADEL_ACCESS_TOKEN_TTL_SECONDS=60`. Saat dashboard terbuka, tunggu
sekitar 55 detik: Network harus menunjukkan POST refresh 200 dan expiry baru,
tanpa password. Reload halaman harus mempertahankan identitas.

Untuk membuktikan expiry alami, buka tab `http://localhost:8080/health` pada
browser yang sama lalu tutup seluruh tab dashboard agar timer React berhenti.
Jalankan berikut di Console tab health; tunggu 65 detik tanpa mengganti waktu:

```javascript
const before = await fetch('/auth/session').then(r => r.json());
await new Promise(resolve => setTimeout(resolve, 65000));
const expiredResponse = await fetch('/auth/session');
const expired = await expiredResponse.json();
console.assert(expiredResponse.status === 401 && expired.refresh_required === true);
const refreshed = await fetch('/auth/refresh', {
  method: 'POST', headers: {'X-CSRF-Token': expired.csrf_token}
});
console.assert(refreshed.status === 200);
const after = await refreshed.json();
console.assert(before.user.id === after.user.id);
```

Access cookie kedaluwarsa tetapi sesi/refresh token server masih dapat memulihkan
login. Setelah refresh, bridge mencabut access token lama di Zitadel, sehingga
token exchange gateway menolaknya (401).

## Logout dan persistensi

Klik **Keluar**. POST logout membawa CSRF, cookie aplikasi terhapus, dan browser
mengikuti logout Zitadel. Di halaman logout Zitadel V2, pilih kartu akun dengan
tulisan **End Session** untuk mengakhiri sesi provider. Browser kembali ke halaman
masuk aplikasi; login berikutnya harus kembali meminta kredensial.
Membuka `/#/dashboard` kembali harus meminta login.
Pengakhiran SSO bergantung pada provider dapat dihubungi.

Sebelum logout, periksa persistensi dengan:

```powershell
docker compose --env-file .env.auth -f compose.auth.yaml restart auth-bridge
```

Reload halaman: sesi tetap tersedia; access token yang expired dipulihkan.
Akun berada di volume PostgreSQL `auth_postgres`, SQLite sesi di `auth_sessions`.
Volume dibuat pada komputer masing-masing saat startup pertama, bukan dari Git.
Helper setup tidak mengganti secret/password yang sudah diisi.

Hentikan tanpa menghapus data:

```powershell
docker compose --env-file .env.auth -f compose.auth.yaml down
```

Jangan memakai `-v` jika ingin mempertahankan data. Jangan menghapus hanya volume
konfigurasi client: secret client yang sudah dibuat tidak dapat diambil ulang;
bootstrap sengaja tidak merotasinya otomatis.

## Tes browser terhadap Zitadel Docker

Setelah stack healthy, jalankan tes penerimaan tanpa respons HTTP tiruan:

```powershell
npm --prefix services/frontend ci
node tools/test_auth_browser.mjs
```

Script membaca password seed dari `.env.auth`, memakai Microsoft Edge headless
pada Windows, lalu login/logout untuk tiga akun. Tes juga memeriksa cookie,
role, CSRF, refresh otomatis, penolakan bearer lama, expiry alami 60 detik,
dan persistensi SQLite dengan me-restart hanya container `auth-bridge`.
Jangan menjalankan demo interaktif bersamaan dengan tes restart ini.

Proses memerlukan sekitar 2–3 menit karena menunggu umur token secara nyata.
Laporan ada di `.cache/auth-browser-test-results.json`; password, cookie, token,
screenshot login, dan trace tidak disimpan. Script ini ditujukan untuk Compose
Auth standalone dengan backend kosong: request API terautentikasi mendapat 503.

## Tes otomatis tanpa Docker

Gunakan Node.js 24 untuk bridge, serta Python untuk helper:

```powershell
npx nx run auth-bridge:test
npx nx run envoy:validate
python -m unittest discover -s tools -p test_setup_auth_env.py -v
node --test services/zitadel/bootstrap.test.mjs
npm --prefix services/frontend ci
npm --prefix services/frontend test
npm --prefix services/frontend run test:e2e
```

Bridge diuji dengan HTTP provider terkendali bertanda tangan RSA dan library
OIDC sebenarnya. Browser frontend memakai respons sesi yang diintersep. Tes
tersebut tidak menggantikan pengujian form login dan konfigurasi Docker nyata.

Compose utama mewarisi Auth dan gateway Envoy yang sama.
Setelah service BNPB selesai, gunakan kedua env: `--env-file .env --env-file .env.auth`.
Saat ini compose utama masih memuat service BNPB placeholder; gunakan
`compose.auth.yaml` untuk pekerjaan Auth. Lihat [implementasi bridge](../services/auth-bridge/README.md)
dan [kontrak endpoint](../contracts/openapi/auth.yaml).
