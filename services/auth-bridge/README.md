# Auth bridge

Node.js 24 menghubungkan SPA dengan Zitadel menggunakan OpenID Connect
Authorization Code + PKCE. Form login dan database akun ditangani Zitadel.
Bridge tidak menerima password pengguna dan tidak membuat JWT pengganti.

## Alur implementasi

1. `/auth/login` membuat state, nonce, PKCE, dan cookie transaksi sementara,
   lalu mengalihkan browser ke Zitadel.
2. `/auth/callback` memvalidasi transaksi browser dan menukar kode sekali pakai.
   `openid-client` memeriksa signature ID token, issuer, audience, expiry, nonce.
3. Access token diintrospeksi melalui aplikasi API Zitadel terpisah. Respons
   introspeksi membawa klaim `role` dan `scopes` yang ditambahkan target Actions v2
   [`zitadel-claims`](../zitadel-claims/README.md).
4. Browser menerima `bnpb_access` (access token opaque asli) dan `bnpb_session`
   (ID sesi acak). Keduanya HttpOnly, SameSite=Lax, host-only, Path=/, dan Secure
   pada HTTPS. HTTP lokal diaktifkan eksplisit dalam Compose.
5. `/auth/session` memberikan profil, scope, expiry, dan CSRF token. Tidak ada
   access/refresh token dalam JSON atau localStorage/sessionStorage.
6. `/auth/refresh` memakai refresh token di server. Frontend memperbarui sekitar
   lima detik sebelum expiry; tab yang lama tidak aktif juga dapat memulihkan
   access cookie yang kedaluwarsa selama sesi server masih berlaku. Access token
   lama dicabut di Zitadel agar gateway juga menolaknya.
7. `/auth/logout` menghapus sesi, menolak token lama, mencoba revokasi provider,
   dan memberikan URL logout Zitadel untuk dibuka browser.

Pada Login UI V2, pengguna memilih **End Session** pada kartu akun untuk
menyelesaikan logout provider, kemudian kembali ke halaman masuk aplikasi.

POST refresh/logout memerlukan Origin aplikasi yang tepat dan `X-CSRF-Token`
sesi. Untuk mutasi API, gateway Envoy memeriksa Origin dan keberadaan header
`X-CSRF-Token`. CSRF token bukan kredensial API. Logout lokal
tetap berhasil ketika provider tidak dapat dihubungi; pengakhiran SSO provider
pada kondisi itu belum dijamin.

## Penyimpanan dan akun

SQLite `/data/sessions.sqlite` berada di volume `auth_sessions`. Payload sesi
dan transaksi login dienkripsi AES-256-GCM dengan `AUTH_SESSION_KEY`; ID sesi
serta daftar token yang ditolak diindeks dengan SHA-256. Rotasi sesi dan
penolakan token sebelumnya dilakukan dalam satu transaksi.

SQLite ini bukan database akun. Zitadel memakai PostgreSQL volume `auth_postgres`.
Bootstrap membuat `media-demo`, `lapangan-demo`, dan `bnpb-demo` dengan role
masing-masing. Password berasal dari `.env.auth`. Restart/provisioning ulang
mempertahankan password dan client secret. Mengedit password seed di env tidak
otomatis mengubah password akun yang sudah dibuat.

Secret confidential client disimpan pada `auth_client_config/oidc.json`, yang
dibaca bridge dan gateway Envoy (read-only). Signing key Actions berada di volume
terpisah milik `zitadel-claims`. PAT administrator bootstrap tidak dipasang ke bridge/SPA.
Helper `tools/setup_auth_env.py` cukup dijalankan untuk setup awal. File env,
database, PAT, dan volume tidak di-push ke Git.

## Scope

Matriks role → scope dan target Actions v2 Zitadel berada di service terpisah
[`zitadel-claims`](../zitadel-claims/README.md). Bridge hanya memeriksa bahwa
introspeksi membawa `role` yang dikenal dan `scopes` yang tidak kosong; pengguna
tanpa grant tidak mendapat klaim, sehingga login ditolak (`role_required`).

Bridge tidak lagi meneruskan `/api/v1/*`. Gateway Envoy memindahkan cookie
`bnpb_access` ke header bearer dan menukarnya menjadi JWT di Zitadel; lihat
[README Envoy](../envoy/README.md). Service bisnis memeriksa scope dari JWT.
Penyimpanan/koordinasi refresh dirancang untuk satu instance bridge sesuai
Compose tugas. Replikasi memerlukan penyimpanan dan koordinasi bersama.

## File dan pengujian

| File | Tanggung jawab |
| --- | --- |
| `src/config.mjs` | Validasi konfigurasi privat dan URL |
| `src/provider.mjs` | Discovery, PKCE, OIDC, introspeksi, refresh/revokasi, validasi klaim role |
| `src/store.mjs` | SQLite terenkripsi, transaksi, daftar token ditolak |
| `src/server.mjs` | Endpoint sesi, cookie/CSRF |

```powershell
npx nx run auth-bridge:test
```

Tes lokal memakai provider OIDC terkendali bertanda tangan RSA dan library
OIDC sebenarnya. Pengujian Docker dijelaskan di
[panduan testing](../../docs/auth-testing.md); kontrak ada di
[auth.yaml](../../contracts/openapi/auth.yaml).

Referensi implementasi: [endpoint OIDC Zitadel](https://zitadel.com/docs/apis/openidoauth/endpoints)
dan [role pengguna](https://zitadel.com/docs/guides/integrate/retrieve-user-roles).
