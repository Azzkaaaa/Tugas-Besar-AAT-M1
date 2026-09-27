# Auth & Identity (Zitadel)

Rencana service identitas, token, dan otorisasi kini menggunakan **Zitadel** sebagai Identity Provider (IdP) terpusat, menggantikan implementasi custom gRPC sebelumnya. Layanan autentikasi ini terekspos pada `http://auth.localhost:8080`.

- **Input:** Klien melakukan autentikasi menggunakan standar OpenID Connect (OIDC) / OAuth 2.0. Gateway memvalidasi access token eksternal (bearer) ini dan menangani token exchange menjadi short-lived asymmetric JWT untuk otorisasi internal.
- **Output:** Access Token (JWT) terverifikasi yang memuat profil principal beserta role dan scope spesifik mereka (misal: `hazards:read:summary`, `hazards:read:full`). Kegagalan verifikasi token di Gateway tidak boleh menghasilkan akses yang diizinkan.
- **Storage internal:** Data identitas pengguna, verifier secret, sesi pengguna, refresh token, dan state pencabutan (revocation) sepenuhnya disimpan dan dikelola oleh database bawaan Zitadel (menggantikan `AUTH_STORE_DSN` dan `AUTH_CACHE_DSN` kustom).
- **Tanggung jawab:** Mengelola otentikasi untuk tiga kelas client independen (MEDIA, FIELD_TEAM, dan BNPB_INTERNAL). Zitadel bertanggung jawab atas penetapan role/scope, rotasi refresh token, manajemen TTL token, dan pencabutan token (revocation). API Gateway membaca token ini untuk mengeksekusi kontrol akses sebelum meneruskan request ke service gRPC internal.

Health check kini menggunakan endpoint standar bawaan Zitadel. TODO: Skrip provisioning awal Zitadel (setup Project, Applications, Policies, User Grants), pemetaan OIDC custom claims, integrasi middleware validasi token di API Gateway, telemetry bebas token rahasia, serta pengujian E2E untuk token expiry, refresh otomatis, dan stream revocation. Detail setup dan konfigurasi secret ada di [konfigurasi](../../docs/configuration.md).
