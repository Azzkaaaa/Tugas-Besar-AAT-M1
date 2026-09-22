# Auth

Rencana service identitas, token, dan otorisasi, gRPC pada port **9092** melalui
[AuthService](../../contracts/proto/bnpb/auth/v1/auth.proto). Implementasi dan
bahasa belum dipilih.

- **Input:** RPC `Login`, `RefreshToken`, `Logout`, dan `Authorize`; setiap
  pemanggilan memiliki `x-service-token` valid. Authorize juga membawa bearer
  aktor asli. Login/refresh/logout memakai kredensial request masing-masing.
- **Output:** pasangan token atau principal yang terverifikasi dengan scope;
  kegagalan autentikasi/otorisasi tidak boleh menghasilkan akses yang diizinkan.
- **Storage milik sendiri:** `AUTH_STORE_DSN` dan `AUTH_CACHE_DSN`; identitas,
  verifier secret, sesi, versi sesi, refresh token, dan state pencabutan.
- **Tanggung jawab:** tiga identitas client berbeda, role/scope ditetapkan server,
  TTL access token default 60 detik, rotasi refresh token sekali pakai, serta
  pencabutan access token sesi lama segera. Auth membaca signing key dan
  allowlist kredensial semua service yang boleh memanggilnya.

Health memakai `grpc.health.v1.Health` standar. TODO: runtime/Dockerfile,
penyimpanan dan registrasi identitas, penyimpanan verifier secret yang sesuai,
transaksi refresh/logout, validasi online, telemetry tanpa token, serta uji
expiry alami, refresh serentak, role, dan revoke. Detail pembagian secret ada di
[konfigurasi](../../docs/configuration.md).
