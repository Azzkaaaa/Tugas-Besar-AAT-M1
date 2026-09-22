# BMKG Mock

Rencana service REST mandiri pada port **8081**, sesuai
[kontrak BMKG](../../contracts/openapi/bmkg.yaml). Implementasi dan bahasa belum
dipilih.

- **Input:** polling `GET /seismic-events` dan `GET /tsunami-warnings` dengan
  `since`, `X-BMKG-Key`, correlation ID, dan trace context. Health: `GET /health`.
- **Output:** entitas `SeismicEvent` dan `TsunamiWarning` mentah sesuai PDF;
  warning hanya terkait gempa berpotensi tsunami. Tidak mem-push data ke BNPB.
- **Storage milik sendiri:** `BMKG_STORE_DSN`; hanya BMKG Mock mengakses langsung.
- **Tanggung jawab:** validasi API key khusus BMKG, seed minimal 20 record,
  minimal satu event baru per 10 detik, delay tetap 50–150 ms, dan filter waktu
  sesuai kontrak. Tolak kredensial PVMBG/BNPB.

TODO implementasi: runtime, manifest, Dockerfile, storage/seed/generator, handler
HTTP, health check, log terstruktur, instrumentasi latensi, serta uji kontrak dan
kredensial silang. Konfigurasi terkait ada pada
[inventaris konfigurasi](../../docs/configuration.md); jangan membaca secret
service lain.
