# PVMBG Mock

Rencana service REST mandiri pada port **8082**, sesuai
[kontrak PVMBG](../../contracts/openapi/pvmbg.yaml). Implementasi dan bahasa belum
dipilih.

- **Input:** `GET /volcanic-reports?since=...`, `POST /admin/schema-version`,
  `POST /admin/outage`, dan `GET /health`. Jalur data/admin memakai bearer token
  khusus PVMBG; health bersifat publik sesuai kontrak.
- **Output:** `VolcanicReport` mentah. Schema versi 1 tidak memuat
  `confidence_level`; versi 2 menambahkannya tanpa restart.
- **Storage milik sendiri:** `PVMBG_STORE_DSN`; tidak dibagikan dengan BNPB.
- **Tanggung jawab:** seed minimal 20 record, minimal satu event baru per
  10 detik, delay acak yang dapat dikonfigurasi pada baseline 500–3.000 ms,
  dan outage yang dikendalikan operator. Outage menyebabkan data/health 503;
  admin API tetap bisa digunakan untuk pemulihan. Mock tidak mem-push data.

TODO implementasi: runtime, Dockerfile, storage/seed/generator, switch skema dan
outage saat berjalan, autentikasi terpisah, health check, log/telemetry, serta
uji perubahan skema dan outage. Gunakan
[konfigurasi milik PVMBG](../../docs/configuration.md), bukan API key BMKG.
