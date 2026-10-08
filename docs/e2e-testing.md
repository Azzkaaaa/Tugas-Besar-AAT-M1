# Pengujian end-to-end

Suite `tests/test_e2e_*.py` menguji sistem lengkap dari luar, persis seperti klien
API: request melewati Envoy, token exchange Zitadel, api-gateway REST, lalu service
gRPC yang diisi data mock BMKG/PVMBG. Login memakai service user
(`svc-media`, `svc-lapangan`, `svc-bnpb`) dengan client credentials, sehingga
tidak memerlukan browser. Hanya pustaka standar Python yang dipakai.

Suite **tidak** menyalakan atau mematikan Docker. Tanpa `BNPB_E2E=1` semua tes
e2e dilewati, sehingga `python -m unittest discover -s tests` tetap cepat.

## Menjalankan

```sh
python tools/setup_auth_env.py
docker compose --env-file .env --env-file .env.auth up --build -d --wait
BNPB_E2E=1 python -m unittest discover -s tests -p 'test_e2e_*.py' -v
```

PowerShell: `$env:BNPB_E2E = "1"` sebelum perintah `unittest`.

Bootstrap menulis secret service user dan ID project ke `.cache/auth/`; suite
membaca file tersebut. Token, secret, dan body respons tidak dicetak.

| Modul | Cakupan |
| --- | --- |
| `test_e2e_auth.py` | Token tiap role, 401 tanpa/token palsu, 400 cookie + bearer, mutasi bearer tanpa CSRF |
| `test_e2e_hazards.py` | Data dari kedua mock, matriks `view=full` (MEDIA 403), detail/404, filter, paginasi, status sumber |
| `test_e2e_feeds.py` | SSE `events`, `seismic`, `volcanic` menerima `hazard.upsert`; MEDIA `view=full` 403 |
| `test_e2e_webhooks.py` | MEDIA hanya baca; buat/lihat/hapus webhook, ping dan event hazard bertanda tangan HMAC, log delivery |
| `test_e2e_notifications.py` | Daftar/detail notifikasi per role, registrasi dan pencabutan token push |

## Konfigurasi

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `BNPB_E2E` | — | Wajib `1` agar suite berjalan |
| `E2E_HOST` / `E2E_PORT` | `127.0.0.1` / `8080` | Alamat Envoy; host dipilih lewat header `Host` |
| `E2E_APP_HOST` | `localhost:8080` | Host aplikasi (API) |
| `E2E_AUTH_HOST` | `auth.localhost:8080` | Host Zitadel (token endpoint) |
| `E2E_SECRETS_DIR` | `.cache/auth` | Lokasi secret service user dan `project-id` |
| `E2E_TIMEOUT` | `120` | Batas tunggu (detik) data awal, event SSE, dan delivery webhook |
| `E2E_CALLBACK_HOST` | `host.docker.internal` | Host yang dipakai webhook-provider untuk mencapai listener tes |

## Catatan

- Pada stack yang baru dinyalakan, tes menunggu aggregator mengambil data kedua
  mock dan feed menerima event baru; satu putaran memerlukan beberapa menit.
- Listener webhook berjalan di host pada port acak. `compose.yaml` memetakan
  `host.docker.internal` ke gateway Docker untuk webhook-provider. Firewall host
  dapat memblokir koneksi dari bridge Docker; izinkan, atau isi
  `E2E_CALLBACK_HOST` dengan alamat host yang dapat dijangkau container.
- Detail notifikasi dilewati bila belum ada hazard SIAGA/AWAS yang menghasilkan
  notifikasi.
- Webhook yang dibuat tes dihapus kembali, termasuk saat tes gagal.
