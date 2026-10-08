# Auth lokal dengan Zitadel

Stack `compose.auth.yaml` menyediakan PostgreSQL, Zitadel, halaman login resmi
Zitadel Login V2, provisioning awal, Auth bridge, SPA, dan Envoy. Halaman SPA tidak
menerima password; pengguna diarahkan ke halaman login Zitadel.

Jalankan dari root repository setelah Docker Desktop aktif:

```powershell
python tools/setup_auth_env.py
docker compose --env-file .env.auth -f compose.auth.yaml up -d --build
docker compose --env-file .env.auth -f compose.auth.yaml ps -a
```

Buka **http://localhost:8080**. Issuer dan halaman login ada di
**http://auth.localhost:8080**. Gunakan URL ini persis agar issuer dan callback
sesuai; port 8080 tidak boleh sedang dipakai stack Compose lain. Browser modern
memetakan `*.localhost` ke loopback, sedangkan jaringan Docker memakai alias Envoy.

| Akun Zitadel | Role project | Password awal di `.env.auth` |
| --- | --- | --- |
| `media-demo` | `MEDIA` | `AUTH_MEDIA_PASSWORD` |
| `lapangan-demo` | `FIELD_TEAM` | `AUTH_FIELD_TEAM_PASSWORD` |
| `bnpb-demo` | `BNPB_INTERNAL` | `AUTH_BNPB_PASSWORD` |
| `admin@bnpb.local` | Administrator instance, bukan pengguna dashboard | `AUTH_ADMIN_PASSWORD` |
| `svc-media` | `MEDIA` | Service user, secret di `.cache/auth/svc-media.secret` |
| `svc-lapangan` | `FIELD_TEAM` | Service user, secret di `.cache/auth/svc-lapangan.secret` |
| `svc-bnpb` | `BNPB_INTERNAL` | Service user, secret di `.cache/auth/svc-bnpb.secret` |

Keempat akun adalah pengguna sungguhan di database Zitadel. Nama `demo` hanya
menandai akun untuk tugas lokal. Password dihasilkan acak sekali per komputer.
Console administrasi ada di `http://auth.localhost:8080/ui/console`.

## Provisioning dan konfigurasi

`tools/setup_auth_env.py` membuat `.env.auth` dari `.env.auth.example`. Script
mengisi secret kosong atau placeholder, serta mempertahankan nilai yang sudah
ada. File `.env.auth` bersifat lokal dan tidak di-push. Script tidak mereset
password pengguna yang sudah dibuat dalam Zitadel; perubahan password dilakukan
melalui Zitadel.

Container sekali-jalan `auth-bootstrap` menjalankan `bootstrap.mjs` setelah
Zitadel siap. Script memanggil Zitadel langsung (`ZITADEL_API_URL=http://zitadel:8080`
dengan header `Host: auth.localhost:8080`) karena gateway Envoy baru mulai setelah
provisioning selesai. Script membuat:

- project `BNPB M1`, tiga role, dan grant pengguna;
- tiga service user (machine user) `svc-media`, `svc-lapangan`, `svc-bnpb` untuk
  klien non-browser (client credentials), masing-masing dengan grant satu role.
  Client ID sama dengan username. Secret yang dihasilkan Zitadel dan ID project
  ditulis sebagai nilai mentah, satu per file, ke `.cache/auth/` (bind mount,
  di-gitignore), bukan ke `oidc.json` yang dibaca bridge, gateway, dan feed;
- application OIDC confidential untuk Auth bridge: authorization code, PKCE oleh
  bridge, client-secret Basic, access token opaque, dan refresh token. Callback yang
  terdaftar adalah `http://localhost:8080/auth/callback`;
- application API untuk introspection;
- application OIDC `BNPB gateway token exchange` dengan grant type token exchange
  dan access token JWT, dipakai filter token exchange Envoy. Aplikasi ini berada di
  project yang sama karena Zitadel hanya menukar token yang audiensnya memuat
  project tersebut;
- application OIDC `BNPB event feed token exchange` dengan grant yang sama. Event Live
  Feed memakainya untuk menukar ulang JWT aktor sebelum habis sehingga stream tetap
  terbuka (lihat [Event Live Feed](../event-live-feed/README.md)). Client ID/secret
  disimpan sebagai `feed_client_id`/`feed_client_secret` di `oidc.json` dan client ini
  ikut difilter `zitadel-claims` agar JWT hasil exchange tetap membawa `role`/`scopes`;
- feature instance `oidcTokenExchange` (masih beta di Zitadel v4);
- target Actions v2 `BNPB scope claims` ke `http://zitadel-claims:3002/claims`
  beserta execution untuk function `preuserinfo` dan `preaccesstoken`. Target ini
  menambahkan klaim `role` dan `scopes`; lihat [zitadel-claims](../zitadel-claims/README.md).

Signing key target dan secret client gateway hanya diberikan Zitadel sekali saat
dibuat, sehingga disimpan (secret client di `oidc.json`, signing key di
`actions.json`) dan tidak dirotasi otomatis. Volume lama yang masih menyimpan
signing key di `oidc.json` dipindahkan otomatis ke `actions.json` saat provisioning.

Zitadel v4 menolak memanggil alamat privat secara default. `compose.auth.yaml`
menghapus `172.16.0.0/12` dari `ZITADEL_HTTPCLIENT_DENYLIST` agar target Actions di
jaringan Docker dapat dipanggil. Ini hanya untuk stack lokal dan memperluas
paparan SSRF ke jaringan Docker. Bila Docker menempatkan `bnpb_net` di rentang lain
(misalnya `192.168.0.0/16`), sesuaikan daftar tersebut.

`ZITADEL_ACCESS_TOKEN_TTL_SECONDS` mengatur masa berlaku access token dan ID token,
dengan default **60 detik** sesuai M1. `AUTH_SESSION_TTL_SECONDS` membatasi refresh
token dan sesi bridge, default **86400 detik / 24 jam**. Provisioner mengatur
keempat durasi OIDC melalui Admin API, termasuk pada database yang sudah ada.
Setelah mengubah TTL, jalankan ulang provisioning lalu buat sesi login baru:

```powershell
docker compose --env-file .env.auth -f compose.auth.yaml run --rm auth-bootstrap
docker compose --env-file .env.auth -f compose.auth.yaml up -d auth-bridge
```

Provisioning yang diulang tidak menggandakan project, application, user, atau
grant. Password dan client secret tidak diputar otomatis. Jika application sudah
ada tetapi file client secret hilang, provisioning berhenti dengan pesan untuk
memulihkan volume konfigurasi. Hal yang sama berlaku untuk service user: jika
Zitadel sudah memiliki secret tetapi file `.cache/auth/<user>.secret` hilang,
provisioning berhenti; pulihkan file atau buat secret baru lewat Console. Jika grant akun seed sudah diubah, script meminta
pemeriksaan manual; script tidak mengembalikan izin pengguna secara diam-diam.

## Penyimpanan

| Named volume | Isi |
| --- | --- |
| `auth_postgres` | Database identitas, kebijakan, dan sesi Zitadel |
| `zitadel_admin_bootstrap` | PAT administrator untuk provisioning; tidak dipasang pada SPA/bridge |
| `zitadel_login_bootstrap` | PAT khusus Login UI dengan role `IAM_LOGIN_CLIENT` |
| `auth_client_config` | `oidc.json`, client ID/secret dan identitas project/organisasi |
| `auth_actions_config` | `actions.json`, signing key target Actions dan ID yang difilter `zitadel-claims` |
| `auth_sessions` | SQLite `/data/sessions.sqlite` milik Auth bridge |

`oidc.json` dibaca bridge melalui mount read-only. Refresh token disimpan oleh
bridge, bukan di penyimpanan JavaScript browser. Volume dan file SQLite tidak
perlu dibuat manual atau disimpan di Git. Komputer rekan membuat database dan
akun lokalnya sendiri ketika menjalankan stack pertama kali.

`docker compose ... down` mempertahankan volume. Opsi `down -v` menghapus data
Auth, client secret, dan sesi; jangan gunakan untuk sekadar menghentikan demo.
Cadangan `.env.auth` dan seluruh volume Auth harus sesuai. Jangan mengganti
master key atau password database pada instalasi yang sudah berisi data.

## Hubungan dengan backend BNPB

`FRONTEND_DATA_MODE=mock` berarti login OIDC sungguhan dengan kejadian fixture di
browser. BMKG/PVMBG tetap diakses melalui Aggregator dan Gateway ketika backend
tersedia. Untuk integrasi, jalankan `compose.yaml`, set `FRONTEND_DATA_MODE=gateway`,
kemudian build ulang SPA. Service harus memvalidasi JWT hasil token exchange dan
menjaga otorisasi data Media berdasarkan klaim `scopes`.

Envoy meneruskan `/auth/*` ke bridge dan memproses `/api/*` sendiri: cookie menjadi
bearer, ditukar menjadi JWT di Zitadel, lalu ditranskode ke gRPC. Pada stack
standalone service bisnis tidak ada, sehingga endpoint API mengembalikan 503 untuk
pengguna terautentikasi. Sumber BMKG/PVMBG tidak diekspos lewat cookie browser.

HTTP dan cookie tanpa atribut `Secure` hanya untuk loopback lokal pada stack ini.
Server di luar komputer lokal membutuhkan HTTPS, issuer/callback yang sesuai,
cookie `Secure`, dan kebijakan login yang disesuaikan. Port PostgreSQL, Zitadel
internal, dan bridge tidak dipublikasikan ke host.

## Pemeriksaan

```powershell
python -m unittest discover -s tools -p test_setup_auth_env.py -v
node --check services/zitadel/bootstrap.mjs
node --test services/zitadel/bootstrap.test.mjs
docker compose --env-file .env.auth -f compose.auth.yaml config --quiet
docker compose --env-file .env.auth -f compose.auth.yaml logs auth-bootstrap
```

Lihat [panduan Auth](../../docs/auth-testing.md) untuk alur login, cookie, refresh,
logout, dan pemeriksaan hak akses. Pengujian bootstrap lokal memakai provider
HTTP terkendali untuk memeriksa idempotensi; pengujian itu tidak menggantikan
pengujian stack Zitadel sebenarnya. Pengujian browser langsung terhadap Docker
telah lulus 45 pemeriksaan melalui `node tools/test_auth_browser.mjs`, termasuk
expiry alami 60 detik dan restart bridge. Saat logout, selesaikan konfirmasi
**End Session** pada halaman resmi Zitadel.

Konfigurasi mengikuti [Compose resmi versi v4.16.0](https://github.com/zitadel/zitadel/blob/v4.16.0/deploy/compose/docker-compose.yml)
dan [Admin API pengaturan OIDC](https://zitadel.com/docs/reference/api/admin/zitadel.admin.v1.AdminService.UpdateOIDCSettings).
