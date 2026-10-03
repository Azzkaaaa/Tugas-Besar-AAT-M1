# Frontend BNPB

Frontend React + TypeScript menyediakan halaman masuk dan dashboard pemantauan
bencana. Saat Auth dan service BNPB masih dikerjakan, gunakan mode demo untuk
mengembangkan tampilan berdasarkan [kontrak Gateway](../../contracts/openapi/gateway.yaml).
Data demo adalah fixture respons kanonik Gateway; frontend tidak menghubungi
BMKG/PVMBG langsung dan tidak membaca SQLite kedua mock.

## Menjalankan lokal

Gunakan Node.js 22.12+ (atau versi LTS lebih baru yang memenuhi engine package),
lalu jalankan dari root repository:

```powershell
npm --prefix services/frontend ci
npm --prefix services/frontend run dev
```

Buka `http://localhost:5173`. Server development menyediakan mode demo secara
default. Tidak perlu menjalankan Auth, Gateway, mock BMKG/PVMBG, maupun
`tools/setup_mock_env.py` untuk mencoba halaman ini.

Jika ingin mengubah konfigurasi lokal:

```powershell
Copy-Item services/frontend/.env.example services/frontend/.env.local
```

Perubahan environment memerlukan restart server development. Semua variabel
`VITE_*` masuk ke bundle yang dapat dibaca browser. Jangan isi dengan password,
client secret, `BMKG_API_KEY`, atau `PVMBG_BEARER_TOKEN`.

## Preview Docker

Dengan Docker Engine berjalan, dari root repository:

```powershell
docker compose -f compose.frontend.yaml up --build -d --wait
docker compose -f compose.frontend.yaml ps
```

Buka `http://localhost:5173`. Compose khusus frontend secara eksplisit membangun
mode demo dengan `VITE_APP_MODE=demo` dan `VITE_ENABLE_DEMO=true`. Port host dapat
diubah melalui `FRONTEND_HTTP_PORT`. Hentikan preview dengan:

```powershell
docker compose -f compose.frontend.yaml down
```

Container memakai Nginx untuk menyajikan SPA. URL halaman dapat dibuka ulang
langsung; `/health` melaporkan kesehatan server statis. Preview tidak menyediakan
API Gateway: request `/api/` mengembalikan 503 agar kegagalan integrasi tidak
tersamarkan sebagai HTML halaman.

## Alur mencoba halaman

1. Buka halaman masuk. Tombol **Masuk dengan organisasi** menjelaskan bahwa koneksi Auth masih
   menunggu konfigurasi, sehingga belum membuat sesi autentikasi sungguhan.
2. Gunakan akses demo untuk memilih identitas Media, Tim Lapangan, atau BNPB
   Pusat. Identitas ini hanya simulasi tampilan; tidak ada akun/password yang
   dibuat di SQLite atau Zitadel.
3. Di dashboard, coba pencarian, filter, pagination, dan detail kejadian.
   Media menerima proyeksi ringkasan; Tim Lapangan dan BNPB Pusat dapat melihat
   detail tambahan. Pembatasan ini tetap harus ditegakkan backend saat integrasi.
4. Coba skenario normal, sumber terlambat diperbarui, data kosong, dan gagal
   memuat untuk memeriksa kondisi tampilan.
5. Periksa status sumber dan notifikasi. Data demo tidak harus sama dengan
   record generator BMKG/PVMBG; kedua jalur tersebut belum terhubung.
6. Keluar untuk kembali ke halaman masuk, lalu coba identitas lainnya.

Dashboard bersifat read-only. Angka yang dihitung dari halaman data tidak boleh
ditafsirkan sebagai statistik keseluruhan backend. Koneksi dan event dalam mode
demo merupakan simulasi, bukan bukti bahwa layanan BNPB tersedia.

## Build dan pengujian

Jalankan dari root repository:

```powershell
npm --prefix services/frontend run build
npm --prefix services/frontend test
npm --prefix services/frontend run test:e2e
```

Pengujian browser memakai Microsoft Edge yang terpasang pada Windows. Pada
Linux/macOS, pasang Chromium terlebih dahulu dengan
`npm --prefix services/frontend exec -- playwright install chromium`.
Script menjalankan server demo pada port 5173 serta build produksi dengan demo
nonaktif pada port 4173. Pastikan port 4173 bebas; server development tidak perlu
dinyalakan manual. Pengujian mencakup tiga peran, filter, pagination, notifikasi,
live feed, logout, skenario kegagalan, tampilan ponsel, dan penolakan identitas
demo yang disisipkan ke sessionStorage pada build produksi.
Screenshot dan laporan browser tersimpan di `.cache/frontend-e2e-results` dan
`.cache/frontend-e2e-report` pada root repository.
Hasil build berada di `services/frontend/dist` dan tidak perlu di-commit.

Verifikasi implementasi pada 3 Oktober 2026: 12 unit test dan 10 skenario browser
lulus. Menu ponsel juga diuji ulang setelah perapian responsif. Build produksi
dan image Docker berhasil; container lulus health check, SPA fallback, login
mock pada hasil build, serta proyeksi Media. Endpoint `/api/` pada preview
mengembalikan 503 JSON sesuai konfigurasi. Hasil ini belum menguji Auth atau
service BNPB yang sebenarnya.

Build produksi secara default menggunakan mode Gateway. Mode demo dalam build
produksi memerlukan dua pengaturan sekaligus:

```powershell
$env:VITE_APP_MODE = 'demo'
$env:VITE_ENABLE_DEMO = 'true'
npm --prefix services/frontend run build
npm --prefix services/frontend run preview
```

Buka `http://localhost:4173`. Setelah berhenti preview, hapus override lokal
agar perintah berikutnya kembali mengikuti konfigurasi:

```powershell
Remove-Item Env:VITE_APP_MODE -ErrorAction SilentlyContinue
Remove-Item Env:VITE_ENABLE_DEMO -ErrorAction SilentlyContinue
```

## Pembagian implementasi

| Bagian | File | Peran |
| --- | --- | --- |
| SPA dan sesi pratinjau | `src/App.tsx` | Routing hash, identitas demo pada tab yang sama, logout |
| Halaman login | `src/pages/LoginPage.tsx`, `login.css` | Tombol organisasi dan tiga identitas mock |
| Dashboard | `src/pages/DashboardPage.tsx`, `src/styles.css` | Status sumber, daftar, filter, detail, notifikasi, dan kondisi pemuatan/error |
| Identitas mock | `src/auth/demo.ts` | Media, Tim Lapangan, BNPB Pusat; tanpa seeding SQLite |
| Data contoh | `src/mocks/fixtures.ts`, `gateway.ts` | 24 kejadian awal, delay 350 ms, event setiap 15 detik, pagination dan proyeksi sesuai peran |
| Adapter Gateway | `src/api/http.ts` | REST dan SSE dengan penyedia access token yang nantinya dihubungkan ke Auth |
| Konfigurasi | `src/config.ts` | Mode demo/produksi dan URL publik API |

SPA dirender di browser, tanpa SSR. Nginx hanya menyajikan berkas hasil build.
Data contoh frontend terpisah dari SQLite BMKG/PVMBG. Identitas pratinjau bukan
pengguna yang disimpan pada Auth; provisioning user nanti dilakukan oleh layanan
identitas yang disepakati tim.

Angka ringkasan hanya menghitung **halaman yang dimuat**, bukan seluruh kejadian.
Pencarian lokasi juga berlaku pada halaman tersebut; filter sumber, tingkat,
dan waktu diproses adapter. Notifikasi terpisah dari filter tabel dan menampilkan
hingga 100 notifikasi terbaru. Event baru menawarkan tombol muat ulang agar
halaman yang sedang dibaca tetap stabil.

## Penyambungan Auth dan Gateway berikutnya

`VITE_API_BASE_URL` menentukan alamat publik REST Gateway (default `/api/v1`).
Lapisan API dan identitas dipisahkan dari komponen tampilan agar fixture dan
identitas demo dapat diganti saat backend tersedia.

Integrasi berikutnya membutuhkan issuer Zitadel, public client ID SPA, callback
dan post-logout URL, scope/role, serta alur Authorization Code + PKCE yang
disepakati dengan penanggung jawab Auth. Masuk, callback, sesi, refresh token,
logout OIDC, dan proteksi API sungguhan belum selesai. Tidak ada JWT atau token
login palsu yang dibuat untuk meniru autentikasi produksi.

Untuk deployment terintegrasi, gunakan `VITE_APP_MODE=gateway` dan
`VITE_ENABLE_DEMO=false` saat build. Tanpa integrasi Auth, akses dashboard tidak
dibuka sebagai pengguna produksi. Nilai Vite ditanam saat build, sehingga
menambahkan `environment:` ke container Nginx yang sudah dibangun tidak mengubah
URL API maupun mode aplikasi.

Konfigurasi Envoy saat ini membedakan host SPA `localhost:8080` dan host API
`api.localhost:8080`. Sebelum integrasi, sediakan routing API pada origin yang
sama atau gunakan base URL API yang sesuai serta konfigurasi CORS backend.
Setelah Auth siap, hubungkan token pengguna ke Gateway dan sambungkan live feed
SSE sesuai kontrak, termasuk penanganan koneksi putus dan pemuatan ulang snapshot.
Validasi keamanan, refresh/expiry token, serta data nyata harus diuji kembali
pada sistem yang sudah terhubung.
