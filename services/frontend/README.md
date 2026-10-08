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

Standalone Auth memakai oidc + mock + demo false. Compose utama memakai oidc +
gateway. Identitas demo tidak boleh memakai API Gateway. Semua VITE_* masuk bundle
publik; jangan mengisi password, client secret, atau token. Nilai ditanam saat
build sehingga perubahan memerlukan build ulang.

## Implementasi

| File dalam src/ | Tanggung jawab |
| --- | --- |
| App.tsx | Routing hash, pemulihan sesi, refresh sebelum expiry, logout |
| auth/session.ts | GET session, POST refresh/logout, CSRF, satu refresh untuk request bersamaan |
| pages/LoginPage.tsx | Redirect /auth/login; pilihan pratinjau hanya pada mode demo |
| pages/DashboardPage.tsx | Status sumber, filter, tabel, detail, notifikasi, error |
| api/http.ts | REST/SSE dengan cookie same-origin dan satu percobaan refresh setelah 401 |
| mocks/fixtures.ts, mocks/gateway.ts | 24 kejadian awal, delay 350 ms, event simulasi setiap 15 detik |

JavaScript menerima profil, scope, expiry, dan CSRF token. Access token berada
dalam cookie HttpOnly; refresh token disimpan server. Tidak ada token di
localStorage/sessionStorage. Role pratinjau saja yang disimpan sessionStorage;
build produksi tidak mempercayainya.

Media melihat ringkasan; Tim Lapangan/BNPB dapat melihat detail. Bridge memeriksa
izin request API; service bisnis tetap perlu memeriksa proyeksi dan kepemilikan.
Statistik dan pencarian lokasi berlaku pada halaman yang dimuat. Event baru
menawarkan tombol muat ulang agar daftar yang sedang dibaca tetap stabil.

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
