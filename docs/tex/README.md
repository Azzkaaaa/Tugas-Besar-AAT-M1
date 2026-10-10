# Laporan Milestone 1

Kelompok **nasigoreng**. Sistem **ARLOW: Agregasi Real-time Layanan Operasi Waspada**.

Revisi 10 Oktober 2026 mengikuti spesifikasi tugas dan snapshot repository `4a1973b`, menggunakan evidence yang sudah tersedia. Tidak ada pengujian ulang saat penyusunan revisi.

**PDF utama: [IF4031_M1_nasigoreng.pdf](IF4031_M1_nasigoreng.pdf).** `main.pdf` adalah hasil build editor terpisah.

## Isi

- `metadata.tex`: identitas, tanggal, dan kontribusi yang tetap diisi manual oleh anggota.
- `contents/01-sistem.tex`: arsitektur, teknologi, asumsi, dan cara menjalankan.
- `contents/02-p1.tex` sampai `06-p5.tex`: rancangan, alternatif, batasan, hasil aktual, dan screenshot demo.
- `contents/07-bukti.tex`: arsip sumber lampiran lama; tidak dimasukkan ke PDF. Bukti dan pembahasannya tetap tersedia pada bab P1-P5 serta direktori `evidence/`.
- `figures/`: diagram TikZ; Notification kini mencakup SQLite dan push outbox.
- `evidence/manifest.json`: hash semua artefak dan daftar gambar yang dimuat di PDF.

## Bukti yang digunakan

| Rujukan | Berkas di dalam `evidence/` | Cakupan |
|---|---|---|
| Hasil tes mock 30 September | `mock-container-2026-09-30.json` | Perilaku mock dan persistensi database |
| Hasil tes browser Auth 6 Oktober | `auth-browser-2026-10-06.json` | Login, cookie, refresh, dan logout pada arsitektur terdahulu |
| Unit test Aggregator 9 Oktober | `aggregator-unit-2026-10-09.jsonl` | Ingest, pemetaan, proyeksi, dan validasi autentikasi |
| Ringkasan pengujian otomatis | `problem_set_report.md` | Scorecard 19 tes; 0 error/failure dan 0 skipped |
| Screenshot demo tiap problem | Direktori `P1/` sampai `P5/` | Observasi demo pada bab terkait |
| Hasil k6 beban | `P2/aggregator-2026-10-09T15-31-17-502Z.json` | Beban 50 VU selama 60 detik |
| Hasil k6 isolasi | `P2/aggregator-isolation-2026-10-09T15-37-25-357Z.json` | Latensi BMKG ketika ada kontensi PVMBG |

Hasil k6 beban mencatat 50 VU selama fase beban 60 detik, 17.546 request, throughput 292,43 request/detik, dan p95 243,96 ms. Hasil k6 isolasi mencatat p95 BMKG pada kontensi 87,09 ms. Throughput menggunakan durasi fase pengukuran, bukan durasi keseluruhan run termasuk pemanasan.

Interpretasi mengikuti cakupan bukti. Penerbitan token service melalui client credentials dibedakan dari refresh sesi browser. Kepemilikan logis database dibedakan dari isolasi akses konfigurasi. Bukti baru P4 memakai `--no-deps` dan hanya menampilkan build Notification; screenshot lama tetap menjadi riwayat.

`evidence/P5/4-3.png` dan `4-3-alternative.png` tidak dimasukkan ke PDF karena memperlihatkan nilai secret konfigurasi. Gambar expiry P3 dengan fragmen token dan dua gambar refresh baru yang memuat CSRF token tidak dimuat; field relevannya ditranskripsi tanpa token. Bukti asli tetap dipertahankan dan perlu disanitasi tersendiri sebelum publikasi repository.

## Proof tambahan pada commit 4a1973b

- `P3/1-new.png`: dua request kredensial silang ditolak HTTP 401; menggantikan screenshot lama sebagai bukti penolakan. Kontrol positif 404/405 tetap dijelaskan sesuai gambar.
- `P3/3-1-alternative.png` dan `P3/3-2-alternative.png`: sesi browser BNPB Pusat dengan mode OIDC dan expiry bertambah 55 detik; profil dan expiry disajikan sebagai transkripsi pada Bab P3.
- `P4/1-3-new.png`: build Notification dengan `--no-deps`; hanya layanan target tampil pada output.

Scorecard otomatis dan kedua hasil k6 tidak berubah pada commit ini. Manifest memuat semua 52 artefak beserta hash; bukti asli tidak diubah.

## Kompilasi

Prasyarat: MiKTeX atau TeX Live dengan `pdflatex` dan paket pada `preamble.tex`. Dari root repository pada PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File docs/tex/build.ps1
```

Skrip menjalankan tiga putaran, menyimpan berkas bantu dalam `docs/tex/build/`, lalu memperbarui `IF4031_M1_nasigoreng.pdf`. Skrip tidak menimpa `main.pdf`.

## Sebelum pengumpulan

1. Isi kontribusi nyata setiap anggota di `metadata.tex`.
2. Tinjau caption dan cakupan bukti pada tiap bab.
3. Build kembali setelah mengubah LaTeX dan gunakan PDF utama di atas.
4. Jika evidence diganti, perbarui hash manifest. Hash sebelumnya untuk `mock-container-2026-09-30.json` tetap dicatat sebagai riwayat; revisi ini tidak mengubah artefak sumber tersebut. Manifest menggunakan path berkas sebagai identitas langsung.

Panduan: [mock](../mock-testing.md), [Auth](../auth-testing.md), [E2E](../e2e-testing.md), [load testing](../load-testing.md). Menu demo ada di `tests/test_menu.py`; suite memakai `tests/run_problem_set.py`.
