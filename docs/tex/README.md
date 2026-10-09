# Laporan Milestone 1

Laporan mengikuti spesifikasi `IF4031 - Tugas Besar - M1.pdf` dan snapshot kode `64c03cf` (9 Oktober 2026). Nama sistem: **Sistem Koordinasi Kebencanaan**, kelompok **nasgor**.

- `main.tex`: susunan laporan.
- `metadata.tex`: nama sistem, kelompok, tanggal, dan **kontribusi yang diisi manual**.
- `contents/01-sistem.tex`: gambaran, arsitektur, teknologi, asumsi, cara menjalankan.
- `contents/02-p1.tex` sampai `06-p5.tex`: solusi, alternatif, batasan, dan bukti tiap problem.
- `contents/07-bukti.tex`: inventaris bukti, hasil aktual, perintah reproduksi, dan bukti yang masih diperlukan.
- `figures/`: diagram TikZ yang dapat diedit.
- `evidence/`: salinan hasil tes historis mock/Auth serta tes Aggregator aktual; tidak berisi secret.
- `IF4031_M1_nasgor.pdf`: PDF hasil kompilasi untuk ditinjau.

## Kompilasi

Prasyarat: MiKTeX atau TeX Live dengan `pdflatex` dan paket yang disebut dalam `preamble.tex`.
Dari root repository pada PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File docs/tex/build.ps1
```

Atau dari `docs/tex` pada shell apa pun:

```sh
pdflatex -interaction=nonstopmode -halt-on-error -jobname=IF4031_M1_nasgor main.tex
pdflatex -interaction=nonstopmode -halt-on-error -jobname=IF4031_M1_nasgor main.tex
pdflatex -interaction=nonstopmode -halt-on-error -jobname=IF4031_M1_nasgor main.tex
```

Skrip PowerShell menyimpan berkas bantu dalam `build/` yang diabaikan Git lalu menyalin PDF ke folder ini. Gambar sampul lama tidak digunakan; sampul laporan dibuat sederhana dengan identitas akademik.

## Sebelum pengumpulan

1. Isi kontribusi di `metadata.tex` (perancangan, implementasi, penulisan).
2. Lengkapi bukti runtime yang ditandai belum tersedia pada lampiran, terutama k6 P2, integrasi terbaru, dan skenario consumer P5. Angka target bukan hasil uji.
3. Tinjau batasan kode pada P1?P5. Laporan tidak menyatakan seluruh spesifikasi telah lulus.
4. Jika kode atau hasil uji berubah, perbarui tanggal/snapshot, isi bab dan evidence, kemudian build ulang.

Artefak Auth 45 pemeriksaan berasal dari arsitektur terdahulu pada 6 Oktober, bukan bukti bahwa stack terbaru sudah lulus. Hasil mock berasal dari 30 September. Tes Aggregator 9 Oktober mencatat 7 tes ingest dan 15 tes auth lulus; paket domain tidak memiliki test case. Tes bridge lokal belum dimulai karena dependensi `@opentelemetry/api` belum terpasang; bukan kegagalan perilaku autentikasi. Berkas cache/env/database tidak perlu disalin ke laporan.

Panduan testing untuk melengkapi bukti P1?P5: [docs/report-testing.md](../report-testing.md).
