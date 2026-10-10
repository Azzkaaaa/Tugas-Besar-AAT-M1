# Laporan Milestone 1

Revisi 10 Oktober 2026 mengikuti spesifikasi tugas dan snapshot repository `1c2856b`, menggunakan evidence yang sudah tersedia. Tidak ada pengujian ulang saat penyusunan revisi.

**PDF utama: [IF4031_M1_nasgor.pdf](IF4031_M1_nasgor.pdf).** `main.pdf` adalah hasil build editor terpisah.

## Isi

- `metadata.tex`: identitas, tanggal, dan kontribusi yang tetap diisi manual oleh anggota.
- `contents/01-sistem.tex`: arsitektur, teknologi, asumsi, dan cara menjalankan.
- `contents/02-p1.tex` sampai `06-p5.tex`: rancangan, alternatif, batasan, hasil aktual, dan screenshot demo.
- `contents/07-bukti.tex`: inventaris, scorecard suite, hasil historis, reproduksi, dan cakupan kesimpulan.
- `figures/`: diagram TikZ; Notification kini mencakup SQLite dan push outbox.
- `evidence/manifest.json`: hash semua artefak dan daftar gambar yang dimuat di PDF.

## Bukti yang digunakan

| ID | Artefak | Cakupan |
|---|---|---|
| E1-E3 | JSON mock, JSON browser Auth, JSONL unit Aggregator | Bukti historis dengan tanggal asli |
| E4 | `evidence/problem_set_report.md` | Scorecard 19 tes; 0 error/failure dan 0 skipped |
| M1-M5 | Screenshot dalam `evidence/P1/` sampai `P5/` | Observasi demo pada tiap bab |
| K1-K2 | Dua JSON k6 dalam `evidence/P2/` | Beban, isolasi, percentile, error, dan 429 |

K1 mencatat 50 VU selama fase beban 60 detik, 17.546 request, throughput 292,43 request/detik, dan p95 243,96 ms. K2 mencatat p95 BMKG pada kontensi 87,09 ms. Throughput menggunakan durasi fase pengukuran, bukan durasi keseluruhan run termasuk pemanasan.

Interpretasi mengikuti cakupan bukti. Penerbitan token service melalui client credentials dibedakan dari refresh sesi browser. Kepemilikan logis database dibedakan dari isolasi akses konfigurasi. Build yang menyertakan dependency tidak disebut build eksklusif satu layanan.

`evidence/P5/4-3.png` dan `4-3-alternative.png` tidak dimasukkan ke PDF karena memperlihatkan nilai secret konfigurasi. Gambar expiry P3 dengan fragmen token juga tidak dimuat; hasilnya dijelaskan dalam teks. Bukti asli tetap dipertahankan dan perlu disanitasi tersendiri sebelum publikasi repository.

## Kompilasi

Prasyarat: MiKTeX atau TeX Live dengan `pdflatex` dan paket pada `preamble.tex`. Dari root repository pada PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File docs/tex/build.ps1
```

Skrip menjalankan tiga putaran, menyimpan berkas bantu dalam `docs/tex/build/`, lalu memperbarui `IF4031_M1_nasgor.pdf`. Skrip tidak menimpa `main.pdf`.

## Sebelum pengumpulan

1. Isi kontribusi nyata setiap anggota di `metadata.tex`.
2. Tinjau caption dan cakupan bukti pada tiap bab.
3. Build kembali setelah mengubah LaTeX dan gunakan PDF utama di atas.
4. Jika evidence diganti, perbarui hash manifest. Hash E1 sebelumnya tetap dicatat sebagai riwayat; revisi ini tidak mengubah artefak sumber tersebut.

Panduan: [mock](../mock-testing.md), [Auth](../auth-testing.md), [E2E](../e2e-testing.md), [load testing](../load-testing.md). Menu demo ada di `tests/test_menu.py`; suite memakai `tests/run_problem_set.py`.
