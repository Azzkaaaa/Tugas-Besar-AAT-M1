# Laporan Milestone 2

PDF utama: [IF4031_M2_nasigoreng.pdf](../IF4031_M2_nasigoreng.pdf).

Laporan disusun dari spesifikasi M2, laporan/riwayat M1, kode layanan, manifest infrastruktur, dan bukti tersimpan hingga 22 November 2026. Folder `../tex/` beserta PDF M1 dipertahankan. Tidak ada pengujian cluster atau load test baru dalam penyusunan ini.

## Isi

- Gambaran sistem, diagram arsitektur dan deployment, topologi aktual, teknologi, asumsi, resource, dan cara deploy.
- Matriks keputusan yang dipertahankan/diubah/dibatalkan dari M1.
- P1–P5: masalah, mekanisme, alternatif, konfigurasi, konsistensi, hasil per kriteria, dan batas bukti.
- Evaluasi, daftar pekerjaan yang belum terbukti, indeks bukti, serta formulir penggunaan AI.
- `evidence/manifest.json`: lokasi dan SHA-256 sumber yang digunakan.
- `evidence/measurements.json`: ekstraksi numerik terpilih; bukan pengukuran baru.

Kontribusi M2 di `metadata.tex` sengaja kosong sesuai arahan pengguna. Form AI mengisi penggunaan untuk penyusunan laporan ini saja; riwayat penggunaan AI anggota dalam implementasi tetap perlu dilengkapi. Kriteria yang gagal/belum diuji tidak dinyatakan lulus. Tidak ada screenshot M2 yang dibuat-buat; diagram dibuat dari kode/topologi dan tabel ditranskripsikan dari artefak.

## Build

Dari root repository `services` dengan MiKTeX/TeX Live dan PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File docs/tex-m2/build.ps1
```

Tiga putaran `pdflatex` menyelesaikan daftar isi dan rujukan silang. Berkas bantu ada di `build/`; PDF utama ditulis ke `docs/IF4031_M2_nasigoreng.pdf`. Tidak perlu koneksi cluster atau kredensial.

## Memeriksa sumber bukti

`prepare_evidence.py` memerlukan tata letak checkout asli: `services/`, `infrastructure/`, `Tugas-Besar-AAT-M1/`, dan PDF spesifikasi dalam satu direktori induk. Jalankan hanya bila sengaja memperbarui snapshot sumber:

```powershell
python docs/tex-m2/prepare_evidence.py
```

Build PDF biasa tidak memerlukan checkout infrastruktur; daftar bukti LaTeX sudah disertakan. Manifest merujuk artefak sumber, bukan menyalin seluruh log, kredensial, atau payload ke folder laporan. Setiap angka tetap harus ditinjau jika sumber diperbarui; skrip tidak menulis ulang narasi hasil secara otomatis.

## Pelengkapan oleh kelompok

1. Lengkapi riwayat penggunaan AI pada implementasi.
2. Tambahkan tautan video demo.

Hasil utama yang masih terbatas: P1 belum membuktikan kehilangan seluruh worker/pemulihan tanpa intervensi; P2 belum menjalankan 10.000 VU dan gagal mencapai rasio throughput 2×/rollout tanpa error; P4 belum mengejar backlog dalam 60 detik; P5 belum memiliki timestamp emisi independen untuk menghitung SLO penerima. Rincian dan sumber ada di PDF.
