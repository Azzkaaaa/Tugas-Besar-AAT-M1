# Panduan kontribusi

Bahasa dan framework service belum dipilih. `package.json` hanya untuk tooling
kontrak, bukan keputusan bahwa backend atau frontend harus menggunakan Node.js.

1. Tentukan perubahan API di `contracts/` sebelum mengimplementasikan service.
2. Pertahankan endpoint, nama field, dan nilai enum mock yang diwajibkan PDF.
3. Jalankan `npm ci` lalu `npm run check`. Untuk proto, gunakan `npm run format:proto`.
4. Sertakan perubahan contoh payload, dokumentasi, dan pemetaan REST/gRPC bila
   mengubah kontrak. Jelaskan dampaknya pada consumer.
5. Setelah service diimplementasikan, tiap service mempunyai Dockerfile, health
   check, pengujian, konfigurasi environment, dan dependency manifest sendiri.

Perubahan aditif menggunakan versi `v1` selama tetap kompatibel. Jangan mengubah
nomor field Protobuf atau menggunakannya kembali: tandai nama dan nomor yang
dihapus dengan `reserved`. Perubahan breaking memakai package/path versi baru.
Sesudah kontrak pertama masuk branch utama, bandingkan perubahan proto dengan:

```sh
npx buf breaking --against '.git#branch=main'
```

Branch pembanding harus memuat `buf.yaml` dan kontrak sebelumnya. Jalankan
perintah ini dari branch pekerjaan; pengecekan kompatibilitas REST dilakukan
melalui review perubahan OpenAPI (lint saja tidak mendeteksi semua breaking change).

Share hanya kontrak dan kode serialization hasil generator. Business logic,
database client, atau model persistence tidak dibagikan lintas service.
Jangan commit `.env`, token, kredensial demo yang benar-benar berlaku, atau kunci.
