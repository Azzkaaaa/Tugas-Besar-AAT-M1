# Frontend SPA

Folder untuk SPA yang memakai [REST/SSE Gateway](../contracts/openapi/gateway.yaml).
Framework dan implementasi belum dipilih. Browser menghubungi Gateway; gRPC,
database, RabbitMQ, dan mock sumber tetap berada di belakang batas service.

Rencana alur: login sebagai identitas Media/Tim Lapangan/BNPB Pusat, daftar dan
detail hazard, indikator data stale/partial, notifikasi, serta live feed dengan
reconnect dan replay. UI mengikuti hak yang diberikan server; pembatasan field
mentah tetap ditegakkan server. Semua alur M1 read-only.

Token pengguna digunakan pada header `Authorization`; secret antar-service,
API key BMKG, dan token PVMBG tidak dikirim ke browser. Kredensial demo tidak
dibundel ke aset statis. Pilih mekanisme SSE yang dapat mengirim bearer header,
tangani refresh token sekali pakai, dan koordinasikan refresh serentak agar
tidak memakai ulang refresh token. Setelah `reset_required`, ambil snapshot
ulang sebelum melanjutkan feed.

TODO: pilih framework, buat UI, tentukan penyimpanan token dan konfigurasi URL
Gateway, implementasikan state refresh/reconnect/error, Dockerfile, serta uji
alur ketiga identitas. Tidak ada source aplikasi atau perintah menjalankan SPA
pada tahap struktur dan kontrak ini.
