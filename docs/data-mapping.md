# Pemetaan ke HazardEvent

Aturan berikut adalah kontrak perilaku Aggregator; mapper belum diimplementasikan.
Field tanpa padanan kanonik dipertahankan di `attributes`, termasuk field aditif
yang belum dikenal. Nama field kanonik mengikuti spesifikasi M1.

## Identitas dan waktu

BNPB membangkitkan `hazard_id` opaque. Pemetaan persisten `(source, source_ref_id)`
mempertahankan ID pada polling ulang/pembaruan. `occurred_at` mengikuti sumber;
`ingested_at` adalah waktu pertama tersimpan di BNPB, tetap pada pembaruan.
Waktu revisi publikasi ada di envelope broker. Gunakan RFC 3339 dengan timezone
di REST dan Timestamp di gRPC.

## Seismik dan tsunami

| Field kanonik | Asal / aturan |
| --- | --- |
| `source` | BMKG |
| `source_ref_id` | `event_id` |
| `hazard_type` | SEISMIC |
| `area_name` | `region_name` |
| `latitude`, `longitude` | `epicenter_lat`, `epicenter_lon` |
| `occurred_at` | `occurred_at` |
| `attributes` | `magnitude`, `depth_km`, `potential_tsunami`, field tambahan, warning terkait |

Ambil warning melalui `/tsunami-warnings` dan korelasikan
`related_event_id == event_id`. Severity mengikuti warning tertinggi bila ada:
`Awas → AWAS`, `Siaga → SIAGA`, `Waspada → WASPADA`.
Tanpa warning: magnitude `<5.0 → NORMAL`, `5.0 ≤ m <6.5 → WASPADA`,
`m ≥6.5 → SIAGA`. `potential_tsunami=true` sendiri tidak otomatis berarti AWAS.

Simpan warning sebagai array `attributes.tsunami_warnings`, dengan field asli
`warning_id`, `related_event_id`, `threat_level`, `affected_zones`,
`estimated_arrival`, beserta field aditifnya. Array mengakomodasi beberapa warning.
Warning terlambat disimpan sebagai pending join, memperbarui hazard ID yang sama,
lalu memicu publikasi revisi. Warning hanya valid untuk seismic event dengan
`potential_tsunami=true`; anomali dikarantina/dilog.

Contoh batas tanpa warning: 4.9 NORMAL, 5.0 WASPADA, 6.49 WASPADA, 6.5 SIAGA.
Warning Awas terkait membuat severity AWAS.

## Vulkanik

| Field kanonik | Asal / aturan |
| --- | --- |
| `source` | PVMBG |
| `source_ref_id` | `report_id` |
| `hazard_type` | VOLCANIC |
| `area_name` | Nama gunung dari referensi statis BNPB berdasarkan `volcano_id` |
| `latitude`, `longitude` | Koordinat dari referensi statis yang sama |
| `occurred_at` | `reported_at` |
| `severity` | Normal/Waspada/Siaga/Awas → NORMAL/WASPADA/SIAGA/AWAS |
| `attributes` | `eruption_count_24h`, `ash_column_height_m`, confidence bila ada, field tambahan |

`volcano_id` dipakai untuk lookup dan boleh disimpan di attributes untuk pelacakan.
Referensi gunung dimiliki Aggregator dengan ID yang disepakati saat seed mock
dibuat. ID tak dikenal masuk karantina/retry yang terlihat di metrik; jangan
mengarang koordinat 0,0. Seed harus memakai ID yang ada pada referensi.

Mock PVMBG menggunakan lima ID berikut. Tabel ini adalah kesepakatan referensi
untuk implementasi Aggregator berikutnya; mock hanya mengirim `volcano_id`,
bukan nama dan koordinat. Koordinat dibulatkan dan bersifat ilustratif untuk
demo dengan data sintetis, bukan data pemantauan atau referensi operasional.

| `volcano_id` | Nama demo | Latitude ilustratif | Longitude ilustratif |
| --- | --- | --- | --- |
| `merapi` | Gunung Merapi | -7.54 | 110.45 |
| `semeru` | Gunung Semeru | -8.11 | 112.92 |
| `anak-krakatau` | Gunung Anak Krakatau | -6.10 | 105.42 |
| `marapi` | Gunung Marapi | -0.38 | 100.47 |
| `lewotobi-laki-laki` | Gunung Lewotobi Laki-laki | -8.54 | 122.78 |

PVMBG mulai versi 1 tanpa `confidence_level`. POST `/admin/schema-version` dengan
`{"version":2}` mengaktifkan nilai 0–1; versi 1 menonaktifkannya tanpa restart.
Jangan mengisi confidence nol saat field tidak ada; nilai 0 yang benar tetap
disimpan. Simpan record sebelum/sesudah perubahan berdampingan. Perubahan
proyeksi historis mock tidak menghapus atribut kanonik record yang tidak diperbarui.

Implementasi mock menyimpan payload JSON dalam SQLite. Report baru yang dibuat
saat v2 aktif memiliki `confidence_level`; report historis v1 tetap boleh tanpa
field tersebut. Beralih kembali ke v1 hanya menyembunyikan field pada respons,
tanpa menghapus nilainya dari database. Saat kembali ke v2, nilai tersimpan
terlihat lagi. Restart mereset proyeksi aktif ke v1, bukan menghapus record.

## Incremental ingest dan evolusi

`since` inklusif pada mock. Seismik memakai `occurred_at`, vulkanik memakai
`reported_at`. Warning tidak mempunyai timestamp terbit di skema wajib, sehingga
mock menyimpan metadata update internal untuk filter `since`, tanpa menambah
field wajib di payload. `estimated_arrival` bukan watermark. Tanpa `since`, semua
record tersimpan dikembalikan.

Watermark independen per endpoint menggunakan overlap dan deduplikasi ID.
Untuk warning, waktu mulai poll terakhir yang berhasil menjadi watermark poll
berikutnya agar update selama request berjalan tetap tertangkap. Jangan memakai
waktu selesai request; watermark tidak maju bila request/persist gagal.

Perubahan aditif JSON didukung. Rename, penghapusan field wajib, perubahan tipe,
dan nilai enum baru memerlukan versi/penanganan eksplisit. Protobuf tidak otomatis
menyelamatkan field JSON sumber: parser harus mengumpulkannya ke `Struct
attributes`. Konflik field sumber baru dengan field turunan `tsunami_warnings`
dipertahankan di `attributes.upstream_extensions` agar hasil join tidak tertimpa.

REST gateway memakai allowlist ketat pada proyeksi summary/full. Enum proto
berprefiks dipetakan eksplisit ke label REST; jangan mengekspos ProtoJSON langsung.
Field dinamis attributes hanya tersedia pada proyeksi full.
