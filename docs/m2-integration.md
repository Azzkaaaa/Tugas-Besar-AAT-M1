# Integrasi P3 dengan P4–P5

Branch `feat/P4-P5` menggabungkan services `dde7d00` dan infrastructure `513d7ab`
dari main. Verifikasi lokal dilakukan pada **16 November 2026**.

## Perubahan integrasi

- Migrasi Cassandra P3 tetap `0007_problem3.cql`; stream P4/P5 memakai
  `0008_m2_streams.cql`. Keduanya diterapkan dan diuji pada keyspace test baru.
- Aggregator membuka satu listener internal untuk Audit dan Internal Ops P3–P5.
  Worker field report tetap proses terpisah dan tidak menjalankan job satelit/sensor.
- `AUDIT_API_TOKEN` menjadi token bersama; `AUDIT_TOKEN` tetap alias kompatibilitas.
  Jika keduanya diisi, `AUDIT_API_TOKEN` yang digunakan semua route.
- Cassandra menyimpan input invalid satelit/sensor di inbox P3. Audit bisa
  memfilter empat sumber; raw bytes hanya melalui Internal Ops dengan scope
  `ingest:inspect`. Payload besar dipecah ke chunk tanpa membutuhkan filesystem
  sementara baru. Pengulangan input tidak membuat record karantina ganda.
- Urutan intake P3 (`ingest_sequence`) dan stream P4 (`stream_sequence`) tetap
  dicek pada penulisan kanonik. Retract wildfire tetap dikeluarkan dari List.
- `/field-reports` memakai limiter yang sama dengan API unary, termasuk slot
  cadangan berdasarkan JWT terverifikasi untuk Tim Lapangan/Internal.
- Chart gabungan mempertahankan tiga replica Aggregator, kedua sumber envFrom,
  satu port 9092, Secret audit P3, label pemilik store, dan worker terpisah.

## Hasil pengujian

| Pemeriksaan | Hasil |
|---|---|
| `go test ./...` Aggregator | Lulus, termasuk unit test P1/P2/P3/P4/P5 |
| `go test ./...` API Gateway | Lulus, termasuk field report saat pool Media penuh |
| HTTP internal gabungan | Audit semua sumber/filter, events, dan penolakan akses anonim lulus |
| Karantina payload besar | Byte invalid utuh, chunking, dan dedup lulus |
| Cassandra 5.0.9 nyata | 4 tes lulus: repair revision, storage P3–P5, receipt durable/reopen, inbox raw/state |
| PostgreSQL nyata | Crash sebelum checkpoint dan replay idempoten lulus |
| Image resmi Sensor Pantai | Adapter menyimpan COASTAL, checkpoint maju, quality invalid diparkir |
| Mock PVMBG | 10 unit test lulus |
| Mock HTTP BMKG/PVMBG | 16 tes lulus, termasuk schema mode 1–5 |
| Infrastruktur | Helm lint baseline/gabungan dan Gateway, render gabungan, Kustomize, serta konfigurasi Compose P3–P5 lulus |

Tes Cassandra gabungan memeriksa journal immutable, pembacaan lintas partisi
1000 input, CAS checkpoint, stale writer P3/P4, withdrawal, prioritas outbox
sensor dengan slot sumber lain, dan raw quarantine 90 KB. Cassandra test memakai
satu node dengan RF=1: hasil ini tidak membuktikan ketahanan quorum multi-node.

Perintah reproduksi dari `services/aggregator`, dengan database test terpisah:

```powershell
$env:AGG_TEST_CASSANDRA_HOSTS='127.0.0.1:19042'
go test -p 1 -parallel 1 -tags integration ./internal/storage/cassandra -run 'TestProblem3|TestM2SharedStorageWithProblem3|TestRepairCommittedRevision' -count=1 -v
go test ./...
```

Log lokal tersimpan di `.cache/m2/merge-go-tests.txt`, `merge-cassandra-tests.txt`,
`merge-postgres-tests.txt`, dan `merge-mock-tests.txt`. Tidak ada token kelompok
di log/bukti yang di-commit.

## Deployment gabungan dan batas verifikasi

Ikuti `infrastructure/m2/README.md`, aktifkan `problem3.enabled` dan `m2.enabled`,
dan gunakan image services hasil merge. Compose development dapat memakai
`compose.m2.yaml` bersama profil `problem3`; backend P3 memerlukan Cassandra.
Konfigurasi role/scope baru memerlukan Zitadel Claims hasil P3 dan route
`/field-reports` yang dilindungi ext-auth.

Belum dilakukan rollout cluster kelompok, game day node/kernel terpisah,
perbandingan seluruh feed dengan referensi asisten, atau pengukuran receipt
notifikasi sampai perangkat Tim Lapangan. Smoke test sensor belum membuktikan
zero-loss/p95 ≤5 detik. Batas ini sama dengan kebutuhan demo pada panduan P4/P5.
