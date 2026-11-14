# Aggregator

Rencana service ingest dan pemilik data kanonik, gRPC pada port **9091**.
Kontrak: [AggregatorService](../../contracts/proto/bnpb/aggregator/v1/aggregator.proto),
[data bersama](../../contracts/proto/bnpb/common/v1/common.proto), dan
[envelope broker](../../contracts/proto/bnpb/event/v1/event.proto).
Implementasi dan bahasa belum dipilih.

- **Input:** polling REST independen ke BMKG/PVMBG; RPC `ListHazards`,
  `GetHazard`, dan `GetSourceStatuses` dari pemanggil internal yang sah.
- **Output:** proyeksi hazard, freshness/partial, serta publikasi event kanonik
  melalui RabbitMQ. Tidak memanggil setiap consumer secara langsung.
- **Storage milik sendiri:** Canonical Store berisi hazard, checkpoint ingest,
  dan outbox. Hanya Aggregator mengaksesnya langsung. Backend dipilih dengan
  `CANONICAL_STORE_BACKEND` (`postgres` atau `cassandra`; default kode `postgres`,
  compose `cassandra` lewat `AGGREGATOR_STORE_BACKEND`).
- **Tanggung jawab:** korelasi tsunami, normalisasi, attributes dinamis,
  deduplikasi/upsert, isolasi polling, cache saat outage, serta publikasi durable
  setelah commit. Memverifikasi JWT access token melalui JWKS endpoint Zitadel
  (`AUTH_AUTHORITY`); key set di-cache dan di-refresh otomatis saat rotasi kunci.
  Service token antar-service dibandingkan dengan constant-time comparison.

## Storage

Logika bisnis (ingest, gRPC, publisher) hanya memakai interface di
`internal/storage` (`HazardStore`, `SourceStateStore`, `Outbox`) dengan tipe
domain; tidak ada transaksi, query bebas, atau tipe protobuf yang melewati
interface. Backend ada di `internal/storage/postgres` dan
`internal/storage/cassandra`, dan keduanya harus lulus kontrak yang sama
(`internal/storage/storetest`).

- **Upsert + outbox:** `ApplyRevision` menulis satu hazard beserta event
  outbox-nya secara atomik (Postgres: transaksi; Cassandra: logged batch).
  Atomisitas per hazard, bukan per poll: bila poll gagal di tengah, hazard yang
  sudah tertulis tetap ada, watermark tidak maju, dan poll berikutnya mengulang
  sisanya tanpa duplikat (hazard yang isinya sama tidak menghasilkan event).
- **Pagination:** urutan tetap `occurred_at` DESC lalu `hazard_id` ASC; page token
  opaque milik backend. Token tidak valid menghasilkan `INVALID_ARGUMENT`.
- **Presisi waktu:** semua backend menyimpan waktu dalam UTC, milidetik.
- **Outbox:** at-least-once; consumer melakukan dedup dengan `event_id`.
  Publikasi gagal diulang dengan backoff per event (1 detik, dua kali lipat,
  maksimal 1 menit) tanpa menahan event lain, lalu dipindah ke dead letter
  setelah `OUTBOX_MAX_RETRIES`. Event yang sudah terbit atau di-dead-letter
  dihapus setelah `OUTBOX_RETENTION_HOURS` (default 168); dead letter tetap
  disimpan.

### Postgres

- **Banyak replika:** setiap replika melayani gRPC dan mempublikasikan outbox.
  Publisher mengklaim baris dengan `FOR UPDATE SKIP LOCKED` dan lease
  (`next_attempt_at`), sehingga replika mengambil baris yang berbeda; baris milik
  publisher yang mati diambil lagi setelah lease 30 detik.
- **Leader ingest:** polling BMKG/PVMBG dan pembersihan outbox hanya berjalan di
  pemegang advisory lock sesi Postgres. Bila leader mati, replika lain mengambil
  alih dalam ±5 detik. (Mekanisme leader akan dirombak terpisah.)
- **Migrasi:** goose, file SQL di `internal/storage/postgres/migrations/postgres`
  yang di-embed dan dijalankan saat startup dengan session lock Postgres.
  `00001_baseline.sql` adalah skema lama yang ditulis idempoten. Migrasi
  berikutnya menambah `0000N_<nama>.sql` dan mengikuti expand/contract.

### Cassandra

Konfigurasi (dibaca sama oleh aggregator, `storecopy`, dan `storeadmin`):

| Variabel `CANONICAL_STORE_CASSANDRA_…` | Default | Arti |
| --- | --- | --- |
| `HOSTS` | wajib (tools: `localhost:9042`) | Host, dipisah koma |
| `KEYSPACE` | `bnpb_canonical` | Keyspace |
| `USERNAME` / `PASSWORD` | kosong | Autentikasi password |
| `CONSISTENCY` | `LOCAL_QUORUM` | Consistency baca/tulis (nama gocql: `ONE`, `QUORUM`, `LOCAL_QUORUM`, …). Serial consistency LWT tetap `LOCAL_SERIAL`. |
| `LOCAL_DC` | kosong | Datacenter lokal: query diarahkan ke node DC itu dulu (token-aware), sehingga `LOCAL_*` berarti DC itu. Isi bila memakai `NetworkTopologyStrategy`. |
| `REPLICATION_STRATEGY` | `SimpleStrategy` | `SimpleStrategy` atau `NetworkTopologyStrategy` |
| `REPLICATION_FACTOR` | `1` | Replika untuk `SimpleStrategy` |
| `DATACENTERS` | kosong | Replika per DC untuk `NetworkTopologyStrategy`, mis. `dc1:3,dc2:3` |
| `BUCKET_PERIOD` | `1m` | Period awal keyspace baru (lihat di bawah) |

- **Replikasi hanya dipakai saat keyspace dibuat.** Bila keyspace sudah ada dengan
  replikasi lain, aggregator tidak mengubahnya dan hanya mencatat peringatan
  berisi perintah `ALTER KEYSPACE … WITH replication = …` dan
  `nodetool repair -full` yang perlu dijalankan.
- **Faktor replikasi harus ≤ jumlah node.** Compose menjalankan satu node, jadi
  harus 1. Dengan RF 3 dan `LOCAL_QUORUM`, tiap baca/tulis butuh 2 dari 3 replika;
  cluster minimal 3 node dan tahan satu node mati.

- **Tabel:** `hazards_by_id` (Get), `hazard_by_source_ref` (identitas, diklaim
  dengan `INSERT … IF NOT EXISTS`), `hazards_by_bucket` (List; partisi = waktu
  `occurred_at` dibulatkan ke bucket period), `hazard_buckets` (indeks bucket
  yang berisi data per sumber dan `*`; dipakai List dan `HasData`),
  `source_state`, `outbox_events`, `outbox_pending`, `outbox_dead_letters`,
  `bucket_schedule`.
  `attributes` disimpan sebagai teks JSON, jadi skema v1/v2 tetap berdampingan
  tanpa DDL.
- **Bucket period (bisa diubah saat berjalan):** jadwal segmen di
  `bucket_schedule`; tiap segmen berlaku untuk hazard dengan `occurred_at` sejak
  awal segmen. Period harus membagi 24 jam (mis. `1m`, `5m`, `1h`, `24h`).
  `CANONICAL_STORE_CASSANDRA_BUCKET_PERIOD` hanya period awal untuk keyspace
  baru; bila berbeda dari yang berlaku, aggregator hanya mencatat peringatan.
  Ubah period tanpa restart:

  `storeadmin` hanya membuka keyspace yang sudah ada dengan skema terbaru; nama
  keyspace salah langsung gagal, tidak membuat keyspace kosong.

  ```bash
  docker exec <container aggregator> ./storeadmin bucket-period          # lihat
  docker exec <container aggregator> ./storeadmin bucket-period set 5m   # ubah
  ```

  Perubahan berlaku mulai batas pertama yang dibagi period lama dan baru,
  minimal 1 menit dari sekarang (paling lama 24 jam lagi). Aggregator membaca
  ulang jadwal tiap 30 detik, jadi sudah tahu sebelum segmen dimulai. Bucket lama
  tidak ditulis ulang; List membaca bucket lama dan baru sekaligus. Hanya satu
  perubahan boleh menunggu. Tiap baris `hazards_by_id` mencatat bucket tempat ia
  ditulis, sehingga revisi yang memindahkan hazard menghapus baris yang benar.
  Period pendek: partisi kecil, tetapi lebih banyak partisi dibaca per halaman;
  period panjang: sebaliknya.
- **List:** menelusuri bucket berisi data dari terbaru ke terlama, filter
  source/type/severity di memori. Filter yang sangat selektif atas riwayat
  panjang membaca banyak bucket; ini batas skala yang diterima untuk volume
  dua feed nasional.
- **Outbox satu consumer:** event menunggu di satu partisi `outbox_pending`;
  klaim adalah write biasa (tanpa LWT), jadi hanya satu publisher boleh berjalan.
  Event selesai ditulis ulang di `outbox_events` dengan TTL retensi (pruner
  tidak perlu menghapus). `gc_grace_seconds` partisi pending 1 jam agar tombstone
  cepat dibuang.
- **Tanpa leader (sementara):** backend ini belum punya pemilihan leader;
  poller, pruner, dan publisher berjalan di proses itu sendiri. **Jalankan satu
  replika aggregator saja** sampai mekanisme leader dirombak.
- **Migrasi:** file CQL di `internal/storage/cassandra/migrations` (`NNNN_nama.cql`,
  pernyataan diakhiri `;` di akhir baris), dicatat di `schema_migrations` dan
  dijalankan di bawah lease `schema_lock`. Keyspace dibuat bila belum ada.

### Menyalin Postgres → Cassandra (sekali jalan)

`cmd/storecopy` membaca konfigurasi Cassandra yang sama (flag seperti
`-replication-factor`, `-replication-strategy`, `-datacenters`, `-keyspace`
menimpanya) dan membuat keyspace bila belum ada. Ia menyalin hazard (dengan `hazard_id` dan `ingested_at` asli),
state ingest, event outbox yang belum terbit, dan dead letter. Riwayat event yang
sudah terbit tidak disalin. Idempoten, aman diulang; `-dry-run` hanya menghitung.

```bash
docker compose --env-file .env --env-file .env.auth stop aggregator-service
docker compose --env-file .env --env-file .env.auth run --rm --no-deps \
  --entrypoint ./storecopy aggregator-service
AGGREGATOR_STORE_BACKEND=cassandra docker compose --env-file .env --env-file .env.auth up -d aggregator-service
```

Di dalam compose, DSN Postgres, host Cassandra, keyspace, dan bucket period
diambil dari environment service. Jalankan sebelum aggregator mulai ingest ke
Cassandra; hazard yang identitasnya sudah dipakai `hazard_id` lain dilewati dan
dilaporkan.

### Uji integrasi storage

`go test ./...` (dan `nx run aggregator:test`) hanya menjalankan unit test: tidak
butuh database, Docker, atau variabel lingkungan. Uji integrasi diberi build tag
`integration` dan baru dikompilasi bila tag itu dipakai; tanpa `AGG_TEST_*` uji
itu gagal (bukan dilewati).

```bash
docker compose --env-file .env --env-file .env.auth up -d aggregator-db aggregator-cassandra
AGG_TEST_POSTGRES_DSN='postgres://aggregator:<password>@localhost:5432/<db uji>?sslmode=disable' \
AGG_TEST_CASSANDRA_HOSTS=localhost:9042 npx nx run aggregator:integration-test
# atau langsung: go test -tags integration -p 1 -count=1 ./...
```

Uji Postgres meng-`TRUNCATE` tabel aggregator di database tersebut: pakai
database uji terpisah, jangan `bnpb_canonical`. `-p 1` wajib karena paket
memakai database yang sama. Uji Cassandra membuat dan
menghapus keyspace `agg_test_*` sendiri.

Health memakai `grpc.health.v1.Health` standar. TODO: runtime/Dockerfile,
storage fleksibel, worker polling, referensi gunung api, retry/deadline,
outbox/publisher, otorisasi dan proyeksi, telemetry, serta uji P1/P2/P4/P5.
Lihat [konfigurasi](../../docs/configuration.md) dan
[checklist demo](../../docs/milestone-plan.md).
