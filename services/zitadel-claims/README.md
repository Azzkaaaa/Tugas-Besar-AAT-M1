# Zitadel claims

Service Rust kecil yang menjadi target Actions v2 Zitadel `BNPB scope claims`
(`REST_CALL`, `interruptOnError`). Zitadel tidak mendukung scope OAuth kustom,
sehingga matriks role → scope disimpan di [`src/claims.rs`](src/claims.rs) dan
dikirim kembali ke Zitadel sebagai klaim `role` dan `scopes`. Provisioning
mendaftarkan target `POST http://zitadel-claims:3002/claims` untuk function
`preuserinfo` (userinfo, ID token, introspeksi) dan `preaccesstoken` (JWT access
token, termasuk hasil token exchange gateway).

| Role | Klaim `scopes` |
| --- | --- |
| MEDIA | `hazards:read:summary`, `notifications:read`, `webhooks:read` |
| FIELD_TEAM | ringkasan + `hazards:read:full`, `notifications:read`, `webhooks:read`, `webhooks:write` |
| BNPB_INTERNAL | sama dengan FIELD_TEAM |

Endpoint memverifikasi header `ZITADEL-Signature` (`t=<unix>,v1=<hex>`,
HMAC-SHA256 atas `"<t>." + body` dengan `actions_signing_key`, toleransi lima
menit, perbandingan constant-time). Service hanya menjawab untuk client proyek
ini (bridge, introspeksi, gateway) dan hanya memakai grant aktif pada proyek dan
organisasi yang dikonfigurasi. Role dengan prioritas tertinggi dipilih:
BNPB_INTERNAL, FIELD_TEAM, lalu MEDIA. Pengguna tanpa grant tidak mendapat klaim,
sehingga Auth bridge menolak login (`role_required`). Payload dengan bentuk tak
terduga menghasilkan `{}` (tanpa klaim), bukan error, karena error target
menghentikan penerbitan token. Service tidak dirutekan Envoy; hanya dapat dicapai
dari jaringan Compose.

| Endpoint | Keterangan |
| --- | --- |
| `POST /claims` | Target Actions v2; 401 `invalid_signature`, 400 `invalid_payload` (`application/problem+json`) |
| `GET /health` | Healthcheck; image distroless memakai `zitadel-claims --healthcheck` |

## Konfigurasi

`auth-bootstrap` menulis `actions.json` ke volume `auth_actions_config`, terpisah
dari `oidc.json`, sehingga service ini tidak pernah melihat client secret. Isinya:
`actions_target_id`, `actions_signing_key`, `client_ids`, `project_id`, dan
`organization_id`. Service mulai lebih dulu (Zitadel harus dapat me-resolve host
target saat target dibuat) dan menunggu file tersebut hingga sepuluh menit.

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `ACTIONS_CONFIG_FILE` | `/actions/actions.json` | File hasil provisioning, mount read-only |
| `PORT` | `3002` | Listener HTTP internal; tidak dipublikasikan ke host |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | kosong (trace nonaktif), `zitadel-claims` | Trace OTLP/HTTP per request, parent dari `traceparent` Zitadel |

Log JSON request tetap ditulis ke stdout; telemetri hanya menambah span.

## Pengujian

```powershell
npx nx run zitadel-claims:test
npx nx run zitadel-claims:lint
npx nx run zitadel-claims:build
```
