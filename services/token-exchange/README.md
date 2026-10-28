# Token exchange

Service Rust kecil yang dipanggil [Envoy](../envoy/README.md) melalui filter
`ext_authz` (gRPC, `envoy.service.auth.v3.Authorization/Check`) untuk setiap
request `/api/`. Service menukar access token opaque Zitadel milik pemanggil
dengan JWT (RFC 8693) yang membawa klaim `role` dan `scopes` dari
[`zitadel-claims`](../zitadel-claims/README.md), lalu Envoy meneruskan
`Authorization: Bearer <JWT>` ke api-gateway.

Service ini menggantikan filter `token-exchange` Built On Envoy (dynamic module
di dalam Envoy), yang memanggil Zitadel tanpa span dan tanpa `traceparent`.
Kini trace bersambung: span ext_authz Envoy → span server `Check` → span klien
`POST /oauth/v2/token` → Zitadel.

| Kondisi | Respons Envoy |
| --- | --- |
| Token ditukar | Request diteruskan; `authorization` upstream ditimpa `<token_type> <access_token>` |
| `Authorization` kosong, bukan `Bearer`, atau token kosong | 401 |
| Zitadel menjawab 4xx (token kedaluwarsa/dicabut), kecuali 429 | 401, sehingga SPA melakukan `/auth/refresh` |
| Zitadel tidak dapat dihubungi, timeout, 429, 5xx, atau respons tidak valid | 502 |
| Service ini tidak dapat dihubungi | 502 (`status_on_error` Envoy) |

Penolakan menjadi local reply Envoy dan dibungkus `application/problem+json`.
Body form sama dengan filter BOE: `grant_type` token-exchange,
`subject_token_type` access token, `requested_token_type` JWT, dan
`subject_token`; client dikirim dengan HTTP Basic. Tidak ada cache. Envoy hanya
mengirim header `authorization` dalam `CheckRequest`. Token, client secret, dan
body respons Zitadel tidak pernah dicatat di log maupun span.

## Konfigurasi

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `TOKEN_EXCHANGE_URL` | wajib | Endpoint token Zitadel, `http://auth.localhost:8080/oauth/v2/token` (split-horizon DNS: langsung ke Zitadel) |
| `TOKEN_EXCHANGE_CLIENT_ID` / `_SECRET` | kosong | Override client; tanpa nilai ini dibaca `gateway_client_id`/`gateway_client_secret` dari `OIDC_CONFIG_FILE` |
| `OIDC_CONFIG_FILE` | kosong | `oidc.json` hasil `auth-bootstrap` (volume `auth_client_config`, read-only) |
| `TOKEN_EXCHANGE_REQUESTED_TOKEN_TYPE` | `urn:ietf:params:oauth:token-type:jwt` | `requested_token_type` |
| `TOKEN_EXCHANGE_TIMEOUT_MS` | `5000` | Timeout panggilan ke Zitadel; timeout gRPC ext_authz Envoy harus lebih panjang |
| `TOKEN_EXCHANGE_GRPC_PORT` | `9000` | Listener gRPC internal; tidak dipublikasikan ke host |
| `LOG_LEVEL` | `info` | Filter log JSON |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | kosong (tanpa ekspor), `token-exchange` | Trace OTLP/HTTP. Tanpa endpoint span tetap dibuat (tidak diekspor), sehingga `traceparent` Envoy tetap diteruskan ke Zitadel |

Service gagal mulai bila konfigurasi tidak lengkap. Compose menjalankannya
setelah `auth-bootstrap` selesai, sehingga `oidc.json` sudah ada. Healthcheck
memakai gRPC health standar (`token-exchange --healthcheck`); panggilan health
tidak membuat span. Jumlah hasil exchange tersedia dari statistik ext_authz
Envoy (`http.gateway.ext_authz.ok`, `.denied`, `.error`).

## Pengujian

```bash
npx nx run token-exchange:test
npx nx run token-exchange:lint
npx nx run token-exchange:build
```
