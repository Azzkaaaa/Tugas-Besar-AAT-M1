# Envoy: edge proxy

Satu proses Envoy melayani seluruh lalu lintas publik pada `localhost:8080`:
SPA, Auth bridge, hosted login Zitadel, dan API. Envoy hanya mengautentikasi
request API lalu meneruskannya ke [api-gateway](../api-gateway/README.md), yang
mengimplementasikan kontrak REST dan memanggil service gRPC. Request API tidak
melewati Auth bridge.

| Host / prefix | Tujuan |
| --- | --- |
| `auth.localhost` | Zitadel; `/ui/v2/login` ke Login UI v2 |
| `localhost` `/auth/` | Auth bridge (login, callback, sesi, refresh, logout) |
| `localhost` `/api/` | Filter API (di bawah), lalu api-gateway |
| `localhost` `/` | SPA |

## Alur request API

```
browser ──cookie bnpb_access──► Envoy
  1. bnpb.cookie_to_bearer (Lua)   cookie → Authorization: Bearer, Origin/CSRF untuk mutasi
  2. bnpb.token_exchange (BOE)     RFC 8693 ke Zitadel: token opaque → JWT
  3. router                        api-gateway (HTTP/1.1, tanpa timeout route untuk SSE)
```

- **Cookie ke bearer.** Cookie tidak pernah diteruskan ke service. Request yang
  membawa `Authorization` sekaligus cookie `bnpb_*` ditolak 400. Mutasi berbasis
  cookie wajib membawa `Origin` aplikasi dan header `X-CSRF-Token`; halaman lintas
  origin tidak dapat mengirim header khusus tanpa preflight CORS, dan CORS tidak
  diaktifkan.
- **Token exchange.** Filter [`token-exchange`](https://builtonenvoy.io/extensions/token-exchange/)
  dari Built On Envoy v0.6.2 (dynamic module `libcomposer.so`, Envoy 1.38) memakai
  client `BNPB gateway token exchange` dan meminta
  `urn:ietf:params:oauth:token-type:jwt`. Token kedaluwarsa atau dicabut
  menghasilkan 401, sehingga SPA melakukan `/auth/refresh` lalu mengulang request.
  Zitadel tidak dapat dihubungi menghasilkan 502.
- **Scope.** JWT hasil exchange membawa klaim `role` dan `scopes` yang ditambahkan
  Zitadel melalui Actions v2 target
  [`zitadel-claims`](../zitadel-claims/README.md). Envoy hanya mengautentikasi;
  pemeriksaan scope per method (misalnya Media meminta `view=full`) menjadi tugas
  service backend.
- **Kontrak REST.** Transcoder gRPC-JSON dan filter SSE Lua telah dihapus karena
  ProtoJSON tidak sesuai OpenAPI (prefiks enum, pembungkus `oneof`, kode status,
  problem+json, framing SSE). Semua itu kini ditangani api-gateway.
- Error yang dibuat gateway memakai `application/problem+json`.

Pada `compose.auth.yaml` service bisnis tidak ada, sehingga request API yang lolos
autentikasi mendapat 503.

## Konfigurasi

`templates/envoy.yaml` dan `templates/lua/*.lua` adalah template `envsubst`.
[`docker-entrypoint.sh`](docker-entrypoint.sh) merender template ke
`/tmp/envoy` saat container mulai, lalu menjalankan Envoy. Hanya variabel yang
tercantum di skrip yang disubstitusi, dan container gagal mulai bila salah satunya
kosong. Nilai default ada di [`envoy.env.example`](envoy.env.example) dan anchor
`x-envoy-env` pada `compose.auth.yaml`.

| Kelompok | Variabel |
| --- | --- |
| Listener dan host | `ENVOY_LISTEN_PORT`, `APP_HOST`, `APP_ORIGIN`, `AUTH_HOST` |
| Prefix route | `AUTH_ROUTE_PREFIX`, `API_ROUTE_PREFIX`, `LOGIN_UI_ROUTE_PREFIX` |
| Upstream | `ZITADEL_*`, `ZITADEL_LOGIN_*`, `AUTH_BRIDGE_*`, `SPA_*`, `API_GATEWAY_*` (`_ADDRESS`, `_PORT`) |
| Token exchange | `TOKEN_EXCHANGE_URL`, `TOKEN_EXCHANGE_CLIENT_ID`, `TOKEN_EXCHANGE_CLIENT_SECRET` |
| Lua | `AUTH_ACCESS_COOKIE`, `AUTH_COOKIE_PREFIX` |
| Telemetri | `OTEL_COLLECTOR_ADDRESS`, `OTEL_COLLECTOR_GRPC_PORT` (OTLP/gRPC) |

Envoy mengirim trace (`envoy.tracers.opentelemetry`) dan metrics (stats sink
OpenTelemetry) ke cluster `otel_collector`. Span mencatat URL lengkap, sehingga
route `${AUTH_ROUTE_PREFIX}callback` (query berisi authorization code) memakai
`overall_sampling` 0. Pada stack Auth mandiri host `otel-lgtm` tidak ada; ekspor
dibuang tanpa memengaruhi routing.

Client ID/secret token exchange dibuat Zitadel saat provisioning. Entrypoint
membacanya dari `/bootstrap/oidc.json` (volume `auth_client_config`, read-only),
sehingga Envoy baru mulai setelah `auth-bootstrap` selesai.

Image dibangun dari root repository: tahap pertama membangun `libcomposer.so` dari
sumber BOE, tahap akhir memakai `envoyproxy/envoy:v1.38.6`.

## Nx

```bash
npx nx run envoy:build                # docker build -f services/envoy/Dockerfile .
npx nx run envoy:render               # render template ke services/envoy/dist
npx nx run envoy:validate             # envoy --mode validate dengan envoy.env.example
```
