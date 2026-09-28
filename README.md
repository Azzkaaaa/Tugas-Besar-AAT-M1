# IF4031 — Platform Koordinasi Kebencanaan, Milestone 1

## Requirements
- buf
- docker
- docker compose

## Development Setup
1. Build the grpc contract descriptor for envoy
```bash
buf build -o services/envoy/bnpb_descriptor.pb --exclude-imports=false
```
2. Build the grpc stub code for each service
```bash
buf generate
```
