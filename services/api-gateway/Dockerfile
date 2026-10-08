# Build context: repository root (contracts/ and packages/ are needed).
#   docker build -f services/api-gateway/Dockerfile .
FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY contracts/openapi contracts/openapi
COPY packages/bnpb-aggregator-go packages/bnpb-aggregator-go
COPY packages/bnpb-gateway-go packages/bnpb-gateway-go
COPY services/api-gateway/go.mod services/api-gateway/go.sum services/api-gateway/
RUN cd services/api-gateway && go mod download

# gateway.gen.go is not committed: generate it from the contract (oapi-codegen is a Go tool
# dependency of the package, so the version is pinned by its go.mod).
RUN cd packages/bnpb-gateway-go \
    && go tool oapi-codegen -config oapi-codegen.yaml ../../contracts/openapi/gateway.yaml

COPY services/api-gateway services/api-gateway
WORKDIR /app/services/api-gateway
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /app/api-gateway ./cmd

FROM alpine:3.20
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser
COPY --from=builder /app/api-gateway /api-gateway
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=5s --retries=20 CMD ["/api-gateway", "--healthcheck"]
ENTRYPOINT ["/api-gateway"]
