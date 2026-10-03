FROM golang:1.26-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY packages/bnpb-aggregator-go packages/bnpb-aggregator-go
COPY services/webhook-provider services/webhook-provider

WORKDIR /app/services/webhook-provider
# Pure-Go SQLite: CGO_ENABLED=0 is safe with modernc.org/sqlite
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/webhook-provider ./cmd/main.go

###
FROM alpine:3.20 AS runtime
WORKDIR /app

RUN apk add --no-cache ca-certificates
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Data directory for SQLite database file
RUN mkdir -p /data && chown appuser:appgroup /data

USER appuser

COPY --from=builder /app/webhook-provider ./webhook-provider

EXPOSE 9094

ENTRYPOINT ["./webhook-provider"]
