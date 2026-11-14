FROM golang:1.26-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY packages/bnpb-aggregator-go packages/bnpb-aggregator-go
COPY services/aggregator services/aggregator

WORKDIR /app/services/aggregator
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/aggregator ./cmd/main.go && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/storecopy ./cmd/storecopy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/storeadmin ./cmd/storeadmin

###
FROM alpine:3.24 AS runtime
WORKDIR /app

RUN apk upgrade -U -a && apk add --no-cache ca-certificates
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /app/aggregator ./aggregator
# One-off Postgres -> Cassandra copy (see services/aggregator/README.md).
COPY --from=builder /app/storecopy ./storecopy
# Live store administration, e.g. changing the Cassandra bucket period.
COPY --from=builder /app/storeadmin ./storeadmin

EXPOSE 9091

ENTRYPOINT ["./aggregator"]
