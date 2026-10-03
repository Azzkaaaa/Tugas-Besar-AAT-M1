FROM golang:1.24-alpine AS builder
WORKDIR /app

# for go mod download and certificates
RUN apk add --no-cache git ca-certificates

COPY services/aggregator/go.mod services/aggregator/go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o aggregator ./services/aggregator/cmd/main.go

###
FROM alpine:3.20 AS runtime
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /app/aggregator ./aggregator

EXPOSE 9091

ENTRYPOINT ["./aggregator"]
