# Build context: repository root (the COPY paths below are repository-relative).
#   docker build -f services/envoy/Dockerfile .

# Built On Envoy composer: Go dynamic module that embeds the token-exchange filter.
# BOE v0.6.2 requires Envoy >= 1.38.0 and < 1.39.
FROM golang:1.26.4-bookworm AS composer
ARG BOE_VERSION=v0.6.2
RUN git clone --depth 1 --branch ${BOE_VERSION} https://github.com/tetratelabs/built-on-envoy /src
WORKDIR /src/extensions/composer
RUN go mod download
RUN CGO_ENABLED=1 go build -trimpath -buildmode=c-shared -o /out/libcomposer.so ./main

FROM envoyproxy/envoy:v1.38.6
RUN apt-get update \
  && apt-get install -y --no-install-recommends gettext-base \
  && rm -rf /var/lib/apt/lists/*
COPY --from=composer /out/libcomposer.so /usr/local/lib/envoy/libcomposer.so
COPY services/envoy/templates /etc/envoy/templates
# No COPY --chmod: it needs BuildKit, and the legacy builder must work too.
COPY services/envoy/docker-entrypoint.sh /usr/local/bin/gateway-entrypoint.sh
RUN chmod 0755 /usr/local/bin/gateway-entrypoint.sh
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib/envoy \
    GODEBUG=cgocheck=0
ENTRYPOINT ["/usr/local/bin/gateway-entrypoint.sh"]
CMD []
