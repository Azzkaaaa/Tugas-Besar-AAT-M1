# Build context: repository root (the COPY paths below are repository-relative).
#   docker build -f services/envoy/Dockerfile .

FROM envoyproxy/envoy:v1.38.6
RUN apt-get update \
  && apt-get install -y --no-install-recommends gettext-base \
  && rm -rf /var/lib/apt/lists/*
COPY services/envoy/templates /etc/envoy/templates
# No COPY --chmod: it needs BuildKit, and the legacy builder must work too.
COPY services/envoy/docker-entrypoint.sh /usr/local/bin/gateway-entrypoint.sh
RUN chmod 0755 /usr/local/bin/gateway-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/gateway-entrypoint.sh"]
CMD []
