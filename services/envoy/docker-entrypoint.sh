#!/bin/sh
# Renders the envsubst templates (envoy.yaml and Lua filters), then starts Envoy.
#   docker-entrypoint.sh [envoy args...]      render to $ENVOY_RENDER_DIR and run Envoy
#   docker-entrypoint.sh --render-only <dir>  render into <dir> and exit
set -eu

TEMPLATE_DIR=${ENVOY_TEMPLATE_DIR:-/etc/envoy/templates}
export ENVOY_RENDER_DIR="${ENVOY_RENDER_DIR:-/tmp/envoy}"

# Only these variables are substituted; any other `$` in the templates is kept.
VARS="ENVOY_LISTEN_PORT ENVOY_RENDER_DIR ENVOY_HEALTH_PATH
  APP_HOST APP_ORIGIN AUTH_HOST AUTH_ROUTE_PREFIX API_ROUTE_PREFIX LOGIN_UI_ROUTE_PREFIX
  ZITADEL_ADDRESS ZITADEL_PORT ZITADEL_LOGIN_ADDRESS ZITADEL_LOGIN_PORT
  AUTH_BRIDGE_ADDRESS AUTH_BRIDGE_PORT SPA_ADDRESS SPA_PORT
  API_GATEWAY_ADDRESS API_GATEWAY_PORT OTEL_COLLECTOR_ADDRESS OTEL_COLLECTOR_GRPC_PORT
  TOKEN_EXCHANGE_ADDRESS TOKEN_EXCHANGE_PORT TOKEN_EXCHANGE_TIMEOUT
  AUTH_ACCESS_COOKIE AUTH_COOKIE_PREFIX TRACE_ESCAPE_HATCH_ENABLED TRACE_ESCAPE_HATCH_HEADER"

missing=""
for name in $VARS; do
  eval "value=\${$name:-}"
  [ -n "$value" ] || missing="$missing $name"
done
if [ -n "$missing" ]; then
  echo "Envoy gateway: missing configuration:$missing" >&2
  exit 1
fi

render() {
  out=$1
  mkdir -p "$out/lua"
  formats=$(for name in $VARS; do printf '${%s} ' "$name"; done)
  envsubst "$formats" < "$TEMPLATE_DIR/envoy.yaml" > "$out/envoy.yaml"
  for script in "$TEMPLATE_DIR"/lua/*.lua; do
    envsubst "$formats" < "$script" > "$out/lua/$(basename "$script")"
  done
}

if [ "${1:-}" = "--render-only" ]; then
  render "${2:?--render-only requires an output directory}"
  exit 0
fi

render "$ENVOY_RENDER_DIR"
# The upstream image entrypoint drops privileges to the envoy user.
if [ -x /docker-entrypoint.sh ]; then
  exec /docker-entrypoint.sh envoy -c "$ENVOY_RENDER_DIR/envoy.yaml" "$@"
fi
exec envoy -c "$ENVOY_RENDER_DIR/envoy.yaml" "$@"
