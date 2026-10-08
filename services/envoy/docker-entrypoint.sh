#!/bin/sh
# Renders the envsubst templates (envoy.yaml and Lua filters), then starts Envoy.
#   docker-entrypoint.sh [envoy args...]      render to $ENVOY_RENDER_DIR and run Envoy
#   docker-entrypoint.sh --render-only <dir>  render into <dir> and exit
set -eu

TEMPLATE_DIR=${ENVOY_TEMPLATE_DIR:-/etc/envoy/templates}
OIDC_CONFIG_FILE=${OIDC_CONFIG_FILE:-/bootstrap/oidc.json}
export ENVOY_RENDER_DIR="${ENVOY_RENDER_DIR:-/tmp/envoy}"
export ENVOY_DESCRIPTOR_PATH="${ENVOY_DESCRIPTOR_PATH:-/etc/envoy/bnpb_descriptor.pb}"

# Only these variables are substituted; any other `$` in the templates is kept.
VARS="ENVOY_LISTEN_PORT ENVOY_RENDER_DIR ENVOY_DESCRIPTOR_PATH
  APP_HOST APP_ORIGIN AUTH_HOST AUTH_ROUTE_PREFIX API_ROUTE_PREFIX LOGIN_UI_ROUTE_PREFIX
  ZITADEL_ADDRESS ZITADEL_PORT ZITADEL_LOGIN_ADDRESS ZITADEL_LOGIN_PORT
  AUTH_BRIDGE_ADDRESS AUTH_BRIDGE_PORT SPA_ADDRESS SPA_PORT
  AGGREGATOR_ADDRESS AGGREGATOR_PORT WEBHOOK_ADDRESS WEBHOOK_PORT
  NOTIFICATION_ADDRESS NOTIFICATION_PORT FEED_ADDRESS FEED_PORT
  TOKEN_EXCHANGE_URL TOKEN_EXCHANGE_CLIENT_ID TOKEN_EXCHANGE_CLIENT_SECRET
  AUTH_ACCESS_COOKIE AUTH_COOKIE_PREFIX"

# Reads a top-level string from the provisioned oidc.json (one key per line, no jq needed).
oidc_value() {
  sed -n "s/^[[:space:]]*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$OIDC_CONFIG_FILE" | head -n 1
}

# The gateway client is created by auth-bootstrap; explicit env values win (render-only/validate).
if [ -r "$OIDC_CONFIG_FILE" ]; then
  : "${TOKEN_EXCHANGE_CLIENT_ID:=$(oidc_value gateway_client_id)}"
  : "${TOKEN_EXCHANGE_CLIENT_SECRET:=$(oidc_value gateway_client_secret)}"
  export TOKEN_EXCHANGE_CLIENT_ID TOKEN_EXCHANGE_CLIENT_SECRET
fi

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
