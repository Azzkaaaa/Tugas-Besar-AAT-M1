-- Trace-header boundary of the edge. Rendered by envsubst.
-- Inbound trace context is removed earlier (early_header_mutation_extensions in envoy.yaml), so
-- every trace starts at Envoy. Responses never carry trace identifiers unless the development
-- escape hatch is enabled (TRACE_ESCAPE_HATCH_ENABLED=true) and the request sends its header;
-- then the client gets x-trace-id to look the request up in Grafana.
local HATCH_ENABLED = "${TRACE_ESCAPE_HATCH_ENABLED}" == "true"
local HATCH_HEADER = string.lower("${TRACE_ESCAPE_HATCH_HEADER}")
local METADATA_NAMESPACE = "bnpb.trace_headers"

local TRACE_HEADERS = {
  ["traceparent"] = true, ["tracestate"] = true, ["traceresponse"] = true, ["baggage"] = true,
  ["b3"] = true, ["uber-trace-id"] = true, ["x-ot-span-context"] = true, ["grpc-trace-bin"] = true,
  ["x-cloud-trace-context"] = true, ["x-amzn-trace-id"] = true,
  ["x-request-id"] = true, ["x-trace-id"] = true,
}
local TRACE_PREFIXES = { "x-b3-", "x-datadog-", "uberctx-" }

local function is_trace_header(name)
  if TRACE_HEADERS[name] then return true end
  for _, prefix in ipairs(TRACE_PREFIXES) do
    if string.sub(name, 1, #prefix) == prefix then return true end
  end
  return false
end

function envoy_on_request(handle)
  local headers = handle:headers()
  local asked = headers:get(HATCH_HEADER) ~= nil
  -- Upstreams never see the escape-hatch header, enabled or not.
  headers:remove(HATCH_HEADER)
  if HATCH_ENABLED and asked then
    handle:streamInfo():dynamicMetadata():set(METADATA_NAMESPACE, "leak", true)
  end
end

function envoy_on_response(handle)
  local metadata = handle:streamInfo():dynamicMetadata():get(METADATA_NAMESPACE)
  if metadata ~= nil and metadata["leak"] then return end
  local headers = handle:headers()
  -- Collect first: removing while iterating is undefined.
  local names = {}
  for name, _ in pairs(headers) do
    if is_trace_header(name) then names[#names + 1] = name end
  end
  for _, name in ipairs(names) do headers:remove(name) end
end
