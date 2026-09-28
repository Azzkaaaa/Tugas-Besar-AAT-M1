function envoy_on_request(request_handle)
  local host = request_handle:headers():get(":authority")
  if not host or not string.find(host, "api.localhost") then return end

  local path = request_handle:headers():get(":path")
  if not path then return end

  -- Route injection for seismic and volcanic streams
  local is_seismic = string.find(path, "^/api/v1/feeds/seismic")
  local is_volcanic = string.find(path, "^/api/v1/feeds/volcanic")

  if is_seismic or is_volcanic then
    path = string.gsub(path, "^/api/v1/feeds/[a-z]+", "/api/v1/feeds/events")
    local append_char = string.find(path, "?") and "&" or "?"
    local hazard_type = is_seismic and "SEISMIC" or "VOLCANIC"
    path = path .. append_char .. "hazard_type=" .. hazard_type
  end

  -- Query parameter flattening resolver
  if string.find(path, "?") then
    local uri_parts = {}
    for part in string.gmatch(path, "([^?]+)") do
      table.insert(uri_parts, part)
    end

    local base_path = uri_parts[1]
    local query_string = uri_parts[2]
    local new_queries = {}

    for k, v in string.gmatch(query_string, "([^&=]+)=([^&=]+)") do
      if k == "source" or k == "hazard_type" or k == "severity" then
        table.insert(new_queries, "filter." .. k .. "=" .. v)
      elseif k == "page_size" or k == "page_token" then
        table.insert(new_queries, "page." .. k .. "=" .. v)
      else
        table.insert(new_queries, k .. "=" .. v)
      end
    end

    path = base_path .. "?" .. table.concat(new_queries, "&")
  end

  request_handle:headers():replace(":path", path)
end

function envoy_on_response(response_handle)
  local path = response_handle:headers():get(":path")
  -- The transcoder will have rewritten the path to the gRPC method,
  -- so we match on the feed service's gRPC package structure.
  if not path or not string.find(path, "^/bnpb.feed.v1.EventFeedService/") then
    return
  end

  -- SSE Transcoding from NDJSON
  response_handle:headers():replace("content-type", "text/event-stream")
  response_handle:headers():replace("cache-control", "no-cache, no-transform")

  for chunk in response_handle:bodyChunks() do
    if chunk and chunk:length() > 0 then
      local json_str = chunk:getBytes(0, chunk:length())
      if string.find(json_str, "{") then
        local cursor = ""
        local _, _, match = string.find(json_str, '"resume_cursor"%s*:%s*"([^"]+)"')
        if match then cursor = match end

        local sse_payload = ""
        if cursor ~= "" then
          sse_payload = sse_payload .. "id: " .. cursor .. "\n"
        end
        sse_payload = sse_payload .. "event: hazard.upsert\n"
        sse_payload = sse_payload .. "data: " .. json_str .. "\n\n"

        chunk:setBytes(sse_payload)
      end
    end
  end
end
